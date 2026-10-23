# rl_limiter.fleetweb —— 总控制台的发布枢纽与 HTTP 面。
#
# 结构照抄节点侧 webconsole（同一套纪律，便于两边对照维护）：
#   - FleetHub ≙ StatusHub：轮询环每拍喂一帧 fleet 快照 → 留存历史 +
#     广播给 SSE 订阅者；overview/history 是 REST 拉取口径；
#   - build_fleet_app ≙ build_app：静态页 + 只读 API + 写 API（总带宽，
#     令牌鉴权，回写 fleet YAML 热生效）；
#   - run_fleet_console ≙ run_console：出厂默认绑 0.0.0.0（fleetmain 的
#     DEFAULT_CONSOLE_BIND），绑非回环打 WARN 提醒防火墙限源。
#
# 安全边界与节点侧相同：读接口无鉴权（节点清单里的 token **绝不出现**
# 在任何读接口里）；写接口必须令牌。

from __future__ import annotations

import asyncio
import collections
import hmac
import json
import logging
import time
from pathlib import Path
from typing import Any, Callable

from aiohttp import web

from . import fleetconfig
from .webconsole import HISTORY_TICKS, LOGS_PAGE_LIMIT, LogBuffer  # noqa: F401

_STATIC_DIR = Path(__file__).parent / "static"

SUBSCRIBER_QUEUE_DEPTH = 5


def fleet_snapshot(now: float, states: list, total_mbps: float) -> dict:
    """把一轮轮询的节点状态摊成 fleet 快照（页面/SSE 的一帧）。

    单位一律 Mbps（总控的量本来就以 Mbps 流转，与节点 regulation 段
    同一口径）。allocated_mbps = Σ分配 + 冻结记账——它 ≈ 总带宽是总量
    守恒的现场证据，页面拿它和 total 对照展示。
    """
    nodes: dict[str, Any] = {}
    egress = ingress = allocated = 0.0
    online = 0
    for s in states:
        if s.online:
            online += 1
            egress += s.egress_mbps
            ingress += s.ingress_mbps
        if s.desired_mbps is not None:
            allocated += s.desired_mbps
        elif s.quota_mbps:
            allocated += s.quota_mbps          # 冻结节点按自报值记账
        util = (round(s.egress_mbps / s.quota_mbps, 3)
                if s.online and s.quota_mbps else None)
        nodes[s.name] = {
            "online": s.online,
            "degraded": s.degraded,
            "executable": s.executable,
            "egress_mbps": round(s.egress_mbps, 2),
            "ingress_mbps": round(s.ingress_mbps, 2),
            "quota_mbps": s.quota_mbps,
            "desired_mbps": s.desired_mbps,
            "util": util,
            "landed": s.landed,
            "console_url": s.console_url,
            "controllable": bool(s.token),
            "error": s.error,
            "write_err": s.write_err,
        }
    return {
        "ts": now,
        "total_quota_mbps": total_mbps,
        "egress_mbps": round(egress, 2),
        "ingress_mbps": round(ingress, 2),
        "allocated_mbps": round(allocated, 1),
        "nodes_online": online,
        "nodes_total": len(states),
        "nodes": nodes,
    }


class FleetHub:
    """总控实时数据的发布枢纽（单事件循环内使用，无锁）。"""

    def __init__(self, service_version: str,
                 total_fn: Callable[[], float],
                 params_fn: Callable[[], dict]):
        self._service_version = service_version
        # 当前总带宽与分配参数的延迟求值闭包（配置热更后自动跟上）。
        self._total_fn = total_fn
        self._params_fn = params_fn
        self._history: collections.deque = collections.deque(
            maxlen=HISTORY_TICKS)
        self._subs: set = set()
        self._started = time.time()

    def record(self, snap: dict) -> None:
        self._history.append(snap)
        for q in list(self._subs):
            if q.full():
                try:
                    q.get_nowait()
                except asyncio.QueueEmpty:
                    pass
            q.put_nowait(snap)

    def subscribe(self) -> asyncio.Queue:
        q: asyncio.Queue = asyncio.Queue(SUBSCRIBER_QUEUE_DEPTH)
        self._subs.add(q)
        return q

    def unsubscribe(self, q: asyncio.Queue) -> None:
        self._subs.discard(q)

    def close(self) -> None:
        """停机：叫醒并结束全部 SSE 订阅者（同 StatusHub.close）。"""
        for q in list(self._subs):
            if q.full():
                try:
                    q.get_nowait()
                except asyncio.QueueEmpty:
                    pass
            q.put_nowait(None)

    def overview(self) -> dict:
        return {
            "service_version": self._service_version,
            "uptime_s": time.time() - self._started,
            "total_quota_mbps": self._total_fn(),
            # 分配参数的只读视图（页面"算法参数"折叠块）。
            "allocation": self._params_fn(),
            "latest": self._history[-1] if self._history else None,
        }

    def history(self) -> list:
        return list(self._history)


def build_fleet_app(
    hub: FleetHub,
    logbuf: LogBuffer,
    log: logging.Logger,
    yaml_path: str = "",
    api_token: str = "",
    poke: "asyncio.Event | None" = None,
) -> web.Application:
    """组装总控的 aiohttp 应用。写 API 只有一个：总带宽（回写 fleet
    YAML，total_egress_mbps 是配置里唯一开放给写 API 的字段）。"""

    index_html = (_STATIC_DIR / "fleet.html").read_bytes()

    async def handle_index(_request: web.Request) -> web.Response:
        return web.Response(body=index_html, content_type="text/html",
                            charset="utf-8")

    async def handle_overview(_request: web.Request) -> web.Response:
        return web.json_response(hub.overview())

    async def handle_history(_request: web.Request) -> web.Response:
        return web.json_response({"snapshots": hub.history()})

    async def handle_logs(request: web.Request) -> web.Response:
        try:
            after = int(request.query.get("after", "0"))
        except ValueError:
            after = 0
        return web.json_response({"logs": logbuf.since(after)})

    async def handle_stream(request: web.Request) -> web.StreamResponse:
        resp = web.StreamResponse(headers={
            "Content-Type": "text/event-stream; charset=utf-8",
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        })
        await resp.prepare(request)
        q = hub.subscribe()
        try:
            latest = hub.overview()["latest"]
            if latest is not None:
                await resp.write(
                    b"data: " + json.dumps(latest).encode() + b"\n\n")
            while True:
                snap = await q.get()
                if snap is None:
                    break
                await resp.write(
                    b"data: " + json.dumps(snap).encode() + b"\n\n")
        except (ConnectionResetError, ConnectionError, OSError):
            pass
        finally:
            hub.unsubscribe(q)
        return resp

    write_lock = asyncio.Lock()

    def _authorized(request: web.Request) -> "web.Response | None":
        if not api_token:
            return web.json_response(
                {"error": "未配置 RL_API_TOKEN，写接口已禁用——在 rl-fleet"
                          " 的环境变量里设置令牌后重启即可启用"},
                status=403)
        got = ""
        auth = request.headers.get("Authorization", "")
        if auth.startswith("Bearer "):
            got = auth[len("Bearer "):].strip()
        if not got:
            got = request.headers.get("X-API-Token", "").strip()
        if not got or not hmac.compare_digest(got, api_token):
            return web.json_response(
                {"error": "令牌缺失或不正确（请求头 Authorization: "
                          "Bearer <token> 或 X-API-Token: <token>）"},
                status=401)
        return None

    async def handle_put_total(request: web.Request) -> web.Response:
        denied = _authorized(request)
        if denied is not None:
            return denied
        try:
            body = await request.json()
        except Exception:
            return web.json_response(
                {"error": "请求体必须是 JSON（{\"quota_mbps\": <数字>}）"},
                status=400)
        v = body.get("quota_mbps") if isinstance(body, dict) else None
        if isinstance(v, bool) or not isinstance(v, (int, float)) or v < 0:
            return web.json_response(
                {"error": f"quota_mbps 必须是 ≥0 的数字（Mbps；0 = 停止"
                          f"分配，纯观察），当前值 {v!r}"}, status=400)
        async with write_lock:
            try:
                await asyncio.to_thread(
                    fleetconfig.set_total_quota, yaml_path, float(v))
            except ValueError as e:
                return web.json_response({"error": str(e)}, status=400)
        if poke is not None:
            poke.set()
        log.warning("写 API 已修改总流出带宽并回写 YAML（下一个分配周期"
                    "生效） total=%gMbps yaml=%s", v, yaml_path)
        return web.json_response({"ok": True, "total_egress_mbps": float(v)})

    async def handle_delete_total(request: web.Request) -> web.Response:
        denied = _authorized(request)
        if denied is not None:
            return denied
        async with write_lock:
            try:
                await asyncio.to_thread(
                    fleetconfig.set_total_quota, yaml_path, 0.0)
            except ValueError as e:
                return web.json_response({"error": str(e)}, status=400)
        if poke is not None:
            poke.set()
        log.warning("写 API 已取消总量控制（纯观察模式：只汇总不下发；"
                    "各节点保持当前配额）yaml=%s", yaml_path)
        return web.json_response({"ok": True, "total_egress_mbps": 0})

    app = web.Application()
    app.router.add_get("/", handle_index)
    app.router.add_get("/api/overview", handle_overview)
    app.router.add_get("/api/history", handle_history)
    app.router.add_get("/api/logs", handle_logs)
    app.router.add_get("/api/stream", handle_stream)
    if yaml_path:
        app.router.add_put("/api/total-quota", handle_put_total)
        app.router.add_delete("/api/total-quota", handle_delete_total)
    return app


async def run_fleet_console(
    port: int,
    hub: FleetHub,
    logbuf: LogBuffer,
    log: logging.Logger,
    bind: str = "127.0.0.1",
    yaml_path: str = "",
    api_token: str = "",
    poke: "asyncio.Event | None" = None,
) -> None:
    """常驻任务：启动总控 HTTP 服务并挂起到被取消（同 run_console）。"""
    runner = web.AppRunner(
        build_fleet_app(hub, logbuf, log, yaml_path=yaml_path,
                        api_token=api_token, poke=poke),
        access_log=None)
    await runner.setup()
    site = web.TCPSite(runner, bind, port)
    await site.start()
    log.info("总控制台已启动 bind=%s port=%d 写API=%s", bind, port,
             ("已启用（令牌鉴权）" if yaml_path and api_token
              else "已禁用（未配置 RL_API_TOKEN）" if yaml_path
              else "未挂载"))
    if bind not in ("127.0.0.1", "::1", "localhost"):
        log.warning(
            "总控制台绑定在非回环地址上，而**读接口没有任何鉴权**"
            "（暴露全部节点的监控汇总与运行日志）——请确认该地址只在"
            "内网且已由防火墙/安全组限制来源 bind=%s port=%d", bind, port)
    try:
        await asyncio.Event().wait()
    finally:
        hub.close()
        await runner.cleanup()
