# hap_agg.web —— Web 视图与 API。
#
#   - 读接口无鉴权，默认只绑回环，放内网靠防火墙；
#   - 写接口（批量导入/删除目标）必须带令牌（环境变量 HAP_AGG_TOKEN，
#     请求头 Authorization: Bearer 或 X-API-Token；未配置则写接口整体
#     403）；
#   - 目标清单的修改立即生效（下一拍就开始采样）并回写 YAML；
#   - SSE 停机哨兵：hub.close() 叫醒挂着的流，优雅停机不被在途请求卡住。

from __future__ import annotations

import asyncio
import collections
import hmac
import json
import logging
import time
from pathlib import Path
from typing import Any

from aiohttp import web

from . import config as configmod
from . import sampler as samplermod

# 快照留存拍数：1s 一拍 ≈ 10 分钟窗口。
HISTORY_TICKS = 600
SUBSCRIBER_QUEUE_DEPTH = 5

_STATIC_DIR = Path(__file__).resolve().parent / "static"


class AggHub:
    """聚合快照的发布枢纽（历史 + SSE 广播），单事件循环内使用。"""

    def __init__(self, service_version: str):
        self._service_version = service_version
        self._history: collections.deque[dict[str, Any]] = collections.deque(
            maxlen=HISTORY_TICKS)
        self._subs: set[asyncio.Queue] = set()
        self._started = time.time()

    def record(self, snap: dict[str, Any]) -> None:
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
        """停机哨兵：叫醒并结束所有 SSE 流（同 webconsole 的教训——
        不做这一步，优雅停机会被挂着的 /api/stream 卡到超时）。"""
        for q in list(self._subs):
            if q.full():
                try:
                    q.get_nowait()
                except asyncio.QueueEmpty:
                    pass
            q.put_nowait(None)

    def latest(self) -> "dict[str, Any] | None":
        return self._history[-1] if self._history else None

    def history(self) -> list[dict[str, Any]]:
        return list(self._history)

    def overview(self, aggregator: samplermod.Aggregator) -> dict[str, Any]:
        return {
            "service_version": self._service_version,
            "uptime_s": time.time() - self._started,
            "targets": [{"name": t.name, "addr": t.addr}
                        for t in aggregator.targets()],
            "latest": self.latest(),
        }


def _prom_escape(v: str) -> str:
    return v.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")


def render_prometheus(hub: AggHub, aggregator: samplermod.Aggregator) -> str:
    """聚合视图的 Prometheus 文本格式（全部 gauge，取最新一拍）。"""
    o = hub.overview(aggregator)
    lines: list[str] = []

    def m(name: str, value, help_: str = "", labels: "dict | None" = None):
        if help_:
            lines.append(f"# HELP {name} {help_}")
            lines.append(f"# TYPE {name} gauge")
        lab = ""
        if labels:
            lab = "{" + ",".join(
                f'{k}="{_prom_escape(str(v))}"'
                for k, v in labels.items()) + "}"
        lines.append(f"{name}{lab} {value}")

    m("hap_agg_info", 1, "聚合器元信息（值恒为 1）",
      {"version": o["service_version"]})
    m("hap_agg_targets", len(o["targets"]), "已登记的目标数")

    snap = o.get("latest")
    if not snap:
        return "\n".join(lines) + "\n"

    total = snap["total"]
    m("hap_agg_targets_up", total["targets_ok"], "本拍采样成功的目标数")
    m("hap_agg_total_connections", total["conn"],
      "全部健康目标的整机并发连接合计（show info CurrConns）")
    m("hap_agg_total_frontend_connections", total["fe_conn"],
      "全部健康目标的 frontend 并发连接合计")
    m("hap_agg_total_rate_in_bytes_per_second",
      round(total["rate_in_bytes_per_s"], 1), "聚合上行速率（应用层）")
    m("hap_agg_total_rate_out_bytes_per_second",
      round(total["rate_out_bytes_per_s"], 1), "聚合下行速率（应用层）")
    m("hap_agg_total_new_connections_per_second",
      round(total["conn_new_ps"], 2), "聚合每秒新建连接数")
    m("hap_agg_total_denied_per_second",
      round(total["denied_ps"], 2), "聚合每秒被拒绝连接数")

    tgt_metrics = [
        ("conn", "hap_agg_target_connections", "整机并发连接（CurrConns）",
         False),
        ("max_conn", "hap_agg_target_max_connections", "进程连接上限", False),
        ("idle_pct", "hap_agg_target_idle_percent", "HAProxy 自报空闲率",
         False),
        ("uptime_s", "hap_agg_target_uptime_seconds", "目标进程运行秒数",
         False),
        ("rate_in_bytes_per_s", "hap_agg_target_rate_in_bytes_per_second",
         "上行速率（Σ frontend）", True),
        ("rate_out_bytes_per_s", "hap_agg_target_rate_out_bytes_per_second",
         "下行速率（Σ frontend）", True),
        ("conn_new_ps", "hap_agg_target_new_connections_per_second",
         "每秒新建连接数", True),
        ("denied_ps", "hap_agg_target_denied_per_second",
         "每秒被拒绝连接数", True),
    ]
    for name in sorted(snap["targets"]):
        v = snap["targets"][name]
        labels = {"target": name, "addr": v["addr"]}
        if name == sorted(snap["targets"])[0]:
            m("hap_agg_target_up", int(v["ok"]),
              "该目标本拍采样是否成功（0 时其余指标为陈旧值）", labels)
        else:
            m("hap_agg_target_up", int(v["ok"]), "", labels)
    for key, pname, help_, rnd in tgt_metrics:
        first = True
        for name in sorted(snap["targets"]):
            v = snap["targets"][name]
            val = v.get(key, 0)
            m(pname, round(val, 2) if rnd else val,
              help_ if first else "", {"target": name, "addr": v["addr"]})
            first = False

    fe_metrics = [
        ("rate_out_bytes_per_s", "hap_agg_frontend_rate_out_bytes_per_second",
         "跨机合并的 frontend 下行速率"),
        ("rate_in_bytes_per_s", "hap_agg_frontend_rate_in_bytes_per_second",
         "跨机合并的 frontend 上行速率"),
        ("conn", "hap_agg_frontend_connections", "跨机合并的并发连接数"),
        ("conn_new_ps", "hap_agg_frontend_new_connections_per_second",
         "跨机合并的每秒新建连接数"),
        ("denied_ps", "hap_agg_frontend_denied_per_second",
         "跨机合并的每秒被拒绝连接数"),
        ("targets", "hap_agg_frontend_targets", "有这个 frontend 的目标数"),
    ]
    for key, pname, help_ in fe_metrics:
        first = True
        for fname in sorted(snap["frontends"]):
            fv = snap["frontends"][fname]
            val = fv.get(key, 0)
            m(pname, round(val, 2) if isinstance(val, float) else val,
              help_ if first else "", {"frontend": fname})
            first = False
    return "\n".join(lines) + "\n"


def build_app(
    hub: AggHub,
    aggregator: samplermod.Aggregator,
    log: logging.Logger,
    yaml_path: str = "",
    api_token: str = "",
) -> web.Application:
    """组装 hap-agg 的 aiohttp 应用。

    写接口（批量导入/删除目标）只在 yaml_path 非空时挂载
    （令牌 HAP_AGG_TOKEN；未配置则 403）。
    """
    index_html = (_STATIC_DIR / "index.html").read_bytes()

    async def handle_index(_request: web.Request) -> web.Response:
        return web.Response(body=index_html, content_type="text/html",
                            charset="utf-8")

    async def handle_overview(_request: web.Request) -> web.Response:
        return web.json_response(hub.overview(aggregator))

    async def handle_history(_request: web.Request) -> web.Response:
        return web.json_response({"snapshots": hub.history()})

    async def handle_metrics(_request: web.Request) -> web.Response:
        return web.Response(text=render_prometheus(hub, aggregator),
                            content_type="text/plain", charset="utf-8")

    async def handle_stream(request: web.Request) -> web.StreamResponse:
        resp = web.StreamResponse(headers={
            "Content-Type": "text/event-stream; charset=utf-8",
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        })
        await resp.prepare(request)
        q = hub.subscribe()
        try:
            latest = hub.latest()
            if latest is not None:
                await resp.write(
                    b"data: " + json.dumps(latest).encode() + b"\n\n")
            while True:
                snap = await q.get()
                if snap is None:           # 停机哨兵（见 AggHub.close）
                    break
                await resp.write(
                    b"data: " + json.dumps(snap).encode() + b"\n\n")
        except (ConnectionResetError, ConnectionError, OSError):
            pass
        finally:
            hub.unsubscribe(q)
        return resp

    # ---- 写接口：目标清单管理 ----

    write_lock = asyncio.Lock()

    def _authorized(request: web.Request) -> "web.Response | None":
        if not api_token:
            return web.json_response(
                {"error": "未配置 HAP_AGG_TOKEN，写接口已禁用——在服务的"
                          "环境变量里设置令牌后重启即可启用"},
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

    async def handle_import_targets(request: web.Request) -> web.Response:
        """批量导入：body {"text": "IP:port\\n名字 IP:port\\n…"}。

        全部行解析通过才落地（一行错整批拒绝——半批导入比报错更难
        收拾）；与现有目标合并（同名同址幂等跳过，同名异址 400）。
        """
        denied = _authorized(request)
        if denied is not None:
            return denied
        try:
            body = await request.json()
        except Exception:
            return web.json_response(
                {"error": "请求体必须是 JSON（{\"text\": \"IP:port 每行"
                          "一条\"}）"}, status=400)
        text = body.get("text") if isinstance(body, dict) else None
        if not isinstance(text, str) or not text.strip():
            return web.json_response(
                {"error": "请求体缺少 text 字段（IP:port 每行一条，"
                          "可选 '名字 IP:port' 写法，# 为注释）"},
                status=400)
        async with write_lock:
            try:
                new = samplermod.parse_targets_text(text)
                added = aggregator.add_targets(new)
                await asyncio.to_thread(
                    configmod.save_targets, yaml_path, aggregator.targets())
            except (samplermod.TargetError, ValueError) as e:
                return web.json_response({"error": str(e)}, status=400)
        if added:
            log.warning("批量导入了 %d 个聚合目标并回写 YAML added=%s "
                        "yaml=%s", len(added), ",".join(added), yaml_path)
        return web.json_response({
            "ok": True, "added": added,
            "total": len(aggregator.targets()),
        })

    async def handle_delete_target(request: web.Request) -> web.Response:
        denied = _authorized(request)
        if denied is not None:
            return denied
        name = request.match_info["name"]
        async with write_lock:
            removed = aggregator.remove_target(name)
            if removed:
                try:
                    await asyncio.to_thread(
                        configmod.save_targets, yaml_path,
                        aggregator.targets())
                except ValueError as e:
                    return web.json_response({"error": str(e)}, status=400)
        if removed:
            log.warning("已删除聚合目标并回写 YAML target=%s yaml=%s",
                        name, yaml_path)
        return web.json_response({"ok": True, "name": name,
                                  "removed": removed})

    app = web.Application()
    app.router.add_get("/", handle_index)
    app.router.add_get("/api/overview", handle_overview)
    app.router.add_get("/api/history", handle_history)
    app.router.add_get("/api/stream", handle_stream)
    app.router.add_get("/metrics", handle_metrics)
    if yaml_path:
        app.router.add_post("/api/targets", handle_import_targets)
        app.router.add_delete("/api/targets/{name}", handle_delete_target)
    return app


async def run_web(
    port: int,
    hub: AggHub,
    aggregator: samplermod.Aggregator,
    log: logging.Logger,
    bind: str = "127.0.0.1",
    yaml_path: str = "",
    api_token: str = "",
) -> None:
    """常驻任务：起 HTTP 服务，取消时先掐 SSE 再回收。"""
    runner = web.AppRunner(
        build_app(hub, aggregator, log, yaml_path=yaml_path,
                  api_token=api_token),
        access_log=None)
    await runner.setup()
    site = web.TCPSite(runner, bind, port)
    await site.start()
    log.info("hap-agg Web 视图已启动 bind=%s port=%d 写API=%s", bind, port,
             ("已启用（令牌鉴权）" if yaml_path and api_token
              else "已禁用（未配置 HAP_AGG_TOKEN）" if yaml_path
              else "未挂载"))
    if bind not in ("127.0.0.1", "::1", "localhost"):
        log.warning(
            "hap-agg 绑定在非回环地址上，而**读接口没有任何鉴权**（暴露"
            "全部目标的监控数据）——请确认该地址只在内网且已由防火墙/"
            "安全组限制来源 bind=%s port=%d", bind, port)
    try:
        await asyncio.Event().wait()
    finally:
        hub.close()
        await runner.cleanup()
