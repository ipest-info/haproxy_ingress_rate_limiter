# rl_limiter.webconsole —— 内置 Web 控制台：实时观测 + 在线调参。
#
# 设计取向（与 v2.0 集中式架构一致）：
#
#   - 观测侧零额外采集：快环每拍本就产出各环境的速率/均值/连接数/整形值/
#     AIMD 状态，控制台只是把这份内存数据经 StatusHub 留存最近几分钟并
#     以 SSE 推给页面——不引入第二条采集链路，页面看到的就是决策依据；
#   - 调参侧写库不写内存：所有修改（配额、模式、AIMD 参数）UPDATE 到
#     MySQL（配置唯一事实源），由 dbconfig.watch 的既有轮询链路热生效。
#     好处：页面与库永远一致、重启不丢、复用统一校验管线；代价是修改
#     有一个轮询周期（默认几秒）的生效延迟，接口应答里明确告知。
#     未启用数据库配置模式（纯 YAML 部署）时调参接口返回 409 说明原因；
#   - 日志经进程内环形缓冲（LogBuffer 挂在根 logger 上）曝光最近若干条，
#     页面增量拉取；生产量级的持久化检索交给外部日志系统，不在此造轮子；
#   - "限速生效证据"：页面上把 实时速率 / 10s 均值（计费口径）/ 整形值 /
#     配额 画在同一条时间轴上，配合 AIMD 状态与收紧次数——曲线被压在
#     配额线下即是效果本身，无需引入口径含混的"丢包数"。
#
# 安全边界：控制台无鉴权，定位与 HAProxy 的 stats socket 相同——只允许
# 绑定内网/受防火墙保护的端口（docker compose 演示中仅映射到宿主机）。

from __future__ import annotations

import asyncio
import collections
import json
import logging
import time
from pathlib import Path
from typing import Any, Callable

from aiohttp import web

from . import dbconfig, model

# 快照留存拍数：1s 一拍即约 10 分钟窗口，页面刷新后能立即回填完整曲线。
HISTORY_TICKS = 600
# 日志环形缓冲容量。
LOG_BUFFER_SIZE = 1000
# 单次 /api/logs 返回上限。
LOGS_PAGE_LIMIT = 500
# SSE 订阅队列深度：消费慢时丢最旧一帧（页面只关心最新状态）。
SUBSCRIBER_QUEUE_DEPTH = 5
# 调参请求体上限。
MAX_BODY_BYTES = 64 * 1024

_STATIC_DIR = Path(__file__).resolve().parent / "static"


class LogBuffer(logging.Handler):
    """环形日志缓冲：挂在根 logger 上留存最近 N 条结构化日志。

    seq 单调递增，页面用 ?after=<seq> 增量拉取；单事件循环 + logging
    的 Handler 锁保护，emit 里只做 O(1) 追加。
    """

    def __init__(self, capacity: int = LOG_BUFFER_SIZE):
        super().__init__()
        self._buf: collections.deque[dict[str, Any]] = collections.deque(
            maxlen=capacity)
        self._seq = 0

    def emit(self, record: logging.LogRecord) -> None:
        try:
            msg = record.getMessage()
        except Exception:  # 格式化失败不能反过来砸崩业务日志
            msg = str(record.msg)
        self._seq += 1
        self._buf.append({
            "seq": self._seq,
            "ts": record.created,
            "level": record.levelname,
            "logger": record.name,
            "msg": msg,
        })

    def since(self, after: int) -> list[dict[str, Any]]:
        """返回 seq > after 的日志（最多 LOGS_PAGE_LIMIT 条，从旧到新）。"""
        return [e for e in self._buf if e["seq"] > after][:LOGS_PAGE_LIMIT]


class StatusHub:
    """快环实时数据的发布枢纽（单事件循环内使用，无锁）。

    - record：快环 sampler 每拍调用，合成快照 → 留存历史 + 广播给 SSE 订阅者；
    - update_config：配置（引导或热更）经过时调用，缓存配额/挂载点/参数视图，
      使快照能携带 quota 供页面画配额参考线；
    - overview/history：REST 拉取口径。
    """

    def __init__(
        self,
        service_version: str,
        mode_fn: Callable[[], str],
        version_fn: Callable[[], int],
        nodes: list[model.NodeConfig] | None = None,
        degraded_fn: Callable[[], set] | None = None,
    ):
        self._service_version = service_version
        self._mode_fn = mode_fn
        self._version_fn = version_fn
        # 受控节点接线视图（启动时定型，与 RuntimeClient 集合一致）。
        self._nodes = list(nodes or [])
        # 采样已持续失败的节点集合（collector.degraded_nodes 闭包）。
        self._degraded_fn = degraded_fn if degraded_fn is not None else (lambda: set())
        self._history: collections.deque[dict[str, Any]] = collections.deque(
            maxlen=HISTORY_TICKS)
        self._subs: set[asyncio.Queue] = set()
        # env_id → 配置视图（quota/targets/params），来自最近一次应用的配置。
        self._env_config: dict[str, dict[str, Any]] = {}
        # 节点名 → 模式覆盖（不含 = 继承全局默认），随配置热更。
        self._node_modes: dict[str, str] = {}
        self._started = time.time()

    # ---- 配置与数据注入 ----

    def update_config(self, cfg: model.ControllerConfig) -> None:
        self._env_config = {
            e.env_id: {
                "quota_bps": e.quota_bits_per_sec,
                "quota_bytes_per_s": e.quota_bytes_per_sec,
                "targets": [{"node": t.node, "frontend": t.frontend}
                            for t in e.targets],
                "params": e.params.to_dict() if e.params is not None else None,
            }
            for e in cfg.envs
        }
        self._node_modes = dict(cfg.node_modes)

    def _nodes_view(self) -> dict[str, Any]:
        """节点视图：接线 + 生效模式（覆盖或继承全局）+ 采样健康。"""
        degraded = self._degraded_fn()
        default_mode = self._mode_fn()
        return {
            n.name: {
                "host": n.host,
                "port": n.port,
                "override": self._node_modes.get(n.name),
                "mode": self._node_modes.get(n.name) or default_mode,
                "degraded": n.name in degraded,
            }
            for n in self._nodes
        }

    def record(self, now: float, usages, decisions) -> None:
        """快环 sampler 回调：把一拍的采集/决策结果合成快照并发布。"""
        dec_by_env = {d.env_id: d for d in decisions}
        envs: dict[str, Any] = {}
        for u in usages:
            d = dec_by_env.get(u.env_id)
            conf = self._env_config.get(u.env_id, {})
            envs[u.env_id] = {
                "rate_bytes_per_s": u.rate_bps,
                "mean10_bytes_per_s": u.mean10_bps,
                "ewma60_bytes_per_s": u.ewma60_bps,
                "conn": u.conn_cur,
                "degraded": u.degraded,
                "bwlim_bytes_per_s": d.bwlim_bps if d is not None else None,
                "state": str(d.state) if d is not None else None,
                "quota_bytes_per_s": conf.get("quota_bytes_per_s"),
            }
        snap = {
            "ts": now,
            "mode": self._mode_fn(),
            "config_version": self._version_fn(),
            "envs": envs,
            "nodes": self._nodes_view(),
        }
        self._history.append(snap)
        for q in list(self._subs):
            # 慢消费者丢最旧一帧：页面只关心最新状态，绝不反压快环。
            if q.full():
                try:
                    q.get_nowait()
                except asyncio.QueueEmpty:
                    pass
            q.put_nowait(snap)

    # ---- 订阅与查询 ----

    def subscribe(self) -> asyncio.Queue:
        q: asyncio.Queue = asyncio.Queue(SUBSCRIBER_QUEUE_DEPTH)
        self._subs.add(q)
        return q

    def unsubscribe(self, q: asyncio.Queue) -> None:
        self._subs.discard(q)

    def overview(self) -> dict[str, Any]:
        return {
            "service_version": self._service_version,
            "mode": self._mode_fn(),
            "config_version": self._version_fn(),
            "uptime_s": time.time() - self._started,
            "env_config": self._env_config,
            "nodes": self._nodes_view(),
            # AIMD 参数的默认值：页面参数表单以此为占位符/说明，
            # 不在前端硬编码，跟随 model.GovParams 演进。
            "default_params": model.GovParams().to_dict(),
            "latest": self._history[-1] if self._history else None,
        }

    def history(self) -> list[dict[str, Any]]:
        return list(self._history)


def _json_error(status: int, message: str) -> web.Response:
    return web.json_response({"error": message}, status=status)


def build_app(
    hub: StatusHub,
    logbuf: LogBuffer,
    db_opts: dbconfig.MySQLOptions | None,
    log: logging.Logger,
) -> web.Application:
    """组装控制台的 aiohttp 应用（静态页 + 只读 API + 调参 API）。"""

    index_html = (_STATIC_DIR / "index.html").read_bytes()

    async def handle_index(_request: web.Request) -> web.Response:
        return web.Response(body=index_html, content_type="text/html")

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
        """SSE：每拍推一帧快照。断开由写失败/取消自然结束。"""
        resp = web.StreamResponse(headers={
            "Content-Type": "text/event-stream",
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        })
        await resp.prepare(request)
        q = hub.subscribe()
        try:
            # 先补发最新一帧，页面无需等下一拍即可渲染。
            latest = hub.overview()["latest"]
            if latest is not None:
                await resp.write(
                    b"data: " + json.dumps(latest).encode() + b"\n\n")
            while True:
                snap = await q.get()
                await resp.write(
                    b"data: " + json.dumps(snap).encode() + b"\n\n")
        except (ConnectionResetError, ConnectionError, OSError):
            # 客户端关页/刷新导致的断连是 SSE 的正常生命周期终点，
            # 静静收尾即可——重抛会被 aiohttp 记成一条误导性的 ERROR。
            pass
        finally:
            hub.unsubscribe(q)
        return resp

    # ---- 调参（写 MySQL，经轮询热生效）----

    def _mutation_note() -> str:
        poll = db_opts.poll_interval_s if db_opts is not None else 0
        return f"已写入数据库，将在一个轮询周期（约 {poll:g}s）内热生效"

    def _require_db() -> web.Response | None:
        if db_opts is None:
            return _json_error(
                409,
                "当前实例使用本地 YAML 配置（未设置 RL_MYSQL_HOST），"
                "控制台调参依赖 MySQL 配置源，请改用数据库配置模式")
        return None

    async def _mutate(request: web.Request, action,
                      allow_empty_body: bool = False) -> web.Response:
        """调参公共骨架：DB 模式检查 → 解析 JSON → 执行 → 统一应答/报错。"""
        denied = _require_db()
        if denied is not None:
            return denied
        try:
            body = await request.content.read(MAX_BODY_BYTES)
            if allow_empty_body and not body.strip():
                payload = None  # DELETE 类端点：无请求体是常态
            else:
                try:
                    payload = json.loads(body)
                except ValueError as e:
                    raise ValueError(f"请求体不是合法的 JSON: {e}") from None
            await action(payload)
        except ValueError as e:
            return _json_error(400, str(e))
        except Exception as e:
            log.warning("控制台调参写库失败 path=%s err=%s", request.path, e)
            return _json_error(502, f"写入数据库失败：{e}")
        log.info("控制台已写入配置变更 path=%s", request.path)
        return web.json_response({"ok": True, "note": _mutation_note()})

    async def handle_set_mode(request: web.Request) -> web.Response:
        async def action(payload):
            if not isinstance(payload, dict):
                raise ValueError('请求体须为 {"mode": "dry-run"|"enforce"}')
            await dbconfig.update_mode(db_opts, str(payload.get("mode", "")))
        return await _mutate(request, action)

    async def handle_set_quota(request: web.Request) -> web.Response:
        env_id = request.match_info["env_id"]

        async def action(payload):
            if not isinstance(payload, dict) or "quota_bps" not in payload:
                raise ValueError('请求体须为 {"quota_bps": <bits/s 整数>}')
            try:
                quota = int(payload["quota_bps"])
            except (TypeError, ValueError):
                raise ValueError(
                    f"quota_bps 必须是整数（比特每秒），"
                    f"当前值 {payload['quota_bps']!r}") from None
            await dbconfig.update_env_quota(db_opts, env_id, quota)
        return await _mutate(request, action)

    async def handle_set_params(request: web.Request) -> web.Response:
        env_id = request.match_info["env_id"]

        async def action(payload):
            if not isinstance(payload, dict) or "params" not in payload:
                raise ValueError('请求体须为 {"params": {…} 或 null}')
            await dbconfig.update_env_params(db_opts, env_id, payload["params"])
        return await _mutate(request, action)

    async def handle_set_node_mode(request: web.Request) -> web.Response:
        name = request.match_info["name"]

        async def action(payload):
            if not isinstance(payload, dict) or "mode" not in payload:
                raise ValueError(
                    '请求体须为 {"mode": "dry-run"|"enforce"|null}（null=继承全局）')
            mode = payload["mode"]
            await dbconfig.update_node_mode(
                db_opts, name, None if mode is None else str(mode))
        return await _mutate(request, action)

    async def handle_set_targets(request: web.Request) -> web.Response:
        env_id = request.match_info["env_id"]

        async def action(payload):
            if not isinstance(payload, dict) or "targets" not in payload:
                raise ValueError(
                    '请求体须为 {"targets": [{"node": …, "frontend": …}, …]}')
            await dbconfig.update_env_targets(db_opts, env_id, payload["targets"])
        return await _mutate(request, action)

    async def handle_create_env(request: web.Request) -> web.Response:
        async def action(payload):
            if not isinstance(payload, dict):
                raise ValueError(
                    '请求体须为 {"env_id": …, "quota_bps": …, "targets": […]}')
            try:
                quota = int(payload.get("quota_bps", 0))
            except (TypeError, ValueError):
                raise ValueError(
                    f"quota_bps 必须是整数（比特每秒），"
                    f"当前值 {payload.get('quota_bps')!r}") from None
            await dbconfig.create_env(
                db_opts, str(payload.get("env_id", "")), quota,
                payload.get("targets"))
        return await _mutate(request, action)

    async def handle_delete_env(request: web.Request) -> web.Response:
        env_id = request.match_info["env_id"]

        async def action(_payload):
            await dbconfig.delete_env(db_opts, env_id)
        return await _mutate(request, action, allow_empty_body=True)

    app = web.Application(client_max_size=MAX_BODY_BYTES)
    app.router.add_get("/", handle_index)
    app.router.add_get("/api/overview", handle_overview)
    app.router.add_get("/api/history", handle_history)
    app.router.add_get("/api/logs", handle_logs)
    app.router.add_get("/api/stream", handle_stream)
    app.router.add_put("/api/mode", handle_set_mode)
    app.router.add_put("/api/nodes/{name}/mode", handle_set_node_mode)
    app.router.add_post("/api/envs", handle_create_env)
    app.router.add_put("/api/envs/{env_id}/quota", handle_set_quota)
    app.router.add_put("/api/envs/{env_id}/params", handle_set_params)
    app.router.add_put("/api/envs/{env_id}/targets", handle_set_targets)
    app.router.add_delete("/api/envs/{env_id}", handle_delete_env)
    return app


async def run_console(
    port: int,
    hub: StatusHub,
    logbuf: LogBuffer,
    db_opts: dbconfig.MySQLOptions | None,
    log: logging.Logger,
) -> None:
    """常驻任务：启动控制台 HTTP 服务并挂起到被取消，取消时干净回收。"""
    runner = web.AppRunner(build_app(hub, logbuf, db_opts, log), access_log=None)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    log.info(
        "Web 控制台已启动（实时观测 + 在线调参） port=%d db_mode=%s",
        port, db_opts is not None)
    try:
        await asyncio.Event().wait()  # 挂起至任务被取消
    finally:
        await runner.cleanup()
