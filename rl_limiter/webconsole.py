# rl_limiter.webconsole —— 内置 Web 控制台：实时观测 + 配置管理。
#
# 设计取向：
#
#   - 观测侧零额外采集：监控循环每拍本就产出各节点的速率/均值/连接数，
#     控制台只是把这份内存数据经 StatusHub 留存最近几分钟并以 SSE 推给
#     页面——不引入第二条采集链路；
#   - 配置侧写库不写内存：所有修改（节点登记限额、环境分组/挂载点）
#     UPDATE 到 MySQL（配置唯一事实源），由 dbconfig.watch 的既有轮询
#     链路热生效。注意：登记限额只是**监控基准**——真实限速在各节点
#     HAProxy 的 shared bwlim 配置里，需同步修改并 reload（接口应答与
#     页面都有提示）。未启用数据库配置模式（纯 YAML 部署）时写接口
#     返回 409 说明原因；
#   - 日志经进程内环形缓冲（LogBuffer 挂在根 logger 上）曝光最近若干条，
#     页面增量拉取；生产量级的持久化检索交给外部日志系统，不在此造轮子；
#   - "限速生效证据"：页面上把 实时速率 / 10s 均值（计费口径）/ 登记
#     限额 画在同一条时间轴上——曲线被压在限额线下即是效果本身；持续
#     压不住则以超限状态高亮（提示 HAProxy 配置与库不一致）。
#
# 安全边界：控制台无鉴权，定位与 HAProxy 的 stats socket 相同。默认只
# 绑回环（127.0.0.1），要放到内网必须由运维显式设 RL_CONSOLE_BIND 并配合
# 防火墙/安全组限制来源；绑非回环地址时会打一条 warning 留痕。
#
# 同机部署形态下每台 HAProxy 各有一个控制台，只展示本机节点；环境聚合
# 视图退化为"只含本机成员"，页面顶部会挂本地模式横幅说明这一点。

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
    """监控实时数据的发布枢纽（单事件循环内使用，无锁）。

    - record：监控循环 sampler 每拍调用，合成快照 → 留存历史 + 广播给
      SSE 订阅者；
    - update_config：配置（引导或热更）经过时调用，缓存限额/挂载点视图，
      使快照能携带 quota 供页面画限额参考线与超限判定；
    - overview/history：REST 拉取口径。
    """

    def __init__(
        self,
        service_version: str,
        version_fn: Callable[[], int],
        nodes: list[model.NodeConfig] | None = None,
        degraded_fn: Callable[[], set] | None = None,
        scope_node: str | None = None,
    ):
        self._service_version = service_version
        self._version_fn = version_fn
        # 同机部署模式下的本机节点名（None = 集中监控模式）。页面据此
        # 显示"本地模式"横幅：环境聚合视图此时只含本机，不标出来会被
        # 误读成"整个环境只跑了这么多流量"。
        self._scope_node = scope_node
        # 受控节点接线视图（启动时定型，与 RuntimeClient 集合一致）。
        self._nodes = list(nodes or [])
        # 采样已持续失败的节点集合（collector.degraded_nodes 闭包）。
        self._degraded_fn = degraded_fn if degraded_fn is not None else (lambda: set())
        self._history: collections.deque[dict[str, Any]] = collections.deque(
            maxlen=HISTORY_TICKS)
        self._subs: set[asyncio.Queue] = set()
        # 节点名 → 监控单元配置视图（quota/frontends）。监控单元 = 节点
        # （cfg.envs 的 env_id 字段装节点名）。
        self._unit_config: dict[str, dict[str, Any]] = {}
        # 业务环境分组（env_id → 成员节点列表），仅聚合展示。
        self._env_groups: dict[str, list[str]] = {}
        self._started = time.time()

    # ---- 配置与数据注入 ----

    def update_config(self, cfg: model.ControllerConfig) -> None:
        self._unit_config = {
            u.env_id: {
                "quota_bps": u.quota_bits_per_sec,
                "quota_bytes_per_s": u.quota_bytes_per_sec,
                "frontends": [t.frontend for t in u.targets],
            }
            for u in cfg.envs
        }
        self._env_groups = {k: list(v) for k, v in cfg.env_groups.items()}

    def _nodes_view(self) -> dict[str, Any]:
        """节点视图：接线 + 采样健康。"""
        degraded = self._degraded_fn()
        return {
            n.name: {
                "host": n.host,
                "port": n.port,
                # 采样端点的统一展示口径：同机形态是 unix socket 路径，
                # 跨机形态是 host:port。页面直接显示这个字段。
                "endpoint": n.endpoint(),
                "unix": n.is_unix,
                "degraded": n.name in degraded,
            }
            for n in self._nodes
        }

    def record(self, now: float, usages) -> None:
        """监控循环 sampler 回调：把一拍的采集结果合成快照并发布。

        监控单元 = 节点，usages 的 env_id 字段即节点名，快照按节点键
        发布（units）；环境聚合视图由前端按 env_groups 把成员节点的
        序列求和得出，服务端不再有环境级数据。over 为瞬时超限标记
        （mean10 > 登记限额），持续超限的判定与告警在监控循环里。
        """
        units: dict[str, Any] = {}
        for u in usages:
            conf = self._unit_config.get(u.env_id, {})
            quota = conf.get("quota_bytes_per_s")
            units[u.env_id] = {
                "rate_bytes_per_s": u.rate_bps,
                "mean10_bytes_per_s": u.mean10_bps,
                "ewma60_bytes_per_s": u.ewma60_bps,
                "conn": u.conn_cur,
                "degraded": u.degraded,
                "quota_bytes_per_s": quota,
                "over": bool(quota and not u.degraded and u.mean10_bps > quota),
            }
        snap = {
            "ts": now,
            "config_version": self._version_fn(),
            "units": units,
            "env_groups": {k: list(v) for k, v in self._env_groups.items()},
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
            "config_version": self._version_fn(),
            # None = 集中监控；非 None = 同机部署，值为本机节点名。
            "scope_node": self._scope_node,
            "uptime_s": time.time() - self._started,
            # 节点监控单元配置（quota/frontends）与环境分组。
            "node_config": self._unit_config,
            "env_groups": {k: list(v) for k, v in self._env_groups.items()},
            "nodes": self._nodes_view(),
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

    # ---- 配置管理（写 MySQL，经轮询热生效）----

    def _mutation_note(extra: str = "") -> str:
        poll = db_opts.poll_interval_s if db_opts is not None else 0
        note = f"已写入数据库，将在一个轮询周期（约 {poll:g}s）内热生效"
        return note + (f"；{extra}" if extra else "")

    def _require_db() -> web.Response | None:
        if db_opts is None:
            return _json_error(
                409,
                "当前实例使用本地 YAML 配置（未设置 RL_MYSQL_HOST），"
                "控制台配置管理依赖 MySQL 配置源，请改用数据库配置模式")
        return None

    async def _mutate(request: web.Request, action,
                      allow_empty_body: bool = False,
                      note_extra: str = "") -> web.Response:
        """写接口公共骨架：DB 模式检查 → 解析 JSON → 执行 → 统一应答/报错。"""
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
            log.warning("控制台写库失败 path=%s err=%s", request.path, e)
            return _json_error(502, f"写入数据库失败：{e}")
        log.info("控制台已写入配置变更 path=%s", request.path)
        return web.json_response({"ok": True, "note": _mutation_note(note_extra)})

    async def handle_set_node_quota(request: web.Request) -> web.Response:
        name = request.match_info["name"]

        async def action(payload):
            if not isinstance(payload, dict) or "quota_bps" not in payload:
                raise ValueError('请求体须为 {"quota_bps": <bits/s 整数>}')
            try:
                quota = int(payload["quota_bps"])
            except (TypeError, ValueError):
                raise ValueError(
                    f"quota_bps 必须是整数（比特每秒），"
                    f"当前值 {payload['quota_bps']!r}") from None
            await dbconfig.update_node_quota(db_opts, name, quota)
        return await _mutate(
            request, action,
            note_extra="注意：这只更新监控基准——真实限速需同步修改该节点 "
                       "haproxy.cfg 里 shared bwlim 的 limit 并 reload，"
                       "否则将触发持续超限告警")

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
                    '请求体须为 {"env_id": …, "targets": […]}'
                    '（环境是节点分组，限额登记在成员节点上）')
            await dbconfig.create_env(
                db_opts, str(payload.get("env_id", "")),
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
    # 节点登记限额（监控基准；真实限速在该节点 HAProxy 配置里）。
    app.router.add_put("/api/nodes/{name}/quota", handle_set_node_quota)
    app.router.add_post("/api/envs", handle_create_env)
    app.router.add_put("/api/envs/{env_id}/targets", handle_set_targets)
    app.router.add_delete("/api/envs/{env_id}", handle_delete_env)
    return app


async def run_console(
    port: int,
    hub: StatusHub,
    logbuf: LogBuffer,
    db_opts: dbconfig.MySQLOptions | None,
    log: logging.Logger,
    bind: str = "127.0.0.1",
) -> None:
    """常驻任务：启动控制台 HTTP 服务并挂起到被取消，取消时干净回收。

    bind 默认只绑回环：控制台无鉴权且带写接口，默认对外可达不可接受
    （由 RL_CONSOLE_BIND 显式放开，见 __main__）。绑到非回环地址时打一条
    warning，让"我以为它只在本机"的误配在日志里留痕。
    """
    runner = web.AppRunner(build_app(hub, logbuf, db_opts, log), access_log=None)
    await runner.setup()
    site = web.TCPSite(runner, bind, port)
    await site.start()
    log.info(
        "Web 控制台已启动（实时观测 + 配置管理） bind=%s port=%d db_mode=%s",
        bind, port, db_opts is not None)
    if bind not in ("127.0.0.1", "::1", "localhost"):
        log.warning(
            "Web 控制台绑定在非回环地址上，而控制台**没有任何鉴权**且提供"
            "写接口（改登记限额/改挂载点/删环境）——请确认该地址只在内网"
            "且已由防火墙/安全组限制来源 bind=%s port=%d", bind, port)
    try:
        await asyncio.Event().wait()  # 挂起至任务被取消
    finally:
        await runner.cleanup()
