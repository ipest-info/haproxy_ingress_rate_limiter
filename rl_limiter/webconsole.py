# rl_limiter.webconsole —— 内置 Web 控制台：**只读**实时观测。
#
# 设计取向：
#
#   - 观测侧零额外采集：监控循环每拍本就产出各 frontend 的速率/均值/
#     连接数，控制台只是把这份内存数据经 StatusHub 留存最近几分钟并以
#     SSE 推给页面——不引入第二条采集链路；
#   - 控制台**不提供任何写接口**：负载均衡配置的唯一权威是本机
#     haproxy.cfg（运维直接编辑 + reload），限额登记在本地 YAML 的
#     quotas 段；两个文件都由 cfgparse.watch 轮询热生效。要改配置就改
#     文件——控制台只负责让你看清"改了之后发生了什么"；
#   - 日志经进程内环形缓冲（LogBuffer 挂在根 logger 上）曝光最近若干条，
#     页面增量拉取；生产量级的持久化检索交给外部日志系统，不在此造轮子；
#   - "限速生效证据"：页面上把 实时速率 / 10s 均值（计费口径）/ 限额
#     画在同一条时间轴上——曲线被压在限额线下即是效果本身。
#
# 安全边界：控制台无鉴权，定位与 HAProxy 的 stats socket 相同。默认只
# 绑回环（127.0.0.1），要放到内网必须由运维显式设 RL_CONSOLE_BIND 并配合
# 防火墙/安全组限制来源；绑非回环地址时会打一条 warning 留痕。
#
# 单 HAProxy 模型：每台 HAProxy 各有一个控制台，展示本机的全部受管
# frontend。

from __future__ import annotations

import asyncio
import collections
import json
import logging
import time
from pathlib import Path
from typing import Any, Callable

from aiohttp import web

from . import model

# 快照留存拍数：1s 一拍即约 10 分钟窗口，页面刷新后能立即回填完整曲线。
HISTORY_TICKS = 600
# 日志环形缓冲容量。
LOG_BUFFER_SIZE = 1000
# 单次 /api/logs 返回上限。
LOGS_PAGE_LIMIT = 500
# SSE 订阅队列深度：消费慢时丢最旧一帧（页面只关心最新状态）。
SUBSCRIBER_QUEUE_DEPTH = 5

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


def _instance_view(inst) -> dict[str, Any]:
    """把 InstanceUsage 摊成快照里的 instance 段。

    字段名带上口径后缀是有意的：nic_* 与 pkts_/drop_ 全部来自网卡（整机
    范围、链路层字节、无法按 frontend 拆），rate_in/out 来自 HAProxy 的
    frontend 汇总（应用层字节）。两者数值本就不该相等，名字上就区分开，
    免得页面或后来的人把它们当同一回事。
    """
    if inst is None:
        return {}
    return {
        "conn_new_ps": inst.conn_new_ps,
        "conn_denied_ps": inst.conn_denied_ps,
        "conn": inst.conn_cur,
        "active_conns": inst.active_conns,
        "idle_conns": inst.idle_conns,
        "max_conn": inst.max_conn,
        "rate_in_bytes_per_s": inst.rate_in_bps,
        "rate_out_bytes_per_s": inst.rate_out_bps,
        "nic": inst.nic,
        "pkts_in_ps": inst.pkts_in_ps,
        "pkts_out_ps": inst.pkts_out_ps,
        "drop_in_ps": inst.drop_in_ps,
        "drop_out_ps": inst.drop_out_ps,
        "nic_rate_in_bytes_per_s": inst.nic_rate_in_bps,
        "nic_rate_out_bytes_per_s": inst.nic_rate_out_bps,
        "idle_pct": inst.idle_pct,
        "degraded": inst.degraded,
    }


class StatusHub:
    """监控实时数据的发布枢纽（单事件循环内使用，无锁）。

    - record：监控循环 sampler 每拍调用，合成快照 → 留存历史 + 广播给
      SSE 订阅者；
    - update_config：配置（引导或热更）经过时调用，缓存各 frontend 的
      配置视图，使快照能携带 quota 供页面画限额参考线与超限判定；
    - overview/history：REST 拉取口径。
    """

    def __init__(
        self,
        service_version: str,
        version_fn: Callable[[], int],
        haproxy: model.NodeConfig | None = None,
        degraded_fn: Callable[[], bool] | None = None,
    ):
        self._service_version = service_version
        self._version_fn = version_fn
        # 本机 HAProxy 的接线视图（启动时定型，与 RuntimeClient 一致）。
        self._haproxy = haproxy or model.NodeConfig()
        # 采样是否处于降级（collector.degraded 闭包）。
        self._degraded_fn = degraded_fn if degraded_fn is not None else (lambda: False)
        self._history: collections.deque[dict[str, Any]] = collections.deque(
            maxlen=HISTORY_TICKS)
        self._subs: set[asyncio.Queue] = set()
        # frontend 名 → 该 frontend 的配置视图（页面只读展示用）。
        self._fe_config: dict[str, dict[str, Any]] = {}
        self._started = time.time()

    # ---- 配置与数据注入 ----

    def update_config(self, cfg: model.ControllerConfig) -> None:
        """记录当前生效的受管 frontend 配置（页面的配置视图，只读）。"""
        self._fe_config = {f.name: f.to_dict() for f in cfg.frontends}
        # 换算好的 bytes/s 一并给出：图表的限额参考线用它，避免前端各处
        # 重复做 ÷8，单位换算只在服务端一处。
        for name, d in self._fe_config.items():
            d["quota_bytes_per_s"] = d["quota_mbps"] * 1e6 / 8.0

    def _haproxy_view(self) -> dict[str, Any]:
        """本机 HAProxy 视图：接线 + 采样健康。"""
        n = self._haproxy
        return {
            "name": n.name,
            "endpoint": n.endpoint(),
            "unix": n.is_unix,
            "degraded": bool(self._degraded_fn()),
        }

    def record(self, now: float, usages, instance=None) -> None:
        """监控循环 sampler 回调：把一拍的采集结果合成快照并发布。

        快照有两级：units 按 frontend 名发布（监听端口视图），instance 是
        整台 HAProxy（实例视图）。两级出自同一拍，页面切 tab 时曲线的时间
        轴完全对齐。

        over 为瞬时超限标记（mean10 > 限额），持续超限的判定与告警在监控
        循环里。
        """
        units: dict[str, Any] = {}
        for u in usages:
            conf = self._fe_config.get(u.name, {})
            quota = conf.get("quota_bytes_per_s")
            units[u.name] = {
                "rate_bytes_per_s": u.rate_bps,
                "mean10_bytes_per_s": u.mean10_bps,
                "ewma60_bytes_per_s": u.ewma60_bps,
                "conn": u.conn_cur,
                # --- 监控视图字段（见 docs/05-监控视图.md）---
                "rate_in_bytes_per_s": u.rate_in_bps,
                "conn_new_ps": u.conn_new_ps,
                "conn_denied_ps": u.conn_denied_ps,
                "active_conns": u.active_conns,
                "idle_conns": u.idle_conns,
                # 来自内核 tc 的该 frontend 队列（出方向、链路层口径）
                "pkts_out_ps": u.pkts_out_ps,
                "drop_out_ps": u.drop_out_ps,
                "overlimit_ps": u.overlimit_ps,
                "backlog_bytes": u.backlog_bytes,
                "degraded": u.degraded,
                "quota_bytes_per_s": quota,
                "over": bool(quota and not u.degraded and u.mean10_bps > quota),
            }
        snap = {
            "ts": now,
            "config_version": self._version_fn(),
            "units": units,
            "instance": _instance_view(instance),
            "haproxy": self._haproxy_view(),
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
            "uptime_s": time.time() - self._started,
            # 各受管 frontend 的当前配置视图（来自 haproxy.cfg + YAML
            # 限额的解析结果，只读）。
            "frontends": self._fe_config,
            "haproxy": self._haproxy_view(),
            "latest": self._history[-1] if self._history else None,
        }

    def history(self) -> list[dict[str, Any]]:
        return list(self._history)


def build_app(
    hub: StatusHub,
    logbuf: LogBuffer,
    log: logging.Logger,
) -> web.Application:
    """组装控制台的 aiohttp 应用（静态页 + 只读 API）。

    没有写接口：配置的修改入口是 haproxy.cfg 与 YAML 两个文件本身
    （见模块头注释）。"""

    index_html = (_STATIC_DIR / "index.html").read_bytes()

    async def handle_index(_request: web.Request) -> web.Response:
        # charset 必须显式给：不带它浏览器只能猜编码，中文 Windows 上会
        # 猜成 GBK，整个页面变成乱码。页面里也有 <meta charset>，两处都
        # 写是有意的——HTTP 头对浏览器优先级更高，meta 让文件单独打开
        # （或被别的方式分发）时同样正确。
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
        """SSE：每拍推一帧快照。断开由写失败/取消自然结束。"""
        resp = web.StreamResponse(headers={
            # SSE 规范强制 UTF-8，浏览器不会去猜；显式写出来是为了让中间
            # 的反向代理/抓包工具也不必猜。
            "Content-Type": "text/event-stream; charset=utf-8",
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

    app = web.Application()
    app.router.add_get("/", handle_index)
    app.router.add_get("/api/overview", handle_overview)
    app.router.add_get("/api/history", handle_history)
    app.router.add_get("/api/logs", handle_logs)
    app.router.add_get("/api/stream", handle_stream)
    return app


async def run_console(
    port: int,
    hub: StatusHub,
    logbuf: LogBuffer,
    log: logging.Logger,
    bind: str = "127.0.0.1",
) -> None:
    """常驻任务：启动控制台 HTTP 服务并挂起到被取消，取消时干净回收。

    bind 默认只绑回环：控制台虽是只读，但暴露的是全量运行状态与日志，
    默认对外可达不可接受（由 RL_CONSOLE_BIND 显式放开，见 __main__）。
    绑到非回环地址时打一条 warning，让"我以为它只在本机"的误配在日志里
    留痕。
    """
    runner = web.AppRunner(build_app(hub, logbuf, log), access_log=None)
    await runner.setup()
    site = web.TCPSite(runner, bind, port)
    await site.start()
    log.info("Web 控制台已启动（只读实时观测） bind=%s port=%d", bind, port)
    if bind not in ("127.0.0.1", "::1", "localhost"):
        log.warning(
            "Web 控制台绑定在非回环地址上，而控制台**没有任何鉴权**"
            "（暴露全量监控数据与运行日志）——请确认该地址只在内网且已由"
            "防火墙/安全组限制来源 bind=%s port=%d", bind, port)
    try:
        await asyncio.Event().wait()  # 挂起至任务被取消
    finally:
        await runner.cleanup()
