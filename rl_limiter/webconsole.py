# rl_limiter.webconsole —— 内置 Web 控制台：实时观测 + 配置管理。
#
# 设计取向：
#
#   - 观测侧零额外采集：监控循环每拍本就产出各 frontend 的速率/均值/
#     连接数，控制台只是把这份内存数据经 StatusHub 留存最近几分钟并以
#     SSE 推给页面——不引入第二条采集链路；
#   - 配置侧写库不写内存：所有修改（监听端口、限额、后端服务器）写入
#     MySQL（配置唯一事实源），由 dbconfig.watch 的既有轮询链路热生效，
#     再由 enforcer 渲染进本机 haproxy.cfg 并 reload。因此在界面上改完
#     即真正生效，不需要人工再动配置文件。未启用数据库配置模式（纯 YAML
#     部署）时写接口返回 409 说明原因；
#   - 日志经进程内环形缓冲（LogBuffer 挂在根 logger 上）曝光最近若干条，
#     页面增量拉取；生产量级的持久化检索交给外部日志系统，不在此造轮子；
#   - "限速生效证据"：页面上把 实时速率 / 10s 均值（计费口径）/ 限额
#     画在同一条时间轴上——曲线被压在限额线下即是效果本身。
#
# 安全边界：控制台无鉴权，定位与 HAProxy 的 stats socket 相同。默认只
# 绑回环（127.0.0.1），要放到内网必须由运维显式设 RL_CONSOLE_BIND 并配合
# 防火墙/安全组限制来源；绑非回环地址时会打一条 warning 留痕。
#
# 单 HAProxy 模型：每台 HAProxy 各有一个控制台，管理并展示本机的全部
# 受管 frontend。

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
        # 配置自动下发的最近一次结果（None = 未启用该能力）。页面据此
        # 区分"改完就已经生效"与"改完还等人工同步数据面"。
        self._enforce: dict | None = None
        # 采样是否处于降级（collector.degraded 闭包）。
        self._degraded_fn = degraded_fn if degraded_fn is not None else (lambda: False)
        self._history: collections.deque[dict[str, Any]] = collections.deque(
            maxlen=HISTORY_TICKS)
        self._subs: set[asyncio.Queue] = set()
        # frontend 名 → 该 frontend 的完整配置视图（界面表单的初值）。
        self._fe_config: dict[str, dict[str, Any]] = {}
        self._started = time.time()

    # ---- 配置与数据注入 ----

    def record_enforce(self, result) -> None:
        """记录一次配置下发的结果（enforcer 每轮 reconcile 后回调）。

        页面靠它回答运维最关心的那个问题：我刚在这儿改的配置，**数据面
        到底生效了没有**。失败时把原因原样带出来——这时数据面还在按旧
        配置跑，不说清楚就会以为已经改好了。
        """
        self._enforce = {
            "enabled": True,
            "ok": result.ok,
            "error": result.error,
            "last_change_ts": time.time() if result.changed else (
                (self._enforce or {}).get("last_change_ts")),
            "applied": list(result.frontends) if result.changed else (
                (self._enforce or {}).get("applied") or []),
        }

    def update_config(self, cfg: model.ControllerConfig) -> None:
        """记录当前生效的受管 frontend 配置（界面的编辑表单以它为初值）。"""
        self._fe_config = {f.name: f.to_dict() for f in cfg.frontends}
        # 换算好的 bytes/s 一并给出：图表的限额参考线用它，避免前端各处
        # 重复做 ÷8，单位换算只在服务端一处。
        for name, d in self._fe_config.items():
            d["quota_bytes_per_s"] = d["quota_bps"] / 8.0

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
            # None = 未启用配置自动下发（改配置后仍需人工改 cfg + reload）。
            "enforce": self._enforce,
            "uptime_s": time.time() - self._started,
            # 各受管 frontend 的完整配置：界面的编辑表单以它为初值。
            "frontends": self._fe_config,
            "haproxy": self._haproxy_view(),
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
    instance: str = "haproxy",
    store=None,
) -> web.Application:
    """组装控制台的 aiohttp 应用（静态页 + 只读 API + 配置管理 API）。

    instance 是本实例在配置库里对应的 HAProxy 实例名：写接口只会改属于
    自己的那些行，一个配置库服务多台机器时互不越界。
    """

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

    async def handle_metrics(request: web.Request) -> web.Response:
        """历史回查：/api/metrics?scope=&from=&to=[&bucket_s=]

        与 /api/history 的分工：那个读**内存**里最近 10 分钟的 1 秒粒度，
        这个读**库**里的分级聚合（1 分钟 × 7 天 / 5 分钟 × 90 天）。
        不给 bucket_s 时按区间自动选层，见 metricstore.pick_tier。
        """
        if store is None:
            return _json_error(
                409, "未启用监控数据落库（需要数据库配置模式，见 "
                     "docs/07-监控数据回查.md）；实时曲线请用 /api/history")
        q = request.query
        try:
            now = int(time.time())
            start = int(q.get("from") or (now - 3600))
            end = int(q.get("to") or now)
            bucket = int(q["bucket_s"]) if q.get("bucket_s") else None
        except ValueError:
            return _json_error(400, "from/to/bucket_s 必须是整数（unix 秒）")
        if end <= start:
            return _json_error(400, "to 必须大于 from")
        try:
            return web.json_response(
                await store.query(q.get("scope", ""), start, end, bucket))
        except Exception as e:
            log.warning("监控数据回查失败 err=%s", e)
            return _json_error(502, f"查询失败：{e}")

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

    async def handle_upsert_frontend(request: web.Request) -> web.Response:
        """新建或整体更新一个受管 frontend（连同它的后端服务器列表）。

        界面上编辑的是"这个监听端口连同它的后端"这一整体，因此接口也按
        整体提交：服务端在一个事务里替换该 frontend 的全部 server 行，
        不会出现"改了一半"被轮询读到的中间态。
        """
        async def action(payload):
            await dbconfig.upsert_frontend(db_opts, instance, payload)
        return await _mutate(
            request, action,
            note_extra="随后由本机 rl-limiter 写入 haproxy.cfg 受管区块并 "
                       "reload，数据面即时生效")

    async def handle_delete_frontend(request: web.Request) -> web.Response:
        name = request.match_info["name"]

        async def action(_payload):
            await dbconfig.delete_frontend(db_opts, instance, name)
        return await _mutate(
            request, action, allow_empty_body=True,
            note_extra="该监听端口将在下一次 reload 后停止服务")

    app = web.Application(client_max_size=MAX_BODY_BYTES)
    app.router.add_get("/", handle_index)
    app.router.add_get("/api/overview", handle_overview)
    app.router.add_get("/api/history", handle_history)
    app.router.add_get("/api/logs", handle_logs)
    # 历史回查（落库的分级聚合）。/api/history 是内存里的实时曲线，两者
    # 路径与语义都分开，免得有人拿 history 去查昨天。
    app.router.add_get("/api/metrics", handle_metrics)
    app.router.add_get("/api/stream", handle_stream)
    # 受管 frontend 的增删改：监听端口、模式、限额、超时、后端服务器。
    # PUT 用同一个 upsert 语义（存在即整体更新，不存在即新建），界面上
    # "新增"与"保存"因此走同一条路径，少一类边界情况。
    app.router.add_put("/api/frontends/{name}", handle_upsert_frontend)
    app.router.add_post("/api/frontends", handle_upsert_frontend)
    app.router.add_delete("/api/frontends/{name}", handle_delete_frontend)
    return app


async def run_console(
    port: int,
    hub: StatusHub,
    logbuf: LogBuffer,
    db_opts: dbconfig.MySQLOptions | None,
    log: logging.Logger,
    bind: str = "127.0.0.1",
    instance: str = "haproxy",
    store=None,
) -> None:
    """常驻任务：启动控制台 HTTP 服务并挂起到被取消，取消时干净回收。

    bind 默认只绑回环：控制台无鉴权且带写接口，默认对外可达不可接受
    （由 RL_CONSOLE_BIND 显式放开，见 __main__）。绑到非回环地址时打一条
    warning，让"我以为它只在本机"的误配在日志里留痕。
    """
    runner = web.AppRunner(
        build_app(hub, logbuf, db_opts, log, instance, store), access_log=None)
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
