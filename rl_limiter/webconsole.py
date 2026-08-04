# rl_limiter.webconsole —— 内置 Web 控制台：实时观测 + 限额写 API。
#
# 设计取向：
#
#   - 观测侧零额外采集：监控循环每拍本就产出各 frontend 的速率/均值/
#     连接数，控制台只是把这份内存数据经 StatusHub 留存最近几分钟并以
#     SSE 推给页面——不引入第二条采集链路；
#   - **写接口只覆盖限额**（quotas 里的单个 frontend 限额、网卡总限速
#     nic_quota_mbps），回写到 YAML 后经 cfgparse.watch 热生效（写完
#     poke 一下，不等轮询周期）。负载均衡配置的唯一权威仍是本机
#     haproxy.cfg（运维直接编辑 + reload），控制台永远不写它；接线/
#     log_level 这类要重启才生效的字段也不开放；
#   - 日志经进程内环形缓冲（LogBuffer 挂在根 logger 上）曝光最近若干条，
#     页面增量拉取；生产量级的持久化检索交给外部日志系统，不在此造轮子；
#   - "限速生效证据"：页面上把 实时速率 / 10s 均值（计费口径）/ 限额
#     画在同一条时间轴上——曲线被压在限额线下即是效果本身。
#
# 安全边界：**读接口无鉴权**（定位与 HAProxy 的 stats socket 相同），
# 默认只绑回环（127.0.0.1），要放到内网必须由运维显式设 RL_CONSOLE_BIND
# 并配合防火墙/安全组限制来源；绑非回环地址时会打一条 warning 留痕。
# **写接口必须带令牌**（环境变量 RL_API_TOKEN，请求头 Authorization:
# Bearer <token> 或 X-API-Token）；令牌未配置时写接口整体 403——改限速
# 是影响生产流量的操作，"没配令牌就人人可改"不可接受。
#
# 单 HAProxy 模型：每台 HAProxy 各有一个控制台，展示本机的全部受管
# frontend。

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

from . import configstore, model

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
        # 网卡总限速（Mbps；0 = 不限）。随配置热更。
        self._nic_quota_mbps = 0.0
        self._started = time.time()

    # ---- 配置与数据注入 ----

    def update_config(self, cfg: model.ControllerConfig) -> None:
        """记录当前生效的受管 frontend 配置（页面的配置视图，只读）。"""
        self._fe_config = {f.name: f.to_dict() for f in cfg.frontends}
        # 换算好的 bytes/s 一并给出：图表的限额参考线用它，避免前端各处
        # 重复做 ÷8，单位换算只在服务端一处。
        for name, d in self._fe_config.items():
            d["quota_bytes_per_s"] = d["quota_mbps"] * 1e6 / 8.0
        self._nic_quota_mbps = getattr(cfg, "nic_quota_mbps", 0.0)

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

    def close(self) -> None:
        """停机：给所有 SSE 订阅者投一个 None 哨兵，叫醒并结束它们。

        没有这一步，优雅停机会被挂着的 /api/stream 卡住——aiohttp 的
        cleanup 要等所有在途请求结束，而 SSE 处理器在 q.get() 上永远等
        不到下一帧（监控循环已停）。实测：控制台页面开着时 restart 要
        干等 60s（aiohttp 的 shutdown_timeout）才被强制掐断。
        """
        for q in list(self._subs):
            if q.full():
                try:
                    q.get_nowait()
                except asyncio.QueueEmpty:
                    pass
            q.put_nowait(None)

    def overview(self) -> dict[str, Any]:
        return {
            "service_version": self._service_version,
            "config_version": self._version_fn(),
            "uptime_s": time.time() - self._started,
            # 各受管 frontend 的当前配置视图（来自 haproxy.cfg + YAML
            # 限额的解析结果，只读）。
            "frontends": self._fe_config,
            # 网卡总限速（Mbps；0 = 不限）与换算好的 bytes/s（图表参考线）。
            "nic_quota_mbps": self._nic_quota_mbps,
            "nic_quota_bytes_per_s": self._nic_quota_mbps * 1e6 / 8.0,
            "haproxy": self._haproxy_view(),
            "latest": self._history[-1] if self._history else None,
        }

    def history(self) -> list[dict[str, Any]]:
        return list(self._history)


def _prom_escape(v: str) -> str:
    return v.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n")


def render_prometheus(hub: StatusHub) -> str:
    """把 StatusHub 的最新状态渲染成 Prometheus 文本格式。

    指标命名遵循 prometheus 惯例（bytes_per_second 等单位后缀）；速率
    一律 bytes/s（应用层口径，与控制台/告警/落盘日志一致）。启动后还没
    采到第一拍时只输出服务级指标——空值比编造的 0 诚实。
    """
    o = hub.overview()
    lines: list[str] = []

    def m(name: str, value, help_: str = "", labels: dict | None = None):
        if help_:
            lines.append(f"# HELP {name} {help_}")
            lines.append(f"# TYPE {name} gauge")
        lab = ""
        if labels:
            lab = "{" + ",".join(
                f'{k}="{_prom_escape(str(v))}"' for k, v in labels.items()) + "}"
        lines.append(f"{name}{lab} {value}")

    m("rl_limiter_info", 1, "服务元信息（值恒为 1，信息在标签里）",
      {"version": o["service_version"], "haproxy": o["haproxy"]["name"]})
    m("rl_limiter_config_version", o["config_version"], "当前配置的内容校验和")
    m("rl_limiter_uptime_seconds", round(o["uptime_s"], 1), "服务运行秒数")
    m("rl_limiter_haproxy_degraded", int(o["haproxy"]["degraded"]),
      "采样是否失联（1=失联，此时各速率为陈旧值）")
    m("rl_limiter_nic_quota_bytes_per_second",
      o.get("nic_quota_bytes_per_s", 0),
      "网卡总限速（bytes/s；0=不限）")

    # 限额来自配置视图（即便该 frontend 这一拍没有采样行也要暴露）。
    first = True
    for name, f in sorted((o.get("frontends") or {}).items()):
        m("rl_limiter_frontend_quota_bytes_per_second", f["quota_bytes_per_s"],
          "登记限额（bytes/s；0=不限速）" if first else "", {"frontend": name})
        first = False

    latest = o.get("latest")
    if latest:
        fe_metrics = [
            ("rate_bytes_per_s", "rl_limiter_frontend_rate_bytes_per_second",
             "实时下行速率"),
            ("mean10_bytes_per_s", "rl_limiter_frontend_mean10_bytes_per_second",
             "10 秒滑动均值（计费/超限口径）"),
            ("rate_in_bytes_per_s", "rl_limiter_frontend_rate_in_bytes_per_second",
             "实时上行速率"),
            ("conn", "rl_limiter_frontend_connections", "并发连接数"),
            ("active_conns", "rl_limiter_frontend_active_connections", "活跃连接数"),
            ("idle_conns", "rl_limiter_frontend_idle_connections", "空闲连接数"),
            ("conn_new_ps", "rl_limiter_frontend_new_connections_per_second",
             "每秒新建连接数"),
            ("conn_denied_ps", "rl_limiter_frontend_denied_per_second",
             "每秒被拒绝连接数"),
            ("pkts_out_ps", "rl_limiter_frontend_tc_packets_out_per_second",
             "tc 队列每秒流出包数（链路层）"),
            ("drop_out_ps", "rl_limiter_frontend_tc_drops_per_second",
             "tc 队列每秒丢包数（被限速丢弃）"),
            ("overlimit_ps", "rl_limiter_frontend_tc_overlimits_per_second",
             "tc 每秒触发限速次数"),
            ("over", "rl_limiter_frontend_over_quota", "瞬时超限标记（mean10>限额）"),
            ("degraded", "rl_limiter_frontend_degraded", "该 frontend 采样是否失联"),
        ]
        units = latest.get("units") or {}
        for key, pname, help_ in fe_metrics:
            first = True
            for name in sorted(units):
                v = units[name].get(key)
                if v is None:
                    continue
                m(pname, int(v) if isinstance(v, bool) else v,
                  help_ if first else "", {"frontend": name})
                first = False

        inst = latest.get("instance") or {}
        inst_metrics = [
            ("conn", "rl_limiter_instance_connections", "整机并发连接数"),
            ("max_conn", "rl_limiter_instance_max_connections", "进程连接上限"),
            ("conn_new_ps", "rl_limiter_instance_new_connections_per_second",
             "整机每秒新建连接数"),
            ("rate_in_bytes_per_s", "rl_limiter_instance_rate_in_bytes_per_second",
             "整机上行速率（HAProxy 口径）"),
            ("rate_out_bytes_per_s", "rl_limiter_instance_rate_out_bytes_per_second",
             "整机下行速率（HAProxy 口径）"),
            ("nic_rate_in_bytes_per_s", "rl_limiter_nic_rate_in_bytes_per_second",
             "网卡入向速率（链路层、整机）"),
            ("nic_rate_out_bytes_per_s", "rl_limiter_nic_rate_out_bytes_per_second",
             "网卡出向速率（链路层、整机）"),
            ("pkts_in_ps", "rl_limiter_nic_packets_in_per_second", "网卡每秒入包数"),
            ("drop_in_ps", "rl_limiter_nic_drops_in_per_second", "网卡每秒入向丢包数"),
            ("idle_pct", "rl_limiter_haproxy_idle_percent", "HAProxy 自报空闲率"),
        ]
        for key, pname, help_ in inst_metrics:
            v = inst.get(key)
            if v is not None:
                m(pname, v, help_)

    return "\n".join(lines) + "\n"


def build_app(
    hub: StatusHub,
    logbuf: LogBuffer,
    log: logging.Logger,
    yaml_path: str = "",
    api_token: str = "",
    known_frontends_fn: Callable[[], set] | None = None,
    poke: "asyncio.Event | None" = None,
) -> web.Application:
    """组装控制台的 aiohttp 应用（静态页 + 只读 API + 限额写 API）。

    写 API 只在 yaml_path 非空时挂载（测试/极简形态可以只要只读部分），
    且每个请求都要过令牌校验：api_token 为空 = 未配置 RL_API_TOKEN，
    所有写请求 403——绝不允许"没配令牌就人人可改限速"。

    known_frontends_fn 返回当前 haproxy.cfg 里解析到的段名集合，用来
    拒绝给不存在的 frontend 登记限额（多半是拼错了名字；写进去也只会
    被 cfgparse 忽略并 warn，不如在门口就说清楚）。
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

    async def handle_metrics(_request: web.Request) -> web.Response:
        """Prometheus 文本 exposition 格式的当前值（/metrics）。

        数据取自 StatusHub 的最新一拍快照——与控制台曲线同源，不另起
        采集链路。全部是 gauge：速率本就是每秒口径，累计量在采集侧已经
        差分过，counter 反而没有对应的原始值。
        """
        return web.Response(text=render_prometheus(hub),
                            content_type="text/plain", charset="utf-8")

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
                if snap is None:          # 停机哨兵（见 StatusHub.close）
                    break
                await resp.write(
                    b"data: " + json.dumps(snap).encode() + b"\n\n")
        except (ConnectionResetError, ConnectionError, OSError):
            # 客户端关页/刷新导致的断连是 SSE 的正常生命周期终点，
            # 静静收尾即可——重抛会被 aiohttp 记成一条误导性的 ERROR。
            pass
        finally:
            hub.unsubscribe(q)
        return resp

    # ---- 写 API（限额修改，带令牌鉴权，回写 YAML）----

    # 服务内写操作串行化：整份读-改-写不可交叠，否则并发 PUT 会互相
    # 覆盖对方刚写进去的键。
    write_lock = asyncio.Lock()

    def _authorized(request: web.Request) -> "web.Response | None":
        """令牌校验。通过返回 None，否则返回该直接回给客户端的响应。"""
        if not api_token:
            return web.json_response(
                {"error": "未配置 RL_API_TOKEN，写接口已禁用——在服务的"
                          "环境变量里设置令牌后重启即可启用"},
                status=403)
        got = ""
        auth = request.headers.get("Authorization", "")
        if auth.startswith("Bearer "):
            got = auth[len("Bearer "):].strip()
        if not got:
            got = request.headers.get("X-API-Token", "").strip()
        # 常数时间比较：令牌校验不能给旁路计时留缝。
        if not got or not hmac.compare_digest(got, api_token):
            return web.json_response(
                {"error": "令牌缺失或不正确（请求头 Authorization: "
                          "Bearer <token> 或 X-API-Token: <token>）"},
                status=401)
        return None

    async def _read_quota_body(request: web.Request) -> float:
        """解析写请求体 {"quota_mbps": <数字>}，非法时抛 ValueError。"""
        try:
            body = await request.json()
        except Exception:
            raise ValueError("请求体必须是 JSON（{\"quota_mbps\": <数字>}）")
        if not isinstance(body, dict) or "quota_mbps" not in body:
            raise ValueError("请求体缺少字段 quota_mbps")
        v = body["quota_mbps"]
        if isinstance(v, bool) or not isinstance(v, (int, float)):
            raise ValueError(f"quota_mbps 必须是数字（Mbps），"
                             f"当前值 {v!r}")
        return float(v)

    async def handle_put_quota(request: web.Request) -> web.Response:
        denied = _authorized(request)
        if denied is not None:
            return denied
        name = request.match_info["name"]
        try:
            quota = await _read_quota_body(request)
        except ValueError as e:
            return web.json_response({"error": str(e)}, status=400)
        known = known_frontends_fn() if known_frontends_fn is not None else None
        if known is not None and name not in known:
            return web.json_response(
                {"error": f"haproxy.cfg 里没有名为 {name!r} 的 frontend/"
                          f"listen 段（是不是拼错了？）。当前解析到的段："
                          f"{sorted(known)}"},
                status=400)
        async with write_lock:
            try:
                await asyncio.to_thread(
                    configstore.set_quota, yaml_path, name, quota)
            except ValueError as e:
                return web.json_response({"error": str(e)}, status=400)
        if poke is not None:
            poke.set()
        log.warning("写 API 已修改限额并回写 YAML（cfg 轮询即将热生效） "
                    "frontend=%s quota=%gMbps yaml=%s",
                    name, quota, yaml_path)
        return web.json_response({"ok": True, "frontend": name,
                                  "quota_mbps": quota})

    async def handle_delete_quota(request: web.Request) -> web.Response:
        denied = _authorized(request)
        if denied is not None:
            return denied
        name = request.match_info["name"]
        async with write_lock:
            try:
                removed = await asyncio.to_thread(
                    configstore.remove_quota, yaml_path, name)
            except ValueError as e:
                return web.json_response({"error": str(e)}, status=400)
        if removed and poke is not None:
            poke.set()
        if removed:
            log.warning("写 API 已删除限额登记（该 frontend 回到只监控不"
                        "限速） frontend=%s yaml=%s", name, yaml_path)
        return web.json_response({"ok": True, "frontend": name,
                                  "removed": removed})

    async def handle_put_nic_quota(request: web.Request) -> web.Response:
        denied = _authorized(request)
        if denied is not None:
            return denied
        try:
            quota = await _read_quota_body(request)
        except ValueError as e:
            return web.json_response({"error": str(e)}, status=400)
        async with write_lock:
            try:
                await asyncio.to_thread(
                    configstore.set_nic_quota, yaml_path, quota)
            except ValueError as e:
                return web.json_response({"error": str(e)}, status=400)
        if poke is not None:
            poke.set()
        log.warning("写 API 已修改网卡总限速并回写 YAML nic_quota=%gMbps "
                    "yaml=%s", quota, yaml_path)
        return web.json_response({"ok": True, "nic_quota_mbps": quota})

    async def handle_delete_nic_quota(request: web.Request) -> web.Response:
        denied = _authorized(request)
        if denied is not None:
            return denied
        async with write_lock:
            try:
                await asyncio.to_thread(
                    configstore.set_nic_quota, yaml_path, 0.0)
            except ValueError as e:
                return web.json_response({"error": str(e)}, status=400)
        if poke is not None:
            poke.set()
        log.warning("写 API 已取消网卡总限速（恢复不限）并回写 YAML "
                    "yaml=%s", yaml_path)
        return web.json_response({"ok": True, "nic_quota_mbps": 0})

    app = web.Application()
    app.router.add_get("/", handle_index)
    app.router.add_get("/api/overview", handle_overview)
    app.router.add_get("/api/history", handle_history)
    app.router.add_get("/api/logs", handle_logs)
    app.router.add_get("/api/stream", handle_stream)
    # Prometheus 抓取端点：与只读 API 同一个监听面（同样的安全边界——
    # 默认回环，放内网靠防火墙）。
    app.router.add_get("/metrics", handle_metrics)
    if yaml_path:
        app.router.add_put("/api/quotas/{name}", handle_put_quota)
        app.router.add_delete("/api/quotas/{name}", handle_delete_quota)
        app.router.add_put("/api/nic-quota", handle_put_nic_quota)
        app.router.add_delete("/api/nic-quota", handle_delete_nic_quota)
    return app


async def run_console(
    port: int,
    hub: StatusHub,
    logbuf: LogBuffer,
    log: logging.Logger,
    bind: str = "127.0.0.1",
    yaml_path: str = "",
    api_token: str = "",
    known_frontends_fn: Callable[[], set] | None = None,
    poke: "asyncio.Event | None" = None,
) -> None:
    """常驻任务：启动控制台 HTTP 服务并挂起到被取消，取消时干净回收。

    bind 默认只绑回环：控制台暴露的是全量运行状态与日志，且写接口能改
    限速，默认对外可达不可接受（由 RL_CONSOLE_BIND 显式放开，见
    __main__）。绑到非回环地址时打一条 warning，让"我以为它只在本机"的
    误配在日志里留痕。写 API 的参数含义见 build_app。
    """
    runner = web.AppRunner(
        build_app(hub, logbuf, log, yaml_path=yaml_path, api_token=api_token,
                  known_frontends_fn=known_frontends_fn, poke=poke),
        access_log=None)
    await runner.setup()
    site = web.TCPSite(runner, bind, port)
    await site.start()
    log.info("Web 控制台已启动 bind=%s port=%d 写API=%s", bind, port,
             ("已启用（令牌鉴权）" if yaml_path and api_token
              else "已禁用（未配置 RL_API_TOKEN）" if yaml_path
              else "未挂载"))
    if bind not in ("127.0.0.1", "::1", "localhost"):
        log.warning(
            "Web 控制台绑定在非回环地址上，而**读接口没有任何鉴权**"
            "（暴露全量监控数据与运行日志）——请确认该地址只在内网且已由"
            "防火墙/安全组限制来源 bind=%s port=%d", bind, port)
    try:
        await asyncio.Event().wait()  # 挂起至任务被取消
    finally:
        # 先叫醒并结束所有 SSE 流，cleanup 才不会等在途请求等到超时。
        hub.close()
        await runner.cleanup()
