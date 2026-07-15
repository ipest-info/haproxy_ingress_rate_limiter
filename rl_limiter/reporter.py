# rl_limiter.reporter —— 面向管理后台（Controller）的客户端（设计文档
# §3.6 接口），承担三条独立的通信链路：
#
#  1. 配置长轮询：GET /v1/agent/config 携带本地已应用的 config_version，
#     后台有变更立即返回新配置（200），无变更则挂住直至约 30s 超时
#     （204 或客户端侧超时），随即重新发起轮询——变更秒级可达，且空闲
#     时不产生轮询风暴。
#  2. 指标批量上报：POST /v1/agent/metrics，每 5s 一批（内含每秒明细样本）。
#  3. 心跳：POST /v1/agent/heartbeat，每 10s 上报实例身份/模式/配置版本，
#     供后台判断服务存活与配置是否收敛。
#
# fail-static 容错（§3.7）在本模块的落点：每次收到新配置都先原子落盘到
# 本地缓存文件，再投递给核心循环；进程重启后核心通过 load_cache 读回
# "最后一次下发的配置"，在后台不可达期间继续按既有配额限速，绝不因断联
# 而放开为不限速。
#
# 并发模型：三条链路各自是一个常驻 asyncio task；配置投递用
# asyncio.Queue(1) 做"只保留最新一份"的合并——消费方来不及取时，新
# 配置直接覆盖队列里的旧配置，核心循环永远拿到最新版本。不加锁——
# 所有共享状态只在事件循环单线程内被触碰，天然无数据竞争。

from __future__ import annotations

import asyncio
import json
import logging
import os
import random
import ssl
import tempfile
import time
from typing import Any, Callable, Iterable
from urllib.parse import quote

import aiohttp

from . import model
from .config import BackendOptions

# 所有 HTTP 请求的客户端总超时（秒）。必须大于后台的长轮询挂起时长
# （§3.6 约定 30s）：否则每次长轮询都会在服务端返回前被客户端掐断，
# 长轮询退化为纯超时重试。40s 留出 10s 网络余量。
REQUEST_TIMEOUT_S = 40.0
# 指标批量上报的节拍（§3.6：每 5s 批量）。下发配置中的 report_interval_s
# 旋钮当前刻意未接线：骨架阶段保持固定节拍，与配置默认值一致。
DEFAULT_FLUSH_INTERVAL_S = 5.0
# 心跳周期，对应 §3.6 的"每 10s"。
DEFAULT_HEARTBEAT_INTERVAL_S = 10.0
# 后台不可达期间指标缓冲的上限。按核心循环每秒一个样本计算约可缓存
# 10 分钟；超出后丢弃最旧样本（新数据比旧数据更有诊断价值），从而把
# 断联期间的内存占用限制在常数级。
MAX_BUFFERED_SAMPLES = 600
# 配置轮询失败后的指数退避区间：首次失败等 1s，之后逐次翻倍，封顶 30s。
BACKOFF_MIN_S = 1.0
BACKOFF_MAX_S = 30.0
# 读取任何后台响应体的字节数上限（8 MiB），防止异常/恶意响应把服务
# 内存撑爆。
MAX_RESPONSE_BYTES = 8 << 20
# "缓冲满丢弃最旧样本"告警日志的节流粒度：首次丢弃立即告警，之后每累计
# 这么多次丢弃再告警一次，避免长时间断联时每秒刷一条 warn。
DROP_LOG_EVERY = 100

# 后台侧的三个接口路径（§3.6），不可变更。
CONFIG_PATH = "/v1/agent/config"
METRICS_PATH = "/v1/agent/metrics"
HEARTBEAT_PATH = "/v1/agent/heartbeat"


def load_cache(path: str) -> model.ControllerConfig:
    """读取长轮询循环写下的 fail-static 配置缓存（§3.7）。

    服务重启后据此恢复"最后一次下发的配置"，在后台不可达期间维持限速。
    文件不存在时抛 FileNotFoundError，调用方据此区分"首次启动、尚无缓存"
    （正常，走本地 envs 引导）与"缓存损坏"（异常，需要告警）两种情形。
    """
    with open(path, "rb") as f:
        data = f.read()
    cfg = model.ControllerConfig.from_dict(json.loads(data))
    # from_dict 已归一，这里再显式归一一次以固定"所有配置入口必先
    # normalize"的约定（与长轮询收包路径保持同构）。
    cfg.normalize()
    return cfg


def _next_backoff(cur: float) -> float:
    """计算下一档退避：首次失败取 BACKOFF_MIN_S，之后逐次翻倍，封顶
    BACKOFF_MAX_S，即 1s → 2s → 4s → ... → 30s。"""
    if cur <= 0:
        return BACKOFF_MIN_S
    return min(cur * 2, BACKOFF_MAX_S)


def _with_jitter(d: float) -> float:
    """把 d 映射到 [d/2, d] 区间内的均匀随机值。抖动的目的是打散重试
    相位：后台宕机恢复的一瞬间，全体实例若按相同节拍重试会形成同步冲击
    （thundering herd），随机化后重连压力被摊平。"""
    half = d / 2.0
    return half + random.random() * half


def _build_ssl_context(
    opts: BackendOptions, log: logging.Logger
) -> ssl.SSLContext | None:
    """用配置的 CA 证书与客户端证书对组装可用于 mTLS 的 SSLContext。

    三个文件各自可选，但客户端证书与私钥必须同时提供——只给一半无法完成
    TLS 握手。加载失败时记录 error 后返回 None（降级为默认 TLS/明文
    行为），而不是让构造失败：服务必须先启动起来进入 fail-static（按本地
    缓存继续限速），证书问题留给运维修复，不能因此拖垮数据面。
    """
    if not (opts.ca_file or opts.cert_file or opts.key_file):
        return None
    try:
        ctx = ssl.create_default_context(
            cafile=opts.ca_file if opts.ca_file else None
        )
        if opts.cert_file or opts.key_file:
            if not (opts.cert_file and opts.key_file):
                raise ValueError("client TLS requires both cert and key files")
            ctx.load_cert_chain(opts.cert_file, opts.key_file)
        log.info(
            "TLS 客户端已配置完成，与管理后台的通信将走加密链路 "
            "ca_file=%s cert_file=%s mutual_tls=%s",
            opts.ca_file,
            opts.cert_file,
            bool(opts.cert_file),
        )
        return ctx
    except Exception as e:
        log.error(
            "TLS 客户端证书材料加载失败，降级为默认 HTTP 客户端继续运行"
            "（服务不因此退出，证书问题需运维修复） "
            "err=%s ca_file=%s cert_file=%s key_file=%s",
            e,
            opts.ca_file,
            opts.cert_file,
            opts.key_file,
        )
        return None


class Reporter:
    """封装与管理后台的全部通信。

    add_sample 由核心循环每 tick 调用；run 并发跑长轮询/flush/心跳三个
    协程直到任务被取消。心跳中的 mode/config_version 通过构造时传入的
    mode_fn/version_fn 动态获取——它们反映核心循环**实际已应用**的状态，
    而非 Reporter 自己收到过什么（收到 ≠ 应用，收敛判断以核心为准）。
    """

    def __init__(
        self,
        opts: BackendOptions,
        node_id: str,
        service_version: str,
        mode_fn: Callable[[], str] | None = None,
        version_fn: Callable[[], int] | None = None,
        log: logging.Logger | None = None,
        flush_interval_s: float = DEFAULT_FLUSH_INTERVAL_S,
        heartbeat_interval_s: float = DEFAULT_HEARTBEAT_INTERVAL_S,
    ) -> None:
        self._opts = opts
        self._node_id = node_id
        self._service_version = service_version
        # 末尾斜杠剥掉，保证 base + path 拼接不出现双斜杠。
        self._base_url = opts.base_url.rstrip("/")
        self._log = log if log is not None else logging.getLogger(__name__)
        self._mode_fn = mode_fn if mode_fn is not None else (lambda: model.MODE_DRY_RUN)
        self._version_fn = version_fn if version_fn is not None else (lambda: 0)

        # 向核心循环投递新配置的队列：容量 1 且由 _push_config 实现"合并"
        # （coalescing）语义——消费慢时旧配置被新配置顶掉，核心永远只看到
        # 最新一份。配置是全量下发、幂等应用的，中间版本没有单独应用的
        # 价值，跳过它们反而避免核心追着过期配置做无用功。
        self._configs: asyncio.Queue[model.ControllerConfig] = asyncio.Queue(1)
        # 长轮询已应用的最新配置版本，随请求上报给后台作为"我已有到哪个
        # 版本"的水位（幂等应用的依据，§3.6）。
        self._version = 0

        # 有界指标缓冲（上限 MAX_BUFFERED_SAMPLES）与丢弃告警节流计数。
        self._samples: list[dict[str, Any]] = []
        self._drop_events = 0
        self._dropped_total = 0
        # 心跳连续失败计数（仅心跳协程读写）。
        self._heartbeat_failures = 0

        # 两个周期通过构造参数暴露为公开属性，便于测试在 run 之前调短，
        # 让上报/心跳循环在秒级内可观测。
        self.flush_interval = flush_interval_s
        self.heartbeat_interval = heartbeat_interval_s

        # TLS 材料在构造时组装（失败降级默认，见 _build_ssl_context）；
        # ClientSession 惰性创建——它必须在运行中的事件循环里诞生，
        # 构造函数不保证处于协程上下文。
        self._ssl = _build_ssl_context(opts, self._log)
        self._session: aiohttp.ClientSession | None = None

    # ---- 对核心循环暴露的接口 -------------------------------------------

    @property
    def configs(self) -> "asyncio.Queue[model.ControllerConfig]":
        """新配置的投递队列（容量 1、合并语义，见 _push_config）。"""
        return self._configs

    def add_sample(
        self,
        now: float,
        usages: Iterable[model.EnvUsage],
        decisions: Iterable[model.Decision],
        mode: str,
        config_version: int,
    ) -> None:
        """把一次核心循环 tick 记入有界指标缓冲。

        只做内存操作、不含任何 I/O 也不 await，因此对每秒调用一次的核心
        循环是非阻塞的。缓冲已满时丢弃最旧样本（保新弃旧），丢弃告警按
        "首次 + 每 DROP_LOG_EVERY 次"节流。
        """
        # 决策按 env_id 建索引，再与用量逐环境拼合成完整样本。
        by_env = {d.env_id: d for d in decisions}
        envs: list[dict[str, Any]] = []
        for u in usages:
            d = by_env.get(u.env_id)
            envs.append(
                {
                    "env_id": u.env_id,
                    "rate_bps": u.rate_bps,
                    "mean10_bps": u.mean10_bps,
                    "ewma60_bps": u.ewma60_bps,
                    "conn_cur": u.conn_cur,
                    # 无对应决策时统一填零值：0 / "" / False，后台按
                    # "本拍无决策"解读。
                    "bwlim_bps": d.bwlim_bps if d is not None else 0.0,
                    "state": str(d.state) if d is not None else "",
                    "changed": d.changed if d is not None else False,
                }
            )
        self._samples.append(
            {
                "ts": int(now),  # Unix 秒
                "mode": mode,
                "config_version": int(config_version),
                "envs": envs,
            }
        )
        if len(self._samples) > MAX_BUFFERED_SAMPLES:
            dropped = len(self._samples) - MAX_BUFFERED_SAMPLES
            del self._samples[:dropped]
            self._dropped_total += dropped
            self._drop_events += 1
            # 断联期间每秒都会触发丢弃，按"首次 + 每 DROP_LOG_EVERY 次"
            # 节流告警，避免刷屏。
            if self._drop_events == 1 or self._drop_events % DROP_LOG_EVERY == 0:
                self._log.warning(
                    "上报缓冲区已满，丢弃最旧样本（后台长时间不可达） "
                    "dropped_now=%d dropped_total=%d buffer_cap=%d",
                    dropped,
                    self._dropped_total,
                    MAX_BUFFERED_SAMPLES,
                )

    async def run(self) -> None:
        """并发跑配置长轮询、指标上报、心跳三个循环，直到任务被取消。

        base_url 为空（standalone 模式的防御分支）时只挂起、不发任何
        请求——正常流程下 standalone 模式根本不会构造 Reporter，这里
        兜底防止空地址被拼进 URL。
        """
        if not self._base_url:
            self._log.warning(
                "上报器未配置管理后台地址，禁用配置长轮询/指标上报/心跳"
                "（服务按纯本地独立模式运行）"
            )
            await asyncio.Event().wait()  # 挂起直到被取消
            return
        try:
            await asyncio.gather(
                self._poll_loop(),
                self._flush_loop(),
                self._heartbeat_loop(),
            )
        finally:
            # run 结束（通常是被取消）时关闭惰性创建的会话，释放连接。
            if self._session is not None and not self._session.closed:
                await self._session.close()

    # ---- 配置长轮询 -------------------------------------------------------

    async def _poll_loop(self) -> None:
        """长轮询主循环，按结果分三路处理：

        - 200：应用新配置并立即重新轮询；
        - 204 / 客户端长轮询超时：后台无变更，属于正常静默，立即重连
          （这正是长轮询"挂住等变更"的设计，不算失败）；
        - 传输错误 / 非预期状态码：按指数退避重试（1s 起步、逐次翻倍、
          封顶 30s、带抖动），避免后台故障恢复瞬间被全体实例打爆。
        """
        backoff = 0.0
        failures = 0  # 连续失败次数，成功或超时即清零
        while True:
            try:
                cfg = await self._poll_once()
            except asyncio.CancelledError:
                raise
            except TimeoutError:
                # 长轮询到期而后台无动静：正常现象，立即重连。
                # （aiohttp 的总超时抛 asyncio.TimeoutError，3.11 起就是
                # 内建 TimeoutError。）
                self._log.debug(
                    "配置长轮询挂满到期且后台无变更，立即重新发起下一轮"
                    "（属正常静默，非失败） version=%d",
                    self._version,
                )
                backoff = 0.0
                failures = 0
                continue
            except Exception as e:
                failures += 1
                backoff = _next_backoff(backoff)
                d = _with_jitter(backoff)
                self._log.warning(
                    "配置长轮询失败，按指数退避后重试（断联期间按既有配置"
                    "继续限速） err=%s retry_in=%.2fs "
                    "consecutive_failures=%d",
                    e,
                    d,
                    failures,
                )
                await asyncio.sleep(d)
                continue
            backoff = 0.0
            failures = 0
            if cfg is not None:
                self._apply_config(cfg)
            else:
                # 204：后台明确表示"当前版本即最新"，立即重新挂起等待。
                self._log.debug(
                    "配置长轮询返回无更新（204，当前版本即最新），立即重新"
                    "挂起等待 version=%d",
                    self._version,
                )

    async def _poll_once(self) -> model.ControllerConfig | None:
        """发起一次长轮询请求，query 携带 node_id 与本地已应用的配置版本。

        返回 None 表示 204（"暂无更新"）。响应体读取受 MAX_RESPONSE_BYTES
        限制——超长响应会被截断而在 JSON 解析处失败，防止异常/恶意的
        超大回包耗尽内存。
        """
        session = self._ensure_session()
        url = (
            f"{self._base_url}{CONFIG_PATH}"
            f"?node_id={quote(self._node_id)}&version={self._version}"
        )
        async with session.get(url) as resp:
            if resp.status == 200:
                raw = await resp.content.read(MAX_RESPONSE_BYTES)
                return model.ControllerConfig.from_dict(json.loads(raw))
            if resp.status == 204:
                return None
            raise RuntimeError(f"config poll: unexpected status {resp.status}")

    def _apply_config(self, cfg: model.ControllerConfig) -> None:
        """处理一份新收到的配置，顺序刻意为"先落盘、再投递、最后推版本"：

        1. normalize：字段兜底，与 load_cache 读回时的处理保持一致；
        2. _persist_cache：先写 fail-static 缓存——即使落盘失败，配置仍
           在内存中生效（只是断联保护退化到上一份缓存），以 error 告警；
        3. _push_config：投递给核心循环（合并语义）；
        4. 推进长轮询水位，下一次轮询从新版本继续。
        """
        version_old = self._version
        cfg.normalize()
        try:
            self._persist_cache(cfg)
        except Exception as e:
            # 配置仍然在内存中生效；受影响的只有 fail-static 缓存（变旧）。
            self._log.error(
                "fail-static 配置缓存落盘失败（新配置仍在内存生效，但断联"
                "引导将退化为上一份缓存） err=%s path=%s",
                e,
                self._opts.cache_path,
            )
        self._push_config(cfg)
        self._version = cfg.version
        self._log.info(
            "收到管理后台下发的新配置，已投递给核心循环应用 "
            "version_old=%d version_new=%d "
            "mode=%s envs=%d",
            version_old,
            cfg.version,
            cfg.mode,
            len(cfg.envs),
        )

    def _persist_cache(self, cfg: model.ControllerConfig) -> None:
        """原子替换 fail-static 缓存文件：先写同目录下的临时文件并 fsync，
        再 os.replace 到目标路径。

        replace（rename）在同一文件系统内是原子操作，因此无论进程在哪一步
        崩溃，缓存文件要么是完整的旧版本、要么是完整的新版本，绝不会出现
        "写了一半"的残缺 JSON——这是 fail-static（§3.7）成立的前提：重启后
        load_cache 必须能读到一份可解析的配置。临时文件必须与目标同目录，
        否则 replace 可能跨文件系统而失去原子性。
        """
        if not self._opts.cache_path:
            return
        data = json.dumps(cfg.to_dict()).encode("utf-8")
        directory = os.path.dirname(self._opts.cache_path) or "."
        fd, tmp = tempfile.mkstemp(dir=directory, prefix=".config-cache-")
        try:
            with os.fdopen(fd, "wb") as f:
                f.write(data)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, self._opts.cache_path)
        except BaseException:
            # 任何失败都清掉临时文件，避免目录里积累孤儿 .config-cache-*。
            try:
                os.unlink(tmp)
            except OSError:
                pass
            raise
        self._log.debug(
            "配置缓存已原子落盘（断联时据此 fail-static 引导） path=%s bytes=%d",
            self._opts.cache_path,
            len(data),
        )

    def _push_config(self, cfg: model.ControllerConfig) -> None:
        """把 cfg 投入容量为 1 的队列，并实现合并语义：若队列里还压着
        一份未被消费的旧配置，先弹掉它再投递新的，保证慢消费者读到的
        永远是最新版本。生产者只有长轮询协程一个，因此"弹旧-投新"的
        循环必然在有限步内结束。"""
        while True:
            try:
                self._configs.put_nowait(cfg)
                return
            except asyncio.QueueFull:
                try:
                    self._configs.get_nowait()
                except asyncio.QueueEmpty:
                    pass

    # ---- 指标批量上报 -----------------------------------------------------

    async def _flush_loop(self) -> None:
        """按固定节拍（flush_interval，默认 5s）上报缓冲中的指标样本。"""
        while True:
            await asyncio.sleep(self.flush_interval)
            await self._flush_once()

    async def _flush_once(self) -> None:
        """摘走当前缓冲并 POST 给后台。

        失败时把这批样本放回缓冲头部（保持时间顺序，排在上报期间新到的
        样本之前），并重新套用缓冲上限——因此断联期间样本不会丢在半路上，
        只会在缓冲溢出时从最旧的开始被淘汰。单线程事件循环下"摘缓冲/
        回填"天然与 add_sample 互斥（两段代码之间没有 await 切换点）。
        """
        if not self._samples:
            return
        batch = self._samples
        self._samples = []
        payload = {
            "node_id": self._node_id,
            "service_version": self._service_version,
            "samples": batch,
        }
        start = time.monotonic()
        try:
            await self._post_json(METRICS_PATH, payload)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            # 回填批次并重新套用上限，随后记录当前水位便于观察积压程度。
            self._samples = batch + self._samples
            if len(self._samples) > MAX_BUFFERED_SAMPLES:
                del self._samples[: len(self._samples) - MAX_BUFFERED_SAMPLES]
            self._log.warning(
                "用量上报失败，样本保留在缓冲区待下轮补送 err=%s samples=%d "
                "buffered=%d buffer_cap=%d",
                e,
                len(batch),
                len(self._samples),
                MAX_BUFFERED_SAMPLES,
            )
            return
        self._log.debug(
            "用量样本已批量上报管理后台 samples=%d duration=%.3fs",
            len(batch),
            time.monotonic() - start,
        )

    # ---- 心跳 -------------------------------------------------------------

    async def _heartbeat_loop(self) -> None:
        """启动后立即先发一次心跳（服务重启后让后台第一时间看到实例
        回归），之后按 heartbeat_interval 周期发送。"""
        await self._heartbeat_once()
        while True:
            await asyncio.sleep(self.heartbeat_interval)
            await self._heartbeat_once()

    async def _heartbeat_once(self) -> None:
        """发送一次心跳，内容取 mode_fn/version_fn 的当前返回值（核心
        循环实际已应用的模式与配置版本）。失败仅告警不重试（下个周期
        自然重发），并携带连续失败计数——运维可据此结合"失联 > 1 分钟"
        告警判断断联时长。"""
        payload = {
            "node_id": self._node_id,
            "service_version": self._service_version,
            "mode": self._mode_fn(),
            "config_version": int(self._version_fn()),
        }
        try:
            await self._post_json(HEARTBEAT_PATH, payload)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            self._heartbeat_failures += 1
            self._log.warning(
                "心跳上报失败，下个周期自然重发 err=%s consecutive_failures=%d",
                e,
                self._heartbeat_failures,
            )
            return
        self._heartbeat_failures = 0

    # ---- HTTP 基础设施 ----------------------------------------------------

    def _ensure_session(self) -> aiohttp.ClientSession:
        """惰性创建共享的 ClientSession（必须诞生在运行中的事件循环里，
        因此不能放在 __init__）。TLS 上下文构建成功时挂到连接器上。"""
        if self._session is None or self._session.closed:
            timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT_S)
            if self._ssl is not None:
                self._session = aiohttp.ClientSession(
                    timeout=timeout,
                    connector=aiohttp.TCPConnector(ssl=self._ssl),
                )
            else:
                self._session = aiohttp.ClientSession(timeout=timeout)
        return self._session

    async def _post_json(self, path: str, payload: dict[str, Any]) -> None:
        """把 payload 编码为 JSON 后 POST 到 base_url+path，非 2xx 状态码
        视为错误。响应体在 MAX_RESPONSE_BYTES 限制内排空后释放，保证
        底层连接可复用。"""
        session = self._ensure_session()
        body = json.dumps(payload).encode("utf-8")
        async with session.post(
            self._base_url + path,
            data=body,
            headers={"Content-Type": "application/json"},
        ) as resp:
            await resp.content.read(MAX_RESPONSE_BYTES)
            if resp.status // 100 != 2:
                raise RuntimeError(
                    f"POST {path}: unexpected status {resp.status}"
                )
