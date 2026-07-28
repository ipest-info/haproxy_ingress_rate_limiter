# rl_limiter.loop —— 监控主循环（单 HAProxy 模型，监控单位 = frontend）。
#
# 每个 tick 按固定流水线执行"采集 → 超限判定 → 发布"。限速本身由本机
# 内核 tc 执行（见 rl_limiter.tcshaper）；本循环的职责是每秒产出各受管 frontend 的
# 带宽视图，并对照配置里的限额做**持续超限告警**。
#
# 说明：下发不在本循环里，而由两个独立任务承担——enforcer 把监听端口与
# 后端写进 haproxy.cfg，tcshaper 把限额下发到内核 tc。每次配置应用后本
# 循环 set 事件叫醒它们（见 _apply_config 末尾）。
#
# 限额与数据面同源之后，"实测超过限额"基本只剩三种可能：tc 下发失败
# （tcshaper 会另行告警）、流量打满限额而 HTB 有正常的 burst 过冲余量、
# 或者**口径差异**——tc 限的是链路层字节（含 IP/TCP 头），本循环采的是
# HAProxy 的应用层 bytes_out，后者本就应该略低于前者。
#
# 并发模型：整个循环运行在单个 asyncio 任务中，组件间不会并发访问，
# 因此无需任何锁；配置通过 asyncio.Queue 注入，时间通过 tick_interval_s
# 对齐的 asyncio.sleep 推进。

from __future__ import annotations

import asyncio
import logging
import time

from . import model

# 周期状态汇总日志的输出频率：每 60 个 tick（约每分钟）输出一条 info，
# 便于在正常运行时低成本确认"循环活着、配置版本正确、各节点用量正常"。
STATUS_SUMMARY_EVERY_TICKS = 60

# 持续超限告警判据：mean10 连续高于限额这么多秒才告警（瞬时冲高不告），
# 回落同样持续这么多秒才解除——两侧都有滞回，避免在临界值附近抖动刷屏。
OVER_ALERT_AFTER_S = 10

# 超限持续期间的重复提醒间隔（秒）：进入告警后每隔这么久再发一条，
# 避免长期配置漂移只在最初告警一次就淹没在日志里。
OVER_REMIND_EVERY_S = 300


def _summarize_quotas(frontends: list[model.FrontendConfig]) -> str:
    """把各 frontend 限额压缩成单个日志字段，格式
    "fe_main=200000000;fe_api=..."，数值为配置口径的 bits/s。"""
    return ";".join(f"{f.name}={f.quota_bits_per_sec}" for f in frontends)


def _summarize_usages(usages: list[model.FrontendUsage]) -> str:
    """把各 frontend 用量压缩成单个日志字段，格式
    "fe_main:mean10_bytes_per_s=12345,conn=6;..."。mean10 为计费口径的
    10 秒滑动均值（bytes/s），conn 为当前并发连接数。"""
    return ";".join(
        f"{u.name}:mean10_bytes_per_s={u.mean10_bps:.0f},conn={u.conn_cur}"
        for u in usages
    )


class _OverState:
    """单个 frontend 的超限滞回状态（纯计数，秒数即 tick 数）。"""

    __slots__ = ("over_secs", "under_secs", "alerting", "since_last_remind")

    def __init__(self):
        self.over_secs = 0
        self.under_secs = 0
        self.alerting = False
        self.since_last_remind = 0


class MonitorLoop:
    """rl-limiter 的集中监控循环。

    组件契约：
      - collector.set_managed(set[str])；async collector.tick(now)
        -> list[FrontendUsage]；collector.degraded -> bool
      - sampler 可选回调：sampler(now, usages)，供控制台 StatusHub 记录
        每拍快照。
    """

    def __init__(self, collector, sampler=None, log=None,
                 config_applied: "asyncio.Event | list[asyncio.Event] | None" = None):
        self._collector = collector
        self._sampler = sampler
        self._log = log if log is not None else logging.getLogger("rl_limiter.loop")
        # 最近一次成功应用的 ControllerConfig 版本号；单任务访问，无需原子量。
        self._version: int = 0
        # 已处理的 tick 累计数，用于日志节流与错误定位。
        self._ticks: int = 0
        # frontend 名 → 限额（bytes/s），超限判定的基准；随配置热更。
        self._quotas: dict[str, float] = {}
        # 当前生效的受管 frontend 配置，供 enforcer 取用（见 frontends()）。
        self._frontends: list[model.FrontendConfig] = []
        # frontend 名 → 超限滞回状态。
        self._over: dict[str, _OverState] = {}
        # 每次应用配置后被 set 的信号量，供各"下发"类任务（写 cfg 的
        # enforcer、下发限速的 tcshaper）做到"配置一改就立刻生效"，而不是
        # 干等下一个周期性 reconcile。None = 没人关心（纯监控形态）。
        #
        # **每个任务必须各持一个 Event，不能共用**：这些任务在被唤醒后会
        # clear() 自己的事件，共用一个的话，A 先醒来 clear 掉、B 还没回到
        # wait，就会漏掉这次变更（要等 30 秒的周期兜底才补上）。
        # 为兼容只有一个下发任务的调用方，这里也接受单个 Event。
        if config_applied is None:
            self._config_applied: list[asyncio.Event] = []
        elif isinstance(config_applied, asyncio.Event):
            self._config_applied = [config_applied]
        else:
            self._config_applied = list(config_applied)

    @property
    def version(self) -> int:
        """最近一次应用的配置版本号（尚未应用任何配置时为 0）。
        数据库模式下是配置内容的校验和；控制台展示它，便于核对配置
        是否已热更到位。"""
        return self._version

    def frontends(self) -> list[model.FrontendConfig]:
        """当前生效的受管 frontend 配置。

        配置下发任务（enforcer）拿它渲染 haproxy.cfg 的受管区块。每轮
        reconcile 都现取，因此配置热更后拿到的必然是新值。
        """
        return list(self._frontends)

    def seed(self, cfg: model.ControllerConfig) -> None:
        """在 run 启动之前同步应用一份初始配置，即"引导"语义：让循环从
        第一个 tick 起就带着限额基准工作（引导配置来自启动时加载的数据库
        快照或本地 YAML）。

        seed 与 run 内的配置应用走同一条 _apply_config 路径，语义完全一致。
        """
        self._apply_config(cfg)

    def _apply_config(self, cfg: model.ControllerConfig) -> None:
        """把一份配置原子地灌入：更新采集器的受管 frontend 集合与超限判定
        基准，最后记录版本号。调用方保证串行（seed 在 run 之前，run 内单
        任务），组件间不会看到半新半旧的配置。"""
        self._frontends = list(cfg.frontends)
        self._collector.set_managed(cfg.names())
        self._quotas = cfg.quotas()
        # 已下线 frontend 的滞回状态一并丢弃；限额变化的保留计数（判定基准
        # 换了，但"持续性"语义连续——限额下调后本就该尽快告警）。
        for name in list(self._over):
            if name not in self._quotas:
                del self._over[name]
        self._version = cfg.version
        self._log.info(
            "配置已应用到监控循环（受管 frontend / 限额基准已更新） "
            "version=%s frontends=%d quotas=%s",
            cfg.version, len(cfg.frontends), _summarize_quotas(cfg.frontends),
        )
        # 叫醒配置下发任务。放在最后：等本循环的基准先更新完，避免
        # enforcer 已经把新配置写进数据面、监控这边还在按旧基准判超限。
        for ev in self._config_applied:
            ev.set()

    async def run(self, config_queue: asyncio.Queue | None,
                  tick_interval_s: float = 1.0) -> None:
        """驱动循环直至所在任务被取消。

        - config_queue 传递配置源投递的新配置（数据库轮询任务，见
          dbconfig.watch）；standalone 模式下传 None，循环退化为纯 tick
          驱动。
        - 顺序保证：某个 tick 之前已经送达的配置，一定在处理该 tick 之前
          被应用——每拍开头先非阻塞地把队列里排队的配置全部排空再跑流水
          线，超限判定永远基于最新限额。
        - 节拍对齐：用"计算下一拍的绝对时刻再 sleep 差值"的方式推进，
          单拍处理耗时不会累积成节拍漂移。
        """
        ev = asyncio.get_running_loop()
        # 第一拍立即执行：seed 已经就位，无需白等一个周期。
        next_at = ev.time()
        while True:
            # 1) 配置优先于 tick：先排空队列中所有待应用配置（非阻塞）。
            if config_queue is not None:
                while True:
                    try:
                        cfg = config_queue.get_nowait()
                    except asyncio.QueueEmpty:
                        break
                    self._apply_config(cfg)
            # 2) 执行一次完整流水线。
            await self._tick(time.time())
            # 3) 对齐下一拍：按绝对时刻推进，防止处理耗时造成漂移。
            next_at += tick_interval_s
            now_m = ev.time()
            if next_at <= now_m:
                # 本拍耗时已超过一个周期：重新对齐到"当前时刻 + 周期"，
                # 跳过错过的拍而不是连续补拍。
                next_at = now_m + tick_interval_s
            await asyncio.sleep(max(0.0, next_at - ev.time()))

    async def _tick(self, now: float) -> None:
        """一次完整的监控流水线：采集 → 超限判定 → 发布。"""
        self._ticks += 1

        # 采集：本机 HAProxy 的各受管 frontend 统计。
        usages = await self._collector.tick(now)

        # 超限判定（带滞回的告警状态机）。
        for u in usages:
            self._check_over(u)

        # 采样发布：把本 tick 的完整结果交给可选的 sampler 回调（控制台）。
        if self._sampler is not None:
            self._sampler(now, usages)

        # 周期状态汇总：每 STATUS_SUMMARY_EVERY_TICKS 个 tick 输出一次，
        # 正常运行时以约 1 条/分钟的成本留下可核对的运行痕迹。
        if self._ticks % STATUS_SUMMARY_EVERY_TICKS == 0:
            alerting = sorted(
                name for name, st in self._over.items() if st.alerting)
            self._log.info(
                "运行状态周期汇总（每 60 拍输出一次） "
                "tick=%d config_version=%s frontends=%d "
                "sampling_degraded=%s over_quota=%s summary=%s",
                self._ticks, self._version, len(usages),
                self._collector.degraded,
                ",".join(alerting) if alerting else "-",
                _summarize_usages(usages),
            )

    def _check_over(self, u: model.FrontendUsage) -> None:
        """推进单个单元的超限滞回状态机并按需打告警/解除日志。

        判据用 mean10（承诺口径）对比限额；degraded（采样失联，数据陈旧）
        或限额未配置（<=0）时判定暂停、计数原地冻结——陈旧数据既不该
        触发新告警，也不该解除已有告警。
        """
        quota = self._quotas.get(u.name, 0.0)
        if quota <= 0 or u.degraded:
            return
        st = self._over.get(u.name)
        if st is None:
            st = self._over[u.name] = _OverState()

        if u.mean10_bps > quota:
            st.over_secs += 1
            st.under_secs = 0
        else:
            st.under_secs += 1
            st.over_secs = 0

        if not st.alerting and st.over_secs >= OVER_ALERT_AFTER_S:
            st.alerting = True
            st.since_last_remind = 0
            self._log.warning(
                "frontend 带宽持续高于限额（下发正常时这通常只是 HTB 的 "
                "burst 过冲；注意监控是应用层口径、tc 限的是链路层口径，"
                "前者理应略低于后者。若持续偏高很多，请检查 tc 下发是否失败"
                "——tcshaper 会另行告警） frontend=%s mean10_bytes_per_s=%.0f "
                "quota_bytes_per_s=%.0f utilization=%.2f over_secs=%d",
                u.name, u.mean10_bps, quota, u.mean10_bps / quota,
                st.over_secs)
        elif st.alerting and st.under_secs >= OVER_ALERT_AFTER_S:
            st.alerting = False
            self._log.info(
                "frontend 带宽已回落到限额以内，解除持续超限告警 "
                "frontend=%s mean10_bytes_per_s=%.0f quota_bytes_per_s=%.0f",
                u.name, u.mean10_bps, quota)
        elif st.alerting:
            st.since_last_remind += 1
            if st.since_last_remind >= OVER_REMIND_EVERY_S:
                st.since_last_remind = 0
                self._log.warning(
                    "frontend 带宽仍持续高于限额（重复提醒，每 %d 秒一次） "
                    "frontend=%s mean10_bytes_per_s=%.0f quota_bytes_per_s=%.0f "
                    "utilization=%.2f",
                    OVER_REMIND_EVERY_S, u.name, u.mean10_bps, quota,
                    u.mean10_bps / quota)
