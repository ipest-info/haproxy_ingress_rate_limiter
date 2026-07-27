# rl_limiter.loop —— 集中监控主循环（监控单元=节点）。
#
# 每个 tick 按固定流水线执行"采集 → 超限判定 → 发布"。限速本身由各台
# HAProxy 的 shared bwlim（配置常量）执行，rl-limiter 不向数据面写入
# 任何东西；本循环的职责是：每秒聚合各节点带宽视图、对照配置库中的
# 节点限额做**持续超限告警**（实测持续高于限额，通常意味着 HAProxy
# 配置里的 limit 与库中登记值不一致，或该节点漏配了限速）。
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


def _summarize_quotas(envs: list[model.EnvQuota]) -> str:
    """把节点限额压缩成单个日志字段，格式 "hap-1=200000000;hap-2=..."，
    数值为配置口径的 bits/s。"""
    return ";".join(f"{e.env_id}={e.quota_bits_per_sec}" for e in envs)


def _summarize_usages(usages: list[model.EnvUsage]) -> str:
    """把各节点用量压缩成单个日志字段，格式
    "hap-1:mean10_bytes_per_s=12345,conn=6;hap-2:..."。mean10 为计费口径
    的 10 秒滑动均值（bytes/s），conn 为当前并发连接数之和。"""
    return ";".join(
        f"{u.env_id}:mean10_bytes_per_s={u.mean10_bps:.0f},conn={u.conn_cur}"
        for u in usages
    )


class _OverState:
    """单个监控单元的超限滞回状态（纯计数，秒数即 tick 数）。"""

    __slots__ = ("over_secs", "under_secs", "alerting", "since_last_remind")

    def __init__(self):
        self.over_secs = 0
        self.under_secs = 0
        self.alerting = False
        self.since_last_remind = 0


class MonitorLoop:
    """rl-limiter 的集中监控循环。

    组件契约：
      - collector.set_mapping(dict[Target, str])；async collector.tick(now)
        -> list[EnvUsage]；collector.degraded_nodes() -> set[str]
      - sampler 可选回调：sampler(now, usages)，供控制台 StatusHub 记录
        每拍快照。
    """

    def __init__(self, collector, sampler=None, log=None,
                 config_applied: "asyncio.Event | None" = None):
        self._collector = collector
        self._sampler = sampler
        self._log = log if log is not None else logging.getLogger("rl_limiter.loop")
        # 最近一次成功应用的 ControllerConfig 版本号；单任务访问，无需原子量。
        self._version: int = 0
        # 已处理的 tick 累计数，用于日志节流与错误定位。
        self._ticks: int = 0
        # 节点名 → 限额（bytes/s），超限判定的基准；随配置热更。
        self._quotas: dict[str, float] = {}
        # 节点名 → 该节点挂的 frontend 集合（限额自动应用用它定位配置段）。
        self._unit_frontends: dict[str, set[str]] = {}
        # 节点名 → 超限滞回状态。
        self._over: dict[str, _OverState] = {}
        # 每次应用配置后被 set 的信号量：同机部署下，限额应用任务
        # （enforcer）靠它做到"库里一改就立刻落到本机 HAProxy"，而不是
        # 干等下一个周期性 reconcile。None = 没人关心（纯监控形态）。
        self._config_applied = config_applied

    @property
    def version(self) -> int:
        """最近一次应用的配置版本号（尚未应用任何配置时为 0）。
        数据库模式下是配置内容的校验和；控制台展示它，便于核对配置
        是否已热更到位。"""
        return self._version

    def frontend_limits(self) -> dict[str, int]:
        """当前生效配置里"每个受控 frontend 应有的限额"（bytes/s）。

        限额自动应用（enforcer）拿它当目标值。监控单元 = 节点，一个节点
        的限额由它挂的全部 frontend 共同承担；同机形态下本机通常只有一个
        受控 frontend，直接把节点限额给它。

        一个节点挂了多个 frontend 时不做拆分而是整体跳过：把节点限额原样
        写给每个 frontend 会让实际总量翻倍（每个 frontend 各限这么多），
        平均分摊又没有业务依据。这种拓扑必须人工决定怎么分，宁可不动
        数据面并在日志里说清楚。
        """
        out: dict[str, int] = {}
        for env_id, limit in self._quotas.items():
            fes = sorted(self._unit_frontends.get(env_id, ()))
            if len(fes) != 1:
                if fes:
                    self._log.warning(
                        "节点挂了多个受控 frontend，限额自动应用跳过该节点"
                        "（拆分方式需要人工决定：原样各写一份会让实际总量"
                        "翻倍） node=%s frontends=%s", env_id, ",".join(fes))
                continue
            out[fes[0]] = int(limit)
        return out

    def seed(self, cfg: model.ControllerConfig) -> None:
        """在 run 启动之前同步应用一份初始配置，即"引导"语义：让循环从
        第一个 tick 起就带着限额基准工作（引导配置来自启动时加载的数据库
        快照或本地 YAML）。

        seed 与 run 内的配置应用走同一条 _apply_config 路径，语义完全一致。
        """
        self._apply_config(cfg)

    def _apply_config(self, cfg: model.ControllerConfig) -> None:
        """把一份配置原子地灌入：更新采集器的 Target→单元 映射与超限
        判定基准，最后记录版本号。调用方保证串行（seed 在 run 之前，
        run 内单任务），组件间不会看到半新半旧的配置。"""
        self._collector.set_mapping(cfg.target_to_env())
        self._quotas = {e.env_id: e.quota_bytes_per_sec for e in cfg.envs}
        # 单元（节点）→ 它挂的 frontend 集合，供限额自动应用定位改哪一段。
        self._unit_frontends = {
            e.env_id: {t.frontend for t in e.targets} for e in cfg.envs
        }
        # 已下线单元的滞回状态一并丢弃；限额变化的单元保留计数（判定基准
        # 换了，但"持续性"语义连续——限额下调后本就该尽快告警）。
        for env_id in list(self._over):
            if env_id not in self._quotas:
                del self._over[env_id]
        self._version = cfg.version
        self._log.info(
            "配置已应用到监控循环（映射/限额基准已更新） "
            "version=%s units=%d unit_quotas=%s",
            cfg.version, len(cfg.envs), _summarize_quotas(cfg.envs),
        )
        # 叫醒限额应用任务。放在最后：等本循环的基准先更新完，避免
        # enforcer 已经把新限额写进数据面、监控这边还在按旧基准判超限。
        if self._config_applied is not None:
            self._config_applied.set()

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

        # 采集：所有节点的 frontend 统计按监控单元（节点）聚合。
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
            degraded = sorted(self._collector.degraded_nodes())
            alerting = sorted(
                env_id for env_id, st in self._over.items() if st.alerting)
            self._log.info(
                "运行状态周期汇总（每 60 拍输出一次） "
                "tick=%d config_version=%s units=%d "
                "degraded_nodes=%s over_quota_units=%s units_summary=%s",
                self._ticks, self._version, len(usages),
                ",".join(degraded) if degraded else "-",
                ",".join(alerting) if alerting else "-",
                _summarize_usages(usages),
            )

    def _check_over(self, u: model.EnvUsage) -> None:
        """推进单个单元的超限滞回状态机并按需打告警/解除日志。

        判据用 mean10（承诺口径）对比限额；degraded（采样失联，数据陈旧）
        或限额未配置（<=0）时判定暂停、计数原地冻结——陈旧数据既不该
        触发新告警，也不该解除已有告警。
        """
        quota = self._quotas.get(u.env_id, 0.0)
        if quota <= 0 or u.degraded:
            return
        st = self._over.get(u.env_id)
        if st is None:
            st = self._over[u.env_id] = _OverState()

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
                "节点带宽持续高于登记限额（HAProxy 侧 shared bwlim 的 limit "
                "可能与配置库不一致，或该节点漏配限速——请核对该节点 "
                "haproxy.cfg 并 reload） node=%s mean10_bytes_per_s=%.0f "
                "quota_bytes_per_s=%.0f utilization=%.2f over_secs=%d",
                u.env_id, u.mean10_bps, quota, u.mean10_bps / quota,
                st.over_secs)
        elif st.alerting and st.under_secs >= OVER_ALERT_AFTER_S:
            st.alerting = False
            self._log.info(
                "节点带宽已回落到登记限额以内，解除持续超限告警 "
                "node=%s mean10_bytes_per_s=%.0f quota_bytes_per_s=%.0f",
                u.env_id, u.mean10_bps, quota)
        elif st.alerting:
            st.since_last_remind += 1
            if st.since_last_remind >= OVER_REMIND_EVERY_S:
                st.since_last_remind = 0
                self._log.warning(
                    "节点带宽仍持续高于登记限额（重复提醒，每 %d 秒一次） "
                    "node=%s mean10_bytes_per_s=%.0f quota_bytes_per_s=%.0f "
                    "utilization=%.2f",
                    OVER_REMIND_EVERY_S, u.env_id, u.mean10_bps, quota,
                    u.mean10_bps / quota)
