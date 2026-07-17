# rl_limiter.governor —— 每秒执行一次的快环 AIMD 控制器（设计文档 §3.3
# "本地快环控制算法"）。
#
# 架构位置：governor 位于服务核心循环的"决策"环节——上游是 collector
# 每秒产出的各环境用量（10s 滑动均值等），下游是 allocator + executor
# （设计文档 §3.2）负责把决策拆分并真正写入各台 HAProxy。governor 本身
# 是纯决策组件：既不接触 HAProxy，也不读系统时钟，因此可以被完全确定
# 性地单测。
#
# 控制单元=节点（见 model.py 顶部说明）：AIMD 针对单台节点上受控
# frontend 的聚合用量决策，决策携带该节点的 Target（节点, frontend）
# 列表，节点内的拆分与写回交给下游（allocator + executor）。
#
# 控制目标（承诺口径）：10 秒滑动均值 ≤ 约定配额，瞬时允许冲高到弹性
# 上限（默认 quota × 1.10）。由于整形常驻生效，算法退化为对整形值
# bwlim 的动态微调，采用 AIMD（急收慢放）结构：
#
#   - 超限持续 ≥ tighten_after_s 秒 → 乘性收紧：bwlim ×= md_factor
#     （下限 quota × tighten_floor）。乘性收缩能在少数几拍内把均值
#     压回配额以内，响应速度与超限幅度成正比——超得越狠收得越快；
#   - 用量低于低水位持续 ≥ recover_after_s 秒 → 加性放松：bwlim +=
#     quota × ai_step_frac（上限为弹性 ceiling）。放松刻意走慢速线性
#     步进，避免一放开就立即再次超限。
#
# 这种"乘性收紧、加性放松"的不对称结构是 TCP 拥塞控制验证过的稳定
# 形态，专门用来防止"限速→带宽掉→放开→又超限"的锯齿振荡。
#
# 时间语义：tick 的时间戳由调用方注入，算法本身不依赖墙钟差值，而是
# 假定核心循环固定 1 秒一拍，因此持续性计数器（over_secs / under_secs）
# 直接以 tick 计数充当秒数。
#
# 并发说明：本模块运行在单线程 asyncio 事件循环内，update_config 与
# tick 都是同步方法、不含 await 点，执行期间不会让出控制权，天然原子，
# 配置热更新与核心循环不会交错访问 envs 表，无需加锁。

from __future__ import annotations

import logging
import math
from dataclasses import dataclass

from . import model

# changed 发射（emit）迟滞阈值：只有当本拍目标值与上一次以 changed=True
# 发射的值相差超过配额的 0.1% 时，决策才会被标记为 changed。这个
# epsilon 的存在有两个目的：
#
#  1. 抑制无意义的抖动写入——浮点运算带来的微小漂移不值得触发一次
#     HAProxy runtime API 写操作；
#  2. 防止漂移被"静默吞掉"——last_emitted 只在 changed 时才推进，
#     因此若干拍的亚阈值漂移会持续累积，一旦累积量越过 0.1% 便会
#     整体发射出去，长期看目标值不会因迟滞而系统性偏离。
CHANGED_EPSILON_FRAC = 0.001


@dataclass(slots=True)
class _EnvState:
    """单个控制单元（节点）的完整控制状态。所有速率字段单位均为 bytes/s。"""

    targets: list[model.Target]        # 该单元的全部挂载点（同一节点上的受控 frontend）
    quota_bytes: float                 # 该节点自己的带宽限制，bytes/s
    params: model.GovParams            # 快环控制参数（可按节点覆盖）
    bwlim: float                       # 当前整形目标值（节点聚合口径），bytes/s
    over_secs: int = 0                 # mean10 > quota 的连续拍数（收紧持续性计数）
    under_secs: int = 0                # mean10 < quota×low_watermark 的连续拍数（恢复持续性计数）
    state: model.GovState = model.GovState.NORMAL  # 状态机当前状态
    last_emitted: float = 0.0          # 上一次以 changed=True 发射出去的值
    emitted: bool = False              # 首次 changed 发射之前为 False，用于强制首拍发射


class Governor:
    """持有全部已配置环境的快环控制状态。"""

    def __init__(self, log: logging.Logger | None = None) -> None:
        # 传入 None 时回退到模块级 logger。
        self._log = log if log is not None else logging.getLogger(__name__)
        self._envs: dict[str, _EnvState] = {}

    def update_config(self, envs: list[model.EnvQuota]) -> None:
        """用新的环境列表整体替换当前配置。

        - 新增环境：初始 bwlim 停靠在弹性上限（quota × elastic_ceiling），
          状态为 NORMAL——这对应设计文档 §3.3 的"常态"分支；
        - 已有环境的配额或参数发生变化：控制状态重置——bwlim 回到新的
          弹性上限、持续性计数清零、状态回到 NORMAL。重置而非平滑过渡
          的原因是：旧的收紧/恢复进度是基于旧配额算出来的，换了刻度后
          继续沿用没有意义，不如从干净状态重新收敛；
        - 新列表中不存在的环境被直接删除；
        - 重复出现的 env_id 只取第一个，后者告警忽略。

        仅 targets 列表变化不会重置控制状态（配额刻度没变，收敛进度仍然
        有效），但会清掉 emitted 标志强制下一拍发射 changed 决策，确保新
        映射进来的挂载点能立刻拿到当前整形值，而不是等到目标值下次漂移。
        """
        seen: set[str] = set()
        for e in envs:
            if e.env_id in seen:
                self._log.warning("配置中出现重复的 env_id，仅第一条生效，后续重复项已忽略 env=%s", e.env_id)
                continue
            seen.add(e.env_id)

            quota = e.quota_bytes_per_sec
            params = e.effective_params()
            targets = list(e.targets)
            ceil = quota * params.elastic_ceiling

            st = self._envs.get(e.env_id)
            if st is None:
                # 新环境：直接停靠在弹性上限，等待第一拍用量数据。
                self._envs[e.env_id] = _EnvState(
                    targets=targets,
                    quota_bytes=quota,
                    params=params,
                    bwlim=ceil,
                )
                self._log.info(
                    "新增受控环境，整形值初始化为弹性上限，等待首拍用量数据后开始调节 "
                    "env=%s quota_bytes_per_s=%s ceil_bytes_per_s=%s "
                    "bwlim_bytes_per_s=%s targets=%s params=%s",
                    e.env_id, quota, ceil, ceil,
                    [str(t) for t in targets], params.to_dict(),
                )
                continue

            if st.quota_bytes != quota or st.params != params:
                # 配额/参数变化：整套控制状态重置到新刻度下的常态。
                old_quota = st.quota_bytes
                old_bwlim = st.bwlim
                st.quota_bytes = quota
                st.params = params
                st.bwlim = ceil
                st.over_secs = 0
                st.under_secs = 0
                st.state = model.GovState.NORMAL
                st.emitted = False  # 强制重新发射重置后的值
                self._log.info(
                    "环境配额/参数变更，控制状态整体重置，整形值重置为新配额下的弹性上限 env=%s "
                    "old_quota_bytes_per_s=%s new_quota_bytes_per_s=%s "
                    "ceil_bytes_per_s=%s bwlim_old=%s bwlim_new=%s params=%s",
                    e.env_id, old_quota, quota, ceil, old_bwlim, st.bwlim,
                    params.to_dict(),
                )
            if st.targets != targets:
                # targets 集合变化：不动控制状态，只强制下一拍发射，
                # 让新挂载点立即拿到当前整形值。
                st.targets = targets
                st.emitted = False

        # 删除新配置中不再出现的环境。
        for env_id in list(self._envs):
            if env_id not in seen:
                del self._envs[env_id]
                self._log.info("环境已从配置中移除，停止对其限速控制并丢弃其控制状态 env=%s", env_id)

    def tick(self, now: float, usages: list[model.EnvUsage]) -> list[model.Decision]:
        """对每个用量样本推进一步 AIMD，并为每个"已配置且有样本"的环境
        返回一条决策。未配置的 env_id 被忽略；已配置但本拍缺样本的环境
        不产生决策——没有数据时保持现状（hold），绝不凭空猜测。

        now 为注入参数，仅为可测试性保留：算法依赖固定 1s 的 tick 节奏，
        不依赖墙钟差值，因此这里刻意不使用它。
        """
        del now  # 持续性计数器以固定 1s 节奏的 tick 计数充当秒数

        decisions: list[model.Decision] = []
        for u in usages:
            st = self._envs.get(u.env_id)
            if st is None:
                continue
            decisions.append(self._step(st, u))
        return decisions

    def _step(self, st: _EnvState, u: model.EnvUsage) -> model.Decision:
        """将单个环境推进一拍并产出该环境的决策。这里是 AIMD 状态机的
        全部分支逻辑所在。"""
        if u.degraded:
            # 降级冻结：采样链路持续失败，collector 送来的是"保持上次
            # 良好值"的陈旧数据。基于陈旧数据做任何调整都可能放大错误
            # （比如误把已经回落的流量继续收紧），因此冻结 bwlim、状态和
            # 持续性计数，changed=False，等待数据恢复后再继续推进。
            self._log.debug(
                "采样链路降级，本拍数据为陈旧保持值，冻结该环境的整形值与状态等待数据恢复 "
                "env=%s state=%s bwlim_bytes_per_s=%s",
                u.env_id, st.state, st.bwlim,
            )
            return model.Decision(
                env_id=u.env_id,
                targets=list(st.targets),
                bwlim_bps=st.bwlim,
                state=st.state,
                changed=False,
            )

        q = st.quota_bytes
        p = st.params
        ceil = q * p.elastic_ceiling
        m = u.mean10_bps

        old_state = st.state
        old_bwlim = st.bwlim

        if m > q:
            # —— 超限分支：10s 均值越过配额（承诺口径被打破）——
            # 先累计超限持续拍数并清空恢复计数（两个计数器互斥，方向
            # 一旦反转就要求对方重新计满，这本身就是一层防抖）。
            st.over_secs += 1
            st.under_secs = 0
            if st.over_secs >= p.tighten_after_s:
                # 持续超限达到阈值才动手，过滤掉 1~2 秒的瞬时毛刺。
                # 乘性收紧（bwlim ×= md_factor）：收缩量与当前值成正比，
                # 超限越久收得越快，能以几何速度把均值压回配额内；
                # 下限 quota × tighten_floor 保证不会把用户压到远低于
                # 其付费配额的水平——收紧的目标是"压回配额"，不是惩罚。
                st.bwlim = max(q * p.tighten_floor, st.bwlim * p.md_factor)
                st.state = model.GovState.TIGHTENING
        elif m < q * p.low_watermark:
            # —— 低水位分支：均值低于 quota × low_watermark ——
            # 恢复阈值刻意低于配额本身（默认 0.90），与上方的超限阈值
            # (1.0) 之间留出一条死区，避免均值在配额附近来回穿越时
            # 收紧/放松交替触发。
            st.under_secs += 1
            st.over_secs = 0
            if st.bwlim < ceil and st.under_secs >= p.recover_after_s:
                # 加性放松（bwlim += quota × ai_step_frac）：固定小步慢速
                # 归还带宽，比乘性放大稳得多——如果放松也用乘性，刚收
                # 紧完就会被指数级放回去，AIMD 的防振荡结构就失效了。
                # 上限是弹性 ceiling；到顶即回到 NORMAL（常态），否则
                # 停留在 RECOVERING 表示"仍在爬坡途中"。
                st.bwlim = min(ceil, st.bwlim + q * p.ai_step_frac)
                if st.bwlim >= ceil:
                    st.state = model.GovState.NORMAL
                else:
                    st.state = model.GovState.RECOVERING
        else:
            # —— 死区分支：quota×low_watermark ≤ mean10 ≤ quota ——
            # 均值落在承诺口径以内但尚未低到值得放松的程度：保持当前
            # bwlim 不动。两个持续性计数都清零，因为"持续超限/持续
            # 低水位"的判定要求条件连续成立——一旦回到死区，之前的
            # 累计就不再代表一个连续区间，必须重新计数，否则断断续续
            # 的越界会被错误地拼接成"持续越界"。注意非 NORMAL 状态在
            # 死区中被保留：只有 bwlim 真正爬回 ceiling 才算恢复完成。
            st.over_secs = 0
            st.under_secs = 0

        # 状态变迁日志：NORMAL/TIGHTENING/RECOVERING 任意互转都记录一条。
        if st.state != old_state:
            self._log.info(
                "环境 10 秒均值触发限速状态变迁（utilization 为 mean10/配额比值） "
                "env=%s state_from=%s state_to=%s "
                "mean10_bytes_per_s=%s quota_bytes_per_s=%s utilization=%s "
                "bwlim_old=%s bwlim_new=%s over_secs=%d under_secs=%d",
                u.env_id, old_state, st.state, m, q, _utilization(m, q),
                old_bwlim, st.bwlim, st.over_secs, st.under_secs,
            )
        # bwlim 实际调整日志：只有数值真的变了才记（触底/到顶后的空转
        # 不算调整），收紧与放松使用不同的消息便于检索。
        if st.bwlim < old_bwlim:
            self._log.info(
                "均值持续超配额，乘性收紧整形值以压回承诺口径 env=%s bwlim_old=%s bwlim_new=%s "
                "mean10_bytes_per_s=%s quota_bytes_per_s=%s utilization=%s over_secs=%d",
                u.env_id, old_bwlim, st.bwlim, m, q, _utilization(m, q), st.over_secs,
            )
        elif st.bwlim > old_bwlim:
            self._log.info(
                "均值回落至低水位，加性放松整形值逐步归还带宽 env=%s bwlim_old=%s bwlim_new=%s "
                "mean10_bytes_per_s=%s quota_bytes_per_s=%s utilization=%s under_secs=%d",
                u.env_id, old_bwlim, st.bwlim, m, q, _utilization(m, q), st.under_secs,
            )

        # 发射判定：首次必发（emitted=False），此后仅当与上次发射值的
        # 偏差超过配额的 0.1%（CHANGED_EPSILON_FRAC）才标记 changed，
        # 详见常量注释中关于迟滞与漂移累积的说明。
        changed = (not st.emitted) or abs(st.bwlim - st.last_emitted) > CHANGED_EPSILON_FRAC * q
        if changed:
            st.emitted = True
            st.last_emitted = st.bwlim

        # 每拍 per-env 摘要（debug 级），用于问题排查时还原完整时间线。
        self._log.debug(
            "本拍环境控制状态摘要（用于排查时还原时间线） env=%s state=%s mean10_bytes_per_s=%s "
            "quota_bytes_per_s=%s utilization=%s bwlim_bytes_per_s=%s "
            "over_secs=%d under_secs=%d changed=%s",
            u.env_id, st.state, m, q, _utilization(m, q), st.bwlim,
            st.over_secs, st.under_secs, changed,
        )

        return model.Decision(
            env_id=u.env_id,
            targets=list(st.targets),
            bwlim_bps=st.bwlim,
            state=st.state,
            changed=changed,
        )


def _utilization(mean10: float, quota: float) -> float:
    """计算 mean10/quota 的利用率，保留两位小数，仅用于日志展示；
    quota 非正时返回 0 以避免日志里出现 inf/nan。"""
    if quota <= 0:
        return 0.0
    return math.floor(mean10 / quota * 100 + 0.5) / 100
