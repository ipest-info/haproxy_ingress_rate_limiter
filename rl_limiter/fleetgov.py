# rl_limiter.fleetgov —— 总控的按需动态分配算法（纯逻辑，零 I/O）。
#
# 职责：把 total_egress_mbps（全部节点出向合计的目标）切成各节点的
# instance_quota_mbps（实例出向目标）。节点自己的闭环（ingressgov）再把
# 出向目标转成入向限速——总控是慢速外环，节点闭环是快速内环。
#
# 算法（每 alloc_interval_s 一轮，默认 15s）：
#
#   - **观测**：节点出向带宽（与节点闭环同口径：regulation.instance.
#     egress_mbps，即全部 frontend 出向 mean10 之和；由 fleetpoll 提取）。
#     分配输入再过一层 EWMA（smooth_alpha，每分配周期更新一次）——
#     mean10 + EWMA + 写抑制三层叠加防抖；
#   - **需求信号**：被自己配额压着的节点（出向 ≥ press_ratio×配额）无法
#     表达真实需求，按 配额×press_boost 虚报——给上冲空间，防"配额小→
#     观测小→分得更小"的饿死锁定，逐轮迭代自然收敛；
#   - **保底 + 按需比例**：每个可控节点保底 min_share_ratio×池/N，剩余
#     按需求占比瓜分。恒等式：Σ分配 + 冻结记账 = total（总量守恒）；
#   - **冻结记账（fail-static）**：离线/degraded/不可执行/无令牌的节点
#     **配额绝不碰**，但它占着的份额如实从池里扣掉；
#   - **写抑制**：相对变化 < apply_epsilon 且绝对变化 < min_delta_mbps
#     不下发——节点每次写都整份重写自己的 YAML，琐碎变化不值得；
#   - **下限**：分配值永远 ≥ min_quota_mbps。**绝不下发 ≤0**：0 在节点
#     语义里是"不限"，作为份额下发等于放开那台节点；
#   - **取消总量控制 = 解除**：total 从 >0 变为 0 时，全部在册节点标记
#     "待解除"，逐台 DELETE 实例总限速（节点恢复不限）。离线的保持待解除，
#     恢复在线即解；之后总控回到只汇总不下发。只认**运行期的变化**——
#     以 0 启动的总控不碰节点（不知道节点上的限速是谁设的）。
#
# 单协程访问（allocate_once 只被分配环调用），无需加锁。

from __future__ import annotations

import logging
from dataclasses import dataclass, field

from .fleetconfig import AllocConfig


@dataclass
class NodeState:
    """一台节点在总控里的全部状态（fleetpoll 维护观测侧，本模块维护
    需求/分配侧；页面快照直接从这里摊平）。"""

    name: str
    url: str = ""
    console_url: str = ""
    token: str = ""
    # ---- 轮询侧（fleetpoll 每拍更新）----
    online: bool = False
    fail_count: int = 0
    last_ok_ts: float = 0.0
    degraded: bool = False
    executable: bool = True
    # 节点自报的当前 instance_quota_mbps（None = 从未轮询成功）。
    quota_mbps: "float | None" = None
    egress_mbps: float = 0.0
    ingress_mbps: float = 0.0
    landed: "bool | None" = None
    error: str = ""
    # ---- 分配侧（本模块每个分配周期更新）----
    # 平滑后的需求（Mbps；None = 尚无观测，不参与分配）。
    demand_ewma_mbps: "float | None" = None
    # 最近一轮的分配结果（None = 本轮未分配/冻结）。
    desired_mbps: "float | None" = None
    # 总控最近一次**成功下发**的值（由 fleetpoll 在 PUT 成功后回填；
    # 自报值与它不符 = 下发未生效或节点侧被手工改动）。
    applied_mbps: "float | None" = None
    write_err: str = ""
    # 总量控制已取消、该节点的实例总限速还没解除（离线/下发失败时保持，
    # 后续每轮重试；成功 DELETE 或重新设了总带宽即清除）。
    release_pending: bool = False


class FleetGovernor:
    """按需动态分配控制器。无状态字段都在 NodeState 上，本类只持配置。"""

    def __init__(self, log: "logging.Logger | None" = None):
        self._log = log if log is not None else logging.getLogger(
            "rl_limiter.fleetgov")
        # 上一轮的总带宽（None = 还没跑过）：用来识别"取消总量控制"
        # 这个**变化**——以 0 启动不算取消。
        self._last_total: "float | None" = None

    def allocate_once(self, states: list, total: float,
                      params: AllocConfig) -> list:
        """跑一轮分配。更新各 NodeState 的 demand_ewma/desired，返回
        写清单 [(NodeState, 要下发的 Mbps | None)]（已过写抑制；None =
        解除该节点的实例总限速，即 DELETE）。

        states 是全部在册节点；total ≤ 0 = 不做总量控制：只解除取消
        那一刻还在册节点的限速（见 _release），不分配。
        """
        log = self._log
        cancelled = (self._last_total is not None and self._last_total > 0
                     and total <= 0)
        self._last_total = total
        if total <= 0:
            if cancelled:
                self._mark_release(states)
            return self._release(states)
        for s in states:
            s.release_pending = False

        ctrl = [s for s in states
                if s.online and s.executable and not s.degraded and s.token]
        frozen = [s for s in states if s not in ctrl]

        # 冻结记账：fail-static（失联节点的配额绝不碰）与总量守恒的
        # 交点——它占着的份额如实从池里扣掉。
        reserved = 0.0
        for s in frozen:
            s.desired_mbps = None
            if s.quota_mbps is None:
                if s.last_ok_ts == 0:
                    log.warning(
                        "节点从未轮询成功，配额未知，按 0 记账（该节点当前"
                        "不受总量守恒保护） node=%s", s.name)
            else:
                reserved += s.quota_mbps
        pool = max(0.0, total - reserved)
        if reserved > total:
            log.warning(
                "冻结节点（离线/降级/不可执行/无令牌）的配额之和已超过"
                "总带宽（超卖），可控节点将被压到保底份额 "
                "reserved=%.1fMbps total=%gMbps frozen=%d",
                reserved, total, len(frozen))
        if not ctrl:
            log.warning(
                "没有任何可控节点（在线+可执行+未降级+有令牌），本轮"
                "不下发 total=%gMbps nodes=%d", total, len(states))
            return []

        # 需求信号 + EWMA（每个分配周期更新一次；冻结节点不更新——
        # 陈旧观测不配参与平滑）。
        for s in ctrl:
            raw = max(0.0, s.egress_mbps)
            q = s.quota_mbps or 0.0
            if q > 0 and raw >= params.press_ratio * q:
                raw = q * params.press_boost
            if s.demand_ewma_mbps is None:
                s.demand_ewma_mbps = raw
            else:
                s.demand_ewma_mbps += params.smooth_alpha * (
                    raw - s.demand_ewma_mbps)

        n = len(ctrl)
        desired: dict = {}
        if pool < n * params.min_quota_mbps:
            # 池子连每节点的下限都给不起：尽力均分并大声说出来。
            log.error(
                "总带宽减去冻结记账后的可分池太小，连每节点 %gMbps 的"
                "下限都给不起，按均分尽力而为 pool=%.1fMbps nodes=%d"
                "——请调大 total_egress_mbps 或处理冻结节点",
                params.min_quota_mbps, pool, n)
            for s in ctrl:
                desired[s.name] = pool / n
        elif sum(s.demand_ewma_mbps for s in ctrl) < n * params.idle_mbps:
            # 全体空闲：没有需求信号时按需比例无意义，均分让任何节点
            # 来流量都立刻有承接额度；写抑制保证空闲期分配值不抖。
            for s in ctrl:
                desired[s.name] = pool / n
        else:
            guaranteed = params.min_share_ratio * pool / n
            remaining = pool - guaranteed * n
            total_d = sum(max(s.demand_ewma_mbps, 1e-9) for s in ctrl)
            for s in ctrl:
                desired[s.name] = guaranteed + remaining * max(
                    s.demand_ewma_mbps, 1e-9) / total_d
            # 下限钳制 + 守恒修正：抬到下限的超额从份额最大者身上扣回。
            deficit = 0.0
            for s in ctrl:
                if desired[s.name] < params.min_quota_mbps:
                    deficit += params.min_quota_mbps - desired[s.name]
                    desired[s.name] = params.min_quota_mbps
            if deficit > 1e-9:
                for s in sorted(ctrl, key=lambda x: -desired[x.name]):
                    room = desired[s.name] - params.min_quota_mbps
                    take = min(room, deficit)
                    desired[s.name] -= take
                    deficit -= take
                    if deficit <= 1e-9:
                        break

        # 取整到 0.1Mbps（减少琐碎写），落到各自的 desired。
        for s in ctrl:
            s.desired_mbps = max(params.min_quota_mbps,
                                 round(desired[s.name], 1))

        # 写判定。
        writes = []
        for s in ctrl:
            base = s.quota_mbps if s.quota_mbps is not None else 0.0
            if (s.applied_mbps is not None
                    and abs(base - s.applied_mbps) > params.min_delta_mbps):
                log.warning(
                    "节点自报的实例配额与总控最近一次下发值不符（下发未"
                    "生效，或节点侧被手工改动）——总控是总量控制的权威，"
                    "将按本轮分配覆盖 node=%s 自报=%.1fMbps 下发过="
                    "%.1fMbps 本轮分配=%.1fMbps",
                    s.name, base, s.applied_mbps, s.desired_mbps)
            if abs(s.desired_mbps - base) < max(
                    base * params.apply_epsilon, params.min_delta_mbps):
                continue                      # 写抑制：变化太小不值得
            writes.append((s, s.desired_mbps))
            log.info(
                "分配调整 node=%s %.1f→%.1fMbps（需求=%.1fMbps 池="
                "%.1fMbps 可控节点=%d 冻结记账=%.1fMbps）",
                s.name, base, s.desired_mbps,
                s.demand_ewma_mbps or 0.0, pool, n, reserved)
        return writes

    def _mark_release(self, states: list) -> None:
        """总量控制刚被取消：在册节点全部标记待解除。"""
        later = []
        for s in states:
            # 无令牌的节点从没被总控控制过（也写不了），无需解除。
            s.release_pending = bool(s.token)
            if s.release_pending and not s.online:
                later.append(s.name)
        self._log.warning(
            "总量控制已取消：解除各节点的实例总限速（恢复不限），之后只"
            "汇总不下发 nodes=%d%s", sum(s.release_pending for s in states),
            f"；离线节点恢复在线后再解除：{'、'.join(later)}" if later
            else "")

    def _release(self, states: list) -> list:
        """total ≤ 0 时的写清单：待解除且可写的节点逐台 DELETE。"""
        writes = []
        for s in states:
            s.desired_mbps = None
            if not s.release_pending or not s.online:
                continue                  # 离线：保持待解除，恢复后再解
            if s.quota_mbps == 0:
                s.release_pending = False  # 自报已是不限（如被手工解除）
                continue
            writes.append((s, None))
            self._log.info("解除实例总限速 node=%s %.1fMbps→不限",
                           s.name, s.quota_mbps or 0.0)
        return writes
