"""入向动态限速控制器——双网卡形态的闭环。

形态（本分支的推荐部署）：**出向不做 tc 限速**（RL_TC_IFACE=-），限速
全靠入向 police；本控制器根据**实测出向带宽**（响应方向，HAProxy 口径
的 mean10）对照出向目标（quotas 登记值），动态调整各 frontend 入向
police 的带宽上限：

    请求进得少 → 响应自然出得少 → 出向带宽被"间接"压在目标内。

为什么不直接在出向上 HTB：HTB 把超速流量排队，响应延迟全体抬高；入向
police 只掐请求流（体量远小于响应流），响应路径零排队——对客户端更
温和（没被掐的请求全速完成）。代价是控制是间接的、有滞后，所以算法
必须平缓，宁可收敛慢也不要振荡。

算法（刻意平缓——对照动态 maxconn 分支的 AIMD 乘性 0.7 急砍，这里
两个方向都是小步）：

  - 观测：该 frontend 出向带宽的 10s 均值（mean10，与超限告警同口径，
    毛刺不触发调整）；采样失联（degraded）的轮次不动作——陈旧数据
    不配驱动调整。
  - 死区：mean10/目标 ∈ [low_ratio, high_ratio] → 不动。上下都留滞回
    带，流量在目标附近小幅波动时入向上限保持稳定。
  - 超了（> high_ratio）：下调。降幅与超出比例成正比（超得越多降得
    越快）但**封顶 step_down_max**（默认单次最多 -10%），下限
    floor_ratio×上限（默认 10%——再低新请求基本进不来，等于误伤）。
  - 低了（< low_ratio）：上调固定小步 step_up×上限（默认 +5%），
    封顶回到登记的上限（request_limits.mbps）。流量退潮后入向上限
    会逐步爬回全开。
  - 变化小于 apply_epsilon（默认 2%）不下发：入向规则是整组重建的，
    琐碎变化只会白白清零 tc 统计。

接线（见 __main__）：
  - record() 挂在监控循环的 sampler 链上，每拍收下各 frontend 的
    mean10；
  - run_governor() 每 interval_s 跑一轮 adjust_once()，有变化就 set
    下发事件叫醒 run_ingress 立刻 reconcile；
  - run_ingress 的 desired_fn 经 effective_limits() 取**动态后的**
    request_limits——静态登记值只是上限，不再直接下发。

目标从哪来：quotas（出向限额登记）。本分支里它不再驱动出向 HTB，而是
作为出向带宽的**目标值**供本控制器用——同一个数字，换了执行机制。
frontend 没登记 quotas 目标、或 request_limits 没登记 mbps 的，不参与
动态调节（后者无物可调，前者保持静态值下发）。
"""
from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass

from . import model


@dataclass
class IngressGovConfig:
    """`dynamic_ingress` 配置段（启动时定型，运行期改动不热生效）。"""

    enabled: bool = False
    # 调节周期。比采样（1s）长一个量级：mean10 本身要 10s 才能反映一次
    # 调整的效果，更快地调只会追着噪声跑。
    interval_s: float = 15.0
    # 死区下/上边界（相对出向目标的比例）。
    low_ratio: float = 0.85
    high_ratio: float = 1.0
    # 上调步长（相对登记上限 max_mbps 的比例，加性）。
    step_up: float = 0.05
    # 单次下调的最大降幅（相对当前值的比例，乘性封顶）。
    step_down_max: float = 0.10
    # 动态值的下限（相对登记上限的比例）。
    floor_ratio: float = 0.10
    # 低于该比例的变化不下发（重建 tc 规则有成本且清零统计）。
    apply_epsilon: float = 0.02


class IngressGovernor:
    """按出向带宽平缓调节入向 police 上限的控制器。

    单协程访问（record 由监控循环调、adjust_once 由 run_governor 调，
    都在同一个事件循环里），无需加锁。
    """

    def __init__(self, cfg: IngressGovConfig,
                 log: logging.Logger | None = None):
        self._cfg = cfg
        self._log = log if log is not None else logging.getLogger(
            "rl_limiter.ingressgov")
        # frontend 名 → 当前动态入向上限（Mbps）。**不在这里 = 静态**
        # （按登记值 max_mbps 下发）；只有调节真的动过才记录。
        self._cur: dict[str, float] = {}
        # frontend 名 → 最近一拍的出向 mean10（bytes/s）。
        self._mean: dict[str, float] = {}

    # ---- 监控循环的 sampler 链挂点（每拍一次） ----

    def record(self, now: float, usages: list, instance=None) -> None:
        for u in usages:
            self._mean[u.name] = u.mean10_bps

    # ---- 下发侧取值 ----

    def current(self) -> dict[str, float]:
        """当前动态值（frontend 名 → Mbps），只含被调节过的。"""
        return dict(self._cur)

    def effective_limits(self, limits: dict) -> dict:
        """静态登记 + 动态当前值 → 实际要下发的 request_limits。

        conn_rate 永远用静态登记值：新建连接速率是防滥用阈值，不参与
        带宽闭环。
        """
        out = {}
        for name, rl in limits.items():
            cur = self._cur.get(name)
            if cur is None or rl.mbps <= 0:
                out[name] = rl
            else:
                out[name] = model.RequestLimit(mbps=cur,
                                               conn_rate=rl.conn_rate)
        return out

    # ---- 调节 ----

    def adjust_once(self, frontends: list, limits: dict,
                    degraded: bool = False) -> bool:
        """跑一轮调节，返回是否有值发生变化（变了调用方去触发下发）。

        frontends/limits 每轮现取（配置热更后自动跟上）。
        """
        if degraded:
            # 采样失联：mean10 是陈旧值，按它调只会调错。保持现状。
            return False
        targets = {f.name: f.quota_mbps for f in frontends if f.limited}
        changed = False

        # 清理：登记被删 / mbps 清零 / 出向目标被删 → 回到静态语义。
        for name in list(self._cur):
            rl = limits.get(name)
            if rl is None or rl.mbps <= 0 or name not in targets:
                del self._cur[name]
                changed = True
                self._log.info(
                    "入向动态限速退出 fe=%s（登记或出向目标已变更，回到"
                    "静态值下发）", name)

        for name, rl in limits.items():
            if rl.mbps <= 0:
                continue                      # 没登记入向带宽，无物可调
            target = targets.get(name)
            if not target or target <= 0:
                continue                      # 没有出向目标，保持静态
            max_mbps = rl.mbps
            floor = max_mbps * self._cfg.floor_ratio
            cur = self._cur.get(name, max_mbps)
            mean_mbps = self._mean.get(name, 0.0) * 8 / 1e6
            ratio = mean_mbps / target

            if ratio > self._cfg.high_ratio:
                # 平缓下调：降幅 = 超出比例的一半，封顶 step_down_max。
                down = min(self._cfg.step_down_max,
                           (ratio - self._cfg.high_ratio) * 0.5)
                new = max(floor, cur * (1.0 - down))
            elif ratio < self._cfg.low_ratio and cur < max_mbps:
                new = min(max_mbps, cur + max_mbps * self._cfg.step_up)
            else:
                continue                      # 死区（或已到顶）

            if abs(new - cur) < cur * self._cfg.apply_epsilon:
                continue                      # 变化太小，不值得重建规则
            self._cur[name] = new
            changed = True
            self._log.info(
                "入向动态限速调整 fe=%s %.1f→%.1fMbps（出向 mean10="
                "%.1fMbps 目标=%gMbps 上限=%gMbps）",
                name, cur, new, mean_mbps, target, max_mbps)
            if new == max_mbps:
                # 爬回了全开：删掉记录，语义与"从未调节过"一致。
                del self._cur[name]
        return changed


async def run_governor(gov: IngressGovernor,
                       desired_fn,
                       poke: asyncio.Event,
                       log: logging.Logger,
                       interval_s: float,
                       degraded_fn=None) -> None:
    """常驻任务：周期跑调节，有变化就 set 下发事件（run_ingress 被叫醒
    后经 effective_limits 拿到新值立刻 reconcile）。

    desired_fn() 返回 (frontends, request_limits)，与 run_ingress 的
    同名参数语义一致，每轮现取。
    """
    while True:
        await asyncio.sleep(interval_s)
        try:
            frontends, limits = desired_fn()
            degraded = bool(degraded_fn()) if degraded_fn else False
            if gov.adjust_once(frontends, limits, degraded=degraded):
                poke.set()
        except asyncio.CancelledError:
            raise
        except Exception as e:              # 常驻任务不能因单次异常退出
            log.error("入向动态限速调节出现未预期的异常，本轮跳过 err=%s", e)
