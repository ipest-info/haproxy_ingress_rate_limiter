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
  - 低了（< low_ratio）：上调小步 step_up×**当前值**（默认 +5%，
    绝对下限 0.1Mbps 免得贴地时爬不动），封顶回到登记的上限
    （request_limits.mbps）。流量退潮后入向上限会逐步爬回全开。
    上调必须锚定当前值而不是上限：入向因 ACK 杠杆比出向小一两个
    数量级，动态值收敛在上限的几十分之一处，step_up×上限在那个
    刻度上是一步翻倍的巨步——上冲过头→猛降回来→再上冲，稳定的
    极限环（带宽一会高一会低，实测抓到）。两个方向都乘性锚定当前
    值后步长对称，流量稳定时动态值收敛到死区内不动。
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

**实例级环**（instance_quota_mbps = 实例出向总带宽目标）：与单端口环
同一套算法，观测换成全部 frontend 出向 mean10 之和，执行换成 IFB 上的
HTB **聚合类**（层级模式，与出向实例模式同一结构：全部端口挂进总闸，
总闸速率即动态值）。总闸的上限 = 登记的 request_limits.mbps 之和
（一个都没登记时退化为目标值本身——入向远小于出向，等于全开但有界）。
仅 ifb 执行模式支持（police 没有层级可言），见 instance_capable。
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
    # 单次上调步长（相对当前值的比例，乘性；绝对下限 0.1Mbps）。
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
                 log: logging.Logger | None = None,
                 instance_capable: bool = True):
        self._cfg = cfg
        self._log = log if log is not None else logging.getLogger(
            "rl_limiter.ingressgov")
        # frontend 名 → 当前动态入向上限（Mbps）。**不在这里 = 静态**
        # （按登记值 max_mbps 下发）；只有调节真的动过才记录。
        self._cur: dict[str, float] = {}
        # frontend 名 → 最近一拍的出向 mean10（bytes/s）。
        self._mean: dict[str, float] = {}
        # 实例级环：执行侧是否支持聚合总闸（仅 ifb 模式支持——police
        # 没有层级），以及当前动态总闸值（Mbps；None = 全开=上限）。
        self._instance_capable = instance_capable
        self._cur_inst: "float | None" = None
        # 落位等待标记：首次收紧时实测暂缺（差分基准刚建）就等一轮，
        # 第二轮仍没有才回退"从上限爬降"——落位值得等 5~15s，但不能
        # 让一个坏掉的测量源把实例环永远卡死。
        self._land_waited = False
        # 落位锚：落位后实例环的下限改锚定在落位值上（floor_ratio×落位
        # 值，绝对下限 0.1Mbps）。没登记 request_limits 时 max_inst 退化
        # 为出向目标值，floor_ratio×它是**出向刻度**的下限——比入向的
        # 均衡点高一个量级，总闸会贴着它卡死、出向永远压不到目标
        # （e2e 实测抓到）。下限必须跟流入刻度走。
        self._inst_anchor: "float | None" = None
        # 全部 frontend 出向 mean10 之和（bytes/s），实例级环的观测值。
        self._mean_total = 0.0
        # 最近一轮调节时的实测流入带宽（Mbps；None = 还没有实测）。
        # 只为调节视图展示留的缓存：IFB 的速率测量是差分计数器，只能
        # 有一个读者（run_governor），页面要看的话读这里的缓存。
        self._last_ingress_mbps: "float | None" = None

    # ---- 监控循环的 sampler 链挂点（每拍一次） ----

    def record(self, now: float, usages: list, instance=None) -> None:
        for u in usages:
            self._mean[u.name] = u.mean10_bps
        self._mean_total = sum(u.mean10_bps for u in usages)

    # ---- 下发侧取值 ----

    def current(self) -> dict[str, float]:
        """当前动态值（frontend 名 → Mbps），只含被调节过的。"""
        return dict(self._cur)

    def current_instance(self) -> "float | None":
        """入向总闸的当前动态值（Mbps；None = 全开，即等于上限）。"""
        return self._cur_inst

    def _instance_max(self, limits: dict, inst_target: float) -> float:
        """总闸的上限：登记的入向上限之和；一个都没登记时退化为目标值
        本身（下载型业务入向远小于出向，= 实际上全开但有界）。"""
        s = sum(rl.mbps for rl in limits.values() if rl.mbps > 0)
        return s if s > 0 else float(inst_target)

    def effective_instance_bits(self, limits: dict,
                                inst_target: float) -> int:
        """下发侧取总闸速率（bit/s；0 = 不启用层级/没有实例目标）。"""
        if not self._instance_capable or not inst_target or inst_target <= 0:
            return 0
        cur = self._cur_inst if self._cur_inst is not None \
            else self._instance_max(limits, inst_target)
        return int(cur * 1e6)

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

    def regulation_view(self, frontends: list, limits: dict,
                        inst_target: float = 0.0) -> "dict | None":
        """调节视图的数据源（控制台快照的 regulation 字段，单位 Mbps）。

        把闭环的全部调节量摆出来：实例级环的目标/动态总闸/上限/观测/
        实测流入，以及每个受调端口的目标/动态上限/登记上限/观测。没有
        任何环在工作（没实例目标也没受管端口）返回 None，页面的调节
        视图卡保持隐藏。
        """
        targets = {f.name: f.quota_mbps for f in frontends if f.limited}
        fes = {}
        for name, rl in limits.items():
            if rl.mbps <= 0:
                continue
            t = targets.get(name)
            if not t or t <= 0:
                continue
            fes[name] = {
                "target_mbps": t,
                "max_mbps": rl.mbps,
                # None = 没被调节过（静态值下发，等于全开到登记上限）。
                "cap_mbps": self._cur.get(name),
                "egress_mbps": round(
                    self._mean.get(name, 0.0) * 8 / 1e6, 3),
            }
        inst = None
        if self._instance_capable and inst_target and inst_target > 0:
            inst = {
                "target_mbps": inst_target,
                # None = 全开（生效值即 gate_max_mbps）。
                "gate_mbps": self._cur_inst,
                "gate_max_mbps": self._instance_max(limits, inst_target),
                "ingress_mbps": self._last_ingress_mbps,
                "egress_mbps": round(self._mean_total * 8 / 1e6, 3),
                "landed": self._inst_anchor is not None,
            }
        if inst is None and not fes:
            return None
        return {"instance": inst, "frontends": fes}

    # ---- 调节 ----

    def adjust_once(self, frontends: list, limits: dict,
                    inst_target: float = 0.0,
                    ingress_now_mbps: "float | None" = None,
                    degraded: bool = False) -> bool:
        """跑一轮调节，返回是否有值发生变化（变了调用方去触发下发）。

        frontends/limits/inst_target（实例出向总带宽目标，Mbps，0 =
        未设）每轮现取（配置热更后自动跟上）。ingress_now_mbps 是执行
        面实测的当前入向线上速率（IFB 根 qdisc 差分；None = 没有实测）
        ——总闸首次收紧时的落位参考。
        """
        if ingress_now_mbps is not None:
            self._last_ingress_mbps = ingress_now_mbps
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
                # 乘性上调锚当前值（与下调对称），绝对步长下限 0.1Mbps
                # ——纯乘性在贴地的动态值上永远迈不过 apply_epsilon。
                new = min(max_mbps,
                          cur + max(0.1, cur * self._cfg.step_up))
            else:
                continue                      # 死区（或已到顶）

            # epsilon 不拦"封顶回上限"的最后一步：那一步的语义是退出
            # 动态、回到静态全开，不放行的话乘性上调的末步永远小于
            # epsilon，动态值会永远卡在上限脚下差一口气。
            if new < max_mbps and abs(new - cur) < cur * self._cfg.apply_epsilon:
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

        # ---- 实例级环：出向总带宽 对照 实例出向目标 ----
        if (self._instance_capable and inst_target and inst_target > 0):
            max_inst = self._instance_max(limits, inst_target)
            floor = max_inst * self._cfg.floor_ratio
            cur = self._cur_inst if self._cur_inst is not None else max_inst
            mean_mbps = self._mean_total * 8 / 1e6
            ratio = mean_mbps / inst_target
            new = cur
            if ratio > self._cfg.high_ratio:
                land = (min(max_inst, max(0.1, ingress_now_mbps * 1.1))
                        if ingress_now_mbps else None)
                if (self._cur_inst is None and land is None
                        and not self._land_waited):
                    # 首次收紧但还没有流入带宽实测（差分基准刚建立）：
                    # 等一轮。直接回退从上限爬降的话，落位就永远赶不上
                    # 第一步（真实用户场景抓到的缺陷）。
                    self._land_waited = True
                    self._log.info(
                        "入向总闸需要收紧，但流入带宽实测尚未就绪"
                        "（差分基准刚建立），等下一轮落位")
                elif self._cur_inst is None and land is not None and land < cur:
                    # 首次收紧：**落位到实测入向带宽附近**（×1.1 留一
                    # 口气），而不是从上限/出向目标开始一步步爬下来——
                    # 入向因 ACK 杠杆比出向小一两个数量级，从上限起步
                    # 前几十步全是空转。落位本身不掐流量（闸 ≈ 当前
                    # 流量 + 余量），真正的收紧仍由后续小步完成，柔和
                    # 性质不变。没有实测（None）回退旧行为。
                    new = land
                    self._inst_anchor = land
                    self._log.info(
                        "入向总闸落位：按当前实测入向带宽起调 "
                        "%.1fMbps×1.1→%.1fMbps（出向总 mean10=%.1fMbps "
                        "实例目标=%gMbps；不再从上限 %gMbps 爬降）",
                        ingress_now_mbps, new, mean_mbps, inst_target,
                        max_inst)
                else:
                    down = min(self._cfg.step_down_max,
                               (ratio - self._cfg.high_ratio) * 0.5)
                    # 下限：落位过就锚定在落位值的刻度上（见 _inst_anchor
                    # 注释），没落位过才用 floor_ratio×上限的旧语义。
                    floor_eff = (max(0.1, self._inst_anchor
                                     * self._cfg.floor_ratio)
                                 if self._inst_anchor is not None else floor)
                    new = max(floor_eff, cur * (1.0 - down))
            elif ratio < self._cfg.low_ratio and cur < max_inst:
                # 同单端口环：乘性上调锚当前值。没登记 request_limits 时
                # max_inst 退化为出向目标，总闸收敛在它的几十分之一
                # （ACK 杠杆），step_up×max_inst 在那个刻度上是一步翻倍
                # 的巨步——上冲→出向猛涨→猛降，极限环（实测抓到：出向
                # 在目标上下来回摆）。
                new = min(max_inst,
                          cur + max(0.1, cur * self._cfg.step_up))
            # 同单端口环：封顶回上限（=退出动态、全开）的最后一步不受
            # epsilon 拦截。
            if new != cur and (new >= max_inst or
                               abs(new - cur) >= cur * self._cfg.apply_epsilon):
                self._cur_inst = None if new >= max_inst else new
                changed = True
                self._log.info(
                    "入向总闸动态调整 %.1f→%.1fMbps（出向总 mean10="
                    "%.1fMbps 实例目标=%gMbps 上限=%gMbps）",
                    cur, new, mean_mbps, inst_target, max_inst)
        elif self._cur_inst is not None:
            # 实例目标被删/执行侧不支持 → 回到全开语义并触发下发。
            self._cur_inst = None
            self._land_waited = False
            self._inst_anchor = None
            changed = True
            self._log.info("入向总闸退出动态调节（实例出向目标已移除）")
        return changed


async def run_governor(gov: IngressGovernor,
                       desired_fn,
                       poke: asyncio.Event,
                       log: logging.Logger,
                       interval_s: float,
                       degraded_fn=None,
                       ingress_rate_fn=None) -> None:
    """常驻任务：周期跑调节，有变化就 set 下发事件（run_ingress 被叫醒
    后经 effective_limits 拿到新值立刻 reconcile）。

    desired_fn() 返回 (frontends, request_limits, 实例出向目标 Mbps)，
    每轮现取。ingress_rate_fn 是执行面的实测入向速率（async，bit/s 或
    None）——总闸落位用，见 adjust_once。
    """
    while True:
        await asyncio.sleep(interval_s)
        try:
            frontends, limits, inst_target = desired_fn()
            degraded = bool(degraded_fn()) if degraded_fn else False
            now_mbps = None
            if ingress_rate_fn is not None:
                bits = await ingress_rate_fn()
                if bits:
                    now_mbps = bits / 1e6
            if inst_target:
                # 闭环每轮的观测快照：落位靠它，排障时第一眼要看的数。
                log.debug("闭环观测 流入实测=%s 出向总目标=%gMbps",
                         f"{now_mbps:.2f}Mbps" if now_mbps else "无",
                         inst_target)
            if gov.adjust_once(frontends, limits, inst_target=inst_target,
                               ingress_now_mbps=now_mbps,
                               degraded=degraded):
                poke.set()
        except asyncio.CancelledError:
            raise
        except Exception as e:              # 常驻任务不能因单次异常退出
            log.error("入向动态限速调节出现未预期的异常，本轮跳过 err=%s", e)
