# rl_limiter.collector —— 采样层：每秒从本机 HAProxy 取一次 `show stat`，
# 差分出各受管 frontend 的下行速率，产出带 10s 均值与 60s EWMA 的用量样本。
#
# 三条采样链路（每拍各取一次，彼此独立容错）：
#   1. `show stat`  —— 各 frontend 的字节/连接/拒绝计数。**主链路**：限速
#      与超限告警只依赖它，它失败即整拍 fail-static；
#   2. `show info`  —— 整台 HAProxy 的并发/累计连接数、空闲率。只喂实例
#      视图，失败时沿用上一拍的值，绝不影响限速判定；
#   3. /proc/net/dev —— 网卡包计数（HAProxy 根本不统计数据包，见 netdev
#      模块）。同样只喂实例视图，失败即静默沿用。
# 分开容错的理由很实际：监控视图是"看"的，限速是"管"的，不能因为多画了
# 几条曲线就让限速的判定链路多几种失败方式。
#
# 单 HAProxy 模型（v0.4 起）：**监控单位 = frontend**。一个 frontend 就是
# 一个监听端口 + 一个 tc 速率类（tc 按源端口分类），与限速机制一一对应，
# 因此不存在跨 frontend 的聚合——每个 frontend 自己算自己的速率与均值。
# （v0.3 及以前是"多节点 + 按节点聚合其全部 frontend"，见 tag
# v0.3.0-colocated；改为同机单实例后那层聚合失去意义，已移除。）
#
# 采集口径遵循设计文档 §3.1：以 frontend 的 bytes_out（HAProxy 发回客户端
# 的应用层字节数）为准，而非网卡计数——口径与计费一致，且天然按 frontend
# 拆分。在每秒瞬时速率之上维护两条平滑曲线：10 秒滑动窗口均值（承诺口径，
# 超限告警判据输入）与 60 秒 EWMA（趋势观测），瞬时毛刺不会直接触发任何
# 告警。
#
# 三条必须记住的时序规则：
#   1. **首次采样只建基线**：累计计数只有一个值时无法差分，该秒速率未知，
#      不喂窗口（喂 0 会把均值拖低）；
#   2. **计数回绕沿用上一秒**：HAProxy reload 后 bytes_out 从零重来，差分
#      为负不可用，沿用上一秒速率并用新值重建基线；
#   3. **采样失败 fail-static**：沿用上一秒速率继续推进窗口，而不是留空洞
#      或骤降为零——骤降为零会让均值失真、触发假的"恢复正常"。
#
# 速率口径：主循环以固定 1 秒节奏调用 tick，因此"相邻两次累计值之差"本身
# 就是 bytes/s，无需再除以墙钟间隔（config 强制 tick_interval_s == 1.0）。

from __future__ import annotations

import asyncio
import logging

from . import model, netdev
from .haproxy import RuntimeClient
from .window import Ewma, SlidingWindow

# 滑动窗口容量（单位：秒）。10 秒滑动均值是已拍板的承诺口径——"10 秒均值
# ≤ 约定带宽，瞬时容忍至 110%"（设计文档 §3.1/§3.3），因此该值与业务承诺
# 绑定，不是随意可调的平滑参数。
WINDOW10_SIZE = 10
# 按经典 span 公式 α = 2/(N+1) 取 N=60，在 1 秒 tick 节奏下近似 60 秒 EWMA。
# EWMA 相比再开一个 60 格窗口只需 O(1) 状态，趋势观测不需要精确窗口语义。
EWMA60_ALPHA = 2.0 / (60 + 1)
# 触发降级的连续采样失败次数（连续 10s 失败则告警并打 degraded 标记）。
# 达到阈值只是打标记与告警，不清空任何状态——差分基线保留，恢复后差分
# 立即可用。
DEGRADED_FAILURE_THRESHOLD = 10
# 限制"frontend 从采样结果中消失后其计数基线还保留多少个成功 tick"。
# 保留一段时间是为了容忍 HAProxy reload 等短暂消失场景（回来后差分依旧
# 连续）；但不能无限保留，否则被真正下线的 frontend 会造成状态泄漏。60 个
# 成功 tick（约 1 分钟）后基线被淘汰，此后若同名 frontend 再出现则按首次
# 采样重新建基线。
ABSENT_TICK_LIMIT = 60


class _DiffCounter:
    """单调累计计数器的"差分成每秒速率"的最小状态机。

    只服务于**监控视图**字段（上行字节、新建连接数、拒绝数、网卡计数）。
    计费口径的 bytes_out 没有用它——那条路径要额外记 has_rate、要在回绕时
    打日志、要参与滑动窗口，语义比这里复杂得多，硬套一个抽象只会把两边
    都讲不清楚。

    回绕（HAProxy reload 后计数从零重来）的处理与 bytes_out 一致：本次
    沿用上一次的速率，并用新值重建基线。
    """

    __slots__ = ("last", "rate", "primed")

    def __init__(self, value: int = 0, primed: bool = False):
        self.last = value
        self.rate = 0.0
        # 是否已有基线。无基线时首次 push 只建基线，速率保持 0（"未知"在
        # 监控视图里按 0 展示即可，它不参与任何判定）。构造时给了初值的
        # （frontend 首次采样那一拍）直接算已建基线，下一拍就有速率——与
        # bytes_out 主链路的时序保持一致。
        self.primed = primed

    def push(self, value: int) -> float:
        if self.primed and value >= self.last:
            self.rate = float(value - self.last)
        # value < last：回绕，沿用上次速率，下面照常重建基线。
        self.last = value
        self.primed = True
        return self.rate

    def reset_rate(self) -> None:
        """把速率归零（对应"该 frontend 本拍确实不存在"）。"""
        self.rate = 0.0


class _FrontendState:
    """按 frontend 维护的计数差分基线 + 平滑状态。

    基线独立于"是否受管"存在：即使某个 frontend 当前不在受管清单里，其
    基线也照常刷新，将来被纳管时差分从第一秒起就是连续的，不必重走
    "首次采样丢一秒"。
    """

    __slots__ = ("last_bytes_out", "last_rate", "last_conn", "has_rate",
                 "absent_ticks", "window", "ewma",
                 "c_in", "c_conn_new", "c_denied", "last_active", "last_idle",
                 "c_tx_pkts", "c_tx_drops", "c_overlimits", "last_backlog")

    def __init__(self, stat: model.FrontendStat):
        self.last_bytes_out = stat.bytes_out
        self.last_rate = 0.0
        self.last_conn = stat.conn_cur
        # 是否已产出过至少一次基于差分的速率。刚建基线的 frontend 速率是
        # "未知"而非 0，两者对窗口的影响完全不同。
        self.has_rate = False
        self.absent_ticks = 0
        self.window = SlidingWindow(WINDOW10_SIZE)
        self.ewma = Ewma(EWMA60_ALPHA)
        # --- 监控视图字段的差分基线与瞬时值 ---
        self.c_in = _DiffCounter(stat.bytes_in, primed=True)
        self.c_conn_new = _DiffCounter(stat.conn_tot, primed=True)
        self.c_denied = _DiffCounter(stat.denied_total, primed=True)
        self.last_active = stat.active_conns
        self.last_idle = stat.idle_conns
        # tc 队列统计（出方向、链路层口径）。与 HAProxy 的计数各走各的
        # 差分：来源不同、失败方式也不同，混在一起会让排障难做。
        self.c_tx_pkts = _DiffCounter()
        self.c_tx_drops = _DiffCounter()
        self.c_overlimits = _DiffCounter()
        self.last_backlog = 0

    def observe_tc(self, st) -> None:
        """刷新来自 tc 的队列统计（st 是 tcshaper.TcClassStat）。"""
        self.c_tx_pkts.push(st.packets)
        self.c_tx_drops.push(st.drops)
        self.c_overlimits.push(st.overlimits)
        self.last_backlog = st.backlog

    def blank_tc(self) -> None:
        """该 frontend 在 tc 上没有对应的类（还没下发/被删）：归零。"""
        self.c_tx_pkts.reset_rate()
        self.c_tx_drops.reset_rate()
        self.c_overlimits.reset_rate()
        self.last_backlog = 0

    def observe(self, stat: model.FrontendStat) -> None:
        """刷新监控视图字段（与 bytes_out 的主链路各走各的）。"""
        self.c_in.push(stat.bytes_in)
        self.c_conn_new.push(stat.conn_tot)
        self.c_denied.push(stat.denied_total)
        self.last_active = stat.active_conns
        self.last_idle = stat.idle_conns

    def blank(self) -> None:
        """该 frontend 本拍在 stats 里不存在：监控视图字段全部归零。

        与 fail-static 的区别很关键——"采不到"要沿用旧值，"确实没有"是
        真实的零。这个方法只在采样**成功**的拍上被调用。
        """
        self.c_in.reset_rate()
        self.c_conn_new.reset_rate()
        self.c_denied.reset_rate()
        self.last_active = 0
        self.last_idle = 0


class _InstanceState:
    """实例视图的差分基线（show info + 全体 frontend 汇总 + 网卡）。"""

    __slots__ = ("c_conn_new", "c_denied", "c_in", "c_out",
                 "c_pkt_in", "c_pkt_out", "c_drop_in", "c_drop_out",
                 "c_nic_in", "c_nic_out")

    def __init__(self):
        self.c_conn_new = _DiffCounter()   # Σ frontend conn_tot（不是 CumConns，
                                           # 理由见 _tick_instance 的注释）
        self.c_denied = _DiffCounter()     # Σ frontend denied_total
        self.c_in = _DiffCounter()         # Σ frontend bytes_in
        self.c_out = _DiffCounter()        # Σ frontend bytes_out
        self.c_pkt_in = _DiffCounter()     # 网卡 rx_packets
        self.c_pkt_out = _DiffCounter()
        self.c_drop_in = _DiffCounter()    # 网卡 rx_dropped
        self.c_drop_out = _DiffCounter()
        self.c_nic_in = _DiffCounter()     # 网卡 rx_bytes（与 HAProxy 口径对照）
        self.c_nic_out = _DiffCounter()


class Collector:
    """从本机 HAProxy 采样并产出各受管 frontend 的用量样本。"""

    def __init__(self, client: RuntimeClient,
                 log: logging.Logger | None = None,
                 nic: str = "",
                 tc_stats=None):
        self._client = client
        self._log = log if log is not None else logging.getLogger("rl_limiter.collector")
        # 受管 frontend 名集合，由配置层下发；决定哪些 frontend 产出用量。
        self._managed: set[str] = set()
        # frontend 名 → 差分基线与平滑状态。
        self._states: dict[str, _FrontendState] = {}
        # 连续采样失败次数与降级标记（单 HAProxy，故是全局状态）。
        self._failures = 0
        self._degraded = False
        # 只提示一次的日志去重集合。
        self._logged_unmanaged: set[str] = set()
        # --- 实例视图（监控用，不参与限速判定）---
        # 要采样的网卡；空串 = 不采（探测失败或运维禁用），包统计整体缺席。
        self._nic = nic
        # 可选的异步回调，返回 {frontend 名: tcshaper.TcClassStat}。
        # None = 未启用 tc 限速（纯监控形态），相关曲线为空。
        self._tc_stats = tc_stats
        self._inst_state = _InstanceState()
        # 最近一拍的实例用量。采不到时原样留着上一拍的值（fail-static），
        # 因此这里从构造起就必须是一个可用对象而不是 None。
        self._instance = model.InstanceUsage(nic=nic)
        # show info / 网卡这两条副链路的失败只记一次日志，避免每秒刷屏。
        self._logged_info_error = ""
        self._logged_nic_error = ""
        self._logged_tc_error = ""

    def set_managed(self, names: set[str]) -> None:
        """整体替换受管 frontend 集合（配置下发时调用）。"""
        self._managed = set(names)
        self._log.info(
            "已更新受管 frontend 集合，后续采样按新集合产出用量 frontends=%s",
            ",".join(sorted(self._managed)) or "（空）")

    @property
    def degraded(self) -> bool:
        """采样通道是否处于降级（连续失败达阈值）。"""
        return self._degraded

    @property
    def instance(self) -> model.InstanceUsage:
        """最近一拍的整机用量视图（实例监控视图的数据源）。

        每次 tick 后被整体替换；采不到时沿用上一拍并打上 degraded。
        """
        return self._instance

    async def tick(self, now: float) -> list[model.FrontendUsage]:
        """采样一次，返回按名字排序的各受管 frontend 用量。

        now 由调用方注入以便测试确定性；速率计算依赖固定 1s 节奏而非墙钟
        差值，now 仅为将来扩展保留。

        实例视图（instance 属性）在同一拍内一并刷新，但走独立的容错路径：
        它失败不影响这里的返回值，因此限速判定的可靠性不会被"多画几条
        曲线"拖累。

        **两条 runtime 命令必须并发发，不能串行**。runtime socket 一次连接
        只服务一条命令，两条命令本就是两条独立连接，串行的唯一效果是把最坏
        延迟翻倍：单条超时 0.5s，串行下最坏 1.0s，正好吃满一个 1 秒的 tick。
        而主循环发现本拍耗时超过一个周期时会重新对齐到"当前时刻 + 周期"
        （见 loop.run），实际节拍就变成 2 秒——差分代码却按"1 拍 = 1 秒"
        算速率，于是**所有速率读数翻倍**，连超限告警的判据一起失真。
        并发发之后最坏延迟回到 0.5s，节拍不会被挤掉。
        """
        _ = now
        stat_res, info_res = await asyncio.gather(
            self._client.show_stat(), self._client.show_info(),
            return_exceptions=True)
        # gather(return_exceptions=True) 会把子任务自己抛出的 CancelledError
        # 也当成结果收走。停机时主循环靠取消任务退出，吞掉它会让服务停不下来，
        # 因此原样重抛。（外层被取消时 gather 自身就会抛，不走这里。）
        for r in (stat_res, info_res):
            if isinstance(r, asyncio.CancelledError):
                raise r

        if isinstance(stat_res, BaseException):
            usages = self._on_failure(stat_res)
            stats = None
        else:
            stats = stat_res
            usages = self._on_success(stats)
        await self._tick_tc()
        self._tick_instance(stats, info_res)
        return usages

    # ------------------------------------------------------------------
    # 采样成功 / 失败两条路径
    # ------------------------------------------------------------------

    def _on_success(self, stats: list[model.FrontendStat]) -> list[model.FrontendUsage]:
        # 任何一次成功采样都会清零失败计数并解除降级——降级只反映"当下是否
        # 连续采不到数据"，不做粘滞。
        if self._failures > 0:
            was_degraded = self._degraded
            prev = self._failures
            self._failures = 0
            self._degraded = False
            if was_degraded:
                self._log.info(
                    "采样恢复正常，解除降级状态并清零连续失败计数，"
                    "差分基线在失联期间未动、速率立即恢复连续 "
                    "previous_consecutive_failures=%d degraded_threshold=%d",
                    prev, DEGRADED_FAILURE_THRESHOLD)
            else:
                self._log.debug(
                    "采样在达到降级阈值前恢复正常，清零连续失败计数，未触发降级 "
                    "previous_consecutive_failures=%d", prev)

        present: set[str] = set()
        # None = 本 tick 速率未知（仅建基线），与 0.0 语义不同。
        rates: dict[str, float | None] = {}
        conns: dict[str, int] = {}

        for fs in stats:
            present.add(fs.name)
            managed = fs.name in self._managed
            st = self._states.get(fs.name)

            if st is None:
                if not managed:
                    # 未纳管且从未见过：不建基线（省状态），首次出现记一条
                    # debug，避免每秒刷同样的日志。
                    if fs.name not in self._logged_unmanaged:
                        self._logged_unmanaged.add(fs.name)
                        self._log.debug(
                            "发现未纳管的 frontend，不建差分基线并忽略其流量"
                            "（仅首次提示） frontend=%s bytes_out=%d conn_cur=%d",
                            fs.name, fs.bytes_out, fs.conn_cur)
                    continue
                # 首次采样：只有一个累计值、没有前值可差分，速率未知，本
                # tick 仅记录基线。连接数是瞬时值不依赖差分，可直接计入。
                self._states[fs.name] = _FrontendState(fs)
                rates[fs.name] = None
                conns[fs.name] = fs.conn_cur
                self._log.info(
                    "首次采样到新 frontend，本秒仅建立计数差分基线（速率未知不喂窗口），"
                    "下一秒起正常产出速率 frontend=%s bytes_out=%d conn_cur=%d",
                    fs.name, fs.bytes_out, fs.conn_cur)
                continue

            st.absent_ticks = 0
            if fs.bytes_out >= st.last_bytes_out:
                rate = float(fs.bytes_out - st.last_bytes_out)
            else:
                # 计数回绕（典型场景：HAProxy reload 后计数从零重来）：差分
                # 为负不可用，沿用上一秒速率顶过这一秒，同时用新累计值重建
                # 基线，下一秒差分即恢复正常。
                rate = st.last_rate
                self._log.info(
                    "检测到 bytes_out 计数器回绕（多为 HAProxy reload 后计数清零），"
                    "本秒沿用上一秒速率并用新累计值重建差分基线，下一秒差分即恢复正常 "
                    "frontend=%s previous_bytes_out=%d current_bytes_out=%d "
                    "held_rate_bps=%.1f",
                    fs.name, st.last_bytes_out, fs.bytes_out, st.last_rate)
            st.last_bytes_out = fs.bytes_out
            st.last_rate = rate
            st.last_conn = fs.conn_cur
            st.has_rate = True
            # 监控视图字段：与计费口径的 bytes_out 各走各的差分（见
            # _DiffCounter 的说明）。未纳管的 frontend 也照常刷新，理由
            # 与基线相同——将来纳管时第一秒就有正确数值。
            st.observe(fs)
            # 已知但当前未纳管的 frontend 也在上面刷新了基线：将来被纳管
            # 时差分从第一秒起就连续，不必重走"首采样丢一秒"。
            if managed:
                rates[fs.name] = rate
                conns[fs.name] = fs.conn_cur

        # 受管但本 tick 完全没出现在 stats 里的 frontend：那是真实的零
        # ——frontend 不存在就没有流量，零值必须进窗口，否则 mean10 会
        # 停在旧值上虚高（典型场景：配置里加了 frontend 但 cfg 还没下发）。
        for name in self._managed:
            if name not in rates:
                rates[name] = 0.0
                conns.setdefault(name, 0)
                # 监控视图字段同样归零：这是"确实没有"而不是"采不到"。
                st = self._states.get(name)
                if st is not None:
                    st.blank()

        self._evict_absent(present)
        self._reset_unmanaged_smoothing()
        return self._build(rates, conns, degraded=False)

    def _on_failure(self, err: BaseException) -> list[model.FrontendUsage]:
        """采样失败：全部受管 frontend 沿用上一秒速率继续参与窗口推进。

        （设计 §3.7 fail-static）速率骤降为零会让 10s 均值失真，进而触发
        假的"已恢复正常"——宁可用陈旧值顶着，同时把 degraded 标出来让
        超限判定暂停、控制台标红。
        """
        self._failures += 1
        crossed = self._failures == DEGRADED_FAILURE_THRESHOLD
        if self._failures >= DEGRADED_FAILURE_THRESHOLD:
            self._degraded = True

        rates: dict[str, float | None] = {}
        conns: dict[str, int] = {}
        held = 0
        for name in self._managed:
            st = self._states.get(name)
            if st is None:
                continue
            # 失联前就已缺席的 frontend 不参与沿用：最后一次成功采样里它
            # 本来就没有贡献，沿用陈旧速率反而会凭空抬高数值。
            if st.absent_ticks > 0:
                continue
            if st.has_rate:
                rates[name] = st.last_rate
                conns[name] = st.last_conn
                held += 1
            else:
                # 刚建基线就失联：速率仍是未知而非零，按 baseline-only 处理。
                rates[name] = None
                conns[name] = st.last_conn

        self._log.warning(
            "采样失败，各受管 frontend 本秒沿用上一秒速率与连接数继续推进窗口"
            "（fail-static，避免速率骤降为零让均值/告警判定失真） "
            "err=%s consecutive_failures=%d degraded_threshold=%d degraded=%s "
            "held_frontends=%d",
            err, self._failures, DEGRADED_FAILURE_THRESHOLD, self._degraded, held)
        if crossed:
            self._log.error(
                "连续采样失败达到降级阈值，进入降级状态，带宽视图停留在陈旧"
                "数据、超限判定暂停，直至采样恢复 threshold=%d "
                "consecutive_failures=%d",
                DEGRADED_FAILURE_THRESHOLD, self._failures)
        return self._build(rates, conns, degraded=self._degraded)

    # ------------------------------------------------------------------
    # 状态维护与输出组装
    # ------------------------------------------------------------------

    def _evict_absent(self, present: set[str]) -> None:
        """淘汰长期缺席的 frontend 基线，防止已下线的 frontend 泄漏状态。

        只在采样**成功**的 tick 累加缺席计数——失联时 frontend 不算缺席
        （它只是采不到，不是没了）。
        """
        for name in list(self._states):
            if name in present:
                continue
            st = self._states[name]
            st.absent_ticks += 1
            if st.absent_ticks >= ABSENT_TICK_LIMIT:
                del self._states[name]
                self._log.info(
                    "frontend 已连续多个采样周期未出现，超过保留上限，淘汰其计数"
                    "差分基线；此后同名 frontend 再出现将按首次采样重建基线 "
                    "frontend=%s absent_ticks=%d absent_tick_limit=%d",
                    name, st.absent_ticks, ABSENT_TICK_LIMIT)

    def _reset_unmanaged_smoothing(self) -> None:
        """清掉已移出受管清单的 frontend 的平滑状态。

        陈旧的窗口/EWMA 若保留，该 frontend 将来重新纳管时会带着过期历史
        起步（比如刚加回来就直接报超限）。差分基线本身保留——它无害且能让
        重新纳管时的第一秒就有正确速率。
        """
        for name, st in self._states.items():
            if name in self._managed:
                continue
            if st.window.count or st.ewma.value:
                st.window = SlidingWindow(WINDOW10_SIZE)
                st.ewma = Ewma(EWMA60_ALPHA)
                self._log.debug(
                    "frontend 已移出受管清单，丢弃其滑动窗口与 EWMA 状态，"
                    "避免将来重新纳管时携带过期历史 frontend=%s", name)

    def _build(self, rates: dict[str, float | None], conns: dict[str, int],
               degraded: bool) -> list[model.FrontendUsage]:
        """把本 tick 的速率喂进窗口并组装输出（按名字排序，便于测试与阅读）。"""
        usages: list[model.FrontendUsage] = []
        for name in sorted(self._managed):
            st = self._states.get(name)
            if st is None:
                # 受管但还没有任何状态（cfg 尚未下发、frontend 还不存在）：
                # 输出零值占位，让下游看到稳定的单元集合。
                usages.append(model.FrontendUsage(name=name, degraded=degraded))
                continue
            rate = rates.get(name)
            if rate is not None:
                # 仅建基线的 tick 速率未知，跳过窗口/EWMA（见文件头规则 1）。
                st.window.push(rate)
                st.ewma.update(rate)
            usages.append(model.FrontendUsage(
                name=name,
                rate_bps=rate or 0.0,
                mean10_bps=st.window.mean(),
                ewma60_bps=st.ewma.value,
                conn_cur=conns.get(name, 0),
                # 监控视图字段直接取自状态：采样失败时它们没被刷新，
                # 于是天然沿用上一拍的值（fail-static），与主链路一致。
                rate_in_bps=st.c_in.rate,
                conn_new_ps=st.c_conn_new.rate,
                conn_denied_ps=st.c_denied.rate,
                active_conns=st.last_active,
                idle_conns=st.last_idle,
                pkts_out_ps=st.c_tx_pkts.rate,
                drop_out_ps=st.c_tx_drops.rate,
                overlimit_ps=st.c_overlimits.rate,
                backlog_bytes=st.last_backlog,
                degraded=degraded,
            ))

        if self._log.isEnabledFor(logging.DEBUG):
            for u in usages:
                self._log.debug(
                    "本秒采样完成，输出该 frontend 的用量样本 "
                    "frontend=%s rate_bps=%.1f mean10_bps=%.1f ewma60_bps=%.1f "
                    "conn_cur=%d conn_new_ps=%.1f conn_denied_ps=%.1f "
                    "active_conns=%d idle_conns=%d degraded=%s",
                    u.name, u.rate_bps, u.mean10_bps, u.ewma60_bps,
                    u.conn_cur, u.conn_new_ps, u.conn_denied_ps,
                    u.active_conns, u.idle_conns, u.degraded)
        return usages

    async def _tick_tc(self) -> None:
        """刷新来自内核 tc 的每 frontend 队列统计（第四条采样链路）。

        与 show info / 网卡两条链路同样是**监控专用**：失败只沿用上一拍并
        记一次日志，绝不影响限速判定。tc 上没有对应类的 frontend（配置刚
        加、还没下发）按"确实没有"归零，而不是留着上一拍的陈旧值。
        """
        if self._tc_stats is None:
            return
        try:
            by_name = await self._tc_stats()
        except Exception as e:
            self._log_once("tc", e,
                           "读取 tc 队列统计失败，监听端口视图的包/丢包曲线"
                           "沿用上一拍的值（限速与其余监控不受影响）")
            return
        self._logged_tc_error = ""
        for name in self._managed:
            st = self._states.get(name)
            if st is None:
                continue
            cs = by_name.get(name)
            if cs is None:
                st.blank_tc()
            else:
                st.observe_tc(cs)

    # ------------------------------------------------------------------
    # 实例视图（监控专用副链路，失败不影响限速判定）
    # ------------------------------------------------------------------

    def _tick_instance(self, stats: "list[model.FrontendStat] | None",
                       info_res: "model.InstanceStat | BaseException") -> None:
        """刷新整机用量视图。

        两个入参都是 tick 已经取好的结果（含异常对象），本方法只做归并——
        采样本身在 tick 里并发发出，理由见那里。

        stats 为 None 表示本拍 `show stat` 失败——此时按 fail-static 保留
        上一拍的 HAProxy 口径数值，只把 degraded 打上；网卡计数与 HAProxy
        无关，仍照常采（它往往正是"HAProxy 挂了但机器还在收包"的证据）。

        本方法**不抛异常**：任何一条副链路失败都只记日志并沿用旧值。
        """
        prev = self._instance
        inst = model.InstanceUsage(nic=self._nic, degraded=self._degraded)

        # --- 1) show info：整机连接数。show stat 的行按 proxy 拆，加总会
        # 把同一条连接重复计，因此并发连接数只能从这里取。
        info: model.InstanceStat | None = None
        if isinstance(info_res, BaseException):
            self._log_once("info", info_res,
                           "show info 采样失败，实例视图的连接数沿用上一拍的值"
                           "（限速与 frontend 视图不受影响）")
        else:
            info = info_res
        if info is not None:
            self._logged_info_error = ""
            inst.conn_cur = info.curr_conns
            inst.max_conn = info.max_conn
            inst.idle_pct = info.idle_pct
        else:
            inst.conn_cur = prev.conn_cur
            inst.max_conn = prev.max_conn
            inst.idle_pct = prev.idle_pct

        # --- 2) 全体 frontend 汇总：新建连接数、带宽与拒绝数。刻意**不**
        # 限于受管 frontend——"整个实例的视图"就该覆盖这台 HAProxy 上的全部
        # 监听端口，哪怕它们是运维手写、不归 rl-limiter 管的。
        #
        # 新建连接数为什么不用 show info 的 CumConns：**它把 rl-limiter 自己
        # 对 runtime API 的连接也算进去了**。runtime socket 一次连接只服务
        # 一条命令，本采集器每秒建两条（show stat + show info），于是
        # CumConns 的差分恒有 +2 的底噪，空载时曲线会稳稳停在 2/秒。实测
        # 佐证：同一时刻 CumConns=256，而各 frontend 的 conn_tot 之和只有
        # 28——差额全是采集器自己。Σ conn_tot 只统计真正经监听端口进来的
        # 连接，与"丢失连接数"同源，两条曲线也才可比。
        if stats is not None:
            s = self._inst_state
            inst.conn_new_ps = s.c_conn_new.push(sum(x.conn_tot for x in stats))
            inst.rate_in_bps = s.c_in.push(sum(x.bytes_in for x in stats))
            inst.rate_out_bps = s.c_out.push(sum(x.bytes_out for x in stats))
            inst.conn_denied_ps = s.c_denied.push(
                sum(x.denied_total for x in stats))
            inst.active_conns = sum(x.active_conns for x in stats)
            inst.idle_conns = sum(x.idle_conns for x in stats)
        else:
            inst.conn_new_ps = prev.conn_new_ps
            inst.rate_in_bps = prev.rate_in_bps
            inst.rate_out_bps = prev.rate_out_bps
            inst.conn_denied_ps = prev.conn_denied_ps
            inst.active_conns = prev.active_conns
            inst.idle_conns = prev.idle_conns

        # --- 3) 网卡：数据包与丢包。HAProxy 完全不统计包，只此一途
        # （见 netdev 模块）。整机口径，无法按 frontend 拆。
        nic: model.NicStat | None = None
        if self._nic:
            try:
                nic = netdev.read_nic_stat(self._nic)
            except Exception as e:
                self._log_once("nic", e,
                               "网卡计数器读取失败，实例视图的数据包/丢包曲线"
                               "沿用上一拍的值（其余监控不受影响）")
        if nic is not None:
            self._logged_nic_error = ""
            s = self._inst_state
            inst.pkts_in_ps = s.c_pkt_in.push(nic.rx_packets)
            inst.pkts_out_ps = s.c_pkt_out.push(nic.tx_packets)
            inst.drop_in_ps = s.c_drop_in.push(nic.rx_dropped)
            inst.drop_out_ps = s.c_drop_out.push(nic.tx_dropped)
            inst.nic_rate_in_bps = s.c_nic_in.push(nic.rx_bytes)
            inst.nic_rate_out_bps = s.c_nic_out.push(nic.tx_bytes)
        else:
            inst.pkts_in_ps = prev.pkts_in_ps
            inst.pkts_out_ps = prev.pkts_out_ps
            inst.drop_in_ps = prev.drop_in_ps
            inst.drop_out_ps = prev.drop_out_ps
            inst.nic_rate_in_bps = prev.nic_rate_in_bps
            inst.nic_rate_out_bps = prev.nic_rate_out_bps

        self._instance = inst

    def _log_once(self, kind: str, err: BaseException, msg: str) -> None:
        """副链路的失败日志去重：同一种错误只在首次出现时记一条 warning。

        这两条链路每秒都会重试，一直失败（比如网卡被改名）时不去重就会
        每秒一条 warning 把日志刷没。错误措辞变化时会再记一条，因此故障
        转移不会被静默吃掉。
        """
        key = f"{type(err).__name__}: {err}"
        attr = {"info": "_logged_info_error", "nic": "_logged_nic_error",
                "tc": "_logged_tc_error"}[kind]
        if getattr(self, attr) == key:
            return
        setattr(self, attr, key)
        self._log.warning("%s err=%s", msg, key)
