# rl_limiter.collector —— 采样层：每秒从本机 HAProxy 取一次 `show stat`，
# 差分出各受管 frontend 的下行速率，产出带 10s 均值与 60s EWMA 的用量样本。
#
# 单 HAProxy 模型（v0.4 起）：**监控单位 = frontend**。一个 frontend 就是
# 一个监听端口 + 一个 shared bwlim 速率桶，与 HAProxy 的限速机制一一对应，
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

import logging

from . import model
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


class _FrontendState:
    """按 frontend 维护的计数差分基线 + 平滑状态。

    基线独立于"是否受管"存在：即使某个 frontend 当前不在受管清单里，其
    基线也照常刷新，将来被纳管时差分从第一秒起就是连续的，不必重走
    "首次采样丢一秒"。
    """

    __slots__ = ("last_bytes_out", "last_rate", "last_conn", "has_rate",
                 "absent_ticks", "window", "ewma")

    def __init__(self, bytes_out: int, conn_cur: int):
        self.last_bytes_out = bytes_out
        self.last_rate = 0.0
        self.last_conn = conn_cur
        # 是否已产出过至少一次基于差分的速率。刚建基线的 frontend 速率是
        # "未知"而非 0，两者对窗口的影响完全不同。
        self.has_rate = False
        self.absent_ticks = 0
        self.window = SlidingWindow(WINDOW10_SIZE)
        self.ewma = Ewma(EWMA60_ALPHA)


class Collector:
    """从本机 HAProxy 采样并产出各受管 frontend 的用量样本。"""

    def __init__(self, client: RuntimeClient,
                 log: logging.Logger | None = None):
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

    async def tick(self, now: float) -> list[model.FrontendUsage]:
        """采样一次，返回按名字排序的各受管 frontend 用量。

        now 由调用方注入以便测试确定性；速率计算依赖固定 1s 节奏而非墙钟
        差值，now 仅为将来扩展保留。
        """
        _ = now
        try:
            stats = await self._client.show_stat()
        except Exception as e:
            return self._on_failure(e)
        return self._on_success(stats)

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
                self._states[fs.name] = _FrontendState(fs.bytes_out, fs.conn_cur)
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
                degraded=degraded,
            ))

        if self._log.isEnabledFor(logging.DEBUG):
            for u in usages:
                self._log.debug(
                    "本秒采样完成，输出该 frontend 的用量样本 "
                    "frontend=%s rate_bps=%.1f mean10_bps=%.1f ewma60_bps=%.1f "
                    "conn_cur=%d degraded=%s",
                    u.name, u.rate_bps, u.mean10_bps, u.ewma60_bps,
                    u.conn_cur, u.degraded)
        return usages
