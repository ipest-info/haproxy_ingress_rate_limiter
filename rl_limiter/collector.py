# rl_limiter.collector —— 集中式快环的"输入级"：每秒并发采样所有 HAProxy
# 节点的 frontend 统计，把 bytes_out 累计计数差分成每秒速率，并按
# Target(node, frontend) → env 映射聚合成各环境的**全局**用量样本，供
# governor（快环限速决策）与执行路径（加权分配）消费。
#
# 多节点设计要点：v2.0 下同一环境的 frontend 可能分布在多台 HAProxy
# 上，因此：
#   - 差分基线从 per-frontend 升级为 per-Target（(node, frontend) 二元组）；
#   - 采样失败的容错从"整体降级"细化为**单节点失败隔离**：一台 HAProxy
#     失联不影响其他节点的测量，只有失联节点上的 Target 沿用上一秒速率
#     （fail-static，§3.7——速率归零会诱导快环误放松限速，方向上不安全）；
#   - 除 env 级 EWMA 外，另维护 per-Target 的 60s EWMA，作为执行路径按
#     挂载点加权分配整形值的输入（原慢环算法的输入，v2.0 下沉到这里）。
#
# 采集口径遵循设计文档 §3.1：以 frontend 的 bytes_out（HAProxy 发回客户端
# 的应用层字节数）为准，而非网卡计数——口径与计费一致，且天然按 frontend
# 拆分。在每秒瞬时速率之上维护两条平滑曲线：10 秒滑动窗口均值（承诺口径，
# 快环决策输入）与 60 秒 EWMA（加权分配输入），瞬时毛刺不会直接触发任何
# 限速动作。

from __future__ import annotations

import asyncio
import logging

from . import model
from .haproxy import RuntimeClient
from .window import Ewma, SlidingWindow

# 滑动窗口容量（单位：秒）。10 秒滑动均值是已拍板的承诺口径——"10 秒均值
# ≤ 约定带宽，瞬时容忍至 110%"（设计文档 §3.1/§3.3），因此该值与业务承诺
# 绑定，不是随意可调的平滑参数。
WINDOW10_SIZE = 10
# 按经典 span 公式 α = 2/(N+1) 取 N=60，在 1 秒 tick 节奏下近似 60 秒 EWMA。
# EWMA 相比再开一个 60 格窗口只需 O(1) 状态，且对加权分配来说"平滑趋势"
# 比"精确窗口语义"更重要。
EWMA60_ALPHA = 2.0 / (60 + 1)
# 触发单节点降级的连续采样失败次数（§3.7："连续 10s 失败则告警并保持当前
# 整形值不动"）。达到阈值只是打标记与告警，不清空任何状态——差分基线
# 保留，节点恢复后差分立即可用。
DEGRADED_FAILURE_THRESHOLD = 10
# 限制"Target 从采样结果中消失后其计数基线还保留多少个该节点的成功 tick"。
# 保留一段时间是为了容忍 HAProxy reload 等短暂消失场景（回来后差分依旧
# 连续）；但不能无限保留，否则被真正下线的 frontend 会造成状态泄漏。60 个
# 成功 tick（约 1 分钟）后基线被淘汰，此后若同名 frontend 再出现则按首次
# 采样重新建基线。
ABSENT_TICK_LIMIT = 60


class _TargetState:
    """按 Target 维护的计数差分基线。

    它独立于 mapping 存在：即使某个 Target 当前未映射到任何 env，其基线也
    持续刷新，这样 set_mapping 换新映射后，下一个 tick 的差分依然连续，
    不会因为"刚被映射进来"而出现一次虚高或缺失的速率。
    """

    __slots__ = ("last_bytes_out", "last_rate", "last_conn", "absent_ticks", "has_rate")

    def __init__(self, bytes_out: int, conn_cur: int):
        self.last_bytes_out = bytes_out
        self.last_rate = 0.0     # 上一秒速率（bytes/s）；回绕/节点失联时沿用该值（§3.7）
        self.last_conn = conn_cur  # 上一次观测到的并发连接数（节点失联时无法重采，保持不变）
        self.absent_ticks = 0    # 连续多少个"该节点成功采样"的 tick 未见到该 Target
        # 是否已经产出过至少一次基于差分的速率。刚建基线的 Target 速率是
        # "未知"而非零——节点随即失联时，不能把这个 0 当作测量值喂进窗口
        # （见 _Agg 的 measured/baselined 注释）。
        self.has_rate = False


class _EnvState:
    """按环境维护的聚合状态：滑动窗口与 EWMA。"""

    __slots__ = ("window", "ewma")

    def __init__(self):
        self.window = SlidingWindow(WINDOW10_SIZE)
        self.ewma = Ewma(EWMA60_ALPHA)


class _Agg:
    """单个 tick 内某环境的聚合中间量。

    measured：至少一个已映射 Target 贡献了基于差分（或 fail-static 沿用）
    的速率。baselined：至少一个已映射 Target 本 tick 是首次采样（只建
    基线）。仅建基线的 tick（baselined 且非 measured）速率是"未知"而非
    零：服务重启后若把 0 喂进窗口/EWMA，mean10 会被压低约 10 秒、EWMA
    输入被压低数十秒，导致该收紧时收紧变慢——所以未知就跳过，让窗口只吃
    真实测量值。多节点下"部分节点 baseline、部分节点有测量值"时以有测量
    值的为准照常喂入（求和里 baseline Target 的贡献为 0，属于可接受的
    暂时低估，好过整体不喂）。
    """

    __slots__ = ("rate", "conn", "measured", "baselined")

    def __init__(self):
        self.rate = 0.0
        self.conn = 0
        self.measured = False
        self.baselined = False


class Collector:
    """把多节点原始 frontend 统计转换成按环境全局聚合的用量样本。

    并发约定：tick 由单一协程（服务核心循环）以固定 1 秒节奏调用；
    set_mapping / degraded_nodes 也在同一事件循环内调用。asyncio 单线程
    模型下无数据竞争，因此不加任何锁。
    """

    def __init__(self, clients: dict[str, RuntimeClient],
                 log: logging.Logger | None = None):
        # node 名 → 客户端。节点集合是基础设施配置（本地 YAML），运行期
        # 不变；mapping（业务配置）才会热替换。
        self._clients = dict(clients)
        self._log = log if log is not None else logging.getLogger(__name__)

        self._mapping: dict[model.Target, str] = {}   # Target -> env id，由配置层下发
        self._failures: dict[str, int] = {}           # node -> 连续采样失败次数
        self._degraded: set[str] = set()              # 已越过失败阈值的节点集合
        self._targets: dict[model.Target, _TargetState] = {}  # 差分基线
        self._envs: dict[str, _EnvState] = {}         # env 级窗口/EWMA
        self._target_ewma: dict[model.Target, Ewma] = {}  # per-Target 60s EWMA（加权分配输入）
        # 以下两个集合只为"同类日志只记一次"，防止每秒刷屏。
        self._logged_unmapped: set[model.Target] = set()
        self._logged_missing_node: set[model.Target] = set()

    def set_mapping(self, target_to_env: dict[model.Target, str]) -> None:
        """整体替换 Target→env 映射（配置下发时调用）。输入 dict 被拷贝
        一份，调用方之后可以继续改动自己的副本。"""
        self._mapping = dict(target_to_env)
        self._log.debug("target mapping replaced target_count=%d", len(self._mapping))

    def degraded_nodes(self) -> set[str]:
        """返回当前处于 degraded 状态（连续失败 ≥ 阈值）的节点名集合。
        上层据此告警并冻结对应环境的整形值（fail-static）。"""
        return set(self._degraded)

    async def tick(self, now: float) -> list[model.EnvUsage]:
        """并发采样所有节点一次，返回按 env_id 排序的各环境用量。

        now 由调用方注入以便测试确定性；核心循环以固定 1 秒节奏调用 tick，
        因此计数差分本身就是 bytes/s 速率，无需再除以真实时间间隔。
        """
        _ = now  # 速率计算依赖固定 1s 节奏而非墙钟差值，now 仅为将来扩展保留
        mapping = self._mapping

        # 并发采样全部节点（包括暂无映射 Target 的节点：其 frontend 基线仍
        # 需持续刷新，将来被映射进来时差分从第一秒起就是连续的）。
        # return_exceptions=True 实现单节点失败隔离——一台失联不拖累其他。
        nodes = list(self._clients)
        results = await asyncio.gather(
            *(self._clients[n].show_stat() for n in nodes),
            return_exceptions=True)

        # mapping 中引用到的每个 env 都会被输出，哪怕本 tick 没有任何存活
        # 的 Target——下游（governor/执行路径）因此看到稳定的 env 集合，
        # 无需处理"env 忽隐忽现"的情况。
        sums: dict[str, _Agg] = {}
        env_targets: dict[str, list[model.Target]] = {}
        for target, env_id in mapping.items():
            sums.setdefault(env_id, _Agg())
            env_targets.setdefault(env_id, []).append(target)
            if target.node not in self._clients and target not in self._logged_missing_node:
                # 映射引用了未配置的节点：多半是配置错误（节点名拼写不一致
                # 或 YAML 漏配）。warn 一次后忽略该 Target——它永远采不到数。
                self._logged_missing_node.add(target)
                self._log.warning(
                    "target references unknown haproxy node; ignoring node=%s frontend=%s env=%s",
                    target.node, target.frontend, env_id)

        ok_nodes: set[str] = set()
        present: set[model.Target] = set()

        for node, result in zip(nodes, results):
            if isinstance(result, BaseException):
                self._node_failed(node, result, mapping, sums)
                continue
            ok_nodes.add(node)
            self._node_ok(node, result, mapping, sums, present)

        # 基线生命周期：只对"本 tick 采样成功的节点"上消失的 Target 累加
        # 缺席计数——节点失联时 Target 不算缺席（它只是采不到，不是没了）。
        # 超过 ABSENT_TICK_LIMIT（约 1 分钟）后淘汰基线，防止已下线的
        # frontend 造成状态泄漏；限期内回归的 Target（如 reload 抖动）差分
        # 保持连续。淘汰基线的同时淘汰其 per-Target EWMA：一个消失一分钟的
        # 挂载点不应再以冻结的旧用量参与加权分配。
        for target in list(self._targets):
            if target.node not in ok_nodes or target in present:
                continue
            ts = self._targets[target]
            ts.absent_ticks += 1
            if ts.absent_ticks >= ABSENT_TICK_LIMIT:
                del self._targets[target]
                self._target_ewma.pop(target, None)
                self._log.info(
                    "dropping counter baseline for absent frontend node=%s frontend=%s "
                    "absent_ticks=%d absent_tick_limit=%d",
                    target.node, target.frontend, ts.absent_ticks, ABSENT_TICK_LIMIT)

        # 聚合状态生命周期：mapping 里不再出现的 env，其窗口/EWMA 一并
        # 丢弃——陈旧的平滑状态若保留，env 将来重新上线时会带着过期历史
        # 起步。per-Target EWMA 同理跟随 mapping 生命周期。
        for env_id in list(self._envs):
            if env_id not in sums:
                del self._envs[env_id]
                self._log.debug("dropping aggregation state for unmapped env env=%s", env_id)
        for target in list(self._target_ewma):
            if target not in mapping:
                del self._target_ewma[target]

        # env.degraded = 该 env 存在挂在 degraded 节点上的 Target（按 mapping
        # 判定，而非本 tick 是否实际采到——degraded 描述的是采样通道健康度）。
        usages: list[model.EnvUsage] = []
        for env_id, agg in sums.items():
            st = self._envs.get(env_id)
            if st is None:
                st = _EnvState()
                self._envs[env_id] = st
            # 仅建基线的 tick（baselined 且非 measured）速率未知，跳过窗口/
            # EWMA，原因见 _Agg 注释。而"既无测量也无基线"的 env（映射里
            # 有、但本 tick 没有任何存活 Target）是真实的零：没有 frontend
            # 就没有流量，零值必须进入窗口，否则 mean10 会停留在旧值上虚高。
            if agg.measured or not agg.baselined:
                st.window.push(agg.rate)
                st.ewma.update(agg.rate)
            degraded = any(t.node in self._degraded for t in env_targets.get(env_id, ()))
            usages.append(model.EnvUsage(
                env_id=env_id,
                rate_bps=agg.rate,
                mean10_bps=st.window.mean(),
                ewma60_bps=st.ewma.value,
                conn_cur=agg.conn,
                degraded=degraded,
                # 只输出已有测量历史的 Target（EWMA 未 seed 的不输出，
                # 避免加权分配把"未知"当作 0 权重与真实 0 混淆）。
                target_ewma={
                    t: self._target_ewma[t].value
                    for t in env_targets.get(env_id, ())
                    if t in self._target_ewma
                },
            ))
        usages.sort(key=lambda u: u.env_id)  # 输出顺序确定，便于测试比对与日志稳定阅读

        # 每 tick 的 per-env 汇总仅在 debug 级输出（每秒每环境一条，量大）。
        if self._log.isEnabledFor(logging.DEBUG):
            for u in usages:
                self._log.debug(
                    "tick env usage env=%s rate_bps=%.1f mean10_bps=%.1f ewma60_bps=%.1f "
                    "conn_cur=%d degraded=%s",
                    u.env_id, u.rate_bps, u.mean10_bps, u.ewma60_bps, u.conn_cur, u.degraded)
        return usages

    # ------------------------------------------------------------------
    # 单节点采样结果处理
    # ------------------------------------------------------------------

    def _node_failed(self, node: str, err: BaseException,
                     mapping: dict[model.Target, str],
                     sums: dict[str, _Agg]) -> None:
        """处理采样失败的节点（§3.7 fail-static 的节点级版本）。

        该节点全部已知且已映射的 Target 沿用上一秒的速率与连接数继续参与
        聚合，让 mean10/ewma60 在陈旧数据上继续推进，而不是留下空洞或骤降
        为零——速率归零会诱导快环误放松限速，方向上不安全。连续失败计数
        达到阈值时把该节点标记为 degraded 并升级为 error 日志（只在恰好
        越线的那一次发，避免每秒重复告警）。
        """
        self._failures[node] = self._failures.get(node, 0) + 1
        failures = self._failures[node]
        crossed = failures == DEGRADED_FAILURE_THRESHOLD
        degraded = failures >= DEGRADED_FAILURE_THRESHOLD
        if degraded:
            self._degraded.add(node)

        held = 0
        for target, ts in self._targets.items():
            if target.node != node:
                continue
            # 失联前就已缺席的 Target 不参与沿用：最后一次成功采样里它本来
            # 就没有贡献，沿用它的陈旧速率反而会凭空抬高聚合值。
            if ts.absent_ticks > 0:
                continue
            env_id = mapping.get(target)
            if env_id is None:
                continue
            agg = sums[env_id]
            if ts.has_rate:
                agg.rate += ts.last_rate
                agg.conn += ts.last_conn
                agg.measured = True
                held += 1
                # 沿用值同样喂 per-Target EWMA，与 env 级"陈旧数据上继续
                # 推进"的口径保持一致（分子分母同源，加权比例不失真）。
                self._feed_target_ewma(target, ts.last_rate)
            else:
                # 刚建基线就失联：速率仍是未知而非零，按 baseline-only 处理。
                agg.conn += ts.last_conn
                agg.baselined = True

        self._log.warning(
            "stats sample failed; holding last rates node=%s err=%s consecutive_failures=%d "
            "degraded_threshold=%d degraded=%s held_targets=%d",
            node, err, failures, DEGRADED_FAILURE_THRESHOLD, degraded, held)
        if crossed:
            self._log.error(
                "collector node degraded: consecutive stats sample failures reached threshold "
                "node=%s threshold=%d consecutive_failures=%d",
                node, DEGRADED_FAILURE_THRESHOLD, failures)

    def _node_ok(self, node: str, stats: list[model.FrontendStat],
                 mapping: dict[model.Target, str],
                 sums: dict[str, _Agg],
                 present: set[model.Target]) -> None:
        """处理采样成功的节点：差分各 Target 的 bytes_out、按 mapping 聚合
        到 env，并维护基线状态。"""
        # 任何一次成功采样都会清零该节点的失败计数并解除降级——降级只
        # 反映"当下是否连续采不到数据"，不做粘滞。
        prev_failures = self._failures.get(node, 0)
        if prev_failures > 0:
            was_degraded = node in self._degraded
            self._failures[node] = 0
            self._degraded.discard(node)
            if was_degraded:
                self._log.info(
                    "stats sampling recovered node=%s previous_consecutive_failures=%d "
                    "degraded_threshold=%d",
                    node, prev_failures, DEGRADED_FAILURE_THRESHOLD)
            else:
                self._log.debug(
                    "stats sampling recovered before degradation node=%s "
                    "previous_consecutive_failures=%d",
                    node, prev_failures)

        for fs in stats:
            target = model.Target(node, fs.name)
            present.add(target)
            env_id = mapping.get(target)
            ts = self._targets.get(target)

            if ts is None:
                if env_id is None:
                    # 未映射且从未见过的 Target：不建基线（省状态），只在
                    # 首次出现时记一条 debug，避免每秒刷同样的日志。
                    if target not in self._logged_unmapped:
                        self._logged_unmapped.add(target)
                        self._log.debug(
                            "ignoring unmapped frontend node=%s frontend=%s bytes_out=%d conn_cur=%d",
                            node, fs.name, fs.bytes_out, fs.conn_cur)
                    continue
                # 首次采样：只有一个累计值、没有前值可差分，速率未知，本
                # tick 仅记录基线。连接数是瞬时值不依赖差分，可直接计入。
                self._targets[target] = _TargetState(fs.bytes_out, fs.conn_cur)
                agg = sums[env_id]
                agg.conn += fs.conn_cur
                agg.baselined = True
                self._log.info(
                    "baselined new frontend node=%s frontend=%s env=%s bytes_out=%d conn_cur=%d",
                    node, fs.name, env_id, fs.bytes_out, fs.conn_cur)
                continue

            ts.absent_ticks = 0
            if fs.bytes_out >= ts.last_bytes_out:
                rate = float(fs.bytes_out - ts.last_bytes_out)
            else:
                # 计数回绕（典型场景：HAProxy reload 后计数从零重来）：差分
                # 为负不可用，按 §3.7 沿用上一秒速率顶过这一秒，同时用新
                # 累计值重建基线，下一秒差分即恢复正常。
                rate = ts.last_rate
                self._log.info(
                    "bytes_out counter went backwards; holding previous rate node=%s frontend=%s "
                    "previous_bytes_out=%d current_bytes_out=%d held_rate_bps=%.1f",
                    node, fs.name, ts.last_bytes_out, fs.bytes_out, ts.last_rate)
            ts.last_bytes_out = fs.bytes_out
            ts.last_rate = rate
            ts.last_conn = fs.conn_cur
            ts.has_rate = True
            # 已知但当前未映射的 Target 也在上面刷新了基线：将来被重新映射
            # 进来时，差分从第一秒起就是连续正确的，而不必重走"首采样建
            # 基线"丢掉一秒数据。
            if env_id is not None:
                agg = sums[env_id]
                agg.rate += rate
                agg.conn += fs.conn_cur
                agg.measured = True
                self._feed_target_ewma(target, rate)

    def _feed_target_ewma(self, target: model.Target, rate: float) -> None:
        """把一次测量值（或 fail-static 沿用值）折入该 Target 的 60s EWMA。
        首次测量即 seed（见 Ewma 注释），baseline-only 的 tick 不会走到
        这里——EWMA 只吃"已知"的速率。"""
        e = self._target_ewma.get(target)
        if e is None:
            e = Ewma(EWMA60_ALPHA)
            self._target_ewma[target] = e
        e.update(rate)
