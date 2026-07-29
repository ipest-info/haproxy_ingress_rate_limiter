# rl_limiter.metricstore —— 监控数据的落库与回查（90 天窗口）。
#
# ## 为什么不能直接把每秒的样本存下来
#
# 监控循环每秒产出一拍。90 天 = 7,776,000 秒。按每行约 200 字节、10 个
# frontend 估算：
#
#   | 粒度   | 10 个 frontend 的行数 | 占用    |
#   | ------ | --------------------- | ------- |
#   | 1 秒   | 77,760,000            | 14.5 GB |
#   | 1 分钟 | 1,296,000             | 247 MB  |
#   | 5 分钟 | 259,200               | 49 MB   |
#
# 1 秒粒度存 90 天既写不动也查不动（单表近亿行，一次范围扫描就是灾难）。
# 因此采用**分级保留**（RRD/Prometheus 都是这个路子）：
#
#   | 层     | 粒度   | 保留   | 用途                             |
#   | ------ | ------ | ------ | -------------------------------- |
#   | 内存   | 1 秒   | 10 分钟| 控制台实时曲线（本模块不管）     |
#   | 落库   | 1 分钟 | 7 天   | "昨天下午三点前后到底怎么了"     |
#   | 落库   | 5 分钟 | 90 天  | 回查 + **计费**                  |
#
# 落库合计约 69 MB —— 90 天回查这件事本身几乎不花钱，贵的是粒度。
#
# **5 分钟这个粒度不是拍脑袋的**：带宽计费的行业惯例（95 计费）就是按
# 5 分钟采样点统计的。本项目服务的正是带宽计费场景，所以最长的那一层
# 直接对齐计费口径，回查与对账用同一份数据，不会出现"运维看的数和账单
# 上的数对不上"。
#
# ## 每个桶存什么：三类量三种聚合
#
# 这是设计里最容易做错的一处。把所有指标一律存平均值，回查时就会发现
# 该问的问题一个都答不了：
#
#   - **速率类**（出/入带宽）存 avg + max。avg 是计费口径（95 计费就是
#     对 5 分钟 avg 取分位），max 用来找峰值——只存 avg 会把尖峰抹平，
#     "那天下午到底冲到多少"就永远查不出来了。
#   - **计数类**（新建连接、丢包、被拒绝）存 sum。这类问题是"那天一共
#     丢了多少包"，存平均速率再乘回去会因为桶不满而失真。
#   - **瞬时类**（并发/活跃/空闲连接）存 avg + max。
#
# 另外每个桶都记下**当时的限额**。回查一条带宽曲线时，没有当时的限额做
# 参照就读不出"有没有打满"——而限额是会被人改的，事后从配置里查到的是
# 现在的值，不是当时的值。
#
# 还记 samples（该桶实际采到几拍）与 degraded（其中几拍是失联时沿用的
# 陈旧值）。缺了这两个数，一个"半空的桶"和一个"真的很闲的桶"在图上长得
# 一模一样。
#
# ## 与限速、与实时监控的关系：完全隔离
#
# 与本项目其它副链路一致——**落库失败绝不影响限速，也不影响实时控制台**。
# 写入走一个有界队列，库挂了就丢最旧的批次并告警，监控循环永远不会被
# 数据库的 IO 卡住。

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from typing import Any

from . import dbconfig, model

# 落库的两层粒度（秒）与各自的保留天数。
TIER_MINUTE = 60
TIER_FIVE_MINUTE = 300
RETENTION_DAYS = {TIER_MINUTE: 7, TIER_FIVE_MINUTE: 90}

# 实例级数据在 scope 列里用这个占位（frontend 名不可能是空串）。
INSTANCE_SCOPE = ""

# 待写队列上限（按"批"计，一批 = 一个桶关闭时产出的全部行）。库不可用时
# 丢最旧的批，宁可丢历史也不能让内存无界增长或把监控循环拖住。
MAX_PENDING_BATCHES = 240        # 1 分钟一批 → 约 4 小时的积压容量

# 单次剪枝删除的行数上限。一次删太多会长时间持锁，把配置读写一起拖慢。
PRUNE_BATCH = 5000
# 剪枝周期（秒）。数据只增不改，一小时清一次绰绰有余。
PRUNE_INTERVAL_S = 3600

# 查询一次最多返回多少个点。超过就自动降到更粗的层——否则"查 90 天"
# 会一次拉回 13 万个点，浏览器和数据库一起遭殃。
MAX_QUERY_POINTS = 2000


@dataclass(slots=True)
class _Acc:
    """单个 scope 在单个桶内的累加器。

    只保留聚合所需的最小状态：和、最大值、计数。不留原始样本——留了就
    等于把 1 秒粒度又搬进内存一遍。
    """

    samples: int = 0
    degraded: int = 0
    out_sum: float = 0.0
    out_max: float = 0.0
    in_sum: float = 0.0
    in_max: float = 0.0
    conn_sum: float = 0.0
    conn_max: int = 0
    active_sum: float = 0.0
    idle_sum: float = 0.0
    conn_new_sum: float = 0.0
    conn_denied_sum: float = 0.0
    pkts_out_sum: float = 0.0
    drop_out_sum: float = 0.0
    overlimit_sum: float = 0.0
    quota: float = 0.0

    def add_frontend(self, u: model.FrontendUsage, quota: float) -> None:
        self.samples += 1
        if u.degraded:
            self.degraded += 1
        self.out_sum += u.rate_bps
        self.out_max = max(self.out_max, u.rate_bps)
        self.in_sum += u.rate_in_bps
        self.in_max = max(self.in_max, u.rate_in_bps)
        self.conn_sum += u.conn_cur
        self.conn_max = max(self.conn_max, u.conn_cur)
        self.active_sum += u.active_conns
        self.idle_sum += u.idle_conns
        self.conn_new_sum += u.conn_new_ps
        self.conn_denied_sum += u.conn_denied_ps
        self.pkts_out_sum += u.pkts_out_ps
        self.drop_out_sum += u.drop_out_ps
        self.overlimit_sum += u.overlimit_ps
        # 限额取桶内最后一次的值：桶中途改了限额时，记后半段那个更贴近
        # "这个桶结束时的约束"。桶只有 1~5 分钟，中途改限额本就罕见。
        self.quota = quota

    def add_instance(self, i: model.InstanceUsage) -> None:
        self.samples += 1
        if i.degraded:
            self.degraded += 1
        self.out_sum += i.rate_out_bps
        self.out_max = max(self.out_max, i.rate_out_bps)
        self.in_sum += i.rate_in_bps
        self.in_max = max(self.in_max, i.rate_in_bps)
        self.conn_sum += i.conn_cur
        self.conn_max = max(self.conn_max, i.conn_cur)
        self.active_sum += i.active_conns
        self.idle_sum += i.idle_conns
        self.conn_new_sum += i.conn_new_ps
        self.conn_denied_sum += i.conn_denied_ps
        # 实例级的包统计来自网卡（整机口径），与 frontend 级的 tc 口径
        # 不是一回事，但落进同一列——查询时按 scope 区分即可。
        self.pkts_out_sum += i.pkts_out_ps
        self.drop_out_sum += i.drop_out_ps

    def row(self, instance: str, scope: str, bucket_s: int, ts: int) -> tuple:
        """产出一行（列顺序与 _INSERT_SQL 严格对应）。

        平均值按**实际采到的拍数**算而不是按桶的理论长度：桶不满时
        （启动、重启、失联）除以理论长度会把平均值系统性地拉低。
        """
        n = max(self.samples, 1)
        return (
            instance, scope, bucket_s, ts, self.samples, self.degraded,
            int(self.out_sum / n), int(self.out_max),
            int(self.in_sum / n), int(self.in_max),
            int(self.conn_sum / n), int(self.conn_max),
            int(self.active_sum / n), int(self.idle_sum / n),
            int(self.conn_new_sum), int(self.conn_denied_sum),
            int(self.pkts_out_sum), int(self.drop_out_sum),
            int(self.overlimit_sum), int(self.quota),
        )


_COLUMNS = (
    "instance, scope, bucket_s, ts, samples, degraded, "
    "out_avg, out_max, in_avg, in_max, "
    "conn_avg, conn_max, active_avg, idle_avg, "
    "conn_new_sum, conn_denied_sum, "
    "pkts_out_sum, drop_out_sum, overlimit_sum, quota"
)
_INSERT_SQL = (
    f"INSERT INTO metric_rollup ({_COLUMNS}) "
    f"VALUES ({', '.join(['%s'] * 20)}) "
    # 重复写同一个桶只可能发生在进程重启且时钟回拨的边角场景。用
    # ON DUPLICATE 覆盖而不是报错——历史数据不值得为此让写入任务退出。
    f"ON DUPLICATE KEY UPDATE samples=VALUES(samples), degraded=VALUES(degraded), "
    f"out_avg=VALUES(out_avg), out_max=VALUES(out_max), "
    f"in_avg=VALUES(in_avg), in_max=VALUES(in_max), "
    f"conn_avg=VALUES(conn_avg), conn_max=VALUES(conn_max), "
    f"active_avg=VALUES(active_avg), idle_avg=VALUES(idle_avg), "
    f"conn_new_sum=VALUES(conn_new_sum), conn_denied_sum=VALUES(conn_denied_sum), "
    f"pkts_out_sum=VALUES(pkts_out_sum), drop_out_sum=VALUES(drop_out_sum), "
    f"overlimit_sum=VALUES(overlimit_sum), quota=VALUES(quota)"
)


def bucket_start(ts: float, bucket_s: int) -> int:
    """把时刻对齐到桶的起点（向下取整）。

    对齐到绝对时间边界而不是"进程启动后第 N 个桶"：多台机器写进同一张表
    时，只有绝对对齐才能把它们的行按时间列并排比较。
    """
    return int(ts) // bucket_s * bucket_s


class _Tier:
    """单个粒度层的累加与出桶。"""

    def __init__(self, bucket_s: int):
        self.bucket_s = bucket_s
        self.ts: int | None = None
        self.accs: dict[str, _Acc] = {}

    def add(self, now: float, usages, instance_usage, quotas: dict[str, float]
            ) -> list[_Acc] | None:
        """喂一拍。跨过桶边界时返回上一桶的累加结果，否则返回 None。"""
        b = bucket_start(now, self.bucket_s)
        closed = None
        if self.ts is None:
            self.ts = b
        elif b != self.ts:
            closed = (self.ts, self.accs)
            self.ts, self.accs = b, {}
        for u in usages:
            self.accs.setdefault(u.name, _Acc()).add_frontend(
                u, quotas.get(u.name, 0.0))
        if instance_usage is not None:
            self.accs.setdefault(INSTANCE_SCOPE, _Acc()).add_instance(instance_usage)
        return closed


class MetricStore:
    """把每拍的监控数据聚合成分级桶并落库；提供回查查询。

    与监控循环的耦合只有一个方法：record()。它只做内存累加，**不做任何
    IO**——落库由独立任务从队列里取。这样即便数据库卡住几十秒，监控循环
    的节拍也不受任何影响。
    """

    def __init__(self, opts: "dbconfig.MySQLOptions", instance: str,
                 log: logging.Logger | None = None):
        self._opts = opts
        self._instance = instance
        self._log = log if log is not None else logging.getLogger("rl_limiter.metrics")
        self._tiers = [_Tier(TIER_MINUTE), _Tier(TIER_FIVE_MINUTE)]
        # 待写批次队列（有界）。每项是 (bucket_s, ts, {scope: _Acc})。
        self._pending: asyncio.Queue = asyncio.Queue(MAX_PENDING_BATCHES)
        self._dropped = 0
        self._written = 0
        self._logged_error = ""

    # ---- 写入侧 ----

    def record(self, now: float, usages, instance_usage=None,
               quotas: dict[str, float] | None = None) -> None:
        """监控循环 sampler 回调：把一拍喂进各层累加器（纯内存，无 IO）。"""
        q = quotas or {}
        for tier in self._tiers:
            closed = tier.add(now, usages, instance_usage, q)
            if closed is None:
                continue
            ts, accs = closed
            self._enqueue(tier.bucket_s, ts, accs)

    def _enqueue(self, bucket_s: int, ts: int, accs: dict[str, _Acc]) -> None:
        if not accs:
            return
        item = (bucket_s, ts, accs)
        if self._pending.full():
            # 丢最旧的：新数据比旧数据有用，且无界增长会拖垮进程。
            try:
                self._pending.get_nowait()
                self._dropped += 1
            except asyncio.QueueEmpty:
                pass
        self._pending.put_nowait(item)

    def flush(self) -> None:
        """把各层**尚未关闭**的桶也推进待写队列（停机时调用）。

        桶要等下一拍跨过边界才会关闭并入队，所以正常运行时最新的那个桶
        一直悬在内存里。不 flush 的话每次重启都会丢掉最多一个桶——5 分钟
        那层就是 5 分钟数据，而生产上重启（发版、配置变更）并不罕见，
        日积月累会在 90 天的曲线上留下一串规律的缺口。

        产出的桶是"半截"的（samples < 桶长），但那正是 samples 列存在的
        意义：查询侧看得出这个点不完整，不会把它当成一次真实的低谷。
        """
        for tier in self._tiers:
            if tier.ts is not None and tier.accs:
                self._enqueue(tier.bucket_s, tier.ts, tier.accs)
                tier.accs = {}

    async def drain(self, timeout_s: float = 5.0) -> None:
        """停机收尾：flush 未关闭的桶，并等队列写完（有超时上限）。

        超时上限是必要的：停机不该因为数据库慢就无限期挂着——监控数据
        丢一点点，远好过让进程停不下来。
        """
        self.flush()
        try:
            async with asyncio.timeout(timeout_s):
                while not self._pending.empty():
                    await asyncio.sleep(0.05)
        except asyncio.TimeoutError:
            self._log.warning(
                "停机时监控数据未能在超时内写完，剩余批次丢弃（不阻塞停机） "
                "pending_batches=%d", self._pending.qsize())

    async def run_writer(self) -> None:
        """常驻任务：从队列取批次写库。失败重试，绝不退出。"""
        while True:
            bucket_s, ts, accs = await self._pending.get()
            rows = [a.row(self._instance, scope, bucket_s, ts)
                    for scope, a in sorted(accs.items())]
            try:
                await self._write(rows)
                self._written += len(rows)
                self._logged_error = ""
            except Exception as e:
                key = f"{type(e).__name__}: {e}"
                if key != self._logged_error:
                    self._logged_error = key
                    self._log.warning(
                        "监控数据落库失败，本批丢弃（实时监控与限速不受影响；"
                        "队列满时会丢最旧的批次） bucket_s=%d ts=%d rows=%d "
                        "dropped_batches=%d err=%s",
                        bucket_s, ts, len(rows), self._dropped, key)

    async def _write(self, rows: list[tuple]) -> None:
        import aiomysql  # 延迟导入：不启用数据库时不需要这个依赖

        conn = await aiomysql.connect(
            host=self._opts.host, port=self._opts.port, user=self._opts.user,
            password=self._opts.password, db=self._opts.database,
            connect_timeout=self._opts.connect_timeout_s,
            charset="utf8mb4", autocommit=True)
        try:
            async with conn.cursor() as cur:
                await cur.executemany(_INSERT_SQL, rows)
        finally:
            conn.close()

    # ---- 剪枝 ----

    async def run_pruner(self, interval_s: float = PRUNE_INTERVAL_S) -> None:
        """常驻任务：按各层的保留期删除过期行。

        分批删而不是一条 DELETE 扫全表：这张表和配置表在同一个库里，
        长事务持锁会把配置读写一起拖慢。
        """
        while True:
            await asyncio.sleep(interval_s)
            for bucket_s, days in RETENTION_DAYS.items():
                cutoff = int(time.time()) - days * 86400
                try:
                    n = await self._prune(bucket_s, cutoff)
                    if n:
                        self._log.info(
                            "已清理过期的监控数据 bucket_s=%d retention_days=%d "
                            "cutoff_ts=%d deleted_rows=%d",
                            bucket_s, days, cutoff, n)
                except Exception as e:
                    self._log.warning("清理过期监控数据失败，下轮重试 err=%s", e)

    async def _prune(self, bucket_s: int, cutoff: int) -> int:
        import aiomysql

        conn = await aiomysql.connect(
            host=self._opts.host, port=self._opts.port, user=self._opts.user,
            password=self._opts.password, db=self._opts.database,
            connect_timeout=self._opts.connect_timeout_s,
            charset="utf8mb4", autocommit=True)
        total = 0
        try:
            async with conn.cursor() as cur:
                while True:
                    await cur.execute(
                        "DELETE FROM metric_rollup WHERE bucket_s=%s AND ts<%s "
                        "LIMIT %s", (bucket_s, cutoff, PRUNE_BATCH))
                    if not cur.rowcount:
                        break
                    total += cur.rowcount
                    if cur.rowcount < PRUNE_BATCH:
                        break
                    await asyncio.sleep(0)     # 让出事件循环，别把一轮删成长任务
        finally:
            conn.close()
        return total

    # ---- 查询侧 ----

    async def query(self, scope: str, start: int, end: int,
                    bucket_s: int | None = None) -> dict[str, Any]:
        """回查：返回 [start, end) 区间内某个 scope 的时间序列。

        bucket_s 不给时**自动选层**：优先用最细的那层，但要同时满足
        "该层的保留期覆盖得到 start" 与 "点数不超过 MAX_QUERY_POINTS"。
        查 90 天却拿 1 分钟粒度会一次拉回 13 万个点，浏览器和数据库一起
        遭殃；而查最近半小时用 5 分钟粒度又太糊。
        """
        chosen = bucket_s or pick_tier(start, end)
        rows = await self._select(scope, chosen, start, end)
        return {
            "scope": scope,
            "bucket_s": chosen,
            "start": start,
            "end": end,
            "points": rows,
        }

    async def _select(self, scope: str, bucket_s: int,
                      start: int, end: int) -> list[dict[str, Any]]:
        import aiomysql

        conn = await aiomysql.connect(
            host=self._opts.host, port=self._opts.port, user=self._opts.user,
            password=self._opts.password, db=self._opts.database,
            connect_timeout=self._opts.connect_timeout_s,
            charset="utf8mb4", autocommit=True)
        try:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute(
                    f"SELECT {_COLUMNS} FROM metric_rollup "
                    f"WHERE instance=%s AND scope=%s AND bucket_s=%s "
                    f"AND ts>=%s AND ts<%s ORDER BY ts",
                    (self._instance, scope, bucket_s, start, end))
                return list(await cur.fetchall())
        finally:
            conn.close()

    def stats(self) -> dict[str, int]:
        """写入侧的健康计数，供控制台展示。"""
        return {"written_rows": self._written,
                "dropped_batches": self._dropped,
                "pending_batches": self._pending.qsize()}


def pick_tier(start: int, end: int) -> int:
    """按查询区间自动选粒度层。

    规则很简单：**能用细的就用细的，前提是保留期覆盖得到、且点数不爆**。
    """
    span = max(end - start, 1)
    now = int(time.time())
    for bucket_s in (TIER_MINUTE, TIER_FIVE_MINUTE):
        covered = now - RETENTION_DAYS[bucket_s] * 86400
        if start >= covered and span / bucket_s <= MAX_QUERY_POINTS:
            return bucket_s
    return TIER_FIVE_MINUTE


async def run_metric_store(store: MetricStore, log: logging.Logger) -> None:
    """把写入与剪枝两个常驻任务跑起来（供 __main__ 单点接线）。"""
    log.info(
        "监控数据落库已启用：%d 秒粒度保留 %d 天，%d 秒粒度保留 %d 天"
        "（5 分钟粒度与带宽 95 计费口径一致）",
        TIER_MINUTE, RETENTION_DAYS[TIER_MINUTE],
        TIER_FIVE_MINUTE, RETENTION_DAYS[TIER_FIVE_MINUTE])
    await asyncio.gather(store.run_writer(), store.run_pruner())
