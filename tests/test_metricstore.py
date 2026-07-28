# tests.test_metricstore —— 监控数据的分级聚合与回查。
#
# 测试重点是**聚合语义**，因为这几条错了不会报错，只会让 90 天之后有人
# 拿着一张读不出结论的图来问："这天到底发生了什么？"
#   1. 速率类留 max —— 只留 avg 会把尖峰抹平，峰值永远查不回来；
#   2. 计数类求 sum —— 问题形如"那天一共丢了多少包"；
#   3. 平均按**实际采到的拍数**算 —— 桶不满时除以理论长度会系统性偏低；
#   4. 记下当时的限额 —— 限额会被人改，事后查到的是现在的值；
#   5. 落库失败绝不影响监控循环 —— 队列满就丢最旧的，不阻塞、不抛。
#
# 库交互（写入/剪枝/查询 SQL）不在这里测：那些已经在真实 MariaDB 上跑过
# 并记进了 docs/07（含 90 天数据量下的查询与剪枝耗时）。

from __future__ import annotations

import time

from rl_limiter import metricstore as M
from rl_limiter import model


def fu(name="fe_a", rate=0.0, rate_in=0.0, conn=0, active=0, idle=0,
       new=0.0, denied=0.0, pkts=0.0, drops=0.0, over=0.0, degraded=False):
    return model.FrontendUsage(
        name=name, rate_bps=rate, rate_in_bps=rate_in, conn_cur=conn,
        active_conns=active, idle_conns=idle, conn_new_ps=new,
        conn_denied_ps=denied, pkts_out_ps=pkts, drop_out_ps=drops,
        overlimit_ps=over, degraded=degraded)


def store():
    from rl_limiter import dbconfig
    opts = dbconfig.MySQLOptions(host="127.0.0.1")
    return M.MetricStore(opts, "hap-1")


def rows_of(st):
    """取出待写队列里的全部批次，摊成 (bucket_s, ts, scope, row) 列表。"""
    out = []
    while not st._pending.empty():
        bucket_s, ts, accs = st._pending.get_nowait()
        for scope, acc in accs.items():
            out.append((bucket_s, ts, scope, acc.row("hap-1", scope, bucket_s, ts)))
    return out


# 行内各列的下标（与 metricstore._COLUMNS 一致）
I_SAMPLES, I_DEGRADED = 4, 5
I_OUT_AVG, I_OUT_MAX = 6, 7
I_CONN_AVG, I_CONN_MAX = 10, 11
I_NEW_SUM, I_DENIED_SUM = 14, 15
I_DROP_SUM = 17
I_QUOTA = 19


# ---------------------------------------------------------------------------
# 桶对齐
# ---------------------------------------------------------------------------

def test_buckets_align_to_absolute_time():
    """对齐到绝对时间边界，不是"进程启动后第 N 个桶"——多台机器写进同一
    张表时，只有绝对对齐才能把它们的行按时间并排比较。"""
    assert M.bucket_start(1700000123, 60) == 1700000100
    assert M.bucket_start(1700000123, 300) == 1700000100
    assert M.bucket_start(1700000399, 300) == 1700000100
    assert M.bucket_start(1700000400, 300) == 1700000400


# ---------------------------------------------------------------------------
# 聚合语义：三类量三种算法
# ---------------------------------------------------------------------------

def test_rate_keeps_both_average_and_peak():
    """速率类必须同时留 avg 与 max。只留 avg 的话，一个 5 分钟桶里 10 秒
    的尖峰会被彻底抹平——而"那天下午到底冲到多少"正是回查最常见的问题。"""
    st = store()
    base = M.bucket_start(time.time(), 60)
    for sec in range(60):
        rate = 9_000_000 if sec == 30 else 1_000_000     # 一个孤立尖峰
        st.record(base + sec, [fu(rate=rate)], None, {"fe_a": 5_000_000})
    st.flush()
    r = [x for x in rows_of(st) if x[0] == 60][0][3]
    assert r[I_OUT_MAX] == 9_000_000, "尖峰必须留住"
    assert 1_000_000 < r[I_OUT_AVG] < 1_300_000, "均值不应被尖峰带跑"


def test_counters_are_summed_not_averaged():
    """计数类求和：问题是"这段时间一共丢了多少包/拒了多少连接"。"""
    st = store()
    base = M.bucket_start(time.time(), 60)
    for sec in range(60):
        st.record(base + sec, [fu(new=2.0, denied=1.0, drops=3.0)], None, {})
    st.flush()
    r = [x for x in rows_of(st) if x[0] == 60][0][3]
    assert r[I_NEW_SUM] == 120        # 60 拍 × 2
    assert r[I_DENIED_SUM] == 60
    assert r[I_DROP_SUM] == 180


def test_average_uses_actual_sample_count_not_bucket_length():
    """桶不满时（启动、重启、采样失联）按理论长度除会把均值系统性拉低。

    这里只喂 10 拍，均值必须是这 10 拍的均值，而不是除以 60。
    """
    st = store()
    base = M.bucket_start(time.time(), 60)
    for sec in range(10):
        st.record(base + sec, [fu(rate=6_000_000, conn=100)], None, {})
    st.flush()
    r = [x for x in rows_of(st) if x[0] == 60][0][3]
    assert r[I_SAMPLES] == 10
    assert r[I_OUT_AVG] == 6_000_000, "不能除以 60"
    assert r[I_CONN_AVG] == 100


def test_bucket_records_quota_in_effect_at_the_time():
    """限额会被人改。回查一条曲线时若没有当时的限额做参照，就读不出
    "有没有打满"——事后从配置里查到的是现在的值，不是当时的值。"""
    st = store()
    base = M.bucket_start(time.time(), 60)
    for sec in range(60):
        st.record(base + sec, [fu(rate=1_000_000)], None, {"fe_a": 40_000_000})
    st.flush()
    r = [x for x in rows_of(st) if x[0] == 60][0][3]
    assert r[I_QUOTA] == 40_000_000


def test_degraded_ticks_are_counted():
    """记下桶内有几拍是采样失联时沿用的陈旧值。缺了这个数，一个数据不可信
    的桶和一个正常的桶在图上长得一模一样。"""
    st = store()
    base = M.bucket_start(time.time(), 60)
    for sec in range(60):
        st.record(base + sec, [fu(rate=1_000, degraded=sec < 20)], None, {})
    st.flush()
    r = [x for x in rows_of(st) if x[0] == 60][0][3]
    assert (r[I_SAMPLES], r[I_DEGRADED]) == (60, 20)


# ---------------------------------------------------------------------------
# 分层与出桶
# ---------------------------------------------------------------------------

def test_both_tiers_accumulate_from_the_same_samples():
    """1 分钟与 5 分钟两层各自独立累加同一批样本——不做"库里再 rollup"
    那一步，也就不需要额外的调度任务。"""
    st = store()
    base = M.bucket_start(time.time(), 300)
    for sec in range(300):
        st.record(base + sec, [fu(rate=2_000_000)], None, {})
    st.flush()
    got = rows_of(st)
    minute = [x for x in got if x[0] == 60]
    five = [x for x in got if x[0] == 300]
    assert len(minute) == 5, "300 秒应产出 5 个 1 分钟桶"
    assert len(five) == 1, "并产出 1 个 5 分钟桶"


def test_open_bucket_is_not_written_until_flush():
    """未关闭的桶悬在内存里，直到跨过边界或停机 flush。不 flush 就重启会
    在历史曲线上留下规律的缺口。"""
    st = store()
    base = M.bucket_start(time.time(), 60)
    for sec in range(30):
        st.record(base + sec, [fu(rate=1_000_000)], None, {})
    assert rows_of(st) == [], "桶没关，不该有待写数据"
    st.flush()
    assert [x for x in rows_of(st) if x[0] == 60], "flush 后应写出半截桶"


def test_instance_scope_uses_empty_string():
    st = store()
    base = M.bucket_start(time.time(), 60)
    for sec in range(60):
        st.record(base + sec, [fu(rate=1)],
                  model.InstanceUsage(conn_cur=7, rate_out_bps=5_000), {})
    st.flush()
    scopes = {x[2] for x in rows_of(st) if x[0] == 60}
    assert scopes == {"fe_a", M.INSTANCE_SCOPE}


# ---------------------------------------------------------------------------
# 与限速/实时监控的隔离
# ---------------------------------------------------------------------------

def test_record_never_blocks_and_drops_oldest_when_full():
    """落库队列满时丢最旧的批次，**绝不阻塞也绝不抛**——监控循环每秒都要
    走这条路，让它等数据库等于让限速判定跟着卡。"""
    st = store()
    base = M.bucket_start(time.time(), 60)
    # 灌远超队列容量的桶数
    for minute in range(M.MAX_PENDING_BATCHES + 50):
        for sec in (0, 1):
            st.record(base + minute * 60 + sec, [fu(rate=1_000)], None, {})
    assert st._pending.qsize() <= M.MAX_PENDING_BATCHES
    assert st._dropped > 0, "应记录被丢弃的批次数，便于运维知情"


# ---------------------------------------------------------------------------
# 查询：自动选层
# ---------------------------------------------------------------------------

def test_pick_tier_prefers_fine_grain_when_it_fits():
    now = int(time.time())
    assert M.pick_tier(now - 1800, now) == M.TIER_MINUTE, "半小时用 1 分钟粒度"
    assert M.pick_tier(now - 3600, now) == M.TIER_MINUTE


def test_pick_tier_falls_back_when_out_of_retention():
    """1 分钟那层只保留 7 天。查 30 天必须落到 5 分钟层，否则查到的是
    一段被剪枝剪掉的空白。"""
    now = int(time.time())
    assert M.pick_tier(now - 30 * 86400, now) == M.TIER_FIVE_MINUTE
    assert M.pick_tier(now - 90 * 86400, now) == M.TIER_FIVE_MINUTE


def test_pick_tier_falls_back_when_too_many_points():
    """即便还在保留期内，点数超上限也要降粒度——一次拉回十几万个点，
    浏览器和数据库一起遭殃。"""
    now = int(time.time())
    span = (M.MAX_QUERY_POINTS + 100) * M.TIER_MINUTE
    assert M.pick_tier(now - span, now) == M.TIER_FIVE_MINUTE
