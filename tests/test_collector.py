# tests.test_collector —— 采样层的时序语义测试（单 HAProxy，单位 = frontend）。
#
# 重点全在"时序规则"上，因为这几条错了不会报错、只会让曲线与告警悄悄失真：
#   1. 首次采样只建基线（速率未知，不喂窗口）；
#   2. 计数回绕（HAProxy reload）沿用上一秒速率并重建基线；
#   3. 采样失败 fail-static（沿用上一秒），连续失败进 degraded；
#   4. 受管但 stats 里不存在的 frontend 是真实的零，必须进窗口。

from __future__ import annotations

import pytest

from rl_limiter import model
from rl_limiter.collector import DEGRADED_FAILURE_THRESHOLD, Collector


class FakeClient:
    """假 RuntimeClient：按脚本逐次返回 stats 或抛异常。

    show_info 走独立的 info_script（默认每拍返回一份固定的 InstanceStat）
    ——这正是被测的分链路容错：show info 挂掉不该影响 frontend 采样。
    """

    def __init__(self, script, info_script=None):
        self._script = list(script)
        self._info_script = list(info_script or [])
        self.calls = 0
        self.info_calls = 0

    async def show_stat(self):
        self.calls += 1
        item = self._script.pop(0) if self._script else []
        if isinstance(item, BaseException):
            raise item
        return item

    async def show_info(self):
        self.info_calls += 1
        item = self._info_script.pop(0) if self._info_script else \
            model.InstanceStat(curr_conns=0)
        if isinstance(item, BaseException):
            raise item
        return item


def st(name, bytes_out, conn=0, **kw):
    return model.FrontendStat(name=name, bytes_out=bytes_out, conn_cur=conn, **kw)


def collector(script, managed=("fe_a",), info_script=None, nic=""):
    c = Collector(FakeClient(script, info_script), nic=nic)
    c.set_managed(set(managed))
    return c


async def test_first_tick_only_baselines():
    """首次采样只建基线：速率未知，既不产出速率也不喂窗口。"""
    c = collector([[st("fe_a", 1000, 3)]])
    (u,) = await c.tick(0.0)
    assert u.name == "fe_a"
    assert u.rate_bps == 0.0
    assert u.mean10_bps == 0.0      # 未喂窗口，均值仍是空窗口的 0
    assert u.conn_cur == 3          # 连接数是瞬时值，不依赖差分


async def test_rate_is_difference_of_counters():
    """第二拍起产出速率 = 相邻两次 bytes_out 之差（1 拍 = 1 秒）。"""
    c = collector([[st("fe_a", 1000)], [st("fe_a", 3500)], [st("fe_a", 4500)]])
    await c.tick(0.0)
    (u2,) = await c.tick(1.0)
    assert u2.rate_bps == 2500
    assert u2.mean10_bps == 2500     # 窗口里只有这一个样本
    (u3,) = await c.tick(2.0)
    assert u3.rate_bps == 1000
    assert u3.mean10_bps == pytest.approx((2500 + 1000) / 2)


async def test_counter_reset_holds_previous_rate():
    """计数回绕（reload 后从零重来）：差分为负不可用，沿用上一秒速率
    并用新值重建基线，下一秒差分即恢复正常。"""
    c = collector([
        [st("fe_a", 1000)], [st("fe_a", 3000)],   # 建基线 → 速率 2000
        [st("fe_a", 50)],                          # 回绕 → 沿用 2000
        [st("fe_a", 1050)],                        # 从新基线差分 → 1000
    ])
    await c.tick(0.0)
    await c.tick(1.0)
    (u3,) = await c.tick(2.0)
    assert u3.rate_bps == 2000, "回绕当秒应沿用上一秒速率"
    (u4,) = await c.tick(3.0)
    assert u4.rate_bps == 1000, "下一秒应从新基线正常差分"


async def test_sampling_failure_is_fail_static():
    """采样失败沿用上一秒速率继续推进窗口——骤降为零会让均值失真、
    进而触发假的"已恢复正常"。"""
    c = collector([
        [st("fe_a", 0)], [st("fe_a", 2000)],       # 速率 2000
        ConnectionError("socket 没了"),
    ])
    await c.tick(0.0)
    await c.tick(1.0)
    (u,) = await c.tick(2.0)
    assert u.rate_bps == 2000, "失败当秒应沿用上一秒速率"
    assert u.degraded is False, "单次失败还不到降级阈值"


async def test_degraded_after_consecutive_failures_and_recovers():
    """连续失败达阈值进入 degraded；任何一次成功立即解除（不粘滞）。"""
    script = [[st("fe_a", 0)], [st("fe_a", 1000)]]
    script += [ConnectionError("down")] * DEGRADED_FAILURE_THRESHOLD
    script += [[st("fe_a", 2000)]]
    c = collector(script)
    await c.tick(0.0)
    await c.tick(1.0)
    for i in range(DEGRADED_FAILURE_THRESHOLD):
        (u,) = await c.tick(2.0 + i)
    assert u.degraded is True and c.degraded is True
    (u,) = await c.tick(100.0)
    assert u.degraded is False and c.degraded is False


async def test_managed_but_missing_frontend_counts_as_zero():
    """受管却不在 stats 里的 frontend 是真实的零（cfg 还没下发就是这种
    情况）：零值必须进窗口，否则均值会停在旧值上虚高。"""
    c = collector([[st("fe_a", 0)], [st("fe_a", 8000)], []], managed=("fe_a",))
    await c.tick(0.0)
    (u2,) = await c.tick(1.0)
    assert u2.rate_bps == 8000
    (u3,) = await c.tick(2.0)
    assert u3.rate_bps == 0.0
    assert u3.mean10_bps == pytest.approx(8000 / 2), "零值应进入窗口拉低均值"


async def test_unmanaged_frontend_is_ignored():
    """未纳管的 frontend 不产出用量。"""
    c = collector([[st("fe_a", 0), st("other", 500)]], managed=("fe_a",))
    usages = await c.tick(0.0)
    assert [u.name for u in usages] == ["fe_a"]


async def test_newly_managed_frontend_has_continuous_rate():
    """先前已见过（基线在刷）的 frontend 被纳管后，第一秒就有正确速率，
    不必重走"首采样丢一秒"。"""
    client = FakeClient([[st("fe_x", 1000)], [st("fe_x", 3000)]])
    c = Collector(client)
    c.set_managed({"fe_x"})
    await c.tick(0.0)
    (u,) = await c.tick(1.0)
    assert u.rate_bps == 2000


async def test_output_is_sorted_and_stable():
    """输出按名字排序且集合稳定（下游不必处理"单元忽隐忽现"）。"""
    c = collector([[st("fe_b", 0), st("fe_a", 0)]], managed=("fe_a", "fe_b"))
    usages = await c.tick(0.0)
    assert [u.name for u in usages] == ["fe_a", "fe_b"]


# ---------------------------------------------------------------------------
# 监控视图字段（docs/05-监控视图.md）
#
# 这一组的主张：新加的监控字段与限速链路**共享时序规则**（首拍建基线、
# 回绕沿用、fail-static、缺席归零），但**不共享失败**——show info 或网卡
# 采不到时，限速那条链路必须毫发无损。
# ---------------------------------------------------------------------------

async def test_frontend_monitoring_fields_are_differences():
    """上行字节、新建连接数、拒绝数都是相邻两拍的差分，与下行同一时序。"""
    c = collector([
        [st("fe_a", 0, bytes_in=0, conn_tot=0, denied_conn=0)],
        [st("fe_a", 100, bytes_in=500, conn_tot=12, denied_conn=1, denied_req=2)],
    ])
    await c.tick(0.0)
    (u,) = await c.tick(1.0)
    assert u.rate_in_bps == 500
    assert u.conn_new_ps == 12
    assert u.conn_denied_ps == 3          # dcon + dreq


async def test_frontend_monitoring_fields_ready_on_second_tick():
    """首拍建基线的那一拍也把监控字段的基线一起建了——否则监控曲线会
    比限速曲线晚一秒起步，看起来像是数据对不齐。"""
    c = collector([
        [st("fe_a", 0, conn_tot=100)],
        [st("fe_a", 10, conn_tot=105)],
    ])
    await c.tick(0.0)
    (u,) = await c.tick(1.0)
    assert u.conn_new_ps == 5, "第二拍就该有速率，而不是又丢一拍"


async def test_active_idle_conns_passed_through():
    """活跃/空闲是瞬时值，不差分；http 按在途流拆，tcp 全算活跃。"""
    c = collector(
        [[st("fe_a", 0, conn=8, mode="http", open_conns=8, open_streams=3),
          st("fe_b", 0, conn=5, mode="tcp")]],
        managed=("fe_a", "fe_b"))
    a, b = await c.tick(0.0)
    assert (a.active_conns, a.idle_conns) == (3, 5)
    assert (b.active_conns, b.idle_conns) == (5, 0)


async def test_monitoring_fields_are_fail_static():
    """采样失败时监控字段与速率一样沿用上一拍——骤降为零会在图上画出
    一个并不存在的低谷。"""
    c = collector([
        [st("fe_a", 0, conn_tot=0)],
        [st("fe_a", 10, conn_tot=7)],
        ConnectionError("socket 没了"),
    ])
    await c.tick(0.0)
    await c.tick(1.0)
    (u,) = await c.tick(2.0)
    assert u.conn_new_ps == 7


async def test_monitoring_fields_zero_when_frontend_absent():
    """受管却不在 stats 里 = 确实没有，监控字段必须归零（区别于采不到）。"""
    c = collector([
        [st("fe_a", 0, conn_tot=0)],
        [st("fe_a", 10, conn_tot=7)],
        [],
    ])
    await c.tick(0.0)
    await c.tick(1.0)
    (u,) = await c.tick(2.0)
    assert u.conn_new_ps == 0.0 and u.active_conns == 0


# ---- 实例视图 ----

async def test_instance_view_aggregates_all_frontends():
    """实例带宽/拒绝数汇总**全部** frontend，包括不受管的那些——
    "整个实例的视图"就该是整个实例。"""
    c = collector([
        [st("fe_a", 0, bytes_in=0, denied_conn=0),
         st("unmanaged", 0, bytes_in=0, denied_req=0)],
        [st("fe_a", 100, bytes_in=10, denied_conn=1),
         st("unmanaged", 900, bytes_in=90, denied_req=4)],
    ], managed=("fe_a",))
    await c.tick(0.0)
    await c.tick(1.0)
    inst = c.instance
    assert inst.rate_out_bps == 1000    # 100 + 900
    assert inst.rate_in_bps == 100      # 10 + 90
    assert inst.conn_denied_ps == 5     # 1 + 4


async def test_instance_concurrent_conns_come_from_show_info():
    """并发连接数只能取 show info：show stat 的行按 proxy 拆，加总会把
    同一条连接重复计。"""
    c = collector(
        [[st("fe_a", 0)], [st("fe_a", 0)]],
        info_script=[
            model.InstanceStat(curr_conns=17, max_conn=400, idle_pct=90),
            model.InstanceStat(curr_conns=19, max_conn=400, idle_pct=88),
        ])
    await c.tick(0.0)
    await c.tick(1.0)
    assert c.instance.conn_cur == 19
    assert c.instance.max_conn == 400
    assert c.instance.idle_pct == 88


async def test_instance_new_conns_exclude_our_own_runtime_api_traffic():
    """实例的"每秒新建连接数"必须取 Σ frontend conn_tot，而不是 show info
    的 CumConns。

    CumConns 是进程范围的，把采集器自己每秒两条 runtime API 连接
    （show stat + show info）也算了进去——用它做差分，空载时曲线会稳稳
    停在 2/秒。实测佐证：同一时刻 CumConns=256，各 frontend 的 conn_tot
    之和只有 28，差额全是采集器自己。

    这里用"CumConns 每拍 +5、frontend conn_tot 每拍 +3"的脚本把两个来源
    分开：结果必须是 3。
    """
    c = collector(
        [[st("fe_a", 0, conn_tot=0)], [st("fe_a", 0, conn_tot=3)]],
        info_script=[model.InstanceStat(curr_conns=1, cum_conns=100),
                     model.InstanceStat(curr_conns=1, cum_conns=105)])
    await c.tick(0.0)
    await c.tick(1.0)
    assert c.instance.conn_new_ps == 3


async def test_show_info_failure_does_not_break_frontend_sampling():
    """副链路失败必须被隔离：show info 每拍都抛，frontend 的速率照常产出。"""
    c = collector(
        [[st("fe_a", 0)], [st("fe_a", 2500)]],
        info_script=[ConnectionError("info 挂了")] * 2)
    await c.tick(0.0)
    (u,) = await c.tick(1.0)
    assert u.rate_bps == 2500, "show info 挂掉不该动到限速链路"
    assert u.degraded is False


async def test_show_info_failure_holds_previous_instance_values():
    c = collector(
        [[st("fe_a", 0)], [st("fe_a", 0)]],
        info_script=[model.InstanceStat(curr_conns=17),
                     ConnectionError("info 挂了")])
    await c.tick(0.0)
    await c.tick(1.0)
    assert c.instance.conn_cur == 17, "采不到时沿用上一拍，而不是掉到 0"


async def test_stat_failure_holds_instance_bandwidth_but_still_reads_nic(
        monkeypatch):
    """show stat 挂了：HAProxy 口径的实例带宽沿用旧值，网卡照常采——
    "HAProxy 挂了但机器还在收包"正是要靠这个看出来的。"""
    pkts = iter([100, 250, 400])

    def fake_read(iface, *_a, **_kw):
        return model.NicStat(iface=iface, rx_packets=next(pkts))

    monkeypatch.setattr("rl_limiter.collector.netdev.read_nic_stat", fake_read)
    c = collector([
        [st("fe_a", 0, bytes_in=0)],
        [st("fe_a", 800, bytes_in=200)],
        ConnectionError("stat 挂了"),
    ], nic="eth0")
    await c.tick(0.0)
    await c.tick(1.0)
    assert c.instance.rate_out_bps == 800
    await c.tick(2.0)
    assert c.instance.rate_out_bps == 800, "HAProxy 口径沿用上一拍"
    assert c.instance.pkts_in_ps == 150, "网卡与 HAProxy 无关，照常采"


async def test_nic_counters_are_differenced(monkeypatch):
    script = [
        model.NicStat(iface="eth0", rx_bytes=0, rx_packets=0, rx_dropped=0,
                      tx_bytes=0, tx_packets=0, tx_dropped=0),
        model.NicStat(iface="eth0", rx_bytes=5000, rx_packets=40, rx_dropped=2,
                      tx_bytes=9000, tx_packets=60, tx_dropped=1),
    ]
    it = iter(script)
    monkeypatch.setattr("rl_limiter.collector.netdev.read_nic_stat",
                        lambda *_a, **_kw: next(it))
    c = collector([[st("fe_a", 0)], [st("fe_a", 0)]], nic="eth0")
    await c.tick(0.0)
    await c.tick(1.0)
    inst = c.instance
    assert (inst.pkts_in_ps, inst.pkts_out_ps) == (40, 60)
    assert (inst.drop_in_ps, inst.drop_out_ps) == (2, 1)
    assert (inst.nic_rate_in_bps, inst.nic_rate_out_bps) == (5000, 9000)
    assert inst.nic == "eth0"


async def test_nic_disabled_leaves_packet_curves_empty():
    """未配置网卡（探测失败或显式禁用）：包统计缺席，其余照常。"""
    c = collector([[st("fe_a", 0)], [st("fe_a", 500)]], nic="")
    await c.tick(0.0)
    (u,) = await c.tick(1.0)
    assert u.rate_bps == 500
    assert c.instance.nic == "" and c.instance.pkts_in_ps == 0.0


async def test_nic_read_failure_is_logged_once(caplog):
    """网卡读不到每秒都会重试，不去重会把日志刷没。"""
    import logging

    c = Collector(FakeClient([[st("fe_a", 0)]] * 5),
                  logging.getLogger("t.nic"), nic="does-not-exist-0")
    c.set_managed({"fe_a"})
    with caplog.at_level(logging.WARNING, logger="t.nic"):
        for i in range(5):
            await c.tick(float(i))
    hits = [r for r in caplog.records if "网卡计数器读取失败" in r.getMessage()]
    assert len(hits) == 1, f"同一错误只该记一条，实得 {len(hits)}"
