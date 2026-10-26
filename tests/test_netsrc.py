# tests.test_netsrc —— 通用内核数据源（/proc/net/snmp 的 TCP 计数器）。

from __future__ import annotations

import pytest

from rl_limiter import collector, model, netsrc

pytestmark = pytest.mark.asyncio

SNMP = """\
Ip: Forwarding DefaultTTL InReceives
Ip: 1 64 100
Tcp: RtoAlgorithm RtoMin RtoMax MaxConn ActiveOpens PassiveOpens AttemptFails EstabResets CurrEstab InSegs OutSegs RetransSegs InErrs OutRsts InCsumErrors
Tcp: 1 200 120000 -1 777 1500 3 2 42 999 888 5 0 1 0
Udp: InDatagrams NoPorts
Udp: 10 0
"""


def test_parse_proc_snmp_tcp_by_header_position():
    """按表头对位解析，不假设字段顺序/数量（内核版本间会增列）。"""
    tcp = netsrc.parse_proc_snmp_tcp(SNMP)
    assert tcp["CurrEstab"] == 42
    assert tcp["PassiveOpens"] == 1500
    assert tcp["ActiveOpens"] == 777
    assert tcp["MaxConn"] == -1          # 负值原样保留（-1=无上限）


def test_parse_rejects_truncated_tcp_section():
    with pytest.raises(netsrc.NetSourceError, match="Tcp"):
        netsrc.parse_proc_snmp_tcp("Ip: A B\nIp: 1 2\n")


async def test_show_info_maps_kernel_counters(tmp_path):
    p = tmp_path / "snmp"
    p.write_text(SNMP, encoding="ascii")
    src = netsrc.NetSource(snmp_path=str(p))
    info = await src.show_info()
    assert info.curr_conns == 42          # CurrEstab → 并发连接
    assert info.cum_conns == 1500         # PassiveOpens → 新建连接累计
    assert info.cum_req == 777            # ActiveOpens（排障对照）
    assert info.idle_pct == 100 and info.max_conn == 0


async def test_show_stat_is_always_empty(tmp_path):
    """没有服务配合就没有按端口拆分的应用层观测：恒空表。"""
    assert await netsrc.NetSource(snmp_path=str(tmp_path / "x")).show_stat() == []


async def test_source_desc_duckface():
    d = netsrc.SourceDesc("edge-01")
    assert d.name == "edge-01" and not d.is_unix
    assert d.endpoint() == netsrc.PROC_NET_SNMP


async def test_missing_procfs_raises_loud(tmp_path):
    with pytest.raises(netsrc.NetSourceError, match="读取"):
        await netsrc.NetSource(snmp_path=str(tmp_path / "nope")).show_info()


# ---------------------------------------------------------------------------
# 与 collector 的集成：空 stats + 内核累计量 → 实例每秒新建连接
# ---------------------------------------------------------------------------

def snmp_write(tmp_path, passive, estab=42):
    p = tmp_path / "snmp"
    p.write_text(
        "Tcp: PassiveOpens ActiveOpens CurrEstab\n"
        f"Tcp: {passive} 0 {estab}\n", encoding="ascii")
    return p


async def test_collector_instance_conn_new_from_kernel(tmp_path):
    """show_stat 恒空的源：实例每秒新建连接改用 show_info 的累计量差分
    （netsrc 把 PassiveOpens 装在 cum_conns 里），整机口径。"""
    p = snmp_write(tmp_path, 1000)
    c = collector.Collector(netsrc.NetSource(snmp_path=str(p)))
    await c.tick(0.0)
    snmp_write(tmp_path, 1030, estab=55)
    await c.tick(1.0)
    inst = c.instance
    assert inst.conn_new_ps == 30.0, "PassiveOpens 差分 = 每秒 accept 数"
    assert inst.conn_cur == 55
    assert inst.degraded is False
