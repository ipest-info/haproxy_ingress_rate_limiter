# tests.test_netdev —— 网卡计数器采样（/proc/net/dev）。
#
# 这个模块存在的唯一理由是"HAProxy 不统计数据包"，所以测试的重点是
# **口径正确**（收/发两组各 8 个字段的位置别串了，drop 别读成 errs）
# 与**失败不上升为事故**（读不到就返回空/抛出，由 collector 沿用旧值）。

from __future__ import annotations

import pytest

from rl_limiter import netdev

# 逐字节复刻真实 /proc/net/dev 的排版：两行表头、右对齐的网卡名、
# receive 8 列 + transmit 8 列。
PROC_NET_DEV = """\
Inter-|   Receive                                                |  Transmit
 face |bytes    packets errs drop fifo frame compressed multicast|bytes    packets errs drop fifo colls carrier compressed
    lo:  103005    1298    0    0    0     0          0         0   103005    1298    0    0    0     0       0          0
  eth0: 1373664    6485    1   19    0     0          0         0 53914459    6685    2    7    0     0       0          0
"""

PROC_NET_ROUTE = """\
Iface\tDestination\tGateway \tFlags\tRefCnt\tUse\tMetric\tMask\tMTU\tWindow\tIRTT
eth1\t0000A8C0\t00000000\t0001\t0\t0\t0\t00FFFFFF\t0\t0\t0
eth0\t00000000\t0100A8C0\t0003\t0\t0\t0\t00000000\t0\t0\t0
"""


def dev(tmp_path, text=PROC_NET_DEV):
    p = tmp_path / "dev"
    p.write_text(text, encoding="ascii")
    return str(p)


def route(tmp_path, text=PROC_NET_ROUTE):
    p = tmp_path / "route"
    p.write_text(text, encoding="ascii")
    return str(p)


def test_read_nic_stat_maps_columns(tmp_path):
    """收/发两组各 8 列，drop 在第 4 位、errs 在第 3 位——串了不会报错，
    只会让"丢包"曲线画的是别的东西，所以必须逐字段钉住。"""
    s = netdev.read_nic_stat("eth0", dev(tmp_path))
    assert s.iface == "eth0"
    assert (s.rx_bytes, s.rx_packets, s.rx_errs, s.rx_dropped) == (1373664, 6485, 1, 19)
    assert (s.tx_bytes, s.tx_packets, s.tx_errs, s.tx_dropped) == (53914459, 6685, 2, 7)


def test_read_nic_stat_handles_right_aligned_name(tmp_path):
    """网卡名右对齐补空格（lo 前面有 4 个空格），不 strip 就永远匹配不上。"""
    assert netdev.read_nic_stat("lo", dev(tmp_path)).rx_packets == 1298


def test_read_nic_stat_unknown_iface_raises(tmp_path):
    with pytest.raises(netdev.NetdevError, match="没有网卡"):
        netdev.read_nic_stat("eth9", dev(tmp_path))


def test_read_nic_stat_missing_file_raises(tmp_path):
    with pytest.raises(netdev.NetdevError):
        netdev.read_nic_stat("eth0", str(tmp_path / "nope"))


def test_read_nic_stat_short_row_raises(tmp_path):
    p = dev(tmp_path, "Inter-|\n face |\n  eth0: 1 2 3\n")
    with pytest.raises(netdev.NetdevError, match="字段数异常"):
        netdev.read_nic_stat("eth0", p)


def test_default_iface_prefers_default_route(tmp_path):
    """默认路由的出口网卡才是"对外那张"。注意路由表里 eth1 排在前面，
    按顺序取第一条会选错——必须按 Destination == 00000000 认。"""
    assert netdev.default_iface(route(tmp_path), dev(tmp_path)) == "eth0"


def test_default_iface_falls_back_to_first_non_lo(tmp_path):
    """没有默认路由（容器里常见）时退而求其次：第一张非 lo 的网卡。"""
    assert netdev.default_iface(str(tmp_path / "nope"), dev(tmp_path)) == "eth0"


def test_default_iface_returns_empty_when_nothing_available(tmp_path):
    """两条路都走不通就返回空串——包统计缺席，但监控绝不能因此停摆。"""
    assert netdev.default_iface(str(tmp_path / "a"), str(tmp_path / "b")) == ""


def test_resolve_iface_prefers_explicit_configuration():
    """多网卡机器上自动探测未必选对，运维显式指定必须优先。"""
    assert netdev.resolve_iface("eth7") == "eth7"
