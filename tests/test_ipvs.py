# tests.test_ipvs —— LVS（IPVS）数据源：解析、采样客户端、受管清单。
#
# 全部纯单元：proc 文件用逐字节复刻排版的字符串（照 test_netdev 的
# 样式），ipvsadm 用可注入的假 runner，不碰真实内核。

from __future__ import annotations

import pytest

from rl_limiter import ipvs, model

# 逐字节复刻 /proc/net/ip_vs 的排版（地址/端口是十六进制；RS 行的
# ActiveConn/InActConn 要按服务求和）。
PROC_IP_VS = """\
IP Virtual Server version 1.2.1 (size=4096)
Prot LocalAddress:Port Scheduler Flags
  -> RemoteAddress:Port Forward Weight ActiveConn InActConn
TCP  0A000001:1F90 rr
  -> 0AD3020A:2328      Masq    1      3          11
  -> 0AD3020B:2328      Masq    1      2          7
UDP  0A000001:0035 rr
  -> 0AD3020A:0035      Masq    1      0          4
FWM  00000001 wlc
  -> 0AD3020A:0000      Masq    1      9          9
"""

IPVSADM_STATS = """\
IP Virtual Server version 1.2.1 (size=4096)
Prot LocalAddress:Port               Conns   InPkts  OutPkts  InBytes OutBytes
  -> RemoteAddress:Port
TCP  10.0.0.1:8080                    1053    24923    18290  2001203  9382011
  -> 10.211.2.10:9000                  527    12461     9145  1000601  4691005
UDP  10.0.0.1:53                        10       20       10     2000     4000
"""

# /proc/net/ip_vs_stats：**十六进制**。0x41C=1052。
PROC_IP_VS_STATS = """\
   Total Incoming Outgoing         Incoming         Outgoing
   Conns  Packets  Packets            Bytes            Bytes
     41C      9C5      82D           5C21BA           F23C1E

 Conns/s   Pkts/s   Pkts/s          Bytes/s          Bytes/s
       3       28       19             14A3             38F6
"""


# ---------------------------------------------------------------------------
# 十六进制地址解码
# ---------------------------------------------------------------------------

def test_decode_hex_endpoint():
    assert ipvs.decode_hex_endpoint("0A000001:0050") == ("10.0.0.1", 80)
    assert ipvs.decode_hex_endpoint("FFFFFFFF:FFFF") == (
        "255.255.255.255", 65535)
    assert ipvs.decode_hex_endpoint("00000000:0000") == ("0.0.0.0", 0)


def test_decode_hex_endpoint_rejects_ipv6_and_garbage():
    for bad in ("fe80::1:0050", "[::1]:0050", "0A00:0001:0050",
                "ZZZZZZZZ:0050", "0A01:0050"):
        with pytest.raises(ipvs.IpvsParseError):
            ipvs.decode_hex_endpoint(bad)


# ---------------------------------------------------------------------------
# /proc/net/ip_vs
# ---------------------------------------------------------------------------

def test_parse_proc_ip_vs_services_and_rs_sums():
    svcs = ipvs.parse_proc_ip_vs(PROC_IP_VS)
    assert [(s.proto, s.vip, s.port) for s in svcs] == [
        ("tcp", "10.0.0.1", 8080), ("udp", "10.0.0.1", 53)]
    tcp = svcs[0]
    assert tcp.scheduler == "rr"
    assert (tcp.active_conn, tcp.inact_conn) == (5, 18)   # 两个 RS 求和
    assert svcs[0].name == "vs_10.0.0.1_8080"
    # FWM 服务与它的 RS 行整体跳过（没把 9/9 记到任何服务头上）。
    assert svcs[1].active_conn == 0 and svcs[1].inact_conn == 4


def test_parse_proc_ip_vs_empty_table_is_ok():
    svcs = ipvs.parse_proc_ip_vs(
        "IP Virtual Server version 1.2.1 (size=4096)\n"
        "Prot LocalAddress:Port Scheduler Flags\n"
        "  -> RemoteAddress:Port Forward Weight ActiveConn InActConn\n")
    assert svcs == []


def test_parse_proc_ip_vs_rejects_garbage():
    with pytest.raises(ipvs.IpvsParseError):
        ipvs.parse_proc_ip_vs("this is not the file you are looking for")


# ---------------------------------------------------------------------------
# ipvsadm --stats --exact
# ---------------------------------------------------------------------------

def test_parse_ipvsadm_stats():
    stats = ipvs.parse_ipvsadm_stats(IPVSADM_STATS)
    assert stats[("tcp", "10.0.0.1", 8080)] == (1053, 2001203, 9382011)
    assert stats[("udp", "10.0.0.1", 53)] == (10, 2000, 4000)
    assert len(stats) == 2                      # RS 行（->）没被当成服务


def test_parse_ipvsadm_stats_rejects_missing_header():
    with pytest.raises(ipvs.IpvsParseError):
        ipvs.parse_ipvsadm_stats("IP Virtual Server version 1.2.1\n")


def test_parse_ipvsadm_stats_rejects_abbreviated_numbers():
    """漏了 --exact 时大计数被缩写（101M）——必须整体失败并点出原因。"""
    bad = IPVSADM_STATS.replace("9382011", "9382K")
    with pytest.raises(ipvs.IpvsParseError) as ei:
        ipvs.parse_ipvsadm_stats(bad)
    assert "--exact" in str(ei.value)


# ---------------------------------------------------------------------------
# /proc/net/ip_vs_stats
# ---------------------------------------------------------------------------

def test_parse_proc_ip_vs_stats_hex():
    inst = ipvs.parse_proc_ip_vs_stats(PROC_IP_VS_STATS)
    assert inst.cum_conns == 0x41C
    assert inst.idle_pct == 100 and inst.max_conn == 0


def test_parse_proc_ip_vs_stats_rejects_garbage():
    with pytest.raises(ipvs.IpvsParseError):
        ipvs.parse_proc_ip_vs_stats("nope\n")
