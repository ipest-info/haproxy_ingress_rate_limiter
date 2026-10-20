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


# ---------------------------------------------------------------------------
# IpvsClient（假 runner + tmp 假 proc 文件）
# ---------------------------------------------------------------------------

import asyncio
import logging

from rl_limiter.collector import Collector

log = logging.getLogger("t.ipvs")


def fake_runner(out=IPVSADM_STATS, rc=0, err="", delay=0.0):
    async def _run(argv):
        assert argv[-3:] == ["-Ln", "--stats", "--exact"]
        if delay:
            await asyncio.sleep(delay)
        return rc, out, err
    return _run


def client(tmp_path, proc=PROC_IP_VS, stats=PROC_IP_VS_STATS, **kw):
    p1 = tmp_path / "ip_vs"
    p1.write_text(proc, encoding="utf-8")
    p2 = tmp_path / "ip_vs_stats"
    p2.write_text(stats, encoding="utf-8")
    kw.setdefault("runner", fake_runner())
    return ipvs.IpvsClient(proc_path=str(p1), stats_path=str(p2),
                           timeout_s=0.2, log=log, **kw)


async def test_client_show_stat_merges_proc_and_stats(tmp_path):
    c = client(tmp_path)
    rows = {r.name: r for r in await c.show_stat()}
    tcp = rows["vs_10.0.0.1_8080"]
    assert tcp.bytes_out == 9382011 and tcp.bytes_in == 2001203
    assert tcp.conn_tot == 1053
    assert tcp.conn_cur == 23                    # Active 5 + InAct 18
    assert (tcp.active_conns, tcp.idle_conns) == (5, 18)
    assert tcp.mode == "tcp"
    udp = rows["vs_10.0.0.1_53"]
    assert udp.bytes_out == 4000 and udp.mode == "udp"


async def test_client_show_info_reads_global_and_cached_conns(tmp_path):
    c = client(tmp_path)
    await c.show_stat()
    info = await c.show_info()
    assert info.cum_conns == 0x41C
    assert info.curr_conns == 23 + 4             # 两个服务的 Σ(Act+InAct)
    assert info.max_conn == 0 and info.idle_pct == 100


async def test_client_stats_mismatch_falls_back_to_zero(tmp_path):
    """两次读取之间规则变了（竞态）：stats 对不上的服务字节取 0，
    不抛——collector 按新基线/回绕处理。"""
    c = client(tmp_path, runner=fake_runner(
        out=IPVSADM_STATS.replace("10.0.0.1:8080", "10.9.9.9:8080")))
    rows = {r.name: r for r in await c.show_stat()}
    assert rows["vs_10.0.0.1_8080"].bytes_out == 0


async def test_client_ipvsadm_failure_and_timeout(tmp_path):
    with pytest.raises(ipvs.IpvsCommandError, match="rc=2"):
        await client(tmp_path, runner=fake_runner(rc=2, err="boom")).show_stat()
    with pytest.raises(ipvs.IpvsCommandError, match="超时"):
        await client(tmp_path, runner=fake_runner(delay=5)).show_stat()


async def test_client_missing_proc_raises_unavailable(tmp_path):
    c = ipvs.IpvsClient(proc_path=str(tmp_path / "nope"),
                        stats_path=str(tmp_path / "nope2"),
                        runner=fake_runner(), log=log)
    with pytest.raises(ipvs.IpvsUnavailableError, match="ip_vs"):
        await c.show_stat()


async def test_collector_reuse_end_to_end(tmp_path):
    """复用验证：IpvsClient 直接喂现有 Collector——两拍后产出速率差分，
    证明采集链（基线/差分/窗口）无需任何改动。"""
    p1 = tmp_path / "ip_vs"
    p1.write_text(PROC_IP_VS, encoding="utf-8")
    p2 = tmp_path / "ip_vs_stats"
    p2.write_text(PROC_IP_VS_STATS, encoding="utf-8")
    outs = [IPVSADM_STATS,
            IPVSADM_STATS.replace("9382011", "9882011")]   # +500000 B

    async def scripted(argv):
        return 0, outs.pop(0), ""

    c = Collector(ipvs.IpvsClient(proc_path=str(p1), stats_path=str(p2),
                                  runner=scripted, log=log), log)
    c.set_managed({"vs_10.0.0.1_8080", "vs_10.0.0.1_53"})
    await c.tick(0.0)
    usages = {u.name: u for u in await c.tick(1.0)}
    assert usages["vs_10.0.0.1_8080"].rate_bps == 500000.0


# ---------------------------------------------------------------------------
# 受管清单 loader 与同端口冲突裁决
# ---------------------------------------------------------------------------

def test_load_sections_shape(tmp_path):
    p = tmp_path / "ip_vs"
    p.write_text(PROC_IP_VS, encoding="utf-8")
    secs = ipvs.load_sections(str(p))
    assert [(s.name, s.kind, s.binds, s.mode) for s in secs] == [
        ("vs_10.0.0.1_8080", "virtual_server", [("10.0.0.1", 8080)], "tcp"),
        ("vs_10.0.0.1_53", "virtual_server", [("10.0.0.1", 53)], "udp")]


def test_load_sections_missing_proc(tmp_path):
    with pytest.raises(ipvs.IpvsUnavailableError, match="modprobe"):
        ipvs.load_sections(str(tmp_path / "nope"))


def test_load_frontends_merges_quota_and_resolves_conflicts(tmp_path):
    """两个 VIP 共用 8080：名序第一个保限额，其余归 0 只监控 + error
    一次（去重）。"""
    p = tmp_path / "ip_vs"
    p.write_text(
        "IP Virtual Server version 1.2.1 (size=4096)\n"
        "Prot LocalAddress:Port Scheduler Flags\n"
        "  -> RemoteAddress:Port Forward Weight ActiveConn InActConn\n"
        "TCP  0A000001:1F90 rr\n"          # 10.0.0.1:8080
        "TCP  0A000002:1F90 rr\n",         # 10.0.0.2:8080
        encoding="utf-8")
    quotas = {"vs_10.0.0.1_8080": 40.0, "vs_10.0.0.2_8080": 20.0}

    class CountLog:
        def __init__(self):
            self.errors = 0
        def error(self, *a, **k):
            self.errors += 1
        def warning(self, *a, **k):
            pass
        def info(self, *a, **k):
            pass

    clog = CountLog()
    warned: set = set()
    fes = {f.name: f for f in ipvs.load_frontends(
        quotas, clog, warned, proc_path=str(p))}
    assert fes["vs_10.0.0.1_8080"].quota_mbps == 40.0
    assert fes["vs_10.0.0.2_8080"].quota_mbps == 0.0      # 被裁为只监控
    assert clog.errors == 1
    # 再跑一轮（watch 场景）：去重后不再刷 error。
    ipvs.load_frontends(quotas, clog, warned, proc_path=str(p))
    assert clog.errors == 1
