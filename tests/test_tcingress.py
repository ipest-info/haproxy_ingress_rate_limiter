# tests.test_tcingress —— 请求方向（入向）限速：双网卡形态的 tc police。
#
# 与 test_tcshaper 同一套测法（假 runner 记录 argv，不依赖内核模块），
# 重点钉住：
#   1. 规则合并口径（frontends × request_limits，未匹配名告警）；
#   2. 命令序列：ingress qdisc 非致命、按 prio 整段清、u32 dport police
#      字节速率（IPv4+IPv6 都在）、flower SYN pkts_rate **非致命**；
#   3. 幂等：实况一致不发命令；速率漂移/只改 conn_rate 都触发重建；
#   4. 空清单 = 撤掉 ingress qdisc（有东西才动手）。

from __future__ import annotations

import logging

import pytest

from rl_limiter import model
from rl_limiter import tcingress as TI

IFACE = "eth-in"
log = logging.getLogger("t.tcin")


def fe(name="fe_main", port=8080, quota=0.0):
    return model.FrontendConfig(name=name, bind_port=port, quota_mbps=quota)


def rl(mbps=10.0, conn_rate=0):
    return model.RequestLimit(mbps=mbps, conn_rate=conn_rate)


class FakeTc:
    """假 tc：记录 argv，filter show 返回预置文本。"""

    def __init__(self, filters="", fail_on=None):
        self.calls: list[list[str]] = []
        self._filters = filters
        self._fail_on = fail_on or ()

    async def __call__(self, argv):
        self.calls.append(list(argv))
        joined = " ".join(argv)
        for bad in self._fail_on:
            if bad in joined:
                return 1, "", f"fake failure: {bad}"
        # 同样不看固定位置：`tc filter show` 和 `tc -s filter show`
        # 都该吐预置的过滤器文本。
        if "filter" in argv and "show" in argv:
            return 0, self._filters, ""
        return 0, "", ""

    def mutations(self):
        # 按"是否含 show"过滤而不是看固定位置——`tc -s filter show`
        # 的 -s 会把动词挤到别的下标去。
        return [c for c in self.calls if "show" not in c]


def shaper(**kw):
    f = FakeTc(**kw)
    return TI.TcIngress(IFACE, log, runner=f), f


# 与 _rebuild_cmds([8080@10Mbps]) 一致的实况（iproute2 打印口径：dport
# 是 20/40 偏移处 32 位字的低 16 位；0x1f90 = 8080）。
FILTERS_OK = """\
filter protocol ip pref 1 u32 chain 0
filter protocol ip pref 1 u32 chain 0 fh 800: ht divisor 1
filter protocol ip pref 1 u32 chain 0 fh 800::800 order 2048 key ht 800 bkt 0 terminal flowid not_in_hw
  match 00001f90/0000ffff at 20
\tpolice 0x1 rate 10Mbit burst 12500b mtu 2Kb action drop overhead 0b
\tref 1 bind 1
filter protocol ipv6 pref 1 u32 chain 0
filter protocol ipv6 pref 1 u32 chain 0 fh 801::800 order 2048 key ht 801 bkt 0 terminal flowid not_in_hw
  match 00001f90/0000ffff at 40
\tpolice 0x2 rate 10Mbit burst 12500b mtu 2Kb action drop overhead 0b
"""


# ---------------------------------------------------------------------------
# 规则合并与解析
# ---------------------------------------------------------------------------

def test_desired_rules_join_and_unknown():
    fes = [fe("fe_main", 8080), fe("fe_free", 9090)]
    limits = {"fe_main": rl(10.0, 200), "fe_typo": rl(5.0)}
    rules, unknown = TI.desired_rules(fes, limits)
    assert [(r.name, r.port, r.rate_bits, r.conn_rate) for r in rules] == [
        ("fe_main", 8080, 10_000_000, 200)]
    assert unknown == ["fe_typo"]


def test_desired_rules_skips_all_zero_entry():
    """mbps 与 conn_rate 都为 0 的登记不生成规则（等价于没登记）。"""
    rules, unknown = TI.desired_rules([fe()], {"fe_main": rl(0.0, 0)})
    assert rules == [] and unknown == []


def test_parse_ingress_filters():
    assert TI.parse_ingress_filters(FILTERS_OK) == {8080: 10_000_000}
    assert TI.parse_ingress_filters("") == {}


def test_parse_ignores_other_prios():
    """pref 2（我们的 SYN 过滤器）与别人的 pref 的块不参与字节速率比对。"""
    out = FILTERS_OK + """\
filter protocol ip pref 2 flower chain 0
filter protocol ip pref 2 flower chain 0 handle 0x1
  dst_port 8080
\tpolice 0x3 rate 0bit burst 0b mtu 2Kb action drop overhead 0b
filter protocol ip pref 7 u32 chain 0 fh 802::800 terminal flowid not_in_hw
  match 00000016/0000ffff at 20
\tpolice 0x9 rate 1Mbit burst 1600b mtu 2Kb action drop overhead 0b
"""
    assert TI.parse_ingress_filters(out) == {8080: 10_000_000}


# ---------------------------------------------------------------------------
# 命令序列
# ---------------------------------------------------------------------------

async def test_rebuild_from_scratch_builds_both_families():
    sh, fake = shaper()
    res = await sh.reconcile([fe(port=8080)], {"fe_main": rl(10.0, 200)})
    assert res.ok and res.changed and res.action == "rebuild"
    cmds = [" ".join(c) for c in fake.mutations()]
    # ingress qdisc 先确保在（已存在时报错无妨——非致命）。
    assert cmds[0] == f"tc qdisc add dev {IFACE} ingress"
    # 两个 prio 整段清（首次没有也无妨）。
    assert f"tc filter del dev {IFACE} parent ffff: prio 1" in cmds
    assert f"tc filter del dev {IFACE} parent ffff: prio 2" in cmds
    # 字节速率：IPv4 + IPv6 的 u32 dport police，rate 与 burst 都显式给。
    assert any("protocol ip prio 1 u32 match ip dport 8080 0xffff police "
               "rate 10000000bit burst 12500b drop" in c for c in cmds), cmds
    assert any("protocol ipv6 prio 1 u32 match ip6 dport 8080 0xffff" in c
               for c in cmds), cmds
    # SYN 速率：flower tcp_flags SYN/非ACK + pkts_rate（IPv4 + IPv6）。
    syn = [c for c in cmds if "flower" in c]
    assert len(syn) == 2
    assert all("dst_port 8080 tcp_flags 0x02/0x12 action police "
               "pkts_rate 200 pkts_burst 200 conform-exceed drop" in c
               for c in syn), syn


async def test_syn_filter_failure_is_not_fatal():
    """老内核缺 flower/pkts_rate：SYN 过滤器加不上只丢连接速率限制，
    字节限速照常生效，整体结果仍是成功。"""
    sh, fake = shaper(fail_on=("flower",))
    res = await sh.reconcile([fe(port=8080)], {"fe_main": rl(10.0, 200)})
    assert res.ok and res.changed


async def test_rate_filter_failure_is_fatal():
    sh, fake = shaper(fail_on=("u32",))
    res = await sh.reconcile([fe(port=8080)], {"fe_main": rl(10.0)})
    assert not res.ok and res.error


async def test_conn_rate_only_builds_no_u32():
    """只限连接速率（mbps=0）：没有 u32 字节过滤器，只有 SYN 过滤器。"""
    sh, fake = shaper()
    res = await sh.reconcile([fe(port=8080)], {"fe_main": rl(0.0, 300)})
    assert res.ok and res.changed
    cmds = [" ".join(c) for c in fake.mutations()]
    assert not any("u32" in c for c in cmds)
    assert any("pkts_rate 300" in c for c in cmds)


# ---------------------------------------------------------------------------
# 幂等与漂移
# ---------------------------------------------------------------------------

async def test_consistent_state_is_noop():
    sh, fake = shaper(filters=FILTERS_OK)
    limits = {"fe_main": rl(10.0)}
    res = await sh.reconcile([fe(port=8080)], limits)     # 首轮：记指纹
    assert res.changed
    fake.calls.clear()
    res = await sh.reconcile([fe(port=8080)], limits)     # 次轮：全一致
    assert res.ok and not res.changed
    assert fake.mutations() == []


async def test_rate_drift_triggers_rebuild():
    sh, fake = shaper(filters=FILTERS_OK)
    await sh.reconcile([fe(port=8080)], {"fe_main": rl(10.0)})
    fake.calls.clear()
    res = await sh.reconcile([fe(port=8080)], {"fe_main": rl(20.0)})
    assert res.changed and res.action == "rebuild"


async def test_conn_rate_change_triggers_rebuild():
    """conn_rate 不在网卡实况比对里（SYN 过滤器是非致命特性），但只改
    它也必须生效——靠配置指纹检测触发重建。"""
    sh, fake = shaper(filters=FILTERS_OK)
    await sh.reconcile([fe(port=8080)], {"fe_main": rl(10.0, 100)})
    fake.calls.clear()
    res = await sh.reconcile([fe(port=8080)], {"fe_main": rl(10.0, 500)})
    assert res.changed and res.action == "rebuild"
    assert any("pkts_rate 500" in " ".join(c) for c in fake.mutations())


async def test_manual_tamper_rebuilt_by_periodic_pass():
    """有人手工清了入向过滤器：实况与期望不符，周期兜底轮重建。"""
    sh, fake = shaper(filters=FILTERS_OK)
    await sh.reconcile([fe(port=8080)], {"fe_main": rl(10.0)})
    fake._filters = ""                       # 被手工清掉
    fake.calls.clear()
    res = await sh.reconcile([fe(port=8080)], {"fe_main": rl(10.0)})
    assert res.changed and res.action == "rebuild"


# ---------------------------------------------------------------------------
# 空清单与边界
# ---------------------------------------------------------------------------

async def test_empty_limits_with_nothing_observed_is_noop():
    sh, fake = shaper()
    res = await sh.reconcile([fe()], {})
    assert res.ok and not res.changed
    assert fake.mutations() == []


async def test_empty_limits_tears_down_ingress():
    sh, fake = shaper(filters=FILTERS_OK)
    res = await sh.reconcile([fe()], {})
    assert res.ok and res.changed and res.action == "teardown"
    assert [" ".join(c) for c in fake.mutations()] == [
        f"tc qdisc del dev {IFACE} ingress"]


async def test_duplicate_port_rejected():
    sh, fake = shaper()
    res = await sh.reconcile(
        [fe("a", 8080), fe("b", 8080)],
        {"a": rl(10.0), "b": rl(5.0)})
    assert not res.ok and "8080" in res.error
    assert fake.mutations() == []


async def test_unknown_names_warned_once(caplog):
    sh, fake = shaper()
    with caplog.at_level(logging.WARNING, logger="t.tcin"):
        await sh.reconcile([fe()], {"fe_typo": rl(10.0)})
        await sh.reconcile([fe()], {"fe_typo": rl(10.0)})
    hits = [r for r in caplog.records if "fe_typo" in r.getMessage()]
    assert len(hits) == 1


# ---------------------------------------------------------------------------
# debug_state：控制台 tc 调试面板的入向 section
# ---------------------------------------------------------------------------

async def test_debug_state_is_readonly_and_labeled():
    """两段原始输出（ingress qdisc + 带统计的 police 过滤器），每段带
    完整命令做标签；全程只读——调试面板绝不能碰内核状态。"""
    sh, fake = shaper(filters="filter protocol ip pref 1 u32 ... police")
    st = await sh.debug_state()
    assert st["iface"] == IFACE
    assert [r["label"] for r in st["raw"]] == [
        f"tc qdisc show dev {IFACE} ingress",
        f"tc -s filter show dev {IFACE} parent ffff:"]
    assert fake.mutations() == []
    assert "police" in st["raw"][1]["text"]
    assert st["raw"][0]["text"] == "(空)"
    # 随附解释必须在：help 非空、每段带 note——面板是给不熟 tc 的人用的。
    assert st["help"]
    assert all(r["note"] for r in st["raw"])


async def test_debug_state_renders_failures_inline():
    sh, fake = shaper(fail_on=("qdisc show",))
    st = await sh.debug_state()
    assert "[命令失败 rc=1]" in st["raw"][0]["text"]
    assert "[命令失败" not in st["raw"][1]["text"]
