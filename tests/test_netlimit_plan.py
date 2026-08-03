# tests.test_netlimit_plan —— 命令生成与收敛。
#
# 这里生成的每一条命令都会以 root 身份打在生产机的数据面上，所以测试的
# 重点不是"命令拼对了没有"，而是每条边界都不留烂摊子：
#
#   1. 计划不成立 → 一条命令都不发（不是发一半再报错）；
#   2. 已经一致 → 一条命令都不发（否则周期性运行会不停重建、不停抖流量）；
#   3. 只有额度变了 → class change，绝不重建（重建期间有一段不整形的窗口）；
#   4. 多网卡 → 必须是"一份令牌桶"，不能变成每张网卡一份；
#   5. 关限速 → 拆干净，且机器上本来没限速时什么都不做。
#
# 用假 runner 记录 argv，不依赖本机内核有没有 HTB/IFB（开发机两样都没有，
# 见 netlimit/plan.py 模块头）。

from __future__ import annotations

import pytest

from netlimit import plan as P
from netlimit import tc


def one(rate=1000.0, **kw):
    return P.LimitPlan.from_mbps(["eth0"], rate, **kw)


def two(rate=2000.0, **kw):
    return P.LimitPlan.from_mbps(["eth0", "eth1"], rate, **kw)


def argvs(cmds):
    return [" ".join(a) for a, _ in cmds]


class FakeTc:
    """假 tc：记录收到的 argv，按预置输出应答 show。"""

    def __init__(self, classes="", fail_on=()):
        self.calls: list[str] = []
        self._classes = classes
        self._fail_on = fail_on

    def __call__(self, argv, *, fatal=True, dry=False):
        self.calls.append(" ".join(argv))
        for bad in self._fail_on:
            if bad in " ".join(argv):
                if fatal:
                    raise tc.TcError(f"假失败：{bad}")
                return ""
        if argv[:3] == ["tc", "class", "show"]:
            return self._classes
        return ""

    @property
    def mutations(self):
        """只保留会改变机器状态的命令（show 是只读的）。"""
        return [c for c in self.calls if " show " not in c]


# 一份贴近真实 `tc class show` 输出的样例。tc 会把速率换算成可读单位，
# 字节数按 1024 折算且有损——样例必须照真实输出写，自洽的假数据会把
# 解析上的错误盖住。
CLASSES_OK = """\
class htb 1:1 root prio 0 rate 100Gbit ceil 100Gbit burst 8Mb cburst 8Mb
class htb 1:2 root leaf 2: prio 0 rate 1000Mbit ceil 1000Mbit burst 1220Kb cburst 1220Kb
"""
# 漏给 cburst 的版本会在机器上留下的样子：速率对，桶被 tc 按 MTU 量级兜底。
CLASSES_BAD_CBURST = """\
class htb 1:1 root prio 0 rate 100Gbit ceil 100Gbit burst 8Mb cburst 8Mb
class htb 1:2 root leaf 2: prio 0 rate 1000Mbit ceil 1000Mbit burst 1220Kb cburst 1600b
"""


# ---------------------------------------------------------------------------
# 纯函数
# ---------------------------------------------------------------------------

def test_minor_is_hex():
    """tc 的次要号被 iproute2 按**十六进制**解析（get_tc_classid 里的
    strtoul(str, &p, 16)）。本模块只用 1/2 这种小数字，十进制与十六进制
    同形，所以错了也看不出来——这条测试盯的是以后有人加个 >9 的编号时
    不会静默踩进去。"""
    assert P.classid(1) == "1:1"
    assert P.classid(2) == "1:2"
    assert P.classid(16) == "1:10", "16 必须写成十六进制的 10"
    assert P.classid(0xFFFF) == "1:ffff"
    with pytest.raises(P.PlanError):
        P.classid(0x10000)


def test_burst_has_a_floor_and_a_ceiling():
    """桶太小达不到限额，太大则秒级限速失真。"""
    assert P.burst_bytes(1_000_000_000) == 1_250_000       # 1 Gbit 的 10 毫秒
    assert P.burst_bytes(1000) == P.MIN_BURST_BYTES        # 极小速率走下限
    # 免限类是 100 Gbit，按 10 毫秒算是 125 MB —— 那个数字没有意义。
    assert P.burst_bytes(P.FREE_RATE_BPS) == P.MAX_BURST_BYTES


def test_every_class_gets_cburst():
    """**cburst 必须显式给。** 它管的是 ceil 那一路的桶；漏给的话 tc 会
    兜底成 MTU 量级，`rate` 显示完全正常而实际吞吐远低于限额——上一个
    项目就是这么让整机降速的。"""
    for cmd in argvs(P.build(one())):
        if " class add " in cmd or " class change " in cmd:
            assert " cburst " in cmd, cmd


# ---------------------------------------------------------------------------
# 单网卡
# ---------------------------------------------------------------------------

def test_single_nic_tree_has_no_ifb_and_no_redirect():
    """一张网卡时 HTB 直接挂上去。引入 IFB 是白担风险——多一层重定向就多
    一处可能出错、且那条内核行为本仓库没实测过。"""
    cmds = argvs(P.build(one()))
    assert not any("ifb" in c for c in cmds), cmds
    assert not any("mirred" in c or "clsact" in c for c in cmds), cmds
    assert any("htb default 2" in c for c in cmds)
    assert sum(" class add " in c for c in cmds) == 2, "免限类 + 限速类"


def test_exempt_rules_cover_ipv4_and_ipv6():
    """只写 IPv4 的话，链路打满时 IPv6 的 SSH 照样进不来——而那往往是
    最后一条还能用的路。"""
    f = [c for c in argvs(P.build(one())) if " filter add " in c]
    assert len(f) == 2
    assert any("protocol ip " in c and "sport 22" in c for c in f)
    assert any("protocol ipv6 " in c and "sport 22" in c for c in f)
    assert all(f"flowid {P.classid(P.FREE_MINOR)}" in c for c in f)


def test_no_exempt_ports_means_no_filters():
    cmds = argvs(P.build(one(exempt_ports=())))
    assert not any(" filter add " in c for c in cmds)


# ---------------------------------------------------------------------------
# 多网卡：必须是一份令牌桶
# ---------------------------------------------------------------------------

def test_multi_nic_shapes_once_on_the_ifb():
    """**整机一个总闸门**：HTB 只能有一棵，且必须在 IFB 上。

    每张网卡各挂一棵 = N 份额度，总量变成 N 倍——这是这个方案最容易犯
    也最难在生产上发现的错（每张网卡看起来都限得好好的）。
    """
    cmds = argvs(P.build(two()))
    htb_roots = [c for c in cmds if "root handle 1: htb" in c]
    assert len(htb_roots) == 1, f"整机只能有一棵 HTB：{htb_roots}"
    assert "dev ifb0" in htb_roots[0]
    # 两张网卡都要把出向交出去，一张都不能漏——漏掉的那张等于不限速。
    for iface in ("eth0", "eth1"):
        assert any(f"filter add dev {iface} egress matchall" in c
                   and "mirred egress redirect dev ifb0" in c for c in cmds), iface


def test_multi_nic_brings_the_ifb_up():
    """IFB 没 up 的话，重定向过去的包会被直接丢掉 —— 表现为**整机断网**。
    所以 `link set up` 必须是致命命令，失败要停。"""
    cmds = P.build(two())
    up = [(a, fatal) for a, fatal in cmds if a[:3] == ["ip", "link", "set"]]
    assert len(up) == 1
    argv, fatal = up[0]
    assert argv == ["ip", "link", "set", "ifb0", "up"]
    assert fatal, "IFB 没起来还继续下发 = 整机断网"


def test_ifb_add_is_not_fatal_but_tree_is():
    """IFB 设备可能上一次就建好了，add 失败不致命；队列树的命令必须致命
    ——"失败了但继续跑"等于静默不限速。"""
    by_cmd = {" ".join(a): fatal for a, fatal in P.build(two())}
    assert by_cmd["ip link add ifb0 type ifb"] is False
    assert by_cmd["tc qdisc add dev ifb0 root handle 1: htb default 2"] is True
    assert by_cmd["tc qdisc add dev eth0 clsact"] is True


def test_shaping_device_switches_with_nic_count():
    assert one().shaping_dev == "eth0"
    assert two().shaping_dev == "ifb0"
    assert not one().aggregated and two().aggregated


# ---------------------------------------------------------------------------
# 拒绝路径：不成立就一条都不发
# ---------------------------------------------------------------------------

def test_rejects_empty_iface_list():
    with pytest.raises(P.PlanError, match="没有要限的网卡"):
        P.check(P.LimitPlan.from_mbps([], 1000.0))


def test_rejects_ifb_among_the_shaped_nics():
    """把聚合设备本身当成待限网卡 = 流量被重定向回自己，同一份字节整形
    两遍，实际吞吐掉到限额一半以下。"""
    with pytest.raises(P.PlanError, match="重定向给自己"):
        P.check(P.LimitPlan.from_mbps(["eth0", "ifb0"], 1000.0))


def test_rejects_duplicate_nics():
    with pytest.raises(P.PlanError, match="重复"):
        P.check(P.LimitPlan.from_mbps(["eth0", "eth0"], 1000.0))


@pytest.mark.parametrize("port", [0, 65536, -1])
def test_rejects_out_of_range_exempt_port(port):
    with pytest.raises(P.PlanError, match="免限端口"):
        P.check(one(exempt_ports=(port,)))


def test_unset_rate_is_off_but_zero_is_an_error():
    """**留空（不限速）与填 0（配错了）是两件事。**

    合并成一个 0 的话，"我配了限速但把额度打错了"会静默变成不限速——
    静默不限速是这类工具最不能接受的故障。
    """
    off = P.LimitPlan.from_mbps(["eth0"], None)
    assert off.off
    P.check(off)                        # 合法，而且是默认
    with pytest.raises(P.PlanError, match="留空"):
        P.check(one(rate=0.0))
    with pytest.raises(P.PlanError, match="留空"):
        P.check(one(rate=-5.0))


def test_default_plan_is_off():
    """不给任何参数造出来的计划 = 不限速。默认值不该成为限制。"""
    assert P.LimitPlan().off


# ---------------------------------------------------------------------------
# 关限速
# ---------------------------------------------------------------------------

def test_teardown_removes_both_hooks_on_every_nic():
    """拆的时候 clsact 与 root 都要拆，而且每张网卡都要拆干净——
    漏一个 clsact 就会有流量还在往一个已经没有队列树的 IFB 上重定向。"""
    cmds = argvs(P.teardown(two()))
    for iface in ("eth0", "eth1"):
        assert f"tc qdisc del dev {iface} clsact" in cmds
        assert f"tc qdisc del dev {iface} root" in cmds
    assert "tc qdisc del dev ifb0 root" in cmds


def test_teardown_is_all_nonfatal():
    """拆的时候东西本来就可能不在（没建过、被人手工删过、上次拆了一半）。
    拆不干净比拆报错更危险，所以每一条都要发出去。"""
    assert all(not fatal for _, fatal in P.teardown(two()))


def test_teardown_keeps_the_ifb_device():
    """不删 ifb0 设备本身：删了对下一次生效毫无帮助，反而可能把别的工具
    正在用的 ifb0 掀掉。"""
    assert not any("link del" in c for c in argvs(P.teardown(two())))


def test_rate_change_cmd_refuses_when_off():
    with pytest.raises(P.PlanError, match="拆掉"):
        P.rate_change_cmd(P.LimitPlan.from_mbps(["eth0"], None))


# ---------------------------------------------------------------------------
# reconcile 的四条路径
# ---------------------------------------------------------------------------

def test_noop_when_already_consistent():
    """已经一致就一条命令都不发：周期性运行时不做这个判断，就会每个周期
    重建一次队列树、抖一次全机流量。"""
    fake = FakeTc(classes=CLASSES_OK)
    res = tc.reconcile(one(1000.0), runner=fake, log=lambda *_: None)
    assert res.ok and not res.changed
    assert fake.mutations == []


def test_rate_only_change_does_not_rebuild():
    """只改额度走 class change：整机这个总闸门被调的频率只会更高（扩容、
    削峰），每次都重建就是每次调额度都抖一下全机流量。"""
    fake = FakeTc(classes=CLASSES_OK)
    res = tc.reconcile(one(2000.0), runner=fake, log=lambda *_: None)
    assert res.ok and res.changed and res.action == "rate-change"
    assert len(fake.mutations) == 1
    assert "class change" in fake.mutations[0]
    assert "rate 2000000000bit" in fake.mutations[0]
    assert "cburst" in fake.mutations[0], "改速率必须把桶一起改"


def test_rebuild_when_tree_missing():
    fake = FakeTc(classes="")
    res = tc.reconcile(one(1000.0), runner=fake, log=lambda *_: None)
    assert res.ok and res.action == "rebuild"
    assert any("htb default 2" in c for c in fake.mutations)


def test_bad_cburst_gets_repaired_without_rebuilding():
    """从漏给 cburst 的版本升上来：速率对、桶坏了。修它不需要重建整棵树。"""
    fake = FakeTc(classes=CLASSES_BAD_CBURST)
    res = tc.reconcile(one(1000.0), runner=fake, log=lambda *_: None)
    assert res.changed, "cburst 坏了不能报告『无变化』"
    assert res.action == "rate-change"
    assert "cburst 1250000" in fake.mutations[0]


def test_off_on_a_clean_machine_sends_nothing():
    """机器上本来没有限速时一条命令都不发：否则周期性运行会不停地
    qdisc del，还会反复删掉别人手工建的整形规则。"""
    fake = FakeTc(classes="")
    res = tc.reconcile(P.LimitPlan.from_mbps(["eth0"], None),
                       runner=fake, log=lambda *_: None)
    assert res.ok and not res.changed
    assert fake.mutations == []


def test_off_tears_down_an_existing_tree():
    fake = FakeTc(classes=CLASSES_OK)
    res = tc.reconcile(P.LimitPlan.from_mbps(["eth0"], None),
                       runner=fake, log=lambda *_: None)
    assert res.ok and res.changed and res.action == "teardown"
    assert any("qdisc del dev eth0 root" in c for c in fake.mutations)


def test_reconcile_reports_failure_instead_of_pretending():
    fake = FakeTc(classes="", fail_on=("qdisc add dev eth0 root",))
    res = tc.reconcile(one(1000.0), runner=fake, log=lambda *_: None)
    assert not res.ok and res.error, "下发失败必须如实报告，不能假装成功"


def test_invalid_plan_sends_no_commands_at_all():
    fake = FakeTc(classes="")
    res = tc.reconcile(P.LimitPlan.from_mbps([], 1000.0),
                       runner=fake, log=lambda *_: None)
    assert not res.ok
    assert fake.calls == [], "计划不成立时连 show 都不该发"


# ---------------------------------------------------------------------------
# 读回解析
# ---------------------------------------------------------------------------

def test_parse_classes_reads_hex_minors_and_units():
    got = tc.parse_classes(CLASSES_OK)
    assert got.rates[P.LIMIT_MINOR] == 1_000_000_000
    assert got.rates[P.FREE_MINOR] == 100_000_000_000
    assert got.cbursts[P.FREE_MINOR] == 8 * 1024 ** 2


def test_size_and_rate_use_different_bases():
    """tc 打**字节**按 1024 折算，打**速率**按 1000。混用会让比对全错。"""
    assert tc.parse_size("8Mb") == 8 * 1024 ** 2
    assert tc.parse_rate("8Mbit") == 8_000_000
    assert tc.parse_size("1600b") == 1600


def test_cburst_comparison_tolerates_tc_rounding():
    """tc 打字节数有损：5000000 会打成 "4883Kb"（= 5000192）。精确比对
    必然误判，所以留了余量——但 MTU 量级的兜底值仍然要能认出来。"""
    assert not tc.cburst_too_small(5000192, 4_000_000_000)
    assert tc.cburst_too_small(1600, 4_000_000_000)
    assert tc.cburst_too_small(None, 4_000_000_000)


# ---------------------------------------------------------------------------
# 多网卡最危险的失效方式：看起来限着，其实漏了一整张网卡
# ---------------------------------------------------------------------------

class FakeTcWithFilters(FakeTc):
    """能回答 `tc filter show dev X egress` 的假 tc。"""

    def __init__(self, classes="", redirected=()):
        super().__init__(classes=classes)
        self._redirected = set(redirected)

    def __call__(self, argv, *, fatal=True, dry=False):
        if argv[:3] == ["tc", "filter", "show"]:
            self.calls.append(" ".join(argv))
            iface = argv[argv.index("dev") + 1]
            return ("filter protocol all pref 49152 matchall\n"
                    "  action order 1: mirred (Egress Redirect to device ifb0)\n"
                    if iface in self._redirected else "")
        return super().__call__(argv, fatal=fatal, dry=dry)


def test_missing_redirect_on_one_nic_triggers_rebuild():
    """**后插一张网卡（或有人删了 clsact）时，那张网卡的流量会完全绕过
    限速——而 IFB 上的类看起来一切正常。**

    只核对 IFB 上的队列树是不够的，必须逐张网卡核对重定向还在不在。
    """
    fake = FakeTcWithFilters(classes=CLASSES_OK, redirected=("eth0",))
    res = tc.reconcile(two(1000.0), runner=fake, log=lambda *_: None)
    assert res.action == "rebuild", "eth1 没有重定向，必须重建把它补上"
    assert any("dev eth1" in c and "mirred" in c for c in fake.mutations)


def test_all_redirects_present_is_a_noop():
    fake = FakeTcWithFilters(classes=CLASSES_OK, redirected=("eth0", "eth1"))
    res = tc.reconcile(two(1000.0), runner=fake, log=lambda *_: None)
    assert res.ok and not res.changed
    assert fake.mutations == []


def test_single_nic_does_not_query_redirects():
    """单网卡没有重定向这回事，不该去查——多发的每一条命令都是多一分
    出错和误导的机会。"""
    fake = FakeTcWithFilters(classes=CLASSES_OK)
    tc.reconcile(one(1000.0), runner=fake, log=lambda *_: None)
    assert not any("filter show" in c for c in fake.calls)


def test_ifb_is_created_before_any_redirect_references_it():
    """**次序不变量**：mirred 在 iproute2 的**解析阶段**就要把目标设备名
    解析成 ifindex，设备不存在时直接报 `bad action parsing`。

    实测确认过：ifb0 不存在时那条 filter 命令连解析都过不去；造一个同名
    设备之后立刻就能解析了。所以 `ip link add ifb0` 必须排在所有 mirred
    规则之前——这个次序错了，多网卡形态下**一条重定向都建不起来**，而
    IFB 上的队列树却建得好好的（= 看起来限着，其实一张网卡都没接进来）。
    """
    cmds = argvs(P.build(two()))
    ifb_created = next(i for i, c in enumerate(cmds) if c.startswith("ip link add ifb0"))
    ifb_up = next(i for i, c in enumerate(cmds) if c.startswith("ip link set ifb0 up"))
    first_mirred = next(i for i, c in enumerate(cmds) if "mirred" in c)
    assert ifb_created < ifb_up < first_mirred, cmds
