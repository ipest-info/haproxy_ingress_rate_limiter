# tests.test_tcifb —— 入向平稳整形（IFB + HTB）。
#
# 与 tcshaper/tcingress 的测试同一套路：假 runner 记录 argv、按预置输出
# 应答，验证命令序列与幂等语义。字节整形本体（HTB 树）由 TcShaper 承担
# 且有自己的测试，这里盯 tcifb 自己的职责：管路（IFB 设备/重定向）、
# 入向登记到 shaper 清单的翻译、conn_rate 的 SYN 规则、清空撤除。
import logging

from rl_limiter import model
from rl_limiter import tcifb as TI

log = logging.getLogger("t.tcifb")
IFACE = "eth0"
IFB = "ifbrl0"

# TcShaper.observe 读类实况用的样例：一个 8080 端口类，10Mbit（与登记
# 一致时 shaper 判定"无需变更"）。格式说明见 test_tcshaper。
CLASSES_10M = """\
class htb 1:1 root prio 0 rate 100Gbit ceil 100Gbit burst 8Mb cburst 8Mb
class htb 1:1f90 root leaf 1f90: prio 0 rate 10Mbit ceil 10Mbit burst 12500b cburst 12500b
"""
REDIRECT_PRESENT = (
    "filter protocol ip pref 5 u32 chain 0 fh 800::800 terminal flowid \n"
    "  match 00000000/00000000 at 0\n"
    "\taction order 1: mirred (Egress Redirect to device ifbrl0) stolen\n")
# IFB 上的分类实况（8080 的类已被指向）：shaper.observe 读它判断分类
# 规则是否齐全，缺了会判整树重建。
FILTERS_IFB = (
    "filter protocol ip pref 1 u32 chain 0 fh 800::800 terminal "
    "flowid 1:1f90 not_in_hw \n  match 00001f90/0000ffff at 20\n")


class FakeRunner:
    """假 ip/tc：记录 argv；按场景预置 show 类命令的输出。"""

    def __init__(self, redirect_out="", classes="", ifb_filters=None,
                 fail_on=None):
        self.calls: list[list[str]] = []
        self.redirect_out = redirect_out   # 真实网卡 ffff: 的 filter show
        self.classes = classes             # IFB 上的 class show
        # IFB 上的 filter show：给了 classes 默认也给上分类实况，两者
        # 配套才构成"与登记一致"的网卡状态。
        self.ifb_filters = (FILTERS_IFB if classes else "") \
            if ifb_filters is None else ifb_filters
        self._fail_on = fail_on or ()

    async def __call__(self, argv):
        self.calls.append(list(argv))
        joined = " ".join(argv)
        for bad in self._fail_on:
            if bad in joined:
                return 1, "", f"fake failure: {bad}"
        if argv[:2] == ["ip", "link"] and "add" in argv:
            return 1, "", "RTNETLINK answers: File exists"  # 设备已在：常态
        if "filter" in argv and "show" in argv and IFACE in argv:
            return 0, self.redirect_out, ""
        if "filter" in argv and "show" in argv and IFB in argv:
            return 0, self.ifb_filters, ""
        if "class" in argv and "show" in argv:
            return 0, self.classes, ""
        if argv[:2] == ["tc", "qdisc"] and "show" in argv:
            return 0, "qdisc ingress ffff: parent ffff:fff1", ""
        return 0, "", ""

    def muts(self, *needles):
        """含全部关键词的命令（排除只读 show）。"""
        return [c for c in self.calls
                if "show" not in c and all(n in c for n in needles)]


def fe(name="fe_main", port=8080):
    return model.FrontendConfig(name=name, bind_port=port)


LIMITS = {"fe_main": model.RequestLimit(mbps=10.0, conn_rate=150)}


def shaper(**kw):
    f = FakeRunner(**kw)
    return TI.TcIfbIngress(IFACE, ifb=IFB, log=log, runner=f), f


# ---------------------------------------------------------------------------
# 首次建立：管路 + HTB + SYN 闸
# ---------------------------------------------------------------------------

async def test_first_reconcile_builds_everything():
    sh, f = shaper()
    res = await sh.reconcile([fe()], LIMITS)
    assert res.ok
    # 管路：设备 up、ingress 钩子、IPv4/IPv6 两条 mirred 重定向。
    assert f.muts("ip", "set", IFB, "up")
    assert f.muts("qdisc", "add", "ingress", IFACE)
    redirs = f.muts("mirred", "redirect", IFB)
    assert len(redirs) == 2
    assert {c[c.index("protocol") + 1] for c in redirs} == {"ip", "ipv6"}
    # 字节整形在 IFB 上：HTB 根 + 端口类（10Mbit）+ **目的端口**分类。
    assert f.muts("qdisc", "add", IFB, "htb")
    assert f.muts("class", "add", IFB, "1:1f90")
    flt = f.muts("filter", "add", IFB, "dport")
    assert len(flt) == 2 and not f.muts("filter", "add", IFB, "sport")
    # conn_rate：真实网卡上的 SYN flower police（IPv4/IPv6）。
    syn = f.muts("flower", "pkts_rate", "150")
    assert len(syn) == 2 and all(IFACE in c for c in syn)


async def test_redirect_not_duplicated_when_present():
    """重定向已在（filter show 能看到 mirred→ifb）→ 不重复添加。"""
    sh, f = shaper(redirect_out=REDIRECT_PRESENT, classes=CLASSES_10M)
    await sh.reconcile([fe()], LIMITS)
    assert not f.muts("mirred")
    # IFB 上的类与登记一致 → shaper 一条变更命令都不发。
    assert not f.muts("class")


async def test_syn_rules_refresh_only_on_config_change():
    sh, f = shaper(redirect_out=REDIRECT_PRESENT, classes=CLASSES_10M)
    await sh.reconcile([fe()], LIMITS)
    n = len(f.muts("flower"))
    assert n == 2
    await sh.reconcile([fe()], LIMITS)          # 同配置：不重刷
    assert len(f.muts("flower")) == n
    await sh.reconcile([fe()], {"fe_main": model.RequestLimit(
        mbps=10.0, conn_rate=80)})              # conn_rate 变了：重刷
    assert f.muts("flower", "pkts_rate", "80")


async def test_rate_change_goes_in_place():
    """动态闭环只改 mbps 时走 TcShaper 的就地变更（class change），
    不整树重建、不动重定向——这正是换 IFB 方案的核心收益。"""
    sh, f = shaper(redirect_out=REDIRECT_PRESENT, classes=CLASSES_10M)
    await sh.reconcile([fe()], {"fe_main": model.RequestLimit(
        mbps=7.0, conn_rate=150)})
    assert f.muts("class", "change", IFB)
    assert not f.muts("qdisc", "del", IFB)
    assert not f.muts("mirred")


# ---------------------------------------------------------------------------
# 清空与失败
# ---------------------------------------------------------------------------

async def test_empty_limits_tears_down():
    sh, f = shaper(redirect_out=REDIRECT_PRESENT)
    res = await sh.reconcile([fe()], {})
    assert res.ok and res.action == "teardown"
    assert f.muts("qdisc", "del", IFACE, "ingress")
    assert f.muts("qdisc", "del", IFB, "root")
    assert f.muts("ip", "set", IFB, "down")


async def test_missing_ifb_support_fails_loud_not_silent():
    """内核没有 ifb（ip link show 都失败）→ reconcile 返回失败并报错，
    绝不静默降级成"没在限速"。"""
    sh, f = shaper(fail_on=("link show",))
    res = await sh.reconcile([fe()], LIMITS)
    assert not res.ok and res.error
    # 管路都没建成，绝不能继续下发整形/SYN 规则。
    assert not f.muts("htb") and not f.muts("flower")


async def test_unknown_registration_warned_and_ignored():
    sh, f = shaper(redirect_out=REDIRECT_PRESENT, classes=CLASSES_10M)
    res = await sh.reconcile(
        [fe()], dict(LIMITS, fe_gone=model.RequestLimit(mbps=5)))
    assert res.ok
    # 只有 fe_main 的类，不存在 fe_gone 的任何命令。
    assert not [c for c in f.calls if "5000000" in " ".join(c)]


# ---------------------------------------------------------------------------
# debug_state
# ---------------------------------------------------------------------------

async def test_debug_state_readonly_and_covers_both_devices():
    sh, f = shaper(redirect_out=REDIRECT_PRESENT, classes=CLASSES_10M)
    st = await sh.debug_state()
    assert st["iface"] == f"{IFACE} → {IFB}"
    labels = " ".join(r["label"] for r in st["raw"])
    assert IFACE in labels and IFB in labels
    assert st["help"] and all(r["note"] for r in st["raw"])
    assert not [c for c in f.calls if "show" not in c]


async def test_instance_bits_builds_hierarchy_with_all_frontends():
    """实例级闭环的执行面：instance_bits > 0 → IFB 上切层级——全部
    frontend（含没登记入向上限的）挂进 1:ffff 总闸，总闸速率即动态值；
    登记了上限的端口在总闸内保留各自的 ceil。"""
    sh, f = shaper(redirect_out=REDIRECT_PRESENT)
    res = await sh.reconcile(
        [fe(), fe(name="fe_other", port=9090)], LIMITS,
        instance_bits=int(8e6))
    assert res.ok
    # 总闸类 + 两个端口类都在（9090 没登记也要挂进总闸）。
    assert f.muts("class", "add", IFB, "1:ffff")
    assert f.muts("class", "add", IFB, "1:1f90")
    assert f.muts("class", "add", IFB, "1:2382")   # 0x2382 = 9090
    # 端口类挂的是总闸（parent 1:ffff）。
    assert all("1:ffff" in c for c in f.muts("class", "add", "1:1f90"))


async def test_instance_bits_without_rules_keeps_running():
    """只有实例目标、一个 request_limits 都没登记：不是 teardown——
    总闸本身就是执行面，全部端口挂进去只受总闸管。"""
    sh, f = shaper(redirect_out=REDIRECT_PRESENT)
    res = await sh.reconcile([fe()], {}, instance_bits=int(40e6))
    assert res.ok
    assert f.muts("class", "add", IFB, "1:ffff")
    assert not f.muts("qdisc", "del", IFACE, "ingress")


async def test_ingress_rate_bits_diffs_root_qdisc():
    """实测入向速率：IFB 根 qdisc 的 Sent 字节差分。首次调用建基准
    返回 None；统计清零（整形树重建）返回 None 并重新落基准。"""
    import json as _json
    sh, f = shaper()
    seq = [1_000_000, 2_250_000, 100]      # 第三次：计数器被重建清零
    async def runner(argv):
        f.calls.append(list(argv))
        if "-j" in argv and "qdisc" in argv:
            return 0, _json.dumps([{"kind": "htb", "handle": "1:",
                                    "bytes": seq.pop(0)}]), ""
        return 0, "", ""
    sh._run = runner
    assert await sh.ingress_rate_bits() is None          # 建基准
    sh._rate_prev = (sh._rate_prev[0] - 2.0, sh._rate_prev[1])  # 模拟隔 2s
    r = await sh.ingress_rate_bits()
    assert r is not None and abs(r - 1_250_000 * 8 / 2.0) / r < 0.2
    sh._rate_prev = (sh._rate_prev[0] - 2.0, sh._rate_prev[1])
    assert await sh.ingress_rate_bits() is None          # 回卷 → None
