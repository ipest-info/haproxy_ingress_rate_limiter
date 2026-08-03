# tests.test_tcshaper —— 用内核 tc（HTB）做限速。
#
# 这是唯一一个**以 root 权限对生产数据面下命令**的模块，
# 所以测试的重点同样不是"命令拼对了没有"，而是每条边界都不留烂摊子：
#   1. 空清单/非法端口/限额过小 → 拒绝执行，不下任何命令；
#   2. 已经一致 → 一条命令都不发（否则每 30 秒重建一次队列树 = 每 30 秒
#      抖一次流量）；
#   3. 只有速率变了 → 走 `tc class change`，**绝不重建**（重建会短暂不整形）；
#   4. 结构变了 → 重建，且重建序列里每个 frontend 的类/叶子/IPv4+IPv6
#      分类一个都不少。
#
# 用假的 runner 记录 argv，不依赖本机内核有没有 htb（本沙箱内核只内建
# pfifo，见 docs/06 的"未验证的部分"）。

from __future__ import annotations

import pytest

from rl_limiter import model
from rl_limiter import tcshaper as T

IFACE = "eth0"


def fe(name="fe_main", port=8080, quota=40.0):   # quota 单位 = Mbps
    return model.FrontendConfig(name=name, bind_port=port, quota_mbps=quota)


class FakeTc:
    """假 tc：记录收到的 argv，按预置的 show 输出应答。"""

    def __init__(self, classes="", filters="", fail_on=None):
        self.calls: list[list[str]] = []
        self._classes = classes
        self._filters = filters
        self._fail_on = fail_on or ()      # 命中该子串的命令返回非零

    async def __call__(self, argv):
        self.calls.append(list(argv))
        joined = " ".join(argv)
        for bad in self._fail_on:
            if bad in joined:
                return 1, "", f"fake failure: {bad}"
        if argv[:3] == ["tc", "class", "show"]:
            return 0, self._classes, ""
        if argv[:3] == ["tc", "filter", "show"]:
            return 0, self._filters, ""
        return 0, "", ""

    def mutations(self):
        """只保留会改变内核状态的命令（show 是只读的）。"""
        return [c for c in self.calls if c[2] != "show"]


def shaper(**kw):
    f = FakeTc(**kw)
    return T.TcShaper(IFACE, runner=f), f


# 一份贴近真实 `tc class show` 输出的样例（htb 会把速率换算成可读单位）。
# classid 的次要号是**十六进制**：0x1f90 = 8080。样例必须照 tc 的真实
# 输出写——早先这里写成 1:8080 是自洽的假数据，正好把十六进制这件事盖住了。
# cburst 必须是我们自己算的值（8Mb / 50000b），不是 tc 按 MTU 兜底的
# 1600b —— 后者是线上事故现场的样子，见 CLASSES_BAD_CBURST。
CLASSES_OK = """\
class htb 1:1 root prio 0 rate 100Gbit ceil 100Gbit burst 8Mb cburst 8Mb
class htb 1:1f90 root leaf 1f90: prio 0 rate 40Mbit ceil 40Mbit burst 50000b cburst 50000b
"""
# 带 bug 的旧版本在网卡上留下的样子：速率对，cburst 被 tc 按 MTU 量级兜底。
CLASSES_BAD_CBURST = """\
class htb 1:1 root prio 0 rate 100Gbit ceil 100Gbit burst 2400b cburst 2400b
class htb 1:1f90 root leaf 1f90: prio 0 rate 40Mbit ceil 40Mbit burst 50000b cburst 1600b
"""
FILTERS_OK = """\
filter parent 1: protocol ip pref 1 u32 chain 0
filter parent 1: protocol ip pref 1 u32 chain 0 fh 800: ht divisor 1
filter parent 1: protocol ip pref 1 u32 chain 0 fh 800::800 order 2048 key ht 800 bkt 0 flowid 1:1f90 not_in_hw
  match 00001f90/0000ffff at 20
"""


# ---------------------------------------------------------------------------
# 纯函数：classid / burst / 解析
# ---------------------------------------------------------------------------

def test_classid_is_the_listen_port():
    """classid 次要号直接取监听端口：端口天然唯一，因此 classid 稳定——
    增删 frontend 不会让别人的 classid 漂移，reconcile 才能只改不重建。"""
    assert T.classid_for(8080) == "1:1f90"      # 0x1f90 = 8080
    assert T.classid_for(443) == "1:1bb"


@pytest.mark.parametrize("port", [2, 80, 443, 8080, 9999, 10000, 14223, 65535])
def test_classid_minor_is_hex_and_always_within_16_bits(port):
    """**线上事故的回归测试**：classid 的次要号被 iproute2 按十六进制解析
    （get_tc_classid 里的 strtoul(str, &p, 16)），而这里原先是按十进制拼的。

        tc class add ... classid 1:14223 ...
        Error: argument "1:14223" is wrong: invalid class ID

    边界正好在端口 10000：1–9999 的十进制串碰巧也是合法十六进制
    （0x9999 < 0xFFFF）所以"能用"，**10000 以上的端口全部下发失败——那些
    frontend 根本没有限速**。这就是为什么全部用 8080 做样例的老测试一条都
    没红：8080 恰好是那个能蒙混过关的区间。
    """
    cid = T.classid_for(port)
    minor = cid.split(":", 1)[1]
    assert int(minor, 16) == port, "次要号按十六进制读回来必须等于端口本身"
    assert 1 <= int(minor, 16) <= 0xFFFF, "次要号是 16 位，超了 tc 直接拒绝"
    assert len(minor) <= 4, f"{cid} 超过 4 个十六进制位，tc 会判为 invalid class ID"


def test_classid_round_trips_through_tc_output():
    """下发用的 classid 与从 tc 输出读回的次要号必须是同一个数。

    两边只要有一边用错进制，比对就永远不相等——每一轮 reconcile 都判定
    "类不存在"然后重建整棵树，限速被反复推倒重来。
    """
    for port in (80, 8080, 14223, 65535):
        cid = T.classid_for(port)
        line = f"class htb {cid} root leaf x: prio 0 rate 40Mbit ceil 40Mbit\n"
        assert T.parse_classes(line) == {port: 40_000_000}
        flt = f"filter parent 1: u32 fh 800::800 flowid {cid} not_in_hw\n"
        assert T.parse_filter_minors(flt) == {port}


@pytest.mark.parametrize("rate_bytes,expect", [
    (1_000, 3000),              # 极小限额：10ms 额度只有 10 字节，夹到下限 2×MTU
    (5_000_000, 50_000),        # 40 Mbps：10ms = 50000 字节
    (125_000_000, 1_250_000),   # 1 Gbps
    (10_000_000_000, 8 * 1024 * 1024),   # 极大限额夹到上限
])
def test_burst_is_ten_milliseconds_clamped(rate_bytes, expect):
    """burst 太小达不到设定速率，太大则秒级限速失真；取 10ms 额度并夹在
    [2×MTU, 8MB]。"""
    assert T.burst_bytes(rate_bytes) == expect


@pytest.mark.parametrize("text,expect", [
    ("40Mbit", 40_000_000),
    ("1250000bit", 1_250_000),
    ("100Gbit", 100_000_000_000),
    ("1Kbit", 1_000),
])
def test_parse_rate_handles_tc_units(text, expect):
    """tc 会按可读性自动换算单位，比较速率必须解析成数值——比字符串会让
    "40Mbit" 与 "40000000bit" 被当成不同的值而反复重建。"""
    assert T.parse_rate(text) == expect


def test_parse_classes_and_filters():
    assert T.parse_classes(CLASSES_OK) == {1: 100_000_000_000, 8080: 40_000_000}
    assert T.parse_filter_minors(FILTERS_OK) == {8080}


def test_parse_tolerates_empty_output():
    """网卡上还没有任何 qdisc 时 tc 输出为空——那是首次运行的正常状态。"""
    assert T.parse_classes("") == {}
    assert T.parse_filter_minors("") == set()


# ---------------------------------------------------------------------------
# 拒绝执行的边界
# ---------------------------------------------------------------------------

async def test_empty_list_refused_without_touching_kernel():
    """空清单 = 撤掉全部限速。那是事故不是配置操作，且必须**一条命令都不发**。"""
    sh, fake = shaper()
    res = await sh.reconcile([])
    assert not res.ok and "撤掉全部限速" in res.error
    assert fake.calls == []


async def test_port_colliding_with_default_class_refused():
    """classid 次要号取端口，1 号被兜底类占了——必须报清楚而不是让 tc
    抛一句难懂的错。"""
    sh, fake = shaper()
    res = await sh.reconcile([fe(port=1)])
    assert not res.ok and "兜底类" in res.error
    assert fake.calls == []


async def test_duplicate_port_refused():
    sh, _ = shaper()
    res = await sh.reconcile([fe("a", port=8080), fe("b", port=8080)])
    assert not res.ok and "被多个 frontend 使用" in res.error


async def test_quota_too_small_refused():
    sh, _ = shaper()
    res = await sh.reconcile([fe(quota=0.000004)])   # 4 bit/s < 1 byte/s
    assert not res.ok and "无法整形" in res.error


# ---------------------------------------------------------------------------
# reconcile 的三条路径
# ---------------------------------------------------------------------------

async def test_no_change_when_already_consistent():
    """已经一致就一条命令都不发——否则周期兜底会每 30 秒重建一次队列树，
    每次重建都有一个不整形的窗口。"""
    sh, fake = shaper(classes=CLASSES_OK, filters=FILTERS_OK)
    res = await sh.reconcile([fe(port=8080, quota=40.0)])
    assert res.ok and not res.changed
    assert fake.mutations() == []


async def test_rate_only_change_uses_class_change_not_rebuild():
    """只改限额时绝不能重建：`tc class change` 不打断任何连接，而且**存量
    连接立刻按新限额跑**——这正是 tc 方案相对 bwlim 的优势（bwlim 改限额
    要 reload，存量连接还得等 hard-stop-after 宽限期）。"""
    sh, fake = shaper(classes=CLASSES_OK, filters=FILTERS_OK)
    res = await sh.reconcile([fe(port=8080, quota=80.0)])
    assert res.ok and res.changed and res.action == "rate-change"
    muts = fake.mutations()
    assert len(muts) == 1
    assert muts[0][:3] == ["tc", "class", "change"]
    assert "80000000bit" in muts[0]
    assert not any(c[:3] == ["tc", "qdisc", "del"] for c in muts), "不该重建"


async def test_new_frontend_triggers_rebuild():
    """结构变化（新增 frontend）只能重建——tc 没有"插入一个类并保持其余
    不动"的原子操作。"""
    sh, fake = shaper(classes=CLASSES_OK, filters=FILTERS_OK)
    res = await sh.reconcile([fe("a", port=8080), fe("b", port=9090)])
    assert res.ok and res.changed and res.action == "rebuild"
    joined = [" ".join(c) for c in fake.mutations()]
    assert any("qdisc del" in c for c in joined), "重建要先清旧树"
    assert any("classid 1:2382" in c for c in joined)   # 0x2382 = 9090


async def test_rebuild_covers_every_frontend_completely():
    """每个 frontend 都要有：HTB 类、叶子队列、IPv4 分类、IPv6 分类。
    少了 IPv6 那条，客户端走 IPv6 进来时限速会整个失效。"""
    sh, fake = shaper()          # 空状态 = 首次运行
    await sh.reconcile([fe("a", port=8080, quota=40.0)])
    joined = [" ".join(c) for c in fake.mutations()]
    # 单位回环：配置口径 40 Mbps → 内部 5_000_000 bytes/s → tc 口径
    # 40_000_000 bit/s。这条断言就是在钉这个来回不许错 8 倍。
    # classid 走十六进制（0x1f90 = 8080），而 u32 的 match sport 走十进制
    # ——两边格式不同是 tc 自己的约定，这几条断言把它钉死。
    assert any("class add" in c and "classid 1:1f90" in c and "rate 40000000bit" in c
               for c in joined), "配置的 40 Mbps 必须原样落到 tc 上"
    assert any("qdisc add" in c and "parent 1:1f90" in c for c in joined)
    assert any("filter add" in c and "protocol ip " in c + " " and "sport 8080" in c
               for c in joined), "u32 的源端口匹配是十进制，不跟着 classid 变"
    assert any("filter add" in c and "protocol ipv6" in c and "sport 8080" in c
               for c in joined)


async def test_upgrade_from_the_buggy_decimal_layout_rebuilds():
    """从带 bug 的老版本升上来时，网卡上留着按十进制拼出的旧类
    （端口 8080 → 类 1:8080，按十六进制读回来是 32896）。

    这些旧类对不上任何监听端口，结构判定必须因此失败并**整棵重建**——
    重建第一步就是 qdisc del root，把旧树连同残留的类一起清掉。若这里
    误判成"结构一致"，就会走 rate-change 去改一个并不存在的类，限速
    悄悄停留在旧配置上。
    """
    old_layout = (
        "class htb 1:1 root prio 0 rate 100Gbit ceil 100Gbit burst 0b cburst 0b\n"
        "class htb 1:8080 root leaf 8080: prio 0 rate 40Mbit ceil 40Mbit\n")
    old_filters = "filter parent 1: u32 fh 800::800 flowid 1:8080 not_in_hw\n"
    sh, fake = shaper(classes=old_layout, filters=old_filters)
    res = await sh.reconcile([fe("a", port=8080, quota=40.0)])
    assert res.ok and res.changed and res.action == "rebuild", (
        "旧布局必须触发重建，不能被当成结构一致")
    joined = [" ".join(c) for c in fake.mutations()]
    assert any("qdisc del" in c for c in joined), "重建要先把旧树清掉"
    assert any("classid 1:1f90" in c for c in joined), "新类要用十六进制次要号"


async def test_high_ports_produce_ids_tc_will_accept():
    """**线上事故的回归测试（第二处）**：不只是 classid，叶子 qdisc 的
    handle 也是十六进制的 16 位数。

        tc qdisc add ... parent 1:378f handle 14223: fq_codel
        Error: argument "14223:" is wrong: invalid qdisc ID

    这条比 classid 那处更阴——它是**非致命命令**，失败只记一行日志，表现为
    "限速在跑，但类内没有公平队列"，不会有人注意到。

    所以这里不逐条断言字面量，而是把规则本身钉住：**凡是 tc 的 ID 位置
    （classid / parent / handle 的 <数字>: 部分），都必须是 ≤4 位十六进制，
    且按十六进制读回来等于端口。** 用真实 tc 跑过一遍这些命令确认无
    "invalid class ID / invalid qdisc ID"。
    """
    import re as _re
    ports = [2, 80, 443, 8080, 9999, 10000, 14223, 65535]
    sh, fake = shaper()
    await sh.reconcile([fe(f"fe{p}", port=p) for p in ports])
    ids = set()
    for cmd in fake.mutations():
        for i, tok in enumerate(cmd):
            # classid/parent/handle 后面跟的那个 token 才是 ID
            if i and cmd[i - 1] in ("classid", "parent", "handle"):
                ids.add(tok)
    assert ids, "没抓到任何 tc ID，测试本身失效了"
    for tok in ids:
        major, _, minor = tok.partition(":")
        for part in (major, minor):
            if not part:
                continue
            assert _re.fullmatch(r"[0-9a-f]{1,4}", part), (
                f"tc ID {tok!r} 里的 {part!r} 不是 ≤4 位十六进制，"
                f"tc 会判 invalid class/qdisc ID")
    # 每个端口都得有自己的类与叶子队列，且两处用的是同一个十六进制值
    for port in ports:
        assert f"1:{port:x}" in ids, f"端口 {port} 的 classid 没下发"
        assert f"{port:x}:" in ids, f"端口 {port} 的叶子 qdisc handle 没下发"


async def test_every_htb_class_sets_both_burst_and_cburst():
    """**线上事故的回归测试**：HTB 有两个令牌桶，`rate` 那路看 `burst`，
    `ceil` 那路看 **`cburst`**。本项目 rate == ceil，所以真正决定吞吐上限
    的是 cburst。

    漏掉的话 iproute2 按 `rate / get_hz() + mtu` 自己算——现代内核 psched
    是纳秒级（/proc/net/psched 第三字段 1000000），第一项几乎归零，于是
    **无论速率填多大，算出来都只有一个 MTU 的量级**。事故现场：

        class htb 1:1    rate 100Gbit ceil 100Gbit burst 2400b    cburst 2400b
        class htb 1:378f rate 4Gbit   ceil 4Gbit   burst 5000000b cburst 1600b

    桶只有 1600~2400 字节，类的实际吞吐被压在远低于配置速率的水平，而
    `tc class show` 里的 `rate` 显示得好好的——不看 cburst 根本发现不了。

    兜底类那条最要命：没被 filter 匹配的流量**全部**落在里面，也就是整机
    除受管端口之外的所有流量。所以这里对**每一个** htb 类都查，一个都不放过。
    """
    sh, fake = shaper()
    await sh.reconcile([fe("a", port=8080, quota=40.0),
                        fe("b", port=14223, quota=4000.0)])
    classes = [c for c in fake.mutations()
               if c[:3] == ["tc", "class", "add"] and "htb" in c]
    assert len(classes) == 3, "兜底类 + 两个 frontend 类，一个都不能少"
    for cmd in classes:
        cid = cmd[cmd.index("classid") + 1]
        for key in ("rate", "ceil", "burst", "cburst"):
            assert key in cmd, f"{cid} 少了 {key}——漏了就会被 tc 按 MTU 量级兜底"
        # cburst 必须和 burst 一样大：rate == ceil，两个桶不该有区别
        assert cmd[cmd.index("burst") + 1] == cmd[cmd.index("cburst") + 1], \
            f"{cid} 的 burst 与 cburst 不一致"
        assert int(cmd[cmd.index("cburst") + 1]) >= T.MIN_BURST_BYTES


async def test_rate_change_also_carries_cburst():
    """改限额走的是 `tc class change`，它同样会重置没给的参数。

    只改 rate/ceil 不带 cburst 的话，一次调额就把桶打回 MTU 量级——限速
    看着改成功了，实际吞吐塌下来。
    """
    sh, fake = shaper(classes=CLASSES_OK, filters=FILTERS_OK)
    res = await sh.reconcile([fe(port=8080, quota=80.0)])
    assert res.action == "rate-change"
    cmd = [c for c in fake.mutations() if c[:3] == ["tc", "class", "change"]][0]
    assert "cburst" in cmd, "class change 也必须显式给 cburst"
    assert cmd[cmd.index("burst") + 1] == cmd[cmd.index("cburst") + 1]


@pytest.mark.parametrize("mbps,expect", [
    (40.0, 50_000),            # 40 Mbps → 10ms = 50000 字节
    (4000.0, 5_000_000),       # 4 Gbps
    (100000.0, 8 * 1024 * 1024),   # 100 Gbps（兜底类）夹到上限
])
def test_burst_scales_with_rate_not_with_mtu(mbps, expect):
    """burst 必须随速率走。tc 的默认算法在纳秒时钟下几乎只剩 mtu 那一项，
    这里钉住"我们自己算"这件事——差别就是 5000000 与 1600。"""
    assert T.burst_bytes(mbps * 1e6 / 8) == expect


async def test_default_class_is_created_and_unshaped():
    """没被分类的流量（SSH、监控、后端方向）必须落进一个不整形的兜底类，
    否则一开限速整台机器的其它流量都被拖下水。"""
    sh, fake = shaper()
    await sh.reconcile([fe()])
    joined = [" ".join(c) for c in fake.mutations()]
    assert any("htb default 1" in c for c in joined)
    assert any(f"classid 1:1 htb rate {T.DEFAULT_CLASS_RATE_BPS}bit" in c
               for c in joined)


# ---------------------------------------------------------------------------
# 失败路径
# ---------------------------------------------------------------------------

async def test_missing_root_qdisc_on_first_run_is_not_fatal():
    """首次运行时 `tc qdisc del root` 必然失败（本来就没有），不能因此
    放弃整次下发。"""
    sh, _ = shaper(fail_on=["qdisc del"])
    res = await sh.reconcile([fe()])
    assert res.ok and res.changed


async def test_missing_leaf_qdisc_is_not_fatal():
    """老内核可能没有 fq_codel。缺了只是失去类内公平性，限速本身照常——
    不该让整个限速下发失败。"""
    sh, _ = shaper(fail_on=["fq_codel"])
    res = await sh.reconcile([fe()])
    assert res.ok and res.changed


async def test_class_add_failure_is_reported_not_swallowed():
    """真正的限速命令失败必须上报：此时限速没生效，静默等于假装限住了。"""
    sh, _ = shaper(fail_on=["class add"])
    res = await sh.reconcile([fe()])
    assert not res.ok and "tc 命令失败" in res.error


async def test_commands_are_argv_never_shell_strings():
    """本模块以 root/CAP_NET_ADMIN 执行命令，必须逐个参数传递——走 shell
    等于把配置里的值暴露给命令行解析。"""
    sh, fake = shaper()
    await sh.reconcile([fe()])
    for argv in fake.calls:
        assert isinstance(argv, list) and all(isinstance(a, str) for a in argv)
        assert argv[0] == "tc"
        # 任何一个参数里都不该混进 shell 元字符（值全是整数/网卡名）
        assert not any(ch in a for a in argv for ch in ";|&$`\n")


# ---------------------------------------------------------------------------
# 类统计（监控用）
#
# 这是限速迁到 tc 之后白捡的能力：tc 的每个类正好对应一个 frontend，
# 于是"按监听端口统计数据包与丢包"第一次成立（HAProxy 完全不统计包，
# 网卡计数又无法按 frontend 拆）。
# ---------------------------------------------------------------------------

# `tc -s -j class show` 的真实输出形态（字段名取自本机 iproute2 6.1 实测）。
CLASS_STATS_JSON = """[
 {"class":"htb","handle":"1:1","bytes":10,"packets":1,"drops":0,
  "overlimits":0,"backlog":0,"qlen":0},
 {"class":"htb","handle":"1:1f90","bytes":123456,"packets":100,"drops":7,
  "overlimits":42,"backlog":2048,"qlen":3}
]"""


def test_parse_class_stats_maps_port_to_counters():
    st = T.parse_class_stats(CLASS_STATS_JSON)
    assert set(st) == {8080}, "兜底类 1:1 不是任何 frontend，必须跳过"
    s = st[8080]
    assert (s.packets, s.drops, s.overlimits) == (100, 7, 42)
    assert (s.bytes, s.backlog, s.qlen) == (123456, 2048, 3)


def test_parse_class_stats_tolerates_empty_and_garbage():
    """网卡上还没建树时输出为空——那是首次运行的正常状态，不该抛。"""
    assert T.parse_class_stats("") == {}
    assert T.parse_class_stats("   ") == {}
    with pytest.raises(T.TcError):
        T.parse_class_stats("not json at all")


async def test_class_stats_failure_returns_empty_not_raise():
    """统计是监控用的副链路：读不到就当没有，绝不能把异常抛给采集器——
    限速判定不该被"多画几条曲线"拖累。"""
    sh, _ = shaper(fail_on=["class show"])
    assert await sh.class_stats() == {}


# ---------------------------------------------------------------------------
# 临时端口冲突（单网卡部署的一个真实隐患）
# ---------------------------------------------------------------------------

def test_ephemeral_conflict_detected():
    """分类规则只匹配源端口。单网卡时 HAProxy 连后端的包也从同一张网卡出去，
    其源端口是内核分配的临时端口——一旦撞上某个 frontend 的监听端口，那条
    连接的出向流量就会被误判进该 frontend 的限速类。这是**随机偶发**的，
    必须在启动时就喊出来而不是等人去排查。"""
    rng = (32768, 60999)
    assert T.ephemeral_conflicts([fe("ok", port=8080)], rng) == []
    assert T.ephemeral_conflicts([fe("ok", port=443)], rng) == []
    assert T.ephemeral_conflicts([fe("bad", port=40000)], rng) == ["bad"]
    # 边界包含在内
    assert T.ephemeral_conflicts([fe("lo", port=32768)], rng) == ["lo"]
    assert T.ephemeral_conflicts([fe("hi", port=60999)], rng) == ["hi"]


def test_ephemeral_range_falls_back_when_unreadable():
    """读不到 procfs（非 Linux、容器裁剪）时用常见默认值，不能抛。"""
    assert T.ephemeral_range("/nonexistent/path") == (32768, 60999)


async def test_ephemeral_conflict_warns_but_does_not_block(caplog):
    """只告警不拒绝：两网卡部署下监听端口落在临时范围内是完全安全的，
    因为后端流量根本不经过被限速的那张网卡——不该一刀切拦住。"""
    import logging
    sh, _ = shaper()
    sh._log = logging.getLogger("t.eph")
    with caplog.at_level(logging.WARNING, logger="t.eph"):
        res = await sh.reconcile([fe("bad", port=40000)])
    assert res.ok, "只告警，不阻断"
    assert any("临时端口范围" in r.getMessage() for r in caplog.records)


# ---------------------------------------------------------------------------
# 从带 bug 的版本升上来：坏掉的 cburst 必须被修回去
# ---------------------------------------------------------------------------

async def test_upgrade_repairs_frontend_cburst_even_when_rate_matches():
    """升级场景：限额一个字没改，但网卡上留着旧版本写的 cburst 1600。

    只比速率的话这里会判"完全一致、什么都不做"，坏桶就永远留着——修复
    等于没上线。所以 cburst 偏小也要算作漂移，走 class change 重写。
    """
    # 兜底类是好的，只有 frontend 的 cburst 坏了——这样走的才是 rate-change
    # 那条不打断连接的路径（兜底类坏了只能重建，见下一条）。
    only_fe_bad = (
        "class htb 1:1 root prio 0 rate 100Gbit ceil 100Gbit burst 8Mb cburst 8Mb\n"
        "class htb 1:1f90 root leaf 1f90: prio 0 rate 40Mbit ceil 40Mbit "
        "burst 50000b cburst 1600b\n")
    sh, fake = shaper(classes=only_fe_bad, filters=FILTERS_OK)
    res = await sh.reconcile([fe(port=8080, quota=40.0)])   # 限额与现状相同
    assert res.ok and res.changed, "cburst 坏了就不能报告『无变化』"
    cmds = [" ".join(c) for c in fake.mutations()]
    assert any("class change" in c and "cburst 50000" in c for c in cmds), cmds


async def test_upgrade_repairs_default_class_cburst_by_rebuilding():
    """兜底类的 cburst 坏了更严重——它承载全机未受管流量，而且不属于任何
    frontend，rate-change 那条路径根本不会碰它。只能靠重建修。

    这一条就是事故的核心：所有 frontend 的速率都显示正常，整机却在降速。
    """
    good_fe_bad_default = (
        "class htb 1:1 root prio 0 rate 100Gbit ceil 100Gbit burst 2400b cburst 2400b\n"
        "class htb 1:1f90 root leaf 1f90: prio 0 rate 40Mbit ceil 40Mbit "
        "burst 50000b cburst 50000b\n")
    sh, fake = shaper(classes=good_fe_bad_default, filters=FILTERS_OK)
    res = await sh.reconcile([fe(port=8080, quota=40.0)])
    assert res.action == "rebuild", "兜底类只能靠重建修"
    cmds = [" ".join(c) for c in fake.mutations()]
    assert any("classid 1:1 htb" in c and "cburst 8388608" in c for c in cmds), cmds


def test_parse_size_handles_tc_1024_based_units():
    """tc 打字节数按 1024 折算且**有损**：5000000 会打成 "4883Kb"（5000192）。
    精确比对必然误判，所以 _cburst_too_small 留了余量。"""
    assert T.parse_size("1600b") == 1600
    assert T.parse_size("8Mb") == 8 * 1024 ** 2
    assert T.parse_size("4883Kb") == 5000192
    # 折算误差不该被当成"偏小"
    assert not T._cburst_too_small(5000192, 4_000_000_000)
    assert T._cburst_too_small(1600, 4_000_000_000)
    assert T._cburst_too_small(None, 4_000_000_000)
