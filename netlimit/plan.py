# netlimit.plan —— 把"整机限多少"翻译成一串 tc 命令。
#
# 目标很窄：**本机全部出向流量合起来不超过一个总额度**，不管它从哪张网卡
# 出去、源地址是哪个 IP。窄目标带来一个很大的好处——**不需要任何分类器**。
# 受限流量就是 HTB 的兜底类，没被免限规则挑走的全部落进去。按端口/按 IP
# 分类的那一整类坑（u32 的进制、临时端口撞车、每加一个对象就重建树）在这
# 里根本不存在。
#
# 两种形态，由"要限几张网卡"决定：
#
# ── 单网卡 ────────────────────────────────────────────────────────────
#   HTB 直接挂在网卡上。没有任何额外机制，也就没有任何额外风险。
#
#       eth0 ── [HTB 1:2 = 总额度] ──> 线路
#
# ── 多网卡 ────────────────────────────────────────────────────────────
#   各网卡的出向先用 clsact 的 egress 钩子 + matchall + mirred 重定向到一个
#   IFB 设备，HTB 挂在 IFB 上。这样**一份令牌桶服务所有网卡** = 真正的整机
#   总闸门。
#
#       eth0 ─┐
#       eth1 ─┼─(mirred egress redirect)─> ifb0 ── [HTB 1:2 = 总额度] ─> 各自的线路
#       eth2 ─┘
#
#   为什么不能"每张网卡各挂一棵 HTB"：那是 N 张网卡 N 份额度，总量变成
#   N 倍，与"整机一个总闸门"直接矛盾。把总额度除以 N 也不行——流量在网卡
#   之间从来不是均分的，除完的结果是某张网卡先被掐死而总带宽远没用满。
#
# 两条从上一个项目用真实事故换来的硬规则，这里原样保留：
#
#   1. **classid / qdisc handle 的次要号按十六进制解析**（iproute2 的
#      get_tc_classid 用 strtoul(str, &p, 16)）。本模块的次要号都是很小的
#      常量（1/2），十进制与十六进制同形，看起来无所谓——但仍然统一用
#      _minor() 生成，免得以后有人加一个 >9 的次要号时踩进去。
#   2. **HTB 的 cburst 必须显式给**。cburst 管的是 ceil 那一路的桶，漏给的
#      话 tc 会兜底成 MTU 量级，速率显示完全正常、实际吞吐却远低于限额。
#      上一个项目就是这么让整机降速的，查了很久。

from __future__ import annotations

from dataclasses import dataclass

# ── tc 对象的编号 ────────────────────────────────────────────────────────
ROOT_HANDLE = "1:"
# 兜底/免限类：按线速放行，承载免限规则挑出来的流量。
FREE_MINOR = 1
# 整机限速类：HTB 的 default，没被挑走的全部落进来。
LIMIT_MINOR = 2
# 免限类的速率。给一个远高于任何真实链路的值 = 实际不整形。
FREE_RATE_BPS = 100_000_000_000        # 100 Gbit/s
LEAF_QDISC = "fq_codel"

# 默认免限的本机服务端口。整机限速把本机全部出向都罩住，SSH 也不例外；
# 链路打满时登不上机器是运维事故，而 SSH 的流量量级完全可以忽略。
DEFAULT_EXEMPT_PORTS = (22,)

# 令牌桶按"约 10 毫秒的额度"给，并设一个下限。
# 太小：达不到限额（桶还没攒够就得等下一个时钟）；
# 太大：秒级限速失真（一瞬间放过去一大坨）。
BURST_SECONDS = 0.01
MIN_BURST_BYTES = 4 * 1024
# 上限。免限类的速率是 100 Gbit/s，按 10 毫秒算出来是 125 MB——那个数字
# 没有意义（那条路本来就不整形），而且过大的 burst 会让 tc 内部的时间换算
# 失真。8 MiB 是上一个项目在真机上用过的值。
MAX_BURST_BYTES = 8 * 1024 * 1024


class PlanError(Exception):
    """计划本身不成立——**在下发任何一条命令之前**抛出。"""


def _minor(n: int) -> str:
    """tc 的次要号：一律按十六进制写。见模块头第 1 条。"""
    if not (1 <= n <= 0xFFFF):
        raise PlanError(f"tc 次要号 {n} 超出 16 位范围")
    return f"{n:x}"


def classid(n: int) -> str:
    return f"{ROOT_HANDLE}{_minor(n)}"


def burst_bytes(rate_bits_per_s: int) -> int:
    """该速率下的令牌桶大小（字节）。"""
    want = int(rate_bits_per_s / 8 * BURST_SECONDS)
    return max(MIN_BURST_BYTES, min(MAX_BURST_BYTES, want))


def _htb_rate_args(rate_bits_per_s: int) -> list[str]:
    """HTB 的速率四件套。**burst 与 cburst 必须成对显式给**，见模块头第 2 条。"""
    b = str(burst_bytes(rate_bits_per_s))
    return ["rate", f"{rate_bits_per_s}bit", "ceil", f"{rate_bits_per_s}bit",
            "burst", b, "cburst", b]


@dataclass(frozen=True)
class LimitPlan:
    """一次限速下发要表达的全部意图。"""

    # 要限的网卡（已经过 discover 筛选）。
    ifaces: tuple[str, ...] = ()
    # 整机总额度（bit/s）。**None = 没设 = 不限速**，这是默认。
    # None 与 0 是两件事：0 会被判成配错了（0 Mbps 谁也跑不动），
    # 用 0 兼表"没设"会让打错一个字静默变成不限速。
    rate_bits_per_s: int | None = None
    # 免限端口（本机作为服务端的端口，按源端口匹配）。
    exempt_ports: tuple[int, ...] = DEFAULT_EXEMPT_PORTS
    # 多网卡聚合用的 IFB 设备名。
    ifb: str = "ifb0"

    @classmethod
    def from_mbps(cls, ifaces, mbps: float | None, **kw) -> "LimitPlan":
        """限额的配置单位一律是 Mbps；换算成 bit/s 只在这一处发生。"""
        return cls(
            ifaces=tuple(ifaces),
            rate_bits_per_s=(None if mbps is None
                             else int(round(mbps * 1_000_000))),
            **kw)

    @property
    def off(self) -> bool:
        """这份计划是不是"不限速"。"""
        return self.rate_bits_per_s is None

    @property
    def aggregated(self) -> bool:
        """是否要经 IFB 聚合（要限的网卡多于一张）。"""
        return len(self.ifaces) > 1

    @property
    def shaping_dev(self) -> str:
        """HTB 实际挂在哪个设备上。"""
        return self.ifb if self.aggregated else (self.ifaces[0] if self.ifaces else "")

    def describe(self) -> str:
        if not self.ifaces:
            return "没有要限的网卡"
        who = ",".join(self.ifaces)
        if self.off:
            return f"不限速（网卡 {who}）"
        mbps = self.rate_bits_per_s / 1e6
        how = f"经 {self.ifb} 聚合" if self.aggregated else "直接挂在网卡上"
        ex = ",".join(map(str, self.exempt_ports)) or "无"
        return f"整机出向 {mbps:g} Mbps（网卡 {who}，{how}；免限端口 {ex}）"


def check(plan: LimitPlan) -> None:
    """下发前的最后一道校验。**任何一条不过就一条命令都不发。**"""
    if not plan.ifaces:
        raise PlanError(
            "没有要限的网卡。整机限速至少得有一张网卡可限——"
            "用 `netlimit status` 看自动发现排除了什么，或用 --only 显式指定")
    for name in plan.ifaces:
        if not name or "/" in name or len(name) > 15:
            raise PlanError(f"网卡名不合法：{name!r}")
    if len(set(plan.ifaces)) != len(plan.ifaces):
        raise PlanError(f"网卡名重复：{plan.ifaces}")
    if plan.aggregated and plan.ifb in plan.ifaces:
        # 把聚合用的 IFB 自己也当成待限网卡，会让流量被重定向回自己：
        # 同一份字节整形两遍，实际吞吐掉到限额的一半以下。
        raise PlanError(
            f"聚合设备 {plan.ifb} 出现在待限网卡里——它会把流量重定向给自己，"
            f"同一份字节被整形两遍")
    if plan.off:
        return                          # 没设限额 = 不限速，合法（且是默认）
    if plan.rate_bits_per_s < 8:
        raise PlanError(
            f"整机限额 {plan.rate_bits_per_s} bit/s 不足 1 字节/秒，无法整形。"
            f"不想限速就把限额留空，而不是填 0")
    for p in plan.exempt_ports:
        if not (1 <= p <= 65535):
            raise PlanError(f"免限端口 {p} 越界")


# ── 命令生成 ─────────────────────────────────────────────────────────────
#
# 返回 (argv, fatal)：fatal 为假的命令失败只记警告不中断——删一个本来就
# 不存在的 qdisc、老内核没有 fq_codel，都属于这一类。**其余一律致命**：
# 限速这种事，"失败了但继续跑"等于静默不生效。

Cmd = tuple[list[str], bool]


def _shaping_tree(dev: str, plan: LimitPlan) -> list[Cmd]:
    """在 dev 上建那棵固定的队列树：一个免限类 + 一个整机限速类。

    树的形状与网卡数量、IP 数量都无关——这正是整机限速相对按对象限速最大
    的好处：加地址、加网卡、加业务都不会动这棵树。
    """
    rate = plan.rate_bits_per_s
    assert rate is not None                     # 调用方已判过 plan.off
    cmds: list[Cmd] = [
        (["tc", "qdisc", "del", "dev", dev, "root"], False),
        (["tc", "qdisc", "add", "dev", dev, "root", "handle", ROOT_HANDLE,
          "htb", "default", _minor(LIMIT_MINOR)], True),
        (["tc", "class", "add", "dev", dev, "parent", ROOT_HANDLE,
          "classid", classid(FREE_MINOR), "htb", *_htb_rate_args(FREE_RATE_BPS)], True),
        (["tc", "class", "add", "dev", dev, "parent", ROOT_HANDLE,
          "classid", classid(LIMIT_MINOR), "htb", *_htb_rate_args(rate)], True),
        # 类内公平：一条大象连接不该把整台机器的额度吃光。老内核没有
        # fq_codel 时不致命——缺了只是失去公平性，限速本身照常。
        (["tc", "qdisc", "add", "dev", dev, "parent", classid(LIMIT_MINOR),
          "handle", f"{_minor(LIMIT_MINOR)}:", LEAF_QDISC], False),
    ]
    # 免限规则：把本机服务端口（SSH）挑进免限类。
    # IPv4 与 IPv6 各一条——只写 IPv4 的话，链路打满时 IPv6 的 SSH 照样
    # 进不来，而那往往是最后一条能用的路。
    for port in plan.exempt_ports:
        for proto, match in (("ip", "ip"), ("ipv6", "ip6")):
            cmds.append((
                ["tc", "filter", "add", "dev", dev, "protocol", proto,
                 "parent", ROOT_HANDLE, "prio", "1", "u32",
                 "match", match, "sport", str(port), "0xffff",
                 "flowid", classid(FREE_MINOR)], True))
    return cmds


def _redirect_cmds(iface: str, plan: LimitPlan) -> list[Cmd]:
    """把一张网卡的**出向**全部重定向到 IFB 设备。

    clsact 是专门为"在收发两侧挂过滤器但不接管排队"设计的 qdisc：它不改变
    该网卡自己的排队行为，只提供 ingress/egress 两个钩子。所以原网卡上仍然
    是默认的 pfifo_fast，整形只发生在 IFB 上——一份令牌桶，多张网卡共享。

    matchall 匹配一切，不需要任何字段判断，因此**与源 IP 无关**：单网卡多
    IP 也好、多网卡多 IP 也好，全部照单重定向。
    """
    return [
        (["tc", "qdisc", "del", "dev", iface, "clsact"], False),
        (["tc", "qdisc", "add", "dev", iface, "clsact"], True),
        (["tc", "filter", "add", "dev", iface, "egress", "matchall",
          "action", "mirred", "egress", "redirect", "dev", plan.ifb], True),
    ]


def build(plan: LimitPlan) -> list[Cmd]:
    """整套下发序列。**先校验，不通过一条都不发。**"""
    check(plan)
    if plan.off:
        return teardown(plan)

    if not plan.aggregated:
        # 单网卡：最省事的一条路，没有 IFB、没有重定向。
        return _shaping_tree(plan.ifaces[0], plan)

    cmds: list[Cmd] = [
        # IFB 设备可能已经在（上一次跑留下的），所以 add 失败不致命；
        # 但紧接着的 `link set up` 必须成功——设备没 up 的话重定向过去的
        # 包会被直接丢掉，表现为**整机断网**，这是最不能忍的失败方式。
        (["ip", "link", "add", plan.ifb, "type", "ifb"], False),
        (["ip", "link", "set", plan.ifb, "up"], True),
    ]
    cmds += _shaping_tree(plan.ifb, plan)
    for iface in plan.ifaces:
        cmds += _redirect_cmds(iface, plan)
    return cmds


def teardown(plan: LimitPlan) -> list[Cmd]:
    """撤掉本工具建立的一切，把机器还原成不限速。

    全部非致命：拆的时候东西本来就可能不在（没建过、被人手工删过、
    上一次拆了一半）。拆不干净比拆报错更危险，所以每一条都发出去。
    """
    cmds: list[Cmd] = []
    for iface in plan.ifaces:
        cmds.append((["tc", "qdisc", "del", "dev", iface, "clsact"], False))
        cmds.append((["tc", "qdisc", "del", "dev", iface, "root"], False))
    cmds.append((["tc", "qdisc", "del", "dev", plan.ifb, "root"], False))
    # IFB 设备本身留着不删：删了对下一次生效毫无帮助，反而在别的工具也
    # 用着 ifb0 时把人家掀了。
    return cmds


def rate_change_cmd(plan: LimitPlan) -> list[str]:
    """只改额度、不动树。存量连接立刻跟上，一个连接都不断。

    整机这个总闸门被调的频率只会比单个业务更高（扩容、削峰、临时放行），
    每次都重建整棵树的话，就是每次调限额都抖一下全机流量。
    """
    check(plan)
    if plan.off:
        raise PlanError("不限速不能靠改速率实现，要拆掉整棵树（见 teardown）")
    return ["tc", "class", "change", "dev", plan.shaping_dev,
            "parent", ROOT_HANDLE, "classid", classid(LIMIT_MINOR), "htb",
            *_htb_rate_args(plan.rate_bits_per_s)]
