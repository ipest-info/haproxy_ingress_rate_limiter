# netlimit.discover —— 本机网卡与 IP 的发现：到底该限哪几张网卡。
#
# 这是"全网限速"与"给某个服务限速"最不一样的地方：没有配置文件告诉你要限
# 什么，得自己把这台机器的网络长相摸清楚。而机器的长相有两种常见形态，
# 都要能对：
#
#   单网卡多 IP    一张物理网卡上绑了一堆地址（云主机加弹性 IP 的常态）
#   多网卡多 IP    几张网卡，每张各带若干地址
#
# **多 IP 对限速树本身毫无影响**——整机一个总闸门不按 IP 分类，几个 IP 都
# 走同一个闸门。IP 在这里只用于两件事：给运维看清楚限的是哪台机器的哪些
# 地址，以及判断某张网卡是不是真的在用（没有任何地址的网卡不值得限）。
#
# 真正影响架构的是**网卡有几张**：
#   1 张 → HTB 直接挂在它上面，最简单，没有任何额外机制；
#   ≥2 张 → 各网卡的出向要先汇聚到一个 IFB 设备上再整形，否则"整机一个
#           总闸门"不成立（每张网卡各挂一棵 HTB = N 份额度）。
#
# 哪些网卡要排除，是这个模块最容易出事的地方，所以规则写死并逐条说明：
# 限错网卡的后果不是"限速不准"，而是**把本机的容器网络/回环流量一起掐住**。

from __future__ import annotations

import os
import re
from dataclasses import dataclass, field

SYS_NET = "/sys/class/net"

# 默认排除的网卡（按名字前缀匹配）。每一条都有具体理由，不是随手列的：
#
#   lo          回环。限它等于限本机进程之间的通信，纯粹的自伤。
#   ifb         我们自己用来做聚合的虚拟设备。限它 = 自己限自己，会形成
#               "整形后的流量再进一次整形"的重复计费。
#   docker/br-  Docker 的网桥。容器出公网的流量最终还是要从物理网卡出去，
#               在网桥上再限一次 = 同一份字节被限两遍。
#   veth        容器/netns 的一端，同上，而且名字随容器增删不断变化。
#   virbr/vnet  libvirt 的网桥与虚机 tap，同理。
#   tun/tap     VPN 隧道。隧道内的包出去时还会经过物理网卡，限两遍。
#   wg          WireGuard，同上。
#   bond 的成员口也要排除，但那个判断不看名字（见 _is_bond_slave）。
DEFAULT_EXCLUDE_PREFIXES = (
    "lo", "ifb", "docker", "br-", "veth", "virbr", "vnet", "tun", "tap", "wg",
)


@dataclass(frozen=True)
class Link:
    """一张网卡，以及它上面的地址。"""

    name: str
    # 运行状态："up" / "down" / "unknown"（/sys/class/net/X/operstate 原文）。
    operstate: str
    # 是否有载波（网线插着 / 虚拟设备就绪）。没有 carrier 的网卡限了也没用。
    carrier: bool
    # 这张网卡上的地址。单网卡多 IP 时这里会有很多个——**不影响限速树**，
    # 只用于展示与"这张网卡在不在用"的判断。
    addrs_v4: tuple[str, ...] = ()
    addrs_v6: tuple[str, ...] = ()
    # 物理网卡（有 device 符号链接）还是纯虚拟设备。
    physical: bool = False
    # 是不是某个 bond/team 的成员口。
    bond_slave: bool = False
    # 排除原因；非空即表示不限这张网卡。
    skip_reason: str = ""

    @property
    def addr_count(self) -> int:
        return len(self.addrs_v4) + len(self.addrs_v6)

    @property
    def shaped(self) -> bool:
        return not self.skip_reason

    def describe(self) -> str:
        v4 = ",".join(self.addrs_v4) or "-"
        n6 = f" +{len(self.addrs_v6)}个IPv6" if self.addrs_v6 else ""
        tail = f"  跳过：{self.skip_reason}" if self.skip_reason else ""
        return f"{self.name:<12} {self.operstate:<8} {v4}{n6}{tail}"


@dataclass
class Topology:
    """本机网络长相的一次快照。"""

    links: list[Link] = field(default_factory=list)

    @property
    def shaped(self) -> list[Link]:
        return [x for x in self.links if x.shaped]

    @property
    def skipped(self) -> list[Link]:
        return [x for x in self.links if not x.shaped]

    @property
    def needs_ifb(self) -> bool:
        """要限的网卡多于一张时，必须走 IFB 聚合。

        这是整个方案里唯一由"机器长相"决定的架构分叉：一张网卡时 HTB 直接
        挂上去就行；两张以上，各挂各的 HTB 就变成"每张网卡一份额度"，
        与"整机一个总闸门"直接矛盾。
        """
        return len(self.shaped) > 1

    def summary(self) -> str:
        n_ip = sum(x.addr_count for x in self.shaped)
        return (f"要限 {len(self.shaped)} 张网卡、共 {n_ip} 个地址"
                f"（{'经 IFB 聚合' if self.needs_ifb else '直接挂在网卡上'}）")


def _read(path: str, default: str = "") -> str:
    try:
        with open(path, encoding="utf-8", errors="replace") as f:
            return f.read().strip()
    except OSError:
        return default


def _is_bond_slave(name: str, sysfs: str = SYS_NET) -> bool:
    """bond/team 的成员口。

    必须排除：成员口上的整形毫无意义（调度发生在 bond 主设备上，成员口
    只是出口），而且两个成员口各限一份会让总量变成两倍。名字上看不出来
    （成员口就叫 eth0/eth1），只能看 /sys 里有没有 master 且 master 是 bond。

    sysfs 必须是参数而不是直接用常量：写死 SYS_NET 的话这段逻辑在测试里
    根本走不到——本机没有 bond，这条最容易写错的规则就永远没人验。
    """
    master = os.path.join(sysfs, name, "master")
    if not os.path.islink(master):
        return False
    mname = os.path.basename(os.path.realpath(master))
    # bonding 会在成员口上建 bonding_slave 目录；team 用 team_port。
    return (os.path.isdir(os.path.join(sysfs, name, "bonding_slave"))
            or os.path.isdir(os.path.join(sysfs, name, "team_port"))
            or os.path.isdir(os.path.join(sysfs, mname, "bonding")))


def _excluded_by_name(name: str, prefixes: tuple[str, ...]) -> str:
    for p in prefixes:
        # "lo" 要精确匹配，否则会把 "lolo0"（假想）之类误伤；其余按前缀。
        if (name == p) if p == "lo" else name.startswith(p):
            return f"名字匹配排除前缀 {p!r}"
    return ""


_IP_LINE = re.compile(r"^\s*inet6?\s+([0-9a-fA-F:.]+)/\d+")


def parse_ip_addr(output: str) -> dict[str, tuple[list[str], list[str]]]:
    """解析 `ip -o addr show` 的输出，返回 网卡 → (IPv4 列表, IPv6 列表)。

    用 `-o`（每条一行）而不是默认多行格式：后者的缩进与字段顺序在
    iproute2 各版本间会变，按行解析要脆得多。

    链路本地地址（169.254.0.0/16、fe80::/10）被丢掉——它们不代表这张网卡
    真的在承载业务流量，留着只会让"这张网卡有地址"的判断失真。
    """
    out: dict[str, tuple[list[str], list[str]]] = {}
    for line in output.splitlines():
        # 形如: "2: eth0    inet 10.0.0.5/24 brd ... scope global eth0\..."
        parts = line.split()
        if len(parts) < 4:
            continue
        name = parts[1]
        if name.endswith(":"):          # 某些版本会带冒号
            name = name[:-1]
        fam, addr = parts[2], parts[3].split("/")[0]
        if fam not in ("inet", "inet6"):
            continue
        if addr.startswith("169.254.") or addr.lower().startswith("fe80:"):
            continue
        v4, v6 = out.setdefault(name, ([], []))
        (v4 if fam == "inet" else v6).append(addr)
    return out


def discover(
    ip_addr_output: str,
    names: list[str] | None = None,
    exclude_prefixes: tuple[str, ...] = DEFAULT_EXCLUDE_PREFIXES,
    include: tuple[str, ...] = (),
    exclude: tuple[str, ...] = (),
    sysfs: str = SYS_NET,
) -> Topology:
    """摸清本机网络长相。

    ip_addr_output 由调用方给（`ip -o addr show` 的输出），这样这个函数是
    纯的、可测；真正跑命令的是 cli 层。

    include 非空时**只**限这些网卡（运维显式点名，绕过全部自动判断——
    自动判断再周全也有猜错的时候，必须留一个手动出口）。
    exclude 是在自动判断之外额外排除的名字。
    """
    if names is None:
        try:
            names = sorted(os.listdir(sysfs))
        except OSError:
            names = []
    addrs = parse_ip_addr(ip_addr_output)

    # /sys/class/net 是设备的权威清单；`ip addr` 只用来给它们标上地址。
    # 早先这里对两者取并集，结果是调用方给了 names 也限定不住范围——
    # 真机上两者本就一致，所以这个 bug 只会在测试和"指定网卡"时露头。
    links: list[Link] = []
    for name in sorted(names):
        v4, v6 = addrs.get(name, ([], []))
        operstate = _read(os.path.join(sysfs, name, "operstate"), "unknown")
        carrier = _read(os.path.join(sysfs, name, "carrier")) == "1"
        physical = os.path.exists(os.path.join(sysfs, name, "device"))
        slave = _is_bond_slave(name, sysfs)

        if include:
            reason = "" if name in include else "不在 --only 指定的网卡里"
        elif name in exclude:
            reason = "运维显式排除（--exclude）"
        elif slave:
            reason = "bond/team 成员口（应在主设备上限速）"
        else:
            reason = _excluded_by_name(name, exclude_prefixes)
            if not reason and operstate == "down":
                reason = "网卡处于 down 状态"
            elif not reason and not v4 and not v6:
                # 没有任何地址的网卡不承载流量。限它不会出错，但会让
                # "限了几张网卡"这个数字变得没有意义，排障时容易误导。
                reason = "没有配置任何地址"

        links.append(Link(
            name=name, operstate=operstate, carrier=carrier,
            addrs_v4=tuple(v4), addrs_v6=tuple(v6),
            physical=physical, bond_slave=slave, skip_reason=reason))
    return Topology(links=links)
