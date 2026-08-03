# rl_limiter.tcshaper —— 用内核 tc（HTB）做限速，取代 HAProxy 的 shared bwlim。
#
# ## 为什么换掉 bwlim
#
# 实测（HAProxy 2.8.16）：**只要 frontend 上挂了 bwlim 滤镜，HAProxy 就会
# 完全关闭内核 splice（零拷贝转发）**——
#
#   | 场景            | 出向字节  | 其中走 splice 的 |
#   | --------------- | --------- | ---------------- |
#   | bwlim + splice  | 74867160  | **0**            |
#   | 无 bwlim        | 131073235 | 131072000        |
#
# 道理说得通：限速要按字节计量并延迟发送，数据必须经过用户态。代价是同样
# 的转发量要多花一倍 CPU（同吞吐 2000 Mbps 下实测，各测两次）：
#
#   | 方案                  | HAProxy CPU | CPU 秒/GB   | splice |
#   | --------------------- | ----------- | ----------- | ------ |
#   | bwlim 限速            | 14~16%      | 0.56~0.63   | 0%     |
#   | 无 bwlim + splice     | 8%          | 0.32~0.33   | 100%   |
#
# 把限速下沉到内核后，HAProxy 只做纯转发、splice 全程可用，CPU 减半。
#
# **诚实的边界**：上表测的是 **HAProxy 进程**的 CPU。tc 自己的整形开销发生
# 在内核 softirq 上下文里，不计入该进程，本项目没有测过（见文件末尾"未验证
# 的部分"）。所以正确的说法是"HAProxy 侧 CPU 减半"，不是"整机 CPU 减半"。
#
# ## 怎么把"按 frontend 限速"映射到 tc
#
# tc 作用在网卡上，不认识 frontend。但有一个天然的对应关系：**HAProxy 发给
# 客户端的数据包，源端口就是该 frontend 的监听端口**。于是在出方向网卡上
# 按源端口分类即可：
#
#     tc qdisc add dev eth0 root handle 1: htb default 1
#     tc class add dev eth0 parent 1: classid 1:1    htb rate <线速>      # 兜底类，不整形
#     tc class add dev eth0 parent 1: classid 1:1f90 htb rate 40mbit ceil 40mbit
#     tc filter add dev eth0 protocol ip parent 1: prio 1 u32 \
#         match ip sport 8080 0xffff flowid 1:1f90
#
# **classid 的次要号直接取监听端口**：端口在本模型里天然唯一（一个 frontend
# = 一个监听端口），因此 classid 稳定且无需额外分配表——改配置、重排序、
# 增删 frontend 都不会让别的 frontend 的 classid 漂移。次要号 1 留给兜底类，
# 所以拒绝监听 1 端口（那也不是现实中会用的端口）。
#
# 注意上面 8080 出现了两次、写法却不同：**classid 的次要号是十六进制**
# （0x1f90 = 8080），而 u32 的 `match ip sport` 取的是十进制。两边格式不同
# 是 tc 自己的约定，不是笔误——写混了的后果见 classid_for 的注释。
#
# ## 与 bwlim 的三处行为差异（都要向运维讲清楚）
#
#   1. **口径**：bwlim 数的是应用层字节；tc 数的是链路层字节，含以太网/IP/
#      TCP 头部与重传。同样填 40 Mbps，tc 方案的应用层吞吐会略低（头部开销
#      典型 3%~8%）。要抵消可给 class 加 `overhead`/`linklayer` 参数。
#   2. **作用范围**：bwlim 只管这个 frontend；tc 管的是网卡出方向按源端口
#      匹配到的**全部**流量。如果同一端口上还有非 HAProxy 的流量（不该有），
#      也会一起被限。
#   3. **生效时机**：bwlim 改限额要 reload HAProxy；tc 改限额是
#      `tc class change`，**连 reload 都不需要**，存量连接立刻跟上新限额——
#      这比 bwlim 方案还好（bwlim 下存量连接要等 hard-stop-after 宽限期）。
#
# ## 安全边界
#
# 本模块会以 root（或 CAP_NET_ADMIN）执行 tc 命令，因此
#   - **命令一律用 argv 列表拼装，绝不拼 shell 字符串**，配置里的值（端口、
#     限额）在进入 argv 之前全部过整数校验，不存在注入面；
#   - 网卡名只来自本机环境变量/自动探测，绝不从配置文件读；
#   - 空清单拒绝执行：那意味着"把所有限速撤掉"，是事故而不是配置操作。

from __future__ import annotations

import asyncio
import json
import logging
import re
from dataclasses import dataclass, field

from . import model

# 根 qdisc 句柄。整个方案只用一层 HTB，够用且好读。
ROOT_HANDLE = "1:"
# 兜底类的次要号：没被任何 filter 匹配到的流量（SSH、监控、后端方向的
# 连接…）都落在这里，按线速跑，等于不整形。
DEFAULT_CLASS_MINOR = 1
# 兜底类的速率。取一个远高于任何真实网卡的值 = 实际不构成约束。
DEFAULT_CLASS_RATE_BPS = 100_000_000_000  # 100 Gbit/s

# burst：HTB 令牌桶的深度。太小则达不到设定速率（每个调度周期都被卡住），
# 太大则限速在短时间尺度上形同虚设。取"10 毫秒的额度"是常见工程取值，
# 并保证不低于两个 MTU（否则大包根本发不出去）。
BURST_SECONDS = 0.01
MTU_BYTES = 1500
MIN_BURST_BYTES = 2 * MTU_BYTES
MAX_BURST_BYTES = 8 * 1024 * 1024

# 叶子 qdisc：在每个限速类下面挂一个公平队列，避免单条连接把该类的队列
# 占满导致同 frontend 的其它连接饿死。fq_codel 还能压低排队时延。
# 老内核可能没有这个 qdisc，因此它的失败**不致命**（见 _rebuild_cmds）。
LEAF_QDISC = "fq_codel"

# 本机临时端口范围（内核给出向连接分配源端口的区间）。
PROC_EPHEMERAL_RANGE = "/proc/sys/net/ipv4/ip_local_port_range"


@dataclass(slots=True)
class TcClassStat:
    """单个 HTB 类的统计（= 单个 frontend 的出方向实况）。

    **这是限速迁到 tc 之后白捡的监控能力**：tc 的每个 class 天然带
    发送字节/包数、丢包数、超限次数，而每个 class 正好对应一个
    frontend——于是"按监听端口统计数据包/丢包"这件事第一次成立了
    （HAProxy 自己完全不统计数据包，网卡计数又无法按 frontend 拆）。

    口径：链路层字节（含以太网/IP/TCP 头），只有**出方向**（tc 挂在
    出向队列上，入向看不到）。
    """

    port: int              # = classid 次要号 = frontend 的监听端口
    bytes: int = 0         # 已发送字节（链路层口径）
    packets: int = 0       # 已发送包数
    drops: int = 0         # 被丢弃的包数——限速丢包就统计在这里
    overlimits: int = 0    # 触发限速被延迟的次数（限额是否吃紧的直接信号）
    backlog: int = 0       # 当前排队字节数
    qlen: int = 0          # 当前排队包数


class TcError(RuntimeError):
    """tc 操作失败（命令返回非零、网卡不存在、配置不可整形等）。"""


@dataclass
class TcResult:
    """一次 reconcile 的结果。"""

    ok: bool
    changed: bool
    # 本次生效的 frontend 名（按名字排序），供控制台展示。
    frontends: list[str] = field(default_factory=list)
    # changed 为真时说明走的是哪条路径："rebuild"（重建整棵树）或
    # "rate-change"（只改速率，不动结构、不打断流量）。
    action: str = ""
    error: str = ""


def ephemeral_range(path: str = PROC_EPHEMERAL_RANGE) -> tuple[int, int]:
    """读本机的临时端口范围。读不到就返回 Linux 的常见默认值。"""
    try:
        with open(path, encoding="ascii") as f:
            lo, hi = f.read().split()[:2]
        return int(lo), int(hi)
    except (OSError, ValueError):
        return 32768, 60999


def ephemeral_conflicts(frontends: list[model.FrontendConfig],
                        rng: tuple[int, int] | None = None) -> list[str]:
    """挑出**监听端口落在临时端口范围内**的 frontend 名。

    为什么这是个真问题（**单网卡部署时**）：本模块的分类规则只匹配源端口
    ——`match ip sport <监听端口>`。HAProxy 发给客户端的包源端口确实是监听
    端口，但**HAProxy 连后端的包，源端口是内核分配的临时端口**，而单网卡
    时它们从同一张网卡出去。一旦某次连后端抽到的临时端口正好等于某个
    frontend 的监听端口，那条连接的出向流量就会被误判进该 frontend 的限速
    类，吃掉本该给客户端的配额——而且是**随机偶发**的，极难排查。

    两条出路（见 docs/06 的"单网卡还是两网卡"）：
      1. 把监听端口挪到临时端口范围之外（<32768，也就是 80/443/8080 这类
         正常的入口端口）——推荐，零成本；
      2. 用两张网卡，只在客户端侧那张上限速，后端流量根本不经过限速树。

    返回空列表表示没有这个风险。
    """
    lo, hi = rng if rng is not None else ephemeral_range()
    return [f.name for f in frontends if lo <= f.bind_port <= hi]


def burst_bytes(rate_bytes_per_s: float) -> int:
    """按限额算 HTB 的 burst（字节）。

    取 10ms 的额度并夹在 [2×MTU, 8MB]：下限保证大包发得出去，上限避免
    高限额下攒出一个大到让秒级限速失真的桶。

    **burst 与 cburst 必须都显式给，两个都不能漏**（线上事故，见下）。
    """
    return max(MIN_BURST_BYTES,
               min(MAX_BURST_BYTES, int(rate_bytes_per_s * BURST_SECONDS)))


def _htb_rate_args(rate_bits_per_s: int) -> list[str]:
    """一个 HTB 类的 rate/ceil/burst/cburst 四件套。

    ## 为什么 cburst 必须显式给（一次线上事故）

    HTB 有两个令牌桶：`rate` 那路由 `burst` 控制，`ceil` 那路由 **`cburst`**
    控制。本项目 rate == ceil，所以**真正决定吞吐上限的是 cburst**。

    漏掉任何一个，iproute2 会自己算一个默认值：

        burst  = rate / get_hz() + mtu
        cburst = ceil / get_hz() + mtu

    现代内核的 psched 时钟是纳秒级（/proc/net/psched 第三字段 1000000），
    第一项几乎归零，于是**无论速率填多大，算出来都只有一个 MTU 的量级**。
    线上实测到的就是这个：

        class htb 1:1    rate 100Gbit ceil 100Gbit burst 2400b    cburst 2400b
        class htb 1:378f rate 4Gbit   ceil 4Gbit   burst 5000000b cburst 1600b

    桶只有 1600~2400 字节，每个调度周期就只能放这么多出去——**类的实际
    吞吐被压在远低于配置速率的水平**，而 `tc class show` 里的 `rate` 却
    显示得好好的，不看 cburst 根本发现不了。

    后果比"限速类跑不满"严重得多：**没被 filter 匹配的流量全都落在兜底类
    1:1 里**，也就是这台机器上除受管端口之外的所有流量。事故现场的表现是
    整机出向被压到入向的 80%（899 vs 1115 Mbps），`tc qdisc del root` 之后
    立刻恢复。
    """
    burst = burst_bytes(rate_bits_per_s / 8)
    return ["rate", f"{rate_bits_per_s}bit", "ceil", f"{rate_bits_per_s}bit",
            "burst", str(burst), "cburst", str(burst)]


def classid_for(port: int) -> str:
    """监听端口 → classid。次要号直接取端口，理由见文件头。

    **次要号必须按十六进制写**。iproute2 解析 classid 用的是
    `strtoul(str, &p, 16)`（lib/utils.c 的 get_tc_classid），也就是说
    `1:14223` 里的 14223 会被当成 0x14223 = 82467，超出次要号的 16 位上限
    直接报错：

        Error: argument "1:14223" is wrong: invalid class ID

    实测边界正好在端口 10000：1–9999 的十进制写法碰巧也是合法的十六进制
    （0x9999 = 39321 < 0xFFFF），所以"能用"——但它落到的次要号并不是端口
    本身，只是恰好自洽（写进去和读出来是同一个字符串）。**10000 以上的端口
    全部下发失败，那些 frontend 根本没有限速。**

    改成十六进制之后，次要号在数值上真正等于端口，1–65535 全部落在
    0x0001–0xFFFF 内。读回时务必用 int(minor, 16) 配对——tc 输出的
    classid 也是十六进制且不带 0x 前缀。
    """
    return f"{ROOT_HANDLE}{port:x}"


def _check_shapeable(frontends: list[model.FrontendConfig]) -> None:
    """进入 argv 之前的最后一道校验。

    这些值会被交给以 root 执行的 tc，虽然全部是整数、不存在注入面，但
    越界的值会让 tc 报出难懂的错误，不如在这里给出人话。
    """
    if not frontends:
        raise TcError(
            "受管 frontend 清单为空：那意味着撤掉全部限速，是事故而不是"
            "配置操作，已拒绝执行")
    seen: set[int] = set()
    for f in frontends:
        if not (1 <= f.bind_port <= 65535):
            raise TcError(f"frontend {f.name} 的监听端口 {f.bind_port} 越界")
        if f.bind_port == DEFAULT_CLASS_MINOR:
            raise TcError(
                f"frontend {f.name} 监听在 {DEFAULT_CLASS_MINOR} 端口，与 tc "
                f"兜底类的 classid 冲突（classid 次要号直接取端口，见模块头）。"
                f"换一个端口即可")
        if f.bind_port in seen:
            raise TcError(f"监听端口 {f.bind_port} 被多个 frontend 使用，"
                          f"无法按端口分类限速")
        seen.add(f.bind_port)
        if int(f.quota_bytes_per_sec) < 1:
            raise TcError(f"frontend {f.name} 的限额 {f.quota_mbps} Mbps "
                          f"不足 1 字节/秒，无法整形")


def desired_rates(frontends: list[model.FrontendConfig]) -> dict[int, int]:
    """期望状态：监听端口 → 限额（bit/s，tc 的口径）。

    内部一律 bytes/s，只在这里换回 bit/s——因为 tc 的 rate 参数用 bit。
    """
    return {f.bind_port: int(f.quota_bytes_per_sec) * 8 for f in frontends}


def _rebuild_cmds(iface: str,
                  frontends: list[model.FrontendConfig]) -> list[tuple[list[str], bool]]:
    """重建整棵 tc 树的命令序列。

    返回 (argv, fatal) 列表：fatal 为假的命令失败只记警告不中断——
    删除不存在的根 qdisc、老内核没有 fq_codel，都属于这一类。
    """
    cmds: list[tuple[list[str], bool]] = [
        # 先清掉旧树。首次运行时根本没有根 qdisc，报错是正常的，故非致命。
        (["tc", "qdisc", "del", "dev", iface, "root"], False),
        (["tc", "qdisc", "add", "dev", iface, "root", "handle", ROOT_HANDLE,
          "htb", "default", str(DEFAULT_CLASS_MINOR)], True),
        # 兜底类：没匹配到 filter 的流量走这里，线速放行。
        #
        # **这一条最要命**：本机上除受管端口之外的所有流量都落在这个类里。
        # 早先这里只给了 rate、没给 burst/cburst，tc 自己算出来的默认值只有
        # 2400 字节，于是整机流量被这个桶卡住——线上实测出向被压到入向的
        # 80%。详见 _htb_rate_args 的注释。
        (["tc", "class", "add", "dev", iface, "parent", ROOT_HANDLE,
          "classid", classid_for(DEFAULT_CLASS_MINOR), "htb",
          *_htb_rate_args(DEFAULT_CLASS_RATE_BPS)], True),
    ]
    for f in sorted(frontends, key=lambda x: x.bind_port):
        port = f.bind_port
        rate = int(f.quota_bytes_per_sec) * 8
        cid = classid_for(port)
        cmds.append((
            ["tc", "class", "add", "dev", iface, "parent", ROOT_HANDLE,
             "classid", cid, "htb", *_htb_rate_args(rate)], True))
        # 叶子队列：同一 frontend 内各连接之间公平排队。老内核可能没有
        # fq_codel，缺了只是失去类内公平性，限速本身不受影响 → 非致命。
        #
        # handle 的主要号和 classid 的次要号一样是**十六进制的 16 位数**，
        # 同样不能拿十进制端口去拼：
        #     tc qdisc add ... handle 14223: fq_codel
        #     Error: argument "14223:" is wrong: invalid qdisc ID
        # 这一条是非致命命令，失败只会记一行日志——真出事的话表现为
        # "限速在跑但类内没有公平队列"，比 class 失败更难发现。
        cmds.append((
            ["tc", "qdisc", "add", "dev", iface, "parent", cid,
             "handle", f"{port:x}:", LEAF_QDISC], False))
        # 分类：出方向、源端口 = 该 frontend 的监听端口。IPv4/IPv6 各一条
        # ——只写 IPv4 的话，客户端走 IPv6 进来时限速会整个失效。
        cmds.append((
            ["tc", "filter", "add", "dev", iface, "protocol", "ip",
             "parent", ROOT_HANDLE, "prio", "1", "u32",
             "match", "ip", "sport", str(port), "0xffff", "flowid", cid], True))
        cmds.append((
            ["tc", "filter", "add", "dev", iface, "protocol", "ipv6",
             "parent", ROOT_HANDLE, "prio", "1", "u32",
             "match", "ip6", "sport", str(port), "0xffff", "flowid", cid], True))
    return cmds


def rate_change_cmd(iface: str, port: int, rate_bits_per_s: int) -> list[str]:
    """只改某个类的速率，不动结构。

    这是本方案相对 bwlim 的一处实打实的优势：改限额不需要 reload HAProxy，
    **存量连接立刻按新限额跑**（bwlim 下存量连接要等 hard-stop-after 宽限期
    被断开重连才会跟上）。
    """
    return ["tc", "class", "change", "dev", iface, "parent", ROOT_HANDLE,
            "classid", classid_for(port), "htb", *_htb_rate_args(rate_bits_per_s)]


# `tc class show` 的一行形如（端口 8080 = 0x1f90）：
#   class htb 1:1f90 root prio 0 rate 40Mbit ceil 40Mbit burst 50000b cburst 50000b
#
# **classid 一律是十六进制**（tc 输出不带 0x 前缀），所以这里要认 a-f，
# 读回时也必须 int(minor, 16)。用 \d+ 会让 "1:378f" 整行匹配不上，比对
# 就以为这个类不存在，于是每一轮都重建——限速被反复推倒重来。
_CLASS_RE = re.compile(
    r"^class\s+htb\s+[0-9a-f]+:(?P<minor>[0-9a-f]+)\b.*?\brate\s+(?P<rate>\S+)",
    re.M | re.I)
# cburst 与 rate 在同一行，但可能在 rate 之前也可能之后，单独抓。
_CBURST_RE = re.compile(
    r"^class\s+htb\s+[0-9a-f]+:(?P<minor>[0-9a-f]+)\b.*?\bcburst\s+(?P<cburst>\S+)",
    re.M | re.I)
# tc 打印字节数时会按 1024 进制折算（sprint_size），且是**有损**的：
# 5000000 会打成 "4883Kb"（= 5000192）。所以比对 cburst 必须留余量，
# 见 CBURST_SLACK_BYTES。
_SIZE_UNITS = {"b": 1, "": 1, "kb": 1024, "mb": 1024 ** 2, "gb": 1024 ** 3}
# 比对 cburst 时的容差：tc 的 1024 进制折算最多差 1 KiB，给两倍余量。
# 只在"实际值明显偏小"时才判定漂移——偏大不是本模块会造成的故障。
CBURST_SLACK_BYTES = 2048


def parse_size(text: str) -> int:
    """把 tc 输出里的字节数（"1600b"、"4883Kb"、"8Mb"）解析成字节。"""
    m = re.fullmatch(r"(\d+(?:\.\d+)?)\s*([A-Za-z]*)", text.strip())
    if not m:
        raise TcError(f"无法解析 tc 字节数: {text!r}")
    unit = m.group(2).lower()
    if unit not in _SIZE_UNITS:
        raise TcError(f"无法识别 tc 字节单位: {text!r}")
    return int(float(m.group(1)) * _SIZE_UNITS[unit])


def parse_class_cbursts(out: str) -> dict[int, int]:
    """解析 `tc class show`：classid 次要号 → cburst（字节）。

    为什么要读它：**cburst 才是 ceil 那一路的桶**，漏给的话 tc 会按
    `ceil/get_hz()+mtu` 算出一个 MTU 量级的值，类的吞吐被死死压住而
    `rate` 看着完全正常（详见 _htb_rate_args）。只比 rate 的话，从带
    bug 的版本升上来时会判定"一致"，坏的 cburst 就一直留着了。
    """
    res: dict[int, int] = {}
    for m in _CBURST_RE.finditer(out):
        try:
            res[int(m.group("minor"), 16)] = parse_size(m.group("cburst"))
        except TcError:
            continue
    return res
# `tc filter show` 里 u32 匹配项的 flowid 行与 match 行是分开的两行：
#   filter parent 1: protocol ip pref 1 u32 chain 0 fh 800::800 order 2048 key ht 800 bkt 0 flowid 1:1f90
#     match 00001f90/0000ffff at 20
_FILTER_FLOWID_RE = re.compile(r"\bflowid\s+[0-9a-f]+:(?P<minor>[0-9a-f]+)", re.I)

_UNITS = {"": 1, "bit": 1, "kbit": 1_000, "mbit": 1_000_000, "gbit": 1_000_000_000,
          "tbit": 1_000_000_000_000,
          "kibit": 1024, "mibit": 1024 ** 2, "gibit": 1024 ** 3}


def parse_rate(text: str) -> int:
    """把 tc 输出里的速率（如 "40Mbit"、"1250000bit"）解析成 bit/s。

    tc 输出的单位大小写不固定（Mbit/MBit），且会按可读性自动换算，因此
    比较速率时必须解析成数值再比，不能比字符串。
    """
    m = re.fullmatch(r"(\d+(?:\.\d+)?)\s*([A-Za-z]*)", text.strip())
    if not m:
        raise TcError(f"无法解析 tc 速率: {text!r}")
    val, unit = float(m.group(1)), m.group(2).lower()
    if unit.endswith("bps"):          # tc 也会输出 "Kbps" 表示 kilobyte/s
        unit = unit[:-3] + "bit"
        val *= 8
    if unit not in _UNITS:
        raise TcError(f"无法识别 tc 速率单位: {text!r}")
    return int(val * _UNITS[unit])


def parse_classes(out: str) -> dict[int, int]:
    """解析 `tc class show`：classid 次要号 → 速率（bit/s）。

    次要号按十六进制读（tc 就是这么输出的），与 classid_for 配对。
    """
    res: dict[int, int] = {}
    for m in _CLASS_RE.finditer(out):
        try:
            res[int(m.group("minor"), 16)] = parse_rate(m.group("rate"))
        except TcError:
            continue          # 单条解析不了不该毁掉整次比对
    return res



def _cburst_too_small(actual: int | None, rate_bits_per_s: int) -> bool:
    """网卡上实际的 cburst 是不是明显小于我们该写的值。

    只判"偏小"：偏大不是本模块会造成的故障，而且 tc 打印字节数时按 1024
    进制折算是有损的（5000000 会打成 "4883Kb" = 5000192），精确比对必然
    误判，所以留 CBURST_SLACK_BYTES 的余量。

    读不到（None）也算偏小——那多半是解析不了的老格式，重写一遍最省心。
    """
    if actual is None:
        return True
    return actual + CBURST_SLACK_BYTES < burst_bytes(rate_bits_per_s / 8)


def parse_filter_minors(out: str) -> set[int]:
    """解析 `tc filter show`：已被分类指向的 classid 次要号集合。

    只取 flowid 而不解析 u32 的匹配掩码——匹配项是本模块自己写的，结构
    固定；真正需要发现的是"某个 frontend 的分类规则丢了/多了"，看 flowid
    集合就够，解析 match 反而会因 tc 输出格式变化而变脆。
    """
    return {int(m.group("minor"), 16) for m in _FILTER_FLOWID_RE.finditer(out)}


class TcShaper:
    """把受管 frontend 的限额落到本机网卡的 tc 上（幂等 reconcile）。

    与 haproxy.cfg 的分工：监听端口/后端服务器由运维直接写在 cfg 里
    （cfg 不含任何限速指令），限速全部由本模块的 tc 规则承担。
    """

    def __init__(self, iface: str, log: logging.Logger | None = None,
                 runner=None):
        self.iface = iface
        self._log = log if log is not None else logging.getLogger("rl_limiter.tc")
        # 临时端口冲突告警的去重键（同一组 frontend 只说一次）。
        self._logged_ephemeral: str | None = None
        # 注入点：测试用假 runner 验证命令序列，生产用真 tc。
        self._run = runner if runner is not None else _run_argv

    async def _tc(self, argv: list[str], fatal: bool = True) -> str:
        rc, out, err = await self._run(argv)
        if rc != 0:
            msg = (err or out or "").strip()
            if fatal:
                raise TcError(f"tc 命令失败（rc={rc}）: {' '.join(argv)}: {msg}")
            self._log.debug("tc 命令失败但不致命，继续 cmd=%s err=%s",
                            " ".join(argv), msg)
        return out

    async def observe(self) -> tuple[dict[int, int], dict[int, int], set[int]]:
        """读回网卡实况：(classid→速率, classid→cburst, 已分类的 classid 集合)。

        cburst 也要读回来——它才是 ceil 那一路的桶。只比速率的话，从漏给
        cburst 的旧版本升上来时会判定"一致"，坏的桶就一直留着（见
        _htb_rate_args 里那次事故）。
        """
        out = await self._tc(["tc", "class", "show", "dev", self.iface],
                             fatal=False)
        classes = parse_classes(out)
        cbursts = parse_class_cbursts(out)
        minors = parse_filter_minors(await self._tc(
            ["tc", "filter", "show", "dev", self.iface], fatal=False))
        return classes, cbursts, minors

    def _warn_ephemeral(self, frontends: list[model.FrontendConfig]) -> None:
        """监听端口撞进临时端口范围时告警（同一组只说一次）。"""
        bad = ephemeral_conflicts(frontends)
        key = ",".join(sorted(bad))
        if key == self._logged_ephemeral:
            return
        self._logged_ephemeral = key
        if not bad:
            return
        lo, hi = ephemeral_range()
        self._log.warning(
            "以下 frontend 的监听端口落在本机临时端口范围内，**单网卡部署时"
            "限速可能偶发失准**：HAProxy 连后端用的临时源端口可能撞上监听"
            "端口，那条连接的出向流量会被误判进该 frontend 的限速类。"
            "两条出路：把监听端口挪到 %d 以下，或用两张网卡、只在客户端侧"
            "那张上限速（见 docs/06-tc限速方案.md） frontends=%s "
            "ephemeral_range=%d-%d",
            lo, key, lo, hi)

    async def class_stats(self) -> dict[int, TcClassStat]:
        """读回各 HTB 类的统计，按监听端口索引（监控用，只读）。

        用 `tc -s -j` 取 JSON 而不是解析人类可读文本：文本格式在不同
        iproute2 版本间会变（换行、单位、字段顺序），JSON 的键名稳定得多。
        取不到就返回空字典——监控缺一拍不该影响限速。
        """
        try:
            out = await self._tc(
                ["tc", "-s", "-j", "class", "show", "dev", self.iface],
                fatal=False)
            return parse_class_stats(out)
        except Exception as e:
            self._log.debug("读取 tc 类统计失败，本拍跳过 err=%s", e)
            return {}

    async def reconcile(self, frontends: list[model.FrontendConfig]) -> TcResult:
        """把网卡上的限速状态收敛到配置描述的样子。

        三条路径，按"对流量的打扰程度"从小到大：
          1. 完全一致 → 什么都不做；
          2. 结构一致、只有速率不同 → `tc class change`，**不打断任何连接**；
          3. 结构不一致（新增/删除 frontend、树被人手工改过、首次运行）
             → 整棵重建。重建期间有一个极短的窗口不整形，这是必要代价。
        """
        try:
            _check_shapeable(frontends)
        except TcError as e:
            return TcResult(ok=False, changed=False, error=str(e))

        names = sorted(f.name for f in frontends)
        self._warn_ephemeral(frontends)
        want = desired_rates(frontends)
        try:
            classes, cbursts, filtered = await self.observe()
        except Exception as e:                      # 读状态失败按重建处理
            self._log.warning("读取 tc 当前状态失败，按重建处理 err=%s", e)
            classes, cbursts, filtered = {}, {}, set()

        have_ports = set(classes) - {DEFAULT_CLASS_MINOR}
        structure_ok = (
            DEFAULT_CLASS_MINOR in classes
            and have_ports == set(want)
            and filtered == set(want)
        )
        # 兜底类的 cburst 也要核。它承载全机未受管流量，被 tc 按 MTU 量级
        # 兜底过的话整台机器都会降速，且**没有任何 frontend 的速率会显示
        # 异常**——只能靠这里发现。它不属于 want，所以单独判，并且只能靠
        # 重建来修（rate-change 只走 want 里的类）。
        if structure_ok and _cburst_too_small(
                cbursts.get(DEFAULT_CLASS_MINOR), DEFAULT_CLASS_RATE_BPS):
            self._log.warning(
                "tc 兜底类的 cburst 偏小（%s 字节，期望约 %d），本机未受管流量"
                "会被它压住——按重建处理 iface=%s",
                cbursts.get(DEFAULT_CLASS_MINOR),
                burst_bytes(DEFAULT_CLASS_RATE_BPS / 8), self.iface)
            structure_ok = False

        try:
            if structure_ok:
                # 速率变了要改，cburst 偏小同样要改——后者是升级场景：
                # 速率没动，但旧版本留下的桶只有 MTU 量级。rate_change_cmd
                # 现在会把 burst/cburst 一起重新写上。
                drifted = {p: r for p, r in want.items()
                           if classes.get(p) != r
                           or _cburst_too_small(cbursts.get(p), r)}
                if not drifted:
                    return TcResult(ok=True, changed=False, frontends=names)
                for port, rate in sorted(drifted.items()):
                    await self._tc(rate_change_cmd(self.iface, port, rate))
                self._log.warning(
                    "已就地调整 tc 限速（未重建队列树，存量连接立刻按新限额跑，"
                    "无需 reload HAProxy） iface=%s changed=%s",
                    self.iface,
                    ";".join(f"{p}={r}bit" for p, r in sorted(drifted.items())))
                return TcResult(ok=True, changed=True, frontends=names,
                                action="rate-change")

            for argv, fatal in _rebuild_cmds(self.iface, frontends):
                await self._tc(argv, fatal=fatal)
            self._log.warning(
                "已重建本机网卡的 tc 限速队列树（数据面已按新配置整形） "
                "iface=%s frontends=%s",
                self.iface,
                ";".join(f"{f.name}@:{f.bind_port}={int(f.quota_bytes_per_sec)*8}bit"
                         for f in sorted(frontends, key=lambda x: x.bind_port)))
            return TcResult(ok=True, changed=True, frontends=names,
                            action="rebuild")
        except TcError as e:
            self._log.error(
                "tc 限速下发失败，**限速可能未按新配置生效**，将在下一轮重试 "
                "iface=%s err=%s", self.iface, e)
            return TcResult(ok=False, changed=False, frontends=names, error=str(e))

    async def teardown(self) -> None:
        """撤掉本模块建立的整棵树（停机/切回 bwlim 方案时用）。"""
        await self._tc(["tc", "qdisc", "del", "dev", self.iface, "root"],
                       fatal=False)


def parse_class_stats(out: str) -> dict[int, TcClassStat]:
    """解析 `tc -s -j class show` 的 JSON，返回 端口 → TcClassStat。

    只保留 HTB 叶子类里 classid 次要号能对上监听端口的那些；兜底类
    （次要号 1）不是任何 frontend，跳过。

    handle 里的次要号同样是**十六进制**（tc 的 JSON 与文本输出用的是同一套
    格式化）。按十进制读的话 "1:1f90" 会抛 ValueError 被下面的 continue
    悄悄跳过——监控视图里那条"数据包与丢包"曲线就会一直是空的，而且不报错。
    """
    if not out.strip():
        return {}
    try:
        rows = json.loads(out)
    except ValueError as e:
        raise TcError(f"tc 类统计不是合法 JSON: {e}") from None
    res: dict[int, TcClassStat] = {}
    for r in rows if isinstance(rows, list) else []:
        handle = str(r.get("handle") or "")
        if ":" not in handle:
            continue
        try:
            minor = int(handle.split(":", 1)[1], 16)
        except ValueError:
            continue
        if minor == DEFAULT_CLASS_MINOR:
            continue
        res[minor] = TcClassStat(
            port=minor,
            bytes=int(r.get("bytes") or 0),
            packets=int(r.get("packets") or 0),
            drops=int(r.get("drops") or 0),
            overlimits=int(r.get("overlimits") or 0),
            backlog=int(r.get("backlog") or 0),
            qlen=int(r.get("qlen") or 0),
        )
    return res


async def _run_argv(argv: list[str]) -> tuple[int, str, str]:
    """执行 argv 并返回 (rc, stdout, stderr)。

    刻意用 argv 而不是 shell 字符串：本模块以 root/CAP_NET_ADMIN 运行，
    走 shell 等于把配置文件里的值暴露在命令行解析面前。
    """
    proc = await asyncio.create_subprocess_exec(
        *argv, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
    out, err = await proc.communicate()
    return (proc.returncode or 0,
            out.decode("utf-8", "replace"), err.decode("utf-8", "replace"))


async def run_shaper(shaper: TcShaper,
                     desired_fn,
                     config_applied: asyncio.Event,
                     log: logging.Logger,
                     on_result=None,
                     period_s: float = 30.0) -> None:
    """常驻任务：配置一变就下发 tc，另有周期性兜底 reconcile。

    事件驱动保证"改完立刻生效"，周期兜底负责纠正有人手工动过 tc
    （`tc qdisc del` 之类）造成的漂移。
    """
    while True:
        try:
            res = await shaper.reconcile(desired_fn())
            if on_result is not None:
                on_result(res)
        except asyncio.CancelledError:
            raise
        except Exception as e:                      # 常驻任务不能因单次异常退出
            log.error("tc 限速 reconcile 出现未预期的异常，本轮跳过 err=%s", e)
        try:
            await asyncio.wait_for(config_applied.wait(), timeout=period_s)
            config_applied.clear()
        except asyncio.TimeoutError:
            pass


def resolve_iface(configured: str, log: logging.Logger | None = None) -> str:
    """定下要在哪张网卡上整形。

    configured 非空即以它为准（多网卡机器上必须能指定）；否则取默认路由的
    出口网卡——那就是客户端流量真正走的那张。探测不出来返回空串，由调用方
    决定是否放弃启用（限速是核心功能，调用方应视为致命错误）。
    """
    log = log if log is not None else logging.getLogger("rl_limiter.tc")
    if configured:
        return configured
    try:
        with open("/proc/net/route", encoding="ascii", errors="replace") as f:
            for line in f.read().split("\n")[1:]:
                cols = line.split()
                if len(cols) >= 2 and cols[1] == "00000000":   # 目的 0.0.0.0
                    log.info("未指定限速网卡，已自动选用默认路由的出口网卡 "
                             "nic=%s", cols[0])
                    return cols[0]
    except OSError:
        pass
    return ""
