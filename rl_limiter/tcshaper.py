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
#     tc class add dev eth0 parent 1: classid 1:8080 htb rate 40mbit ceil 40mbit
#     tc filter add dev eth0 protocol ip parent 1: prio 1 u32 \
#         match ip sport 8080 0xffff flowid 1:8080
#
# **classid 的次要号直接取监听端口**：端口在本模型里天然唯一（一个 frontend
# = 一个监听端口），因此 classid 稳定且无需额外分配表——改配置、重排序、
# 增删 frontend 都不会让别的 frontend 的 classid 漂移。次要号 1 留给兜底类，
# 所以拒绝监听 1 端口（那也不是现实中会用的端口）。
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
# 与 enforcer 同源的考虑：本模块会以 root（或 CAP_NET_ADMIN）执行 tc 命令，
# 因此
#   - **命令一律用 argv 列表拼装，绝不拼 shell 字符串**，配置里的值（端口、
#     限额）在进入 argv 之前全部过整数校验，不存在注入面；
#   - 网卡名只来自本机环境变量/自动探测，**绝不从配置库读**；
#   - 空清单拒绝执行（与 enforcer 一致）：那意味着"把所有限速撤掉"，是事故
#     而不是配置操作。

from __future__ import annotations

import asyncio
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


class TcError(RuntimeError):
    """tc 操作失败（命令返回非零、网卡不存在、配置不可整形等）。"""


@dataclass
class TcResult:
    """一次 reconcile 的结果，语义与 enforcer.ApplyResult 对齐。"""

    ok: bool
    changed: bool
    # 本次生效的 frontend 名（按名字排序），供控制台展示。
    frontends: list[str] = field(default_factory=list)
    # changed 为真时说明走的是哪条路径："rebuild"（重建整棵树）或
    # "rate-change"（只改速率，不动结构、不打断流量）。
    action: str = ""
    error: str = ""


def burst_bytes(rate_bytes_per_s: float) -> int:
    """按限额算 HTB 的 burst（字节）。

    取 10ms 的额度并夹在 [2×MTU, 8MB]：下限保证大包发得出去，上限避免
    高限额下攒出一个大到让秒级限速失真的桶。
    """
    return max(MIN_BURST_BYTES,
               min(MAX_BURST_BYTES, int(rate_bytes_per_s * BURST_SECONDS)))


def classid_for(port: int) -> str:
    """监听端口 → classid。次要号直接取端口，理由见文件头。"""
    return f"{ROOT_HANDLE}{port}"


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
            raise TcError(f"frontend {f.name} 的限额 {f.quota_bits_per_sec} bit/s "
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
        (["tc", "class", "add", "dev", iface, "parent", ROOT_HANDLE,
          "classid", classid_for(DEFAULT_CLASS_MINOR),
          "htb", "rate", f"{DEFAULT_CLASS_RATE_BPS}bit"], True),
    ]
    for f in sorted(frontends, key=lambda x: x.bind_port):
        port = f.bind_port
        rate = int(f.quota_bytes_per_sec) * 8
        cid = classid_for(port)
        cmds.append((
            ["tc", "class", "add", "dev", iface, "parent", ROOT_HANDLE,
             "classid", cid, "htb",
             "rate", f"{rate}bit", "ceil", f"{rate}bit",
             "burst", str(burst_bytes(f.quota_bytes_per_sec))], True))
        # 叶子队列：同一 frontend 内各连接之间公平排队。老内核可能没有
        # fq_codel，缺了只是失去类内公平性，限速本身不受影响 → 非致命。
        cmds.append((
            ["tc", "qdisc", "add", "dev", iface, "parent", cid,
             "handle", f"{port}:", LEAF_QDISC], False))
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
            "classid", classid_for(port), "htb",
            "rate", f"{rate_bits_per_s}bit", "ceil", f"{rate_bits_per_s}bit",
            "burst", str(burst_bytes(rate_bits_per_s / 8))]


# `tc class show` 的一行形如：
#   class htb 1:8080 root prio 0 rate 40Mbit ceil 40Mbit burst 50000b cburst 1600b
_CLASS_RE = re.compile(
    r"^class\s+htb\s+\d+:(?P<minor>\d+)\b.*?\brate\s+(?P<rate>\S+)", re.M)
# `tc filter show` 里 u32 匹配项的 flowid 行与 match 行是分开的两行：
#   filter parent 1: protocol ip pref 1 u32 chain 0 fh 800::800 order 2048 key ht 800 bkt 0 flowid 1:8080
#     match 00001f90/0000ffff at 20
_FILTER_FLOWID_RE = re.compile(r"\bflowid\s+\d+:(?P<minor>\d+)")

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
    """解析 `tc class show`：classid 次要号 → 速率（bit/s）。"""
    res: dict[int, int] = {}
    for m in _CLASS_RE.finditer(out):
        try:
            res[int(m.group("minor"))] = parse_rate(m.group("rate"))
        except TcError:
            continue          # 单条解析不了不该毁掉整次比对
    return res


def parse_filter_minors(out: str) -> set[int]:
    """解析 `tc filter show`：已被分类指向的 classid 次要号集合。

    只取 flowid 而不解析 u32 的匹配掩码——匹配项是本模块自己写的，结构
    固定；真正需要发现的是"某个 frontend 的分类规则丢了/多了"，看 flowid
    集合就够，解析 match 反而会因 tc 输出格式变化而变脆。
    """
    return {int(m.group("minor")) for m in _FILTER_FLOWID_RE.finditer(out)}


class TcShaper:
    """把受管 frontend 的限额落到本机网卡的 tc 上（幂等 reconcile）。

    与 enforcer 的分工：enforcer 负责把监听端口/后端服务器写进 haproxy.cfg
    （不再写任何限速指令），本模块负责限速。两者互不依赖，各自幂等。
    """

    def __init__(self, iface: str, log: logging.Logger | None = None,
                 runner=None):
        self.iface = iface
        self._log = log if log is not None else logging.getLogger("rl_limiter.tc")
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

    async def observe(self) -> tuple[dict[int, int], set[int]]:
        """读回当前网卡上的实际状态：(classid→速率, 已分类的 classid 集合)。"""
        classes = parse_classes(await self._tc(
            ["tc", "class", "show", "dev", self.iface], fatal=False))
        minors = parse_filter_minors(await self._tc(
            ["tc", "filter", "show", "dev", self.iface], fatal=False))
        return classes, minors

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
        want = desired_rates(frontends)
        try:
            classes, filtered = await self.observe()
        except Exception as e:                      # 读状态失败按重建处理
            self._log.warning("读取 tc 当前状态失败，按重建处理 err=%s", e)
            classes, filtered = {}, set()

        have_ports = set(classes) - {DEFAULT_CLASS_MINOR}
        structure_ok = (
            DEFAULT_CLASS_MINOR in classes
            and have_ports == set(want)
            and filtered == set(want)
        )

        try:
            if structure_ok:
                drifted = {p: r for p, r in want.items() if classes.get(p) != r}
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


async def _run_argv(argv: list[str]) -> tuple[int, str, str]:
    """执行 argv 并返回 (rc, stdout, stderr)。

    刻意用 argv 而不是 shell 字符串：本模块以 root/CAP_NET_ADMIN 运行，
    走 shell 等于把配置库里的值暴露在命令行解析面前。
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

    与 enforcer 的 run_enforcer 同构：事件驱动保证"改完立刻生效"，周期兜底
    负责纠正有人手工动过 tc（`tc qdisc del` 之类）造成的漂移。
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
