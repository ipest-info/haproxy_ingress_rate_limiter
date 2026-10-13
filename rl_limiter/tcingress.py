# rl_limiter.tcingress —— 请求方向（入向）的 tc 限速（双网卡形态）。
#
# ## 它限的是什么
#
# quotas/tcshaper 限的是**响应方向**（HAProxy 发回客户端的下行带宽，
# 出向 HTB）。本模块限的是**请求方向**：客户端打进 HAProxy 的流量——
# 在客户端侧网卡的 **ingress（入向）** 上，按目的端口（= frontend 的
# 监听端口）police：
#
#   1. **入向带宽**（request_limits.<名>.mbps）：u32 按 `ip dport` 分类
#      + police 字节速率，超额的包直接丢弃（police 没有队列，这是入向
#      与出向整形的本质差异——入向只能丢，靠 TCP 重传/收敛把发送端压
#      下来）；
#   2. **每秒新建连接**（request_limits.<名>.conn_rate）：flower 匹配
#      TCP SYN（SYN=1,ACK=0）+ police 包速率（pkts_rate），超速的建连
#      尝试被丢弃、客户端重试退避。这是 tc 能表达的最接近"请求速率"的
#      口径——L4 上数不出 HTTP 请求，能数的是连接建立。真正按 HTTP
#      请求数限速属于 HAProxy 自身（stick-table/http_req_rate），不在
#      tc 的能力范围。
#
# ## 为什么要求双网卡
#
# 入向按 dport 匹配本身没有歧义（打到监听端口的入向包就是客户端请求），
# 但把入向 police 放在哪张网卡上必须明确：单网卡机器上这张网卡同时承载
# 后端回程流量，万一监听端口与后端端口重合（或经 NAT 改写）就会误伤。
# 双网卡（客户端一张、后端一张）把两类流量物理分开，入向 police 只放在
# **客户端侧**那张上——因此执行网卡只能来自 RL_TC_IN_IFACE 显式指定，
# **绝不自动探测**；配置了 request_limits 却没指定网卡会启动失败。
#
# ## 与出向模块的分工
#
# 完全独立：可以只配 quotas（只限下行）、只配 request_limits（只限请求
# 方向）、或两者都配。入向用 ingress qdisc + 无类 police，没有 HTB 的
# classid/借带宽概念，也就没有层级模式——每个端口各限各的。
#
# ## 安全边界（与 tcshaper 相同）
#
#   - 命令一律 argv 列表拼装，端口/速率先过整数校验，不存在注入面；
#   - 网卡名只来自本机环境变量，绝不从配置文件读；
#   - 空清单 = 显式撤掉全部入向限速：删掉 ingress qdisc，网卡恢复原状。

from __future__ import annotations

import logging
import re
from dataclasses import dataclass

from . import model
from .tcshaper import TcError, TcResult, _run_argv, burst_bytes, parse_rate

# ingress qdisc 的固定句柄（内核约定，add 时不用也不能自选别的）。
INGRESS_PARENT = "ffff:"
# 过滤器优先级：1 = 字节速率（u32 police），2 = SYN 速率（flower）。
# 重建时按 prio 整段删除，不会碰别人挂在同一网卡入向的其它 prio。
PRIO_RATE = "1"
PRIO_SYN = "2"


@dataclass(**model.SLOTS)
class IngressRule:
    """一个监听端口的入向限速规则（由 frontends × request_limits 合并出）。"""

    name: str            # frontend 名（日志用）
    port: int            # 监听端口（入向包的 dport）
    rate_bits: int = 0   # 入向带宽上限（bit/s；0 = 不限带宽）
    conn_rate: int = 0   # 每秒新建连接上限（0 = 不限）


def desired_rules(frontends: list[model.FrontendConfig],
                  limits: dict) -> tuple[list[IngressRule], list[str]]:
    """frontends（cfg 解析）× request_limits（YAML 登记）→ 入向规则。

    返回 (规则列表, 未匹配的登记名)：登记了、cfg 里没有的名字交给调用方
    告警（段被删了或名字拼错——与 quotas 的合并规则一致）。
    两项都为 0 的登记不生成规则（等价于没登记）。
    """
    by_name = {f.name: f for f in frontends}
    rules: list[IngressRule] = []
    unknown: list[str] = []
    for name, rl in sorted(limits.items()):
        f = by_name.get(name)
        if f is None:
            unknown.append(name)
            continue
        rate_bits = int(rl.mbps * 1_000_000)
        if rate_bits <= 0 and rl.conn_rate <= 0:
            continue
        rules.append(IngressRule(name=name, port=f.bind_port,
                                 rate_bits=rate_bits,
                                 conn_rate=int(rl.conn_rate)))
    return rules, unknown


def _check_rules(rules: list[IngressRule]) -> None:
    """进入 argv 前的最后一道校验（值会交给以 root 执行的 tc）。"""
    seen: set[int] = set()
    for r in rules:
        if not (1 <= r.port <= 65535):
            raise TcError(f"frontend {r.name} 的监听端口 {r.port} 越界")
        if r.port in seen:
            raise TcError(f"监听端口 {r.port} 被多个 frontend 使用，"
                          f"无法按端口分类限速")
        seen.add(r.port)
        if r.rate_bits and r.rate_bits < 8:
            raise TcError(f"frontend {r.name} 的入向限额不足 1 字节/秒，"
                          f"无法执行")


def _rebuild_cmds(iface: str,
                  rules: list[IngressRule]) -> list[tuple[list[str], bool]]:
    """整组重建的命令序列（(argv, fatal) 列表，语义同 tcshaper）。

    先确保 ingress qdisc 在（已存在时 add 报错，非致命），再把我们的两个
    prio 整段清掉（首次运行本就没有，非致命），然后逐端口加过滤器。
    """
    cmds: list[tuple[list[str], bool]] = [
        (["tc", "qdisc", "add", "dev", iface, "ingress"], False),
        (["tc", "filter", "del", "dev", iface, "parent", INGRESS_PARENT,
          "prio", PRIO_RATE], False),
        (["tc", "filter", "del", "dev", iface, "parent", INGRESS_PARENT,
          "prio", PRIO_SYN], False),
    ]
    for r in sorted(rules, key=lambda x: x.port):
        if r.rate_bits:
            burst = burst_bytes(r.rate_bits / 8)
            # IPv4/IPv6 各一条——只写 IPv4 的话 v6 请求完全不受限。
            cmds.append((
                ["tc", "filter", "add", "dev", iface,
                 "parent", INGRESS_PARENT, "protocol", "ip",
                 "prio", PRIO_RATE, "u32",
                 "match", "ip", "dport", str(r.port), "0xffff",
                 "police", "rate", f"{r.rate_bits}bit",
                 "burst", f"{burst}b", "drop"], True))
            cmds.append((
                ["tc", "filter", "add", "dev", iface,
                 "parent", INGRESS_PARENT, "protocol", "ipv6",
                 "prio", PRIO_RATE, "u32",
                 "match", "ip6", "dport", str(r.port), "0xffff",
                 "police", "rate", f"{r.rate_bits}bit",
                 "burst", f"{burst}b", "drop"], True))
        if r.conn_rate:
            # SYN 速率：flower 的 tcp_flags 0x02/0x12 = SYN 置位且 ACK
            # 清零（放过 SYN-ACK——那是出向握手回包，不该出现在入向，
            # 防御性写上）。pkts_burst 给 1 秒的额度：客户端的建连天然
            # 突发（页面一开一排连接），桶太浅会把正常突发误杀。
            #
            # **非致命**：flower + police pkts_rate 需要较新的内核
            # （cls_flower + act_police 的包速率支持）。缺了只是没有
            # 连接速率限制，字节限速仍然生效——失败会记日志。
            for proto in ("ip", "ipv6"):
                cmds.append((
                    ["tc", "filter", "add", "dev", iface,
                     "parent", INGRESS_PARENT, "protocol", proto,
                     "prio", PRIO_SYN, "flower", "ip_proto", "tcp",
                     "dst_port", str(r.port), "tcp_flags", "0x02/0x12",
                     "action", "police",
                     "pkts_rate", str(r.conn_rate),
                     "pkts_burst", str(max(r.conn_rate, 10)),
                     "conform-exceed", "drop"], False))
    return cmds


# `tc filter show dev X parent ffff:` 里 u32 dport 匹配行的样子（iproute2
# 的 parse_u16/print 约定：dport 是 20/40 偏移处 32 位字的**低 16 位**）：
#   IPv4:  match 00001f90/0000ffff at 20
#   IPv6:  match 00001f90/0000ffff at 40
# police 行形如：
#   police 0x1 rate 10Mbit burst 12500b mtu 2Kb action drop overhead 0b
# 注意：**本解析格式未在真机上核对过**（开发沙箱内核没有 act_police，
# 见 docs/10 的"未验证的部分"）——按 iproute2 源码的打印路径写成，首次
# 上线请用 `tc filter show` 人工比对一次，不符会表现为每轮都重建
# （日志里连续的 rebuild 即是信号）。
_DPORT_RE = re.compile(
    r"^\s*match 0000(?P<port>[0-9a-f]{4})/0000ffff at (?:20|40)\s*$",
    re.I | re.M)
_POLICE_RATE_RE = re.compile(r"^\s*police .*?\brate (?P<rate>\S+)", re.I)


def parse_ingress_filters(out: str) -> dict[int, int]:
    """解析入向过滤器实况：监听端口 → police 字节速率（bit/s）。

    按行扫描：u32 的 dport 匹配行给出端口，紧随其后的 police 行给出
    速率；遇到下一个 filter 头就重置配对。只收 prio 1（字节速率）的
    结果——SYN 速率过滤器（flower）不参与一致性比对，见 reconcile。
    """
    res: dict[int, int] = {}
    in_pref1 = False
    pending: "int | None" = None
    for line in out.split("\n"):
        if line.startswith("filter"):
            # 新的过滤器块：只认 pref 1 的 u32 段，别的 prio（我们的
            # SYN 过滤器、其它程序挂的）整块跳过。
            m = re.search(r"\bpref (\d+)\b", line)
            in_pref1 = bool(m and m.group(1) == "1")
            pending = None
            continue
        if not in_pref1:
            continue
        m = _DPORT_RE.match(line)
        if m:
            pending = int(m.group("port"), 16)
            continue
        m = _POLICE_RATE_RE.match(line)
        if m and pending is not None:
            try:
                res[pending] = parse_rate(m.group("rate"))
            except TcError:
                pass
            pending = None
    return res


class TcIngress:
    """把 request_limits 落到客户端侧网卡的入向 police（幂等 reconcile）。"""

    def __init__(self, iface: str, log: logging.Logger | None = None,
                 runner=None):
        self.iface = iface
        self._log = log if log is not None else logging.getLogger(
            "rl_limiter.tcin")
        self._run = runner if runner is not None else _run_argv
        # 未匹配登记名的告警去重（同一组只说一次），与 tcshaper 的
        # ephemeral 告警同一套路。
        self._warned_unknown: "str | None" = None
        # 上次成功下发的配置指纹：conn_rate 不在网卡实况比对里（见
        # reconcile），靠它做"配置变没变"的检测。
        self._last_applied = ""

    async def _tc(self, argv: list[str], fatal: bool = True) -> str:
        rc, out, err = await self._run(argv)
        if rc != 0:
            msg = (err or out or "").strip()
            if fatal:
                raise TcError(f"tc 命令失败（rc={rc}）: {' '.join(argv)}: {msg}")
            self._log.debug("tc 命令失败但不致命，继续 cmd=%s err=%s",
                            " ".join(argv), msg)
        return out

    async def observe(self) -> dict[int, int]:
        """读回入向实况：监听端口 → police 字节速率（bit/s）。"""
        out = await self._tc(
            ["tc", "filter", "show", "dev", self.iface,
             "parent", INGRESS_PARENT], fatal=False)
        return parse_ingress_filters(out)

    def _warn_unknown(self, unknown: list[str]) -> None:
        key = ",".join(sorted(unknown))
        if key == self._warned_unknown:
            return
        self._warned_unknown = key
        if unknown:
            self._log.warning(
                "request_limits 里登记的这些名字在 haproxy.cfg 里不存在，"
                "条目被忽略（段被删了或名字拼错）：%s", key)

    async def reconcile(self, frontends: list[model.FrontendConfig],
                        limits: dict) -> TcResult:
        """把入向限速收敛到配置描述的样子。

        一致性只比**字节速率**（端口集合 + police rate）：SYN 速率过滤
        器是非致命特性（老内核可能加不上），把它纳入比对会在那些内核上
        每轮触发重建。代价是"只改 conn_rate"也走整组重建——入向过滤器
        重建只是删两组 prio 再加回，瞬间完成且 police 无队列状态可言，
        没有出向 rate-change 快路径那样的保留价值。
        """
        rules, unknown = desired_rules(frontends, limits)
        self._warn_unknown(unknown)
        try:
            _check_rules(rules)
        except TcError as e:
            return TcResult(ok=False, changed=False, error=str(e))

        names = sorted(r.name for r in rules)
        try:
            have = await self.observe()
        except Exception as e:
            self._log.warning("读取入向过滤器实况失败，按重建处理 err=%s", e)
            have = {}

        if not rules:
            if not have:
                return TcResult(ok=True, changed=False)
            # 显式撤掉全部入向限速：删 ingress qdisc，网卡入向恢复原状。
            try:
                await self._tc(["tc", "qdisc", "del", "dev", self.iface,
                                "ingress"])
            except TcError as e:
                self._log.warning(
                    "撤掉入向限速失败，下一轮兜底重试 iface=%s err=%s",
                    self.iface, e)
                return TcResult(ok=False, changed=False, error=str(e))
            self._log.warning(
                "已撤掉客户端侧网卡的入向限速（request_limits 已清空），"
                "入向恢复原状 iface=%s", self.iface)
            return TcResult(ok=True, changed=True, action="teardown")

        want = {r.port: r.rate_bits for r in rules if r.rate_bits}
        # 只有 conn_rate 的部署没有可比对的字节过滤器：want/have 都空也
        # 视为一致（SYN 过滤器在每次配置变化时随重建刷新，由 run_ingress
        # 的事件驱动保证）。
        if want == have and not self._config_changed(rules):
            return TcResult(ok=True, changed=False, frontends=names)

        try:
            for argv, fatal in _rebuild_cmds(self.iface, rules):
                await self._tc(argv, fatal=fatal)
        except TcError as e:
            self._log.error(
                "入向限速下发失败，**请求方向可能未按配置限速**，将在下一轮"
                "重试 iface=%s err=%s", self.iface, e)
            return TcResult(ok=False, changed=False, frontends=names,
                            error=str(e))
        self._remember(rules)
        self._log.warning(
            "已重建客户端侧网卡的入向限速（请求方向已按配置 police） "
            "iface=%s rules=%s", self.iface,
            ";".join(f"{r.name}@:{r.port}"
                     f"={r.rate_bits}bit/{r.conn_rate}cps" for r in rules))
        return TcResult(ok=True, changed=True, frontends=names,
                        action="rebuild")

    def _config_changed(self, rules: list[IngressRule]) -> bool:
        return self._fingerprint(rules) != self._last_applied

    def _remember(self, rules: list[IngressRule]) -> None:
        self._last_applied = self._fingerprint(rules)

    @staticmethod
    def _fingerprint(rules: list[IngressRule]) -> str:
        return ";".join(f"{r.port}|{r.rate_bits}|{r.conn_rate}"
                        for r in sorted(rules, key=lambda x: x.port))

    async def teardown(self) -> None:
        """撤掉本模块建立的入向限速（停机/下线时用）。"""
        await self._tc(["tc", "qdisc", "del", "dev", self.iface, "ingress"],
                       fatal=False)

    async def debug_state(self) -> dict:
        """调试快照：入向 qdisc 与 police 过滤器的原始输出（控制台的
        "tc 实况"面板用）。与 TcShaper.debug_state 同一约定：全部只读、
        非致命，失败的命令把错误文本当输出展示。"""
        async def run(argv: list[str]) -> str:
            rc, out, err = await self._run(argv)
            text = (out or "").strip()
            if rc != 0:
                text = (text + "\n" if text else "") + \
                    f"[命令失败 rc={rc}] {(err or '').strip()}"
            return text or "(空)"

        return {
            "iface": self.iface,
            "raw": [
                {"label": f"tc qdisc show dev {self.iface} ingress",
                 "text": await run(["tc", "qdisc", "show", "dev",
                                    self.iface, "ingress"])},
                # -s 带上 police 的命中/丢弃计数——限速到底有没有在丢包，
                # 看这里。
                {"label": f"tc -s filter show dev {self.iface} parent ffff:",
                 "text": await run(["tc", "-s", "filter", "show", "dev",
                                    self.iface, "parent", "ffff:"])},
            ],
        }


async def run_ingress(shaper: TcIngress,
                      desired_fn,
                      config_applied,
                      log: logging.Logger,
                      period_s: float = 30.0) -> None:
    """常驻任务：配置一变就下发入向限速，另有周期性兜底 reconcile。

    desired_fn() 返回 (frontends, request_limits)。结构与 tcshaper 的
    run_shaper 相同：事件驱动保证改完立刻生效，周期兜底纠正手工动过
    tc 造成的漂移。
    """
    import asyncio
    while True:
        try:
            frontends, limits = desired_fn()
            await shaper.reconcile(frontends, limits)
        except asyncio.CancelledError:
            raise
        except Exception as e:                      # 常驻任务不能因单次异常退出
            log.error("入向限速 reconcile 出现未预期的异常，本轮跳过 err=%s", e)
        try:
            await asyncio.wait_for(config_applied.wait(), timeout=period_s)
            config_applied.clear()
        except asyncio.TimeoutError:
            pass
