# rl_limiter.tcifb —— 入向**平稳整形**：IFB + HTB（police 的升级路线）。
#
# police（tcingress）的本质是"超了就丢"：无队列，超额包直接丢给 TCP
# 重传去兜，曲线毛糙、对高 RTT 客户端不友好。本模块给入向装上与出向
# 同款的整形队列：
#
#   客户端 ─→ [真实网卡 ingress 钩子] ──mirred 重定向──→ [IFB 虚拟网卡]
#                                                          └─ HTB（排队慢放）
#                                                              ─→ 协议栈
#
# 内核的 ingress 路径本身没有队列（包一到就得处理），所以借 IFB
# （Intermediate Functional Block）这个专为此设计的虚拟设备：入向包被
# mirred 整段搬到 IFB 的**出向**路径上，就能用出向的全套整形机制
# （HTB/排队/借带宽）了——这是 Linux 世界入向整形的标准做法。
#
# 分工与复用：
#   - 字节整形：完全复用 tcshaper.TcShaper（挂在 IFB 上，match_dport=True
#     ——重定向过来的是客户端打进来的包，frontend 监听端口在 dport 侧）。
#     于是入向免费获得出向的全部成熟能力：显式 burst/cburst、幂等
#     reconcile、**rate 就地变更**（动态闭环每次调速不再整组重建、不清
#     零统计）、每类的包/丢包统计；
#   - conn_rate（SYN 速率）：仍是真实网卡 ingress 上的 flower+police
#     ——"每秒新建连接数"是包速率语义，HTB 表达不了；保持尽力而为
#     （老内核缺模块只告警不阻断，字节整形不受影响）；
#   - 与 tcingress.TcIngress 同一个 reconcile(frontends, limits) 接口，
#     __main__ 按 RL_TC_IN_MODE 二选一，运行循环（run_ingress）与动态
#     闭环（ingressgov）对两者一视同仁。
#
# 内核前提：ifb、act_mirred、sch_htb（均为各发行版通用内核标配，比
# police 模式要求的 act_police 更常见）。缺了会在 reconcile 时大声报错
# 并每轮重试，绝不静默降级。
from __future__ import annotations

import logging

from . import model, tcshaper
from .tcingress import (INGRESS_PARENT, PRIO_SYN, IngressRule, TcResult,
                        _check_rules, desired_rules)
from .tcshaper import TcError, _run_argv

# 重定向过滤器的 prio。SYN police 用 tcingress 的 PRIO_SYN（=2），重定向
# 放在其后：SYN 包先过速率闸（超了丢、没超放行终止于 police 的 ok 动作
# ——SYN 只有几十字节，不整形无伤大雅），其余包落到这条全匹配重定向。
PRIO_REDIRECT = "5"

# IFB 设备默认名。刻意不用惯例的 ifb0——那是教程和手工脚本最爱用的
# 名字，撞上运维自己建的设备会互相踩；独占一个带项目印记的名字。
DEFAULT_IFB = "ifbrl0"


def _redirect_cmds(iface: str, ifb: str) -> list[tuple[list[str], bool]]:
    """把真实网卡入向的全部 IP 流量搬到 IFB 的过滤器（IPv4/IPv6 各一条）。"""
    return [
        (["tc", "filter", "add", "dev", iface, "parent", INGRESS_PARENT,
          "protocol", proto, "prio", PRIO_REDIRECT, "u32",
          "match", "u32", "0", "0",
          "action", "mirred", "egress", "redirect", "dev", ifb], True)
        for proto in ("ip", "ipv6")
    ]


def _syn_cmds(iface: str,
              rules: list[IngressRule]) -> list[tuple[list[str], bool]]:
    """conn_rate 的 SYN 速率过滤器（与 tcingress 同一套，全部非致命）。"""
    cmds: list[tuple[list[str], bool]] = [
        (["tc", "filter", "del", "dev", iface, "parent", INGRESS_PARENT,
          "prio", PRIO_SYN], False),
    ]
    for r in sorted(rules, key=lambda x: x.port):
        if not r.conn_rate:
            continue
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


class TcIfbIngress:
    """入向平稳整形（IFB+HTB）的幂等 reconcile，接口与 TcIngress 相同。"""

    mode = "ifb"

    def __init__(self, iface: str, ifb: str = DEFAULT_IFB,
                 log: logging.Logger | None = None, runner=None):
        self.iface = iface          # 真实网卡（客户端侧）
        self.ifb = ifb              # IFB 虚拟网卡（整形发生地）
        self._log = log if log is not None else logging.getLogger(
            "rl_limiter.tcifb")
        self._run = runner if runner is not None else _run_argv
        # 字节整形本体：IFB 上的 HTB，按目的端口分类。runner 原样注入，
        # 测试能看到完整命令序列。
        self._shaper = tcshaper.TcShaper(ifb, log=self._log, runner=runner,
                                         match_dport=True)
        self._warned_unknown: "str | None" = None
        # conn_rate 的配置指纹（SYN 过滤器非致命、不读回比对，与
        # tcingress 同一取舍）。
        self._syn_fp = ""

    async def _tc(self, argv: list[str], fatal: bool = True) -> str:
        rc, out, err = await self._run(argv)
        if rc != 0:
            msg = (err or out or "").strip()
            if fatal:
                raise TcError(f"命令失败（rc={rc}）: {' '.join(argv)}: {msg}")
            self._log.debug("命令失败但不致命，继续 cmd=%s err=%s",
                            " ".join(argv), msg)
        return out

    def _warn_unknown(self, unknown: list[str]) -> None:
        key = ",".join(sorted(unknown))
        if key == self._warned_unknown:
            return
        self._warned_unknown = key
        if unknown:
            self._log.warning(
                "request_limits 里登记的这些名字在 haproxy.cfg 里不存在，"
                "条目被忽略（段被删了或名字拼错）：%s", key)

    async def _ensure_plumbing(self) -> None:
        """确保 IFB 设备在且 up、ingress 钩子在、重定向过滤器在。

        全程幂等，每轮 reconcile 都跑：周期兜底要能纠正"有人手工删了
        重定向/down 了 IFB"——那种状态下限速整个失效，还没有任何报错。
        """
        # 设备可能已存在（上轮建的/重启残留），add 失败非致命；随后的
        # show 才是"设备到底在不在"的裁决，不在就致命——没有 IFB 一切
        # 免谈，宁可大声失败让周期重试，也不要装作在限速。
        await self._tc(["ip", "link", "add", self.ifb, "type", "ifb"],
                       fatal=False)
        await self._tc(["ip", "link", "show", self.ifb])
        await self._tc(["ip", "link", "set", self.ifb, "up"])
        await self._tc(["tc", "qdisc", "add", "dev", self.iface, "ingress"],
                       fatal=False)
        out = await self._tc(["tc", "filter", "show", "dev", self.iface,
                              "parent", INGRESS_PARENT], fatal=False)
        # iproute2 把 mirred 动作打印成 "Egress Redirect to device <名>"；
        # 松匹配设备名 + mirred 关键字，不绑定完整格式。
        if self.ifb not in out or "irred" not in out:
            for argv, fatal in _redirect_cmds(self.iface, self.ifb):
                await self._tc(argv, fatal=fatal)
            self._log.warning(
                "已把 %s 的入向流量重定向到 %s（IFB+HTB 整形模式）",
                self.iface, self.ifb)

    async def reconcile(self, frontends: list[model.FrontendConfig],
                        limits: dict) -> TcResult:
        """把入向整形收敛到配置描述的样子。

        字节整形的一致性/变更检测全部委托给 TcShaper（读回 IFB 上的类
        与速率，支持 rate 就地变更）；conn_rate 走配置指纹。
        """
        rules, unknown = desired_rules(frontends, limits)
        self._warn_unknown(unknown)
        try:
            _check_rules(rules)
        except TcError as e:
            return TcResult(ok=False, changed=False, error=str(e))

        if not rules:
            return await self._teardown_rules()

        try:
            await self._ensure_plumbing()
        except TcError as e:
            self._log.error(
                "入向整形的管路建立失败（IFB/重定向），**请求方向可能未按"
                "配置限速**，将在下一轮重试 iface=%s ifb=%s err=%s",
                self.iface, self.ifb, e)
            return TcResult(ok=False, changed=False, error=str(e))

        # 字节整形：把入向登记翻译成 TcShaper 的 frontend 清单。quota
        # 单位换算（bit/s → Mbps）只在这一处。
        fes = [model.FrontendConfig(name=r.name, bind_port=r.port,
                                    quota_mbps=r.rate_bits / 1e6)
               for r in rules if r.rate_bits]
        res = await self._shaper.reconcile(fes, 0)

        # conn_rate：真实网卡上的 SYN police，配置变了才重刷。
        fp = ";".join(f"{r.port}|{r.conn_rate}"
                      for r in sorted(rules, key=lambda x: x.port))
        if fp != self._syn_fp:
            for argv, fatal in _syn_cmds(self.iface, rules):
                await self._tc(argv, fatal=fatal)
            self._syn_fp = fp
        return res

    async def _teardown_rules(self) -> TcResult:
        """request_limits 清空：撤掉 ingress 钩子（重定向随之消失）与
        IFB 上的整形树，入向恢复原状。IFB 设备留着但 down 掉——删了
        再建毫无意义，down 的设备不参与任何转发。"""
        out = await self._tc(["tc", "qdisc", "show", "dev", self.iface,
                              "ingress"], fatal=False)
        if "ingress" not in out:
            return TcResult(ok=True, changed=False)
        try:
            await self._tc(["tc", "qdisc", "del", "dev", self.iface,
                            "ingress"])
        except TcError as e:
            self._log.warning(
                "撤掉入向重定向失败，下一轮兜底重试 iface=%s err=%s",
                self.iface, e)
            return TcResult(ok=False, changed=False, error=str(e))
        await self._shaper.teardown()
        await self._tc(["ip", "link", "set", self.ifb, "down"], fatal=False)
        self._syn_fp = ""
        self._log.warning(
            "已撤掉客户端侧网卡的入向整形（request_limits 已清空），"
            "入向恢复原状 iface=%s ifb=%s", self.iface, self.ifb)
        return TcResult(ok=True, changed=True, action="teardown")

    async def teardown(self) -> None:
        """停机/下线：撤钩子、清 IFB 整形树、删 IFB 设备。"""
        await self._tc(["tc", "qdisc", "del", "dev", self.iface, "ingress"],
                       fatal=False)
        await self._shaper.teardown()
        await self._tc(["ip", "link", "del", self.ifb], fatal=False)

    async def debug_state(self) -> dict:
        """调试快照（控制台"tc 实况"面板）：重定向 + IFB 上的整形树。"""
        async def run(argv: list[str]) -> str:
            rc, out, err = await self._run(argv)
            text = (out or "").strip()
            if rc != 0:
                text = (text + "\n" if text else "") + \
                    f"[命令失败 rc={rc}] {(err or '').strip()}"
            return text or "(空)"

        return {
            "iface": f"{self.iface} → {self.ifb}",
            "help": [
                "入向整形的原理：内核的入向路径没有队列，所以把真实网卡"
                "入向的包用 mirred 整段重定向到 IFB 虚拟网卡的出向路径，"
                "在那里用与出向同款的 HTB **排队慢放**——对比 police 的"
                "\"超了就丢\"，客户端看到的是平稳的慢，不是丢包重传。",
                "第一段是真实网卡上的入向过滤器：mirred（Egress Redirect "
                "to device …）即重定向规则；flower+police pkts_rate 是 "
                "SYN 速率闸（每秒新建连接上限，老内核缺模块时可能不在，"
                "字节整形不受影响）。",
                "后面是 IFB 上的 HTB，读法与出向完全一样：classid 次要号"
                "是十六进制端口（按**目的端口**分类）、rate=ceil 即硬限、"
                "dropped/overlimits 在涨说明限速正在生效、backlog 是此刻"
                "排队中的量、tokens 负数 = 正在刹车。",
                "动态闭环调速时这里的 rate 会就地变（tc class change），"
                "统计不清零——对照「期望配置」行里的动态值看。",
                "排障顺序：先看重定向在不在（没有它 IFB 上永远没流量、"
                "Sent 恒为 0）；再对 IFB 上的端口类与速率。",
            ],
            "raw": [
                {"label": f"tc filter show dev {self.iface} parent ffff:",
                 "note": "真实网卡入向：mirred 重定向（搬去 IFB）+ SYN "
                         "速率 police。",
                 "text": await run(["tc", "filter", "show", "dev",
                                    self.iface, "parent", INGRESS_PARENT])},
                {"label": f"tc -s qdisc show dev {self.ifb}",
                 "note": "IFB 上的根队列总体统计——入向整形（排队慢放）"
                         "发生在这里；Sent 恒为 0 说明重定向没生效。",
                 "text": await run(["tc", "-s", "qdisc", "show",
                                    "dev", self.ifb])},
                {"label": f"tc -s class show dev {self.ifb}",
                 "note": "一个 frontend 一个类（按目的端口分类），速率与"
                         "统计都在这段——看入向限速执行情况主要看它。",
                 "text": await run(["tc", "-s", "class", "show",
                                    "dev", self.ifb])},
                {"label": f"tc filter show dev {self.ifb}",
                 "note": "IFB 上的分流规则：match 00001f90/0000ffff at 20 "
                         "即「目的端口 = 0x1f90（8080）」。",
                 "text": await run(["tc", "filter", "show",
                                    "dev", self.ifb])},
            ],
        }
