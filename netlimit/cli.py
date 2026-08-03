# netlimit.cli —— 命令行入口。
#
#   netlimit status            看本机网络长相 + 当前限速实况（只读，不改任何东西）
#   netlimit doctor            体检：内核/工具/权限够不够（只读）
#   netlimit plan  -r 1000     干跑：只打印将要执行的命令，一条都不执行
#   netlimit apply -r 1000     下发：把整机出向限到 1000 Mbps
#   netlimit off               关掉限速，拆掉队列树
#
# 设计上刻意分成"只读"与"会改机器"两组，而且 plan 与 apply 用的是**同一个**
# 计划生成函数——干跑打印的就是真正会执行的东西，不存在两份逻辑对不上。

from __future__ import annotations

import argparse
import os
import sys

from . import discover as dis
from . import plan as planmod
from . import tc

DEFAULT_IFB = "ifb0"

# 环境变量接线。systemd 的 EnvironmentFile 用它，命令行参数优先。
#
# 让 CLI 自己读环境变量、而不是在 unit 里把参数拼出来：systemd 没法表达
# "这个变量为空时就整个不要 --rate 这个参数"，硬拼的话空值会变成
# `--rate ''` 之类的东西。读环境变量还顺带让"配置文件里留空 = 不限速"
# 这个语义在两条入口上完全一致。
ENV_MBPS = "NETLIMIT_MBPS"
ENV_ONLY = "NETLIMIT_ONLY"
ENV_EXCLUDE = "NETLIMIT_EXCLUDE"
ENV_EXEMPT = "NETLIMIT_EXEMPT"
ENV_IFB = "NETLIMIT_IFB"


def _env_list(name: str) -> list[str]:
    return (os.environ.get(name) or "").split()


def apply_env_defaults(args) -> None:
    """命令行没给的项，用环境变量补上。**命令行永远优先。**"""
    if getattr(args, "rate", None) is None:
        raw = (os.environ.get(ENV_MBPS) or "").strip()
        if raw:
            try:
                args.rate = float(raw)
            except ValueError:
                raise SystemExit(
                    f"{ENV_MBPS} 必须是数字（Mbps），当前值 {raw!r}；"
                    f"不想限速就把它留空") from None
    if not args.only:
        args.only = _env_list(ENV_ONLY)
    if not args.exclude:
        args.exclude = _env_list(ENV_EXCLUDE)
    if args.ifb == DEFAULT_IFB and os.environ.get(ENV_IFB):
        args.ifb = os.environ[ENV_IFB].strip()
    if args.exempt == list(planmod.DEFAULT_EXEMPT_PORTS) and os.environ.get(ENV_EXEMPT):
        try:
            args.exempt = [int(x) for x in _env_list(ENV_EXEMPT)]
        except ValueError:
            raise SystemExit(f"{ENV_EXEMPT} 必须是空格分隔的端口号") from None


def _ip_addr_output() -> str:
    if not tc.have("ip"):
        print("找不到 ip 命令（iproute2 没装）", file=sys.stderr)
        raise SystemExit(1)
    return tc.run(["ip", "-o", "addr", "show"], fatal=False)


def _topology(args) -> dis.Topology:
    return dis.discover(
        _ip_addr_output(),
        include=tuple(args.only or ()),
        exclude=tuple(args.exclude or ()))


def _plan(args, topo: dis.Topology) -> planmod.LimitPlan:
    return planmod.LimitPlan.from_mbps(
        [x.name for x in topo.shaped],
        getattr(args, "rate", None),
        exempt_ports=tuple(args.exempt),
        ifb=args.ifb)


def cmd_status(args) -> int:
    topo = _topology(args)
    print("本机网络长相：")
    for link in topo.links:
        print("  " + link.describe())
    print(f"\n{topo.summary()}")
    if topo.needs_ifb:
        print(f"  多张网卡 → 出向会先汇聚到 {args.ifb} 再整形，"
              f"这样才是**一份**总额度而不是每张网卡一份")

    if not topo.shaped:
        return 0
    p = _plan(args, topo)
    dev = p.shaping_dev
    print(f"\n{dev} 上的限速实况：")
    cur = tc.observe(dev)
    if cur.empty:
        print("  没有限速队列树 —— **当前不限速**")
        return 0
    rate = cur.rates.get(planmod.LIMIT_MINOR)
    if rate is None:
        print(f"  有队列树但没有整机限速类 {planmod.classid(planmod.LIMIT_MINOR)}"
              f" —— 多半不是本工具建的")
        return 1
    print(f"  整机限速类 {planmod.classid(planmod.LIMIT_MINOR)}："
          f"{rate / 1e6:g} Mbps")
    bad = tc.cburst_too_small(cur.cbursts.get(planmod.LIMIT_MINOR), rate)
    if bad:
        print(f"  [FAIL] cburst 只有 {cur.cbursts.get(planmod.LIMIT_MINOR)} 字节"
              f" —— **实际吞吐会远低于限额**，跑一次 apply 修好")
    else:
        print("  cburst 正常")
    return 1 if bad else 0


def cmd_doctor(args) -> int:
    ok = True
    print("整机限速前提体检：")

    for cmd, why in (("tc", "下发限速"), ("ip", "看网卡与建 IFB")):
        if tc.have(cmd):
            print(f"  [ok]   {cmd} 已安装（{why}）")
        else:
            print(f"  [FAIL] 缺 {cmd}（apt install iproute2）")
            ok = False
    if os.geteuid() != 0:
        print("  [warn] 当前不是 root：status/plan/doctor 能跑，apply/off 需要 "
              "root 或 CAP_NET_ADMIN")

    topo = _topology(args)
    need_ifb = topo.needs_ifb
    feats = [("NET_SCH_HTB", "HTB 队列规则", True, "限速本身")]
    if need_ifb:
        # 只有多网卡聚合才需要这三样。单网卡不需要，别拿无关的失败去吓人。
        feats += [
            ("IFB", "IFB 虚拟设备", True, "把多张网卡的出向汇到一处"),
            ("NET_ACT_MIRRED", "mirred 动作", True, "把出向重定向到 IFB"),
            ("NET_CLS_MATCHALL", "matchall 分类器", True, "匹配全部出向包"),
        ]
    feats += [
        ("NET_CLS_U32", "u32 分类器", False, "免限端口（SSH）"),
        ("NET_SCH_FQ_CODEL", "fq_codel", False, "类内公平排队"),
    ]
    for sym, name, required, why in feats:
        v = tc.kernel_config_verdict(sym)
        tag = "FAIL" if required else "warn"
        if v == "present":
            print(f"  [ok]   内核有 {name}（{why}）")
        elif v == "unknown":
            print(f"  [warn] 取不到内核 config，无法确认 {name}；"
                  f"实际下发时才会知道")
        else:
            print(f"  [{tag}] 内核没有 {name} —— 影响：{why}")
            print(f"         → CONFIG_{sym} is not set，**根本没编进这个内核**"
                  f"，不是没加载")
            if v == "absent-nomodules":
                print("         → 而且这个内核连模块支持都关了，无法事后加载，"
                      "只能换内核")
            if required:
                ok = False

    if need_ifb:
        print("  [warn] 多网卡聚合依赖「出向重定向到 IFB 后不会再次触发重定向」"
              "这条内核行为")
        print("         本仓库**没有实测过**它（开发机内核没有 IFB）。"
              "上线前请按 docs/11 的方法核对一次")
    print("\n体检结论：" + ("全部通过，限速可以工作" if ok
                            else "**有失败项，限速不会生效**"))
    return 0 if ok else 1


def cmd_plan(args) -> int:
    topo = _topology(args)
    p = _plan(args, topo)
    try:
        cmds = planmod.build(p)
    except planmod.PlanError as e:
        print(f"计划不成立：{e}", file=sys.stderr)
        return 1
    print(f"# {p.describe()}")
    if p.off:
        print("# 未给 -r/--rate，含义是**不限速**：下面是把已有限速拆掉的命令。")
    print("# 以下命令一条都没有执行：")
    for argv, fatal in cmds:
        print("  " + " ".join(argv) + ("" if fatal else "    # 失败不致命"))
    if not p.off:
        print("\n# 只改额度时不重建、不打断连接：")
        print("  " + " ".join(planmod.rate_change_cmd(p)))
    return 0


def _require_root() -> None:
    if os.geteuid() != 0:
        print("需要 root（或 CAP_NET_ADMIN）才能改限速", file=sys.stderr)
        raise SystemExit(1)


def cmd_apply(args) -> int:
    _require_root()
    topo = _topology(args)
    p = _plan(args, topo)
    print(f"目标：{p.describe()}")
    res = tc.reconcile(p)
    if not res.ok:
        print(f"下发失败，**限速未生效**：{res.error}", file=sys.stderr)
        return 1
    if not res.changed:
        print("已经是目标状态，未做任何改动")
    return 0


def cmd_off(args) -> int:
    _require_root()
    topo = _topology(args)
    args.rate = None                    # off 就是"没设限额"这个状态
    p = _plan(args, topo)
    res = tc.reconcile(p)
    if not res.ok:
        print(f"关闭失败：{res.error}", file=sys.stderr)
        return 1
    if not res.changed:
        print("本机本来就没有限速，未做任何改动")
    return 0


def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(
        prog="netlimit",
        description="本机全网出向限速：整机一个总闸门，支持单网卡多 IP / 多网卡多 IP")
    ap.add_argument("--only", action="append", metavar="IFACE",
                    help="只限这些网卡（可重复）。给了它就完全绕过自动发现"
                         "——自动判断再周全也有猜错的时候")
    ap.add_argument("--exclude", action="append", metavar="IFACE",
                    help="在自动发现之外额外排除的网卡（可重复）")
    ap.add_argument("--ifb", default=DEFAULT_IFB,
                    help="多网卡聚合用的 IFB 设备名（默认 %(default)s）")
    ap.add_argument("--exempt", type=int, action="append",
                    default=list(planmod.DEFAULT_EXEMPT_PORTS), metavar="PORT",
                    help="免限的本机服务端口（可重复，默认 22）。"
                         "整机限速把 SSH 也罩住，链路打满时会登不上机器")
    sub = ap.add_subparsers(dest="cmd", required=True)

    def _shared(sp):
        """全局选项在子命令上再收一遍，`netlimit plan --only eth0` 与
        `netlimit --only eth0 plan` 两种写法就都能用——运维不该被 argparse
        的选项位置规则绊住。

        子命令上一律用 SUPPRESS 而不是 default=...：否则子命令的默认值会
        把主解析器**已经解析到的**值覆盖掉（给了 --only 反而没生效）。
        """
        sp.add_argument("--only", action="append", default=argparse.SUPPRESS)
        sp.add_argument("--exclude", action="append", default=argparse.SUPPRESS)
        sp.add_argument("--ifb", default=argparse.SUPPRESS)
        sp.add_argument("--exempt", type=int, action="append",
                        default=argparse.SUPPRESS)
        return sp

    for name, fn, desc in (
        ("status", cmd_status, "看网络长相与限速实况（只读）"),
        ("doctor", cmd_doctor, "体检：内核/工具/权限够不够（只读）"),
        ("off", cmd_off, "关掉限速，拆掉队列树"),
    ):
        _shared(sub.add_parser(name, help=desc)).set_defaults(func=fn)

    for name, fn, desc in (("plan", cmd_plan, "干跑：只打印命令，不执行"),
                           ("apply", cmd_apply, "下发限速")):
        sp = _shared(sub.add_parser(name, help=desc))
        sp.add_argument("-r", "--rate", type=float, metavar="MBPS",
                        help="整机出向总额度（**Mbps**）。"
                             "不给 = 不限速（默认值不该成为限制）")
        sp.set_defaults(func=fn)
    return ap


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    apply_env_defaults(args)
    try:
        return args.func(args)
    except (planmod.PlanError, tc.TcError) as e:
        print(f"错误：{e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
