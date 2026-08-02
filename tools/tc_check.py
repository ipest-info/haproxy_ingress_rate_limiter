#!/usr/bin/env python3
# tools/tc_check.py —— 检查 tc 限速：干跑看命令、体检看环境、核对看实况。
#
# 限速静默失效是本项目最不能接受的故障（以为限住了，其实没有）。这个脚本
# 就是为"怎么确认它真的在限"准备的，三个子命令对应三个时机：
#
#   plan    上线前：只打印**将要执行**的 tc 命令，一条都不执行。
#           可以直接 diff 给同事看，或者手工粘到机器上试。
#   doctor  部署时：逐条实测三个前提（tc 在不在、有没有 CAP_NET_ADMIN、
#           内核有没有 HTB），缺哪个说哪个。容器入口脚本做的是同一套检查。
#   verify  运行中：读回网卡上的实际状态，与配置逐条核对，不一致就指出来。
#
# 用法（配置来源与 rl-limiter 一致：设了 RL_MYSQL_HOST 走数据库，否则 -c）：
#
#   python3 tools/tc_check.py plan   -c /etc/rl-limiter/config.yaml -i eth0
#   python3 tools/tc_check.py doctor -i eth0
#   python3 tools/tc_check.py verify -c /etc/rl-limiter/config.yaml -i eth0

from __future__ import annotations

import argparse
import asyncio
import os
import subprocess
import sys

# 本脚本以 `python3 tools/tc_check.py` 直跑，sys.path[0] 是 tools/ 目录，
# 因此要把仓库根加进来才导得到 rl_limiter 包（与其它 tools 脚本同样的处境，
# 只是它们不需要导这个包）。
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from rl_limiter import config as configmod  # noqa: E402
from rl_limiter import tcshaper as T  # noqa: E402


def _load_cfg(path: str):
    """读配置。**配置写错是运维最常见的情况，不该甩一段 traceback 出来**
    ——校验器的报错信息本身就是人话（"限速范围是 host 时必须给正的
    haproxy.host_quota_mbps，当前值 0.0"），直接打出来即可。"""
    try:
        return configmod.load(path)
    except (ValueError, OSError) as e:
        print(f"读取配置失败：{e}", file=sys.stderr)
        raise SystemExit(1) from None


def _plan(cfg) -> "T.ShapePlan":
    """从配置造出限速计划——两种范围（整机 / 按 frontend）都走这一条。"""
    return T.ShapePlan.from_config(
        cfg.haproxy.limit_scope, cfg.haproxy.host_quota_mbps, cfg.frontends)


def cmd_plan(args) -> int:
    """只打印将要执行的 tc 命令，绝不执行。"""
    cfg = _load_cfg(args.config)
    plan = _plan(cfg)
    try:
        T._check_plan(plan)
    except T.TcError as e:
        print(f"配置无法整形：{e}", file=sys.stderr)
        return 1

    print(f"# 网卡 {args.iface}，限速范围 {plan.scope}——{plan.describe()}")
    if plan.shaping_off:
        # 默认状态：整机范围但没设限额。这里**没有命令可打**，说清楚就行
        # ——打一句"不限速"比打一堆命令再让人自己看出来要好。
        print("#")
        print("# 这台机器当前不做限速，网卡上不会有任何 tc 队列树。")
        print("# 要限速：给 haproxy.host_quota_mbps 填一个正数（Mbps），")
        print("# 或者把 limit_scope 改成 frontend 按监听端口分别限速。")
        return 0

    print("# 首次运行 / 结构变化时执行以下序列（重建整棵队列树）：")
    for argv, fatal in T._rebuild_cmds(args.iface, plan):
        note = "" if fatal else "    # 失败不致命"
        print("  " + " ".join(argv) + note)
    print()
    print("# 只改限额时不重建，就地改（不打断任何连接）：")
    for minor, rate in sorted(T.desired_rates(plan).items()):
        label = ("整机" if plan.scope == T.SCOPE_HOST
                 else next(f.name for f in cfg.frontends if f.bind_port == minor))
        print(f"  # {label}: {rate / 1e6:g} Mbps")
        print("  " + " ".join(T.rate_change_cmd(args.iface, minor, rate)))
    return 0


def cmd_doctor(args) -> int:
    """逐条实测限速的三个前提。三条全过才可能真的限得住。"""
    ok = True

    def probe(desc: str, argv: list[str], hint: str) -> bool:
        p = subprocess.run(argv, capture_output=True, text=True)
        if p.returncode == 0:
            print(f"  [ok]   {desc}")
            return True
        print(f"  [FAIL] {desc}")
        print(f"         {(p.stderr or p.stdout).strip().splitlines()[:1]}")
        print(f"         → {hint}")
        return False

    print("限速前提体检：")
    if subprocess.run(["which", "tc"], capture_output=True).returncode != 0:
        print("  [FAIL] 找不到 tc（iproute2 未安装）")
        print("         → 容器：镜像里 apt install iproute2；宿主机同理")
        return 1
    print("  [ok]   tc 已安装")

    # CAP_NET_ADMIN 与 HTB 支持：都用 lo 上加一个再删掉来实测，比解析
    # capsh 输出或猜内核配置可靠。
    ok &= probe("有 CAP_NET_ADMIN（能操作网络设备）",
                ["tc", "qdisc", "add", "dev", "lo", "root", "handle", "9999:", "pfifo"],
                "容器里加 cap_add: [\"NET_ADMIN\"]；systemd 用 AmbientCapabilities")
    subprocess.run(["tc", "qdisc", "del", "dev", "lo", "root"], capture_output=True)

    ok &= probe("内核支持 HTB 调度器（sch_htb）",
                ["tc", "qdisc", "add", "dev", "lo", "root", "handle", "9999:", "htb"],
                "宿主机 modprobe sch_htb；内核没编译该模块则需换内核")
    subprocess.run(["tc", "qdisc", "del", "dev", "lo", "root"], capture_output=True)

    if args.iface:
        ok &= probe(f"网卡 {args.iface} 存在",
                    ["tc", "qdisc", "show", "dev", args.iface],
                    "用 ip link 确认网卡名；或用 RL_TC_IFACE 指定正确的网卡")
    print("体检结论：" + ("全部通过，限速可以工作" if ok else "**有失败项，限速不会生效**"))
    return 0 if ok else 1


def cmd_verify(args) -> int:
    """读回网卡实况，与配置逐条核对。"""
    cfg = _load_cfg(args.config)
    fes = cfg.frontends
    plan = _plan(cfg)
    sh = T.TcShaper(args.iface)
    classes, cbursts, filtered = asyncio.run(sh.observe())

    print(f"网卡 {args.iface} 实况核对：")
    if plan.shaping_off:
        # 配置说不限速，那"网卡上干干净净"才是一致，有队列树反而是不一致
        # （多半是改成不限速之前留下的，下一轮 reconcile 会拆掉）。
        if not classes:
            print("  [ok]   配置为不限速，网卡上确实没有限速队列树")
            print("核对结论：与配置一致")
            return 0
        print(f"  [warn] 配置为不限速，但网卡上还有 {len(classes)} 个限速类"
              f" —— 多半是改成不限速之前留下的，下一轮 reconcile 会拆掉")
        print("核对结论：**数据面还在限速，配置说不限**（等一个 reconcile 周期）")
        return 1

    want = T.desired_rates(plan)
    bad_default = 0
    if T.DEFAULT_CLASS_MINOR not in classes:
        print(f"  [FAIL] 没有兜底类 1:{T.DEFAULT_CLASS_MINOR} —— 队列树多半"
              f"根本没建起来（rl-limiter 没在跑？或 tc 下发失败）")
        return 1
    if T._cburst_too_small(cbursts.get(T.DEFAULT_CLASS_MINOR),
                           T.DEFAULT_CLASS_RATE_BPS):
        # 这一条是整机级的：兜底类承载本机所有未受管流量，它的桶被兜底成
        # MTU 量级会让整台机器降速，而每个 frontend 看起来都完全正常。
        print(f"  [FAIL] 兜底类 1:{T.DEFAULT_CLASS_MINOR} 的 cburst 只有 "
              f"{cbursts.get(T.DEFAULT_CLASS_MINOR)} 字节 —— "
              f"**本机未受管流量会被它整体压住**（见 docs/06 §9）")
        bad_default = 1
    else:
        print("  [ok]   兜底类存在且 cburst 正常（未分类流量按线速放行）")

    bad = bad_default
    if plan.scope == T.SCOPE_HOST:
        # 整机范围下只有一个受限类，没有"每个 frontend 一条"这回事。
        rate = want[T.HOST_CLASS_MINOR]
        got = classes.get(T.HOST_CLASS_MINOR)
        if got is None:
            print(f"  [FAIL] 没有整机限速类 {T.classid_for(T.HOST_CLASS_MINOR)}"
                  f" —— **本机未被限速**")
            bad += 1
        elif got != rate:
            print(f"  [FAIL] 整机限额不符：配置 {rate} bit/s，实际 {got} bit/s")
            bad += 1
        elif T._cburst_too_small(cbursts.get(T.HOST_CLASS_MINOR), rate):
            print(f"  [FAIL] 整机限速类的 cburst 只有 "
                  f"{cbursts.get(T.HOST_CLASS_MINOR)} 字节 —— 实际吞吐会远低于限额")
            bad += 1
        else:
            print(f"  [ok]   整机限速 {rate / 1e6:g} Mbps 已生效")
        print("核对结论：" + ("与配置一致" if bad == 0
                          else f"**{bad} 项不一致，限速未按配置生效**"))
        return 0 if bad == 0 else 1

    by_port = {f.bind_port: f for f in fes}
    for port in sorted(want):
        f = by_port[port]
        got = classes.get(port)
        has_filter = port in filtered
        if got is None:
            print(f"  [FAIL] {f.name} (:{port}) 没有对应的限速类 —— **该端口未被限速**")
            bad += 1
        elif got != want[port]:
            print(f"  [FAIL] {f.name} (:{port}) 限额不符："
                  f"配置 {want[port]} bit/s，实际 {got} bit/s")
            bad += 1
        elif not has_filter:
            print(f"  [FAIL] {f.name} (:{port}) 有限速类但**没有分类规则**，"
                  f"流量不会进这个类 —— 等于没限速")
            bad += 1
        elif T._cburst_too_small(cbursts.get(port), want[port]):
            # cburst 才是 ceil 那一路的桶。它被 tc 按 MTU 量级兜底的话，
            # rate 显示完全正常，实际吞吐却上不去（见 docs/06 §9 那次事故）。
            print(f"  [FAIL] {f.name} (:{port}) cburst 只有 "
                  f"{cbursts.get(port)} 字节，应约 "
                  f"{T.burst_bytes(want[port] / 8)} —— **实际吞吐会远低于限额**")
            bad += 1
        else:
            print(f"  [ok]   {f.name} (:{port}) {got} bit/s，分类规则在位")

    extra = (set(classes) - {T.DEFAULT_CLASS_MINOR}) - set(want)
    for port in sorted(extra):
        print(f"  [warn] 网卡上有配置里没有的限速类 {T.classid_for(port)}"
              f"（端口 {port}）—— "
              f"多半是删掉的 frontend 残留，下一轮 reconcile 会清掉")

    print("核对结论：" + ("与配置一致" if bad == 0 else f"**{bad} 项不一致，限速未按配置生效**"))
    return 0 if bad == 0 else 1


DEFAULT_CONFIG = "/etc/rl-limiter/config.yaml"


def main() -> int:
    ap = argparse.ArgumentParser(
        description="检查 tc 限速：plan 干跑看命令 / doctor 体检环境 / verify 核对实况")
    # 主解析器与各子命令都收 -c/-i，这样 `tc_check.py plan -i eth0` 与
    # `tc_check.py -i eth0 plan` 两种写法都能用——运维不该被 argparse 的
    # 选项位置规则绊住。子命令上用 SUPPRESS 而不是 default=None：否则
    # 子命令的默认值会把主解析器已经解析到的值覆盖掉。
    ap.add_argument("-c", "--config", default=DEFAULT_CONFIG,
                    help="YAML 配置路径（默认 %(default)s）")
    ap.add_argument("-i", "--iface", default="",
                    help="网卡名；不给则自动取默认路由的出口网卡")
    sub = ap.add_subparsers(dest="cmd", required=True)
    for name, fn, desc in (
        ("plan", cmd_plan, "只打印将要执行的 tc 命令，一条都不执行"),
        ("doctor", cmd_doctor, "逐条实测限速的三个前提"),
        ("verify", cmd_verify, "读回网卡实况，与配置逐条核对"),
    ):
        sp = sub.add_parser(name, help=desc)
        sp.add_argument("-c", "--config", default=argparse.SUPPRESS)
        sp.add_argument("-i", "--iface", default=argparse.SUPPRESS)
        sp.set_defaults(func=fn)

    args = ap.parse_args()
    if not args.iface:
        args.iface = T.resolve_iface("")
        if not args.iface and args.cmd != "doctor":
            print("无法自动确定网卡，请用 -i 指定", file=sys.stderr)
            return 1
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
