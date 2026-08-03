# netlimit.tc —— 跑 tc 命令、读回网卡实况、把现状收敛到计划。
#
# 这个模块以 root 身份对生产机的数据面下命令，所以两条铁律：
#
#   1. **失败要吵。** 致命命令失败就抛，绝不"记一行日志然后继续"——限速
#      这件事最不能接受的故障不是报错，是"以为限住了，其实没有"。
#   2. **只看读回来的东西。** 判断"是否已经生效"一律以 `tc ... show` 的
#      实际输出为准，不以"我刚才发过命令"为准。上一个项目正是靠读回
#      cburst 才发现旧版本在网卡上留下的坏桶。

from __future__ import annotations

import re
import shutil
import subprocess
from dataclasses import dataclass

from . import plan as planmod


class TcError(Exception):
    pass


def run(argv: list[str], *, fatal: bool = True, dry: bool = False) -> str:
    """跑一条命令。dry=True 时只回显不执行。"""
    if dry:
        return ""
    try:
        p = subprocess.run(argv, capture_output=True, text=True)
    except FileNotFoundError:
        raise TcError(f"找不到命令 {argv[0]}（iproute2 没装？）") from None
    if p.returncode != 0:
        msg = (p.stderr or p.stdout).strip() or f"rc={p.returncode}"
        if fatal:
            raise TcError(f"命令失败：{' '.join(argv)}\n  {msg}")
        return ""
    return p.stdout


# ── 读回实况 ─────────────────────────────────────────────────────────────
#
# 次要号按**十六进制**读（与下发端一致）。两边只要有一边用错进制，比对就
# 永远不相等，每一轮 reconcile 都判定"不一致"然后重建整棵树。
_CLASS_RE = re.compile(
    r"class htb 1:([0-9a-f]+).*?\brate (\S+).*?\bcburst (\S+)", re.I)

_SIZE_UNITS = {"": 1, "b": 1, "k": 1024, "kb": 1024, "m": 1024 ** 2,
               "mb": 1024 ** 2, "g": 1024 ** 3, "gb": 1024 ** 3}
_RATE_UNITS = {"bit": 1, "kbit": 1000, "mbit": 1000 ** 2, "gbit": 1000 ** 3,
               "tbit": 1000 ** 4}


def parse_size(s: str) -> int:
    """tc 打出来的字节数（"1600b" / "8Mb" / "4883Kb"）→ 字节。

    注意 tc 打字节数按 **1024** 折算且**有损**：5000000 会打成 "4883Kb"
    （= 5000192）。所以任何"读回来的值等于我写下去的值"式的精确比对都会
    误判——比对必须留余量，见 _cburst_too_small。
    """
    m = re.fullmatch(r"(\d+(?:\.\d+)?)\s*([a-zA-Z]*)", s.strip())
    if not m:
        raise ValueError(f"看不懂的大小：{s!r}")
    return int(float(m.group(1)) * _SIZE_UNITS[m.group(2).lower()])


def parse_rate(s: str) -> int:
    """tc 打出来的速率（"1000Mbit"）→ bit/s。速率按 1000 折算，与字节不同。"""
    m = re.fullmatch(r"(\d+(?:\.\d+)?)\s*([a-zA-Z]*)", s.strip())
    if not m:
        raise ValueError(f"看不懂的速率：{s!r}")
    unit = m.group(2).lower() or "bit"
    if unit not in _RATE_UNITS:
        raise ValueError(f"看不懂的速率单位：{s!r}")
    return int(float(m.group(1)) * _RATE_UNITS[unit])


@dataclass
class Observed:
    """网卡上读回来的实况。"""

    rates: dict[int, int]                   # 次要号 → rate（bit/s）
    cbursts: dict[int, int]                 # 次要号 → cburst（字节）

    @property
    def empty(self) -> bool:
        return not self.rates


def parse_classes(out: str) -> Observed:
    rates: dict[int, int] = {}
    cbursts: dict[int, int] = {}
    for minor, rate, cburst in _CLASS_RE.findall(out):
        try:
            n = int(minor, 16)
            rates[n] = parse_rate(rate)
            cbursts[n] = parse_size(cburst)
        except ValueError:
            continue
    return Observed(rates=rates, cbursts=cbursts)


# tc 的 1024 折算误差（见 parse_size）足以让精确比对误判，因此留一点余量。
# 这个值只要远小于"MTU 量级"就能把真正的事故认出来：漏给 cburst 时 tc 会
# 兜底成 1600/2400 字节这种量级，而我们自己算的值至少是 4 KiB。
CBURST_SLACK_BYTES = 2048


def cburst_too_small(got: int | None, rate_bits_per_s: int) -> bool:
    """cburst 是不是被 tc 按 MTU 量级兜底了。

    这一条单独判，是因为它的故障形态极其阴：`rate` 显示得完全正确，实际
    吞吐却远低于限额——只看速率的话，一切正常。
    """
    if got is None:
        return True
    return got + CBURST_SLACK_BYTES < planmod.burst_bytes(rate_bits_per_s)


def observe(dev: str, *, runner=run) -> Observed:
    return parse_classes(runner(["tc", "class", "show", "dev", dev], fatal=False))


def redirect_present(iface: str, ifb: str, *, runner=run) -> bool:
    """这张网卡的出向是不是已经在往 ifb 重定向。

    **多网卡时必须逐张查**，不能只看 IFB 上的队列树。否则机器上后插一张
    网卡（或有人把 clsact 删了）时，那张网卡的流量会完全绕过限速——而
    IFB 上的类看起来一切正常，`status` 也报"限速在跑"。这是这个方案最
    危险的失效方式：**看起来限着，其实漏了一整张网卡。**
    """
    out = runner(["tc", "filter", "show", "dev", iface, "egress"], fatal=False)
    return f"redirect dev {ifb}" in out or f"Egress Redirect to device {ifb}" in out


@dataclass
class Result:
    ok: bool
    changed: bool
    action: str = ""            # "" | rate-change | rebuild | teardown
    error: str = ""


def reconcile(p: planmod.LimitPlan, *, runner=run, log=print) -> Result:
    """把机器的实际限速状态收敛到计划描述的样子。

    三条路径，按对流量的打扰程度从小到大：
      1. 已经一致 → 一条命令都不发。周期性跑的时候这一条最重要：不做
         "一致就不动"的判断，就会每个周期重建一次队列树、抖一次全机流量。
      2. 只有额度不同 → `tc class change`，不打断任何连接。
      3. 结构不对（首次、网卡增删、被人手工改过）→ 整棵重建。
    """
    try:
        planmod.check(p)
    except planmod.PlanError as e:
        return Result(ok=False, changed=False, error=str(e))

    if p.off:
        return _reconcile_off(p, runner=runner, log=log)

    dev = p.shaping_dev
    try:
        cur = observe(dev, runner=runner)
    except Exception as e:                  # 读不到就按重建处理
        log(f"读取 {dev} 的 tc 状态失败，按重建处理：{e}")
        cur = Observed(rates={}, cbursts={})

    want = p.rate_bits_per_s
    structure_ok = (planmod.FREE_MINOR in cur.rates
                    and planmod.LIMIT_MINOR in cur.rates)
    # 多网卡：逐张核对重定向还在不在。漏掉这一步的话，后插的网卡会
    # 完全绕过限速，而 IFB 上的类看起来完全正常（见 redirect_present）。
    if structure_ok and p.aggregated:
        missing = [i for i in p.ifaces
                   if not redirect_present(i, p.ifb, runner=runner)]
        if missing:
            log(f"这些网卡没有把出向交给 {p.ifb}，流量会绕过限速："
                f"{','.join(missing)} —— 按重建处理")
            structure_ok = False
    # 免限类的桶坏了同样要修：它承载 SSH，桶被兜底成 MTU 量级会让链路
    # 打满时连登录都变慢——而且**限速类看起来完全正常**，只能靠这里发现。
    if structure_ok and cburst_too_small(
            cur.cbursts.get(planmod.FREE_MINOR), planmod.FREE_RATE_BPS):
        log("免限类的 cburst 偏小（旧版本留下的？），按重建处理")
        structure_ok = False

    try:
        if structure_ok:
            drift_rate = cur.rates.get(planmod.LIMIT_MINOR) != want
            drift_burst = cburst_too_small(
                cur.cbursts.get(planmod.LIMIT_MINOR), want)
            if not drift_rate and not drift_burst:
                return Result(ok=True, changed=False)
            runner(planmod.rate_change_cmd(p))
            log(f"已就地调整整机限额（未重建队列树，存量连接立刻跟上）："
                f"{want / 1e6:g} Mbps")
            return Result(ok=True, changed=True, action="rate-change")

        for argv, fatal in planmod.build(p):
            runner(argv, fatal=fatal)
        log(f"已建立整机限速：{p.describe()}")
        return Result(ok=True, changed=True, action="rebuild")
    except TcError as e:
        return Result(ok=False, changed=False, error=str(e))


def _reconcile_off(p: planmod.LimitPlan, *, runner=run, log=print) -> Result:
    """"不限速"这个状态的收敛。

    机器上本来就没有限速树时**一条命令都不发**：否则周期性运行会不停地
    `qdisc del`，日志刷屏不说，别人手工建的整形规则也会被反复删掉。
    """
    dev = p.shaping_dev
    try:
        cur = observe(dev, runner=runner)
    except Exception:
        return Result(ok=True, changed=False)
    if cur.empty:
        return Result(ok=True, changed=False)
    for argv, fatal in planmod.teardown(p):
        runner(argv, fatal=fatal)
    log("整机限额已清空，**限速已关闭**（队列树已拆除，按线速放行）")
    return Result(ok=True, changed=True, action="teardown")


# ── 环境体检 ─────────────────────────────────────────────────────────────

def kernel_config_verdict(sym: str) -> str:
    """内核有没有编进某个特性——**分得清"没加载"和"根本没编"**。

    这两种情况的处置完全不同：前者 modprobe 就行，后者只能换内核。有
    /proc/config.gz 或 /boot/config-$(uname -r) 时这是确定答案，不用猜。
    """
    import gzip
    import os
    key = f"CONFIG_{sym.upper()}"
    text = ""
    try:
        with gzip.open("/proc/config.gz", "rt", errors="replace") as f:
            text = f.read()
    except OSError:
        try:
            with open(f"/boot/config-{os.uname().release}",
                      encoding="utf-8", errors="replace") as f:
                text = f.read()
        except OSError:
            return "unknown"
    if f"# {key} is not set" in text:
        # 连模块机制都关了的话，任何特性都无法事后加载。
        if "# CONFIG_MODULES is not set" in text:
            return "absent-nomodules"
        return "absent"
    if re.search(rf"^{key}=[ym]$", text, re.M):
        return "present"
    return "unknown"


def have(cmd: str) -> bool:
    return shutil.which(cmd) is not None
