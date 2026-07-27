# rl_limiter.enforcer —— 把配置库里登记的限额**落到本机 HAProxy 上**。
#
# 这是同机部署的核心价值：rl-limiter 与 HAProxy 在同一台机器上，因此能
# 直接改本机 haproxy.cfg 并触发 reload，让"改完立刻生效"成立——跨机的
# 集中服务做不到这件事（要么开 SSH，要么另装 agent）。
#
# ## 为什么必须走"改配置 + reload"，而不是 runtime API
#
# 实测 HAProxy 2.8.16（Ubuntu 24.04 自带版本）：
#
#   - **shared bwlim 的 limit 是配置常量，运行期改不了**。给
#     `set-bandwidth-limit` 带上动态 limit 表达式会在配置解析阶段就被
#     拒绝：`set-bandwidth-limit rule cannot define a limit for a shared
#     bwlim filter`；runtime API 的命令表里也没有任何 bwlim/bandwidth
#     相关命令（`show/set` 都没有）。
#   - 带动态 limit（map 表 + `set map` 热更）只有 **per-stream** 形态支持
#     ——而 per-stream 正是设计文档 §3.2 记载的、生产事故后废弃的方案
#     （限速值建连时定格、按连接数均分、正反馈锁死）。不能为了"免 reload"
#     退回去。
#
# 所以路径只剩一条：**原地改 cfg 里的 limit 数值 + reload**。实测这条路
# 足够快，"立刻生效"名副其实（同一台机器、400MB 下载、hard-stop-after 6s）：
#
#   | 观测项 | 实测 |
#   | ------ | ---- |
#   | 新 worker 接管 | ~23 ms |
#   | reload 后**新建**连接 | 立刻按新限额（1→4 MB/s，实测稳定 4.00 MB/s）|
#   | **存量**连接 | 保持旧限额，直到 hard-stop-after 宽限期结束被断开重连 |
#
# 存量连接的行为由运维自己的 `hard-stop-after` 决定，本模块不碰它：
# 宽限期短 = 限额调整对存量连接也快速生效（代价是长下载被断开重来），
# 宽限期长/不设 = 存量连接自然放完。这是业务取舍，不该由限速器替运维定。
#
# ## 安全边界（本模块唯一有写权限的地方，逐条都是刻意的）
#
#   1. **只做定点数值替换，绝不重新生成整份 cfg**。运维手写的一切
#      （ACL、后端、TLS、日志……）原样不动——本模块只认受控 frontend 段
#      里那一行 `filter bwlim-out ... limit <数字>`，且只改那个数字。
#   2. **reload 前必过 `haproxy -c`**。把坏配置 reload 进生产 = 整台机器
#      的入口挂掉，比限额没改过去严重得多。校验不过就原样留着并告警。
#   3. **写入是原子的**（同目录临时文件 + rename），并留一份 .bak。
#      任何一步失败都回到调用前的状态。
#   4. **reload 命令绝不来自数据库**。它是本机环境变量。若允许配置库指定
#      要执行的命令，拿到库写权限 = 在每一台 HAProxy 上远程执行任意命令。
#   5. **默认关闭**。不设 RL_APPLY_HAPROXY_CFG 就完全不写盘、不 reload，
#      退化成原来的只读监控 + 漂移告警。

from __future__ import annotations

import asyncio
import logging
import os
import re
import shlex
import tempfile
from dataclasses import dataclass, field

# 段落起始关键字：出现在行首（顶格）时开启一个新的配置段。用来把 cfg
# 切成段，从而把"改哪一行"限制在目标 frontend 自己的段落里——同一份 cfg
# 里多个 frontend 各有自己的 bwlim 行，认错段就会改到别的环境头上。
_SECTION_KEYWORDS = (
    "global", "defaults", "listen", "frontend", "backend", "resolvers",
    "peers", "userlist", "ring", "http-errors", "program", "mailers",
    "cache", "log-forward", "traces", "crt-store", "acme",
)

# 顶格的段落头，形如 `listen fe_env_a` / `frontend fe_env_a` / `global`。
_SECTION_RE = re.compile(
    r"^(" + "|".join(_SECTION_KEYWORDS) + r")(?:\s+(\S+))?\s*$"
)

# 受控的 shared bwlim 行。只匹配 bwlim-out（下行整形；上行由 TCP 背压
# 自然收敛，见设计 §3.2），且必须带 `limit <数字>`——per-stream 形态用的
# 是 `default-limit`，不会被这个正则命中，等于天然把"只改 shared 限额"
# 这条约束写进了匹配规则。
_BWLIM_RE = re.compile(
    r"^(?P<head>\s*filter\s+bwlim-out\s+\S+\s+(?:.*?\s)??limit\s+)"
    r"(?P<limit>\d+)"
    r"(?P<tail>(?:\s.*)?)$"
)


class EnforceError(Exception):
    """应用限额失败。消息面向运维，说清楚"卡在哪一步、现在是什么状态"。"""


@dataclass(slots=True)
class EnforceResult:
    """一次 reconcile 的结果，用于日志与控制台展示。"""

    changed: bool = False                      # 是否真的改了 cfg 并 reload
    applied: dict[str, int] = field(default_factory=dict)   # frontend → 新 limit(bytes/s)
    previous: dict[str, int] = field(default_factory=dict)  # frontend → 旧 limit(bytes/s)
    error: str = ""                            # 非空表示失败（保持原状）

    @property
    def ok(self) -> bool:
        return not self.error


def parse_limits(text: str) -> dict[str, int]:
    """从 haproxy.cfg 文本里读出每个 frontend 当前的 shared bwlim limit
    （bytes/s）。

    返回 {frontend 名: limit}。没有 bwlim 行的段落不出现在结果里——
    调用方据此判断"这个 frontend 根本没配限速"，那是配置缺失，不是 0。
    """
    out: dict[str, int] = {}
    section_name = ""
    section_kind = ""
    for line in text.splitlines():
        m = _SECTION_RE.match(line)
        if m:
            section_kind, section_name = m.group(1), (m.group(2) or "")
            continue
        if section_kind not in ("listen", "frontend") or not section_name:
            continue
        bm = _BWLIM_RE.match(line)
        if bm:
            out[section_name] = int(bm.group("limit"))
    return out


def replace_limits(text: str, desired: dict[str, int]) -> tuple[str, dict[str, int]]:
    """把 desired（frontend → 目标 limit bytes/s）写进 cfg 文本。

    返回 (新文本, {frontend: 旧值})，只有确实发生变化的 frontend 才出现在
    旧值字典里。不做任何其它改动：行的缩进、注释、参数顺序全部保留，
    只有那个十进制数字被换掉。

    目标 frontend 不存在、或它的段里没有 shared bwlim 行、或有多行，
    一律抛 EnforceError——这三种情况下"猜一个去改"比不改危险得多。
    """
    lines = text.splitlines(keepends=True)
    section_name = ""
    section_kind = ""
    hits: dict[str, list[int]] = {}
    for i, line in enumerate(lines):
        m = _SECTION_RE.match(line.rstrip("\n"))
        if m:
            section_kind, section_name = m.group(1), (m.group(2) or "")
            continue
        if section_kind not in ("listen", "frontend") or section_name not in desired:
            continue
        if _BWLIM_RE.match(line.rstrip("\n")):
            hits.setdefault(section_name, []).append(i)

    missing = [f for f in desired if f not in hits]
    if missing:
        raise EnforceError(
            f"haproxy.cfg 里找不到这些受控 frontend 的 shared bwlim 配置行："
            f"{', '.join(sorted(missing))}——请确认该 frontend 段里有形如 "
            f"`filter bwlim-out <名字> limit <字节/秒> key ...` 的一行"
            f"（per-stream 的 default-limit 不算，见 deploy/haproxy/bwlim-example.cfg）"
        )
    ambiguous = {f: len(v) for f, v in hits.items() if len(v) > 1}
    if ambiguous:
        raise EnforceError(
            f"这些 frontend 的段里有多行 shared bwlim 配置，无法确定改哪一行："
            f"{ambiguous}——请人工收敛成一行后再启用自动应用"
        )

    previous: dict[str, int] = {}
    for frontend, idxs in hits.items():
        i = idxs[0]
        raw = lines[i]
        newline = "\n" if raw.endswith("\n") else ""
        bm = _BWLIM_RE.match(raw.rstrip("\n"))
        assert bm is not None                       # 上面已按同一正则筛过
        old = int(bm.group("limit"))
        want = desired[frontend]
        if old == want:
            continue
        previous[frontend] = old
        lines[i] = f"{bm.group('head')}{want}{bm.group('tail')}{newline}"
    return "".join(lines), previous


class HAProxyEnforcer:
    """把限额落到本机 haproxy.cfg 并 reload。

    cfg_path / reload_cmd / validate_cmd 全部来自**本机**配置（环境变量），
    绝不来自配置库——见模块头 §安全边界 第 4 条。
    """

    def __init__(self, cfg_path: str, reload_cmd: str,
                 log: logging.Logger | None = None,
                 validate_cmd: str = "haproxy -c -f {cfg}"):
        self._cfg_path = cfg_path
        self._reload_cmd = reload_cmd
        self._validate_cmd = validate_cmd
        self._log = log if log is not None else logging.getLogger(__name__)

    @property
    def cfg_path(self) -> str:
        return self._cfg_path

    def current_limits(self) -> dict[str, int]:
        """读出本机 cfg 当前的限额（bytes/s），供控制台展示"数据面实际值"。"""
        with open(self._cfg_path, "r", encoding="utf-8") as f:
            return parse_limits(f.read())

    async def reconcile(self, desired: dict[str, int]) -> EnforceResult:
        """让本机 cfg 收敛到 desired（frontend → limit bytes/s）。

        幂等：已经一致就什么都不做（不写盘、不 reload），返回 changed=False。
        因此它既是"配置变更时立刻生效"的执行者，也是"有人手改了 cfg"时的
        自动纠偏——配置库是唯一真相源这件事，靠周期性调用它来维持。
        """
        if not desired:
            return EnforceResult()
        try:
            with open(self._cfg_path, "r", encoding="utf-8") as f:
                original = f.read()
            new_text, previous = replace_limits(original, desired)
        except EnforceError as e:
            return EnforceResult(error=str(e))
        except OSError as e:
            return EnforceResult(error=f"读取 {self._cfg_path} 失败: {e}")

        if not previous:
            return EnforceResult()                  # 已一致，无需改动

        try:
            await self._write_validated(new_text)
        except EnforceError as e:
            return EnforceResult(error=str(e))

        try:
            await self._run(self._reload_cmd, "reload")
        except EnforceError as e:
            # reload 失败时 HAProxy 仍在按**旧**配置服务（新 worker 没起来）。
            # 把文件也退回旧内容，让磁盘状态与实际运行状态保持一致——否则
            # 下次机器重启会悄悄用上这份从未被验证过能 reload 的配置。
            await self._restore_backup()
            return EnforceResult(
                error=f"{e}（cfg 已回滚，HAProxy 仍按调整前的限额运行）")

        applied = {f: desired[f] for f in previous}
        self._log.warning(
            "已把登记限额应用到本机 HAProxy 并 reload（数据面已按新限额执行） "
            "cfg=%s changes=%s reload_cmd=%r",
            self._cfg_path,
            ";".join(f"{f}:{previous[f]}->{applied[f]}bytes/s" for f in sorted(previous)),
            self._reload_cmd,
        )
        return EnforceResult(changed=True, applied=applied, previous=previous)

    async def _write_validated(self, new_text: str) -> None:
        """原子替换 cfg，但**先让 haproxy -c 校验通过**。

        临时文件放在 cfg 同目录：一来 rename 才是原子的（跨文件系统不是），
        二来 cfg 里的相对路径 include/crt 才能被校验命中。
        """
        d = os.path.dirname(os.path.abspath(self._cfg_path)) or "."
        tmp_path = ""
        try:
            fd, tmp_path = tempfile.mkstemp(
                dir=d, prefix=".rl-limiter-", suffix=".cfg")
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                f.write(new_text)
            # 校验用的临时文件权限默认 0600，haproxy -c 以当前用户跑，没问题；
            # 但最终落盘的文件要沿用原文件的权限，避免把 cfg 改成 0600 后
            # 别的运维工具读不了。
            try:
                os.chmod(tmp_path, os.stat(self._cfg_path).st_mode & 0o7777)
            except OSError:
                pass
            await self._run(
                self._validate_cmd.format(cfg=shlex.quote(tmp_path)),
                "配置校验", cwd=d)
            os.replace(self._cfg_path, self._cfg_path + ".rl-bak")
            os.replace(tmp_path, self._cfg_path)
            tmp_path = ""
        except EnforceError:
            raise
        except OSError as e:
            raise EnforceError(f"写入 {self._cfg_path} 失败: {e}") from None
        finally:
            if tmp_path and os.path.exists(tmp_path):
                try:
                    os.unlink(tmp_path)
                except OSError:
                    pass

    async def _restore_backup(self) -> None:
        bak = self._cfg_path + ".rl-bak"
        try:
            if os.path.exists(bak):
                os.replace(bak, self._cfg_path)
        except OSError as e:
            self._log.error(
                "回滚 haproxy.cfg 失败，磁盘上的配置可能与运行中的不一致，"
                "请人工核对 cfg=%s bak=%s err=%s", self._cfg_path, bak, e)

    async def _run(self, cmd: str, what: str, cwd: str | None = None) -> str:
        """跑一条本机命令，失败抛 EnforceError（带 stderr，便于排障）。"""
        try:
            proc = await asyncio.create_subprocess_shell(
                cmd, cwd=cwd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT)
            out, _ = await asyncio.wait_for(proc.communicate(), timeout=30)
        except asyncio.TimeoutError:
            raise EnforceError(f"{what}超时（30s）: {cmd!r}") from None
        except OSError as e:
            raise EnforceError(f"{what}无法执行: {cmd!r} ({e})") from None
        text = (out or b"").decode("utf-8", "replace").strip()
        if proc.returncode != 0:
            raise EnforceError(
                f"{what}失败（退出码 {proc.returncode}）: {cmd!r}\n{text}")
        return text


async def run_enforcer(
    enforcer: HAProxyEnforcer,
    desired_fn,
    config_applied: asyncio.Event,
    log: logging.Logger,
    on_result=None,
    period_s: float = 30.0,
) -> None:
    """常驻任务：让本机 HAProxy 的限额持续收敛到配置库登记值。

    两种触发，缺一不可：

      - **配置变更即触发**（config_applied 事件）：这是"改完立刻生效"
        那条路径——控制台或 SQL 改了 quota_bps，配置轮询把它送进监控
        循环，循环 set 事件，这里立刻 reconcile 并 reload。
      - **周期性兜底**（period_s）：有人手改了 cfg、或上一次 reload 失败
        需要重试时，靠它把状态拉回来。配置库是唯一真相源这件事，是靠
        持续 reconcile 维持的，不是靠"变更时改一次"。

    desired_fn() 返回 {frontend: limit bytes/s}；失败只记录不抛，让任务
    活到下一轮——限额应用失败绝不能顺带把监控也带走。
    """
    log.info(
        "限额自动应用已启用（本机 HAProxy 的 limit 将持续跟随配置库） "
        "cfg=%s reload_cmd=%r reconcile_period_s=%s",
        enforcer.cfg_path, enforcer._reload_cmd, period_s)
    last_error = ""
    while True:
        try:
            desired = desired_fn()
        except Exception as e:                       # pragma: no cover - 防御
            log.error("计算目标限额失败，本轮跳过 err=%s", e)
            desired = {}
        if desired:
            result = await enforcer.reconcile(desired)
            if result.error:
                # 同一个错误反复刷屏没有信息量；变化了才再喊一次。
                if result.error != last_error:
                    log.error(
                        "把登记限额应用到本机 HAProxy 失败，数据面维持原状"
                        "（限速仍在按调整前的值执行，监控与告警不受影响） "
                        "cfg=%s err=%s", enforcer.cfg_path, result.error)
                last_error = result.error
            else:
                last_error = ""
            if on_result is not None:
                on_result(result)
        config_applied.clear()
        try:
            await asyncio.wait_for(config_applied.wait(), timeout=period_s)
        except asyncio.TimeoutError:
            pass                                     # 周期性兜底轮次
