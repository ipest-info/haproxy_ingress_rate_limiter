# rl_limiter.enforcer —— 把配置渲染进本机 haproxy.cfg 的**受管区块**并 reload。
#
# 这是同机部署的核心价值：rl-limiter 与 HAProxy 在同一台机器上，因此能
# 直接改本机 haproxy.cfg 并触发 reload，让"在 Web 界面上改完立刻生效"
# 成立——跨机的集中服务做不到这件事（要么开 SSH，要么另装 agent）。
#
# ## 受管区块（managed block）
#
# cfg 里用一对标记圈出一段，**只有这段由 rl-limiter 生成**：
#
#     # >>> BEGIN rl-limiter managed >>>
#     listen fe_main
#         bind :8080
#         ...
#     # <<< END rl-limiter managed <<<
#
# 标记之外的一切（global、defaults、TLS、ACL、日志、运维手写的其它
# backend…）原样保留，rl-limiter 一个字节都不碰。这样既能把监听端口、
# 后端服务器、限额都做进标准化界面，又不夺走运维手写配置的空间。
#
# 首次运行时若文件里没有标记，区块会被**追加到文件末尾**——这让"给一台
# 已有的 HAProxy 接上 rl-limiter"不需要先手工改配置。
#
# ## 为什么限速必须走"改配置 + reload"，而不是 runtime API
#
# 实测 HAProxy 2.8.16（Ubuntu 24.04 自带版本）：
#
#   - **shared bwlim 的 limit 是配置常量，运行期改不了**。给
#     `set-bandwidth-limit` 带上动态 limit 表达式会在配置解析阶段就被
#     拒绝：`set-bandwidth-limit rule cannot define a limit for a shared
#     bwlim filter`；runtime API 的命令表里也没有任何 bwlim/bandwidth
#     相关命令。
#   - 带动态 limit（map 表 + `set map` 热更）只有 **per-stream** 形态支持
#     ——而 per-stream 正是设计文档 §3.2 记载的、生产事故后废弃的方案
#     （限速值建连时定格、按连接数均分、正反馈锁死）。不能为了"免 reload"
#     退回去。
#
# 何况监听端口、后端服务器这些本来就只能靠改配置 + reload 生效。实测这
# 条路足够快，"立刻生效"名副其实（同机、400MB 下载、hard-stop-after 6s）：
#
#   | 观测项 | 实测 |
#   | ------ | ---- |
#   | 一次完整应用（校验 + 原子写 + reload） | ~74 ms |
#   | 新 worker 接管 | ~23 ms |
#   | reload 后**新建**连接 | 立刻按新限额（1→4 MB/s，实测稳定 4.00 MB/s）|
#   | **存量**连接 | 保持旧限额，直到 hard-stop-after 宽限期结束被断开重连 |
#
# 存量连接的行为由运维自己的 `hard-stop-after` 决定，本模块不碰它。
#
# ## 安全边界（本模块是全服务唯一有写权限的地方，逐条都是刻意的）
#
#   1. **只重写标记之间的内容**，标记之外一律不动；
#   2. **reload 前必过 `haproxy -c`**。把坏配置 reload 进生产 = 整台机器
#      的入口挂掉，比配置没改过去严重得多。校验不过就原样留着并告警；
#   3. **写入是原子的**（同目录临时文件 + rename），并留一份 .rl-bak。
#      任何一步失败都回到调用前的状态；
#   4. **reload 命令绝不来自数据库**。它是本机环境变量。若允许配置库指定
#      要执行的命令，拿到库写权限 = 在每一台 HAProxy 上远程执行任意命令；
#   5. **进入区块的每个值都过白名单校验**（config._validate 里的字符集/
#      范围检查）。这些值被原样渲染进配置文件，放任任意字符等于允许通过
#      配置库往 haproxy.cfg 注入任意指令；本模块渲染前还会再查一遍，
#      两道闸都过不去的值宁可整份不写；
#   6. **默认关闭**。不设 RL_APPLY_HAPROXY_CFG 就完全不写盘、不 reload。

from __future__ import annotations

import asyncio
import logging
import os
import re
import shlex
import tempfile
from dataclasses import dataclass, field

from . import model

BEGIN_MARKER = "# >>> BEGIN rl-limiter managed >>>"
END_MARKER = "# <<< END rl-limiter managed <<<"

_BLOCK_RE = re.compile(
    re.escape(BEGIN_MARKER) + r".*?" + re.escape(END_MARKER),
    re.DOTALL,
)

# 渲染前的最后一道防线。config._validate 已经按同样的规则校验过，这里
# 再查一遍是因为本模块是"把字符串写进配置文件"的那一步：任何绕过配置
# 校验的路径（未来新增的写接口、手工构造的对象）都不能突破这里。
_SAFE_NAME = re.compile(r"^[A-Za-z0-9._-]{1,64}$")
_SAFE_ADDR = re.compile(r"^[A-Za-z0-9._:-]{1,255}$")
_SAFE_MODE = ("tcp", "http")
_SAFE_BALANCE = ("roundrobin", "static-rr", "leastconn", "first", "source", "random")


class EnforceError(Exception):
    """应用配置失败。消息面向运维，说清楚"卡在哪一步、现在是什么状态"。"""


@dataclass(slots=True)
class EnforceResult:
    """一次 reconcile 的结果，用于日志与控制台展示。"""

    changed: bool = False       # 是否真的改了 cfg 并 reload
    error: str = ""             # 非空表示失败（数据面保持原状）
    frontends: list[str] = field(default_factory=list)   # 本次写入的 frontend 名

    @property
    def ok(self) -> bool:
        return not self.error


def _check_renderable(frontends: list[model.FrontendConfig]) -> None:
    """渲染前的白名单复核（见模块头 §安全边界 第 5 条）。"""
    for f in frontends:
        if not _SAFE_NAME.match(f.name):
            raise EnforceError(f"frontend 名 {f.name!r} 含非法字符，拒绝写入配置文件")
        if f.mode not in _SAFE_MODE:
            raise EnforceError(f"frontend {f.name}: mode {f.mode!r} 非法")
        if f.balance not in _SAFE_BALANCE:
            raise EnforceError(f"frontend {f.name}: balance {f.balance!r} 非法")
        if not (1 <= f.bind_port <= 65535):
            raise EnforceError(f"frontend {f.name}: bind_port {f.bind_port} 越界")
        if f.bind_address and not _SAFE_ADDR.match(f.bind_address):
            raise EnforceError(
                f"frontend {f.name}: bind_address {f.bind_address!r} 含非法字符")
        if int(f.quota_bytes_per_sec) < 1:
            raise EnforceError(
                f"frontend {f.name}: 限额 {f.quota_bits_per_sec} bit/s 换算后不足 "
                f"1 byte/s，HAProxy 会拒绝")
        for s in f.servers:
            if not _SAFE_NAME.match(s.name):
                raise EnforceError(
                    f"frontend {f.name}: server 名 {s.name!r} 含非法字符")
            if not _SAFE_ADDR.match(s.address):
                raise EnforceError(
                    f"frontend {f.name}/{s.name}: address {s.address!r} 含非法字符")
            if not (1 <= s.port <= 65535):
                raise EnforceError(
                    f"frontend {f.name}/{s.name}: port {s.port} 越界")


def render_block(frontends: list[model.FrontendConfig]) -> str:
    """把受管 frontend 清单渲染成受管区块文本（含首尾标记）。

    每个 frontend 渲染成一个 `listen` 段：监听端口、模式、超时、
    shared bwlim 限速、后端服务器。用 listen 而不是 frontend+backend
    分写，是因为本模型里两者一一对应，合成一段更短、也让 stats 里的
    pxname 与配置里的段名直接相等（采样按 pxname 匹配）。

    输出是**确定性**的（同样的输入永远得到同样的字节），reconcile 的
    幂等性依赖这一点——否则每轮都会认为"有变化"而反复 reload。
    """
    _check_renderable(frontends)
    lines = [
        BEGIN_MARKER,
        "# 本区块由 rl-limiter 自动生成，请勿手工编辑——改动会在下一次",
        "# reconcile（默认 30 秒内）被原样覆盖。要调整监听端口、限额或",
        "# 后端服务器，请用 rl-limiter 的 Web 控制台（或直接改配置库）。",
        "# 标记之外的内容 rl-limiter 一个字节都不会碰。",
    ]
    for f in frontends:
        limit_bytes = int(f.quota_bytes_per_sec)
        lines.append("")
        lines.append(f"listen {f.name}")
        lines.append(f"    bind {f.bind_spec}")
        lines.append(f"    mode {f.mode}")
        if f.maxconn > 0:
            # 资源保护水位，不是限速手段：限速导致连接堆积时防止耗尽内存/fd。
            lines.append(f"    maxconn {f.maxconn}")
        lines.append(f"    balance {f.balance}")
        lines.append(f"    timeout connect {f.timeout_connect_ms}ms")
        lines.append(f"    timeout client {f.timeout_client_ms}ms")
        lines.append(f"    timeout server {f.timeout_server_ms}ms")
        # 连续统计：不开的话 TCP 长连接的 bytes_out 只在会话结束时跳变，
        # 逐秒差分出来的速率会是"0 与巨大脉冲交替"，监控完全不可用。
        lines.append("    option contstats")
        # shared bwlim 的速率桶存在这张 stick-table 里，key 取 frontend 名
        # ——本段全部连接共用一个桶，于是限的是"该端口的总下行速率"。
        lines.append(
            "    stick-table type string len 64 size 1k expire 1h "
            "store bytes_out_rate(1s)")
        # min-size 1460：小于一个 MSS 的报文不参与整形，避免把小包切碎。
        lines.append(
            f"    filter bwlim-out rl-limit limit {limit_bytes} "
            f"key fe_name min-size 1460")
        lines.append("    tcp-request content set-bandwidth-limit rl-limit")
        for s in f.servers:
            parts = [f"    server {s.name} {s.address}:{s.port}"]
            parts.append(f"weight {s.weight}")
            if s.check:
                parts.append(f"check inter {s.check_inter_ms}ms")
            lines.append(" ".join(parts))
    lines.append(END_MARKER)
    return "\n".join(lines) + "\n"


def splice_block(text: str, block: str) -> str:
    """把受管区块塞回 cfg 文本：有标记就替换，没有就追加到末尾。

    追加而不是报错，是为了让"给一台已有的 HAProxy 接上 rl-limiter"这件事
    不需要先手工改配置——第一次 reconcile 自己把区块建出来。
    """
    if _BLOCK_RE.search(text):
        return _BLOCK_RE.sub(lambda _: block.rstrip("\n"), text, count=1)
    sep = "" if text.endswith("\n") or not text else "\n"
    return f"{text}{sep}\n{block}"


def extract_block(text: str) -> str | None:
    """取出当前 cfg 里的受管区块原文；没有标记时返回 None。"""
    m = _BLOCK_RE.search(text)
    return m.group(0) if m else None


class HAProxyEnforcer:
    """把受管 frontend 配置落到本机 haproxy.cfg 并 reload。

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

    @property
    def reload_cmd(self) -> str:
        return self._reload_cmd

    def current_block(self) -> str | None:
        """读出本机 cfg 当前的受管区块，供控制台展示"数据面实际配置"。"""
        with open(self._cfg_path, "r", encoding="utf-8") as f:
            return extract_block(f.read())

    async def reconcile(self, frontends: list[model.FrontendConfig]) -> EnforceResult:
        """让本机 cfg 的受管区块收敛到 frontends。

        幂等：渲染结果与文件里现有区块逐字节相同就什么都不做（不写盘、
        不 reload），返回 changed=False。因此它既是"配置一变立刻生效"的
        执行者，也是"有人手改了 cfg"时的自动纠偏——配置库是唯一真相源
        这件事，靠周期性调用它来维持。
        """
        if not frontends:
            # 空清单会把区块写成空的，等于删掉全部监听端口。配置校验已经
            # 拒绝了空 frontends，这里是防御性兜底：宁可不动。
            return EnforceResult(
                error="受管 frontend 清单为空，拒绝写入（那会删掉全部监听端口）")
        try:
            block = render_block(frontends)
        except EnforceError as e:
            return EnforceResult(error=str(e))

        try:
            with open(self._cfg_path, "r", encoding="utf-8") as f:
                original = f.read()
        except OSError as e:
            return EnforceResult(error=f"读取 {self._cfg_path} 失败: {e}")

        if extract_block(original) == block.rstrip("\n"):
            return EnforceResult()          # 已一致，无需改动

        new_text = splice_block(original, block)
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
                error=f"{e}（cfg 已回滚，HAProxy 仍按调整前的配置运行）")

        names = [f.name for f in frontends]
        self._log.warning(
            "已把受管配置写入本机 haproxy.cfg 并 reload（数据面已按新配置运行） "
            "cfg=%s frontends=%s reload_cmd=%r",
            self._cfg_path,
            ";".join(
                f"{f.name}:{f.bind_spec}:{int(f.quota_bytes_per_sec)}bytes/s:"
                f"{len(f.servers)}srv" for f in frontends),
            self._reload_cmd,
        )
        return EnforceResult(changed=True, frontends=names)

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
            # 临时文件默认 0600；最终落盘的文件要沿用原文件的权限，
            # 避免把 cfg 改成 0600 后别的运维工具读不了。
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
    """常驻任务：让本机 HAProxy 的受管区块持续收敛到配置。

    两种触发，缺一不可：

      - **配置变更即触发**（config_applied 事件）：这是"在界面上改完立刻
        生效"那条路径——控制台或 SQL 改了配置，轮询把它送进监控循环，
        循环 set 事件，这里立刻 reconcile 并 reload。
      - **周期性兜底**（period_s）：有人手改了 cfg、或上一次 reload 失败
        需要重试时，靠它把状态拉回来。配置库是唯一真相源这件事，是靠
        持续 reconcile 维持的，不是靠"变更时改一次"。

    desired_fn() 返回受管 frontend 列表；失败只记录不抛，让任务活到下一轮
    ——配置下发失败绝不能顺带把监控也带走。
    """
    log.info(
        "配置自动下发已启用（本机 haproxy.cfg 的受管区块将持续跟随配置） "
        "cfg=%s reload_cmd=%r reconcile_period_s=%s",
        enforcer.cfg_path, enforcer.reload_cmd, period_s)
    last_error = ""
    while True:
        try:
            desired = desired_fn()
        except Exception as e:                       # pragma: no cover - 防御
            log.error("计算目标配置失败，本轮跳过 err=%s", e)
            desired = []
        if desired:
            result = await enforcer.reconcile(desired)
            if result.error:
                # 同一个错误反复刷屏没有信息量；变化了才再喊一次。
                if result.error != last_error:
                    log.error(
                        "把配置写入本机 haproxy.cfg 失败，数据面维持原状"
                        "（HAProxy 仍按调整前的配置运行，监控与告警不受影响） "
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
