# hapagg.collect —— 并发向一批 HAProxy 拉取监控数据。
#
# 协议要点（与真实 runtime socket 一致，不是可以省的细节）：**非交互模式
# 下一次连接只服务一条命令**，服务端应答完即关闭连接。所以每条命令都要
# 重新拨号，不能复用长连接——复用会读到空回包，表现为"这台机器时好时坏"。
#
# 这个模块的立场只有一条：**部分失败必须看得见。**
#
# 监控几十台机器，任何一次采集都可能有几台连不上（网络抖动、机器重启、
# 防火墙改了）。把连上的那几台加起来当作总量，视图会给出一个看起来完全
# 正常、但偏低的数字——**没有任何迹象表明少了几台**。这是这类聚合工具
# 最容易犯也最难被发现的错，所以：
#
#   - 每台的失败原因单独留存，视图必须展示；
#   - 任何"总量"旁边都带着"由几台里的几台合成"；
#   - 一台都没成功时直接报错退出，而不是给一屏漂亮的 0。

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field

from . import stats
from .targets import Target

# 采集用的命令。
#
# `show stat` 不加参数 = 全部 proxy、全部类型、全部 server。rl-limiter 那边
# 用的是 "show stat -1 1 -1"（只要 frontend）因为它只关心限速；这个工具
# 要的是**各种监控维度**，backend 与 server 行正是健康状态、排队、后端错误
# 的唯一来源，不能省。
SHOW_STAT = "show stat"
SHOW_INFO = "show info"

# 单台机器的默认超时。给得比较紧：一台卡住的机器不该拖慢整屏视图，
# 它超时了会如实显示成"这台失败了"，比让人盯着空屏等强。
DEFAULT_TIMEOUT_S = 3.0
# 同时拨号的上限。几百台机器一起拨会把本机的 fd 与对端的 accept 队列
# 打满，反而制造出"很多台连不上"的假象。
DEFAULT_CONCURRENCY = 32


@dataclass
class NodeSnapshot:
    """一台机器的一次采集结果。"""

    target: Target
    ok: bool = False
    error: str = ""
    rows: list[stats.Row] = field(default_factory=list)
    info: dict[str, str] = field(default_factory=dict)
    # 这次采集耗时，用来发现"能连但很慢"的机器。
    elapsed_ms: int = 0
    version: str = ""

    @property
    def label(self) -> str:
        return self.target.label


@dataclass
class Collection:
    """一轮采集的全部结果。"""

    ts: float
    nodes: list[NodeSnapshot] = field(default_factory=list)

    @property
    def ok_nodes(self) -> list[NodeSnapshot]:
        return [n for n in self.nodes if n.ok]

    @property
    def failed_nodes(self) -> list[NodeSnapshot]:
        return [n for n in self.nodes if not n.ok]

    @property
    def complete(self) -> bool:
        """是不是所有目标都采到了。**任何总量都要带上这个判断。**"""
        return bool(self.nodes) and not self.failed_nodes

    def coverage(self) -> str:
        return f"{len(self.ok_nodes)}/{len(self.nodes)} 台"


class CollectError(RuntimeError):
    pass


async def _exec_cmd(host: str, port: int, cmd: str, timeout_s: float) -> str:
    """连一次、发一条命令、读到对端关闭为止。"""
    reader, writer = await asyncio.wait_for(
        asyncio.open_connection(host, port), timeout=timeout_s)
    try:
        writer.write((cmd + "\n").encode())
        await writer.drain()
        data = await asyncio.wait_for(reader.read(-1), timeout=timeout_s)
    finally:
        writer.close()
        try:
            await writer.wait_closed()
        except Exception:
            pass                                  # 关连接失败不影响已读到的数据
    return data.decode("utf-8", errors="replace")


def _looks_like_error(out: str) -> str:
    """runtime socket 的失败回包没有统一格式，只能按已知前缀识别。

    最要紧的是权限不足：stats socket 配成 `level user` 时 `show stat` 能用，
    但有些命令会被拒。这时回包是一行人话而不是 CSV——不识别的话会被
    当成"解析失败"，运维就得去猜到底是网络问题还是权限问题。
    """
    head = out.strip().splitlines()[0] if out.strip() else ""
    for pre in ("Unknown command", "Permission denied", "Can't connect",
                "No such", "[ALERT]", "[CFGERR]"):
        if head.startswith(pre):
            return head
    return ""


async def fetch_one(t: Target, timeout_s: float = DEFAULT_TIMEOUT_S
                    ) -> NodeSnapshot:
    """采一台。**任何异常都转成 ok=False + 原因**，绝不向上抛。

    向上抛的话，一台机器的故障会中断整轮采集——那正好是"部分失败"最不该
    有的后果。
    """
    snap = NodeSnapshot(target=t)
    t0 = time.monotonic()
    try:
        out = await _exec_cmd(t.host, t.port, SHOW_STAT, timeout_s)
        bad = _looks_like_error(out)
        if bad:
            raise CollectError(f"show stat 被拒：{bad}")
        snap.rows = stats.parse_stat_csv(out)

        # show info 是**次要链路**：拿不到只是少几个进程级指标，不该让
        # 整台机器判为失败——那会让一个小问题吃掉这台机器的全部流量数据。
        try:
            iout = await _exec_cmd(t.host, t.port, SHOW_INFO, timeout_s)
            if not _looks_like_error(iout):
                snap.info = stats.parse_info(iout)
                snap.version = snap.info.get("Version", "")
        except Exception:
            pass

        snap.ok = True
    except asyncio.TimeoutError:
        snap.error = f"超时（{timeout_s:g}s）"
    except (OSError, ConnectionError) as e:
        snap.error = f"连不上：{e}"
    except (stats.ParseError, CollectError) as e:
        snap.error = str(e)
    except Exception as e:                        # 兜底：绝不让一台拖垮一轮
        snap.error = f"{type(e).__name__}: {e}"
    snap.elapsed_ms = int((time.monotonic() - t0) * 1000)
    return snap


async def collect(targets: list[Target], timeout_s: float = DEFAULT_TIMEOUT_S,
                  concurrency: int = DEFAULT_CONCURRENCY) -> Collection:
    """并发采一批。返回时**每个目标都有一条结果**，成功失败都在里面。"""
    if not targets:
        raise CollectError("目标清单为空——没有可采集的 HAProxy")
    sem = asyncio.Semaphore(max(1, concurrency))

    async def one(t: Target) -> NodeSnapshot:
        async with sem:
            return await fetch_one(t, timeout_s)

    nodes = await asyncio.gather(*(one(t) for t in targets))
    return Collection(ts=time.time(), nodes=list(nodes))
