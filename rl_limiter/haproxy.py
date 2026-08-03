# rl_limiter.haproxy —— HAProxy runtime API（TCP stats socket）异步客户端。
#
# 这是 rl-limiter 服务与数据面（各台 HAProxy 进程）之间唯一的交互通道，
# 且是**只读**的：collector 每秒通过 show_stat 拉取各 frontend 的
# bytes_out 累计值与当前并发连接数，作为计费口径的原始输入（设计文档
# §3.1：选用 frontend bytes_out 而非网卡计数，保证口径与"HAProxy 发回
# 客户端的字节数"精确一致，且天然按 frontend 拆分）。限速本身由 HAProxy
# 内核 tc 执行（见 rl_limiter.tcshaper），rl-limiter 不写入任何 HAProxy 的
# 运行期状态，因此
# stats socket 用 level user（只读）即够。
#
# 接线有两种形态（见 model.NodeConfig）：同机部署走本机 unix stats
# socket（`stats socket /run/haproxy/admin.sock mode 660 level user`，
# 不占网络端口、按文件权限授权，推荐）；跨机监控走内网 TCP stats socket
# （`stats socket ipv4@<内网IP>:9999 level user`）。两者只有"怎么建连"
# 一步不同，命令语义与回包解析完全一致。
#
# HAProxy runtime socket 在非交互模式下"一次连接只服务一条命令"，命令
# 执行完即由服务端关闭连接。因此 exec_cmd 每次调用都重新建连，而不是
# 复用长连接——这不是性能疏忽，而是协议要求。

from __future__ import annotations

import asyncio
import logging
import time

from . import model

# 采集用的固定命令。三个参数依次为 "<proxy_id> <type> <server_id>"：
# proxy_id = -1 表示全部代理；type = 1 是类型掩码"仅 frontend"（我们只关心
# frontend 的下行字节，backend/server 行既冗余又会显著增大回包体积）；
# server_id = -1 表示全部 server（对 frontend 行无实际筛选作用，按惯例传 -1）。
SHOW_STAT_CMD = "show stat -1 1 -1"

# 进程级指标（并发/累计连接数、连接速率、空闲率…）。回包是 "Key: value"
# 的逐行文本，不是 CSV。它与 show stat 互补：show stat 给不出"整台 HAProxy
# 当前有多少连接"，show stat 的行是按 proxy 拆的。
SHOW_INFO_CMD = "show info"

# 列举"回包首行以此开头即可断定命令失败"的前缀。runtime socket 的失败回包
# 没有统一格式，只能靠已知前缀识别：命令不存在（"Unknown command"）、socket
# 权限级别不足（"Permission denied"）、以及 "[ALERT]"/"[CFGERR]" 这类方括号
# 包裹的诊断信息。不在此列的回包一律原样返回，由调用方按各自命令的语义
# 解释——例如 "show stat" 的正常回包是 CSV。
_ERROR_REPLY_PREFIXES = ("Unknown command", "Permission denied", "[")


class RuntimeAPIError(RuntimeError):
    """runtime API 交互失败的基类（命令被拒绝、回包无法解析等）。"""


class CommandError(RuntimeAPIError):
    """命令被 HAProxy 明确拒绝（回包命中已知错误前缀）。

    保留 cmd 与完整回包原文（reply），便于上层记录与按回包措辞做
    针对性处理。
    """

    def __init__(self, cmd: str, reply: str, detail: str | None = None):
        self.cmd = cmd
        self.reply = reply
        super().__init__(detail or f"haproxy: command {cmd!r} rejected: {_first_line(reply)}")


class StatParseError(RuntimeAPIError):
    """show stat 回包不是可用的 CSV（缺表头/缺必需列/数值非法）。"""


class InfoParseError(RuntimeAPIError):
    """show info 回包不是可用的 "Key: value" 文本（缺必需键）。"""


class RuntimeClient:
    """与单台 HAProxy 的 stats socket 通信的客户端（unix 或 TCP）。

    自身无状态（不缓存连接），可被多个协程并发使用；timeout_s 约束每条
    命令的端到端耗时（连接 + 写入 + 读取全过程）；timeout_s <= 0 表示关闭
    客户端侧预算，完全交由调用方控制（测试场景常用）。

    socket_path 非空时走本机 unix socket（同机部署形态），host/port 被
    忽略；否则走 TCP。两种形态的差异被完全收敛在 _open 一个方法里。
    """

    def __init__(self, host: str = "", port: int = 0, timeout_s: float = 0.5,
                 log: logging.Logger | None = None, socket_path: str = ""):
        self._host = host
        self._port = port
        self._socket_path = socket_path
        self._timeout_s = timeout_s
        # 日志仅用于调试观测（命令、耗时、回退事件），不参与控制逻辑。
        self._log = log if log is not None else logging.getLogger(__name__)

    @classmethod
    def from_node(cls, node: model.NodeConfig,
                  log: logging.Logger | None = None) -> "RuntimeClient":
        """按节点配置构造客户端——把"该用 unix 还是 TCP"的判断收在一处，
        调用方（__main__ 的接线装配）不必重复分支。"""
        return cls(node.host, node.port, node.timeout_s, log,
                   socket_path=node.socket_path)

    def endpoint(self) -> str:
        """人类可读的端点描述，用于日志。"""
        return self._socket_path if self._socket_path else f"{self._host}:{self._port}"

    async def _open(self):
        """建立到 stats socket 的连接，返回 (reader, writer)。"""
        if self._socket_path:
            return await asyncio.open_unix_connection(self._socket_path)
        return await asyncio.open_connection(self._host, self._port)

    async def exec_cmd(self, cmd: str) -> str:
        """发送一条命令并返回去除首尾空白后的回包。

        每次调用都新建连接：HAProxy 在非交互模式下执行完一条命令就会
        关闭 runtime socket，长连接复用在协议上不可行。读到 EOF 即为"回包
        结束"的信号，无需（也无法）依赖长度前缀或分隔符。回包若命中已知
        错误前缀则抛 CommandError（回包原文在异常属性上，便于上层记录）；
        其余回包原样返回，由调用方按命令语义解析。

        超时用 asyncio.wait_for 统一覆盖整个过程——连接、写命令、读回包
        共享同一个截止时刻，asyncio 的取消机制天然能唤醒阻塞中的读写，
        无需哨兵协程。（不用 3.11 才有的 asyncio.timeout：生产存量机器
        还有 Python 3.9。）
        """
        start = time.monotonic()

        async def _exchange() -> bytes:
            reader, writer = await self._open()
            try:
                writer.write((cmd + "\n").encode())
                await writer.drain()
                # 读到 EOF 为止：服务端执行完命令即关闭连接。
                return await reader.read(-1)
            finally:
                writer.close()
                # 对端往往已先关闭；wait_closed 的次生错误不应污染主流程。
                try:
                    await writer.wait_closed()
                except (ConnectionError, OSError):
                    pass

        # timeout <= 0 时传 None：wait_for(None) 即"无超时"。
        budget = self._timeout_s if self._timeout_s > 0 else None
        raw = await asyncio.wait_for(_exchange(), budget)

        out = raw.decode("utf-8", errors="replace").strip()
        self._log.debug(
            "已执行 HAProxy runtime API 命令并完整读取回包 "
            "endpoint=%s cmd=%r duration_ms=%d reply_bytes=%d",
            self.endpoint(), cmd, int((time.monotonic() - start) * 1000), len(raw))
        if _is_error_reply(out):
            raise CommandError(cmd, out)
        return out

    async def show_stat(self) -> list[model.FrontendStat]:
        """执行一次 "show stat" 采样并返回全部 frontend 行。

        列位置按 CSV 表头中的列名解析（原因见 parse_show_stat）；只保留
        svname 为 "FRONTEND" 的汇总行，且剔除 HAProxy 内建的 "stats" 管理
        frontend——那是运维自身的流量，不属于计费口径（设计文档 §3.1）。
        """
        out = await self.exec_cmd(SHOW_STAT_CMD)
        return parse_show_stat(out)

    async def show_info(self) -> model.InstanceStat:
        """执行一次 "show info" 采样并返回进程级指标。

        用途见 model.InstanceUsage：整台 HAProxy 的并发/新建连接数只有
        这里给得出（show stat 的行按 proxy 拆，加总会把同一条连接在多个
        proxy 上重复计）。
        """
        return parse_show_info(await self.exec_cmd(SHOW_INFO_CMD))


# show stat 的**必需列**：缺任何一列都拿不到计费口径，必须整体失败。
# 其余列（监控视图用的那些）一律可选——HAProxy 各版本列集合有增删，
# 少一列只该让对应曲线为空，不该让整次采样连同限速判定一起垮掉。
_REQUIRED_STAT_COLS = ("pxname", "svname", "scur", "bytes_out|bout")


def parse_show_stat(out: str) -> list[model.FrontendStat]:
    """解析 "show stat" 的 CSV 回包。

    为什么按列名而不是列下标解析：HAProxy 的 stat 列集合随版本增删（2.8
    的表头有 205 列，2.x 各小版本都不一样），列的绝对位置完全不可依赖；
    唯一稳定的契约是表头行（以 "# " 开头）中的列名。因此先从表头建立
    名字→下标 索引，再取行内字段。"bytes_out"/"bytes_in" 被接受为 HAProxy
    原生列名 "bout"/"bin" 的别名，以兼容测试桩及可能的代理层改写。

    **必需列与可选列**：pxname/svname/scur/bout 缺失即抛 StatParseError
    （见 _REQUIRED_STAT_COLS）；监控视图用的那些列缺失时按 0 处理，只让
    对应曲线为空——不能因为某个版本少了一列 h1_open_streams 就让限速
    监控整个停摆。

    只保留 svname == "FRONTEND" 的汇总行：type 掩码虽已请求"仅 frontend"，
    但按行再校验一次可以防御掩码语义变化或桩数据混入其他行。内建 "stats"
    frontend 被剔除，理由见 show_stat 的注释。
    """
    stats: list[model.FrontendStat] = []
    col_idx: dict[str, int] | None = None
    i_pxname = i_svname = i_scur = i_bytes = -1

    for line in out.split("\n"):
        line = line.rstrip("\r")
        if not line:
            continue
        if line.startswith("#"):
            # 表头行：重建列索引。理论上回包只有一个表头，但循环内处理
            # 使解析器天然容忍"多段 CSV 拼接"的输入。
            col_idx = {}
            for i, name in enumerate(line.removeprefix("#").strip().split(",")):
                col_idx[name.strip()] = i
            i_pxname = col_idx.get("pxname", -1)
            i_svname = col_idx.get("svname", -1)
            i_scur = col_idx.get("scur", -1)
            i_bytes = col_idx.get("bytes_out", -1)
            if i_bytes < 0:
                i_bytes = col_idx.get("bout", -1)  # HAProxy 原生列名
            if i_pxname < 0 or i_svname < 0 or i_scur < 0 or i_bytes < 0:
                # 缺任何一列都无法给出正确口径，必须整体失败而不是带着残缺
                # 数据继续——collector 的容错逻辑（§3.7）会兜住这次失败。
                raise StatParseError(
                    "haproxy: show stat header missing required columns "
                    f"({'/'.join(_REQUIRED_STAT_COLS)}): {line}")
            continue
        if col_idx is None:
            # 数据行先于表头出现：回包不是合法的 show stat CSV。
            raise StatParseError(
                f"haproxy: show stat output has no CSV header: {_first_line(out)}")

        fields = line.split(",")
        pxname = _field_at(fields, i_pxname)
        svname = _field_at(fields, i_svname)
        if svname != "FRONTEND" or not pxname:
            continue
        if pxname == "stats":  # 内建管理 frontend，不属于计费流量（§3.1）
            continue

        try:
            bytes_out = _parse_int_field(_field_at(fields, i_bytes))
        except ValueError as e:
            raise StatParseError(f"haproxy: frontend {pxname}: bad bytes_out: {e}") from e
        try:
            scur = _parse_int_field(_field_at(fields, i_scur))
        except ValueError as e:
            raise StatParseError(f"haproxy: frontend {pxname}: bad scur: {e}") from e

        def opt(*names: str, _f=fields, _c=col_idx) -> int:
            return _optional_int(_f, _c, *names)

        stats.append(model.FrontendStat(
            name=pxname,
            bytes_out=bytes_out,
            conn_cur=scur,
            bytes_in=opt("bytes_in", "bin"),
            conn_tot=opt("conn_tot"),
            sess_tot=opt("stot"),
            denied_conn=opt("dcon"),
            denied_sess=opt("dses"),
            denied_req=opt("dreq"),
            denied_resp=opt("dresp"),
            err_req=opt("ereq"),
            # 只取 h1：h2/h3 没有 frontend 方向的 open_streams 列，
            # 理由见 model.FrontendStat 的字段注释。
            open_conns=opt("h1_open_connections"),
            open_streams=opt("h1_open_streams"),
            mode=_field_at(fields, col_idx.get("mode", -1)),
        ))

    if col_idx is None:
        # 连表头都没有：空回包或完全非预期的输出，按失败处理。
        raise StatParseError("haproxy: empty show stat output")
    return stats


def parse_show_info(out: str) -> model.InstanceStat:
    """解析 "show info" 的 "Key: value" 逐行文本。

    与 parse_show_stat 同样的取舍：CurrConns 是必需键（缺了就说明这根本
    不是 show info 的回包），其余按 0/缺省处理。键名大小写与 HAProxy 输出
    一致；未知键直接忽略——show info 的键集合同样随版本增删。
    """
    kv: dict[str, str] = {}
    for line in out.split("\n"):
        line = line.strip()
        if not line:
            continue
        k, sep, v = line.partition(":")
        if sep:
            kv[k.strip()] = v.strip()

    if "CurrConns" not in kv:
        raise InfoParseError(
            f"haproxy: show info output missing CurrConns: {_first_line(out)}")

    def num(key: str, default: int = 0) -> int:
        raw = kv.get(key, "")
        try:
            return _parse_int_field(raw) if raw else default
        except ValueError:
            return default

    return model.InstanceStat(
        curr_conns=num("CurrConns"),
        cum_conns=num("CumConns"),
        cum_req=num("CumReq"),
        conn_rate=num("ConnRate"),
        sess_rate=num("SessRate"),
        max_conn=num("Maxconn"),
        run_queue=num("Run_queue"),
        idle_pct=num("Idle_pct", 100),
        uptime_s=num("Uptime_sec"),
    )


def _is_error_reply(out: str) -> bool:
    """判断回包首行是否命中已知错误前缀。只看首行：错误回包的后续行
    （若有）是补充说明，不影响成败判定。"""
    line = _first_line(out)
    return line.startswith(_ERROR_REPLY_PREFIXES)


def _first_line(s: str) -> str:
    """返回 s 的第一行（无换行符时返回原串），用于把多行回包压缩成可读的
    单行错误信息。"""
    i = s.find("\n")
    return s[:i] if i >= 0 else s


def _field_at(fields: list[str], i: int) -> str:
    """取第 i 个字段并去除首尾空白。HAProxy 某些行的尾部字段可能缺省，
    导致行内字段数少于表头列数；这里把越界读取容忍为返回空串，交由数值
    解析函数按"空即为 0"处理，而不是让整次采样崩掉。行尾多余的逗号只会
    产生多余的空字段，按列名索引取值时自然被忽略。"""
    if i < 0 or i >= len(fields):
        return ""
    return fields[i].strip()


def _optional_int(fields: list[str], col_idx: dict[str, int], *names: str) -> int:
    """取一个**可选**数值列：按 names 的先后顺序找第一个存在的列名。

    缺列、空值、非法值一律返回 0。这与必需列的处理刻意相反：监控视图的
    列少一个只该让对应曲线为空，不该让整次采样（连同限速超限判定）失败。
    """
    for n in names:
        i = col_idx.get(n, -1)
        if i < 0:
            continue
        try:
            return _parse_int_field(_field_at(fields, i))
        except ValueError:
            return 0
    return 0


def _parse_int_field(s: str) -> int:
    """解析数值列。空字段视为 0：stat 输出中"该指标不适用"就表现为空串，
    语义上等价于零值。非法内容抛 ValueError，由上层包装为 StatParseError。"""
    if not s:
        return 0
    return int(s)
