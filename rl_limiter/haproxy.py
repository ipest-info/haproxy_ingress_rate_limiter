# rl_limiter.haproxy —— HAProxy runtime API（TCP stats socket）异步客户端。
#
# 这是 rl-limiter 服务与数据面（各台 HAProxy 进程）之间唯一的交互通道：
#
#   - 采集侧：collector 每秒通过 show_stat 拉取各 frontend 的 bytes_out 累计值
#     与当前并发连接数，作为计费口径的原始输入（设计文档 §3.1：选用 frontend
#     bytes_out 而非网卡计数，保证口径与"HAProxy 发回客户端的字节数"精确一致，
#     且天然按 frontend 拆分以支持一台 HAProxy 服务多个环境）；
#   - 执行侧：executor 通过 set_map_entry 把快环算出的整形值写入 runtime map，
#     驱动 bwlim-out 过滤器动态调整聚合限速（设计文档 §3.2/§3.3）。
#
# v2.0 变化：runtime API 不再是本机 unix socket，而是 HAProxy 在内网监听的
# TCP stats socket（haproxy.cfg：`stats socket ipv4@<内网IP>:9999 level admin`）。
# 协议本身不变——HAProxy runtime socket 在非交互模式下"一次连接只服务一条
# 命令"，命令执行完即由服务端关闭连接。因此 exec_cmd 每次调用都重新建连，
# 而不是复用长连接——这不是性能疏忽，而是协议要求。

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

# 列举"回包首行以此开头即可断定命令失败"的前缀。runtime socket 的失败回包
# 没有统一格式，只能靠已知前缀识别：命令不存在（"Unknown command"）、socket
# 权限级别不足（"Permission denied"）、以及 "[ALERT]"/"[CFGERR]" 这类方括号
# 包裹的诊断信息。不在此列的回包一律原样返回，由调用方按各自命令的语义
# 解释——例如 "show stat" 的正常回包是 CSV，"set map" 成功时回包为空。
_ERROR_REPLY_PREFIXES = ("Unknown command", "Permission denied", "[")


class RuntimeAPIError(RuntimeError):
    """runtime API 交互失败的基类（命令被拒绝、回包无法解析等）。"""


class CommandError(RuntimeAPIError):
    """命令被 HAProxy 明确拒绝（回包命中已知错误前缀）。

    保留 cmd 与完整回包原文（reply），便于上层记录与 set_map_entry 的
    回退判断——回退逻辑需要检查回包的具体措辞，仅有异常消息不够用。
    """

    def __init__(self, cmd: str, reply: str, detail: str | None = None):
        self.cmd = cmd
        self.reply = reply
        super().__init__(detail or f"haproxy: command {cmd!r} rejected: {_first_line(reply)}")


class StatParseError(RuntimeAPIError):
    """show stat 回包不是可用的 CSV（缺表头/缺必需列/数值非法）。"""


class RuntimeClient:
    """与单台 HAProxy 的 TCP stats socket 通信的客户端。

    自身无状态（不缓存连接），可被多个协程并发使用；timeout_s 约束每条
    命令的端到端耗时（连接 + 写入 + 读取全过程）；timeout_s <= 0 表示关闭
    客户端侧预算，完全交由调用方控制（测试场景常用）。
    """

    def __init__(self, host: str, port: int, timeout_s: float = 0.5,
                 log: logging.Logger | None = None):
        self._host = host
        self._port = port
        self._timeout_s = timeout_s
        # 日志仅用于调试观测（命令、耗时、回退事件），不参与控制逻辑。
        self._log = log if log is not None else logging.getLogger(__name__)

    async def exec_cmd(self, cmd: str) -> str:
        """发送一条命令并返回去除首尾空白后的回包。

        每次调用都新建 TCP 连接：HAProxy 在非交互模式下执行完一条命令就会
        关闭 runtime socket，长连接复用在协议上不可行。读到 EOF 即为"回包
        结束"的信号，无需（也无法）依赖长度前缀或分隔符。回包若命中已知
        错误前缀则抛 CommandError（回包原文在异常属性上，便于上层记录）；
        其余回包原样返回，由调用方按命令语义解析。

        超时用 asyncio.timeout 统一覆盖整个过程——连接、写命令、读回包
        共享同一个截止时刻，asyncio 的取消机制天然能唤醒阻塞中的读写，
        无需哨兵协程。
        """
        start = time.monotonic()
        # timeout <= 0 时传 None：asyncio.timeout(None) 即"无超时"。
        budget = self._timeout_s if self._timeout_s > 0 else None
        async with asyncio.timeout(budget):
            reader, writer = await asyncio.open_connection(self._host, self._port)
            try:
                writer.write((cmd + "\n").encode())
                await writer.drain()
                # 读到 EOF 为止：服务端执行完命令即关闭连接。
                raw = await reader.read(-1)
            finally:
                writer.close()
                # 对端往往已先关闭；wait_closed 的次生错误不应污染主流程。
                try:
                    await writer.wait_closed()
                except (ConnectionError, OSError):
                    pass

        out = raw.decode("utf-8", errors="replace").strip()
        self._log.debug(
            "haproxy runtime command executed cmd=%r duration_ms=%d reply_bytes=%d",
            cmd, int((time.monotonic() - start) * 1000), len(raw))
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

    async def set_map_entry(self, map_path: str, key: str, value: str) -> None:
        """更新 runtime map 中 key 对应的条目。

        HAProxy 对 "set map" 成功时回包为空；若 key 尚不存在（例如 map 文件
        初始为空、或 HAProxy reload 后 map 被重建），"set map" 会返回 "not
        found" 类回包。此时自动回退一次 "add map"，让首次出现的 key 被透明
        创建——调用方（executor）因此无需关心"该 key 是否已存在"，两条路径
        对外语义一致。回退只做一次：若 "add map" 仍失败，说明是 map 路径
        错误等真实故障，直接上抛。
        """
        cmd = f"set map {map_path} {key} {value}"
        err: CommandError | None = None
        try:
            out = await self.exec_cmd(cmd)
        except CommandError as e:
            # 即使回包命中错误前缀，也先检查是否属于"条目不存在"的
            # 措辞变体，再决定是回退还是上抛。
            out, err = e.reply, e

        if _is_missing_entry_reply(out):
            # key 不存在不算错误，是"首次写入"的正常路径；记 info 便于确认
            # map 冷启动/重建后的首次填充时点。
            self._log.info(
                "set map entry missing, falling back to add map map_path=%s key=%s value=%s",
                map_path, key, value)
            add_cmd = f"add map {map_path} {key} {value}"
            out = await self.exec_cmd(add_cmd)  # CommandError 直接上抛（回退只做一次）
            if out:
                # "add map" 成功时同样应回空包；任何非空回包都是未知情况，
                # 宁可报错也不能假装写入成功（限速值未生效属于危险方向）。
                raise RuntimeAPIError(
                    f"haproxy: add map {map_path} {key}: unexpected reply: {_first_line(out)}")
            return

        if err is not None:
            raise err
        if out:
            raise RuntimeAPIError(
                f"haproxy: set map {map_path} {key}: unexpected reply: {_first_line(out)}")


def parse_show_stat(out: str) -> list[model.FrontendStat]:
    """解析 "show stat" 的 CSV 回包。

    为什么按列名而不是列下标解析：HAProxy 的 stat 列集合随版本增删（2.x 各
    小版本都有变化），列的绝对位置完全不可依赖；唯一稳定的契约是表头行
    （以 "# " 开头）中的列名。因此先从表头建立 名字→下标 索引，再取行内
    字段。"bytes_out" 被接受为 HAProxy 原生列名 "bout" 的别名，以兼容测试
    桩及可能的代理层改写。

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
                    f"(pxname/svname/scur/bytes_out|bout): {line}")
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
        stats.append(model.FrontendStat(name=pxname, bytes_out=bytes_out, conn_cur=scur))

    if col_idx is None:
        # 连表头都没有：空回包或完全非预期的输出，按失败处理。
        raise StatParseError("haproxy: empty show stat output")
    return stats


def _is_error_reply(out: str) -> bool:
    """判断回包首行是否命中已知错误前缀。只看首行：错误回包的后续行
    （若有）是补充说明，不影响成败判定。"""
    line = _first_line(out)
    return line.startswith(_ERROR_REPLY_PREFIXES)


def _is_missing_entry_reply(out: str) -> bool:
    """识别 "set map" 的"条目不存在"回包。不同 HAProxy 版本的措辞不完全
    一致（"entry not found" / "unable to find ..."），因此用小写子串匹配
    兜住两种已知变体，而不是精确比对。"""
    lowered = out.lower()
    return "not found" in lowered or "unable to find" in lowered


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


def _parse_int_field(s: str) -> int:
    """解析数值列。空字段视为 0：stat 输出中"该指标不适用"就表现为空串，
    语义上等价于零值。非法内容抛 ValueError，由上层包装为 StatParseError。"""
    if not s:
        return 0
    return int(s)
