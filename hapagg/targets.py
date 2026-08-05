# hapagg.targets —— 批量导入被监控的 HAProxy 列表。
#
# 目标的来源就是一行行的 `IP:port`，对应各台机器 haproxy.cfg 里的
#
#     stats socket ipv4@*:9999 level admin expose-fd listeners
#
# 支持的写法（同一份清单里可以混用）：
#
#     10.0.0.11:9999                # 最常见：地址:端口
#     10.0.0.12                     # 省略端口 → 用默认端口
#     hap-cn-1 = 10.0.0.13:9999     # 起个名字，视图里按名字显示
#     [2001:db8::5]:9999            # IPv6 必须加方括号（否则冒号没法区分）
#     10.0.0.20-24:9999             # 末段区间，展开成 5 台
#     # 这一行是注释
#
# 为什么要支持区间与命名：这个工具的输入是"一批机器"，几十台是常态。
# 让人一行行敲 IP 既慢又容易漏，而漏掉一台的后果是**聚合视图里少了一台
# 的流量，但视图本身看起来完全正常**——这类静默偏差比报错难查得多。
#
# 解析失败一律**报错到具体行**，绝不跳过：清单里有一行写错就说明运维的
# 意图没被完整表达，静默忽略等于让聚合结果少一台。

from __future__ import annotations

import ipaddress
import re
from dataclasses import dataclass

DEFAULT_PORT = 9999


class TargetError(ValueError):
    """清单里有写错的行。带行号与原文，直接可定位。"""


@dataclass(frozen=True)
class Target:
    """一台被监控的 HAProxy。"""

    host: str
    port: int = DEFAULT_PORT
    # 显示名。没显式给就用 host:port，视图里当标识用。
    name: str = ""

    @property
    def label(self) -> str:
        return self.name or self.endpoint

    @property
    def endpoint(self) -> str:
        # IPv6 的字面量在展示时也加方括号，免得和"host:port"的冒号混淆。
        if ":" in self.host:
            return f"[{self.host}]:{self.port}"
        return f"{self.host}:{self.port}"

    def __str__(self) -> str:                     # 日志里直接可读
        return self.label


# "10.0.0.20-24" 这种末段区间。只支持 IPv4 末段：IPv6 的区间在运维现场
# 几乎不会手写，支持它只会让解析规则变复杂、错误信息变难懂。
_RANGE_RE = re.compile(r"^(\d{1,3}\.\d{1,3}\.\d{1,3}\.)(\d{1,3})-(\d{1,3})$")


def _split_host_port(token: str) -> tuple[str, int | None]:
    """把 "host[:port]" 拆开。IPv6 必须写成 "[addr]:port" 或裸 "addr"。"""
    token = token.strip()
    if token.startswith("["):
        host, sep, rest = token[1:].partition("]")
        if not sep:
            raise TargetError(f"IPv6 地址缺右方括号：{token!r}")
        if not rest:
            return host, None
        if not rest.startswith(":"):
            raise TargetError(f"IPv6 地址后面只能跟 :端口：{token!r}")
        return host, _port(rest[1:], token)
    # 裸 IPv6（不带端口）会有多个冒号；带端口的写法必须加方括号，
    # 否则 "2001:db8::5:9999" 根本无法区分最后一段是端口还是地址。
    if token.count(":") > 1:
        return token, None
    host, sep, port = token.partition(":")
    if not sep:
        return host, None
    return host, _port(port, token)


def _port(s: str, whole: str) -> int:
    try:
        p = int(s)
    except ValueError:
        raise TargetError(f"端口不是数字：{whole!r}") from None
    if not (1 <= p <= 65535):
        raise TargetError(f"端口 {p} 越界（1-65535）：{whole!r}")
    return p


def _expand_hosts(host: str) -> list[str]:
    """展开末段区间；不是区间就原样返回一个。"""
    m = _RANGE_RE.match(host)
    if not m:
        return [host]
    prefix, lo, hi = m.group(1), int(m.group(2)), int(m.group(3))
    if lo > hi:
        raise TargetError(f"区间起点大于终点：{host!r}")
    if hi > 255:
        raise TargetError(f"区间末段越界：{host!r}")
    return [f"{prefix}{i}" for i in range(lo, hi + 1)]


def _check_host(host: str) -> None:
    """地址必须是能连的东西。

    IP 字面量严格校验；主机名只做字符集检查——DNS 名字的合法性交给解析器，
    这里拦的是明显的手误（空格、斜杠这种一看就不对的）。
    """
    if not host:
        raise TargetError("地址为空")
    try:
        ipaddress.ip_address(host)
        return
    except ValueError:
        pass
    if not re.fullmatch(r"[A-Za-z0-9]([A-Za-z0-9._-]*[A-Za-z0-9])?", host):
        raise TargetError(f"地址不合法：{host!r}")


def parse_line(line: str, default_port: int = DEFAULT_PORT) -> list[Target]:
    """解析一行，返回 0 个（空行/注释）或多个（区间）目标。"""
    # 行内注释：`10.0.0.11:9999  # 北京一区` 是很自然的写法。
    line = line.split("#", 1)[0].strip()
    if not line:
        return []

    name = ""
    if "=" in line:
        name, _, line = line.partition("=")
        name, line = name.strip(), line.strip()
        if not name:
            raise TargetError("`=` 左边的名字为空")
        if not line:
            raise TargetError(f"名字 {name!r} 后面没有地址")

    host, port = _split_host_port(line)
    hosts = _expand_hosts(host)
    for h in hosts:
        _check_host(h)
    port = default_port if port is None else port

    if len(hosts) > 1 and name:
        # 区间 + 名字：名字后面补序号，否则几台机器会同名，视图里根本
        # 分不出谁是谁。
        return [Target(host=h, port=port, name=f"{name}-{i}")
                for i, h in enumerate(hosts, 1)]
    return [Target(host=h, port=port, name=name) for h in hosts]


def parse_targets(text: str, default_port: int = DEFAULT_PORT) -> list[Target]:
    """解析整份清单。

    去重按 (host, port)：同一台机器写了两遍会让它的流量在聚合里被**算两
    次**，而视图看起来完全正常。这类静默翻倍比报错难查得多，所以宁可去重
    也不报错——重复登记是无害的手误，不是意图表达错误。
    """
    out: list[Target] = []
    seen: set[tuple[str, int]] = set()
    for lineno, raw in enumerate(text.splitlines(), 1):
        try:
            for t in parse_line(raw, default_port):
                key = (t.host, t.port)
                if key in seen:
                    continue
                seen.add(key)
                out.append(t)
        except TargetError as e:
            raise TargetError(f"第 {lineno} 行：{e}（原文：{raw.strip()!r}）") from None
    return out


def load_targets(paths: list[str], inline: list[str],
                 default_port: int = DEFAULT_PORT) -> list[Target]:
    """从文件与命令行汇总目标清单，两边同一套解析规则。"""
    chunks: list[str] = []
    for p in paths:
        try:
            with open(p, encoding="utf-8") as f:
                chunks.append(f.read())
        except OSError as e:
            raise TargetError(f"读不了目标清单 {p}：{e}") from None
    # 命令行上允许 `-t a:1,b:2` 这种逗号分隔，写起来顺手。
    chunks.extend(x.replace(",", "\n") for x in inline)
    return parse_targets("\n".join(chunks), default_port)
