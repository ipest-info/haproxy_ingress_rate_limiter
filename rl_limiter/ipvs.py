# rl_limiter.ipvs —— LVS（IPVS）形态的数据源：采样客户端 + 受管清单。
#
# 本分支把"本机 HAProxy"换成"本机 LVS director（NAT/FULLNAT 模式）"，
# 其余组件（采集差分、监控循环、tc 执行、闭环、控制台、总控）全部复用
# ——它们只消费 FrontendUsage/FrontendConfig 抽象。本模块收口 IPVS 的
# 一切：数据从哪读、受管清单从哪来、名字怎么合成。
#
# ## 数据源（三个，各取所长）
#
#   /proc/net/ip_vs            服务清单（协议/VIP/端口/调度器）+ 每服务
#                              的 ActiveConn/InActConn（RS 行按服务求和）。
#                              **没有字节数**。
#   ipvsadm -Ln --stats --exact  每服务的 Conns/InBytes/OutBytes 累计值
#                              ——按服务的字节数只有这一条路（或 genl
#                              netlink，纯 stdlib 要手写 TLV 解析，留作
#                              将来优化）。--exact 必须带：不带时大计数
#                              被缩写成 "101M"，解析必错。子进程成本
#                              ≈2~5ms/次，1Hz 采样下可忽略（tcshaper 本
#                              来每拍也在跑 tc 子进程）。
#   /proc/net/ip_vs_stats      全局合计（实例视图）。**数值是十六进制**。
#
# 差分/回绕/fail-static 全部不在这里做：本模块只返回累计值，交给
# collector 现成的 _DiffCounter——`ipvsadm -C` 清表重建导致计数回零时，
# 正好落进 collector "回绕沿用上一秒"的既有分支。
#
# ## 受管清单 = 内核 IPVS 实况表
#
# 对应基线里 haproxy.cfg 的角色，但不解析任何配置文件：清单直接读内核
# （keepalived / 手工 ipvsadm 都兼容），永远与真实转发状态一致。虚拟
# 服务的合成名 `vs_<VIP>_<端口>`（如 vs_10.0.0.1_8080）就是 quotas /
# request_limits 的登记键。
#
# ## 主备（keepalived VRRP）设计要点
#
# 两台 director 都跑 rl-limiter、同一份 YAML：
#   1. keepalived 的 virtual_server 在主备**都**装进内核 IPVS 表（VRRP
#      只漂 VIP），所以备机的受管清单与主机完全一致，tc 规则照常建好；
#   2. 备机上没有 VIP 流量，tc 类匹配不到包——零开销空转；VIP 漂过来
#      那一刻规则已就位，限速零窗口期生效。这是"实况表为权威"的直接
#      收益：不解析 keepalived.conf 就天然主备对称；
#   3. 入向闭环的动态值在备机会因出向≈0 爬回全开，漂移后头几个调节
#      周期内可能短暂超目标、随后收敛；静态出向 HTB 漂移即精确；
#   4. fleet 总控把主备都登记为节点：备机需求≈0 只占保底份额，漂移后
#      需求转移、配额跟着走。
#
# v1 边界：仅 IPv4；同端口多 VIP 时 tc 无法按源端口区分（classid 次要
# 号=端口），只有一个服务参与限速、其余只监控（resolve_port_conflicts，
# 监控本身仍按服务精确）。

from __future__ import annotations

import re
from dataclasses import dataclass

from . import model


class IpvsError(RuntimeError):
    """IPVS 数据源错误的基类。"""


class IpvsUnavailableError(IpvsError):
    """/proc/net/ip_vs 不存在：内核没有加载 ip_vs 模块。"""


class IpvsCommandError(IpvsError):
    """ipvsadm 执行失败（非零退出/超时），信息里带原始 stderr。"""


class IpvsParseError(IpvsError):
    """proc 文件 / ipvsadm 回包的格式不符合预期。"""


@dataclass(**model.SLOTS)
class VirtualService:
    """一个 IPVS 虚拟服务（VIP:端口）的采样快照（原始累计值）。"""

    proto: str                # "tcp" | "udp"
    vip: str                  # 点分十进制
    port: int
    scheduler: str = ""
    conns: int = 0            # 累计连接数（--stats 的 Conns）
    bytes_in: int = 0         # 累计入向字节（InBytes）
    bytes_out: int = 0        # 累计出向字节（OutBytes；NAT 下是真实值）
    active_conn: int = 0      # Σ RS ActiveConn（/proc/net/ip_vs）
    inact_conn: int = 0       # Σ RS InActConn

    @property
    def name(self) -> str:
        """合成名 = quotas/request_limits 的登记键。字符集落在配置名
        校验（_NAME_RE：字母数字._-）内，最长 25 字符。"""
        return f"vs_{self.vip}_{self.port}"


def decode_hex_endpoint(s: str) -> "tuple[str, int]":
    """'0A000001:0050' → ('10.0.0.1', 80)。

    /proc/net/ip_vs 里地址与端口都是十六进制。IPv6（多段冒号/方括号）
    抛 IpvsParseError——v1 只支持 IPv4，调用方跳过该行并 warn-once。
    """
    parts = s.split(":")
    if len(parts) != 2:
        raise IpvsParseError(f"不认识的地址格式（IPv6 暂不支持）：{s!r}")
    addr_hex, port_hex = parts
    if len(addr_hex) != 8:
        raise IpvsParseError(f"不认识的地址格式（IPv6 暂不支持）：{s!r}")
    try:
        addr = int(addr_hex, 16)
        port = int(port_hex, 16)
    except ValueError:
        raise IpvsParseError(f"地址/端口不是十六进制：{s!r}") from None
    vip = ".".join(str((addr >> shift) & 0xFF) for shift in (24, 16, 8, 0))
    return vip, port


# /proc/net/ip_vs 的服务行：  TCP  0A000001:0050 rr [persistent ...]
_PROC_SVC_RE = re.compile(r"^(TCP|UDP)\s+(\S+)\s+(\S+)")
# RS 行：  -> 0A000002:1F90  Masq  1  3  11
_PROC_RS_RE = re.compile(
    r"^\s*->\s+\S+\s+\S+\s+\d+\s+(\d+)\s+(\d+)\s*$")


def parse_proc_ip_vs(text: str) -> "list[VirtualService]":
    """解析 /proc/net/ip_vs：服务清单 + 每服务的连接数。

    服务行（TCP/UDP 开头）建条目；紧随其后的 "-> " RS 行把
    ActiveConn/InActConn 求和进当前服务。FWM（防火墙标记）、SCTP、
    IPv6 服务行跳过——v1 不支持，由调用方按需 warn。整体格式不对
    （比如根本不是这个文件）抛 IpvsParseError。
    """
    if "Prot LocalAddress:Port" not in text and "IP Virtual Server" not in text:
        raise IpvsParseError(
            "/proc/net/ip_vs 内容不符合预期（没有 IPVS 表头）")
    services: list[VirtualService] = []
    cur: "VirtualService | None" = None
    for line in text.splitlines():
        m = _PROC_SVC_RE.match(line)
        if m:
            proto, endpoint, sched = m.groups()
            try:
                vip, port = decode_hex_endpoint(endpoint)
            except IpvsParseError:
                cur = None                 # IPv6 等不支持的服务：跳过
                continue
            cur = VirtualService(proto=proto.lower(), vip=vip, port=port,
                                 scheduler=sched)
            services.append(cur)
            continue
        if line.startswith(("FWM", "SCTP")):
            cur = None                     # 不支持的服务类型：跳过其 RS 行
            continue
        rs = _PROC_RS_RE.match(line)
        if rs and cur is not None:
            cur.active_conn += int(rs.group(1))
            cur.inact_conn += int(rs.group(2))
    return services


def parse_ipvsadm_stats(text: str) -> "dict[tuple[str, str, int], tuple[int, int, int]]":
    """解析 `ipvsadm -Ln --stats --exact`。

    返回 (proto, vip, port) → (Conns, InBytes, OutBytes)。只取服务行
    （TCP/UDP 开头，服务行的统计就是该服务合计，无需加 RS 行）；
    "->" 开头的 RS 行跳过。表头缺失或服务行列数不足抛 IpvsParseError
    ——字节数是限速判定的主链路，解析必须整体成功或整体失败，不能
    悄悄漏服务。
    """
    if "Conns" not in text or "OutBytes" not in text:
        raise IpvsParseError(
            "ipvsadm --stats 回包缺少统计表头（确认 ipvsadm 版本支持 "
            "-Ln --stats --exact）")
    out: dict = {}
    for line in text.splitlines():
        if not line.startswith(("TCP", "UDP")):
            continue
        parts = line.split()
        # Prot Addr:Port Conns InPkts OutPkts InBytes OutBytes
        if len(parts) < 7:
            raise IpvsParseError(
                f"ipvsadm --stats 服务行列数不足（期望 7 列）：{line!r}")
        proto = parts[0].lower()
        addr = parts[1].rsplit(":", 1)
        if len(addr) != 2:
            continue                       # IPv6/异常地址：跳过
        try:
            vip, port = addr[0], int(addr[1])
            conns, in_bytes, out_bytes = (int(parts[2]), int(parts[5]),
                                          int(parts[6]))
        except ValueError:
            raise IpvsParseError(
                f"ipvsadm --stats 服务行数值非法（是否漏了 --exact？"
                f"大计数会被缩写成 101M 之类）：{line!r}") from None
        out[(proto, vip, port)] = (conns, in_bytes, out_bytes)
    return out


def parse_proc_ip_vs_stats(text: str) -> model.InstanceStat:
    """解析 /proc/net/ip_vs_stats（全局合计，**数值是十六进制**）。

    取第一行数据（Total 累计行）：Conns → InstanceStat.cum_conns。
    字节/包的全局合计不进 InstanceStat——实例级带宽由 collector 按
    各服务汇总（与 HAProxy 形态同一条链路）。IPVS 没有 maxconn /
    空闲率概念：max_conn=0（页面不画水位线）、idle_pct=100。
    """
    for line in text.splitlines():
        parts = line.split()
        if len(parts) == 5 and all(
                re.fullmatch(r"[0-9A-Fa-f]+", p) for p in parts):
            try:
                conns = int(parts[0], 16)
            except ValueError:
                break
            return model.InstanceStat(cum_conns=conns, idle_pct=100)
    raise IpvsParseError(
        "/proc/net/ip_vs_stats 内容不符合预期（没有找到合计数据行）")
