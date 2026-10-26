# rl_limiter.netsrc —— 通用内核数据源（本分支替代 HAProxy 采样）。
#
# 本分支的定位：**不依赖任何具体服务**的服务器级监控与限速。机器上跑的
# 是 nginx、HAProxy、自研 TCP 服务还是别的什么，一概不关心：
#   - 带宽/数据包/丢包：双网卡设备视图（netdev 模块，/proc/net/dev）；
#   - 连接统计：内核 TCP 计数器（本模块，/proc/net/snmp）；
#   - 限速：客户端侧网卡的入向 tc（闭环压出向 + 按端口静态限速），
#     执行面在内核，与上面的服务无关。
#
# 鸭子类型契约（与被替换的 haproxy.RuntimeClient 对齐，collector 原样
# 复用）：
#   - show_stat()  -> list[FrontendStat]：恒返回空表——没有服务配合，
#     不存在"按监听端口拆分"的应用层观测（端口级带宽物理上只有服务
#     自己知道）。端口仍可登记用于**限速执行**（YAML ports 段），只是
#     没有端口级监控曲线；
#   - show_info()  -> InstanceStat：整机 TCP 视图，字段映射见下。
#
# /proc/net/snmp 的 Tcp 两行（一行表头一行数值，字段位置按表头解析）：
#   CurrEstab     当前 ESTABLISHED 连接数   -> curr_conns
#   PassiveOpens  被动打开累计（服务端 accept 口径，≈ 新建入站连接）
#                                           -> cum_conns（差分=每秒新建）
#   ActiveOpens   主动打开累计（本机外连）  -> cum_req（排障对照用）
#   InErrs/RetransSegs 等暂不取——设备视图的丢包已覆盖主要症状。
#
# 口径说明（与 HAProxy 形态的差异，文档同步）：
#   - 连接数是**整机 TCP** 口径：包含 SSH、监控自身、一切进程的连接；
#   - "每秒新建连接数"= PassiveOpens 差分，即整机每秒 accept 数——
#     L4 视角下每连接≈每请求（QPM 口径同源）；
#   - 没有 denied/queue 等服务侧概念，相应字段恒 0。

from __future__ import annotations

import logging

from . import model

PROC_NET_SNMP = "/proc/net/snmp"


class NetSourceError(RuntimeError):
    """内核 TCP 计数器不可读（非 Linux、procfs 未挂载等）。"""


def parse_proc_snmp_tcp(text: str) -> dict[str, int]:
    """解析 /proc/net/snmp 的 Tcp 段：表头行 + 数值行 → {字段: 值}。

    按表头对位，不假设字段顺序/数量（内核版本间会增列）。负值原样保留
    （MaxConn 恒为 -1，语义"无上限"）。"""
    header: list[str] | None = None
    for line in text.split("\n"):
        if not line.startswith("Tcp:"):
            continue
        cols = line.split()[1:]
        if header is None:
            header = cols
            continue
        try:
            return dict(zip(header, (int(c) for c in cols)))
        except ValueError as e:
            raise NetSourceError(f"netsrc: Tcp 数值行解析失败: {e}") from e
    raise NetSourceError("netsrc: /proc/net/snmp 里没有完整的 Tcp 两行"
                         "（表头+数值）")


class SourceDesc:
    """数据源描述（控制台页头/快照的 source 视图用）。

    与 NodeConfig 的最小共同面：name / endpoint() / is_unix。"""

    def __init__(self, name: str = "server"):
        self.name = name

    @property
    def is_unix(self) -> bool:
        return False

    def endpoint(self) -> str:
        return PROC_NET_SNMP


class NetSource:
    """内核 TCP 计数器采样客户端（纯读本地 procfs，μs 级，无超时概念）。

    与 RuntimeClient 同为鸭子类型：方法是 async 的（collector 用
    asyncio.gather 并发调），内部是同步文件读。"""

    def __init__(self, log: logging.Logger | None = None,
                 snmp_path: str = PROC_NET_SNMP):
        self._log = log if log is not None else logging.getLogger(
            "rl_limiter.netsrc")
        self._snmp_path = snmp_path

    async def show_stat(self) -> list:
        """无按服务拆分的观测源：恒空表（见模块头注释）。"""
        return []

    async def show_info(self) -> model.InstanceStat:
        try:
            with open(self._snmp_path, encoding="ascii",
                      errors="replace") as f:
                tcp = parse_proc_snmp_tcp(f.read())
        except OSError as e:
            raise NetSourceError(
                f"netsrc: 读取 {self._snmp_path} 失败: {e}") from e
        return model.InstanceStat(
            curr_conns=int(tcp.get("CurrEstab", 0)),
            cum_conns=int(tcp.get("PassiveOpens", 0)),
            cum_req=int(tcp.get("ActiveOpens", 0)),
            max_conn=0,           # 内核无进程级上限概念；0 = 不画水位线
            idle_pct=100,         # 无服务自报忙闲；恒 100 表示"不适用"
        )
