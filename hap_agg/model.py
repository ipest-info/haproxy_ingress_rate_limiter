# hap_agg.model —— 共享领域类型（HAProxy 采样的原始统计行）。
#
# 只有两个类型：FrontendStat（show stat 的 FRONTEND 行）与 InstanceStat
# （show info 的进程级指标）。字段取舍原则：只收「HAProxy 真的统计了、
# 监控视图真的用到」的列——stats CSV 有 200+ 列，绝大多数与聚合监控
# 无关。

from __future__ import annotations

import sys
from dataclasses import dataclass

# Python 3.9 兼容：dataclass 的 slots 参数 3.10 才有。
SLOTS: dict = {"slots": True} if sys.version_info >= (3, 10) else {}


@dataclass(**SLOTS)
class FrontendStat:
    """从 HAProxy 的 `show stat` 采样得到的单个 frontend 行（原始累计值）。

    这里只收「HAProxy 真的统计了」的列——stats CSV 有 200+ 列，绝大多数
    是 QUIC/H3 的细分错误码，与本项目的监控视图无关。字段的取舍依据见
    docs/05-监控视图.md 的可行性分析。
    """

    name: str            # frontend 名称（pxname）
    bytes_out: int       # 下行累计字节（bout）；速率由相邻两秒差分得出
    conn_cur: int        # 当前并发连接数（scur）
    bytes_in: int = 0    # 上行累计字节（bin）
    # 累计连接/会话数。conn_tot 是 TCP 连接，stot 是会话——HTTP keep-alive
    # 下一条连接可承载多个会话，两者不等价。
    conn_tot: int = 0
    sess_tot: int = 0
    # 被拒绝的连接/会话/请求/响应。实测：`tcp-request connection reject`
    # 计入 denied_conn，`tcp-request content reject` 计入 denied_req。
    # 这几项之和是本项目对"丢失连接数"的口径（HAProxy 没有"丢包"概念）。
    denied_conn: int = 0
    denied_sess: int = 0
    denied_req: int = 0
    denied_resp: int = 0
    err_req: int = 0     # 请求错误数（ereq）
    # HTTP/1 的连接/流计数（h1_open_connections / h1_open_streams）。实测
    # 这两列是**按 frontend** 统计的，可据此拆出活跃/空闲：有在途流的连接
    # 算活跃，建着但没有流的（keep-alive 空等）算空闲。
    #
    # 为什么只取 h1：stats CSV 里 h2 只有 h2_open_connections 与
    # h2_backend_open_streams——**前端方向的 open_streams 根本没有这一列**，
    # h3 连 open_connections 都没有。拿 h2 的连接数配 h1 的流数会把 h2 连接
    # 全算成空闲，比不算更糟。只要 cfg 里的 bind 不带 alpn，协商不到
    # h2/h3，因此本项目自己生成的 frontend 全部落在 h1 口径内。
    #
    # 注意 mode tcp 下没有"流"的概念，这两个值都是 0——TCP 模式下每条
    # 连接就是一条数据通道，全部按活跃计。
    open_conns: int = 0
    open_streams: int = 0
    mode: str = ""       # tcp | http（决定活跃/空闲怎么算）

    @property
    def denied_total(self) -> int:
        """本项目对"丢失连接数"的口径：连接级 + 会话级 + 请求级的拒绝之和。

        HAProxy 没有"丢包"概念，能称得上"丢失"的只有它自己主动拒掉的那些。
        denied_resp 是响应方向的拒绝（后端已经应答过了），不算连接丢失，
        故不计入。
        """
        return self.denied_conn + self.denied_sess + self.denied_req

    @property
    def active_conns(self) -> int:
        """活跃连接数：有在途请求/流的连接。

        mode tcp 下没有流的概念，每条连接都在传数据，全部计为活跃。
        """
        if self.mode == "http" and self.open_conns:
            return min(self.open_streams, self.open_conns)
        return self.conn_cur

    @property
    def idle_conns(self) -> int:
        """空闲连接数：建立着但当前没有在途流（HTTP keep-alive 等待中）。"""
        if self.mode == "http" and self.open_conns:
            return max(0, self.open_conns - self.open_streams)
        return 0


@dataclass(**SLOTS)
class InstanceStat:
    """从 HAProxy 的 `show info` 采样得到的进程级指标（原始值）。

    与 FrontendStat 互补：前者是"这台 HAProxy 整体"，后者是"某个监听端口"。
    实例级的带宽/拒绝数由各 frontend 汇总得出（show info 不给这些）。
    """

    curr_conns: int = 0      # CurrConns：当前连接数
    # 以下三项是**进程范围**的累计量，包含 hap-agg 自己对 runtime API
    # 的连接（每秒两条）。因此实例视图的"每秒新建连接数"不用它们，改用
    # Σ frontend conn_tot（见 collector._tick_instance 的注释与实测数据）。
    # 保留解析是因为它们对排障有用（比如核对采集器自身的开销）。
    cum_conns: int = 0       # CumConns：累计连接数
    cum_req: int = 0         # CumReq：累计请求数
    conn_rate: int = 0       # ConnRate：HAProxy 自己算的每秒新建连接数
    sess_rate: int = 0       # SessRate：每秒新建会话数
    max_conn: int = 0        # Maxconn：进程连接上限（画水位线用）
    run_queue: int = 0       # Run_queue：任务队列长度
    idle_pct: int = 100      # Idle_pct：HAProxy 自报的空闲率，越低越忙
    uptime_s: int = 0
