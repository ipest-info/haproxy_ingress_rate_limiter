# rl_limiter.model —— 全服务共享的领域类型（"词汇表"层）。
#
# 架构背景（v0.4 起的单 HAProxy 模型）：一个 rl-limiter 实例管**一台**
# 与它同机的 HAProxy。限速由**内核 tc（HTB）**执行（见 rl_limiter.tcshaper）；
# rl-limiter 负责两件事：
#   1. 监控——每秒经本机 unix stats socket 采样各受管 frontend 的
#      bytes_out，对照限额做持续超限告警；
#   2. 下发——把配置（监听端口、限额、后端服务器）渲染进 haproxy.cfg 的
#      受管区块并 reload，让改动即时生效。
#
# **监控与限速的单位都是 frontend**：一个 frontend = 一个监听端口 +
# 一个 tc 速率类 + 一组后端服务器。tc 按**源端口**分类，而源端口就是该
# frontend 的监听端口，因此这个对应关系是天然的，也不存在"跨 frontend
# 的总限额"这种东西。
#
# 历史包袱说明：v0.3 及以前有"节点 / 业务环境（env）"两层分组，用于一个
# 集中服务监控多台 HAProxy。改为同机部署后一个实例只对一台 HAProxy 负责，
# 那两层分组失去意义，已整体移除（EnvQuota/EnvUsage/Target/env_groups
# 及 envs、env_targets 两张表）。对应的旧版本见 tag v0.3.0-colocated。
#
# 单位约定（非常重要，混淆会带来 8 倍误差）：
#   - **配置的限额单位一律是 Mbps**（`quota_mbps`，允许小数）。数据库、
#     本地 YAML、控制台接口三个配置入口用的都是这一个字段、这一个单位，
#     不存在"这里填 bit/s、那里填 Mbps"的分裂。40 就是 40 Mbps。
#   - 内部所有速率一律为「字节每秒」（bytes/s，float）。HAProxy stats 的
#     bytes_out 本身就是字节计数，内部保持字节口径避免反复换算。
#   - 换算只在 FrontendConfig 的两个 property 里发生
#     （quota_bits_per_sec = ×1e6，quota_bytes_per_sec = 再 ÷8），
#     其余代码一律不得再做单位换算。

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(slots=True)
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
    # 全算成空闲，比不算更糟。受管区块渲染出的 bind 不带 alpn，协商不到
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


@dataclass(slots=True)
class InstanceStat:
    """从 HAProxy 的 `show info` 采样得到的进程级指标（原始值）。

    与 FrontendStat 互补：前者是"这台 HAProxy 整体"，后者是"某个监听端口"。
    实例级的带宽/拒绝数由各 frontend 汇总得出（show info 不给这些）。
    """

    curr_conns: int = 0      # CurrConns：当前连接数
    # 以下三项是**进程范围**的累计量，包含 rl-limiter 自己对 runtime API
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


@dataclass(slots=True)
class NicStat:
    """从 /proc/net/dev 采样得到的网卡计数器（原始累计值）。

    **为什么需要它**：HAProxy 是 L4/L7 代理，只统计字节与连接，
    **完全不统计数据包**，更没有"丢包"的概念。监控视图里要求的
    "每秒流入/流出数据包数""每秒丢失入/出包数"只能从网卡取。

    同机部署（rl-limiter 与 HAProxy 在同一台机器）才拿得到这份数据——
    这是同机形态的又一处收益。

    口径提醒：网卡计数是**整机**的，含 HAProxy 之外的全部流量，也无法
    按 frontend 拆分。控制台上它只出现在"实例"视图并标注为网卡口径。
    """

    iface: str = ""
    rx_bytes: int = 0
    rx_packets: int = 0
    rx_dropped: int = 0
    rx_errs: int = 0
    tx_bytes: int = 0
    tx_packets: int = 0
    tx_dropped: int = 0
    tx_errs: int = 0


@dataclass(slots=True)
class ServerEntry:
    """受管 frontend 背后的一台后端服务器（haproxy.cfg 里的一行 server）。

    字段刻意只覆盖"标准化 Web 界面"需要的那几项：地址、端口、权重、
    健康检查。更冷门的 server 参数（ssl、sni、cookie…）不在受管区块的
    表达能力内——需要时应把该 frontend 从受管区块移出、改为手写。
    """

    name: str                    # server 条目名（同一 frontend 内唯一）
    address: str                 # 后端地址（IP 或可解析的主机名）
    port: int                    # 后端端口
    weight: int = 100            # 负载权重；balance 算法按它分配
    check: bool = True           # 是否启用主动健康检查
    check_inter_ms: int = 2000   # 健康检查间隔（毫秒），check 为真时才写入

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name, "address": self.address, "port": self.port,
            "weight": self.weight, "check": self.check,
            "check_inter_ms": self.check_inter_ms,
        }

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "ServerEntry":
        return cls(
            name=str(d["name"]), address=str(d["address"]), port=int(d["port"]),
            weight=int(d.get("weight", 100)),
            check=bool(d.get("check", True)),
            check_inter_ms=int(d.get("check_inter_ms", 2000)),
        )


@dataclass(slots=True)
class FrontendConfig:
    """一个受管 frontend：监听端口 + 限速 + 后端服务器清单。

    对应 haproxy.cfg 受管区块里的一个 `listen` 段（listen 而非
    frontend+backend 分写，是因为本项目里两者一一对应，合成一段能让
    生成的配置更短、也更贴近 stats 里的 pxname）。

quota_mbps 同时是两件事的依据：下发到内核 tc 的类速率（真实限速），
    以及监控侧的超限告警基准。两者同源，因此 v0.3 那种"库里改了、数据面
    忘了改"的配置漂移在本模型下不可能发生。
    """

    name: str                       # frontend 名（= stats 里的 pxname，全局唯一）
    bind_port: int                  # 监听端口
    quota_mbps: float               # 限额（Mbps），**配置的唯一单位**，允许小数
    bind_address: str = ""          # 监听地址；空 = 所有地址（HAProxy 的 `bind :port`）
    mode: str = "tcp"               # tcp | http
    maxconn: int = 0                # 0 = 不写该指令，沿用 global/defaults
    balance: str = "roundrobin"     # 后端负载均衡算法
    timeout_connect_ms: int = 5000
    timeout_client_ms: int = 50000
    timeout_server_ms: int = 50000
    servers: list[ServerEntry] = field(default_factory=list)

    @property
    def quota_bits_per_sec(self) -> int:
        """Mbps → bit/s。tc 的 rate 参数用 bit，这里换过去。

        取整到整数 bit/s：tc 本身也只接受整数，留小数只会让"配置里写的"和
        "实际下发的"对不上。0.0000001 Mbps 这种输入由校验拦掉，不在这里兜。
        """
        return int(round(self.quota_mbps * 1_000_000))

    @property
    def quota_bytes_per_sec(self) -> float:
        """Mbps → bytes/s。全服务的单位换算只在这两个 property 里发生。

        下发给 tc 的类速率（tcshaper 会再 ×8 换回 bit/s）与监控侧的判定
        基准都取这个值。
        """
        return self.quota_bits_per_sec / 8.0

    @property
    def bind_spec(self) -> str:
        """haproxy.cfg 里 `bind` 指令的参数形态。"""
        return f"{self.bind_address}:{self.bind_port}" if self.bind_address \
            else f":{self.bind_port}"

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "bind_address": self.bind_address,
            "bind_port": self.bind_port,
            "quota_mbps": self.quota_mbps,
            "mode": self.mode,
            "maxconn": self.maxconn,
            "balance": self.balance,
            "timeout_connect_ms": self.timeout_connect_ms,
            "timeout_client_ms": self.timeout_client_ms,
            "timeout_server_ms": self.timeout_server_ms,
            "servers": [s.to_dict() for s in self.servers],
        }

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "FrontendConfig":
        return cls(
            name=str(d["name"]),
            bind_port=int(d["bind_port"]),
            quota_mbps=float(d["quota_mbps"]),
            bind_address=str(d.get("bind_address", "") or ""),
            mode=str(d.get("mode", "tcp")),
            maxconn=int(d.get("maxconn", 0) or 0),
            balance=str(d.get("balance", "roundrobin")),
            timeout_connect_ms=int(d.get("timeout_connect_ms", 5000)),
            timeout_client_ms=int(d.get("timeout_client_ms", 50000)),
            timeout_server_ms=int(d.get("timeout_server_ms", 50000)),
            servers=[ServerEntry.from_dict(s) for s in (d.get("servers") or [])],
        )


@dataclass(slots=True)
class FrontendUsage:
    """采集器每个 tick（1s）为单个受管 frontend 产出的用量视图。

    速率字段一律 bytes/s；控制台展示时才换算成 Mbps。

    字段分两组：**限速相关**（rate/mean10/ewma60，限速与超限告警的判据，
    自项目第一版就有）与**监控视图相关**（其余，为 docs/05-监控视图.md 的
    "监听端口视图"补齐）。前者不可随意改动——它们是计费与告警口径。
    """

    name: str
    # 本 tick 的瞬时下行速率（相邻两秒 bytes_out 差分）。
    rate_bps: float = 0.0
    # 10 秒滑动窗口均值——承诺口径，也是超限告警的判据（毛刺不告警）。
    mean10_bps: float = 0.0
    # 60 秒 EWMA，仅供趋势观测。
    ewma60_bps: float = 0.0
    # 当前并发连接数，用于资源保护水位观测。
    conn_cur: int = 0
    # --- 以下为监控视图字段（不参与限速/告警判定）---
    # 上行速率（bytes_in 差分）。限速只作用于下行，上行仅作观测。
    rate_in_bps: float = 0.0
    # 每秒新建连接数（conn_tot 差分）。
    conn_new_ps: float = 0.0
    # 每秒丢失连接数（FrontendStat.denied_total 差分）。
    conn_denied_ps: float = 0.0
    # 活跃 / 非活跃（空闲）连接数，见 FrontendStat 的同名属性。
    active_conns: int = 0
    idle_conns: int = 0
    # --- 以下四项来自内核 tc 的该 frontend 专属队列（见 tcshaper）---
    # 这是限速迁到 tc 之后白捡的能力：HAProxy 完全不统计数据包，网卡计数
    # 又无法按 frontend 拆，而 tc 的每个 class 正好对应一个 frontend。
    # 口径：**出方向、链路层字节**（含 IP/TCP 头），与上面 HAProxy 口径
    # 的 rate_bps 并列展示时会略高，那是正常的。
    pkts_out_ps: float = 0.0     # 每秒流出数据包数
    drop_out_ps: float = 0.0     # 每秒丢弃包数——限速丢的包就在这里
    overlimit_ps: float = 0.0    # 每秒触发限速被延迟的次数（限额吃紧的直接信号）
    backlog_bytes: int = 0       # 当前排队字节数
    # 采样失联：本 tick 的值是沿用上一秒的陈旧值（fail-static），
    # 超限判定应暂停，控制台标红。
    degraded: bool = False


@dataclass(slots=True)
class InstanceUsage:
    """采集器每个 tick 产出的**整台 HAProxy** 的用量视图。

    与 FrontendUsage 的关系：后者是单个监听端口，前者是这台 HAProxy 的
    全貌。实例级的连接数直接来自 `show info`；带宽与拒绝数 `show info`
    不给（TotalBytesOut 只统计出向且含 backend），因此由**全部 frontend
    汇总**得出——包括不在受管清单里的那些，"整个实例"就该是整个实例。

    数据包相关的四个字段来自网卡（/proc/net/dev），是**整机口径**，含
    HAProxy 之外的流量：HAProxy 作为 L4/L7 代理完全不统计数据包，这部分
    只能从网卡取。控制台上须标注清楚，不能与 HAProxy 口径混为一谈。
    """

    # --- 连接视图（show info + frontend 汇总）---
    # 每秒新建连接数取 Σ frontend conn_tot 的差分，**不是** show info 的
    # CumConns——后者含采集器自身对 runtime API 的连接，会有恒定底噪。
    conn_new_ps: float = 0.0
    conn_denied_ps: float = 0.0  # 每秒丢失连接数（Σ frontend denied 差分）
    conn_cur: int = 0            # 并发连接数（CurrConns）
    active_conns: int = 0        # 活跃连接数（Σ frontend）
    idle_conns: int = 0          # 非活跃连接数（Σ frontend）
    max_conn: int = 0            # 进程连接上限，画水位线用
    # --- 带宽视图（HAProxy 口径，Σ frontend 差分，bytes/s）---
    rate_in_bps: float = 0.0
    rate_out_bps: float = 0.0
    # --- 数据包视图（网卡口径，整机范围）---
    nic: str = ""                # 采样的网卡名；空串 = 未取到网卡数据
    pkts_in_ps: float = 0.0
    pkts_out_ps: float = 0.0
    drop_in_ps: float = 0.0      # 每秒丢失入包数（rx_dropped 差分）
    drop_out_ps: float = 0.0     # 每秒丢失出包数（tx_dropped 差分）
    nic_rate_in_bps: float = 0.0   # 网卡口径入向速率，与 HAProxy 口径对照用
    nic_rate_out_bps: float = 0.0
    # --- 进程健康 ---
    idle_pct: int = 100          # HAProxy 自报空闲率，越低越忙
    degraded: bool = False       # 采样失联，本 tick 是陈旧值


@dataclass(slots=True)
class ControllerConfig:
    """投递给监控主循环的运行期配置文档（业务配置的内存形态）。

    version 是配置版本号：数据库模式下取内容校验和，本地 YAML 模式恒为 0。
    控制台展示它，便于核对热更新是否已到位。
    """

    version: int = 0
    frontends: list[FrontendConfig] = field(default_factory=list)

    def quotas(self) -> dict[str, float]:
        """frontend 名 → 限额（bytes/s），供超限判定使用。"""
        return {f.name: f.quota_bytes_per_sec for f in self.frontends}

    def names(self) -> set[str]:
        """全部受管 frontend 名，供采集器过滤 `show stat` 的行。"""
        return {f.name for f in self.frontends}

    def to_dict(self) -> dict[str, Any]:
        return {
            "version": self.version,
            "frontends": [f.to_dict() for f in self.frontends],
        }

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "ControllerConfig":
        return cls(
            version=int(d.get("version", 0)),
            frontends=[FrontendConfig.from_dict(f) for f in (d.get("frontends") or [])],
        )


@dataclass(slots=True)
class NodeConfig:
    """本机 HAProxy 的连接配置（基础设施配置，启动时定型）。

    采样通道二选一，由配置决定（校验强制恰好给一种）：

    - **本机 unix socket（同机部署，推荐）**：rl-limiter 与 HAProxy 装在
      同一台服务器上，haproxy.cfg 写
      `stats socket /run/haproxy/admin.sock mode 660 level user`，配置里
      填 socket_path。stats socket 完全不占网络端口，访问权靠文件属主/
      属组控制——同机形态下这是最小攻击面的接法。
    - **内网 TCP（远程只读观测，兼容保留）**：haproxy.cfg 写
      `stats socket ipv4@<内网IP>:9999 level user`，配置里填 host/port。
      端口必须只绑内网并用安全组/防火墙限制仅监控服务可达。注意这种形态
      下**无法下发配置**（改 cfg + reload 必须在本机做）。

    两种形态下 rl-limiter 对 stats socket 都只做只读采样，`level user`
    即够；配置下发走的是文件 + reload，与 stats socket 无关。
    """

    name: str = "haproxy"    # 本机 HAProxy 的标识名（多机共用配置库时区分用）
    host: str = ""           # 内网地址（TCP 形态）
    port: int = 0            # TCP stats socket 端口（TCP 形态）
    timeout_s: float = 0.5   # 单次 runtime API 命令超时（连接 + 读写）
    socket_path: str = ""    # 本机 unix stats socket 路径（同机形态）

    @property
    def is_unix(self) -> bool:
        """该节点是否走本机 unix socket 采样（同机部署形态）。"""
        return bool(self.socket_path)

    def endpoint(self) -> str:
        """人类可读的采样端点描述，用于日志与控制台展示。"""
        return self.socket_path if self.is_unix else f"{self.host}:{self.port}"
