# rl_limiter.model —— 全服务共享的领域类型（"词汇表"层）。
#
# 架构背景（v0.4 起的单 HAProxy 模型）：一个 rl-limiter 实例管**一台**
# 与它同机的 HAProxy。限速由该 HAProxy 自身的 shared bwlim（聚合限速）
# 执行；rl-limiter 负责两件事：
#   1. 监控——每秒经本机 unix stats socket 采样各受管 frontend 的
#      bytes_out，对照限额做持续超限告警；
#   2. 下发——把配置（监听端口、限额、后端服务器）渲染进 haproxy.cfg 的
#      受管区块并 reload，让改动即时生效。
#
# **监控与限速的单位都是 frontend**：一个 frontend = 一个监听端口 +
# 一个 shared bwlim 速率桶 + 一组后端服务器。这与 HAProxy 的实际机制
# 一一对应——shared bwlim 的速率桶本就按 frontend 建，不存在"跨 frontend
# 的总限额"这种东西。
#
# 历史包袱说明：v0.3 及以前有"节点 / 业务环境（env）"两层分组，用于一个
# 集中服务监控多台 HAProxy。改为同机部署后一个实例只对一台 HAProxy 负责，
# 那两层分组失去意义，已整体移除（EnvQuota/EnvUsage/Target/env_groups
# 及 envs、env_targets 两张表）。对应的旧版本见 tag v0.3.0-colocated。
#
# 单位约定（非常重要，混淆会带来 8 倍误差）：
#   - 内部所有速率一律为「字节每秒」（bytes/s，float）。HAProxy stats 的
#     bytes_out 本身就是字节计数，内部保持字节口径避免反复换算。
#   - 配置中的限额（数据库与本地 YAML 的 quota_bps 字段）一律为
#     「比特每秒」（bits/s），遵循运维习惯：200_000_000 表示 200 Mbps。
#   - 两种口径只在 FrontendConfig.quota_bytes_per_sec 这一处转换
#     （除以 8），其余代码不得再做单位换算。

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(slots=True)
class FrontendStat:
    """从 HAProxy 的 `show stat` 采样得到的单个 frontend 行。

    只保留控制与展示需要的三个字段；stats CSV 的其余列一律忽略，
    避免把 HAProxy 版本间的列差异渗进内部模型。
    """

    name: str       # frontend 名称（pxname 列）
    bytes_out: int  # 下行方向累计字节数（单调递增计数器，速率由相邻两秒差分得出）
    conn_cur: int   # 当前并发连接数（scur 列），用于资源保护水位观测


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

    quota_bits_per_sec 同时是两件事的依据：写进 cfg 的 shared bwlim
    `limit`（真实限速），以及监控侧的超限告警基准。两者同源，因此
    v0.3 那种"库里改了、cfg 忘了改"的配置漂移在本模型下不可能发生。
    """

    name: str                       # frontend 名（= stats 里的 pxname，全局唯一）
    bind_port: int                  # 监听端口
    quota_bits_per_sec: int         # 限额（bit/s），运维口径
    bind_address: str = ""          # 监听地址；空 = 所有地址（HAProxy 的 `bind :port`）
    mode: str = "tcp"               # tcp | http
    maxconn: int = 0                # 0 = 不写该指令，沿用 global/defaults
    balance: str = "roundrobin"     # 后端负载均衡算法
    timeout_connect_ms: int = 5000
    timeout_client_ms: int = 50000
    timeout_server_ms: int = 50000
    servers: list[ServerEntry] = field(default_factory=list)

    @property
    def quota_bytes_per_sec(self) -> float:
        """bits/s → bytes/s 的唯一换算边界（除以 8）。

        写进 haproxy.cfg 的 bwlim `limit` 与监控侧的判定基准都取这个值，
        单位换算全服务只此一处。
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
            "quota_bps": self.quota_bits_per_sec,
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
            quota_bits_per_sec=int(d["quota_bps"]),
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
    """

    name: str
    # 本 tick 的瞬时速率（相邻两秒 bytes_out 差分）。
    rate_bps: float = 0.0
    # 10 秒滑动窗口均值——承诺口径，也是超限告警的判据（毛刺不告警）。
    mean10_bps: float = 0.0
    # 60 秒 EWMA，仅供趋势观测。
    ewma60_bps: float = 0.0
    # 当前并发连接数，用于资源保护水位观测。
    conn_cur: int = 0
    # 采样失联：本 tick 的值是沿用上一秒的陈旧值（fail-static），
    # 超限判定应暂停，控制台标红。
    degraded: bool = False


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
