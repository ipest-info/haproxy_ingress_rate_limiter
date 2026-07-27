# rl_limiter.model —— 全服务共享的领域类型（"词汇表"层）。
#
# 架构背景：限速由各台 HAProxy 自身的 shared bwlim（聚合限速，配置常量
# + reload 调整）执行；rl-limiter 是与 HAProxy 分离部署的**集中监控**
# 服务——通过内网 TCP 连接多台 HAProxy 的 stats socket，每秒采样各节点
# 受控 frontend 的 bytes_out，按节点聚合出带宽视图，对照配置库中的节点
# 限额（quota）做持续超限告警。**监控单元 = 单台节点**；业务"环境"是
# 节点分组（一个环境可含多台节点、一台节点只服务一个环境），仅用于
# 聚合展示。历史上代码以"环境"为单元，故 EnvQuota/EnvUsage 的 env_id
# 字段如今装的是节点名——结构保持不变以复用采集聚合链路。
#
# 单位约定（非常重要，混淆会带来 8 倍误差）：
#   - 内部所有速率一律为「字节每秒」（bytes/s，float）。HAProxy stats 的
#     bytes_out 本身就是字节计数，内部保持字节口径避免反复换算。
#   - 配置中的限额（数据库与本地 YAML 的 quota_bps 字段）一律为
#     「比特每秒」（bits/s），遵循运维习惯：200_000_000 表示 200 Mbps。
#   - 两种口径只在 EnvQuota.quota_bytes_per_sec 这一处转换（除以 8），
#     其余代码不得再做单位换算。

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, NamedTuple


class Target(NamedTuple):
    """监控目标：某台 HAProxy 节点上的某个 frontend。

    集中式服务同时监控多台 HAProxy，不同节点上的 frontend 可能重名，
    因此 (node, frontend) 二元组才是采集的最小单位。
    """

    node: str      # HAProxy 节点名（与配置 haproxy_nodes[].name 对应）
    frontend: str  # 该节点上的 frontend 名（stats 输出的 pxname 列）

    def __str__(self) -> str:
        return f"{self.node}/{self.frontend}"


@dataclass(slots=True)
class FrontendStat:
    """从某台 HAProxy 的 `show stat` 采样得到的单个 frontend 行。

    统计口径遵循设计文档 §3.1：用 frontend 的 bytes_out（发回客户端的
    应用层字节数）而非网卡计数，与计费口径一致且天然按 frontend 拆分。
    前提是 HAProxy 开启 option contstats（否则长连接的计数成块跳变）。
    """

    name: str       # frontend 名称（pxname 列）
    bytes_out: int  # 下行方向累计字节数（单调递增计数器，速率由相邻两秒差分得出）
    conn_cur: int   # 当前并发连接数（scur 列），用于资源保护水位观测


@dataclass(slots=True)
class EnvUsage:
    """采集器每个 tick（1s）按监控单元（节点）聚合出的用量视图。

    env_id 字段承载节点名（见 EnvQuota），聚合范围是该节点上的全部
    受控 Target——即"该节点当前的下行带宽用量"。
    """

    env_id: str
    # 瞬时速率（bytes/s）：本秒与上一秒计数器的差分之和。噪声最大，
    # 仅作观测参考，不直接驱动告警判定。
    rate_bps: float = 0.0
    # 10 秒滑动窗口均值（bytes/s）。承诺/计费口径（已拍板：10s 均值 ≤
    # 约定带宽），是持续超限告警判据的直接输入。
    mean10_bps: float = 0.0
    # 约 60 秒 EWMA（bytes/s），平滑趋势观测。
    ewma60_bps: float = 0.0
    # 该单元全部 Target 当前并发连接数之和。
    conn_cur: int = 0
    # 采样已持续失败（节点失联等），速率值沿用最后一次成功采样的结果
    # （设计文档 §3.7 fail-static 的监控侧对应）。degraded 时超限判定
    # 暂停（陈旧数据不触发告警翻转）。
    degraded: bool = False


@dataclass(slots=True)
class EnvQuota:
    """一个**监控单元**的限额与挂载点清单。

    监控单元 = 单台 HAProxy 节点：quota 是该节点在配置库中登记的约定
    带宽（应与该节点 haproxy.cfg 里 shared bwlim 的 limit 一致，一致性
    由发布流程保证、由持续超限告警兜底检验）。env_id 字段装的是节点名，
    targets 是该节点上的全部受控 frontend；业务"环境"退化为节点分组，
    只用于聚合展示（见 ControllerConfig.env_groups）。
    """

    env_id: str
    # 节点限额，单位「比特每秒」（bits/s，运维口径）。全代码库唯一以
    # bits/s 存储的速率字段，进入内部计算前必须经 quota_bytes_per_sec。
    quota_bits_per_sec: int
    targets: list[Target] = field(default_factory=list)

    @property
    def quota_bytes_per_sec(self) -> float:
        """bits/s → bytes/s 的唯一换算边界（除以 8）。"""
        return self.quota_bits_per_sec / 8.0

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "EnvQuota":
        targets = [
            Target(t["node"], t["frontend"]) for t in d.get("targets", [])
        ]
        return cls(
            env_id=d.get("env_id", ""),
            quota_bits_per_sec=int(d.get("quota_bps", 0)),
            targets=targets,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "env_id": self.env_id,
            "quota_bps": self.quota_bits_per_sec,
            "targets": [{"node": t.node, "frontend": t.frontend} for t in self.targets],
        }


@dataclass(slots=True)
class ControllerConfig:
    """投递给监控主循环的运行期配置文档（业务配置的内存形态）。

    由配置来源组装：MySQL 数据库轮询（dbconfig.watch，生产）或本地
    YAML 引导（standalone）。version 是内容校验和——轮询任务据此判断
    配置是否变化，变了才投递给主循环热应用。
    注意：HAProxy 节点的连接信息（地址/超时）属于基础设施配置
    （NodeConfig），进程启动时定型，不在本文档内热更。
    """

    version: int = 0
    # 监控单元清单：每个元素对应**一台节点**（env_id=节点名，quota=该
    # 节点登记的约定带宽），见 EnvQuota 说明。
    envs: list[EnvQuota] = field(default_factory=list)
    # 业务环境分组（env_id → 节点名列表）：纯展示信息——环境只提供
    # "聚合查看成员节点带宽之和"的视图。
    env_groups: dict[str, list[str]] = field(default_factory=dict)

    def target_to_env(self) -> dict[Target, str]:
        """把单元列表展平为 Target → 单元名 查找表，供采集器聚合。
        同一 Target 被多个单元声明时后者覆盖前者（配置校验应阻止）。"""
        m: dict[Target, str] = {}
        for e in self.envs:
            for t in e.targets:
                m[t] = e.env_id
        return m

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "ControllerConfig":
        return cls(
            version=int(d.get("version", 0)),
            envs=[EnvQuota.from_dict(e) for e in d.get("envs", [])],
            env_groups={
                str(k): [str(n) for n in v]
                for k, v in (d.get("env_groups") or {}).items()
            },
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "version": self.version,
            "envs": [e.to_dict() for e in self.envs],
            "env_groups": {k: list(v) for k, v in self.env_groups.items()},
        }


@dataclass(slots=True)
class NodeConfig:
    """一台受控 HAProxy 节点的连接配置（基础设施配置，启动时定型）。

    采样通道二选一，由配置决定（校验强制恰好给一种）：

    - **本机 unix socket（同机部署，推荐）**：rl-limiter 与 HAProxy 装在
      同一台服务器上，haproxy.cfg 写
      `stats socket /run/haproxy/admin.sock mode 660 level user`，配置里
      填 socket_path。stats socket 完全不占网络端口，访问权靠文件属主/
      属组控制——同机形态下这是最小攻击面的接法。
    - **内网 TCP（跨机监控，兼容保留）**：haproxy.cfg 写
      `stats socket ipv4@<内网IP>:9999 level user`，配置里填 host/port。
      端口必须只绑内网并用安全组/防火墙限制仅监控服务可达。

    两种形态下 rl-limiter 都只做只读采样，`level user` 即够。
    """

    name: str                # 节点名（Target.node 引用它）
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
