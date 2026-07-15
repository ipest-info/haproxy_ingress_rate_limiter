# rl_limiter.model —— 全服务共享的领域类型（"词汇表"层）。
#
# v2.0 架构背景（重大调整）：限速服务与 HAProxy 分离部署，一个 rl-limiter
# 服务通过内网 TCP 连接并控制多台 HAProxy。原设计的"每节点 Agent 快环 +
# 中心慢环"合并为一个集中式快环：服务每秒采样所有节点，把同一环境分布在
# 多台 HAProxy 上的流量全局聚合后做 AIMD 决策，再按各挂载点近期用量加权
# 把整形值分配写回各节点（原慢环的加权分配算法降级为执行路径的一步）。
#
# 单位约定（非常重要，混淆会带来 8 倍误差）：
#   - 内部所有速率一律为「字节每秒」（bytes/s，float）。HAProxy stats 的
#     bytes_out 本身就是字节计数，内部保持字节口径避免反复换算。
#   - 配置中的配额（本地 YAML 与管理后台 JSON 的 quota_bps 字段）一律为
#     「比特每秒」（bits/s），遵循运维习惯：200_000_000 表示 200 Mbps。
#   - 两种口径只在 EnvQuota.quota_bytes_per_sec 这一处转换（除以 8），
#     其余代码不得再做单位换算。

from __future__ import annotations

import enum
from dataclasses import dataclass, field
from typing import Any, NamedTuple

# 执行器（executor）的两种运行模式：
#   - dry-run：只计算并记录本应写入的整形值，不真正改动 HAProxy，
#     用于灰度观察与新环境验证（安全默认值）；
#   - enforce：把整形值真实写入各 HAProxy 的 bwlim map，实际生效限速。
# 非法模式一律归一为 dry-run（安全方向）。
MODE_DRY_RUN = "dry-run"
MODE_ENFORCE = "enforce"


class Target(NamedTuple):
    """限速目标：某台 HAProxy 节点上的某个 frontend。

    v2.0 的关键变化——同一个环境的 frontend 可能分布在多台 HAProxy 上，
    因此 (node, frontend) 二元组才是采集与执行的最小单位；纯 frontend
    名字不再全局唯一。
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
    """

    name: str       # frontend 名称（pxname 列）
    bytes_out: int  # 下行方向累计字节数（单调递增计数器，速率由相邻两秒差分得出）
    conn_cur: int   # 当前并发连接数（scur 列），用于资源保护水位观测


class GovState(enum.Enum):
    """决策器对某环境所处的 AIMD 状态（设计文档 §3.3 三段）。"""

    NORMAL = "normal"          # 常态：整形值停在弹性上限（quota × elastic_ceiling）
    TIGHTENING = "tightening"  # 收紧中：mean10 持续超配额，按 md_factor 乘性下压
    RECOVERING = "recovering"  # 恢复中：mean10 持续低于低水位，按 ai_step_frac 加性放松

    def __str__(self) -> str:
        return self.value


@dataclass(slots=True)
class EnvUsage:
    """采集器每个 tick（1s）按环境聚合出的用量视图。

    v2.0：聚合范围是该环境在**所有节点**上的全部 Target——环境总带宽
    直接全局可见，无需再经过慢环上报汇总。
    """

    env_id: str
    # 瞬时速率（bytes/s）：本秒与上一秒计数器的差分之和。噪声最大，
    # 仅作观测参考，不直接驱动限速决策。
    rate_bps: float = 0.0
    # 10 秒滑动窗口均值（bytes/s）。承诺/计费口径（已拍板：10s 均值 ≤
    # 约定带宽，瞬时容忍 110%），是快环收紧/恢复判据的直接输入。
    mean10_bps: float = 0.0
    # 约 60 秒 EWMA（bytes/s），平滑趋势观测与加权分配的兜底输入。
    ewma60_bps: float = 0.0
    # 该环境全部 Target 当前并发连接数之和。
    conn_cur: int = 0
    # 采样已持续失败（节点失联等），速率值沿用最后一次成功采样的结果
    # （设计文档 §3.7 fail-static）。degraded 时 governor 冻结该环境。
    degraded: bool = False
    # 每个 Target 的 60s EWMA 用量（bytes/s），是执行路径按挂载点加权
    # 分配整形值的输入（原慢环算法的输入，v2.0 下沉到这里）。
    target_ewma: dict[Target, float] = field(default_factory=dict)


@dataclass(slots=True)
class Decision:
    """决策器每 tick 针对单个环境产出的执行指令。"""

    env_id: str
    targets: list[Target]  # 该环境的全部挂载点（分配器据此拆分聚合值）
    bwlim_bps: float       # 目标聚合整形值（bytes/s，环境全局口径）
    state: GovState
    # bwlim_bps 相比上次产出是否变化（迟滞 epsilon = 0.1% × quota）。
    # 执行器据此跳过无变化的写入，减少 runtime API 压力。
    changed: bool = False


@dataclass(slots=True)
class GovParams:
    """本地快环控制参数（设计文档 §3.3），可由管理后台下发并按环境覆盖。

    默认值即 §3.3 拍板组合：1.10 / 0.90 / 3s / 5s / ×0.9 / 0.95 / +5%。
    """

    # 弹性上限系数：ceil = quota × elastic_ceiling。常态下允许冲高到
    # 配额的 110%（瞬时容忍口径）。
    elastic_ceiling: float = 1.10
    # 恢复判据低水位：mean10 < quota × low_watermark 持续 recover_after_s
    # 秒后开始放松。
    low_watermark: float = 0.90
    # mean10 > quota 需持续的秒数，达到后触发乘性收紧。
    tighten_after_s: int = 3
    # mean10 低于低水位需持续的秒数，达到后触发加性恢复。急收慢放：
    # 恢复等待窗口比收紧窗口长。
    recover_after_s: int = 5
    # 乘性收紧系数：bwlim = max(quota × tighten_floor, bwlim × md_factor)。
    md_factor: float = 0.9
    # 收紧下限系数：整形值永不低于 quota × tighten_floor，防止过度惩罚。
    tighten_floor: float = 0.95
    # 加性恢复步长：每秒放松 quota × ai_step_frac，直至回到弹性上限。
    ai_step_frac: float = 0.05

    def normalize(self) -> None:
        """把零值/非法字段回填为默认值，使局部覆盖也能得到自洽参数。

        特别地 md_factor 必须落在 (0,1) 开区间才有"乘性收紧"的意义。
        """
        d = GovParams()
        if self.elastic_ceiling <= 0:
            self.elastic_ceiling = d.elastic_ceiling
        if self.low_watermark <= 0:
            self.low_watermark = d.low_watermark
        if self.tighten_after_s <= 0:
            self.tighten_after_s = d.tighten_after_s
        if self.recover_after_s <= 0:
            self.recover_after_s = d.recover_after_s
        if self.md_factor <= 0 or self.md_factor >= 1:
            self.md_factor = d.md_factor
        if self.tighten_floor <= 0:
            self.tighten_floor = d.tighten_floor
        if self.ai_step_frac <= 0:
            self.ai_step_frac = d.ai_step_frac

    @classmethod
    def from_dict(cls, d: dict[str, Any] | None) -> "GovParams | None":
        if d is None:
            return None
        p = cls(**{k: v for k, v in d.items() if k in cls.__dataclass_fields__})
        p.normalize()
        return p

    def to_dict(self) -> dict[str, Any]:
        return {
            "elastic_ceiling": self.elastic_ceiling,
            "low_watermark": self.low_watermark,
            "tighten_after_s": self.tighten_after_s,
            "recover_after_s": self.recover_after_s,
            "md_factor": self.md_factor,
            "tighten_floor": self.tighten_floor,
            "ai_step_frac": self.ai_step_frac,
        }


@dataclass(slots=True)
class EnvQuota:
    """一个环境的配额与挂载点清单，配置分发的最小单元（§3.5 数据模型）。

    v2.0：targets 显式携带节点维度——同一环境可以横跨多台 HAProxy。
    """

    env_id: str
    # 环境配额，单位「比特每秒」（bits/s，运维口径）。全代码库唯一以
    # bits/s 存储的速率字段，进入内部计算前必须经 quota_bytes_per_sec。
    quota_bits_per_sec: int
    targets: list[Target] = field(default_factory=list)
    # 快环参数覆盖；None 表示整体使用默认参数。
    params: GovParams | None = None

    @property
    def quota_bytes_per_sec(self) -> float:
        """bits/s → bytes/s 的唯一换算边界（除以 8）。"""
        return self.quota_bits_per_sec / 8.0

    def effective_params(self) -> GovParams:
        """返回实际生效参数：无覆盖用默认；有覆盖经 normalize 补齐。"""
        if self.params is None:
            return GovParams()
        p = GovParams(**self.params.to_dict())
        p.normalize()
        return p

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "EnvQuota":
        targets = [
            Target(t["node"], t["frontend"]) for t in d.get("targets", [])
        ]
        return cls(
            env_id=d.get("env_id", ""),
            quota_bits_per_sec=int(d.get("quota_bps", 0)),
            targets=targets,
            params=GovParams.from_dict(d.get("params")),
        )

    def to_dict(self) -> dict[str, Any]:
        out: dict[str, Any] = {
            "env_id": self.env_id,
            "quota_bps": self.quota_bits_per_sec,
            "targets": [{"node": t.node, "frontend": t.frontend} for t in self.targets],
        }
        if self.params is not None:
            out["params"] = self.params.to_dict()
        return out


@dataclass(slots=True)
class ControllerConfig:
    """管理后台通过长轮询接口（§3.6）下发的带版本配置文档。

    同时也是 fail-static 本地缓存的持久化格式（§3.7：与后台断联时按
    最后一次下发的配置继续限速，恢复后先拉全量）。version 单调比较：
    服务以本地版本号发起长轮询，后台仅在版本不一致时立即返回新配置。
    注意：HAProxy 节点的连接信息（地址/超时/map 路径）属于基础设施
    配置，只在本地 YAML 维护，不随后台配置下发。
    """

    version: int = 0
    mode: str = MODE_DRY_RUN
    envs: list[EnvQuota] = field(default_factory=list)
    report_interval_s: int = 5    # 用量样本上报间隔（秒）
    heartbeat_interval_s: int = 10  # 心跳间隔（秒）

    def normalize(self) -> None:
        """模式非法归一为 dry-run（安全方向），间隔非正取默认，并逐个
        归一各环境的参数覆盖。所有配置入口（后台下发、本地 YAML、缓存
        加载）都必须先经过这里。"""
        if self.mode != MODE_ENFORCE:
            self.mode = MODE_DRY_RUN
        if self.report_interval_s <= 0:
            self.report_interval_s = 5
        if self.heartbeat_interval_s <= 0:
            self.heartbeat_interval_s = 10
        for e in self.envs:
            if e.params is not None:
                e.params.normalize()

    def target_to_env(self) -> dict[Target, str]:
        """把环境列表展平为 Target → env_id 查找表，供采集器聚合。
        同一 Target 被多个环境声明时后者覆盖前者（配置校验应阻止）。"""
        m: dict[Target, str] = {}
        for e in self.envs:
            for t in e.targets:
                m[t] = e.env_id
        return m

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "ControllerConfig":
        cfg = cls(
            version=int(d.get("version", 0)),
            mode=d.get("mode", MODE_DRY_RUN),
            envs=[EnvQuota.from_dict(e) for e in d.get("envs", [])],
            report_interval_s=int(d.get("report_interval_s", 5)),
            heartbeat_interval_s=int(d.get("heartbeat_interval_s", 10)),
        )
        cfg.normalize()
        return cfg

    def to_dict(self) -> dict[str, Any]:
        return {
            "version": self.version,
            "mode": self.mode,
            "envs": [e.to_dict() for e in self.envs],
            "report_interval_s": self.report_interval_s,
            "heartbeat_interval_s": self.heartbeat_interval_s,
        }


@dataclass(slots=True)
class NodeConfig:
    """一台受控 HAProxy 节点的连接配置（基础设施配置，仅本地 YAML）。

    v2.0：runtime API 不再是本机 unix socket，而是 HAProxy 在内网监听
    的 TCP stats socket（haproxy.cfg：`stats socket ipv4@<内网IP>:9999
    level admin`）。该端口具备 admin 权限，必须只绑内网并用安全组/防火
    墙限制仅限速服务可达。
    """

    name: str                # 节点名（Target.node 引用它）
    host: str                # 内网地址
    port: int                # TCP stats socket 端口
    bwlim_map_path: str = "/etc/haproxy/maps/bwlim.map"  # 该节点上 bwlim map 的路径（map 标识）
    timeout_s: float = 0.5   # 单次 runtime API 命令超时（连接 + 读写）
