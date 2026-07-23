# rl_limiter.config —— 服务配置的解析与校验（服务的"启动契约"层）。
#
# 服务配置提供三类信息：
#   1. HAProxy 节点（haproxy_nodes）：接线字段（内网 TCP stats socket
#      地址、超时）+ quota_bps（该节点登记的约定带宽——超限告警基准，
#      应与该节点 haproxy.cfg 里 shared bwlim 的 limit 一致）；
#   2. 环境分组（envs）：业务环境 = 节点分组 + 挂载点归属（节点 ×
#      frontend），不携带限额，仅供控制台聚合查看；
#   3. 服务级运行参数：log_level、tick_interval_s。
#
# 配置来源有两种，共用同一套解析/校验管线（from_raw）：
#   - MySQL 数据库（dbconfig 模块，生产权威）：数据库各表的行被组装成
#     与 YAML 解析结果同构的原始 dict 后走 from_raw——校验规则只写一遍，
#     两种来源的错误信息与拒绝行为完全一致；
#   - 本地 YAML 文件（load）：standalone / 开发联调。
#
# 单位约定：限额一律按运维口径的「比特每秒」（quota_bps，200000000 =
# 200 Mbps）书写，内部统一换算为 bytes/s（见 model.EnvQuota）。
#
# 加载流程：读文件 → yaml.safe_load → 补默认值 → 校验。load 本身不打日志，
# 成功日志由 main 统一输出；失败通过异常信息精确指出问题字段、当前值与
# 拒绝原因（中文），让运维不需要翻代码就能定位配置错误。

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import yaml

from . import model

# 各键缺失时补上的默认值。与部署示例配置保持一致。
DEFAULT_LOG_LEVEL = "info"
# 快环 tick 周期（秒）：每秒采样/决策/执行一轮。
DEFAULT_TICK_INTERVAL_S = 1.0
# 单次 HAProxy runtime API 调用默认超时（毫秒）。500ms 远大于内网 TCP 的
# 正常往返，又不至于拖住每秒一次的采样循环。
DEFAULT_TIMEOUT_MS = 500

# 合法日志级别枚举。拼错的级别若静默回落会让运维误以为已调级，必须显式拒绝。
_LOG_LEVELS = ("debug", "info", "warn", "error")


@dataclass(slots=True)
class ServiceConfig:
    """完整的 rl-limiter 服务配置（from_raw / load 的返回类型）。"""

    log_level: str = DEFAULT_LOG_LEVEL
    # 采样 tick 周期（秒）。
    tick_interval_s: float = DEFAULT_TICK_INTERVAL_S
    # 受控 HAProxy 节点清单（基础设施配置，启动时定型）。
    nodes: list[model.NodeConfig] = field(default_factory=list)
    # 节点登记限额（超限告警基准，可热更）。限速执行在各节点 HAProxy 的
    # shared bwlim 配置里，这里的值应与之一致（由发布流程保证）。
    node_quotas: dict[str, int] = field(default_factory=dict)
    # 业务环境分组（env_id → 节点名列表）：环境没有限额/调节语义，
    # 只提供"聚合查看成员节点带宽之和"的视图。
    env_groups: dict[str, list[str]] = field(default_factory=dict)
    # 监控单元清单（解析末尾由节点限额+挂载点**组装**而来）：每台有
    # 挂载点的节点一个单元，env_id 字段=节点名，quota=节点限额。
    envs: list[model.EnvQuota] = field(default_factory=list)


def load(path: str) -> ServiceConfig:
    """读取 path 指向的 YAML 配置：解析、补默认值、校验，全部通过后返回。

    任何解析/校验失败都抛 ValueError，信息中带上文件路径与具体原因；
    文件不存在等 I/O 错误按原生 OSError 抛出（调用方能区分"配置写错"
    与"文件缺失"两类问题）。
    """
    with open(path, "r", encoding="utf-8") as f:
        text = f.read()
    try:
        raw = yaml.safe_load(text)
    except yaml.YAMLError as e:
        raise ValueError(f"配置文件 {path} 解析失败（不是合法的 YAML）: {e}") from e
    return from_raw(raw, source=f"配置文件 {path}")


def from_raw(raw: Any, source: str) -> ServiceConfig:
    """把已就位的原始 dict（YAML 解析结果，或数据库行组装出的同构 dict）
    转为 ServiceConfig：补默认值、逐条校验，全部通过后返回。

    source 是人类可读的配置来源描述（如 "配置文件 /etc/rl-limiter/config.yaml"
    或 "MySQL 数据库 mysql:3306/rl_limiter"），拼进所有错误信息，让多来源
    部署时能一眼定位出错的是哪份配置。
    """
    if raw is None:
        raw = {}
    if not isinstance(raw, dict):
        raise ValueError(
            f"{source} 无效: 顶层必须是键值映射（mapping），"
            f"当前为 {type(raw).__name__}"
        )
    try:
        cfg = _parse(raw)
        _validate(cfg)
        _assemble_units(cfg)
    except ValueError as e:
        raise ValueError(f"{source} 无效: {e}") from None
    return cfg


def _assemble_units(cfg: ServiceConfig) -> None:
    """校验通过后，把"环境分组"重组为 per-node 监控单元。

    把 cfg.envs 从"环境 → 挂载点"重排为"节点 → 挂载点"（每台有挂载点
    的节点一个 EnvQuota，env_id=节点名，quota=节点限额），同时生成
    env_groups（环境 → 成员节点）供聚合展示。节点独占校验保证一个节点
    的全部挂载点来自同一环境，重排无二义。
    """
    groups: dict[str, list[str]] = {}
    node_targets: dict[str, list[model.Target]] = {}
    for g in cfg.envs:
        members: list[str] = []
        for t in g.targets:
            node_targets.setdefault(t.node, []).append(t)
            if t.node not in members:
                members.append(t.node)
        groups[g.env_id] = members
    cfg.env_groups = groups
    cfg.envs = [
        model.EnvQuota(
            env_id=node,
            quota_bits_per_sec=cfg.node_quotas[node],
            targets=targets,
        )
        for node, targets in sorted(node_targets.items())
    ]


def _parse(raw: dict[str, Any]) -> ServiceConfig:
    """把 yaml.safe_load 的原始 dict 组装为 ServiceConfig 并填默认值。

    只做结构转换与兜底，不做业务校验（校验集中在 _validate，便于逐条
    给出带上下文的错误信息）。
    """
    # 旧形态（服务/节点级 mode、节点级 AIMD params）明确拒绝并给出迁移
    # 指引：限速已改由 HAProxy shared bwlim 配置执行，这些键不再有任何
    # 效果——静默忽略会让运维误以为还能热切模式/调参。
    if "mode" in raw:
        raise ValueError(
            "字段 'mode' 已废弃——限速由各节点 HAProxy 的 shared bwlim "
            "配置执行，rl-limiter 只做监控与超限告警，没有 dry-run/enforce "
            "模式"
        )
    cfg = ServiceConfig(
        log_level=str(raw.get("log_level", "") or "") or DEFAULT_LOG_LEVEL,
    )

    # tick_interval_s：缺失取默认 1.0；显式给出的值原样保留，交给校验
    # 判定（显式写 0 或负数属于配置错误，不能静默回落默认值）。
    tick = raw.get("tick_interval_s", DEFAULT_TICK_INTERVAL_S)
    try:
        cfg.tick_interval_s = float(tick)
    except (TypeError, ValueError):
        raise ValueError(
            f"tick_interval_s 必须是数字（秒），当前值 {tick!r}"
        ) from None

    # ---- haproxy_nodes：受控节点接线 ----
    nodes_raw = raw.get("haproxy_nodes") or []
    if not isinstance(nodes_raw, list):
        raise ValueError(
            f"haproxy_nodes 必须是节点列表，当前为 {type(nodes_raw).__name__}"
        )
    for i, n in enumerate(nodes_raw):
        if not isinstance(n, dict):
            raise ValueError(
                f"haproxy_nodes[{i}]: 每个节点必须是键值映射，"
                f"当前为 {type(n).__name__}"
            )
        port_raw = n.get("port", 0)
        try:
            port = int(port_raw)
        except (TypeError, ValueError):
            raise ValueError(
                f"haproxy_nodes[{i}]: port 必须是整数，当前值 {port_raw!r}"
            ) from None
        timeout_ms_raw = n.get("timeout_ms", 0)
        try:
            timeout_ms = int(timeout_ms_raw)
        except (TypeError, ValueError):
            raise ValueError(
                f"haproxy_nodes[{i}]: timeout_ms 必须是整数（毫秒），"
                f"当前值 {timeout_ms_raw!r}"
            ) from None
        # timeout_ms（运维口径，毫秒）在这里一次性换算为内部口径的秒；
        # 非正值（缺省或写错）兜底为默认 500ms，而非报错——超时属于可
        # 安全取默认值的调优项。
        if timeout_ms <= 0:
            timeout_ms = DEFAULT_TIMEOUT_MS
        name = str(n.get("name", "") or "")
        # 旧形态的节点级 mode / AIMD params 明确拒绝（理由见顶部 mode）。
        for legacy in ("mode", "params", "bwlim_map_path"):
            if legacy in n:
                raise ValueError(
                    f"haproxy_nodes[{i}] ({name or '?'}): 字段 {legacy!r} 已"
                    f"废弃——限速由该节点 HAProxy 的 shared bwlim 配置执行，"
                    f"rl-limiter 不再写 map、无运行模式与 AIMD 参数"
                )
        cfg.nodes.append(
            model.NodeConfig(
                name=name,
                host=str(n.get("host", "") or ""),
                port=port,
                timeout_s=timeout_ms / 1000.0,
            )
        )
        # 节点登记限额（bits/s，运维口径）——超限告警基准。有挂载点的
        # 节点必填，数值合法性与必填校验在 _validate。
        quota_raw = n.get("quota_bps")
        if quota_raw is not None:
            try:
                cfg.node_quotas[name] = int(quota_raw)
            except (TypeError, ValueError):
                raise ValueError(
                    f"haproxy_nodes[{i}] ({name}): quota_bps 必须是整数"
                    f"（比特每秒），当前值 {quota_raw!r}"
                ) from None

    # ---- envs：业务环境分组（环境只是节点分组 + 挂载点归属，
    # 不携带限额——限额登记在节点上）。----
    envs_raw = raw.get("envs") or []
    if not isinstance(envs_raw, list):
        raise ValueError(
            f"envs 必须是环境列表，当前为 {type(envs_raw).__name__}"
        )
    for i, e in enumerate(envs_raw):
        if not isinstance(e, dict):
            raise ValueError(
                f"envs[{i}]: 每个环境必须是键值映射，当前为 {type(e).__name__}"
            )
        # 老形态（环境带配额/参数）明确拒绝并给出迁移指引，而不是静默
        # 忽略——静默忽略会让运维以为配额还生效着。
        for legacy in ("quota_bps", "params"):
            if legacy in e:
                raise ValueError(
                    f"envs[{i}] ({e.get('env_id', '?')}): 字段 {legacy!r} 已"
                    f"废弃——限额登记在节点上（haproxy_nodes[].quota_bps），"
                    f"环境仅作节点分组与聚合查看"
                )
        try:
            # 复用 EnvQuota 解析 targets 结构；quota 置 1 只为通过结构
            # 构造，组装监控单元时不使用环境级配额。
            group = model.EnvQuota.from_dict({**e, "quota_bps": 1})
        except (KeyError, TypeError, ValueError) as ex:
            raise ValueError(
                f"envs[{i}] ({e.get('env_id', '?')}) 结构错误: {ex!r}；"
                f"每个 target 必须是 {{node, frontend}} 键值映射"
            ) from None
        cfg.envs.append(group)
    return cfg


def _validate(cfg: ServiceConfig) -> None:
    """校验服务其余部分赖以运行的结构性不变量，逐条业务原因：

    - log_level 枚举校验：拼错的级别若静默回落会让运维误以为已调级；
    - tick_interval_s 必须 > 0：它是采样差分与告警判据窗口的时基；
    - 节点 name 唯一且非空：Target.node 以名字引用节点，重名会让引用
      二义、空名无法引用；host 非空、port 1-65535 是 TCP 接线的底线；
    - env_id 必填且唯一、节点 quota_bps > 0：限额是超限告警的基准，
      零或负值无意义；
    - 每个环境至少一个 target，target.node 必须已在 haproxy_nodes 中
      声明：引用未声明的节点意味着采不到用量；
    - 同一 Target 不得映射到两个环境：采集按 Target → 单元 归并用量，
      一对多映射会导致同一份流量被重复计入；
    - 节点是环境的独占资源：一个环境可以横跨多台 HAProxy，但一台
      HAProxy 只允许服务一个环境。混挂会让环境聚合视图失真——节点上的
      他环境流量会被计入本环境。
    """
    if cfg.log_level not in _LOG_LEVELS:
        raise ValueError(
            f"log_level 值非法：{cfg.log_level!r}，"
            f"必须是 {'/'.join(_LOG_LEVELS)} 之一"
        )
    if cfg.tick_interval_s <= 0:
        raise ValueError(
            f"tick_interval_s 必须 > 0（秒），当前值 {cfg.tick_interval_s!r}："
            f"它是采样差分与告警判据窗口的时基"
        )

    # 节点名 → 下标，用于查重与后续 target 引用校验。
    node_index: dict[str, int] = {}
    for i, n in enumerate(cfg.nodes):
        if not n.name:
            raise ValueError(
                f"haproxy_nodes[{i}]: name 不能为空——"
                f"envs 中的 target.node 以名字引用节点"
            )
        if n.name in node_index:
            raise ValueError(
                f"haproxy_nodes[{i}] 的 name {n.name!r} 与 "
                f"haproxy_nodes[{node_index[n.name]}] 重复："
                f"节点名是 target.node 的引用键，必须唯一"
            )
        node_index[n.name] = i
        if not n.host:
            raise ValueError(
                f"haproxy_nodes[{i}] ({n.name}): host 不能为空——"
                f"它是该节点 TCP stats socket 的内网地址"
            )
        if n.port < 1 or n.port > 65535:
            raise ValueError(
                f"haproxy_nodes[{i}] ({n.name}): port 必须在 1-65535 范围内，"
                f"当前值 {n.port!r}"
            )

    # Target → env_id 的归属表，用于检出跨环境（或同环境重复书写）的冲突。
    target_owner: dict[model.Target, str] = {}
    # 节点 → env_id 的独占归属表：一台 HAProxy 只允许服务一个环境。
    node_env_owner: dict[str, str] = {}
    seen_env_ids: dict[str, int] = {}
    for i, e in enumerate(cfg.envs):
        if not e.env_id:
            raise ValueError(f"envs[{i}]: env_id 不能为空")
        if e.env_id in seen_env_ids:
            raise ValueError(
                f"envs[{i}] 的 env_id {e.env_id!r} 与 "
                f"envs[{seen_env_ids[e.env_id]}] 重复：环境标识必须唯一"
            )
        seen_env_ids[e.env_id] = i
        if not e.targets:
            raise ValueError(
                f"envs[{i}] ({e.env_id}): 至少需要一个 target——"
                f"没有挂载点的环境采不到任何用量"
            )
        for t in e.targets:
            if not t.frontend:
                raise ValueError(
                    f"envs[{i}] ({e.env_id}): target 的 frontend 不能为空"
                    f"（节点 {t.node!r}）"
                )
            if t.node not in node_index:
                raise ValueError(
                    f"envs[{i}] ({e.env_id}) 的 target {t}: "
                    f"节点 {t.node!r} 未在 haproxy_nodes 中声明——"
                    f"引用未接线的节点采不到任何用量"
                )
            if t in target_owner:
                raise ValueError(
                    f"Target {t} 同时映射到环境 {target_owner[t]!r} 与 "
                    f"{e.env_id!r}：同一 (node, frontend) 只能属于一个环境，"
                    f"否则同一份流量会被重复计入两个环境"
                )
            target_owner[t] = e.env_id
            node_owner = node_env_owner.get(t.node)
            if node_owner is not None and node_owner != e.env_id:
                raise ValueError(
                    f"envs[{i}] ({e.env_id}) 的 target {t}: 节点 {t.node!r} "
                    f"已属于环境 {node_owner!r}——一个环境可以横跨多台 "
                    f"HAProxy，但一台 HAProxy 只允许服务一个环境（节点是"
                    f"环境的独占资源：混挂会让环境聚合视图失真）"
                )
            node_env_owner[t.node] = e.env_id

    # 被挂载的节点必须登记限额：它是该节点超限告警的基准，缺失或非正
    # 意味着该节点的监控单元无法构造。
    for node in node_env_owner:
        q = cfg.node_quotas.get(node)
        if q is None or q <= 0:
            raise ValueError(
                f"节点 {node!r} 已被挂载但未设置有效的 quota_bps"
                f"（当前值 {q!r}）：限额按节点登记"
                f"（haproxy_nodes[].quota_bps，单位比特每秒，"
                f"例如 40000000 表示 40 Mbps），应与该节点 haproxy.cfg 里 "
                f"shared bwlim 的 limit 保持一致"
            )
