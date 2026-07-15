# rl_limiter.config —— 服务配置的解析与校验（服务的"启动契约"层）。
#
# v2.0 集中式架构下，服务配置提供四类信息：
#   1. 服务身份（node_id）：rl-limiter 实例的唯一标识，配置长轮询、指标
#      上报、心跳都用它向管理后台归属数据；
#   2. HAProxy 节点接线（haproxy_nodes）：受控节点的内网 TCP stats socket
#      地址与 bwlim map 路径。这属于基础设施配置，只在本地维护，不随管理
#      后台配置下发（见 model.ControllerConfig 的注释）；
#   3. 管理后台接入（backend）：base_url、fail-static 缓存路径、mTLS 材料。
#      base_url 留空表示 standalone 模式：不连接后台，只用本地 envs 运行；
#   4. 引导配额（envs）：standalone 模式下的唯一配额来源；接入后台时仅作
#      首启引导，后台下发配置后以下发为准。
#
# 配置来源有两种，共用同一套解析/校验管线（from_raw）：
#   - 本地 YAML 文件（load）：传统部署方式；
#   - MySQL 数据库（dbconfig 模块）：数据库各表的行被组装成与 YAML 解析
#     结果同构的原始 dict 后走 from_raw——校验规则只写一遍，两种来源的
#     错误信息与拒绝行为完全一致。
#
# 单位约定：配额一律按运维口径的「比特每秒」（quota_bps，200000000 =
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
# 正常往返，又不至于拖住每秒一次的核心循环。
DEFAULT_TIMEOUT_MS = 500
# bwlim map 默认路径，必须与各节点 haproxy 配置中 map_str_int(...) 引用的
# 路径一致，否则限速值写了也不会生效。
DEFAULT_BWLIM_MAP_PATH = "/etc/haproxy/maps/bwlim.map"
# fail-static 配置缓存默认落盘路径（设计文档 §3.7）：与后台断联时按缓存中
# 最后一次下发的配置继续限速。
DEFAULT_CACHE_PATH = "/var/lib/rl-limiter/config-cache.json"

# 合法日志级别枚举。拼错的级别若静默回落会让运维误以为已调级，必须显式拒绝。
_LOG_LEVELS = ("debug", "info", "warn", "error")


@dataclass(slots=True)
class BackendOptions:
    """管理后台（Controller）接入方式。

    base_url 留空即 standalone 模式：Reporter.run 只挂起不发请求（防御
    分支），配额完全来自本地 envs。ca/cert/key 是可选的 mTLS 材料
    （§3.6：双向认证防伪造上报/伪造下发），客户端证书与私钥必须成对。
    """

    base_url: str = ""
    cache_path: str = DEFAULT_CACHE_PATH
    ca_file: str = ""
    cert_file: str = ""
    key_file: str = ""


@dataclass(slots=True)
class ServiceConfig:
    """磁盘上完整的 rl-limiter 服务配置（load 的返回类型）。"""

    # 本服务实例的唯一标识，随配置轮询/指标/心跳上报，须与管理后台记录一致。
    node_id: str
    # 运行模式：dry-run（只算不写，观测模式）或 enforce（真实下发限速）。
    # 默认 dry-run，确保误部署时不产生任何数据面影响。
    mode: str = model.MODE_DRY_RUN
    log_level: str = DEFAULT_LOG_LEVEL
    # 快环 tick 周期（秒）。
    tick_interval_s: float = DEFAULT_TICK_INTERVAL_S
    # 受控 HAProxy 节点清单（基础设施配置，仅本地维护）。
    nodes: list[model.NodeConfig] = field(default_factory=list)
    # 本地静态/引导配额。
    envs: list[model.EnvQuota] = field(default_factory=list)
    backend: BackendOptions = field(default_factory=BackendOptions)


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
    except ValueError as e:
        raise ValueError(f"{source} 无效: {e}") from None
    return cfg


def _parse(raw: dict[str, Any]) -> ServiceConfig:
    """把 yaml.safe_load 的原始 dict 组装为 ServiceConfig 并填默认值。

    只做结构转换与兜底，不做业务校验（校验集中在 _validate，便于逐条
    给出带上下文的错误信息）。
    """
    cfg = ServiceConfig(
        node_id=str(raw.get("node_id", "") or ""),
        mode=str(raw.get("mode", "") or "") or model.MODE_DRY_RUN,
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
        cfg.nodes.append(
            model.NodeConfig(
                name=str(n.get("name", "") or ""),
                host=str(n.get("host", "") or ""),
                port=port,
                bwlim_map_path=str(n.get("bwlim_map_path", "") or "")
                or DEFAULT_BWLIM_MAP_PATH,
                timeout_s=timeout_ms / 1000.0,
            )
        )

    # ---- envs：引导配额（结构与管理后台下发一致，直接复用 from_dict）----
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
        try:
            cfg.envs.append(model.EnvQuota.from_dict(e))
        except (KeyError, TypeError, ValueError) as ex:
            # from_dict 对缺键/类型错误直接抛异常，这里补上环境下标与
            # 结构提示，避免运维面对裸 KeyError 无从下手。
            raise ValueError(
                f"envs[{i}] ({e.get('env_id', '?')}) 结构错误: {ex!r}；"
                f"每个 target 必须是 {{node, frontend}} 键值映射，"
                f"params 必须是键值映射"
            ) from None

    # ---- backend：管理后台接入 ----
    backend_raw = raw.get("backend") or {}
    if not isinstance(backend_raw, dict):
        raise ValueError(
            f"backend 必须是键值映射，当前为 {type(backend_raw).__name__}"
        )
    tls_raw = backend_raw.get("tls") or {}
    if not isinstance(tls_raw, dict):
        raise ValueError(
            f"backend.tls 必须是键值映射，当前为 {type(tls_raw).__name__}"
        )
    cfg.backend = BackendOptions(
        base_url=str(backend_raw.get("base_url", "") or ""),
        cache_path=str(backend_raw.get("cache_path", "") or "")
        or DEFAULT_CACHE_PATH,
        ca_file=str(tls_raw.get("ca_file", "") or ""),
        cert_file=str(tls_raw.get("cert_file", "") or ""),
        key_file=str(tls_raw.get("key_file", "") or ""),
    )
    return cfg


def _validate(cfg: ServiceConfig) -> None:
    """校验服务其余部分赖以运行的结构性不变量，逐条业务原因：

    - node_id 必填：它是管理后台识别本实例的唯一键，缺失则轮询/上报/
      心跳全部无法归属；
    - mode 只能是 dry-run 或 enforce：写错模式的后果不对称（该限不限 /
      不该限乱限），必须在启动前拦下而不是静默取默认；
    - log_level 枚举校验：拼错的级别若静默回落会让运维误以为已调级；
    - tick_interval_s 必须 > 0：它是采样差分与 AIMD 判据窗口的时基；
    - 节点 name 唯一且非空：Target.node 以名字引用节点，重名会让引用
      二义、空名无法引用；host 非空、port 1-65535 是 TCP 接线的底线；
    - env_id 必填且唯一、quota_bps > 0：配额是限速计算的分母/基准，
      零或负配额无意义且会把环境限死；
    - 每个环境至少一个 target，target.node 必须已在 haproxy_nodes 中
      声明：引用未声明的节点意味着采不到用量也无处写限速值；
    - 同一 Target 不得映射到两个环境：采集按 Target → env 归并用量、
      执行按 env 写整形值，一对多映射会导致同一份流量被重复计入两个
      环境、限速值互相覆盖，必须拒绝。
    """
    if not cfg.node_id:
        raise ValueError(
            "node_id 不能为空：它是本服务实例的唯一标识，"
            "配置轮询/指标上报/心跳都用它向管理后台归属数据"
        )
    if cfg.mode not in (model.MODE_DRY_RUN, model.MODE_ENFORCE):
        raise ValueError(
            f"mode 值非法：{cfg.mode!r}，必须是 {model.MODE_DRY_RUN!r} 或 "
            f"{model.MODE_ENFORCE!r}（写错模式的后果不对称，不做静默回落）"
        )
    if cfg.log_level not in _LOG_LEVELS:
        raise ValueError(
            f"log_level 值非法：{cfg.log_level!r}，"
            f"必须是 {'/'.join(_LOG_LEVELS)} 之一"
        )
    if cfg.tick_interval_s <= 0:
        raise ValueError(
            f"tick_interval_s 必须 > 0（秒），当前值 {cfg.tick_interval_s!r}："
            f"它是采样差分与 AIMD 判据窗口的时基"
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
        if e.quota_bits_per_sec <= 0:
            raise ValueError(
                f"envs[{i}] ({e.env_id}): quota_bps 必须 > 0，"
                f"当前值 {e.quota_bits_per_sec!r}"
                f"（单位为比特每秒，例如 200000000 表示 200 Mbps）"
            )
        if not e.targets:
            raise ValueError(
                f"envs[{i}] ({e.env_id}): 至少需要一个 target——"
                f"没有挂载点的环境既采不到用量也无处下发限速"
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
                    f"引用未接线的节点既采不到用量也无处写限速值"
                )
            if t in target_owner:
                raise ValueError(
                    f"Target {t} 同时映射到环境 {target_owner[t]!r} 与 "
                    f"{e.env_id!r}：同一 (node, frontend) 只能属于一个环境，"
                    f"否则同一份流量会被重复计入两个环境、限速值互相覆盖"
                )
            target_owner[t] = e.env_id
