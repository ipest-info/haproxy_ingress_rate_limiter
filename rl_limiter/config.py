# rl_limiter.config —— 服务配置的解析与校验（服务的"启动契约"层）。
#
# 配置来源只有一个：**本地 YAML 文件**（-c 指定）。它提供三类信息：
#   1. **本机 HAProxy 的接线**（haproxy 段）：stats socket 怎么连（本机
#      unix socket 或内网 TCP）、命令超时，以及 **cfg_path**——本机
#      haproxy.cfg 的路径。负载均衡配置（监听端口、模式、后端服务器）
#      不在 YAML 里：**haproxy.cfg 才是它们的唯一权威**，rl-limiter 从
#      cfg 直接解析受管 frontend 清单（见 cfgparse 模块）；
#   2. **限额登记**（quotas 段 + instance_quota_mbps）：frontend 名 →
#      限额（Mbps），以及可选的**实例总限速**。cfg 里
#      存在、这里登记了正限额的 frontend 参与限速（tc）与超限告警；
#      未登记的只监控不限速（启动会 warn 一次提醒）；**显式写 0 表示
#      "确认不限速"**——语义与不登记相同，但不再提醒，且清单收缩到
#      全 0/全空时会把网卡上的 tc 队列树整个撤掉；
#   3. 服务级运行参数：log_level、tick_interval_s。
#
# 单位约定：**限额的配置单位一律是 Mbps**（40 = 40 Mbps，允许小数）；
# 内部统一换算为 bytes/s（换算点只有 model.FrontendConfig 的两个
# property）。
#
# 加载流程：读文件 → yaml.safe_load → 补默认值 → 校验。load 本身不打日志，
# 成功日志由 main 统一输出；失败通过异常信息精确指出问题字段、当前值与
# 拒绝原因（中文），让运维不需要翻代码就能定位配置错误。

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any

import yaml

from . import model

# 各键缺失时补上的默认值。与部署示例配置保持一致。
DEFAULT_LOG_LEVEL = "info"
# 快环 tick 周期（秒）：每秒采样一轮。
DEFAULT_TICK_INTERVAL_S = 1.0
# 单次 HAProxy runtime API 调用默认超时（毫秒）。500ms 远大于内网 TCP 的
# 正常往返，又不至于拖住每秒一次的采样循环。
DEFAULT_TIMEOUT_MS = 500

# 合法日志级别枚举。拼错的级别若静默回落会让运维误以为已调级，必须显式拒绝。
_LOG_LEVELS = ("debug", "info", "warn", "error")

# unix socket 路径长度上限：AF_UNIX 的 sun_path 在 Linux 上是 108 字节的
# 定长数组（含结尾 NUL），故可用长度为 107。超限只会在 connect() 时报
# "AF_UNIX path too long"，必须在启动校验里提前拦下。
_UNIX_PATH_MAX = 107

# quotas 键（frontend 名）允许的字符：与 HAProxy proxy 名的约束一致，
# 也保证日志/控制台里的名字可读且无歧义。
_NAME_RE = re.compile(r"^[A-Za-z0-9._-]{1,64}$")


@dataclass
class ServiceConfig:
    """一份完整的服务配置（本机 HAProxy 接线 + 限额登记）。

    注意 frontends **不在这里**：受管 frontend 清单来自 haproxy.cfg 的
    解析（cfgparse.load_frontends），quotas 只是给其中一部分登记限额。
    """

    log_level: str = DEFAULT_LOG_LEVEL
    tick_interval_s: float = DEFAULT_TICK_INTERVAL_S
    # 实例总限速（Mbps；0 = 不限）。罩住本机 HAProxy **全部 frontend**
    # 的出向流量合计（含只监控未登记限额的段）；SSH/监控/系统流量不在
    # 总闸之内、不受影响。tc 侧实现见 tcshaper 的层级模式。
    instance_quota_mbps: float = 0.0
    # 本机 HAProxy 的接线（stats socket + cfg 路径）。
    haproxy: model.NodeConfig = field(default_factory=model.NodeConfig)
    # 限额登记：frontend 名 → 限额（Mbps，≥ 0；0 = 显式不限速）。
    quotas: dict[str, float] = field(default_factory=dict)


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
    """把已解析的原始结构转为 ServiceConfig，统一补默认值与校验。"""
    if raw is None:
        raw = {}
    if not isinstance(raw, dict):
        raise ValueError(f"{source} 无效: 顶层必须是键值映射，实际是 {type(raw).__name__}")
    try:
        cfg = _parse(raw)
        _validate(cfg)
    except ValueError as e:
        raise ValueError(f"{source} 无效: {e}") from None
    return cfg


def _parse(raw: dict[str, Any]) -> ServiceConfig:
    """结构解析 + 默认值填充。只做类型转换，语义校验在 _validate。"""
    # 旧形态（YAML 里带 frontends 段：监听端口/后端服务器/balance…）明确
    # 拒绝并给出迁移指引：负载均衡配置已改为从 haproxy.cfg 直接读取，
    # 静默忽略会让运维以为这些配置还生效着。
    if "frontends" in raw:
        raise ValueError(
            "字段 'frontends' 已废弃——负载均衡配置（监听端口、模式、后端"
            "服务器）以 haproxy.cfg 为唯一权威，rl-limiter 直接解析 cfg；"
            "限额改在 'quotas' 段登记（frontend 名 → Mbps）")
    # 旧字段名显式拒绝：静默忽略会让运维以为总限速还生效着。
    if "nic_quota_mbps" in raw:
        raise ValueError(
            "字段 'nic_quota_mbps' 已更名为 'instance_quota_mbps'，且语义"
            "从「整张网卡的总限速」改为「HAProxy 实例的总限速」：总闸只"
            "罩全部 frontend 的出向流量合计（含未登记限额的段），SSH/"
            "监控等系统流量不再受影响——请改用新字段名")

    cfg = ServiceConfig()

    cfg.log_level = str(raw.get("log_level", DEFAULT_LOG_LEVEL)).strip().lower()

    tick_raw = raw.get("tick_interval_s", DEFAULT_TICK_INTERVAL_S)
    try:
        cfg.tick_interval_s = float(tick_raw)
    except (TypeError, ValueError):
        raise ValueError(
            f"tick_interval_s 必须是数字，当前值 {tick_raw!r}"
        ) from None

    inst_raw = raw.get("instance_quota_mbps", 0) or 0
    try:
        cfg.instance_quota_mbps = float(inst_raw)
    except (TypeError, ValueError):
        raise ValueError(
            f"instance_quota_mbps 必须是数字（Mbps），"
            f"当前值 {inst_raw!r}") from None

    cfg.haproxy = _parse_haproxy(raw.get("haproxy") or {})

    quotas_raw = raw.get("quotas") or {}
    if not isinstance(quotas_raw, dict):
        raise ValueError(
            f"quotas 必须是键值映射（frontend 名 → 限额 Mbps），"
            f"实际是 {type(quotas_raw).__name__}")
    for name, v in quotas_raw.items():
        try:
            cfg.quotas[str(name).strip()] = float(v)
        except (TypeError, ValueError):
            raise ValueError(
                f"quotas.{name} 必须是数字（Mbps），当前值 {v!r}") from None
    return cfg


def _parse_haproxy(h: Any) -> model.NodeConfig:
    """解析本机 HAProxy 的接线段。"""
    if not isinstance(h, dict):
        raise ValueError(
            f"haproxy 必须是键值映射，实际是 {type(h).__name__}")

    port_raw = h.get("port", 0) or 0
    try:
        port = int(port_raw)
    except (TypeError, ValueError):
        raise ValueError(f"haproxy.port 必须是整数，当前值 {port_raw!r}") from None

    timeout_raw = h.get("timeout_ms", DEFAULT_TIMEOUT_MS)
    try:
        timeout_ms = int(timeout_raw)
    except (TypeError, ValueError):
        raise ValueError(
            f"haproxy.timeout_ms 必须是整数，当前值 {timeout_raw!r}") from None
    # 非正超时若穿透到网络层会让每次采样立即失败，按缺失处理回落默认值。
    if timeout_ms <= 0:
        timeout_ms = DEFAULT_TIMEOUT_MS

    return model.NodeConfig(
        name=str(h.get("name", "haproxy") or "haproxy").strip(),
        host=str(h.get("host", "") or "").strip(),
        port=port,
        timeout_s=timeout_ms / 1000.0,
        socket_path=str(h.get("socket_path", "") or "").strip(),
        cfg_path=str(h.get("cfg_path", "") or "").strip(),
    )


def _validate(cfg: ServiceConfig) -> None:
    """语义校验。任何一条不通过都抛 ValueError，信息里点名字段、当前值与原因。"""
    if cfg.log_level not in _LOG_LEVELS:
        raise ValueError(
            f"log_level 取值非法: {cfg.log_level!r}，"
            f"可选 {'/'.join(_LOG_LEVELS)}")

    if cfg.tick_interval_s <= 0:
        raise ValueError(
            f"tick_interval_s 必须为正数，当前值 {cfg.tick_interval_s}")
    if abs(cfg.tick_interval_s - 1.0) > 1e-9:
        # 速率差分、10 秒窗口、超限持续秒数都以"1 拍 = 1 秒"为前提；
        # 改这个值会让速率与告警口径整体偏移（0.5 秒一拍时速率翻倍）。
        raise ValueError(
            f"tick_interval_s 目前只支持 1.0（当前值 {cfg.tick_interval_s}）："
            f"速率差分与 10 秒窗口、超限持续秒数均以「1 拍 = 1 秒」为前提，"
            f"改成别的值会让速率口径与告警阈值整体失真")

    _validate_haproxy(cfg.haproxy)

    if cfg.instance_quota_mbps < 0:
        raise ValueError(
            f"instance_quota_mbps 不能为负数"
            f"（当前值 {cfg.instance_quota_mbps!r}）"
            f"——正数是本机 HAProxy 实例的总限速（Mbps），0 表示不限")
    if 0 < cfg.instance_quota_mbps * 1_000_000 / 8 < 1:
        raise ValueError(
            f"instance_quota_mbps={cfg.instance_quota_mbps} 太小（换算成 "
            f"bytes/s 后不足 1），tc 会拒绝这个速率；要不限就写 0")

    for name, mbps in cfg.quotas.items():
        if not _NAME_RE.match(name):
            raise ValueError(
                f"quotas 的键 {name!r} 非法——应为 haproxy.cfg 里的 "
                f"frontend/listen 段名（字母、数字、点、下划线、连字符，"
                f"长度 1-64）")
        if mbps < 0:
            raise ValueError(
                f"quotas.{name} 不能为负数（当前值 {mbps!r}）——正数是限额"
                f"（Mbps），0 表示显式不限速（撤掉该端口的 tc 限速类）")
        if 0 < mbps * 1_000_000 / 8 < 1:
            raise ValueError(
                f"quotas.{name}={mbps} 太小（换算成 bytes/s 后不足 1），"
                f"tc 会拒绝这个速率；要不限速请写 0")


def _validate_haproxy(n: model.NodeConfig) -> None:
    """校验本机 HAProxy 的接线段（采样通道二选一 + cfg 路径）。"""
    if not _NAME_RE.match(n.name):
        raise ValueError(
            f"haproxy.name 非法（当前值 {n.name!r}）——只允许字母、数字、"
            f"点、下划线、连字符，长度 1-64")
    # 采样通道二选一：本机 unix socket（同机部署）或内网 TCP（远程观测）。
    # 两者都给会产生"到底连哪个"的二义，一个都不给则根本无法接线。
    if n.socket_path and (n.host or n.port):
        raise ValueError(
            f"haproxy: socket_path 与 host/port 只能二选一（当前 "
            f"socket_path={n.socket_path!r}、host={n.host!r}、port={n.port!r}）"
            f"——同机部署填 socket_path（本机 unix stats socket），"
            f"远程只读观测填 host/port（内网 TCP stats socket）")
    if not n.socket_path:
        if not n.host:
            raise ValueError(
                "haproxy: host 不能为空——远程观测形态下它是 stats socket 的"
                "内网地址；同机部署请改填 socket_path"
                "（本机 unix stats socket 路径）")
        if n.port < 1 or n.port > 65535:
            raise ValueError(
                f"haproxy: port 必须在 1-65535 范围内，当前值 {n.port!r}")
    elif not n.socket_path.startswith("/"):
        # 相对路径的解析结果取决于服务的工作目录（systemd 下通常是 /），
        # 同一份配置在不同启动方式下会连到不同的位置——必须写绝对路径。
        raise ValueError(
            f"haproxy: socket_path 必须是绝对路径（当前值 {n.socket_path!r}）"
            f"——相对路径的解析依赖服务的工作目录，同一份配置换个启动方式"
            f"就会连到别处")
    elif len(n.socket_path.encode("utf-8")) > _UNIX_PATH_MAX:
        # AF_UNIX 的 sun_path 是定长数组（Linux 上 108 字节含结尾 NUL）。
        # 超长路径要到 connect() 才报 "AF_UNIX path too long"，而采样
        # 每秒一次——那会变成每秒一条看不懂的告警。启动时拦下。
        raise ValueError(
            f"haproxy: socket_path 过长"
            f"（{len(n.socket_path.encode('utf-8'))} 字节，上限 "
            f"{_UNIX_PATH_MAX}）——unix socket 路径长度受内核 sun_path 限制，"
            f"请换用更短的路径（如 /run/haproxy/admin.sock）")

    # cfg_path：受管 frontend 清单的来源，必填。存在性/可读性在引导时
    # 检查（cfgparse.load_frontends 直接读），这里只拦结构性错误。
    if not n.cfg_path:
        raise ValueError(
            "haproxy: cfg_path 不能为空——受管 frontend 清单（监听端口、"
            "模式）从本机 haproxy.cfg 直接解析而来，必须给出它的路径"
            "（如 /etc/haproxy/haproxy.cfg）")
    if not n.cfg_path.startswith("/"):
        raise ValueError(
            f"haproxy: cfg_path 必须是绝对路径（当前值 {n.cfg_path!r}）")
