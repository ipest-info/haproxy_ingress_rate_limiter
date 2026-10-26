# rl_limiter.config —— 服务配置的解析与校验（服务的"启动契约"层）。
#
# 本分支是**通用形态**：不依赖任何具体服务（监控=双网卡设备视图 +
# 内核 TCP 计数器，限速=客户端侧网卡入向 tc），因此配置也不再有任何
# 服务接线。配置来源只有一个：**本地 YAML 文件**（-c 指定），内容四类：
#   1. **实例总限速**（instance_quota_mbps）：本机出向带宽目标（Mbps，
#      网卡口径），由闭环动态收紧入向执行。这是本形态的主限速入口；
#   2. **端口登记**（ports 段，可选）：名字 → 本机监听端口。只为**按
#      端口的静态入向限速**服务（request_limits 按这些名字登记），
#      与机器上跑什么服务无关；不登记任何端口照样做整机监控与总量
#      限速；
#   3. **入向限速登记**（request_limits 段，可选）：ports 里的名字 →
#      {mbps, conn_rate}，纯 tc 执行；
#   4. 服务级运行参数：log_level、tick_interval_s、name（实例名，
#      页面/日志展示）。
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

from . import ingressgov, model

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
    """一份完整的服务配置（通用形态：无任何服务接线）。"""

    # 实例名（页面/日志展示；一台机器一个 rl-limiter）。
    name: str = "server"
    log_level: str = DEFAULT_LOG_LEVEL
    tick_interval_s: float = DEFAULT_TICK_INTERVAL_S
    # 实例总限速（Mbps；0 = 不限）。罩住本机 HAProxy **全部 frontend**
    # 的出向流量合计（含只监控未登记限额的段）；SSH/监控/系统流量不在
    # 总闸之内、不受影响。tc 侧实现见 tcshaper 的层级模式。
    instance_quota_mbps: float = 0.0
    # 请求方向（入向）限速登记：frontend 名 → RequestLimit（mbps /
    # conn_rate）。双网卡形态专用，入向执行网卡由 RL_TC_IN_IFACE 指定。
    request_limits: dict = field(default_factory=dict)
    # 入向动态限速（闭环）：按实测出向带宽对照 quotas 目标，平缓调整
    # 入向 police 的带宽上限。启动时定型。见 rl_limiter/ingressgov.py。
    dynamic_ingress: "ingressgov.IngressGovConfig" = field(
        default_factory=lambda: ingressgov.IngressGovConfig())
    # 端口登记（可选）：名字 → 本机监听端口。只为按端口的静态入向限速
    # 服务（request_limits 按这些名字登记端口清单），与跑什么服务无关。
    ports: dict[str, int] = field(default_factory=dict)
    # 历史字段（本形态恒空）：端口级**出向目标**需要端口级出向观测，
    # 通用形态没有这个数据源（网卡拆不了端口），故不提供。保留字段是
    # 为了共享代码路径（cfgparse/webconsole 等按名取限额处）零改动。
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

    # 旧形态的服务接线段显式拒绝 + 迁移指引：本分支是通用形态，不采样
    # 任何服务，静默忽略会让运维以为还在按 HAProxy 的口径监控。
    if "haproxy" in raw:
        raise ValueError(
            "字段 'haproxy' 已移除——本形态不依赖任何具体服务：监控是"
            "双网卡设备视图 + 内核 TCP 计数器（/proc/net/snmp），限速是"
            "客户端侧网卡的入向 tc。删掉 haproxy 段即可；按端口限速改在"
            " 'ports' 段登记名字→端口，再在 request_limits 按名字登记"
            "限额。要按 HAProxy 口径监控请用 HAProxy 形态分支")
    if "quotas" in raw and (raw.get("quotas") or {}):
        raise ValueError(
            "字段 'quotas'（端口级出向带宽目标）在本形态不可用——端口级"
            "出向观测需要服务配合，通用形态没有这个数据源（网卡无法按"
            "端口拆分）。总量限速用 instance_quota_mbps（出向目标，闭环"
            "执行）；按端口限请求方向用 ports + request_limits")

    cfg = ServiceConfig()

    cfg.name = str(raw.get("name", "server") or "server").strip() or "server"
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

    ports_raw = raw.get("ports") or {}
    if not isinstance(ports_raw, dict):
        raise ValueError(
            f"ports 必须是键值映射（名字 → 本机监听端口），"
            f"实际是 {type(ports_raw).__name__}")
    for pname, pv in ports_raw.items():
        try:
            cfg.ports[str(pname).strip()] = int(pv)
        except (TypeError, ValueError):
            raise ValueError(
                f"ports.{pname} 必须是整数端口号，当前值 {pv!r}") from None

    req_raw = raw.get("request_limits") or {}
    if not isinstance(req_raw, dict):
        raise ValueError(
            f"request_limits 必须是键值映射（frontend 名 → {{mbps, "
            f"conn_rate}}），实际是 {type(req_raw).__name__}")
    for name, item in req_raw.items():
        if not isinstance(item, dict):
            raise ValueError(
                f"request_limits.{name} 必须是映射（可含 mbps / conn_rate "
                f"两个键），实际是 {type(item).__name__}")
        unknown = set(item) - {"mbps", "conn_rate"}
        if unknown:
            raise ValueError(
                f"request_limits.{name} 有不认识的键 {sorted(unknown)}"
                f"——只支持 mbps（入向带宽，Mbps）与 conn_rate（每秒新建"
                f"连接上限）")
        try:
            mbps = float(item.get("mbps", 0) or 0)
        except (TypeError, ValueError):
            raise ValueError(
                f"request_limits.{name}.mbps 必须是数字（Mbps），"
                f"当前值 {item.get('mbps')!r}") from None
        try:
            conn_rate = int(item.get("conn_rate", 0) or 0)
        except (TypeError, ValueError):
            raise ValueError(
                f"request_limits.{name}.conn_rate 必须是整数（个/秒），"
                f"当前值 {item.get('conn_rate')!r}") from None
        cfg.request_limits[str(name).strip()] = model.RequestLimit(
            mbps=mbps, conn_rate=conn_rate)

    cfg.dynamic_ingress = _parse_dynamic_ingress(
        raw.get("dynamic_ingress") or {})

    return cfg


def _parse_dynamic_ingress(d: Any) -> ingressgov.IngressGovConfig:
    """解析入向动态限速段（dynamic_ingress）。启动时定型：运行期改动
    不热生效（cfg 轮询检测到变化会提示需重启）。"""
    if not isinstance(d, dict):
        raise ValueError(
            f"dynamic_ingress 必须是键值映射，实际是 {type(d).__name__}")
    g = ingressgov.IngressGovConfig()
    known = {"enabled", "interval_s", "low_ratio", "high_ratio",
             "step_up", "step_down_max", "floor_ratio", "apply_epsilon"}
    unknown = set(d) - known
    if unknown:
        raise ValueError(
            f"dynamic_ingress 有不认识的键 {sorted(unknown)}"
            f"（支持 {sorted(known)}）")
    g.enabled = bool(d.get("enabled", g.enabled))
    g.explicit = "enabled" in d
    for key in ("interval_s", "low_ratio", "high_ratio", "step_up",
                "step_down_max", "floor_ratio", "apply_epsilon"):
        if key in d:
            try:
                setattr(g, key, float(d[key]))
            except (TypeError, ValueError):
                raise ValueError(
                    f"dynamic_ingress.{key} 必须是数字，"
                    f"当前值 {d[key]!r}") from None
    return g



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

    for pname, port in cfg.ports.items():
        if not _NAME_RE.match(pname):
            raise ValueError(
                f"ports 的键 {pname!r} 非法——给端口起的名字（字母、数字、"
                f"点、下划线、连字符，长度 1-64），request_limits 按它登记")
        if not (1 <= port <= 65535):
            raise ValueError(
                f"ports.{pname} 端口号必须在 1~65535（当前值 {port}）")
    seen_ports: dict[int, str] = {}
    for pname, port in cfg.ports.items():
        if port in seen_ports:
            raise ValueError(
                f"ports.{pname} 与 ports.{seen_ports[port]} 登记了同一个"
                f"端口 {port}——一个端口一个名字，别重复")
        seen_ports[port] = pname

    if cfg.instance_quota_mbps < 0:
        raise ValueError(
            f"instance_quota_mbps 不能为负数"
            f"（当前值 {cfg.instance_quota_mbps!r}）"
            f"——正数是本机 HAProxy 实例的总限速（Mbps），0 表示不限")
    if 0 < cfg.instance_quota_mbps * 1_000_000 / 8 < 1:
        raise ValueError(
            f"instance_quota_mbps={cfg.instance_quota_mbps} 太小（换算成 "
            f"bytes/s 后不足 1），tc 会拒绝这个速率；要不限就写 0")

    for name, rl in cfg.request_limits.items():
        if not _NAME_RE.match(name):
            raise ValueError(
                f"request_limits 的键 {name!r} 非法——应为 ports 段登记的"
                f"名字（字母、数字、点、下划线、连字符，长度 1-64）")
        if name not in cfg.ports:
            raise ValueError(
                f"request_limits.{name} 没有对应的端口登记——先在 ports "
                f"段写上 '{name}: <端口号>'，tc 才知道限哪个端口")
        if rl.mbps < 0:
            raise ValueError(
                f"request_limits.{name}.mbps 不能为负数（当前值 "
                f"{rl.mbps!r}）——正数是请求方向带宽上限（Mbps），0 表示"
                f"不限带宽")
        if 0 < rl.mbps * 1_000_000 / 8 < 1:
            raise ValueError(
                f"request_limits.{name}.mbps={rl.mbps} 太小（换算成 "
                f"bytes/s 后不足 1），tc 会拒绝这个速率；要不限就写 0")
        if rl.conn_rate < 0:
            raise ValueError(
                f"request_limits.{name}.conn_rate 不能为负数（当前值 "
                f"{rl.conn_rate!r}）——正数是每秒新建连接上限，0 表示不限")

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

    g = cfg.dynamic_ingress
    if g.interval_s < 5:
        raise ValueError(
            f"dynamic_ingress.interval_s 必须 ≥ 5（当前值 {g.interval_s}）"
            f"——观测用的是 10s 均值，调得比观测窗口还快只会追着噪声振荡")
    if not (0 < g.low_ratio <= g.high_ratio):
        raise ValueError(
            f"dynamic_ingress 需要 0 < low_ratio ≤ high_ratio（当前 "
            f"low_ratio={g.low_ratio} high_ratio={g.high_ratio}）——"
            f"[low, high]×目标 是不动作的死区")
    if not (0 < g.step_up <= 0.5):
        raise ValueError(
            f"dynamic_ingress.step_up 必须在 (0, 0.5] 内（当前值 "
            f"{g.step_up}）——这是刻意平缓的控制器，单步上调超过上限的"
            f"一半就不平缓了")
    if not (0 < g.step_down_max <= 0.5):
        raise ValueError(
            f"dynamic_ingress.step_down_max 必须在 (0, 0.5] 内（当前值 "
            f"{g.step_down_max}）——单次砍一半以上请直接用静态限速")
    if not (0 < g.floor_ratio < 1):
        raise ValueError(
            f"dynamic_ingress.floor_ratio 必须在 (0, 1) 内（当前值 "
            f"{g.floor_ratio}）——0 会把入向掐死，≥1 等于不调")
    if not (0 <= g.apply_epsilon <= 0.2):
        raise ValueError(
            f"dynamic_ingress.apply_epsilon 必须在 [0, 0.2] 内（当前值 "
            f"{g.apply_epsilon}）——太大的死区会让调节永远下发不出去")


