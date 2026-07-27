# rl_limiter.config —— 服务配置的解析与校验（服务的"启动契约"层）。
#
# 单 HAProxy 模型（v0.4 起）。服务配置提供三类信息：
#   1. **本机 HAProxy 的接线**（haproxy 段）：stats socket 怎么连
#      （本机 unix socket 或内网 TCP）、命令超时；
#   2. **受管 frontend 清单**（frontends 段）：每个 frontend 的监听端口、
#      限额（quota_bps）、模式、超时、后端服务器列表。这份清单既是监控
#      的判定基准，也是写进 haproxy.cfg 受管区块的**唯一数据源**——
#      两者同源，v0.3 那种"库里改了、cfg 忘了改"的配置漂移不再可能；
#   3. 服务级运行参数：log_level、tick_interval_s。
#
# 配置来源有两种，共用同一套解析/校验管线（from_raw）：
#   - MySQL 数据库（dbconfig 模块，生产权威）：数据库各表的行被组装成
#     与 YAML 解析结果同构的原始 dict 后走 from_raw——校验规则只写一遍，
#     两种来源的错误信息与拒绝行为完全一致；
#   - 本地 YAML 文件（load）：standalone / 开发联调。
#
# 单位约定：限额一律按运维口径的「比特每秒」（quota_bps，200000000 =
# 200 Mbps）书写，内部统一换算为 bytes/s（见 model.FrontendConfig）。
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
# 快环 tick 周期（秒）：每秒采样/决策/执行一轮。
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

# HAProxy 的 proxy 名与 server 名允许的字符。收紧到这个集合有两个理由：
# 一是 HAProxy 自身对 proxy 名就有限制（不能含空格等）；二是这些名字会被
# 原样渲染进 haproxy.cfg 的受管区块，放任任意字符等于允许通过配置库往
# 配置文件里注入任意指令（换行 + 任意配置行）。见 enforcer 的受管区块生成。
_NAME_RE = re.compile(r"^[A-Za-z0-9._-]{1,64}$")
# 后端地址：IP 或主机名。同样会被渲染进 cfg，同样必须限制字符集。
_ADDRESS_RE = re.compile(r"^[A-Za-z0-9._:-]{1,255}$")

_MODES = ("tcp", "http")
# HAProxy 支持的 balance 算法里，适用于本项目这种"一组等价后端"场景的子集。
# 白名单而非黑名单：这个值同样直接进 cfg。
_BALANCE_ALGOS = (
    "roundrobin", "static-rr", "leastconn", "first", "source", "random",
)


@dataclass
class ServiceConfig:
    """一份完整的服务配置（本机 HAProxy + 它的受管 frontend 清单）。"""

    log_level: str = DEFAULT_LOG_LEVEL
    tick_interval_s: float = DEFAULT_TICK_INTERVAL_S
    # 本机 HAProxy 的接线（stats socket）。
    haproxy: model.NodeConfig = field(default_factory=model.NodeConfig)
    # 受管 frontend 清单。
    frontends: list[model.FrontendConfig] = field(default_factory=list)

    def to_controller_config(self, version: int = 0) -> model.ControllerConfig:
        """转成投递给监控主循环的运行期配置。"""
        return model.ControllerConfig(version=version, frontends=list(self.frontends))


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
    """把已解析的原始结构（YAML 或数据库行组装出的 dict）转为 ServiceConfig。

    这是 YAML 与数据库两条来源的**唯一**汇合点：校验规则只写一遍，两种
    来源的拒绝行为与错误信息完全一致。source 用于错误信息前缀（"配置文件
    /etc/... " 或 "MySQL 配置库 ..."），让运维一眼看出是哪份配置写错了。
    """
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
    cfg = ServiceConfig()

    cfg.log_level = str(raw.get("log_level", DEFAULT_LOG_LEVEL)).strip().lower()

    tick_raw = raw.get("tick_interval_s", DEFAULT_TICK_INTERVAL_S)
    try:
        cfg.tick_interval_s = float(tick_raw)
    except (TypeError, ValueError):
        raise ValueError(
            f"tick_interval_s 必须是数字，当前值 {tick_raw!r}"
        ) from None

    cfg.haproxy = _parse_haproxy(raw.get("haproxy") or {})

    fronts_raw = raw.get("frontends") or []
    if not isinstance(fronts_raw, list):
        raise ValueError(
            f"frontends 必须是列表，实际是 {type(fronts_raw).__name__}")
    for i, f in enumerate(fronts_raw):
        if not isinstance(f, dict):
            raise ValueError(
                f"frontends[{i}] 结构错误：应为键值映射，"
                f"实际是 {type(f).__name__}")
        cfg.frontends.append(_parse_frontend(i, f))
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
    )


def _int_field(d: dict[str, Any], key: str, default: int, where: str) -> int:
    raw = d.get(key, default)
    if raw is None or raw == "":
        return default
    try:
        return int(raw)
    except (TypeError, ValueError):
        raise ValueError(f"{where}.{key} 必须是整数，当前值 {raw!r}") from None


def _parse_frontend(i: int, f: dict[str, Any]) -> model.FrontendConfig:
    where = f"frontends[{i}]"
    name = str(f.get("name", "") or "").strip()

    servers_raw = f.get("servers") or []
    if not isinstance(servers_raw, list):
        raise ValueError(
            f"{where}.servers 必须是列表，实际是 {type(servers_raw).__name__}")
    servers: list[model.ServerEntry] = []
    for j, s in enumerate(servers_raw):
        if not isinstance(s, dict):
            raise ValueError(
                f"{where}.servers[{j}] 结构错误：应为键值映射，"
                f"实际是 {type(s).__name__}")
        sw = f"{where}.servers[{j}]"
        servers.append(model.ServerEntry(
            name=str(s.get("name", "") or "").strip(),
            address=str(s.get("address", "") or "").strip(),
            port=_int_field(s, "port", 0, sw),
            weight=_int_field(s, "weight", 100, sw),
            check=bool(s.get("check", True)),
            check_inter_ms=_int_field(s, "check_inter_ms", 2000, sw),
        ))

    return model.FrontendConfig(
        name=name,
        bind_port=_int_field(f, "bind_port", 0, where),
        quota_bits_per_sec=_int_field(f, "quota_bps", 0, where),
        bind_address=str(f.get("bind_address", "") or "").strip(),
        mode=str(f.get("mode", "tcp") or "tcp").strip().lower(),
        maxconn=_int_field(f, "maxconn", 0, where),
        balance=str(f.get("balance", "roundrobin") or "roundrobin").strip().lower(),
        timeout_connect_ms=_int_field(f, "timeout_connect_ms", 5000, where),
        timeout_client_ms=_int_field(f, "timeout_client_ms", 50000, where),
        timeout_server_ms=_int_field(f, "timeout_server_ms", 50000, where),
        servers=servers,
    )


def _validate(cfg: ServiceConfig) -> None:
    """语义校验。任何一条不通过都抛 ValueError，信息里点名字段、当前值与原因。

    校验的严格程度是刻意的：这些值会被渲染进 haproxy.cfg 并 reload，
    一个坏值就能让整台机器的入口挂掉。宁可启动失败，也不要写出一份
    HAProxy 起不来的配置（虽然 enforcer 还有 `haproxy -c` 这道闸，但
    在这里拒绝能给出远比 HAProxy 报错更易懂的中文原因）。
    """
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

    if not cfg.frontends:
        # 没有受管 frontend 时服务无事可做：既没有要监控的对象，受管区块
        # 也会被生成成空的（等于删掉全部监听端口）。这多半是配置写漏了。
        raise ValueError(
            "frontends 不能为空——至少要有一个受管 frontend"
            "（监听端口 + 限额 + 后端服务器）")

    seen_names: dict[str, int] = {}
    seen_ports: dict[tuple[str, int], str] = {}
    for i, f in enumerate(cfg.frontends):
        where = f"frontends[{i}]"
        if not _NAME_RE.match(f.name):
            raise ValueError(
                f"{where}: name 非法（当前值 {f.name!r}）——只允许字母、数字、"
                f"点、下划线、连字符，长度 1-64。该名字会被原样写进 "
                f"haproxy.cfg，放宽字符集等于允许往配置文件注入任意指令")
        if f.name in seen_names:
            raise ValueError(
                f"{where}: frontend 名 {f.name!r} 与 "
                f"frontends[{seen_names[f.name]}] 重复——名字要与 stats 里的 "
                f"pxname 一一对应，重名会让采样数据张冠李戴")
        seen_names[f.name] = i

        if f.bind_port < 1 or f.bind_port > 65535:
            raise ValueError(
                f"{where} ({f.name}): bind_port 必须在 1-65535 范围内，"
                f"当前值 {f.bind_port!r}")
        key = (f.bind_address, f.bind_port)
        if key in seen_ports:
            raise ValueError(
                f"{where} ({f.name}): 监听地址端口 {f.bind_spec} 与 frontend "
                f"{seen_ports[key]!r} 冲突——两个 frontend 绑同一个端口会让 "
                f"HAProxy 启动失败")
        seen_ports[key] = f.name

        if f.quota_bits_per_sec <= 0:
            raise ValueError(
                f"{where} ({f.name}): quota_bps 必须为正数（当前值 "
                f"{f.quota_bits_per_sec!r}）——它既是写进 shared bwlim 的 "
                f"limit，也是超限告警基准；HAProxy 也不接受 limit 0")
        if f.quota_bytes_per_sec < 1:
            raise ValueError(
                f"{where} ({f.name}): quota_bps={f.quota_bits_per_sec} 太小"
                f"（不足 8 bit/s），换算成 bytes/s 后不足 1，HAProxy 会拒绝")

        if f.mode not in _MODES:
            raise ValueError(
                f"{where} ({f.name}): mode 取值非法 {f.mode!r}，"
                f"可选 {'/'.join(_MODES)}")
        if f.balance not in _BALANCE_ALGOS:
            raise ValueError(
                f"{where} ({f.name}): balance 取值非法 {f.balance!r}，"
                f"可选 {'/'.join(_BALANCE_ALGOS)}")
        if f.maxconn < 0:
            raise ValueError(
                f"{where} ({f.name}): maxconn 不能为负，当前值 {f.maxconn!r}")
        for tname in ("timeout_connect_ms", "timeout_client_ms",
                      "timeout_server_ms"):
            tv = getattr(f, tname)
            if tv <= 0:
                raise ValueError(
                    f"{where} ({f.name}): {tname} 必须为正数，当前值 {tv!r}")

        if not f.servers:
            raise ValueError(
                f"{where} ({f.name}): servers 不能为空——没有后端服务器的 "
                f"frontend 会把所有请求返回 503")
        seen_srv: dict[str, int] = {}
        for j, s in enumerate(f.servers):
            sw = f"{where}.servers[{j}]"
            if not _NAME_RE.match(s.name):
                raise ValueError(
                    f"{sw}: name 非法（当前值 {s.name!r}）——只允许字母、数字、"
                    f"点、下划线、连字符，长度 1-64")
            if s.name in seen_srv:
                raise ValueError(
                    f"{sw}: server 名 {s.name!r} 在 frontend {f.name!r} 内与 "
                    f"servers[{seen_srv[s.name]}] 重复")
            seen_srv[s.name] = j
            if not _ADDRESS_RE.match(s.address):
                raise ValueError(
                    f"{sw} ({s.name}): address 非法（当前值 {s.address!r}）——"
                    f"只允许字母、数字、点、冒号、下划线、连字符。该值会被"
                    f"原样写进 haproxy.cfg")
            if s.port < 1 or s.port > 65535:
                raise ValueError(
                    f"{sw} ({s.name}): port 必须在 1-65535 范围内，"
                    f"当前值 {s.port!r}")
            if s.weight < 0 or s.weight > 256:
                raise ValueError(
                    f"{sw} ({s.name}): weight 必须在 0-256 范围内"
                    f"（HAProxy 的取值范围），当前值 {s.weight!r}")
            if s.check_inter_ms <= 0:
                raise ValueError(
                    f"{sw} ({s.name}): check_inter_ms 必须为正数，"
                    f"当前值 {s.check_inter_ms!r}")


def _validate_haproxy(n: model.NodeConfig) -> None:
    """校验本机 HAProxy 的接线段（采样通道二选一）。"""
    if not _NAME_RE.match(n.name):
        raise ValueError(
            f"haproxy.name 非法（当前值 {n.name!r}）——只允许字母、数字、"
            f"点、下划线、连字符，长度 1-64")
    # 采样通道二选一：本机 unix socket（同机部署）或内网 TCP（远程观测）。
    # 两者都给会产生"到底连哪个"的二义（同机部署里 socket_path 与一个
    # 陈旧的 host:port 并存，是最容易采错对象的配错法），一个都不给则
    # 根本无法接线——两种情况都必须在启动时拦下。
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
