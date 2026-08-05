# rl_limiter.aggconfig —— hap-agg（监控聚合）的 YAML 配置。
#
# 与 rl-limiter 的 config.py 是两份独立的契约：聚合器不接线本机
# HAProxy、不做限速，配置只有目标清单 + 少量运行参数：
#
#   log_level: info
#   poll_interval_s: 1.0        # 采样节拍（秒）
#   timeout_ms: 500             # 单目标单命令超时
#   targets:                    # 两种写法等价
#     - 10.0.0.11:9999
#     - name: sg-02
#       addr: 10.0.0.12:9999
#
# 批量导入 API 修改目标清单后**回写本文件**（原子替换，做法与
# configstore 相同；手写注释会丢，头部注明）。运行期不轮询本文件——
# 目标集的运行态以进程内存为准（写 API 是唯一的运行期入口），手工编辑
# 文件要重启才生效；这一点与 rl-limiter 的 quotas 热更新刻意不同：
# 聚合器没有"配置与数据面漂移"的风险，双写入口反而引入合并问题。

from __future__ import annotations

import os
import tempfile
from dataclasses import dataclass, field
from typing import Any

import yaml

from .agg import Target, TargetError, parse_target_line

DEFAULT_LOG_LEVEL = "info"
DEFAULT_POLL_INTERVAL_S = 1.0
DEFAULT_TIMEOUT_MS = 500

_LOG_LEVELS = ("debug", "info", "warn", "error")

_HEADER = (
    "# 本文件由 hap-agg 的批量导入 API 重写过：手写注释不会保留（YAML\n"
    "# 数据往返的限制）。字段说明见 deploy/config/hap-agg.example.yaml。\n"
)


@dataclass
class AggConfig:
    log_level: str = DEFAULT_LOG_LEVEL
    poll_interval_s: float = DEFAULT_POLL_INTERVAL_S
    timeout_s: float = DEFAULT_TIMEOUT_MS / 1000.0
    targets: list[Target] = field(default_factory=list)


def load(path: str) -> AggConfig:
    """读取并校验配置。解析/校验失败抛 ValueError（带文件路径与原因），
    I/O 错误按原生 OSError 抛出。"""
    with open(path, "r", encoding="utf-8") as f:
        text = f.read()
    try:
        raw = yaml.safe_load(text)
    except yaml.YAMLError as e:
        raise ValueError(f"配置文件 {path} 解析失败（不是合法的 YAML）: {e}") from e
    try:
        return from_raw(raw)
    except ValueError as e:
        raise ValueError(f"配置文件 {path} 无效: {e}") from None


def from_raw(raw: Any) -> AggConfig:
    if raw is None:
        raw = {}
    if not isinstance(raw, dict):
        raise ValueError(f"顶层必须是键值映射，实际是 {type(raw).__name__}")

    cfg = AggConfig()
    cfg.log_level = str(raw.get("log_level", DEFAULT_LOG_LEVEL)).strip().lower()
    if cfg.log_level not in _LOG_LEVELS:
        raise ValueError(f"log_level 取值非法: {cfg.log_level!r}，"
                         f"可选 {'/'.join(_LOG_LEVELS)}")

    poll_raw = raw.get("poll_interval_s", DEFAULT_POLL_INTERVAL_S)
    try:
        cfg.poll_interval_s = float(poll_raw)
    except (TypeError, ValueError):
        raise ValueError(f"poll_interval_s 必须是数字，当前值 {poll_raw!r}") from None
    if not (0.2 <= cfg.poll_interval_s <= 60):
        raise ValueError(
            f"poll_interval_s 必须在 0.2-60 秒之间（当前值 "
            f"{cfg.poll_interval_s}）——太密会打爆目标的 stats socket，"
            f"太疏则速率曲线失去意义")

    timeout_raw = raw.get("timeout_ms", DEFAULT_TIMEOUT_MS)
    try:
        timeout_ms = int(timeout_raw)
    except (TypeError, ValueError):
        raise ValueError(f"timeout_ms 必须是整数，当前值 {timeout_raw!r}") from None
    if timeout_ms <= 0:
        timeout_ms = DEFAULT_TIMEOUT_MS
    cfg.timeout_s = timeout_ms / 1000.0

    cfg.targets = _parse_targets(raw.get("targets") or [], cfg.timeout_s)
    return cfg


def _parse_targets(raw: Any, timeout_s: float) -> list[Target]:
    if not isinstance(raw, list):
        raise ValueError(
            f"targets 必须是列表（每项 'IP:port' 或 {{name, addr}}），"
            f"实际是 {type(raw).__name__}")
    targets: list[Target] = []
    seen_names: set[str] = set()
    seen_addrs: set[str] = set()
    for i, item in enumerate(raw):
        if isinstance(item, str):
            line = item
        elif isinstance(item, dict):
            addr = str(item.get("addr", "") or "").strip()
            name = str(item.get("name", "") or "").strip()
            if not addr:
                raise ValueError(f"targets[{i}] 缺少 addr 字段")
            line = f"{name} {addr}" if name else addr
        else:
            raise ValueError(
                f"targets[{i}] 必须是 'IP:port' 字符串或 {{name, addr}} "
                f"映射，实际是 {type(item).__name__}")
        try:
            name, host, port = parse_target_line(line)
        except TargetError as e:
            raise ValueError(f"targets[{i}]: {e}") from None
        t = Target(name=name, host=host, port=port, timeout_s=timeout_s)
        if t.name in seen_names:
            raise ValueError(f"targets[{i}]: 目标名重复：{t.name!r}")
        if t.addr in seen_addrs:
            raise ValueError(f"targets[{i}]: 目标地址重复：{t.addr}")
        seen_names.add(t.name)
        seen_addrs.add(t.addr)
        targets.append(t)
    return targets


def save_targets(path: str, targets: list[Target]) -> None:
    """把目标清单回写到 YAML（其余字段原样保留，原子替换）。

    写法与 configstore 相同：改内存结构 → 整份 from_raw 校验 → 临时
    文件 + os.replace。名字等于地址的目标写成短形式（一行字符串）。
    """
    try:
        with open(path, "r", encoding="utf-8") as f:
            raw = yaml.safe_load(f.read())
    except OSError as e:
        raise ValueError(f"读不到配置文件 {path}：{e}") from None
    except yaml.YAMLError as e:
        raise ValueError(
            f"配置文件 {path} 不是合法 YAML，拒绝回写（请先手工修复）：{e}"
        ) from None
    if raw is None:
        raw = {}
    if not isinstance(raw, dict):
        raise ValueError(f"配置文件 {path} 顶层不是键值映射，拒绝回写")

    entries: list[Any] = []
    for t in targets:
        if t.name == t.addr:
            entries.append(t.addr)
        else:
            entries.append({"name": t.name, "addr": t.addr})
    raw["targets"] = entries
    from_raw(raw)                            # 回写前整份校验

    text = _HEADER + yaml.safe_dump(
        raw, allow_unicode=True, sort_keys=False, default_flow_style=False)
    d = os.path.dirname(os.path.abspath(path)) or "."
    fd, tmp = tempfile.mkstemp(dir=d, prefix=".hap-agg-", suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write(text)
            f.flush()
            os.fsync(f.fileno())
        try:
            os.chmod(tmp, os.stat(path).st_mode & 0o7777)
        except OSError:
            pass
        os.replace(tmp, path)
    except OSError as e:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise ValueError(f"写配置文件 {path} 失败：{e}") from None
