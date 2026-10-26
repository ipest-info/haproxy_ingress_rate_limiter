# rl_limiter.fleetconfig —— 总控制台（rl-fleet）配置的解析/校验/回写/热更。
#
# 总控的配置来源只有一个：**本地 YAML 文件**（rl-fleet -c 指定），没有
# 数据库。它提供三类信息：
#   1. **总量目标**：total_egress_mbps——全部节点出向带宽合计的目标值
#      （0 = 纯观察模式，只汇总不下发）。可经总控的写 API 热改；
#   2. **节点清单**（nodes 段）：每台节点的名称、控制台地址与写 API
#      令牌。改文件热生效（增删节点不用重启总控），也可经总控的写 API
#      （页面「节点管理」）添加/编辑/删除；
#   3. **运行参数**：轮询/分配周期、离线判定、分配算法参数（allocation
#      段）。周期类字段启动时定型，其余热生效。
#
# 回写（set_total_quota）与节点侧 configstore 同一套纪律：改之前整份
# 重新校验（写出去的配置一定能被重新加载）、临时文件 + os.replace 原子
# 替换、手写注释会丢。开放给写 API 的只有 total_egress_mbps 与 nodes
# 两处——算法参数与周期属于运维调参，编辑文件即可（热生效/重启）。

from __future__ import annotations

import asyncio
import logging
import os
import re
import tempfile
import urllib.parse
from dataclasses import dataclass, field
from typing import Any

import yaml

DEFAULT_LOG_LEVEL = "info"
# 轮询周期（秒）：页面实时性与几十台节点的请求开销的平衡点。
DEFAULT_POLL_INTERVAL_S = 2.0
# 分配周期（秒）：观测口径是节点的 10s 均值（mean10），且节点自己的
# 闭环还要几十秒才能把一次目标变更消化成入向限速——总控调得更快只会
# 和节点内环打架。校验强制 ≥ 10。
DEFAULT_ALLOC_INTERVAL_S = 15.0
# 单节点 HTTP 超时（秒）。
DEFAULT_NODE_TIMEOUT_S = 3.0
# 连续失败多少次判离线。
DEFAULT_OFFLINE_AFTER = 3

# 文件变化轮询周期（秒），与节点侧 cfgparse.watch 同节奏。
WATCH_INTERVAL_S = 5.0

# 节点名约束与节点侧 frontend 名一致：日志/页面里的名字可读且无歧义。
_NAME_RE = re.compile(r"^[A-Za-z0-9._-]{1,64}$")

_LOG_LEVELS = ("debug", "info", "warn", "error")


@dataclass
class AllocConfig:
    """allocation 段：按需动态分配算法的参数（整段热生效）。

    各参数在算法里的角色见 fleetgov 模块头注释。
    """

    # 保底池占比：每个可控节点保底 = min_share_ratio × 可分池 / 节点数。
    # 防饿死——零流量节点也留得住承接新流量的最小额度。
    min_share_ratio: float = 0.2
    # 需求信号的 EWMA 平滑系数（每个分配周期更新一次）。
    smooth_alpha: float = 0.3
    # 出向 ≥ press_ratio×自身配额 视为"被配额压着"（有未表达的需求）。
    press_ratio: float = 0.9
    # 被压节点的需求虚报系数：给上冲空间，防止"配额小→观测小→分得更
    # 小"的饿死锁定；逐轮迭代自然收敛。
    press_boost: float = 1.3
    # 需求低于该值（Mbps）视为空闲。
    idle_mbps: float = 1.0
    # 写抑制：相对变化低于该比例不下发（节点每次写都整份重写 YAML）。
    apply_epsilon: float = 0.05
    # 写抑制：绝对变化低于该值（Mbps）也不下发。
    min_delta_mbps: float = 1.0
    # 单节点下发下限（Mbps）。**绝不下发 ≤0**：0 在节点语义里是"不限"，
    # 作为份额下发等于放开那台节点。
    min_quota_mbps: float = 0.5


@dataclass
class FleetNode:
    """nodes 段的一项：一台受管节点的接线。"""

    name: str = ""
    # 节点控制台地址（轮询与下发都走它），http(s)://host:port。
    url: str = ""
    # 节点写 API 的令牌（= 该节点的 RL_API_TOKEN）。空 = 只读观测，
    # 该节点不可控（加载时 WARN 提示）。
    token: str = ""
    # 页面跳转地址（内外网地址不同时填；缺省 = url）。
    console_url: str = ""


@dataclass
class FleetConfig:
    """一份完整的总控配置。"""

    log_level: str = DEFAULT_LOG_LEVEL
    # 总流出带宽目标（Mbps；0 = 纯观察，不下发）。热生效，可经写 API 改。
    total_egress_mbps: float = 0.0
    poll_interval_s: float = DEFAULT_POLL_INTERVAL_S
    alloc_interval_s: float = DEFAULT_ALLOC_INTERVAL_S
    node_timeout_s: float = DEFAULT_NODE_TIMEOUT_S
    offline_after: int = DEFAULT_OFFLINE_AFTER
    alloc: AllocConfig = field(default_factory=AllocConfig)
    nodes: list = field(default_factory=list)


def load(path: str) -> FleetConfig:
    """读取 YAML：解析、补默认值、校验，全部通过后返回。

    解析/校验失败抛 ValueError（带路径与具体原因）；I/O 错误按原生
    OSError 抛出。
    """
    with open(path, "r", encoding="utf-8") as f:
        text = f.read()
    try:
        raw = yaml.safe_load(text)
    except yaml.YAMLError as e:
        raise ValueError(f"配置文件 {path} 不是合法 YAML: {e}") from None
    return from_raw(raw, source=f"配置文件 {path}")


def from_raw(raw: Any, source: str) -> FleetConfig:
    if raw is None:
        raw = {}
    if not isinstance(raw, dict):
        raise ValueError(
            f"{source} 无效: 顶层必须是键值映射，实际是 {type(raw).__name__}")
    try:
        cfg = _parse(raw)
        _validate(cfg)
    except ValueError as e:
        raise ValueError(f"{source} 无效: {e}") from None
    return cfg


def _num(raw: dict, key: str, default) -> float:
    v = raw.get(key, default)
    if isinstance(v, bool) or not isinstance(v, (int, float)):
        raise ValueError(f"{key} 必须是数字，当前值 {v!r}")
    return float(v)


def _parse(raw: dict[str, Any]) -> FleetConfig:
    known = {"log_level", "total_egress_mbps", "poll_interval_s",
             "alloc_interval_s", "node_timeout_s", "offline_after",
             "allocation", "nodes"}
    unknown = set(raw) - known
    if unknown:
        raise ValueError(
            f"有不认识的键 {sorted(unknown)}，可用：{sorted(known)}")

    cfg = FleetConfig()
    cfg.log_level = str(raw.get("log_level", DEFAULT_LOG_LEVEL)).strip().lower()
    cfg.total_egress_mbps = _num(raw, "total_egress_mbps", 0.0)
    cfg.poll_interval_s = _num(raw, "poll_interval_s",
                               DEFAULT_POLL_INTERVAL_S)
    cfg.alloc_interval_s = _num(raw, "alloc_interval_s",
                                DEFAULT_ALLOC_INTERVAL_S)
    cfg.node_timeout_s = _num(raw, "node_timeout_s", DEFAULT_NODE_TIMEOUT_S)
    off_raw = raw.get("offline_after", DEFAULT_OFFLINE_AFTER)
    if isinstance(off_raw, bool) or not isinstance(off_raw, int):
        raise ValueError(f"offline_after 必须是整数，当前值 {off_raw!r}")
    cfg.offline_after = off_raw

    a_raw = raw.get("allocation") or {}
    if not isinstance(a_raw, dict):
        raise ValueError(
            f"allocation 必须是键值映射，实际是 {type(a_raw).__name__}")
    a = AllocConfig()
    a_known = {"min_share_ratio", "smooth_alpha", "press_ratio",
               "press_boost", "idle_mbps", "apply_epsilon",
               "min_delta_mbps", "min_quota_mbps"}
    a_unknown = set(a_raw) - a_known
    if a_unknown:
        raise ValueError(
            f"allocation 有不认识的键 {sorted(a_unknown)}，"
            f"可用：{sorted(a_known)}")
    for k in a_known:
        if k in a_raw:
            setattr(a, k, _num(a_raw, k, getattr(a, k)))
    cfg.alloc = a

    nodes_raw = raw.get("nodes") or []
    if not isinstance(nodes_raw, list):
        raise ValueError(
            f"nodes 必须是列表，实际是 {type(nodes_raw).__name__}")
    for i, n_raw in enumerate(nodes_raw):
        if not isinstance(n_raw, dict):
            raise ValueError(f"nodes[{i}] 必须是映射（name/url/token），"
                             f"实际是 {type(n_raw).__name__}")
        n_known = {"name", "url", "token", "console_url"}
        n_unknown = set(n_raw) - n_known
        if n_unknown:
            raise ValueError(
                f"nodes[{i}] 有不认识的键 {sorted(n_unknown)}，"
                f"可用：{sorted(n_known)}")
        n = FleetNode(
            name=str(n_raw.get("name", "") or "").strip(),
            url=str(n_raw.get("url", "") or "").strip().rstrip("/"),
            token=str(n_raw.get("token", "") or "").strip(),
            console_url=str(n_raw.get("console_url", "") or "").strip(),
        )
        cfg.nodes.append(n)
    return cfg


def _validate(cfg: FleetConfig) -> None:
    if cfg.log_level not in _LOG_LEVELS:
        raise ValueError(
            f"log_level 必须是 {_LOG_LEVELS} 之一，当前值 {cfg.log_level!r}")
    if cfg.total_egress_mbps < 0:
        raise ValueError(
            f"total_egress_mbps 必须 ≥ 0（0 = 纯观察不下发），"
            f"当前值 {cfg.total_egress_mbps:g}")
    if cfg.poll_interval_s < 1:
        raise ValueError(
            f"poll_interval_s 必须 ≥ 1，当前值 {cfg.poll_interval_s:g}")
    if cfg.alloc_interval_s < 10:
        raise ValueError(
            f"alloc_interval_s 必须 ≥ 10（观测口径是节点的 10s 均值，且"
            f"节点内环还要时间消化目标变更——分配得更快只会追着噪声跑、"
            f"和节点闭环打架），当前值 {cfg.alloc_interval_s:g}")
    if cfg.node_timeout_s <= 0:
        raise ValueError(
            f"node_timeout_s 必须 > 0，当前值 {cfg.node_timeout_s:g}")
    if cfg.offline_after < 1:
        raise ValueError(
            f"offline_after 必须 ≥ 1，当前值 {cfg.offline_after}")

    a = cfg.alloc
    if not 0 < a.min_share_ratio <= 0.5:
        raise ValueError(
            f"allocation.min_share_ratio 必须在 (0, 0.5] 区间（保底池"
            f"超过一半，按需分配就名存实亡），当前值 {a.min_share_ratio:g}")
    if not 0 < a.smooth_alpha <= 1:
        raise ValueError(
            f"allocation.smooth_alpha 必须在 (0, 1] 区间，"
            f"当前值 {a.smooth_alpha:g}")
    if not 0 < a.press_ratio <= 1:
        raise ValueError(
            f"allocation.press_ratio 必须在 (0, 1] 区间，"
            f"当前值 {a.press_ratio:g}")
    if a.press_boost < 1:
        raise ValueError(
            f"allocation.press_boost 必须 ≥ 1，当前值 {a.press_boost:g}")
    for k in ("idle_mbps", "apply_epsilon", "min_delta_mbps"):
        if getattr(a, k) < 0:
            raise ValueError(
                f"allocation.{k} 必须 ≥ 0，当前值 {getattr(a, k):g}")
    if a.min_quota_mbps <= 0:
        raise ValueError(
            f"allocation.min_quota_mbps 必须 > 0（0 在节点语义里是"
            f"\"不限\"，绝不能作为份额下发），当前值 {a.min_quota_mbps:g}")

    seen: set = set()
    for i, n in enumerate(cfg.nodes):
        if not _NAME_RE.match(n.name or ""):
            raise ValueError(
                f"nodes[{i}].name 非法——应为 1~64 位字母/数字/._-，"
                f"当前值 {n.name!r}")
        if n.name in seen:
            raise ValueError(f"nodes 里节点名重复：{n.name!r}")
        seen.add(n.name)
        if (not (n.url.startswith("http://") or n.url.startswith("https://"))
                or not urllib.parse.urlsplit(n.url).netloc):
            raise ValueError(
                f"nodes[{i}]({n.name}).url 必须是 http(s)://<主机>:<端口> "
                f"地址（节点控制台），当前值 {n.url!r}")


# ---------------------------------------------------------------------------
# 回写（写 API 能改的只有 total_egress_mbps 与 nodes）
# ---------------------------------------------------------------------------

_HEADER = (
    "# 本文件由 rl-fleet 的写 API 重写过：手写注释不会保留（YAML 数据\n"
    "# 往返的限制）。字段含义见 deploy/config/fleet.example.yaml。\n"
)


class FleetConfigError(ValueError):
    """回写失败（配置不合法 / 文件读写问题）。信息面向 API 调用者。"""


class NodeExistsError(FleetConfigError):
    """添加节点时重名（不覆盖已登记的节点——要改用编辑）。"""


class NodeNotFoundError(FleetConfigError):
    """编辑/删除的节点不在清单里。"""


def _read_raw(path: str) -> dict:
    """读出当前 YAML 的原始映射（回写的起点）。"""
    try:
        with open(path, "r", encoding="utf-8") as f:
            raw = yaml.safe_load(f.read())
    except OSError as e:
        raise FleetConfigError(f"读不到配置文件 {path}：{e}") from None
    except yaml.YAMLError as e:
        raise FleetConfigError(
            f"配置文件 {path} 不是合法 YAML，拒绝回写（请先手工修复）：{e}"
        ) from None
    if raw is None:
        raw = {}
    if not isinstance(raw, dict):
        raise FleetConfigError(
            f"配置文件 {path} 顶层不是键值映射，拒绝回写（请先手工修复）")
    return raw


def _write_raw(path: str, raw: dict) -> None:
    """整份校验后原子替换（与节点侧 configstore 同一套纪律）。"""
    # 与启动完全相同的校验链：能写出去的配置一定能被重新加载。
    from_raw(raw, source="回写后的配置")
    text = _HEADER + yaml.safe_dump(
        raw, allow_unicode=True, sort_keys=False, default_flow_style=False)
    d = os.path.dirname(os.path.abspath(path)) or "."
    fd, tmp = tempfile.mkstemp(dir=d, prefix=".fleet-", suffix=".tmp")
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
        raise FleetConfigError(f"写配置文件 {path} 失败：{e}") from None


def set_total_quota(path: str, total_mbps: float) -> None:
    """设置总流出带宽（Mbps；0 = 停止分配，纯观察）。"""
    raw = _read_raw(path)
    if total_mbps:
        raw["total_egress_mbps"] = total_mbps
    else:
        raw.pop("total_egress_mbps", None)
    _write_raw(path, raw)


# 节点项里写 API 可设的字段（name 只在添加时给，之后不可改——改名 =
# 删了重加，节点状态与日志都以名字为键）。
NODE_FIELDS = ("url", "token", "console_url")


def _node_value(key: str, v: Any) -> str:
    if v is None:
        return ""
    if not isinstance(v, str):
        raise FleetConfigError(f"{key} 必须是字符串，当前值 {v!r}")
    v = v.strip()
    # 令牌随请求头发给节点：空白/控制字符要么被拒、要么拼出畸形请求头。
    if key == "token" and any(c.isspace() or ord(c) < 32 for c in v):
        raise FleetConfigError("token 不能含空白或控制字符")
    return v


def _raw_nodes(raw: dict) -> list:
    nodes = raw.get("nodes")
    if nodes is None:
        nodes = []
    if not isinstance(nodes, list):
        raise FleetConfigError(
            "配置文件里 nodes 不是列表，拒绝回写（请先手工修复）")
    raw["nodes"] = nodes
    return nodes


def _find(nodes: list, name: str) -> int:
    for i, n in enumerate(nodes):
        if isinstance(n, dict) and str(n.get("name", "")).strip() == name:
            return i
    return -1


def add_node(path: str, name: str, fields: dict) -> None:
    """登记一台新节点（追加到清单末尾）。重名抛 NodeExistsError——绝不
    静默覆盖已登记节点的接线与令牌。"""
    raw = _read_raw(path)
    nodes = _raw_nodes(raw)
    name = _node_value("name", name)
    if _find(nodes, name) >= 0:
        raise NodeExistsError(f"节点 {name!r} 已在清单里（要改请用编辑）")
    item: dict[str, str] = {"name": name}
    for k in NODE_FIELDS:
        v = _node_value(k, fields.get(k))
        if v or k != "console_url":            # console_url 空就不写
            item[k] = v
    nodes.append(item)
    _write_raw(path, raw)


def update_node(path: str, name: str, fields: dict) -> None:
    """修改已登记节点：fields 里出现的键才改（token 给空串 = 清除令牌，
    该节点转只读观测）；没出现的键保持原值。"""
    raw = _read_raw(path)
    nodes = _raw_nodes(raw)
    i = _find(nodes, name)
    if i < 0:
        raise NodeNotFoundError(f"节点 {name!r} 不在清单里")
    item = dict(nodes[i])
    for k in NODE_FIELDS:
        if k in fields:
            item[k] = _node_value(k, fields[k])
    nodes[i] = item
    _write_raw(path, raw)


def remove_node(path: str, name: str) -> None:
    """从清单删除节点。只是停止轮询与下发：该节点的实例配额保持现状
    （移除 ≠ 释放，见 fleetpoll.set_nodes）。"""
    raw = _read_raw(path)
    nodes = _raw_nodes(raw)
    i = _find(nodes, name)
    if i < 0:
        raise NodeNotFoundError(f"节点 {name!r} 不在清单里")
    del nodes[i]
    _write_raw(path, raw)


# ---------------------------------------------------------------------------
# 热更轮询
# ---------------------------------------------------------------------------

def _hot_view(cfg: FleetConfig):
    """热生效字段的规范化视图（变没变比较它）。"""
    return (
        cfg.total_egress_mbps,
        cfg.offline_after,
        tuple(sorted(
            (n.name, n.url, n.token, n.console_url) for n in cfg.nodes)),
        (cfg.alloc.min_share_ratio, cfg.alloc.smooth_alpha,
         cfg.alloc.press_ratio, cfg.alloc.press_boost, cfg.alloc.idle_mbps,
         cfg.alloc.apply_epsilon, cfg.alloc.min_delta_mbps,
         cfg.alloc.min_quota_mbps),
    )


def _fixed_view(cfg: FleetConfig):
    """启动定型字段的视图（变了只提示重启，不应用）。"""
    return (cfg.log_level, cfg.poll_interval_s, cfg.alloc_interval_s,
            cfg.node_timeout_s)


async def watch(path: str,
                on_change,
                log: logging.Logger,
                poke: "asyncio.Event | None" = None,
                interval_s: float = WATCH_INTERVAL_S) -> None:
    """常驻任务：轮询 fleet YAML，热生效字段变化即回调 on_change(cfg)。

    读不到/解析失败时 fail-static：沿用当前配置继续运行并 WARN，下一轮
    再试。poke（写 API 改完 set）能即时触发重读，不用等轮询周期。
    """
    last_hot = None
    last_fixed = None
    while True:
        if poke is None:
            await asyncio.sleep(interval_s)
        else:
            try:
                await asyncio.wait_for(poke.wait(), timeout=interval_s)
            except asyncio.TimeoutError:
                pass
            poke.clear()
        try:
            cfg = load(path)
        except Exception as e:
            log.warning(
                "重读总控配置失败，保留当前配置继续运行（fail-static），"
                "下一轮再试 yaml=%s err=%s", path, e)
            continue
        fixed = _fixed_view(cfg)
        if last_fixed is None:
            last_fixed = fixed
        elif fixed != last_fixed:
            last_fixed = fixed
            log.warning(
                "检测到 YAML 里运行参数发生变化（log_level、轮询/分配"
                "周期、节点超时）：这些字段在进程启动时定型，热更新不"
                "生效，请重启 rl-fleet")
        hot = _hot_view(cfg)
        if hot == last_hot:
            continue
        last_hot = hot
        try:
            on_change(cfg)
        except Exception as e:                    # 回调异常不该杀掉轮询
            log.error("应用总控配置变更失败 err=%s", e)
