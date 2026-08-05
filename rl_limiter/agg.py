# rl_limiter.agg —— 多台 HAProxy 的监控聚合采样器（hap-agg 的核心）。
#
# 与 rl-limiter 主程序的关系：**只共享客户端与解析层**（haproxy.py 的
# RuntimeClient / parse_show_stat / parse_show_info），定位完全不同——
# rl-limiter 与 HAProxy 同机、做限速；hap-agg 是**纯只读的跨机观测**，
# 经各台机器 haproxy.cfg 里暴露的内网 TCP stats socket
# （`stats socket ipv4@*:9999 level admin expose-fd listeners` 之类）
# 批量采样，把 N 台 HAProxy 合并成一个视图。它不写任何 HAProxy 状态、
# 不做 tc，也不要求目标机器装任何东西。
#
# ## 采样与差分
#
# 每个目标每拍并发地跑一次 `show stat`（frontend 行）+ `show info`
# （进程级），速率类指标由相邻两拍的累计计数器差分得出：
#
#   - 差分为负（HAProxy reload/重启导致计数器清零）按 0 处理，下一拍
#     基线即恢复——一拍的空洞比一根荒谬的负速率曲线诚实；
#   - 单个目标失败不影响其它目标（gather 各自兜错）；连续失败 ≥ 3 拍
#     标记 degraded，其行仍出现在视图里（带错误原因），但**不计入聚合
#     合计**——陈旧数字混进总数会让总带宽虚高且无从发现。
#
# ## 聚合口径
#
# 快照三层：targets（每台一行）、total（所有健康目标合计）、frontends
# （按 frontend 名跨机合并——多台机器上同名的 frontend 视为同一个业务
# 入口，这正是"多台 HAProxy 水平扩容、配置相同"的常见形态）。
# 所有速率一律 bytes/s（应用层口径，与 rl-limiter 一致）。

from __future__ import annotations

import asyncio
import logging
import re
import time
from dataclasses import dataclass, field
from typing import Any

from . import model
from .haproxy import RuntimeClient

# 连续失败多少拍后标记 degraded（并从聚合合计中剔除）。
DEGRADED_AFTER_FAILURES = 3

# 目标名的约束：与 quotas 键一致（可读、无歧义、能进 URL）。
_NAME_RE = re.compile(r"^[A-Za-z0-9._:\[\]-]{1,128}$")


class TargetError(ValueError):
    """目标定义非法（地址格式/端口越界/名字冲突）。信息面向使用者。"""


@dataclass(**model.SLOTS)
class Target:
    """一台被聚合的 HAProxy（内网 TCP stats socket 端点）。"""

    name: str            # 展示名；批量导入不给名字时取 addr 本身
    host: str
    port: int
    timeout_s: float = 0.5

    @property
    def addr(self) -> str:
        return f"{self.host}:{self.port}"


def parse_target_line(line: str) -> tuple[str, str, int]:
    """解析一行目标定义，返回 (name, host, port)。

    接受两种写法（批量导入的文本每行一条，# 起头为注释）：

        10.0.0.11:9999            # 名字缺省 = 地址本身
        sg-01 10.0.0.12:9999      # 显式命名（空白分隔）

    host:port 按**最后一个冒号**拆分，IPv6 字面量请写成 [::1]:9999。
    """
    s = line.strip()
    parts = s.split()
    if len(parts) == 1:
        name, addr = "", parts[0]
    elif len(parts) == 2:
        name, addr = parts[0], parts[1]
    else:
        raise TargetError(
            f"目标行格式不对：{line!r}——应为 'IP:port' 或 '名字 IP:port'")
    host, sep, port_s = addr.rpartition(":")
    if not sep or not host:
        raise TargetError(
            f"目标地址缺少端口：{addr!r}——应为 IP:port（例如 10.0.0.11:9999）")
    if host.startswith("[") and host.endswith("]"):
        host = host[1:-1]              # IPv6 字面量 [::1]:9999
    try:
        port = int(port_s)
    except ValueError:
        raise TargetError(f"目标端口不是数字：{addr!r}") from None
    if not (1 <= port <= 65535):
        raise TargetError(f"目标端口越界：{addr!r}（应在 1-65535）")
    # 名字缺省取地址**原样**（IPv6 保留方括号写法，与导入文本一致）。
    name = name or addr
    if not _NAME_RE.match(name):
        raise TargetError(
            f"目标名 {name!r} 非法——只允许字母、数字、点、冒号、下划线、"
            f"连字符、方括号，长度 1-128")
    return name, host, port


def parse_targets_text(text: str,
                       timeout_s: float = 0.5) -> list[Target]:
    """解析批量导入文本（每行一条，空行/# 注释跳过），名字与地址都查重。

    这是批量导入 API 与 YAML 配置共用的入口——两条路解析出的目标
    完全一致。
    """
    targets: list[Target] = []
    seen_names: set[str] = set()
    seen_addrs: set[str] = set()
    for raw_line in text.split("\n"):
        line = raw_line.split("#", 1)[0].strip()
        if not line:
            continue
        name, host, port = parse_target_line(line)
        t = Target(name=name, host=host, port=port, timeout_s=timeout_s)
        if t.name in seen_names:
            raise TargetError(f"目标名重复：{t.name!r}")
        if t.addr in seen_addrs:
            raise TargetError(f"目标地址重复：{t.addr}")
        seen_names.add(t.name)
        seen_addrs.add(t.addr)
        targets.append(t)
    return targets


def _diff(cur: int, prev: int, dt: float) -> float:
    """计数器差分 → 每秒速率。负差分（reload 清零）按 0 处理。"""
    if dt <= 0:
        return 0.0
    d = cur - prev
    return d / dt if d > 0 else 0.0


class TargetSampler:
    """单个目标的采样与差分状态。"""

    def __init__(self, target: Target, log: logging.Logger,
                 client: "RuntimeClient | None" = None):
        self.target = target
        self._log = log
        self._client = client if client is not None else RuntimeClient(
            target.host, target.port, target.timeout_s, log)
        self._prev_ts: float = 0.0
        self._prev_fes: dict[str, model.FrontendStat] = {}
        self._failures = 0
        # 最近一次成功的视图：失败拍里带着 degraded 标记继续展示（陈旧
        # 总比一片空白好），但绝不计入聚合合计。
        self._last: dict[str, Any] | None = None

    @property
    def failures(self) -> int:
        return self._failures

    async def sample(self, now: float) -> dict[str, Any]:
        """采一拍，返回该目标的视图（含 ok/degraded/error 状态字段）。"""
        try:
            fes, info = await asyncio.gather(self._client.show_stat(),
                                             self._client.show_info())
        except asyncio.CancelledError:
            raise
        except Exception as e:
            self._failures += 1
            degraded = self._failures >= DEGRADED_AFTER_FAILURES
            if degraded and self._failures == DEGRADED_AFTER_FAILURES:
                self._log.warning(
                    "聚合目标连续 %d 拍采样失败，标记 degraded（不再计入"
                    "合计） target=%s addr=%s err=%s",
                    self._failures, self.target.name, self.target.addr, e)
            view = dict(self._last) if self._last else self._empty_view()
            view.update(ok=False, degraded=degraded, error=str(e),
                        consec_failures=self._failures)
            return view

        if self._failures >= DEGRADED_AFTER_FAILURES:
            self._log.info("聚合目标采样已恢复 target=%s addr=%s",
                           self.target.name, self.target.addr)
        self._failures = 0

        dt = now - self._prev_ts if self._prev_ts else 0.0
        prev = self._prev_fes
        fe_views: dict[str, Any] = {}
        t_out = t_in = t_new = t_denied = 0.0
        t_conn = 0
        for fe in fes:
            p = prev.get(fe.name)
            if p is not None and dt > 0:
                rate_out = _diff(fe.bytes_out, p.bytes_out, dt)
                rate_in = _diff(fe.bytes_in, p.bytes_in, dt)
                conn_new = _diff(fe.conn_tot, p.conn_tot, dt)
                denied = _diff(
                    fe.denied_conn + fe.denied_sess
                    + fe.denied_req + fe.denied_resp,
                    p.denied_conn + p.denied_sess
                    + p.denied_req + p.denied_resp, dt)
            else:
                rate_out = rate_in = conn_new = denied = 0.0
            fe_views[fe.name] = {
                "rate_out_bytes_per_s": rate_out,
                "rate_in_bytes_per_s": rate_in,
                "conn": fe.conn_cur,
                "conn_new_ps": conn_new,
                "denied_ps": denied,
                "mode": fe.mode,
            }
            t_out += rate_out
            t_in += rate_in
            t_new += conn_new
            t_denied += denied
            t_conn += fe.conn_cur
        self._prev_ts = now
        self._prev_fes = {fe.name: fe for fe in fes}

        view = {
            "name": self.target.name,
            "addr": self.target.addr,
            "ok": True,
            "degraded": False,
            "error": "",
            "consec_failures": 0,
            # --- show info（进程级）---
            "conn": info.curr_conns,        # 整机并发（含非 frontend 的连接）
            "max_conn": info.max_conn,
            "idle_pct": info.idle_pct,
            "sess_rate": info.sess_rate,
            "uptime_s": info.uptime_s,
            # --- Σ frontend（应用层口径）---
            "fe_conn": t_conn,
            "rate_in_bytes_per_s": t_in,
            "rate_out_bytes_per_s": t_out,
            "conn_new_ps": t_new,
            "denied_ps": t_denied,
            "frontends": fe_views,
        }
        self._last = view
        return view

    def _empty_view(self) -> dict[str, Any]:
        return {
            "name": self.target.name, "addr": self.target.addr,
            "conn": 0, "max_conn": 0, "idle_pct": 0, "sess_rate": 0,
            "uptime_s": 0, "fe_conn": 0,
            "rate_in_bytes_per_s": 0.0, "rate_out_bytes_per_s": 0.0,
            "conn_new_ps": 0.0, "denied_ps": 0.0, "frontends": {},
        }


class Aggregator:
    """N 个目标的并发采样 + 合并视图。目标集可在运行期增删（写 API）。"""

    def __init__(self, targets: list[Target], log: logging.Logger,
                 client_factory=None):
        self._log = log
        # 注入点：测试用假客户端。生产为 None → TargetSampler 自建。
        self._client_factory = client_factory
        self._samplers: dict[str, TargetSampler] = {}
        for t in targets:
            self._add(t)

    def _add(self, t: Target) -> None:
        client = (self._client_factory(t)
                  if self._client_factory is not None else None)
        self._samplers[t.name] = TargetSampler(t, self._log, client=client)

    # ---- 目标集管理（写 API 调用；单事件循环内，无需加锁）----

    def targets(self) -> list[Target]:
        return [s.target for s in self._samplers.values()]

    def add_targets(self, new: list[Target]) -> list[str]:
        """并入一批目标，返回真正新增的名字（同名同址的重复项跳过）。

        名字相同但地址不同视为冲突（抛 TargetError）——静默替换会让
        运维以为旧地址还在被监控。
        """
        cur_by_name = {t.name: t for t in self.targets()}
        cur_addrs = {t.addr for t in self.targets()}
        added: list[str] = []
        for t in new:
            old = cur_by_name.get(t.name)
            if old is not None:
                if old.addr != t.addr:
                    raise TargetError(
                        f"目标名 {t.name!r} 已存在且地址不同"
                        f"（现 {old.addr}，导入 {t.addr}）——先删除旧目标"
                        f"或换个名字")
                continue                     # 同名同址：幂等跳过
            if t.addr in cur_addrs:
                continue                     # 同址不同名：视为已导入过
            self._add(t)
            cur_by_name[t.name] = t
            cur_addrs.add(t.addr)
            added.append(t.name)
        return added

    def remove_target(self, name: str) -> bool:
        return self._samplers.pop(name, None) is not None

    # ---- 采样与合并 ----

    async def tick(self, now: float | None = None) -> dict[str, Any]:
        """并发采样全部目标并返回合并快照。"""
        now = time.time() if now is None else now
        samplers = list(self._samplers.values())
        views = await asyncio.gather(*(s.sample(now) for s in samplers))
        targets = {v["name"]: v for v in views}

        total = {
            "targets": len(views),
            "targets_ok": sum(1 for v in views if v["ok"]),
            "conn": 0, "fe_conn": 0, "max_conn": 0,
            "rate_in_bytes_per_s": 0.0, "rate_out_bytes_per_s": 0.0,
            "conn_new_ps": 0.0, "denied_ps": 0.0, "sess_rate": 0,
        }
        frontends: dict[str, Any] = {}
        for v in views:
            if not v["ok"]:
                continue                    # 失败目标不计入合计（陈旧值）
            total["conn"] += v["conn"]
            total["fe_conn"] += v["fe_conn"]
            total["max_conn"] += v["max_conn"]
            total["rate_in_bytes_per_s"] += v["rate_in_bytes_per_s"]
            total["rate_out_bytes_per_s"] += v["rate_out_bytes_per_s"]
            total["conn_new_ps"] += v["conn_new_ps"]
            total["denied_ps"] += v["denied_ps"]
            total["sess_rate"] += v["sess_rate"]
            for fname, fv in v["frontends"].items():
                agg = frontends.setdefault(fname, {
                    "rate_out_bytes_per_s": 0.0, "rate_in_bytes_per_s": 0.0,
                    "conn": 0, "conn_new_ps": 0.0, "denied_ps": 0.0,
                    "targets": 0, "mode": fv.get("mode", ""),
                })
                agg["rate_out_bytes_per_s"] += fv["rate_out_bytes_per_s"]
                agg["rate_in_bytes_per_s"] += fv["rate_in_bytes_per_s"]
                agg["conn"] += fv["conn"]
                agg["conn_new_ps"] += fv["conn_new_ps"]
                agg["denied_ps"] += fv["denied_ps"]
                agg["targets"] += 1

        return {"ts": now, "targets": targets, "total": total,
                "frontends": frontends}
