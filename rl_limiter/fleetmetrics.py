# rl_limiter.fleetmetrics —— 总控的监控数据与限速变更落**本地日志文件**
# （JSONL，按天轮转），并提供按时间段反查。
#
# 定位与节点侧 metricslog 相同：控制台的内存曲线只留最近约 10 分钟、重启
# 即失；这里把总控看到的一切以**分钟粒度**追加到本机文件——重启不丢、
# 可 grep/jq、可被日志采集系统直接摄取；页面「历史回查」与
# GET /api/metrics-log 读的也是它。
#
# 三种行（kind 区分，同一个文件、同一条时间线）：
#
#   - **fleet**（每分钟一行）：总带宽目标 total_mbps（**当时的**——它会被
#     人改，回查要的是当时的值）、总出向/总入向（Σ在线节点，avg/max）、
#     已分配合计、在线节点数（该分钟最低值）、over_samples（该分钟内总
#     出向高于总带宽目标的采样次数）；
#   - **node**（每节点每分钟一行）：出向/入向 avg/max（只计在线的采样
#     ——离线时的观测是陈旧值）、节点自报配额 quota_mbps（0 = 不限）、
#     总控本轮分配 desired_mbps、在线采样数、待解除、最近错误；
#   - **event**（发生即写，ts 精确到秒）：总控启动、总带宽变更、给节点
#     下发配额 / 解除限速、节点清单增删改——"什么时候、谁的限速从多少
#     改成了多少"直接按时间查。
#
# 单位一律 Mbps（总控的量本来就以 Mbps 流转，与页面一致；节点侧落盘是
# bytes/s，对照时注意换算）。采样来自轮询环的每一帧快照（默认 2s 一帧，
# 一分钟约 30 个采样，samples 字段如实记录）。
#
# 轮转：logging.TimedRotatingFileHandler 每天 UTC 零点轮转（旧文件名
# 追加 .YYYY-MM-DD），保留 retention_days 天。写失败不影响总控主链路。
#
# 体量账：1 行约 200 字节 ×（节点数 + 1）× 1440 分钟/天——30 台节点
# 一天约 9 MB，默认保留 90 天约 800 MB。

from __future__ import annotations

import datetime as _dt
import json
import logging
import logging.handlers
import os
import time
from typing import Any

# 聚合粒度（秒）。与节点侧一致，不做成配置——改粒度会让不同时期的行
# 没法直接对比。
BUCKET_S = 60
DEFAULT_RETENTION_DAYS = 90
# 单次反查最多返回的行数（防一次查 90 天全量把总控拖死）。
QUERY_LIMIT_MAX = 50000
QUERY_LIMIT_DEFAULT = 20000

KINDS = ("fleet", "node", "event")


def _r(v: "float | None", nd: int = 2) -> "float | None":
    return None if v is None else round(float(v), nd)


class _NodeAgg:
    __slots__ = ("n", "online_n", "eg_sum", "eg_max", "in_sum", "in_max",
                 "quota", "desired", "pending", "error", "write_err")

    def __init__(self):
        self.n = 0
        self.online_n = 0
        self.eg_sum = self.eg_max = 0.0
        self.in_sum = self.in_max = 0.0
        self.quota: "float | None" = None
        self.desired: "float | None" = None
        self.pending = False
        self.error = ""
        self.write_err = ""


class _FleetAgg:
    __slots__ = ("n", "eg_sum", "eg_max", "in_sum", "in_max", "total",
                 "allocated", "online_min", "nodes_total", "over_n")

    def __init__(self):
        self.n = 0
        self.eg_sum = self.eg_max = 0.0
        self.in_sum = self.in_max = 0.0
        self.total = 0.0
        self.allocated = 0.0
        self.online_min: "int | None" = None
        self.nodes_total = 0
        self.over_n = 0


class FleetMetricsLog:
    """总控监控数据的分钟级 JSONL 落盘器 + 事件记录器。

    record(snap) 由轮询环每帧调用（与控制台 FleetHub 同一帧、同一份
    快照）；跨过分钟边界时把上一分钟的聚合写盘。event(...) 当场写一行。
    """

    def __init__(self, path: str, log: logging.Logger,
                 retention_days: int = DEFAULT_RETENTION_DAYS):
        self.path = path
        self._log = log
        os.makedirs(os.path.dirname(os.path.abspath(path)) or ".",
                    exist_ok=True)
        # 独立 logger + 不向根传播：数据不是日志，不能混进 stderr/控制台
        # 日志缓冲。
        self._writer = logging.getLogger(
            f"rl_limiter.fleetmetrics.{id(self)}")
        self._writer.setLevel(logging.INFO)
        self._writer.propagate = False
        handler = logging.handlers.TimedRotatingFileHandler(
            path, when="midnight", backupCount=retention_days,
            encoding="utf-8", utc=True)
        handler.setFormatter(logging.Formatter("%(message)s"))
        self._writer.addHandler(handler)
        self._bucket: "int | None" = None
        self._fleet = _FleetAgg()
        self._nodes: dict[str, _NodeAgg] = {}

    # ---- 采样注入（轮询环每帧调用）----

    def record(self, snap: dict) -> None:
        now = float(snap.get("ts") or time.time())
        bucket = int(now) // BUCKET_S * BUCKET_S
        if self._bucket is None:
            self._bucket = bucket
        elif bucket != self._bucket:
            self._flush()
            self._bucket = bucket

        f = self._fleet
        f.n += 1
        eg = float(snap.get("egress_mbps") or 0.0)
        ing = float(snap.get("ingress_mbps") or 0.0)
        f.eg_sum += eg
        f.eg_max = max(f.eg_max, eg)
        f.in_sum += ing
        f.in_max = max(f.in_max, ing)
        f.total = float(snap.get("total_quota_mbps") or 0.0)
        f.allocated = float(snap.get("allocated_mbps") or 0.0)
        online = int(snap.get("nodes_online") or 0)
        f.online_min = online if f.online_min is None else min(
            f.online_min, online)
        f.nodes_total = int(snap.get("nodes_total") or 0)
        if f.total > 0 and eg > f.total:
            f.over_n += 1

        for name, n in (snap.get("nodes") or {}).items():
            a = self._nodes.setdefault(name, _NodeAgg())
            a.n += 1
            if n.get("online"):
                a.online_n += 1
                e = float(n.get("egress_mbps") or 0.0)
                i = float(n.get("ingress_mbps") or 0.0)
                a.eg_sum += e
                a.eg_max = max(a.eg_max, e)
                a.in_sum += i
                a.in_max = max(a.in_max, i)
            if n.get("quota_mbps") is not None:
                a.quota = float(n["quota_mbps"])
            a.desired = n.get("desired_mbps")
            a.pending = a.pending or bool(n.get("release_pending"))
            a.error = n.get("error") or a.error
            a.write_err = n.get("write_err") or a.write_err

    def event(self, type_: str, ts: "float | None" = None,
              **fields: Any) -> None:
        """当场写一行变更事件（kind=event）。fields 里**绝不放令牌**。"""
        row = {"ts": int(ts if ts is not None else time.time()),
               "kind": "event", "v": 1, "type": type_}
        row.update(fields)
        self._emit(row)

    def close(self) -> None:
        """停机收尾：把最后一个未满的分钟也写出去（宁可短桶，不可丢数）。"""
        self._flush()
        for h in list(self._writer.handlers):
            h.close()
            self._writer.removeHandler(h)

    # ---- 落盘 ----

    def _flush(self) -> None:
        if self._bucket is None:
            return
        ts = self._bucket
        f = self._fleet
        if f.n:
            self._emit({
                "ts": ts,                       # 分钟起点（unix 秒，UTC）
                "kind": "fleet", "v": 1,
                "samples": f.n,
                "total_mbps": _r(f.total, 1),   # 0 = 未做总量控制
                "egress_avg": _r(f.eg_sum / f.n),
                "egress_max": _r(f.eg_max),
                "ingress_avg": _r(f.in_sum / f.n),
                "ingress_max": _r(f.in_max),
                "allocated_mbps": _r(f.allocated, 1),
                "nodes_online_min": f.online_min or 0,
                "nodes_total": f.nodes_total,
                "over_samples": f.over_n,
            })
        for name in sorted(self._nodes):
            a = self._nodes[name]
            on = a.online_n
            self._emit({
                "ts": ts, "kind": "node", "v": 1, "name": name,
                "samples": a.n,
                "online_samples": on,
                "egress_avg": _r(a.eg_sum / on) if on else None,
                "egress_max": _r(a.eg_max) if on else None,
                "ingress_avg": _r(a.in_sum / on) if on else None,
                "ingress_max": _r(a.in_max) if on else None,
                "quota_mbps": _r(a.quota, 1),   # 节点自报；0 = 不限
                "desired_mbps": _r(a.desired, 1),
                "release_pending": a.pending,
                "error": a.error,
                "write_err": a.write_err,
            })
        self._fleet = _FleetAgg()
        self._nodes = {}

    def _emit(self, row: dict) -> None:
        try:
            self._writer.info(json.dumps(row, ensure_ascii=False,
                                         separators=(",", ":")))
        except Exception as e:  # pragma: no cover - IO 故障不还手打断总控
            self._log.warning("总控监控数据落盘失败（不影响分配与轮询） "
                              "path=%s err=%s", self.path, e)


# ---------------------------------------------------------------------------
# 反查
# ---------------------------------------------------------------------------

def _candidate_files(path: str, start: float, end: float) -> list:
    """区间可能落在哪些文件里：当前文件 + 日期后缀与区间相邻的轮转文件。

    轮转文件名 = 原名 + ".YYYY-MM-DD"（UTC，文件里是那一天的数据；进程
    停过的边界可能跨一点，所以前后各放宽一天）。按时间先后返回。
    """
    d = os.path.dirname(os.path.abspath(path))
    base = os.path.basename(path)
    lo = _dt.datetime.fromtimestamp(start, _dt.timezone.utc).date() \
        - _dt.timedelta(days=1)
    hi = _dt.datetime.fromtimestamp(end, _dt.timezone.utc).date() \
        + _dt.timedelta(days=1)
    rotated = []
    try:
        names = os.listdir(d)
    except OSError:
        names = []
    for n in names:
        if not n.startswith(base + "."):
            continue
        try:
            day = _dt.date.fromisoformat(n[len(base) + 1:])
        except ValueError:
            continue
        if lo <= day <= hi:
            rotated.append((day, os.path.join(d, n)))
    files = [p for _, p in sorted(rotated)]
    if os.path.exists(path):
        files.append(path)
    return files


def query(path: str, start: float, end: float,
          kinds: "tuple | list | None" = None, node: str = "",
          limit: int = QUERY_LIMIT_DEFAULT) -> dict:
    """按时间段反查落盘数据（同步、只读；调用方放线程里跑）。

    kinds 为空 = 三种都要；node 非空时 node 行只留该节点、event 行只留
    该节点的与全局的（总带宽变更、启动等没有 node 字段的事件）。结果按
    ts 排序，超过 limit 截断并标 truncated。
    """
    want = set(kinds or KINDS)
    limit = max(1, min(int(limit), QUERY_LIMIT_MAX))
    rows: list = []
    truncated = False
    files = _candidate_files(path, start, end)
    for fp in files:
        try:
            fh = open(fp, encoding="utf-8", errors="replace")
        except OSError:
            continue
        with fh:
            for line in fh:
                try:
                    r = json.loads(line)
                except ValueError:
                    continue                    # 半行/坏行：跳过
                if not isinstance(r, dict):
                    continue
                k = r.get("kind")
                ts = r.get("ts")
                if k not in want or not isinstance(ts, (int, float)):
                    continue
                if not start <= ts < end:
                    continue
                if node:
                    if k == "node" and r.get("name") != node:
                        continue
                    if k == "event" and r.get("node", node) != node:
                        continue
                if len(rows) >= limit:
                    truncated = True
                    break
                rows.append(r)
        if truncated:
            break
    rows.sort(key=lambda r: (r["ts"], r.get("kind") != "event"))
    return {"rows": rows, "truncated": truncated, "files": len(files)}
