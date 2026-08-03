# rl_limiter.metricslog —— 监控数据落**本地日志文件**（JSONL，按天轮转）。
#
# 定位：控制台的内存曲线只留最近约 10 分钟，重启即失；这里把监控数据
# 以**分钟粒度**追加到本机日志文件，重启不丢、可 grep、可被日志采集
# 系统直接摄取——历史回查/计费对账用它，实时观测仍走控制台与 /metrics。
#
# 口径（与已下线的 metricstore 的 1 分钟层一致）：
#   - 每个自然分钟聚合一次，每个 frontend 一行 + 整机（instance）一行；
#   - 速率字段取该分钟内逐秒采样的 avg/max（bytes/s，应用层口径）；
#   - mean10_max 是该分钟内 10s 滑动均值的最大值——**计费/超限口径**，
#     "这一分钟内是否曾贴到限额"看它；
#   - quota 随行记录：限额会被人改，回查时要的是**当时的**限额；
#   - over_s 是该分钟内 mean10 高于限额的秒数（degraded 的秒不计）。
#
# 落盘形态：一行一个 JSON 对象（JSONL），logging.TimedRotatingFileHandler
# 按天轮转、保留 retention_days 天——轮转与清理都交给标准库，本模块只管
# 聚合与格式。写失败不影响监控主链路（logging 自身吞掉 IO 错误）。
#
# 体量账：1 行约 250 字节 ×（frontend 数 + 1）× 1440 分钟/天——
# 10 个 frontend 一天约 4 MB，默认保留 90 天约 360 MB，可接受。

from __future__ import annotations

import json
import logging
import logging.handlers
import os
from typing import Any, Callable

from . import model

# 聚合粒度（秒）。与原 metricstore 的 1 分钟层对齐；不做成配置——
# 改粒度会让不同时期的日志行没法直接对比。
BUCKET_S = 60

# 默认保留天数（按天轮转后删除更旧的文件）。
DEFAULT_RETENTION_DAYS = 90


class _Agg:
    """单个统计对象（一个 frontend 或整机）在当前分钟内的累积。"""

    __slots__ = ("n", "rate_sum", "rate_max", "rate_in_sum", "mean10_max",
                 "conn_max", "denied_sum", "over_s", "degraded_s")

    def __init__(self):
        self.n = 0
        self.rate_sum = 0.0
        self.rate_max = 0.0
        self.rate_in_sum = 0.0
        self.mean10_max = 0.0
        self.conn_max = 0
        self.denied_sum = 0.0
        self.over_s = 0
        self.degraded_s = 0


class MetricsLog:
    """监控数据的分钟级 JSONL 落盘器。

    record(now, usages, instance) 由监控循环的 sampler 每拍调用（与控制台
    的 StatusHub 同一拍、同一份数据）；跨过分钟边界时把上一分钟的聚合
    写盘。quotas_fn 延迟取当前限额（bytes/s），保证行里记的是**采样当时**
    生效的限额。
    """

    def __init__(self, path: str, log: logging.Logger,
                 retention_days: int = DEFAULT_RETENTION_DAYS,
                 quotas_fn: Callable[[], dict[str, float]] | None = None):
        self.path = path
        self._log = log
        self._quotas_fn = quotas_fn or (lambda: {})
        # 独立 logger + 不向根传播：监控数据是数据不是日志，不能混进
        # stderr/控制台日志缓冲里。
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        self._writer = logging.getLogger(f"rl_limiter.metricslog.{id(self)}")
        self._writer.setLevel(logging.INFO)
        self._writer.propagate = False
        handler = logging.handlers.TimedRotatingFileHandler(
            path, when="midnight", backupCount=retention_days,
            encoding="utf-8", utc=True)
        handler.setFormatter(logging.Formatter("%(message)s"))
        self._writer.addHandler(handler)
        self._bucket_start: int | None = None
        self._fe: dict[str, _Agg] = {}
        self._inst = _Agg()

    def set_quotas_fn(self, fn: Callable[[], dict[str, float]]) -> None:
        """接线期注入"当前限额"视图（构造时监控循环还没建出来）。"""
        self._quotas_fn = fn

    # ---- 采样注入（监控循环 sampler 每拍调用）----

    def record(self, now: float, usages, instance=None) -> None:
        bucket = int(now) // BUCKET_S * BUCKET_S
        if self._bucket_start is None:
            self._bucket_start = bucket
        elif bucket != self._bucket_start:
            self._flush()
            self._bucket_start = bucket

        quotas = self._quotas_fn()
        for u in usages:
            a = self._fe.setdefault(u.name, _Agg())
            a.n += 1
            a.rate_sum += u.rate_bps
            a.rate_max = max(a.rate_max, u.rate_bps)
            a.rate_in_sum += u.rate_in_bps
            a.mean10_max = max(a.mean10_max, u.mean10_bps)
            a.conn_max = max(a.conn_max, u.conn_cur)
            a.denied_sum += u.conn_denied_ps
            if u.degraded:
                a.degraded_s += 1
            else:
                q = quotas.get(u.name, 0.0)
                if q > 0 and u.mean10_bps > q:
                    a.over_s += 1
        if instance is not None:
            a = self._inst
            a.n += 1
            a.rate_sum += instance.rate_out_bps
            a.rate_max = max(a.rate_max, instance.rate_out_bps)
            a.rate_in_sum += instance.rate_in_bps
            a.conn_max = max(a.conn_max, instance.conn_cur)
            a.denied_sum += instance.conn_denied_ps

    def close(self) -> None:
        """停机收尾：把最后一个未满的分钟也写出去（宁可短桶，不可丢数）。"""
        self._flush()
        for h in list(self._writer.handlers):
            h.close()
            self._writer.removeHandler(h)

    # ---- 落盘 ----

    def _flush(self) -> None:
        if self._bucket_start is None:
            return
        ts = self._bucket_start
        quotas = self._quotas_fn()
        for name in sorted(self._fe):
            a = self._fe[name]
            if a.n:
                self._emit(self._row(ts, "frontend", name, a,
                                     quota=quotas.get(name, 0.0)))
        if self._inst.n:
            self._emit(self._row(ts, "instance", "", self._inst))
        self._fe.clear()
        self._inst = _Agg()

    def _row(self, ts: int, kind: str, name: str, a: _Agg,
             quota: float | None = None) -> dict[str, Any]:
        row: dict[str, Any] = {
            "ts": ts,                       # 分钟起点（unix 秒，UTC）
            "kind": kind,                   # frontend | instance
            "samples": a.n,                 # 该分钟实际采到的拍数（≤60）
            # 速率一律 bytes/s（应用层口径，与控制台/告警一致）
            "rate_avg": round(a.rate_sum / a.n, 1),
            "rate_max": round(a.rate_max, 1),
            "rate_in_avg": round(a.rate_in_sum / a.n, 1),
            "conn_max": a.conn_max,
            "denied": round(a.denied_sum, 1),
        }
        if kind == "frontend":
            row["name"] = name
            row["mean10_max"] = round(a.mean10_max, 1)
            row["quota"] = round(quota or 0.0, 1)   # 0 = 不限速
            row["over_s"] = a.over_s
            row["degraded_s"] = a.degraded_s
        return row

    def _emit(self, row: dict[str, Any]) -> None:
        try:
            self._writer.info(json.dumps(row, ensure_ascii=False,
                                         separators=(",", ":")))
        except Exception as e:  # pragma: no cover - IO 故障不还手打断监控
            self._log.warning("监控数据落盘失败（不影响监控主链路） "
                              "path=%s err=%s", self.path, e)


def quotas_from_frontends(
        frontends_fn: Callable[[], list[model.FrontendConfig]],
) -> Callable[[], dict[str, float]]:
    """把"当前受管清单"适配成 record 需要的 名字→限额(bytes/s) 视图。"""
    def fn() -> dict[str, float]:
        return {f.name: f.quota_bytes_per_sec for f in frontends_fn()}
    return fn
