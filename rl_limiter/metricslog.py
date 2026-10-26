# rl_limiter.metricslog —— 监控数据落**本地日志文件**（JSONL，按天轮转）。
#
# 定位：控制台的内存曲线只留最近约 10 分钟，重启即失；这里把监控数据
# 以**分钟粒度**追加到本机日志文件，重启不丢、可 grep、可被日志采集
# 系统直接摄取——历史回查/计费对账用它，实时观测仍走控制台与 /metrics。
#
# 口径：
#   - 每个自然分钟聚合一次，每个 frontend 一行 + 整机（instance）一行；
#   - **整机行（v=2）**：带宽为**客户端侧网卡口径**（锚点网卡的入/出
#     bytes/s，avg/max；与按量计费、闭环观测同口径），行里的 nic 字段
#     记录采的是哪张卡——设备数据缺席（RL_NIC=-）时回退 HAProxy 应用层
#     口径、nic 置空串；另记 conn_new_pm（该分钟新建连接总数——L4 下
#     每连接≈每请求，即 QPM 口径）、denied_pm、conn_max（并发峰值）、
#     drop_in/out_pm（网卡丢包）、degraded_s；
#   - **frontend 行**：带宽保持 HAProxy 应用层口径（网卡无法按端口拆，
#     这是唯一的端口级数据源），速率取该分钟逐秒采样的 avg/max；
#     mean10_max 是该分钟内 10s 滑动均值的最大值——**超限判定口径**，
#     "这一分钟内是否曾贴到限额"看它；另记 conn_new_pm；
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
    """单个统计对象（一个 frontend 或整机）在当前分钟内的累积。

    整机行的 rate_* 装的是网卡口径（锚点网卡入/出；缺设备数据时回退
    HAProxy 口径），frontend 行装的是 HAProxy 应用层口径——字段复用，
    口径由 _row 的 kind 决定怎么写。"""

    __slots__ = ("n", "rate_sum", "rate_max", "rate_in_sum", "rate_in_max",
                 "mean10_max", "conn_max", "conn_new_sum", "denied_sum",
                 "drop_in_sum", "drop_out_sum", "over_s", "degraded_s")

    def __init__(self):
        self.n = 0
        self.rate_sum = 0.0        # 出向
        self.rate_max = 0.0
        self.rate_in_sum = 0.0     # 入向
        self.rate_in_max = 0.0
        self.mean10_max = 0.0
        self.conn_max = 0
        self.conn_new_sum = 0.0    # 该分钟新建连接总数（整机行 ≈ QPM）
        self.denied_sum = 0.0
        self.drop_in_sum = 0.0     # 网卡丢包（仅整机行）
        self.drop_out_sum = 0.0
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
        # 整机行带宽采的网卡名（最近一拍；空串 = 回退 HAProxy 口径）。
        self._inst_nic = ""

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
            a.conn_new_sum += u.conn_new_ps
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
            # 整机带宽记**客户端侧网卡口径**（锚点网卡的入/出；与按量
            # 计费、闭环观测同口径）。设备数据缺席（RL_NIC=- 或从未采到）
            # 回退 HAProxy 应用层口径，nic 置空串让回查时口径可辨。
            if instance.nic:
                self._inst_nic = instance.nic
                out_bps, in_bps = (instance.nic_rate_out_bps,
                                   instance.nic_rate_in_bps)
            else:
                self._inst_nic = ""
                out_bps, in_bps = instance.rate_out_bps, instance.rate_in_bps
            a.rate_sum += out_bps
            a.rate_max = max(a.rate_max, out_bps)
            a.rate_in_sum += in_bps
            a.rate_in_max = max(a.rate_in_max, in_bps)
            a.conn_max = max(a.conn_max, instance.conn_cur)
            a.conn_new_sum += instance.conn_new_ps
            a.denied_sum += instance.conn_denied_ps
            a.drop_in_sum += instance.drop_in_ps
            a.drop_out_sum += instance.drop_out_ps
            if instance.degraded:
                a.degraded_s += 1

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
        if kind == "instance":
            # 整机行 schema v2：带宽为客户端侧网卡口径（bytes/s），
            # nic 记录采的卡（空串 = 回退 HAProxy 应用层口径）。
            return {
                "ts": ts,                   # 分钟起点（unix 秒，UTC）
                "kind": "instance",
                "v": 2,
                "samples": a.n,             # 该分钟实际采到的拍数（≤60）
                "nic": self._inst_nic,
                "rate_in_avg": round(a.rate_in_sum / a.n, 1),
                "rate_in_max": round(a.rate_in_max, 1),
                "rate_out_avg": round(a.rate_sum / a.n, 1),
                "rate_out_max": round(a.rate_max, 1),
                # 该分钟新建连接总数——L4 下每连接≈每请求，即 QPM 口径。
                "conn_new_pm": round(a.conn_new_sum),
                "denied_pm": round(a.denied_sum),
                "conn_max": a.conn_max,     # 并发连接峰值
                "drop_in_pm": round(a.drop_in_sum),   # 网卡丢包（整机）
                "drop_out_pm": round(a.drop_out_sum),
                "degraded_s": a.degraded_s,
            }
        # frontend 行：带宽保持 HAProxy 应用层口径（唯一的端口级数据源）。
        return {
            "ts": ts,
            "kind": "frontend",
            "samples": a.n,
            "name": name,
            # 速率一律 bytes/s（应用层口径，与控制台/告警一致）
            "rate_avg": round(a.rate_sum / a.n, 1),
            "rate_max": round(a.rate_max, 1),
            "rate_in_avg": round(a.rate_in_sum / a.n, 1),
            "conn_max": a.conn_max,
            "conn_new_pm": round(a.conn_new_sum),
            "denied": round(a.denied_sum, 1),
            "mean10_max": round(a.mean10_max, 1),
            "quota": round(quota or 0.0, 1),   # 0 = 不限速
            "over_s": a.over_s,
            "degraded_s": a.degraded_s,
        }

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
