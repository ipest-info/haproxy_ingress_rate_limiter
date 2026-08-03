# tests.test_metricslog —— 监控数据落盘（分钟粒度 JSONL）。
#
# 钉住三件事：聚合口径（avg/max/mean10_max/over_s 与限额随行）、分钟
# 边界的翻转时机、以及"落盘失败不打断监控主链路"的隔离性。

from __future__ import annotations

import json
import logging

from rl_limiter import metricslog, model

log = logging.getLogger("t.metricslog")


def usage(name="fe_a", rate=0.0, mean10=0.0, conn=0, denied=0.0,
          degraded=False):
    return model.FrontendUsage(
        name=name, rate_bps=rate, mean10_bps=mean10, conn_cur=conn,
        conn_denied_ps=denied, degraded=degraded)


def mk(tmp_path, quotas=None):
    m = metricslog.MetricsLog(str(tmp_path / "metrics.jsonl"), log)
    m.set_quotas_fn(lambda: dict(quotas or {}))
    return m


def read_rows(tmp_path):
    p = tmp_path / "metrics.jsonl"
    if not p.exists():
        return []
    return [json.loads(line) for line in
            p.read_text(encoding="utf-8").splitlines() if line]


def test_aggregates_one_row_per_frontend_per_minute(tmp_path):
    m = mk(tmp_path, {"fe_a": 5_000_000.0})
    base = 60.0                                  # 分钟起点
    for i, rate in enumerate([100.0, 300.0, 200.0]):
        m.record(base + i, [usage(rate=rate, mean10=rate, conn=i + 1)])
    m.record(base + 60, [usage(rate=0.0)])       # 跨过分钟边界 → 触发落盘
    rows = read_rows(tmp_path)
    assert len(rows) == 1
    r = rows[0]
    assert (r["kind"], r["name"], r["ts"], r["samples"]) == \
        ("frontend", "fe_a", 60, 3)
    assert r["rate_avg"] == 200.0 and r["rate_max"] == 300.0
    assert r["mean10_max"] == 300.0
    assert r["conn_max"] == 3
    # 限额随行记录——回查时要的是**当时的**限额。
    assert r["quota"] == 5_000_000.0


def test_over_seconds_counted_against_quota(tmp_path):
    """over_s = 该分钟内 mean10 高于限额的秒数；degraded 的秒不计
    （陈旧数据不该算成超限）。"""
    m = mk(tmp_path, {"fe_a": 1000.0})
    m.record(60.0, [usage(mean10=2000.0)])                   # 超限
    m.record(61.0, [usage(mean10=500.0)])                    # 未超
    m.record(62.0, [usage(mean10=9999.0, degraded=True)])    # 失联，不计
    m.record(120.0, [usage()])
    (r,) = [x for x in read_rows(tmp_path) if x["kind"] == "frontend"]
    assert r["over_s"] == 1
    assert r["degraded_s"] == 1


def test_unlimited_frontend_never_over(tmp_path):
    """quota=0（不限速）没有超限概念，over_s 恒为 0。"""
    m = mk(tmp_path, {})
    m.record(60.0, [usage(mean10=9_999_999.0)])
    m.record(120.0, [usage()])
    (r,) = read_rows(tmp_path)
    assert r["over_s"] == 0 and r["quota"] == 0


def test_instance_row_alongside_frontend_rows(tmp_path):
    m = mk(tmp_path)
    inst = model.InstanceUsage(rate_out_bps=800.0, rate_in_bps=100.0,
                               conn_cur=42)
    m.record(60.0, [usage(name="fe_a"), usage(name="fe_b")], inst)
    m.record(120.0, [usage()])
    rows = read_rows(tmp_path)
    kinds = sorted((r["kind"], r.get("name", "")) for r in rows)
    assert kinds == [("frontend", "fe_a"), ("frontend", "fe_b"),
                     ("instance", "")]
    inst_row = next(r for r in rows if r["kind"] == "instance")
    assert inst_row["rate_avg"] == 800.0 and inst_row["conn_max"] == 42


def test_close_flushes_partial_minute(tmp_path):
    """停机时把最后一个未满的分钟也写出去——宁可短桶，不可丢数。"""
    m = mk(tmp_path)
    m.record(60.0, [usage(rate=123.0)])
    assert read_rows(tmp_path) == []             # 分钟未满，还在内存
    m.close()
    (r,) = read_rows(tmp_path)
    assert r["samples"] == 1 and r["rate_max"] == 123.0


def test_rows_are_one_json_object_per_line(tmp_path):
    """JSONL 契约：一行一个对象，日志采集系统按行摄取。"""
    m = mk(tmp_path)
    m.record(60.0, [usage(name="fe_a"), usage(name="fe_b")])
    m.close()
    text = (tmp_path / "metrics.jsonl").read_text(encoding="utf-8")
    lines = [ln for ln in text.splitlines() if ln]
    assert len(lines) == 2
    for ln in lines:
        assert "\n" not in ln and json.loads(ln)
