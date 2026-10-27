# tests.test_fleetmetrics —— 总控监控落盘（分钟 JSONL + 事件）与反查。
#
# 钉住：fleet/node 分钟行的聚合口径（离线采样不进带宽均值、限额随行记
# 当时值、超目标计数）、事件当场落盘且不带令牌、跨轮转文件按时间段/
# 类型/节点反查、截断。

from __future__ import annotations

import datetime as dt
import json
import logging

from rl_limiter import fleetmetrics

log = logging.getLogger("t.fleetmetrics")


def snap(ts, total=100.0, egress=50.0, ingress=2.0, nodes=None,
         online=None, allocated=100.0):
    nodes = nodes or {"n1": node()}
    return {
        "ts": ts, "total_quota_mbps": total, "egress_mbps": egress,
        "ingress_mbps": ingress, "allocated_mbps": allocated,
        "nodes_online": (sum(1 for n in nodes.values() if n["online"])
                         if online is None else online),
        "nodes_total": len(nodes), "nodes": nodes,
    }


def node(online=True, egress=50.0, ingress=2.0, quota=100.0, desired=None,
         pending=False, error="", write_err=""):
    return {"online": online, "egress_mbps": egress, "ingress_mbps": ingress,
            "quota_mbps": quota, "desired_mbps": desired,
            "release_pending": pending, "error": error,
            "write_err": write_err}


def rows(path):
    if not path.exists():
        return []
    return [json.loads(x) for x in path.read_text(encoding="utf-8")
            .splitlines() if x]


def test_fleet_and_node_rows_per_minute(tmp_path):
    p = tmp_path / "fleet-metrics.jsonl"
    m = fleetmetrics.FleetMetricsLog(str(p), log)
    m.record(snap(60, egress=80, nodes={"n1": node(egress=80, desired=90)}))
    m.record(snap(62, egress=120, nodes={"n1": node(egress=120, quota=90,
                                                    desired=90)}))
    # 离线：fleetpoll 保留最后一次观测（陈旧值）与最后自报的配额。
    m.record(snap(64, egress=40, nodes={"n1": node(online=False, egress=999,
                                                   quota=90)}, online=0))
    assert rows(p) == []                       # 分钟没过完不落盘
    m.record(snap(120))                        # 跨过分钟边界
    f, n1 = rows(p)
    assert (f["kind"], f["ts"], f["samples"]) == ("fleet", 60, 3)
    assert f["total_mbps"] == 100.0
    assert f["egress_avg"] == 80.0 and f["egress_max"] == 120.0
    assert f["over_samples"] == 1              # 只有 120 > 100
    assert f["nodes_online_min"] == 0 and f["nodes_total"] == 1
    assert (n1["kind"], n1["name"], n1["samples"], n1["online_samples"]) == \
        ("node", "n1", 3, 2)
    # 离线那拍的 999 是陈旧观测，不进均值/峰值。
    assert n1["egress_avg"] == 100.0 and n1["egress_max"] == 120.0
    # 限额随行记**当时**的值（最后一次自报）。
    assert n1["quota_mbps"] == 90.0 and n1["desired_mbps"] is None


def test_offline_whole_minute_has_null_rates_and_keeps_errors(tmp_path):
    p = tmp_path / "m.jsonl"
    m = fleetmetrics.FleetMetricsLog(str(p), log)
    m.record(snap(60, nodes={"n1": node(online=False, error="超时",
                                        pending=True)}))
    m.close()                                  # 收尾：短桶也写出去
    n1 = [r for r in rows(p) if r["kind"] == "node"][0]
    assert n1["online_samples"] == 0 and n1["egress_avg"] is None
    assert n1["error"] == "超时" and n1["release_pending"] is True


def test_total_zero_never_counts_over(tmp_path):
    p = tmp_path / "m.jsonl"
    m = fleetmetrics.FleetMetricsLog(str(p), log)
    m.record(snap(60, total=0, egress=500))
    m.close()
    assert rows(p)[0]["over_samples"] == 0


def test_events_written_immediately(tmp_path):
    p = tmp_path / "m.jsonl"
    m = fleetmetrics.FleetMetricsLog(str(p), log)
    m.event("quota", ts=75.4, node="n1", from_mbps=100.0, to_mbps=80.0)
    r = rows(p)
    assert r == [{"ts": 75, "kind": "event", "v": 1, "type": "quota",
                  "node": "n1", "from_mbps": 100.0, "to_mbps": 80.0}]


def _write(path, lines):
    path.write_text("".join(json.dumps(x) + "\n" for x in lines),
                    encoding="utf-8")


def test_query_spans_rotated_files_and_filters(tmp_path):
    p = tmp_path / "fleet-metrics.jsonl"
    day = lambda s: int(dt.datetime.fromisoformat(s).replace(  # noqa: E731
        tzinfo=dt.timezone.utc).timestamp())
    d1, d2, d3 = day("2026-10-25T12:00"), day("2026-10-26T12:00"), \
        day("2026-10-27T12:00")
    _write(tmp_path / "fleet-metrics.jsonl.2026-10-25", [
        {"ts": d1, "kind": "fleet", "total_mbps": 100}])
    _write(tmp_path / "fleet-metrics.jsonl.2026-10-26", [
        {"ts": d2, "kind": "fleet", "total_mbps": 200},
        {"ts": d2, "kind": "node", "name": "a", "quota_mbps": 120},
        {"ts": d2, "kind": "node", "name": "b", "quota_mbps": 80},
        {"ts": d2 + 5, "kind": "event", "type": "quota", "node": "b"},
        {"ts": d2 + 6, "kind": "event", "type": "total", "to_mbps": 0},
    ])
    (tmp_path / "fleet-metrics.jsonl.2026-10-26.bak").write_text("x")
    p.write_text(json.dumps({"ts": d3, "kind": "fleet"}) + "\n{半行",
                 encoding="utf-8")

    q = fleetmetrics.query(str(p), d2 - 60, d2 + 3600)
    assert [r["kind"] for r in q["rows"]] == \
        ["fleet", "node", "node", "event", "event"]
    assert not q["truncated"]

    # 只要 b：node 行只留 b，事件留 b 的与全局的（总带宽变更）。
    q = fleetmetrics.query(str(p), d2 - 60, d2 + 3600, node="b")
    assert [(r["kind"], r.get("name") or r.get("type")) for r in q["rows"]] \
        == [("fleet", None), ("node", "b"), ("event", "quota"),
            ("event", "total")]

    q = fleetmetrics.query(str(p), d1, d3 + 1, kinds=["fleet"])
    assert [r["ts"] for r in q["rows"]] == [d1, d2, d3]   # 坏行跳过

    q = fleetmetrics.query(str(p), d1, d3 + 1, limit=2)
    assert len(q["rows"]) == 2 and q["truncated"]


def test_query_missing_file_is_empty(tmp_path):
    q = fleetmetrics.query(str(tmp_path / "nope.jsonl"), 0, 10)
    assert q["rows"] == [] and q["files"] == 0
