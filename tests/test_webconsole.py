# tests.test_webconsole —— 控制台的 StatusHub（快照合成/订阅/概览）与
# 写接口的应答契约。HTTP 层用 aiohttp 的测试工具直接打真实路由。

from __future__ import annotations

import asyncio

import pytest
from aiohttp.test_utils import TestClient, TestServer

from rl_limiter import model, webconsole


def fe(name="fe_a", quota_bps=8_000_000, port=8080):
    return model.FrontendConfig(
        name=name, bind_port=port, quota_bits_per_sec=quota_bps,
        servers=[model.ServerEntry(name="s1", address="10.0.0.1", port=80)])


def usage(name="fe_a", mean10=0.0, rate=0.0, conn=0, degraded=False):
    return model.FrontendUsage(name=name, rate_bps=rate, mean10_bps=mean10,
                               conn_cur=conn, degraded=degraded)


def hub(**kw):
    h = webconsole.StatusHub("test", version_fn=lambda: 1, **kw)
    h.update_config(model.ControllerConfig(version=1, frontends=[fe()]))
    return h


# ---------------------------------------------------------------------------
# 快照与概览
# ---------------------------------------------------------------------------

def test_snapshot_merges_usage_and_quota():
    h = hub()
    h.record(100.0, [usage(rate=500.0, mean10=400.0, conn=7)])
    snap = h.history()[-1]
    u = snap["units"]["fe_a"]
    assert u["rate_bytes_per_s"] == 500.0
    assert u["conn"] == 7
    # 限额随快照下发，页面据此画参考线；换算（÷8）只在服务端做。
    assert u["quota_bytes_per_s"] == 1_000_000
    assert u["over"] is False


def test_snapshot_over_flag():
    h = hub()
    h.record(1.0, [usage(mean10=2_000_000)])       # 限额 1_000_000 bytes/s
    assert h.history()[-1]["units"]["fe_a"]["over"] is True


def test_degraded_suppresses_over_flag():
    """采样失联时数据陈旧，不该据此判超限（判定在循环里也是暂停的）。"""
    h = hub()
    h.record(1.0, [usage(mean10=2_000_000, degraded=True)])
    assert h.history()[-1]["units"]["fe_a"]["over"] is False


def test_haproxy_view_reports_endpoint_and_health():
    h = hub(haproxy=model.NodeConfig(name="hap", socket_path="/run/h.sock"),
            degraded_fn=lambda: True)
    v = h.overview()["haproxy"]
    assert v == {"name": "hap", "endpoint": "/run/h.sock",
                 "unix": True, "degraded": True}


def test_overview_exposes_frontend_config():
    """界面的编辑表单以 overview 的 frontends 为初值。"""
    o = hub().overview()
    f = o["frontends"]["fe_a"]
    assert f["bind_port"] == 8080 and f["quota_bps"] == 8_000_000
    assert f["quota_bytes_per_s"] == 1_000_000
    assert [s["name"] for s in f["servers"]] == ["s1"]


def test_history_is_bounded():
    h = hub()
    for i in range(webconsole.HISTORY_TICKS + 50):
        h.record(float(i), [usage()])
    assert len(h.history()) == webconsole.HISTORY_TICKS


def test_subscriber_drops_oldest_when_slow():
    """慢消费者丢最旧一帧：页面只关心最新状态，绝不反压快环。"""
    h = hub()
    q = h.subscribe()
    for i in range(webconsole.SUBSCRIBER_QUEUE_DEPTH + 3):
        h.record(float(i), [usage()])
    assert q.qsize() == webconsole.SUBSCRIBER_QUEUE_DEPTH


def test_record_enforce_tracks_success_and_failure():
    h = hub()
    assert h.overview()["enforce"] is None      # 未启用下发

    class R:
        def __init__(self, ok, changed, err="", fes=()):
            self.ok, self.changed, self.error = ok, changed, err
            self.frontends = list(fes)

    h.record_enforce(R(True, True, fes=["fe_a"]))
    e = h.overview()["enforce"]
    assert e["enabled"] and e["ok"] and e["applied"] == ["fe_a"]
    assert e["last_change_ts"] is not None

    h.record_enforce(R(False, False, err="haproxy -c 挂了"))
    e = h.overview()["enforce"]
    assert not e["ok"] and "haproxy -c 挂了" in e["error"]
    # 失败不该抹掉"上次成功下发的时间"，那是排障时的重要参照。
    assert e["last_change_ts"] is not None


# ---------------------------------------------------------------------------
# HTTP 接口
# ---------------------------------------------------------------------------

@pytest.fixture
async def client():
    app = webconsole.build_app(hub(), webconsole.LogBuffer(), None,
                               __import__("logging").getLogger("t"))
    async with TestClient(TestServer(app)) as c:
        yield c


async def test_overview_endpoint(client):
    r = await client.get("/api/overview")
    assert r.status == 200
    body = await r.json()
    assert "fe_a" in body["frontends"]


async def test_write_endpoints_require_db_mode(client):
    """纯 YAML 部署没有可写的配置源：写接口应 409 并说清原因，而不是
    假装成功。"""
    for method, path in (("put", "/api/frontends/fe_a"),
                         ("post", "/api/frontends"),
                         ("delete", "/api/frontends/fe_a")):
        r = await getattr(client, method)(path, data="{}")
        assert r.status == 409, path
        assert "MySQL" in (await r.json())["error"]


async def test_index_is_served(client):
    r = await client.get("/")
    assert r.status == 200
    assert b"rl-limiter" in await r.read()
