# tests.test_webconsole —— 控制台的 StatusHub（快照合成/订阅/概览）。
# 控制台是**只读**的（配置改文件，不走页面）；HTTP 层用 aiohttp 的
# 测试工具直接打真实路由。

from __future__ import annotations

import pytest
from aiohttp.test_utils import TestClient, TestServer

from rl_limiter import model, webconsole


def fe(name="fe_a", quota_mbps=8.0, port=8080):
    return model.FrontendConfig(name=name, bind_port=port, quota_mbps=quota_mbps)


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


def test_snapshot_carries_frontend_monitoring_fields():
    """监听端口视图的那几条曲线（新建/丢失连接、活跃/空闲、上行）随快照
    一起下发——页面不该为了画图再去发第二种请求。"""
    h = hub()
    h.record(1.0, [model.FrontendUsage(
        name="fe_a", rate_in_bps=88.0, conn_new_ps=12.0, conn_denied_ps=3.0,
        active_conns=4, idle_conns=5)])
    u = h.history()[-1]["units"]["fe_a"]
    assert u["rate_in_bytes_per_s"] == 88.0
    assert (u["conn_new_ps"], u["conn_denied_ps"]) == (12.0, 3.0)
    assert (u["active_conns"], u["idle_conns"]) == (4, 5)


def test_snapshot_carries_instance_view():
    """实例视图与 frontend 视图出自同一拍，装在同一帧里下发，页面切 tab
    时两条时间轴才对得齐。"""
    h = hub()
    h.record(1.0, [usage()], model.InstanceUsage(
        conn_new_ps=33.0, conn_denied_ps=2.0, conn_cur=17, active_conns=9,
        idle_conns=8, max_conn=4000, rate_in_bps=1000.0, rate_out_bps=5000.0,
        nic="eth0", pkts_in_ps=40.0, pkts_out_ps=60.0,
        drop_in_ps=1.0, drop_out_ps=0.0, idle_pct=87))
    i = h.history()[-1]["instance"]
    assert (i["conn"], i["active_conns"], i["idle_conns"]) == (17, 9, 8)
    assert (i["conn_new_ps"], i["conn_denied_ps"]) == (33.0, 2.0)
    assert (i["rate_in_bytes_per_s"], i["rate_out_bytes_per_s"]) == (1000.0, 5000.0)
    # 网卡口径的字段名带 nic_/pkts_/drop_ 前缀，与 HAProxy 口径分得开。
    assert i["nic"] == "eth0"
    assert (i["pkts_in_ps"], i["pkts_out_ps"]) == (40.0, 60.0)
    assert (i["drop_in_ps"], i["drop_out_ps"]) == (1.0, 0.0)
    assert i["idle_pct"] == 87


def test_snapshot_without_instance_view_is_still_valid():
    """未提供实例视图（纯 frontend 采样的老接线）时快照照常成立。"""
    h = hub()
    h.record(1.0, [usage()])
    assert h.history()[-1]["instance"] == {}


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
    """页面的配置视图（只读）以 overview 的 frontends 为数据源。"""
    o = hub().overview()
    f = o["frontends"]["fe_a"]
    assert f["bind_port"] == 8080 and f["quota_mbps"] == 8.0
    assert f["quota_bytes_per_s"] == 1_000_000


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


# ---------------------------------------------------------------------------
# HTTP 接口
# ---------------------------------------------------------------------------

@pytest.fixture
async def client():
    app = webconsole.build_app(hub(), webconsole.LogBuffer(),
                               __import__("logging").getLogger("t"))
    async with TestClient(TestServer(app)) as c:
        yield c


async def test_overview_endpoint(client):
    r = await client.get("/api/overview")
    assert r.status == 200
    body = await r.json()
    assert "fe_a" in body["frontends"]


async def test_no_write_routes(client):
    """只读契约：控制台不提供任何配置写接口——配置的修改入口是
    haproxy.cfg 与 YAML 文件本身。"""
    for method, path in (("put", "/api/frontends/fe_a"),
                         ("post", "/api/frontends"),
                         ("delete", "/api/frontends/fe_a")):
        r = await getattr(client, method)(path, data="{}")
        assert r.status in (404, 405), path


async def test_index_declares_utf8(client):
    """页面全是中文：Content-Type 不带 charset 时浏览器只能猜编码，
    中文 Windows 上会猜成 GBK，整页变乱码。两道声明都要在。"""
    r = await client.get("/")
    assert r.status == 200
    assert r.charset == "utf-8", "HTTP 头必须声明 charset"
    body = await r.read()
    # <meta charset> 必须落在前 1024 字节内，否则浏览器已经开始按猜测
    # 的编码解析了。
    assert b'<meta charset="utf-8">' in body[:1024]
    # 中文能按 UTF-8 正确解回来（内容本身没被写坏）。
    assert "控制台" in body.decode("utf-8")


async def test_sse_declares_utf8(client):
    r = await client.get("/api/stream")
    assert r.headers["Content-Type"].startswith("text/event-stream")
    assert "charset=utf-8" in r.headers["Content-Type"]
    r.close()


async def test_json_endpoints_declare_utf8(client):
    """日志接口会带中文消息，同样不能让浏览器猜。"""
    for path in ("/api/overview", "/api/logs", "/api/history"):
        r = await client.get(path)
        assert r.charset == "utf-8", path
