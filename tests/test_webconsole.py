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
    h = hub()
    app = webconsole.build_app(h, webconsole.LogBuffer(),
                               __import__("logging").getLogger("t"))
    app["hub"] = h          # 测试里要往里灌样本
    async with TestClient(TestServer(app)) as c:
        yield c


async def test_overview_endpoint(client):
    r = await client.get("/api/overview")
    assert r.status == 200
    body = await r.json()
    assert "fe_a" in body["frontends"]


async def test_no_write_routes_without_yaml_path(client):
    """不给 yaml_path 时（测试/极简形态）连写路由都不挂载：负载均衡
    配置（frontends 本体）在任何形态下都没有写接口——那以 haproxy.cfg
    为唯一权威。"""
    for method, path in (("put", "/api/frontends/fe_a"),
                         ("post", "/api/frontends"),
                         ("delete", "/api/frontends/fe_a"),
                         ("put", "/api/quotas/fe_a"),
                         ("put", "/api/nic-quota")):
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


async def test_prometheus_metrics_endpoint(client):
    """/metrics：Prometheus 文本格式，frontend/instance 两级指标齐全，
    与控制台曲线同源（都取自 StatusHub 的最新一拍）。"""
    r = await client.get("/metrics")
    assert r.status == 200
    assert r.headers["Content-Type"].startswith("text/plain")
    body = await r.text()
    assert 'rl_limiter_info{' in body
    # 限额来自配置视图：fe_a 登记 8 Mbps = 1_000_000 bytes/s。
    assert ('rl_limiter_frontend_quota_bytes_per_second{frontend="fe_a"} '
            "1000000.0") in body
    # HELP/TYPE 注释要在（Prometheus 解析器靠它识别指标）。
    assert "# TYPE rl_limiter_frontend_quota_bytes_per_second gauge" in body


async def test_metrics_carries_latest_sample(client):
    """采过样之后，速率与超限标记跟着最新一拍走。"""
    h = client.app["hub"]
    h.record(1.0, [usage(rate=500.0, mean10=2_000_000, conn=7)])
    body = await (await client.get("/metrics")).text()
    assert 'rl_limiter_frontend_rate_bytes_per_second{frontend="fe_a"} 500.0' in body
    assert 'rl_limiter_frontend_connections{frontend="fe_a"} 7' in body
    # mean10 2M > 限额 1M → over=1（与控制台的超限徽标同一判定）。
    assert 'rl_limiter_frontend_over_quota{frontend="fe_a"} 1' in body


async def test_sse_stream_ends_on_hub_close(client):
    """停机回归：hub.close() 必须让挂着的 /api/stream 立刻收尾。

    否则优雅停机会被 SSE 卡住——aiohttp 的 cleanup 等在途请求，SSE 又
    永远不返回，实测控制台页面开着时 systemctl restart 要干等 60s+。"""
    import asyncio as _asyncio
    h = client.app["hub"]
    resp = await client.get("/api/stream")
    h.record(1.0, [usage(rate=5.0)])
    # 等到确实收到过数据帧（连接已建立、处理器在 q.get() 上等着）
    line = await _asyncio.wait_for(resp.content.readline(), 2)
    assert line.startswith(b"data:")
    h.close()
    # 处理器应随哨兵返回，连接随之走到 EOF——限时读完剩余字节。
    await _asyncio.wait_for(resp.content.read(), 2)
    assert resp.content.at_eof()


async def test_json_endpoints_declare_utf8(client):
    """日志接口会带中文消息，同样不能让浏览器猜。"""
    for path in ("/api/overview", "/api/logs", "/api/history"):
        r = await client.get(path)
        assert r.charset == "utf-8", path


# ---------------------------------------------------------------------------
# 写 API（限额修改：令牌鉴权 + YAML 回写 + poke 热生效）
# ---------------------------------------------------------------------------

import asyncio

from rl_limiter import config as configmod

TOKEN = "test-token-123"

BASE_YAML = """\
haproxy:
  socket_path: /run/haproxy/admin.sock
  cfg_path: /etc/haproxy/haproxy.cfg
quotas:
  fe_a: 8
"""


def hub_with_nic(nic=0.0):
    h = webconsole.StatusHub("test", version_fn=lambda: 1)
    h.update_config(model.ControllerConfig(
        version=1, frontends=[fe()], nic_quota_mbps=nic))
    return h


@pytest.fixture
async def wclient(tmp_path):
    """带写 API 的控制台：真实 YAML 文件 + 令牌 + poke 事件。"""
    yml = tmp_path / "config.yaml"
    yml.write_text(BASE_YAML, encoding="utf-8")
    poke = asyncio.Event()
    h = hub()
    app = webconsole.build_app(
        h, webconsole.LogBuffer(), __import__("logging").getLogger("t"),
        yaml_path=str(yml), api_token=TOKEN,
        known_frontends_fn=lambda: {"fe_a", "fe_b"}, poke=poke)
    app["yml"], app["poke"], app["hub"] = yml, poke, h
    async with TestClient(TestServer(app)) as c:
        yield c


def auth(token=TOKEN):
    return {"X-API-Token": token}


async def test_write_requires_token(wclient):
    """没带令牌/带错令牌 → 401，文件一个字节不动。"""
    before = wclient.app["yml"].read_text(encoding="utf-8")
    r = await wclient.put("/api/quotas/fe_a", json={"quota_mbps": 20})
    assert r.status == 401
    r = await wclient.put("/api/quotas/fe_a", json={"quota_mbps": 20},
                          headers=auth("wrong"))
    assert r.status == 401
    assert wclient.app["yml"].read_text(encoding="utf-8") == before
    assert not wclient.app["poke"].is_set()


async def test_write_disabled_without_configured_token(tmp_path):
    """服务端未配置 RL_API_TOKEN → 写接口整体 403（错误信息说明怎么
    启用），绝不允许"没配令牌就人人可改"。"""
    yml = tmp_path / "config.yaml"
    yml.write_text(BASE_YAML, encoding="utf-8")
    app = webconsole.build_app(
        hub(), webconsole.LogBuffer(), __import__("logging").getLogger("t"),
        yaml_path=str(yml), api_token="")
    async with TestClient(TestServer(app)) as c:
        r = await c.put("/api/quotas/fe_a", json={"quota_mbps": 20},
                        headers=auth())
        assert r.status == 403
        assert "RL_API_TOKEN" in (await r.json())["error"]


async def test_put_quota_writes_yaml_and_pokes(wclient):
    r = await wclient.put("/api/quotas/fe_a", json={"quota_mbps": 20},
                          headers=auth())
    assert r.status == 200
    cfg = configmod.load(str(wclient.app["yml"]))
    assert cfg.quotas["fe_a"] == 20.0
    assert wclient.app["poke"].is_set(), "回写后必须 poke，热生效不等轮询"


async def test_put_quota_accepts_bearer_header(wclient):
    r = await wclient.put("/api/quotas/fe_a", json={"quota_mbps": 30},
                          headers={"Authorization": f"Bearer {TOKEN}"})
    assert r.status == 200
    assert configmod.load(str(wclient.app["yml"])).quotas["fe_a"] == 30.0


async def test_put_quota_zero_is_explicit_unlimited(wclient):
    r = await wclient.put("/api/quotas/fe_a", json={"quota_mbps": 0},
                          headers=auth())
    assert r.status == 200
    assert configmod.load(str(wclient.app["yml"])).quotas["fe_a"] == 0.0


async def test_put_quota_unknown_frontend_rejected(wclient):
    """cfg 里没有的段名 → 400 并列出现有段名：写进去也只会被 cfgparse
    忽略并 warn，不如在门口就说清楚（多半是拼错了）。"""
    before = wclient.app["yml"].read_text(encoding="utf-8")
    r = await wclient.put("/api/quotas/fe_typo", json={"quota_mbps": 20},
                          headers=auth())
    assert r.status == 400
    body = await r.json()
    assert "fe_typo" in body["error"] and "fe_a" in body["error"]
    assert wclient.app["yml"].read_text(encoding="utf-8") == before


@pytest.mark.parametrize("body", [
    {"quota_mbps": -1},          # 校验链拒绝（负数）
    {"quota_mbps": "abc"},       # 不是数字
    {"quota_mbps": True},        # bool 不是数字
    {},                          # 缺字段
])
async def test_put_quota_bad_body_rejected(wclient, body):
    r = await wclient.put("/api/quotas/fe_a", json=body, headers=auth())
    assert r.status == 400


async def test_delete_quota(wclient):
    r = await wclient.delete("/api/quotas/fe_a", headers=auth())
    assert r.status == 200 and (await r.json())["removed"] is True
    assert configmod.load(str(wclient.app["yml"])).quotas == {}
    # 再删一次：没登记，removed=False，也不 poke。
    wclient.app["poke"].clear()
    r = await wclient.delete("/api/quotas/fe_a", headers=auth())
    assert (await r.json())["removed"] is False
    assert not wclient.app["poke"].is_set()


async def test_put_and_delete_nic_quota(wclient):
    r = await wclient.put("/api/nic-quota", json={"quota_mbps": 800},
                          headers=auth())
    assert r.status == 200
    assert configmod.load(str(wclient.app["yml"])).nic_quota_mbps == 800.0
    r = await wclient.delete("/api/nic-quota", headers=auth())
    assert r.status == 200
    assert configmod.load(str(wclient.app["yml"])).nic_quota_mbps == 0.0


async def test_read_endpoints_need_no_token(wclient):
    """读接口不受令牌影响（读的安全边界是绑定地址 + 防火墙）。"""
    for path in ("/api/overview", "/api/history", "/metrics", "/"):
        r = await wclient.get(path)
        assert r.status == 200, path


def test_overview_carries_nic_quota():
    o = hub_with_nic(800.0).overview()
    assert o["nic_quota_mbps"] == 800.0
    assert o["nic_quota_bytes_per_s"] == 100_000_000.0


def test_metrics_carries_nic_quota():
    body = webconsole.render_prometheus(hub_with_nic(800.0))
    assert "rl_limiter_nic_quota_bytes_per_second 100000000.0" in body
