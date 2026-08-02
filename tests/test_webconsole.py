# tests.test_webconsole —— 控制台的 StatusHub（快照合成/订阅/概览）与
# 写接口的应答契约。HTTP 层用 aiohttp 的测试工具直接打真实路由。

from __future__ import annotations

import pytest
from aiohttp.test_utils import TestClient, TestServer

from rl_limiter import model, webconsole


def fe(name="fe_a", quota_mbps=8.0, port=8080):
    return model.FrontendConfig(
        name=name, bind_port=port, quota_mbps=quota_mbps,
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
    """界面的编辑表单以 overview 的 frontends 为初值。"""
    o = hub().overview()
    f = o["frontends"]["fe_a"]
    assert f["bind_port"] == 8080 and f["quota_mbps"] == 8.0
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
                         ("delete", "/api/frontends/fe_a"),
                         ("put", "/api/limit")):
        r = await getattr(client, method)(path, data="{}")
        assert r.status == 409, path
        assert "MySQL" in (await r.json())["error"]


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


# ---------------------------------------------------------------------------
# 限速范围（实例级）
# ---------------------------------------------------------------------------

def test_overview_exposes_limit_scope():
    """界面必须能显示当前限速范围。看不到它的话，一台整机限速的机器上
    每个端口都标着自己的限额，看起来像各限各的，实际全被一个总闸门罩着。"""
    h = hub()
    # 默认：整机范围 + 没设限额 = 不限速。
    assert h.overview()["limit"] == {"scope": "host", "host_quota_mbps": None}
    h.update_config(model.ControllerConfig(
        version=2, frontends=[fe()], limit_scope="host", host_quota_mbps=2000.0))
    assert h.overview()["limit"] == {"scope": "host", "host_quota_mbps": 2000.0}


def test_limit_payload_distinguishes_unset_from_zero():
    """**没设限额（= 不限速，默认）与限成 0（= 配错了）必须分开。**

    合并成一个 0 的话，"我配了整机限速但把限额打错了"会静默变成不限速
    ——静默不限速是这个项目最不能接受的故障。
    """
    from rl_limiter import dbconfig
    # 没填 / 填空串 = 不限速，合法。
    assert dbconfig._validate_limit_payload({"limit_scope": "host"})[1] is None
    assert dbconfig._validate_limit_payload(
        {"limit_scope": "host", "host_quota_mbps": None})[1] is None
    assert dbconfig._validate_limit_payload(
        {"limit_scope": "host", "host_quota_mbps": ""})[1] is None
    # 明确填了 0 / 负数 = 配错了，拒。
    for bad in (0, -1, "0"):
        with pytest.raises(ValueError, match="正数"):
            dbconfig._validate_limit_payload(
                {"limit_scope": "host", "host_quota_mbps": bad})


def test_limit_payload_rejects_unknown_scope():
    from rl_limiter import dbconfig
    with pytest.raises(ValueError, match="limit_scope"):
        dbconfig._validate_limit_payload({"limit_scope": "global"})
    with pytest.raises(ValueError, match="limit_scope"):
        dbconfig._validate_limit_payload({})


def test_limit_payload_accepts_both_scopes():
    from rl_limiter import dbconfig
    assert dbconfig._validate_limit_payload(
        {"limit_scope": "frontend"}) == ("frontend", None)
    assert dbconfig._validate_limit_payload(
        {"limit_scope": "host", "host_quota_mbps": 1500.5}) == ("host", 1500.5)
    # 界面上的输入框给过来的是字符串，别在这儿卡住。
    assert dbconfig._validate_limit_payload(
        {"limit_scope": "host", "host_quota_mbps": "1500.5"}) == ("host", 1500.5)


def test_limit_payload_rejects_non_numeric_quota():
    from rl_limiter import dbconfig
    with pytest.raises(ValueError, match="必须是数字"):
        dbconfig._validate_limit_payload(
            {"limit_scope": "host", "host_quota_mbps": "一千"})


def test_limit_payload_validation_is_the_same_one_config_uses():
    """控制台与"直接写库后被加载"两条路径的接受集合必须完全一致，否则会
    出现"界面上保存成功了、服务却加载不了这份配置"。这里盯的是它确实走的
    config._validate，而不是另抄了一份规则。"""
    from rl_limiter import config as configmod
    from rl_limiter import dbconfig
    calls = []
    orig = configmod._validate

    def spy(cfg):
        calls.append(cfg.haproxy.limit_scope)
        return orig(cfg)
    configmod._validate = spy
    try:
        dbconfig._validate_limit_payload({"limit_scope": "host",
                                          "host_quota_mbps": 100})
    finally:
        configmod._validate = orig
    assert calls == ["host"]


def test_hidden_quota_box_is_actually_hidden_by_css():
    """`[hidden]` 的 display:none 来自 UA 样式表，优先级最低——任何 class
    选择器上的 display 都会盖掉它。整机限额输入框正是这么被"藏"漏的：
    JS 把 hidden 设成 true 了，框子照样显示在页面上。

    这条盯的是那个补丁还在。"""
    css = (webconsole._STATIC_DIR / "index.html").read_text(encoding="utf-8")
    assert ".limit-quota { display: flex" in css
    assert ".limit-quota[hidden] { display: none; }" in css, (
        "给 .limit-quota 设了 display 就必须补 [hidden] 那一条，否则藏不住")


def test_radio_width_is_reset_from_the_global_input_rule():
    """全局 `input, select { width: 100% }` 是给文本框写的。单选框套上去会
    撑满整行、把旁边的文案挤成竖排——实测过一次。"""
    css = (webconsole._STATIC_DIR / "index.html").read_text(encoding="utf-8")
    assert "input, select {" in css and "width: 100%" in css
    i = css.index(".limit-row input[type=radio] {")
    assert "width: auto" in css[i:i + 200]
