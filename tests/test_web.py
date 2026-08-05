# tests.test_aggweb —— hap-agg 的 Web 视图与目标管理 API。
# 写接口鉴权：HAP_AGG_TOKEN（Bearer / X-API-Token），未配置整体 403。

from __future__ import annotations

import asyncio
import logging

import pytest
from aiohttp.test_utils import TestClient, TestServer

from hap_agg import config as aggconfig
from hap_agg import model
from hap_agg import sampler as agg
from hap_agg import web as aggweb

log = logging.getLogger("t.aggweb")
TOKEN = "agg-token-1"


class FakeClient:
    def __init__(self):
        self.fes = [model.FrontendStat(name="fe_main", bytes_out=0,
                                       conn_cur=2, mode="tcp")]
        self.info = model.InstanceStat(curr_conns=5, max_conn=100,
                                       idle_pct=90, uptime_s=60)

    async def show_stat(self):
        return list(self.fes)

    async def show_info(self):
        return self.info


def make_agg(targets_text="t1 10.0.0.1:9999\n"):
    return agg.Aggregator(agg.parse_targets_text(targets_text), log,
                          client_factory=lambda t: FakeClient())


@pytest.fixture
async def client(tmp_path):
    yml = tmp_path / "agg.yaml"
    yml.write_text("targets:\n  - t1 10.0.0.1:9999\n", encoding="utf-8")
    a = make_agg()
    hub = aggweb.AggHub("test")
    app = aggweb.build_app(hub, a, log, yaml_path=str(yml), api_token=TOKEN)
    app["agg"], app["hub"], app["yml"] = a, hub, yml
    async with TestClient(TestServer(app)) as c:
        yield c


def auth(token=TOKEN):
    return {"X-API-Token": token}


async def seed_snap(client):
    snap = await client.app["agg"].tick(1.0)
    client.app["hub"].record(snap)
    return snap


# ---------------------------------------------------------------------------
# 读接口
# ---------------------------------------------------------------------------

async def test_overview_and_history(client):
    await seed_snap(client)
    o = await (await client.get("/api/overview")).json()
    assert o["targets"] == [{"name": "t1", "addr": "10.0.0.1:9999"}]
    assert o["latest"]["total"]["targets_ok"] == 1
    assert o["latest"]["targets"]["t1"]["conn"] == 5
    h = await (await client.get("/api/history")).json()
    assert len(h["snapshots"]) == 1


async def test_metrics_endpoint(client):
    await seed_snap(client)
    r = await client.get("/metrics")
    assert r.status == 200
    body = await r.text()
    assert "hap_agg_targets_up 1" in body
    assert 'hap_agg_target_up{target="t1",addr="10.0.0.1:9999"} 1' in body
    assert 'hap_agg_target_connections{target="t1",addr="10.0.0.1:9999"} 5' in body
    assert 'hap_agg_frontend_connections{frontend="fe_main"} 2' in body
    assert "# TYPE hap_agg_total_rate_out_bytes_per_second gauge" in body


async def test_index_declares_utf8(client):
    r = await client.get("/")
    assert r.status == 200 and r.charset == "utf-8"
    body = await r.read()
    assert b'<meta charset="utf-8">' in body[:1024]
    assert "聚合" in body.decode("utf-8")


async def test_sse_stream_ends_on_hub_close(client):
    """停机回归（与 webconsole 同一教训）：hub.close() 必须让挂着的
    SSE 立刻收尾，否则优雅停机被在途请求卡住。"""
    h = client.app["hub"]
    resp = await client.get("/api/stream")
    await seed_snap(client)
    line = await asyncio.wait_for(resp.content.readline(), 2)
    assert line.startswith(b"data:")
    h.close()
    await asyncio.wait_for(resp.content.read(), 2)
    assert resp.content.at_eof()


# ---------------------------------------------------------------------------
# 写接口：批量导入 / 删除
# ---------------------------------------------------------------------------

async def test_import_requires_token(client):
    r = await client.post("/api/targets", json={"text": "10.0.0.2:9999"})
    assert r.status == 401
    r = await client.post("/api/targets", json={"text": "10.0.0.2:9999"},
                          headers=auth("wrong"))
    assert r.status == 401


async def test_write_disabled_without_configured_token(tmp_path):
    yml = tmp_path / "agg.yaml"
    yml.write_text("targets: []\n", encoding="utf-8")
    app = aggweb.build_app(aggweb.AggHub("t"), make_agg(""), log,
                           yaml_path=str(yml), api_token="")
    async with TestClient(TestServer(app)) as c:
        r = await c.post("/api/targets", json={"text": "10.0.0.2:9999"},
                         headers=auth())
        assert r.status == 403
        assert "HAP_AGG_TOKEN" in (await r.json())["error"]


async def test_import_applies_and_persists(client):
    r = await client.post("/api/targets", headers=auth(), json={
        "text": "10.0.0.2:9999\nsg-03 10.0.0.3:9999\n"})
    assert r.status == 200
    body = await r.json()
    assert body["added"] == ["10.0.0.2:9999", "sg-03"] and body["total"] == 3
    # 立即生效：下一拍就采样新目标。
    snap = await seed_snap(client)
    assert set(snap["targets"]) == {"t1", "10.0.0.2:9999", "sg-03"}
    # 回写 YAML：用配置加载链读回完全一致。
    cfg = aggconfig.load(str(client.app["yml"]))
    assert {t.name for t in cfg.targets} == {"t1", "10.0.0.2:9999", "sg-03"}


async def test_import_is_idempotent(client):
    r = await client.post("/api/targets", headers=auth(),
                          json={"text": "t1 10.0.0.1:9999\n"})
    assert (await r.json())["added"] == []


async def test_import_rejects_bad_batch_atomically(client):
    """一行错整批拒绝：目标集与 YAML 都不动（半批导入比报错难收拾）。"""
    before = client.app["yml"].read_text(encoding="utf-8")
    r = await client.post("/api/targets", headers=auth(), json={
        "text": "10.0.0.2:9999\nbad-line-without-port\n"})
    assert r.status == 400
    assert "缺少端口" in (await r.json())["error"]
    assert len(client.app["agg"].targets()) == 1
    assert client.app["yml"].read_text(encoding="utf-8") == before


async def test_import_name_conflict_rejected(client):
    r = await client.post("/api/targets", headers=auth(),
                          json={"text": "t1 9.9.9.9:1234\n"})
    assert r.status == 400
    assert "地址不同" in (await r.json())["error"]


async def test_delete_target(client):
    r = await client.delete("/api/targets/t1", headers=auth())
    assert r.status == 200 and (await r.json())["removed"] is True
    assert client.app["agg"].targets() == []
    assert aggconfig.load(str(client.app["yml"])).targets == []
    r = await client.delete("/api/targets/t1", headers=auth())
    assert (await r.json())["removed"] is False


async def test_no_write_routes_without_yaml_path():
    app = aggweb.build_app(aggweb.AggHub("t"), make_agg(""), log)
    async with TestClient(TestServer(app)) as c:
        r = await c.post("/api/targets", json={"text": "x"}, headers=auth())
        assert r.status in (404, 405)
