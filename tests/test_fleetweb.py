# tests.test_fleetweb —— 总控的发布枢纽与 HTTP 面。

from __future__ import annotations

import asyncio
import logging

import pytest
from aiohttp.test_utils import TestClient, TestServer

from rl_limiter import fleetconfig, fleetgov, fleetweb

log = logging.getLogger("test")
TOKEN = "fleet-token"

FLEET_YAML = """\
total_egress_mbps: 300
nodes:
  - name: node1
    url: http://10.0.0.1:8090
    token: t1
"""


def snap(ts=1.0, total=300.0):
    s = fleetgov.NodeState("node1", token="t1")
    s.online = True
    s.quota_mbps = 120.0
    s.egress_mbps = 80.0
    s.ingress_mbps = 1.5
    s.desired_mbps = 110.0
    off = fleetgov.NodeState("node2", token="t2")
    off.quota_mbps = 50.0
    return fleetweb.fleet_snapshot(ts, [s, off], total)


def hub():
    h = fleetweb.FleetHub("test", total_fn=lambda: 300.0,
                          params_fn=lambda: {"min_share_ratio": 0.2})
    return h


def test_snapshot_aggregates_and_accounts_frozen():
    f = snap()
    assert f["egress_mbps"] == 80.0            # 只计在线节点
    assert f["nodes_online"] == 1 and f["nodes_total"] == 2
    # 已分配合计 = 可控分配 110 + 冻结记账 50。
    assert f["allocated_mbps"] == 160.0
    n1 = f["nodes"]["node1"]
    assert n1["util"] == pytest.approx(80 / 120, abs=1e-3)
    assert n1["controllable"] is True
    # 快照里绝不带令牌。
    assert "token" not in n1
    assert f["nodes"]["node2"]["online"] is False


def test_hub_overview_and_history():
    h = hub()
    assert h.overview()["latest"] is None
    h.record(snap(1.0))
    h.record(snap(2.0))
    o = h.overview()
    assert o["total_quota_mbps"] == 300.0
    assert o["allocation"]["min_share_ratio"] == 0.2
    assert o["latest"]["ts"] == 2.0
    assert len(h.history()) == 2


@pytest.fixture
async def client(tmp_path):
    yml = tmp_path / "fleet.yaml"
    yml.write_text(FLEET_YAML, encoding="utf-8")
    poke = asyncio.Event()
    h = hub()
    h.record(snap())
    app = fleetweb.build_fleet_app(
        h, fleetweb.LogBuffer(), log,
        yaml_path=str(yml), api_token=TOKEN, poke=poke)
    app["yml"], app["poke"] = yml, poke
    async with TestClient(TestServer(app)) as c:
        yield c


def auth(token=TOKEN):
    return {"X-API-Token": token}


async def test_read_endpoints(client):
    o = await (await client.get("/api/overview")).json()
    assert o["total_quota_mbps"] == 300.0
    h = await (await client.get("/api/history")).json()
    assert len(h["snapshots"]) == 1
    page = await (await client.get("/")).text()
    assert "总控制台" in page


async def test_put_total_quota_auth_and_validation(client):
    before = client.app["yml"].read_text(encoding="utf-8")
    r = await client.put("/api/total-quota", json={"quota_mbps": 500})
    assert r.status == 401
    r = await client.put("/api/total-quota", json={"quota_mbps": 500},
                         headers=auth("wrong"))
    assert r.status == 401
    r = await client.put("/api/total-quota", json={"quota_mbps": -1},
                         headers=auth())
    assert r.status == 400
    r = await client.put("/api/total-quota", json={"quota_mbps": True},
                         headers=auth())
    assert r.status == 400
    assert client.app["yml"].read_text(encoding="utf-8") == before
    assert not client.app["poke"].is_set()


async def test_put_total_quota_writes_yaml_and_pokes(client):
    r = await client.put("/api/total-quota", json={"quota_mbps": 500},
                         headers=auth())
    assert r.status == 200
    cfg = fleetconfig.load(str(client.app["yml"]))
    assert cfg.total_egress_mbps == 500.0
    assert cfg.nodes[0].name == "node1"        # 其它段原样保留
    assert client.app["poke"].is_set()
    # DELETE = 置 0（纯观察）。
    r = await client.delete("/api/total-quota", headers=auth())
    assert r.status == 200
    assert fleetconfig.load(str(client.app["yml"])).total_egress_mbps == 0.0


async def test_write_disabled_without_token(tmp_path):
    yml = tmp_path / "fleet.yaml"
    yml.write_text(FLEET_YAML, encoding="utf-8")
    app = fleetweb.build_fleet_app(
        hub(), fleetweb.LogBuffer(), log, yaml_path=str(yml), api_token="")
    async with TestClient(TestServer(app)) as c:
        r = await c.put("/api/total-quota", json={"quota_mbps": 1},
                        headers=auth())
        assert r.status == 403


async def test_sse_stream_delivers_latest_frame(client):
    resp = await client.get("/api/stream")
    line = await asyncio.wait_for(resp.content.readline(), timeout=2)
    assert line.startswith(b"data: ")
    resp.close()
