# tests.test_fleetweb —— 总控的发布枢纽与 HTTP 面。

from __future__ import annotations

import asyncio
import json
import logging

import pytest
from aiohttp.test_utils import TestClient, TestServer

from rl_limiter import fleetconfig, fleetgov, fleetweb

log = logging.getLogger("test")
TOKEN = "4d0f049407ac1f0523f1ef306948f85c64b6e5e95b701a7e"  # 写 API 令牌须过强度规则

FLEET_YAML = """\
total_egress_mbps: 300
nodes:
  - name: node1
    url: http://10.0.0.1:8090
    token: fe8890a02029cbe0d9a67dd1cb2cbd8f1daa11101d13ef31
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


def hub(nodes_fn=None):
    h = fleetweb.FleetHub("test", total_fn=lambda: 300.0,
                          params_fn=lambda: {"min_share_ratio": 0.2},
                          nodes_fn=nodes_fn)
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
    assert n1["release_pending"] is False
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
    # 节点清单视图跟着 YAML 走（真实进程里是热更后的 cur["cfg"]）。
    h = hub(nodes_fn=lambda: fleetconfig.load(str(yml)).nodes)
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


# ---------------------------------------------------------------------------
# 节点清单（后台手动添加/编辑/删除）
# ---------------------------------------------------------------------------

async def test_nodes_read_view_never_leaks_tokens(client):
    j = await (await client.get("/api/nodes")).json()
    assert j["nodes"] == [{"name": "node1", "url": "http://10.0.0.1:8090",
                           "console_url": "", "has_token": True,
                           "token_weak": False}]
    assert "fe8890a02029cbe0d9a67dd1cb2cbd8f1daa11101d13ef31" not in json.dumps(j)


async def test_node_writes_require_token(client):
    before = client.app["yml"].read_text(encoding="utf-8")
    for method, url in (("POST", "/api/nodes"), ("PUT", "/api/nodes/node1"),
                        ("DELETE", "/api/nodes/node1")):
        for hdr in ({}, auth("wrong")):
            r = await client.request(method, url, headers=hdr,
                                     json={"name": "x", "url": "http://x:1"})
            assert r.status == 401, (method, url)
    assert client.app["yml"].read_text(encoding="utf-8") == before
    assert not client.app["poke"].is_set()


async def test_add_edit_delete_node_roundtrip(client):
    yml, poke = client.app["yml"], client.app["poke"]
    r = await client.post("/api/nodes", headers=auth(), json={
        "name": "node2", "url": "http://10.0.0.2:8090", "token": "4b4ee4871702687022f6fe251e37f4939373663d60927da2"})
    assert r.status == 200, await r.text()
    assert poke.is_set()
    cfg = fleetconfig.load(str(yml))
    assert [(n.name, n.token) for n in cfg.nodes] == \
        [("node1", "fe8890a02029cbe0d9a67dd1cb2cbd8f1daa11101d13ef31"), ("node2", "4b4ee4871702687022f6fe251e37f4939373663d60927da2")]
    j = await (await client.get("/api/nodes")).json()
    assert [n["name"] for n in j["nodes"]] == ["node1", "node2"]

    # 重名不覆盖。
    r = await client.post("/api/nodes", headers=auth(), json={
        "name": "node2", "url": "http://10.0.0.99:8090"})
    assert r.status == 409
    assert fleetconfig.load(str(yml)).nodes[1].url == "http://10.0.0.2:8090"

    # 编辑：只改给出的字段；令牌保持。
    r = await client.put("/api/nodes/node2", headers=auth(),
                         json={"console_url": "http://pub:8092"})
    assert r.status == 200
    n2 = fleetconfig.load(str(yml)).nodes[1]
    assert (n2.console_url, n2.token) == ("http://pub:8092", "4b4ee4871702687022f6fe251e37f4939373663d60927da2")

    r = await client.delete("/api/nodes/node2", headers=auth())
    assert r.status == 200
    assert [n.name for n in fleetconfig.load(str(yml)).nodes] == ["node1"]


@pytest.mark.parametrize("method, url, body, status", [
    ("POST", "/api/nodes", {"name": "n2", "url": "ftp://x"}, 400),
    ("POST", "/api/nodes", {"name": "bad name", "url": "http://x:1"}, 400),
    ("POST", "/api/nodes", {"url": "http://x:1"}, 400),
    ("POST", "/api/nodes", ["not", "a", "dict"], 400),
    ("PUT", "/api/nodes/node1", {}, 400),
    ("PUT", "/api/nodes/node1", {"name": "renamed"}, 400),
    ("PUT", "/api/nodes/nope", {"token": "x"}, 404),
    ("DELETE", "/api/nodes/nope", None, 404),
])
async def test_node_write_errors(client, method, url, body, status):
    before = client.app["yml"].read_text(encoding="utf-8")
    r = await client.request(method, url, headers=auth(), json=body)
    assert r.status == status, await r.text()
    assert "error" in await r.json()
    assert client.app["yml"].read_text(encoding="utf-8") == before


async def test_node_writes_disabled_without_token(tmp_path):
    yml = tmp_path / "fleet.yaml"
    yml.write_text(FLEET_YAML, encoding="utf-8")
    app = fleetweb.build_fleet_app(
        hub(), fleetweb.LogBuffer(), log, yaml_path=str(yml), api_token="")
    async with TestClient(TestServer(app)) as c:
        r = await c.post("/api/nodes", headers=auth(),
                         json={"name": "n2", "url": "http://x:1"})
        assert r.status == 403


# ---------------------------------------------------------------------------
# 监控落盘反查 /api/metrics-log
# ---------------------------------------------------------------------------

async def test_metrics_log_disabled_says_how_to_enable(client):
    r = await client.get("/api/metrics-log")
    assert r.status == 404
    assert "RL_METRICS_LOG" in (await r.json())["error"]


async def test_metrics_log_query(tmp_path):
    path = tmp_path / "fm.jsonl"
    path.write_text("\n".join(json.dumps(x) for x in [
        {"ts": 1000, "kind": "fleet", "total_mbps": 100},
        {"ts": 1000, "kind": "node", "name": "a", "quota_mbps": 60},
        {"ts": 1000, "kind": "node", "name": "b", "quota_mbps": 40},
        {"ts": 1010, "kind": "event", "type": "quota", "node": "b"},
        {"ts": 5000, "kind": "fleet", "total_mbps": 0},
    ]) + "\n", encoding="utf-8")
    app = fleetweb.build_fleet_app(hub(), fleetweb.LogBuffer(), log,
                                   metrics_path=str(path))
    async with TestClient(TestServer(app)) as c:
        j = await (await c.get("/api/metrics-log",
                               params={"from": "900", "to": "2000"})).json()
        assert [r["kind"] for r in j["rows"]] == \
            ["fleet", "node", "node", "event"]
        assert (j["start"], j["end"], j["truncated"]) == (900, 2000, False)

        j = await (await c.get("/api/metrics-log", params={
            "from": "900", "to": "2000", "kind": "node,event",
            "node": "b"})).json()
        assert [(r["kind"], r.get("name") or r.get("type"))
                for r in j["rows"]] == [("node", "b"), ("event", "quota")]

        # ISO 8601（带时区）也认。
        j = await (await c.get("/api/metrics-log", params={
            "from": "1970-01-01T00:16:40+00:00",
            "to": "1970-01-01T00:30:00+00:00", "kind": "fleet"})).json()
        assert [r["ts"] for r in j["rows"]] == [1000]

        r = await c.get("/api/metrics-log", params={
            "from": "0", "to": "9999", "format": "jsonl"})
        assert r.status == 200
        assert "attachment" in r.headers["Content-Disposition"]
        assert len((await r.text()).splitlines()) == 5


@pytest.mark.parametrize("params", [
    {"from": "昨天"},
    {"kind": "fleet,bogus"},
    {"from": "2000", "to": "1000"},
    {"limit": "many"},
])
async def test_metrics_log_bad_params(tmp_path, params):
    app = fleetweb.build_fleet_app(hub(), fleetweb.LogBuffer(), log,
                                   metrics_path=str(tmp_path / "fm.jsonl"))
    async with TestClient(TestServer(app)) as c:
        r = await c.get("/api/metrics-log", params=params)
        assert r.status == 400 and (await r.json())["error"]


# ---------------------------------------------------------------------------
# 令牌强度：弱令牌 = 没有令牌
# ---------------------------------------------------------------------------

async def test_weak_fleet_token_disables_writes(tmp_path):
    """总控自己的 RL_API_TOKEN 不达标：带着它本身也改不了，403 说原因。"""
    yml = tmp_path / "fleet.yaml"
    yml.write_text(FLEET_YAML, encoding="utf-8")
    weak = "fleet-token"
    app = fleetweb.build_fleet_app(
        hub(), fleetweb.LogBuffer(), log, yaml_path=str(yml), api_token=weak)
    async with TestClient(TestServer(app)) as c:
        for method, url, body in (
                ("PUT", "/api/total-quota", {"quota_mbps": 1}),
                ("POST", "/api/nodes", {"name": "n2", "url": "http://x:1"})):
            r = await c.request(method, url, json=body, headers=auth(weak))
            assert r.status == 403
            err = (await r.json())["error"]
            assert "强度不足" in err and weak not in err
    assert yml.read_text(encoding="utf-8") == FLEET_YAML


async def test_weak_node_token_rejected_on_add_and_edit(client):
    before = client.app["yml"].read_text(encoding="utf-8")
    r = await client.post("/api/nodes", headers=auth(), json={
        "name": "node2", "url": "http://10.0.0.2:8090", "token": "node2-pass"})
    assert r.status == 400 and "强度不足" in (await r.json())["error"]
    r = await client.put("/api/nodes/node1", headers=auth(),
                         json={"token": "a" * 48})
    assert r.status == 400
    assert client.app["yml"].read_text(encoding="utf-8") == before


async def test_weak_node_token_in_yaml_shows_as_weak(tmp_path):
    """手写进 YAML 的弱节点令牌：加载时作废（只读观测），读接口标出来。"""
    yml = tmp_path / "fleet.yaml"
    yml.write_text("nodes:\n  - name: n1\n    url: http://x:1\n"
                   "    token: weakweak\n", encoding="utf-8")
    h = hub(nodes_fn=lambda: fleetconfig.load(str(yml)).nodes)
    app = fleetweb.build_fleet_app(h, fleetweb.LogBuffer(), log)
    async with TestClient(TestServer(app)) as c:
        j = await (await c.get("/api/nodes")).json()
    assert j["nodes"][0]["has_token"] is False
    assert j["nodes"][0]["token_weak"] is True


# ---------------------------------------------------------------------------
# 粘贴接入串批量导入 /api/nodes/import
# ---------------------------------------------------------------------------

IMP_TOK = "fe8890a02029cbe0d9a67dd1cb2cbd8f1daa11101d13ef31"


async def test_import_preview_then_commit(client):
    yml, poke = client.app["yml"], client.app["poke"]
    text = (f"install.sh: 完成\n"
            f"rl-node name=node2 url=http://10.0.0.2:8090 token={IMP_TOK}\n"
            f"rl-node name=node3 url=http://10.0.0.3:8090\n"
            f"rl-node name=node4 url=http://10.0.0.4:8090 token=weak\n")
    before = yml.read_text(encoding="utf-8")
    r = await client.post("/api/nodes/import", headers=auth(),
                          json={"text": text, "dry_run": True})
    assert r.status == 200
    j = await r.json()
    assert (j["add"], j["update"], j["error"], j["ignored_lines"]) == \
        (2, 0, 1, 1)
    assert [i["action"] for i in j["items"]] == ["add", "add", "error"]
    assert j["items"][0]["has_token"] is True
    assert IMP_TOK not in json.dumps(j)                  # 不回显令牌
    assert yml.read_text(encoding="utf-8") == before and not poke.is_set()

    r = await client.post("/api/nodes/import", headers=auth(),
                          json={"text": text})
    j = await r.json()
    assert (j["dry_run"], j["add"]) == (False, 2) and poke.is_set()
    names = [n.name for n in fleetconfig.load(str(yml)).nodes]
    assert names == ["node1", "node2", "node3"]


@pytest.mark.parametrize("body, status", [
    ({"text": "没有接入串"}, 400),
    ({"lines": []}, 400),
])
async def test_import_bad_requests(client, body, status):
    r = await client.post("/api/nodes/import", headers=auth(), json=body)
    assert r.status == status


async def test_import_requires_fleet_token(client):
    r = await client.post("/api/nodes/import", json={
        "text": "rl-node name=n9 url=http://x:1", "dry_run": True})
    assert r.status == 401
