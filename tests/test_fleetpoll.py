# tests.test_fleetpoll —— 节点轮询与下发（aiohttp 假节点，不出测试进程）。

from __future__ import annotations

import asyncio
import logging

import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer

from rl_limiter import fleetpoll
from rl_limiter.fleetconfig import FleetNode

log = logging.getLogger("test")


def overview_payload(quota=120.0, egress_mbps=80.0, degraded=False,
                     executable=True, with_regulation=True):
    ov = {
        "instance_quota_mbps": quota,
        "instance_quota_executable": executable,
        "haproxy": {"degraded": degraded},
        "latest": {
            "units": {
                "fe_a": {"mean10_bytes_per_s": 30e6 / 8},   # 30 Mbps
                "fe_b": {"mean10_bytes_per_s": 20e6 / 8},   # 20 Mbps
            },
            "instance": {"rate_in_bytes_per_s": 2e6 / 8},   # 2 Mbps
        },
    }
    if with_regulation:
        ov["latest"]["regulation"] = {"instance": {
            "target_mbps": quota, "gate_mbps": 1.0, "gate_max_mbps": 10.0,
            "ingress_mbps": 1.5, "egress_mbps": egress_mbps, "landed": True,
        }}
    return ov


class FakeNode:
    """可注入行为的假节点：载荷/状态码/超时开关 + PUT 记录。"""

    def __init__(self):
        self.payload = overview_payload()
        self.fail = False            # True → overview 500
        self.hang = False            # True → 挂住直到客户端超时
        self.put_status = 200
        self.puts: list = []
        app = web.Application()
        app.router.add_get("/api/overview", self._overview)
        app.router.add_put("/api/instance-quota", self._put)
        self.server = TestServer(app)

    async def _overview(self, _req):
        if self.hang:
            await asyncio.sleep(30)
        if self.fail:
            return web.json_response({"error": "boom"}, status=500)
        return web.json_response(self.payload)

    async def _put(self, req):
        self.puts.append({
            "token": req.headers.get("X-API-Token", ""),
            "body": await req.json(),
        })
        if self.put_status != 200:
            return web.json_response({"error": "拒绝"},
                                     status=self.put_status)
        return web.json_response({"ok": True})

    @property
    def url(self):
        return str(self.server.make_url("")).rstrip("/")


@pytest.fixture
async def fake():
    n = FakeNode()
    await n.server.start_server()
    yield n
    await n.server.close()


@pytest.fixture
async def poller():
    p = fleetpoll.NodePoller(log, timeout_s=0.5, offline_after=3)
    yield p
    await p.close()


async def test_poll_extracts_regulation_fields(fake, poller):
    poller.set_nodes([FleetNode(name="n1", url=fake.url, token="t")])
    await poller.poll_once()
    s = poller.states()[0]
    assert s.online and not s.degraded and s.executable
    assert s.quota_mbps == 120.0
    assert s.egress_mbps == 80.0          # regulation 口径
    assert s.ingress_mbps == 1.5
    assert s.landed is True and s.error == ""


async def test_poll_falls_back_to_units_sum(fake, poller):
    """节点还没在调（无 regulation）：退回 units mean10 求和口径。"""
    fake.payload = overview_payload(with_regulation=False)
    poller.set_nodes([FleetNode(name="n1", url=fake.url, token="t")])
    await poller.poll_once()
    s = poller.states()[0]
    assert s.egress_mbps == pytest.approx(50.0)    # 30 + 20
    assert s.ingress_mbps == pytest.approx(2.0)
    assert s.landed is None


async def test_offline_after_consecutive_failures_then_recover(fake, poller):
    poller.set_nodes([FleetNode(name="n1", url=fake.url, token="t")])
    await poller.poll_once()
    s = poller.states()[0]
    s.demand_ewma_mbps = 55.0
    fake.fail = True
    for i in range(3):
        await poller.poll_once()
        assert s.online is (i < 2)        # 第 3 次失败才判离线
    assert s.fail_count == 3 and s.error
    # 恢复：一次成功即复活，且需求 EWMA 被重置（陈旧均值不参与分配）。
    fake.fail = False
    await poller.poll_once()
    assert s.online and s.fail_count == 0
    assert s.demand_ewma_mbps is None


async def test_slow_node_does_not_block_others(fake, poller):
    slow = FakeNode()
    slow.hang = True
    await slow.server.start_server()
    try:
        poller.set_nodes([FleetNode(name="fast", url=fake.url, token="t"),
                          FleetNode(name="slow", url=slow.url, token="t")])
        await asyncio.wait_for(poller.poll_once(), timeout=5)
        by = {s.name: s for s in poller.states()}
        assert by["fast"].online
        assert by["slow"].fail_count == 1      # 超时只算它自己的失败
    finally:
        await slow.server.close()


async def test_apply_puts_with_token_and_records_applied(fake, poller):
    poller.set_nodes([FleetNode(name="n1", url=fake.url, token="sek")])
    await poller.poll_once()
    s = poller.states()[0]
    await poller.apply([(s, 66.6)])
    assert fake.puts == [{"token": "sek", "body": {"quota_mbps": 66.6}}]
    assert s.applied_mbps == 66.6 and s.write_err == ""


async def test_apply_401_records_error_not_frozen(fake, poller):
    fake.put_status = 401
    poller.set_nodes([FleetNode(name="n1", url=fake.url, token="bad")])
    await poller.poll_once()
    s = poller.states()[0]
    await poller.apply([(s, 50.0)])
    assert "401" in s.write_err
    assert s.applied_mbps is None
    assert s.online                        # 观测不受下发失败影响


async def test_remove_node_keeps_quota_untouched(fake, poller):
    poller.set_nodes([FleetNode(name="n1", url=fake.url, token="t"),
                      FleetNode(name="n2", url=fake.url, token="t")])
    await poller.poll_once()
    poller.set_nodes([FleetNode(name="n1", url=fake.url, token="t")])
    assert [s.name for s in poller.states()] == ["n1"]
    assert fake.puts == []                 # 移除绝不触发任何下发


async def test_poll_reads_source_or_haproxy_alias(fake, poller):
    """节点 overview 的采样源视图：只给新键 source / 只给旧键 haproxy /
    两者都给，degraded 都要读得到（节点新旧版本混跑时总控照常工作）。"""
    poller.set_nodes([FleetNode(name="n1", url=fake.url, token="t")])
    base = overview_payload(degraded=True)
    # 只给 source。
    fake.payload = dict(base)
    fake.payload["source"] = fake.payload.pop("haproxy")
    await poller.poll_once()
    assert poller.states()[0].degraded is True
    # 只给 haproxy（旧节点）。
    fake.payload = overview_payload(degraded=False)
    await poller.poll_once()
    assert poller.states()[0].degraded is False
