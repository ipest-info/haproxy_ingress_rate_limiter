# tests.test_fleetmain —— 总控进程主体的端到端行为（真 HTTP，假节点）。
#
# 钉住"取消总量控制"的完整链路：页面/API 把总带宽置 0 → 回写 YAML →
# 热更 → **立即**（不等分配周期）向各节点 DELETE 实例总限速，节点恢复
# 不限。此前的行为是"停止分配、各节点停在最后一次分配值上"，运维点了
# 取消、节点却还限着。

from __future__ import annotations

import asyncio
import json
import logging
import socket

import aiohttp

from rl_limiter import fleetconfig, fleetmain, fleetmetrics

from .test_fleetpoll import FakeNode

log = logging.getLogger("test")
TOKEN = "fleet-token"


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


async def _until(cond, timeout=5.0):
    for _ in range(int(timeout / 0.05)):
        if cond():
            return
        await asyncio.sleep(0.05)
    raise AssertionError("等待超时")


async def test_cancel_total_releases_nodes_immediately(tmp_path):
    node = FakeNode()
    await node.server.start_server()
    yml = tmp_path / "fleet.yaml"
    yml.write_text(
        "total_egress_mbps: 0\n"
        "alloc_interval_s: 10\n"
        "nodes:\n"
        f"  - name: n1\n    url: {node.url}\n    token: node-tok\n",
        encoding="utf-8")
    cfg = fleetconfig.load(str(yml))
    port = _free_port()
    mlog = fleetmetrics.FleetMetricsLog(str(tmp_path / "fm.jsonl"), log)
    task = asyncio.create_task(fleetmain._amain(
        cfg, log, str(yml), console_port=port, console_bind="127.0.0.1",
        api_token=TOKEN, mlog=mlog))
    base = f"http://127.0.0.1:{port}"
    hdr = {"X-API-Token": TOKEN}
    try:
        async with aiohttp.ClientSession() as http:
            async def call(method, body=None):
                for _ in range(100):             # 等控制台起来
                    try:
                        async with http.request(
                                method, base + "/api/total-quota",
                                json=body, headers=hdr) as r:
                            assert r.status == 200, await r.text()
                            return
                    except aiohttp.ClientConnectionError:
                        await asyncio.sleep(0.05)
                raise AssertionError("控制台没起来")

            # 以 0 启动：只汇总，绝不碰节点。
            await asyncio.sleep(0.5)
            assert node.puts == [] and node.deletes == []

            # 设总带宽：立即分配下发（alloc_interval_s=10，远不用等）。
            await call("PUT", {"quota_mbps": 100})
            await _until(lambda: node.puts, timeout=3)
            assert node.puts[-1] == {"token": "node-tok",
                                     "body": {"quota_mbps": 100.0}}

            # 取消总量控制：立即解除节点的实例总限速。
            await call("DELETE")
            await _until(lambda: node.deletes, timeout=3)
            assert node.deletes == ["node-tok"]

            # 之后只汇总：不会反复解除，也不再下发。
            n_puts = len(node.puts)
            await asyncio.sleep(1.0)
            assert node.deletes == ["node-tok"] and len(node.puts) == n_puts

            # 反查：整条变更时间线都在落盘里，按时间查得回来。
            async with http.get(base + "/api/metrics-log",
                                params={"kind": "event"}) as r:
                assert r.status == 200
                ev = (await r.json())["rows"]
            got = [(e["type"], e.get("node"), e.get("from_mbps"),
                    e.get("to_mbps")) for e in ev]
            assert got == [
                ("start", None, None, None),
                ("total", None, 0.0, 100.0),
                ("quota", "n1", 120.0, 100.0),   # 自报 120 → 下发 100
                ("total", None, 100.0, 0.0),
                ("release", "n1", 120.0, 0.0),
            ]
            assert "node-tok" not in json.dumps(ev)   # 令牌绝不落盘
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await node.server.close()
