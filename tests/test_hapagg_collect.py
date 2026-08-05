# tests.test_hapagg_collect —— 真实 TCP 上的采集（不是假 runner）。
#
# 这一层能真跑：起真的 asyncio 服务端、走真的 socket。前面几个项目里
# tc/IFB 那些东西受内核限制只能用假 runner，这里没有那个限制，所以
# **协议行为与故障处理全部实测**：
#
#   - runtime socket "一次连接一条命令"（应答完服务端就关连接）；
#   - 一台超时不能拖垮整轮；
#   - 一台回错误不能被当成正常数据；
#   - show info 挂了不该让这台机器的流量数据一起消失。

from __future__ import annotations

import asyncio

import pytest

from hapagg import collect
from hapagg.targets import Target

HEAD = "# pxname,svname,scur,smax,stot,bin,bout,status,type"
CSV = HEAD + "\nfe,FRONTEND,10,90,100,1000,2000,OPEN,0\n"
INFO = "Name: HAProxy\nVersion: 2.8.16\nCurrConns: 42\nIdle_pct: 97\n"


class FakeServer:
    """一台假 HAProxy。记录收到的命令，便于验证协议行为。"""

    def __init__(self, stat=CSV, info=INFO, delay=0.0, close_early=False):
        self.stat, self.info, self.delay = stat, info, delay
        self.close_early = close_early
        self.cmds: list[str] = []
        self.conns = 0
        self.port = 0
        self._srv = None

    async def start(self):
        self._srv = await asyncio.start_server(self._handle, "127.0.0.1", 0)
        self.port = self._srv.sockets[0].getsockname()[1]
        return self

    async def stop(self):
        self._srv.close()
        await self._srv.wait_closed()

    async def _handle(self, reader, writer):
        self.conns += 1
        try:
            line = await asyncio.wait_for(reader.readline(), timeout=5)
            cmd = line.decode().strip()
            self.cmds.append(cmd)
            if self.delay:
                await asyncio.sleep(self.delay)
            if self.close_early:
                return
            if cmd.startswith("show stat"):
                writer.write(self.stat.encode())
            elif cmd.startswith("show info"):
                writer.write(self.info.encode())
            else:
                writer.write(b"Unknown command.\n")
            await writer.drain()
        except (asyncio.TimeoutError, ConnectionError):
            pass
        finally:
            writer.close()          # 真实 runtime socket 应答完即关闭


def tgt(srv, name="n"):
    return Target(host="127.0.0.1", port=srv.port, name=name)


async def test_fetch_one_reads_stat_and_info():
    srv = await FakeServer().start()
    try:
        snap = await collect.fetch_one(tgt(srv))
    finally:
        await srv.stop()
    assert snap.ok and not snap.error
    assert len(snap.rows) == 1 and snap.rows[0].pxname == "fe"
    assert snap.version == "2.8.16"


async def test_each_command_uses_a_fresh_connection():
    """**协议要求**：runtime socket 非交互模式下一次连接只服务一条命令。

    复用长连接会读到空回包，表现为"这台机器时好时坏"——最难查的那种。
    """
    srv = await FakeServer().start()
    try:
        await collect.fetch_one(tgt(srv))
    finally:
        await srv.stop()
    assert srv.conns == 2, "show stat 与 show info 必须各自拨号"
    assert srv.cmds == ["show stat", "show info"]


async def test_show_stat_asks_for_every_row_type():
    """监控聚合要的是**各种维度**：backend/server 行是健康状态、排队、
    后端错误的唯一来源，不能像限速那样只取 frontend。"""
    srv = await FakeServer().start()
    try:
        await collect.fetch_one(tgt(srv))
    finally:
        await srv.stop()
    assert srv.cmds[0] == "show stat", "不带类型过滤参数"


async def test_timeout_is_reported_not_raised():
    """一台卡住只该让**这一台**显示成失败，绝不能中断整轮采集。"""
    srv = await FakeServer(delay=1.0).start()
    try:
        snap = await collect.fetch_one(tgt(srv), timeout_s=0.2)
    finally:
        await srv.stop()
    assert not snap.ok and "超时" in snap.error


async def test_connection_refused_is_reported():
    # 端口 1 上不会有东西监听
    snap = await collect.fetch_one(Target("127.0.0.1", 1, "dead"), timeout_s=1)
    assert not snap.ok and snap.error


async def test_error_reply_is_not_mistaken_for_data():
    """权限不足时回包是一行人话而不是 CSV。识别不出来的话运维得去猜
    到底是网络问题还是权限问题。"""
    srv = await FakeServer(stat="Permission denied.\n").start()
    try:
        snap = await collect.fetch_one(tgt(srv))
    finally:
        await srv.stop()
    assert not snap.ok and "Permission denied" in snap.error


async def test_garbage_reply_fails_loudly():
    srv = await FakeServer(stat="fe,FRONTEND,1,2,3\n").start()   # 没有列头
    try:
        snap = await collect.fetch_one(tgt(srv))
    finally:
        await srv.stop()
    assert not snap.ok and "列头" in snap.error


async def test_broken_info_does_not_lose_the_stats():
    """show info 是次要链路。它挂了只该少几个进程级指标，
    **不该让这台机器的全部流量数据消失**。"""
    srv = await FakeServer(info="garbage without colons\n").start()
    try:
        snap = await collect.fetch_one(tgt(srv))
    finally:
        await srv.stop()
    assert snap.ok, "info 解析失败不该判整台失败"
    assert snap.rows and not snap.info


async def test_collect_returns_one_result_per_target_even_when_some_fail():
    good = await FakeServer().start()
    slow = await FakeServer(delay=1.0).start()
    try:
        targets = [tgt(good, "good"), tgt(slow, "slow"),
                   Target("127.0.0.1", 1, "dead")]
        c = await collect.collect(targets, timeout_s=0.3)
    finally:
        await good.stop()
        await slow.stop()
    assert len(c.nodes) == 3, "每个目标都要有一条结果，成功失败都在"
    assert len(c.ok_nodes) == 1 and len(c.failed_nodes) == 2
    assert not c.complete
    assert c.coverage() == "1/3 台"


async def test_one_slow_node_does_not_serialize_the_others():
    """并发采集：10 台各睡 0.3 秒，总耗时必须远小于 3 秒。

    串行的话监控几十台机器就完全不可用了。
    """
    srvs = [await FakeServer(delay=0.3).start() for _ in range(10)]
    try:
        t0 = asyncio.get_running_loop().time()
        c = await collect.collect([tgt(s, f"n{i}") for i, s in enumerate(srvs)],
                                  timeout_s=2.0, concurrency=10)
        el = asyncio.get_running_loop().time() - t0
    finally:
        for s in srvs:
            await s.stop()
    assert len(c.ok_nodes) == 10
    assert el < 1.5, f"并发采集不该串行，实际耗时 {el:.2f}s"


async def test_empty_target_list_is_an_error():
    with pytest.raises(collect.CollectError, match="为空"):
        await collect.collect([])


async def test_server_closing_without_reply_fails_that_node_only():
    srv = await FakeServer(close_early=True).start()
    try:
        snap = await collect.fetch_one(tgt(srv), timeout_s=1)
    finally:
        await srv.stop()
    assert not snap.ok
