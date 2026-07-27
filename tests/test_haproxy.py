# RuntimeClient 的集成风格测试：用 asyncio.start_server 起一个"假 HAProxy
# stats socket"（TCP），复刻 runtime API 的协议行为——每个连接只服务一条
# 命令，回包写完即关闭连接（EOF 即回包结束）。

import asyncio
import contextlib

import pytest

from rl_limiter import model
from rl_limiter.haproxy import (
    SHOW_STAT_CMD,
    CommandError,
    RuntimeClient,
    StatParseError,
    parse_show_stat,
)


class FakeHAProxy:
    """假 stats socket 服务：每连接读一行命令，按 reply_fn 回包后关连接。

    reply_fn(cmd) 返回回包字符串；返回 None 表示"挂住不回包也不关连接"，
    用于测试客户端超时。挂住的连接在 stop() 时统一放行并关闭，避免事件
    循环结束时残留悬挂任务。
    """

    def __init__(self, reply_fn):
        self._reply_fn = reply_fn
        self.commands: list[str] = []  # 按到达顺序记录收到的命令（无换行）
        self._server = None
        self._release = asyncio.Event()  # stop() 时放行所有挂住的连接
        self.port = 0

    async def start(self):
        self._server = await asyncio.start_server(self._handle, "127.0.0.1", 0)
        self.port = self._server.sockets[0].getsockname()[1]

    async def stop(self):
        self._release.set()
        self._server.close()
        await self._server.wait_closed()
        await asyncio.sleep(0)  # 让被放行的 handler 跑完收尾

    async def _handle(self, reader, writer):
        try:
            line = await reader.readline()
            cmd = line.decode().rstrip("\n")
            self.commands.append(cmd)
            reply = self._reply_fn(cmd)
            if reply is None:
                await self._release.wait()  # 模拟服务端不响应（客户端应超时）
                return
            writer.write(reply.encode())
            await writer.drain()
        finally:
            writer.close()
            with contextlib.suppress(ConnectionError, OSError):
                await writer.wait_closed()


@contextlib.asynccontextmanager
async def fake_haproxy(reply_fn):
    srv = FakeHAProxy(reply_fn)
    await srv.start()
    try:
        yield srv
    finally:
        await srv.stop()


# ---------------------------------------------------------------------------
# exec_cmd
# ---------------------------------------------------------------------------

async def test_exec_cmd_roundtrip_strips_reply():
    async with fake_haproxy(lambda cmd: f"echo:{cmd}\n\n") as srv:
        c = RuntimeClient("127.0.0.1", srv.port)
        out = await c.exec_cmd("hello world")
        assert out == "echo:hello world"        # 首尾空白被 strip
        assert srv.commands == ["hello world"]  # 服务端收到的是不带换行的原命令


async def test_exec_cmd_error_reply_raises():
    # 命中已知错误前缀的回包必须抛异常，且异常携带回包原文。
    async with fake_haproxy(lambda cmd: "Unknown command. Please enter one of...\n") as srv:
        c = RuntimeClient("127.0.0.1", srv.port)
        with pytest.raises(CommandError) as ei:
            await c.exec_cmd("bogus")
        assert ei.value.reply.startswith("Unknown command")


async def test_exec_cmd_permission_denied_raises():
    async with fake_haproxy(lambda cmd: "Permission denied\n") as srv:
        c = RuntimeClient("127.0.0.1", srv.port)
        with pytest.raises(CommandError):
            await c.exec_cmd("set map /x k v")


async def test_exec_cmd_timeout():
    # 服务端收到命令后既不回包也不关连接：客户端必须在 timeout_s 内放弃。
    async with fake_haproxy(lambda cmd: None) as srv:
        c = RuntimeClient("127.0.0.1", srv.port, timeout_s=0.1)
        with pytest.raises(TimeoutError):
            await c.exec_cmd("show stat -1 1 -1")


# ---------------------------------------------------------------------------
# show_stat
# ---------------------------------------------------------------------------

# 贴近真实 HAProxy 输出的 CSV：原生列名 bout、行尾逗号、混入 backend/server
# 行、内建 stats frontend、空字段（fe_idle 的 scur/bout 为空）。
CANNED_CSV = (
    "# pxname,svname,qcur,scur,smax,bin,bout,\n"
    "stats,FRONTEND,,1,2,10,999,\n"
    "fe_env1,FRONTEND,,3,10,100,12345,\n"
    "fe_env1,srv1,0,1,1,50,111,\n"
    "be_env1,BACKEND,0,2,5,60,222,\n"
    "fe_idle,FRONTEND,,,,,,\n"
)


async def test_show_stat_parses_frontend_rows():
    async with fake_haproxy(lambda cmd: CANNED_CSV) as srv:
        c = RuntimeClient("127.0.0.1", srv.port)
        stats = await c.show_stat()
        # 固定命令：type=1 仅 frontend。
        assert srv.commands == [SHOW_STAT_CMD]
        # 只留 FRONTEND 行；剔除内建 stats；空字段按 0。
        assert stats == [
            model.FrontendStat(name="fe_env1", bytes_out=12345, conn_cur=3),
            model.FrontendStat(name="fe_idle", bytes_out=0, conn_cur=0),
        ]


async def test_show_stat_shuffled_columns():
    # 列位置无关：乱序表头下仍按列名取值（HAProxy 各版本列集合随意增删）。
    csv = (
        "# bout,pxname,foo,scur,svname\n"
        "5000,fe_a,x,7,FRONTEND\n"
        "1,be_a,x,9,BACKEND\n"
    )
    async with fake_haproxy(lambda cmd: csv) as srv:
        c = RuntimeClient("127.0.0.1", srv.port)
        stats = await c.show_stat()
        assert stats == [model.FrontendStat(name="fe_a", bytes_out=5000, conn_cur=7)]


def test_parse_show_stat_bytes_out_alias():
    # "bytes_out" 作为 bout 的别名被接受（测试桩/代理层改写兼容）。
    csv = "# pxname,svname,scur,bytes_out\nfe_b,FRONTEND,2,777\n"
    stats = parse_show_stat(csv)
    assert stats == [model.FrontendStat(name="fe_b", bytes_out=777, conn_cur=2)]


def test_parse_show_stat_short_row_tolerated():
    # 行内字段数少于表头列数：越界字段按空串→0 处理，不炸整次采样。
    csv = "# pxname,svname,scur,bout\nfe_c,FRONTEND\n"
    stats = parse_show_stat(csv)
    assert stats == [model.FrontendStat(name="fe_c", bytes_out=0, conn_cur=0)]


def test_parse_show_stat_missing_column_raises():
    with pytest.raises(StatParseError):
        parse_show_stat("# pxname,svname,scur\nfe,FRONTEND,1\n")  # 缺 bout/bytes_out


def test_parse_show_stat_no_header_raises():
    with pytest.raises(StatParseError):
        parse_show_stat("fe,FRONTEND,1,2\n")


def test_parse_show_stat_empty_output_raises():
    with pytest.raises(StatParseError):
        parse_show_stat("")


def test_parse_show_stat_bad_number_raises():
    with pytest.raises(StatParseError):
        parse_show_stat("# pxname,svname,scur,bout\nfe,FRONTEND,1,notanumber\n")


# ---------------------------------------------------------------------------
# 同机部署形态：本机 unix stats socket
# ---------------------------------------------------------------------------

class FakeUnixHAProxy(FakeHAProxy):
    """同 FakeHAProxy，但监听 unix socket——复刻同机部署下 haproxy.cfg 的
    `stats socket /run/haproxy/admin.sock mode 660 level user`。"""

    def __init__(self, reply_fn, path):
        super().__init__(reply_fn)
        self.path = str(path)

    async def start(self):
        self._server = await asyncio.start_unix_server(self._handle, self.path)


@contextlib.asynccontextmanager
async def fake_unix_haproxy(reply_fn, path):
    srv = FakeUnixHAProxy(reply_fn, path)
    await srv.start()
    try:
        yield srv
    finally:
        await srv.stop()


async def test_show_stat_over_unix_socket(tmp_path):
    """unix socket 形态下命令语义与回包解析与 TCP 完全一致——两种接线
    只有"怎么建连"一步不同。"""
    sock = tmp_path / "admin.sock"
    async with fake_unix_haproxy(lambda cmd: CANNED_CSV, sock) as srv:
        c = RuntimeClient(socket_path=str(sock))
        stats = await c.show_stat()
        assert srv.commands == [SHOW_STAT_CMD]
        assert stats == [
            model.FrontendStat(name="fe_env1", bytes_out=12345, conn_cur=3),
            model.FrontendStat(name="fe_idle", bytes_out=0, conn_cur=0),
        ]
        assert c.endpoint() == str(sock)


async def test_from_node_picks_wiring(tmp_path):
    """from_node 是"该走 unix 还是 TCP"的唯一判断点，接线装配不必重复分支。"""
    sock = tmp_path / "admin.sock"
    async with fake_unix_haproxy(lambda cmd: CANNED_CSV, sock) as srv:
        node = model.NodeConfig(name="hap-1", socket_path=str(sock))
        stats = await RuntimeClient.from_node(node).show_stat()
        assert [s.name for s in stats] == ["fe_env1", "fe_idle"]
        assert srv.commands == [SHOW_STAT_CMD]

    async with fake_haproxy(lambda cmd: CANNED_CSV) as srv:
        node = model.NodeConfig(name="hap-2", host="127.0.0.1", port=srv.port)
        stats = await RuntimeClient.from_node(node).show_stat()
        assert [s.name for s in stats] == ["fe_env1", "fe_idle"]


async def test_unix_socket_missing_raises(tmp_path):
    """socket 文件不存在（HAProxy 未起/路径写错）按普通采样失败上抛，
    由 collector 的单节点容错兜住（fail-static + degraded）。"""
    c = RuntimeClient(socket_path=str(tmp_path / "nope.sock"), timeout_s=1.0)
    with pytest.raises((FileNotFoundError, ConnectionError, OSError)):
        await c.show_stat()
