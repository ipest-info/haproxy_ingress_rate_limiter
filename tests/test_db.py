# tests/test_db.py —— MySQL 配置源（rl_limiter.db）单元测试。
#
# 不依赖真实 MySQL：用可注入的假连接（FakeConn/FakeCursor）返回预置行，
# 覆盖"SQL 取数 → 领域对象"的解析、版本轮询、样本/心跳写入的 SQL 形态，
# 以及 DbBackend 的缓冲/合并/落库编排。

from __future__ import annotations

import asyncio
import json

import pytest

from rl_limiter import db, model


# ---- 假 PyMySQL 连接 ------------------------------------------------------


class FakeCursor:
    """按 SQL 关键字返回预置结果的假游标；记录所有 execute/executemany。"""

    def __init__(self, conn: "FakeConn"):
        self._conn = conn
        self._result: list[dict] | None = None

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, sql: str, params=None):
        self._conn.executed.append((sql, params))
        s = sql.strip()
        if s.startswith("SELECT v FROM settings WHERE k = 'mode'"):
            self._result = [{"v": self._conn.mode}] if self._conn.mode is not None else []
        elif "UNIX_TIMESTAMP(MAX(u))" in s:
            self._result = [{"v": self._conn.version}]
        elif "FROM envs" in s and "quota_mbps" in s:
            self._result = list(self._conn.env_rows)
        elif "FROM env_targets" in s:
            self._result = list(self._conn.target_rows)
        elif "FROM haproxy_nodes" in s:
            self._result = list(self._conn.node_rows)
        elif s.startswith("SELECT 1"):
            self._result = [{"1": 1}]
        elif s.startswith("INSERT INTO heartbeats"):
            self._conn.heartbeats.append(params)
            self._result = []
        else:
            self._result = []

    def executemany(self, sql: str, seq):
        self._conn.executed.append((sql, "many"))
        self._conn.inserted_samples.extend(seq)

    def fetchone(self):
        return self._result[0] if self._result else None

    def fetchall(self):
        return list(self._result or [])


class FakeConn:
    def __init__(self, **kw):
        self.mode = kw.get("mode", "enforce")
        self.version = kw.get("version", 1700000000)
        self.env_rows = kw.get("env_rows", [])
        self.target_rows = kw.get("target_rows", [])
        self.node_rows = kw.get("node_rows", [])
        self.executed: list = []
        self.inserted_samples: list = []
        self.heartbeats: list = []
        self.closed = False

    def cursor(self):
        return FakeCursor(self)

    def close(self):
        self.closed = True


def make_db(**conn_kwargs) -> tuple[db.Database, list[FakeConn]]:
    """返回 (Database, 已创建连接列表)。每次操作开新连接，便于断言连接
    被正确关闭、以及一次操作用一条连接。"""
    conns: list[FakeConn] = []

    def connect(cfg):
        c = FakeConn(**conn_kwargs)
        conns.append(c)
        return c

    return db.Database(db.DbConfig(), connect=connect), conns


# ---- DbConfig.from_env ----------------------------------------------------


def test_dbconfig_from_env_defaults():
    cfg = db.DbConfig.from_env({})
    assert cfg.host == "127.0.0.1" and cfg.port == 3306
    assert cfg.user == "rl" and cfg.database == "rl_limiter"
    assert cfg.poll_interval_s == 5.0


def test_dbconfig_from_env_override():
    cfg = db.DbConfig.from_env({
        "RL_MYSQL_HOST": "db.internal", "RL_MYSQL_PORT": "3307",
        "RL_MYSQL_USER": "svc", "RL_MYSQL_PASSWORD": "s3cret",
        "RL_MYSQL_DB": "prod", "RL_MYSQL_POLL_INTERVAL_S": "2.5",
    })
    assert cfg.host == "db.internal" and cfg.port == 3307
    assert cfg.user == "svc" and cfg.password == "s3cret"
    assert cfg.database == "prod" and cfg.poll_interval_s == 2.5


# ---- 纯解析函数 -----------------------------------------------------------


def test_rows_to_nodes_maps_and_converts_timeout():
    rows = [
        {"name": "hap-1", "host": "10.0.0.1", "port": 9999,
         "bwlim_map_path": "/x/m.map", "timeout_ms": 250},
        {"name": "hap-2", "host": "10.0.0.2", "port": 9999,
         "bwlim_map_path": None, "timeout_ms": None},
    ]
    nodes = db.rows_to_nodes(rows)
    assert [n.name for n in nodes] == ["hap-1", "hap-2"]
    assert nodes[0].timeout_s == pytest.approx(0.25)
    # 空值回落默认：map 路径与 500ms。
    assert nodes[1].bwlim_map_path == "/etc/haproxy/maps/bwlim.map"
    assert nodes[1].timeout_s == pytest.approx(0.5)


def test_rows_to_controller_config_groups_targets_and_parses_params():
    env_rows = [
        {"env_id": "env-a", "quota_mbps": 200.0, "params_json": None},
        {"env_id": "env-b", "quota_mbps": 50.0,
         "params_json": json.dumps({"md_factor": 0.8})},
    ]
    target_rows = [
        {"env_id": "env-a", "node": "hap-1", "frontend": "fe_env_a"},
        {"env_id": "env-a", "node": "hap-2", "frontend": "fe_env_a"},
        {"env_id": "env-b", "node": "hap-1", "frontend": "fe_env_b"},
    ]
    cfg = db.rows_to_controller_config("enforce", 42, env_rows, target_rows)
    assert cfg.version == 42 and cfg.mode == "enforce"
    ea = next(e for e in cfg.envs if e.env_id == "env-a")
    assert ea.quota_mbps == 200.0
    assert ea.quota_bytes_per_sec == pytest.approx(25_000_000.0)
    assert ea.targets == [
        model.Target("hap-1", "fe_env_a"),
        model.Target("hap-2", "fe_env_a"),
    ]
    assert ea.params is None
    eb = next(e for e in cfg.envs if e.env_id == "env-b")
    assert eb.params is not None and eb.params.md_factor == pytest.approx(0.8)


def test_rows_to_controller_config_accepts_dict_json():
    # JSON 列在某些驱动下已是 dict，也要能解析。
    env_rows = [{"env_id": "e", "quota_mbps": 10.0,
                 "params_json": {"elastic_ceiling": 1.2}}]
    cfg = db.rows_to_controller_config("dry-run", 1, env_rows, [])
    assert cfg.envs[0].params.elastic_ceiling == pytest.approx(1.2)


def test_rows_to_controller_config_invalid_mode_normalized():
    cfg = db.rows_to_controller_config("bogus", 1, [], [])
    assert cfg.mode == model.MODE_DRY_RUN  # 非法模式归一为安全的 dry-run


# ---- Database async 方法（假连接）----------------------------------------


async def test_fetch_nodes():
    d, conns = make_db(node_rows=[
        {"name": "n1", "host": "h1", "port": 9999,
         "bwlim_map_path": "/m", "timeout_ms": 500},
    ])
    nodes = await d.fetch_nodes()
    assert nodes[0].name == "n1"
    assert conns[0].closed  # 连接用后即关


async def test_fetch_config_reads_mode_version_envs_targets():
    d, conns = make_db(
        mode="enforce", version=99,
        env_rows=[{"env_id": "env-a", "quota_mbps": 200.0, "params_json": None}],
        target_rows=[{"env_id": "env-a", "node": "n1", "frontend": "fe"}],
    )
    cfg = await d.fetch_config()
    assert cfg.mode == "enforce" and cfg.version == 99
    assert cfg.envs[0].env_id == "env-a"
    assert cfg.envs[0].targets == [model.Target("n1", "fe")]


async def test_fetch_config_defaults_mode_when_settings_empty():
    d, _ = make_db(mode=None, version=0)
    cfg = await d.fetch_config()
    assert cfg.mode == model.MODE_DRY_RUN


async def test_fetch_version():
    d, _ = make_db(version=12345)
    assert await d.fetch_version() == 12345


async def test_write_samples_executemany():
    d, conns = make_db()
    samples = [{"ts": 1.0, "node_id": "n", "env_id": "e", "rate_mbps": 1.0,
                "mean10_mbps": 1.0, "ewma60_mbps": 1.0, "conn_cur": 3,
                "bwlim_mbps": 2.0, "state": "normal", "changed": 1}]
    await d.write_samples(samples)
    assert conns[0].inserted_samples == samples


async def test_write_samples_empty_is_noop():
    d, conns = make_db()
    await d.write_samples([])
    # 空批次不应插入任何行。
    assert conns[0].inserted_samples == []


async def test_write_heartbeat_upsert():
    d, conns = make_db()
    await d.write_heartbeat("node-1", "1.2.3", "enforce", 77)
    assert conns[0].heartbeats == [("node-1", "1.2.3", "enforce", 77)]


async def test_ping_and_wait_ready():
    d, conns = make_db()
    await d.ping()
    await d.wait_ready(attempts=1)
    assert any("SELECT 1" in sql for sql, _ in conns[0].executed)


async def test_wait_ready_retries_then_succeeds():
    calls = {"n": 0}

    def connect(cfg):
        calls["n"] += 1
        if calls["n"] < 3:
            raise ConnectionError("mysql not ready")
        return FakeConn()

    d = db.Database(db.DbConfig(), connect=connect)
    await d.wait_ready(attempts=5, delay_s=0.0)
    assert calls["n"] == 3


async def test_wait_ready_gives_up():
    def connect(cfg):
        raise ConnectionError("down")

    d = db.Database(db.DbConfig(), connect=connect)
    with pytest.raises(ConnectionError):
        await d.wait_ready(attempts=2, delay_s=0.0)


# ---- DbBackend ------------------------------------------------------------


def _usage(env_id="env-a", mean10_bps=1_000_000.0):
    return model.EnvUsage(env_id=env_id, rate_bps=mean10_bps,
                          mean10_bps=mean10_bps, ewma60_bps=mean10_bps, conn_cur=5)


def _decision(env_id="env-a", bwlim_bps=2_000_000.0, changed=True):
    return model.Decision(env_id=env_id, targets=[model.Target("n1", "fe")],
                          bwlim_bps=bwlim_bps, state=model.GovState.NORMAL,
                          changed=changed)


def make_backend(db_obj) -> db.DbBackend:
    return db.DbBackend(
        db_obj, node_id="node-1", service_version="test",
        mode_fn=lambda: "enforce", version_fn=lambda: 7,
        poll_interval_s=0.01, flush_interval_s=0.01, heartbeat_interval_s=0.01,
    )


def test_add_sample_converts_to_mbps():
    d, _ = make_db()
    b = make_backend(d)
    b.add_sample(1.0, [_usage(mean10_bps=12_500_000.0)],
                 [_decision(bwlim_bps=13_750_000.0)], "enforce", 7)
    assert len(b._samples) == 1
    row = b._samples[0]
    # 12_500_000 bytes/s = 100 Mbps；13_750_000 = 110 Mbps。
    assert row["mean10_mbps"] == pytest.approx(100.0)
    assert row["bwlim_mbps"] == pytest.approx(110.0)
    assert row["node_id"] == "node-1" and row["changed"] == 1


def test_add_sample_no_decision_fills_zero():
    d, _ = make_db()
    b = make_backend(d)
    b.add_sample(1.0, [_usage()], [], "enforce", 7)
    row = b._samples[0]
    assert row["bwlim_mbps"] == 0.0 and row["state"] == "" and row["changed"] == 0


def test_add_sample_buffer_bound():
    d, _ = make_db()
    b = make_backend(d)
    for i in range(db.MAX_BUFFERED_SAMPLES + 50):
        b.add_sample(float(i), [_usage()], [_decision()], "enforce", 7)
    assert len(b._samples) == db.MAX_BUFFERED_SAMPLES
    assert b._dropped_total == 50


async def test_backend_poll_pushes_on_version_change():
    d, _ = make_db(mode="enforce", version=100,
                   env_rows=[{"env_id": "e", "quota_mbps": 10.0, "params_json": None}])
    b = make_backend(d)
    b.set_initial_version(1)  # 已应用版本 1，库里是 100 → 应推送
    task = asyncio.create_task(b.run())
    cfg = await asyncio.wait_for(b.configs.get(), timeout=1.0)
    assert cfg.version == 100
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)


async def test_backend_poll_skips_when_version_unchanged():
    d, _ = make_db(version=5)
    b = make_backend(d)
    b.set_initial_version(5)  # 版本一致 → 不应推送
    task = asyncio.create_task(b.run())
    await asyncio.sleep(0.05)
    assert b.configs.empty()
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)


async def test_backend_flush_writes_samples():
    d, conns = make_db(version=1)
    b = make_backend(d)
    b.add_sample(1.0, [_usage()], [_decision()], "enforce", 7)
    task = asyncio.create_task(b.run())
    # 等 flush 循环把样本写库。
    for _ in range(100):
        await asyncio.sleep(0.01)
        if any(c.inserted_samples for c in conns):
            break
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)
    assert any(c.inserted_samples for c in conns)
    assert not b._samples  # 成功后缓冲清空


async def test_backend_heartbeat_written():
    d, conns = make_db(version=1)
    b = make_backend(d)
    task = asyncio.create_task(b.run())
    for _ in range(100):
        await asyncio.sleep(0.01)
        if any(c.heartbeats for c in conns):
            break
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)
    hb = [h for c in conns for h in c.heartbeats]
    assert hb and hb[0] == ("node-1", "test", "enforce", 7)
