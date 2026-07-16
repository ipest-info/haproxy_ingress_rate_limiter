# webconsole（内置 Web 控制台）纯逻辑部分的测试：日志环形缓冲、
# StatusHub 的快照合成/历史留存/订阅广播、dbconfig 调参入口的本地校验。
# HTTP 层（aiohttp 路由）与真实 MySQL 写回由 docker compose 环境做集成验证。

import asyncio
import logging

import pytest

from rl_limiter import dbconfig, model, webconsole


def _quota(env_id="env-a", quota_bps=80_000_000):
    return model.EnvQuota(
        env_id=env_id, quota_bits_per_sec=quota_bps,
        targets=[model.Target("hap-1", "fe_env_a")])


def _usage(env_id="env-a", rate=1000.0):
    return model.EnvUsage(
        env_id=env_id, rate_bps=rate, mean10_bps=rate, ewma60_bps=rate,
        conn_cur=3)


def _decision(env_id="env-a", bwlim=11_000_000.0):
    return model.Decision(
        env_id=env_id, targets=[model.Target("hap-1", "fe_env_a")],
        bwlim_bps=bwlim, state=model.GovState.NORMAL)


def make_hub():
    return webconsole.StatusHub(
        "test", mode_fn=lambda: "enforce", version_fn=lambda: 42)


# ---- LogBuffer ----

def test_log_buffer_since_and_capacity():
    buf = webconsole.LogBuffer(capacity=3)
    log = logging.getLogger("test_webconsole")
    log.setLevel(logging.INFO)
    log.addHandler(buf)
    try:
        for i in range(5):
            log.info("消息 %d", i)
    finally:
        log.removeHandler(buf)
    entries = buf.since(0)
    # 容量 3：只留最新 3 条，seq 连续且消息已完成 % 格式化。
    assert [e["seq"] for e in entries] == [3, 4, 5]
    assert entries[-1]["msg"] == "消息 4"
    assert entries[-1]["level"] == "INFO"
    # 增量拉取：after=4 只给最后一条。
    assert [e["seq"] for e in buf.since(4)] == [5]


# ---- StatusHub ----

def test_hub_snapshot_merges_usage_decision_and_quota():
    hub = make_hub()
    hub.update_config(model.ControllerConfig(
        version=1, mode="enforce", envs=[_quota()]))
    hub.record(1000.0, [_usage(rate=5000.0)], [_decision()])

    ov = hub.overview()
    assert ov["mode"] == "enforce" and ov["config_version"] == 42
    env = ov["latest"]["envs"]["env-a"]
    assert env["rate_bytes_per_s"] == 5000.0
    assert env["bwlim_bytes_per_s"] == 11_000_000.0
    assert env["state"] == "normal"
    # 配额来自 update_config 缓存的配置视图（bits → bytes 已换算）。
    assert env["quota_bytes_per_s"] == 10_000_000.0
    # targets 以结构化对象暴露（控制台挂载点编辑需要 node/frontend 字段）。
    assert ov["env_config"]["env-a"]["targets"] == [
        {"node": "hap-1", "frontend": "fe_env_a"}]


def test_hub_history_is_bounded():
    hub = make_hub()
    for i in range(webconsole.HISTORY_TICKS + 50):
        hub.record(float(i), [_usage()], [_decision()])
    h = hub.history()
    assert len(h) == webconsole.HISTORY_TICKS
    assert h[0]["ts"] == 50.0  # 最旧的 50 拍被挤出


async def test_hub_subscriber_drops_oldest_when_slow():
    hub = make_hub()
    q = hub.subscribe()
    try:
        # 灌满队列后继续发布：慢消费者收到的是最新的 N 帧，而不是最旧的。
        total = webconsole.SUBSCRIBER_QUEUE_DEPTH + 3
        for i in range(total):
            hub.record(float(i), [_usage()], [_decision()])
        got = [q.get_nowait()["ts"] for _ in range(q.qsize())]
        assert got == [float(i) for i in range(3, total)]
        with pytest.raises(asyncio.QueueEmpty):
            q.get_nowait()
    finally:
        hub.unsubscribe(q)
    # 退订后发布不应再进队列。
    hub.record(99.0, [_usage()], [_decision()])
    assert q.qsize() == 0


# ---- dbconfig 调参入口的本地校验（不触网） ----

async def test_update_mode_rejects_bad_value():
    opts = dbconfig.MySQLOptions(host="unused")
    with pytest.raises(ValueError, match="mode 值非法"):
        await dbconfig.update_mode(opts, "observe")


async def test_update_env_quota_rejects_nonpositive():
    opts = dbconfig.MySQLOptions(host="unused")
    with pytest.raises(ValueError, match="quota_bps"):
        await dbconfig.update_env_quota(opts, "env-a", 0)


@pytest.mark.parametrize("params, match", [
    ({"md_factor": 0.8, "bogus_key": 1}, "未知键"),
    ({"md_factor": "0.8"}, "必须是数值"),
    ({"md_factor": True}, "必须是数值"),
    ("not-a-dict", "键值映射"),
])
async def test_update_env_params_validates_locally(params, match):
    """坏参数必须在写库前拦下，而不是等下一轮 fetch 触发 fail-static。"""
    opts = dbconfig.MySQLOptions(host="unused")
    with pytest.raises(ValueError, match=match):
        await dbconfig.update_env_params(opts, "env-a", params)


def test_hub_nodes_view_effective_mode_and_degraded():
    """节点视图：覆盖优先、未覆盖继承全局；失联集合来自 degraded_fn。"""
    nodes = [
        model.NodeConfig(name="hap-1", host="haproxy1", port=9999),
        model.NodeConfig(name="hap-2", host="haproxy2", port=9999),
    ]
    hub = webconsole.StatusHub(
        "test", mode_fn=lambda: "dry-run", version_fn=lambda: 1,
        nodes=nodes, degraded_fn=lambda: {"hap-2"})
    hub.update_config(model.ControllerConfig(
        version=1, mode="dry-run", envs=[_quota()],
        node_modes={"hap-1": "enforce"}))
    view = hub.overview()["nodes"]
    assert view["hap-1"] == {
        "host": "haproxy1", "port": 9999,
        "override": "enforce", "mode": "enforce", "degraded": False}
    assert view["hap-2"]["override"] is None
    assert view["hap-2"]["mode"] == "dry-run"   # 继承全局
    assert view["hap-2"]["degraded"] is True
    # 快照同样携带节点视图（SSE 帧里实时可见模式与健康）。
    hub.record(1.0, [_usage()], [_decision()])
    assert hub.history()[-1]["nodes"]["hap-1"]["mode"] == "enforce"
