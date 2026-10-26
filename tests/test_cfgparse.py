# tests.test_cfgparse —— 受管端口清单构建 + YAML 热更轮询（通用形态）。
#
# 本分支没有服务配置可解析：端口清单派生自 YAML 的 ports 段。这里覆盖
# 派生规则、canonical/checksum 的内容身份，以及 watch 的热更/fail-static/
# poke/dynamic_ingress 回调语义。

from __future__ import annotations

import asyncio
import logging

import pytest

from rl_limiter import cfgparse, model

pytestmark = pytest.mark.asyncio
log = logging.getLogger("t.cfgparse")


# ---------------------------------------------------------------------------
# 端口登记 → 受管清单
# ---------------------------------------------------------------------------

def test_sections_from_ports():
    secs = cfgparse.sections_from_ports({"web": 8080, "api": 8443})
    by = {s.name: s for s in secs}
    assert by["web"].binds == [("", 8080)] and by["web"].mode == "tcp"
    assert by["api"].binds == [("", 8443)]


def test_load_frontends_from_ports():
    fes = cfgparse.load_frontends({"web": 8080}, {}, log)
    (fe,) = fes
    assert fe.name == "web" and fe.bind_port == 8080
    assert not fe.limited, "通用形态没有端口级出向限额（quotas 恒空）"


def test_load_frontends_empty_ports_is_valid():
    """不登记任何端口是正常状态：整机监控与总量限速不需要它。"""
    assert cfgparse.load_frontends({}, {}, log) == []


# ---------------------------------------------------------------------------
# canonical / checksum：配置内容身份
# ---------------------------------------------------------------------------

def fe(name="web", port=8080, quota=0.0):
    return model.FrontendConfig(name=name, bind_port=port, quota_mbps=quota)


def test_canonical_is_order_insensitive():
    a = [fe("a", 1), fe("b", 2)]
    b = [fe("b", 2), fe("a", 1)]
    assert cfgparse.canonical(a) == cfgparse.canonical(b)


def test_checksum_is_instance_quota_sensitive():
    assert cfgparse.checksum([fe()]) != cfgparse.checksum([fe()], 100.0)
    assert cfgparse.checksum([fe()], 100.0) == cfgparse.checksum([fe()], 100.0)


def test_checksum_is_request_limits_sensitive():
    rl = {"web": model.RequestLimit(mbps=10.0, conn_rate=100)}
    assert cfgparse.checksum([fe()]) != cfgparse.checksum([fe()], 0.0, rl)


# ---------------------------------------------------------------------------
# watch：YAML 轮询热更
# ---------------------------------------------------------------------------

def write_yaml(tmp_path, ports_line="web: 8080", extra=""):
    y = tmp_path / "config.yaml"
    y.write_text(f"ports:\n  {ports_line}\n{extra}", encoding="utf-8")
    return y


async def run_watch_once(yaml_path, boot, interval_s=0.02, rounds=6):
    """跑几个轮询周期后取消，返回 queue 里收到的配置。"""
    q: asyncio.Queue = asyncio.Queue()
    task = asyncio.create_task(cfgparse.watch(
        str(yaml_path), q, boot, log, interval_s=interval_s))
    await asyncio.sleep(interval_s * rounds)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    out = []
    while not q.empty():
        out.append(q.get_nowait())
    return out


async def test_watch_pushes_on_ports_change(tmp_path):
    yaml_file = write_yaml(tmp_path)
    boot = cfgparse.load_frontends({"web": 8080}, {}, log)

    # 内容没变：不该有任何投递（mtime 变化也不触发——比较解析结果）。
    yaml_file.touch()
    assert await run_watch_once(yaml_file, boot) == []

    # 增加端口登记：一个周期内投出新配置。
    write_yaml(tmp_path, ports_line="web: 8080\n  api: 8443")
    got = await run_watch_once(yaml_file, boot)
    assert got and {f.name for f in got[-1].frontends} == {"web", "api"}


async def test_watch_fail_static_on_unreadable_yaml(tmp_path, caplog):
    """YAML 被原子替换的瞬间读不到属于预期内抖动：保留当前配置重试。"""
    yaml_file = write_yaml(tmp_path)
    boot = cfgparse.load_frontends({"web": 8080}, {}, log)
    yaml_file.unlink()
    with caplog.at_level(logging.WARNING, logger="t.cfgparse"):
        got = await run_watch_once(yaml_file, boot)
    assert got == []
    assert [r for r in caplog.records if "fail-static" in r.getMessage()]


async def test_watch_warns_on_startup_fixed_change(tmp_path, caplog):
    """log_level 等启动定型字段：改了要提示重启，而不是装作生效。"""
    yaml_file = write_yaml(tmp_path)
    boot = cfgparse.load_frontends({"web": 8080}, {}, log)

    async def change_log_level():
        await asyncio.sleep(0.06)
        write_yaml(tmp_path, extra="log_level: debug\n")

    with caplog.at_level(logging.WARNING, logger="t.cfgparse"):
        change = asyncio.ensure_future(change_log_level())
        await run_watch_once(yaml_file, boot, rounds=10)
        await change
    assert [r for r in caplog.records if "请重启" in r.getMessage()]


async def test_watch_poke_triggers_immediate_reread(tmp_path):
    """写 API 回写 YAML 后 set poke：watch 不等轮询周期立即重读。"""
    yaml_file = write_yaml(tmp_path)
    boot = cfgparse.load_frontends({"web": 8080}, {}, log)

    q: asyncio.Queue = asyncio.Queue()
    poke = asyncio.Event()
    task = asyncio.create_task(cfgparse.watch(
        str(yaml_file), q, boot, log, interval_s=30.0, poke=poke))
    try:
        await asyncio.sleep(0.05)
        write_yaml(tmp_path, extra="instance_quota_mbps: 90\n")
        poke.set()
        got = await asyncio.wait_for(q.get(), 2)
        assert got.instance_quota_mbps == 90.0
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task


async def test_watch_pushes_on_request_limits_change(tmp_path):
    yaml_file = write_yaml(tmp_path)
    boot = cfgparse.load_frontends({"web": 8080}, {}, log)
    write_yaml(tmp_path, extra="request_limits:\n  web: {mbps: 10}\n")
    got = await run_watch_once(yaml_file, boot)
    assert got and got[-1].request_limits["web"].mbps == 10.0


async def test_watch_hot_applies_dynamic_ingress(tmp_path, caplog):
    """dynamic_ingress 整段热更：改 YAML 后 watch 回调新参数（不触发
    限额配置队列投递、不提示重启）。"""
    yaml_file = write_yaml(tmp_path)
    boot = cfgparse.load_frontends({"web": 8080}, {}, log)

    got_cfgs = []
    q: asyncio.Queue = asyncio.Queue()
    task = asyncio.create_task(cfgparse.watch(
        str(yaml_file), q, boot, log, interval_s=0.02,
        dyn_ingress_cb=got_cfgs.append))
    try:
        await asyncio.sleep(0.06)   # 先建基线
        write_yaml(tmp_path,
                   extra="dynamic_ingress:\n  enabled: false\n"
                         "  interval_s: 30\n")
        with caplog.at_level(logging.INFO, logger="t.cfgparse"):
            await asyncio.sleep(0.12)
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    assert got_cfgs and got_cfgs[-1].enabled is False
    assert got_cfgs[-1].interval_s == 30.0
    assert q.empty()
    assert [r for r in caplog.records if "热更新已生效" in r.getMessage()]
    assert not [r for r in caplog.records if "请重启" in r.getMessage()]
