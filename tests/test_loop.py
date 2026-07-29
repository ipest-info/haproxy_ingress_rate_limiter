# tests.test_loop —— 监控主循环的测试（单 HAProxy，单位 = frontend）。
#
# 覆盖：配置应用（受管集合/限额基准/叫醒 enforcer）、超限告警的滞回状态机、
# degraded 时暂停判定、配置队列优先于 tick。

from __future__ import annotations

import asyncio
import logging

import pytest

from rl_limiter import model
from rl_limiter.loop import OVER_ALERT_AFTER_S, MonitorLoop


class FakeCollector:
    """假采集器：记录收到的受管集合，按脚本返回用量。"""

    def __init__(self, usages=None, instance=None):
        self.managed: set[str] = set()
        self.degraded = False
        self._usages = usages or []
        # 采集器契约的一部分：tick 之后 instance 是本拍的整机视图。
        self.instance = instance or model.InstanceUsage()
        self.ticks = 0

    def set_managed(self, names):
        self.managed = set(names)

    async def tick(self, now):
        self.ticks += 1
        return self._usages


def fe(name="fe_a", quota_bps=8_000_000, port=8080, servers=None):
    return model.FrontendConfig(
        name=name, bind_port=port, quota_bits_per_sec=quota_bps,
        servers=servers or [model.ServerEntry(name="s1", address="10.0.0.1", port=80)])


def usage(name="fe_a", mean10=0.0, degraded=False):
    return model.FrontendUsage(name=name, mean10_bps=mean10, degraded=degraded)


def cfg(*frontends, version=1):
    return model.ControllerConfig(version=version, frontends=list(frontends))


# ---------------------------------------------------------------------------
# 配置应用
# ---------------------------------------------------------------------------

def test_apply_config_sets_managed_and_quotas():
    col = FakeCollector()
    ctl = MonitorLoop(col)
    ctl.seed(cfg(fe("fe_a", 8_000_000), fe("fe_b", 16_000_000, port=8081)))
    assert col.managed == {"fe_a", "fe_b"}
    assert ctl.version == 1
    # 限额基准换算成 bytes/s（÷8）——单位换算只在 model 一处。
    assert [f.quota_bits_per_sec for f in ctl.frontends()] == [8_000_000, 16_000_000]


def test_frontends_follows_hot_reload():
    """enforcer 每轮现取，因此配置热更后拿到的必然是新值。"""
    ctl = MonitorLoop(FakeCollector())
    ctl.seed(cfg(fe("fe_a", 8_000_000)))
    assert ctl.frontends()[0].quota_bits_per_sec == 8_000_000
    ctl.seed(cfg(fe("fe_a", 4_000_000), version=2))
    assert ctl.frontends()[0].quota_bits_per_sec == 4_000_000


def test_config_applied_event_wakes_enforcer():
    """每次应用配置都叫醒下发任务——"改完立刻生效"靠的就是这个事件。"""
    ev = asyncio.Event()
    ctl = MonitorLoop(FakeCollector(), config_applied=ev)
    assert not ev.is_set()
    ctl.seed(cfg(fe()))
    assert ev.is_set()


def test_removed_frontend_drops_hysteresis_state():
    """下线的 frontend 其超限滞回状态一并丢弃，避免重新上线时带着旧计数。"""
    col = FakeCollector()
    ctl = MonitorLoop(col)
    ctl.seed(cfg(fe("fe_a"), fe("fe_b", port=8081)))
    from rl_limiter.loop import _OverState
    ctl._over["fe_b"] = _OverState()          # 制造一份滞回状态
    ctl.seed(cfg(fe("fe_a"), version=2))
    assert "fe_b" not in ctl._over
    assert col.managed == {"fe_a"}


# ---------------------------------------------------------------------------
# 超限告警的滞回状态机
# ---------------------------------------------------------------------------

async def drive(ctl, col, usages, ticks):
    col._usages = usages
    for i in range(ticks):
        await ctl._tick(float(i))


async def test_over_quota_alert_needs_sustained_excess(caplog):
    """瞬时冲高不告警：mean10 需连续高于限额 OVER_ALERT_AFTER_S 秒。"""
    col = FakeCollector()
    ctl = MonitorLoop(col, log=logging.getLogger("t.over"))
    ctl.seed(cfg(fe("fe_a", 8_000_000)))          # 限额 = 1_000_000 bytes/s
    over = [usage("fe_a", mean10=1_500_000)]

    with caplog.at_level(logging.WARNING, logger="t.over"):
        await drive(ctl, col, over, OVER_ALERT_AFTER_S - 1)
        assert not [r for r in caplog.records if "持续高于限额" in r.getMessage()]
        await drive(ctl, col, over, 1)
        assert [r for r in caplog.records if "持续高于限额" in r.getMessage()]


async def test_over_quota_clears_after_sustained_recovery(caplog):
    col = FakeCollector()
    ctl = MonitorLoop(col, log=logging.getLogger("t.clear"))
    ctl.seed(cfg(fe("fe_a", 8_000_000)))
    with caplog.at_level(logging.INFO, logger="t.clear"):
        await drive(ctl, col, [usage("fe_a", mean10=1_500_000)], OVER_ALERT_AFTER_S)
        caplog.clear()
        await drive(ctl, col, [usage("fe_a", mean10=100_000)], OVER_ALERT_AFTER_S)
        assert [r for r in caplog.records if "回落到限额以内" in r.getMessage()]


async def test_degraded_pauses_over_quota_judgement(caplog):
    """采样失联时数据是陈旧的：既不该触发新告警，也不该解除已有告警。"""
    col = FakeCollector()
    ctl = MonitorLoop(col, log=logging.getLogger("t.deg"))
    ctl.seed(cfg(fe("fe_a", 8_000_000)))
    with caplog.at_level(logging.WARNING, logger="t.deg"):
        await drive(ctl, col,
                    [usage("fe_a", mean10=9_999_999, degraded=True)],
                    OVER_ALERT_AFTER_S * 3)
        assert not [r for r in caplog.records if "持续高于限额" in r.getMessage()]


# ---------------------------------------------------------------------------
# 采样发布
# ---------------------------------------------------------------------------

async def test_sampler_receives_frontend_and_instance_from_same_tick():
    """两级视图必须同拍交出：分两次发布会让页面上两个 tab 的曲线差半秒，
    看起来像是数据对不上。"""
    inst = model.InstanceUsage(conn_cur=17)
    col = FakeCollector([usage("fe_a", mean10=5.0)], instance=inst)
    seen = []
    ctl = MonitorLoop(col, sampler=lambda now, us, i: seen.append((now, us, i)))
    ctl.seed(cfg(fe("fe_a")))
    await ctl._tick(42.0)
    (now, usages, got) = seen[-1]
    assert now == 42.0
    assert [u.name for u in usages] == ["fe_a"]
    assert got is inst


# ---------------------------------------------------------------------------
# 配置优先于 tick
# ---------------------------------------------------------------------------

async def test_config_applied_before_tick():
    """某拍之前送达的配置，一定在该拍之前被应用——超限判定永远基于最新限额。"""
    col = FakeCollector([usage("fe_a")])
    ctl = MonitorLoop(col)
    ctl.seed(cfg(fe("fe_a", 8_000_000)))
    q: asyncio.Queue = asyncio.Queue()
    q.put_nowait(cfg(fe("fe_a", 4_000_000), version=7))
    task = asyncio.create_task(ctl.run(q, tick_interval_s=0.01))
    await asyncio.sleep(0.05)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert ctl.version == 7
    assert ctl.frontends()[0].quota_bits_per_sec == 4_000_000
