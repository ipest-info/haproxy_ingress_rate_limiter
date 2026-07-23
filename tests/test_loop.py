# tests/test_loop.py —— MonitorLoop 单元测试。
#
# 全部依赖用假采集器驱动，验证循环的编排语义：
#   - 配置先于 tick 生效（队列中的配置在下一拍流水线前被应用）；
#   - sampler 回调收到每拍的 (now, usages)；
#   - version 属性跟踪 seed 与队列下发的配置版本；
#   - 每 60 拍输出一条 "status summary"；
#   - 持续超限告警状态机：连续超限 OVER_ALERT_AFTER_S 秒才告警、回落
#     同样持续才解除；degraded / 无限额时判定暂停。

from __future__ import annotations

import asyncio
import contextlib
import logging
import time

from rl_limiter import loop as loop_mod
from rl_limiter import model
from rl_limiter.loop import MonitorLoop

# ---------------------------------------------------------------------------
# 假组件
# ---------------------------------------------------------------------------


class FakeCollector:
    def __init__(self, events: list | None = None):
        self.events = events if events is not None else []
        self.mapping: dict | None = None
        self.usages: list[model.EnvUsage] = []
        self.degraded: set[str] = set()
        self.ticks = 0

    def set_mapping(self, mapping):
        self.mapping = mapping
        self.events.append(("set_mapping", dict(mapping)))

    async def tick(self, now):
        self.ticks += 1
        self.events.append(("collector.tick", self.ticks))
        return list(self.usages)

    def degraded_nodes(self):
        return set(self.degraded)


# ---------------------------------------------------------------------------
# 工具
# ---------------------------------------------------------------------------

T1 = model.Target("hap-1", "fe_a")
T2 = model.Target("hap-2", "fe_a")


def make_config(version: int, quota: int = 80_000_000) -> model.ControllerConfig:
    # 监控单元=节点：env_id 装节点名，这里沿用一个单元挂两个 Target 的
    # 结构性写法（loop 不关心 Target 落在哪台节点）。
    return model.ControllerConfig(
        version=version,
        envs=[model.EnvQuota(env_id="hap-1", quota_bits_per_sec=quota,
                             targets=[T1, T2])])


async def run_until(ctl: MonitorLoop, queue, cond, interval=0.002, timeout=4.0):
    """后台跑 ctl.run，直到 cond() 为真（或超时断言失败），随后取消。"""
    task = asyncio.create_task(ctl.run(queue, tick_interval_s=interval))
    try:
        deadline = time.monotonic() + timeout
        while not cond() and time.monotonic() < deadline:
            await asyncio.sleep(0.002)
        assert cond(), "condition not reached before timeout"
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task


def make_loop(sampler=None):
    col = FakeCollector()
    ctl = MonitorLoop(col, sampler=sampler,
                      log=logging.getLogger("test.loop"))
    return ctl, col


# ---------------------------------------------------------------------------
# 测试
# ---------------------------------------------------------------------------


async def test_seed_applies_config_and_version():
    ctl, col = make_loop()
    assert ctl.version == 0
    ctl.seed(make_config(5))
    assert ctl.version == 5
    assert col.mapping == {T1: "hap-1", T2: "hap-1"}


async def test_config_applied_before_first_tick():
    # 配置优先于 tick：启动前排队的配置必须在第一拍流水线之前生效，
    # 超限判定才不会拿旧限额做基准。
    ctl, col = make_loop()
    ctl.seed(make_config(1, quota=100))
    queue: asyncio.Queue = asyncio.Queue()
    queue.put_nowait(make_config(2, quota=999))

    await run_until(ctl, queue, lambda: col.ticks >= 1)

    assert ctl.version == 2
    # 事件顺序：seed 的 set_mapping → 队列配置的 set_mapping → 第一拍采集。
    kinds = [e[0] for e in col.events]
    assert kinds[:3] == ["set_mapping", "set_mapping", "collector.tick"]


async def test_config_queue_updates_version_mid_run():
    ctl, col = make_loop()
    ctl.seed(make_config(3))
    queue: asyncio.Queue = asyncio.Queue()

    task = asyncio.create_task(ctl.run(queue, tick_interval_s=0.002))
    try:
        deadline = time.monotonic() + 4.0
        while col.ticks < 2 and time.monotonic() < deadline:
            await asyncio.sleep(0.002)
        queue.put_nowait(make_config(7, quota=555))
        while ctl.version != 7 and time.monotonic() < deadline:
            await asyncio.sleep(0.002)
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

    assert ctl.version == 7


async def test_sampler_called_each_tick():
    samples: list[tuple] = []
    ctl, col = make_loop(sampler=lambda now, usages: samples.append((now, usages)))
    ctl.seed(make_config(1))
    usage = model.EnvUsage(env_id="hap-1", mean10_bps=123.0, conn_cur=4)
    col.usages = [usage]

    await run_until(ctl, None, lambda: len(samples) >= 2)

    now, usages = samples[0]
    assert isinstance(now, float)
    assert usages == [usage]


async def test_status_summary_every_60_ticks(caplog):
    ctl, col = make_loop()
    ctl.seed(make_config(9))
    col.degraded = {"hap-2"}

    with caplog.at_level(logging.INFO, logger="test.loop"):
        # 极小 tick 间隔快速推进 60+ 拍（<0.5s）。
        await run_until(ctl, None, lambda: col.ticks >= 61, interval=0.001)

    summaries = [r.getMessage() for r in caplog.records
                 if "运行状态周期汇总" in r.getMessage()]
    assert summaries, "expected a status summary at tick 60"
    assert "tick=60" in summaries[0]
    assert "config_version=9" in summaries[0]
    assert "degraded_nodes=hap-2" in summaries[0]


async def test_over_quota_alert_and_recovery(caplog):
    """持续超限才告警（滞回）：超限满 OVER_ALERT_AFTER_S 拍打 warning，
    回落满同样拍数打解除 info；期间不重复刷屏。"""
    ctl, col = make_loop()
    # quota 800 bits/s = 100 bytes/s；mean10 = 150 bytes/s 即超限。
    ctl.seed(make_config(1, quota=800))
    col.usages = [model.EnvUsage(env_id="hap-1", mean10_bps=150.0)]

    need = loop_mod.OVER_ALERT_AFTER_S
    with caplog.at_level(logging.INFO, logger="test.loop"):
        await run_until(ctl, None, lambda: col.ticks >= need + 2,
                        interval=0.001)
        warns = [r for r in caplog.records
                 if r.levelno == logging.WARNING
                 and "持续高于登记限额" in r.getMessage()]
        assert len(warns) == 1  # 只在越过阈值那一拍告警一次
        assert "node=hap-1" in warns[0].getMessage()

        # 回落到限额以内：持续满阈值后解除。
        col.usages = [model.EnvUsage(env_id="hap-1", mean10_bps=50.0)]
        base = col.ticks
        await run_until(ctl, None, lambda: col.ticks >= base + need + 2,
                        interval=0.001)

    clears = [r for r in caplog.records
              if "解除持续超限告警" in r.getMessage()]
    assert len(clears) == 1


async def test_over_quota_alert_paused_when_degraded(caplog):
    """degraded（采样失联，数据陈旧）时超限判定暂停：不触发新告警。"""
    ctl, col = make_loop()
    ctl.seed(make_config(1, quota=800))
    col.usages = [model.EnvUsage(env_id="hap-1", mean10_bps=150.0,
                                 degraded=True)]

    need = loop_mod.OVER_ALERT_AFTER_S
    with caplog.at_level(logging.WARNING, logger="test.loop"):
        await run_until(ctl, None, lambda: col.ticks >= need + 3,
                        interval=0.001)

    warns = [r for r in caplog.records
             if "持续高于登记限额" in r.getMessage()]
    assert not warns
