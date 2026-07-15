# tests/test_loop.py —— ControlLoop 单元测试。
#
# 全部依赖用假组件驱动（不触碰并行开发中的真实 collector/governor/
# executor/allocator 模块），验证循环的编排语义：
#   - 配置先于 tick 生效（队列中的配置在下一拍流水线前被应用）；
#   - executor.apply 返回的异常只记日志、绝不中断循环；
#   - sampler 回调收到每拍的 (now, usages, decisions)；
#   - version 属性跟踪 seed 与队列下发的配置版本；
#   - 每 60 拍输出一条 "status summary"；
#   - allocator.allocate 按 decision 逐个调用，结果成对交给 executor。

from __future__ import annotations

import asyncio
import contextlib
import logging
import sys
import time
import types

# 并行开发中真实的 rl_limiter.allocator 可能尚不存在；在导入 rl_limiter.loop
# 之前注入一个假模块占位（loop 只依赖其 allocate(bwlim_bps, targets,
# target_ewma) 签名）。若真实模块已存在但尚未被导入，这里的占位同样生效，
# 保证测试始终以受控的假实现运行；各测试再用 monkeypatch 把 loop 模块内
# 的 allocator 引用换成带记录功能的实例。
import rl_limiter

_placeholder = types.ModuleType("rl_limiter.allocator")
_placeholder.allocate = lambda bwlim_bps, targets, target_ewma: {
    t: int(bwlim_bps) // max(len(targets), 1) for t in targets
}
sys.modules.setdefault("rl_limiter.allocator", _placeholder)
if not hasattr(rl_limiter, "allocator"):
    rl_limiter.allocator = sys.modules["rl_limiter.allocator"]

from rl_limiter import loop as loop_mod  # noqa: E402
from rl_limiter import model  # noqa: E402
from rl_limiter.loop import ControlLoop  # noqa: E402

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


class FakeGovernor:
    def __init__(self):
        self.current_envs: list[model.EnvQuota] = []
        self.updates: list[list[model.EnvQuota]] = []
        # 每次 tick 时记录"当时生效的配置"，用于验证配置先于 tick 生效。
        self.seen_at_tick: list[list[model.EnvQuota]] = []
        self.decisions: list[model.Decision] = []

    def update_config(self, envs):
        self.current_envs = list(envs)
        self.updates.append(list(envs))

    def tick(self, now, usages):
        self.seen_at_tick.append(list(self.current_envs))
        return list(self.decisions)


class FakeExecutor:
    def __init__(self):
        self._mode = ""
        self.applied: list[list] = []
        self.errs: list[Exception] = []

    def set_mode(self, mode):
        self._mode = mode

    def mode(self):  # 契约允许"mode 属性或 mode()"，这里用方法形态
        return self._mode

    async def apply(self, batch):
        self.applied.append(list(batch))
        return list(self.errs)

    def snapshot(self):
        return {}


class RecordingAllocator:
    """替身 allocator 模块：记录每次 allocate 调用并返回均分结果。"""

    def __init__(self):
        self.calls: list[tuple[float, tuple, dict]] = []

    def allocate(self, bwlim_bps, targets, target_ewma):
        self.calls.append((bwlim_bps, tuple(targets), dict(target_ewma)))
        share = int(bwlim_bps) // max(len(targets), 1)
        return {t: share for t in targets}


# ---------------------------------------------------------------------------
# 工具
# ---------------------------------------------------------------------------

T1 = model.Target("hap-1", "fe_a")
T2 = model.Target("hap-2", "fe_a")


def make_config(version: int, mode: str = model.MODE_DRY_RUN,
                quota: int = 80_000_000) -> model.ControllerConfig:
    return model.ControllerConfig(
        version=version, mode=mode,
        envs=[model.EnvQuota(env_id="env-a", quota_bits_per_sec=quota,
                             targets=[T1, T2])])


async def run_until(ctl: ControlLoop, queue, cond, interval=0.002, timeout=4.0):
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


def make_loop(monkeypatch, sampler=None):
    col, gov, exe = FakeCollector(), FakeGovernor(), FakeExecutor()
    alloc = RecordingAllocator()
    monkeypatch.setattr(loop_mod, "allocator", alloc)
    ctl = ControlLoop(col, gov, exe, sampler=sampler,
                      log=logging.getLogger("test.loop"))
    return ctl, col, gov, exe, alloc


# ---------------------------------------------------------------------------
# 测试
# ---------------------------------------------------------------------------


async def test_seed_applies_config_and_version(monkeypatch):
    ctl, col, gov, exe, _ = make_loop(monkeypatch)
    assert ctl.version == 0
    ctl.seed(make_config(5, mode=model.MODE_ENFORCE))
    assert ctl.version == 5
    assert exe.mode() == model.MODE_ENFORCE
    assert gov.updates and gov.updates[0][0].env_id == "env-a"
    assert col.mapping == {T1: "env-a", T2: "env-a"}


async def test_seed_normalizes_illegal_mode(monkeypatch):
    # 非法模式必须归一为 dry-run（安全方向），与 model.normalize 契约一致。
    ctl, _, _, exe, _ = make_loop(monkeypatch)
    ctl.seed(make_config(1, mode="whatever"))
    assert exe.mode() == model.MODE_DRY_RUN


async def test_config_applied_before_first_tick(monkeypatch):
    # 配置优先于 tick：启动前排队的配置必须在第一拍流水线之前生效，
    # 否则该拍会按旧配额/旧模式多放行一秒（参照 Go 版嵌套 select 注释）。
    ctl, col, gov, exe, _ = make_loop(monkeypatch)
    ctl.seed(make_config(1, quota=100))
    queue: asyncio.Queue = asyncio.Queue()
    queue.put_nowait(make_config(2, mode=model.MODE_ENFORCE, quota=999))

    await run_until(ctl, queue, lambda: col.ticks >= 1)

    assert ctl.version == 2
    assert exe.mode() == model.MODE_ENFORCE
    # 第一拍决策时 governor 看到的已经是 version=2 的配额。
    assert gov.seen_at_tick[0][0].quota_bits_per_sec == 999
    # 事件顺序：seed 的 set_mapping → 队列配置的 set_mapping → 第一拍采集。
    kinds = [e[0] for e in col.events]
    assert kinds[:3] == ["set_mapping", "set_mapping", "collector.tick"]


async def test_config_queue_updates_version_mid_run(monkeypatch):
    # 版本跟踪：运行中经队列下发的配置同样被应用并更新 version。
    ctl, col, gov, _, _ = make_loop(monkeypatch)
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
    # 应用新配置之后的某一拍，governor 看到的是新配额。
    assert any(envs and envs[0].quota_bits_per_sec == 555
               for envs in gov.seen_at_tick)


async def test_apply_errors_logged_but_loop_continues(monkeypatch, caplog):
    # executor.apply 返回异常列表：逐条 error 日志（带 tick 序号），
    # 循环继续推进，绝不中断。
    ctl, col, gov, exe, _ = make_loop(monkeypatch)
    ctl.seed(make_config(1))
    gov.decisions = [model.Decision(env_id="env-a", targets=[T1, T2],
                                    bwlim_bps=1000.0, state=model.GovState.NORMAL,
                                    changed=True)]
    exe.errs = [RuntimeError("socket boom"), RuntimeError("node down")]

    with caplog.at_level(logging.ERROR, logger="test.loop"):
        await run_until(ctl, None, lambda: col.ticks >= 3)

    errors = [r for r in caplog.records
              if r.levelno == logging.ERROR and "executor apply failed" in r.getMessage()]
    # 每拍 2 条错误，至少跑了 3 拍。
    assert len(errors) >= 6
    assert "tick=" in errors[0].getMessage()
    assert col.ticks >= 3  # 循环未被错误中断


async def test_sampler_called_each_tick(monkeypatch):
    samples: list[tuple] = []
    ctl, col, gov, _, _ = make_loop(
        monkeypatch, sampler=lambda now, usages, ds: samples.append((now, usages, ds)))
    ctl.seed(make_config(1))
    usage = model.EnvUsage(env_id="env-a", mean10_bps=123.0, conn_cur=4)
    col.usages = [usage]
    gov.decisions = [model.Decision(env_id="env-a", targets=[T1],
                                    bwlim_bps=500.0, state=model.GovState.NORMAL)]

    await run_until(ctl, None, lambda: len(samples) >= 2)

    now, usages, decisions = samples[0]
    assert isinstance(now, float)
    assert usages == [usage]
    assert decisions == gov.decisions


async def test_status_summary_every_60_ticks(monkeypatch, caplog):
    ctl, col, _, _, _ = make_loop(monkeypatch)
    ctl.seed(make_config(9))
    col.degraded = {"hap-2"}

    with caplog.at_level(logging.INFO, logger="test.loop"):
        # 极小 tick 间隔快速推进 60+ 拍（<0.5s）。
        await run_until(ctl, None, lambda: col.ticks >= 61, interval=0.001)

    summaries = [r.getMessage() for r in caplog.records
                 if "status summary" in r.getMessage()]
    assert summaries, "expected a status summary at tick 60"
    assert "tick=60" in summaries[0]
    assert "config_version=9" in summaries[0]
    assert "degraded_nodes=hap-2" in summaries[0]


async def test_allocate_called_per_decision_and_passed_to_executor(monkeypatch):
    ctl, col, gov, exe, alloc = make_loop(monkeypatch)
    ctl.seed(make_config(1))
    ewma = {T1: 700.0, T2: 300.0}
    col.usages = [
        model.EnvUsage(env_id="env-a", target_ewma=ewma),
        model.EnvUsage(env_id="env-b", target_ewma={}),
    ]
    d_a = model.Decision(env_id="env-a", targets=[T1, T2], bwlim_bps=1000.0,
                         state=model.GovState.NORMAL, changed=True)
    d_b = model.Decision(env_id="env-b", targets=[T2], bwlim_bps=200.0,
                         state=model.GovState.TIGHTENING, changed=True)
    gov.decisions = [d_a, d_b]

    await run_until(ctl, None, lambda: col.ticks >= 1)

    # 每个 decision 恰好触发一次 allocate（看第一拍的两次调用）。
    first_two = alloc.calls[:2]
    assert first_two[0] == (1000.0, (T1, T2), ewma)
    assert first_two[1] == (200.0, (T2,), {})
    # executor 收到 (decision, allocations) 成对批次。
    batch = exe.applied[0]
    assert [d for d, _ in batch] == [d_a, d_b]
    assert batch[0][1] == {T1: 500, T2: 500}
    assert batch[1][1] == {T2: 200}
