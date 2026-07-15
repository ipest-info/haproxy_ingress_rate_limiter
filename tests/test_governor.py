# tests/test_governor.py —— governor 快环 AIMD 的单元测试。
# 覆盖状态机各分支（正常/收紧/恢复）、滞回计数、弹性上限、
# 配置热更新与日志输出等场景。

from __future__ import annotations

import logging
import math

import pytest

from rl_limiter import model
from rl_limiter.governor import Governor

# 80 Mbit/s 配额 → 内部 10 MB/s。
QUOTA_BITS = 80_000_000
QUOTA_BYTES = 10_000_000.0

T0 = 1_700_000_000.0  # 起始时间戳（governor 不消费，仅按 tick 计数）


def new_governor() -> Governor:
    log = logging.getLogger("test.governor")
    return Governor(log=log)


def env_quota(
    env_id: str,
    bits: int,
    params: model.GovParams | None = None,
    *targets: model.Target,
) -> model.EnvQuota:
    return model.EnvQuota(
        env_id=env_id,
        # 入参沿用 bits/s（历史用法，便于与 QUOTA_BYTES 等常量对齐），
        # 换算为配置口径的 Mbps：quota_bytes_per_sec 结果不变（bits/8）。
        quota_mbps=bits / 1_000_000,
        targets=list(targets) or [model.Target("n1", "fe_a")],
        params=params,
    )


def usage(env_id: str, mean10: float, degraded: bool = False) -> model.EnvUsage:
    return model.EnvUsage(
        env_id=env_id, rate_bps=mean10, mean10_bps=mean10, degraded=degraded
    )


def approx(a: float, b: float) -> bool:
    return abs(a - b) <= 1e-6 * max(1.0, abs(b))


def tick_one(g: Governor, now: float, u: model.EnvUsage) -> model.Decision:
    """跑一拍且必须恰好产出一条决策。"""
    ds = g.tick(now, [u])
    assert len(ds) == 1, f"tick returned {len(ds)} decisions, want 1"
    return ds[0]


def test_sustained_overload_converges_to_floor():
    """mean10 恒为 1.3×q 时，bwlim 必须从 ceiling 收敛到 quota×tighten_floor
    （在 tighten_after_s + 几拍内），且永不跌破下限。"""
    g = new_governor()
    g.update_config([env_quota("env-a", QUOTA_BITS)])

    p = model.GovParams()
    floor = QUOTA_BYTES * p.tighten_floor  # 0.95q
    ceil = QUOTA_BYTES * p.elastic_ceiling
    now = T0

    reached_floor_at = 0
    for i in range(1, 31):
        d = tick_one(g, now, usage("env-a", 1.3 * QUOTA_BYTES))
        now += 1.0

        assert d.bwlim_bps >= floor - 1e-6, f"tick {i}: bwlim fell below floor"
        if i < p.tighten_after_s:
            # 持续性未计满：仍停靠在 ceiling。
            assert approx(d.bwlim_bps, ceil), f"tick {i}: moved before tighten_after_s"
        if reached_floor_at == 0 and approx(d.bwlim_bps, floor):
            reached_floor_at = i
            assert d.state is model.GovState.TIGHTENING
        if reached_floor_at != 0 and i > reached_floor_at:
            assert approx(d.bwlim_bps, floor), f"tick {i}: should stay at floor"
            assert not d.changed, f"tick {i}: changed=True while parked at floor"

    # 默认参数下：第 3 拍 ceil×0.9=0.99q，第 4 拍触底。
    assert 0 < reached_floor_at <= p.tighten_after_s + 3


def test_deadband_holds_value():
    """死区内跑 100 拍：恰好一条 changed 决策（首拍强制发射），数值全程不动。"""
    g = new_governor()
    g.update_config([env_quota("env-a", QUOTA_BITS)])

    ceil = QUOTA_BYTES * model.GovParams().elastic_ceiling
    now = T0
    changed_count = 0
    for i in range(1, 101):
        d = tick_one(g, now, usage("env-a", 0.95 * QUOTA_BYTES))
        now += 1.0
        if d.changed:
            changed_count += 1
        assert approx(d.bwlim_bps, ceil), f"tick {i}: bwlim moved in deadband"
        assert d.state is model.GovState.NORMAL
    assert changed_count == 1, f"changed decisions = {changed_count}, want exactly 1"


def test_recovery_ramps_and_parks_at_ceiling():
    """收紧到底后持续低用量：先等满 recover_after_s，再每拍 +quota×ai_step_frac
    爬坡，到 ceiling 停靠并回到 NORMAL。"""
    g = new_governor()
    g.update_config([env_quota("env-a", QUOTA_BITS)])

    p = model.GovParams()
    floor = QUOTA_BYTES * p.tighten_floor
    ceil = QUOTA_BYTES * p.elastic_ceiling
    step = QUOTA_BYTES * p.ai_step_frac
    now = T0

    # 先压到下限。
    d = None
    for _ in range(p.tighten_after_s + 3):
        d = tick_one(g, now, usage("env-a", 1.3 * QUOTA_BYTES))
        now += 1.0
    assert approx(d.bwlim_bps, floor), "setup: not at floor"

    # 死区间奏：收紧值保持不动、状态保留 TIGHTENING。
    for _ in range(3):
        d = tick_one(g, now, usage("env-a", 0.95 * QUOTA_BYTES))
        now += 1.0
        assert approx(d.bwlim_bps, floor) and not d.changed
        assert d.state is model.GovState.TIGHTENING, "deadband must keep state"

    # 低用量：recover_after_s 计满之前不动。
    low = 0.5 * QUOTA_BYTES
    for i in range(1, p.recover_after_s):
        d = tick_one(g, now, usage("env-a", low))
        now += 1.0
        assert approx(d.bwlim_bps, floor) and not d.changed, f"pre-recovery tick {i}"

    # 爬坡：每拍精确 +step，直到 ceiling。
    prev = floor
    i = 0
    while True:
        assert i <= 20, "recovery did not reach the ceiling within 20 ticks"
        d = tick_one(g, now, usage("env-a", low))
        now += 1.0
        want = min(ceil, prev + step)
        assert approx(d.bwlim_bps, want), f"recovery tick {i}: wrong step"
        assert d.changed, f"recovery tick {i}: changed=False during ramp"
        prev = d.bwlim_bps
        if approx(d.bwlim_bps, ceil):
            assert d.state is model.GovState.NORMAL
            break
        assert d.state is model.GovState.RECOVERING
        i += 1

    # 到顶停靠：继续低用量不再有任何变化。
    for i in range(5):
        d = tick_one(g, now, usage("env-a", low))
        now += 1.0
        assert approx(d.bwlim_bps, ceil) and not d.changed
        assert d.state is model.GovState.NORMAL


def test_quota_change_resets_to_new_ceiling():
    """配额变化必须把 bwlim 重置到新 ceiling、清零持续性计数并重新发射。"""
    g = new_governor()
    g.update_config([env_quota("env-a", QUOTA_BITS)])

    p = model.GovParams()
    now = T0

    # 旧配额下先压到下限。
    for _ in range(p.tighten_after_s + 3):
        tick_one(g, now, usage("env-a", 1.3 * QUOTA_BYTES))
        now += 1.0

    new_quota_bytes = 2 * QUOTA_BYTES
    g.update_config([env_quota("env-a", 2 * QUOTA_BITS)])

    new_ceil = new_quota_bytes * p.elastic_ceiling
    # 对新配额超限：计数必须从零起步，前 tighten_after_s-1 拍停在新 ceiling。
    for i in range(1, p.tighten_after_s):
        d = tick_one(g, now, usage("env-a", 1.3 * new_quota_bytes))
        now += 1.0
        assert approx(d.bwlim_bps, new_ceil), f"tick {i} after quota change"
        if i == 1:
            assert d.changed, "first decision after quota change must be changed"
            assert d.state is model.GovState.NORMAL

    # 计数此刻才计满 → 首次收紧。
    d = tick_one(g, now, usage("env-a", 1.3 * new_quota_bytes))
    want = max(new_quota_bytes * p.tighten_floor, new_ceil * p.md_factor)
    assert approx(d.bwlim_bps, want)
    assert d.state is model.GovState.TIGHTENING


def test_unchanged_config_keeps_state():
    """重新下发完全相同的配置不得重置已收紧的值。"""
    g = new_governor()
    cfg = [env_quota("env-a", QUOTA_BITS)]
    g.update_config(cfg)

    p = model.GovParams()
    now = T0
    for _ in range(p.tighten_after_s + 3):
        tick_one(g, now, usage("env-a", 1.3 * QUOTA_BYTES))
        now += 1.0

    g.update_config([env_quota("env-a", QUOTA_BITS)])
    d = tick_one(g, now, usage("env-a", 0.95 * QUOTA_BYTES))
    floor = QUOTA_BYTES * p.tighten_floor
    assert approx(d.bwlim_bps, floor) and not d.changed


def test_degraded_freezes():
    """degraded 用量必须冻结 bwlim、不推进也不清零计数、changed=False——
    即使是首拍也不发射。"""
    g = new_governor()
    g.update_config([env_quota("env-a", QUOTA_BITS)])

    p = model.GovParams()
    ceil = QUOTA_BYTES * p.elastic_ceiling
    now = T0

    # 史上第一拍就 degraded：不得发射首值。
    d = tick_one(g, now, usage("env-a", 1.3 * QUOTA_BYTES, degraded=True))
    now += 1.0
    assert not d.changed and approx(d.bwlim_bps, ceil)

    # tighten_after_s-1 拍真实超限：计数差一拍到阈值。
    for _ in range(1, p.tighten_after_s):
        d = tick_one(g, now, usage("env-a", 1.3 * QUOTA_BYTES))
        now += 1.0
    assert approx(d.bwlim_bps, ceil), "setup: moved before tighten_after_s"

    # degraded 间奏：值与计数全部冻结。
    for i in range(5):
        d = tick_one(g, now, usage("env-a", 1.3 * QUOTA_BYTES, degraded=True))
        now += 1.0
        assert not d.changed and approx(d.bwlim_bps, ceil)
        assert d.state is model.GovState.NORMAL, f"degraded tick {i}: state moved"

    # 再来一拍真实超限恰好补满计数：证明 degraded 拍既没清零也没推进 over_secs。
    d = tick_one(g, now, usage("env-a", 1.3 * QUOTA_BYTES))
    want = max(QUOTA_BYTES * p.tighten_floor, ceil * p.md_factor)
    assert approx(d.bwlim_bps, want)
    assert d.state is model.GovState.TIGHTENING
    assert d.changed


def test_per_env_param_override():
    """环境级 GovParams 覆盖必须驱动算法的每一个阈值与步长。"""
    over = model.GovParams(
        elastic_ceiling=1.2,
        low_watermark=0.8,
        tighten_after_s=1,
        recover_after_s=2,
        md_factor=0.5,
        tighten_floor=0.8,
        ai_step_frac=0.1,
    )
    g = new_governor()
    g.update_config([env_quota("env-a", QUOTA_BITS, over)])
    now = T0

    # 覆盖后的 ceiling（0.9q 落在 0.8q~q 死区内）。
    d = tick_one(g, now, usage("env-a", 0.9 * QUOTA_BYTES))
    now += 1.0
    assert approx(d.bwlim_bps, 1.2 * QUOTA_BYTES)

    # tighten_after_s=1：单拍超限立即收紧；md_factor=0.5 打穿下限，
    # tighten_floor=0.8 钳位。
    d = tick_one(g, now, usage("env-a", 1.5 * QUOTA_BYTES))
    now += 1.0
    assert approx(d.bwlim_bps, 0.8 * QUOTA_BYTES)
    assert d.state is model.GovState.TIGHTENING

    # recover_after_s=2、ai_step_frac=0.1：第二拍低用量爬 0.1q。
    low = usage("env-a", 0.5 * QUOTA_BYTES)  # 低于 0.8q 水位
    d = tick_one(g, now, low)
    now += 1.0
    assert approx(d.bwlim_bps, 0.8 * QUOTA_BYTES) and not d.changed
    d = tick_one(g, now, usage("env-a", 0.5 * QUOTA_BYTES))
    assert approx(d.bwlim_bps, 0.9 * QUOTA_BYTES)
    assert d.state is model.GovState.RECOVERING


def test_env_scoping():
    """未配置的用量被忽略、配置了但无用量的环境不发射、移除后停止发射、
    决策携带配置的 targets。"""
    g = new_governor()
    ta1 = model.Target("n1", "fe_a1")
    ta2 = model.Target("n2", "fe_a2")
    tb = model.Target("n1", "fe_b")
    g.update_config([
        env_quota("env-a", QUOTA_BITS, None, ta1, ta2),
        env_quota("env-b", QUOTA_BITS, None, tb),
    ])

    # env-b 本拍没有用量；env-x 未配置。
    ds = g.tick(T0, [
        usage("env-a", 0.95 * QUOTA_BYTES),
        usage("env-x", 5 * QUOTA_BYTES),
    ])
    assert len(ds) == 1 and ds[0].env_id == "env-a"
    assert ds[0].targets == [ta1, ta2]

    # 移除 env-a：它的用量不再产生决策。
    g.update_config([env_quota("env-b", QUOTA_BITS, None, tb)])
    ds = g.tick(T0 + 1.0, [usage("env-a", 0.95 * QUOTA_BYTES)])
    assert ds == []


def test_duplicate_env_id_ignored(caplog):
    """同一配置里重复的 env_id：第一个生效，后者被告警忽略。"""
    g = new_governor()
    with caplog.at_level(logging.WARNING, logger="test.governor"):
        g.update_config([
            env_quota("env-a", QUOTA_BITS),          # quota → ceil 1.1×10MB/s
            env_quota("env-a", 4 * QUOTA_BITS),      # 重复：必须被忽略
        ])
    assert any("配置中出现重复的 env_id" in r.getMessage() for r in caplog.records)

    d = tick_one(g, T0, usage("env-a", 0.95 * QUOTA_BYTES))
    ceil = QUOTA_BYTES * model.GovParams().elastic_ceiling
    assert approx(d.bwlim_bps, ceil), "first duplicate must win"


def test_state_transition_and_adjustment_logs(caplog):
    """状态变迁与收紧/放松动作各自产生 info 日志。"""
    g = new_governor()
    g.update_config([env_quota("env-a", QUOTA_BITS)])
    p = model.GovParams()
    now = T0

    with caplog.at_level(logging.INFO, logger="test.governor"):
        for _ in range(p.tighten_after_s + 1):
            tick_one(g, now, usage("env-a", 1.3 * QUOTA_BYTES))
            now += 1.0
        for _ in range(p.recover_after_s + 1):
            tick_one(g, now, usage("env-a", 0.5 * QUOTA_BYTES))
            now += 1.0
    msgs = [r.getMessage() for r in caplog.records]
    assert any("触发限速状态变迁" in m for m in msgs)
    assert any("乘性收紧整形值" in m for m in msgs)
    assert any("加性放松整形值" in m for m in msgs)
