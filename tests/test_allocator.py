# tests/test_allocator.py —— 加权分配算法（docs/01 §2.1 落地）的单元测试。

from __future__ import annotations

import math

from rl_limiter import allocator, model
from rl_limiter.allocator import FLOOR_FRAC, allocate

T1 = model.Target("n1", "fe1")
T2 = model.Target("n2", "fe2")
T3 = model.Target("n3", "fe3")


def test_empty_targets_returns_empty_dict():
    assert allocate(1_000_000.0, [], {}) == {}


def test_zero_ewma_splits_evenly():
    """Σewma == 0（环境刚上线毫无流量）：全部预算均分。"""
    bwlim = 3_000_000.0
    out = allocate(bwlim, [T1, T2, T3], {})
    assert set(out) == {T1, T2, T3}
    # 均分：保底 + 加权部分都均摊，每个恰为 bwlim/N（取整前精确相等）。
    each = int(math.floor(bwlim / 3))
    assert out[T1] == out[T2] == out[T3] == each


def test_zero_ewma_values_split_evenly():
    """ewma 表存在但值全为 0，与缺失等价：均分。"""
    bwlim = 1_000_000.0
    out = allocate(bwlim, [T1, T2], {T1: 0.0, T2: 0.0})
    assert out[T1] == out[T2] == int(math.floor(bwlim / 2))


def test_weighted_allocation_correctness():
    """加权正确性：95% 按 EWMA 占比分，5% 均摊保底，逐项向下取整。"""
    bwlim = 1_000_000.0
    ewma = {T1: 300.0, T2: 100.0}  # 3:1
    out = allocate(bwlim, [T1, T2], ewma)

    n = 2
    floor_each = bwlim * FLOOR_FRAC / n            # 25_000
    rest = bwlim * (1 - FLOOR_FRAC)                # 950_000
    want1 = int(math.floor(floor_each + rest * 0.75))  # 737_500
    want2 = int(math.floor(floor_each + rest * 0.25))  # 262_500
    assert out[T1] == want1
    assert out[T2] == want2


def test_sum_never_exceeds_budget():
    """Σ分配 ≤ bwlim：包括除不尽的权重组合。"""
    bwlim = 1_000_000.9
    ewma = {T1: 1.0, T2: 2.0, T3: 4.0}  # 1:2:4，除不尽
    out = allocate(bwlim, [T1, T2, T3], ewma)
    assert sum(out.values()) <= bwlim
    # 且不会因取整损失超过 N bytes/s（每项至多丢 1）。
    assert sum(out.values()) >= bwlim - 3 - 1


def test_floor_guarantees_idle_target_share():
    """保底生效：某 Target ewma=0 时仍获得 bwlim×5%/N，不被饿死。"""
    bwlim = 1_000_000.0
    ewma = {T1: 12_345.0, T2: 0.0}
    out = allocate(bwlim, [T1, T2], ewma)
    floor_each = int(math.floor(bwlim * FLOOR_FRAC / 2))  # 25_000
    assert out[T2] == floor_each, "idle target must keep its 5%/N floor"
    # 有用量的 Target 拿走全部加权部分 + 自己的保底。
    assert out[T1] == int(math.floor(bwlim * FLOOR_FRAC / 2 + bwlim * (1 - FLOOR_FRAC)))


def test_missing_ewma_entry_treated_as_zero():
    """ewma 表缺条目按 0 计：只丢加权份额，保底仍在。"""
    bwlim = 800_000.0
    out = allocate(bwlim, [T1, T2], {T1: 500.0})  # T2 缺失
    floor_each = int(math.floor(bwlim * FLOOR_FRAC / 2))
    assert out[T2] == floor_each


def test_result_is_int_bytes_per_sec():
    out = allocate(123_456.789, [T1, T2], {T1: 1.0, T2: 3.0})
    assert all(isinstance(v, int) for v in out.values())
    assert sum(out.values()) <= 123_456.789


def test_floor_frac_module_level_configurable(monkeypatch):
    """FLOOR_FRAC 是模块级可配常量：调整后保底比例随之变化。"""
    monkeypatch.setattr(allocator, "FLOOR_FRAC", 0.10)
    bwlim = 1_000_000.0
    out = allocator.allocate(bwlim, [T1, T2], {T1: 1.0, T2: 0.0})
    assert out[T2] == int(math.floor(bwlim * 0.10 / 2))
