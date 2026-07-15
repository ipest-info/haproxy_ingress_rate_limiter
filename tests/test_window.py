# 平滑原语（SlidingWindow / Ewma）的单元测试，语义对照 Go 版
# agent/internal/collector/window.go。

import pytest

from rl_limiter.window import Ewma, SlidingWindow


class TestSlidingWindow:
    def test_empty_mean_is_zero(self):
        w = SlidingWindow(10)
        assert w.mean() == 0.0

    def test_partial_fill_averages_over_pushed_count(self):
        # 未满窗口只在已推入的样本上求均值——冷启动不用零值凑满窗口。
        w = SlidingWindow(10)
        w.push(100.0)
        assert w.mean() == pytest.approx(100.0)
        w.push(200.0)
        assert w.mean() == pytest.approx(150.0)
        w.push(300.0)
        assert w.mean() == pytest.approx(200.0)

    def test_full_window_evicts_oldest(self):
        # 推入 15 个样本后，均值只反映最近 10 个（环形覆盖最旧）。
        w = SlidingWindow(10)
        for v in range(15):  # 0..14
            w.push(float(v))
        assert w.mean() == pytest.approx(sum(range(5, 15)) / 10)

    def test_exactly_full(self):
        w = SlidingWindow(3)
        for v in (1.0, 2.0, 3.0):
            w.push(v)
        assert w.mean() == pytest.approx(2.0)
        w.push(4.0)  # 淘汰 1.0
        assert w.mean() == pytest.approx(3.0)

    def test_nonpositive_capacity_clamped_to_one(self):
        # capacity <= 0 被钳制为 1，push/mean 永不崩溃。
        for cap in (0, -5):
            w = SlidingWindow(cap)
            w.push(7.0)
            w.push(9.0)
            assert w.mean() == pytest.approx(9.0)  # 容量 1：只保留最新样本


class TestEwma:
    def test_first_sample_seeds(self):
        # 首样本直接落位，而不是从 0 衰减爬升。
        e = Ewma(2.0 / 61)
        assert e.value == 0.0
        e.update(500.0)
        assert e.value == pytest.approx(500.0)

    def test_recurrence(self):
        alpha = 0.5
        e = Ewma(alpha)
        e.update(100.0)
        e.update(200.0)
        assert e.value == pytest.approx(alpha * 200.0 + (1 - alpha) * 100.0)  # 150
        e.update(0.0)
        assert e.value == pytest.approx(75.0)

    def test_span_alpha_smooths_slowly(self):
        # alpha = 2/(60+1)：单个新样本只应小幅移动均值（约 3.3%）。
        alpha = 2.0 / 61
        e = Ewma(alpha)
        e.update(1000.0)
        e.update(0.0)
        assert e.value == pytest.approx(1000.0 * (1 - alpha))

    def test_constant_input_stays_constant(self):
        e = Ewma(2.0 / 61)
        for _ in range(100):
            e.update(42.0)
        assert e.value == pytest.approx(42.0)
