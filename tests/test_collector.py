# tests.test_collector —— 采样层的时序语义测试（单 HAProxy，单位 = frontend）。
#
# 重点全在"时序规则"上，因为这几条错了不会报错、只会让曲线与告警悄悄失真：
#   1. 首次采样只建基线（速率未知，不喂窗口）；
#   2. 计数回绕（HAProxy reload）沿用上一秒速率并重建基线；
#   3. 采样失败 fail-static（沿用上一秒），连续失败进 degraded；
#   4. 受管但 stats 里不存在的 frontend 是真实的零，必须进窗口。

from __future__ import annotations

import pytest

from rl_limiter import model
from rl_limiter.collector import DEGRADED_FAILURE_THRESHOLD, Collector


class FakeClient:
    """假 RuntimeClient：按脚本逐次返回 stats 或抛异常。"""

    def __init__(self, script):
        self._script = list(script)
        self.calls = 0

    async def show_stat(self):
        self.calls += 1
        item = self._script.pop(0) if self._script else []
        if isinstance(item, BaseException):
            raise item
        return item


def st(name, bytes_out, conn=0):
    return model.FrontendStat(name=name, bytes_out=bytes_out, conn_cur=conn)


def collector(script, managed=("fe_a",)):
    c = Collector(FakeClient(script))
    c.set_managed(set(managed))
    return c


async def test_first_tick_only_baselines():
    """首次采样只建基线：速率未知，既不产出速率也不喂窗口。"""
    c = collector([[st("fe_a", 1000, 3)]])
    (u,) = await c.tick(0.0)
    assert u.name == "fe_a"
    assert u.rate_bps == 0.0
    assert u.mean10_bps == 0.0      # 未喂窗口，均值仍是空窗口的 0
    assert u.conn_cur == 3          # 连接数是瞬时值，不依赖差分


async def test_rate_is_difference_of_counters():
    """第二拍起产出速率 = 相邻两次 bytes_out 之差（1 拍 = 1 秒）。"""
    c = collector([[st("fe_a", 1000)], [st("fe_a", 3500)], [st("fe_a", 4500)]])
    await c.tick(0.0)
    (u2,) = await c.tick(1.0)
    assert u2.rate_bps == 2500
    assert u2.mean10_bps == 2500     # 窗口里只有这一个样本
    (u3,) = await c.tick(2.0)
    assert u3.rate_bps == 1000
    assert u3.mean10_bps == pytest.approx((2500 + 1000) / 2)


async def test_counter_reset_holds_previous_rate():
    """计数回绕（reload 后从零重来）：差分为负不可用，沿用上一秒速率
    并用新值重建基线，下一秒差分即恢复正常。"""
    c = collector([
        [st("fe_a", 1000)], [st("fe_a", 3000)],   # 建基线 → 速率 2000
        [st("fe_a", 50)],                          # 回绕 → 沿用 2000
        [st("fe_a", 1050)],                        # 从新基线差分 → 1000
    ])
    await c.tick(0.0)
    await c.tick(1.0)
    (u3,) = await c.tick(2.0)
    assert u3.rate_bps == 2000, "回绕当秒应沿用上一秒速率"
    (u4,) = await c.tick(3.0)
    assert u4.rate_bps == 1000, "下一秒应从新基线正常差分"


async def test_sampling_failure_is_fail_static():
    """采样失败沿用上一秒速率继续推进窗口——骤降为零会让均值失真、
    进而触发假的"已恢复正常"。"""
    c = collector([
        [st("fe_a", 0)], [st("fe_a", 2000)],       # 速率 2000
        ConnectionError("socket 没了"),
    ])
    await c.tick(0.0)
    await c.tick(1.0)
    (u,) = await c.tick(2.0)
    assert u.rate_bps == 2000, "失败当秒应沿用上一秒速率"
    assert u.degraded is False, "单次失败还不到降级阈值"


async def test_degraded_after_consecutive_failures_and_recovers():
    """连续失败达阈值进入 degraded；任何一次成功立即解除（不粘滞）。"""
    script = [[st("fe_a", 0)], [st("fe_a", 1000)]]
    script += [ConnectionError("down")] * DEGRADED_FAILURE_THRESHOLD
    script += [[st("fe_a", 2000)]]
    c = collector(script)
    await c.tick(0.0)
    await c.tick(1.0)
    for i in range(DEGRADED_FAILURE_THRESHOLD):
        (u,) = await c.tick(2.0 + i)
    assert u.degraded is True and c.degraded is True
    (u,) = await c.tick(100.0)
    assert u.degraded is False and c.degraded is False


async def test_managed_but_missing_frontend_counts_as_zero():
    """受管却不在 stats 里的 frontend 是真实的零（cfg 还没下发就是这种
    情况）：零值必须进窗口，否则均值会停在旧值上虚高。"""
    c = collector([[st("fe_a", 0)], [st("fe_a", 8000)], []], managed=("fe_a",))
    await c.tick(0.0)
    (u2,) = await c.tick(1.0)
    assert u2.rate_bps == 8000
    (u3,) = await c.tick(2.0)
    assert u3.rate_bps == 0.0
    assert u3.mean10_bps == pytest.approx(8000 / 2), "零值应进入窗口拉低均值"


async def test_unmanaged_frontend_is_ignored():
    """未纳管的 frontend 不产出用量。"""
    c = collector([[st("fe_a", 0), st("other", 500)]], managed=("fe_a",))
    usages = await c.tick(0.0)
    assert [u.name for u in usages] == ["fe_a"]


async def test_newly_managed_frontend_has_continuous_rate():
    """先前已见过（基线在刷）的 frontend 被纳管后，第一秒就有正确速率，
    不必重走"首采样丢一秒"。"""
    client = FakeClient([[st("fe_x", 1000)], [st("fe_x", 3000)]])
    c = Collector(client)
    c.set_managed({"fe_x"})
    await c.tick(0.0)
    (u,) = await c.tick(1.0)
    assert u.rate_bps == 2000


async def test_output_is_sorted_and_stable():
    """输出按名字排序且集合稳定（下游不必处理"单元忽隐忽现"）。"""
    c = collector([[st("fe_b", 0), st("fe_a", 0)]], managed=("fe_a", "fe_b"))
    usages = await c.tick(0.0)
    assert [u.name for u in usages] == ["fe_a", "fe_b"]
