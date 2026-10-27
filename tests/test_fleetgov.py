# tests.test_fleetgov —— 总控分配算法（纯函数级）。
#
# 核心性质：总量守恒（Σ分配 + 冻结记账 = total）、保底防饿死、被压
# 节点能长大、写抑制、fail-static（冻结节点配额绝不碰）、分配值永远
# ≥ min_quota（绝不下发 ≤0——0 在节点语义是"不限"）。

from __future__ import annotations

import logging

import pytest

from rl_limiter import fleetgov
from rl_limiter.fleetconfig import AllocConfig

log = logging.getLogger("test")


def node(name="n1", online=True, executable=True, degraded=False,
         token="tok", quota=None, egress=0.0, ewma=None, applied=None,
         last_ok=1.0):
    s = fleetgov.NodeState(name=name, token=token)
    s.online = online
    s.executable = executable
    s.degraded = degraded
    s.quota_mbps = quota
    s.egress_mbps = egress
    s.demand_ewma_mbps = ewma
    s.applied_mbps = applied
    s.last_ok_ts = last_ok
    return s


def gov():
    return fleetgov.FleetGovernor(log)


def P(**kw):
    p = AllocConfig()
    for k, v in kw.items():
        setattr(p, k, v)
    return p


def test_proportional_allocation_conserves_total():
    """按需比例分配，Σdesired = total（无冻结时）。"""
    a = node("a", quota=100, egress=90)     # 被压（90 ≥ 0.9×100）→ 虚报
    b = node("b", quota=100, egress=30)
    c = node("c", quota=100, egress=10)
    states = [a, b, c]
    writes = gov().allocate_once(states, 300, P(smooth_alpha=1.0))
    total = sum(s.desired_mbps for s in states)
    assert total == pytest.approx(300, abs=0.5)     # 0.1 取整的漂移
    # 需求大者份额大。
    assert a.desired_mbps > b.desired_mbps > c.desired_mbps
    assert writes      # 与现值差距大，必然有下发


def test_guaranteed_share_prevents_starvation():
    """零流量节点拿到保底份额，绝不为 0。"""
    busy = node("busy", quota=0, egress=280)
    idle = node("idle", quota=0, egress=0)
    g = gov()
    g.allocate_once([busy, idle], 300, P(smooth_alpha=1.0))
    assert idle.desired_mbps >= 0.2 * 300 / 2 - 0.1   # guaranteed
    assert busy.desired_mbps + idle.desired_mbps == pytest.approx(300, abs=0.5)


def test_pressed_node_grows_round_by_round():
    """被配额压着的节点经 press_boost 虚报需求，逐轮长大。"""
    pressed = node("p", quota=50, egress=50)      # 贴着配额跑
    idle = node("i", quota=250, egress=5)
    g = gov()
    p = P(smooth_alpha=1.0)
    g.allocate_once([pressed, idle], 300, p)
    first = pressed.desired_mbps
    assert first > 50                             # 第一轮就该长
    # 模拟节点吃满新配额后再跑一轮：继续长。
    pressed.quota_mbps = first
    pressed.egress_mbps = first * 0.95
    idle.quota_mbps = idle.desired_mbps
    g.allocate_once([pressed, idle], 300, p)
    assert pressed.desired_mbps > first


def test_frozen_nodes_reserved_and_untouched():
    """离线/degraded/不可执行/无令牌 → 冻结：配额绝不动（不进写清单），
    份额如实扣池。"""
    offline = node("off", online=False, quota=80)
    degraded = node("deg", degraded=True, quota=40)
    noexec = node("ne", executable=False, quota=30)
    notoken = node("nt", token="", quota=10)
    live = node("live", quota=0, egress=50)
    states = [offline, degraded, noexec, notoken, live]
    writes = gov().allocate_once(states, 300, P(smooth_alpha=1.0))
    assert {s.name for s, _ in writes} == {"live"}
    assert live.desired_mbps == pytest.approx(300 - 80 - 40 - 30 - 10, abs=0.5)
    for s in (offline, degraded, noexec, notoken):
        assert s.desired_mbps is None


def test_oversold_clamps_to_minimum():
    """冻结配额之和超过总带宽（超卖）：可控节点压到均分的残池/下限，
    绝不出现负值或 0。"""
    offline = node("off", online=False, quota=400)
    live1 = node("l1", quota=10, egress=5)
    live2 = node("l2", quota=10, egress=5)
    gov().allocate_once([offline, live1, live2], 300, P())
    assert live1.desired_mbps >= 0.5
    assert live2.desired_mbps >= 0.5


def test_all_idle_splits_evenly_and_write_suppressed():
    """全体空闲：均分；分配值等于现值时写抑制 → 连续轮零写。"""
    a = node("a", quota=150, egress=0, ewma=0.0)
    b = node("b", quota=150, egress=0, ewma=0.0)
    g = gov()
    w1 = g.allocate_once([a, b], 300, P())
    assert a.desired_mbps == b.desired_mbps == pytest.approx(150, abs=0.1)
    assert w1 == []                                  # 与现值一致，零写
    w2 = g.allocate_once([a, b], 300, P())
    assert w2 == []


def test_write_suppression_thresholds():
    """分配值与节点现值几乎一致（差 < max(5%, 1Mbps)）时不下发。"""
    # 需求 50:100、保底 0.2 → 分配 = 30+80 / 30+160 = 110/190。
    a = node("a", quota=110, egress=50, ewma=50.0)
    b = node("b", quota=190, egress=100, ewma=100.0)
    writes = gov().allocate_once([a, b], 300, P(smooth_alpha=0.01))
    assert writes == []
    assert (a.desired_mbps, b.desired_mbps) == (110.0, 190.0)


def test_desired_never_at_or_below_zero():
    """极小的池也绝不产生 ≤0 的分配（0 = 节点语义"不限"）。"""
    a = node("a", quota=1, egress=1)
    b = node("b", quota=1, egress=0)
    gov().allocate_once([a, b], 1.5, P())
    assert a.desired_mbps >= 0.5 and b.desired_mbps >= 0.5


def test_starting_at_zero_never_touches_nodes():
    """以 total=0 启动 = 只汇总：不知道节点上的限速是谁设的，绝不解除。"""
    a = node("a", quota=100, egress=50)
    g = gov()
    for _ in range(3):
        assert g.allocate_once([a], 0, P()) == []
    assert a.desired_mbps is None and not a.release_pending


def test_cancel_releases_every_node_immediately():
    """total 从 >0 改成 0：在线有令牌的节点逐台解除（None = DELETE）。"""
    a = node("a", quota=100, egress=50)
    b = node("b", quota=80, egress=10)
    free = node("free", quota=0)              # 自报已是不限：无需再写
    off = node("off", quota=60, online=False)
    ro = node("ro", quota=40, token="")      # 无令牌：从没被总控控制过
    states = [a, b, free, off, ro]
    g = gov()
    g.allocate_once(states, 300, P())
    writes = g.allocate_once(states, 0, P())
    assert sorted((s.name, v) for s, v in writes) == \
        [("a", None), ("b", None)]
    assert all(s.desired_mbps is None for s in states)
    assert not free.release_pending          # 已不限，直接了结
    assert off.release_pending               # 离线：等恢复
    assert not ro.release_pending


def test_pending_release_retried_until_done():
    a = node("a", quota=100)
    off = node("off", quota=60, online=False)
    g = gov()
    g.allocate_once([a, off], 300, P())
    assert [s.name for s, _ in g.allocate_once([a, off], 0, P())] == ["a"]
    # 下发失败（release_pending 没被清）：下一轮再来。
    assert [s.name for s, _ in g.allocate_once([a, off], 0, P())] == ["a"]
    # a 解除成功（fleetpoll 清标记），off 恢复在线：轮到 off。
    a.release_pending = False
    off.online = True
    assert [(s.name, v) for s, v in g.allocate_once([a, off], 0, P())] == \
        [("off", None)]
    # 都解除完：之后只汇总，绝不再碰节点（哪怕有人又手工设了限速）。
    off.release_pending = False
    a.quota_mbps = 50
    assert g.allocate_once([a, off], 0, P()) == []


def test_new_total_cancels_pending_release():
    off = node("off", quota=60, online=False)
    g = gov()
    g.allocate_once([off], 300, P())
    g.allocate_once([off], 0, P())
    assert off.release_pending
    g.allocate_once([off], 200, P())
    assert not off.release_pending           # 重新做总量控制：作废


def test_never_polled_node_counts_zero():
    """从未轮询成功的节点（quota 未知）按 0 记账，不挤占池。"""
    ghost = node("g", online=False, quota=None, last_ok=0)
    live = node("l", quota=0, egress=10)
    gov().allocate_once([ghost, live], 300, P(smooth_alpha=1.0))
    assert live.desired_mbps == pytest.approx(300, abs=0.5)


def test_manual_change_detected_and_overridden():
    """节点自报配额 ≠ 总控下发值：WARN + 本轮按分配覆盖（进写清单）。"""
    a = node("a", quota=999, egress=50, applied=120.0)   # 被人手工改成 999
    b = node("b", quota=120, egress=50, applied=120.0)
    writes = gov().allocate_once([a, b], 240, P(smooth_alpha=1.0))
    assert any(s.name == "a" for s, _ in writes)


def test_ewma_smooths_alternating_traffic():
    """流量在两值间交替时，EWMA 让分配不跟着来回翻。"""
    a = node("a", quota=150, egress=100, ewma=100.0)
    b = node("b", quota=150, egress=100, ewma=100.0)
    g = gov()
    p = P(smooth_alpha=0.3)
    seen = []
    for i in range(6):
        a.egress_mbps = 140 if i % 2 == 0 else 60
        b.egress_mbps = 60 if i % 2 == 0 else 140
        g.allocate_once([a, b], 300, p)
        seen.append((a.desired_mbps, b.desired_mbps))
    # 相邻两轮 a 的分配变化远小于流量摆幅（80Mbps）。
    deltas = [abs(seen[i][0] - seen[i - 1][0]) for i in range(1, len(seen))]
    assert max(deltas) < 40
