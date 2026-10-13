# tests/test_ingressgov —— 入向动态限速控制器（闭环）的单元测试。
#
# 控制器是纯函数式的（record 喂观测、adjust_once 出决策），不碰 tc、
# 不碰网络——下发侧（tcingress）有自己的测试，这里只验算法：
#   平缓性（步幅封顶）、死区、下限/上限、epsilon 跳过、degraded 冻结、
#   登记变更时的清理语义。
import logging

import pytest

from rl_limiter import ingressgov, model

log = logging.getLogger("test")


def fe(name="fe_main", port=8080, quota=40.0):
    return model.FrontendConfig(name=name, bind_port=port, quota_mbps=quota)


def gov(**kw):
    cfg = ingressgov.IngressGovConfig(enabled=True, **kw)
    return ingressgov.IngressGovernor(cfg, log)


def usage(name="fe_main", mean_mbps=0.0):
    # record 只读 name 与 mean10_bps。
    return model.FrontendUsage(name=name, mean10_bps=mean_mbps * 1e6 / 8)


LIMITS = {"fe_main": model.RequestLimit(mbps=10.0, conn_rate=150)}


def feed(g, mean_mbps, name="fe_main"):
    g.record(0.0, [usage(name, mean_mbps)])


# ---------------------------------------------------------------------------
# 基本方向：超了压、低了松、死区不动
# ---------------------------------------------------------------------------

def test_over_target_decreases_gently():
    """超过目标 → 下调，但降幅封顶 step_down_max（刻意平缓）。"""
    g = gov()
    feed(g, 80.0)                      # 目标 40，超 100%
    assert g.adjust_once([fe()], LIMITS)
    cur = g.current()["fe_main"]
    # 超得再狠，单次也只降 10%：10 → 9。
    assert cur == pytest.approx(10.0 * 0.90)


def test_small_overshoot_decreases_proportionally():
    """小幅超出 → 按超出比例的一半下调（不到封顶值）。"""
    g = gov()
    feed(g, 44.0)                      # ratio=1.1 → 降 0.05
    assert g.adjust_once([fe()], LIMITS)
    assert g.current()["fe_main"] == pytest.approx(10.0 * 0.95)


def test_deadband_no_action():
    """死区 [low_ratio, high_ratio]×目标内不动作——这就是"平缓"的
    另一半：目标附近的小波动不折腾 tc。"""
    g = gov()
    for mean in (34.5, 38.0, 40.0):    # 0.86~1.0 × 40
        feed(g, mean)
        assert not g.adjust_once([fe()], LIMITS)
    assert g.current() == {}


def test_under_target_recovers_in_small_steps_to_max():
    """流量低了逐步上调（+5%×上限/次），爬回上限后回到静态语义。"""
    g = gov()
    feed(g, 80.0)
    g.adjust_once([fe()], LIMITS)      # 10 → 9
    feed(g, 10.0)                      # 远低于目标 → 该松了
    assert g.adjust_once([fe()], LIMITS)
    assert g.current()["fe_main"] == pytest.approx(9.0 + 0.5)
    # 一路爬回 10（上限封顶），到顶后 current() 清空=静态全开。
    for _ in range(10):
        g.adjust_once([fe()], LIMITS)
    assert g.current() == {}


def test_floor_is_respected():
    """连续超限也压不穿下限 floor_ratio×上限。"""
    g = gov()
    for _ in range(100):
        feed(g, 400.0)
        g.adjust_once([fe()], LIMITS)
    assert g.current()["fe_main"] >= 10.0 * 0.10 - 1e-9


def test_epsilon_skips_tiny_changes():
    """变化低于 apply_epsilon 不下发（返回 False、不记录）——
    入向规则整组重建，琐碎变化只会清零统计。"""
    g = gov(apply_epsilon=0.2)         # 夸大 epsilon 便于触发
    feed(g, 44.0)                      # 本该 -5%，低于 20% 的门槛
    assert not g.adjust_once([fe()], LIMITS)
    assert g.current() == {}


# ---------------------------------------------------------------------------
# 冻结与清理语义
# ---------------------------------------------------------------------------

def test_degraded_freezes():
    """采样失联时冻结现状——陈旧的 mean10 不配驱动调整。"""
    g = gov()
    feed(g, 400.0)
    assert not g.adjust_once([fe()], LIMITS, degraded=True)
    assert g.current() == {}


def test_no_target_no_dynamics():
    """没有出向目标（quotas 未登记/0）→ 静态下发，不参与调节。"""
    g = gov()
    feed(g, 400.0)
    assert not g.adjust_once([fe(quota=0.0)], LIMITS)
    assert g.current() == {}


def test_removed_registration_restores_static():
    """登记被删/清零/目标被删 → 动态值清掉，回到静态语义并触发下发。"""
    g = gov()
    feed(g, 80.0)
    g.adjust_once([fe()], LIMITS)
    assert g.current()
    # 出向目标被删（quotas 清掉）。
    assert g.adjust_once([fe(quota=0.0)], LIMITS)
    assert g.current() == {}


def test_effective_limits_merges_dynamic_values():
    """effective_limits：动态值替换 mbps，conn_rate 永远用静态登记。"""
    g = gov()
    feed(g, 80.0)
    g.adjust_once([fe()], LIMITS)
    eff = g.effective_limits(LIMITS)
    assert eff["fe_main"].mbps == pytest.approx(9.0)
    assert eff["fe_main"].conn_rate == 150
    # 没被调节过的照抄静态登记。
    other = {"fe_x": model.RequestLimit(mbps=5.0, conn_rate=0)}
    assert g.effective_limits(other)["fe_x"] is other["fe_x"]


def test_no_samples_yet_climbs_from_max_is_noop():
    """还没有任何样本（mean=0）且当前已全开 → 不动作（0 < low 但已到
    顶，不应该反复返回"有变化"去空转下发）。"""
    g = gov()
    assert not g.adjust_once([fe()], LIMITS)
    assert g.current() == {}
