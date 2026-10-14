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


# ---------------------------------------------------------------------------
# 实例级环：实例出向总带宽 对照 instance_quota 目标 → 入向总闸
# ---------------------------------------------------------------------------

def feed_many(g, means, names_ports=None):
    """按 {名: 出向 mean Mbps} 喂一拍（总量 = 各端口之和）。"""
    g.record(0.0, [usage(n, m) for n, m in means.items()])


def test_instance_over_target_shrinks_gate_gently():
    """总出向超实例目标 → 总闸下调，单步封顶 -10%，上限 = 登记的
    入向上限之和（fe_main 10Mbps）。"""
    g = gov()
    feed_many(g, {"fe_main": 200.0})
    # frontend 不带单端口 quotas 目标：只有实例环动作。
    assert g.adjust_once([fe(quota=0.0)], LIMITS, inst_target=40.0)
    assert g.current_instance() == pytest.approx(10.0 * 0.90)
    assert g.effective_instance_bits(LIMITS, 40.0) == int(9.0e6)
    assert g.current() == {}            # 单端口环没动


def test_instance_deadband_and_recovery():
    g = gov()
    feed_many(g, {"fe_main": 200.0})
    g.adjust_once([fe(quota=0.0)], LIMITS, inst_target=40.0)   # 10→9
    # 死区：总出向落在 [0.85, 1.0]×40 → 不动。
    feed_many(g, {"fe_main": 38.0})
    assert not g.adjust_once([fe(quota=0.0)], LIMITS, inst_target=40.0)
    # 低了 → +5%×上限/步爬回；到顶退出动态（None = 全开）。
    feed_many(g, {"fe_main": 10.0})
    assert g.adjust_once([fe(quota=0.0)], LIMITS, inst_target=40.0)
    assert g.current_instance() == pytest.approx(9.5)
    for _ in range(5):
        g.adjust_once([fe(quota=0.0)], LIMITS, inst_target=40.0)
    assert g.current_instance() is None
    assert g.effective_instance_bits(LIMITS, 40.0) == int(10e6)


def test_instance_max_falls_back_to_target_without_limits():
    """一个 request_limits 都没登记：总闸上限退化为目标值本身（有界
    的"全开"），实例环照样可用。"""
    g = gov()
    assert g.effective_instance_bits({}, 40.0) == int(40e6)
    feed_many(g, {"fe_main": 200.0})
    assert g.adjust_once([fe(quota=0.0)], {}, inst_target=40.0)
    assert g.current_instance() == pytest.approx(36.0)


def test_instance_target_removed_restores_flat():
    g = gov()
    feed_many(g, {"fe_main": 200.0})
    g.adjust_once([fe(quota=0.0)], LIMITS, inst_target=40.0)
    assert g.current_instance() is not None
    assert g.adjust_once([fe(quota=0.0)], LIMITS, inst_target=0.0)
    assert g.current_instance() is None
    assert g.effective_instance_bits(LIMITS, 0.0) == 0


def test_instance_loop_disabled_when_not_capable():
    """police 模式没有层级可言：instance_capable=False 时实例环整个
    关闭（不调、不下发总闸），单端口环不受影响。"""
    cfg = ingressgov.IngressGovConfig(enabled=True)
    g = ingressgov.IngressGovernor(cfg, log, instance_capable=False)
    feed_many(g, {"fe_main": 200.0})
    assert not g.adjust_once([fe(quota=0.0)], LIMITS, inst_target=40.0)
    assert g.effective_instance_bits(LIMITS, 40.0) == 0


def test_both_loops_coexist():
    """单端口环与实例环同时在跑：各调各的，互不覆盖。"""
    g = gov()
    feed_many(g, {"fe_main": 200.0})
    assert g.adjust_once([fe(quota=40.0)], LIMITS, inst_target=50.0)
    assert g.current()["fe_main"] == pytest.approx(9.0)
    assert g.current_instance() == pytest.approx(9.0)


def test_instance_first_tighten_lands_on_measured_ingress():
    """首次收紧带实测入向带宽：总闸直接落位到 实测×1.1（而不是从
    上限爬降）——入向因 ACK 杠杆比出向小一两个数量级，从上限起步
    前几十步全是空转。落位后续步仍是小步细调。"""
    g = gov()
    feed_many(g, {"fe_main": 200.0})
    assert g.adjust_once([fe(quota=0.0)], LIMITS, inst_target=40.0,
                         ingress_now_mbps=1.5)
    assert g.current_instance() == pytest.approx(1.65)   # 1.5×1.1
    # 第二轮起回到小步：不再重复落位。
    feed_many(g, {"fe_main": 200.0})
    g.adjust_once([fe(quota=0.0)], LIMITS, inst_target=40.0,
                  ingress_now_mbps=1.5)
    assert g.current_instance() == pytest.approx(1.65 * 0.9)


def test_instance_landing_clamped_to_floor_and_max():
    g = gov()
    feed_many(g, {"fe_main": 200.0})
    # 实测高于上限（max=10）：落位没有收紧效果（≥ 当前值），必须退回
    # 正常步降而不是每轮空转。
    assert g.adjust_once([fe(quota=0.0)], LIMITS, inst_target=40.0,
                         ingress_now_mbps=50.0)
    assert g.current_instance() == pytest.approx(9.0)
    # 实测低于下限（floor=1.0）→ 托到下限。
    g2 = gov()
    feed_many(g2, {"fe_main": 200.0})
    assert g2.adjust_once([fe(quota=0.0)], LIMITS, inst_target=40.0,
                          ingress_now_mbps=0.1)
    assert g2.current_instance() == pytest.approx(1.0)


def test_instance_no_measurement_falls_back_to_stepdown():
    """没有实测（None）→ 回退旧行为：从上限按步幅爬降，绝不拿猜的
    数字当实测。"""
    g = gov()
    feed_many(g, {"fe_main": 200.0})
    assert g.adjust_once([fe(quota=0.0)], LIMITS, inst_target=40.0,
                         ingress_now_mbps=None)
    assert g.current_instance() == pytest.approx(9.0)
