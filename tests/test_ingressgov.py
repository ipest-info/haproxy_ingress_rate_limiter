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
    """流量低了逐步上调（+5%×**当前值**/次，乘性），爬回上限后回到静态
    语义。锚上限的加性步在动态值远低于上限时是一步翻倍的巨步，会与
    下调形成极限环（带宽一会高一会低）——所以必须锚当前值。"""
    g = gov()
    feed(g, 80.0)
    g.adjust_once([fe()], LIMITS)      # 10 → 9
    feed(g, 10.0)                      # 远低于目标 → 该松了
    assert g.adjust_once([fe()], LIMITS)
    assert g.current()["fe_main"] == pytest.approx(9.0 * 1.05)
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
    入向上限之和（fe_main 10Mbps）。无实测时第一轮等待（落位值得等
    一轮），第二轮回退步降。"""
    g = gov()
    feed_many(g, {"fe_main": 200.0})
    # frontend 不带单端口 quotas 目标：只有实例环动作。
    assert not g.adjust_once([fe(quota=0.0)], LIMITS, inst_target=40.0)
    assert g.adjust_once([fe(quota=0.0)], LIMITS, inst_target=40.0)
    assert g.current_instance() == pytest.approx(10.0 * 0.90)
    assert g.effective_instance_bits(LIMITS, 40.0) == int(9.0e6)
    assert g.current() == {}            # 单端口环没动


def test_instance_deadband_and_recovery():
    g = gov()
    feed_many(g, {"fe_main": 200.0})
    g.adjust_once([fe(quota=0.0)], LIMITS, inst_target=40.0)   # 等实测
    g.adjust_once([fe(quota=0.0)], LIMITS, inst_target=40.0)   # 10→9
    # 死区：总出向落在 [0.85, 1.0]×40 → 不动。
    feed_many(g, {"fe_main": 38.0})
    assert not g.adjust_once([fe(quota=0.0)], LIMITS, inst_target=40.0)
    # 低了 → +5%×当前值/步（乘性）爬回；到顶退出动态（None = 全开）。
    feed_many(g, {"fe_main": 10.0})
    assert g.adjust_once([fe(quota=0.0)], LIMITS, inst_target=40.0)
    assert g.current_instance() == pytest.approx(9.0 * 1.05)
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
    assert not g.adjust_once([fe(quota=0.0)], {}, inst_target=40.0)  # 等实测
    assert g.adjust_once([fe(quota=0.0)], {}, inst_target=40.0)
    assert g.current_instance() == pytest.approx(36.0)


def test_instance_target_removed_restores_flat():
    g = gov()
    feed_many(g, {"fe_main": 200.0})
    g.adjust_once([fe(quota=0.0)], LIMITS, inst_target=40.0,
                  ingress_now_mbps=1.5)
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
    assert g.adjust_once([fe(quota=40.0)], LIMITS, inst_target=50.0,
                         ingress_now_mbps=5.0)
    assert g.current()["fe_main"] == pytest.approx(9.0)
    assert g.current_instance() == pytest.approx(5.5)   # 5.0×1.1 落位


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
    # 实测极低 → 落位只受 0.1Mbps 绝对下限保护，**不**被
    # floor_ratio×上限（出向刻度）托高——那个下限比入向均衡点高一个
    # 量级，会让总闸卡死、出向永远压不到目标（e2e 实测抓到）。
    g2 = gov()
    feed_many(g2, {"fe_main": 200.0})
    assert g2.adjust_once([fe(quota=0.0)], LIMITS, inst_target=40.0,
                          ingress_now_mbps=0.05)
    assert g2.current_instance() == pytest.approx(0.1)


def test_instance_no_measurement_waits_once_then_falls_back():
    """没有实测（None）：第一轮等待（差分基准刚建，落位值得等 5~15s），
    第二轮仍没有才回退从上限爬降——坏掉的测量源不能把实例环卡死。"""
    g = gov()
    feed_many(g, {"fe_main": 200.0})
    assert not g.adjust_once([fe(quota=0.0)], LIMITS, inst_target=40.0,
                             ingress_now_mbps=None)
    assert g.current_instance() is None
    assert g.adjust_once([fe(quota=0.0)], LIMITS, inst_target=40.0,
                         ingress_now_mbps=None)
    assert g.current_instance() == pytest.approx(9.0)
    # 等待后下一轮实测到了也照样能落位（新 governor 验证顺序无妨）。
    g2 = gov()
    feed_many(g2, {"fe_main": 200.0})
    assert not g2.adjust_once([fe(quota=0.0)], LIMITS, inst_target=40.0)
    assert g2.adjust_once([fe(quota=0.0)], LIMITS, inst_target=40.0,
                          ingress_now_mbps=1.5)
    assert g2.current_instance() == pytest.approx(1.65)


def test_instance_floor_anchors_to_landing():
    """落位后下限锚定在落位值刻度（floor_ratio×落位值，绝对下限
    0.1）——后续步降能走到入向均衡点，不被出向刻度的旧下限卡住。"""
    g = gov()
    feed_many(g, {"fe_main": 200.0})
    g.adjust_once([fe(quota=0.0)], {}, inst_target=40.0,
                  ingress_now_mbps=1.5)         # 落位 1.65（上限退化=40）
    for _ in range(40):
        feed_many(g, {"fe_main": 200.0})
        g.adjust_once([fe(quota=0.0)], {}, inst_target=40.0)
    # 旧语义下限 = 0.1×40 = 4.0；新语义 = max(0.1, 0.1×1.65) = 0.165。
    assert g.current_instance() == pytest.approx(0.165, rel=0.01)


def test_instance_up_step_anchors_to_current_not_max():
    """上调锚**当前值**（乘性），绝对步长下限 0.1Mbps。

    回归：此前上调是 step_up×上限 的加性步。总闸落位后收敛在上限的
    几十分之一刻度（ACK 杠杆），那一步等于一步翻倍——上冲过头→出向
    猛涨→猛降回来，稳定的极限环（页面上带宽一会高一会低）。乘性锚
    当前值后两个方向步长对称，流量稳定时收敛进死区不再折腾。"""
    g = gov()
    feed_many(g, {"fe_main": 200.0})
    g.adjust_once([fe(quota=0.0)], LIMITS, inst_target=40.0,
                  ingress_now_mbps=0.9)          # 落位 0.99
    assert g.current_instance() == pytest.approx(0.99)
    # 低了 → 上调一步：0.99 + max(0.1, 0.99×0.05) = 1.09（绝对下限
    # 兜底生效）。旧算法是 0.99 + 0.05×10 = 1.49，一步 +50%。
    feed_many(g, {"fe_main": 10.0})
    assert g.adjust_once([fe(quota=0.0)], LIMITS, inst_target=40.0)
    assert g.current_instance() == pytest.approx(1.09)
    # 再上一步：1.09 + max(0.1, 0.0545) = 1.19——步幅跟着当前值的
    # 刻度走，不会随上限/目标的刻度跳变。
    assert g.adjust_once([fe(quota=0.0)], LIMITS, inst_target=40.0)
    assert g.current_instance() == pytest.approx(1.19)


# ---------------------------------------------------------------------------
# 调节视图数据源
# ---------------------------------------------------------------------------

def test_regulation_view_exposes_all_loop_values():
    """regulation_view 把两个环的全部调节量摊平（Mbps）：实例环的
    目标/动态总闸/上限/观测/实测流入/落位标记 + 每个受调端口的
    目标/动态上限/登记上限/观测。"""
    g = gov()
    feed_many(g, {"fe_main": 200.0})
    g.adjust_once([fe(quota=40.0)], LIMITS, inst_target=50.0,
                  ingress_now_mbps=2.0)
    v = g.regulation_view([fe(quota=40.0)], LIMITS, inst_target=50.0)
    i = v["instance"]
    assert i["target_mbps"] == 50.0
    assert i["gate_mbps"] == pytest.approx(2.2)       # 2.0×1.1 落位
    assert i["gate_max_mbps"] == 10.0
    assert i["ingress_mbps"] == 2.0                   # 最近一轮的实测缓存
    assert i["egress_mbps"] == pytest.approx(200.0)
    assert i["landed"] is True
    f = v["frontends"]["fe_main"]
    assert f["target_mbps"] == 40.0 and f["max_mbps"] == 10.0
    assert f["cap_mbps"] == pytest.approx(9.0)        # 单端口环也收紧了
    assert f["egress_mbps"] == pytest.approx(200.0)


def test_regulation_view_none_when_no_loop_active():
    """没实例目标、也没有任何"登记了入向上限 + 出向目标"的端口：没有
    环在工作，返回 None（页面的调节视图卡保持隐藏）。"""
    g = gov()
    assert g.regulation_view([fe(quota=0.0)], LIMITS, inst_target=0.0) is None
    # 没被调过的端口 cap_mbps=None（静态/全开），但视图照常给出。
    v = g.regulation_view([fe(quota=40.0)], LIMITS, inst_target=0.0)
    assert v["instance"] is None
    assert v["frontends"]["fe_main"]["cap_mbps"] is None
