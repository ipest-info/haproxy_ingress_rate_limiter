# Collector（多节点采集聚合）的行为测试。用假 RuntimeClient（内存 dict，
# 无真网络）驱动，覆盖：单节点失败隔离、per-Target 差分基线、
# baseline-only 跳过、per-Target EWMA、稳定 env 集合等场景。

import logging

import pytest

from rl_limiter import model
from rl_limiter.collector import (
    ABSENT_TICK_LIMIT,
    DEGRADED_FAILURE_THRESHOLD,
    Collector,
)

T = model.Target


class FakeNode:
    """假 RuntimeClient：show_stat 返回内存中的 frontend 表或抛出预设异常。"""

    def __init__(self):
        self.frontends: dict[str, tuple[int, int]] = {}  # name -> (bytes_out, conn_cur)
        self.fail: Exception | None = None

    def set(self, name: str, bytes_out: int, conn: int = 0):
        self.frontends[name] = (bytes_out, conn)

    def add(self, name: str, delta: int, conn: int | None = None):
        b, c = self.frontends[name]
        self.frontends[name] = (b + delta, c if conn is None else conn)

    def remove(self, name: str):
        del self.frontends[name]

    async def show_stat(self):
        if self.fail is not None:
            raise self.fail
        return [
            model.FrontendStat(name=n, bytes_out=b, conn_cur=c)
            for n, (b, c) in self.frontends.items()
        ]


def by_env(usages):
    return {u.env_id: u for u in usages}


# ---------------------------------------------------------------------------
# 稳态速率与窗口
# ---------------------------------------------------------------------------

async def test_steady_rate_window_and_baseline_only_skip():
    n1 = FakeNode()
    col = Collector({"n1": n1})
    col.set_mapping({T("n1", "fe"): "env1"})

    # tick1：首次采样只建基线——速率未知，rate=0 且**不喂窗口**，但连接数
    # 是瞬时值可直接计入。
    n1.set("fe", 1000, conn=5)
    us = by_env(await col.tick(1.0))
    assert list(us) == ["env1"]
    u = us["env1"]
    assert u.rate_bps == 0.0
    assert u.mean10_bps == 0.0
    assert u.conn_cur == 5
    assert u.degraded is False

    # tick2：差分出 1000 B/s。若 tick1 的 0 被误喂进窗口，这里 mean10 会是
    # 500——断言 1000 即证明 baseline-only 被正确跳过。
    n1.set("fe", 2000, conn=6)
    u = by_env(await col.tick(2.0))["env1"]
    assert u.rate_bps == 1000.0
    assert u.mean10_bps == pytest.approx(1000.0)
    assert u.ewma60_bps == pytest.approx(1000.0)  # EWMA 首样本 seed
    assert u.conn_cur == 6

    # tick3/4：窗口均值在真实测量值上推进。
    n1.set("fe", 3000)
    u = by_env(await col.tick(3.0))["env1"]
    assert u.mean10_bps == pytest.approx(1000.0)
    n1.set("fe", 3400)
    u = by_env(await col.tick(4.0))["env1"]
    assert u.rate_bps == 400.0
    assert u.mean10_bps == pytest.approx((1000 + 1000 + 400) / 3)


async def test_multi_node_sum():
    # 同一环境横跨两台 HAProxy：聚合 = 各节点 Target 速率/连接数之和。
    n1, n2 = FakeNode(), FakeNode()
    col = Collector({"n1": n1, "n2": n2})
    col.set_mapping({T("n1", "fe"): "env1", T("n2", "fe"): "env1"})

    n1.set("fe", 0, conn=3)
    n2.set("fe", 0, conn=4)
    await col.tick(1.0)  # 双基线

    n1.add("fe", 600)
    n2.add("fe", 400)
    u = by_env(await col.tick(2.0))["env1"]
    assert u.rate_bps == pytest.approx(1000.0)
    assert u.conn_cur == 7


# ---------------------------------------------------------------------------
# 单节点失败隔离：fail-static、degraded 阈值与恢复
# ---------------------------------------------------------------------------

async def test_node_failure_hold_degraded_threshold_and_recovery(caplog):
    n1, n2 = FakeNode(), FakeNode()
    col = Collector({"n1": n1, "n2": n2})
    col.set_mapping({T("n1", "fe"): "env1", T("n2", "fe"): "env2"})

    # 建基线 + 各测一秒：env1=1000、env2=500。
    n1.set("fe", 0, conn=2)
    n2.set("fe", 0, conn=1)
    await col.tick(1.0)
    n1.add("fe", 1000)
    n2.add("fe", 500)
    await col.tick(2.0)

    # n1 失联：env1 沿用上一秒速率（fail-static），env2 完全不受影响（隔离）。
    n1.fail = ConnectionRefusedError("connection refused")
    with caplog.at_level(logging.DEBUG, logger="rl_limiter.collector"):
        for i in range(1, DEGRADED_FAILURE_THRESHOLD):  # 失败 1..9 次
            n2.add("fe", 500)
            us = by_env(await col.tick(2.0 + i))
            assert us["env1"].rate_bps == pytest.approx(1000.0)  # 沿用
            assert us["env1"].mean10_bps == pytest.approx(1000.0)  # 窗口在陈旧值上推进
            assert us["env1"].conn_cur == 2       # 连接数无法重采，保持不变
            assert us["env1"].degraded is False   # 未到阈值
            assert us["env2"].rate_bps == pytest.approx(500.0)  # 隔离：n2 正常测量
            assert col.degraded_nodes() == set()

        # 第 10 次连续失败：n1 越线降级，env1 打 degraded 标记，env2 不受牵连。
        n2.add("fe", 500)
        us = by_env(await col.tick(20.0))
        assert col.degraded_nodes() == {"n1"}
        assert us["env1"].degraded is True
        assert us["env1"].rate_bps == pytest.approx(1000.0)  # 仍然 fail-static
        assert us["env2"].degraded is False

    # 越线的那一次（且仅那一次）发 error 日志。
    errors = [r for r in caplog.records if r.levelno == logging.ERROR]
    assert len(errors) == 1
    assert "降级" in errors[0].getMessage()

    # 恢复：一次成功采样即清零失败计数并解除降级；失联期间基线未动，差分
    # 立即恢复连续（计数器在故障期间照常增长）。
    n1.fail = None
    n1.set("fe", 1000 + 12345, conn=2)
    n2.add("fe", 500)
    caplog.clear()
    with caplog.at_level(logging.INFO, logger="rl_limiter.collector"):
        us = by_env(await col.tick(21.0))
    assert col.degraded_nodes() == set()
    assert us["env1"].degraded is False
    assert us["env1"].rate_bps == pytest.approx(12345.0)  # 与故障前基线的连续差分
    assert any("采样恢复正常" in r.getMessage() for r in caplog.records)


async def test_failure_before_any_measurement_stays_baseline_only():
    # 刚建基线节点就失联：速率仍是"未知"，不能把 0 当测量值喂窗口。
    n1 = FakeNode()
    col = Collector({"n1": n1})
    col.set_mapping({T("n1", "fe"): "env1"})

    n1.set("fe", 5000, conn=1)
    await col.tick(1.0)  # baseline-only
    n1.fail = OSError("network down")
    u = by_env(await col.tick(2.0))["env1"]
    assert u.rate_bps == 0.0
    assert u.mean10_bps == 0.0  # 窗口仍为空——未知不等于零

    # 恢复后第一次差分照常成为首个测量值。
    n1.fail = None
    n1.set("fe", 5800, conn=1)
    u = by_env(await col.tick(3.0))["env1"]
    assert u.rate_bps == pytest.approx(800.0)
    assert u.mean10_bps == pytest.approx(800.0)  # 窗口只有这一个真实样本


# ---------------------------------------------------------------------------
# 计数回绕（HAProxy reload）
# ---------------------------------------------------------------------------

async def test_counter_wraparound_holds_rate_and_rebaselines():
    n1 = FakeNode()
    col = Collector({"n1": n1})
    col.set_mapping({T("n1", "fe"): "env1"})

    n1.set("fe", 10_000)
    await col.tick(1.0)
    n1.set("fe", 11_000)
    u = by_env(await col.tick(2.0))["env1"]
    assert u.rate_bps == pytest.approx(1000.0)

    # reload：计数回落到 300。差分为负不可用 → 沿用上一秒 1000，同时以
    # 300 重建基线。
    n1.set("fe", 300)
    u = by_env(await col.tick(3.0))["env1"]
    assert u.rate_bps == pytest.approx(1000.0)

    # 下一秒起差分基于新基线恢复正常。
    n1.set("fe", 800)
    u = by_env(await col.tick(4.0))["env1"]
    assert u.rate_bps == pytest.approx(500.0)


# ---------------------------------------------------------------------------
# 多节点下"部分 baseline、部分有测量"
# ---------------------------------------------------------------------------

async def test_partial_baseline_with_measurement_still_feeds():
    n1, n2 = FakeNode(), FakeNode()
    col = Collector({"n1": n1, "n2": n2})
    col.set_mapping({T("n1", "fe1"): "env1", T("n2", "fe2"): "env1"})

    # 只有 n1 的 frontend 先存在。
    n1.set("fe1", 0)
    await col.tick(1.0)
    n1.add("fe1", 1000)
    await col.tick(2.0)

    # n2 的 frontend 此刻才出现：n2 侧只建基线，但 n1 侧有真实测量——
    # 以测量值为准照常喂入（1000 而非跳过）。
    n1.add("fe1", 1000)
    n2.set("fe2", 99_999)
    u = by_env(await col.tick(3.0))["env1"]
    assert u.rate_bps == pytest.approx(1000.0)
    assert u.mean10_bps == pytest.approx(1000.0)  # 三个 tick 里喂进去的都是 1000

    # 次秒起两边都参与求和。
    n1.add("fe1", 1000)
    n2.add("fe2", 500)
    u = by_env(await col.tick(4.0))["env1"]
    assert u.rate_bps == pytest.approx(1500.0)


# ---------------------------------------------------------------------------
# mapping 热替换
# ---------------------------------------------------------------------------

async def test_mapping_hot_swap_drops_old_env_and_keeps_baseline_continuity():
    n1 = FakeNode()
    col = Collector({"n1": n1})
    col.set_mapping({T("n1", "fe"): "env1"})

    # fe2 一直存在但未映射：只会有一次 debug，且基线持续刷新。
    n1.set("fe", 0)
    n1.set("fe2", 0)
    await col.tick(1.0)
    n1.add("fe", 1000)
    n1.add("fe2", 250)
    await col.tick(2.0)

    # 热替换：fe 改属 env2，fe2 新映射进 env3；env1 从 mapping 消失。
    col.set_mapping({T("n1", "fe"): "env2", T("n1", "fe2"): "env3"})
    n1.add("fe", 700)
    n1.add("fe2", 250)
    us = by_env(await col.tick(3.0))
    assert sorted(us) == ["env2", "env3"]  # env1 不再输出，状态被丢弃

    # fe 曾被映射过（已知 Target），基线独立于 mapping 存在：换映射后的
    # 第一个 tick 速率就连续正确，不必重走"首采样建基线"。
    assert us["env2"].rate_bps == pytest.approx(700.0)
    assert us["env2"].mean10_bps == pytest.approx(700.0)  # 全新窗口，只有本样本
    # fe2 此前"未映射且从未建过基线"（采集器为省状态不给无映射 frontend
    # 建基线）：新映射进来的第一个 tick 是 baseline-only，速率未知为 0。
    assert us["env3"].rate_bps == 0.0
    assert us["env3"].mean10_bps == 0.0  # baseline-only 不喂窗口

    # 次 tick 起 env3 差分正常。
    n1.add("fe", 700)
    n1.add("fe2", 250)
    us = by_env(await col.tick(3.5))
    assert us["env3"].rate_bps == pytest.approx(250.0)
    assert us["env3"].mean10_bps == pytest.approx(250.0)

    # env1 若将来回归，窗口从零起步（无过期历史）：重新映射回来验证。
    col.set_mapping({T("n1", "fe"): "env1"})
    n1.add("fe", 300)
    u = by_env(await col.tick(4.0))["env1"]
    assert u.rate_bps == pytest.approx(300.0)
    assert u.mean10_bps == pytest.approx(300.0)  # 不是旧 env1 窗口的延续


async def test_unmapped_frontend_logged_once(caplog):
    n1 = FakeNode()
    col = Collector({"n1": n1})
    col.set_mapping({})
    n1.set("fe_x", 100)
    with caplog.at_level(logging.DEBUG, logger="rl_limiter.collector"):
        await col.tick(1.0)
        n1.add("fe_x", 100)
        await col.tick(2.0)
        await col.tick(3.0)
    hits = [r for r in caplog.records if "未映射到任何环境的 frontend" in r.getMessage()]
    assert len(hits) == 1  # 只在首次出现时记一条，防刷屏


# ---------------------------------------------------------------------------
# 同节点多 frontend 求和
# ---------------------------------------------------------------------------

async def test_multi_frontend_same_node_sum():
    n1 = FakeNode()
    col = Collector({"n1": n1})
    t1, t2 = T("n1", "fe_a"), T("n1", "fe_b")
    col.set_mapping({t1: "env1", t2: "env1"})

    n1.set("fe_a", 0)
    n1.set("fe_b", 0)
    await col.tick(1.0)

    for i in range(5):
        n1.add("fe_a", 300)
        n1.add("fe_b", 700)
        u = by_env(await col.tick(2.0 + i))["env1"]
    assert u.rate_bps == pytest.approx(1000.0)


# ---------------------------------------------------------------------------
# 消失 Target 的基线生命周期
# ---------------------------------------------------------------------------

async def test_absent_target_within_limit_keeps_diff_continuity():
    n1 = FakeNode()
    col = Collector({"n1": n1})
    col.set_mapping({T("n1", "fe"): "env1"})

    n1.set("fe", 0)
    await col.tick(1.0)
    n1.add("fe", 1000)
    await col.tick(2.0)

    # 短暂消失 5 个 tick（reload 抖动）：期间 env 是"真实零"（没有 frontend
    # 就没有流量），零进入窗口。
    n1.remove("fe")
    for i in range(5):
        u = by_env(await col.tick(3.0 + i))["env1"]
        assert u.rate_bps == 0.0

    # 限期内回归：基线仍在，差分连续（一次性补上消失期间的累计增量）。
    n1.set("fe", 1000 + 6000, 0)
    u = by_env(await col.tick(9.0))["env1"]
    assert u.rate_bps == pytest.approx(6000.0)


async def test_absent_target_evicted_after_limit(caplog):
    n1 = FakeNode()
    col = Collector({"n1": n1})
    target = T("n1", "fe")
    col.set_mapping({target: "env1"})

    n1.set("fe", 0)
    await col.tick(1.0)
    n1.add("fe", 1000)
    await col.tick(2.0)

    # 消失满 ABSENT_TICK_LIMIT 个成功 tick：基线被淘汰，防止已下线的
    # frontend 造成状态泄漏。
    n1.remove("fe")
    with caplog.at_level(logging.INFO, logger="rl_limiter.collector"):
        for i in range(ABSENT_TICK_LIMIT):
            await col.tick(3.0 + i)
    assert any("淘汰其计数差分基线" in r.getMessage() for r in caplog.records)

    # 同名 frontend 再出现：按首次采样重新建基线（baseline-only），巨大的
    # 新累计值不会被差分成一次天文数字的速率尖峰。
    n1.set("fe", 10_000_000)
    u = by_env(await col.tick(100.0))["env1"]
    assert u.rate_bps == 0.0
    n1.add("fe", 800)
    u = by_env(await col.tick(101.0))["env1"]
    assert u.rate_bps == pytest.approx(800.0)


async def test_node_failure_does_not_count_absence():
    # 节点失联的 tick 不累加缺席计数：Target 只是采不到，不是没了。
    n1 = FakeNode()
    col = Collector({"n1": n1})
    col.set_mapping({T("n1", "fe"): "env1"})

    n1.set("fe", 0)
    await col.tick(1.0)
    n1.add("fe", 1000)
    await col.tick(2.0)

    n1.fail = OSError("down")
    for i in range(ABSENT_TICK_LIMIT + 5):  # 远超缺席上限的失败 tick
        await col.tick(3.0 + i)
    n1.fail = None
    n1.add("fe", 500)
    u = by_env(await col.tick(200.0))["env1"]
    assert u.rate_bps == pytest.approx(500.0)  # 基线未被淘汰，差分连续


# ---------------------------------------------------------------------------
# 稳定 env 集合 / 未知节点 / 输出顺序
# ---------------------------------------------------------------------------

async def test_env_without_live_targets_still_output_as_real_zero():
    n1 = FakeNode()
    col = Collector({"n1": n1})
    # env_ghost 的 Target 在 stats 中从未出现：env 仍每 tick 输出，且零值
    # 进入窗口（没有 frontend 就没有流量，零是真实的）。
    col.set_mapping({T("n1", "fe"): "env1", T("n1", "fe_missing"): "env_ghost"})
    n1.set("fe", 0)
    await col.tick(1.0)
    n1.add("fe", 100)
    us = by_env(await col.tick(2.0))
    assert sorted(us) == ["env1", "env_ghost"]
    assert us["env_ghost"].rate_bps == 0.0
    assert us["env_ghost"].mean10_bps == 0.0
    assert us["env_ghost"].degraded is False


async def test_target_on_unknown_node_warned_once_and_ignored(caplog):
    n1 = FakeNode()
    col = Collector({"n1": n1})
    # "ghost" 节点未配置在 clients：warn 一次并忽略，其 env 仍稳定输出。
    col.set_mapping({T("ghost", "fe"): "env_g", T("n1", "fe"): "env1"})
    n1.set("fe", 0)
    with caplog.at_level(logging.WARNING, logger="rl_limiter.collector"):
        await col.tick(1.0)
        n1.add("fe", 100)
        us = by_env(await col.tick(2.0))
    assert sorted(us) == ["env1", "env_g"]
    assert us["env_g"].rate_bps == 0.0
    warns = [r for r in caplog.records if "未在本地配置的 HAProxy 节点" in r.getMessage()]
    assert len(warns) == 1


async def test_output_sorted_by_env_id():
    n1 = FakeNode()
    col = Collector({"n1": n1})
    col.set_mapping({
        T("n1", "fe_b"): "bbb",
        T("n1", "fe_a"): "aaa",
        T("n1", "fe_c"): "ccc",
    })
    for fe in ("fe_a", "fe_b", "fe_c"):
        n1.set(fe, 0)
    us = await col.tick(1.0)
    assert [u.env_id for u in us] == ["aaa", "bbb", "ccc"]
