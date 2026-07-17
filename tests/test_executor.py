# tests/test_executor.py —— 执行器的单元测试。
# 覆盖 v2.0 多节点结构（clients/map_paths 按节点、分配值按 Target）下的
# dry-run/enforce 模式切换、按 Target 写回去重、节点失败与重同步等场景。

from __future__ import annotations

import logging

import pytest

from rl_limiter import model
from rl_limiter.executor import Executor

LOGGER = "test.executor"

TA1 = model.Target("n1", "fe_a1")
TA2 = model.Target("n1", "fe_a2")
TA3 = model.Target("n2", "fe_a3")
TB1 = model.Target("n2", "fe_b1")
TB2 = model.Target("n2", "fe_b2")
TC1 = model.Target("n1", "fe_c1")


class FakeClient:
    """记录 set_map_entry 调用并可按 key 注入错误的假 runtime client。"""

    def __init__(self, errs: dict[str, Exception] | None = None):
        self.calls: list[tuple[str, str, str]] = []  # (map_path, key, value)
        self.errs = errs or {}

    async def set_map_entry(self, map_path: str, key: str, value: str) -> None:
        self.calls.append((map_path, key, value))
        err = self.errs.get(key)
        if err is not None:
            raise err

    def values_for(self, key: str) -> list[str]:
        return [v for (_, k, v) in self.calls if k == key]


def decision(env, targets, bwlim, changed) -> model.Decision:
    return model.Decision(
        env_id=env,
        targets=list(targets),
        bwlim_bps=bwlim,
        state=model.GovState.NORMAL,
        changed=changed,
    )


def even_alloc(targets, bwlim) -> dict[model.Target, int]:
    """测试用简易分配：均分向下取整（真实路径由 allocator 产出）。"""
    per = int(bwlim // len(targets)) if targets else 0
    return {t: per for t in targets}


def new_executor(mode, clients=None, map_paths=None):
    clients = clients if clients is not None else {"n1": FakeClient(), "n2": FakeClient()}
    map_paths = map_paths if map_paths is not None else {
        "n1": "/etc/haproxy/maps/n1-bwlim.map",
        "n2": "/etc/haproxy/maps/n2-bwlim.map",
    }
    return Executor(clients, map_paths, mode, log=logging.getLogger(LOGGER)), clients


def total_calls(clients) -> int:
    return sum(len(c.calls) for c in clients.values())


async def test_dry_run_logs_without_io(caplog):
    """dry-run：零 I/O，changed 决策记 DRY-RUN 日志并进 snapshot，
    unchanged 决策既不记日志也不进 snapshot。"""
    s, clients = new_executor(model.MODE_DRY_RUN)
    items = [
        (decision("env-a", [TA1, TA2], 12_500_000.0, True), even_alloc([TA1, TA2], 12_500_000)),
        (decision("env-b", [TB1], 6_250_000.0, False), even_alloc([TB1], 6_250_000)),
    ]
    with caplog.at_level(logging.INFO, logger=LOGGER):
        errs = await s.apply(items)
    assert errs == []
    assert total_calls(clients) == 0, "dry-run performed I/O"

    msgs = [r.getMessage() for r in caplog.records]
    assert any("【DRY-RUN 演练】本应下发整形值" in m and "env-a" in m for m in msgs)
    assert not any("env-b" in m for m in msgs), "unchanged decision was logged"

    snap = s.snapshot()
    assert snap.get("env-a") == 12_500_000.0
    assert "env-b" not in snap


async def test_enforce_writes_allocations_to_right_node_and_map():
    """enforce：每个 Target 用其所在节点的 client 写该节点的 map 路径，
    值为分配好的整数（十进制字符串下发）。"""
    s, clients = new_executor(model.MODE_ENFORCE)
    alloc = {TA1: 524_288, TA2: 300_000, TA3: 200_000}
    items = [(decision("env-a", [TA1, TA2, TA3], 1_048_576.9, True), alloc)]

    errs = await s.apply(items)
    assert errs == []
    # n1 拿到 fe_a1/fe_a2，n2 拿到 fe_a3，map 路径各归各节点。
    assert sorted(clients["n1"].calls) == sorted([
        ("/etc/haproxy/maps/n1-bwlim.map", "fe_a1", "524288"),
        ("/etc/haproxy/maps/n1-bwlim.map", "fe_a2", "300000"),
    ])
    assert clients["n2"].calls == [("/etc/haproxy/maps/n2-bwlim.map", "fe_a3", "200000")]
    # snapshot 记录环境聚合值（不是分配后的单点值）。
    assert s.snapshot()["env-a"] == 1_048_576.9


async def test_unchanged_decisions_skipped():
    s, clients = new_executor(model.MODE_ENFORCE)
    items = [(decision("env-a", [TA1], 100.0, False), {TA1: 100})]
    assert await s.apply(items) == []
    assert total_calls(clients) == 0
    assert s.snapshot() == {}


async def test_failed_write_retried_on_unchanged_decision(caplog):
    """写失败的 env 进 pending：即使 governor 之后只发 changed=False，
    executor 也必须自行重试直到写成功；成功后恢复正常跳过。"""
    boom = RuntimeError("socket gone")
    c1 = FakeClient(errs={"fe_a1": boom})
    s, clients = new_executor(model.MODE_ENFORCE, clients={"n1": c1, "n2": FakeClient()})

    changed = [(decision("env-a", [TA1], 1000.0, True), {TA1: 1000})]
    unchanged = [(decision("env-a", [TA1], 1000.0, False), {TA1: 1000})]

    errs = await s.apply(changed)
    assert len(errs) == 1 and errs[0].__cause__ is boom
    assert "env-a" not in s.snapshot(), "failed env recorded in snapshot"

    # runtime API 恢复；governor 此后只发 changed=False（它的 last_emitted
    # 已自行推进），executor 必须重试。
    c1.errs.clear()
    with caplog.at_level(logging.INFO, logger=LOGGER):
        errs = await s.apply(unchanged)
    assert errs == []
    assert len(c1.calls) == 2, "want initial failure + one retry"
    assert s.snapshot()["env-a"] == 1000.0
    assert any("重试队列中的环境整形值写入成功" in r.getMessage() for r in caplog.records)

    # 收敛后 unchanged 决策重新变成 no-op。
    assert await s.apply(unchanged) == []
    assert len(c1.calls) == 2


async def test_partial_failure_keeps_writing_and_collects_errors():
    """单点失败不阻断：同 env 其余 Target 与其余决策照常写；
    任一失败的 env 不进 snapshot、整 env 进 pending。"""
    err_a = RuntimeError("boom-a")
    err_b = RuntimeError("boom-b")
    c1 = FakeClient(errs={"fe_a2": err_a})
    c2 = FakeClient(errs={"fe_b1": err_b})
    s, clients = new_executor(model.MODE_ENFORCE, clients={"n1": c1, "n2": c2})

    items = [
        (decision("env-a", [TA1, TA2, TA3], 1000.0, True), even_alloc([TA1, TA2, TA3], 999)),
        (decision("env-b", [TB1, TB2], 2000.0, True), even_alloc([TB1, TB2], 2000)),
        (decision("env-c", [TC1], 3000.0, True), {TC1: 3000}),
    ]
    errs = await s.apply(items)
    assert len(errs) == 2
    assert {e.__cause__ for e in errs} == {err_a, err_b}

    # 每个 Target 都必须被尝试过恰好一次，包括同决策内失败之后的与后续决策的。
    for c, key in [(c1, "fe_a1"), (c1, "fe_a2"), (c2, "fe_a3"),
                   (c2, "fe_b1"), (c2, "fe_b2"), (c1, "fe_c1")]:
        assert len(c.values_for(key)) == 1, f"target {key} not written exactly once"

    snap = s.snapshot()
    assert "env-a" not in snap and "env-b" not in snap
    assert snap["env-c"] == 3000.0


async def test_pending_env_fully_rewritten_on_retry():
    """pending 重试重写该 env 的全部 Target（不只失败的那个）。"""
    boom = RuntimeError("x")
    c1 = FakeClient(errs={"fe_a2": boom})
    s, clients = new_executor(model.MODE_ENFORCE, clients={"n1": c1, "n2": FakeClient()})

    alloc = even_alloc([TA1, TA2], 1000)
    await s.apply([(decision("env-a", [TA1, TA2], 1000.0, True), alloc)])
    c1.errs.clear()
    await s.apply([(decision("env-a", [TA1, TA2], 1000.0, False), alloc)])
    # fe_a1 也被重写了一次（重试是整 env 粒度）。
    assert len(c1.values_for("fe_a1")) == 2
    assert len(c1.values_for("fe_a2")) == 2


async def test_unknown_node_counts_as_target_failure(caplog):
    """Target 引用的节点不在 clients/map_paths 中：error 日志 + 记为该
    Target 失败，env 进 pending。"""
    s, clients = new_executor(model.MODE_ENFORCE)
    ghost = model.Target("ghost", "fe_g")
    items = [(decision("env-g", [ghost], 500.0, True), {ghost: 500})]
    with caplog.at_level(logging.ERROR, logger=LOGGER):
        errs = await s.apply(items)
    assert len(errs) == 1
    assert any("该挂载点写入失败" in r.getMessage() for r in caplog.records)
    assert "env-g" not in s.snapshot()
    # 处于 pending：changed=False 也会再次尝试（仍失败，因为节点仍缺失）。
    errs = await s.apply([(decision("env-g", [ghost], 500.0, False), {ghost: 500})])
    assert len(errs) == 1


async def test_invalid_mode_falls_back_to_dry_run(caplog):
    with caplog.at_level(logging.WARNING, logger=LOGGER):
        s, _ = new_executor("bogus")
    assert s.mode() == model.MODE_DRY_RUN
    assert any("运行模式配置非法，已安全降级为 dry-run" in r.getMessage()
               for r in caplog.records)

    s.set_mode(model.MODE_ENFORCE)
    assert s.mode() == model.MODE_ENFORCE
    s.set_mode("garbage")
    assert s.mode() == model.MODE_DRY_RUN


async def test_switch_to_enforce_resyncs_once():
    """切 enforce 后第一个非空 apply 全量重写（含 unchanged），且只此一次。"""
    s, clients = new_executor(model.MODE_DRY_RUN)
    alloc = even_alloc([TA1, TA3], 500)
    changed = [(decision("env-a", [TA1, TA3], 500.0, True), alloc)]
    unchanged = [(decision("env-a", [TA1, TA3], 500.0, False), alloc)]

    assert await s.apply(changed) == []
    assert total_calls(clients) == 0, "dry-run made I/O"

    s.set_mode(model.MODE_ENFORCE)

    # 切换后的第一拍：unchanged 也必须重写。
    assert await s.apply(unchanged) == []
    assert total_calls(clients) == 2

    # resync 一次性：下一拍 unchanged 又是 no-op。
    assert await s.apply(unchanged) == []
    assert total_calls(clients) == 2


async def test_resync_not_armed_without_mode_change():
    s, clients = new_executor(model.MODE_ENFORCE)
    s.set_mode(model.MODE_ENFORCE)  # 同模式：no-op，不得武装 resync
    items = [(decision("env-a", [TA1], 500.0, False), {TA1: 500})]
    assert await s.apply(items) == []
    assert total_calls(clients) == 0


async def test_resync_survives_empty_apply():
    """空拍不消费 resync 标志：标志留到真正有决策的那一拍。"""
    s, clients = new_executor(model.MODE_DRY_RUN)
    s.set_mode(model.MODE_ENFORCE)

    assert await s.apply([]) == []
    items = [(decision("env-a", [TA1], 500.0, False), {TA1: 500})]
    assert await s.apply(items) == []
    assert total_calls(clients) == 1, "resync consumed by empty apply"


async def test_switch_back_to_dry_run_drops_pending_resync():
    """切回 dry-run：未消费的 resync 与 pending 都被丢弃。"""
    s, clients = new_executor(model.MODE_DRY_RUN)
    s.set_mode(model.MODE_ENFORCE)
    s.set_mode(model.MODE_DRY_RUN)  # 未消费的 resync 必须作废

    items = [(decision("env-a", [TA1], 500.0, False), {TA1: 500})]
    assert await s.apply(items) == []
    assert total_calls(clients) == 0
    assert s.snapshot() == {}


async def test_switch_to_dry_run_clears_pending():
    """enforce 下失败进 pending；切 dry-run 清空 pending——unchanged
    决策在 dry-run 下不再触发任何记录。"""
    boom = RuntimeError("x")
    c1 = FakeClient(errs={"fe_a1": boom})
    s, clients = new_executor(model.MODE_ENFORCE, clients={"n1": c1, "n2": FakeClient()})

    errs = await s.apply([(decision("env-a", [TA1], 1000.0, True), {TA1: 1000})])
    assert len(errs) == 1

    s.set_mode(model.MODE_DRY_RUN)
    # dry-run 下 unchanged（哪怕曾 pending）不记日志不进 snapshot。
    assert await s.apply([(decision("env-a", [TA1], 1000.0, False), {TA1: 1000})]) == []
    assert s.snapshot() == {}

    # 回到 enforce：resync 全量覆盖一次即收敛，不存在残留的误重试。
    c1.errs.clear()
    s.set_mode(model.MODE_ENFORCE)
    assert await s.apply([(decision("env-a", [TA1], 1000.0, False), {TA1: 1000})]) == []
    assert s.snapshot()["env-a"] == 1000.0


async def test_enforce_env_without_targets_warns_and_skips(caplog):
    s, clients = new_executor(model.MODE_ENFORCE)
    items = [(decision("env-a", [], 500.0, True), {})]
    with caplog.at_level(logging.WARNING, logger=LOGGER):
        assert await s.apply(items) == []
    assert total_calls(clients) == 0
    assert any("决策不含任何挂载点" in r.getMessage() for r in caplog.records)
    assert s.snapshot() == {}


# ---------------------------------------------------------------------------
# 分配漂移再平衡（节点内多挂载点的份额随流量倾斜自动纠偏）
# ---------------------------------------------------------------------------


async def test_allocation_drift_rebalances_without_changed():
    """稳态下（changed=False、bwlim 不变）节点间流量倾斜导致分配结果
    漂移超过 REBALANCE_EPSILON 时，必须重写整个 env 的分配。"""
    exe, clients = new_executor(model.MODE_ENFORCE)
    targets = [TA1, model.Target("n2", "fe_a1")]
    d_first = decision("env-a", targets, 27_500_000, True)
    # 首拍：ewma 尚空，均分落地。
    alloc_even = {targets[0]: 13_750_000, targets[1]: 13_750_000}
    assert await exe.apply([(d_first, alloc_even)]) == []
    n_after_first = total_calls(clients)

    # 稳态拍：changed=False 且分配未变 → 零写入。
    d_steady = decision("env-a", targets, 27_500_000, False)
    assert await exe.apply([(d_steady, alloc_even)]) == []
    assert total_calls(clients) == n_after_first

    # 流量倾斜后 allocator 产出加权分配（漂移远超 5%）→ 强制重写。
    alloc_skewed = {targets[0]: 21_000_000, targets[1]: 6_500_000}
    assert await exe.apply([(d_steady, alloc_skewed)]) == []
    assert total_calls(clients) == n_after_first + 2
    assert clients["n1"].values_for("fe_a1")[-1] == "21000000"
    assert clients["n2"].values_for("fe_a1")[-1] == "6500000"

    # 再平衡后的新分配成为基准：相同分配不再触发写入。
    assert await exe.apply([(d_steady, alloc_skewed)]) == []
    assert total_calls(clients) == n_after_first + 2


async def test_allocation_small_drift_below_epsilon_skipped():
    """漂移低于阈值时不重写：防止每秒抖动写 map。"""
    exe, clients = new_executor(model.MODE_ENFORCE)
    targets = [TA1, model.Target("n2", "fe_a1")]
    base = {targets[0]: 10_000_000, targets[1]: 10_000_000}
    await exe.apply([(decision("env-a", targets, 20_000_000, True), base)])
    n0 = total_calls(clients)

    # 2% 漂移 < 5% 阈值 → 跳过。
    small = {targets[0]: 10_200_000, targets[1]: 9_800_000}
    await exe.apply([(decision("env-a", targets, 20_000_000, False), small)])
    assert total_calls(clients) == n0


async def test_allocation_target_set_change_forces_rewrite():
    """Target 集合变化（挂载点增删）视为无穷大漂移，立即重写。"""
    exe, clients = new_executor(model.MODE_ENFORCE)
    t_extra = model.Target("n2", "fe_a1")
    await exe.apply([(decision("env-a", [TA1], 10_000_000, True), {TA1: 10_000_000})])
    n0 = total_calls(clients)

    d2 = decision("env-a", [TA1, t_extra], 10_000_000, False)
    await exe.apply([(d2, {TA1: 5_000_000, t_extra: 5_000_000})])
    assert total_calls(clients) == n0 + 2


async def test_drift_baseline_cleared_on_dry_run_switch():
    """切到 dry-run 清空落地基准；回到 enforce 由 resync 全量重建。"""
    exe, clients = new_executor(model.MODE_ENFORCE)
    alloc = {TA1: 10_000_000}
    await exe.apply([(decision("env-a", [TA1], 10_000_000, True), alloc)])
    n0 = total_calls(clients)

    exe.set_mode(model.MODE_DRY_RUN)
    exe.set_mode(model.MODE_ENFORCE)  # 武装 resync
    await exe.apply([(decision("env-a", [TA1], 10_000_000, False), alloc)])
    assert total_calls(clients) == n0 + 1  # resync 重写一次并重建基准

    await exe.apply([(decision("env-a", [TA1], 10_000_000, False), alloc)])
    assert total_calls(clients) == n0 + 1  # 基准一致，不再写


# ---------------------------------------------------------------------------
# 按节点模式（node_modes）：全局默认 + 节点覆盖，生产灰度的核心能力。
# ---------------------------------------------------------------------------

async def test_node_modes_split_write_and_drill():
    """混合模式环境：enforce 节点真实写入，dry-run 节点只演练。
    env 横跨 n1(enforce 覆盖) 与 n2(继承全局 dry-run)。"""
    s, clients = new_executor(model.MODE_DRY_RUN)
    s.set_mode(model.MODE_DRY_RUN, {"n1": model.MODE_ENFORCE})

    d = decision("env-a", [TA1, TA3], 1000.0, changed=True)  # TA1@n1, TA3@n2
    errs = await s.apply([(d, even_alloc([TA1, TA3], 1000.0))])
    assert errs == []
    # n1 被真实写入，n2 零 I/O。
    assert clients["n1"].values_for("fe_a1") == ["500"]
    assert len(clients["n2"].calls) == 0
    # 环境整体计入 snapshot（enforce 侧全部成功）。
    assert s.snapshot() == {"env-a": 1000.0}


async def test_node_flip_to_enforce_arms_per_node_resync():
    """单个节点 dry→enforce：只重写涉及该节点的环境，其它环境不动。"""
    s, clients = new_executor(model.MODE_ENFORCE)
    # n2 先覆盖为 dry-run：env-b(纯 n2) 演练，env-a(纯 n1) 真实写入。
    s.set_mode(model.MODE_ENFORCE, {"n2": model.MODE_DRY_RUN})
    da = decision("env-a", [TA1], 1000.0, changed=True)
    db = decision("env-b", [TB1], 2000.0, changed=True)
    await s.apply([(da, even_alloc([TA1], 1000.0)),
                   (db, even_alloc([TB1], 2000.0))])
    assert len(clients["n2"].calls) == 0
    n1_calls = len(clients["n1"].calls)

    # 撤销 n2 的覆盖 → n2 生效模式 dry→enforce，武装 n2 的 resync。
    s.set_mode(model.MODE_ENFORCE, {})
    da2 = decision("env-a", [TA1], 1000.0, changed=False)
    db2 = decision("env-b", [TB1], 2000.0, changed=False)
    await s.apply([(da2, even_alloc([TA1], 1000.0)),
                   (db2, even_alloc([TB1], 2000.0))])
    # env-b 因 n2 resync 被重写；env-a 无触发条件，不重复写。
    assert clients["n2"].values_for("fe_b1") == ["2000"]
    assert len(clients["n1"].calls) == n1_calls


async def test_same_effective_modes_is_noop():
    """覆盖值与全局默认相同：生效模式没变，不武装 resync。"""
    s, clients = new_executor(model.MODE_ENFORCE)
    d = decision("env-a", [TA1], 1000.0, changed=True)
    await s.apply([(d, even_alloc([TA1], 1000.0))])
    n1_calls = len(clients["n1"].calls)

    s.set_mode(model.MODE_ENFORCE, {"n1": model.MODE_ENFORCE})  # 生效面等价
    d2 = decision("env-a", [TA1], 1000.0, changed=False)
    await s.apply([(d2, even_alloc([TA1], 1000.0))])
    assert len(clients["n1"].calls) == n1_calls, "等价模式切换不得触发重写"


async def test_all_nodes_dry_clears_pending():
    """所有节点生效模式都变为 dry-run 时清空 pending（与全局切 dry 同义）。"""
    c1 = FakeClient(errs={"fe_a1": RuntimeError("boom")})
    s, clients = new_executor(model.MODE_ENFORCE, clients={"n1": c1, "n2": FakeClient()})
    d = decision("env-a", [TA1], 1000.0, changed=True)
    errs = await s.apply([(d, even_alloc([TA1], 1000.0))])
    assert errs, "注入的写失败必须上报"

    # 逐节点全部覆盖为 dry-run（全局默认仍是 enforce）→ 等效全 dry。
    s.set_mode(model.MODE_ENFORCE,
               {"n1": model.MODE_DRY_RUN, "n2": model.MODE_DRY_RUN})
    c1.errs = {}
    d2 = decision("env-a", [TA1], 1000.0, changed=False)
    errs = await s.apply([(d2, even_alloc([TA1], 1000.0))])
    # pending 已被清空：unchanged 决策在纯演练下不触发任何写。
    assert errs == [] and c1.values_for("fe_a1") == ["1000"] * 1


async def test_node_modes_accessor_and_invalid_override():
    """node_modes() 返回覆盖副本；非法覆盖值按安全方向降级 dry-run。"""
    s, _ = new_executor(model.MODE_ENFORCE)
    s.set_mode(model.MODE_ENFORCE, {"n1": "garbage"})
    assert s.node_modes() == {"n1": model.MODE_DRY_RUN}
    assert s.mode() == model.MODE_ENFORCE
