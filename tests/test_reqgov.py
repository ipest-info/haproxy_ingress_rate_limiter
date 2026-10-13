# tests.test_reqgov —— 动态请求限速（带宽驱动的 AIMD 控制环）。
#
# 本分支唯一会写 HAProxy 运行期状态的模块，测试重点是控制行为的边界：
#   1. 超限乘性收缩、回落加性恢复、中间滞回带不动作；
#   2. 永不低于 min_conns；恢复基准来自 slim（无则 max_conns 兜底）；
#   3. 降级采样不动作；限额取消恢复基准；权限不足告警一次不炸。

from __future__ import annotations

import asyncio
import logging

import pytest

from rl_limiter import model, reqgov
from rl_limiter.haproxy import CommandError

log = logging.getLogger("t.reqgov")


def fe(name="fe_a", port=8080, quota=8.0):     # 8 Mbps = 1_000_000 B/s
    return model.FrontendConfig(name=name, bind_port=port, quota_mbps=quota)


def usage(name="fe_a", mean10=0.0, degraded=False):
    return model.FrontendUsage(name=name, mean10_bps=mean10,
                               degraded=degraded)


class FakeClient:
    """假 runtime API：记录命令；show stat 返回预置的 slim。"""

    def __init__(self, slims=None, reply=""):
        self.cmds: list[str] = []
        self.slims = slims if slims is not None else {"fe_a": 1000}
        self.reply = reply
        self.fail_with: str | None = None    # 设为回包文本 → 抛 CommandError

    async def exec_cmd(self, cmd: str) -> str:
        if self.fail_with is not None:
            raise CommandError(cmd, self.fail_with)
        self.cmds.append(cmd)
        return self.reply

    async def show_stat(self):
        return [model.FrontendStat(name=n, bytes_out=0, conn_cur=0,
                                   sess_limit=s)
                for n, s in self.slims.items()]


def governor(frontends, client=None, **cfgkw):
    c = client if client is not None else FakeClient()
    cfg = reqgov.ReqGovConfig(enabled=True, **cfgkw)
    g = reqgov.RequestGovernor(c, cfg, lambda: frontends, log)
    return g, c


# ---------------------------------------------------------------------------
# 收缩 / 恢复 / 滞回
# ---------------------------------------------------------------------------

async def test_over_quota_shrinks_multiplicatively():
    g, c = governor([fe()])
    g.record(1.0, [usage(mean10=2_000_000)])      # 限额 1M B/s，超一倍
    await g.adjust_once()
    assert c.cmds == ["set maxconn frontend fe_a 700"]   # 1000 × 0.7
    g.record(2.0, [usage(mean10=2_000_000)])
    await g.adjust_once()
    assert c.cmds[-1] == "set maxconn frontend fe_a 489"  # 700 × 0.7 续收


async def test_never_below_min_conns():
    g, c = governor([fe()], min_conns=400)
    g.record(1.0, [usage(mean10=9_000_000)])
    for i in range(6):
        await g.adjust_once()
    values = [int(cmd.rsplit(" ", 1)[1]) for cmd in c.cmds]
    assert values[-1] == 400 and min(values) == 400


async def test_hysteresis_band_no_action():
    """mean10 落在 low×quota 与 high×quota 之间：不收也不放（滞回带）。"""
    g, c = governor([fe()], high_ratio=1.0, low_ratio=0.85)
    g.record(1.0, [usage(mean10=900_000)])        # 0.9×quota，在带内
    await g.adjust_once()
    assert c.cmds == []


async def test_recovers_additively_back_to_ceiling():
    g, c = governor([fe()])
    g.record(1.0, [usage(mean10=2_000_000)])
    await g.adjust_once()                         # → 700
    g.record(2.0, [usage(mean10=100_000)])        # 远低于 low×quota
    await g.adjust_once()                         # +ceiling//20 = +50
    assert c.cmds[-1] == "set maxconn frontend fe_a 750"
    for i in range(10):                           # 持续回落 → 爬回基准
        await g.adjust_once()
    assert c.cmds[-1] == "set maxconn frontend fe_a 1000"
    n = len(c.cmds)
    await g.adjust_once()                         # 已在基准：不再发命令
    assert len(c.cmds) == n


async def test_ceiling_fallback_when_no_frontend_maxconn():
    """cfg 没给 frontend 级 maxconn（slim=0）：用配置的 max_conns 兜底。"""
    g, c = governor([fe()], client=FakeClient(slims={"fe_a": 0}),
                    max_conns=2000)
    g.record(1.0, [usage(mean10=2_000_000)])
    await g.adjust_once()
    assert c.cmds == ["set maxconn frontend fe_a 1400"]   # 2000 × 0.7


# ---------------------------------------------------------------------------
# 失效安全
# ---------------------------------------------------------------------------

async def test_degraded_sample_means_no_action():
    """采样失联时数据是陈旧的，按它调闸比不调更糟——必须纹丝不动。"""
    g, c = governor([fe()])
    g.record(1.0, [usage(mean10=9_000_000, degraded=True)])
    await g.adjust_once()
    assert c.cmds == []


async def test_quota_removed_restores_baseline_once():
    frontends = [fe()]
    g, c = governor(frontends)
    g.record(1.0, [usage(mean10=2_000_000)])
    await g.adjust_once()                         # 压到 700
    frontends[0] = fe(quota=0.0)                  # 限额清零（不再受管）
    g.record(2.0, [usage(mean10=2_000_000)])
    await g.adjust_once()
    assert c.cmds[-1] == "set maxconn frontend fe_a 1000"
    n = len(c.cmds)
    await g.adjust_once()                         # 只恢复一次
    assert len(c.cmds) == n


async def test_permission_denied_warns_once_and_keeps_state(caplog):
    g, c = governor([fe()])
    c.fail_with = "Permission denied\n"
    with caplog.at_level(logging.ERROR, logger="t.reqgov"):
        g.record(1.0, [usage(mean10=2_000_000)])
        await g.adjust_once()
        await g.adjust_once()
    hits = [r for r in caplog.records if "level admin" in r.getMessage()]
    assert len(hits) == 1                         # 告警一次，不刷屏
    # 命令全失败：内部状态不该前进（恢复权限后从基准重新收，而不是
    # 带着一个从未生效过的假想值继续算）。
    c.fail_with = None
    g.record(2.0, [usage(mean10=2_000_000)])
    await g.adjust_once()
    assert c.cmds == ["set maxconn frontend fe_a 700"]


async def test_startup_restores_baseline_then_loops():
    """启动清场：上个进程可能把 maxconn 压着就崩了，run() 先把受管
    frontend 拉回基准再开始调。"""
    g, c = governor([fe()], adjust_interval_s=30.0)
    task = asyncio.create_task(g.run())
    await asyncio.sleep(0.05)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert c.cmds == ["set maxconn frontend fe_a 1000"]


async def test_ceiling_not_overwritten_by_our_own_compressed_value():
    """**真机实测教训**：slim 反映的是运行期 maxconn——我们压到 700 后
    再刷新基准，读到的 slim 就是 700。基准只记首次值，否则恢复目标会
    越压越矮、永远回不到 cfg 配置的 1000。"""
    g, c = governor([fe()])
    g.record(1.0, [usage(mean10=2_000_000)])
    await g.adjust_once()                         # 压到 700，slim 跟着变
    c.slims = {"fe_a": 700}                       # 真机上 slim 现在是 700
    g._ceilings.clear()                           # 强制下一轮重新取基准…
    await g._refresh_ceilings()                   # …但 fe_a 在 _cur 里
    g.record(2.0, [usage(mean10=100_000)])        # 回落，开始恢复
    for i in range(10):
        await g.adjust_once()
    # 恢复终点必须是 max_conns 兜底/原基准，而不是被 700 卡住。
    assert int(c.cmds[-1].rsplit(" ", 1)[1]) > 700
