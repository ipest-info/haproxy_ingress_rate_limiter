# tests.test_agg —— hap-agg 的采样差分与多目标合并。
#
# 重点钉住三件事：
#   1. 差分口径：速率来自相邻两拍的计数器差分，reload 清零（负差分）
#      按 0 处理而不是给出负速率；
#   2. 失败隔离：单目标失败不影响其它目标；连续失败标记 degraded 且
#      **不计入合计**（陈旧值混进总数会让总带宽虚高）；
#   3. 合并口径：total 是健康目标之和，frontends 按名字跨机合并。

from __future__ import annotations

import pytest

from hap_agg import model, sampler as agg

import logging

log = logging.getLogger("t.agg")


def fstat(name="fe_main", bout=0, bin_=0, scur=0, conn_tot=0,
          dcon=0, mode="tcp"):
    return model.FrontendStat(name=name, bytes_out=bout, bytes_in=bin_,
                              conn_cur=scur, conn_tot=conn_tot,
                              denied_conn=dcon, mode=mode)


def istat(curr=0, maxc=1000, idle=90, sess=0, uptime=100):
    return model.InstanceStat(curr_conns=curr, max_conn=maxc, idle_pct=idle,
                              sess_rate=sess, uptime_s=uptime)


class FakeClient:
    """可编程的假 RuntimeClient：show_stat/show_info 返回预置值。"""

    def __init__(self, fes=None, info=None):
        self.fes = fes if fes is not None else []
        self.info = info if info is not None else istat()
        self.fail = False

    async def show_stat(self):
        if self.fail:
            raise ConnectionRefusedError("connection refused")
        return list(self.fes)

    async def show_info(self):
        if self.fail:
            raise ConnectionRefusedError("connection refused")
        return self.info


def sampler(client, name="t1"):
    t = agg.Target(name=name, host="10.0.0.1", port=9999)
    return agg.TargetSampler(t, log, client=client)


# ---------------------------------------------------------------------------
# 目标定义解析
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("line,expect", [
    ("10.0.0.11:9999", ("10.0.0.11:9999", "10.0.0.11", 9999)),
    ("sg-01 10.0.0.12:9999", ("sg-01", "10.0.0.12", 9999)),
    ("[::1]:9999", ("[::1]:9999", "::1", 9999)),
])
def test_parse_target_line(line, expect):
    assert agg.parse_target_line(line) == expect


@pytest.mark.parametrize("line,match", [
    ("10.0.0.11", "缺少端口"),
    ("10.0.0.11:abc", "不是数字"),
    ("10.0.0.11:99999", "越界"),
    ("a b c", "格式不对"),
])
def test_parse_target_line_rejects(line, match):
    with pytest.raises(agg.TargetError, match=match):
        agg.parse_target_line(line)


def test_parse_targets_text_batch():
    """批量导入文本：空行/注释跳过，名字与地址都查重。"""
    ts = agg.parse_targets_text(
        "10.0.0.11:9999\n"
        "\n"
        "# 注释行\n"
        "sg-02 10.0.0.12:9999   # 行尾注释\n")
    assert [(t.name, t.addr) for t in ts] == [
        ("10.0.0.11:9999", "10.0.0.11:9999"),
        ("sg-02", "10.0.0.12:9999")]
    with pytest.raises(agg.TargetError, match="地址重复"):
        agg.parse_targets_text("a 10.0.0.11:9999\nb 10.0.0.11:9999\n")
    with pytest.raises(agg.TargetError, match="名重复"):
        agg.parse_targets_text("a 10.0.0.11:9999\na 10.0.0.12:9999\n")


# ---------------------------------------------------------------------------
# 单目标采样差分
# ---------------------------------------------------------------------------

async def test_rates_from_counter_diff():
    c = FakeClient([fstat(bout=1000, bin_=100, scur=5, conn_tot=10)],
                   istat(curr=7))
    s = sampler(c)
    v1 = await s.sample(100.0)
    assert v1["ok"] and v1["rate_out_bytes_per_s"] == 0.0   # 首拍无基线

    c.fes = [fstat(bout=3000, bin_=200, scur=6, conn_tot=14)]
    v2 = await s.sample(101.0)
    fe = v2["frontends"]["fe_main"]
    assert fe["rate_out_bytes_per_s"] == 2000.0
    assert fe["rate_in_bytes_per_s"] == 100.0
    assert fe["conn_new_ps"] == 4.0
    assert fe["conn"] == 6
    assert v2["rate_out_bytes_per_s"] == 2000.0      # 目标级 = Σ frontend
    assert v2["conn"] == 7                           # show info 的整机并发


async def test_reload_counter_reset_gives_zero_not_negative():
    """HAProxy reload 后计数器清零：负差分按 0 处理，下一拍恢复。"""
    c = FakeClient([fstat(bout=100000)])
    s = sampler(c)
    await s.sample(1.0)
    c.fes = [fstat(bout=500)]                        # 清零后从头计
    v = await s.sample(2.0)
    assert v["frontends"]["fe_main"]["rate_out_bytes_per_s"] == 0.0
    c.fes = [fstat(bout=1500)]
    v = await s.sample(3.0)
    assert v["frontends"]["fe_main"]["rate_out_bytes_per_s"] == 1000.0


async def test_failure_marks_degraded_after_threshold_and_recovers():
    c = FakeClient([fstat(bout=1000, scur=3)])
    s = sampler(c)
    await s.sample(1.0)
    c.fail = True
    for i in range(agg.DEGRADED_AFTER_FAILURES):
        v = await s.sample(2.0 + i)
        assert not v["ok"]
    assert v["degraded"] is True
    assert "refused" in v["error"]
    # 失败拍保留最近一次成功的静态字段（conn 等），供页面展示陈旧值。
    assert v["frontends"]["fe_main"]["conn"] == 3
    c.fail = False
    v = await s.sample(10.0)
    assert v["ok"] and not v["degraded"]


# ---------------------------------------------------------------------------
# 多目标合并
# ---------------------------------------------------------------------------

def two_target_agg():
    c1 = FakeClient([fstat("fe_main", bout=0, scur=2, conn_tot=0)],
                    istat(curr=5, maxc=100))
    c2 = FakeClient([fstat("fe_main", bout=0, scur=3, conn_tot=0),
                     fstat("fe_extra", bout=0, scur=1, conn_tot=0)],
                    istat(curr=9, maxc=200))
    clients = {"t1": c1, "t2": c2}
    # 运行期新增的目标（add_targets）拿一个默认 FakeClient。
    a = agg.Aggregator(
        [agg.Target(name="t1", host="h1", port=9999),
         agg.Target(name="t2", host="h2", port=9999)],
        log, client_factory=lambda t: clients.setdefault(t.name, FakeClient()))
    return a, c1, c2


async def test_merge_totals_and_frontends():
    a, c1, c2 = two_target_agg()
    await a.tick(1.0)
    c1.fes = [fstat("fe_main", bout=1000, scur=2, conn_tot=2)]
    c2.fes = [fstat("fe_main", bout=3000, scur=3, conn_tot=1),
              fstat("fe_extra", bout=500, scur=1, conn_tot=0)]
    snap = await a.tick(2.0)

    t = snap["total"]
    assert t["targets"] == 2 and t["targets_ok"] == 2
    assert t["conn"] == 14                      # 5 + 9（show info 口径）
    assert t["fe_conn"] == 6                    # 2 + 3 + 1
    assert t["max_conn"] == 300
    assert t["rate_out_bytes_per_s"] == 4500.0  # 1000 + 3000 + 500
    assert t["conn_new_ps"] == 3.0

    # 同名 frontend 跨机合并；只在一台上的段单独成行。
    fe = snap["frontends"]["fe_main"]
    assert fe["rate_out_bytes_per_s"] == 4000.0
    assert fe["conn"] == 5 and fe["targets"] == 2
    assert snap["frontends"]["fe_extra"]["targets"] == 1


async def test_failed_target_excluded_from_totals():
    """失败目标不计入合计（陈旧速率混进总数会让总带宽虚高），但仍出现
    在 targets 里带着错误原因——页面要能看到它坏了。"""
    a, c1, c2 = two_target_agg()
    await a.tick(1.0)
    c1.fes = [fstat("fe_main", bout=1000)]
    c2.fail = True
    snap = await a.tick(2.0)
    assert snap["total"]["targets_ok"] == 1
    assert snap["total"]["rate_out_bytes_per_s"] == 1000.0
    assert snap["total"]["conn"] == 5           # 只有 t1 的
    assert snap["targets"]["t2"]["ok"] is False
    assert snap["frontends"]["fe_main"]["targets"] == 1


# ---------------------------------------------------------------------------
# 目标集管理
# ---------------------------------------------------------------------------

async def test_add_targets_idempotent_and_conflict():
    a, _c1, _c2 = two_target_agg()
    # 同名同址：幂等跳过；全新目标：加入。
    added = a.add_targets(agg.parse_targets_text(
        "t1 h1:9999\nnew1 10.0.0.99:9999\n"))
    assert added == ["new1"]
    assert {t.name for t in a.targets()} == {"t1", "t2", "new1"}
    # 同名异址：冲突，明确拒绝。
    with pytest.raises(agg.TargetError, match="地址不同"):
        a.add_targets(agg.parse_targets_text("t1 10.9.9.9:1234\n"))
    # 同址异名：视为已导入过，跳过。
    assert a.add_targets(agg.parse_targets_text("alias h1:9999\n")) == []


async def test_remove_target():
    a, _c1, _c2 = two_target_agg()
    assert a.remove_target("t1") is True
    assert a.remove_target("t1") is False
    snap = await a.tick(1.0)
    assert set(snap["targets"]) == {"t2"}
