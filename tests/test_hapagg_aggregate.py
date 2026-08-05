# tests.test_hapagg_aggregate —— 多机合并与"部分失败必须看得见"。
#
# 这个工具最危险的失效方式不是崩溃，而是：**5 台里有 2 台连不上，视图
# 照样给出一个漂亮的总量，没有任何迹象表明少了 2 台。** 那个数字偏低，
# 但看起来完全正常，可能好几天都没人发现。所以覆盖度的用例写得最细。

from __future__ import annotations

from hapagg import aggregate, collect, stats
from hapagg.targets import Target

HEAD = ("# pxname,svname,scur,smax,stot,bin,bout,status,type,rate,"
        "qtime,ctime,rtime,ttime")


def csv(*rows):
    return HEAD + "\n" + "\n".join(rows) + "\n"


def node(label, csv_text, info=None, ok=True, error=""):
    t = Target(host="10.0.0.1", port=9999, name=label)
    snap = collect.NodeSnapshot(target=t, ok=ok, error=error)
    if ok:
        snap.rows = stats.parse_stat_csv(csv_text)
        snap.info = info or {}
        snap.version = snap.info.get("Version", "")
    return snap


def coll(*nodes):
    return collect.Collection(ts=0.0, nodes=list(nodes))


def fe(px="fe", scur=10, smax=90, stot=100, bin_=1000, bout=2000,
       status="OPEN", rate=5, qtime=1, ctime=2, rtime=3, ttime=4):
    return (f"{px},FRONTEND,{scur},{smax},{stot},{bin_},{bout},{status},0,"
            f"{rate},{qtime},{ctime},{rtime},{ttime}")


def srv(px="be", sv="srv1", scur=5, stot=50, status="UP", rtime=20):
    return f"{px},{sv},{scur},9,{stot},10,20,{status},2,1,1,2,{rtime},25"


# ---------------------------------------------------------------------------
# 合并
# ---------------------------------------------------------------------------

def test_same_proxy_across_nodes_becomes_one_row():
    """3 台上的 fe_main 合成一行 —— 这正是"合并成一个视图"的字面意思。"""
    v = aggregate.aggregate(coll(
        node("a", csv(fe(scur=10, bout=1000))),
        node("b", csv(fe(scur=20, bout=2000))),
        node("c", csv(fe(scur=30, bout=3000)))))
    (f,) = v.frontends()
    assert f.values["scur"] == 60
    assert f.values["bout"] == 6000
    assert f.nodes == ["a", "b", "c"]


def test_peaks_are_maxed_not_summed():
    v = aggregate.aggregate(coll(
        node("a", csv(fe(smax=90))), node("b", csv(fe(smax=90)))))
    assert v.frontends()[0].values["smax"] == 90, "峰值相加会凭空翻倍"


def test_latency_is_weighted_by_session_count():
    """一台繁忙机器慢、两台空闲机器快，加权后必须体现出慢。"""
    v = aggregate.aggregate(coll(
        node("busy", csv(srv(stot=10000, rtime=100))),
        node("idle1", csv(srv(stot=1, rtime=1))),
        node("idle2", csv(srv(stot=1, rtime=1)))))
    rt = v.servers()[0].values["rtime"]
    assert rt == 100, f"简单平均会得到 34，把问题稀释掉；实际 {rt}"


def test_status_takes_the_worst_and_names_the_bad_node():
    """聚合说"有问题"，per-node 说"在哪台"。少了后者，排障第一步就卡住。"""
    v = aggregate.aggregate(coll(
        node("a", csv(srv(status="UP"))),
        node("b", csv(srv(status="DOWN"))),
        node("c", csv(srv(status="UP")))))
    (s,) = v.servers()
    assert s.status == "DOWN"
    assert s.bad_nodes() == ["b"]


def test_per_node_values_are_kept_for_drilldown():
    v = aggregate.aggregate(coll(
        node("a", csv(fe(bout=1000))), node("b", csv(fe(bout=9000)))))
    (f,) = v.frontends()
    assert f.per_node["a"]["bout"] == 1000
    assert f.per_node["b"]["bout"] == 9000


# ---------------------------------------------------------------------------
# 覆盖度：部分失败必须看得见
# ---------------------------------------------------------------------------

def test_partial_failure_is_visible_in_the_view():
    """**核心用例。** 2 台成功、1 台失败时，总量只含成功的那些，而视图
    必须明说少了谁、为什么。"""
    v = aggregate.aggregate(coll(
        node("a", csv(fe(bout=1000))),
        node("b", csv(fe(bout=2000))),
        node("dead", "", ok=False, error="连不上")))
    assert v.total_nodes == 3 and v.ok_nodes == 2
    assert not v.complete
    assert v.failures == [("dead", "连不上")]
    assert v.frontends()[0].values["bout"] == 3000     # 只含成功的两台
    note = v.coverage_note()
    assert "2/3" in note and "失败" in note


def test_complete_collection_says_so():
    v = aggregate.aggregate(coll(node("a", csv(fe())), node("b", csv(fe()))))
    assert v.complete
    assert "全部采到" in v.coverage_note()


def test_object_present_on_only_some_nodes_is_flagged():
    """各台的 haproxy.cfg 本该一致。某个 proxy 只在一部分机器上存在，
    往往意味着某台漏了一次发布 —— **这个信号只有聚合视图看得出来**，
    单看任何一台都正常。"""
    v = aggregate.aggregate(coll(
        node("a", csv(fe(px="fe_main"), fe(px="fe_api"))),
        node("b", csv(fe(px="fe_main"))),
        node("c", csv(fe(px="fe_main")))))
    odd = [o.pxname for o in v.inconsistent_objects()]
    assert odd == ["fe_api"]
    assert len(v.objects[("fe_main", "FRONTEND")].nodes) == 3


def test_single_node_never_reports_inconsistency():
    """只有一台时"只在 1/1 台上存在"是废话，不该报。"""
    v = aggregate.aggregate(coll(node("a", csv(fe(px="only")))))
    assert v.inconsistent_objects() == []


def test_all_nodes_failed_yields_an_empty_but_honest_view():
    """一台都没成功时不能给一屏漂亮的 0。"""
    v = aggregate.aggregate(coll(
        node("a", "", ok=False, error="超时"),
        node("b", "", ok=False, error="连不上")))
    assert v.ok_nodes == 0 and not v.complete
    assert v.objects == {}
    assert len(v.failures) == 2


# ---------------------------------------------------------------------------
# 进程级指标
# ---------------------------------------------------------------------------

def test_info_merges_by_declared_mode():
    v = aggregate.aggregate(coll(
        node("a", csv(fe()), {"CurrConns": "10", "Idle_pct": "90",
                              "Uptime_sec": "1000", "Version": "2.8.16"}),
        node("b", csv(fe()), {"CurrConns": "20", "Idle_pct": "5",
                              "Uptime_sec": "30", "Version": "2.8.16"})))
    assert v.info["CurrConns"] == 30          # 求和
    assert v.info["Idle_pct"] == 5            # 取最差那台
    assert v.info["Uptime_sec"] == 30         # 最近重启的那台


def test_node_without_info_still_contributes_its_stats():
    """show info 是次要链路：拿不到只是少几个进程级指标，不该让这台机器
    的**流量数据**一起消失。"""
    v = aggregate.aggregate(coll(
        node("a", csv(fe(bout=1000)), {"CurrConns": "10"}),
        node("b", csv(fe(bout=2000)))))          # 没有 info
    assert v.frontends()[0].values["bout"] == 3000
    assert v.info["CurrConns"] == 10


# ---------------------------------------------------------------------------
# 二次汇总
# ---------------------------------------------------------------------------

def test_totals_only_covers_sum_dimensions():
    """把各 frontend 的峰值再相加，得到的是一个从未发生过的数字；把各
    frontend 的平均延迟再平均，权重信息上一步已经丢了。**宁可少几列，
    也不给说不清口径的数。**"""
    v = aggregate.aggregate(coll(node("a", csv(fe(px="f1"), fe(px="f2")))))
    t = aggregate.totals(v, stats.TYPE_FRONTEND)
    assert t["bout"] == 4000                     # sum 类：可以再汇总
    assert "smax" not in t                       # max 类：不汇总
    assert "rtime" not in t                      # wavg 类：不汇总
