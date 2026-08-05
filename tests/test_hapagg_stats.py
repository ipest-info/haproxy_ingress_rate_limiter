# tests.test_hapagg_stats —— CSV/info 解析与跨机合并规则。
#
# 这里是整个工具最容易给出"看起来合理但完全错误"的数字的地方：
#   - 按列序取值 → 换个 HAProxy 版本所有数字串位，图照画，全错；
#   - 合并方式选错 → 3 台的空闲率加起来 300%，或历史峰值相加得出一个
#     从未真实发生过的"总峰值"；
#   - 取不到的列当成 0 → "老版本没这个指标"和"这个值是 0"再也分不开。
# 所以这三件事各有专门的用例盯着。

from __future__ import annotations

import pytest

from hapagg import stats

# 两个版本的列头，**列序刻意不同**（真实版本升级就是这样）。
NEW_HEAD = "# pxname,svname,scur,smax,stot,bin,bout,status,type,rate,qtime,ctime,rtime,ttime,conn_rate,eint"
OLD_HEAD = "# pxname,svname,type,status,bin,bout,scur,smax,stot,rate,qtime,ctime,rtime,ttime"


def new_csv(*rows):
    return NEW_HEAD + "\n" + "\n".join(rows) + "\n"


# ---------------------------------------------------------------------------
# 按列名，不按列序
# ---------------------------------------------------------------------------

def test_same_data_two_column_orders_parse_identically():
    """**这是这个模块存在的理由。**

    同一份数据、两个版本的列序，解出来必须一样。按位置取的话第二种会
    整体串位——而且串完之后视图照样画得出来，只是每个数字都错。
    """
    a = stats.parse_stat_csv(new_csv(
        "fe,FRONTEND,7,9,100,1000,2000,OPEN,0,5,1,2,3,4,6,0"))[0]
    b = stats.parse_stat_csv(
        OLD_HEAD + "\nfe,FRONTEND,0,OPEN,1000,2000,7,9,100,5,1,2,3,4\n")[0]
    for col in ("scur", "smax", "stot", "bin", "bout", "rate"):
        assert a.num(col) == b.num(col), col
    assert a.text("status") == b.text("status") == "OPEN"
    assert a.type == b.type == stats.TYPE_FRONTEND


def test_missing_column_is_none_not_zero():
    """老版本没有 conn_rate 这一列。返回 None（视图显示 "-"）而不是 0，
    否则"没有这个指标"和"速率确实是 0"就再也分不开了。"""
    old = stats.parse_stat_csv(
        OLD_HEAD + "\nfe,FRONTEND,0,OPEN,1,2,3,4,5,6,7,8,9,10\n")[0]
    assert old.num("conn_rate") is None
    assert old.num("scur") == 3


def test_non_numeric_cell_is_none_not_zero():
    """HAProxy 偶有非整数值（空的 check_duration、带百分号的 weight）。
    当作"这格没有可用数字"，不是 0。"""
    row = stats.parse_stat_csv(new_csv(
        "fe,FRONTEND,,9,100,1000,2000,OPEN,0,5,1,2,3,4,6,0"))[0]
    assert row.num("scur") is None


def test_type_column_classifies_rows():
    rows = stats.parse_stat_csv(new_csv(
        "fe,FRONTEND,1,1,1,1,1,OPEN,0,1,1,1,1,1,1,0",
        "be,BACKEND,1,1,1,1,1,UP,1,1,1,1,1,1,1,0",
        "be,srv1,1,1,1,1,1,UP,2,1,1,1,1,1,1,0"))
    assert [r.type for r in rows] == [stats.TYPE_FRONTEND, stats.TYPE_BACKEND,
                                      stats.TYPE_SERVER]


def test_falls_back_to_svname_when_type_column_is_absent():
    """极老的版本没有 type 列。FRONTEND/BACKEND 是 HAProxy 固定使用的
    特殊名字，可以据此回退——总比整批行都判成同一类强。"""
    rows = stats.parse_stat_csv(
        "# pxname,svname,scur\nfe,FRONTEND,1\nbe,BACKEND,2\nbe,srv1,3\n")
    assert [r.type for r in rows] == [stats.TYPE_FRONTEND, stats.TYPE_BACKEND,
                                      stats.TYPE_SERVER]


def test_rejects_reply_without_header():
    """没有列头就没法按列名取值。这时**必须报错**——猜列序正是要避免的事。"""
    with pytest.raises(stats.ParseError, match="列头"):
        stats.parse_stat_csv("fe,FRONTEND,1,2,3\n")


def test_rejects_empty_reply():
    with pytest.raises(stats.ParseError):
        stats.parse_stat_csv("")


# ---------------------------------------------------------------------------
# show info
# ---------------------------------------------------------------------------

def test_parse_info():
    info = stats.parse_info("Name: HAProxy\nVersion: 2.8.16\nCurrConns: 42\n"
                            "Idle_pct: 97\n")
    assert info["Version"] == "2.8.16"
    assert stats.info_num(info, "CurrConns") == 42
    assert stats.info_num(info, "Nope") is None


def test_info_keys_with_spaces_and_parens():
    """真实 show info 里有 "Max connections"、"Idle_pct" 这类键名。"""
    info = stats.parse_info("Max connections: 100\nProcess_num: 1\n")
    assert stats.info_num(info, "Max connections") == 100


# ---------------------------------------------------------------------------
# 合并规则：选错就是"看起来合理但完全错误"
# ---------------------------------------------------------------------------

def test_sum_adds_up():
    assert stats.merge_values(stats.SUM, [10, 20, 30]) == 60


def test_max_does_not_add_peaks():
    """**各机器的峰值不同时发生，相加会凭空放大。**

    3 台各自的历史最高是 100，相加得到 300 —— 那个数字从未真实发生过。
    """
    assert stats.merge_values(stats.MAX, [100, 100, 100]) == 100


def test_min_surfaces_the_worst_node():
    """空闲率这类"越低越危险"的指标取最小。

    求和会得到 285% 这种荒唐数字；取平均会把一台已经跑满的机器藏在
    平均值后面——那正是最需要被看见的那台。
    """
    assert stats.merge_values(stats.MIN, [95, 95, 5]) == 5


def test_weighted_average_is_not_diluted_by_idle_nodes():
    """延迟必须**按请求量加权**。

    一台繁忙机器 100ms（10000 个请求）+ 两台空闲机器 1ms（各 1 个请求）：
    简单平均得 34ms，看着一切正常；加权平均得 100ms，问题立刻可见。
    """
    plain = round((100 + 1 + 1) / 3)
    weighted = stats.merge_values(stats.WAVG, [100, 1, 1], [10000, 1, 1])
    assert plain == 34
    assert weighted == 100, "简单平均会把繁忙机器的高延迟稀释掉"


def test_weighted_average_falls_back_when_no_traffic():
    """权重全为 0（都没跑过请求）时退化成简单平均——这时各台的延迟本来
    也都是 0 或无意义，不会误导。"""
    assert stats.merge_values(stats.WAVG, [4, 6], [0, 0]) == 5


def test_merge_of_nothing_is_none_not_zero():
    """一台都没提供这个指标 → None（"-"），不是 0。"""
    assert stats.merge_values(stats.SUM, []) is None
    assert stats.merge_values(stats.MAX, [None, None]) is None


def test_unknown_merge_mode_is_a_hard_error():
    with pytest.raises(ValueError):
        stats.merge_values("median", [1, 2])


# ---------------------------------------------------------------------------
# 状态：取最坏
# ---------------------------------------------------------------------------

def test_status_merge_takes_the_worst():
    """3 台里 1 台 DOWN，整体就该显示成有问题，不能被 2 台 UP 平均掉。"""
    assert stats.merge_status(["UP", "UP", "DOWN"]) == "DOWN"
    assert stats.merge_status(["UP", "UP", "UP"]) == "UP"
    assert stats.merge_status(["UP", "MAINT"]) == "MAINT"
    assert stats.merge_status([None, None]) is None


def test_status_with_check_counters_still_ranks():
    """真实状态字串常带后缀："UP 2/3"、"DOWN 1/3"。只比第一个词。"""
    assert stats.merge_status(["UP 3/3", "DOWN 1/3"]).startswith("DOWN")


def test_every_dimension_declares_a_known_merge_mode():
    """新增维度时忘了想清楚"跨机怎么合"，是这个工具最容易犯的错。
    这条测试逼着每个维度都显式选一种。"""
    known = {stats.SUM, stats.MAX, stats.MIN, stats.WAVG}
    for d in stats.STAT_DIMS + stats.INFO_DIMS:
        assert d.how in known, d.key
        assert d.label and d.group, d.key


def test_peak_dimensions_say_single_node_in_their_label():
    """峰值取的是"单机最高"，与求和出来的当前值不是同一口径。

    标签不写清楚的话，表上会出现"当前会话 180 / 会话峰值 99"这种自相
    矛盾的组合，整张表的可信度就没了。
    """
    for d in stats.STAT_DIMS:
        if d.how == stats.MAX and "峰值" in d.label:
            assert "单机" in d.label, d.key
