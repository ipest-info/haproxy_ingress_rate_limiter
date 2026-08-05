# tests.test_hapagg_targets —— 批量导入被监控的 HAProxy 列表。
#
# 这一层出错的后果很特别：**清单里少了一台，聚合视图看起来完全正常，
# 只是总量偏低**。没有任何报错、没有任何异常曲线。所以这里测的重点是
# "写错的行必须报到具体位置"，以及"重复的行不能让流量被算两遍"。

from __future__ import annotations

import pytest

from hapagg.targets import (DEFAULT_PORT, Target, TargetError, load_targets,
                            parse_targets)


def names(ts):
    return [t.endpoint for t in ts]


def test_plain_list_with_default_port():
    ts = parse_targets("10.0.0.11:9999\n10.0.0.12\n")
    assert names(ts) == ["10.0.0.11:9999", f"10.0.0.12:{DEFAULT_PORT}"]


def test_comments_and_blank_lines():
    ts = parse_targets("""
        # 北京一区
        10.0.0.11:9999    # 主
                          
        10.0.0.12:9999
    """)
    assert names(ts) == ["10.0.0.11:9999", "10.0.0.12:9999"]


def test_named_targets_show_up_in_the_view():
    """给机器起名字是为了让视图里的一行能被认出来。
    没有名字时退化成 host:port，也仍然可读。"""
    (a, b) = parse_targets("hap-bj-1 = 10.0.0.11:9999\n10.0.0.12:9999\n")
    assert a.name == "hap-bj-1" and a.label == "hap-bj-1"
    assert b.name == "" and b.label == "10.0.0.12:9999"


def test_range_expands():
    ts = parse_targets("10.0.0.20-24:9999\n")
    assert names(ts) == [f"10.0.0.{i}:9999" for i in range(20, 25)]


def test_named_range_gets_numbered():
    """区间 + 名字：名字必须带序号，否则几台机器同名，视图里分不出谁是谁。"""
    ts = parse_targets("hap = 10.0.0.20-22:9999\n")
    assert [t.name for t in ts] == ["hap-1", "hap-2", "hap-3"]


def test_ipv6_needs_brackets_when_a_port_is_given():
    """裸 IPv6 带端口无法解析——"2001:db8::5:9999" 的最后一段到底是端口
    还是地址的一部分，没有任何办法判断。所以强制方括号。"""
    (t,) = parse_targets("[2001:db8::5]:9999\n")
    assert t.host == "2001:db8::5" and t.port == 9999
    assert t.endpoint == "[2001:db8::5]:9999"
    (t2,) = parse_targets("2001:db8::5\n")
    assert t2.host == "2001:db8::5" and t2.port == DEFAULT_PORT


def test_duplicates_are_dropped_not_doubled():
    """**同一台写两遍会让它的流量在聚合里被算两次，而视图完全正常。**

    这种静默翻倍比报错难查得多。重复登记是无害的手误（不是意图表达错误），
    所以去重而不是报错。
    """
    ts = parse_targets("10.0.0.11:9999\nhap-1 = 10.0.0.11:9999\n10.0.0.11\n",
                       default_port=9999)
    assert len(ts) == 1


def test_errors_point_at_the_offending_line():
    """清单几十行，报错不给行号等于让人从头看一遍。"""
    with pytest.raises(TargetError, match="第 3 行"):
        parse_targets("10.0.0.11:9999\n10.0.0.12:9999\n10.0.0.13:abc\n")


@pytest.mark.parametrize("bad, why", [
    ("10.0.0.1:0", "端口"),
    ("10.0.0.1:70000", "端口"),
    ("10.0.0.20-10:9999", "区间"),
    ("10.0.0.20-300:9999", "区间"),
    ("[2001:db8::5:9999", "方括号"),
    ("bad host name:9999", "不合法"),
    ("= 10.0.0.1", "名字为空"),
    ("name =", "没有地址"),
])
def test_rejects_malformed_lines(bad, why):
    with pytest.raises(TargetError, match=why):
        parse_targets(bad)


def test_hostnames_are_allowed():
    """内网常用主机名而不是 IP。DNS 名的合法性交给解析器，这里只拦手误。"""
    (t,) = parse_targets("hap-node-1.internal:9999\n")
    assert t.host == "hap-node-1.internal"


def test_load_merges_file_and_inline(tmp_path):
    f = tmp_path / "t.txt"
    f.write_text("10.0.0.11:9999\n", encoding="utf-8")
    ts = load_targets([str(f)], ["10.0.0.12:9999,10.0.0.13:9999"])
    assert names(ts) == ["10.0.0.11:9999", "10.0.0.12:9999", "10.0.0.13:9999"]


def test_missing_file_is_reported_clearly():
    with pytest.raises(TargetError, match="读不了"):
        load_targets(["/nonexistent/targets.txt"], [])


def test_target_is_hashable_and_comparable():
    """去重与集合运算要用到。"""
    assert Target("10.0.0.1", 9999) == Target("10.0.0.1", 9999)
    assert len({Target("10.0.0.1", 9999), Target("10.0.0.1", 9999)}) == 1
