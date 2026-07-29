# tests.test_bulkfill —— 控制台「批量填充」的粘贴解析。
#
# 这几个函数是纯 JS，住在 index.html 的 BULKFILL 标记之间；这里把那段抽出来
# 用 node 跑。之所以值得为几十行 JS 单开一个测试：**解析错了不会报错**，
# 只会把地址悄悄拼歪——比如把 fd00::5 猜成主机 "fd00:" 加端口 5，或者把
# web-1.internal 当成 IP 区间展开。等发现时流量已经打到错误的后端了。
#
# 没有 node 就跳过（CI 里装了才跑）。

from __future__ import annotations

import json
import pathlib
import shutil
import subprocess

import pytest

INDEX = pathlib.Path(__file__).resolve().parents[1] / "rl_limiter/static/index.html"
BEGIN, END = "// >>> BULKFILL >>>", "// <<< BULKFILL <<<"

node = shutil.which("node")
pytestmark = pytest.mark.skipif(node is None, reason="需要 node 才能跑前端解析函数")


def source() -> str:
    text = INDEX.read_text(encoding="utf-8")
    assert text.count(BEGIN) == 1 and text.count(END) == 1, (
        "index.html 里的 BULKFILL 标记不见了或不唯一——这段测试靠它定位被测代码"
    )
    return text.split(BEGIN, 1)[1].split(END, 1)[0]


def run(expr: str):
    """在抽出来的 JS 上求值，结果按 JSON 取回。"""
    script = f"{source()}\nconsole.log(JSON.stringify({expr}));"
    out = subprocess.run([node, "-e", script], capture_output=True, text=True)
    assert out.returncode == 0, out.stderr
    return json.loads(out.stdout)


def addrs(text: str):
    return [(a["address"], a["port"]) for a in run(f"parseAddresses({text!r})")]


# ---------------------------------------------------------------------------
# 地址：分隔符与 host:port
# ---------------------------------------------------------------------------

def test_any_common_separator_works():
    """运维手边的清单可能是换行、逗号、空格甚至混着来的，都得能贴。"""
    want = [("10.0.0.1", None), ("10.0.0.2", None), ("10.0.0.3", None)]
    for text in ["10.0.0.1\n10.0.0.2\n10.0.0.3",
                 "10.0.0.1, 10.0.0.2, 10.0.0.3",
                 "10.0.0.1 10.0.0.2\t10.0.0.3",
                 "10.0.0.1;10.0.0.2\n\n10.0.0.3\n"]:
        assert addrs(text) == want, f"分隔符没吃下: {text!r}"


def test_inline_port_is_taken_from_the_address():
    assert addrs("10.0.0.5:9000") == [("10.0.0.5", 9000)]
    assert addrs("web-a.internal:8080") == [("web-a.internal", 8080)]


def test_bare_ipv6_is_not_mistaken_for_host_port():
    """裸 IPv6 一堆冒号。猜成"主机 fd00: + 端口 5"会静默把地址拼坏，
    所以只认方括号写法，其余原样当地址、让人自己补端口。"""
    assert addrs("fd00::5") == [("fd00::5", None)]
    assert addrs("[fd00::5]:9000") == [("fd00::5", 9000)]
    assert addrs("[fd00::5]") == [("fd00::5", None)]


# ---------------------------------------------------------------------------
# 地址：区间展开
# ---------------------------------------------------------------------------

def test_last_octet_range_expands():
    assert addrs("10.0.0.20-22") == [
        ("10.0.0.20", None), ("10.0.0.21", None), ("10.0.0.22", None)]


def test_range_keeps_the_inline_port_on_every_expanded_address():
    assert addrs("10.0.0.20-21:9000") == [("10.0.0.20", 9000), ("10.0.0.21", 9000)]


def test_hostnames_with_dashes_are_not_treated_as_ranges():
    """web-1.internal、srv-01 这类名字里就有连字符。当区间展开会凭空造出
    一批根本不存在的后端。"""
    assert addrs("web-1.internal") == [("web-1.internal", None)]
    assert addrs("srv-01") == [("srv-01", None)]
    assert addrs("10.0.0.a-c") == [("10.0.0.a-c", None)]


def test_reversed_or_out_of_range_octets_are_left_alone():
    """起点大于终点、或超过 255 的，宁可原样留着让校验去报错，
    也不要展开出一串垃圾地址。"""
    assert addrs("10.0.0.20-10") == [("10.0.0.20-10", None)]
    assert addrs("10.0.0.1-300") == [("10.0.0.1-300", None)]


# ---------------------------------------------------------------------------
# 端口
# ---------------------------------------------------------------------------

def test_ports_accept_single_range_and_mixed():
    assert run("parsePorts('9000')")["ports"] == [9000]
    assert run("parsePorts('9000-9003')")["ports"] == [9000, 9001, 9002, 9003]
    assert run("parsePorts('9000,9100 9200')")["ports"] == [9000, 9100, 9200]


def test_bad_port_text_reports_instead_of_silently_dropping():
    assert "不是数字" in run("parsePorts('90a0')")["error"]
    assert "起点比终点大" in run("parsePorts('9100-9000')")["error"]
    assert "超过 1024" in run("parsePorts('1-60000')")["error"]


# ---------------------------------------------------------------------------
# 组合：地址 × 端口
# ---------------------------------------------------------------------------

def test_single_port_applies_to_every_address():
    """最常见的一种：一列 IP + 一个端口。"""
    r = run("combineServers('10.0.0.1\\n10.0.0.2\\n10.0.0.3', '9000', 'srv', [])")
    assert [(x["name"], x["address"], x["port"]) for x in r["rows"]] == [
        ("srv1", "10.0.0.1", 9000),
        ("srv2", "10.0.0.2", 9000),
        ("srv3", "10.0.0.3", 9000)]


def test_ports_zip_with_addresses_one_to_one():
    r = run("combineServers('10.0.0.1 10.0.0.2', '9000 9001', 'srv', [])")
    assert [(x["address"], x["port"]) for x in r["rows"]] == [
        ("10.0.0.1", 9000), ("10.0.0.2", 9001)]


def test_mismatched_counts_report_instead_of_guessing():
    """3 个地址配 2 个端口——不管怎么补都是猜。猜错的后果是流量打到错误的
    端口，而界面上看着一切正常，所以宁可报错。"""
    r = run("combineServers('10.0.0.1 10.0.0.2 10.0.0.3', '9000 9001', 'srv', [])")
    assert "对不上" in r["error"]


def test_inline_port_wins_over_the_port_box():
    r = run("combineServers('10.0.0.1:8080\\n10.0.0.2', '9000', 'srv', [])")
    assert [(x["address"], x["port"]) for x in r["rows"]] == [
        ("10.0.0.1", 8080), ("10.0.0.2", 9000)]


def test_missing_port_is_reported_with_the_offending_address():
    r = run("combineServers('10.0.0.1', '', 'srv', [])")
    assert "10.0.0.1" in r["error"]


def test_generated_names_skip_ones_already_in_use():
    """追加时不能撞上表格里已有的名字——重名的 server 行 HAProxy 直接
    拒绝加载，配置下发会整体失败。"""
    r = run("combineServers('10.0.0.8 10.0.0.9', '9000', 'srv', ['srv1','srv3'])")
    assert [x["name"] for x in r["rows"]] == ["srv2", "srv4"]


def test_custom_prefix_is_used():
    r = run("combineServers('10.0.0.1', '9000', 'web', [])")
    assert r["rows"][0]["name"] == "web1"
