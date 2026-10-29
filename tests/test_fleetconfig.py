# tests.test_fleetconfig —— 总控配置的解析/校验/回写/热更。
#
# 与节点侧 config/configstore 同一套承诺：非法配置逐条拒绝且指明字段；
# 回写前整份校验、原子替换；watch 失败 fail-static。

from __future__ import annotations

import asyncio

import pytest
import yaml

from rl_limiter import fleetconfig

BASE = """\
total_egress_mbps: 300
nodes:
  - name: node1
    url: http://10.0.0.1:8090
    token: fe8890a02029cbe0d9a67dd1cb2cbd8f1daa11101d13ef31
  - name: node2
    url: http://10.0.0.2:8090
    token: 4b4ee4871702687022f6fe251e37f4939373663d60927da2
    console_url: http://pub.example:8092
"""


@pytest.fixture
def yml(tmp_path):
    p = tmp_path / "fleet.yaml"
    p.write_text(BASE, encoding="utf-8")
    return p


def test_load_fills_defaults(yml):
    cfg = fleetconfig.load(str(yml))
    assert cfg.total_egress_mbps == 300.0
    assert cfg.poll_interval_s == 2.0
    assert cfg.alloc_interval_s == 15.0
    assert cfg.offline_after == 3
    assert cfg.alloc.min_share_ratio == 0.2
    assert cfg.alloc.min_quota_mbps == 0.5
    assert [n.name for n in cfg.nodes] == ["node1", "node2"]
    # url 尾部斜杠归一；console_url 缺省为空（调用方回退到 url）。
    assert cfg.nodes[0].console_url == ""
    assert cfg.nodes[1].console_url == "http://pub.example:8092"


@pytest.mark.parametrize("mutate, keyword", [
    ({"total_egress_mbps": -1}, "total_egress_mbps"),
    ({"alloc_interval_s": 5}, "alloc_interval_s"),
    ({"poll_interval_s": 0}, "poll_interval_s"),
    ({"offline_after": 0}, "offline_after"),
    ({"offline_after": 1.5}, "offline_after"),
    ({"allocation": {"min_share_ratio": 0.6}}, "min_share_ratio"),
    ({"allocation": {"smooth_alpha": 0}}, "smooth_alpha"),
    ({"allocation": {"min_quota_mbps": 0}}, "min_quota_mbps"),
    ({"allocation": {"press_boost": 0.5}}, "press_boost"),
    ({"allocation": {"nope": 1}}, "nope"),
    ({"bogus_key": 1}, "bogus_key"),
])
def test_rejects_invalid_fields(mutate, keyword):
    raw = yaml.safe_load(BASE)
    raw.update(mutate)
    with pytest.raises(ValueError) as ei:
        fleetconfig.from_raw(raw, source="测试")
    assert keyword in str(ei.value)


def test_rejects_bad_nodes():
    for nodes, kw in [
        ([{"name": "a b", "url": "http://x:1"}], "name"),
        ([{"name": "n1", "url": "ftp://x"}], "url"),
        ([{"name": "n1", "url": "http://x:1"},
          {"name": "n1", "url": "http://y:1"}], "重复"),
        ([{"name": "n1", "url": "http://x:1", "extra": 1}], "extra"),
        ("not-a-list", "列表"),
    ]:
        with pytest.raises(ValueError) as ei:
            fleetconfig.from_raw({"nodes": nodes}, source="测试")
        assert kw in str(ei.value)


def test_empty_token_is_allowed():
    """token 为空合法（只读观测节点），校验不拒绝——提醒在运行时打。"""
    cfg = fleetconfig.from_raw(
        {"nodes": [{"name": "n1", "url": "http://x:1"}]}, source="测试")
    assert cfg.nodes[0].token == ""


# ---------------------------------------------------------------------------
# 回写
# ---------------------------------------------------------------------------

def test_set_total_quota_roundtrip(yml):
    fleetconfig.set_total_quota(str(yml), 500)
    cfg = fleetconfig.load(str(yml))
    assert cfg.total_egress_mbps == 500.0
    # 其它段原样保留。
    assert [n.name for n in cfg.nodes] == ["node1", "node2"]
    # 0 = 删键（纯观察）。
    fleetconfig.set_total_quota(str(yml), 0)
    assert fleetconfig.load(str(yml)).total_egress_mbps == 0.0
    assert "total_egress_mbps" not in yml.read_text(encoding="utf-8")


def test_set_total_quota_refuses_broken_yaml(yml):
    yml.write_text("nodes: [", encoding="utf-8")
    before = yml.read_text(encoding="utf-8")
    with pytest.raises(fleetconfig.FleetConfigError):
        fleetconfig.set_total_quota(str(yml), 100)
    assert yml.read_text(encoding="utf-8") == before


def test_add_node_appends_and_keeps_other_sections(yml):
    fleetconfig.add_node(str(yml), "node3",
                         {"url": "http://10.0.0.3:8090/", "token": " 272a3e101a6d5e039a5c7739f8c097f879fcd70a04213a1f "})
    cfg = fleetconfig.load(str(yml))
    assert [n.name for n in cfg.nodes] == ["node1", "node2", "node3"]
    n3 = cfg.nodes[2]
    assert (n3.url, n3.token, n3.console_url) == \
        ("http://10.0.0.3:8090", "272a3e101a6d5e039a5c7739f8c097f879fcd70a04213a1f", "")
    assert cfg.total_egress_mbps == 300.0
    # 已有节点的令牌原样保留。
    assert cfg.nodes[0].token == "fe8890a02029cbe0d9a67dd1cb2cbd8f1daa11101d13ef31"


def test_add_node_without_token_is_read_only(yml):
    fleetconfig.add_node(str(yml), "obs", {"url": "http://10.0.0.9:8090"})
    assert fleetconfig.load(str(yml)).nodes[-1].token == ""


def test_add_node_into_empty_file(tmp_path):
    p = tmp_path / "fleet.yaml"
    p.write_text("nodes: []\n", encoding="utf-8")
    fleetconfig.add_node(str(p), "n1", {"url": "http://x:8090"})
    assert [n.name for n in fleetconfig.load(str(p)).nodes] == ["n1"]


@pytest.mark.parametrize("name, fields, err", [
    ("node1", {"url": "http://10.0.0.9:8090"}, fleetconfig.NodeExistsError),
    ("bad name", {"url": "http://x:1"}, ValueError),
    ("n9", {"url": "ftp://x"}, ValueError),
    ("n9", {"url": "http://"}, ValueError),
    ("n9", {}, ValueError),                                # 缺 url
    ("n9", {"url": "http://x:1", "token": "a b"}, ValueError),
    ("n9", {"url": "http://x:1", "token": 123}, ValueError),
])
def test_add_node_rejects_and_leaves_file_untouched(yml, name, fields, err):
    before = yml.read_text(encoding="utf-8")
    with pytest.raises(err):
        fleetconfig.add_node(str(yml), name, fields)
    assert yml.read_text(encoding="utf-8") == before


def test_update_node_changes_only_given_fields(yml):
    fleetconfig.update_node(str(yml), "node2", {"url": "http://10.0.0.22:8090"})
    n2 = fleetconfig.load(str(yml)).nodes[1]
    assert n2.url == "http://10.0.0.22:8090"
    assert n2.token == "4b4ee4871702687022f6fe251e37f4939373663d60927da2"                         # 没给 = 不动
    assert n2.console_url == "http://pub.example:8092"
    # token 给空串 = 清除（转只读观测）。
    fleetconfig.update_node(str(yml), "node2", {"token": ""})
    assert fleetconfig.load(str(yml)).nodes[1].token == ""


def test_update_and_remove_unknown_node(yml):
    before = yml.read_text(encoding="utf-8")
    with pytest.raises(fleetconfig.NodeNotFoundError):
        fleetconfig.update_node(str(yml), "nope", {"token": "x"})
    with pytest.raises(fleetconfig.NodeNotFoundError):
        fleetconfig.remove_node(str(yml), "nope")
    assert yml.read_text(encoding="utf-8") == before


def test_remove_node(yml):
    fleetconfig.remove_node(str(yml), "node1")
    cfg = fleetconfig.load(str(yml))
    assert [n.name for n in cfg.nodes] == ["node2"]
    assert cfg.total_egress_mbps == 300.0


def test_node_writes_refuse_broken_nodes_section(tmp_path):
    p = tmp_path / "fleet.yaml"
    p.write_text("nodes: oops\n", encoding="utf-8")
    with pytest.raises(fleetconfig.FleetConfigError):
        fleetconfig.add_node(str(p), "n1", {"url": "http://x:1"})
    assert p.read_text(encoding="utf-8") == "nodes: oops\n"


def test_weak_node_token_in_yaml_counts_as_none():
    """配置文件里的弱节点令牌：配置照常加载，但该节点当作没有令牌（只读
    观测、绝不下发），原因记在 token_rejected。"""
    cfg = fleetconfig.from_raw({"nodes": [
        {"name": "weak", "url": "http://x:1", "token": "tok-123"},
        {"name": "strong", "url": "http://y:1",
         "token": "5e87f5b9c50fca3df7adef8427563b8ddfe798c5397cea8f"},
    ]}, source="测试")
    weak, strong = cfg.nodes
    assert weak.token == "" and "太短" in weak.token_rejected
    assert strong.token and strong.token_rejected == ""


@pytest.mark.parametrize("tok", ["tok3", "x" * 40, "node-01-secret-key-for-fleet-xx"])
def test_node_writes_refuse_weak_tokens(yml, tok):
    before = yml.read_text(encoding="utf-8")
    with pytest.raises(fleetconfig.FleetConfigError, match="强度不足"):
        fleetconfig.add_node(str(yml), "n9", {"url": "http://x:1",
                                                "token": tok})
    with pytest.raises(fleetconfig.FleetConfigError, match="强度不足"):
        fleetconfig.update_node(str(yml), "node1", {"token": tok})
    assert yml.read_text(encoding="utf-8") == before


# ---------------------------------------------------------------------------
# 接入串（粘贴批量导入）
# ---------------------------------------------------------------------------

TOK_A = "d9253b4e26b5a7f47d1c1dfd422437a8365824e8c0625dd0"
TOK_B = "272a3e101a6d5e039a5c7739f8c097f879fcd70a04213a1f"


def test_join_line_roundtrip():
    line = fleetconfig.join_line("n1", "http://10.0.0.9:8090", TOK_A,
                                 "http://pub:8092")
    assert line.startswith("rl-node ")
    (e,), ignored = fleetconfig.parse_join_text(line)
    assert ignored == 0 and e["error"] == ""
    assert (e["name"], e["url"], e["token"], e["console_url"]) == \
        ("n1", "http://10.0.0.9:8090", TOK_A, "http://pub:8092")


def test_parse_tolerates_pasted_install_output():
    """整段安装输出直接粘：非接入串的行忽略，行首前缀也忽略。"""
    text = f"""install.sh: 完成。下一步：
install.sh:   2. systemctl enable --now rl-limiter
  rl-node name=a url=http://10.0.0.1:8090/ token={TOK_A}

rl-node name=b url=http://10.0.0.2:8090
"""
    entries, ignored = fleetconfig.parse_join_text(text)
    assert ignored == 2
    assert [(e["name"], e["url"], bool(e["token"]), e["error"])
            for e in entries] == [
        ("a", "http://10.0.0.1:8090", True, ""),
        ("b", "http://10.0.0.2:8090", False, ""),       # 没令牌 = 只读观测
    ]


@pytest.mark.parametrize("line, kw", [
    ("rl-node url=http://x:1", "缺 name"),
    ("rl-node name=a", "缺 url"),
    ("rl-node name=a b url=http://x:1", "key=value"),
    ("rl-node name=a url=http://x:1 foo=1", "不认识的字段"),
    ("rl-node name=a! url=http://x:1", "name"),
    ("rl-node name=a url=ftp://x", "url"),
    ("rl-node name=a url=http://x:1 token=short", "强度不足"),
    ("rl-node name=a url=http://x:1 console=x", "console"),
])
def test_parse_reports_bad_lines(line, kw):
    (e,), _ = fleetconfig.parse_join_text(line)
    assert kw in e["error"]


def test_parse_flags_duplicate_names_in_one_paste():
    entries, _ = fleetconfig.parse_join_text(
        "rl-node name=a url=http://x:1\nrl-node name=a url=http://y:1\n")
    assert entries[0]["error"] == ""
    assert "与第 1 行重名" in entries[1]["error"]


def test_import_adds_updates_and_skips(yml):
    text = "\n".join([
        f"rl-node name=node3 url=http://10.0.0.3:8090 token={TOK_A}",  # 新增
        f"rl-node name=node1 url=http://10.0.0.1:8090 token={TOK_B}",  # 换令牌
        "rl-node name=bad url=ftp://x",                                 # 错误
    ])
    entries, _ = fleetconfig.parse_join_text(text)
    before = yml.read_text(encoding="utf-8")
    preview = fleetconfig.import_nodes(str(yml), entries, dry_run=True)
    assert [(r["name"], r["action"]) for r in preview] == \
        [("node3", "add"), ("node1", "update"), ("bad", "error")]
    assert preview[1]["changes"] == ["token"]
    assert yml.read_text(encoding="utf-8") == before      # 预览不写盘

    fleetconfig.import_nodes(str(yml), entries)
    cfg = fleetconfig.load(str(yml))
    by = {n.name: n for n in cfg.nodes}
    assert by["node3"].token == TOK_A
    assert by["node1"].token == TOK_B
    assert by["node2"].console_url == "http://pub.example:8092"   # 不相关的不动
    assert "bad" not in by

    # 再粘一遍：全部无变化，不重写文件。
    again = fleetconfig.import_nodes(str(yml), entries[:2])
    assert [r["action"] for r in again] == ["same", "same"]


def test_import_update_without_token_clears_it(yml):
    """接入串没带令牌 = 节点当前没令牌：总控清掉旧令牌（转只读观测），
    留着只会一直 401。console_url 没带则保留原值。"""
    entries, _ = fleetconfig.parse_join_text(
        "rl-node name=node2 url=http://10.0.0.2:8090")
    (r,) = fleetconfig.import_nodes(str(yml), entries)
    assert r["action"] == "update" and r["changes"] == ["token"]
    n2 = fleetconfig.load(str(yml)).nodes[1]
    assert n2.token == "" and n2.console_url == "http://pub.example:8092"


# ---------------------------------------------------------------------------
# 热更轮询
# ---------------------------------------------------------------------------

async def _run_watch_once(yml, poke, applied):
    task = asyncio.ensure_future(fleetconfig.watch(
        str(yml), applied.append,
        __import__("logging").getLogger("t"), poke=poke, interval_s=60))
    # 首轮由 poke 触发。
    poke.set()
    for _ in range(100):
        await asyncio.sleep(0.01)
        if applied:
            break
    return task


async def test_watch_applies_and_fail_static(tmp_path):
    yml = tmp_path / "fleet.yaml"
    yml.write_text(BASE, encoding="utf-8")
    poke = asyncio.Event()
    applied: list = []
    task = await _run_watch_once(yml, poke, applied)
    try:
        assert applied and applied[-1].total_egress_mbps == 300.0
        # 热更字段变化 → 再次回调。
        yml.write_text(BASE.replace("300", "600"), encoding="utf-8")
        poke.set()
        for _ in range(100):
            await asyncio.sleep(0.01)
            if len(applied) >= 2:
                break
        assert applied[-1].total_egress_mbps == 600.0
        # 文件写坏 → fail-static：不回调、不崩。
        yml.write_text("nodes: [", encoding="utf-8")
        poke.set()
        await asyncio.sleep(0.1)
        assert len(applied) == 2
        assert not task.done()
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
