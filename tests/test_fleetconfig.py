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
