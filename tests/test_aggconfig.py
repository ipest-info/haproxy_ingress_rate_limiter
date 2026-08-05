# tests.test_aggconfig —— hap-agg 的 YAML 配置解析与目标清单回写。

from __future__ import annotations

import pytest
import yaml

from rl_limiter import aggconfig
from rl_limiter.agg import Target

VALID = """\
log_level: debug
poll_interval_s: 2.0
timeout_ms: 300
targets:
  - 10.0.0.11:9999
  - name: sg-02
    addr: 10.0.0.12:9999
"""


def load_from(tmp_path, text=VALID):
    p = tmp_path / "agg.yaml"
    p.write_text(text, encoding="utf-8")
    return p, aggconfig.load(str(p))


def test_load_full(tmp_path):
    _p, cfg = load_from(tmp_path)
    assert cfg.log_level == "debug"
    assert cfg.poll_interval_s == 2.0
    assert cfg.timeout_s == 0.3
    assert [(t.name, t.addr) for t in cfg.targets] == [
        ("10.0.0.11:9999", "10.0.0.11:9999"), ("sg-02", "10.0.0.12:9999")]
    # 目标的命令超时跟随全局 timeout_ms。
    assert all(t.timeout_s == 0.3 for t in cfg.targets)


def test_empty_targets_is_valid(tmp_path):
    """空清单合法：先起服务、再从页面批量导入是正常使用路径。"""
    _p, cfg = load_from(tmp_path, "targets: []\n")
    assert cfg.targets == []
    _p, cfg = load_from(tmp_path, "")
    assert cfg.targets == []


@pytest.mark.parametrize("text,match", [
    ("log_level: verbose\n", "log_level"),
    ("poll_interval_s: 0.05\n", "poll_interval_s"),
    ("poll_interval_s: abc\n", "必须是数字"),
    ("targets: {a: 1}\n", "targets 必须是列表"),
    ("targets: [10.0.0.1]\n", "缺少端口"),
    ("targets: [{name: x}]\n", "缺少 addr"),
    ("targets: ['a 1.1.1.1:9999', 'a 2.2.2.2:9999']\n", "目标名重复"),
    ("targets: ['a 1.1.1.1:9999', 'b 1.1.1.1:9999']\n", "目标地址重复"),
])
def test_rejections(tmp_path, text, match):
    p = tmp_path / "agg.yaml"
    p.write_text(text, encoding="utf-8")
    with pytest.raises(ValueError, match=match):
        aggconfig.load(str(p))


def test_save_targets_roundtrip_preserves_other_fields(tmp_path):
    p, cfg = load_from(tmp_path)
    cfg.targets.append(Target(name="new", host="10.0.0.13", port=9999))
    aggconfig.save_targets(str(p), cfg.targets)
    cfg2 = aggconfig.load(str(p))
    assert [(t.name, t.addr) for t in cfg2.targets] == [
        ("10.0.0.11:9999", "10.0.0.11:9999"),
        ("sg-02", "10.0.0.12:9999"),
        ("new", "10.0.0.13:9999")]
    # 其余字段原样保留。
    assert cfg2.poll_interval_s == 2.0 and cfg2.log_level == "debug"
    # 名字 = 地址的目标写成短形式（一行字符串），可读性更好。
    raw = yaml.safe_load(p.read_text(encoding="utf-8"))
    assert raw["targets"][0] == "10.0.0.11:9999"
    assert raw["targets"][2] == {"name": "new", "addr": "10.0.0.13:9999"}


def test_save_refuses_broken_yaml(tmp_path):
    p = tmp_path / "agg.yaml"
    p.write_text("targets: [broken\n", encoding="utf-8")
    before = p.read_text(encoding="utf-8")
    with pytest.raises(ValueError, match="合法 YAML"):
        aggconfig.save_targets(str(p), [])
    assert p.read_text(encoding="utf-8") == before
