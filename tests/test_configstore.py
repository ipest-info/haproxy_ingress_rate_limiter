# tests.test_configstore —— 写 API 的 YAML 回写层。
#
# 这是全项目唯一写配置文件的地方，测试重点是两条安全性质：
#   1. **写出去的永远是能重新加载的配置**：改完先过与启动完全相同的
#      校验链，不合法就拒绝，文件一个字节都不动；
#   2. **原子性**：替换用 os.replace，读者（cfgparse.watch / 重启的
#      服务）绝不会见到半份文件。

from __future__ import annotations

import pytest
import yaml

from rl_limiter import config as configmod
from rl_limiter import configstore

BASE = """\
# 手写注释：回写后会丢（有专门的测试钉住这个行为）
haproxy:
  socket_path: /run/haproxy/admin.sock
  cfg_path: /etc/haproxy/haproxy.cfg
quotas:
  fe_main: 40
"""


@pytest.fixture
def yml(tmp_path):
    p = tmp_path / "config.yaml"
    p.write_text(BASE, encoding="utf-8")
    return p


def reload_cfg(p):
    """用与服务启动完全相同的加载链读回——回写层的核心承诺。"""
    return configmod.load(str(p))


def test_set_quota_updates_and_stays_loadable(yml):
    configstore.set_quota(str(yml), "fe_api", 8.5)
    cfg = reload_cfg(yml)
    assert cfg.quotas == {"fe_main": 40.0, "fe_api": 8.5}


def test_set_quota_zero_is_explicit_unlimited(yml):
    configstore.set_quota(str(yml), "fe_main", 0)
    assert reload_cfg(yml).quotas["fe_main"] == 0.0


def test_set_quota_rejects_invalid_and_leaves_file_intact(yml):
    before = yml.read_text(encoding="utf-8")
    with pytest.raises(ValueError, match="不能为负数"):
        configstore.set_quota(str(yml), "fe_main", -1)
    assert yml.read_text(encoding="utf-8") == before, "拒绝时文件不能被碰"


def test_remove_quota(yml):
    assert configstore.remove_quota(str(yml), "fe_main") is True
    assert reload_cfg(yml).quotas == {}
    # 本来就没登记：返回 False 且不写文件（mtime 都不该变）。
    before = yml.read_text(encoding="utf-8")
    assert configstore.remove_quota(str(yml), "fe_main") is False
    assert yml.read_text(encoding="utf-8") == before


def test_set_and_clear_instance_quota(yml):
    configstore.set_instance_quota(str(yml), 800)
    assert reload_cfg(yml).instance_quota_mbps == 800.0
    # 0 = 不限：直接删字段而不是留一个 0（与"不写"同义，文件更干净）。
    configstore.set_instance_quota(str(yml), 0)
    assert reload_cfg(yml).instance_quota_mbps == 0.0
    raw = yaml.safe_load(yml.read_text(encoding="utf-8"))
    assert "instance_quota_mbps" not in raw


def test_rewrite_keeps_other_fields_and_notes_comment_loss(yml):
    """回写只动目标字段，其余数据原样保留；手写注释会丢，但文件头部
    必须写明这一点——运维打开文件第一眼就该知道为什么。"""
    configstore.set_quota(str(yml), "fe_api", 8)
    text = yml.read_text(encoding="utf-8")
    assert "回写后会丢" not in text            # 原来的手写注释没了
    assert "手写注释不会保留" in text          # 头部说明
    cfg = reload_cfg(yml)
    assert cfg.haproxy.socket_path == "/run/haproxy/admin.sock"
    assert cfg.haproxy.cfg_path == "/etc/haproxy/haproxy.cfg"


def test_refuses_to_touch_broken_yaml(yml):
    """现存文件本身坏了（手工编辑出语法错误）：拒绝回写并点明先手工
    修复——自动"修好"等于悄悄扔掉运维写了一半的东西。"""
    yml.write_text("quotas: [broken\n", encoding="utf-8")
    before = yml.read_text(encoding="utf-8")
    with pytest.raises(ValueError, match="合法 YAML"):
        configstore.set_quota(str(yml), "fe_main", 10)
    assert yml.read_text(encoding="utf-8") == before


def test_missing_file_reports_clearly(tmp_path):
    with pytest.raises(ValueError, match="读不到配置文件"):
        configstore.set_quota(str(tmp_path / "nope.yaml"), "fe_main", 10)


def test_preserves_file_permissions(yml):
    """systemd 部署下 YAML 可能是 640（组内可读）：mkstemp 的 0600 不能
    把权限位悄悄收紧，否则回写一次之后别的读者就读不到了。"""
    import os
    os.chmod(yml, 0o640)
    configstore.set_quota(str(yml), "fe_api", 8)
    assert (yml.stat().st_mode & 0o777) == 0o640


def test_rewrite_preserves_dynamic_sections(yml):
    """写 API 只动自己的键：request_limits 与 dynamic_ingress（闭环参数）
    必须原样留下——丢了它们等于静默关掉入向限速。"""
    yml.write_text(BASE + (
        "request_limits:\n  fe_main: {mbps: 10, conn_rate: 150}\n"
        "dynamic_ingress:\n  enabled: true\n  interval_s: 30\n"),
        encoding="utf-8")
    configstore.set_quota(str(yml), "fe_main", 80)
    cfg = configmod.load(str(yml))
    assert cfg.quotas["fe_main"] == 80.0
    assert cfg.request_limits["fe_main"].mbps == 10.0
    assert cfg.dynamic_ingress.enabled
    assert cfg.dynamic_ingress.interval_s == 30.0


# ---------------------------------------------------------------------------
# request_limits 回写（配置页的入向限速编辑）
# ---------------------------------------------------------------------------

def test_set_request_limit_creates_entry(yml):
    entry = configstore.set_request_limit(str(yml), "fe_main",
                                          mbps=10.0, conn_rate=150)
    assert entry == {"mbps": 10.0, "conn_rate": 150}
    cfg = reload_cfg(yml)
    assert cfg.request_limits["fe_main"].mbps == 10.0
    assert cfg.request_limits["fe_main"].conn_rate == 150


def test_set_request_limit_partial_update_keeps_other_key(yml):
    """部分更新：只改 mbps 时 conn_rate 保留现值（反之亦然）——配置页
    的两个输入框各自独立保存，不能互相清零。"""
    configstore.set_request_limit(str(yml), "fe_main", mbps=10.0,
                                  conn_rate=150)
    configstore.set_request_limit(str(yml), "fe_main", mbps=20.0)
    cfg = reload_cfg(yml)
    assert cfg.request_limits["fe_main"].mbps == 20.0
    assert cfg.request_limits["fe_main"].conn_rate == 150
    configstore.set_request_limit(str(yml), "fe_main", conn_rate=99)
    cfg = reload_cfg(yml)
    assert cfg.request_limits["fe_main"].mbps == 20.0
    assert cfg.request_limits["fe_main"].conn_rate == 99


def test_set_request_limit_requires_at_least_one_field(yml):
    with pytest.raises(configstore.ConfigStoreError):
        configstore.set_request_limit(str(yml), "fe_main")


def test_remove_request_limit(yml):
    configstore.set_request_limit(str(yml), "fe_main", mbps=10.0)
    assert configstore.remove_request_limit(str(yml), "fe_main") is True
    assert configstore.remove_request_limit(str(yml), "fe_main") is False
    cfg = reload_cfg(yml)
    assert "fe_main" not in cfg.request_limits
