# tests.test_config —— 配置解析/默认值/校验（通用形态）。
#
# 本分支的 YAML 不含任何服务接线：内容是 name / 运行参数 /
# instance_quota_mbps / ports（名字→端口，为按端口静态入向限速服务）/
# request_limits / dynamic_ingress。每条拒绝路径都用 match 断言把
# "错误信息要点名字段"这一契约固定下来。

from __future__ import annotations

import textwrap

import pytest

from rl_limiter import config, model

VALID = """\
name: edge-01
log_level: debug
tick_interval_s: 1.0
instance_quota_mbps: 800
ports:
  web: 8080
  api: 8443
request_limits:
  web:
    mbps: 10
    conn_rate: 150
"""


def load_from(tmp_path, text=VALID):
    p = tmp_path / "config.yaml"
    p.write_text(textwrap.dedent(text), encoding="utf-8")
    return config.load(str(p))


def minimal(extra=""):
    return f"instance_quota_mbps: 0\n{extra}"


def test_load_full_config(tmp_path):
    cfg = load_from(tmp_path)
    assert cfg.name == "edge-01"
    assert cfg.log_level == "debug"
    assert cfg.instance_quota_mbps == 800.0
    assert cfg.ports == {"web": 8080, "api": 8443}
    assert cfg.request_limits["web"].mbps == 10.0
    assert cfg.request_limits["web"].conn_rate == 150
    assert cfg.quotas == {}, "quotas 在本形态恒空（共享代码路径保留字段）"


def test_defaults_filled(tmp_path):
    cfg = load_from(tmp_path, "")
    assert cfg.name == "server"
    assert cfg.log_level == "info"
    assert cfg.tick_interval_s == 1.0
    assert cfg.instance_quota_mbps == 0.0
    assert cfg.ports == {} and cfg.request_limits == {}


# ---------------------------------------------------------------------------
# ports 段（名字 → 本机监听端口）
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("text,match", [
    (minimal("ports: [8080]\n"), r"ports 必须是键值映射"),
    (minimal("ports: {web: abc}\n"), r"ports.web 必须是整数端口号"),
    (minimal("ports: {web: 0}\n"), r"1~65535"),
    (minimal("ports: {web: 70000}\n"), r"1~65535"),
    (minimal("ports: {'bad name': 8080}\n"), r"ports 的键"),
    (minimal("ports: {a: 8080, b: 8080}\n"), r"同一个\s*端口"),
])
def test_ports_rejections(tmp_path, text, match):
    with pytest.raises(ValueError, match=match):
        load_from(tmp_path, text)


def test_request_limit_requires_port_registration(tmp_path):
    """request_limits 的名字必须先在 ports 里登记——没有端口号 tc 不知道
    限哪个端口，静默吞掉等于没限。"""
    with pytest.raises(ValueError, match="没有对应的端口登记"):
        load_from(tmp_path, minimal(
            "request_limits: {web: {mbps: 10}}\n"))


# ---------------------------------------------------------------------------
# 旧形态字段：显式拒绝 + 迁移指引
# ---------------------------------------------------------------------------

def test_haproxy_section_rejected_with_migration_hint(tmp_path):
    """本形态不依赖任何服务：haproxy 接线段必须显式拒绝并说明迁移方向，
    静默忽略会让运维以为还在按 HAProxy 的口径监控。"""
    with pytest.raises(ValueError) as ei:
        load_from(tmp_path, minimal(
            "haproxy: {socket_path: /run/h.sock, cfg_path: /etc/h.cfg}\n"))
    msg = str(ei.value)
    assert "haproxy" in msg and "ports" in msg and "不依赖" in msg


def test_quotas_rejected_with_guidance(tmp_path):
    """端口级出向目标需要端口级出向观测——通用形态没有这个数据源，
    必须显式拒绝并指向 instance_quota_mbps / request_limits。"""
    with pytest.raises(ValueError) as ei:
        load_from(tmp_path, minimal("quotas: {web: 40}\n"))
    msg = str(ei.value)
    assert "quotas" in msg and "instance_quota_mbps" in msg


def test_legacy_frontends_key_rejected(tmp_path):
    with pytest.raises(ValueError, match="frontends"):
        load_from(tmp_path, minimal(
            "frontends:\n  - {name: fe_a, bind_port: 8080}\n"))


def test_old_nic_quota_key_rejected_with_migration_hint(tmp_path):
    with pytest.raises(ValueError, match="instance_quota_mbps"):
        load_from(tmp_path, minimal("nic_quota_mbps: 100\n"))


# ---------------------------------------------------------------------------
# 运行参数
# ---------------------------------------------------------------------------

def test_tick_interval_locked_to_one_second(tmp_path):
    """速率差分与 10 秒窗口都以「1 拍 = 1 秒」为前提，改这个值会让速率
    口径与告警阈值整体失真。"""
    with pytest.raises(ValueError, match="只支持 1.0"):
        load_from(tmp_path, minimal("tick_interval_s: 0.5\n"))


def test_log_level_rejected(tmp_path):
    with pytest.raises(ValueError, match="log_level 取值非法"):
        load_from(tmp_path, minimal("log_level: loud\n"))


def test_error_message_contains_path(tmp_path):
    """所有校验错误都要带上配置文件路径，方便多配置文件部署时定位。"""
    p = tmp_path / "config.yaml"
    p.write_text("log_level: loud\n", encoding="utf-8")
    with pytest.raises(ValueError, match=str(p)):
        config.load(str(p))


def test_shipped_example_config_is_valid():
    """仓库自带的示例配置必须永远能通过当前校验——它是运维的起点。"""
    cfg = config.load("deploy/config/limiter.example.yaml")
    assert cfg.log_level in ("debug", "info", "warn", "error")


# ---------------------------------------------------------------------------
# 单位换算（model 层，与配置口径一次性对齐）
# ---------------------------------------------------------------------------

def test_quota_unit_is_mbps_end_to_end():
    """8 倍（bit/byte）与 1e6 倍（Mbps/bps）的混淆是全项目最贵的错误。"""
    fe = model.FrontendConfig(name="fe_a", bind_port=8080, quota_mbps=40)
    assert fe.quota_bits_per_sec == 40_000_000        # 下发给 tc 的 rate
    assert fe.quota_bytes_per_sec == 5_000_000        # 内部与告警判定口径
    assert fe.limited


def test_quota_accepts_fractions():
    fe = model.FrontendConfig(name="fe_a", bind_port=8080, quota_mbps=0.5)
    assert fe.quota_bits_per_sec == 500_000
    assert fe.quota_bytes_per_sec == 62_500


def test_zero_quota_means_monitor_only():
    fe = model.FrontendConfig(name="fe_a", bind_port=8080)
    assert fe.quota_mbps == 0.0 and not fe.limited


def test_quota_survives_the_dict_round_trip():
    fe = model.FrontendConfig(name="fe_a", bind_port=8080, quota_mbps=40.5,
                              bind_address="10.0.0.1", mode="http")
    again = model.FrontendConfig.from_dict(fe.to_dict())
    assert again == fe
    assert again.quota_bits_per_sec == fe.quota_bits_per_sec


# ---------------------------------------------------------------------------
# 实例总限速（instance_quota_mbps）
# ---------------------------------------------------------------------------

def test_instance_quota_parsed(tmp_path):
    assert load_from(tmp_path, minimal(
        "instance_quota_mbps: 800\n")).instance_quota_mbps == 800.0


@pytest.mark.parametrize("text,match", [
    (minimal("instance_quota_mbps: -5\n"), r"不能为负数"),
    (minimal("instance_quota_mbps: 0.000004\n"), r"太小"),
    (minimal("instance_quota_mbps: abc\n"), r"必须是数字"),
])
def test_instance_quota_rejections(tmp_path, text, match):
    with pytest.raises(ValueError, match=match):
        load_from(tmp_path, text)


# ---------------------------------------------------------------------------
# request_limits（按端口的静态入向限速）
# ---------------------------------------------------------------------------

def test_request_limits_parsed(tmp_path):
    cfg = load_from(tmp_path, minimal(
        "ports: {web: 8080}\n"
        "request_limits:\n  web: {mbps: 10, conn_rate: 150}\n"))
    assert cfg.request_limits["web"].mbps == 10.0
    assert cfg.request_limits["web"].conn_rate == 150


@pytest.mark.parametrize("text,match", [
    (minimal("ports: {w: 1}\nrequest_limits: {w: {mbps: -1}}\n"),
     r"mbps 不能为负数"),
    (minimal("ports: {w: 1}\nrequest_limits: {w: {conn_rate: -1}}\n"),
     r"conn_rate 不能为负数"),
    (minimal("ports: {w: 1}\nrequest_limits: {w: {speed: 1}}\n"),
     r"不认识的键"),
    (minimal("ports: {w: 1}\nrequest_limits: {w: [1]}\n"), r"必须是映射"),
])
def test_request_limits_rejections(tmp_path, text, match):
    with pytest.raises(ValueError, match=match):
        load_from(tmp_path, text)


# ---------------------------------------------------------------------------
# 入向动态限速（dynamic_ingress，闭环）
# ---------------------------------------------------------------------------

def test_dynamic_ingress_default_enabled(tmp_path):
    """闭环默认启用：出厂形态下它是限速的唯一执行者。"""
    g = load_from(tmp_path, minimal()).dynamic_ingress
    assert g.enabled
    assert not g.explicit


def test_dynamic_ingress_explicit_disable(tmp_path):
    g = load_from(tmp_path, minimal("dynamic_ingress:\n  enabled: false\n")
                  ).dynamic_ingress
    assert not g.enabled and g.explicit


def test_dynamic_ingress_parsed(tmp_path):
    g = load_from(tmp_path, minimal(
        "dynamic_ingress:\n"
        "  enabled: true\n  interval_s: 30\n  low_ratio: 0.8\n"
        "  high_ratio: 1.05\n  step_up: 0.1\n  step_down_max: 0.2\n"
        "  floor_ratio: 0.2\n  apply_epsilon: 0.05\n")).dynamic_ingress
    assert g.enabled and g.interval_s == 30.0
    assert g.low_ratio == 0.8 and g.high_ratio == 1.05
    assert g.step_up == 0.1 and g.step_down_max == 0.2
    assert g.floor_ratio == 0.2 and g.apply_epsilon == 0.05


def test_dynamic_ingress_unknown_key_rejected(tmp_path):
    with pytest.raises(ValueError, match="不认识的键"):
        load_from(tmp_path, minimal("dynamic_ingress: {enable: true}\n"))


def test_dynamic_ingress_bad_values_rejected(tmp_path):
    for extra, msg in (
        ("dynamic_ingress: {interval_s: 2}", "interval_s"),
        ("dynamic_ingress: {low_ratio: 1.2, high_ratio: 1.0}", "low_ratio"),
        ("dynamic_ingress: {step_up: 0.9}", "step_up"),
        ("dynamic_ingress: {step_down_max: 0.9}", "step_down_max"),
        ("dynamic_ingress: {floor_ratio: 0}", "floor_ratio"),
        ("dynamic_ingress: {apply_epsilon: 0.5}", "apply_epsilon"),
        ("dynamic_ingress: {interval_s: x}", "必须是数字"),
    ):
        with pytest.raises(ValueError, match=msg):
            load_from(tmp_path, minimal(extra + "\n"))
