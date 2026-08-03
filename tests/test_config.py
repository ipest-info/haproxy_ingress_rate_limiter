# tests.test_config —— 配置解析/默认值/校验（YAML = 接线 + 限额登记）。
#
# 负载均衡配置（监听端口、模式、后端服务器）**不在 YAML 里**——那些以
# haproxy.cfg 为唯一权威（解析见 tests/test_cfgparse.py）。这里只覆盖
# YAML 自身的三类内容：haproxy 接线段、quotas 限额登记、运行参数。
# 每条拒绝路径都用 match 断言把"错误信息要点名字段"这一契约固定下来——
# 运维应当不必翻代码就能改对。

from __future__ import annotations

import textwrap

import pytest

from rl_limiter import config, model

VALID = """\
log_level: debug
tick_interval_s: 1.0
haproxy:
  name: haproxy
  socket_path: /run/haproxy/admin.sock
  cfg_path: /etc/haproxy/haproxy.cfg
  timeout_ms: 250
quotas:
  fe_main: 40
  fe_api: 8.5
"""


def load_from(tmp_path, text=VALID):
    p = tmp_path / "config.yaml"
    p.write_text(textwrap.dedent(text), encoding="utf-8")
    return config.load(str(p))


def minimal(hap="{socket_path: /run/h.sock, cfg_path: /etc/haproxy/haproxy.cfg}",
            quotas="{fe_a: 8}", extra=""):
    return f"haproxy: {hap}\nquotas: {quotas}\n{extra}"


def test_load_full_config(tmp_path):
    cfg = load_from(tmp_path)
    assert cfg.log_level == "debug"
    assert cfg.haproxy.socket_path == "/run/haproxy/admin.sock"
    assert cfg.haproxy.cfg_path == "/etc/haproxy/haproxy.cfg"
    assert cfg.haproxy.timeout_s == 0.25          # ms → s
    assert cfg.quotas == {"fe_main": 40.0, "fe_api": 8.5}


def test_defaults_filled(tmp_path):
    cfg = load_from(tmp_path, minimal())
    assert cfg.log_level == config.DEFAULT_LOG_LEVEL
    assert cfg.tick_interval_s == config.DEFAULT_TICK_INTERVAL_S
    assert cfg.haproxy.timeout_s == config.DEFAULT_TIMEOUT_MS / 1000.0


def test_empty_quotas_is_valid(tmp_path):
    """quotas 可以整段不写：所有 frontend 只监控不限速是合法形态。"""
    cfg = load_from(tmp_path, minimal(quotas="{}"))
    assert cfg.quotas == {}


# ---------------------------------------------------------------------------
# 拒绝路径
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("text,match", [
    (minimal(quotas="{'fe bad': 8}"), r"quotas 的键"),
    (minimal(quotas="{fe_a: 0}"), r"quotas.fe_a 必须为正数"),
    (minimal(quotas="{fe_a: -1}"), r"quotas.fe_a 必须为正数"),
    (minimal(quotas="{fe_a: 0.000004}"), r"太小"),
    (minimal(quotas="{fe_a: abc}"), r"quotas.fe_a 必须是数字"),
    (minimal(quotas="[fe_a]"), r"quotas 必须是键值映射"),
    (minimal(extra="log_level: loud\n"), r"log_level 取值非法"),
])
def test_validation_rejects(tmp_path, text, match):
    with pytest.raises(ValueError, match=match):
        load_from(tmp_path, text)


def test_legacy_frontends_key_rejected_with_migration_hint(tmp_path):
    """旧形态（YAML 带 frontends 段）必须显式拒绝并指出迁移方向——
    静默忽略会让运维以为那些负载均衡配置还生效着。"""
    text = minimal() + (
        "frontends:\n  - {name: fe_a, bind_port: 8080, quota_mbps: 8}\n")
    with pytest.raises(ValueError) as ei:
        load_from(tmp_path, text)
    msg = str(ei.value)
    assert "frontends" in msg and "haproxy.cfg" in msg and "quotas" in msg


def test_tick_interval_locked_to_one_second(tmp_path):
    """速率差分与 10 秒窗口都以「1 拍 = 1 秒」为前提，改这个值会让速率
    口径与告警阈值整体失真。"""
    with pytest.raises(ValueError, match="只支持 1.0"):
        load_from(tmp_path, minimal(extra="tick_interval_s: 0.5\n"))


def test_error_message_contains_path(tmp_path):
    """所有校验错误都要带上配置文件路径，方便多配置文件部署时定位。"""
    p = tmp_path / "config.yaml"
    p.write_text("log_level: loud\nhaproxy: {socket_path: /run/h.sock, "
                 "cfg_path: /etc/haproxy/haproxy.cfg}\n", encoding="utf-8")
    with pytest.raises(ValueError, match=str(p)):
        config.load(str(p))


# ---------------------------------------------------------------------------
# HAProxy 接线段（采样通道二选一 + cfg_path 必填）
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("hap,match", [
    ("{socket_path: /run/h.sock, host: 10.0.0.1, port: 9999, "
     "cfg_path: /etc/haproxy/haproxy.cfg}", r"只能二选一"),
    ("{cfg_path: /etc/haproxy/haproxy.cfg}", r"host 不能为空"),
    ("{socket_path: run/h.sock, cfg_path: /etc/haproxy/haproxy.cfg}",
     r"必须是绝对路径"),
    ("{socket_path: /" + "x" * 120 + ", cfg_path: /etc/haproxy/haproxy.cfg}",
     r"过长"),
    ("{socket_path: /run/h.sock}", r"cfg_path 不能为空"),
    ("{socket_path: /run/h.sock, cfg_path: etc/haproxy.cfg}",
     r"cfg_path 必须是绝对路径"),
])
def test_haproxy_wiring_validation(tmp_path, hap, match):
    with pytest.raises(ValueError, match=match):
        load_from(tmp_path, minimal(hap=hap))


def test_tcp_wiring_accepted(tmp_path):
    """远程只读观测形态：填 host/port 走内网 TCP。"""
    cfg = load_from(tmp_path, minimal(
        hap="{host: 10.0.0.11, port: 9999, cfg_path: /etc/haproxy/haproxy.cfg}"))
    assert cfg.haproxy.is_unix is False
    assert cfg.haproxy.endpoint() == "10.0.0.11:9999"


def test_shipped_example_config_is_valid():
    """随仓库发布的示例配置必须能被真实加载——它是运维的起点。"""
    cfg = config.load("deploy/config/limiter.example.yaml")
    assert cfg.haproxy.cfg_path.startswith("/")
    assert cfg.quotas, "示例配置应至少演示一条限额登记"


# ---------------------------------------------------------------------------
# 单位契约：配置一律 Mbps
# ---------------------------------------------------------------------------

def test_quota_unit_is_mbps_end_to_end():
    """配置里填的 40 就是 40 Mbps，一路换算到 tc 与内部口径都不许错。

    这条钉的是全项目最贵的一类错误：8 倍（bit/byte）与 1e6 倍（Mbps/bps）
    的混淆。换算只发生在 FrontendConfig 的两个 property 里，这里把三个
    口径一次性对齐。
    """
    fe = model.FrontendConfig(name="fe_a", bind_port=8080, quota_mbps=40)
    assert fe.quota_mbps == 40
    assert fe.quota_bits_per_sec == 40_000_000        # 下发给 tc 的 rate
    assert fe.quota_bytes_per_sec == 5_000_000        # 内部与告警判定口径
    assert fe.limited


def test_quota_accepts_fractions():
    """0.5 Mbps 这种小额度是真实需求，不能因为字段是整数而被截断。"""
    fe = model.FrontendConfig(name="fe_a", bind_port=8080, quota_mbps=0.5)
    assert fe.quota_bits_per_sec == 500_000
    assert fe.quota_bytes_per_sec == 62_500


def test_zero_quota_means_monitor_only():
    """quota_mbps=0 表示"只监控不限速"：不建 tc 类、不做超限判定。"""
    fe = model.FrontendConfig(name="fe_a", bind_port=8080)
    assert fe.quota_mbps == 0.0
    assert not fe.limited


def test_quota_survives_the_dict_round_trip():
    """控制台的配置视图走 to_dict/from_dict，单位不能在这来回里漂。"""
    fe = model.FrontendConfig(name="fe_a", bind_port=8080, quota_mbps=40.5,
                              bind_address="10.0.0.1", mode="http")
    again = model.FrontendConfig.from_dict(fe.to_dict())
    assert again == fe
    assert again.quota_bits_per_sec == fe.quota_bits_per_sec
