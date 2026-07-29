# tests.test_config —— 配置解析/默认值/校验（单 HAProxy 模型）。
#
# 校验的严格程度是刻意的：这些值会被渲染进 haproxy.cfg 并 reload，
# 一个坏值就能让整台机器的入口挂掉。每条拒绝路径都用 match 断言把
# "错误信息要点名字段"这一契约固定下来——运维应当不必翻代码就能改对。

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
  timeout_ms: 250
frontends:
  - name: fe_main
    bind_port: 8080
    quota_bps: 40000000
    maxconn: 2000
    servers:
      - {name: web1, address: 10.0.0.21, port: 9000}
      - {name: web2, address: 10.0.0.22, port: 9000, weight: 50, check: false}
  - name: fe_api
    bind_address: 127.0.0.1
    bind_port: 8081
    mode: http
    quota_bps: 8000000
    balance: leastconn
    servers:
      - {name: api1, address: 10.0.0.31, port: 8000}
"""


def load_from(tmp_path, text=VALID):
    p = tmp_path / "config.yaml"
    p.write_text(textwrap.dedent(text), encoding="utf-8")
    return config.load(str(p))


def one_fe(**over):
    """构造只含一个 frontend 的最小 YAML，便于逐字段做拒绝路径测试。"""
    f = {"name": "fe_a", "bind_port": 8080, "quota_bps": 8000000}
    f.update(over)
    body = "\n".join(f"    {k}: {v!r}" for k, v in f.items() if k != "servers")
    srv = over.get("servers", "\n      - {name: s1, address: 1.2.3.4, port: 80}")
    return ("haproxy:\n  socket_path: /run/haproxy/admin.sock\n"
            "frontends:\n  -\n" + body + "\n    servers:" + srv + "\n")


def test_load_full_config(tmp_path):
    cfg = load_from(tmp_path)
    assert cfg.log_level == "debug"
    assert cfg.haproxy.socket_path == "/run/haproxy/admin.sock"
    assert cfg.haproxy.timeout_s == 0.25          # ms → s
    assert [f.name for f in cfg.frontends] == ["fe_main", "fe_api"]

    main = cfg.frontends[0]
    assert main.bind_spec == ":8080" and main.mode == "tcp"
    assert main.quota_bytes_per_sec == 5_000_000  # 40 Mbps ÷ 8
    assert main.maxconn == 2000
    assert [(s.name, s.address, s.port, s.weight, s.check) for s in main.servers] == [
        ("web1", "10.0.0.21", 9000, 100, True),
        ("web2", "10.0.0.22", 9000, 50, False),
    ]
    api = cfg.frontends[1]
    assert api.bind_spec == "127.0.0.1:8081" and api.mode == "http"
    assert api.balance == "leastconn"


def test_defaults_filled(tmp_path):
    cfg = load_from(tmp_path, one_fe())
    f = cfg.frontends[0]
    assert (f.mode, f.balance, f.maxconn) == ("tcp", "roundrobin", 0)
    assert (f.timeout_connect_ms, f.timeout_client_ms, f.timeout_server_ms) == \
        (5000, 50000, 50000)
    assert cfg.log_level == config.DEFAULT_LOG_LEVEL
    assert cfg.haproxy.timeout_s == config.DEFAULT_TIMEOUT_MS / 1000.0


def test_controller_config_roundtrip(tmp_path):
    cfg = load_from(tmp_path)
    ctl = cfg.to_controller_config(version=7)
    assert ctl.version == 7
    assert ctl.names() == {"fe_main", "fe_api"}
    assert ctl.quotas()["fe_main"] == 5_000_000
    # to_dict/from_dict 往返不丢字段（控制台写接口依赖它）。
    back = model.ControllerConfig.from_dict(ctl.to_dict())
    assert back.to_dict() == ctl.to_dict()


# ---------------------------------------------------------------------------
# 拒绝路径
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("text,match", [
    ("haproxy: {socket_path: /run/h.sock}\nfrontends: []\n",
     r"frontends 不能为空"),
    (one_fe(name="fe bad"), r"name 非法"),
    (one_fe(bind_port=0), r"bind_port 必须在 1-65535"),
    (one_fe(quota_bps=0), r"quota_bps 必须为正数"),
    (one_fe(quota_bps=4), r"太小"),
    (one_fe(mode="udp"), r"mode 取值非法"),
    (one_fe(balance="magic"), r"balance 取值非法"),
    (one_fe(maxconn=-1), r"maxconn 不能为负"),
    (one_fe(timeout_client_ms=0), r"timeout_client_ms 必须为正数"),
    (one_fe(servers=" []"), r"servers 不能为空"),
    (one_fe(servers="\n      - {name: s1, address: 'a b', port: 80}"),
     r"address 非法"),
    (one_fe(servers="\n      - {name: s1, address: 1.2.3.4, port: 70000}"),
     r"port 必须在 1-65535"),
    (one_fe(servers="\n      - {name: s1, address: 1.2.3.4, port: 80, weight: 999}"),
     r"weight 必须在 0-256"),
])
def test_validation_rejects(tmp_path, text, match):
    with pytest.raises(ValueError, match=match):
        load_from(tmp_path, text)


def test_duplicate_frontend_name_rejected(tmp_path):
    """重名会让采样数据张冠李戴（名字要与 stats 的 pxname 一一对应）。"""
    text = ("haproxy: {socket_path: /run/h.sock}\nfrontends:\n"
            "  - {name: fe_a, bind_port: 1, quota_bps: 8000, servers: [{name: s, address: 1.1.1.1, port: 1}]}\n"
            "  - {name: fe_a, bind_port: 2, quota_bps: 8000, servers: [{name: s, address: 1.1.1.1, port: 1}]}\n")
    with pytest.raises(ValueError, match="重复"):
        load_from(tmp_path, text)


def test_duplicate_bind_port_rejected(tmp_path):
    """两个 frontend 绑同一端口会让 HAProxy 起不来——启动时就拦下。"""
    text = ("haproxy: {socket_path: /run/h.sock}\nfrontends:\n"
            "  - {name: fe_a, bind_port: 8080, quota_bps: 8000, servers: [{name: s, address: 1.1.1.1, port: 1}]}\n"
            "  - {name: fe_b, bind_port: 8080, quota_bps: 8000, servers: [{name: s, address: 1.1.1.1, port: 1}]}\n")
    with pytest.raises(ValueError, match="冲突"):
        load_from(tmp_path, text)


def test_duplicate_server_name_within_frontend_rejected(tmp_path):
    text = one_fe(servers="\n      - {name: s1, address: 1.1.1.1, port: 1}"
                          "\n      - {name: s1, address: 2.2.2.2, port: 2}")
    with pytest.raises(ValueError, match="重复"):
        load_from(tmp_path, text)


def test_tick_interval_locked_to_one_second(tmp_path):
    """速率差分与 10 秒窗口都以「1 拍 = 1 秒」为前提，改这个值会让速率
    口径与告警阈值整体失真。"""
    with pytest.raises(ValueError, match="只支持 1.0"):
        load_from(tmp_path, "tick_interval_s: 0.5\n" + one_fe())


def test_error_message_contains_path(tmp_path):
    """所有校验错误都要带上配置文件路径，方便多配置文件部署时定位。"""
    p = tmp_path / "config.yaml"
    p.write_text("frontends: []\n", encoding="utf-8")
    with pytest.raises(ValueError, match=str(p)):
        config.load(str(p))


# ---------------------------------------------------------------------------
# HAProxy 接线段（采样通道二选一）
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("hap,match", [
    ("{socket_path: /run/h.sock, host: 10.0.0.1, port: 9999}", r"只能二选一"),
    ("{}", r"host 不能为空"),
    ("{socket_path: run/h.sock}", r"必须是绝对路径"),
    ("{socket_path: /" + "x" * 120 + "}", r"过长"),
])
def test_haproxy_wiring_validation(tmp_path, hap, match):
    text = ("haproxy: " + hap + "\nfrontends:\n"
            "  - {name: fe_a, bind_port: 1, quota_bps: 8000, servers: [{name: s, address: 1.1.1.1, port: 1}]}\n")
    with pytest.raises(ValueError, match=match):
        load_from(tmp_path, text)


def test_tcp_wiring_accepted(tmp_path):
    """远程只读观测形态：填 host/port 走内网 TCP。"""
    text = ("haproxy: {host: 10.0.0.11, port: 9999}\nfrontends:\n"
            "  - {name: fe_a, bind_port: 1, quota_bps: 8000, servers: [{name: s, address: 1.1.1.1, port: 1}]}\n")
    cfg = load_from(tmp_path, text)
    assert cfg.haproxy.is_unix is False
    assert cfg.haproxy.endpoint() == "10.0.0.11:9999"


def test_shipped_example_config_is_valid():
    """随仓库发布的示例配置必须能被真实加载——它是运维的起点。"""
    cfg = config.load("deploy/config/limiter.example.yaml")
    assert [f.name for f in cfg.frontends] == ["fe_main", "fe_api"]
