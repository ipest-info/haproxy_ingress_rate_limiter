# tests.test_config —— rl_limiter.config 的加载/默认值/校验测试。
#
# 覆盖面：
#   - 全量字段的正常解析（含 timeout_ms → timeout_s 的单位换算）；
#   - 最小配置下的全部默认值；
#   - 每一条校验分支的拒绝路径（错误信息必须点名问题字段，让运维可以
#     直接按报错修配置，这里用 match 断言把这一契约固定下来）。

from __future__ import annotations

import textwrap

import pytest

from rl_limiter import config, model


def load_from(tmp_path, text: str) -> config.ServiceConfig:
    """把 YAML 文本写入临时文件后走真实的 load 路径（读文件 + safe_load）。"""
    p = tmp_path / "config.yaml"
    p.write_text(textwrap.dedent(text), encoding="utf-8")
    return config.load(str(p))


# 后续错误用例在这份合法配置的基础上做最小改动，保证报错确实来自
# 被改动的字段而不是别处。
VALID_YAML = """\
node_id: svc-1
mode: enforce
log_level: debug
tick_interval_s: 0.5
haproxy_nodes:
  - name: lb-1
    host: 10.0.0.1
    port: 9999
    bwlim_map_path: /etc/haproxy/maps/custom.map
    timeout_ms: 250
  - name: lb-2
    host: 10.0.0.2
    port: 9999
envs:
  - env_id: env-a
    quota_bps: 200000000
    targets:
      - {node: lb-1, frontend: fe_a}
      - {node: lb-2, frontend: fe_a}
    params:
      md_factor: 0.8
  - env_id: env-b
    quota_bps: 100000000
    targets:
      - {node: lb-1, frontend: fe_b}
backend:
  base_url: https://backend:9090/
  cache_path: /tmp/cache.json
  tls:
    ca_file: /x/ca.pem
    cert_file: /x/cert.pem
    key_file: /x/key.pem
"""


def test_load_full_config(tmp_path):
    cfg = load_from(tmp_path, VALID_YAML)
    assert cfg.node_id == "svc-1"
    assert cfg.mode == model.MODE_ENFORCE
    assert cfg.log_level == "debug"
    assert cfg.tick_interval_s == 0.5

    assert [n.name for n in cfg.nodes] == ["lb-1", "lb-2"]
    n1, n2 = cfg.nodes
    assert n1.host == "10.0.0.1"
    assert n1.port == 9999
    assert n1.bwlim_map_path == "/etc/haproxy/maps/custom.map"
    # timeout_ms（毫秒，运维口径）在加载时一次性换算为内部口径的秒。
    assert n1.timeout_s == pytest.approx(0.25)
    # lb-2 未写 bwlim_map_path/timeout_ms → 各自取默认。
    assert n2.bwlim_map_path == config.DEFAULT_BWLIM_MAP_PATH
    assert n2.timeout_s == pytest.approx(config.DEFAULT_TIMEOUT_MS / 1000.0)

    assert [e.env_id for e in cfg.envs] == ["env-a", "env-b"]
    ea = cfg.envs[0]
    assert ea.quota_bits_per_sec == 200_000_000
    # 配额单位是 bits/s，quota_bytes_per_sec 是唯一的换算边界（÷8）。
    assert ea.quota_bytes_per_sec == pytest.approx(25_000_000.0)
    assert ea.targets == [
        model.Target("lb-1", "fe_a"),
        model.Target("lb-2", "fe_a"),
    ]
    assert ea.params is not None and ea.params.md_factor == pytest.approx(0.8)
    # params 局部覆盖经 normalize 补齐其余字段（零值回填默认）。
    assert ea.params.elastic_ceiling == pytest.approx(1.10)
    assert cfg.envs[1].params is None

    assert cfg.backend.base_url == "https://backend:9090/"
    assert cfg.backend.cache_path == "/tmp/cache.json"
    assert cfg.backend.ca_file == "/x/ca.pem"
    assert cfg.backend.cert_file == "/x/cert.pem"
    assert cfg.backend.key_file == "/x/key.pem"


def test_load_minimal_config_defaults(tmp_path):
    """只写 node_id 时其余字段全部取安全默认：dry-run（误部署不产生数据
    面影响）、info 级日志、1s tick、空节点/环境、standalone 后台。"""
    cfg = load_from(tmp_path, "node_id: svc-1\n")
    assert cfg.node_id == "svc-1"
    assert cfg.mode == model.MODE_DRY_RUN
    assert cfg.log_level == "info"
    assert cfg.tick_interval_s == 1.0
    assert cfg.nodes == []
    assert cfg.envs == []
    assert cfg.backend.base_url == ""  # 空 = standalone
    assert cfg.backend.cache_path == config.DEFAULT_CACHE_PATH
    assert cfg.backend.ca_file == ""
    assert cfg.backend.cert_file == ""
    assert cfg.backend.key_file == ""


def test_load_missing_file():
    with pytest.raises(FileNotFoundError):
        config.load("/nonexistent/rl-limiter/config.yaml")


def test_load_invalid_yaml(tmp_path):
    with pytest.raises(ValueError, match="解析失败"):
        load_from(tmp_path, "node_id: [unclosed\n")


def test_load_non_mapping_root(tmp_path):
    with pytest.raises(ValueError, match="顶层必须是键值映射"):
        load_from(tmp_path, "- just\n- a\n- list\n")


# ---- 校验分支：每条一个最小化的坏配置，match 锁定错误信息点名的字段 ----

# 大部分坏配置在这个骨架上改动：一个节点 + 一个引用它的环境。
BASE = """\
node_id: svc-1
haproxy_nodes:
  - {name: lb-1, host: 10.0.0.1, port: 9999}
envs:
  - env_id: env-a
    quota_bps: 200000000
    targets:
      - {node: lb-1, frontend: fe_a}
"""


@pytest.mark.parametrize(
    ("yaml_text", "match"),
    [
        # node_id 非空：缺失与显式空串都要拒绝。
        pytest.param("mode: dry-run\n", "node_id 不能为空", id="node-id-missing"),
        pytest.param('node_id: ""\n', "node_id 不能为空", id="node-id-empty"),
        # mode 只能是 dry-run / enforce，不做静默回落。
        pytest.param(
            "node_id: svc-1\nmode: observe\n",
            "mode 值非法.*observe",
            id="mode-invalid",
        ),
        # log_level 枚举。
        pytest.param(
            "node_id: svc-1\nlog_level: verbose\n",
            "log_level 值非法.*verbose",
            id="log-level-invalid",
        ),
        # tick_interval_s 必须 > 0（显式写 0/负数是配置错误，不回落默认）。
        pytest.param(
            "node_id: svc-1\ntick_interval_s: 0\n",
            "tick_interval_s 必须 > 0",
            id="tick-zero",
        ),
        pytest.param(
            "node_id: svc-1\ntick_interval_s: -1.5\n",
            "tick_interval_s 必须 > 0",
            id="tick-negative",
        ),
        # 节点：name 非空且唯一，host 非空，port 1-65535。
        pytest.param(
            "node_id: svc-1\nhaproxy_nodes:\n"
            "  - {host: 10.0.0.1, port: 9999}\n",
            r"haproxy_nodes\[0\].*name 不能为空",
            id="node-name-empty",
        ),
        pytest.param(
            "node_id: svc-1\nhaproxy_nodes:\n"
            "  - {name: lb-1, host: 10.0.0.1, port: 9999}\n"
            "  - {name: lb-1, host: 10.0.0.2, port: 9999}\n",
            r"haproxy_nodes\[1\].*'lb-1'.*重复",
            id="node-name-duplicate",
        ),
        pytest.param(
            "node_id: svc-1\nhaproxy_nodes:\n  - {name: lb-1, port: 9999}\n",
            r"haproxy_nodes\[0\].*host 不能为空",
            id="node-host-empty",
        ),
        pytest.param(
            "node_id: svc-1\nhaproxy_nodes:\n"
            "  - {name: lb-1, host: 10.0.0.1}\n",
            r"port 必须在 1-65535",
            id="node-port-missing",
        ),
        pytest.param(
            "node_id: svc-1\nhaproxy_nodes:\n"
            "  - {name: lb-1, host: 10.0.0.1, port: 0}\n",
            r"port 必须在 1-65535.*0",
            id="node-port-zero",
        ),
        pytest.param(
            "node_id: svc-1\nhaproxy_nodes:\n"
            "  - {name: lb-1, host: 10.0.0.1, port: 70000}\n",
            r"port 必须在 1-65535.*70000",
            id="node-port-too-big",
        ),
        # 环境：env_id 非空且唯一。
        pytest.param(
            BASE.replace("env_id: env-a", 'env_id: ""'),
            r"envs\[0\].*env_id 不能为空",
            id="env-id-empty",
        ),
        pytest.param(
            BASE
            + "  - env_id: env-a\n"
            "    quota_bps: 100000000\n"
            "    targets:\n"
            "      - {node: lb-1, frontend: fe_b}\n",
            r"envs\[1\].*'env-a'.*重复",
            id="env-id-duplicate",
        ),
        # 配额必须为正（0 与负数都会把环境限死或让计算失去基准）。
        pytest.param(
            BASE.replace("quota_bps: 200000000", "quota_bps: 0"),
            r"env-a.*quota_bps 必须 > 0",
            id="quota-zero",
        ),
        pytest.param(
            BASE.replace("quota_bps: 200000000", "quota_bps: -5"),
            r"env-a.*quota_bps 必须 > 0.*-5",
            id="quota-negative",
        ),
        # targets 非空。
        pytest.param(
            "node_id: svc-1\nhaproxy_nodes:\n"
            "  - {name: lb-1, host: 10.0.0.1, port: 9999}\n"
            "envs:\n  - {env_id: env-a, quota_bps: 200000000}\n",
            r"env-a.*至少需要一个 target",
            id="targets-missing",
        ),
        pytest.param(
            "node_id: svc-1\nhaproxy_nodes:\n"
            "  - {name: lb-1, host: 10.0.0.1, port: 9999}\n"
            "envs:\n"
            "  - {env_id: env-a, quota_bps: 200000000, targets: []}\n",
            r"env-a.*至少需要一个 target",
            id="targets-empty",
        ),
        # target.frontend 非空。
        pytest.param(
            BASE.replace("frontend: fe_a", 'frontend: ""'),
            r"env-a.*frontend 不能为空",
            id="target-frontend-empty",
        ),
        # target.node 必须已在 haproxy_nodes 中声明。
        pytest.param(
            BASE.replace("node: lb-1, frontend: fe_a", "node: lb-9, frontend: fe_a"),
            r"env-a.*'lb-9' 未在 haproxy_nodes 中声明",
            id="target-node-undeclared",
        ),
        # 同一 Target 不得映射到两个环境（用量重复计入、限速值互相覆盖）。
        pytest.param(
            BASE
            + "  - env_id: env-b\n"
            "    quota_bps: 100000000\n"
            "    targets:\n"
            "      - {node: lb-1, frontend: fe_a}\n",
            r"lb-1/fe_a 同时映射到环境 'env-a' 与 'env-b'",
            id="target-duplicate-across-envs",
        ),
        # 同一环境内重复书写同一 Target 同样拒绝。
        pytest.param(
            BASE + "      - {node: lb-1, frontend: fe_a}\n",
            r"lb-1/fe_a 同时映射到环境 'env-a' 与 'env-a'",
            id="target-duplicate-same-env",
        ),
        # target 结构残缺（缺 frontend 键）→ 报环境下标与结构提示。
        pytest.param(
            BASE.replace("- {node: lb-1, frontend: fe_a}", "- {node: lb-1}"),
            r"envs\[0\].*结构错误",
            id="target-malformed",
        ),
    ],
)
def test_validation_errors(tmp_path, yaml_text, match):
    with pytest.raises(ValueError, match=match):
        load_from(tmp_path, yaml_text)


def test_error_message_contains_path(tmp_path):
    """所有校验错误都要带上配置文件路径，方便多配置文件部署时定位。"""
    p = tmp_path / "config.yaml"
    p.write_text("mode: dry-run\n", encoding="utf-8")
    with pytest.raises(ValueError, match=str(p)):
        config.load(str(p))


def test_timeout_ms_nonpositive_falls_back_to_default(tmp_path):
    """timeout_ms 显式写 0/负数按"缺失"处理回落默认（对非正超时兜底，
    避免 0/负超时穿透到网络层）。"""
    cfg = load_from(
        tmp_path,
        "node_id: svc-1\nhaproxy_nodes:\n"
        "  - {name: lb-1, host: 10.0.0.1, port: 9999, timeout_ms: -100}\n",
    )
    assert cfg.nodes[0].timeout_s == pytest.approx(
        config.DEFAULT_TIMEOUT_MS / 1000.0
    )
