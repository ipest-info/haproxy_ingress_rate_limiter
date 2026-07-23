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
log_level: debug
tick_interval_s: 0.5
haproxy_nodes:
  - name: lb-1
    host: 10.0.0.1
    port: 9999
    timeout_ms: 250
    quota_bps: 200000000
  - name: lb-2
    host: 10.0.0.2
    port: 9999
    quota_bps: 150000000
  - name: lb-3
    host: 10.0.0.3
    port: 9999
    quota_bps: 100000000
envs:
  - env_id: env-a
    targets:
      - {node: lb-1, frontend: fe_a}
      - {node: lb-2, frontend: fe_a}
  - env_id: env-b
    targets:
      - {node: lb-3, frontend: fe_b}
"""


def test_load_full_config(tmp_path):
    cfg = load_from(tmp_path, VALID_YAML)
    assert cfg.log_level == "debug"
    assert cfg.tick_interval_s == 0.5

    assert [n.name for n in cfg.nodes] == ["lb-1", "lb-2", "lb-3"]
    n1, n2, _n3 = cfg.nodes
    assert n1.host == "10.0.0.1"
    assert n1.port == 9999
    # timeout_ms（毫秒，运维口径）在加载时一次性换算为内部口径的秒。
    assert n1.timeout_s == pytest.approx(0.25)
    # lb-2 未写 timeout_ms → 取默认。
    assert n2.timeout_s == pytest.approx(config.DEFAULT_TIMEOUT_MS / 1000.0)

    # cfg.envs 是 per-node 监控单元（env_id=节点名，quota=节点限额），
    # 业务环境只保留在 env_groups（分组 → 成员节点）里。
    assert [u.env_id for u in cfg.envs] == ["lb-1", "lb-2", "lb-3"]
    assert cfg.env_groups == {"env-a": ["lb-1", "lb-2"], "env-b": ["lb-3"]}
    u1 = cfg.envs[0]
    assert u1.quota_bits_per_sec == 200_000_000
    # 限额单位是 bits/s，quota_bytes_per_sec 是唯一的换算边界（÷8）。
    assert u1.quota_bytes_per_sec == pytest.approx(25_000_000.0)
    assert u1.targets == [model.Target("lb-1", "fe_a")]
    assert cfg.node_quotas == {
        "lb-1": 200_000_000, "lb-2": 150_000_000, "lb-3": 100_000_000}


def test_load_minimal_config_defaults(tmp_path):
    """空配置时全部字段取安全默认：info 级日志、1s tick、空节点/环境。"""
    cfg = load_from(tmp_path, "{}\n")
    assert cfg.log_level == "info"
    assert cfg.tick_interval_s == 1.0
    assert cfg.nodes == []
    assert cfg.envs == []


def test_load_missing_file():
    with pytest.raises(FileNotFoundError):
        config.load("/nonexistent/rl-limiter/config.yaml")


def test_load_invalid_yaml(tmp_path):
    with pytest.raises(ValueError, match="解析失败"):
        load_from(tmp_path, "mode: [unclosed\n")


def test_load_non_mapping_root(tmp_path):
    with pytest.raises(ValueError, match="顶层必须是键值映射"):
        load_from(tmp_path, "- just\n- a\n- list\n")


# ---- 校验分支：每条一个最小化的坏配置，match 锁定错误信息点名的字段 ----

# 大部分坏配置在这个骨架上改动：一个节点 + 一个引用它的环境。
BASE = """\
haproxy_nodes:
  - {name: lb-1, host: 10.0.0.1, port: 9999, quota_bps: 200000000}
envs:
  - env_id: env-a
    targets:
      - {node: lb-1, frontend: fe_a}
"""


@pytest.mark.parametrize(
    ("yaml_text", "match"),
    [
        # 旧形态字段（服务级 mode）明确拒绝并给迁移指引，不静默忽略。
        pytest.param(
            "mode: dry-run\n",
            "'mode' 已废弃.*shared bwlim",
            id="mode-legacy-rejected",
        ),
        pytest.param(
            BASE.replace("port: 9999,", "port: 9999, mode: enforce,"),
            r"haproxy_nodes\[0\].*'mode' 已废弃",
            id="node-mode-legacy-rejected",
        ),
        # log_level 枚举。
        pytest.param(
            "log_level: verbose\n",
            "log_level 值非法.*verbose",
            id="log-level-invalid",
        ),
        # tick_interval_s 必须 > 0（显式写 0/负数是配置错误，不回落默认）。
        pytest.param(
            "tick_interval_s: 0\n",
            "tick_interval_s 必须 > 0",
            id="tick-zero",
        ),
        pytest.param(
            "tick_interval_s: -1.5\n",
            "tick_interval_s 必须 > 0",
            id="tick-negative",
        ),
        # 节点：name 非空且唯一，host 非空，port 1-65535。
        pytest.param(
            "haproxy_nodes:\n"
            "  - {host: 10.0.0.1, port: 9999}\n",
            r"haproxy_nodes\[0\].*name 不能为空",
            id="node-name-empty",
        ),
        pytest.param(
            "haproxy_nodes:\n"
            "  - {name: lb-1, host: 10.0.0.1, port: 9999}\n"
            "  - {name: lb-1, host: 10.0.0.2, port: 9999}\n",
            r"haproxy_nodes\[1\].*'lb-1'.*重复",
            id="node-name-duplicate",
        ),
        pytest.param(
            "haproxy_nodes:\n  - {name: lb-1, port: 9999}\n",
            r"haproxy_nodes\[0\].*host 不能为空",
            id="node-host-empty",
        ),
        pytest.param(
            "haproxy_nodes:\n"
            "  - {name: lb-1, host: 10.0.0.1}\n",
            r"port 必须在 1-65535",
            id="node-port-missing",
        ),
        pytest.param(
            "haproxy_nodes:\n"
            "  - {name: lb-1, host: 10.0.0.1, port: 0}\n",
            r"port 必须在 1-65535.*0",
            id="node-port-zero",
        ),
        pytest.param(
            "haproxy_nodes:\n"
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
            "    targets:\n"
            "      - {node: lb-1, frontend: fe_b}\n",
            r"envs\[1\].*'env-a'.*重复",
            id="env-id-duplicate",
        ),
        # v2.1：配额设在节点上，被挂载的节点必须有有效 quota_bps。
        pytest.param(
            BASE.replace(", quota_bps: 200000000", ""),
            r"'lb-1' 已被挂载但未设置有效的 quota_bps",
            id="node-quota-missing",
        ),
        pytest.param(
            BASE.replace("quota_bps: 200000000", "quota_bps: 0"),
            r"'lb-1' 已被挂载但未设置有效的 quota_bps",
            id="node-quota-zero",
        ),
        pytest.param(
            BASE.replace("quota_bps: 200000000", "quota_bps: -5"),
            r"'lb-1' 已被挂载但未设置有效的 quota_bps",
            id="node-quota-negative",
        ),
        # 老形态（环境带配额）明确拒绝并给迁移指引，不静默忽略。
        pytest.param(
            BASE.replace("targets:", "quota_bps: 200000000\n    targets:"),
            r"quota_bps.*已废弃.*节点",
            id="env-quota-legacy-rejected",
        ),
        # targets 非空。
        pytest.param(
            "haproxy_nodes:\n"
            "  - {name: lb-1, host: 10.0.0.1, port: 9999, quota_bps: 200000000}\n"
            "envs:\n  - {env_id: env-a}\n",
            r"env-a.*至少需要一个 target",
            id="targets-missing",
        ),
        pytest.param(
            "haproxy_nodes:\n"
            "  - {name: lb-1, host: 10.0.0.1, port: 9999, quota_bps: 200000000}\n"
            "envs:\n"
            "  - {env_id: env-a, targets: []}\n",
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
    p.write_text("mode: observe\n", encoding="utf-8")
    with pytest.raises(ValueError, match=str(p)):
        config.load(str(p))


def test_timeout_ms_nonpositive_falls_back_to_default(tmp_path):
    """timeout_ms 显式写 0/负数按"缺失"处理回落默认（对非正超时兜底，
    避免 0/负超时穿透到网络层）。"""
    cfg = load_from(
        tmp_path,
        "haproxy_nodes:\n"
        "  - {name: lb-1, host: 10.0.0.1, port: 9999, timeout_ms: -100}\n",
    )
    assert cfg.nodes[0].timeout_s == pytest.approx(
        config.DEFAULT_TIMEOUT_MS / 1000.0
    )


def test_node_exclusive_to_single_env(tmp_path):
    """节点是环境的独占资源：一个环境可横跨多台 HAProxy，但一台 HAProxy
    只允许服务一个环境（不同 frontend 也不行）。"""
    yaml_text = (
        "haproxy_nodes:\n"
        "  - {name: lb-1, host: 10.0.0.1, port: 9999, quota_bps: 100000000}\n"
        "envs:\n"
        "  - env_id: env-a\n"
        "    targets:\n"
        "      - {node: lb-1, frontend: fe_a}\n"
        "  - env_id: env-b\n"
        "    targets:\n"
        "      - {node: lb-1, frontend: fe_b}\n"
    )
    with pytest.raises(ValueError, match="只允许服务一个环境"):
        load_from(tmp_path, yaml_text)
