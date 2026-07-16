# dbconfig（MySQL 配置源）纯函数部分的测试：数据库行 → 原始 dict 的组装、
# 与 YAML 管线共用的校验行为、内容校验和（版本号）、环境变量解析。
# 真实的 SQL 交互（_fetch_raw/watch）依赖 aiomysql 与活的 MySQL，由
# docker compose 演示环境做集成验证，不进单元测试。

import pytest

from rl_limiter import config, dbconfig, model

# 与 deploy/mysql/init.sql 种子数据同构的行样本。
SERVICE_ROW = ("rl-limiter-01", "enforce", "info", 1.0)
NODE_ROWS = [("hap-1", "haproxy", 9999, "/etc/haproxy/maps/bwlim.map", 500)]
ENV_ROWS = [("env-a", 80_000_000, None)]
TARGET_ROWS = [("env-a", "hap-1", "fe_env_a")]


def build(service_row=SERVICE_ROW, node_rows=NODE_ROWS,
          env_rows=ENV_ROWS, target_rows=TARGET_ROWS) -> config.ServiceConfig:
    raw = dbconfig.rows_to_raw(service_row, node_rows, env_rows, target_rows)
    return config.from_raw(raw, source="测试数据库")


def test_rows_roundtrip_to_service_config():
    """种子数据经 行组装 → 统一校验管线 得到与 YAML 加载同构的 ServiceConfig。"""
    cfg = build()
    assert cfg.node_id == "rl-limiter-01"
    assert cfg.mode == model.MODE_ENFORCE
    assert cfg.log_level == "info"
    assert cfg.tick_interval_s == 1.0
    assert len(cfg.nodes) == 1
    n = cfg.nodes[0]
    assert (n.name, n.host, n.port) == ("hap-1", "haproxy", 9999)
    assert n.bwlim_map_path == "/etc/haproxy/maps/bwlim.map"
    assert n.timeout_s == 0.5  # timeout_ms=500 → 秒口径
    assert len(cfg.envs) == 1
    e = cfg.envs[0]
    assert e.env_id == "env-a"
    assert e.quota_bits_per_sec == 80_000_000
    assert e.targets == [model.Target("hap-1", "fe_env_a")]
    assert e.params is None


def test_service_row_defaults_and_timeout_fallback():
    """mode/log_level 空值取默认；timeout_ms 非正回落默认 500ms——与
    YAML 管线的兜底行为一字不差。"""
    cfg = build(
        service_row=("node-x", None, None, None),
        node_rows=[("hap-1", "haproxy", 9999, "", 0)],
    )
    assert cfg.mode == model.MODE_DRY_RUN
    assert cfg.log_level == "info"
    assert cfg.tick_interval_s == 1.0
    assert cfg.nodes[0].timeout_s == 0.5
    assert cfg.nodes[0].bwlim_map_path == config.DEFAULT_BWLIM_MAP_PATH


def test_missing_service_row_rejected_as_missing_node_id():
    """service_config 表缺 id=1 的行 → 走统一校验的 node_id 非空规则拒绝。"""
    with pytest.raises(ValueError, match="node_id"):
        build(service_row=None)


def test_unknown_target_node_rejected_like_yaml():
    """target 引用未声明节点：数据库来源与 YAML 来源同一条拒绝规则。"""
    with pytest.raises(ValueError, match="未在 haproxy_nodes 中声明"):
        build(target_rows=[("env-a", "hap-ghost", "fe_env_a")])


def test_params_json_parsed_into_gov_params():
    cfg = build(env_rows=[
        ("env-a", 80_000_000, '{"md_factor": 0.8, "recover_after_s": 10}')])
    p = cfg.envs[0].params
    assert p is not None
    assert p.md_factor == 0.8
    assert p.recover_after_s == 10
    # 未覆盖的字段维持默认。
    assert p.elastic_ceiling == model.GovParams().elastic_ceiling


@pytest.mark.parametrize("bad_json, match", [
    ('{"md_factor": 0.8', "不是合法的 JSON"),   # 截断的 JSON
    ('[1, 2, 3]', "必须是 JSON 对象"),          # 合法 JSON 但不是对象
])
def test_bad_params_json_rejected_with_env_id(bad_json, match):
    """params_json 写坏时错误信息必须带 env_id，运维才知道改哪一行。"""
    with pytest.raises(ValueError, match=match) as ei:
        dbconfig.rows_to_raw(
            SERVICE_ROW, NODE_ROWS,
            [("env-a", 80_000_000, bad_json)], TARGET_ROWS)
    assert "env-a" in str(ei.value)


def test_checksum_stable_and_content_sensitive():
    """校验和 = 配置版本号：同内容恒定，配额/模式/参数任一变化必变。"""
    cfg = build()
    base = dbconfig.config_checksum(cfg.mode, cfg.envs)
    assert base == dbconfig.config_checksum(cfg.mode, cfg.envs)  # 稳定

    quota_changed = build(env_rows=[("env-a", 40_000_000, None)])
    assert dbconfig.config_checksum(
        quota_changed.mode, quota_changed.envs) != base

    assert dbconfig.config_checksum(model.MODE_DRY_RUN, cfg.envs) != base

    params_changed = build(env_rows=[("env-a", 80_000_000, '{"md_factor": 0.8}')])
    assert dbconfig.config_checksum(
        params_changed.mode, params_changed.envs) != base


def test_from_env_disabled_without_host():
    assert dbconfig.from_env({}) is None
    assert dbconfig.from_env({"RL_MYSQL_HOST": "  "}) is None


def test_from_env_defaults_and_overrides():
    opts = dbconfig.from_env({"RL_MYSQL_HOST": "mysql"})
    assert opts is not None
    assert (opts.host, opts.port, opts.user, opts.database) == (
        "mysql", 3306, "rl", "rl_limiter")
    assert opts.poll_interval_s == 5.0

    opts = dbconfig.from_env({
        "RL_MYSQL_HOST": "db.internal",
        "RL_MYSQL_PORT": "3307",
        "RL_MYSQL_USER": "svc",
        "RL_MYSQL_PASSWORD": "secret",
        "RL_MYSQL_DB": "cfg",
        "RL_MYSQL_POLL_S": "2.5",
    })
    assert (opts.host, opts.port, opts.user, opts.password, opts.database) == (
        "db.internal", 3307, "svc", "secret", "cfg")
    assert opts.poll_interval_s == 2.5


@pytest.mark.parametrize("env, match", [
    ({"RL_MYSQL_HOST": "m", "RL_MYSQL_PORT": "abc"}, "RL_MYSQL_PORT"),
    ({"RL_MYSQL_HOST": "m", "RL_MYSQL_PORT": "0"}, "1-65535"),
    ({"RL_MYSQL_HOST": "m", "RL_MYSQL_POLL_S": "x"}, "RL_MYSQL_POLL_S"),
    ({"RL_MYSQL_HOST": "m", "RL_MYSQL_POLL_S": "-1"}, "RL_MYSQL_POLL_S"),
    # NaN 与任何数比较都是 False、inf > 0：单纯的 <=0 守卫拦不住它们，
    # 而 asyncio.sleep(nan/inf) 永不返回会让轮询任务静默挂死。
    ({"RL_MYSQL_HOST": "m", "RL_MYSQL_POLL_S": "nan"}, "RL_MYSQL_POLL_S"),
    ({"RL_MYSQL_HOST": "m", "RL_MYSQL_POLL_S": "inf"}, "RL_MYSQL_POLL_S"),
])
def test_from_env_rejects_bad_values(env, match):
    """host 已设置但数值写错：半吊子的数据库配置必须在启动时拦下。"""
    with pytest.raises(ValueError, match=match):
        dbconfig.from_env(env)


def test_canonical_config_is_exact_identity():
    """变更检测的身份是规范化 JSON 字符串本身（精确比较，不经哈希），
    同内容恒等、任一字段变化必不等。"""
    cfg = build()
    base = dbconfig.canonical_config(cfg.mode, cfg.envs)
    assert base == dbconfig.canonical_config(cfg.mode, cfg.envs)
    changed = build(env_rows=[("env-a", 40_000_000, None)])
    assert dbconfig.canonical_config(changed.mode, changed.envs) != base
