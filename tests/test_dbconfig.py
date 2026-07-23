# dbconfig（MySQL 配置源）纯函数部分的测试：数据库行 → 原始 dict 的组装、
# 与 YAML 管线共用的校验行为、内容校验和（版本号）、环境变量解析。
# 真实的 SQL 交互（_fetch_raw/watch）依赖 aiomysql 与活的 MySQL，由
# docker compose 演示环境做集成验证，不进单元测试。

import pytest

from rl_limiter import config, dbconfig, model

# 与 deploy/mysql/init.sql 种子数据同构的行样本。节点行末列 quota_bps 为
# 唯一可热更运行列（登记限额=超限告警基准）；环境行只剩分组标识。
SERVICE_ROW = ("info", 1.0)
NODE_ROWS = [("hap-1", "haproxy1", 9999, 500, 80_000_000)]
ENV_ROWS = [("env-a",)]
TARGET_ROWS = [("env-a", "hap-1", "fe_env_a")]


def build(service_row=SERVICE_ROW, node_rows=NODE_ROWS,
          env_rows=ENV_ROWS, target_rows=TARGET_ROWS) -> config.ServiceConfig:
    raw = dbconfig.rows_to_raw(service_row, node_rows, env_rows, target_rows)
    return config.from_raw(raw, source="测试数据库")


def test_rows_roundtrip_to_service_config():
    """种子数据经 行组装 → 统一校验管线 得到与 YAML 加载同构的 ServiceConfig。"""
    cfg = build()
    assert cfg.log_level == "info"
    assert cfg.tick_interval_s == 1.0
    assert len(cfg.nodes) == 1
    n = cfg.nodes[0]
    assert (n.name, n.host, n.port) == ("hap-1", "haproxy1", 9999)
    assert n.timeout_s == 0.5  # timeout_ms=500 → 秒口径
    # 监控单元 = 节点（env_id 字段装节点名，quota 来自节点行）。
    assert len(cfg.envs) == 1
    u = cfg.envs[0]
    assert u.env_id == "hap-1"
    assert u.quota_bits_per_sec == 80_000_000
    assert u.targets == [model.Target("hap-1", "fe_env_a")]
    assert cfg.env_groups == {"env-a": ["hap-1"]}


def test_service_row_defaults_and_timeout_fallback():
    """log_level 空值取默认；timeout_ms 非正回落默认 500ms——与 YAML
    管线的兜底行为一字不差。"""
    cfg = build(
        service_row=(None, None),
        node_rows=[("hap-1", "haproxy", 9999, 0, 80_000_000)],
    )
    assert cfg.log_level == "info"
    assert cfg.tick_interval_s == 1.0
    assert cfg.nodes[0].timeout_s == 0.5


def test_missing_service_row_falls_back_to_safe_defaults():
    """service_config 表缺 id=1 的行 → 服务级键全部取安全默认。"""
    cfg = build(service_row=None)
    assert cfg.log_level == "info"
    assert cfg.tick_interval_s == 1.0


def test_unknown_target_node_rejected_like_yaml():
    """target 引用未声明节点：数据库来源与 YAML 来源同一条拒绝规则。"""
    with pytest.raises(ValueError, match="未在 haproxy_nodes 中声明"):
        build(target_rows=[("env-a", "hap-ghost", "fe_env_a")])


def test_mounted_node_without_quota_rejected():
    """被挂载的节点必须登记限额（它是超限告警的基准）。"""
    with pytest.raises(ValueError, match="未设置有效的 quota_bps"):
        build(node_rows=[("hap-1", "haproxy1", 9999, 500, None)])


def test_checksum_stable_and_content_sensitive():
    """校验和 = 配置版本号：同内容恒定，限额变化必变。"""
    cfg = build()
    base = dbconfig.config_checksum(cfg.envs)
    assert base == dbconfig.config_checksum(cfg.envs)  # 稳定

    quota_changed = build(node_rows=[
        ("hap-1", "haproxy1", 9999, 500, 40_000_000)])
    assert dbconfig.config_checksum(quota_changed.envs) != base


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
    base = dbconfig.canonical_config(cfg.envs)
    assert base == dbconfig.canonical_config(cfg.envs)
    changed = build(node_rows=[
        ("hap-1", "haproxy1", 9999, 500, 40_000_000)])
    assert dbconfig.canonical_config(changed.envs) != base
    # 纯重分组（单元不变、只换环境归属）也必须改变身份。
    regrouped = build(env_rows=[("env-x",)],
                      target_rows=[("env-x", "hap-1", "fe_env_a")])
    assert dbconfig.canonical_config(regrouped.envs, regrouped.env_groups) != \
        dbconfig.canonical_config(cfg.envs, cfg.env_groups)
