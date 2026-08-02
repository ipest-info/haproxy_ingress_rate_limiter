# tests.test_dbconfig —— MySQL 配置源的纯函数部分：行 → 原始 dict 的组装、
# 与 YAML 共用的校验行为、内容校验和、环境变量解析。
#
# 真实的 SQL 交互（_fetch_raw/watch/写接口）依赖 aiomysql 与活的 MySQL，
# 由 docker compose 演示环境做集成验证，不进单元测试。

from __future__ import annotations

import pytest

from rl_limiter import config, dbconfig, model

# 与 deploy/mysql/init.sql 种子同构的行样本。
SERVICE_ROW = ("info", 1.0)
# 列序与 dbconfig._INSTANCE_SQL 一致：
#   name, host, port, socket_path, timeout_ms, limit_scope, host_quota_mbps
INSTANCE_ROW = ("haproxy", None, None, "/run/haproxy/admin.sock", 500,
                "frontend", 0.0)
FRONTEND_ROWS = [
    # name, bind_address, bind_port, mode, quota_mbps, maxconn, balance,
    # timeout_connect_ms, timeout_client_ms, timeout_server_ms
    ("fe_main", "", 8080, "tcp", 40.0, 2000, "roundrobin", 5000, 50000, 50000),
    ("fe_api", "127.0.0.1", 8081, "http", 8.0, None, "leastconn", None, None, None),
]
SERVER_ROWS = [
    # frontend, name, address, port, weight, check_enabled, check_inter_ms
    ("fe_api", "api1", "10.0.0.31", 8000, 100, 1, 2000),
    ("fe_main", "web1", "10.0.0.21", 9000, 100, 1, 2000),
    ("fe_main", "web2", "10.0.0.22", 9000, 50, 0, 2000),
]


def build(service_row=SERVICE_ROW, instance_row=INSTANCE_ROW,
          frontend_rows=None, server_rows=None) -> config.ServiceConfig:
    raw = dbconfig.rows_to_raw(
        service_row, instance_row,
        FRONTEND_ROWS if frontend_rows is None else frontend_rows,
        SERVER_ROWS if server_rows is None else server_rows)
    return config.from_raw(raw, source="测试数据库")


def test_rows_roundtrip_to_service_config():
    """种子数据经 行组装 → 统一校验管线 得到与 YAML 加载同构的 ServiceConfig。"""
    cfg = build()
    assert cfg.log_level == "info" and cfg.tick_interval_s == 1.0
    assert cfg.haproxy.socket_path == "/run/haproxy/admin.sock"
    assert cfg.haproxy.is_unix is True
    assert cfg.haproxy.timeout_s == 0.5

    assert [f.name for f in cfg.frontends] == ["fe_main", "fe_api"]
    main = cfg.frontends[0]
    assert main.bind_spec == ":8080" and main.maxconn == 2000
    assert main.quota_bytes_per_sec == 5_000_000
    assert [s.name for s in main.servers] == ["web1", "web2"]
    assert main.servers[1].check is False and main.servers[1].weight == 50


def test_null_columns_fall_back_to_defaults():
    """超时列允许为 NULL：留空即采用配置层默认值，不必在库里逐行填。"""
    api = build().frontends[1]
    assert (api.timeout_connect_ms, api.timeout_client_ms,
            api.timeout_server_ms) == (5000, 50000, 50000)
    assert api.maxconn == 0


def test_tcp_instance_row():
    """远程观测形态：host/port 有值、socket_path 为 NULL。"""
    cfg = build(instance_row=("haproxy", "10.0.0.11", 9999, None, 500, "frontend", 0.0))
    assert cfg.haproxy.is_unix is False
    assert cfg.haproxy.endpoint() == "10.0.0.11:9999"


def test_db_and_yaml_share_validation():
    """数据库来源的坏数据被同一套校验拒绝，错误信息带来源前缀。"""
    bad = [("fe bad", "", 8080, "tcp", 8000, 0, "roundrobin", None, None, None)]
    with pytest.raises(ValueError, match="测试数据库"):
        build(frontend_rows=bad, server_rows=[("fe bad", "s", "1.1.1.1", 80, 100, 1, 2000)])


def test_frontend_without_servers_rejected():
    """没有后端的 frontend 会把所有请求返回 503——拒绝而不是放行。"""
    with pytest.raises(ValueError, match="servers 不能为空"):
        build(server_rows=[])


# ---------------------------------------------------------------------------
# 变更检测
# ---------------------------------------------------------------------------

def test_checksum_changes_with_content():
    base = build()
    before = dbconfig.config_checksum(base.frontends)

    changed = [list(r) for r in FRONTEND_ROWS]
    changed[0][4] = 20_000_000                     # 改 fe_main 限额
    after = dbconfig.config_checksum(
        build(frontend_rows=[tuple(r) for r in changed]).frontends)
    assert after != before


def test_checksum_covers_backend_servers():
    """后端服务器变化也必须被检测到——否则加/删一台机器不会触发下发。"""
    before = dbconfig.config_checksum(build().frontends)
    fewer = [r for r in SERVER_ROWS if r[1] != "web2"]
    assert dbconfig.config_checksum(build(server_rows=fewer).frontends) != before


def test_canonical_is_stable_across_equal_content():
    """同样内容必须得到同样字符串，否则每轮轮询都会误判成"配置变了"。"""
    a = dbconfig.canonical_config(build().frontends)
    b = dbconfig.canonical_config(build().frontends)
    assert a == b


# ---------------------------------------------------------------------------
# 写接口的载荷校验（控制台经它写库）
# ---------------------------------------------------------------------------

def test_frontend_payload_validation_accepts_good():
    fe = dbconfig._validate_frontend_payload({
        "name": "fe_x", "bind_port": 9000, "quota_mbps": 8.0,
        "servers": [{"name": "s1", "address": "10.0.0.1", "port": 80}],
    })["frontend"]
    assert isinstance(fe, model.FrontendConfig) and fe.name == "fe_x"


@pytest.mark.parametrize("payload,match", [
    ("not a dict", "必须是 JSON 对象"),
    ({"name": "fe_x"}, "字段缺失或类型错误"),
    ({"name": "fe x", "bind_port": 1, "quota_mbps": 8,
      "servers": [{"name": "s", "address": "1.1.1.1", "port": 1}]}, "name 非法"),
    ({"name": "fe_x", "bind_port": 1, "quota_mbps": 8, "servers": []},
     "servers 不能为空"),
])
def test_frontend_payload_validation_rejects(payload, match):
    """经控制台写入与直接写库后被加载，两条路径的接受集合必须一致。"""
    with pytest.raises(ValueError, match=match):
        dbconfig._validate_frontend_payload(payload)


# ---------------------------------------------------------------------------
# 环境变量解析
# ---------------------------------------------------------------------------

def test_from_env_disabled_without_host():
    assert dbconfig.from_env({}) is None


def test_from_env_reads_all_fields():
    opts = dbconfig.from_env({
        "RL_MYSQL_HOST": "db", "RL_MYSQL_PORT": "3307",
        "RL_MYSQL_USER": "u", "RL_MYSQL_PASSWORD": "p",
        "RL_MYSQL_DB": "d", "RL_MYSQL_POLL_S": "9",
    })
    assert (opts.host, opts.port, opts.user, opts.password, opts.database,
            opts.poll_interval_s) == ("db", 3307, "u", "p", "d", 9.0)


# ---------------------------------------------------------------------------
# 限速范围与整机限额：NULL 一路传到底
# ---------------------------------------------------------------------------

def test_null_host_quota_stays_null_all_the_way():
    """库里 host_quota_mbps 为 NULL（= 没设 = 不限速）时，**不能在组装
    这一步被压成 0**。

    压成 0 的后果不是"不限速"，而是配置校验直接把它判成非法（0 Mbps 谁也
    跑不动），整台机器的 rl-limiter 起不来——一个 `or 0` 就能造成的停机。
    """
    row = ("haproxy", None, None, "/run/haproxy/admin.sock", 500, "host", None)
    raw = dbconfig.rows_to_raw(SERVICE_ROW, row, FRONTEND_ROWS, SERVER_ROWS)
    assert raw["haproxy"]["host_quota_mbps"] is None
    cfg = config.from_raw(raw, source="测试数据库")     # 不该抛
    assert cfg.haproxy.limit_scope == "host"
    assert not cfg.haproxy.has_host_quota


def test_zero_host_quota_from_db_is_rejected():
    """库里明确写了 0 则是另一回事：那是配错了，必须拒——否则打错一个字
    就静默变成不限速。"""
    row = ("haproxy", None, None, "/run/haproxy/admin.sock", 500, "host", 0.0)
    raw = dbconfig.rows_to_raw(SERVICE_ROW, row, FRONTEND_ROWS, SERVER_ROWS)
    with pytest.raises(ValueError, match="留空"):
        config.from_raw(raw, source="测试数据库")


def test_positive_host_quota_from_db_flows_through():
    row = ("haproxy", None, None, "/run/haproxy/admin.sock", 500, "host", 2000.0)
    cfg = build(instance_row=row)
    assert cfg.haproxy.has_host_quota
    assert cfg.haproxy.host_quota_mbps == 2000.0
