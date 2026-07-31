# tests.test_bare_deploy —— 裸机部署脚本与 systemd unit 模板的护栏。
#
# 这些东西平时没人跑测试就发现不了问题：装机脚本一年跑几次，出错时人
# 已经在生产机上了。所以把"能靠静态检查抓住的"都钉在这里：
#   1. unit 模板里的每个 @占位符@ 脚本都替换了（漏一个 systemd 会拒绝
#      加载，或者更糟——把字面量 "@VENV@/bin/rl-limiter" 当路径）；
#   2. unit 必须带 CAP_NET_ADMIN（限速换成 tc 之后这是硬前提，少了它
#      服务起不来，而旧版 unit 的注释还写着"无需 CAP_NET_ADMIN"）；
#   3. bootstrap 对已有配置只增不改（用 INSERT IGNORE，不是 REPLACE）——
#      重跑装机脚本把运维调好的限额冲回默认值是不可接受的故障。

from __future__ import annotations

import pathlib
import re
import shutil
import subprocess

import pytest
import yaml

ROOT = pathlib.Path(__file__).resolve().parents[1]
BARE = ROOT / "deploy/bare"
SCRIPT = BARE / "rl-limiter.sh"
UNIT_IN = BARE / "rl-limiter.service.in"
ENV_EXAMPLE = BARE / "rl-limiter.env.example"
COMPOSE = BARE / "docker-compose.mysql.yml"
BOOTSTRAP = ROOT / "tools/bootstrap_db.py"


# ---------------------------------------------------------------------------
# systemd unit 模板
# ---------------------------------------------------------------------------

def test_every_placeholder_in_the_unit_is_substituted():
    """模板里的每个 @占位符@ 脚本都得替换。

    漏一个的后果不是报错而是更难查的东西：systemd 会拿字面量
    "@VENV@/bin/rl-limiter" 当可执行文件路径，报 203/EXEC。
    """
    placeholders = set(re.findall(r"@([A-Z_]+)@", UNIT_IN.read_text(encoding="utf-8")))
    assert placeholders, "模板里一个占位符都没有？"
    script = SCRIPT.read_text(encoding="utf-8")
    for ph in sorted(placeholders):
        assert f"s|@{ph}@|" in script, (
            f"unit 模板里有 @{ph}@，但 rl-limiter.sh 的 sed 没替换它")


def test_unit_grants_net_admin_for_tc():
    """限速已经从 HAProxy bwlim 换成内核 tc（docs/06），tc 要操作网络设备。

    没有 CAP_NET_ADMIN 服务直接起不来——这是刻意的，"静默地没在限速"比
    起不来危险得多。旧版 unit 的注释里还写着"无需 CAP_NET_ADMIN"，那是
    bwlim 时代的说法。
    """
    unit = UNIT_IN.read_text(encoding="utf-8")
    assert "AmbientCapabilities=CAP_NET_ADMIN" in unit
    # Ambient 能力必须同时在 BoundingSet 里，否则内核不会授予
    assert "CapabilityBoundingSet=CAP_NET_ADMIN" in unit
    assert "无需 CAP_NET_ADMIN" not in unit


def test_unit_can_write_the_haproxy_config_directory():
    """ProtectSystem=strict 会把 /etc 整个挂只读。配置下发要在 cfg 所在
    **目录**里建临时文件再 rename（原子替换），所以放开的必须是目录。"""
    unit = UNIT_IN.read_text(encoding="utf-8")
    assert "ProtectSystem=strict" in unit
    assert re.search(r"^ReadWritePaths=@HAPROXY_CFG_DIR@", unit, re.M)


def test_unit_does_not_hard_require_haproxy():
    """haproxy 挂了 rl-limiter 只是采不到数（打 degraded 告警），不该被
    连坐停掉——那会让监控和限速一起消失。所以只能 After，不能 Requires。"""
    unit = UNIT_IN.read_text(encoding="utf-8")
    assert "After=" in unit and "haproxy.service" in unit
    assert not re.search(r"^Requires=.*haproxy", unit, re.M)


# ---------------------------------------------------------------------------
# 环境变量模板：不能引用已经不存在的变量名
# ---------------------------------------------------------------------------

def test_env_example_only_uses_real_env_vars():
    """模板里写的 RL_* 变量必须是代码真认的。写错一个不会报错，只是那项
    配置**静默不生效**——本项目最不能接受的故障形态。"""
    text = ENV_EXAMPLE.read_text(encoding="utf-8")
    used = set(re.findall(r"^#?\s*(RL_[A-Z_]+)=", text, re.M))
    src = "\n".join((ROOT / "rl_limiter" / f).read_text(encoding="utf-8")
                    for f in ("__main__.py", "dbconfig.py"))
    known = set(re.findall(r'"(RL_[A-Z_]+)"', src))
    unknown = used - known
    assert not unknown, f"模板里这些变量代码不认：{sorted(unknown)}"


def test_env_example_defaults_to_loopback_console():
    """控制台无鉴权且带写接口，模板给的默认值必须是回环。"""
    text = ENV_EXAMPLE.read_text(encoding="utf-8")
    assert re.search(r"^RL_CONSOLE_BIND=127\.0\.0\.1", text, re.M)


# ---------------------------------------------------------------------------
# 只起 MySQL 的 compose
# ---------------------------------------------------------------------------

def test_mysql_compose_has_only_the_database():
    """裸机形态下 HAProxy 与 rl-limiter 都在本机跑，这份 compose 里
    **只该有数据库**。多一个服务就说明形态又混回去了。"""
    doc = yaml.safe_load(COMPOSE.read_text(encoding="utf-8"))
    assert list(doc["services"]) == ["mysql"]


def test_mysql_port_is_bound_to_loopback_only():
    """库里存着各 frontend 的限额，没有任何理由暴露到网外；rl-limiter
    与它同机，走 127.0.0.1 即可。"""
    doc = yaml.safe_load(COMPOSE.read_text(encoding="utf-8"))
    for spec in doc["services"]["mysql"]["ports"]:
        assert str(spec).startswith("127.0.0.1:"), f"端口 {spec} 没绑回环"


def test_mysql_compose_loads_the_same_init_sql():
    """建表语句只有一份。演示环境和裸机环境用不同的 schema 迟早会漂。"""
    doc = yaml.safe_load(COMPOSE.read_text(encoding="utf-8"))
    mounts = doc["services"]["mysql"]["volumes"]
    assert any("mysql/init.sql" in str(m) for m in mounts), mounts


# ---------------------------------------------------------------------------
# bootstrap：只增不改
# ---------------------------------------------------------------------------

def test_bootstrap_never_overwrites_existing_frontends():
    """**这条是硬要求**：限额是运维在控制台上调出来的值，重跑装机脚本
    把它冲回默认值是不可接受的故障。所以 frontend 与后端服务器一律用
    INSERT IGNORE；只有实例接线信息（socket 路径）才 upsert。"""
    src = BOOTSTRAP.read_text(encoding="utf-8")
    assert "INSERT IGNORE INTO haproxy_frontends" in src
    assert "INSERT IGNORE INTO haproxy_servers" in src
    # 这两张表上绝不能出现覆盖式写法
    for danger in ("REPLACE INTO haproxy_frontends", "REPLACE INTO haproxy_servers"):
        assert danger not in src
    assert "ON DUPLICATE KEY UPDATE" in src.split("haproxy_frontends")[0], \
        "实例行应该 upsert（接线信息以本机为准）"


@pytest.mark.parametrize("spec,expect", [
    ("10.0.0.21:9000", ("10.0.0.21", 9000)),
    ("web.internal:8080", ("web.internal", 8080)),
    ("[fd00::5]:9000", ("fd00::5", 9000)),
])
def test_backend_spec_parsing(spec, expect):
    import importlib.util
    spec_ = importlib.util.spec_from_file_location("bs", BOOTSTRAP)
    m = importlib.util.module_from_spec(spec_)
    spec_.loader.exec_module(m)
    assert m.parse_backend(spec) == expect


@pytest.mark.parametrize("spec", ["10.0.0.21", "10.0.0.21:0", "10.0.0.21:99999", ":9000"])
def test_bad_backend_spec_is_rejected(spec):
    """裸 IPv6 里全是冒号，猜错的后果是流量打到不存在的后端——宁可报错。"""
    import argparse
    import importlib.util
    spec_ = importlib.util.spec_from_file_location("bs", BOOTSTRAP)
    m = importlib.util.module_from_spec(spec_)
    spec_.loader.exec_module(m)
    with pytest.raises(argparse.ArgumentTypeError):
        m.parse_backend(spec)


# ---------------------------------------------------------------------------
# 脚本本身
# ---------------------------------------------------------------------------

@pytest.mark.skipif(shutil.which("bash") is None, reason="需要 bash")
def test_script_parses():
    r = subprocess.run(["bash", "-n", str(SCRIPT)], capture_output=True, text=True)
    assert r.returncode == 0, r.stderr


def test_script_never_rewrites_the_users_haproxy_config():
    """本机 HAProxy 是既有的生产配置。脚本只能**读**它（查 stats socket），
    绝不能改——受管区块由 rl-limiter 自己按标记渲染，那是另一回事。"""
    src = SCRIPT.read_text(encoding="utf-8")
    # 找所有对 haproxy.cfg 的写操作痕迹
    for danger in ("> \"$cfg\"", ">\"$cfg\"", "sed -i", "tee "):
        # sed -i 只允许出现在生成 unit 的地方（那是我们自己的文件）
        for line in src.splitlines():
            if danger in line and "haproxy" in line.lower():
                pytest.fail(f"脚本疑似在改 haproxy 配置：{line.strip()}")
