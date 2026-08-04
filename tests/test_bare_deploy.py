# tests.test_bare_deploy —— 裸机部署脚本与 systemd unit 模板的护栏。
#
# 这些东西平时没人跑测试就发现不了问题：装机脚本一年跑几次，出错时人
# 已经在生产机上了。所以把"能靠静态检查抓住的"都钉在这里：
#   1. unit 模板里的每个 @占位符@ 脚本都替换了（漏一个 systemd 会拒绝
#      加载，或者更糟——把字面量 "@VENV@/bin/rl-limiter" 当路径）；
#   2. unit 必须带 CAP_NET_ADMIN（tc 限速的硬前提，少了它服务起不来）；
#   3. 脚本绝不改写别人的 haproxy.cfg——它是负载均衡配置的唯一权威，
#      归运维手工编辑。

from __future__ import annotations

import pathlib
import re
import shutil
import subprocess

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[1]
BARE = ROOT / "deploy/bare"
SCRIPT = BARE / "rl-limiter.sh"
UNIT_IN = BARE / "rl-limiter.service.in"
ENV_EXAMPLE = BARE / "rl-limiter.env.example"


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
    """限速由内核 tc 执行（docs/06），tc 要操作网络设备。

    没有 CAP_NET_ADMIN 服务直接起不来——这是刻意的，"静默地没在限速"比
    起不来危险得多。
    """
    unit = UNIT_IN.read_text(encoding="utf-8")
    assert "AmbientCapabilities=CAP_NET_ADMIN" in unit
    # Ambient 能力必须同时在 BoundingSet 里，否则内核不会授予
    assert "CapabilityBoundingSet=CAP_NET_ADMIN" in unit
    assert "无需 CAP_NET_ADMIN" not in unit


def test_unit_write_access_is_own_config_dir_only():
    """文件系统写权限的边界：ProtectSystem=strict 全只读，唯一的
    ReadWritePaths 是**自己的配置目录**（写 API 回写 quotas/
    nic_quota_mbps 用）。绝不能出现 haproxy 的路径——"rl-limiter 不改
    cfg、不 reload haproxy"是架构承诺，unit 层面必须钉死。"""
    unit = UNIT_IN.read_text(encoding="utf-8")
    assert "ProtectSystem=strict" in unit
    rwp = re.findall(r"^ReadWritePaths=(.+)$", unit, re.M)
    assert rwp == ["@CONF_DIR@"], f"ReadWritePaths 只许放开自身配置目录：{rwp}"
    assert "haproxy" not in " ".join(rwp)


def test_unit_does_not_hard_require_haproxy():
    """haproxy 挂了 rl-limiter 只是采不到数（打 degraded 告警），不该被
    连坐停掉——那会让监控和限速调整一起消失。所以只能 After，不能 Requires。"""
    unit = UNIT_IN.read_text(encoding="utf-8")
    assert "After=" in unit and "haproxy.service" in unit
    assert not re.search(r"^Requires=.*haproxy", unit, re.M)


def test_unit_passes_yaml_config_path():
    """ExecStart 必须带 -c 指向 YAML 配置——它是 quotas 与接线的来源。"""
    unit = UNIT_IN.read_text(encoding="utf-8")
    assert re.search(r"^ExecStart=.*rl-limiter -c @CONF_DIR@/config\.yaml", unit, re.M)


# ---------------------------------------------------------------------------
# 环境变量模板：不能引用已经不存在的变量名
# ---------------------------------------------------------------------------

def test_env_example_only_uses_real_env_vars():
    """模板里写的 RL_* 变量必须是代码真认的。写错一个不会报错，只是那项
    配置**静默不生效**——本项目最不能接受的故障形态。"""
    text = ENV_EXAMPLE.read_text(encoding="utf-8")
    used = set(re.findall(r"^#?\s*(RL_[A-Z_]+)=", text, re.M))
    src = (ROOT / "rl_limiter" / "__main__.py").read_text(encoding="utf-8")
    known = set(re.findall(r'"(RL_[A-Z_]+)"', src))
    unknown = used - known
    assert not unknown, f"模板里这些变量代码不认：{sorted(unknown)}"


def test_env_example_has_no_mysql_vars():
    """MySQL 配置源已删除：模板里再出现 RL_MYSQL_* 就是回归。"""
    assert "RL_MYSQL" not in ENV_EXAMPLE.read_text(encoding="utf-8")


def test_env_example_defaults_to_loopback_console():
    """控制台无鉴权（暴露全量监控与日志），模板给的默认值必须是回环。"""
    text = ENV_EXAMPLE.read_text(encoding="utf-8")
    assert re.search(r"^RL_CONSOLE_BIND=127\.0\.0\.1", text, re.M)


# ---------------------------------------------------------------------------
# 脚本本身
# ---------------------------------------------------------------------------

@pytest.mark.skipif(shutil.which("bash") is None, reason="需要 bash")
def test_script_parses():
    r = subprocess.run(["bash", "-n", str(SCRIPT)], capture_output=True, text=True)
    assert r.returncode == 0, r.stderr


def test_script_never_rewrites_the_users_haproxy_config():
    """本机 HAProxy 是既有的生产配置，也是负载均衡配置的唯一权威。脚本
    只能**读**它（查 stats socket / 解析校验），绝不能改。"""
    src = SCRIPT.read_text(encoding="utf-8")
    # 找所有对 haproxy.cfg 的写操作痕迹
    for danger in ("> \"$cfg\"", ">\"$cfg\"", "sed -i", "tee "):
        for line in src.splitlines():
            if danger in line and "haproxy" in line.lower():
                pytest.fail(f"脚本疑似在改 haproxy 配置：{line.strip()}")


def test_script_has_no_mysql_leftovers():
    """配置库已删除：脚本里再出现 mysql/bootstrap_db 就是回归。"""
    src = SCRIPT.read_text(encoding="utf-8").lower()
    assert "mysql" not in src
    assert "bootstrap_db" not in src
