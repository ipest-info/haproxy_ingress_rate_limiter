# tests.test_release_install —— 二进制发布包 install.sh 的行为护栏。
#
# 装机脚本一年跑几次，出错时人已经在生产机上了。这里在临时目录里把它
# 真跑一遍：PATH 里放假的 id/useradd/chgrp/systemctl/curl（不需要 root、
# 不碰网络），SYS_CLASS_NET 指向假的 /sys/class/net（想要几张物理网卡
# 就造几张），PREFIX/CONF_DIR/UNIT_DIR 全部落在临时目录。
#
# 钉住的约定：
#   1. 节点安装必须双网卡（物理网卡 < 2 拒绝安装，什么都不落盘）；
#   2. 客户侧/后端侧网卡相斥、必须存在；令牌与网卡写进 rl-limiter.env；
#   3. 已有配置不覆盖，只改命令行显式给出的项；
#   4. --fleet 只装总控（不查网卡），--token 写进 rl-fleet.env；
#   5. --download 的参数解析与透传、临时目录用完即清；
#   6. --uninstall 停服务、清 tc 限速规则、删单元，两种角色都卸完才删
#      二进制；--purge 才删配置。

from __future__ import annotations

import os
import pathlib
import platform
import shutil
import subprocess
import tarfile
import hashlib

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "deploy/release/install.sh"

pytestmark = pytest.mark.skipif(shutil.which("bash") is None,
                                reason="需要 bash")

_STUBS = {
    # 假装是 root；"id rl-limiter" 一律当用户已存在。
    "id": '[ "$1" = "-u" ] && echo 0; exit 0',
    "useradd": "exit 0",
    "chgrp": "exit 0",
    "systemctl": 'echo "systemctl $*" >>"$FAKE_TC_LOG"; exit 0',
    # 假 tc/ip：记录调用；show 的输出由 FAKE_TC_INGRESS/FAKE_TC_ROOT/
    # FAKE_IFB 控制（模拟网卡上有没有 rl-limiter 建的规则）。
    "tc": r'''
echo "tc $*" >>"$FAKE_TC_LOG"
case "$*" in
  "qdisc show dev "*" ingress")
    [ -n "$FAKE_TC_INGRESS" ] && echo "qdisc ingress ffff: parent ffff:fff1" ;;
  "qdisc show dev "*" root") echo "${FAKE_TC_ROOT:-qdisc mq 0: root}" ;;
esac
exit 0
''',
    "ip": r'''
echo "ip $*" >>"$FAKE_TC_LOG"
case "$1 $2" in "link show") [ -n "$FAKE_IFB" ] || exit 1 ;; esac
exit 0
''',
    # 假 curl：-w（查最新版）打印 $FAKE_LATEST；-o 下载从 $FAKE_ASSETS 拷。
    "curl": r'''
out="" url="" w=""
while [ $# -gt 0 ]; do
  case "$1" in
    -o) out=$2; shift ;;
    -w) w=$2; shift ;;
    --retry) shift ;;
    -*) ;;
    *) url=$1 ;;
  esac
  shift
done
echo "$url" >>"$FAKE_CURL_LOG"
if [ -n "$w" ]; then
  [ -n "$FAKE_LATEST" ] || exit 22
  printf '%s' "$FAKE_LATEST"; exit 0
fi
f="$FAKE_ASSETS/${url##*/}"
[ -f "$f" ] || exit 22
cp "$f" "$out"
''',
}


def _make_pkg(dst: pathlib.Path) -> pathlib.Path:
    """造一个发布包目录（假二进制 + 真模板 + 真 install.sh）。"""
    (dst / "bin").mkdir(parents=True)
    (dst / "config").mkdir()
    for b in ("rl-limiter", "rl-fleet"):
        p = dst / "bin" / b
        p.write_text(f"#!/bin/sh\necho {b} 9.9.9\n")
        p.chmod(0o755)
    for src in ("deploy/config/limiter.example.yaml",
                "deploy/config/fleet.example.yaml",
                "deploy/config/rl-fleet.env.example",
                "deploy/bare/rl-limiter.env.example"):
        shutil.copy(ROOT / src, dst / "config")
    shutil.copy(SCRIPT, dst / "install.sh")
    (dst / "install.sh").chmod(0o755)
    return dst


@pytest.fixture
def box(tmp_path):
    """一台假机器：返回 (run, paths)。run(*args, nics=[...]) 跑脚本。"""
    stubs = tmp_path / "stubs"
    stubs.mkdir()
    for name, body in _STUBS.items():
        p = stubs / name
        p.write_text("#!/bin/sh\n" + body + "\n")
        p.chmod(0o755)
    paths = {
        "prefix": tmp_path / "prefix",
        "conf": tmp_path / "etc",
        "units": tmp_path / "units",
        "sys": tmp_path / "sys",
        "tmp": tmp_path / "tmpdir",
        "assets": tmp_path / "assets",
        "curl_log": tmp_path / "curl.log",
        "tc_log": tmp_path / "tc.log",
        "pkg": _make_pkg(tmp_path / "pkg"),
    }
    paths["tmp"].mkdir()
    paths["assets"].mkdir()

    def run(*args, nics=("eth0", "eth1"), virtual=("lo",), latest="",
            script=None, env=None):
        sysd = paths["sys"]
        if sysd.exists():
            shutil.rmtree(sysd)
        sysd.mkdir()
        for n in nics:                       # 物理网卡：有 device 链接
            (sysd / n).mkdir()
            (sysd / n / "device").write_text("")
        for n in virtual:
            (sysd / n).mkdir()
        extra = env or {}
        env = dict(os.environ,
                   PATH=f"{stubs}:{os.environ['PATH']}",
                   PREFIX=str(paths["prefix"]),
                   CONF_DIR=str(paths["conf"]),
                   UNIT_DIR=str(paths["units"]),
                   SYS_CLASS_NET=str(sysd),
                   TMPDIR=str(paths["tmp"]),
                   FAKE_ASSETS=str(paths["assets"]),
                   FAKE_CURL_LOG=str(paths["curl_log"]),
                   FAKE_TC_LOG=str(paths["tc_log"]),
                   FAKE_LATEST=latest, **extra)
        return subprocess.run(
            ["bash", str(script or paths["pkg"] / "install.sh"), *args],
            capture_output=True, text=True, env=env)

    return run, paths


def _env(paths, name="rl-limiter.env") -> dict:
    out = {}
    for line in (paths["conf"] / name).read_text().splitlines():
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            out[k.strip()] = v.strip()
    return out


def test_script_parses():
    r = subprocess.run(["bash", "-n", str(SCRIPT)], capture_output=True,
                       text=True)
    assert r.returncode == 0, r.stderr


# ---- 节点：双网卡检查 ------------------------------------------------------

def test_single_nic_node_install_is_refused(box):
    run, paths = box
    r = run(nics=["eth0"], virtual=["lo", "docker0"])
    assert r.returncode != 0
    assert "双网卡" in r.stderr and "不允许安装" in r.stderr
    # 拒绝 = 什么都不落盘。
    assert not paths["prefix"].exists()
    assert not paths["conf"].exists()
    assert not paths["units"].exists()


def test_virtual_nics_do_not_count_as_dual_nic(box):
    """lo/网桥/veth 没有 device 链接，不算物理网卡。"""
    run, _ = box
    r = run(nics=["eth0"], virtual=["lo", "eth1"])
    assert r.returncode != 0 and "双网卡" in r.stderr


# ---- 节点：网卡与令牌 ------------------------------------------------------

def test_node_install_writes_token_and_nics(box):
    run, paths = box
    r = run("--token", "tok-0123456789", "--client-nic", "ens33",
            "--backend-nic", "ens34", nics=["ens33", "ens34"])
    assert r.returncode == 0, r.stderr
    env = _env(paths)
    assert env["RL_TC_IN_IFACE"] == "ens33"
    assert env["RL_BACKEND_IFACE"] == "ens34"
    assert env["RL_API_TOKEN"] == "tok-0123456789"
    # 每个键只有一行生效（systemd 后写覆盖先写，重复行等于埋雷）。
    text = (paths["conf"] / "rl-limiter.env").read_text()
    for k in ("RL_TC_IN_IFACE", "RL_BACKEND_IFACE", "RL_API_TOKEN"):
        assert sum(1 for ln in text.splitlines()
                   if ln.startswith(k + "=")) == 1, k
    assert (paths["units"] / "rl-limiter.service").exists()
    assert not (paths["units"] / "rl-fleet.service").exists()
    assert (paths["prefix"] / "bin/rl-limiter").exists()
    assert (paths["conf"] / "config.yaml").exists()


def test_node_install_defaults_to_factory_wiring(box):
    run, paths = box
    r = run()
    assert r.returncode == 0, r.stderr
    env = _env(paths)
    assert env["RL_TC_IN_IFACE"] == "eth0"
    assert env["RL_BACKEND_IFACE"] == "eth1"
    assert "RL_API_TOKEN" not in env          # 没给 --token：保持注释
    assert "未设节点令牌" in r.stderr


def test_same_nic_for_client_and_backend_is_refused(box):
    run, paths = box
    r = run("--client-nic", "eth1", "--backend-nic", "eth1")
    assert r.returncode != 0
    assert "相斥" in r.stderr
    assert not paths["conf"].exists()


@pytest.mark.parametrize("args, missing", [
    (("--client-nic", "ens9"), "ens9"),
    (("--backend-nic", "ens9"), "ens9"),
])
def test_missing_nic_is_refused(box, args, missing):
    run, _ = box
    r = run(*args)
    assert r.returncode != 0
    assert missing in r.stderr and "不存在" in r.stderr


def test_default_nics_missing_asks_for_explicit_flags(box):
    """命名不是 eth0/eth1 的机器：不猜，要求显式给。"""
    run, _ = box
    r = run(nics=["ens33", "ens34"])
    assert r.returncode != 0
    assert "--client-nic" in r.stderr


@pytest.mark.parametrize("args", [
    ("--token", "short"),                 # 太短
    ("--token", "has space here"),        # 空格进不了 EnvironmentFile
    ("--token", "a'b\"c$d1234"),          # 引号/展开符
    ("--token",),                         # 缺值
    ("--token", "--fleet"),               # 不许把下一个选项吞成值
    ("--client-nic", "eth0/x"),
    ("--bogus",),
])
def test_bad_arguments_are_refused(box, args):
    run, paths = box
    r = run(*args)
    assert r.returncode != 0
    assert r.stderr
    assert not paths["conf"].exists()


def test_equals_form_is_accepted(box):
    run, paths = box
    r = run("--token=-starts-with-dash", "--client-nic=eth1",
            "--backend-nic=eth0")
    assert r.returncode == 0, r.stderr
    env = _env(paths)
    assert env["RL_API_TOKEN"] == "-starts-with-dash"
    assert env["RL_TC_IN_IFACE"] == "eth1"
    assert env["RL_BACKEND_IFACE"] == "eth0"


def test_rerun_keeps_config_and_only_applies_given_flags(box):
    run, paths = box
    assert run("--token", "first-token-1").returncode == 0
    envf = paths["conf"] / "rl-limiter.env"
    # 运维手工调过的项：重跑不许动。
    envf.write_text(envf.read_text().replace(
        "RL_CONSOLE_PORT=8090", "RL_CONSOLE_PORT=9999"))
    (paths["conf"] / "config.yaml").write_text("instance_quota_mbps: 42\n")

    r = run("--token", "second-token-2")
    assert r.returncode == 0, r.stderr
    env = _env(paths)
    assert env["RL_API_TOKEN"] == "second-token-2"
    assert env["RL_CONSOLE_PORT"] == "9999"
    assert env["RL_TC_IN_IFACE"] == "eth0"
    assert (paths["conf"] / "config.yaml").read_text() == \
        "instance_quota_mbps: 42\n"


def test_rerun_validates_wiring_already_in_env(box):
    """已有 env 里的接线也要过相斥校验（命令行没给时取 env 里的值）。"""
    run, paths = box
    assert run().returncode == 0
    r = run("--backend-nic", "eth0")      # 客户侧（env 里）也是 eth0
    assert r.returncode != 0 and "相斥" in r.stderr
    assert _env(paths)["RL_BACKEND_IFACE"] == "eth1"


def test_old_env_without_backend_nic_gets_a_hint(box):
    run, paths = box
    paths["conf"].mkdir()
    (paths["conf"] / "rl-limiter.env").write_text(
        "RL_TC_IN_IFACE=eth0\nRL_CONSOLE_PORT=8090\n")
    r = run()
    assert r.returncode == 0, r.stderr
    assert "--backend-nic" in r.stderr
    r = run("--backend-nic", "eth1")
    assert r.returncode == 0, r.stderr
    assert _env(paths)["RL_BACKEND_IFACE"] == "eth1"


# ---- 总控 ------------------------------------------------------------------

def test_fleet_install_writes_token_and_skips_nic_check(box):
    run, paths = box
    r = run("--fleet", "--token", "fleet-token-1", nics=[])
    assert r.returncode == 0, r.stderr
    env = _env(paths, "rl-fleet.env")
    assert env["RL_API_TOKEN"] == "fleet-token-1"
    assert env["RL_CONSOLE_PORT"] == "8095"
    assert (paths["conf"] / "fleet.yaml").exists()
    assert (paths["units"] / "rl-fleet.service").exists()
    # 总控安装不装节点那一套。
    assert not (paths["units"] / "rl-limiter.service").exists()
    assert not (paths["conf"] / "rl-limiter.env").exists()


def test_fleet_rejects_nic_flags(box):
    run, _ = box
    r = run("--fleet", "--client-nic", "eth0")
    assert r.returncode != 0 and "只用于节点" in r.stderr


def test_fleet_env_example_only_uses_real_env_vars():
    """同节点侧模板的护栏：写错变量名不会报错，只会静默不生效。"""
    import re
    text = (ROOT / "deploy/config/rl-fleet.env.example").read_text(
        encoding="utf-8")
    used = set(re.findall(r"^#?\s*(RL_[A-Z_]+)=", text, re.M))
    src = (ROOT / "rl_limiter/fleetmain.py").read_text(encoding="utf-8")
    known = set(re.findall(r'"(RL_[A-Z_]+)"', src))
    assert used and not (used - known), sorted(used - known)


def test_shipped_fleet_template_loads_with_empty_node_list():
    """总控装出来的 fleet.yaml 必须能直接启动（空清单，页面里再加节点）。"""
    from rl_limiter import fleetconfig
    cfg = fleetconfig.load(str(ROOT / "deploy/config/fleet.example.yaml"))
    assert cfg.nodes == []


# ---- --download ------------------------------------------------------------

def _publish(paths, tag: str) -> None:
    """把包目录打成 Release 附件（tar.gz + .sha256）放进假的 Release。"""
    name = f"rl-limiter-{tag}-linux-{platform.machine()}"
    tgz = paths["assets"] / f"{name}.tar.gz"
    with tarfile.open(tgz, "w:gz") as tf:
        tf.add(paths["pkg"], arcname=name)
    digest = hashlib.sha256(tgz.read_bytes()).hexdigest()
    (paths["assets"] / f"{name}.tar.gz.sha256").write_text(
        f"{digest}  {name}.tar.gz\n")


def test_download_latest_passes_options_through_and_cleans_up(box):
    run, paths = box
    _publish(paths, "v9.9.9")
    r = run("--download", "--token", "dl-token-123", "--client-nic", "eth1",
            "--backend-nic", "eth0", script=SCRIPT,
            latest="https://github.com/x/y/releases/tag/v9.9.9")
    assert r.returncode == 0, r.stderr
    assert "最新版本：v9.9.9" in r.stdout
    env = _env(paths)
    assert env["RL_API_TOKEN"] == "dl-token-123"
    assert env["RL_TC_IN_IFACE"] == "eth1"
    assert env["RL_BACKEND_IFACE"] == "eth0"
    # 临时目录（下载包 + 解包的二进制）必须清掉。
    assert list(paths["tmp"].iterdir()) == []
    # 只用 github.com，不走 api.github.com（匿名限流）。
    assert "api.github.com" not in paths["curl_log"].read_text()


def test_download_tag_without_v_and_fleet(box):
    run, paths = box
    _publish(paths, "v9.9.9")
    r = run("--download", "9.9.9", "--fleet", "--token", "fleet-token-2",
            script=SCRIPT, nics=[])
    assert r.returncode == 0, r.stderr
    assert _env(paths, "rl-fleet.env")["RL_API_TOKEN"] == "fleet-token-2"


@pytest.mark.parametrize("args, latest, msg", [
    (("--download",), "", "查询最新版本失败"),
    (("--download",), "https://github.com/x/y/releases", "没查到正式 Release"),
    (("--download", "v1.2.3"), "", "下载失败"),
    (("--download", "latest-ish"), "", "不符合 tag 规则"),
    (("--download", ""), "", "tag 为空"),
])
def test_download_failures_say_why(box, args, latest, msg):
    run, paths = box
    r = run(*args, script=SCRIPT, latest=latest)
    assert r.returncode != 0
    assert msg in r.stderr, r.stderr
    assert list(paths["tmp"].iterdir()) == []


def test_download_checks_nics_before_downloading(box):
    """参数/接线错了要在下载之前就失败，不白下几十 MB。"""
    run, paths = box
    r = run("--download", "v9.9.9", script=SCRIPT, nics=["eth0"])
    assert r.returncode != 0 and "双网卡" in r.stderr
    assert not paths["curl_log"].exists()


# ---- --uninstall -----------------------------------------------------------

def _calls(paths) -> list:
    f = paths["tc_log"]
    return f.read_text().splitlines() if f.exists() else []


def test_uninstall_node_clears_tc_and_keeps_config(box):
    run, paths = box
    assert run("--token", "node-token-1").returncode == 0
    paths["tc_log"].unlink(missing_ok=True)
    # 卸载不查双网卡：网卡拆了也能卸。
    r = run("--uninstall", nics=[],
            env={"FAKE_TC_INGRESS": "1", "FAKE_IFB": "1"})
    assert r.returncode == 0, r.stderr
    calls = _calls(paths)
    # 先停服务（否则它会把规则建回来），再清 tc。
    stop = calls.index("systemctl disable --now rl-limiter")
    assert calls.index("tc qdisc del dev eth0 ingress") > stop
    assert "ip link del ifbrl0" in calls
    # 出向没接（RL_TC_IFACE=-）：绝不碰 root qdisc。
    assert not any("root" in c and "del" in c for c in calls)
    assert not (paths["units"] / "rl-limiter.service").exists()
    assert not (paths["prefix"] / "bin/rl-limiter").exists()
    assert not (paths["prefix"] / "bin/rl-fleet").exists()
    # 不加 --purge：配置保留，重装沿用。
    assert _env(paths)["RL_API_TOKEN"] == "node-token-1"
    assert (paths["conf"] / "config.yaml").exists()


def test_uninstall_skips_tc_that_is_not_there(box):
    run, paths = box
    assert run().returncode == 0
    r = run("--uninstall")                    # 网卡上没有规则、没有 IFB
    assert r.returncode == 0, r.stderr
    assert not any(" del " in c for c in _calls(paths)
                   if c.startswith(("tc ", "ip ")))


@pytest.mark.parametrize("root, deleted", [
    ("qdisc htb 1: root refcnt 2 r2q 10 default 0x1", True),
    ("qdisc mq 0: root", False),              # 不是我们建的，不动
])
def test_uninstall_egress_htb_only_when_ours(box, root, deleted):
    run, paths = box
    assert run().returncode == 0
    envf = paths["conf"] / "rl-limiter.env"
    envf.write_text(envf.read_text().replace("RL_TC_IFACE=-",
                                             "RL_TC_IFACE=eth0"))
    r = run("--uninstall", env={"FAKE_TC_ROOT": root})
    assert r.returncode == 0, r.stderr
    assert ("tc qdisc del dev eth0 root" in _calls(paths)) is deleted


def test_uninstall_purge_removes_config(box):
    run, paths = box
    assert run().returncode == 0
    r = run("--uninstall", "--purge")
    assert r.returncode == 0, r.stderr
    assert not paths["conf"].exists()         # 空了连目录一起删


def test_uninstall_keeps_binaries_while_other_role_remains(box):
    run, paths = box
    assert run().returncode == 0
    assert run("--fleet", "--token", "fleet-token-1").returncode == 0

    r = run("--uninstall")
    assert r.returncode == 0, r.stderr
    assert (paths["prefix"] / "bin/rl-fleet").exists()
    assert (paths["units"] / "rl-fleet.service").exists()

    r = run("--uninstall", "--fleet", "--purge")
    assert r.returncode == 0, r.stderr
    assert "systemctl disable --now rl-fleet" in _calls(paths)
    assert not (paths["prefix"] / "bin/rl-fleet").exists()
    assert not (paths["conf"] / "fleet.yaml").exists()
    assert not (paths["conf"] / "rl-fleet.env").exists()
    # 节点那份没 purge：还在，目录也就还在。
    assert (paths["conf"] / "rl-limiter.env").exists()


@pytest.mark.parametrize("args", [
    ("--uninstall", "--token", "abcdefgh1"),
    ("--uninstall", "--download"),
    ("--uninstall", "--client-nic", "eth0"),
    ("--purge",),                              # 只用于卸载
])
def test_uninstall_rejects_install_options(box, args):
    run, paths = box
    r = run(*args)
    assert r.returncode != 0 and r.stderr
    assert not paths["conf"].exists()


# ---- 总控监控落盘：新装默认开，老 env 升级补项 ---------------------------

def test_fleet_install_enables_metrics_log(box):
    run, paths = box
    assert run("--fleet", nics=[]).returncode == 0
    assert _env(paths, "rl-fleet.env")["RL_METRICS_LOG"] == \
        "/var/log/rl-limiter/fleet-metrics.jsonl"


def test_fleet_upgrade_adds_metrics_log_to_old_env(box):
    run, paths = box
    paths["conf"].mkdir()
    old = "RL_CONSOLE_PORT=8095\nRL_API_TOKEN=fleet-token-1\n"
    (paths["conf"] / "rl-fleet.env").write_text(old)
    r = run("--fleet", nics=[])
    assert r.returncode == 0, r.stderr
    env = _env(paths, "rl-fleet.env")
    assert env["RL_METRICS_LOG"] == "/var/log/rl-limiter/fleet-metrics.jsonl"
    assert env["RL_API_TOKEN"] == "fleet-token-1"       # 其余不动
    # 重跑不重复追加。
    assert run("--fleet", nics=[]).returncode == 0
    text = (paths["conf"] / "rl-fleet.env").read_text()
    assert text.count("RL_METRICS_LOG=") == 1


def test_fleet_upgrade_respects_disabled_metrics_log(box):
    """注释掉过 = 有意关闭，升级不许擅自打开。"""
    run, paths = box
    paths["conf"].mkdir()
    (paths["conf"] / "rl-fleet.env").write_text(
        "RL_CONSOLE_PORT=8095\n# RL_METRICS_LOG=/var/log/x.jsonl\n")
    assert run("--fleet", nics=[]).returncode == 0
    assert "RL_METRICS_LOG" not in _env(paths, "rl-fleet.env")
