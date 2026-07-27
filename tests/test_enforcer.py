# tests.test_enforcer —— 限额自动应用（改本机 haproxy.cfg + reload）的测试。
#
# 这是全项目唯一会写盘并对生产数据面下命令的模块，所以测试的重点不是
# "改对了没有"，而是**每条失败路径都不留下烂摊子**：校验不过不 reload、
# reload 失败要回滚、目标不明确宁可不动。
#
# 用假的校验/reload 命令（true/false）替代真 haproxy，测试不依赖环境里
# 装没装 haproxy；与真实 HAProxy 2.8.16 的联调另行做过（见模块头实测表）。

from __future__ import annotations

import textwrap

import pytest

from rl_limiter import enforcer as E

# 与 deploy/docker/haproxy-env-a.cfg 同构：两个受控 frontend 各有自己的
# shared bwlim 行，用来验证"只改目标那一段"。
CFG = textwrap.dedent("""\
    global
        stats socket /run/haproxy/admin.sock mode 660 level user
        hard-stop-after 15s

    defaults
        mode tcp
        option contstats

    listen fe_env_a
        bind :8080
        stick-table type string len 64 size 1k expire 1h store bytes_out_rate(1s)
        filter bwlim-out node-agg limit 5000000 key fe_name min-size 1460
        tcp-request content set-bandwidth-limit node-agg
        server web1 web:9000

    listen fe_env_b
        bind :8081
        filter bwlim-out node-agg limit 3000000 key fe_name min-size 1460
        server web1 web:9000
    """)


def write(tmp_path, text=CFG):
    p = tmp_path / "haproxy.cfg"
    p.write_text(text, encoding="utf-8")
    return p


# ---------------------------------------------------------------------------
# 解析与替换（纯函数）
# ---------------------------------------------------------------------------

def test_parse_limits():
    assert E.parse_limits(CFG) == {"fe_env_a": 5000000, "fe_env_b": 3000000}


def test_parse_ignores_per_stream_form():
    """per-stream 形态用的是 default-limit，不该被当成可自动调整的 shared
    限额——它正是设计文档里生产事故后废弃的那种配置。"""
    cfg = "listen fe_x\n    filter bwlim-out ps default-limit 100000 default-period 1s\n"
    assert E.parse_limits(cfg) == {}


def test_replace_only_touches_target_section():
    """只改目标 frontend 那一行，其余整份文件逐字节不变。"""
    new, prev = E.replace_limits(CFG, {"fe_env_a": 2500000})
    assert prev == {"fe_env_a": 5000000}
    assert E.parse_limits(new) == {"fe_env_a": 2500000, "fe_env_b": 3000000}
    # 除了那一个数字，其它行原样保留（缩进/参数顺序/注释都不动）。
    diff = [(a, b) for a, b in zip(CFG.splitlines(), new.splitlines()) if a != b]
    assert diff == [
        ("    filter bwlim-out node-agg limit 5000000 key fe_name min-size 1460",
         "    filter bwlim-out node-agg limit 2500000 key fe_name min-size 1460")
    ]


def test_replace_noop_when_already_equal():
    """已经是目标值 → 不报告变化（上层据此跳过写盘与 reload）。"""
    new, prev = E.replace_limits(CFG, {"fe_env_a": 5000000})
    assert prev == {} and new == CFG


def test_replace_unknown_frontend_rejected():
    with pytest.raises(E.EnforceError, match="找不到这些受控 frontend"):
        E.replace_limits(CFG, {"fe_missing": 100})


def test_replace_frontend_without_bwlim_rejected():
    """frontend 存在但没配 shared bwlim：这是"漏配限速"，不能靠自动应用
    悄悄补上一行——那等于替运维决定了限速策略。"""
    cfg = CFG + "\nlisten fe_env_c\n    bind :8082\n    server web1 web:9000\n"
    with pytest.raises(E.EnforceError, match="找不到这些受控 frontend"):
        E.replace_limits(cfg, {"fe_env_c": 100})


def test_replace_ambiguous_multiple_bwlim_rejected():
    """一段里有多行 bwlim → 改哪行都是猜，宁可不动。"""
    cfg = CFG.replace(
        "    filter bwlim-out node-agg limit 5000000 key fe_name min-size 1460",
        "    filter bwlim-out a limit 5000000 key fe_name\n"
        "    filter bwlim-out b limit 6000000 key fe_name")
    with pytest.raises(E.EnforceError, match="多行 shared bwlim"):
        E.replace_limits(cfg, {"fe_env_a": 1})


def test_section_scoping_not_fooled_by_backend_of_same_name():
    """同名的 backend 段不该被误认成受控 frontend。"""
    cfg = ("backend fe_env_a\n"
           "    filter bwlim-out x limit 111 key k\n"
           "frontend fe_env_a\n"
           "    filter bwlim-out y limit 222 key k\n")
    assert E.parse_limits(cfg) == {"fe_env_a": 222}
    new, prev = E.replace_limits(cfg, {"fe_env_a": 333})
    assert prev == {"fe_env_a": 222}
    assert "limit 111" in new and "limit 333" in new


# ---------------------------------------------------------------------------
# reconcile：写盘 + 校验 + reload 的编排与失败路径
# ---------------------------------------------------------------------------

async def test_reconcile_applies_and_reloads(tmp_path):
    p = write(tmp_path)
    marker = tmp_path / "reloaded"
    en = E.HAProxyEnforcer(str(p), f"touch {marker}", validate_cmd="true")
    res = await en.reconcile({"fe_env_a": 2500000})
    assert res.ok and res.changed
    assert res.applied == {"fe_env_a": 2500000}
    assert res.previous == {"fe_env_a": 5000000}
    assert E.parse_limits(p.read_text()) == {"fe_env_a": 2500000, "fe_env_b": 3000000}
    assert marker.exists(), "应触发 reload"


async def test_reconcile_is_idempotent(tmp_path):
    """已一致时不写盘也不 reload——周期性 reconcile 才不会每 30 秒
    重启一次数据面。"""
    p = write(tmp_path)
    marker = tmp_path / "reloaded"
    en = E.HAProxyEnforcer(str(p), f"touch {marker}", validate_cmd="true")
    res = await en.reconcile({"fe_env_a": 5000000})
    assert res.ok and not res.changed
    assert not marker.exists(), "无变化时不应 reload"


async def test_reconcile_validation_failure_keeps_original(tmp_path):
    """`haproxy -c` 不过 → 绝不 reload，文件保持原样。把坏配置 reload 进
    生产会让整台机器的入口挂掉，比限额没改过去严重得多。"""
    p = write(tmp_path)
    marker = tmp_path / "reloaded"
    en = E.HAProxyEnforcer(str(p), f"touch {marker}", validate_cmd="false")
    res = await en.reconcile({"fe_env_a": 2500000})
    assert not res.ok and "配置校验失败" in res.error
    assert p.read_text() == CFG, "校验不过时原文件必须一字不动"
    assert not marker.exists(), "校验不过时绝不能 reload"


async def test_reconcile_reload_failure_rolls_back(tmp_path):
    """reload 失败时 HAProxy 仍按旧配置服务，磁盘上的 cfg 也要退回旧内容
    ——否则下次重启会悄悄用上一份从未验证过能 reload 的配置。"""
    p = write(tmp_path)
    en = E.HAProxyEnforcer(str(p), "false", validate_cmd="true")
    res = await en.reconcile({"fe_env_a": 2500000})
    assert not res.ok and "已回滚" in res.error
    assert p.read_text() == CFG


async def test_reconcile_leaves_no_temp_files(tmp_path):
    """失败路径不能在 cfg 目录里留下临时文件（运维看到一堆 .rl-limiter-*
    会以为出了大事）。"""
    p = write(tmp_path)
    en = E.HAProxyEnforcer(str(p), "true", validate_cmd="false")
    await en.reconcile({"fe_env_a": 2500000})
    leftovers = [f.name for f in tmp_path.iterdir() if f.name.startswith(".rl-limiter-")]
    assert leftovers == []


async def test_reconcile_preserves_file_mode(tmp_path):
    """原子替换后权限位要沿用原文件，别把 cfg 变成 0600 让别的工具读不了。"""
    p = write(tmp_path)
    p.chmod(0o644)
    en = E.HAProxyEnforcer(str(p), "true", validate_cmd="true")
    assert (await en.reconcile({"fe_env_a": 2500000})).ok
    assert p.stat().st_mode & 0o777 == 0o644


async def test_reconcile_empty_desired_is_noop(tmp_path):
    p = write(tmp_path)
    en = E.HAProxyEnforcer(str(p), "false", validate_cmd="false")
    res = await en.reconcile({})
    assert res.ok and not res.changed and p.read_text() == CFG


async def test_reconcile_unknown_frontend_reports_without_writing(tmp_path):
    p = write(tmp_path)
    en = E.HAProxyEnforcer(str(p), "true", validate_cmd="true")
    res = await en.reconcile({"fe_nope": 100})
    assert not res.ok and p.read_text() == CFG


async def test_current_limits_reads_from_disk(tmp_path):
    p = write(tmp_path)
    en = E.HAProxyEnforcer(str(p), "true")
    assert en.current_limits() == {"fe_env_a": 5000000, "fe_env_b": 3000000}
