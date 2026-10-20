# tests.test_cfgparse —— haproxy.cfg 解析与限额合并。
#
# cfg 是负载均衡配置的唯一权威，解析口径刻意保守（见 cfgparse 模块头）：
# 这里钉住的是与限速/监控直接相关的行为——段识别、bind 端口提取、mode
# 继承、与 YAML quotas 的合并规则、以及各类配置错位时的告警（而非崩溃）。

from __future__ import annotations

import asyncio
import logging

import pytest

from rl_limiter import cfgparse, model

log = logging.getLogger("t.cfgparse")

CFG = """\
global
    daemon
    stats socket /run/haproxy/admin.sock mode 660 level user

defaults
    mode tcp
    timeout connect 5s

frontend stats
    bind :8404
    mode http

listen fe_main
    bind :8080
    option contstats
    server web1 10.0.0.21:9000 check

listen fe_api
    bind 127.0.0.1:8081
    mode http
    server api1 10.0.0.31:8000

frontend fe_edge
    bind ipv4@0.0.0.0:8082
    default_backend be_edge

backend be_edge
    server web2 10.0.0.22:9000
"""


# ---------------------------------------------------------------------------
# 解析
# ---------------------------------------------------------------------------

def test_parse_sections_and_binds():
    secs = {s.name: s for s in cfgparse.parse_haproxy_cfg(CFG)}
    # backend 段没有监听端口，不该出现在解析结果里。
    assert set(secs) == {"stats", "fe_main", "fe_api", "fe_edge"}
    assert secs["fe_main"].binds == [("", 8080)]
    assert secs["fe_api"].binds == [("127.0.0.1", 8081)]
    # ipv4@0.0.0.0 属于"所有地址"，剥掉前缀与通配后归一为空串。
    assert secs["fe_edge"].binds == [("", 8082)]


def test_mode_inherits_from_defaults():
    secs = {s.name: s for s in cfgparse.parse_haproxy_cfg(CFG)}
    assert secs["fe_main"].mode == "tcp"     # 继承 defaults
    assert secs["fe_api"].mode == "http"     # 段内显式值优先


def test_mode_defaults_to_tcp_without_defaults_section():
    secs = cfgparse.parse_haproxy_cfg("frontend fe_a\n    bind :1\n")
    assert secs[0].mode == "tcp"


@pytest.mark.parametrize("spec,expect", [
    (":8080", ("", 8080)),
    ("10.0.0.1:8080", ("10.0.0.1", 8080)),
    ("*:8080", ("", 8080)),
    ("ipv4@10.0.0.1:8080", ("10.0.0.1", 8080)),
    ("[::]:8080", ("", 8080)),
    ("/run/frontend.sock", None),          # unix bind：没有端口
    ("unix@/run/frontend.sock", None),
    ("abns@name", None),
])
def test_split_bind_endpoint(spec, expect):
    assert cfgparse._split_bind_endpoint(spec) == expect


def test_unparseable_lines_are_ignored():
    """解析失败宁可少不可错：看不懂的行忽略，语法校验是 haproxy -c 的事。"""
    secs = cfgparse.parse_haproxy_cfg(
        "listen fe_a\n"
        "    bind\n"                       # 缺参数
        "    tcp-request connection reject if { src 1.2.3.4 }\n"
        "    bind :9090\n")
    assert secs[0].binds == [("", 9090)]


# ---------------------------------------------------------------------------
# 与 quotas 的合并
# ---------------------------------------------------------------------------

def build(quotas, cfg=CFG, warned=None):
    return cfgparse.build_frontends(
        cfgparse.parse_haproxy_cfg(cfg), quotas, log, warned)


def test_merge_quota_and_monitor_only():
    fes = {f.name: f for f in build({"fe_main": 40})}
    # stats 段被跳过；其余段全部纳入监控。
    assert set(fes) == {"fe_main", "fe_api", "fe_edge"}
    assert fes["fe_main"].quota_mbps == 40 and fes["fe_main"].limited
    # 未登记限额 → 只监控不限速。
    assert fes["fe_api"].quota_mbps == 0 and not fes["fe_api"].limited


def test_unregistered_frontend_notes_once_at_info(caplog):
    """端口默认不限速：未登记限额只记 info（不是 warning——那是正常
    状态），且同一集合内只记一次不刷屏。"""
    warned: set[str] = set()
    with caplog.at_level(logging.INFO, logger="t.cfgparse"):
        build({"fe_main": 40}, warned=warned)
        notes = [r for r in caplog.records if "默认不限速" in r.getMessage()]
        assert len(notes) == 2              # fe_api、fe_edge 各一条
        assert all(r.levelno == logging.INFO for r in notes)
        build({"fe_main": 40}, warned=warned)   # 同一集合：不再刷屏
        assert len([r for r in caplog.records
                    if "默认不限速" in r.getMessage()]) == 2


def test_explicit_zero_quota_is_silent_monitor_only(caplog):
    """quotas 显式写 0 = 运维确认过的不限速：行为与未登记相同（不建 tc
    类），连 info 也不记——那条 info 是给"核对到底限了谁"准备的。"""
    with caplog.at_level(logging.INFO, logger="t.cfgparse"):
        fes = {f.name: f for f in build({"fe_main": 40, "fe_api": 0,
                                         "fe_edge": 0})}
    assert not fes["fe_api"].limited and not fes["fe_edge"].limited
    assert not [r for r in caplog.records if "默认不限速" in r.getMessage()]


def test_orphan_quota_warns_and_is_ignored(caplog):
    with caplog.at_level(logging.WARNING, logger="t.cfgparse"):
        fes = build({"fe_gone": 40})
    assert "fe_gone" not in {f.name for f in fes}
    assert [r for r in caplog.records if "不存在" in r.getMessage()]


def test_multi_bind_takes_first_port_and_warns(caplog):
    cfg = "listen fe_a\n    bind :8080\n    bind :8081\n"
    with caplog.at_level(logging.WARNING, logger="t.cfgparse"):
        fes = build({"fe_a": 8}, cfg=cfg)
    assert fes[0].bind_port == 8080
    assert [r for r in caplog.records if "多条 bind" in r.getMessage()]


def test_duplicate_section_name_takes_first(caplog):
    cfg = "listen fe_a\n    bind :8080\nlisten fe_a\n    bind :9090\n"
    with caplog.at_level(logging.WARNING, logger="t.cfgparse"):
        fes = build({}, cfg=cfg)
    assert [(f.name, f.bind_port) for f in fes] == [("fe_a", 8080)]
    assert [r for r in caplog.records if "重名" in r.getMessage()]


# ---------------------------------------------------------------------------
# 内容身份（变更检测）
# ---------------------------------------------------------------------------

def test_canonical_is_order_insensitive_and_quota_sensitive():
    a = model.FrontendConfig(name="fe_a", bind_port=1, quota_mbps=8)
    b = model.FrontendConfig(name="fe_b", bind_port=2)
    assert cfgparse.canonical([a, b]) == cfgparse.canonical([b, a])
    a2 = model.FrontendConfig(name="fe_a", bind_port=1, quota_mbps=9)
    assert cfgparse.canonical([a, b]) != cfgparse.canonical([a2, b])
    assert cfgparse.checksum([a, b]) != cfgparse.checksum([a2, b])


# ---------------------------------------------------------------------------
# watch：轮询热更新
# ---------------------------------------------------------------------------

def write_yaml(tmp_path, cfg_path, quotas_line="fe_main: 40"):
    y = tmp_path / "config.yaml"
    y.write_text(
        f"quotas:\n  {quotas_line}\n", encoding="utf-8")
    return y


async def run_watch_once(cfg_path, yaml_path, boot, interval_s=0.02, rounds=6):
    """跑几个轮询周期后取消，返回 queue 里收到的配置。"""
    q: asyncio.Queue = asyncio.Queue()
    task = asyncio.create_task(cfgparse.watch(
        str(cfg_path), str(yaml_path), q, boot, log, interval_s=interval_s))
    await asyncio.sleep(interval_s * rounds)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    out = []
    while not q.empty():
        out.append(q.get_nowait())
    return out


async def test_watch_pushes_on_quota_change(tmp_path):
    cfg_file = tmp_path / "haproxy.cfg"
    cfg_file.write_text("listen fe_main\n    bind :8080\n", encoding="utf-8")
    yaml_file = write_yaml(tmp_path, cfg_file)
    boot = cfgparse.load_frontends(str(cfg_file), {"fe_main": 40}, log)

    # 内容没变：不该有任何投递（mtime 变化也不触发——watch 比较解析结果）。
    yaml_file.touch()
    assert await run_watch_once(cfg_file, yaml_file, boot) == []

    # 改限额：一个周期内投出新配置。
    write_yaml(tmp_path, cfg_file, quotas_line="fe_main: 20")
    got = await run_watch_once(cfg_file, yaml_file, boot)
    assert got and got[-1].frontends[0].quota_mbps == 20
    assert got[-1].version == cfgparse.checksum(got[-1].frontends)


async def test_watch_picks_up_cfg_change(tmp_path):
    cfg_file = tmp_path / "haproxy.cfg"
    cfg_file.write_text("listen fe_main\n    bind :8080\n", encoding="utf-8")
    yaml_file = write_yaml(tmp_path, cfg_file)
    boot = cfgparse.load_frontends(str(cfg_file), {"fe_main": 40}, log)

    cfg_file.write_text(
        "listen fe_main\n    bind :8080\nlisten fe_new\n    bind :8081\n",
        encoding="utf-8")
    got = await run_watch_once(cfg_file, yaml_file, boot)
    assert got and {f.name for f in got[-1].frontends} == {"fe_main", "fe_new"}


async def test_watch_fail_static_on_unreadable_cfg(tmp_path, caplog):
    """cfg 被原子替换的瞬间读不到属于预期内抖动：保留当前配置，warn 后重试。"""
    cfg_file = tmp_path / "haproxy.cfg"
    cfg_file.write_text("listen fe_main\n    bind :8080\n", encoding="utf-8")
    yaml_file = write_yaml(tmp_path, cfg_file)
    boot = cfgparse.load_frontends(str(cfg_file), {"fe_main": 40}, log)

    cfg_file.unlink()
    with caplog.at_level(logging.WARNING, logger="t.cfgparse"):
        got = await run_watch_once(cfg_file, yaml_file, boot)
    assert got == []
    assert [r for r in caplog.records if "fail-static" in r.getMessage()]


async def test_watch_warns_on_wiring_change(tmp_path, caplog):
    """接线/运行参数是启动时定型的：改了要提示重启，而不是装作生效。"""
    cfg_file = tmp_path / "haproxy.cfg"
    cfg_file.write_text("listen fe_main\n    bind :8080\n", encoding="utf-8")
    yaml_file = write_yaml(tmp_path, cfg_file)
    boot = cfgparse.load_frontends(str(cfg_file), {"fe_main": 40}, log)

    async def change_log_level():
        await asyncio.sleep(0.06)
        yaml_file.write_text(
            "log_level: debug\nlvs:\n  name: lvs2\nquotas:\n"
            "  fe_main: 40\n", encoding="utf-8")

    with caplog.at_level(logging.WARNING, logger="t.cfgparse"):
        change = asyncio.ensure_future(change_log_level())
        await run_watch_once(cfg_file, yaml_file, boot, rounds=10)
        await change
    assert [r for r in caplog.records if "请重启" in r.getMessage()]


def test_checksum_is_instance_quota_sensitive():
    """实例总限速参与内容身份：只改 instance_quota_mbps 也必须触发一次
    热更（tc 要切层级模式），版本号不变的话监控循环会把它当同一份配置。"""
    a = model.FrontendConfig(name="fe_a", bind_port=1, quota_mbps=8)
    assert cfgparse.checksum([a]) != cfgparse.checksum([a], 100.0)
    assert cfgparse.checksum([a], 100.0) == cfgparse.checksum([a], 100.0)


async def test_watch_poke_triggers_immediate_reread(tmp_path):
    """写 API 回写 YAML 后 set poke 事件：watch 不等轮询周期立即重读。
    轮询间隔故意设得很长——没有 poke 的话本测试必然超时。"""
    cfg_file = tmp_path / "haproxy.cfg"
    cfg_file.write_text("listen fe_main\n    bind :8080\n", encoding="utf-8")
    yaml_file = write_yaml(tmp_path, cfg_file)
    boot = cfgparse.load_frontends(str(cfg_file), {"fe_main": 40}, log)

    q: asyncio.Queue = asyncio.Queue()
    poke = asyncio.Event()
    task = asyncio.create_task(cfgparse.watch(
        str(cfg_file), str(yaml_file), q, boot, log,
        interval_s=30.0, poke=poke))
    try:
        await asyncio.sleep(0.05)          # 等 watch 进入等待
        write_yaml(tmp_path, cfg_file, quotas_line="fe_main: 20")
        poke.set()
        got = await asyncio.wait_for(q.get(), 2)
        assert got.frontends[0].quota_mbps == 20
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task


async def test_watch_pushes_on_instance_quota_change(tmp_path):
    """只改 instance_quota_mbps（quotas/cfg 都没动）也要推出一份新配置。"""
    cfg_file = tmp_path / "haproxy.cfg"
    cfg_file.write_text("listen fe_main\n    bind :8080\n", encoding="utf-8")
    yaml_file = write_yaml(tmp_path, cfg_file)
    boot = cfgparse.load_frontends(str(cfg_file), {"fe_main": 40}, log)

    yaml_file.write_text(
        "instance_quota_mbps: 500\nquotas:\n  fe_main: 40\n",
        encoding="utf-8")
    got = await run_watch_once(cfg_file, yaml_file, boot)
    assert got and got[-1].instance_quota_mbps == 500.0


def test_checksum_is_request_limits_sensitive():
    """请求方向限速参与内容身份：只改 request_limits 也必须触发热更。"""
    a = model.FrontendConfig(name="fe_a", bind_port=1, quota_mbps=8)
    lim1 = {"fe_a": model.RequestLimit(mbps=10.0, conn_rate=100)}
    lim2 = {"fe_a": model.RequestLimit(mbps=10.0, conn_rate=200)}
    assert cfgparse.checksum([a], 0.0, lim1) != cfgparse.checksum([a], 0.0, lim2)
    assert cfgparse.checksum([a], 0.0, lim1) == cfgparse.checksum([a], 0.0, dict(lim1))
    assert cfgparse.checksum([a]) != cfgparse.checksum([a], 0.0, lim1)


async def test_watch_pushes_on_request_limits_change(tmp_path):
    """只改 request_limits（quotas/cfg 都没动）也要推出一份新配置。"""
    cfg_file = tmp_path / "haproxy.cfg"
    cfg_file.write_text("listen fe_main\n    bind :8080\n", encoding="utf-8")
    yaml_file = write_yaml(tmp_path, cfg_file)
    boot = cfgparse.load_frontends(str(cfg_file), {"fe_main": 40}, log)

    yaml_file.write_text(
        "quotas:\n  fe_main: 40\n"
        "request_limits:\n  fe_main: {mbps: 10, conn_rate: 200}\n",
        encoding="utf-8")
    got = await run_watch_once(cfg_file, yaml_file, boot)
    assert got and got[-1].request_limits["fe_main"].mbps == 10.0
    assert got[-1].request_limits["fe_main"].conn_rate == 200


# ---------------------------------------------------------------------------
# 受管清单来源注入（LVS 等非 haproxy.cfg 来源的通用入口）
# ---------------------------------------------------------------------------

def test_load_frontends_from_injected_loader_equals_file_path(tmp_path):
    """注入 loader 与读文件两条路产出完全相同的清单。"""
    text = "frontend fe_a\n    bind :8080\n    mode tcp\n"
    p = tmp_path / "haproxy.cfg"
    p.write_text(text, encoding="utf-8")
    log = logging.getLogger("t")
    via_file = cfgparse.load_frontends(str(p), {"fe_a": 40.0}, log)
    via_loader = cfgparse.load_frontends_from(
        lambda: cfgparse.parse_haproxy_cfg(text), {"fe_a": 40.0}, log)
    assert cfgparse.canonical(via_file) == cfgparse.canonical(via_loader)


def test_build_frontends_accepts_udp_mode():
    """IPVS 的虚拟服务可以是 UDP：mode 原样保留而不是回落 tcp。"""
    sec = cfgparse.ParsedSection(name="vs_10.0.0.1_53", kind="virtual_server",
                                 binds=[("10.0.0.1", 53)], mode="udp")
    fes = cfgparse.build_frontends([sec], {}, logging.getLogger("t"))
    assert fes[0].mode == "udp"


async def test_watch_uses_injected_section_loader(tmp_path):
    """watch 传 section_loader 时受管清单来自回调（cfg_path 只是日志
    标注）——LVS 分支接内核实况表的切口。"""
    yml = tmp_path / "config.yaml"
    yml.write_text(
        "quotas:\n  vs_10.0.0.1_8080: 40\n", encoding="utf-8")
    sections = [cfgparse.ParsedSection(
        name="vs_10.0.0.1_8080", kind="virtual_server",
        binds=[("10.0.0.1", 8080)], mode="tcp")]
    q: asyncio.Queue = asyncio.Queue()
    poke = asyncio.Event()
    task = asyncio.ensure_future(cfgparse.watch(
        "/nonexistent", str(yml), q, [], logging.getLogger("t"),
        interval_s=60, poke=poke, section_loader=lambda: list(sections)))
    try:
        poke.set()
        cfg = await asyncio.wait_for(q.get(), timeout=2)
        assert [f.name for f in cfg.frontends] == ["vs_10.0.0.1_8080"]
        assert cfg.frontends[0].quota_mbps == 40.0
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
