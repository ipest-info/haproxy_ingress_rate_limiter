# tests.test_enforcer —— 配置下发（渲染受管区块 + 写 haproxy.cfg + reload）。
#
# 这是全项目唯一会写盘并对生产数据面下命令的模块，所以测试的重点不是
# "渲染对了没有"，而是**每条失败路径都不留下烂摊子**：校验不过不 reload、
# reload 失败要回滚、标记之外的内容一个字节都不许动。
#
# 用假的校验/reload 命令（true/false）替代真 haproxy，测试不依赖环境里
# 装没装 haproxy；生成的配置能被真实 HAProxy 2.8.16 接受一事另行实测过。

from __future__ import annotations

import pytest

from rl_limiter import enforcer as E
from rl_limiter import model

# 标记之外有运维手写的内容——每个用例都要确认它们原样保留。
BASE = """\
global
    stats socket /run/haproxy/admin.sock mode 660 level user
    hard-stop-after 15s

defaults
    log global

backend hand_written
    server x 10.9.9.9:80
"""


def fe(name="fe_main", port=8080, quota=40_000_000, servers=None, **kw):
    return model.FrontendConfig(
        name=name, bind_port=port, quota_bits_per_sec=quota,
        servers=servers if servers is not None else
        [model.ServerEntry(name="web1", address="10.0.0.21", port=9000)],
        **kw)


def write(tmp_path, text=BASE):
    p = tmp_path / "haproxy.cfg"
    p.write_text(text, encoding="utf-8")
    return p


# ---------------------------------------------------------------------------
# 渲染（纯函数）
# ---------------------------------------------------------------------------

def test_render_contains_listen_bwlim_and_servers():
    block = E.render_block([fe(servers=[
        model.ServerEntry(name="web1", address="10.0.0.21", port=9000),
        model.ServerEntry(name="web2", address="10.0.0.22", port=9000,
                          weight=50, check=False)])])
    assert "listen fe_main" in block
    assert "bind :8080" in block
    # 限额换算：40 Mbps ÷ 8 = 5_000_000 bytes/s
    # 40 Mbps ÷ 8 = 5_000_000 bytes/s；min-size 按限额比例取（5_000_000/1000）
    assert "filter bwlim-out rl-limit limit 5000000 key fe_name min-size 5000" in block
    assert "tcp-request content set-bandwidth-limit rl-limit" in block
    # 不开 contstats，TCP 长连接的 bytes_out 只在会话结束时跳变，监控不可用。
    assert "option contstats" in block
    assert "server web1 10.0.0.21:9000 weight 100 check inter 2000ms" in block
    assert "server web2 10.0.0.22:9000 weight 50" in block
    assert "check" not in block.split("server web2")[1]


def test_render_is_deterministic():
    """幂等 reconcile 依赖这一点：同样输入必须得到同样字节，否则每轮都
    会认为"有变化"而反复 reload。"""
    assert E.render_block([fe()]) == E.render_block([fe()])


def test_render_bind_address_and_maxconn():
    block = E.render_block([fe(bind_address="127.0.0.1", maxconn=2000)])
    assert "bind 127.0.0.1:8080" in block
    assert "maxconn 2000" in block
    # maxconn=0 表示"不写该指令"，沿用 global/defaults。
    assert "maxconn" not in E.render_block([fe(maxconn=0)])


@pytest.mark.parametrize("bad,match", [
    (fe(name="fe bad"), "含非法字符"),
    (fe(servers=[model.ServerEntry(name="s;rm -rf /", address="1.1.1.1", port=1)]),
     "含非法字符"),
    (fe(servers=[model.ServerEntry(name="s", address="1.1.1.1\n    acl x", port=1)]),
     "含非法字符"),
    (fe(quota=4), "不足"),
])
def test_render_refuses_injection_and_bad_values(bad, match):
    """渲染前的白名单复核：这些值会被原样写进配置文件，放行任意字符
    等于允许通过配置库往 haproxy.cfg 注入指令。"""
    with pytest.raises(E.EnforceError, match=match):
        E.render_block([bad])


# ---------------------------------------------------------------------------
# 区块拼接
# ---------------------------------------------------------------------------

def test_splice_appends_when_no_marker():
    """首次接入一台已有的 HAProxy 不需要先手工改配置：区块自动追加。"""
    out = E.splice_block(BASE, E.render_block([fe()]))
    assert out.startswith(BASE)
    assert E.BEGIN_MARKER in out and E.END_MARKER in out


def test_splice_replaces_existing_block_and_preserves_outside():
    once = E.splice_block(BASE, E.render_block([fe(quota=40_000_000)]))
    twice = E.splice_block(once, E.render_block([fe(quota=8_000_000)]))
    assert twice.count(E.BEGIN_MARKER) == 1, "区块不应重复追加"
    assert "limit 1000000" in twice and "limit 5000000" not in twice
    # 标记之外的手写内容一字未动。
    assert "backend hand_written" in twice
    assert twice.split(E.BEGIN_MARKER)[0] == once.split(E.BEGIN_MARKER)[0]


def test_splice_is_idempotent():
    block = E.render_block([fe()])
    once = E.splice_block(BASE, block)
    assert E.splice_block(once, block) == once


# ---------------------------------------------------------------------------
# reconcile：写盘 + 校验 + reload 的编排与失败路径
# ---------------------------------------------------------------------------

async def test_reconcile_writes_and_reloads(tmp_path):
    p = write(tmp_path)
    marker = tmp_path / "reloaded"
    en = E.HAProxyEnforcer(str(p), f"touch {marker}", validate_cmd="true")
    res = await en.reconcile([fe()])
    assert res.ok and res.changed and res.frontends == ["fe_main"]
    assert "listen fe_main" in p.read_text()
    assert marker.exists(), "应触发 reload"


async def test_reconcile_is_idempotent(tmp_path):
    """已一致时不写盘也不 reload——周期性 reconcile 才不会每 30 秒重启
    一次数据面。"""
    p = write(tmp_path)
    marker = tmp_path / "reloaded"
    en = E.HAProxyEnforcer(str(p), f"touch {marker}", validate_cmd="true")
    assert (await en.reconcile([fe()])).changed is True
    marker.unlink()
    res = await en.reconcile([fe()])
    assert res.ok and not res.changed
    assert not marker.exists(), "无变化时不应 reload"


async def test_reconcile_validation_failure_keeps_original(tmp_path):
    """`haproxy -c` 不过 → 绝不 reload，文件保持原样。把坏配置 reload 进
    生产会让整台机器的入口挂掉，比配置没改过去严重得多。"""
    p = write(tmp_path)
    marker = tmp_path / "reloaded"
    en = E.HAProxyEnforcer(str(p), f"touch {marker}", validate_cmd="false")
    res = await en.reconcile([fe()])
    assert not res.ok and "配置校验失败" in res.error
    assert p.read_text() == BASE, "校验不过时原文件必须一字不动"
    assert not marker.exists(), "校验不过时绝不能 reload"


async def test_reconcile_reload_failure_rolls_back(tmp_path):
    """reload 失败时 HAProxy 仍按旧配置服务，磁盘上的 cfg 也要退回旧内容
    ——否则下次重启会悄悄用上一份从未验证过能 reload 的配置。"""
    p = write(tmp_path)
    en = E.HAProxyEnforcer(str(p), "false", validate_cmd="true")
    res = await en.reconcile([fe()])
    assert not res.ok and "已回滚" in res.error
    assert p.read_text() == BASE


async def test_reconcile_leaves_no_temp_files(tmp_path):
    """失败路径不能在 cfg 目录里留下临时文件。"""
    p = write(tmp_path)
    en = E.HAProxyEnforcer(str(p), "true", validate_cmd="false")
    await en.reconcile([fe()])
    assert [f.name for f in tmp_path.iterdir()
            if f.name.startswith(".rl-limiter-")] == []


async def test_reconcile_preserves_file_mode(tmp_path):
    """原子替换后权限位要沿用原文件，别把 cfg 变成 0600 让别的工具读不了。"""
    p = write(tmp_path)
    p.chmod(0o644)
    en = E.HAProxyEnforcer(str(p), "true", validate_cmd="true")
    assert (await en.reconcile([fe()])).ok
    assert p.stat().st_mode & 0o777 == 0o644


async def test_reconcile_empty_list_refused(tmp_path):
    """空清单会把受管区块写空 = 摘掉全部监听端口。那是事故不是配置操作。"""
    p = write(tmp_path)
    en = E.HAProxyEnforcer(str(p), "false", validate_cmd="false")
    res = await en.reconcile([])
    assert not res.ok and "全部监听端口" in res.error
    assert p.read_text() == BASE


async def test_current_block_reads_from_disk(tmp_path):
    p = write(tmp_path)
    en = E.HAProxyEnforcer(str(p), "true", validate_cmd="true")
    assert en.current_block() is None
    await en.reconcile([fe()])
    assert "listen fe_main" in (en.current_block() or "")


# ---------------------------------------------------------------------------
# min-size：整形的 CPU 开销主要由它决定（见 enforcer 模块的实测数据）
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("limit_bytes,expect", [
    (125_000,       1460),    # 1 Mbps：按比例算只有 125，夹到下限
    (1_000_000,     1460),    # 8 Mbps：1000 < 下限
    (5_000_000,     5000),    # 40 Mbps
    (20_000_000,   20000),    # 160 Mbps
    (100_000_000,  65536),    # 800 Mbps：100000 > 上限，夹到上限
    (10_000_000_000, 65536),  # 极大限额也不超过上限
])
def test_min_size_scales_with_limit(limit_bytes, expect):
    """min-size 太小 → 放行次数多、CPU 高（实测 1460 比 65536 贵 3 倍）；
    太大 → 相对每秒配额过大，放不满额度（实测 1MB/s 配 64KB 只跑到 74%）。
    因此按限额比例取值并夹在 [1460, 65536]。"""
    assert E.bwlim_min_size(limit_bytes) == expect


def test_min_size_appears_in_rendered_block():
    block = E.render_block([fe(quota=160_000_000)])   # 160 Mbps = 20 MB/s
    assert "min-size 20000" in block
