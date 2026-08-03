# tests.test_netlimit_discover —— 本机网卡发现。
#
# 这个模块决定"限哪几张网卡"，而限错网卡的后果不是限速不准，是**把容器
# 网络或回环流量一起掐住**。所以测试的重点全在排除规则上：每一条排除都
# 有具体理由，每一条都得有用例盯着。
#
# 另一个重点是 IP 数量与限速树**无关**——单网卡挂 10 个地址和挂 1 个，
# 生成的队列树必须一模一样。这是"整机一个总闸门"的直接推论，也是这套
# 方案相对按 IP 限速最省事的地方。

from __future__ import annotations

from netlimit import discover as dis

# 真实的 `ip -o addr show` 输出片段（含 IPv6、链路本地、多地址）。
# 样例必须照真实输出写：自洽的假数据会把解析上的错误盖住。
IP_OUT = """\
1: lo    inet 127.0.0.1/8 scope host lo\\       valid_lft forever preferred_lft forever
1: lo    inet6 ::1/128 scope host \\       valid_lft forever preferred_lft forever
2: eth0    inet 10.0.0.5/24 brd 10.0.0.255 scope global eth0\\       valid_lft forever preferred_lft forever
2: eth0    inet 10.0.0.6/24 brd 10.0.0.255 scope global secondary eth0\\       valid_lft forever preferred_lft forever
2: eth0    inet 169.254.3.4/16 brd 169.254.255.255 scope link eth0\\       valid_lft forever preferred_lft forever
2: eth0    inet6 2001:db8::5/64 scope global \\       valid_lft forever preferred_lft forever
2: eth0    inet6 fe80::1/64 scope link \\       valid_lft forever preferred_lft forever
3: eth1    inet 10.9.0.9/24 brd 10.9.0.255 scope global eth1\\       valid_lft forever preferred_lft forever
4: docker0    inet 172.17.0.1/16 brd 172.17.255.255 scope global docker0\\       valid_lft forever preferred_lft forever
"""


def fake_sysfs(tmp_path, links: dict[str, dict]):
    """造一个假的 /sys/class/net。真实的那个读不到测试想要的组合。"""
    for name, attrs in links.items():
        d = tmp_path / name
        d.mkdir()
        (d / "operstate").write_text(attrs.get("operstate", "up"))
        (d / "carrier").write_text(attrs.get("carrier", "1"))
        if attrs.get("physical", True):
            (d / "device").mkdir()
    return str(tmp_path)


# ---------------------------------------------------------------------------
# ip -o addr 解析
# ---------------------------------------------------------------------------

def test_parses_multiple_addresses_per_link():
    """单网卡多 IP 是本方案要对付的两种形态之一，得完整解出来。"""
    got = dis.parse_ip_addr(IP_OUT)
    v4, v6 = got["eth0"]
    assert v4 == ["10.0.0.5", "10.0.0.6"]
    assert v6 == ["2001:db8::5"]


def test_drops_link_local_addresses():
    """169.254/16 与 fe80::/10 不代表这张网卡真的在承载业务流量。

    留着它们的话，一张什么都没配的网卡会因为自带 fe80:: 而被判成"有地址"，
    于是被纳入限速——"限了几张网卡"这个数字就再也不能信了。
    """
    v4, v6 = dis.parse_ip_addr(IP_OUT)["eth0"]
    assert not any(a.startswith("169.254.") for a in v4)
    assert not any(a.lower().startswith("fe80:") for a in v6)


def test_parse_survives_trailing_colon_on_name():
    """iproute2 有的版本会在网卡名后带冒号。带不带都得认。"""
    got = dis.parse_ip_addr("2: eth0:    inet 10.0.0.5/24 scope global eth0")
    assert got["eth0"][0] == ["10.0.0.5"]


def test_parse_ignores_junk_lines():
    assert dis.parse_ip_addr("") == {}
    assert dis.parse_ip_addr("garbage\n2: eth0\n") == {}


# ---------------------------------------------------------------------------
# 排除规则
# ---------------------------------------------------------------------------

def test_excludes_loopback_bridges_and_veth(tmp_path):
    """回环、Docker 网桥、veth 一律不限。

    网桥/veth 上的流量出公网时还要再经过物理网卡，在这里限一次等于**同一
    份字节被限两遍**；回环则纯粹是自伤。
    """
    sysfs = fake_sysfs(tmp_path, {
        "lo": {}, "eth0": {}, "docker0": {"physical": False},
        "br-abc123": {"physical": False}, "veth9f2": {"physical": False},
    })
    topo = dis.discover(IP_OUT, names=["lo", "eth0", "docker0", "br-abc123",
                                       "veth9f2"], sysfs=sysfs)
    assert [x.name for x in topo.shaped] == ["eth0"]
    skipped = {x.name: x.skip_reason for x in topo.skipped}
    assert all(skipped[n] for n in ("lo", "docker0", "br-abc123", "veth9f2"))


def test_excludes_the_ifb_device_itself(tmp_path):
    """聚合用的 IFB 绝不能被当成待限网卡。

    限它 = 把已经整形过的流量再整形一遍，实际吞吐掉到限额一半以下；而且
    重定向会指向自己。plan.check 那边还有一道兜底，这里是第一道。
    """
    sysfs = fake_sysfs(tmp_path, {"eth0": {}, "ifb0": {"physical": False}})
    topo = dis.discover(IP_OUT + "9: ifb0    inet 10.5.5.5/24 scope global ifb0\n",
                        names=["eth0", "ifb0"], sysfs=sysfs)
    assert [x.name for x in topo.shaped] == ["eth0"]


def test_excludes_bond_slaves(tmp_path):
    """bond 成员口要排除：调度发生在主设备上，两个成员各限一份 = 两倍额度。

    名字上看不出来（成员口就叫 eth0/eth1），只能看 /sys 里的 master。
    """
    sysfs = fake_sysfs(tmp_path, {"eth0": {}, "bond0": {"physical": False}})
    (tmp_path / "bond0" / "bonding").mkdir()
    (tmp_path / "eth0" / "master").symlink_to(tmp_path / "bond0")
    ip_out = (IP_OUT + "5: bond0    inet 10.7.0.1/24 scope global bond0\n")
    topo = dis.discover(ip_out, names=["eth0", "bond0"], sysfs=sysfs)
    names = [x.name for x in topo.shaped]
    assert names == ["bond0"], f"该限主设备而不是成员口：{names}"
    assert "bond" in dict((x.name, x.skip_reason) for x in topo.skipped)["eth0"]


def test_excludes_down_and_addressless_links(tmp_path):
    sysfs = fake_sysfs(tmp_path, {
        "eth0": {}, "eth1": {"operstate": "down"}, "eth2": {}})
    # eth2 在 ip 输出里没有任何地址
    topo = dis.discover(IP_OUT, names=["eth0", "eth1", "eth2"], sysfs=sysfs)
    assert [x.name for x in topo.shaped] == ["eth0"]
    reasons = {x.name: x.skip_reason for x in topo.skipped}
    assert "down" in reasons["eth1"]
    assert "地址" in reasons["eth2"]


def test_only_overrides_every_automatic_rule(tmp_path):
    """--only 是手动出口：自动判断再周全也有猜错的时候。

    显式点名的网卡即使叫 docker0、即使是 down 的，也照限——运维说了算。
    """
    sysfs = fake_sysfs(tmp_path, {"eth0": {}, "docker0": {"physical": False}})
    topo = dis.discover(IP_OUT, names=["eth0", "docker0"],
                        include=("docker0",), sysfs=sysfs)
    assert [x.name for x in topo.shaped] == ["docker0"]


def test_explicit_exclude(tmp_path):
    sysfs = fake_sysfs(tmp_path, {"eth0": {}, "eth1": {}})
    topo = dis.discover(IP_OUT, names=["eth0", "eth1"],
                        exclude=("eth1",), sysfs=sysfs)
    assert [x.name for x in topo.shaped] == ["eth0"]


# ---------------------------------------------------------------------------
# 架构分叉：要不要 IFB
# ---------------------------------------------------------------------------

def test_single_nic_does_not_need_ifb(tmp_path):
    sysfs = fake_sysfs(tmp_path, {"lo": {}, "eth0": {}})
    topo = dis.discover(IP_OUT, names=["lo", "eth0"], sysfs=sysfs)
    assert not topo.needs_ifb, "一张网卡直接挂 HTB 就够，引入 IFB 是白担风险"


def test_two_nics_need_ifb(tmp_path):
    """两张网卡各挂各的 HTB = 两份额度，与"整机一个总闸门"直接矛盾。"""
    sysfs = fake_sysfs(tmp_path, {"eth0": {}, "eth1": {}})
    topo = dis.discover(IP_OUT, names=["eth0", "eth1"], sysfs=sysfs)
    assert topo.needs_ifb


def test_many_ips_on_one_nic_still_one_nic(tmp_path):
    """**单网卡多 IP 不需要 IFB。**

    地址数量与限速架构完全无关：整机限速不按 IP 分类，10 个地址和 1 个
    地址走的是同一个闸门。把"多 IP"错当成"要聚合"会白白引入 IFB 那一
    整套机制和它的风险。
    """
    sysfs = fake_sysfs(tmp_path, {"eth0": {}})
    topo = dis.discover(IP_OUT, names=["eth0"], sysfs=sysfs)
    (link,) = topo.shaped
    assert link.addr_count == 3, "3 个地址（2 个 v4 + 1 个 v6）"
    assert not topo.needs_ifb
