# tests.test_main_env —— 节点入口的环境变量护栏（客户侧/后端侧网卡相斥）。
#
# 双网卡形态下入向限速作用在客户侧网卡上；后端侧网卡若与之相同，限速会
# 连带卡住后端回程流量——这类"配置看着对、限速却伤了别处"的错误必须在
# 启动时大声失败。

from __future__ import annotations

from rl_limiter.__main__ import ENV_BACKEND_IFACE, _nic_role_conflict


def test_same_nic_for_both_roles_is_a_conflict():
    msg = _nic_role_conflict("eth0", "eth0")
    assert msg
    assert "eth0" in msg and ENV_BACKEND_IFACE in msg


def test_distinct_nics_are_fine():
    assert _nic_role_conflict("eth0", "eth1") == ""
    assert _nic_role_conflict("ens33", "ens34") == ""


def test_unregistered_backend_skips_the_check():
    """老部署的 env 没有 RL_BACKEND_IFACE：行为不变，不做校验。"""
    assert _nic_role_conflict("eth0", "") == ""


def test_ingress_disabled_has_nothing_to_conflict_with():
    """RL_TC_IN_IFACE=- 时没有客户侧执行网卡，后端侧写什么都不冲突。"""
    assert _nic_role_conflict("-", "eth0") == ""
