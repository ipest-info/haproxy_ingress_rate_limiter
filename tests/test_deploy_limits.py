# tests.test_deploy_limits —— 部署文件之间的数值一致性护栏。
#
# haproxy-base.cfg 的注释里写着"改 maxconn 或 maxpipes 时记得同步改
# compose 的 ulimits"。注释拦不住人。这两处一旦对不上，症状是容器起不来
# 并无限重启（HAProxy 拒绝启动），要翻日志才能看出是 FD 不够。
#
# 另一条更隐蔽：cfg 里的 `backlog` 被 net.core.somaxconn 封顶，对不上的话
# **不报错也不重启**，只是配置写的值静默失效，高并发时表现为客户端偶发
# 连接超时。所以调优表里的 somaxconn 目标值必须跟得上 cfg 里的 backlog。

from __future__ import annotations

import pathlib
import re

import yaml

ROOT = pathlib.Path(__file__).resolve().parents[1]
BASE_CFG = ROOT / "deploy/docker/haproxy-base.cfg"
COMPOSE = ROOT / "docker-compose.yml"
TUNE = ROOT / "deploy/sysctl/tune-kernel.sh"


def cfg_directive(name: str) -> int | None:
    """取 haproxy-base.cfg 里某条指令的数值（跳过注释行）。"""
    pat = re.compile(rf"^\s*{name}\s+(\d+)\s*$")
    for line in BASE_CFG.read_text(encoding="utf-8").splitlines():
        m = pat.match(line)
        if m:
            return int(m.group(1))
    return None


def tune_target(key: str) -> str:
    """取调优表里某个 sysctl 的目标值（表格式 key|mode|value|说明）。"""
    for line in TUNE.read_text(encoding="utf-8").splitlines():
        parts = line.split("|")
        if len(parts) >= 3 and parts[0] == key:
            return parts[2]
    raise AssertionError(f"调优表里找不到 {key}")


def node_services() -> dict:
    doc = yaml.safe_load(COMPOSE.read_text(encoding="utf-8"))
    return {n: s for n, s in doc["services"].items() if n.startswith("node")}


def test_compose_nofile_covers_haproxy_fd_need():
    """FD 需求 = maxconn×2 + maxpipes×2 + 34（管道那项是 splice 用的）。

    给不够 HAProxy **拒绝启动**：
        [ALERT] Cannot raise FD limit to 400034, limit is 4096.
    """
    maxconn = cfg_directive("maxconn")
    maxpipes = cfg_directive("maxpipes")
    assert maxconn and maxpipes, "基线 cfg 里应显式写死这两项"
    need = maxconn * 2 + maxpipes * 2 + 34

    services = node_services()
    assert services, "compose 里应有 node* 服务"
    for name, svc in services.items():
        nofile = svc["ulimits"]["nofile"]
        for which in ("soft", "hard"):
            assert nofile[which] >= need, (
                f"{name} 的 ulimits.nofile.{which}={nofile[which]} 不够 "
                f"{need}（maxconn={maxconn} maxpipes={maxpipes}）"
            )


def test_somaxconn_target_covers_configured_backlog():
    """cfg 里写的 backlog 会被 net.core.somaxconn 静默封顶。

    实测：somaxconn=4096 时，listen(backlog=65536) 的实际队列就是 4096
    （`ss -lnt` 的 Send-Q 列），内核不给任何提示。所以调优表的目标值必须
    不低于 cfg 里的 backlog，否则那行配置等于白写。
    """
    backlog = cfg_directive("backlog")
    assert backlog, "基线 cfg 的 defaults 里应显式写 backlog"
    assert int(tune_target("net.core.somaxconn")) >= backlog


def test_nr_open_target_covers_container_nofile():
    """fs.nr_open 是单进程 RLIMIT_NOFILE 的硬天花板，**容器里改不动**。

    compose 里的 nofile 超过它的话，容器根本起不来（runc 设 rlimit 就失败），
    而这一项只能在宿主机上调。
    """
    nr_open = int(tune_target("fs.nr_open"))
    for name, svc in node_services().items():
        assert svc["ulimits"]["nofile"]["hard"] <= nr_open, (
            f"{name} 的 nofile 超过了 fs.nr_open 目标值 {nr_open}；"
            "要么调低 maxconn，要么在宿主机上把 fs.nr_open 抬上去"
        )
