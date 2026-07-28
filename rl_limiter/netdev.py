# rl_limiter.netdev —— 网卡计数器采样（/proc/net/dev）。
#
# **为什么这个模块必须存在**：监控视图要求"每秒流入/流出数据包数"与
# "每秒丢失入/出包数"，而 HAProxy 是 L4/L7 代理，只统计字节与连接，
# **完全不统计数据包**——`show stat` 的 205 列里没有任何一列是包计数，
# `show info` 也没有。唯一的来源是内核的网卡计数器。
#
# 这也是 rl-limiter 与 HAProxy 同机部署（v0.4 起的形态）才拿得到的数据：
# 跨机监控只有一条 stats socket，看不到对端的 /proc。
#
# **口径必须说清楚**（控制台上也标注了）：
#   - 网卡计数是**整机**的，包含 HAProxy 之外的一切流量（SSH、备份、
#     其它进程），与 HAProxy 的 bytes_in/bytes_out 不是同一口径；
#   - 网卡计数是**链路层**的，含以太网/IP/TCP 头部与重传，因此比
#     HAProxy 的应用层字节数偏大（典型 3%~8%）；
#   - 网卡计数**无法按 frontend 拆分**——内核不知道一个包属于哪个
#     监听端口的哪次会话。因此包相关的曲线只出现在"实例"视图。
#
# 采样是同步读一个几百字节的 procfs 文件（约几十微秒），放在事件循环里
# 直接调用即可，不值得为它开线程池。

from __future__ import annotations

import logging

from . import model

PROC_NET_DEV = "/proc/net/dev"
# 默认路由所在的网卡即"对外那张网卡"，也就是 HAProxy 流量真正走的那张。
PROC_NET_ROUTE = "/proc/net/route"


class NetdevError(RuntimeError):
    """网卡计数器不可读（非 Linux、procfs 未挂载、网卡名不存在等）。"""


def read_nic_stat(iface: str, path: str = PROC_NET_DEV) -> model.NicStat:
    """读取指定网卡的一行计数器。

    /proc/net/dev 的格式是固定的两行表头 + 每网卡一行，形如：

        eth0: 1373664 6485 0 19 0 0 0 0  53914459 6685 0 0 0 0 0 0

    冒号后依次是 receive 的 8 个字段（bytes packets errs drop fifo frame
    compressed multicast）与 transmit 的 8 个字段（bytes packets errs drop
    fifo colls carrier compressed）。列顺序自 Linux 2.x 起未变过，这里按
    位置解析是安全的——procfs 的这个文件没有表头可依赖，只有注释行。
    """
    try:
        with open(path, encoding="ascii", errors="replace") as f:
            text = f.read()
    except OSError as e:
        raise NetdevError(f"netdev: 读取 {path} 失败: {e}") from e

    for line in text.split("\n"):
        name, sep, rest = line.partition(":")
        if not sep:
            continue  # 两行表头没有冒号
        if name.strip() != iface:
            continue
        cols = rest.split()
        if len(cols) < 16:
            raise NetdevError(
                f"netdev: {path} 中 {iface} 的字段数异常（期望 16，实得 "
                f"{len(cols)}）: {line.strip()}")
        try:
            v = [int(c) for c in cols[:16]]
        except ValueError as e:
            raise NetdevError(f"netdev: {iface} 计数器非法: {e}") from e
        return model.NicStat(
            iface=iface,
            rx_bytes=v[0], rx_packets=v[1], rx_errs=v[2], rx_dropped=v[3],
            tx_bytes=v[8], tx_packets=v[9], tx_errs=v[10], tx_dropped=v[11],
        )

    raise NetdevError(f"netdev: {path} 中没有网卡 {iface}")


def default_iface(route_path: str = PROC_NET_ROUTE,
                  dev_path: str = PROC_NET_DEV) -> str:
    """猜出"对外那张网卡"：默认路由的出口网卡。

    先看 /proc/net/route 里目的地址为 0.0.0.0（十六进制 "00000000"）的
    那条路由；取不到时回退为 /proc/net/dev 里第一张非 lo 的网卡。两条
    都不成立就返回空串，由调用方决定是否放弃包统计（不该因此让整个
    监控停摆）。
    """
    try:
        with open(route_path, encoding="ascii", errors="replace") as f:
            for line in f.read().split("\n")[1:]:  # 首行是表头
                cols = line.split()
                # 字段：Iface Destination Gateway Flags ...
                if len(cols) >= 2 and cols[1] == "00000000":
                    return cols[0]
    except OSError:
        pass

    try:
        with open(dev_path, encoding="ascii", errors="replace") as f:
            for line in f.read().split("\n"):
                name, sep, _rest = line.partition(":")
                name = name.strip()
                if sep and name and name != "lo":
                    return name
    except OSError:
        pass
    return ""


def resolve_iface(configured: str, log: logging.Logger | None = None) -> str:
    """定下本次运行要采样哪张网卡。

    configured 非空即以它为准（运维显式指定，多网卡机器上必须能指定）；
    为空则自动探测。探测失败返回空串——此时包统计整体缺席，其余监控
    照常工作。
    """
    log = log if log is not None else logging.getLogger(__name__)
    if configured:
        return configured
    iface = default_iface()
    if iface:
        log.info(
            "未显式指定网卡，已自动选用默认路由的出口网卡采集数据包统计"
            "（整机口径，含 HAProxy 之外的流量） nic=%s", iface)
    else:
        log.warning(
            "未能确定要采样的网卡，实例视图的数据包/丢包曲线将为空"
            "（其余监控不受影响）；可用环境变量 RL_NIC 显式指定网卡名")
    return iface
