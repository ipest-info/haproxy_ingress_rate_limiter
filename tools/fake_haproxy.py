#!/usr/bin/env python3
# tools/fake_haproxy.py —— 本地演示/联调用的"假 HAProxy stats socket"。
#
# 用途：在没有真实 HAProxy 的开发机上模拟 v2.0 架构里的一台受控节点——
# 一个监听内网 TCP 的 stats socket（真实部署中对应 haproxy.cfg 的
# `stats socket ipv4@<内网IP>:9999 level admin`）。rl-limiter 连上来后：
#   - "show stat -1 1 -1"：返回带 "# " 列头的 CSV，其中各 frontend 的
#     bytes_out 计数器按 --frontends 指定的下行速率(Mbps) × 真实流逝时间持续增长
#     （附带 --jitter 抖动），scur 在 5~50 之间随机游走，模拟真实流量；
#   - "set map <path> <key> <value>" / "add map ..."：记录到内存 map 并在
#     值变化时打 info 日志（这就是观察 enforce 模式下发效果的窗口），
#     回包为空（与真实 HAProxy 成功时的行为一致）。
#
# 协议要点（与真实 runtime socket 一致）：非交互模式下一次连接只服务
# 一条命令，应答完即由服务端关闭连接——客户端每条命令都要重新拨号。
#
# 用法示例（模拟两台 HAProxy，见 docs/03-限速服务运行指南.md）：
#   python3 tools/fake_haproxy.py --port 19991 --frontends fe_env_a:16,fe_env_b:4
#   python3 tools/fake_haproxy.py --port 19992 --frontends fe_env_a:12

from __future__ import annotations

import argparse
import asyncio
import logging
import random
import time

log = logging.getLogger("fake_haproxy")


class FakeFrontend:
    """单个 frontend 的模拟状态：累计字节计数器 + 并发连接数。"""

    def __init__(self, name: str, rate_bps: float, jitter: float):
        self.name = name
        self.rate_bps = rate_bps      # 每秒增长的字节数（bytes/s）
        self.jitter = jitter          # 抖动幅度（0.2 = ±20%）
        self.bytes_out = 0            # 累计计数器（单调递增，模拟 stats 的 bout）
        self.scur = random.randint(5, 50)  # 当前并发连接数，5~50 随机游走
        self._last = time.monotonic()

    def advance(self) -> None:
        """按真实流逝的时间推进计数器：懒计算，只在被采样时更新。
        用真实时间差而不是固定步长，使采样节奏与增长速率解耦——
        rl-limiter 差分出的速率因此接近 --frontends 声明的目标值。"""
        now = time.monotonic()
        dt = now - self._last
        self._last = now
        factor = 1.0 + random.uniform(-self.jitter, self.jitter)
        self.bytes_out += max(0, int(self.rate_bps * dt * factor))
        # 并发连接数缓慢游走，保持在 5~50 区间内。
        self.scur = min(50, max(5, self.scur + random.randint(-3, 3)))


class FakeHAProxy:
    """假 HAProxy：持有全部 frontend 状态与 runtime map 的内存副本。"""

    def __init__(self, frontends: list[FakeFrontend]):
        self.frontends = {f.name: f for f in frontends}
        # (map_path, key) -> value：set/add map 写入的最新值。
        self.maps: dict[tuple[str, str], str] = {}

    # ---- 命令实现 -------------------------------------------------------

    def show_stat(self) -> str:
        """构造 "show stat" 的 CSV 回包。列头以 "# " 开头，字段名与真实
        HAProxy 对齐（采集端按列名解析，bytes_out 是 bout 的公认别名）。
        额外附带一行内建 "stats" frontend——真实 HAProxy 的回包里也有它，
        采集端应当忽略（不属于计费口径）。"""
        lines = ["# pxname,svname,scur,smax,bin,bytes_out,status"]
        for f in self.frontends.values():
            f.advance()
            lines.append(f"{f.name},FRONTEND,{f.scur},50,0,{f.bytes_out},OPEN")
        lines.append("stats,FRONTEND,1,1,0,0,OPEN")
        return "\n".join(lines) + "\n"

    def set_map(self, cmd: str, peer: str) -> str:
        """处理 "set map <path> <key> <value>" / "add map ..."。

        真实 HAProxy 对不存在的 key 会回 "entry not found"，客户端随即
        回退 "add map"；这个假实现直接把两条命令都当作 upsert 接受，
        避免联调结果依赖客户端是否实现了回退路径。成功回包为空。"""
        parts = cmd.split()
        if len(parts) < 5:
            return f"Unknown command. Malformed map command: {cmd}\n"
        _, _, map_path, key, value = parts[0], parts[1], parts[2], parts[3], parts[4]
        old = self.maps.get((map_path, key))
        self.maps[(map_path, key)] = value
        if old != value:
            # 值发生变化才打日志：这是观察限速值真实下发的主要窗口。
            log.info("收到 map 更新，本节点限速值已改写（enforce 下发的观察窗口） "
                     "peer=%s map=%s key=%s old=%s new=%s",
                     peer, map_path, key, old if old is not None else "-", value)
        return "\n"

    # ---- 连接处理 -------------------------------------------------------

    async def handle(self, reader: asyncio.StreamReader,
                     writer: asyncio.StreamWriter) -> None:
        """每连接：读一行命令 → 应答 → 关闭连接（非交互模式协议）。"""
        peername = writer.get_extra_info("peername")
        peer = f"{peername[0]}:{peername[1]}" if peername else "?"
        try:
            raw = await asyncio.wait_for(reader.readline(), timeout=5.0)
            cmd = raw.decode("utf-8", "replace").strip()
            if cmd.startswith("show stat"):
                reply = self.show_stat()
                log.debug("已应答一次 show stat 采样请求 peer=%s frontends=%d", peer, len(self.frontends))
            elif cmd.startswith("set map") or cmd.startswith("add map"):
                reply = self.set_map(cmd, peer)
            elif cmd == "":
                reply = ""
            else:
                reply = f"Unknown command: {cmd}\n"
                log.warning("收到无法识别的命令，已按错误应答 peer=%s cmd=%s", peer, cmd)
            writer.write(reply.encode("utf-8"))
            await writer.drain()
        except asyncio.TimeoutError:
            log.warning("等待客户端命令超时，关闭本次连接 peer=%s", peer)
        except (ConnectionResetError, BrokenPipeError):
            pass  # 客户端提前断开：演示场景无需关心
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except (ConnectionResetError, BrokenPipeError):
                pass


# 1 Mbps = 1_000_000 bit/s = 125_000 byte/s（十进制兆，网络带宽惯例）。
_BYTES_PER_MBIT = 125_000


def parse_frontends(spec: str, jitter: float) -> list[FakeFrontend]:
    """解析 --frontends 参数："fe_a:16,fe_b:4" → 每个 frontend 一个模拟器。

    冒号后的数值是**下行速率 Mbps**（人类可读口径，如 16 表示 16 Mbps），
    内部换算成 bytes/s 驱动 bytes_out 计数器增长。"""
    frontends: list[FakeFrontend] = []
    for item in spec.split(","):
        item = item.strip()
        if not item:
            continue
        name, _, mbps = item.partition(":")
        if not name or not mbps:
            raise ValueError(f"bad frontend spec: {item!r} (expected name:mbps)")
        frontends.append(FakeFrontend(name, float(mbps) * _BYTES_PER_MBIT, jitter))
    if not frontends:
        raise ValueError("no frontends specified")
    return frontends


async def amain(args: argparse.Namespace) -> None:
    fake = FakeHAProxy(parse_frontends(args.frontends, args.jitter))
    server = await asyncio.start_server(fake.handle, host=args.host, port=args.port)
    log.info("假 HAProxy stats socket 已开始监听，等待 rl-limiter 接入 "
             "addr=%s:%d frontends=%s jitter=%.2f",
             args.host, args.port,
             ",".join(f"{f.name}:{f.rate_bps / _BYTES_PER_MBIT:.2f}Mbps"
                      for f in fake.frontends.values()),
             args.jitter)
    async with server:
        await server.serve_forever()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="假 HAProxy stats socket（TCP），本地演示/联调 rl-limiter 用")
    parser.add_argument("--host", default="127.0.0.1", help="监听地址（默认 %(default)s）")
    parser.add_argument("--port", type=int, required=True, help="监听端口（模拟内网 TCP stats socket）")
    parser.add_argument(
        "--frontends", default="fe_env_a:16",
        help="frontend 清单：name:下行速率Mbps，逗号分隔（如 fe_a:16,fe_b:4；默认 %(default)s）")
    parser.add_argument(
        "--jitter", type=float, default=0.2,
        help="流量抖动幅度，0.2 表示 ±20%%（默认 %(default)s）")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S%z")
    try:
        asyncio.run(amain(args))
    except KeyboardInterrupt:
        log.info("假 HAProxy 已停止")


if __name__ == "__main__":
    main()
