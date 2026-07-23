#!/usr/bin/env python3
# tools/loadgen.py —— 可在线调节并发数的 HTTP 压测服务（模拟并发带宽）。
#
# 角色：docker compose 演示环境里的"客户端群"替身。维持 N 个并发 worker
# 持续请求目标 URL（经 HAProxy 入口，可给多个、逗号分隔，每请求随机挑一），
# 把响应体完整读完并累计字节数——制造出可控强度的下行带宽压力，供观察
# 各入口 shared bwlim 聚合限速的效果（总速率贴限额、连接间动态分享）。
#
# 并发数可两种方式调节：
#   - 启动参数 --concurrency / 环境变量 LOADGEN_CONCURRENCY：初始并发；
#   - 运行中经控制端口在线调节（无需重启，秒级生效）：
#       curl http://localhost:8081/status                          # 查看状态
#       curl -X PUT http://localhost:8081/concurrency -d '{"concurrency": 32}'
#       curl -X PUT http://localhost:8081/concurrency -d '32'      # 裸数字也行
#     并发调到 0 即暂停打流（worker 全部收回，随时可再调起）。
#
# 观测：每 --report-s 秒输出一次吞吐报告——首行为汇总，随后是**按目标
# 逐行的表格**（每个 HAProxy 入口一行：并发/速率/请求数/错误数/状态），
# 某个入口打不通时该行速率归零、错误上涨、状态标"不通"，一眼定位。
# Mbps 口径与 rl-limiter 的 quota_bps 一致；/status 返回同样的分目标
# 数据，便于脚本化断言"吞吐已被压到配额附近"。
#
# 实现要点：
#   - worker 是纯 asyncio 任务：加/减并发 = 补/撤任务，撤销点在请求边界
#     （cancel 由 aiohttp 在 await 处响应），不会留下半截连接；
#   - 每个 worker 独立 keep-alive 连接、周期轮转（REQUESTS_PER_CONNECTION
#     注释解释了为什么既不能长期持有同一连接、也不能用 Connection: close）；
#   - 请求失败（HAProxy 未就绪、后端重启等）只计数并小睡退避，worker
#     永不退出——压测器要比被测对象皮实。

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
import time

import aiohttp
from aiohttp import web

from _common import env_int

log = logging.getLogger("loadgen")

DEFAULT_CONCURRENCY = 8
DEFAULT_CONTROL_PORT = 8081
DEFAULT_REPORT_S = 2.0
# 每条 TCP 连接复用的请求数，之后由**客户端侧**优雅关闭并重建。
# 两头兼顾：TCP L4 整形的每连接限速在建连时定格，周期轮转连接让
# 限额调整后新连接尽快进入新额度；同时绝不能用 Connection: close
# 让服务端先关（server 提前 FIN 会与 HAProxy bwlim 过滤器尚未放完的
# 整形数据竞争，导致 client 侧响应截断——实测短连接全部报
# ContentLengthError，keep-alive 由客户端关则完全正常）。
REQUESTS_PER_CONNECTION = 8
# 并发上限：防御性钳制，防止一条打错的 curl（比如 32000）把演示环境
# 的文件描述符耗尽。演示所需并发远低于此。
MAX_CONCURRENCY = 1024


def _disp_width(s: str) -> int:
    """终端显示宽度：CJK 字符按 2 列（表格对齐用，够准即可）。"""
    return sum(2 if ord(c) > 0x2E7F else 1 for c in s)


def _pad(s: str, width: int, right_align: bool = False) -> str:
    """按显示宽度补空格（含中文的表头/状态列也能对齐）。"""
    fill = " " * max(0, width - _disp_width(s))
    return fill + s if right_align else s + fill


class TargetStats:
    """单个目标（HAProxy 入口）的累计计数与最近窗口速率。"""

    __slots__ = ("bytes_total", "requests_total", "errors_total",
                 "rate_bytes_per_s", "rate_requests_per_s", "window_errors")

    def __init__(self):
        self.bytes_total = 0
        self.requests_total = 0
        self.errors_total = 0
        # 最近一个报告窗口的差分结果（报告循环回填）。
        self.rate_bytes_per_s = 0.0
        self.rate_requests_per_s = 0.0
        self.window_errors = 0

    def health(self) -> str:
        """入口健康态（按最近窗口判定）：
        正常 = 有成功请求；不通 = 无成功且有错误；空闲 = 无流量。"""
        if self.rate_requests_per_s > 0:
            return "正常"
        return "不通" if self.window_errors > 0 else "空闲"


class LoadGen:
    """维持目标并发数的压测核心：worker 池 + 吞吐计数器。

    单事件循环内使用，无锁；set_concurrency 同步增删 worker 任务，
    计数器由各 worker 直接累加。targets 支持多个入口（多台 HAProxy ×
    多个 frontend）：worker 按序轮转固定绑定到入口——每个入口有独立的
    客户端群，被整形变慢的入口不会把 worker 都吸走、饿死其它入口
    （随机每请求挑目标会出现这种排队效应）。
    """

    def __init__(self, targets: list[str]):
        self._targets = list(targets)
        self._workers: list[asyncio.Task] = []
        # 分目标计数器：吞吐/请求/错误全部按入口归账，报告与 /status
        # 逐行展示——某台 HAProxy 打不通时能立刻看出是哪一台。
        self._stats: dict[str, TargetStats] = {t: TargetStats() for t in self._targets}
        # 汇总口径的最近窗口速率（报告循环回填，/status 用）。
        self.rate_bytes_per_s = 0.0
        self.rate_requests_per_s = 0.0

    # 汇总累计值 = 各目标之和（对外口径不变）。
    @property
    def bytes_total(self) -> int:
        return sum(s.bytes_total for s in self._stats.values())

    @property
    def requests_total(self) -> int:
        return sum(s.requests_total for s in self._stats.values())

    @property
    def errors_total(self) -> int:
        return sum(s.errors_total for s in self._stats.values())

    def workers_for(self, target: str) -> int:
        """该目标当前绑定的 worker 数（轮转分配的确定性结果）。"""
        idx = self._targets.index(target)
        n, k = len(self._workers), len(self._targets)
        return n // k + (1 if idx < n % k else 0)

    @property
    def concurrency(self) -> int:
        return len(self._workers)

    def set_concurrency(self, n: int) -> int:
        """把 worker 池调整到 n 个（钳制在 [0, MAX_CONCURRENCY]），返回
        生效值。增：补任务；减：从尾部撤任务（cancel 即可，worker 对
        取消是干净的）。"""
        n = max(0, min(int(n), MAX_CONCURRENCY))
        while len(self._workers) < n:
            wid = len(self._workers)
            target = self._targets[wid % len(self._targets)]  # 轮转绑定入口
            self._workers.append(asyncio.create_task(
                self._worker(target), name=f"loadgen-worker-{wid}"))
        while len(self._workers) > n:
            self._workers.pop().cancel()
        return n

    async def _worker(self, target: str) -> None:
        """单个"客户端"：循环请求绑定的目标并把响应体完整读完。

        - 连接生命周期：每个 worker 独立 session（keep-alive，连接池 1），
          复用 REQUESTS_PER_CONNECTION 个请求后由客户端优雅关闭重建
          （理由见常量注释：跟随限额调整 + 避开 server 先 FIN 的截断竞争）；
        - 读响应用分块迭代而不是 read()——限速生效时单个响应会拖长到
          数秒，分块累计让吞吐统计平滑跟随实际到达的字节，而不是在
          响应结束时跳变一大块。
        """
        st = self._stats[target]
        timeout = aiohttp.ClientTimeout(total=None, connect=10.0)
        while True:
            try:
                connector = aiohttp.TCPConnector(limit=1)
                async with aiohttp.ClientSession(
                        timeout=timeout, connector=connector) as sess:
                    for _ in range(REQUESTS_PER_CONNECTION):
                        async with sess.get(target) as resp:
                            if resp.status == 200:
                                async for chunk in resp.content.iter_chunked(64 * 1024):
                                    st.bytes_total += len(chunk)
                                st.requests_total += 1
                            else:
                                # 非 200（如后端未就绪时 HAProxy 的 503）：
                                # 错误页字节不计入吞吐——它没走整形数据
                                # 路径，混进 bytes_total 会让 /status 在
                                # 故障窗口显示虚假流量。排空响应体后小睡
                                # 退避：这类响应几乎瞬时返回，不退避会
                                # 变成紧密循环刷错误。
                                await resp.read()
                                st.errors_total += 1
                                await asyncio.sleep(0.5)
            except asyncio.CancelledError:
                raise
            except Exception:
                # 目标未就绪/连接被拒等：计一次错误，小睡退避再试，
                # worker 本身永不退出。
                st.errors_total += 1
                await asyncio.sleep(0.5)

    async def report_loop(self, interval_s: float) -> None:
        """周期输出吞吐报告：首行汇总 + 按目标逐行的表格（一条日志）。

        每个入口一行（并发/速率/请求/错误/状态），入口打不通时该行速率
        归零、窗口错误上涨、状态标"不通"——不用再从合计里猜是哪台
        HAProxy 出了问题。速率由各目标计数器的窗口差分得出。
        """
        last = {t: (s.bytes_total, s.requests_total, s.errors_total)
                for t, s in self._stats.items()}
        last_t = time.monotonic()
        while True:
            await asyncio.sleep(interval_s)
            now = time.monotonic()
            dt = max(now - last_t, 1e-9)
            last_t = now
            total_bytes_rate = 0.0
            total_req_rate = 0.0
            for t, s in self._stats.items():
                lb, lr, le = last[t]
                s.rate_bytes_per_s = (s.bytes_total - lb) / dt
                s.rate_requests_per_s = (s.requests_total - lr) / dt
                s.window_errors = s.errors_total - le
                last[t] = (s.bytes_total, s.requests_total, s.errors_total)
                total_bytes_rate += s.rate_bytes_per_s
                total_req_rate += s.rate_requests_per_s
            self.rate_bytes_per_s = total_bytes_rate
            self.rate_requests_per_s = total_req_rate

            # 组表：目标列宽随最长 URL 自适应，数字列右对齐。
            name_w = max(24, max(_disp_width(t) for t in self._targets))
            header = "  ".join([
                _pad("目标", name_w),
                _pad("并发", 4, True), _pad("速率Mbps", 8, True),
                _pad("请求/s", 6, True), _pad("累计请求", 8, True),
                _pad("累计错误", 8, True), _pad("状态", 4, True),
            ])
            rows = []
            for t in self._targets:
                s = self._stats[t]
                rows.append("  ".join([
                    _pad(t, name_w),
                    _pad(str(self.workers_for(t)), 4, True),
                    _pad(f"{s.rate_bytes_per_s * 8 / 1e6:.1f}", 8, True),
                    _pad(f"{s.rate_requests_per_s:.1f}", 6, True),
                    _pad(str(s.requests_total), 8, True),
                    _pad(str(s.errors_total), 8, True),
                    _pad(s.health(), 4, True),
                ]))
            log.info(
                "吞吐观测 concurrency=%d total_mbps=%.1f total_req_per_s=%.1f "
                "requests_total=%d errors_total=%d\n  %s\n  %s",
                self.concurrency,
                total_bytes_rate * 8 / 1e6,  # Mbps：与 quota_bps 同口径
                total_req_rate,
                self.requests_total, self.errors_total,
                header, "\n  ".join(rows))

    def status(self) -> dict:
        return {
            "targets": [
                {
                    "target": t,
                    "workers": self.workers_for(t),
                    "rate_bytes_per_s": round(s.rate_bytes_per_s),
                    "rate_mbps": round(s.rate_bytes_per_s * 8 / 1e6, 2),
                    "rate_requests_per_s": round(s.rate_requests_per_s, 1),
                    "requests_total": s.requests_total,
                    "errors_total": s.errors_total,
                    "health": s.health(),
                }
                for t, s in self._stats.items()
            ],
            "concurrency": self.concurrency,
            "max_concurrency": MAX_CONCURRENCY,
            "rate_bytes_per_s": round(self.rate_bytes_per_s),
            "rate_mbps": round(self.rate_bytes_per_s * 8 / 1e6, 2),
            "rate_requests_per_s": round(self.rate_requests_per_s, 1),
            "bytes_total": self.bytes_total,
            "requests_total": self.requests_total,
            "errors_total": self.errors_total,
        }


def make_control_app(gen: LoadGen) -> web.Application:
    """控制面：查看状态 / 在线调节并发数。"""

    async def handle_status(_request: web.Request) -> web.Response:
        return web.json_response(gen.status())

    async def handle_set_concurrency(request: web.Request) -> web.Response:
        # 接受两种请求体：{"concurrency": 32} 或裸数字 "32"——后者让
        # 手敲 curl 不必纠结 JSON 引号。
        body = (await request.text()).strip()
        try:
            parsed = json.loads(body)
            value = parsed["concurrency"] if isinstance(parsed, dict) else parsed
            n = int(value)
        except (ValueError, TypeError, KeyError):
            return web.json_response(
                {"error": "请求体须为 {\"concurrency\": N} 或裸整数 N",
                 "got": body[:200]},
                status=400)
        prev = gen.concurrency
        applied = gen.set_concurrency(n)
        log.info("并发数已在线调整 from=%d requested=%d applied=%d",
                 prev, n, applied)
        return web.json_response(
            {"previous": prev, "requested": n, "concurrency": applied})

    app = web.Application()
    app.router.add_get("/", handle_status)
    app.router.add_get("/status", handle_status)
    app.router.add_put("/concurrency", handle_set_concurrency)
    app.router.add_post("/concurrency", handle_set_concurrency)  # PUT/POST 等价
    return app


async def amain(args: argparse.Namespace) -> None:
    # 连接由各 worker 自建（每 worker 独立 keep-alive session，周期轮转，
    # 见 _worker/REQUESTS_PER_CONNECTION 注释），这里只负责压测器骨架。
    targets = [u.strip() for u in args.target.split(",") if u.strip()]
    gen = LoadGen(targets)
    applied = gen.set_concurrency(args.concurrency)

    runner = web.AppRunner(make_control_app(gen))
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", args.control_port)
    await site.start()

    log.info(
        "压测服务已启动 targets=%s concurrency=%d control_port=%d "
        "（在线调节：curl -X PUT http://<host>:%d/concurrency -d '{\"concurrency\": N}'）",
        ",".join(targets), applied, args.control_port, args.control_port)
    try:
        await gen.report_loop(args.report_s)
    finally:
        gen.set_concurrency(0)
        await runner.cleanup()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="可在线调节并发数的 HTTP 压测服务：N 个并发 worker "
                    "持续请求目标 URL 以模拟并发带宽")
    parser.add_argument("--target", required=True,
                        help="压测目标 URL，逗号分隔可给多个（经 HAProxy 入口，"
                             "例如 http://haproxy1:8080/,http://haproxy2:8080/），"
                             "每个请求随机挑一个目标")
    parser.add_argument("--concurrency", type=int,
                        default=env_int("LOADGEN_CONCURRENCY", DEFAULT_CONCURRENCY, "loadgen"),
                        help="初始并发数（默认 %(default)s，环境变量 LOADGEN_CONCURRENCY）")
    parser.add_argument("--control-port", type=int,
                        default=env_int("LOADGEN_CONTROL_PORT", DEFAULT_CONTROL_PORT, "loadgen"),
                        help="控制端口：GET /status 查询、PUT /concurrency 调节"
                             "（默认 %(default)s，环境变量 LOADGEN_CONTROL_PORT）")
    parser.add_argument("--report-s", type=float, default=DEFAULT_REPORT_S,
                        help="吞吐日志输出周期（秒，默认 %(default)s）")
    args = parser.parse_args()

    if args.concurrency < 0 or args.concurrency > MAX_CONCURRENCY:
        print(f"loadgen: --concurrency 必须在 0-{MAX_CONCURRENCY} 范围内，"
              f"当前值 {args.concurrency}", file=sys.stderr)
        raise SystemExit(1)
    if args.report_s <= 0:
        print(f"loadgen: --report-s 必须 > 0，当前值 {args.report_s}",
              file=sys.stderr)
        raise SystemExit(1)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S%z")
    try:
        asyncio.run(amain(args))
    except KeyboardInterrupt:
        pass
    log.info("压测服务已停止")


if __name__ == "__main__":
    main()
