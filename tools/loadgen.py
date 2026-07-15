#!/usr/bin/env python3
# tools/loadgen.py —— 可在线调节并发数的 HTTP 压测服务（模拟并发带宽）。
#
# 角色：docker compose 演示环境里的"客户端群"替身。维持 N 个并发 worker
# 持续请求目标 URL（经 HAProxy 入口），把响应体完整读完并累计字节数——
# 制造出可控强度的下行带宽压力，供观察 rl-limiter 的 AIMD 收紧/恢复行为。
#
# 并发数可两种方式调节：
#   - 启动参数 --concurrency / 环境变量 LOADGEN_CONCURRENCY：初始并发；
#   - 运行中经控制端口在线调节（无需重启，秒级生效）：
#       curl http://localhost:8081/status                          # 查看状态
#       curl -X PUT http://localhost:8081/concurrency -d '{"concurrency": 32}'
#       curl -X PUT http://localhost:8081/concurrency -d '32'      # 裸数字也行
#     并发调到 0 即暂停打流（worker 全部收回，随时可再调起）。
#
# 观测：每 --report-s 秒输出一行吞吐日志（Mbps 口径与 rl-limiter 的
# quota_bps 一致），/status 返回同样的速率数据，便于脚本化断言
# "吞吐已被压到配额附近"。
#
# 实现要点：
#   - worker 是纯 asyncio 任务：加/减并发 = 补/撤任务，撤销点在请求边界
#     （cancel 由 aiohttp 在 await 处响应），不会留下半截连接；
#   - 共享一个 ClientSession，连接池上限放开（limit=0）——并发度由
#     worker 数唯一决定，不被连接池二次钳制；
#   - 请求失败（HAProxy 未就绪、后端重启等）只计数并小睡退避，worker
#     永不退出——压测器要比被测对象皮实。

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import sys
import time

import aiohttp
from aiohttp import web

log = logging.getLogger("loadgen")

DEFAULT_CONCURRENCY = 8
DEFAULT_CONTROL_PORT = 8081
DEFAULT_REPORT_S = 2.0
# 并发上限：防御性钳制，防止一条打错的 curl（比如 32000）把演示环境
# 的文件描述符耗尽。演示所需并发远低于此。
MAX_CONCURRENCY = 1024


def _env_int(name: str, default: int) -> int:
    raw = (os.environ.get(name) or "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        print(f"loadgen: 环境变量 {name} 必须是整数，当前值 {raw!r}",
              file=sys.stderr)
        raise SystemExit(1)


class LoadGen:
    """维持目标并发数的压测核心：worker 池 + 吞吐计数器。

    单事件循环内使用，无锁；set_concurrency 同步增删 worker 任务，
    计数器由各 worker 直接累加。
    """

    def __init__(self, target: str, session: aiohttp.ClientSession):
        self._target = target
        self._session = session
        self._workers: list[asyncio.Task] = []
        # 生命周期累计值（/status 展示；速率由报告循环差分得出）。
        self.bytes_total = 0
        self.requests_total = 0
        self.errors_total = 0
        # 最近一个报告窗口的速率快照（bytes/s、req/s），报告循环回填。
        self.rate_bytes_per_s = 0.0
        self.rate_requests_per_s = 0.0

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
            self._workers.append(
                asyncio.create_task(self._worker(), name=f"loadgen-worker-{wid}"))
        while len(self._workers) > n:
            self._workers.pop().cancel()
        return n

    async def _worker(self) -> None:
        """单个"客户端"：循环请求目标并把响应体完整读完。

        读响应用分块迭代而不是 read()——限速生效时单个响应会拖长到
        数秒，分块累计让吞吐统计平滑跟随实际到达的字节，而不是在
        响应结束时跳变一大块。
        """
        while True:
            try:
                async with self._session.get(self._target) as resp:
                    async for chunk in resp.content.iter_chunked(64 * 1024):
                        self.bytes_total += len(chunk)
                    if resp.status == 200:
                        self.requests_total += 1
                    else:
                        # 非 200（如后端未就绪时 HAProxy 的 503）也要退避：
                        # 这类响应几乎瞬时返回，不退避会变成紧密循环刷错误。
                        self.errors_total += 1
                        await asyncio.sleep(0.5)
            except asyncio.CancelledError:
                raise
            except Exception:
                # 目标未就绪/连接被拒等：计一次错误，小睡退避再试，
                # worker 本身永不退出。
                self.errors_total += 1
                await asyncio.sleep(0.5)

    async def report_loop(self, interval_s: float) -> None:
        """周期输出吞吐日志并回填 /status 用的速率快照。"""
        last_bytes = self.bytes_total
        last_reqs = self.requests_total
        last_t = time.monotonic()
        while True:
            await asyncio.sleep(interval_s)
            now = time.monotonic()
            dt = max(now - last_t, 1e-9)
            d_bytes = self.bytes_total - last_bytes
            d_reqs = self.requests_total - last_reqs
            last_bytes, last_reqs, last_t = self.bytes_total, self.requests_total, now
            self.rate_bytes_per_s = d_bytes / dt
            self.rate_requests_per_s = d_reqs / dt
            log.info(
                "吞吐观测 concurrency=%d rate_mbps=%.1f rate_bytes_per_s=%.0f "
                "req_per_s=%.1f requests_total=%d errors_total=%d",
                self.concurrency,
                self.rate_bytes_per_s * 8 / 1e6,  # Mbps：与 quota_bps 同口径
                self.rate_bytes_per_s,
                self.rate_requests_per_s,
                self.requests_total, self.errors_total)

    def status(self) -> dict:
        return {
            "target": self._target,
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
    # total=None：限速压到很低时单响应可能拖到分钟级，压测器不设总超时；
    # 连接阶段仍给 10s，防止对无路由地址无限等待。
    timeout = aiohttp.ClientTimeout(total=None, connect=10.0)
    connector = aiohttp.TCPConnector(limit=0)  # 并发度只由 worker 数决定
    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        gen = LoadGen(args.target, session)
        applied = gen.set_concurrency(args.concurrency)

        runner = web.AppRunner(make_control_app(gen))
        await runner.setup()
        site = web.TCPSite(runner, "0.0.0.0", args.control_port)
        await site.start()

        log.info(
            "压测服务已启动 target=%s concurrency=%d control_port=%d "
            "（在线调节：curl -X PUT http://<host>:%d/concurrency -d '{\"concurrency\": N}'）",
            args.target, applied, args.control_port, args.control_port)
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
                        help="压测目标 URL（经 HAProxy 入口，例如 http://haproxy:8080/）")
    parser.add_argument("--concurrency", type=int,
                        default=_env_int("LOADGEN_CONCURRENCY", DEFAULT_CONCURRENCY),
                        help="初始并发数（默认 %(default)s，环境变量 LOADGEN_CONCURRENCY）")
    parser.add_argument("--control-port", type=int,
                        default=_env_int("LOADGEN_CONTROL_PORT", DEFAULT_CONTROL_PORT),
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
