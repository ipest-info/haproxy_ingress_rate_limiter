#!/usr/bin/env python3
# tools/loadgen.py —— 可在线调节并发数的 HTTP 压测服务（模拟并发带宽）。
#
# 角色：docker compose 演示环境里的"客户端群"替身。维持 N 个并发 worker
# 持续请求目标 URL（经 HAProxy 入口，可给多个、逗号分隔，每请求随机挑一），
# 把响应体完整读完并累计字节数——制造出可控强度的下行带宽压力，供观察
# 各入口聚合限速的效果（总速率贴限额、连接间动态分享）。限速在内核 tc 上。
#
# 并发数可两种方式调节：
#   - 启动参数 --concurrency / 环境变量 LOADGEN_CONCURRENCY：初始并发；
#   - 运行中经控制端口在线调节（无需重启，秒级生效）：
#       curl http://localhost:8081/status                          # 查看状态
#       curl -X PUT http://localhost:8081/concurrency -d '{"concurrency": 32}'
#       curl -X PUT http://localhost:8081/concurrency -d '32'      # 裸数字也行
#     并发调到 0 即暂停打流（worker 全部收回，随时可再调起）。
#
# 长连接大文件下载场景（验证限速对长连接的持续作用）：
#   每目标 N 个"大文件 worker"——同一条 TCP 长连接上循环请求 /big
#   （单个响应数百 MB，限速下要下载数分钟），连接**永不主动轮转**。
#   限额调整（reload + hard-stop-after）会把在途下载掐断：worker 记一次
#   "中断"并重连继续，正好复现真实长下载客户端的行为。
#   - 启动参数 --big-per-target / 环境变量 LOADGEN_BIG_PER_TARGET；
#   - 在线调节：curl -X PUT http://localhost:8081/big -d '{"per_target": 1}'
#   - /status 的 big 字段带每个在途下载的进度/速率/连接年龄。
#
# 观测：每 --report-s 秒输出一次吞吐报告——首行为汇总，随后是**按目标
# 逐行的表格**（每个 HAProxy 入口一行：并发/速率/请求数/错误数/状态），
# 某个入口打不通时该行速率归零、错误上涨、状态标"不通"，一眼定位。
# Mbps 口径与监控页面一致；/status 返回同样的分目标
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
# 让服务端先关（server 提前 FIN 会与整形器尚未放完的
# 整形数据竞争，导致 client 侧响应截断——实测短连接全部报
# ContentLengthError，keep-alive 由客户端关则完全正常）。
REQUESTS_PER_CONNECTION = 8
# 并发上限：防御性钳制，防止一条打错的 curl（比如 32000）把演示环境
# 的文件描述符耗尽。演示所需并发远低于此。
MAX_CONCURRENCY = 1024
# 每目标大文件 worker 上限（每个都是一条长期占带宽的下载，几个就够演示）。
MAX_BIG_PER_TARGET = 16
# 大文件下载端点（相对目标 URL）。
BIG_PATH = "big"


def _disp_width(s: str) -> int:
    """终端显示宽度：CJK 字符按 2 列（表格对齐用，够准即可）。"""
    return sum(2 if ord(c) > 0x2E7F else 1 for c in s)


def _pad(s: str, width: int, right_align: bool = False) -> str:
    """按显示宽度补空格（含中文的表头/状态列也能对齐）。"""
    fill = " " * max(0, width - _disp_width(s))
    return fill + s if right_align else s + fill


class TargetStats:
    """单个目标（HAProxy 入口）的累计计数与最近窗口速率。

    大文件下载的字节同样计入 bytes_total（HAProxy 侧限的就是总量，
    速率列因此反映该入口的全部下行流量）；完成/中断另有专用计数。
    """

    __slots__ = ("bytes_total", "requests_total", "errors_total",
                 "rate_bytes_per_s", "rate_requests_per_s", "window_errors",
                 "big_done_total", "big_interrupted_total")

    def __init__(self):
        self.bytes_total = 0
        self.requests_total = 0
        self.errors_total = 0
        # 最近一个报告窗口的差分结果（报告循环回填）。
        self.rate_bytes_per_s = 0.0
        self.rate_requests_per_s = 0.0
        self.window_errors = 0
        # 大文件下载：完整下完的个数 / 半途被断开的个数（reload 的
        # hard-stop、后端重启等）。
        self.big_done_total = 0
        self.big_interrupted_total = 0

    def health(self) -> str:
        """入口健康态（按最近窗口判定）：
        正常 = 有成功请求或有字节在到达（大文件下载中）；
        不通 = 无成功且有错误；空闲 = 无流量。"""
        if self.rate_requests_per_s > 0 or self.rate_bytes_per_s > 0:
            return "正常"
        return "不通" if self.window_errors > 0 else "空闲"


class BigState:
    """单个大文件 worker 的实时进度（/status 曝光用）。"""

    __slots__ = ("target", "conn_started", "req_started", "got", "size")

    def __init__(self, target: str):
        self.target = target
        self.conn_started = 0.0   # 当前 TCP 连接建立时刻（monotonic）
        self.req_started = 0.0    # 当前下载开始时刻
        self.got = 0              # 当前下载已收字节
        self.size = 0             # 当前下载总大小（Content-Length）


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
        # 大文件 worker：每目标 N 个（(task, state) 成对管理）。
        self._big: list[tuple[asyncio.Task, BigState]] = []
        self._big_per_target = 0
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

    @property
    def big_per_target(self) -> int:
        return self._big_per_target

    def set_big_per_target(self, n: int) -> int:
        """把每目标的大文件 worker 数调整到 n（钳制 [0, MAX_BIG_PER_TARGET]），
        返回生效值。总任务数 = n × 目标数；减量从尾部撤（cancel 干净）。"""
        n = max(0, min(int(n), MAX_BIG_PER_TARGET))
        want = n * len(self._targets)
        while len(self._big) < want:
            wid = len(self._big)
            target = self._targets[wid % len(self._targets)]  # 轮转绑定入口
            state = BigState(target)
            task = asyncio.create_task(
                self._big_worker(target, state), name=f"loadgen-big-{wid}")
            self._big.append((task, state))
        while len(self._big) > want:
            task, _ = self._big.pop()
            task.cancel()
        self._big_per_target = n
        return n

    def big_workers_for(self, target: str) -> int:
        return sum(1 for _, st in self._big if st.target == target)

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

    async def _big_worker(self, target: str, state: BigState) -> None:
        """长连接大文件下载客户端：同一条 TCP 连接上循环请求 /big。

        与普通 worker 的关键区别：
        - **连接永不主动轮转**——一条连接可以挂着下载几十分钟，专门检验
          限速对存量长连接的持续作用；
        - 单个响应就是一次长下载（默认 512 MiB，40 Mbps 限速下约 100s），
          分块读、逐块计入吞吐；
        - 下载半途被断开（HAProxy reload 的 hard-stop、后端重启）时记一次
          "中断"并告警说明，随后**重建连接重新下载**——复现真实长下载
          客户端（下载工具重试/续传）的行为。
        """
        st = self._stats[target]
        url = target + BIG_PATH if target.endswith("/") else target + "/" + BIG_PATH
        timeout = aiohttp.ClientTimeout(total=None, connect=10.0)
        while True:
            try:
                connector = aiohttp.TCPConnector(limit=1)
                async with aiohttp.ClientSession(
                        timeout=timeout, connector=connector) as sess:
                    state.conn_started = time.monotonic()
                    while True:  # 同一条连接上循环下载，永不主动断开
                        state.req_started = time.monotonic()
                        state.got = 0
                        async with sess.get(url) as resp:
                            if resp.status != 200:
                                await resp.read()
                                st.errors_total += 1
                                await asyncio.sleep(0.5)
                                continue
                            state.size = int(resp.headers.get("Content-Length", 0))
                            async for chunk in resp.content.iter_chunked(64 * 1024):
                                st.bytes_total += len(chunk)
                                state.got += len(chunk)
                        dur = time.monotonic() - state.req_started
                        st.requests_total += 1
                        st.big_done_total += 1
                        log.info(
                            "大文件下载完成（同一长连接继续下一个） target=%s "
                            "bytes=%d duration_s=%.1f avg_mbps=%.1f conn_age_s=%.0f",
                            target, state.got, dur,
                            state.got * 8 / max(dur, 1e-9) / 1e6,
                            time.monotonic() - state.conn_started)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                st.errors_total += 1
                st.big_interrupted_total += 1
                log.warning(
                    "大文件下载被中断（多半是限额调整 reload 的 hard-stop 断连，"
                    "或后端/HAProxy 重启），重建连接后重新下载 target=%s "
                    "got_bytes=%d of=%d conn_age_s=%.0f err=%s",
                    target, state.got, state.size,
                    time.monotonic() - state.conn_started, e)
                await asyncio.sleep(1.0)

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
            # 大文件列 = 该入口的长连接下载 worker 数（0 则整列仍在，便于对齐）。
            name_w = max(24, max(_disp_width(t) for t in self._targets))
            header = "  ".join([
                _pad("目标", name_w),
                _pad("并发", 4, True), _pad("大文件", 6, True),
                _pad("速率Mbps", 8, True),
                _pad("请求/s", 6, True), _pad("累计请求", 8, True),
                _pad("累计错误", 8, True), _pad("状态", 4, True),
            ])
            rows = []
            for t in self._targets:
                s = self._stats[t]
                rows.append("  ".join([
                    _pad(t, name_w),
                    _pad(str(self.workers_for(t)), 4, True),
                    _pad(str(self.big_workers_for(t)), 6, True),
                    _pad(f"{s.rate_bytes_per_s * 8 / 1e6:.1f}", 8, True),
                    _pad(f"{s.rate_requests_per_s:.1f}", 6, True),
                    _pad(str(s.requests_total), 8, True),
                    _pad(str(s.errors_total), 8, True),
                    _pad(s.health(), 4, True),
                ]))
            log.info(
                "吞吐观测 concurrency=%d big_per_target=%d total_mbps=%.1f "
                "total_req_per_s=%.1f requests_total=%d errors_total=%d\n  %s\n  %s",
                self.concurrency, self._big_per_target,
                total_bytes_rate * 8 / 1e6,  # Mbps：与 quota_mbps 同口径
                total_req_rate,
                self.requests_total, self.errors_total,
                header, "\n  ".join(rows))

    def status(self) -> dict:
        now = time.monotonic()
        return {
            "targets": [
                {
                    "target": t,
                    "workers": self.workers_for(t),
                    "big_workers": self.big_workers_for(t),
                    "rate_bytes_per_s": round(s.rate_bytes_per_s),
                    "rate_mbps": round(s.rate_bytes_per_s * 8 / 1e6, 2),
                    "rate_requests_per_s": round(s.rate_requests_per_s, 1),
                    "requests_total": s.requests_total,
                    "errors_total": s.errors_total,
                    "big_done_total": s.big_done_total,
                    "big_interrupted_total": s.big_interrupted_total,
                    "health": s.health(),
                }
                for t, s in self._stats.items()
            ],
            # 在途大文件下载的实时进度（长连接年龄是"存量连接持续受控"
            # 的直接证据）。
            "big_downloads": [
                {
                    "target": st.target,
                    "conn_age_s": round(now - st.conn_started, 1),
                    "download_age_s": round(now - st.req_started, 1),
                    "got_bytes": st.got,
                    "size_bytes": st.size,
                    "progress": round(st.got / st.size, 3) if st.size else None,
                }
                for _, st in self._big
            ],
            "big_per_target": self._big_per_target,
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

    async def handle_set_big(request: web.Request) -> web.Response:
        # 接受 {"per_target": 1} 或裸数字 "1"。
        body = (await request.text()).strip()
        try:
            parsed = json.loads(body)
            value = parsed["per_target"] if isinstance(parsed, dict) else parsed
            n = int(value)
        except (ValueError, TypeError, KeyError):
            return web.json_response(
                {"error": "请求体须为 {\"per_target\": N} 或裸整数 N",
                 "got": body[:200]},
                status=400)
        prev = gen.big_per_target
        applied = gen.set_big_per_target(n)
        log.info("大文件下载 worker 已在线调整 per_target: from=%d requested=%d applied=%d",
                 prev, n, applied)
        return web.json_response(
            {"previous": prev, "requested": n, "big_per_target": applied})

    app = web.Application()
    app.router.add_get("/", handle_status)
    app.router.add_get("/status", handle_status)
    app.router.add_put("/concurrency", handle_set_concurrency)
    app.router.add_post("/concurrency", handle_set_concurrency)  # PUT/POST 等价
    app.router.add_put("/big", handle_set_big)
    app.router.add_post("/big", handle_set_big)
    return app


async def amain(args: argparse.Namespace) -> None:
    # 连接由各 worker 自建（每 worker 独立 keep-alive session，周期轮转，
    # 见 _worker/REQUESTS_PER_CONNECTION 注释），这里只负责压测器骨架。
    targets = [u.strip() for u in args.target.split(",") if u.strip()]
    gen = LoadGen(targets)
    applied = gen.set_concurrency(args.concurrency)
    applied_big = gen.set_big_per_target(args.big_per_target)

    runner = web.AppRunner(make_control_app(gen))
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", args.control_port)
    await site.start()

    log.info(
        "压测服务已启动 targets=%s concurrency=%d big_per_target=%d control_port=%d "
        "（在线调节：curl -X PUT http://<host>:%d/concurrency -d '{\"concurrency\": N}'；"
        "大文件长连接：PUT /big -d '{\"per_target\": N}'）",
        ",".join(targets), applied, applied_big,
        args.control_port, args.control_port)
    try:
        await gen.report_loop(args.report_s)
    finally:
        gen.set_concurrency(0)
        gen.set_big_per_target(0)
        await runner.cleanup()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="可在线调节并发数的 HTTP 压测服务：N 个并发 worker "
                    "持续请求目标 URL 以模拟并发带宽")
    parser.add_argument("--target", required=True,
                        help="压测目标 URL，逗号分隔可给多个（经 HAProxy 入口，"
                             "例如 http://node1:8080/,http://node2:8080/），"
                             "每个请求随机挑一个目标")
    parser.add_argument("--concurrency", type=int,
                        default=env_int("LOADGEN_CONCURRENCY", DEFAULT_CONCURRENCY, "loadgen"),
                        help="初始并发数（默认 %(default)s，环境变量 LOADGEN_CONCURRENCY）")
    parser.add_argument("--big-per-target", type=int,
                        default=env_int("LOADGEN_BIG_PER_TARGET", 0, "loadgen"),
                        help="每目标的长连接大文件下载 worker 数（默认 %(default)s，"
                             "环境变量 LOADGEN_BIG_PER_TARGET；运行中可 PUT /big 调节）")
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
    if args.big_per_target < 0 or args.big_per_target > MAX_BIG_PER_TARGET:
        print(f"loadgen: --big-per-target 必须在 0-{MAX_BIG_PER_TARGET} 范围内，"
              f"当前值 {args.big_per_target}", file=sys.stderr)
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
