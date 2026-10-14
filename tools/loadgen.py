#!/usr/bin/env python3
# tools/loadgen.py —— 可在线调节并发数的 HTTP 压测服务（模拟并发带宽）。
#
# 角色：docker compose 演示环境里的"客户端群"替身。维持 N 个并发 worker
# 持续请求目标 URL（经 HAProxy 入口，可给多个、逗号分隔，worker 轮转
# 绑定入口），把响应体完整读完并累计字节数——制造出可控强度的下行带宽
# 压力，供观察各入口聚合限速的效果（总速率贴限额、连接间动态分享）。
#
# **默认场景：小文件 + 慢客户端 + 短连接**（贴近真实业务）：
#   - 后端返回随机大小的小文件（random_web 默认 4 KiB–256 KiB）；
#   - 每个请求按 --per-req-kbs 限读（默认 100 KB/s，模拟终端用户的
#     下载速度；0 = 不限）——限速在客户端侧，经 TCP 接收窗口把背压
#     传回 HAProxy/后端，这正是海量慢客户端的真实形态；
#   - **每个请求响应读完即由客户端断开连接，绝不复用**（无 keep-alive
#     复用）——高连接翻新率（conn churn）是场景的一部分。
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
# Mbps 口径与 rl-limiter 的 quota_mbps 一致；/status 返回同样的分目标
# 数据，便于脚本化断言"吞吐已被压到配额附近"。
#
# 实现要点：
#   - worker 是纯 asyncio 任务：加/减并发 = 补/撤任务，撤销点在请求边界
#     （cancel 由 aiohttp 在 await 处响应），不会留下半截连接；
#   - 短连接的断开方向在**客户端侧**（TCPConnector force_close：响应读
#     完后由客户端关闭）。绝不能用 Connection: close 让服务端先关——
#     server 提前 FIN 会与 L4 整形器尚未放完的整形数据竞争，导致 client
#     侧响应截断（实测短连接全部报 ContentLengthError）；客户端读完再关
#     则完全正常；
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
# 每请求的客户端侧限读速度（KB/s，1 KB = 1024 字节）。模拟终端用户的
# 下载速度：真实业务里单个用户远跑不满入口带宽，聚合压力来自"很多个
# 慢客户端"。0 = 不限（回到尽力读）。
DEFAULT_PER_REQ_KBS = 100.0
# 限读的分块大小：块越小 pacing 越平滑；16 KiB 在 100 KB/s 下约每
# 160ms 一拍，足够平滑又不至于 syscall 刷屏。
THROTTLE_CHUNK = 16 * 1024
# 并发上限：防御性钳制，防止一条打错的 curl（比如 320000）把演示环境
# 的文件描述符耗尽。每个 worker 一条连接 ≈ 一个 fd，压满上限前记得把
# 容器/进程的 nofile 抬到 ≳ 上限 + 余量（compose 的 ulimits 已给足）。
MAX_CONCURRENCY = 10240
# 压测进程数：Python 的 asyncio 是单线程事件循环，一个进程至多吃满一个
# CPU 核；上万并发时事件循环本身先饱和（每请求分片限读 ≈ 每秒几万次
# 协程唤醒），CPU 和吞吐都卡在单核上。多进程分片突破：父进程只管控制
# 端口/聚合/监督，N 个子进程各自跑一份事件循环、分摊并发。
# 0 = 自动（CPU 核数，封顶 MAX_PROCS）；1 = 单进程（旧行为）。
DEFAULT_PROCS = 0
MAX_PROCS = 16
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

    def __init__(self, targets: list[str], per_req_kbs: float = DEFAULT_PER_REQ_KBS):
        self._targets = list(targets)
        # 每请求限读速度（bytes/s；0 = 不限）。
        self.per_req_rate = per_req_kbs * 1024.0
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

    async def _read_throttled(self, resp, st: TargetStats) -> None:
        """把响应体读完，按 per_req_rate 限读（0 = 不限）。

        pacing 放在客户端读侧：读慢了 → TCP 接收窗口收紧 → 背压经
        HAProxy 传回后端，与真实慢客户端的行为一致。分块累计让吞吐
        统计平滑跟随实际到达的字节。
        """
        rate = self.per_req_rate
        if rate <= 0:
            async for chunk in resp.content.iter_chunked(64 * 1024):
                st.bytes_total += len(chunk)
            return
        start = time.monotonic()
        got = 0
        async for chunk in resp.content.iter_chunked(THROTTLE_CHUNK):
            st.bytes_total += len(chunk)
            got += len(chunk)
            # 已到字节对应的"应耗时"与实际耗时的差 = 需要补睡的时间。
            ahead = got / rate - (time.monotonic() - start)
            if ahead > 0:
                await asyncio.sleep(ahead)

    async def _worker(self, target: str) -> None:
        """单个"客户端"：循环请求绑定的目标，读完即断连，绝不复用。

        - **短连接**：TCPConnector(force_close=True) 让每个响应读完后由
          客户端关闭连接，下个请求重新建连——没有任何 keep-alive 复用。
          断开方向在客户端侧（server 先 FIN 会与 L4 整形竞争出截断，
          见文件头注释）；
        - 每请求按 per_req_rate 限读（_read_throttled），模拟慢客户端。
        """
        st = self._stats[target]
        timeout = aiohttp.ClientTimeout(total=None, connect=10.0)
        while True:
            try:
                connector = aiohttp.TCPConnector(limit=1, force_close=True)
                async with aiohttp.ClientSession(
                        timeout=timeout, connector=connector) as sess:
                    while True:
                        async with sess.get(target) as resp:
                            if resp.status == 200:
                                await self._read_throttled(resp, st)
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

    async def report_loop(self, interval_s: float,
                          quiet: bool = False) -> None:
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
            if quiet:
                # 分片子进程：速率窗口照常刷新（/status 要用），日志
                # 留给父进程聚合输出——N 份一样的表格只会刷屏。
                continue

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
            "per_req_kbs": round(self.per_req_rate / 1024.0, 1),
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


def _split(total: int, n: int) -> list[int]:
    """把 total 尽量均匀地拆成 n 份（前 total%n 份多 1）。"""
    base, extra = divmod(max(0, total), n)
    return [base + (1 if i < extra else 0) for i in range(n)]


async def parent_amain(args: argparse.Namespace, procs: int) -> None:
    """分片父进程：对外控制端口 + 聚合观测 + 子进程监督。

    子进程 = 同一个脚本加 --shard（控制口绑 127.0.0.1:父端口+1+i，
    report_loop 安静化），并发/大文件数按份拆给各分片。对外接口与单
    进程完全一致（/status、PUT /concurrency、PUT /big），脚本和演示
    文档不用区分形态。
    """
    import os

    shares = _split(min(args.concurrency, MAX_CONCURRENCY), procs)
    big_shares = _split(args.big_per_target, procs)
    ports = [args.control_port + 1 + i for i in range(procs)]
    script = os.path.abspath(__file__)

    children: list[asyncio.subprocess.Process] = []
    for i in range(procs):
        children.append(await asyncio.create_subprocess_exec(
            sys.executable, script,
            "--target", args.target,
            "--concurrency", str(shares[i]),
            "--per-req-kbs", str(args.per_req_kbs),
            "--big-per-target", str(big_shares[i]),
            "--control-port", str(ports[i]),
            "--report-s", str(args.report_s),
            "--shard"))
    log.info("压测父进程已启动 procs=%d（单进程 asyncio 只能吃满一个 "
             "CPU 核，分片突破单核瓶颈） 并发分片=%s control_port=%d",
             procs, "+".join(map(str, shares)), args.control_port)

    session = aiohttp.ClientSession(
        timeout=aiohttp.ClientTimeout(total=3))
    state = {"concurrency": sum(shares), "big": sum(big_shares)}

    async def _fan_get(path: str) -> list[dict]:
        out = []
        for port in ports:
            try:
                async with session.get(
                        f"http://127.0.0.1:{port}{path}") as r:
                    out.append(await r.json())
            except Exception as e:
                out.append({"error": str(e), "_port": port})
        return out

    async def _fan_put(path: str, values: list[int], key: str) -> None:
        for port, v in zip(ports, values):
            try:
                async with session.put(
                        f"http://127.0.0.1:{port}{path}",
                        data=json.dumps({key: v})) as r:
                    await r.read()
            except Exception as e:
                log.warning("分片调节失败 port=%d err=%s", port, e)

    def _aggregate(parts: list[dict]) -> dict:
        targets: dict[str, dict] = {}
        agg = {"concurrency": 0, "rate_bytes_per_s": 0.0,
               "rate_requests_per_s": 0.0, "bytes_total": 0,
               "requests_total": 0, "errors_total": 0,
               "big_downloads": [], "big_per_target": 0}
        alive = 0
        for part in parts:
            if "error" in part:
                continue
            alive += 1
            for k in ("concurrency", "rate_bytes_per_s",
                      "rate_requests_per_s", "bytes_total",
                      "requests_total", "errors_total", "big_per_target"):
                agg[k] += part.get(k, 0)
            agg["big_downloads"].extend(part.get("big_downloads", []))
            for t in part.get("targets", []):
                d = targets.setdefault(t["target"], dict(
                    t, workers=0, big_workers=0, rate_bytes_per_s=0,
                    rate_mbps=0.0, rate_requests_per_s=0.0,
                    requests_total=0, errors_total=0,
                    big_done_total=0, big_interrupted_total=0))
                for k in ("workers", "big_workers", "rate_bytes_per_s",
                          "rate_mbps", "rate_requests_per_s",
                          "requests_total", "errors_total",
                          "big_done_total", "big_interrupted_total"):
                    d[k] += t.get(k, 0)
                if t.get("health") != "正常":
                    d["health"] = t.get("health")
        agg["targets"] = list(targets.values())
        agg["rate_mbps"] = round(agg["rate_bytes_per_s"] * 8 / 1e6, 2)
        agg["rate_bytes_per_s"] = round(agg["rate_bytes_per_s"])
        agg["rate_requests_per_s"] = round(agg["rate_requests_per_s"], 1)
        agg["max_concurrency"] = MAX_CONCURRENCY
        agg["per_req_kbs"] = args.per_req_kbs
        agg["procs"] = procs
        agg["shards_alive"] = alive
        return agg

    async def handle_status(_request: web.Request) -> web.Response:
        return web.json_response(_aggregate(await _fan_get("/status")))

    async def handle_set_concurrency(request: web.Request) -> web.Response:
        body = (await request.text()).strip()
        try:
            parsed = json.loads(body)
            value = parsed["concurrency"] if isinstance(parsed, dict) else parsed
            n = int(value)
        except (ValueError, TypeError, KeyError):
            return web.json_response(
                {"error": "请求体须为 {\"concurrency\": N} 或裸整数 N",
                 "got": body[:200]}, status=400)
        n = max(0, min(n, MAX_CONCURRENCY))
        prev = state["concurrency"]
        await _fan_put("/concurrency", _split(n, procs), "concurrency")
        state["concurrency"] = n
        log.info("并发数已在线调整（分片下发） from=%d applied=%d procs=%d",
                 prev, n, procs)
        return web.json_response(
            {"previous": prev, "requested": n, "concurrency": n,
             "procs": procs})

    async def handle_set_big(request: web.Request) -> web.Response:
        body = (await request.text()).strip()
        try:
            parsed = json.loads(body)
            value = parsed["per_target"] if isinstance(parsed, dict) else parsed
            n = int(value)
        except (ValueError, TypeError, KeyError):
            return web.json_response(
                {"error": "请求体须为 {\"per_target\": N} 或裸整数 N",
                 "got": body[:200]}, status=400)
        n = max(0, min(n, MAX_BIG_PER_TARGET))
        prev = state["big"]
        await _fan_put("/big", _split(n, procs), "per_target")
        state["big"] = n
        return web.json_response(
            {"previous": prev, "requested": n, "big_per_target": n})

    app = web.Application()
    app.router.add_get("/", handle_status)
    app.router.add_get("/status", handle_status)
    app.router.add_put("/concurrency", handle_set_concurrency)
    app.router.add_post("/concurrency", handle_set_concurrency)
    app.router.add_put("/big", handle_set_big)
    app.router.add_post("/big", handle_set_big)
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", args.control_port).start()

    async def report():
        while True:
            await asyncio.sleep(args.report_s)
            agg = _aggregate(await _fan_get("/status"))
            rows = []
            name_w = max(24, max((_disp_width(t["target"])
                                  for t in agg["targets"]), default=24))
            for t in sorted(agg["targets"], key=lambda x: x["target"]):
                rows.append("  ".join([
                    _pad(t["target"], name_w),
                    _pad(str(t["workers"]), 4, True),
                    _pad(f"{t['rate_bytes_per_s'] * 8 / 1e6:.1f}", 8, True),
                    _pad(f"{t['rate_requests_per_s']:.1f}", 6, True),
                    _pad(str(t["errors_total"]), 8, True),
                    _pad(t.get("health", "?"), 4, True),
                ]))
            log.info("吞吐观测（聚合 %d 分片）concurrency=%d total_mbps=%.1f "
                     "total_req_per_s=%.1f errors_total=%d\n  %s",
                     agg["shards_alive"], agg["concurrency"],
                     agg["rate_mbps"], agg["rate_requests_per_s"],
                     agg["errors_total"], "\n  ".join(rows))

    report_task = asyncio.create_task(report())
    waiters = {asyncio.create_task(c.wait()): i
               for i, c in enumerate(children)}
    try:
        done, _ = await asyncio.wait(waiters, return_when=asyncio.FIRST_COMPLETED)
        # 任一分片退出都整体退出（compose restart 拉起全套）——比带着
        # 缺一片的状态继续跑诚实。
        idx = waiters[done.pop()]
        log.error("压测分片 %d 意外退出（rc=%s），父进程整体退出",
                  idx, children[idx].returncode)
        raise SystemExit(1)
    finally:
        report_task.cancel()
        for c in children:
            if c.returncode is None:
                c.terminate()
        await asyncio.gather(*(c.wait() for c in children),
                             return_exceptions=True)
        await session.close()
        await runner.cleanup()


async def amain(args: argparse.Namespace) -> None:
    # 连接由各 worker 自建（每 worker 独立 keep-alive session，周期轮转，
    # 见 _worker/REQUESTS_PER_CONNECTION 注释），这里只负责压测器骨架。
    targets = [u.strip() for u in args.target.split(",") if u.strip()]
    gen = LoadGen(targets, per_req_kbs=args.per_req_kbs)
    applied = gen.set_concurrency(args.concurrency)
    applied_big = gen.set_big_per_target(args.big_per_target)

    runner = web.AppRunner(make_control_app(gen))
    await runner.setup()
    # 分片子进程只接受父进程的内部调度，控制口绑回环。
    bind = "127.0.0.1" if args.shard else "0.0.0.0"
    site = web.TCPSite(runner, bind, args.control_port)
    await site.start()

    if args.shard:
        log.info("压测分片已启动 control=127.0.0.1:%d concurrency=%d "
                 "big_per_target=%d", args.control_port, applied, applied_big)
    else:
        log.info(
            "压测服务已启动（小文件短连接场景：每请求限读 %.0f KB/s、读完即断、"
            "不复用连接） targets=%s concurrency=%d big_per_target=%d control_port=%d "
            "（在线调节：curl -X PUT http://<host>:%d/concurrency -d '{\"concurrency\": N}'；"
            "大文件长连接：PUT /big -d '{\"per_target\": N}'）",
            gen.per_req_rate / 1024.0,
            ",".join(targets), applied, applied_big,
            args.control_port, args.control_port)
    try:
        await gen.report_loop(args.report_s, quiet=args.shard)
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
    parser.add_argument("--per-req-kbs", type=float,
                        default=float(env_int("LOADGEN_PER_REQ_KBS",
                                              int(DEFAULT_PER_REQ_KBS), "loadgen")),
                        help="每请求的客户端侧限读速度（KB/s，默认 %(default)s，"
                             "环境变量 LOADGEN_PER_REQ_KBS；0 = 不限）")
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
    parser.add_argument("--procs", type=int,
                        default=env_int("LOADGEN_PROCS", DEFAULT_PROCS,
                                        "loadgen"),
                        help="压测进程数（默认 %(default)s=自动按 CPU 核数，"
                             "封顶 16；1=单进程。单进程 asyncio 只能吃满"
                             "一个核，上万并发要靠多进程分片；环境变量 "
                             "LOADGEN_PROCS）")
    parser.add_argument("--shard", action="store_true",
                        help=argparse.SUPPRESS)   # 内部：分片子进程标记
    args = parser.parse_args()

    if args.concurrency < 0 or args.concurrency > MAX_CONCURRENCY:
        print(f"loadgen: --concurrency 必须在 0-{MAX_CONCURRENCY} 范围内，"
              f"当前值 {args.concurrency}", file=sys.stderr)
        raise SystemExit(1)
    if args.big_per_target < 0 or args.big_per_target > MAX_BIG_PER_TARGET:
        print(f"loadgen: --big-per-target 必须在 0-{MAX_BIG_PER_TARGET} 范围内，"
              f"当前值 {args.big_per_target}", file=sys.stderr)
        raise SystemExit(1)
    if args.per_req_kbs < 0:
        print(f"loadgen: --per-req-kbs 不能为负数，当前值 {args.per_req_kbs}",
              file=sys.stderr)
        raise SystemExit(1)
    if args.report_s <= 0:
        print(f"loadgen: --report-s 必须 > 0，当前值 {args.report_s}",
              file=sys.stderr)
        raise SystemExit(1)
    if args.procs < 0 or args.procs > MAX_PROCS:
        print(f"loadgen: --procs 必须在 0-{MAX_PROCS} 范围内（0=自动），"
              f"当前值 {args.procs}", file=sys.stderr)
        raise SystemExit(1)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S%z")
    import os
    procs = args.procs if args.procs else min(os.cpu_count() or 1, MAX_PROCS)
    try:
        if procs > 1 and not args.shard:
            asyncio.run(parent_amain(args, procs))
        else:
            asyncio.run(amain(args))
    except KeyboardInterrupt:
        pass
    log.info("压测服务已停止")


if __name__ == "__main__":
    main()
