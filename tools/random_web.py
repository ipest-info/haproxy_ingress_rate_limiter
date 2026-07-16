#!/usr/bin/env python3
# tools/random_web.py —— HAProxy 后端的模拟业务 web 服务。
#
# 角色：docker compose 演示环境里挂在 HAProxy backend 后面的"真实业务"
# 替身。每个请求返回一段**随机大小**的响应体（在 [min-bytes, max-bytes]
# 区间内均匀抽取），用来模拟大小不一的业务响应（页面、图片、下载分片），
# 让 HAProxy frontend 的 bytes_out 产生足够真实的波动供 rl-limiter 采样。
#
# 实现要点：
#   - 载荷来自启动时一次性生成的随机字节池，响应只做零拷贝切片——
#     不在请求路径上调用 os.urandom（大响应体会把 CPU 烧在生成随机数上，
#     压测时瓶颈必须留给网络/整形，而不是这个替身）；
#   - 响应头 X-Payload-Bytes 携带本次响应大小，便于压测端/抓包核对；
#   - Content-Type 用 application/octet-stream 并显式关闭压缩协商，
#     保证线上传输字节数 == 载荷字节数，对齐限速的计费口径；
#   - /healthz 是编排层健康检查端点，返回固定 "ok"（不计入随机载荷）。
#
# 用法（docker compose 中由 compose 文件传参）：
#   python3 tools/random_web.py --port 9000 --min-bytes 262144 --max-bytes 2097152
# 参数也可用环境变量 WEB_PORT / WEB_MIN_BYTES / WEB_MAX_BYTES 覆盖默认值。

from __future__ import annotations

import argparse
import logging
import random
import sys

from aiohttp import web

from _common import env_int

log = logging.getLogger("random_web")

DEFAULT_PORT = 9000
DEFAULT_MIN_BYTES = 256 * 1024       # 256 KiB
DEFAULT_MAX_BYTES = 2 * 1024 * 1024  # 2 MiB


def make_app(min_bytes: int, max_bytes: int) -> web.Application:
    # 随机字节池：一次性生成 max_bytes 大小，之后所有响应都是它的切片。
    # 用 random.randbytes 而非 os.urandom——这里只需要"不可压缩的填充"，
    # 不需要密码学强度。
    pool = random.randbytes(max_bytes)

    async def handle_healthz(_request: web.Request) -> web.Response:
        return web.Response(text="ok")

    async def handle_any(_request: web.Request) -> web.Response:
        n = random.randint(min_bytes, max_bytes)
        # memoryview 切片是 O(1) 零拷贝（bytes 切片会 memcpy 一份最多
        # 2 MiB 的新对象，压测下白烧 CPU），aiohttp 原生接受 memoryview
        # 作为响应体。
        return web.Response(
            body=memoryview(pool)[:n],
            content_type="application/octet-stream",
            headers={
                "X-Payload-Bytes": str(n),
                # 明确禁止中间层压缩/改写，保证传输字节数即载荷字节数。
                "Cache-Control": "no-store, no-transform",
            },
        )

    app = web.Application()
    # 精确路由先注册：aiohttp 按注册顺序解析，/healthz 不会落进通配。
    app.router.add_get("/healthz", handle_healthz)
    app.router.add_get("/{tail:.*}", handle_any)
    return app


def main() -> None:
    parser = argparse.ArgumentParser(
        description="HAProxy 后端模拟 web 服务：每次请求返回随机大小的响应体")
    parser.add_argument("--port", type=int,
                        default=env_int("WEB_PORT", DEFAULT_PORT, "random-web"),
                        help="监听端口（默认 %(default)s，可用环境变量 WEB_PORT 覆盖）")
    parser.add_argument("--min-bytes", type=int,
                        default=env_int("WEB_MIN_BYTES", DEFAULT_MIN_BYTES, "random-web"),
                        help="响应体最小字节数（默认 %(default)s，环境变量 WEB_MIN_BYTES）")
    parser.add_argument("--max-bytes", type=int,
                        default=env_int("WEB_MAX_BYTES", DEFAULT_MAX_BYTES, "random-web"),
                        help="响应体最大字节数（默认 %(default)s，环境变量 WEB_MAX_BYTES）")
    args = parser.parse_args()

    if args.min_bytes < 1 or args.max_bytes < args.min_bytes:
        print(f"random-web: 字节区间非法（要求 1 <= min <= max），"
              f"当前 min={args.min_bytes} max={args.max_bytes}", file=sys.stderr)
        raise SystemExit(1)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S%z")

    app = make_app(args.min_bytes, args.max_bytes)
    log.info("模拟业务后端已开始监听（每次请求返回随机大小响应） "
             "port=%d min_bytes=%d max_bytes=%d",
             args.port, args.min_bytes, args.max_bytes)
    # access_log=None：压测场景下每秒成百上千请求，逐条访问日志只会刷屏。
    web.run_app(app, host="0.0.0.0", port=args.port, access_log=None, print=None)
    log.info("模拟业务后端已停止")


if __name__ == "__main__":
    main()
