#!/usr/bin/env python3
# tools/bootstrap_db.py —— 把本机登记进配置库，并保证它至少有一个 frontend。
#
# 为什么需要这一步：rl-limiter 启动时会做两件事的校验——
#   1. RL_NODE_NAME 必须在 haproxy_instances 里有对应行，否则直接失败退出
#      （容忍它会让服务"看起来在跑"却什么都采不到）；
#   2. frontends 不能为空（那意味着撤掉全部限速，是事故而不是配置操作）。
# 也就是说**库里空着的时候服务根本起不来**，而 frontend 平时是在 Web 控制台
# 上加的、控制台又要服务起来才有——先有鸡还是先有蛋。这个脚本就是来打破这
# 个循环的：装机时跑一次，之后都在控制台上改。
#
# ## 幂等语义（重要）
#
#   - 实例行：upsert。接线信息（socket 路径 / 超时）以命令行为准——它描述的
#     是"这台机器长什么样"，重装时按新值更新是对的。
#   - frontend 与后端服务器：**只在不存在时创建，已有的一个字节都不碰**。
#     限额是运维在控制台上调出来的值，重跑装机脚本把它冲回默认值是不可接受
#     的故障。所以这里用 INSERT IGNORE，不是 REPLACE、也不是 ON DUPLICATE
#     KEY UPDATE。
#
# 用法（接入参数与 rl-limiter 一样从 RL_MYSQL_* 环境变量读）：
#
#   python3 tools/bootstrap_db.py --instance hap-1 \
#       --socket /run/haproxy/admin.sock \
#       --frontend fe_main --port 8080 --quota-mbps 1000 \
#       --backend 10.0.0.21:9000 --backend 10.0.0.22:9000
#
#   # 只登记实例、不建 frontend（库里已经有了）
#   python3 tools/bootstrap_db.py --instance hap-1 --socket /run/haproxy/admin.sock

from __future__ import annotations

import argparse
import asyncio
import os
import sys
import warnings

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from rl_limiter import dbconfig  # noqa: E402

# 等库就绪的上限。docker compose 起 MySQL 首次要跑 init.sql，慢的机器
# 上一分钟很正常，所以给得比较宽。
WAIT_TIMEOUT_S = 180
WAIT_INTERVAL_S = 2


def parse_backend(spec: str) -> tuple[str, int]:
    """`地址:端口` → (地址, 端口)。裸 IPv6 要求写成 [::1]:9000。"""
    if spec.startswith("["):
        host, _, port = spec.partition("]:")
        host = host[1:]
    else:
        host, _, port = spec.rpartition(":")
    if not host or not port.isdigit() or not (1 <= int(port) <= 65535):
        raise argparse.ArgumentTypeError(
            f"后端 {spec!r} 格式不对，应为 地址:端口（IPv6 用 [::1]:9000）")
    return host, int(port)


async def _connect(opts: dbconfig.MySQLOptions):
    import aiomysql
    return await aiomysql.connect(
        host=opts.host, port=opts.port, user=opts.user, password=opts.password,
        db=opts.database, connect_timeout=opts.connect_timeout_s,
        charset="utf8mb4", autocommit=False)


async def wait_ready(opts: dbconfig.MySQLOptions, timeout_s: int) -> None:
    """等数据库能连上。首次 docker 起库要跑 init.sql，慢是正常的。"""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout_s
    last = ""
    attempt = 0
    while True:
        attempt += 1
        try:
            conn = await _connect(opts)
            conn.close()
            print(f"配置库已就绪 {opts.host}:{opts.port}/{opts.database}"
                  f"（第 {attempt} 次尝试）")
            return
        except Exception as e:                      # noqa: BLE001 等库就绪，什么错都要重试
            last = f"{type(e).__name__}: {e}"
            if loop.time() >= deadline:
                raise SystemExit(
                    f"等配置库就绪超时（{timeout_s}s）。最后一次错误：{last}\n"
                    f"检查：容器起来了吗（docker ps）、RL_MYSQL_* 对不对、"
                    f"端口是不是只绑了回环而你在别的机器上跑")
            await asyncio.sleep(WAIT_INTERVAL_S)


async def bootstrap(opts: dbconfig.MySQLOptions, args) -> int:
    conn = await _connect(opts)
    created = []
    # INSERT IGNORE 撞主键时 MySQL 会回一条 Warning，aiomysql 把它转成
    # Python 警告打到 stderr。而"撞主键"正是本脚本重跑时的**正常路径**
    # （已有配置一个字节都不碰），刷一屏 Duplicate entry 只会让人以为出事了。
    warnings.filterwarnings("ignore", message=r".*Duplicate entry.*")
    try:
        async with conn.cursor() as cur:
            # 实例行：接线信息以命令行为准，重装时更新是对的。
            #
            # **刻意不碰 limit_scope / host_quota_mbps**：接线信息描述"这台
            # 机器长什么样"，而限速范围与限额是运维调出来的策略。新建时走
            # 列默认值（整机范围 + 不设限额 = 不限速，装完机器不会凭空多出
            # 一个闸门）；重装时保留库里已有的值，不会把人调好的限额冲掉。
            await cur.execute(
                "INSERT INTO haproxy_instances "
                "(name, host, port, socket_path, timeout_ms) "
                "VALUES (%s, NULL, NULL, %s, %s) "
                "ON DUPLICATE KEY UPDATE "
                " socket_path=VALUES(socket_path), host=NULL, port=NULL, "
                " timeout_ms=VALUES(timeout_ms)",
                (args.instance, args.socket, args.timeout_ms))
            print(f"实例已登记 name={args.instance} socket={args.socket}")

            await cur.execute(
                "SELECT COUNT(*) FROM haproxy_frontends WHERE instance=%s",
                (args.instance,))
            have = (await cur.fetchone())[0]

            if args.frontend:
                # INSERT IGNORE：**已有的 frontend 一个字节都不碰**。限额是
                # 运维在控制台上调出来的，重跑装机脚本把它冲掉是不可接受的。
                await cur.execute(
                    "INSERT IGNORE INTO haproxy_frontends "
                    "(instance, name, bind_address, bind_port, mode, "
                    " quota_mbps, balance) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s)",
                    (args.instance, args.frontend, args.bind_address,
                     args.port, args.mode, args.quota_mbps, args.balance))
                if cur.rowcount:
                    created.append(f"frontend {args.frontend}@:{args.port} "
                                   f"限额 {args.quota_mbps} Mbps")
                for i, (addr, port) in enumerate(args.backend, 1):
                    await cur.execute(
                        "INSERT IGNORE INTO haproxy_servers "
                        "(instance, frontend, name, address, port) "
                        "VALUES (%s, %s, %s, %s, %s)",
                        (args.instance, args.frontend, f"srv{i}", addr, port))
                    if cur.rowcount:
                        created.append(f"  后端 srv{i} = {addr}:{port}")
            await conn.commit()
    finally:
        conn.close()

    for line in created:
        print("已创建 " + line)
    if args.frontend and not created:
        print(f"frontend {args.frontend} 已存在，未做任何改动"
              f"（限额等配置以库里现有的为准）")
    if not args.frontend and not have:
        print("警告：这个实例名下**一个 frontend 都没有**，rl-limiter 会拒绝"
              "启动（空清单等于撤掉全部限速）。用 --frontend/--port/--backend "
              "建一个，或先在别处把库填好。", file=sys.stderr)
        return 1
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(
        description="把本机登记进 rl-limiter 配置库，并保证至少有一个 frontend")
    ap.add_argument("--instance", required=True,
                    help="本机在配置库里的实例名（= RL_NODE_NAME）")
    ap.add_argument("--socket", default="/run/haproxy/admin.sock",
                    help="本机 HAProxy 的 unix stats socket（默认 %(default)s）")
    ap.add_argument("--timeout-ms", type=int, default=500,
                    help="单次 runtime API 命令超时（默认 %(default)s）")
    ap.add_argument("--frontend", default="",
                    help="要确保存在的 frontend 名；不给则只登记实例")
    ap.add_argument("--bind-address", default="",
                    help="监听地址，留空 = 所有地址")
    ap.add_argument("--port", type=int, default=0, help="监听端口")
    ap.add_argument("--quota-mbps", type=float, default=1000.0,
                    help="限额（Mbps，默认 %(default)s）；仅在新建时生效")
    ap.add_argument("--mode", default="tcp", choices=("tcp", "http"))
    ap.add_argument("--balance", default="roundrobin")
    ap.add_argument("--backend", type=parse_backend, action="append", default=[],
                    metavar="地址:端口", help="后端服务器，可重复给")
    ap.add_argument("--wait-timeout", type=int, default=WAIT_TIMEOUT_S,
                    help="等库就绪的上限秒数（默认 %(default)s）")
    args = ap.parse_args()

    if args.frontend:
        if not (1 <= args.port <= 65535):
            ap.error("给了 --frontend 就必须给合法的 --port")
        if not args.backend:
            ap.error("给了 --frontend 就必须至少给一个 --backend"
                     "（没有后端的 frontend 会被配置校验拒绝）")

    opts = dbconfig.from_env(os.environ)
    if opts is None:
        raise SystemExit(
            "没有检测到 RL_MYSQL_HOST，本脚本只用于数据库配置模式。\n"
            "先 source 一份环境变量（如 /etc/rl-limiter/rl-limiter.env）再跑。")

    asyncio.run(wait_ready(opts, args.wait_timeout))
    return asyncio.run(bootstrap(opts, args))


if __name__ == "__main__":
    raise SystemExit(main())
