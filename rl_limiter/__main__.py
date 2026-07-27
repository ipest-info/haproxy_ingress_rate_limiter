# rl_limiter.__main__ —— rl-limiter 服务入口（监控单元=节点）。
#
# 限速由各台 HAProxy 自身的 shared bwlim（聚合限速，配置常量 + reload
# 调整）执行；rl-limiter 每秒采样受控 frontend 的 bytes_out，按节点聚合
# 出带宽视图（Web 控制台实时展示），并对照配置库中登记的节点限额做持续
# 超限告警。与配置来源断联时按最后一次加载的配置继续监控（fail-static）。
#
# 两种部署形态，由 RL_NODE_NAME 决定：
#
#   - **同机部署（默认推荐，设 RL_NODE_NAME=<本机节点名>）**：rl-limiter
#     与 HAProxy 装在同一台服务器上，每台机器一个实例，只采本机那台
#     HAProxy（走本机 unix stats socket，不占网络端口）。配置库仍是全量
#     权威、仍做全量校验，只是校验后被裁剪到本机（config.scope_to_node）。
#     好处：stats socket 不必对内网开放；一台机器的监控故障不外溢；
#     监控进程与被监控对象同生共死，不存在"跨机网络分区导致误判"。
#   - **集中监控（不设 RL_NODE_NAME，兼容保留）**：一个实例通过内网 TCP
#     采样多台 HAProxy，提供跨节点的环境聚合视图。

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import signal
import sys

from . import config as configmod
from . import dbconfig
from . import haproxy, model
from . import webconsole
from .collector import Collector
from .loop import MonitorLoop

# 服务版本：优先取安装元数据（pip install 后即 pyproject 里的版本号），
# 源码目录直跑等取不到元数据的场景回退为 "dev"。控制台页面展示它，
# 便于灰度期间核对各实例版本。
try:
    from importlib import metadata as _metadata

    SERVICE_VERSION = _metadata.version("rl-limiter")
except Exception:  # pragma: no cover - 未安装场景
    SERVICE_VERSION = "dev"

# 同机部署模式开关：设为本机在配置库 haproxy_nodes 里的节点名，该实例
# 就只采本机那台 HAProxy（配置在全量校验后被裁剪到本机）。不设则退回
# 集中监控形态（一个实例采多台）。
ENV_NODE_NAME = "RL_NODE_NAME"
# Web 控制台监听地址。默认只绑回环：控制台**没有鉴权**且带写接口（改
# 限额、改挂载点、删环境），默认对外可达是不可接受的。要让同网段访问，
# 由运维显式设成内网地址并配合防火墙/安全组限制来源。
ENV_CONSOLE_BIND = "RL_CONSOLE_BIND"
DEFAULT_CONSOLE_BIND = "127.0.0.1"


def _summarize_nodes(nodes: list[model.NodeConfig]) -> str:
    """把受控 HAProxy 节点清单压缩成单个日志字段，格式：
    "name=hap-1,endpoint=/run/haproxy/admin.sock;..."（同机形态）或
    "name=hap-1,endpoint=10.0.0.11:9999;..."（跨机 TCP 形态）。"""
    return ";".join(
        f"name={n.name},endpoint={n.endpoint()}"
        for n in nodes
    )


def _summarize_envs(envs: list[model.EnvQuota]) -> str:
    """把监控单元（节点）清单压缩成单个日志字段，格式：
    "env_id=hap-1,quota_bps=40000000,targets=hap-1/fe_a;..."。
    env_id 字段承载节点名（监控单元=节点，见 model.py 顶部说明）；
    quota_bps 为配置口径的 bits/s；targets 为 节点/前端 二元组。"""
    return ";".join(
        "env_id={},quota_bps={},targets={}".format(
            e.env_id, e.quota_bits_per_sec, "|".join(str(t) for t in e.targets))
        for e in envs
    )


async def _amain(cfg, log: logging.Logger,
                 db_opts: dbconfig.MySQLOptions | None = None,
                 console_port: int = 0,
                 logbuf: "webconsole.LogBuffer | None" = None,
                 scope_node: str | None = None,
                 console_bind: str = DEFAULT_CONSOLE_BIND) -> None:
    """事件循环内的主体：组装组件、引导配置、并发运行监控循环与配置源。

    db_opts 非 None 表示配置来自 MySQL（数据库配置模式，生产权威）：
    额外运行一个数据库轮询任务，把配置变化经配置队列热应用到监控循环；
    db_opts 为 None 时按引导时的本地 YAML 静态运行（standalone）。

    console_port 非 0 时启动内置 Web 控制台（实时观测，见 webconsole
    模块）：sampler 每拍向 StatusHub 发布一帧快照，配置热更经中继队列
    同步给控制台的配置视图。
    """
    # --- 组装与各台 HAProxy 的 runtime API 客户端（只读采样）。同机形态
    # 下 cfg.nodes 已被裁剪为本机一条，这里天然只建一个 unix 客户端。
    # 客户端字典以节点名为键，与 Target.node / NodeConfig.name 对齐。
    clients = {
        n.name: haproxy.RuntimeClient.from_node(n, log)
        for n in cfg.nodes
    }

    col = Collector(clients, log)

    # --- Web 控制台（可选）：StatusHub 是监控数据的发布枢纽。
    # version_fn 是延迟求值闭包（ctl 在下方才赋值，闭包只会在运行期被
    # 调用，届时 ctl 已完成构造）。
    ctl: MonitorLoop
    hub: webconsole.StatusHub | None = None
    if console_port:
        hub = webconsole.StatusHub(
            SERVICE_VERSION,
            version_fn=lambda: ctl.version,
            nodes=cfg.nodes,
            degraded_fn=col.degraded_nodes,
            scope_node=scope_node)

    sampler = hub.record if hub is not None else None
    ctl = MonitorLoop(col, sampler=sampler, log=log)

    # --- 启动引导（seed）：用加载到的配置（数据库或本地 YAML）构造首份
    # 运行期配置直接喂给监控循环。数据库配置模式下引导配置的版本号取
    # 内容校验和，并把它作为轮询任务的变更检测基准——首轮轮询读到同样
    # 内容时不会再触发一次重复应用。
    env_groups = {
        k: list(v) for k, v in (getattr(cfg, "env_groups", {}) or {}).items()
    }
    seed_version = (
        dbconfig.config_checksum(cfg.envs, env_groups)
        if db_opts is not None else 0
    )
    if cfg.envs:
        seed_cfg = model.ControllerConfig(
            version=seed_version, envs=cfg.envs, env_groups=env_groups)
        ctl.seed(seed_cfg)
        if hub is not None:
            hub.update_config(seed_cfg)
        log.info("已用%s配置完成引导（监控单元=节点） version=%s "
                 "units=%d units_detail=%s",
                 "数据库" if db_opts is not None else "本地静态",
                 seed_version, len(cfg.envs), _summarize_envs(cfg.envs))
    else:
        # 配置里没有任何挂载点（数据库尚未录入 env_targets 等）：空转
        # 启动并等待配置轮询送来首份有效配置。
        log.warning("引导配置中没有任何挂载点，暂无监控对象，等待配置热更")

    # --- 信号处理：SIGINT/SIGTERM 触发优雅退出（记录信号名后取消任务）。
    stop = asyncio.Event()
    ev = asyncio.get_running_loop()

    def _on_signal(name: str) -> None:
        log.info("收到退出信号，开始优雅停机 signal=%s", name)
        stop.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            ev.add_signal_handler(sig, _on_signal, sig.name)
        except NotImplementedError:  # pragma: no cover - 非 Unix 平台兜底
            signal.signal(sig, lambda *_a, _n=sig.name: _on_signal(_n))

    # --- 并发运行：监控循环 + 配置源（数据库轮询）+ 可选的 Web 控制台。
    # config_queue 是监控循环的配置入口（配置优先于 tick）：数据库模式下
    # 由轮询任务投递内容变化；standalone（本地 YAML）没有运行期配置源，
    # 传 None，循环按引导配置静态运行。
    tick_interval_s = getattr(cfg, "tick_interval_s", 1.0) or 1.0
    tasks: list[asyncio.Task] = []

    source_queue: asyncio.Queue | None = None
    if db_opts is not None:
        source_queue = asyncio.Queue()

    # 控制台需要跟随配置热更（限额参考线、分组展示）：在源队列与循环
    # 之间加一级中继，把每份新配置先喂给 hub 再原样转投循环——配置视图
    # 与循环实际应用的内容出自同一份对象，永不发散。无控制台时直连。
    config_queue = source_queue
    if hub is not None and source_queue is not None:
        relay_queue: asyncio.Queue = asyncio.Queue()

        async def _config_relay(src: asyncio.Queue, dst: asyncio.Queue):
            while True:
                c = await src.get()
                hub.update_config(c)
                dst.put_nowait(c)

        tasks.append(asyncio.create_task(
            _config_relay(source_queue, relay_queue),
            name="console-config-relay"))
        config_queue = relay_queue

    tasks.append(asyncio.create_task(
        ctl.run(config_queue, tick_interval_s), name="monitor-loop"))
    if db_opts is not None:
        tasks.append(asyncio.create_task(
            dbconfig.watch(db_opts, source_queue, cfg, log, scope_node),
            name="db-config-watch"))
    if hub is not None:
        tasks.append(asyncio.create_task(
            webconsole.run_console(console_port, hub, logbuf, db_opts, log,
                                   bind=console_bind),
            name="web-console"))

    log.info("rl-limiter 服务已启动，监控循环开始运行 mode=%s nodes=%d version=%s",
             f"同机（本机节点 {scope_node}）" if scope_node else "集中监控",
             len(cfg.nodes), SERVICE_VERSION)

    # 等待退出信号；任一常驻任务意外结束（本应永续运行）也触发整体退出，
    # 交由 systemd Restart=always 拉起，比带着半残状态继续跑更安全。
    stop_task = asyncio.create_task(stop.wait(), name="stop-signal")
    done, _pending = await asyncio.wait(
        [stop_task, *tasks], return_when=asyncio.FIRST_COMPLETED)
    for t in done:
        if t is not stop_task and t.exception() is not None:
            log.error("常驻任务异常退出，服务整体退出交由 systemd 拉起 "
                      "task=%s err=%s", t.get_name(), t.exception())
    for t in (stop_task, *tasks):
        t.cancel()
    await asyncio.gather(stop_task, *tasks, return_exceptions=True)


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="rl-limiter",
        description="HAProxy 入口带宽监控服务：采样 HAProxy 的下行带宽，"
                    "按节点展示并做持续超限告警"
                    "（限速由各节点 HAProxy 的 shared bwlim 配置执行）",
        epilog="部署形态：设置 RL_NODE_NAME=<本机节点名> 进入同机部署模式"
               "（与 HAProxy 同机，只采本机那台，推荐）；不设则为集中监控"
               "模式（一个实例采多台）。"
               "配置来源二选一：设置 RL_MYSQL_HOST（及 RL_MYSQL_PORT/USER/"
               "PASSWORD/DB/POLL_S）后从 MySQL 数据库读取配置并轮询热更新；"
               "未设置时回落到 -c 指定的本地 YAML 文件。"
               "Web 控制台：设置 RL_CONSOLE_PORT 启用，RL_CONSOLE_BIND 指定"
               "监听地址（默认 127.0.0.1——控制台无鉴权且带写接口）。")
    parser.add_argument(
        "-c", "--config", default="/etc/rl-limiter/config.yaml",
        help="YAML 配置文件路径（默认 %(default)s；设置 RL_MYSQL_HOST 时忽略）")
    parser.add_argument(
        "--version", action="store_true", help="打印版本号后退出")
    args = parser.parse_args()

    if args.version:
        print("rl-limiter", SERVICE_VERSION)
        return

    # 日志先以 INFO 起步：数据库配置模式下"等待 MySQL 就绪"的重试告警
    # 发生在配置加载完成之前，此时还不知道配置里的 log_level；加载成功后
    # 再把根 logger 调到配置指定的级别。格式固定为"时间 级别 消息"三段；
    # 消息本体统一为中文描述 + 英文 snake_case 的 key=value 键值对，便于
    # grep 与日志采集系统按字段解析。
    logging.basicConfig(
        stream=sys.stderr,
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S%z",
    )
    log = logging.getLogger("rl_limiter")

    # 内置 Web 控制台（可选）：设置 RL_CONSOLE_PORT 即启用。日志环形缓冲
    # 在这里（配置加载之前）就挂上根 logger，启动阶段的日志也能在页面回看。
    console_port = 0
    raw_console = (os.environ.get("RL_CONSOLE_PORT") or "").strip()
    if raw_console:
        try:
            console_port = int(raw_console)
        except ValueError:
            print(f"rl-limiter: 环境变量 RL_CONSOLE_PORT 必须是整数，"
                  f"当前值 {raw_console!r}", file=sys.stderr)
            raise SystemExit(1)
        if console_port < 1 or console_port > 65535:
            print(f"rl-limiter: 环境变量 RL_CONSOLE_PORT 必须在 1-65535 "
                  f"范围内，当前值 {console_port}", file=sys.stderr)
            raise SystemExit(1)
    logbuf: webconsole.LogBuffer | None = None
    if console_port:
        logbuf = webconsole.LogBuffer()
        logging.getLogger().addHandler(logbuf)
    console_bind = (os.environ.get(ENV_CONSOLE_BIND) or "").strip() \
        or DEFAULT_CONSOLE_BIND

    # 部署形态判定：RL_NODE_NAME 已设置 → 同机部署（只采本机那台 HAProxy）；
    # 未设置 → 集中监控（一个实例采多台）。
    scope_node = (os.environ.get(ENV_NODE_NAME) or "").strip() or None

    # 配置来源判定：RL_MYSQL_HOST 已设置 → 数据库配置模式；否则本地 YAML。
    try:
        db_opts = dbconfig.from_env()
    except ValueError as e:
        print(f"rl-limiter: {e}", file=sys.stderr)
        raise SystemExit(1)

    try:
        if db_opts is not None:
            # 启动加载单独跑一个事件循环：加载内含"等待数据库就绪"的重试，
            # 与主循环生命周期无关，分开跑让失败路径干净退出。
            cfg = asyncio.run(
                dbconfig.load_service_config(db_opts, log, scope_node))
        else:
            cfg = configmod.load(args.config, scope_node)
    except KeyboardInterrupt:
        # 等待数据库就绪的重试窗口（最长两分钟）里按 Ctrl-C 是常规操作，
        # 必须干净退出；KeyboardInterrupt 是 BaseException，不加这条会
        # 绕过下面的 except Exception 直接冲出 main 打印原始 traceback。
        print("rl-limiter: 启动在配置加载阶段被中断（Ctrl-C），已退出",
              file=sys.stderr)
        raise SystemExit(130)
    except Exception as e:
        print(f"rl-limiter: {e}", file=sys.stderr)
        raise SystemExit(1)

    level = getattr(logging, str(cfg.log_level).upper(), logging.INFO)
    logging.getLogger().setLevel(level)

    config_source = db_opts.describe() if db_opts is not None else f"文件 {args.config}"

    # 启动即输出完整配置摘要：现场排障时第一条要看的日志，可直接核对
    # 受控节点清单、限额基准与配置来源。
    log.info(
        "服务配置加载完成，以下为完整配置摘要（排障第一条要看的日志） "
        "config_source=%s deploy_mode=%s scope_node=%s log_level=%s "
        "tick_interval_s=%s nodes=%d nodes_detail=%s units=%d units_detail=%s",
        config_source,
        "colocated" if scope_node else "central",
        scope_node or "-",
        cfg.log_level,
        getattr(cfg, "tick_interval_s", 1.0),
        len(cfg.nodes), _summarize_nodes(cfg.nodes),
        len(cfg.envs), _summarize_envs(cfg.envs),
    )

    try:
        asyncio.run(_amain(cfg, log, db_opts,
                           console_port=console_port, logbuf=logbuf,
                           scope_node=scope_node, console_bind=console_bind))
    except KeyboardInterrupt:  # 信号处理兜底：极端时序下直接吞掉干净退出
        pass
    log.info("rl-limiter 服务已停止")


if __name__ == "__main__":
    main()
