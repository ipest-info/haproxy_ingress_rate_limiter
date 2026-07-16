# rl_limiter.__main__ —— rl-limiter 服务入口（v2.0 集中部署）。
#
# rl-limiter 是与 HAProxy 分离部署的集中式限速服务：通过内网 TCP 连接
# 多台 HAProxy 的 stats socket，每秒采样各节点 frontend 的 bytes_out，
# 把同一环境分布在多台节点上的流量全局聚合后做 AIMD 决策（设计文档
# §3.3），再按各挂载点近期用量加权把整形值写回各节点的 bwlim map
# （dry-run 模式下只记录不写入）。与管理后台断联时按最后一次下发的
# 配置继续限速（fail-static，§3.7）。

from __future__ import annotations

import argparse
import asyncio
import logging
import signal
import sys

from . import config as configmod
from . import dbconfig
from . import haproxy, model
from . import reporter as reportermod
from .collector import Collector
from .executor import Executor
from .governor import Governor
from .loop import ControlLoop, executor_mode

# 服务版本：优先取安装元数据（pip install 后即 pyproject 里的版本号），
# 源码目录直跑等取不到元数据的场景回退为 "dev"。随心跳上报给管理后台，
# 便于灰度期间核对各实例版本。
try:
    from importlib import metadata as _metadata

    SERVICE_VERSION = _metadata.version("rl-limiter")
except Exception:  # pragma: no cover - 未安装场景
    SERVICE_VERSION = "dev"


def _summarize_nodes(nodes: list[model.NodeConfig]) -> str:
    """把受控 HAProxy 节点清单压缩成单个日志字段，格式：
    "name=hap-1,addr=10.0.0.11:9999,map=/etc/haproxy/maps/bwlim.map;..."。"""
    return ";".join(
        f"name={n.name},addr={n.host}:{n.port},map={n.bwlim_map_path}"
        for n in nodes
    )


def _summarize_envs(envs: list[model.EnvQuota]) -> str:
    """把环境清单压缩成单个日志字段，格式：
    "env_id=e1,quota_bps=200000000,targets=hap-1/fe_a|hap-2/fe_a;..."。
    quota_bps 为配置口径的 bits/s；targets 为 节点/前端 二元组。"""
    return ";".join(
        "env_id={},quota_bps={},targets={}".format(
            e.env_id, e.quota_bits_per_sec, "|".join(str(t) for t in e.targets))
        for e in envs
    )


async def _amain(cfg, log: logging.Logger,
                 db_opts: dbconfig.MySQLOptions | None = None) -> None:
    """事件循环内的主体：组装组件、引导配置、并发运行快环与配置源/上报器。

    db_opts 非 None 表示配置来自 MySQL（数据库配置模式）：额外运行一个
    数据库轮询任务，把 mode/envs 的变化经配置队列热应用到快环——角色上
    等价于管理后台的配置长轮询，两者不会同时启用（数据库配置里没有
    backend 段，reporter 天然关闭）。
    """
    # --- 组装与各台 HAProxy 的 runtime API 客户端（v2.0：内网 TCP） ---
    # 客户端字典以节点名为键，与 Target.node / NodeConfig.name 对齐；
    # 同一个客户端同时充当采集来源（show stat）与执行通道（set map）。
    clients = {
        n.name: haproxy.RuntimeClient(n.host, n.port, n.timeout_s, log)
        for n in cfg.nodes
    }
    map_paths = {n.name: n.bwlim_map_path for n in cfg.nodes}

    # --- 快环三组件 ---
    col = Collector(clients, log)
    gov = Governor(log)
    exe = Executor(clients, map_paths, cfg.mode, log)

    # --- 管理后台（可选）：配置了 backend.base_url 才启用上报器与配置
    # 长轮询；否则快环进入纯本地的独立运行模式（config_queue 传 None）。
    backend = getattr(cfg, "backend", None)
    backend_configured = bool(backend is not None and getattr(backend, "base_url", ""))
    rep = None
    if backend_configured:
        # mode_fn/version_fn 让心跳始终上报"实际已应用"的模式与配置版本
        # （而不是启动时的静态值）；ctl 在下方才赋值，闭包延迟求值是安全的
        # ——上报协程首次心跳前 ctl 已完成构造。
        rep = reportermod.Reporter(
            backend,
            cfg.node_id,
            SERVICE_VERSION,
            mode_fn=lambda: executor_mode(exe),
            version_fn=lambda: ctl.version,
            log=log,
        )

    # Sampler 闭包引用 ctl.version；ctl 在下方完成赋值，而 sampler 只会在
    # ctl.run 的 tick 中被调用——首次调用必然晚于赋值，延迟捕获是安全的。
    ctl: ControlLoop
    if rep is not None:
        # 接入后台：每个 tick 的结果交给上报器缓冲，按下发的间隔批量上报。
        def sampler(now, usages, decisions):
            rep.add_sample(now, usages, decisions, executor_mode(exe), ctl.version)
    else:
        sampler = None
    ctl = ControlLoop(col, gov, exe, sampler=sampler, log=log)

    # --- 启动引导（seed）优先级：后台缓存 > 本地静态 envs > 无限速等待。
    # 原因逐条说明：
    #  1. 配置了后台时优先用本地缓存的最后一次下发配置引导——这正是
    #     fail-static（§3.7）在"重启后后台恰好不可达"场景下的延伸：缓存里
    #     的配额比本地静态配置新，用它引导可保证重启前后限速行为连续，
    #     绝不放开为不限速。
    #  2. 无缓存（首次部署/缓存被清）时退回本地静态 envs：粗粒度但安全的
    #     兜底配额，同时也是纯独立运行模式的唯一配置来源。
    #  3. 两者皆无时不限速启动，等待后台首次下发——此时尚无任何配额信息，
    #     凭空限速比不限速更危险（可能误伤全部流量），故记 warn 提醒运维
    #     这是一个需要关注的空窗期。
    seeded = False
    if backend_configured:
        cache_path = getattr(backend, "cache_path", "")
        cached = None
        cache_err: Exception | None = None
        try:
            cached = reportermod.load_cache(cache_path)
        except Exception as e:  # 缓存缺失/损坏都不是致命错误，往下走兜底链
            cache_err = e
        if cached is not None:
            ctl.seed(cached)
            seeded = True
            log.info(
                "已用本地缓存的后台配置完成引导（fail-static，重启前后限速"
                "行为连续） path=%s version=%s mode=%s envs=%d",
                cache_path, cached.version, cached.mode, len(cached.envs))
        else:
            log.warning("后台配置缓存不可用（首次部署或缓存损坏），转本地"
                        "静态配额兜底 path=%s err=%s",
                        cache_path, cache_err if cache_err is not None else "empty")
    # 数据库配置模式：引导配置的版本号取内容校验和，并把它作为轮询任务的
    # 变更检测基准——首轮轮询读到同样内容时不会再触发一次重复应用。
    seed_version = (
        dbconfig.config_checksum(cfg.mode, cfg.envs) if db_opts is not None else 0
    )
    if not seeded and cfg.envs:
        ctl.seed(model.ControllerConfig(
            version=seed_version, mode=cfg.mode, envs=cfg.envs))
        seeded = True
        log.info("已用%s环境配额完成引导 version=%s mode=%s envs=%d envs_detail=%s",
                 "数据库下发的" if db_opts is not None else "本地静态",
                 seed_version, cfg.mode, len(cfg.envs), _summarize_envs(cfg.envs))
    if not seeded:
        log.warning(
            "无任何引导配置，暂不限速，等待后台首次下发（注意此空窗期） "
            "backend_base_url=%s",
            getattr(backend, "base_url", "") if backend is not None else "")

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

    # --- 并发运行：快环 + 配置源（数据库轮询或后台上报器，二选一）。
    # config_queue 是快环的配置入口（配置优先于 tick）：数据库模式下由
    # 轮询任务投递内容变化；后台模式下由上报器的长轮询投递下发配置。
    config_queue: asyncio.Queue | None = None
    tick_interval_s = getattr(cfg, "tick_interval_s", 1.0) or 1.0
    tasks: list[asyncio.Task] = []
    if db_opts is not None:
        config_queue = asyncio.Queue()
        tasks.append(asyncio.create_task(
            dbconfig.watch(db_opts, config_queue, cfg, log),
            name="db-config-watch"))
    elif rep is not None:
        config_queue = rep.configs
    tasks.insert(0, asyncio.create_task(
        ctl.run(config_queue, tick_interval_s), name="control-loop"))
    if rep is not None:
        tasks.append(asyncio.create_task(rep.run(), name="reporter"))

    log.info("rl-limiter 服务已启动，快环与上报器开始运行 "
             "node_id=%s mode=%s nodes=%d backend=%s version=%s",
             cfg.node_id, executor_mode(exe), len(cfg.nodes),
             getattr(backend, "base_url", "") if backend is not None else "",
             SERVICE_VERSION)

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
        description="集中式 HAProxy 入口带宽限速服务：远程控制多台 HAProxy，"
                    "保证各环境下行带宽不超约定配额",
        epilog="配置来源二选一：设置 RL_MYSQL_HOST（及 RL_MYSQL_PORT/USER/"
               "PASSWORD/DB/POLL_S）后从 MySQL 数据库读取配置并轮询热更新；"
               "未设置时回落到 -c 指定的本地 YAML 文件。")
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
            cfg = asyncio.run(dbconfig.load_service_config(db_opts, log))
        else:
            cfg = configmod.load(args.config)
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
    # 服务身份、模式、受控节点清单与配额来源。
    backend = getattr(cfg, "backend", None)
    log.info(
        "服务配置加载完成，以下为完整配置摘要（排障第一条要看的日志） "
        "config_source=%s node_id=%s mode=%s log_level=%s tick_interval_s=%s "
        "nodes=%d nodes_detail=%s envs=%d envs_detail=%s "
        "backend_configured=%s backend_base_url=%s cache_path=%s",
        config_source, cfg.node_id, cfg.mode, cfg.log_level,
        getattr(cfg, "tick_interval_s", 1.0),
        len(cfg.nodes), _summarize_nodes(cfg.nodes),
        len(cfg.envs), _summarize_envs(cfg.envs),
        bool(backend is not None and getattr(backend, "base_url", "")),
        getattr(backend, "base_url", "") if backend is not None else "",
        getattr(backend, "cache_path", "") if backend is not None else "",
    )

    try:
        asyncio.run(_amain(cfg, log, db_opts))
    except KeyboardInterrupt:  # 信号处理兜底：极端时序下直接吞掉干净退出
        pass
    log.info("rl-limiter 服务已停止")


if __name__ == "__main__":
    main()
