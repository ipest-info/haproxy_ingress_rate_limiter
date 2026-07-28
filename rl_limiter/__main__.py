# rl_limiter.__main__ —— rl-limiter 服务入口（单 HAProxy 模型）。
#
# 一个实例管**一台**与它同机的 HAProxy，做两件事：
#
#   1. **监控**：每秒经本机 unix stats socket 采样各受管 frontend 的
#      bytes_out，产出带宽视图（Web 控制台实时展示）并做持续超限告警；
#   2. **下发**：把配置（监听端口、限额、后端服务器）渲染进本机
#      haproxy.cfg 的受管区块并 reload，让 Web 界面上的改动立刻生效
#      （由 RL_APPLY_HAPROXY_CFG 开启，见 enforcer 模块）。
#
# 与配置来源断联时按最后一次加载的配置继续运行（fail-static）。
#
# RL_NODE_NAME 指定本实例对应配置库里的哪个 HAProxy 实例——多台机器可以
# 共用一个配置库，各自只读写属于自己的行。

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import signal
import sys

from . import config as configmod
from . import dbconfig
from . import enforcer as enforcermod
from . import haproxy, model
from . import netdev
from . import tcshaper as tcmod
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

# 本实例对应配置库 haproxy_instances 里的哪一行。多台机器共用一个配置库
# 时靠它区分；数据库模式下必设，本地 YAML 模式忽略（YAML 自带 haproxy 段）。
ENV_NODE_NAME = "RL_NODE_NAME"
DEFAULT_INSTANCE = "haproxy"
# Web 控制台监听地址。默认只绑回环：控制台**没有鉴权**且带写接口（改
# 监听端口、改限额、改后端服务器、删 frontend），默认对外可达是不可
# 接受的。要让同网段访问，由运维显式设成内网地址并配合防火墙/安全组
# 限制来源。
ENV_CONSOLE_BIND = "RL_CONSOLE_BIND"
DEFAULT_CONSOLE_BIND = "127.0.0.1"

# 配置自动下发（同机部署才可能做到的事）：设为本机 haproxy.cfg 路径即
# 启用——配置一改，本机 rl-limiter 立刻把监听端口/限额/后端服务器渲染进
# cfg 的受管区块并 reload。不设则完全不写盘、不 reload，退化为只读监控。
#
# 这两项**只能来自本机环境变量，绝不从配置库读**：若 reload 命令可由库
# 指定，拿到库写权限就等于在每台 HAProxy 上远程执行任意命令。
ENV_APPLY_CFG = "RL_APPLY_HAPROXY_CFG"
ENV_APPLY_RELOAD_CMD = "RL_APPLY_RELOAD_CMD"
DEFAULT_RELOAD_CMD = "systemctl reload haproxy"
# reconcile 兜底周期（秒）：变更是事件驱动的，这个只用来纠正手改 cfg
# 与重试失败的应用。
ENV_APPLY_PERIOD_S = "RL_APPLY_PERIOD_S"
DEFAULT_APPLY_PERIOD_S = 30.0

# 限速网卡。限速由内核 tc 执行（见 tcshaper 模块），作用在这张网卡的
# **出方向**上，按源端口把流量分到各 frontend 自己的 HTB 类里。
# 不设则自动取默认路由的出口网卡；多网卡机器必须显式指定。
# 设为 "-" 表示**关闭限速**（只保留监控与配置下发），此时不做任何 tc 操作。
#
# 与 RL_APPLY_RELOAD_CMD 同理，这一项只能来自本机环境变量、绝不从配置库
# 读——它决定了以 root 权限操作哪张网卡。
ENV_TC_IFACE = "RL_TC_IFACE"

# 实例监控视图里**入向**数据包统计要采样的网卡（/proc/net/dev）。
# 出向的包数/丢包数已由 tc 按 frontend 精确统计（见 tcshaper），不需要
# 网卡口径；入向 tc 的出方向队列看不到，只能从这里取。
# 不设则跟随 RL_TC_IFACE；设为 "-" 表示禁用入向包统计。
ENV_NIC = "RL_NIC"


def _summarize_frontends(frontends: list[model.FrontendConfig]) -> str:
    """把受管 frontend 清单压缩成单个日志字段，格式：
    "fe_main@:8080,quota_bps=40000000,servers=2;..."。
    quota_bps 为配置口径的 bits/s。"""
    return ";".join(
        f"{f.name}@{f.bind_spec},quota_bps={f.quota_bits_per_sec},"
        f"mode={f.mode},servers={len(f.servers)}"
        for f in frontends
    )


async def _amain(cfg, log: logging.Logger,
                 db_opts: dbconfig.MySQLOptions | None = None,
                 console_port: int = 0,
                 logbuf: "webconsole.LogBuffer | None" = None,
                 instance: str = DEFAULT_INSTANCE,
                 console_bind: str = DEFAULT_CONSOLE_BIND,
                 enforcer: "enforcermod.HAProxyEnforcer | None" = None,
                 apply_period_s: float = DEFAULT_APPLY_PERIOD_S,
                 shaper: "tcmod.TcShaper | None" = None,
                 nic: str = "") -> None:
    """事件循环内的主体：组装组件、引导配置、并发运行监控循环与配置源。

    db_opts 非 None 表示配置来自 MySQL（数据库配置模式，生产权威）：
    额外运行一个数据库轮询任务，把配置变化经配置队列热应用到监控循环；
    db_opts 为 None 时按引导时的本地 YAML 静态运行（standalone）。

    console_port 非 0 时启动内置 Web 控制台（实时观测，见 webconsole
    模块）：sampler 每拍向 StatusHub 发布一帧快照，配置热更经中继队列
    同步给控制台的配置视图。
    """
    # --- 与本机 HAProxy 的 runtime API 客户端（只读采样）。单 HAProxy
    # 模型下只有一个，同机形态走本机 unix socket。
    client = haproxy.RuntimeClient.from_node(cfg.haproxy, log)

    # tc 队列统计 → 监控。tc 按 classid（= 监听端口）索引，这里换成
    # frontend 名再交给采集器——采集器只认名字，不必知道端口这回事。
    tc_stats_fn = None
    if shaper is not None:
        async def tc_stats_fn():                     # noqa: F811
            by_port = await shaper.class_stats()
            return {f.name: by_port[f.bind_port]
                    for f in ctl.frontends() if f.bind_port in by_port}

    col = Collector(client, log, nic=nic, tc_stats=tc_stats_fn)

    # --- Web 控制台（可选）：StatusHub 是监控数据的发布枢纽。
    # version_fn 是延迟求值闭包（ctl 在下方才赋值，闭包只会在运行期被
    # 调用，届时 ctl 已完成构造）。
    ctl: MonitorLoop
    hub: webconsole.StatusHub | None = None
    if console_port:
        hub = webconsole.StatusHub(
            SERVICE_VERSION,
            version_fn=lambda: ctl.version,
            haproxy=cfg.haproxy,
            degraded_fn=lambda: col.degraded)

    sampler = hub.record if hub is not None else None
    # 配置下发启用时，监控循环每应用一份配置就 set 这个事件，enforcer
    # 任务据此立刻把新配置写进本机 haproxy.cfg（见 enforcer 模块头）。
    # 配置一变就叫醒"下发"类任务（写 cfg 的 enforcer、下发限速的 tcshaper）。
    # **各持一个 Event**：任务醒来后会 clear 自己的事件，共用会让其中一个
    # 漏掉变更（见 MonitorLoop.__init__ 的说明）。
    cfg_applied = asyncio.Event() if enforcer is not None else None
    tc_applied = asyncio.Event() if shaper is not None else None
    config_applied = [e for e in (cfg_applied, tc_applied) if e is not None]
    ctl = MonitorLoop(col, sampler=sampler, log=log,
                      config_applied=config_applied)

    # --- 启动引导（seed）：用加载到的配置（数据库或本地 YAML）构造首份
    # 运行期配置直接喂给监控循环。数据库配置模式下引导配置的版本号取
    # 内容校验和，并把它作为轮询任务的变更检测基准——首轮轮询读到同样
    # 内容时不会再触发一次重复应用。
    seed_version = (
        dbconfig.config_checksum(cfg.frontends) if db_opts is not None else 0
    )
    seed_cfg = cfg.to_controller_config(seed_version)
    ctl.seed(seed_cfg)
    if hub is not None:
        hub.update_config(seed_cfg)
    log.info("已用%s配置完成引导（监控单位=frontend） version=%s "
             "frontends=%d detail=%s",
             "数据库" if db_opts is not None else "本地静态",
             seed_version, len(cfg.frontends), _summarize_frontends(cfg.frontends))

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
            dbconfig.watch(db_opts, source_queue, cfg, log, instance),
            name="db-config-watch"))
    if hub is not None:
        tasks.append(asyncio.create_task(
            webconsole.run_console(console_port, hub, logbuf, db_opts, log,
                                   bind=console_bind, instance=instance),
            name="web-console"))
    if enforcer is not None and cfg_applied is not None:
        # 目标配置取自监控循环当前生效的那一份。每次 reconcile 现取，
        # 因此配置热更后拿到的必然是新值。
        def _desired() -> list[model.FrontendConfig]:
            return ctl.frontends()

        tasks.append(asyncio.create_task(
            enforcermod.run_enforcer(
                enforcer, _desired, cfg_applied, log,
                on_result=(hub.record_enforce if hub is not None else None),
                period_s=apply_period_s),
            name="cfg-enforcer"))

    if shaper is not None and tc_applied is not None:
        # 限速任务与 cfg 下发任务各自独立 reconcile：一个失败不牵连另一个。
        # 两者共享同一个"配置已更新"事件——asyncio.Event 是电平触发，
        # 一次 set 能同时唤醒多个等待者。
        tasks.append(asyncio.create_task(
            tcmod.run_shaper(shaper, lambda: ctl.frontends(), tc_applied,
                             log, period_s=apply_period_s),
            name="tc-shaper"))

    log.info("rl-limiter 服务已启动，监控循环开始运行 instance=%s "
             "endpoint=%s apply=%s frontends=%d version=%s",
             cfg.haproxy.name, cfg.haproxy.endpoint(),
             enforcer.cfg_path if enforcer is not None else "off",
             len(cfg.frontends), SERVICE_VERSION)

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
        description="HAProxy 入口带宽限速与监控：管理本机 HAProxy 的监听"
                    "端口、限额与后端服务器（写入 cfg 受管区块并 reload），"
                    "并每秒采样各 frontend 的下行带宽做持续超限告警",
        epilog="一个实例管一台同机的 HAProxy。RL_NODE_NAME 指定本实例"
               "对应配置库里的哪个 HAProxy 实例（默认 haproxy）。"
               "配置来源二选一：设置 RL_MYSQL_HOST（及 RL_MYSQL_PORT/USER/"
               "PASSWORD/DB/POLL_S）后从 MySQL 数据库读取配置并轮询热更新；"
               "未设置时回落到 -c 指定的本地 YAML 文件。"
               "配置下发：设置 RL_APPLY_HAPROXY_CFG=<本机 haproxy.cfg 路径> "
               "即启用（改配置后自动写入受管区块并 reload）。"
               "Web 控制台：设置 RL_CONSOLE_PORT 启用，RL_CONSOLE_BIND 指定"
               "监听地址（默认 127.0.0.1——控制台无鉴权且带写接口）。"
               "实例监控视图的数据包统计取自 /proc/net/dev，RL_NIC 指定网卡"
               "（默认自动选默认路由的出口网卡，设 - 则禁用）。")
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

    # 本实例对应配置库里的哪个 HAProxy 实例（数据库模式下用它取行）。
    instance = (os.environ.get(ENV_NODE_NAME) or "").strip() or DEFAULT_INSTANCE

    # 配置自动下发：设了本机 haproxy.cfg 路径即启用。
    apply_cfg = (os.environ.get(ENV_APPLY_CFG) or "").strip()
    enforcer = None
    apply_period_s = DEFAULT_APPLY_PERIOD_S
    if apply_cfg:
        if not os.path.isfile(apply_cfg):
            print(f"rl-limiter: {ENV_APPLY_CFG} 指向的文件不存在: {apply_cfg}",
                  file=sys.stderr)
            raise SystemExit(1)
        raw_period = (os.environ.get(ENV_APPLY_PERIOD_S) or "").strip()
        if raw_period:
            try:
                apply_period_s = float(raw_period)
            except ValueError:
                print(f"rl-limiter: {ENV_APPLY_PERIOD_S} 必须是数字，"
                      f"当前值 {raw_period!r}", file=sys.stderr)
                raise SystemExit(1)
            if apply_period_s <= 0:
                print(f"rl-limiter: {ENV_APPLY_PERIOD_S} 必须为正数，"
                      f"当前值 {apply_period_s}", file=sys.stderr)
                raise SystemExit(1)
        enforcer = enforcermod.HAProxyEnforcer(
            apply_cfg,
            (os.environ.get(ENV_APPLY_RELOAD_CMD) or "").strip()
            or DEFAULT_RELOAD_CMD,
            log)

    # 限速：内核 tc（见 tcshaper 模块）。默认启用——限速是本服务的核心
    # 职责，"静悄悄地没在限"是最不该出现的状态。显式设 RL_TC_IFACE=- 才
    # 关闭；网卡探测不出来直接启动失败，而不是装作在限速。
    shaper = None
    tc_iface = ""
    raw_iface = (os.environ.get(ENV_TC_IFACE) or "").strip()
    if raw_iface == "-":
        print("rl-limiter: 已显式关闭 tc 限速（RL_TC_IFACE=-），本实例只做"
              "监控与配置下发", file=sys.stderr)
    else:
        iface = tcmod.resolve_iface(raw_iface, log)
        if not iface:
            print("rl-limiter: 无法确定要在哪张网卡上限速——/proc/net/route 里"
                  "没有默认路由。\n"
                  f"  请用 {ENV_TC_IFACE}=<网卡名> 显式指定（多网卡机器本来"
                  "就该显式指定）；\n"
                  f"  确实不需要限速时设 {ENV_TC_IFACE}=- 明确关闭。",
                  file=sys.stderr)
            raise SystemExit(1)
        tc_iface = iface
        shaper = tcmod.TcShaper(iface, log)

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
                dbconfig.load_service_config(db_opts, log, instance))
        else:
            cfg = configmod.load(args.config)
    except KeyboardInterrupt:
        # 等待数据库就绪的重试窗口（最长两分钟）里按 Ctrl-C 是常规操作，
        # 必须干净退出；KeyboardInterrupt 是 BaseException，不加这条会
        # 绕过下面的 except Exception 直接冲出 main 打印原始 traceback。
        print("rl-limiter: 启动在配置加载阶段被中断（Ctrl-C），已退出",
              file=sys.stderr)
        raise SystemExit(130)
    except FileNotFoundError as e:
        # 配置文件不存在时，光报一个 errno 帮不上忙——真正的问题往往是
        # "本该走数据库模式却没设 RL_MYSQL_HOST"，于是静默回落到了本地
        # YAML 这条路上（容器里尤其常见）。把两条出路都点明。
        print(
            f"rl-limiter: 找不到配置文件 {args.config}（{e.strerror}）。\n"
            f"  配置来源二选一：\n"
            f"    - 数据库模式：设置 RL_MYSQL_HOST（及 RL_MYSQL_USER/"
            f"PASSWORD/DB 等），此时 -c 会被忽略；\n"
            f"    - 本地 YAML：用 -c 指向一份实际存在的配置文件"
            f"（示例见 deploy/config/limiter.example.yaml）。\n"
            f"  当前 RL_MYSQL_HOST 未设置，因此走的是本地 YAML 这条路。",
            file=sys.stderr)
        raise SystemExit(1)
    except Exception as e:
        print(f"rl-limiter: {e}", file=sys.stderr)
        raise SystemExit(1)

    level = getattr(logging, str(cfg.log_level).upper(), logging.INFO)
    logging.getLogger().setLevel(level)

    config_source = db_opts.describe() if db_opts is not None else f"文件 {args.config}"

    # 实例视图的**入向**包统计要采哪张网卡（"-" = 显式禁用）。出向的包/
    # 丢包由 tc 按 frontend 统计，不走这里。默认跟随限速网卡——两者本来
    # 就该是同一张（客户端流量进出的那张），分开设只会给人配错的机会。
    raw_nic = (os.environ.get(ENV_NIC) or "").strip()
    if raw_nic == "-":
        nic = ""
    elif raw_nic:
        nic = netdev.resolve_iface(raw_nic, log)
    else:
        nic = tc_iface or netdev.resolve_iface("", log)

    # 启动即输出完整配置摘要：现场排障时第一条要看的日志，可直接核对
    # 受控节点清单、限额基准与配置来源。
    log.info(
        "服务配置加载完成，以下为完整配置摘要（排障第一条要看的日志） "
        "config_source=%s instance=%s endpoint=%s log_level=%s "
        "tick_interval_s=%s frontends=%d detail=%s",
        config_source,
        cfg.haproxy.name, cfg.haproxy.endpoint(),
        cfg.log_level,
        getattr(cfg, "tick_interval_s", 1.0),
        len(cfg.frontends), _summarize_frontends(cfg.frontends),
    )

    try:
        asyncio.run(_amain(cfg, log, db_opts,
                           console_port=console_port, logbuf=logbuf,
                           instance=instance, console_bind=console_bind,
                           enforcer=enforcer, apply_period_s=apply_period_s,
                           shaper=shaper, nic=nic))
    except KeyboardInterrupt:  # 信号处理兜底：极端时序下直接吞掉干净退出
        pass
    log.info("rl-limiter 服务已停止")


if __name__ == "__main__":
    main()
