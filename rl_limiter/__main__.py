# rl_limiter.__main__ —— rl-limiter 服务入口（单 HAProxy 模型）。
#
# 一个实例管**一台**与它同机的 HAProxy。**haproxy.cfg 是负载均衡配置的
# 唯一权威**：监听端口、模式、后端服务器都由运维直接写在 cfg 里，
# rl-limiter 只**读** cfg（cfgparse）解析出受管 frontend 清单，与 YAML
# 里登记的限额（quotas）合并后做两件事：
#
#   1. **限速**：把限额下发到本机网卡的内核 tc（HTB，按源端口分类）；
#   2. **监控**：每秒经本机 unix stats socket 采样各 frontend 的
#      bytes_out，产出带宽视图（Web 控制台实时展示）并做持续超限告警。
#
# 配置来源只有本地 YAML（-c）+ haproxy.cfg 两个文件；运行期轮询两者，
# 内容变化即热生效（cfg 改了 reload HAProxy 后，rl-limiter 的限速与
# 监控清单自动跟上）。文件读不到时按最后一次加载的配置继续运行
# （fail-static）。

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import signal
import sys

from . import cfgparse
from . import config as configmod
from . import haproxy, metricslog, model
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

# Web 控制台监听地址。默认只绑回环；要让同网段访问，由运维显式设成
# 内网地址并配合防火墙/安全组限制来源。
ENV_CONSOLE_BIND = "RL_CONSOLE_BIND"
DEFAULT_CONSOLE_BIND = "127.0.0.1"

# tc reconcile 兜底周期（秒）：限额变更是事件驱动的即时下发，这个只用来
# 纠正"有人手动改了 tc 规则"与重试失败的下发。
DEFAULT_APPLY_PERIOD_S = 30.0

# 限速网卡。限速由内核 tc 执行（见 tcshaper 模块），作用在这张网卡的
# **出方向**上，按源端口把流量分到各 frontend 自己的 HTB 类里。
# 不设则自动取默认路由的出口网卡；多网卡机器必须显式指定。
# 设为 "-" 表示**关闭限速**（只保留监控），此时不做任何 tc 操作。
#
# 这一项只能来自本机环境变量、绝不从配置文件读——它决定了以 root
# 权限（CAP_NET_ADMIN）操作哪张网卡。
ENV_TC_IFACE = "RL_TC_IFACE"

# 实例监控视图里**入向**数据包统计要采样的网卡（/proc/net/dev）。
# 出向的包数/丢包数已由 tc 按 frontend 精确统计（见 tcshaper），不需要
# 网卡口径；入向 tc 的出方向队列看不到，只能从这里取。
# 不设则跟随 RL_TC_IFACE；设为 "-" 表示禁用入向包统计。
ENV_NIC = "RL_NIC"

# 监控数据落盘：设为本地文件路径即启用（JSONL，分钟粒度，按天轮转，
# 见 metricslog 模块）。不设 = 不落盘，只有内存实时曲线。
ENV_METRICS_LOG = "RL_METRICS_LOG"
# 落盘文件的保留天数（按天轮转后删除更旧的）。
ENV_METRICS_LOG_DAYS = "RL_METRICS_LOG_DAYS"


def _summarize_frontends(frontends: list[model.FrontendConfig]) -> str:
    """把受管 frontend 清单压缩成单个日志字段，格式：
    "fe_main@:8080,quota=40Mbps;fe_tcp@:8081,quota=不限（仅监控）"。
    限额按配置口径的 Mbps 输出，与 YAML/控制台里填的是同一个数。"""
    return ";".join(
        f"{f.name}@{f.bind_spec},"
        f"quota={f.quota_mbps:g}Mbps" if f.limited else
        f"{f.name}@{f.bind_spec},quota=不限（仅监控）"
        for f in frontends
    )


async def _amain(cfg, log: logging.Logger,
                 yaml_path: str,
                 boot_frontends: list[model.FrontendConfig],
                 console_port: int = 0,
                 logbuf: "webconsole.LogBuffer | None" = None,
                 console_bind: str = DEFAULT_CONSOLE_BIND,
                 apply_period_s: float = DEFAULT_APPLY_PERIOD_S,
                 shaper: "tcmod.TcShaper | None" = None,
                 nic: str = "",
                 mlog: "metricslog.MetricsLog | None" = None) -> None:
    """事件循环内的主体：组装组件、引导配置、并发运行监控循环与配置源。

    boot_frontends 是启动时从 haproxy.cfg 解析并合并限额后的受管清单；
    运行期由 cfgparse.watch 轮询 cfg 与 YAML，两者内容变化即经配置队列
    热应用到监控循环（tc 限速与告警基准随之更新）。

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

    # sampler：监控循环每拍喂给控制台（内存实时曲线）与监控数据落盘器
    # （分钟粒度 JSONL）——同一拍、同一份数据，两个出口。
    sinks = [x.record for x in (hub, mlog) if x is not None]
    if not sinks:
        sampler = None
    elif len(sinks) == 1:
        sampler = sinks[0]
    else:
        def sampler(now, usages, instance=None):
            for sink in sinks:
                sink(now, usages, instance)
    # 配置一变就叫醒"下发"类任务（下发限速的 tcshaper）：监控循环每应用
    # 一份配置就 set 这个事件。
    tc_applied = asyncio.Event() if shaper is not None else None
    config_applied = [e for e in (tc_applied,) if e is not None]
    ctl = MonitorLoop(col, sampler=sampler, log=log,
                      config_applied=config_applied)
    if mlog is not None:
        # 落盘行里的限额要取"采样当时"的值：延迟经 ctl 取当前受管清单。
        mlog.set_quotas_fn(metricslog.quotas_from_frontends(ctl.frontends))

    # --- 启动引导（seed）：用启动时解析出的受管清单构造首份运行期配置
    # 直接喂给监控循环。版本号取内容校验和，watch 以同一算法做变更检测
    # 基准——首轮轮询读到同样内容时不会再触发一次重复应用。
    seed_version = cfgparse.checksum(boot_frontends)
    seed_cfg = model.ControllerConfig(version=seed_version,
                                      frontends=list(boot_frontends))
    ctl.seed(seed_cfg)
    if hub is not None:
        hub.update_config(seed_cfg)
    log.info("已用 haproxy.cfg + YAML 限额完成引导（监控单位=frontend） "
             "version=%s frontends=%d detail=%s",
             seed_version, len(boot_frontends),
             _summarize_frontends(boot_frontends))

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

    # --- 并发运行：监控循环 + 配置源（cfg/YAML 轮询）+ 可选的 Web 控制台。
    # config_queue 是监控循环的配置入口（配置优先于 tick）：由 cfgparse
    # 的轮询任务投递内容变化。
    tick_interval_s = getattr(cfg, "tick_interval_s", 1.0) or 1.0
    tasks: list[asyncio.Task] = []

    source_queue: asyncio.Queue = asyncio.Queue()

    # 控制台需要跟随配置热更（限额参考线、清单变化）：在源队列与循环
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
    tasks.append(asyncio.create_task(
        cfgparse.watch(cfg.haproxy.cfg_path, yaml_path, source_queue,
                       boot_frontends, log),
        name="cfg-watch"))
    if hub is not None:
        tasks.append(asyncio.create_task(
            webconsole.run_console(console_port, hub, logbuf, log,
                                   bind=console_bind),
            name="web-console"))

    if shaper is not None and tc_applied is not None:
        # 限速下发：配置热更即时触发 + 周期 reconcile 兜底（纠正手改的
        # tc 规则、重试失败的下发）。只对登记了限额的 frontend 建类。
        tasks.append(asyncio.create_task(
            tcmod.run_shaper(
                shaper,
                lambda: [f for f in ctl.frontends() if f.limited],
                tc_applied, log, period_s=apply_period_s),
            name="tc-shaper"))

    log.info("rl-limiter 服务已启动，监控循环开始运行 instance=%s "
             "endpoint=%s cfg=%s frontends=%d version=%s",
             cfg.haproxy.name, cfg.haproxy.endpoint(),
             cfg.haproxy.cfg_path,
             len(boot_frontends), SERVICE_VERSION)

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
    if mlog is not None:
        # 把最后一个未满的分钟也写出去，然后关闭文件句柄。
        mlog.close()


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="rl-limiter",
        description="HAProxy 入口带宽限速与监控：以本机 haproxy.cfg 为负载"
                    "均衡配置的唯一权威，解析出受管监听端口清单，把 YAML 里"
                    "登记的限额下发到内核 tc（HTB，按源端口分类），并每秒"
                    "采样各 frontend 的下行带宽做持续超限告警",
        epilog="一个实例管一台同机的 HAProxy。配置来源是两个本地文件："
               "-c 指定的 YAML（接线 + 限额 quotas）与其中 haproxy.cfg_path "
               "指向的 haproxy.cfg（监听端口/模式，由运维直接编辑，"
               "rl-limiter 只读不写）；运行期轮询两者，内容变化即热生效。"
               "限速网卡用 RL_TC_IFACE 指定（默认取默认路由的出口网卡，"
               "设 - 则关闭限速、只保留监控）。"
               "Web 控制台：设置 RL_CONSOLE_PORT 启用，RL_CONSOLE_BIND 指定"
               "监听地址（默认 127.0.0.1——控制台无鉴权）；控制台同端口的 "
               "/metrics 提供 Prometheus 抓取。"
               "监控数据落盘：设 RL_METRICS_LOG=<文件路径> 启用（分钟粒度 "
               "JSONL，按天轮转，RL_METRICS_LOG_DAYS 定保留天数，默认 90）。"
               "实例监控视图的入向数据包统计取自 /proc/net/dev，RL_NIC 指定"
               "网卡（默认跟随限速网卡，设 - 则禁用）。")
    parser.add_argument(
        "-c", "--config", default="/etc/rl-limiter/config.yaml",
        help="YAML 配置文件路径（默认 %(default)s）")
    parser.add_argument(
        "--version", action="store_true", help="打印版本号后退出")
    args = parser.parse_args()

    if args.version:
        print("rl-limiter", SERVICE_VERSION)
        return

    # 日志先以 INFO 起步，配置加载成功后再把根 logger 调到配置指定的级别。
    # 格式固定为"时间 级别 消息"三段；消息本体统一为中文描述 + 英文
    # snake_case 的 key=value 键值对，便于 grep 与日志采集系统按字段解析。
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

    # 限速：内核 tc（见 tcshaper 模块）。默认启用——限速是本服务的核心
    # 职责，"静悄悄地没在限"是最不该出现的状态。显式设 RL_TC_IFACE=- 才
    # 关闭；网卡探测不出来直接启动失败，而不是装作在限速。
    shaper = None
    tc_iface = ""
    raw_iface = (os.environ.get(ENV_TC_IFACE) or "").strip()
    if raw_iface == "-":
        print("rl-limiter: 已显式关闭 tc 限速（RL_TC_IFACE=-），本实例只做"
              "监控", file=sys.stderr)
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

    # --- 配置加载：本地 YAML（接线 + 限额）。
    try:
        cfg = configmod.load(args.config)
    except FileNotFoundError as e:
        print(
            f"rl-limiter: 找不到配置文件 {args.config}（{e.strerror}）。\n"
            f"  用 -c 指向一份实际存在的 YAML 配置"
            f"（示例见 deploy/config/limiter.example.yaml）。",
            file=sys.stderr)
        raise SystemExit(1)
    except Exception as e:
        print(f"rl-limiter: {e}", file=sys.stderr)
        raise SystemExit(1)

    level = getattr(logging, str(cfg.log_level).upper(), logging.INFO)
    logging.getLogger().setLevel(level)

    # --- 引导解析：haproxy.cfg 是负载均衡配置的唯一权威，启动时必须能
    # 读到并解析出受管清单；读不到就启动失败，而不是带着空清单装作在跑。
    # （运行期 cfg 短暂不可读走 fail-static，那是另一回事——见 cfgparse.watch。）
    try:
        boot_frontends = cfgparse.load_frontends(
            cfg.haproxy.cfg_path, cfg.quotas, log)
    except OSError as e:
        print(
            f"rl-limiter: 读不到 haproxy.cfg：{cfg.haproxy.cfg_path}（{e}）。\n"
            f"  haproxy.cfg 是负载均衡配置的唯一权威，启动时必须可读；\n"
            f"  请核对 YAML 里 haproxy.cfg_path 的路径与文件权限。",
            file=sys.stderr)
        raise SystemExit(1)
    if not boot_frontends:
        log.warning("haproxy.cfg 里没有解析到任何带监听端口的 frontend/"
                    "listen——服务照常启动，之后 cfg 内容变化会被轮询接上 "
                    "cfg_path=%s", cfg.haproxy.cfg_path)

    # 监控数据落盘（可选）：设 RL_METRICS_LOG=<文件路径> 即启用。
    mlog = None
    raw_mlog = (os.environ.get(ENV_METRICS_LOG) or "").strip()
    if raw_mlog:
        raw_days = (os.environ.get(ENV_METRICS_LOG_DAYS) or "").strip()
        try:
            mlog_days = int(raw_days) if raw_days else metricslog.DEFAULT_RETENTION_DAYS
        except ValueError:
            print(f"rl-limiter: {ENV_METRICS_LOG_DAYS} 必须是整数，"
                  f"当前值 {raw_days!r}", file=sys.stderr)
            raise SystemExit(1)
        if mlog_days < 1:
            print(f"rl-limiter: {ENV_METRICS_LOG_DAYS} 必须 ≥ 1，"
                  f"当前值 {mlog_days}", file=sys.stderr)
            raise SystemExit(1)
        try:
            mlog = metricslog.MetricsLog(raw_mlog, log, retention_days=mlog_days)
        except OSError as e:
            print(f"rl-limiter: 打不开监控数据落盘文件 {raw_mlog}（{e}）。\n"
                  f"  请确认目录存在且服务用户可写（systemd 部署时 unit 里的 "
                  f"LogsDirectory=rl-limiter 会自动创建 /var/log/rl-limiter）。",
                  file=sys.stderr)
            raise SystemExit(1)
        log.info("监控数据落盘已启用（分钟粒度 JSONL，按天轮转） path=%s "
                 "retention_days=%d", raw_mlog, mlog_days)

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
    # 受管清单、限额基准与两个配置文件的路径。
    log.info(
        "服务配置加载完成，以下为完整配置摘要（排障第一条要看的日志） "
        "yaml=%s cfg=%s instance=%s endpoint=%s log_level=%s "
        "tick_interval_s=%s frontends=%d detail=%s",
        args.config, cfg.haproxy.cfg_path,
        cfg.haproxy.name, cfg.haproxy.endpoint(),
        cfg.log_level,
        getattr(cfg, "tick_interval_s", 1.0),
        len(boot_frontends), _summarize_frontends(boot_frontends),
    )

    try:
        asyncio.run(_amain(cfg, log,
                           yaml_path=args.config,
                           boot_frontends=boot_frontends,
                           console_port=console_port, logbuf=logbuf,
                           console_bind=console_bind,
                           shaper=shaper, nic=nic, mlog=mlog))
    except KeyboardInterrupt:  # 信号处理兜底：极端时序下直接吞掉干净退出
        pass
    log.info("rl-limiter 服务已停止")


if __name__ == "__main__":
    main()
