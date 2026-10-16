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
from . import haproxy, ingressgov, metricslog, model
from . import netdev
from . import tcifb as tcifbmod
from . import tcingress as tcinmod
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

# 请求方向（入向）限速的执行网卡 = **客户端侧**那张（双网卡形态）。
# 配置了 request_limits 就必须显式指定——入向 police 放错网卡会误伤
# 后端流量，所以这一项**绝不自动探测**；设为 "-" 表示明确不做入向限速
# （request_limits 仍会被解析但只告警不执行）。
ENV_TC_IN_IFACE = "RL_TC_IN_IFACE"
# 入向限速的执行机制：
#   ifb（默认）  重定向到 IFB 虚拟网卡 + HTB **排队整形**——平稳慢放，
#                与出向同款机制（内核需 ifb/act_mirred/sch_htb，均为
#                发行版通用内核标配）；
#   police       入向 police **超额丢包**——无队列，靠 TCP 重传兜底
#                （内核需 act_police；曲线毛糙，作为老内核的备选）。
ENV_TC_IN_MODE = "RL_TC_IN_MODE"
# ifb 模式的虚拟网卡名（默认 ifbrl0——刻意避开教程惯用的 ifb0，防止与
# 运维手工建的设备互相踩）。
ENV_TC_IFB = "RL_TC_IFB"

# 监控数据落盘：设为本地文件路径即启用（JSONL，分钟粒度，按天轮转，
# 见 metricslog 模块）。不设 = 不落盘，只有内存实时曲线。
ENV_METRICS_LOG = "RL_METRICS_LOG"
# 落盘文件的保留天数（按天轮转后删除更旧的）。
ENV_METRICS_LOG_DAYS = "RL_METRICS_LOG_DAYS"

# 写 API 的鉴权令牌（控制台/API 修改限额用，见 webconsole 模块）。
# 不设 = 写接口整体 403（只读部分照常）。改限速影响生产流量，绝不允许
# "没配令牌就人人可改"。
ENV_API_TOKEN = "RL_API_TOKEN"


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
                 ingress: "tcinmod.TcIngress | None" = None,
                 nic: str = "",
                 mlog: "metricslog.MetricsLog | None" = None,
                 api_token: str = "") -> None:
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

    # 入向动态限速控制器（闭环）：按实测出向带宽对照目标平缓调整入向
    # 上限。两个环：单端口（quotas → 各端口入向上限）与实例级
    # （instance_quota_mbps = 实例出向总带宽目标 → IFB 上的入向总闸，
    # 仅 ifb 执行模式支持——police 没有层级）。挂 sampler 链吃 mean10。
    in_mode_ifb = getattr(ingress, "mode", "") == "ifb"
    gov: "ingressgov.IngressGovernor | None" = None
    if ingress is not None and cfg.dynamic_ingress.enabled:
        gov = ingressgov.IngressGovernor(cfg.dynamic_ingress, log,
                                         instance_capable=in_mode_ifb)
        log.info("入向动态限速已启用：按出向带宽（目标=quotas 登记值"
                 "%s）平缓调节入向上限 interval=%gs 死区=[%g, %g] "
                 "step_up=%g step_down_max=%g floor=%g",
                 "；实例总限速=实例出向总目标（入向总闸）"
                 if in_mode_ifb else "",
                 cfg.dynamic_ingress.interval_s,
                 cfg.dynamic_ingress.low_ratio,
                 cfg.dynamic_ingress.high_ratio,
                 cfg.dynamic_ingress.step_up,
                 cfg.dynamic_ingress.step_down_max,
                 cfg.dynamic_ingress.floor_ratio)
    elif cfg.dynamic_ingress.enabled:
        log.warning("dynamic_ingress.enabled=true 但入向限速未接线"
                    "（RL_TC_IN_IFACE 未设或为 -），动态调节不会运行")

    # 实例总限速的执行者：出向 HTB（基线形态），或 ifb 闭环的入向总闸
    # （本分支形态）。两者都没有时页面置灰、写接口 409（见 webconsole）。
    inst_executable = shaper is not None or (gov is not None and in_mode_ifb)
    # 调节视图的数据源：闭环控制器把全部调节量（目标/动态总闸/各端口
    # 动态上限/实测流入）摊成快照里的 regulation 字段，页面据此渲染
    # "调节视图"卡。没有控制器（基线形态）不注入，卡保持隐藏。
    # ctl 在下方才赋值——与 version_fn 同一套延迟求值闭包的约定。
    regulation_fn = None
    if gov is not None:
        def regulation_fn():
            return gov.regulation_view(
                list(ctl.frontends()), ctl.request_limits(),
                ctl.instance_quota_mbps)
    hub: webconsole.StatusHub | None = None
    if console_port:
        hub = webconsole.StatusHub(
            SERVICE_VERSION,
            version_fn=lambda: ctl.version,
            haproxy=cfg.haproxy,
            degraded_fn=lambda: col.degraded,
            instance_quota_executable=inst_executable,
            regulation_fn=regulation_fn)

    # sampler：监控循环每拍喂给控制台（内存实时曲线）与监控数据落盘器
    # （分钟粒度 JSONL）——同一拍、同一份数据，两个出口。
    sinks = [x.record for x in (hub, mlog) if x is not None]
    if gov is not None:
        sinks.append(gov.record)
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
    tcin_applied = asyncio.Event() if ingress is not None else None
    config_applied = [e for e in (tc_applied, tcin_applied) if e is not None]
    ctl = MonitorLoop(col, sampler=sampler, log=log,
                      config_applied=config_applied)
    if cfg.instance_quota_mbps and not inst_executable:
        log.warning(
            "YAML 里配置了实例总限速（%gMbps）但本机没有它的执行者"
            "（出向 tc 关闭，且入向闭环不可用/为 police 模式）——"
            "**该限速不会执行**。纯监控节点请用 RL_TC_IFACE 指定出向"
            "网卡；不需要时删掉 instance_quota_mbps 以免误导",
            cfg.instance_quota_mbps)
    elif cfg.instance_quota_mbps and shaper is None:
        log.info(
            "实例总限速（%gMbps）在本形态下作为**实例出向总带宽目标**"
            "由入向闭环执行（IFB 入向总闸动态调节），出向不做 tc 整形",
            cfg.instance_quota_mbps)
    if mlog is not None:
        # 落盘行里的限额要取"采样当时"的值：延迟经 ctl 取当前受管清单。
        mlog.set_quotas_fn(metricslog.quotas_from_frontends(ctl.frontends))

    # --- 启动引导（seed）：用启动时解析出的受管清单构造首份运行期配置
    # 直接喂给监控循环。版本号取内容校验和，watch 以同一算法做变更检测
    # 基准——首轮轮询读到同样内容时不会再触发一次重复应用。
    seed_version = cfgparse.checksum(boot_frontends,
                                     cfg.instance_quota_mbps,
                                     cfg.request_limits)
    seed_cfg = model.ControllerConfig(
        version=seed_version,
        frontends=list(boot_frontends),
        instance_quota_mbps=cfg.instance_quota_mbps,
        request_limits=dict(cfg.request_limits))
    ctl.seed(seed_cfg)
    if hub is not None:
        hub.update_config(seed_cfg)
    log.info("已用 haproxy.cfg + YAML 限额完成引导（监控单位=frontend） "
             "version=%s frontends=%d instance_quota=%gMbps detail=%s",
             seed_version, len(boot_frontends), cfg.instance_quota_mbps,
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

    # 写 API 改完 YAML 后 set 这个事件，叫醒 cfgparse.watch 立即重读——
    # 不用等下一个 5s 轮询周期，页面上的修改近乎即时生效。
    cfg_poke = asyncio.Event()

    tasks.append(asyncio.create_task(
        ctl.run(config_queue, tick_interval_s), name="monitor-loop"))
    tasks.append(asyncio.create_task(
        cfgparse.watch(cfg.haproxy.cfg_path, yaml_path, source_queue,
                       boot_frontends, log,
                       boot_instance_quota_mbps=cfg.instance_quota_mbps,
                       boot_request_limits=cfg.request_limits,
                       poke=cfg_poke),
        name="cfg-watch"))
    # tc 实况（控制台调试面板）：期望配置来自监控循环的当前清单，
    # 实况是 shaper 现场跑 tc show 的原始输出——页面上两边对照着看。
    # 双网卡形态下出向 HTB 与入向 police 各一个 section（页面按数组
    # 渲染，有几个画几个）。
    tc_debug_fn = None
    if shaper is not None or ingress is not None:
        async def tc_debug_fn():                      # noqa: F811
            sections = []
            if shaper is not None:
                desired = "; ".join(
                    f"{f.name}@:{f.bind_port}={f.quota_mbps:g}Mbps"
                    for f in ctl.frontends() if f.limited) or \
                    "(没有登记限额的 frontend)"
                inst = ctl.instance_quota_mbps
                if inst:
                    desired += f"; 实例总限速={inst:g}Mbps（层级模式）"
                section = await shaper.debug_state()
                section.update(key="egress",
                               title="出向 HTB（响应方向限速）",
                               desired=desired)
                sections.append(section)
            if ingress is not None:
                fes = ctl.frontends()
                ports = {f.name: f.bind_port for f in fes}
                targets = {f.name: f.quota_mbps for f in fes if f.limited}
                cur = gov.current() if gov is not None else {}
                parts = []
                for name, rl in sorted(ctl.request_limits().items()):
                    want = []
                    if rl.mbps:
                        if name in cur:
                            # 动态调节中：展示当前生效值与来龙去脉。
                            want.append(
                                f"{cur[name]:.1f}Mbps（动态，上限 "
                                f"{rl.mbps:g}、出向目标 "
                                f"{targets.get(name, 0):g}）")
                        elif gov is not None and targets.get(name):
                            want.append(f"{rl.mbps:g}Mbps（动态上限，"
                                        f"当前全开）")
                        else:
                            want.append(f"{rl.mbps:g}Mbps")
                    if rl.conn_rate:
                        want.append(f"{rl.conn_rate}conn/s")
                    parts.append(f"{name}@:{ports.get(name, '?')}="
                                 + "+".join(want or ["0"]))
                inst_t = ctl.instance_quota_mbps
                if gov is not None and in_mode_ifb and inst_t:
                    cur_i = gov.current_instance()
                    parts.append(
                        f"实例出向目标={inst_t:g}Mbps→入向总闸 "
                        + (f"{cur_i:.1f}Mbps（动态）" if cur_i is not None
                           else "全开"))
                section = await ingress.debug_state()
                section.update(key="ingress",
                               title=("入向整形 IFB+HTB（请求方向限速）"
                                      if getattr(ingress, "mode", "police")
                                      == "ifb"
                                      else "入向 police（请求方向限速）"),
                               desired="; ".join(parts)
                               or "(没有登记 request_limits)")
                sections.append(section)
            return {"sections": sections}

    if hub is not None:
        tasks.append(asyncio.create_task(
            webconsole.run_console(
                console_port, hub, logbuf, log, bind=console_bind,
                yaml_path=yaml_path, api_token=api_token,
                # 写限额前校验段名存在：以当前 cfg 解析出的清单为准。
                known_frontends_fn=lambda: {f.name for f in ctl.frontends()},
                poke=cfg_poke, tc_debug_fn=tc_debug_fn,
                # 实例总限速的执行者：出向 HTB 或 ifb 闭环的入向总闸，
                # 两者有其一写接口就放行（语义见 inst_executable）。
                egress_tc_enabled=inst_executable),
            name="web-console"))

    if shaper is not None and tc_applied is not None:
        # 限速下发：配置热更即时触发 + 周期 reconcile 兜底（纠正手改的
        # tc 规则、重试失败的下发）。平铺模式只对登记了限额的 frontend
        # 建类；设了实例总限速则**全部** frontend 都进总闸（未登记限额
        # 的段也要被总闸罩住，见 tcshaper.instance_layout）。
        def _tc_desired():
            inst_bits = int(ctl.instance_quota_mbps * 1_000_000)
            fes = list(ctl.frontends())
            if not inst_bits:
                fes = [f for f in fes if f.limited]
            return fes, inst_bits

        tasks.append(asyncio.create_task(
            tcmod.run_shaper(
                shaper, _tc_desired,
                tc_applied, log, period_s=apply_period_s),
            name="tc-shaper"))

    if ingress is not None and tcin_applied is not None:
        # 请求方向（入向）限速：事件驱动 + 周期兜底，与出向同一套节奏。
        # 挂了动态控制器时，下发的是控制器调节后的值（登记值只是上限）。
        def _tcin_desired():
            limits = ctl.request_limits()
            if gov is None:
                return list(ctl.frontends()), limits
            eff = gov.effective_limits(limits)
            if in_mode_ifb:
                # ifb 模式多一个入向总闸速率（实例级闭环的执行面；
                # 0 = 没设实例目标，平铺）。
                return (list(ctl.frontends()), eff,
                        gov.effective_instance_bits(
                            limits, ctl.instance_quota_mbps))
            return list(ctl.frontends()), eff

        tasks.append(asyncio.create_task(
            tcinmod.run_ingress(
                ingress, _tcin_desired,
                tcin_applied, log, period_s=apply_period_s),
            name="tc-ingress"))
        if gov is not None:
            # 调节器：周期跑一轮，有变化就 set tcin_applied 叫醒上面的
            # 下发任务立刻 reconcile（与配置热更共用同一个事件，语义
            # 相同——"期望值变了"）。
            tasks.append(asyncio.create_task(
                ingressgov.run_governor(
                    gov,
                    lambda: (list(ctl.frontends()), ctl.request_limits(),
                             ctl.instance_quota_mbps),
                    tcin_applied, log,
                    interval_s=cfg.dynamic_ingress.interval_s,
                    degraded_fn=lambda: col.degraded,
                    # 总闸落位的实测来源：IFB 根 qdisc 的 Sent 差分
                    # （仅 ifb 模式有；police 模式本就没有实例环）。
                    ingress_rate_fn=getattr(ingress, "ingress_rate_bits",
                                            None)),
                name="ingress-governor"))

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
    # 收尾兜底超时：个别任务收不干净（如 aiohttp 等在途请求）时不无限
    # 等——超时直接放弃，进程退出时事件循环会连带终止残余任务。没有
    # 这道闸，systemd stop 会干等到 TimeoutStopSec 再 SIGKILL。
    try:
        await asyncio.wait_for(
            asyncio.gather(stop_task, *tasks, return_exceptions=True),
            timeout=10.0)
    except asyncio.TimeoutError:
        log.warning("部分任务未在 10s 内退出，放弃等待直接停机（残余任务"
                    "随进程终止）")
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
    api_token = (os.environ.get(ENV_API_TOKEN) or "").strip()
    if console_port and not api_token:
        log.warning("未配置 %s：控制台/API 的写接口（修改限额）整体 403 "
                    "禁用，只读观测不受影响；要启用请在环境变量里设置令牌"
                    "后重启", ENV_API_TOKEN)

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

    # 请求方向（入向）限速：双网卡形态，执行网卡 = 客户端侧那张，只认
    # RL_TC_IN_IFACE 显式指定（绝不自动探测——入向 police 放错网卡会误伤
    # 后端流量）。配置了 request_limits 却没指定网卡 → 启动失败，宁可
    # 起不来也不要"以为限了请求方向其实没有"。
    ingress = None
    raw_in_iface = (os.environ.get(ENV_TC_IN_IFACE) or "").strip()
    if raw_in_iface == "-":
        if cfg.request_limits:
            log.warning("已显式关闭入向限速（%s=-），YAML 里的 "
                        "request_limits（%d 条）不会执行",
                        ENV_TC_IN_IFACE, len(cfg.request_limits))
    elif raw_in_iface:
        in_mode = (os.environ.get(ENV_TC_IN_MODE) or "ifb").strip().lower()
        if in_mode == "ifb":
            ifb_dev = (os.environ.get(ENV_TC_IFB) or "").strip() \
                or tcifbmod.DEFAULT_IFB
            # 启动自检：IFB 设备现在就建出来，建不出来**直接启动失败**
            # ——与出向 HTB 的 tc 自检、"配了 request_limits 没给网卡"
            # 同一铁律：宁可起不来，也不要进程活着、以为在限速。失败
            # 的典型场景：精简内核没有 ifb 模块（Docker Desktop 的
            # 虚拟机）、容器没给 CAP_NET_ADMIN。建成的设备留给
            # TcIfbIngress 接着用（它每轮还会幂等核对，防运行期被人删）。
            import subprocess
            add = subprocess.run(
                ["ip", "link", "add", ifb_dev, "type", "ifb"],
                capture_output=True, text=True)
            exists = subprocess.run(
                ["ip", "link", "show", ifb_dev],
                capture_output=True).returncode == 0
            if not exists:
                print(
                    f"rl-limiter: 无法创建 IFB 设备 {ifb_dev}"
                    f"（{(add.stderr or add.stdout).strip() or '原因未知'}）"
                    f"，启动失败。\n"
                    f"  入向平稳整形（{ENV_TC_IN_MODE}=ifb，默认）依赖内核"
                    f" ifb 模块与 CAP_NET_ADMIN：\n"
                    f"  - Docker Desktop(Mac/Win) 等精简虚拟机内核常缺 "
                    f"ifb——改设 {ENV_TC_IN_MODE}=police（丢包式入向限速，"
                    f"实例总闸不可用），或换带通用内核的 Linux 主机；\n"
                    f"  - 容器部署确认 compose 里有 cap_add: [NET_ADMIN]；\n"
                    f"  - 确实不需要入向限速则设 {ENV_TC_IN_IFACE}=-。",
                    file=sys.stderr)
                raise SystemExit(1)
            ingress = tcifbmod.TcIfbIngress(raw_in_iface, ifb=ifb_dev,
                                            log=log)
        elif in_mode == "police":
            ingress = tcinmod.TcIngress(raw_in_iface, log)
        else:
            print(
                f"rl-limiter: {ENV_TC_IN_MODE}={in_mode!r} 不认识——"
                f"可选 ifb（IFB+HTB 排队整形，默认）或 police"
                f"（超额丢包，老内核备选）。", file=sys.stderr)
            raise SystemExit(1)
    elif cfg.request_limits:
        print(
            f"rl-limiter: YAML 里配置了 request_limits（请求方向限速），"
            f"但没有指定入向执行网卡。\n"
            f"  双网卡形态下请设 {ENV_TC_IN_IFACE}=<客户端侧网卡名>"
            f"（这一项不做自动探测——入向 police 放错网卡会误伤后端"
            f"流量）；\n"
            f"  确实不需要请求方向限速时，删掉 request_limits 段或设 "
            f"{ENV_TC_IN_IFACE}=- 明确关闭。",
            file=sys.stderr)
        raise SystemExit(1)

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
    # 出向 tc 关闭的闭环形态（RL_TC_IFACE=-）跟随**入向**执行网卡：闭环
    # 的调节对象就是那张卡，网卡口径的带宽/包统计必须对着它才有意义。
    raw_nic = (os.environ.get(ENV_NIC) or "").strip()
    if raw_nic == "-":
        nic = ""
    elif raw_nic:
        nic = netdev.resolve_iface(raw_nic, log)
    else:
        nic = (tc_iface
               or (raw_in_iface if raw_in_iface != "-" else "")
               or netdev.resolve_iface("", log))

    # 启动即输出完整配置摘要：现场排障时第一条要看的日志，可直接核对
    # 受管清单、限额基准与两个配置文件的路径。
    log.info(
        "服务配置加载完成，以下为完整配置摘要（排障第一条要看的日志） "
        "yaml=%s cfg=%s instance=%s endpoint=%s log_level=%s "
        "tick_interval_s=%s instance_quota=%s request_limits=%d "
        "frontends=%d detail=%s",
        args.config, cfg.haproxy.cfg_path,
        cfg.haproxy.name, cfg.haproxy.endpoint(),
        cfg.log_level,
        getattr(cfg, "tick_interval_s", 1.0),
        (f"{cfg.instance_quota_mbps:g}Mbps" if cfg.instance_quota_mbps
         else "不限（未设实例总限速）"),
        len(cfg.request_limits),
        len(boot_frontends), _summarize_frontends(boot_frontends),
    )

    try:
        asyncio.run(_amain(cfg, log,
                           yaml_path=args.config,
                           boot_frontends=boot_frontends,
                           console_port=console_port, logbuf=logbuf,
                           console_bind=console_bind,
                           shaper=shaper, ingress=ingress, nic=nic,
                           mlog=mlog, api_token=api_token))
    except KeyboardInterrupt:  # 信号处理兜底：极端时序下直接吞掉干净退出
        pass
    log.info("rl-limiter 服务已停止")


if __name__ == "__main__":
    main()
