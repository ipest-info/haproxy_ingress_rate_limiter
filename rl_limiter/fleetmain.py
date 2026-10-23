# rl_limiter.fleetmain —— 总控制台（rl-fleet）的进程入口。
#
# 一台总控管数十台节点：轮询各节点 rl-limiter 的 /api/overview 汇总
# 观测 → 周期把 total_egress_mbps 按需切成各节点的实例出向目标 →
# 经节点写 API 下发；节点自己的闭环再把出向目标转成入向限速。
# 节点侧零改动、零新增组件——总控只是节点控制台 API 的一个客户端。
#
# 结构照节点侧 __main__：环境变量 fail-fast、启动摘要日志、asyncio.run
# 跑 _amain 里的常驻任务组（轮询环 / 分配环 / 配置热更 / Web 控制台）。

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys
import time

from . import fleetconfig, fleetgov, fleetpoll, fleetweb

try:
    from importlib import metadata as _metadata
    SERVICE_VERSION = _metadata.version("rl-limiter")
except Exception:                                      # pragma: no cover
    SERVICE_VERSION = "dev"

# 与节点侧同款默认：绑全部地址（监控页内网直达），写接口靠令牌闸住，
# 读接口无鉴权由防火墙限制来源；要收紧设 RL_CONSOLE_BIND=127.0.0.1。
DEFAULT_CONSOLE_BIND = "0.0.0.0"


async def _amain(cfg: "fleetconfig.FleetConfig",
                 log: logging.Logger,
                 yaml_path: str,
                 console_port: int = 0,
                 logbuf: "fleetweb.LogBuffer | None" = None,
                 console_bind: str = DEFAULT_CONSOLE_BIND,
                 api_token: str = "") -> None:
    poller = fleetpoll.NodePoller(log, timeout_s=cfg.node_timeout_s,
                                  offline_after=cfg.offline_after)
    poller.set_nodes(cfg.nodes)
    gov = fleetgov.FleetGovernor(log)

    # 热更字段的当前视图（watch 回调整体替换；周期类字段启动定型，
    # 轮询/分配环直接用启动时的值）。
    cur = {"cfg": cfg}

    def _on_change(newcfg: "fleetconfig.FleetConfig") -> None:
        old = cur["cfg"]
        cur["cfg"] = newcfg
        poller.set_nodes(newcfg.nodes)
        if newcfg.total_egress_mbps != old.total_egress_mbps:
            log.info("总流出带宽已变更（下一个分配周期生效） "
                     "total=%g→%gMbps",
                     old.total_egress_mbps, newcfg.total_egress_mbps)

    cfg_poke = asyncio.Event()

    hub: "fleetweb.FleetHub | None" = None
    if console_port:
        hub = fleetweb.FleetHub(
            SERVICE_VERSION,
            total_fn=lambda: cur["cfg"].total_egress_mbps,
            params_fn=lambda: {
                k: getattr(cur["cfg"].alloc, k)
                for k in ("min_share_ratio", "smooth_alpha", "press_ratio",
                          "press_boost", "idle_mbps", "apply_epsilon",
                          "min_delta_mbps", "min_quota_mbps")})

    async def poll_loop() -> None:
        """观测环：轮询全部节点 + 给控制台喂帧。"""
        while True:
            try:
                await poller.poll_once()
                if hub is not None:
                    hub.record(fleetweb.fleet_snapshot(
                        time.time(), poller.states(),
                        cur["cfg"].total_egress_mbps))
            except asyncio.CancelledError:
                raise
            except Exception as e:          # 常驻任务不能因单次异常退出
                log.error("节点轮询出现未预期的异常，本轮跳过 err=%s", e)
            await asyncio.sleep(cfg.poll_interval_s)

    async def alloc_loop() -> None:
        """分配环：按需切分总带宽并下发（慢速外环）。"""
        while True:
            await asyncio.sleep(cfg.alloc_interval_s)
            try:
                writes = gov.allocate_once(
                    poller.states(), cur["cfg"].total_egress_mbps,
                    cur["cfg"].alloc)
                await poller.apply(writes)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log.error("分配调度出现未预期的异常，本轮跳过 err=%s", e)

    tasks = [
        asyncio.create_task(poll_loop(), name="fleet-poll"),
        asyncio.create_task(alloc_loop(), name="fleet-alloc"),
        asyncio.create_task(
            fleetconfig.watch(yaml_path, _on_change, log, poke=cfg_poke),
            name="fleet-cfg-watch"),
    ]
    if hub is not None:
        tasks.append(asyncio.create_task(
            fleetweb.run_fleet_console(
                console_port, hub, logbuf or fleetweb.LogBuffer(), log,
                bind=console_bind, yaml_path=yaml_path,
                api_token=api_token, poke=cfg_poke),
            name="fleet-console"))

    log.info("rl-fleet 总控已启动，开始轮询与分配 nodes=%d total=%gMbps "
             "poll=%gs alloc=%gs version=%s",
             len(cfg.nodes), cfg.total_egress_mbps,
             cfg.poll_interval_s, cfg.alloc_interval_s, SERVICE_VERSION)
    try:
        done, _ = await asyncio.wait(tasks,
                                     return_when=asyncio.FIRST_COMPLETED)
        for t in done:                         # 常驻任务不该自己结束
            exc = t.exception()
            if exc is not None:
                raise exc
    finally:
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await poller.close()


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="rl-fleet",
        description="rl-limiter 总控制台：汇总数十台节点的流量，把总流出"
                    "带宽按需动态分配为各节点的实例出向目标（节点自己的"
                    "闭环再转成入向限速）。配置只有一个本地 YAML（节点"
                    "清单 + 总带宽 + 分配参数），轮询热生效。Web 控制台："
                    "设 RL_CONSOLE_PORT 启用，RL_CONSOLE_BIND 指定监听"
                    "地址（默认 0.0.0.0——读接口无鉴权，用防火墙限制"
                    "来源）；改总带宽的"
                    "写 API 用 RL_API_TOKEN 鉴权。")
    parser.add_argument(
        "-c", "--config", default="/etc/rl-limiter/fleet.yaml",
        help="fleet YAML 配置文件路径（默认 %(default)s）")
    parser.add_argument(
        "--version", action="store_true", help="打印版本号后退出")
    args = parser.parse_args()
    if args.version:
        print("rl-fleet", SERVICE_VERSION)
        return

    logging.basicConfig(
        stream=sys.stderr,
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S%z",
    )
    log = logging.getLogger("rl_limiter.fleet")

    console_port = 0
    raw_console = (os.environ.get("RL_CONSOLE_PORT") or "").strip()
    logbuf = None
    if raw_console:
        try:
            console_port = int(raw_console)
        except ValueError:
            print(f"rl-fleet: 环境变量 RL_CONSOLE_PORT 必须是整数，"
                  f"当前值 {raw_console!r}", file=sys.stderr)
            raise SystemExit(1)
        if not 1 <= console_port <= 65535:
            print(f"rl-fleet: 环境变量 RL_CONSOLE_PORT 必须在 1-65535 "
                  f"范围内，当前值 {console_port}", file=sys.stderr)
            raise SystemExit(1)
        logbuf = fleetweb.LogBuffer()
        logging.getLogger().addHandler(logbuf)
    console_bind = (os.environ.get("RL_CONSOLE_BIND") or "").strip() \
        or DEFAULT_CONSOLE_BIND
    api_token = (os.environ.get("RL_API_TOKEN") or "").strip()
    if console_port and not api_token:
        log.warning("未配置 RL_API_TOKEN：总控页面只读（看得到汇总，"
                    "改不了总带宽）。需要写能力请设置令牌后重启")

    try:
        cfg = fleetconfig.load(args.config)
    except (ValueError, OSError) as e:
        print(f"rl-fleet: 加载配置失败：{e}", file=sys.stderr)
        raise SystemExit(1)

    level = getattr(logging, str(cfg.log_level).upper(), logging.INFO)
    logging.getLogger().setLevel(level)
    log.info(
        "总控配置加载完成 yaml=%s total=%gMbps nodes=%d（%s） "
        "poll=%gs alloc=%gs offline_after=%d log_level=%s",
        args.config, cfg.total_egress_mbps, len(cfg.nodes),
        "、".join(n.name for n in cfg.nodes) or "空清单",
        cfg.poll_interval_s, cfg.alloc_interval_s, cfg.offline_after,
        cfg.log_level)
    for n in cfg.nodes:
        if not n.token:
            log.warning("节点未配置令牌，只读观测、不可控（不会向它下发"
                        "配额） node=%s url=%s", n.name, n.url)

    try:
        asyncio.run(_amain(cfg, log, args.config,
                           console_port=console_port, logbuf=logbuf,
                           console_bind=console_bind, api_token=api_token))
    except KeyboardInterrupt:
        log.info("收到中断信号，rl-fleet 退出")


if __name__ == "__main__":
    main()
