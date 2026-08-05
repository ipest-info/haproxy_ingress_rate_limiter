# rl_limiter.aggmain —— hap-agg（HAProxy 监控聚合）服务入口。
#
# 一个 hap-agg 进程监控 N 台 HAProxy：经各台机器 haproxy.cfg 里暴露的
# 内网 TCP stats socket（`stats socket ipv4@*:9999 …`）每秒采样
# show stat / show info，把所有目标合并成一个视图（Web 页面 + JSON API
# + Prometheus /metrics）。**纯只读**：不写任何 HAProxy 状态、不限速、
# 目标机器不需要装任何东西。
#
# 与 rl-limiter 的分工：rl-limiter 与 HAProxy 同机、做限速 + 单机监控；
# hap-agg 部署在任意一台能到达各目标 9999 端口的机器上，做**跨机聚合**
# 观测。两者可以同时使用，互不依赖。

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import signal
import sys
import time

from . import agg as aggmod
from . import aggconfig, aggweb

try:
    from importlib import metadata as _metadata

    SERVICE_VERSION = _metadata.version("rl-limiter")
except Exception:  # pragma: no cover - 未安装场景
    SERVICE_VERSION = "dev"

ENV_API_TOKEN = "RL_API_TOKEN"


async def _amain(cfg: aggconfig.AggConfig, log: logging.Logger,
                 yaml_path: str, port: int, bind: str,
                 api_token: str) -> None:
    aggregator = aggmod.Aggregator(cfg.targets, log)
    hub = aggweb.AggHub(SERVICE_VERSION)

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

    async def poll_loop() -> None:
        """采样主循环：固定节拍，一拍超时/慢了不追赶（跳过比堆积好）。"""
        while True:
            started = time.monotonic()
            try:
                snap = await aggregator.tick()
                hub.record(snap)
            except asyncio.CancelledError:
                raise
            except Exception as e:      # 常驻任务不能因单拍异常退出
                log.error("聚合采样一拍出现未预期异常，本拍跳过 err=%s", e)
            elapsed = time.monotonic() - started
            await asyncio.sleep(max(0.05, cfg.poll_interval_s - elapsed))

    tasks = [
        asyncio.create_task(poll_loop(), name="agg-poll"),
        asyncio.create_task(
            aggweb.run_web(port, hub, aggregator, log, bind=bind,
                           yaml_path=yaml_path, api_token=api_token),
            name="agg-web"),
    ]
    log.info("hap-agg 已启动 targets=%d poll_interval_s=%g port=%d "
             "version=%s detail=%s",
             len(cfg.targets), cfg.poll_interval_s, port, SERVICE_VERSION,
             ";".join(f"{t.name}@{t.addr}" for t in cfg.targets) or "(空)")

    stop_task = asyncio.create_task(stop.wait(), name="stop-signal")
    done, _pending = await asyncio.wait(
        [stop_task, *tasks], return_when=asyncio.FIRST_COMPLETED)
    for t in done:
        if t is not stop_task and t.exception() is not None:
            log.error("常驻任务异常退出，服务整体退出 task=%s err=%s",
                      t.get_name(), t.exception())
    for t in (stop_task, *tasks):
        t.cancel()
    try:
        await asyncio.wait_for(
            asyncio.gather(stop_task, *tasks, return_exceptions=True),
            timeout=10.0)
    except asyncio.TimeoutError:
        log.warning("部分任务未在 10s 内退出，放弃等待直接停机")


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="hap-agg",
        description="HAProxy 多实例监控聚合：经各机内网 TCP stats socket"
                    "（haproxy.cfg 里的 stats socket ipv4@*:9999）批量"
                    "采样 show stat / show info，合并成一个视图（Web 页面"
                    " + JSON API + Prometheus /metrics）。纯只读，目标"
                    "机器无需安装任何东西",
        epilog="目标清单在 -c 指定的 YAML 里（targets: 列表，见 "
               "deploy/config/hap-agg.example.yaml），也可以在页面/API 上"
               "批量导入（每行一条 'IP:port' 或 '名字 IP:port'，需设 "
               "RL_API_TOKEN），导入结果回写 YAML。读接口无鉴权，默认只"
               "绑 127.0.0.1；放内网必须用 --bind 显式指定并配防火墙。")
    parser.add_argument(
        "-c", "--config", default="/etc/hap-agg/config.yaml",
        help="YAML 配置路径（默认 %(default)s）")
    parser.add_argument(
        "-p", "--port", type=int, default=8100,
        help="Web 视图监听端口（默认 %(default)s）")
    parser.add_argument(
        "--bind", default="127.0.0.1",
        help="Web 视图监听地址（默认 %(default)s；读接口无鉴权，放内网"
             "必须配防火墙）")
    parser.add_argument(
        "--version", action="store_true", help="打印版本号后退出")
    args = parser.parse_args()

    if args.version:
        print("hap-agg", SERVICE_VERSION)
        return

    logging.basicConfig(
        stream=sys.stderr,
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S%z",
    )
    log = logging.getLogger("hap_agg")

    try:
        cfg = aggconfig.load(args.config)
    except FileNotFoundError as e:
        print(f"hap-agg: 找不到配置文件 {args.config}（{e.strerror}）。\n"
              f"  用 -c 指向一份实际存在的 YAML"
              f"（示例见 deploy/config/hap-agg.example.yaml）。",
              file=sys.stderr)
        raise SystemExit(1)
    except Exception as e:
        print(f"hap-agg: {e}", file=sys.stderr)
        raise SystemExit(1)

    level = getattr(logging, str(cfg.log_level).upper(), logging.INFO)
    logging.getLogger().setLevel(level)

    if not cfg.targets:
        log.warning("目标清单为空——服务照常启动，可在页面/API 上批量导入"
                    "（需设 %s）或编辑 %s 后重启", ENV_API_TOKEN, args.config)

    api_token = (os.environ.get(ENV_API_TOKEN) or "").strip()
    if not api_token:
        log.warning("未配置 %s：批量导入/删除目标的写接口整体 403 禁用，"
                    "只读观测不受影响", ENV_API_TOKEN)

    try:
        asyncio.run(_amain(cfg, log, yaml_path=args.config, port=args.port,
                           bind=args.bind, api_token=api_token))
    except KeyboardInterrupt:
        pass
    log.info("hap-agg 已停止")


if __name__ == "__main__":
    main()
