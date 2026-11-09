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
import re
import ssl
import sys
import time

from . import (fleetconfig, fleetgov, fleetmetrics, fleetpoll, fleetweb,
               tokenpolicy)

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
                 api_token: str = "",
                 mlog: "fleetmetrics.FleetMetricsLog | None" = None,
                 frame_ancestors: "list | tuple" = (),
                 cors_origins: "list | tuple" = (),
                 ssl_context=None) -> None:
    poller = fleetpoll.NodePoller(log, timeout_s=cfg.node_timeout_s,
                                  offline_after=cfg.offline_after)
    poller.set_nodes(cfg.nodes)
    gov = fleetgov.FleetGovernor(log)

    # 监控落盘（可选）：限速变更当场记事件，分钟数据由轮询环喂。
    if mlog is not None:
        def _on_write(s, before, mbps):
            if mbps is None:
                mlog.event("release", node=s.name, from_mbps=before,
                           to_mbps=0.0)
            else:
                mlog.event("quota", node=s.name, from_mbps=before,
                           to_mbps=mbps)
        poller.on_write = _on_write
        mlog.event("start", version=SERVICE_VERSION,
                   total_mbps=cfg.total_egress_mbps,
                   nodes=[n.name for n in cfg.nodes])

    # 热更字段的当前视图（watch 回调整体替换；周期类字段启动定型，
    # 轮询/分配环直接用启动时的值）。
    cur = {"cfg": cfg}
    # 总带宽变了就立刻跑一轮分配（不等 alloc_interval_s）：页面上点
    # "取消总量控制"要马上解除各节点限速，改总带宽也该马上见效。
    alloc_wake = asyncio.Event()

    def _on_change(newcfg: "fleetconfig.FleetConfig") -> None:
        old = cur["cfg"]
        cur["cfg"] = newcfg
        poller.set_nodes(newcfg.nodes)
        if mlog is not None:
            _record_config_events(mlog, old, newcfg)
        if newcfg.total_egress_mbps != old.total_egress_mbps:
            log.info("总流出带宽已变更，立即重新分配%s total=%g→%gMbps",
                     "（取消总量控制：解除各节点实例总限速）"
                     if not newcfg.total_egress_mbps else "",
                     old.total_egress_mbps, newcfg.total_egress_mbps)
            alloc_wake.set()

    cfg_poke = asyncio.Event()

    def _on_api(actor: str, op: str, detail: dict) -> None:
        """开放 API 写操作的审计：谁（admin / key:<名>）做了什么。"""
        if mlog is not None:
            mlog.event("api", by=actor, op=op, **detail)

    hub: "fleetweb.FleetHub | None" = None
    if console_port:
        hub = fleetweb.FleetHub(
            SERVICE_VERSION,
            total_fn=lambda: cur["cfg"].total_egress_mbps,
            params_fn=lambda: {
                k: getattr(cur["cfg"].alloc, k)
                for k in ("min_share_ratio", "smooth_alpha", "press_ratio",
                          "press_boost", "idle_mbps", "apply_epsilon",
                          "min_delta_mbps", "min_quota_mbps")},
            nodes_fn=lambda: cur["cfg"].nodes)

    async def poll_loop() -> None:
        """观测环：轮询全部节点 + 给控制台/落盘喂帧。"""
        while True:
            try:
                await poller.poll_once()
                if hub is not None or mlog is not None:
                    snap = fleetweb.fleet_snapshot(
                        time.time(), poller.states(),
                        cur["cfg"].total_egress_mbps)
                    if hub is not None:
                        hub.record(snap)
                    if mlog is not None:
                        mlog.record(snap)
            except asyncio.CancelledError:
                raise
            except Exception as e:          # 常驻任务不能因单次异常退出
                log.error("节点轮询出现未预期的异常，本轮跳过 err=%s", e)
            await asyncio.sleep(cfg.poll_interval_s)

    async def alloc_loop() -> None:
        """分配环：按需切分总带宽并下发（慢速外环）。"""
        while True:
            try:
                await asyncio.wait_for(alloc_wake.wait(),
                                       timeout=cfg.alloc_interval_s)
            except asyncio.TimeoutError:
                pass
            alloc_wake.clear()
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
                api_token=api_token, poke=cfg_poke,
                metrics_path=mlog.path if mlog is not None else "",
                keys_fn=lambda: cur["cfg"].api_keys, audit_fn=_on_api,
                frame_ancestors=frame_ancestors, cors_origins=cors_origins,
                ssl_context=ssl_context),
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
        if mlog is not None:
            mlog.close()


def _record_config_events(mlog: "fleetmetrics.FleetMetricsLog",
                          old: "fleetconfig.FleetConfig",
                          new: "fleetconfig.FleetConfig") -> None:
    """配置热更（页面/API/改 YAML 都走这里）落事件：总带宽、节点清单、
    节点固定限速（limit）、API 密钥增删（keys）。

    节点改了哪些字段只记字段名——令牌的值绝不落盘。
    """
    if new.total_egress_mbps != old.total_egress_mbps:
        mlog.event("total", from_mbps=old.total_egress_mbps,
                   to_mbps=new.total_egress_mbps)
    before = {n.name: n for n in old.nodes}
    after = {n.name: n for n in new.nodes}
    changed = []
    for name in sorted(set(before) & set(after)):
        a, b = before[name], after[name]
        fields = [k for k in ("url", "console_url", "token")
                  if getattr(a, k) != getattr(b, k)]
        if fields:
            changed.append({"name": name, "fields": fields})
    for name in sorted(set(before) & set(after)):
        a, b = before[name].fixed_mbps, after[name].fixed_mbps
        if a != b:
            mlog.event("limit", node=name, from_mbps=a, to_mbps=b)
    kb = {k.name for k in old.api_keys}
    ka = {k.name for k in new.api_keys}
    if kb != ka:
        mlog.event("keys", added=sorted(ka - kb), removed=sorted(kb - ka))
    added = sorted(set(after) - set(before))
    removed = sorted(set(before) - set(after))
    if added or removed or changed:
        mlog.event("nodes", added=[{"name": n, "url": after[n].url,
                                    "has_token": bool(after[n].token)}
                                   for n in added],
                   removed=removed, changed=changed)


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="rl-fleet",
        description="rl-limiter 总控制台：汇总数十台节点的流量，把总流出"
                    "带宽按需动态分配为各节点的实例出向目标（节点自己的"
                    "闭环再转成入向限速）。配置只有一个本地 YAML（节点"
                    "清单 + 总带宽 + 分配参数），轮询热生效。Web 控制台："
                    "设 RL_CONSOLE_PORT 启用，RL_CONSOLE_BIND 指定监听"
                    "地址（默认 0.0.0.0——读接口无鉴权，用防火墙限制"
                    "来源）；改总带宽、添加/编辑/删除节点的"
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
    # 原值交给控制台（它自己按规则判，弱令牌的 403 才说得出"强度不足"）；
    # 这里只用判定结果打启动日志。
    api_token = (os.environ.get("RL_API_TOKEN") or "").strip()
    _, token_weak = tokenpolicy.effective(api_token)
    mlog = _open_metrics_log(log)
    if token_weak:
        log.error("RL_API_TOKEN 强度不足，按未配置处理（总控页面只读：改不"
                  "了总带宽、加不了节点）：%s——用 %s 生成新令牌，写进 "
                  "rl-fleet.env 后重启", token_weak,
                  tokenpolicy.GENERATE_HINT)
    elif console_port and not api_token:
        log.warning("未配置 RL_API_TOKEN：总控页面只读（看得到汇总，"
                    "改不了总带宽、加不了节点）。需要写能力请设置令牌后"
                    "重启")

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
        if n.token_rejected:
            log.error("节点令牌强度不足，按未配置处理（只读观测、不可控，"
                      "不会向它下发或解除配额）：%s——在该节点上用 %s "
                      "生成新令牌，并在总控「节点管理」更新 node=%s",
                      n.token_rejected, tokenpolicy.GENERATE_HINT, n.name)
        elif not n.token:
            log.warning("节点未配置令牌，只读观测、不可控（不会向它下发"
                        "配额） node=%s url=%s", n.name, n.url)

    frame_ancestors = _origin_env("RL_FRAME_ANCESTORS", _ANCESTOR_RE,
                                  True, log)
    cors_origins = _origin_env("RL_CORS_ORIGINS", _ORIGIN_RE, False, log)
    ssl_context = _ssl_from_env()
    if console_port:
        log.info("开放 API：API 密钥 %d 个（%s）；iframe 嵌入允许 %s；"
                 "跨域允许 %s；%s",
                 len(cfg.api_keys),
                 "、".join(k.name for k in cfg.api_keys) or "无",
                 "、".join(frame_ancestors) or "仅同源",
                 "、".join(cors_origins) or "无",
                 "HTTPS" if ssl_context is not None else "HTTP（被 HTTPS 页面"
                 "嵌入需要配 RL_TLS_CERT/RL_TLS_KEY）")

    try:
        asyncio.run(_amain(cfg, log, args.config,
                           console_port=console_port, logbuf=logbuf,
                           console_bind=console_bind, api_token=api_token,
                           mlog=mlog, frame_ancestors=frame_ancestors,
                           cors_origins=cors_origins,
                           ssl_context=ssl_context))
    except KeyboardInterrupt:
        log.info("收到中断信号，rl-fleet 退出")


# 允许 iframe 嵌入总控页面的来源（CSP frame-ancestors 源表达式）：
# https://portal.example.com、https://*.example.com、可带端口；单独一个 *
# = 任何网站（等于不设防，启动时告警）。
_ANCESTOR_RE = re.compile(r"^https?://(\*\.)?[A-Za-z0-9.-]+(:\d{1,5})?$")
# 允许浏览器跨域调 /api/ 的来源：必须是精确来源（不收通配）。
_ORIGIN_RE = re.compile(r"^https?://[A-Za-z0-9.-]+(:\d{1,5})?$")


def _origin_env(name: str, pattern: "re.Pattern", allow_star: bool,
                log: logging.Logger) -> list:
    """读逗号/空格分隔的来源清单；有一个不合法就启动失败（配错了宁可
    起不来，也不要以为放行了、实际被浏览器拦）。"""
    items = [x.rstrip("/") for x in
             re.split(r"[\s,]+", os.environ.get(name) or "") if x]
    for x in items:
        if x == "*" and allow_star:
            log.warning("%s=*：任何网站都能用 iframe 嵌入总控页面（有"
                        "点击劫持风险），建议改成具体来源", name)
            continue
        if not pattern.match(x):
            print(f"rl-fleet: {name} 里的 {x!r} 不是合法来源（写成 "
                  f"https://portal.example.com 这样，多个用逗号隔开）",
                  file=sys.stderr)
            raise SystemExit(1)
    return items


def _ssl_from_env() -> "ssl.SSLContext | None":
    """RL_TLS_CERT / RL_TLS_KEY 都给 = 总控页面与 API 走 HTTPS。"""
    cert = (os.environ.get("RL_TLS_CERT") or "").strip()
    key = (os.environ.get("RL_TLS_KEY") or "").strip()
    if not cert and not key:
        return None
    if not (cert and key):
        print("rl-fleet: RL_TLS_CERT 与 RL_TLS_KEY 要一起设（证书链 + 私钥"
              "的 PEM 文件路径）", file=sys.stderr)
        raise SystemExit(1)
    ctx = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
    try:
        ctx.load_cert_chain(cert, key)
    except (OSError, ssl.SSLError) as e:
        print(f"rl-fleet: 加载 HTTPS 证书失败（{e}）：RL_TLS_CERT={cert} "
              f"RL_TLS_KEY={key}——确认文件存在、服务用户可读、证书与私钥"
              f"配对", file=sys.stderr)
        raise SystemExit(1)
    return ctx


def _open_metrics_log(log: logging.Logger
                      ) -> "fleetmetrics.FleetMetricsLog | None":
    """监控落盘：设 RL_METRICS_LOG=<文件路径> 即启用（与节点侧同名同义；
    同机两者都开时用不同文件名）。显式配了却打不开 → 启动失败，不静默
    丢数据。"""
    raw = (os.environ.get("RL_METRICS_LOG") or "").strip()
    if not raw:
        log.info("监控落盘未启用（RL_METRICS_LOG 未设）：总带宽/限速只在"
                 "内存里留约 10 分钟，没法事后反查")
        return None
    raw_days = (os.environ.get("RL_METRICS_LOG_DAYS") or "").strip()
    try:
        days = (int(raw_days) if raw_days
                else fleetmetrics.DEFAULT_RETENTION_DAYS)
    except ValueError:
        print(f"rl-fleet: RL_METRICS_LOG_DAYS 必须是整数，当前值 "
              f"{raw_days!r}", file=sys.stderr)
        raise SystemExit(1)
    if days < 1:
        print(f"rl-fleet: RL_METRICS_LOG_DAYS 必须 ≥ 1，当前值 {days}",
              file=sys.stderr)
        raise SystemExit(1)
    try:
        mlog = fleetmetrics.FleetMetricsLog(raw, log, retention_days=days)
    except OSError as e:
        print(f"rl-fleet: 打不开监控落盘文件 {raw}（{e}）。\n  请确认目录"
              f"存在且服务用户可写（systemd 部署时 unit 里的 "
              f"LogsDirectory=rl-limiter 会自动创建 /var/log/rl-limiter）。",
              file=sys.stderr)
        raise SystemExit(1)
    log.info("监控落盘已启用（分钟粒度 JSONL + 限速变更事件，按天轮转，"
             "页面「历史回查」与 /api/metrics-log 可查） path=%s "
             "retention_days=%d", raw, days)
    return mlog


if __name__ == "__main__":
    main()
