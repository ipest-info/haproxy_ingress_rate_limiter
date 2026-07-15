#!/usr/bin/env python3
# tools/mock_backend.py —— 管理后台的开发替身。
#
# 用一个 JSON 文件充当配置源，实现设计文档 §3.6 定义的三个服务端点：
#
#   GET  /v1/agent/config     长轮询下发配置；version = 配置文件 mtime（unix 秒）
#   POST /v1/agent/metrics    仅记摘要日志后丢弃
#   POST /v1/agent/heartbeat  仅记摘要日志后丢弃
#
# 版本机制（mtime 即版本）：真实后台用数据库里的单调版本号，桩直接借用
# 配置文件的修改时间（unix 秒）——只要编辑/touch 文件，mtime 变化即视为
# 新版本，被挂起的长轮询立即拿到新内容返回。代价是同一秒内的多次修改
# 只能算一个版本，且回拨 mtime 也会被当作"变化"——对开发场景足够。
# 无鉴权、无 TLS、无持久化：仅限开发/联调使用。
#
# 用法示例：
#   python3 tools/mock_backend.py --addr 127.0.0.1:9090 \
#       --config deploy/config/mock-backend-config.json

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import sys
import time
from pathlib import Path

# 允许不 pip install 直接从仓库根目录运行：把仓库根加入 sys.path，
# 使 rl_limiter.model 可导入（已安装时该行为无副作用）。
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from aiohttp import web  # noqa: E402

from rl_limiter import model  # noqa: E402

log = logging.getLogger("mock_backend")

# 长轮询的最长挂起时间：必须小于客户端侧的请求超时（约 40s），保证
# "配置无变化"时以干净的 204 收尾，而不是让客户端观察到超时错误。
LONG_POLL_HOLD_S = 25.0
# 轮询配置文件 mtime 的周期：mtime 变化最迟 1 秒内被发现并广播给所有
# 挂起的长轮询。
RELOAD_INTERVAL_S = 1.0
# 限制读取 POST 请求体的上限，防止异常大包耗尽内存。
MAX_BODY_BYTES = 8 << 20


class Store:
    """持有当前配置，并在配置变化时唤醒所有挂起的长轮询。

    唤醒采用"置位并替换事件"的广播手法：每个等待者持有当前 event，
    set() 置位旧 event（一次唤醒所有等待者）并换上新 event 供下一轮
    等待——事件置位后无法复位重用，替换是让"每次变化广播一次"语义
    成立的关键。单线程 asyncio 下 snapshot/set 天然原子。"""

    def __init__(self, cfg: model.ControllerConfig):
        self.cfg = cfg
        self._changed = asyncio.Event()

    def snapshot(self) -> tuple[model.ControllerConfig, asyncio.Event]:
        """返回当前配置，以及"下一次变化时会被置位"的通知事件。"""
        return self.cfg, self._changed

    def set(self, cfg: model.ControllerConfig) -> None:
        """安装新配置并释放所有挂起的长轮询。"""
        self.cfg = cfg
        old = self._changed
        self._changed = asyncio.Event()
        old.set()


def load_config(path: str) -> model.ControllerConfig:
    """把 path 读取为不含版本号的 ControllerConfig，并用文件 mtime
    （unix 秒）盖章为 version（from_dict 内部已做 normalize）。"""
    mtime = int(os.stat(path).st_mtime)
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    cfg = model.ControllerConfig.from_dict(data)
    cfg.version = mtime
    return cfg


async def watch_config(path: str, store: Store) -> None:
    """每 RELOAD_INTERVAL_S 轮询一次文件 mtime，发现与当前版本不一致就
    重新加载并广播。加载失败（文件被删、JSON 编辑到一半等）时保留旧
    配置继续服务，只记日志——桩也遵循 fail-static 精神。"""
    while True:
        await asyncio.sleep(RELOAD_INTERVAL_S)
        try:
            mtime = int(os.stat(path).st_mtime)
        except OSError as e:
            log.warning("config stat failed; keeping current config path=%s err=%s", path, e)
            continue
        if mtime == store.cfg.version:
            continue
        try:
            cfg = load_config(path)
        except Exception as e:
            log.error("config reload failed; keeping current config path=%s err=%s", path, e)
            continue
        store.set(cfg)
        log.info("config reloaded version=%s mode=%s envs=%d",
                 cfg.version, cfg.mode, len(cfg.envs))


def make_handle_config(store: Store):
    """配置长轮询端点。协议行为：
      - 客户端带上自己已应用的版本号（?version=...）；
      - 版本与服务端不一致（含首次请求 version=0）：立即 200 + JSON 全量配置；
      - 版本一致：挂起等待，直到 (a) 配置变化——被 Store.set 置位的事件
        唤醒，回到循环顶部重新快照并返回新配置；(b) 挂满 LONG_POLL_HOLD_S
        ——返回 204 让客户端立即重发下一轮；(c) 客户端断开——aiohttp
        取消本协程，自然结束。"""

    async def handle_config(request: web.Request) -> web.StreamResponse:
        node_id = request.query.get("node_id", "")
        try:
            client_ver = int(request.query.get("version", "0"))
        except ValueError:
            client_ver = 0
        start = time.monotonic()
        deadline = start + LONG_POLL_HOLD_S

        while True:
            cfg, changed = store.snapshot()
            if cfg.version != client_ver:
                log.info(
                    "config served node_id=%s remote=%s client_version=%d "
                    "version=%s mode=%s envs=%d waited_ms=%d",
                    node_id, request.remote, client_ver,
                    cfg.version, cfg.mode, len(cfg.envs),
                    int((time.monotonic() - start) * 1000))
                return web.json_response(cfg.to_dict())
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return web.Response(status=204)
            try:
                await asyncio.wait_for(changed.wait(), timeout=remaining)
            except asyncio.TimeoutError:
                return web.Response(status=204)

    return handle_config


async def _decode_loose(request: web.Request) -> dict:
    """把请求体解析为无类型的 JSON 对象：上报格式演进时桩不需要同步改。"""
    body = await request.content.read(MAX_BODY_BYTES)
    m = json.loads(body)
    if not isinstance(m, dict):
        raise ValueError("body is not a JSON object")
    return m


async def handle_metrics(request: web.Request) -> web.StreamResponse:
    """接收用量样本批次：只记录节点与样本条数，数据即弃
    （真实后台会以此驱动配额再切分与大盘展示）。"""
    try:
        m = await _decode_loose(request)
    except Exception as e:
        log.warning("metrics decode failed remote=%s err=%s", request.remote, e)
        return web.Response(status=400)
    samples = m.get("samples")
    n = len(samples) if isinstance(samples, list) else 0
    log.info("metrics received node_id=%s remote=%s samples=%d",
             m.get("node_id"), request.remote, n)
    return web.Response(status=204)


async def handle_heartbeat(request: web.Request) -> web.StreamResponse:
    """接收心跳：记录服务身份、版本、运行模式与已应用的配置版本，
    便于联调时确认配置是否推送到位。"""
    try:
        m = await _decode_loose(request)
    except Exception as e:
        log.warning("heartbeat decode failed remote=%s err=%s", request.remote, e)
        return web.Response(status=400)
    log.info(
        "heartbeat received node_id=%s remote=%s service_version=%s mode=%s config_version=%s",
        m.get("node_id"), request.remote,
        m.get("service_version", m.get("agent_version")),
        m.get("mode"), m.get("config_version"))
    return web.Response(status=204)


def parse_addr(addr: str) -> tuple[str, int]:
    """解析 --addr："host:port" 或 ":port"（后者监听全部地址）。"""
    host, _, port = addr.rpartition(":")
    if not port:
        raise ValueError(f"bad addr {addr!r} (expected host:port)")
    return host or "0.0.0.0", int(port)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="管理后台桩：JSON 文件作配置源，version = 文件 mtime（秒）")
    parser.add_argument("--addr", default="127.0.0.1:9090",
                        help="监听地址 host:port（默认 %(default)s）")
    parser.add_argument("--config", required=True,
                        help="ControllerConfig JSON 文件路径（无 version 字段，version = mtime）")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S%z")

    try:
        cfg = load_config(args.config)
    except Exception as e:
        print(f"mock-backend: {e}", file=sys.stderr)
        raise SystemExit(1)
    host, port = parse_addr(args.addr)

    store = Store(cfg)
    log.info("config loaded path=%s version=%s mode=%s envs=%d",
             args.config, cfg.version, cfg.mode, len(cfg.envs))

    app = web.Application(client_max_size=MAX_BODY_BYTES)
    app.router.add_get("/v1/agent/config", make_handle_config(store))
    app.router.add_post("/v1/agent/metrics", handle_metrics)
    app.router.add_post("/v1/agent/heartbeat", handle_heartbeat)

    async def _start_watcher(app: web.Application):
        # 后台 mtime 监视任务：随 app 生命周期启动/回收。
        task = asyncio.create_task(watch_config(args.config, store))
        yield
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    app.cleanup_ctx.append(_start_watcher)

    log.info("mock-backend listening addr=%s:%d config=%s", host, port, args.config)
    # access_log=None：请求级日志由各 handler 自己按 key=value 输出，
    # 关闭 aiohttp 默认访问日志避免长轮询刷屏。
    web.run_app(app, host=host, port=port, access_log=None, print=None)
    log.info("mock-backend stopped")


if __name__ == "__main__":
    main()
