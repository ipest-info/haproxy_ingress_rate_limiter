# tests.test_reporter —— rl_limiter.reporter 的行为测试。
#
# 用 aiohttp.web 起一个本地"假管理后台"（FakeBackend），从真实 HTTP 链路
# 验证三条通信链路的语义：
#   - 长轮询：版本水位推进、200/500 分支、fail-static 缓存原子落盘、
#     配置队列的合并（coalescing）语义；
#   - 指标：有界缓冲上限与丢弃节流、payload 结构、失败保留样本；
#   - 心跳：启动即发、payload 取自 mode_fn/version_fn。
#
# 时序断言只依赖"下界"（退避至少等了多久），不依赖上界的精确调度，
# 避免慢 CI 上抖动导致假失败。

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import time

import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer

from rl_limiter import model, reporter
from rl_limiter.config import BackendOptions

NODE_ID = "svc-1"
SERVICE_VERSION = "v-test"

# 两份可下发的配置文档（管理后台 JSON 口径：quota_bps 为 bits/s）。
CFG_V1 = {
    "version": 1,
    "mode": "enforce",
    "envs": [
        {
            "env_id": "env-a",
            "quota_bps": 200_000_000,
            "targets": [{"node": "lb-1", "frontend": "fe_a"}],
        }
    ],
    "report_interval_s": 5,
    "heartbeat_interval_s": 10,
}
CFG_V2 = {
    "version": 2,
    "mode": "dry-run",
    "envs": [
        {
            "env_id": "env-a",
            "quota_bps": 100_000_000,
            "targets": [{"node": "lb-1", "frontend": "fe_a"}],
        },
        {
            "env_id": "env-b",
            "quota_bps": 50_000_000,
            "targets": [{"node": "lb-1", "frontend": "fe_b"}],
        },
    ],
}


class FakeBackend:
    """脚本化的假管理后台。

    config 接口的行为由测试通过 config_responses / fail_config 控制：
    有待发配置时立即 200，否则挂住模拟长轮询（stop 事件用于测试收尾时
    释放挂住的 handler，避免 aiohttp 优雅关闭等满超时）。
    """

    def __init__(self) -> None:
        self.stop = asyncio.Event()
        self.config_polls: list[tuple[str, str, float]] = []  # (node_id, version, t)
        self.config_responses: list[dict] = []
        self.fail_config = False
        self.metrics: list[dict] = []
        self.metrics_status = 200
        self.heartbeats: list[dict] = []

    async def handle_config(self, request: web.Request) -> web.Response:
        self.config_polls.append(
            (
                request.query.get("node_id", ""),
                request.query.get("version", ""),
                time.monotonic(),
            )
        )
        if self.fail_config:
            return web.Response(status=500)
        # 模拟长轮询：没有新配置就挂住，直到测试补投配置或收尾。
        while not self.config_responses:
            if self.stop.is_set():
                return web.Response(status=204)
            await asyncio.sleep(0.01)
        return web.json_response(self.config_responses.pop(0))

    async def handle_metrics(self, request: web.Request) -> web.Response:
        self.metrics.append(await request.json())
        return web.Response(status=self.metrics_status)

    async def handle_heartbeat(self, request: web.Request) -> web.Response:
        self.heartbeats.append(await request.json())
        return web.Response(status=200)


@contextlib.asynccontextmanager
async def running_reporter(tmp_path, backend: FakeBackend, **rep_kwargs):
    """起假后台 + 构造 Reporter + 启动 run 任务，收尾时按序清理：
    先取消 run（客户端停发请求），再放行挂住的 handler，最后关服务。"""
    app = web.Application()
    app.router.add_get("/v1/agent/config", backend.handle_config)
    app.router.add_post("/v1/agent/metrics", backend.handle_metrics)
    app.router.add_post("/v1/agent/heartbeat", backend.handle_heartbeat)
    server = TestServer(app)
    await server.start_server()
    opts = BackendOptions(
        base_url=str(server.make_url("/")).rstrip("/"),
        cache_path=str(tmp_path / "cache.json"),
    )
    rep = reporter.Reporter(
        opts,
        node_id=NODE_ID,
        service_version=SERVICE_VERSION,
        **rep_kwargs,
    )
    task = asyncio.create_task(rep.run())
    try:
        yield rep, backend, opts
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        backend.stop.set()
        await server.close()


async def eventually(cond, timeout: float = 5.0, interval: float = 0.02):
    """轮询等待条件成立；超时视为断言失败。"""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if cond():
            return
        await asyncio.sleep(interval)
    raise AssertionError("condition not met within %.1fs" % timeout)


def make_usage(env_id: str = "env-a") -> model.EnvUsage:
    return model.EnvUsage(
        env_id=env_id,
        rate_bps=100.5,
        mean10_bps=90.0,
        ewma60_bps=80.0,
        conn_cur=7,
    )


def make_decision(env_id: str = "env-a") -> model.Decision:
    return model.Decision(
        env_id=env_id,
        targets=[model.Target("lb-1", "fe_a")],
        bwlim_bps=25_000_000.0,
        state=model.GovState.TIGHTENING,
        changed=True,
    )


# ---- 长轮询 ---------------------------------------------------------------


async def test_long_poll_advances_version_and_persists_cache(tmp_path):
    backend = FakeBackend()
    backend.config_responses.append(dict(CFG_V1))
    async with running_reporter(tmp_path, backend) as (rep, backend, opts):
        # 第一份配置到达核心侧队列。
        got1 = await asyncio.wait_for(rep.configs.get(), timeout=5)
        assert got1.version == 1
        assert got1.mode == model.MODE_ENFORCE
        assert len(got1.envs) == 1
        assert got1.envs[0].targets == [model.Target("lb-1", "fe_a")]

        # 消费掉 v1 后再补投 v2，避免 coalescing 把 v1 顶掉造成时序歧义。
        backend.config_responses.append(dict(CFG_V2))
        got2 = await asyncio.wait_for(rep.configs.get(), timeout=5)
        assert got2.version == 2
        assert got2.mode == model.MODE_DRY_RUN
        assert len(got2.envs) == 2

        # 版本水位随请求推进：0 →（应用 v1）→ 1 →（应用 v2）→ 2。
        await eventually(lambda: len(backend.config_polls) >= 3)
        assert [p[0] for p in backend.config_polls[:3]] == [NODE_ID] * 3
        assert [p[1] for p in backend.config_polls[:3]] == ["0", "1", "2"]

        # fail-static 缓存已原子落盘为最新版本（写在投递队列之前，因此
        # 消费到 v2 时磁盘上必然已是 v2），且目录中没有残留临时文件。
        cached = json.loads((tmp_path / "cache.json").read_text("utf-8"))
        assert cached["version"] == 2
        assert model.ControllerConfig.from_dict(cached).version == 2
        leftovers = [p.name for p in tmp_path.iterdir() if p.name != "cache.json"]
        assert leftovers == []


async def test_poll_backoff_on_server_error(tmp_path):
    """500 触发指数退避：两次重试之间至少间隔 jitter 下界（backoff/2），
    且不会以忙轮询打爆后台。"""
    backend = FakeBackend()
    backend.fail_config = True
    async with running_reporter(tmp_path, backend) as (rep, backend, opts):
        # 等到第 3 次请求：间隔应为 ~[0.5,1]s 与 ~[1,2]s（抖动区间）。
        await eventually(lambda: len(backend.config_polls) >= 3, timeout=10)
        t1, t2, t3 = (p[2] for p in backend.config_polls[:3])
        # 下界断言（留 20% 调度余量）：证明确实退避了而不是立即重试。
        assert t2 - t1 >= 0.4, f"first retry too fast: {t2 - t1:.3f}s"
        assert t3 - t2 >= 0.8, f"second retry did not back off: {t3 - t2:.3f}s"
        # 第二档退避大于第一档（指数增长的证据，抖动下界 1.0 > 上界前档一半）。
        assert (t3 - t2) > (t2 - t1) * 0.9
        # 短时间窗口内请求次数有限，没有打爆后台。
        assert len(backend.config_polls) <= 5


async def test_config_queue_coalescing_keeps_only_latest(tmp_path):
    """连推两版且核心不消费：队列容量 1 + 弃旧放新，只留最新版本。"""
    backend = FakeBackend()
    backend.config_responses.extend([dict(CFG_V1), dict(CFG_V2)])
    async with running_reporter(tmp_path, backend) as (rep, backend, opts):
        # 等到 reporter 已带着 version=2 发起下一轮长轮询——说明 v1、v2
        # 都已应用（落盘 + 入队）完毕，而队列从未被消费。
        await eventually(
            lambda: any(p[1] == "2" for p in backend.config_polls), timeout=5
        )
        latest = rep.configs.get_nowait()
        assert latest.version == 2  # v1 已被 v2 顶掉
        with pytest.raises(asyncio.QueueEmpty):
            rep.configs.get_nowait()


async def test_run_without_base_url_idles(tmp_path):
    """base_url 为空（standalone 防御分支）：run 只挂起等待取消，
    不发任何请求也不崩溃。"""
    rep = reporter.Reporter(
        BackendOptions(base_url=""),
        node_id=NODE_ID,
        service_version=SERVICE_VERSION,
    )
    task = asyncio.create_task(rep.run())
    await asyncio.sleep(0.2)
    assert not task.done()
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)


# ---- 指标缓冲与上报 ---------------------------------------------------------


def test_add_sample_buffer_cap_and_drop_throttle(caplog):
    """缓冲上限 600、满则弃最旧；丢弃告警只在首次（及每 100 次）出现。"""
    log = logging.getLogger("test.reporter.cap")
    rep = reporter.Reporter(
        BackendOptions(),
        node_id=NODE_ID,
        service_version=SERVICE_VERSION,
        log=log,
    )
    with caplog.at_level(logging.WARNING, logger=log.name):
        for i in range(605):
            rep.add_sample(1000 + i, [make_usage()], [make_decision()], "dry-run", 3)
    assert len(rep._samples) == 600
    # 保新弃旧：最旧的 5 个（ts 1000..1004）被丢弃。
    assert rep._samples[0]["ts"] == 1005
    assert rep._samples[-1]["ts"] == 1604
    drops = [r for r in caplog.records if "metrics buffer full" in r.getMessage()]
    assert len(drops) == 1  # 5 次丢弃事件只告警首次


async def test_flush_payload_structure(tmp_path):
    backend = FakeBackend()
    async with running_reporter(
        tmp_path, backend, flush_interval_s=0.05
    ) as (rep, backend, opts):
        rep.add_sample(
            1234.9, [make_usage()], [make_decision()], "enforce", 5
        )
        await eventually(lambda: len(backend.metrics) >= 1)
        payload = backend.metrics[0]
        # 顶层：实例身份 + 版本 + 样本批。
        assert payload["node_id"] == NODE_ID
        assert payload["service_version"] == SERVICE_VERSION
        assert len(payload["samples"]) == 1
        s = payload["samples"][0]
        assert s["ts"] == 1234  # Unix 秒（截断）
        assert s["mode"] == "enforce"
        assert s["config_version"] == 5
        # 环境切片字段逐项核对上报协议约定。
        assert s["envs"] == [
            {
                "env_id": "env-a",
                "rate_bps": 100.5,
                "mean10_bps": 90.0,
                "ewma60_bps": 80.0,
                "conn_cur": 7,
                "bwlim_bps": 25_000_000.0,
                "state": "tightening",
                "changed": True,
            }
        ]
        # 成功后缓冲清空。
        assert rep._samples == []


async def test_flush_failure_keeps_samples_then_delivers(tmp_path):
    """上报失败时样本放回缓冲（不丢在半路上），后台恢复后原样送达。"""
    backend = FakeBackend()
    backend.metrics_status = 500
    async with running_reporter(
        tmp_path, backend, flush_interval_s=0.05
    ) as (rep, backend, opts):
        rep.add_sample(2000, [make_usage()], [make_decision()], "dry-run", 1)
        # 至少失败一次，且样本仍留在缓冲中。
        await eventually(lambda: len(backend.metrics) >= 1)
        await eventually(lambda: len(rep._samples) == 1)
        # 后台恢复 → 同一样本最终送达，缓冲清空。
        backend.metrics_status = 200
        await eventually(lambda: len(rep._samples) == 0)
        delivered = backend.metrics[-1]["samples"]
        assert [s["ts"] for s in delivered] == [2000]


async def test_sample_without_decision_uses_zero_values(tmp_path):
    """无对应决策的环境样本各字段取零值（0 / "" / False）。"""
    rep = reporter.Reporter(
        BackendOptions(), node_id=NODE_ID, service_version=SERVICE_VERSION
    )
    rep.add_sample(100, [make_usage("env-x")], [], "dry-run", 0)
    env = rep._samples[0]["envs"][0]
    assert env["bwlim_bps"] == 0.0
    assert env["state"] == ""
    assert env["changed"] is False


# ---- 心跳 -------------------------------------------------------------------


async def test_heartbeat_payload(tmp_path):
    """心跳启动即发一次；mode/config_version 动态取自 mode_fn/version_fn
    （核心实际已应用的状态，而非 Reporter 自己收到过什么）。"""
    backend = FakeBackend()
    async with running_reporter(
        tmp_path,
        backend,
        mode_fn=lambda: model.MODE_ENFORCE,
        version_fn=lambda: 7,
        heartbeat_interval_s=3600.0,  # 只观察启动即发的第一跳
    ) as (rep, backend, opts):
        await eventually(lambda: len(backend.heartbeats) >= 1)
        assert backend.heartbeats[0] == {
            "node_id": NODE_ID,
            "service_version": SERVICE_VERSION,
            "mode": model.MODE_ENFORCE,
            "config_version": 7,
        }


# ---- fail-static 缓存与 TLS 降级 --------------------------------------------


async def test_load_cache_missing_file(tmp_path):
    with pytest.raises(FileNotFoundError):
        reporter.load_cache(str(tmp_path / "no-such-cache.json"))


async def test_load_cache_roundtrip_and_normalize(tmp_path):
    """load_cache 读回 + normalize：非法 mode 归一为 dry-run（安全方向）。"""
    p = tmp_path / "cache.json"
    doc = dict(CFG_V1)
    doc["mode"] = "bogus-mode"  # 缓存被人为改坏 mode 也不能放开限速
    p.write_text(json.dumps(doc), encoding="utf-8")
    cfg = reporter.load_cache(str(p))
    assert cfg.version == 1
    assert cfg.mode == model.MODE_DRY_RUN
    assert len(cfg.envs) == 1
    assert cfg.envs[0].quota_bits_per_sec == 200_000_000


def test_tls_config_failure_degrades_to_default(tmp_path, caplog):
    """TLS 材料配置了却加载失败：error 告警后降级默认客户端，构造不失败
    ——服务必须能先启动进入 fail-static，证书问题留给运维修复。"""
    log = logging.getLogger("test.reporter.tls")
    with caplog.at_level(logging.ERROR, logger=log.name):
        rep = reporter.Reporter(
            BackendOptions(
                base_url="https://backend:9090",
                ca_file=str(tmp_path / "missing-ca.pem"),
            ),
            node_id=NODE_ID,
            service_version=SERVICE_VERSION,
            log=log,
        )
    assert rep._ssl is None
    assert any("tls client config failed" in r.getMessage() for r in caplog.records)


def test_tls_cert_without_key_degrades(tmp_path, caplog):
    """客户端证书与私钥必须成对：只给一半按加载失败处理（降级 + error）。"""
    cert = tmp_path / "cert.pem"
    cert.write_text("not-a-real-cert", encoding="utf-8")
    log = logging.getLogger("test.reporter.tls2")
    with caplog.at_level(logging.ERROR, logger=log.name):
        rep = reporter.Reporter(
            BackendOptions(base_url="https://backend:9090", cert_file=str(cert)),
            node_id=NODE_ID,
            service_version=SERVICE_VERSION,
            log=log,
        )
    assert rep._ssl is None
