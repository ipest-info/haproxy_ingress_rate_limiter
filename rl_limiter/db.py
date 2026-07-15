# rl_limiter.db —— MySQL 配置源与用量落库（v3.0：配置从文件改为数据库）。
#
# 背景（重大调整）：配置不再来自本地 YAML / HTTP 后台，而是集中存放在
# MySQL——数据库成为"配额权威"（设计文档 §3.5 数据模型的落地）。rl-limiter
# 启动时只从环境变量读取少量引导信息（数据库连接串、本实例 node_id、
# tick 间隔），其余全部从库里读：
#   - haproxy_nodes 表 → 受控 HAProxy 节点接线（host/port/map 路径/超时）；
#   - envs + env_targets + settings 表 → 环境配额、挂载点、运行模式；
#   - 配置版本 = 上述表 updated_at 的最大值（秒），任一行改动即自增，
#     rl-limiter 每隔 poll_interval_s 轮询一次，版本变化即热重载，改库
#     秒级生效、无需重启（替代原 HTTP 后台的长轮询）。
# 同时把每秒用量样本与心跳写回库（usage_samples / heartbeats 表），直接
# 支撑"用量数据入库供后台展示"（设计文档 §3.8）。
#
# 与事件循环的关系：PyMySQL 是同步驱动，所有 DB 调用都用 asyncio.to_thread
# 包一层放到线程池执行，避免阻塞 1 秒快环的事件循环；每次操作开一条短连接
# （poll/flush 都是 5 秒级、查询很轻），省去长连接保活的复杂度。
#
# 可测试性：pymysql 采用惰性导入（模块级不 import，真正连库时才导），且
# Database 接受可注入的 connect 工厂——单元测试传入假连接（返回预置行），
# 无需真实 MySQL 即可覆盖"SQL 取数 → 领域对象"的解析逻辑。

from __future__ import annotations

import asyncio
import json
import logging
import os
from dataclasses import dataclass
from typing import Any, Callable

from . import model

log = logging.getLogger(__name__)


@dataclass(slots=True)
class DbConfig:
    """MySQL 连接与轮询参数（引导信息，来自环境变量）。"""

    host: str = "127.0.0.1"
    port: int = 3306
    user: str = "rl"
    password: str = ""
    database: str = "rl_limiter"
    connect_timeout_s: float = 5.0
    poll_interval_s: float = 5.0  # 配置版本轮询间隔（秒）

    @classmethod
    def from_env(cls, env: dict[str, str] | None = None) -> "DbConfig":
        """从环境变量构造。docker compose 下所有连接信息都走环境变量，
        符合 12-factor；缺省值面向本地/compose 默认部署。"""
        e = env if env is not None else os.environ
        return cls(
            host=e.get("RL_MYSQL_HOST", "127.0.0.1"),
            port=int(e.get("RL_MYSQL_PORT", "3306")),
            user=e.get("RL_MYSQL_USER", "rl"),
            password=e.get("RL_MYSQL_PASSWORD", ""),
            database=e.get("RL_MYSQL_DB", "rl_limiter"),
            connect_timeout_s=float(e.get("RL_MYSQL_CONNECT_TIMEOUT_S", "5")),
            poll_interval_s=float(e.get("RL_MYSQL_POLL_INTERVAL_S", "5")),
        )


# ---- 纯解析函数（不碰数据库，便于单元测试）--------------------------------


def rows_to_nodes(rows: list[dict[str, Any]]) -> list[model.NodeConfig]:
    """haproxy_nodes 表行 → NodeConfig 列表。timeout_ms（运维口径，毫秒）
    在此一次性换算为内部口径的秒。"""
    nodes: list[model.NodeConfig] = []
    for r in rows:
        nodes.append(model.NodeConfig(
            name=str(r["name"]),
            host=str(r["host"]),
            port=int(r["port"]),
            bwlim_map_path=str(r.get("bwlim_map_path") or "/etc/haproxy/maps/bwlim.map"),
            timeout_s=float(r.get("timeout_ms") or 500) / 1000.0,
        ))
    return nodes


def rows_to_controller_config(
    mode: str,
    version: int,
    env_rows: list[dict[str, Any]],
    target_rows: list[dict[str, Any]],
    report_interval_s: int = 5,
    heartbeat_interval_s: int = 10,
) -> model.ControllerConfig:
    """envs + env_targets + 模式 + 版本 → ControllerConfig（经 normalize）。

    quota_mbps 是人类可读的 Mbps（配置口径），进入算法前由 EnvQuota
    在内部换算为 bytes/s。params_json 为空表示该环境用默认快环参数。
    """
    # 先把挂载点按 env_id 归拢。
    targets_by_env: dict[str, list[model.Target]] = {}
    for t in target_rows:
        targets_by_env.setdefault(str(t["env_id"]), []).append(
            model.Target(str(t["node"]), str(t["frontend"]))
        )

    envs: list[model.EnvQuota] = []
    for r in env_rows:
        env_id = str(r["env_id"])
        params = None
        raw = r.get("params_json")
        if raw:
            # JSON 列在不同驱动下可能已是 dict，也可能是字符串，两者都容忍。
            d = raw if isinstance(raw, dict) else json.loads(raw)
            params = model.GovParams.from_dict(d)
        envs.append(model.EnvQuota(
            env_id=env_id,
            quota_mbps=float(r["quota_mbps"]),
            targets=targets_by_env.get(env_id, []),
            params=params,
        ))

    cfg = model.ControllerConfig(
        version=int(version),
        mode=mode,
        envs=envs,
        report_interval_s=report_interval_s,
        heartbeat_interval_s=heartbeat_interval_s,
    )
    cfg.normalize()
    return cfg


# ---- 数据库访问层 ---------------------------------------------------------


def _default_connect(cfg: DbConfig):
    """真实连接工厂：惰性导入 pymysql（模块导入期不依赖它，便于无库测试）。
    使用 DictCursor 让查询结果为 dict，与纯解析函数对接。"""
    import pymysql  # noqa: PLC0415  惰性导入
    from pymysql.cursors import DictCursor

    return pymysql.connect(
        host=cfg.host,
        port=cfg.port,
        user=cfg.user,
        password=cfg.password,
        database=cfg.database,
        connect_timeout=cfg.connect_timeout_s,
        cursorclass=DictCursor,
        autocommit=True,
        charset="utf8mb4",
    )


# 计算配置版本：取三张配置表 updated_at 的最大值（秒）。任一行新增/修改
# 都会推高该值，rl-limiter 据此判断"配置变了"。空库返回 0。
_VERSION_SQL = (
    "SELECT COALESCE(UNIX_TIMESTAMP(MAX(u)), 0) AS v FROM ("
    "  SELECT MAX(updated_at) AS u FROM envs"
    "  UNION ALL SELECT MAX(updated_at) FROM env_targets"
    "  UNION ALL SELECT MAX(updated_at) FROM settings"
    ") t"
)


class Database:
    """MySQL 访问层。所有公开方法均为 async——同步 PyMySQL 操作经
    asyncio.to_thread 派发到线程池，绝不阻塞事件循环。

    connect: 可注入的连接工厂（默认 _default_connect）；测试传入假工厂
    即可在无 MySQL 的情况下验证取数与解析。
    """

    def __init__(
        self,
        cfg: DbConfig,
        connect: Callable[[DbConfig], Any] | None = None,
        log_: logging.Logger | None = None,
    ) -> None:
        self._cfg = cfg
        self._connect = connect if connect is not None else _default_connect
        self._log = log_ if log_ is not None else log

    # ---- 同步实现（在线程池中运行）------------------------------------

    def _run(self, fn: Callable[[Any], Any]) -> Any:
        """开一条短连接执行 fn(conn) 后关闭，保证连接不泄漏。"""
        conn = self._connect(self._cfg)
        try:
            return fn(conn)
        finally:
            try:
                conn.close()
            except Exception:  # pragma: no cover - 关闭失败无关紧要
                pass

    def _fetch_config_sync(self, conn: Any) -> model.ControllerConfig:
        with conn.cursor() as cur:
            cur.execute("SELECT v FROM settings WHERE k = 'mode'")
            row = cur.fetchone()
            mode = str(row["v"]) if row else model.MODE_DRY_RUN
            cur.execute(_VERSION_SQL)
            version = int(cur.fetchone()["v"])
            cur.execute("SELECT env_id, quota_mbps, params_json FROM envs")
            env_rows = list(cur.fetchall())
            cur.execute("SELECT env_id, node, frontend FROM env_targets")
            target_rows = list(cur.fetchall())
        return rows_to_controller_config(mode, version, env_rows, target_rows)

    def _fetch_version_sync(self, conn: Any) -> int:
        with conn.cursor() as cur:
            cur.execute(_VERSION_SQL)
            return int(cur.fetchone()["v"])

    def _fetch_nodes_sync(self, conn: Any) -> list[model.NodeConfig]:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT name, host, port, bwlim_map_path, timeout_ms FROM haproxy_nodes"
            )
            return rows_to_nodes(list(cur.fetchall()))

    def _write_samples_sync(self, conn: Any, samples: list[dict[str, Any]]) -> None:
        if not samples:
            return
        with conn.cursor() as cur:
            cur.executemany(
                "INSERT INTO usage_samples "
                "(ts, node_id, env_id, rate_mbps, mean10_mbps, ewma60_mbps, "
                " conn_cur, bwlim_mbps, state, changed) "
                "VALUES (%(ts)s, %(node_id)s, %(env_id)s, %(rate_mbps)s, "
                "%(mean10_mbps)s, %(ewma60_mbps)s, %(conn_cur)s, %(bwlim_mbps)s, "
                "%(state)s, %(changed)s)",
                samples,
            )

    def _write_heartbeat_sync(
        self, conn: Any, node_id: str, service_version: str,
        mode: str, config_version: int,
    ) -> None:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO heartbeats "
                "(node_id, service_version, mode, config_version) "
                "VALUES (%s, %s, %s, %s) "
                "ON DUPLICATE KEY UPDATE service_version=VALUES(service_version), "
                "mode=VALUES(mode), config_version=VALUES(config_version)",
                (node_id, service_version, mode, config_version),
            )

    # ---- async 包装 ----------------------------------------------------

    async def ping(self) -> None:
        """探活：连上并执行 SELECT 1。启动时用它等待 MySQL 就绪。"""
        def _do(conn: Any) -> None:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                cur.fetchone()
        await asyncio.to_thread(self._run, _do)

    async def fetch_config(self) -> model.ControllerConfig:
        return await asyncio.to_thread(self._run, self._fetch_config_sync)

    async def fetch_version(self) -> int:
        return await asyncio.to_thread(self._run, self._fetch_version_sync)

    async def fetch_nodes(self) -> list[model.NodeConfig]:
        return await asyncio.to_thread(self._run, self._fetch_nodes_sync)

    async def write_samples(self, samples: list[dict[str, Any]]) -> None:
        await asyncio.to_thread(self._run, lambda c: self._write_samples_sync(c, samples))

    async def write_heartbeat(
        self, node_id: str, service_version: str, mode: str, config_version: int,
    ) -> None:
        await asyncio.to_thread(
            self._run,
            lambda c: self._write_heartbeat_sync(
                c, node_id, service_version, mode, config_version),
        )

    async def wait_ready(self, attempts: int = 30, delay_s: float = 2.0) -> None:
        """启动引导：反复 ping 直到 MySQL 就绪（compose 里 DB 与服务同时
        拉起，服务往往先跑起来）。超过 attempts 次仍失败则抛出最后一次
        异常，交由上层决定退出（systemd/compose 会重启）。"""
        last: Exception | None = None
        for i in range(1, attempts + 1):
            try:
                await self.ping()
                self._log.info(
                    "MySQL 连接就绪 host=%s port=%d db=%s attempt=%d",
                    self._cfg.host, self._cfg.port, self._cfg.database, i)
                return
            except Exception as e:  # noqa: BLE001 探活期各类连接错误都重试
                last = e
                self._log.warning(
                    "等待 MySQL 就绪中，稍后重试 host=%s port=%d attempt=%d/%d err=%s",
                    self._cfg.host, self._cfg.port, i, attempts, e)
                await asyncio.sleep(delay_s)
        assert last is not None
        raise last


# 指标缓冲上限：约 10 分钟（假设每 5s flush、每拍一批）。满则弃最旧。
MAX_BUFFERED_SAMPLES = 600
_DROP_LOG_EVERY = 100


class DbBackend:
    """以 MySQL 为配置源与用量汇的后台适配器，接口与 reporter.Reporter
    对齐（.configs 队列 + add_sample + async run），可在 __main__ 中作为
    HTTP 后台的替代直接接入核心循环。

    三条常驻协程（在 run() 内并发）：
      - 配置轮询：每 poll_interval_s 查一次配置版本，版本变化则拉取全量
        配置推入 configs 队列（合并语义），实现改库热重载；
      - 指标落库：每 flush_interval_s 把缓冲的每秒样本批量 INSERT 进
        usage_samples；失败保留样本待下轮补送；
      - 心跳：每 heartbeat_interval_s upsert 一次 heartbeats（本实例存活
        与当前模式/配置版本，供后台观测各限速实例）。
    """

    def __init__(
        self,
        db: Database,
        node_id: str,
        service_version: str,
        mode_fn: Callable[[], str],
        version_fn: Callable[[], int],
        poll_interval_s: float = 5.0,
        flush_interval_s: float = 5.0,
        heartbeat_interval_s: float = 10.0,
        log_: logging.Logger | None = None,
    ) -> None:
        self._db = db
        self._node_id = node_id
        self._service_version = service_version
        self._mode_fn = mode_fn
        self._version_fn = version_fn
        self._poll_interval = poll_interval_s
        self._flush_interval = flush_interval_s
        self._heartbeat_interval = heartbeat_interval_s
        self._log = log_ if log_ is not None else log

        self._configs: "asyncio.Queue[model.ControllerConfig]" = asyncio.Queue(1)
        self._version = 0  # 已推送给核心循环的最新配置版本
        self._samples: list[dict[str, Any]] = []
        self._drop_events = 0
        self._dropped_total = 0

    @property
    def configs(self) -> "asyncio.Queue[model.ControllerConfig]":
        """新配置的投递队列（容量 1、合并语义）。"""
        return self._configs

    def set_initial_version(self, version: int) -> None:
        """记录首启引导时已应用的配置版本，避免轮询把同一版本再推一遍。"""
        self._version = version

    def add_sample(
        self,
        now: float,
        usages: "list[model.EnvUsage]",
        decisions: "list[model.Decision]",
        mode: str,
        config_version: int,
    ) -> None:
        """把一次 tick 的用量记入有界缓冲（纯内存、非阻塞）。带宽字段
        在此换算为 Mbps 落库（人类可读，后台展示直接可用）。缓冲满则
        保新弃旧，丢弃告警按"首次 + 每 _DROP_LOG_EVERY 次"节流。"""
        by_env = {d.env_id: d for d in decisions}
        for u in usages:
            d = by_env.get(u.env_id)
            row = {
                "ts": now,
                "node_id": self._node_id,
                "env_id": u.env_id,
                "rate_mbps": round(model.to_mbps(u.rate_bps), 3),
                "mean10_mbps": round(model.to_mbps(u.mean10_bps), 3),
                "ewma60_mbps": round(model.to_mbps(u.ewma60_bps), 3),
                "conn_cur": u.conn_cur,
                "bwlim_mbps": round(model.to_mbps(d.bwlim_bps), 3) if d is not None else 0.0,
                "state": str(d.state) if d is not None else "",
                "changed": 1 if (d is not None and d.changed) else 0,
            }
            if len(self._samples) >= MAX_BUFFERED_SAMPLES:
                self._samples.pop(0)
                self._dropped_total += 1
                self._drop_events += 1
                if self._drop_events == 1 or self._drop_events % _DROP_LOG_EVERY == 0:
                    self._log.warning(
                        "指标缓冲区已满，丢弃最旧样本（MySQL 长时间不可写？） "
                        "dropped_now=%d dropped_total=%d buffer_cap=%d",
                        self._drop_events, self._dropped_total, MAX_BUFFERED_SAMPLES)
            self._samples.append(row)

    async def run(self) -> None:
        """并发跑配置轮询、指标落库、心跳三条协程，直到被取消。"""
        await asyncio.gather(
            self._poll_loop(),
            self._flush_loop(),
            self._heartbeat_loop(),
        )

    async def _poll_loop(self) -> None:
        while True:
            await asyncio.sleep(self._poll_interval)
            try:
                version = await self._db.fetch_version()
                if version == self._version:
                    continue  # 配置未变，无需拉取
                cfg = await self._db.fetch_config()
                self._version = cfg.version
                self._push_config(cfg)
                self._log.info(
                    "检测到数据库配置版本变化，已拉取新配置下发核心循环 "
                    "version_new=%d mode=%s envs=%d",
                    cfg.version, cfg.mode, len(cfg.envs))
            except Exception as e:  # noqa: BLE001 轮询失败不致命，下轮再试
                self._log.warning("配置轮询失败，保持当前配置，下轮重试 err=%s", e)

    def _push_config(self, cfg: model.ControllerConfig) -> None:
        """把新配置推入容量 1 的队列：合并语义——若上一份还没被核心
        循环取走，就丢弃旧的换成最新的（全量下发，中间版本无单独应用
        价值）。"""
        try:
            self._configs.put_nowait(cfg)
        except asyncio.QueueFull:
            try:
                self._configs.get_nowait()
            except asyncio.QueueEmpty:
                pass
            self._configs.put_nowait(cfg)

    async def _flush_loop(self) -> None:
        while True:
            await asyncio.sleep(self._flush_interval)
            if not self._samples:
                continue
            batch = self._samples
            self._samples = []
            try:
                await self._db.write_samples(batch)
                self._log.debug("用量样本已批量写入 MySQL samples=%d", len(batch))
            except Exception as e:  # noqa: BLE001 写失败则退回缓冲待下轮补送
                # 退回队首，保持时间顺序；仍受缓冲上限约束（下次 add_sample
                # 触发保新弃旧）。
                self._samples[0:0] = batch
                self._log.warning(
                    "用量样本写入 MySQL 失败，退回缓冲待下轮补送 buffered=%d err=%s",
                    len(self._samples), e)

    async def _heartbeat_loop(self) -> None:
        failures = 0
        while True:
            await asyncio.sleep(self._heartbeat_interval)
            try:
                await self._db.write_heartbeat(
                    self._node_id, self._service_version,
                    self._mode_fn(), self._version_fn())
                failures = 0
            except Exception as e:  # noqa: BLE001
                failures += 1
                self._log.warning("心跳写入 MySQL 失败 consecutive_failures=%d err=%s",
                                  failures, e)
