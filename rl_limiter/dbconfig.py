# rl_limiter.dbconfig —— MySQL 配置源：服务配置从数据库读取并轮询热更新。
#
# 定位：替代本地 YAML 文件成为服务配置的权威来源。四张表对应 YAML 的
# 四块内容（建表与种子数据见 deploy/mysql/init.sql）：
#
#   service_config   单行表：node_id / mode / log_level / tick_interval_s
#   haproxy_nodes    受控 HAProxy 节点清单（name/host/port/map 路径/超时）
#   envs             环境配额（env_id / quota_bps / 可选 params_json）
#   env_targets      环境挂载点（env_id × node × frontend）
#
# 复用而不是重写：数据库行先被组装成与 yaml.safe_load 结果**同构**的原始
# dict（rows_to_raw），再走 config.from_raw 的既有解析/校验管线——校验
# 规则只维护一份，两种配置来源的拒绝行为与错误信息完全一致。
#
# 热更新模型（watch）：
#   - 每 poll_interval_s 拉取一次全量配置（四条 SELECT 包在同一个事务里，
#     InnoDB REPEATABLE READ 保证读到的是同一时刻的一致快照）；
#   - 以配置内容的 CRC32 校验和为"版本号"：内容变了校验和必变，直接把
#     组装好的 ControllerConfig 投入主循环的配置队列热生效（mode 与 envs
#     可热更；haproxy_nodes 变更涉及重建 TCP 客户端，记 warning 提示重启）；
#   - 数据库故障或新配置校验不通过时：保留当前配置继续限速、只记日志，
#     与管理后台断联的 fail-static 行为（设计 §3.7）保持同一精神。
#
# 连接凭据通过 RL_MYSQL_* 环境变量注入（见 from_env），不落任何文件。

from __future__ import annotations

import asyncio
import json
import logging
import os
import zlib
from dataclasses import dataclass
from typing import Any, Mapping, Sequence

from . import config as configmod
from . import model

# 环境变量名 → MySQLOptions 字段的对照（文档口径，也是 from_env 的实现依据）。
ENV_HOST = "RL_MYSQL_HOST"          # 必填：设置了它才启用数据库配置模式
ENV_PORT = "RL_MYSQL_PORT"          # 默认 3306
ENV_USER = "RL_MYSQL_USER"          # 默认 rl
ENV_PASSWORD = "RL_MYSQL_PASSWORD"  # 默认空
ENV_DB = "RL_MYSQL_DB"              # 默认 rl_limiter
ENV_POLL_S = "RL_MYSQL_POLL_S"      # 配置轮询周期（秒），默认 5

DEFAULT_PORT = 3306
DEFAULT_USER = "rl"
DEFAULT_DB = "rl_limiter"
DEFAULT_POLL_INTERVAL_S = 5.0
# 单次连接建立的超时；轮询周期内完不成会被当作本轮失败，下轮重试。
DEFAULT_CONNECT_TIMEOUT_S = 10.0
# 启动阶段允许等待数据库就绪的总时长：docker compose 里 MySQL 首次初始化
# （建库+灌 init.sql）可能需要几十秒，rl-limiter 不应比它先放弃。
STARTUP_RETRY_FOR_S = 120.0
STARTUP_RETRY_INTERVAL_S = 2.0

# 四条快照查询。ORDER BY 让行序稳定，保证同一份数据算出的校验和一致。
_SQL_SERVICE = (
    "SELECT node_id, mode, log_level, tick_interval_s "
    "FROM service_config WHERE id = 1"
)
_SQL_NODES = (
    "SELECT name, host, port, bwlim_map_path, timeout_ms "
    "FROM haproxy_nodes ORDER BY name"
)
_SQL_ENVS = "SELECT env_id, quota_bps, params_json FROM envs ORDER BY env_id"
_SQL_TARGETS = (
    "SELECT env_id, node, frontend FROM env_targets "
    "ORDER BY env_id, node, frontend"
)


@dataclass(slots=True)
class MySQLOptions:
    """MySQL 配置源的接入参数（全部来自环境变量，见 from_env）。"""

    host: str
    port: int = DEFAULT_PORT
    user: str = DEFAULT_USER
    password: str = ""
    database: str = DEFAULT_DB
    poll_interval_s: float = DEFAULT_POLL_INTERVAL_S
    connect_timeout_s: float = DEFAULT_CONNECT_TIMEOUT_S

    def describe(self) -> str:
        """人类可读的来源描述（不含口令），拼进日志与校验错误信息。"""
        return f"MySQL 数据库 {self.user}@{self.host}:{self.port}/{self.database}"


def from_env(env: Mapping[str, str] | None = None) -> MySQLOptions | None:
    """从环境变量读取 MySQL 接入参数。

    未设置 RL_MYSQL_HOST 时返回 None——表示未启用数据库配置模式，调用方
    回落到 YAML 文件；设置了 host 但 port/poll 写错（非数字、非正数）则
    直接抛 ValueError：半吊子的数据库配置比没有更危险，必须在启动时拦下。
    """
    e = os.environ if env is None else env
    host = (e.get(ENV_HOST) or "").strip()
    if not host:
        return None

    def _int(name: str, default: int) -> int:
        raw = (e.get(name) or "").strip()
        if not raw:
            return default
        try:
            return int(raw)
        except ValueError:
            raise ValueError(f"环境变量 {name} 必须是整数，当前值 {raw!r}") from None

    def _float(name: str, default: float) -> float:
        raw = (e.get(name) or "").strip()
        if not raw:
            return default
        try:
            return float(raw)
        except ValueError:
            raise ValueError(f"环境变量 {name} 必须是数字，当前值 {raw!r}") from None

    opts = MySQLOptions(
        host=host,
        port=_int(ENV_PORT, DEFAULT_PORT),
        user=(e.get(ENV_USER) or "").strip() or DEFAULT_USER,
        password=e.get(ENV_PASSWORD) or "",
        database=(e.get(ENV_DB) or "").strip() or DEFAULT_DB,
        poll_interval_s=_float(ENV_POLL_S, DEFAULT_POLL_INTERVAL_S),
    )
    if opts.port < 1 or opts.port > 65535:
        raise ValueError(f"环境变量 {ENV_PORT} 必须在 1-65535 范围内，当前值 {opts.port}")
    if opts.poll_interval_s <= 0:
        raise ValueError(
            f"环境变量 {ENV_POLL_S} 必须 > 0（秒），当前值 {opts.poll_interval_s}"
        )
    return opts


def rows_to_raw(
    service_row: Sequence[Any] | None,
    node_rows: Sequence[Sequence[Any]],
    env_rows: Sequence[Sequence[Any]],
    target_rows: Sequence[Sequence[Any]],
) -> dict[str, Any]:
    """把四张表的行组装成与 yaml.safe_load 结果同构的原始 dict。

    纯函数（不碰数据库），是数据库模式复用 config.from_raw 校验管线的
    衔接点。刻意不在这里做任何业务校验：类型/取值/引用完整性全部交给
    config._validate，保证两种配置来源的拒绝行为一字不差。

    service_row 为 None 表示 service_config 表没有 id=1 的行——组装出的
    dict 缺 node_id，后续校验会以"node_id 不能为空"报出，比在这里另造
    一条错误信息更一致。

    params_json 是 envs 表里可选的 JSON 文本列（per-env 快环参数覆盖）；
    非法 JSON 在这里就地报错并带上 env_id，因为 config 层拿到的已是解析
    后的 dict，无从知道原始文本长什么样。
    """
    raw: dict[str, Any] = {}
    if service_row is not None:
        node_id, mode, log_level, tick = service_row
        raw["node_id"] = "" if node_id is None else str(node_id)
        if mode:
            raw["mode"] = str(mode)
        if log_level:
            raw["log_level"] = str(log_level)
        if tick is not None:
            raw["tick_interval_s"] = tick

    raw["haproxy_nodes"] = [
        {
            "name": name,
            "host": host,
            "port": port,
            "bwlim_map_path": bwlim_map_path or "",
            "timeout_ms": timeout_ms or 0,
        }
        for name, host, port, bwlim_map_path, timeout_ms in node_rows
    ]

    targets_by_env: dict[str, list[dict[str, Any]]] = {}
    for env_id, node, frontend in target_rows:
        targets_by_env.setdefault(str(env_id), []).append(
            {"node": node, "frontend": frontend}
        )

    envs: list[dict[str, Any]] = []
    for env_id, quota_bps, params_json in env_rows:
        env: dict[str, Any] = {
            "env_id": env_id,
            "quota_bps": quota_bps,
            "targets": targets_by_env.get(str(env_id), []),
        }
        if params_json:
            try:
                params = json.loads(params_json)
            except ValueError as e:
                raise ValueError(
                    f"envs 表中环境 {env_id!r} 的 params_json 不是合法的 JSON: {e}"
                ) from None
            if not isinstance(params, dict):
                raise ValueError(
                    f"envs 表中环境 {env_id!r} 的 params_json 必须是 JSON 对象，"
                    f"当前为 {type(params).__name__}"
                )
            env["params"] = params
        envs.append(env)
    raw["envs"] = envs
    return raw


def config_checksum(mode: str, envs: list[model.EnvQuota]) -> int:
    """计算可热更新部分（mode + envs）的内容校验和，充当配置版本号。

    数据库没有现成的单调版本号可用（要求运维每次改配置手动 bump 版本，
    既繁琐又容易忘），因此用内容指纹代替：canonical JSON（键排序、紧凑
    分隔符）的 CRC32。内容不变则校验和恒定（envs 查询带 ORDER BY 保证
    行序稳定），内容一变校验和必变——ControlLoop 只拿它做相等比较与
    日志展示，不要求单调递增。
    """
    payload = {"mode": mode, "envs": [e.to_dict() for e in envs]}
    canonical = json.dumps(
        payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    )
    return zlib.crc32(canonical.encode("utf-8"))


async def _fetch_raw(opts: MySQLOptions) -> dict[str, Any]:
    """连接数据库，在单个事务里拉取四张表的一致快照并组装为原始 dict。

    aiomysql 在函数内延迟导入：只有启用数据库配置模式才需要该依赖，
    纯 YAML 部署与单元测试不必安装。
    """
    import aiomysql  # 延迟导入，见 docstring

    conn = await aiomysql.connect(
        host=opts.host,
        port=opts.port,
        user=opts.user,
        password=opts.password,
        db=opts.database,
        connect_timeout=opts.connect_timeout_s,
        charset="utf8mb4",
        autocommit=False,
    )
    try:
        async with conn.cursor() as cur:
            # 显式事务包住四条 SELECT：InnoDB 默认 REPEATABLE READ 下，
            # 事务内读到的是同一时刻的快照，避免"改到一半的配置"（比如
            # 先插了 env 还没插 target）被拼成半新半旧的组合。
            await conn.begin()
            await cur.execute(_SQL_SERVICE)
            service_row = await cur.fetchone()
            await cur.execute(_SQL_NODES)
            node_rows = await cur.fetchall()
            await cur.execute(_SQL_ENVS)
            env_rows = await cur.fetchall()
            await cur.execute(_SQL_TARGETS)
            target_rows = await cur.fetchall()
            await conn.commit()
    finally:
        conn.close()
    return rows_to_raw(service_row, node_rows, env_rows, target_rows)


async def fetch_service_config(opts: MySQLOptions) -> configmod.ServiceConfig:
    """拉取一次全量配置并走统一校验管线，返回 ServiceConfig。"""
    raw = await _fetch_raw(opts)
    return configmod.from_raw(raw, source=opts.describe())


async def load_service_config(
    opts: MySQLOptions,
    log: logging.Logger,
    retry_for_s: float = STARTUP_RETRY_FOR_S,
    retry_interval_s: float = STARTUP_RETRY_INTERVAL_S,
) -> configmod.ServiceConfig:
    """启动阶段的配置加载：数据库暂不可达时在 retry_for_s 内周期重试。

    为什么要重试而不是立刻失败：docker compose / 服务器重启场景下
    rl-limiter 与 MySQL 几乎同时拉起，MySQL 首次初始化（建库、执行
    init.sql）需要几十秒；把"等待依赖就绪"做进服务比要求编排层精确
    串行更皮实。注意只重试**连接类**失败——配置内容校验失败（ValueError）
    立即上抛：表里的数据写错了，重试一万次也不会自己变对。
    """
    loop = asyncio.get_running_loop()
    deadline = loop.time() + retry_for_s
    attempt = 0
    while True:
        attempt += 1
        try:
            return await fetch_service_config(opts)
        except ValueError:
            raise  # 配置内容错误：重试无意义，带着 from_raw 的中文诊断直接失败
        except Exception as e:
            remaining = deadline - loop.time()
            if remaining <= 0:
                raise RuntimeError(
                    f"{opts.describe()} 在 {retry_for_s:.0f}s 内始终不可用，"
                    f"放弃启动（共尝试 {attempt} 次，最后错误：{e}）"
                ) from e
            log.warning(
                "数据库暂不可用，等待后重试（MySQL 可能仍在初始化） "
                "attempt=%d remaining_s=%.0f err=%s",
                attempt, remaining, e)
            await asyncio.sleep(min(retry_interval_s, remaining))


async def watch(
    opts: MySQLOptions,
    queue: "asyncio.Queue[model.ControllerConfig]",
    initial_version: int,
    log: logging.Logger,
    initial_nodes: list[model.NodeConfig] | None = None,
) -> None:
    """常驻轮询任务：发现配置内容变化就把新 ControllerConfig 投入 queue。

    - 版本判定用 config_checksum（内容指纹），initial_version 是启动引导
      配置的校验和——首轮轮询读到同样内容时不会重复触发一次空应用；
    - 只热更 mode 与 envs（ControllerConfig 的能力边界，与管理后台下发
      一致）；haproxy_nodes 属于基础设施接线，进程内的 TCP 客户端在启动
      时构建，检测到节点表变化时记 warning 提示需要重启生效；
    - 任何失败（连接断、校验不过）都保留当前配置继续限速（fail-static），
      下一轮再试。
    """
    last_version = initial_version
    # 节点接线的变更检测基准：比较 (name, host, port, map, timeout) 五元组。
    def _nodes_key(nodes: list[model.NodeConfig]) -> tuple:
        return tuple(
            (n.name, n.host, n.port, n.bwlim_map_path, n.timeout_s)
            for n in nodes
        )

    last_nodes_key = _nodes_key(initial_nodes) if initial_nodes is not None else None
    while True:
        await asyncio.sleep(opts.poll_interval_s)
        try:
            cfg = await fetch_service_config(opts)
        except Exception as e:
            log.warning(
                "轮询数据库配置失败，保留当前配置继续限速（fail-static），下一轮再试 "
                "poll_interval_s=%s err=%s", opts.poll_interval_s, e)
            continue

        nodes_key = _nodes_key(cfg.nodes)
        if last_nodes_key is not None and nodes_key != last_nodes_key:
            log.warning(
                "检测到 haproxy_nodes 表发生变化：节点接线在进程启动时构建，"
                "热更新不生效，请重启 rl-limiter 使其生效 nodes=%d", len(cfg.nodes))
            last_nodes_key = nodes_key  # 只在变化那一轮告警一次，避免每轮刷屏

        version = config_checksum(cfg.mode, cfg.envs)
        if version == last_version:
            continue
        last_version = version
        ctl = model.ControllerConfig(version=version, mode=cfg.mode, envs=cfg.envs)
        queue.put_nowait(ctl)
        log.info(
            "检测到数据库配置变化，已提交主循环热生效 "
            "version=%s mode=%s envs=%d", version, cfg.mode, len(cfg.envs))
