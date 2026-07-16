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
import math
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
# 单次全量拉取（建连 + 四条 SELECT + 提交）的端到端超时。必须存在的原因：
# aiomysql 的 connect_timeout 只覆盖建连阶段，查询读结果没有任何超时——
# 已建立的连接遇到静默丢包的网络分区会阻塞到内核 TCP 重传上限（几十分钟
# 起），期间轮询循环整个卡死且 fail-static 告警一条都发不出来。用整体
# 超时把这类停摆转化为可观测、可重试的普通失败。
FETCH_TIMEOUT_S = 15.0
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
    # isfinite 与 <=0 缺一不可：float("nan") 与任何数比较都是 False、
    # float("inf") > 0，两者都能溜过纯 <=0 判断，而 asyncio.sleep(nan/inf)
    # 永不返回——轮询任务会静默挂死，配置热更新从此失效。
    if not math.isfinite(opts.poll_interval_s) or opts.poll_interval_s <= 0:
        raise ValueError(
            f"环境变量 {ENV_POLL_S} 必须是 > 0 的有限数值（秒），"
            f"当前值 {opts.poll_interval_s}"
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


def canonical_config(mode: str, envs: list[model.EnvQuota]) -> str:
    """把可热更新部分（mode + envs）序列化为规范化 JSON（键排序、紧凑
    分隔符），作为配置内容的精确身份。

    变更检测必须比较这个字符串本身而不是它的哈希：32 位校验和存在碰撞
    窗口（~2^-32/次，且非密码学哈希对结构化输入可能更差），一旦新旧内容
    碰撞，该次变更会被静默丢弃且永不自愈——watch 手里本就持有全量内容，
    没有理由用有损比较。envs 查询带 ORDER BY，行序稳定，字符串可复现。
    """
    payload = {"mode": mode, "envs": [e.to_dict() for e in envs]}
    return json.dumps(
        payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    )


def config_checksum(mode: str, envs: list[model.EnvQuota]) -> int:
    """内容校验和 = canonical_config 的 CRC32，充当配置版本号。

    数据库没有现成的单调版本号可用（要求运维每次改配置手动 bump 版本，
    既繁琐又容易忘），因此用内容指纹代替。注意角色边界：它只用于
    ControllerConfig.version 的展示/核对（日志、心跳），变更检测一律用
    canonical_config 字符串精确比较（见其 docstring），不依赖此哈希。
    """
    return zlib.crc32(canonical_config(mode, envs).encode("utf-8"))


async def _fetch_raw(opts: MySQLOptions) -> dict[str, Any]:
    """连接数据库，在单个事务里拉取四张表的一致快照并组装为原始 dict。

    整个过程包在 FETCH_TIMEOUT_S 的整体超时里：aiomysql 的 connect_timeout
    只覆盖建连，查询读结果没有超时，已建立连接上的静默网络分区会永久
    阻塞（见 FETCH_TIMEOUT_S 注释）。超时抛 TimeoutError，由调用方按
    普通失败处理（启动路径重试 / 轮询路径 fail-static 告警）。

    aiomysql 在函数内延迟导入：只有启用数据库配置模式才需要该依赖，
    纯 YAML 部署与单元测试不必安装。
    """
    import aiomysql  # 延迟导入，见 docstring

    async with asyncio.timeout(FETCH_TIMEOUT_S):
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
    """拉取一次全量配置并走统一校验管线，返回 ServiceConfig。

    rows_to_raw 阶段的 ValueError（如 params_json 写坏）发生在 from_raw
    的来源包装之前，这里补上同样的来源前缀——保证"所有配置内容错误都
    带来源描述"的承诺对数据库路径同样成立（多实例对接不同配置库时靠它
    定位是哪个库写坏了）。
    """
    try:
        raw = await _fetch_raw(opts)
    except ValueError as e:
        raise ValueError(f"{opts.describe()} 无效: {e}") from None
    return configmod.from_raw(raw, source=opts.describe())


# pymysql/MySQL 的"重试也不会好"的错误码：库/凭据配置写错，属于部署
# 错误而非"MySQL 仍在初始化"。1044 = 无库权限，1045 = 拒绝访问（密码错），
# 1049 = 未知数据库，1698 = auth_socket 拒绝。
_PERMANENT_MYSQL_ERRNOS = frozenset({1044, 1045, 1049, 1698})


def _is_permanent_error(e: Exception) -> bool:
    """判断启动阶段的失败是否属于重试无意义的永久性错误。

    - ImportError/ModuleNotFoundError：aiomysql 没装，环境问题；
    - pymysql 的 OperationalError/ProgrammingError 携带上述错误码：
      凭据或库名写错。pymysql 异常的 args[0] 即 MySQL errno，靠它判断
      可以不在本模块引入 pymysql 依赖。
    其余（连接拒绝、DNS 未就绪、超时等）视作暂态，交给重试。
    """
    if isinstance(e, ImportError):
        return True
    args = getattr(e, "args", None)
    return bool(args) and args[0] in _PERMANENT_MYSQL_ERRNOS


async def load_service_config(
    opts: MySQLOptions,
    log: logging.Logger,
) -> configmod.ServiceConfig:
    """启动阶段的配置加载：数据库暂不可达时在 STARTUP_RETRY_FOR_S 内重试。

    为什么要重试而不是立刻失败：docker compose / 服务器重启场景下
    rl-limiter 与 MySQL 几乎同时拉起，MySQL 首次初始化（建库、执行
    init.sql）需要几十秒；把"等待依赖就绪"做进服务比要求编排层精确
    串行更皮实。两类失败立即上抛、绝不重试：
      - 配置内容校验失败（ValueError）：表里的数据写错了，重试一万次
        也不会自己变对；
      - 永久性环境/凭据错误（_is_permanent_error）：aiomysql 未安装、
        密码错、库名错——重试只会用"MySQL 可能仍在初始化"的告警刷屏
        两分钟，掩盖真实原因。
    """
    loop = asyncio.get_running_loop()
    deadline = loop.time() + STARTUP_RETRY_FOR_S
    attempt = 0
    while True:
        attempt += 1
        try:
            return await fetch_service_config(opts)
        except ValueError:
            raise  # 配置内容错误：重试无意义，带着 from_raw 的中文诊断直接失败
        except Exception as e:
            if _is_permanent_error(e):
                raise RuntimeError(
                    f"{opts.describe()} 连接失败且属于永久性错误"
                    f"（凭据/库名/依赖问题，重试无意义）：{e}"
                ) from e
            remaining = deadline - loop.time()
            if remaining <= 0:
                raise RuntimeError(
                    f"{opts.describe()} 在 {STARTUP_RETRY_FOR_S:.0f}s 内始终"
                    f"不可用，放弃启动（共尝试 {attempt} 次，最后错误：{e}）"
                ) from e
            log.warning(
                "数据库暂不可用，等待后重试（MySQL 可能仍在初始化） "
                "attempt=%d remaining_s=%.0f err=%s",
                attempt, remaining, e)
            await asyncio.sleep(min(STARTUP_RETRY_INTERVAL_S, remaining))


# ---- 配置写回（Web 控制台的"调参"落点）--------------------------------
#
# 控制台的所有参数修改都写数据库而不是直接改进程内存：MySQL 是配置的
# 唯一事实源，写库后由 watch 的既有轮询链路热生效——页面看到的参数
# 永远与库一致，重启也不丢，且天然复用统一校验管线（写坏的参数会在
# 下一轮 fetch 被拒绝并 fail-static，绝不会让坏配置进入快环）。

async def _exec_write(opts: MySQLOptions, sql: str, args: tuple) -> int:
    """执行一条写语句并提交，返回**匹配**行数（FOUND_ROWS 口径）。

    必须用 FOUND_ROWS 而不是默认的"实际改变行数"：把配额 UPDATE 成与
    当前相同的值时默认口径返回 0，会被调用方误判为"环境不存在"。
    """
    import aiomysql  # 延迟导入，理由见 _fetch_raw
    from pymysql.constants import CLIENT  # pymysql 是 aiomysql 的既有依赖

    async with asyncio.timeout(FETCH_TIMEOUT_S):
        conn = await aiomysql.connect(
            host=opts.host, port=opts.port, user=opts.user,
            password=opts.password, db=opts.database,
            connect_timeout=opts.connect_timeout_s,
            charset="utf8mb4", autocommit=False,
            client_flag=CLIENT.FOUND_ROWS,
        )
        try:
            async with conn.cursor() as cur:
                await cur.execute(sql, args)
                rowcount = cur.rowcount
            await conn.commit()
        finally:
            conn.close()
    return rowcount


async def update_mode(opts: MySQLOptions, mode: str) -> None:
    """热切换运行模式（dry-run/enforce）。非法值在这里就拦下，不落库。"""
    if mode not in (model.MODE_DRY_RUN, model.MODE_ENFORCE):
        raise ValueError(
            f"mode 值非法：{mode!r}，必须是 {model.MODE_DRY_RUN!r} 或 "
            f"{model.MODE_ENFORCE!r}")
    await _exec_write(
        opts, "UPDATE service_config SET mode = %s WHERE id = 1", (mode,))


async def update_env_quota(opts: MySQLOptions, env_id: str, quota_bps: int) -> None:
    """更新环境配额（bits/s，运维口径）。环境不存在按错误报出，
    而不是静默 0 行更新——控制台上的拼写错误必须立刻可见。"""
    if quota_bps <= 0:
        raise ValueError(f"quota_bps 必须 > 0（比特每秒），当前值 {quota_bps!r}")
    n = await _exec_write(
        opts, "UPDATE envs SET quota_bps = %s WHERE env_id = %s",
        (int(quota_bps), env_id))
    if n == 0:
        raise ValueError(f"环境 {env_id!r} 不存在于 envs 表")


async def update_env_params(
    opts: MySQLOptions, env_id: str, params: dict[str, Any] | None,
) -> None:
    """更新环境的快环参数覆盖（None 表示清除覆盖、回到默认参数）。

    键集合与数值先在这里按 GovParams 校验（未知键/非数值直接拒绝），
    避免把"下一轮 fetch 才发现的坏 JSON"写进库触发 fail-static 告警。
    """
    params_json: str | None = None
    if params is not None:
        if not isinstance(params, dict):
            raise ValueError(
                f"params 必须是键值映射或 null，当前为 {type(params).__name__}")
        allowed = set(model.GovParams.__dataclass_fields__)
        unknown = sorted(set(params) - allowed)
        if unknown:
            raise ValueError(
                f"params 含未知键 {unknown}，可用键：{sorted(allowed)}")
        for k, v in params.items():
            if not isinstance(v, (int, float)) or isinstance(v, bool):
                raise ValueError(f"params.{k} 必须是数值，当前值 {v!r}")
        params_json = json.dumps(params, ensure_ascii=False)
    n = await _exec_write(
        opts, "UPDATE envs SET params_json = %s WHERE env_id = %s",
        (params_json, env_id))
    if n == 0:
        raise ValueError(f"环境 {env_id!r} 不存在于 envs 表")


async def watch(
    opts: MySQLOptions,
    queue: "asyncio.Queue[model.ControllerConfig]",
    boot_cfg: configmod.ServiceConfig,
    log: logging.Logger,
) -> None:
    """常驻轮询任务：发现配置内容变化就把新 ControllerConfig 投入 queue。

    boot_cfg 是启动时加载并已应用的引导配置，watch 从它自行派生全部
    变更检测基线（内容、节点接线、可写节点集合）——基线与实际已应用的
    内容出自同一份数据，不存在两个模块各算一份、日后悄悄发散的风险。

    - 变更判定：canonical_config 字符串精确比较（不是哈希，见其
      docstring）；首轮轮询读到与引导相同的内容时不会重复触发空应用；
    - 能力边界：只热更 mode 与 envs（与管理后台下发一致）。haproxy_nodes
      属于基础设施接线，进程内的 TCP 客户端在启动时构建：
        * 节点行内容变化 → 记 warning 提示需要重启生效；
        * envs 引用了启动时不存在的节点 → **拒绝应用整份快照**（保留
          当前配置，fail-static）。此时校验虽通过（新节点行在同一快照
          里），但进程内没有它的 client：应用了只会让该环境既采不到量
          也写不进限速值，即实际不受限——比"暂不生效"危险得多；
    - 任何失败（连接断、校验不过）都保留当前配置继续限速（fail-static），
      下一轮再试。
    """
    last_canonical = canonical_config(boot_cfg.mode, boot_cfg.envs)
    last_nodes = list(boot_cfg.nodes)  # NodeConfig 是 dataclass，逐字段相等
    # 启动时完成接线（构建了 RuntimeClient）的节点集合：热更新的硬边界。
    wired_nodes = frozenset(n.name for n in boot_cfg.nodes)
    # 已拒绝快照的内容身份：同一份坏配置只告警一次，避免每轮刷屏。
    last_rejected: str | None = None
    while True:
        await asyncio.sleep(opts.poll_interval_s)
        try:
            cfg = await fetch_service_config(opts)
        except Exception as e:
            log.warning(
                "轮询数据库配置失败，保留当前配置继续限速（fail-static），下一轮再试 "
                "poll_interval_s=%s err=%s", opts.poll_interval_s, e)
            continue

        if cfg.nodes != last_nodes:
            log.warning(
                "检测到 haproxy_nodes 表发生变化：节点接线在进程启动时构建，"
                "热更新不生效，请重启 rl-limiter 使其生效 nodes=%d", len(cfg.nodes))
            last_nodes = list(cfg.nodes)  # 只在变化那一轮告警一次

        canonical = canonical_config(cfg.mode, cfg.envs)
        if canonical == last_canonical:
            continue

        # 热更新硬边界检查：新 envs 只能挂在启动时已接线的节点上。
        unwired = sorted(
            {t.node for e in cfg.envs for t in e.targets} - wired_nodes)
        if unwired:
            if canonical != last_rejected:
                last_rejected = canonical
                log.error(
                    "数据库新配置引用了启动时未接线的节点，拒绝热应用并保留当前配置"
                    "（fail-static）：这些节点没有运行期客户端，应用后对应环境将"
                    "既采不到用量也写不进限速值（实际不受限）。请重启 rl-limiter "
                    "完成新节点接线 unwired_nodes=%s envs=%d",
                    ",".join(unwired), len(cfg.envs))
            continue

        last_canonical = canonical
        last_rejected = None
        version = zlib.crc32(canonical.encode("utf-8"))
        ctl = model.ControllerConfig(version=version, mode=cfg.mode, envs=cfg.envs)
        queue.put_nowait(ctl)
        log.info(
            "检测到数据库配置变化，已提交主循环热生效 "
            "version=%s mode=%s envs=%d", version, cfg.mode, len(cfg.envs))
