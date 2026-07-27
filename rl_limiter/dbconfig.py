# rl_limiter.dbconfig —— MySQL 配置源：服务配置从数据库读取并轮询热更新。
#
# 定位：替代本地 YAML 文件成为服务配置的权威来源。四张表对应 YAML 的
# 四块内容（建表与种子数据见 deploy/mysql/init.sql）：
#
#   service_config   单行表：log_level / tick_interval_s
#   haproxy_nodes    受控 HAProxy 节点清单：接线（socket_path 或
#                    host/port，二选一 + 超时）+ quota_bps（登记限额=
#                    超限告警基准，可热更；应与该节点 haproxy.cfg 里
#                    shared bwlim 的 limit 一致）
#   envs             业务环境分组（仅 env_id：环境=节点分组+聚合视图）
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
#     组装好的 ControllerConfig 投入主循环的配置队列热生效（限额基准与
#     分组可热更；haproxy_nodes 接线变更涉及重建 TCP 客户端，记 warning
#     提示重启）；
#   - 数据库故障或新配置校验不通过时：保留当前配置继续监控、只记日志
#     （fail-static 的监控侧对应：出错不丢弃已知配置）。
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
    "SELECT log_level, tick_interval_s "
    "FROM service_config WHERE id = 1"
)
_SQL_NODES = (
    "SELECT name, host, port, timeout_ms, quota_bps, socket_path "
    "FROM haproxy_nodes ORDER BY name"
)
# v2.1：envs 表只剩分组标识——配额/参数已下沉到 haproxy_nodes。
_SQL_ENVS = "SELECT env_id FROM envs ORDER BY env_id"
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
    dict 服务级键全部缺省（按默认值处理）。
    """
    raw: dict[str, Any] = {}
    if service_row is not None:
        log_level, tick = service_row
        if log_level:
            raw["log_level"] = str(log_level)
        if tick is not None:
            raw["tick_interval_s"] = tick

    nodes_out: list[dict[str, Any]] = []
    for row in node_rows:
        # socket_path 是后加的列；容忍 5 列的旧行序（老库尚未 ALTER TABLE
        # 时不至于整份配置解析不了，而是退化成纯 TCP 形态）。
        name, host, port, timeout_ms, quota_bps = row[:5]
        socket_path = row[5] if len(row) > 5 else None
        node: dict[str, Any] = {
            "name": name,
            # NULL 列统一规整成空/零：同机形态下 host/port 就是空的，
            # 交给 config._validate 判定"二选一"是否成立。
            "host": host or "",
            "port": port or 0,
            "timeout_ms": timeout_ms or 0,
            "socket_path": socket_path or "",
        }
        # quota_bps 是节点行里唯一的**可热更**运行列（其余为接线字段，
        # 重启生效）。
        if quota_bps is not None:
            node["quota_bps"] = quota_bps
        nodes_out.append(node)
    raw["haproxy_nodes"] = nodes_out

    targets_by_env: dict[str, list[dict[str, Any]]] = {}
    for env_id, node, frontend in target_rows:
        targets_by_env.setdefault(str(env_id), []).append(
            {"node": node, "frontend": frontend}
        )

    # v2.1：环境行只剩分组标识（配额/参数已下沉到节点行）。
    raw["envs"] = [
        {"env_id": env_id, "targets": targets_by_env.get(str(env_id), [])}
        for (env_id,) in env_rows
    ]
    return raw


def canonical_config(
    envs: list[model.EnvQuota],
    env_groups: dict[str, list[str]] | None = None,
) -> str:
    """把可热更新部分（监控单元 + 环境分组）序列化为规范化 JSON
    （键排序、紧凑分隔符），作为配置内容的精确身份。

    env_groups 必须纳入：把节点从一个环境移到另一个环境时，监控单元
    （节点限额/挂载点）可能完全不变，只有分组归属变了——不纳入会漏掉
    这类"纯重分组"变更，控制台聚合视图将停在旧分组。

    变更检测必须比较这个字符串本身而不是它的哈希：32 位校验和存在碰撞
    窗口（~2^-32/次，且非密码学哈希对结构化输入可能更差），一旦新旧内容
    碰撞，该次变更会被静默丢弃且永不自愈——watch 手里本就持有全量内容，
    没有理由用有损比较。envs 查询带 ORDER BY，行序稳定，字符串可复现。
    """
    payload = {
        "envs": [e.to_dict() for e in envs],
        "env_groups": {k: list(v) for k, v in (env_groups or {}).items()},
    }
    return json.dumps(
        payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    )


def config_checksum(
    envs: list[model.EnvQuota],
    env_groups: dict[str, list[str]] | None = None,
) -> int:
    """内容校验和 = canonical_config 的 CRC32，充当配置版本号。

    数据库没有现成的单调版本号可用（要求运维每次改配置手动 bump 版本，
    既繁琐又容易忘），因此用内容指纹代替。注意角色边界：它只用于
    ControllerConfig.version 的展示/核对（日志、控制台），变更检测一律用
    canonical_config 字符串精确比较（见其 docstring），不依赖此哈希。
    """
    return zlib.crc32(
        canonical_config(envs, env_groups).encode("utf-8"))


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


async def fetch_service_config(
    opts: MySQLOptions, scope_node: str | None = None,
) -> configmod.ServiceConfig:
    """拉取一次全量配置并走统一校验管线，返回 ServiceConfig。

    rows_to_raw 阶段若抛 ValueError，它发生在 from_raw
    的来源包装之前，这里补上同样的来源前缀——保证"所有配置内容错误都
    带来源描述"的承诺对数据库路径同样成立（多实例对接不同配置库时靠它
    定位是哪个库写坏了）。

    scope_node 非 None（同机部署模式）时，**先在全量配置上完成校验**再
    裁剪到本机节点：节点独占、Target 唯一这些约束是跨节点的不变量，只
    看本机那部分根本校验不出来。
    """
    try:
        raw = await _fetch_raw(opts)
    except ValueError as e:
        raise ValueError(f"{opts.describe()} 无效: {e}") from None
    cfg = configmod.from_raw(raw, source=opts.describe())
    if scope_node is not None:
        cfg = configmod.scope_to_node(cfg, scope_node, source=opts.describe())
    return cfg


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
    scope_node: str | None = None,
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
            return await fetch_service_config(opts, scope_node)
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

# MySQL 完整性错误号 → 运维能看懂的中文解释（IntegrityError.args[0]）。
_INTEGRITY_HINTS = {
    1062: "该挂载点已属于其它环境（同一 节点/frontend 只能属于一个环境）"
          "或主键重复",
    1452: "引用的节点不存在于 haproxy_nodes 表",
    1451: "存在引用该记录的数据，无法删除",
}


async def _exec_tx(
    opts: MySQLOptions, statements: list[tuple[str, tuple]],
) -> list[int]:
    """在单个事务里顺序执行多条写语句并提交，返回各语句的**匹配**行数
    （FOUND_ROWS 口径）。任一语句失败整个事务回滚。

    必须用 FOUND_ROWS 而不是默认的"实际改变行数"：把配额 UPDATE 成与
    当前相同的值时默认口径返回 0，会被调用方误判为"环境不存在"。

    完整性冲突（重复挂载点、引用不存在的节点等）转译成带中文解释的
    ValueError——它们是用户输入问题，调用方（控制台 API）按 400 应答。
    """
    import aiomysql  # 延迟导入，理由见 _fetch_raw
    import pymysql
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
            counts: list[int] = []
            try:
                async with conn.cursor() as cur:
                    await conn.begin()
                    for sql, args in statements:
                        await cur.execute(sql, args)
                        counts.append(cur.rowcount)
                await conn.commit()
            except pymysql.err.IntegrityError as e:
                await conn.rollback()
                errno = e.args[0] if e.args else 0
                hint = _INTEGRITY_HINTS.get(errno, "违反数据完整性约束")
                raise ValueError(f"{hint}（MySQL 错误 {errno}）") from None
        finally:
            conn.close()
    return counts


async def _exec_write(opts: MySQLOptions, sql: str, args: tuple) -> int:
    """单语句便捷入口：见 _exec_tx。"""
    return (await _exec_tx(opts, [(sql, args)]))[0]


async def update_node_quota(opts: MySQLOptions, name: str, quota_bps: int) -> None:
    """更新节点的登记限额（bits/s，运维口径）——超限告警基准。

    注意：这只改**监控基准**；真实限速在该节点 haproxy.cfg 的 shared
    bwlim limit 里，需要同步修改并 reload（不同步会触发持续超限告警）。
    节点不存在按错误报出，而不是静默 0 行更新——控制台上的拼写错误
    必须立刻可见。
    """
    if quota_bps <= 0:
        raise ValueError(f"quota_bps 必须 > 0（比特每秒），当前值 {quota_bps!r}")
    n = await _exec_write(
        opts, "UPDATE haproxy_nodes SET quota_bps = %s WHERE name = %s",
        (int(quota_bps), name))
    if n == 0:
        raise ValueError(f"节点 {name!r} 不存在于 haproxy_nodes 表")


def _validate_targets(targets: Any) -> list[tuple[str, str]]:
    """把 API 传入的 targets 载荷校验/规整为 (node, frontend) 元组表。"""
    if not isinstance(targets, list) or not targets:
        raise ValueError(
            "targets 必须是非空列表（每个环境至少一个挂载点：没有挂载点的"
            "环境采不到任何用量，统一校验管线会拒绝整份配置）")
    out: list[tuple[str, str]] = []
    for i, t in enumerate(targets):
        if not isinstance(t, dict) or not t.get("node") or not t.get("frontend"):
            raise ValueError(
                f"targets[{i}] 必须是 {{\"node\": 节点名, \"frontend\": 前端名}}")
        pair = (str(t["node"]), str(t["frontend"]))
        if pair in out:
            raise ValueError(f"targets[{i}] 与列表中前面的挂载点重复：{pair}")
        out.append(pair)
    return out


async def _write_targets_tx(
    opts: MySQLOptions,
    env_id: str,
    pairs: list[tuple[str, str]],
    create_env_row: bool,
) -> None:
    """create_env / update_env_targets 的共同事务体。

    第一步在事务内做**节点独占检查**（一台 HAProxy 只允许服务一个
    环境）：SELECT ... FOR UPDATE 锁住冲突行，两个并发写不会同时把
    同一节点写进两个环境。必须在写入口就拦下而不是留给下一轮 fetch
    的统一校验——校验失败是 fail-static 拒绝**整份**快照，会把所有
    后续配置变更一起卡死，直到有人手工修表。
    """
    import aiomysql
    import pymysql
    from pymysql.constants import CLIENT

    async with asyncio.timeout(FETCH_TIMEOUT_S):
        conn = await aiomysql.connect(
            host=opts.host, port=opts.port, user=opts.user,
            password=opts.password, db=opts.database,
            connect_timeout=opts.connect_timeout_s,
            charset="utf8mb4", autocommit=False,
            client_flag=CLIENT.FOUND_ROWS,
        )
        try:
            try:
                async with conn.cursor() as cur:
                    await conn.begin()
                    # 节点独占检查：目标节点当前被其它环境占用即拒绝。
                    nodes = sorted({node for node, _ in pairs})
                    ph = ",".join(["%s"] * len(nodes))
                    await cur.execute(
                        f"SELECT DISTINCT node, env_id FROM env_targets "
                        f"WHERE node IN ({ph}) AND env_id <> %s FOR UPDATE",
                        (*nodes, env_id))
                    conflicts = await cur.fetchall()
                    if conflicts:
                        detail = "；".join(
                            f"节点 {n!r} 已属于环境 {o!r}" for n, o in conflicts)
                        raise ValueError(
                            f"{detail}——一台 HAProxy 只允许服务一个环境"
                            f"（一个环境可横跨多台节点，反向不行）。如需"
                            f"迁移节点，先从原环境移除该节点的全部挂载点")
                    if create_env_row:
                        await cur.execute(
                            "INSERT INTO envs (env_id) VALUES (%s)", (env_id,))
                    else:
                        await cur.execute(
                            "SELECT env_id FROM envs WHERE env_id = %s FOR UPDATE",
                            (env_id,))
                        if cur.rowcount == 0:
                            raise ValueError(f"环境 {env_id!r} 不存在于 envs 表")
                        await cur.execute(
                            "DELETE FROM env_targets WHERE env_id = %s", (env_id,))
                    for node, frontend in pairs:
                        await cur.execute(
                            "INSERT INTO env_targets (env_id, node, frontend) "
                            "VALUES (%s, %s, %s)", (env_id, node, frontend))
                await conn.commit()
            except pymysql.err.IntegrityError as e:
                await conn.rollback()
                errno = e.args[0] if e.args else 0
                hint = _INTEGRITY_HINTS.get(errno, "违反数据完整性约束")
                raise ValueError(f"{hint}（MySQL 错误 {errno}）") from None
            except ValueError:
                await conn.rollback()
                raise
        finally:
            conn.close()


async def update_env_targets(
    opts: MySQLOptions, env_id: str, targets: Any,
) -> None:
    """整体替换环境的挂载点列表（单事务：独占检查 → 先删后插，失败全
    回滚）。"节点迁移"由两次调用组成：先从原环境的列表移除该节点的
    挂载点，再加入目标环境——顺序不可反（节点独占检查会拒绝反序）。
    """
    pairs = _validate_targets(targets)
    await _write_targets_tx(opts, env_id, pairs, create_env_row=False)


async def create_env(opts: MySQLOptions, env_id: str, targets: Any) -> None:
    """新建环境分组（环境没有限额，只是节点分组 + 聚合视图；限额登记
    在成员节点上）。必须携带至少一个挂载点：统一校验管线拒绝零挂载点
    的环境，缺挂载点的新环境会让整份配置快照无法生效（fail-static 卡住
    所有后续变更）。"""
    env_id = str(env_id).strip()
    if not env_id:
        raise ValueError("env_id 不能为空")
    pairs = _validate_targets(targets)
    await _write_targets_tx(opts, env_id, pairs, create_env_row=True)


async def delete_env(opts: MySQLOptions, env_id: str) -> None:
    """删除环境（挂载点由外键级联删除）。删除只影响监控分组视图，
    不影响各节点 HAProxy 上的限速。"""
    n = await _exec_write(
        opts, "DELETE FROM envs WHERE env_id = %s", (env_id,))
    if n == 0:
        raise ValueError(f"环境 {env_id!r} 不存在于 envs 表")


async def watch(
    opts: MySQLOptions,
    queue: "asyncio.Queue[model.ControllerConfig]",
    boot_cfg: configmod.ServiceConfig,
    log: logging.Logger,
    scope_node: str | None = None,
) -> None:
    """常驻轮询任务：发现配置内容变化就把新 ControllerConfig 投入 queue。

    boot_cfg 是启动时加载并已应用的引导配置，watch 从它自行派生全部
    变更检测基线（内容、节点接线、可写节点集合）——基线与实际已应用的
    内容出自同一份数据，不存在两个模块各算一份、日后悄悄发散的风险。

    - 变更判定：canonical_config 字符串精确比较（不是哈希，见其
      docstring）；首轮轮询读到与引导相同的内容时不会重复触发空应用；
    - 能力边界：只热更运行字段（限额基准/分组/挂载点）。haproxy_nodes
      属于基础设施接线，进程内的 TCP 客户端在启动时构建：
        * 节点行内容变化 → 记 warning 提示需要重启生效；
        * envs 引用了启动时不存在的节点 → **拒绝应用整份快照**（保留
          当前配置，fail-static）。此时校验虽通过（新节点行在同一快照
          里），但进程内没有它的 client：应用了该单元也采不到任何量；
    - 任何失败（连接断、校验不过）都保留当前配置继续监控（fail-static），
      下一轮再试。

    scope_node（同机部署模式）会一路传给 fetch_service_config，因此变更
    检测天然也是"本机口径"的：兄弟节点改限额/改挂载点不会在本机产生
    一次无谓的热应用与日志。
    """
    last_canonical = canonical_config(boot_cfg.envs, boot_cfg.env_groups)
    last_nodes = list(boot_cfg.nodes)  # NodeConfig 是 dataclass，逐字段相等
    # 启动时完成接线（构建了 RuntimeClient）的节点集合：热更新的硬边界。
    wired_nodes = frozenset(n.name for n in boot_cfg.nodes)
    # 已拒绝快照的内容身份：同一份坏配置只告警一次，避免每轮刷屏。
    last_rejected: str | None = None
    while True:
        await asyncio.sleep(opts.poll_interval_s)
        try:
            cfg = await fetch_service_config(opts, scope_node)
        except Exception as e:
            log.warning(
                "轮询数据库配置失败，保留当前配置继续监控（fail-static），下一轮再试 "
                "poll_interval_s=%s err=%s", opts.poll_interval_s, e)
            continue

        if cfg.nodes != last_nodes:
            log.warning(
                "检测到 haproxy_nodes 表发生变化：节点接线在进程启动时构建，"
                "热更新不生效，请重启 rl-limiter 使其生效 nodes=%d", len(cfg.nodes))
            last_nodes = list(cfg.nodes)  # 只在变化那一轮告警一次

        canonical = canonical_config(cfg.envs, cfg.env_groups)
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
                    "（fail-static）：这些节点没有运行期客户端，应用后对应单元"
                    "采不到任何用量。请重启 rl-limiter 完成新节点接线 "
                    "unwired_nodes=%s envs=%d",
                    ",".join(unwired), len(cfg.envs))
            continue

        last_canonical = canonical
        last_rejected = None
        version = zlib.crc32(canonical.encode("utf-8"))
        ctl = model.ControllerConfig(
            version=version, envs=cfg.envs,
            env_groups={k: list(v) for k, v in cfg.env_groups.items()})
        queue.put_nowait(ctl)
        log.info(
            "检测到数据库配置变化，已提交主循环热生效 version=%s units=%d",
            version, len(cfg.envs))
