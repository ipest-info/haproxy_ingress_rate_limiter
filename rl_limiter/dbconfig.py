# rl_limiter.dbconfig —— MySQL 配置源：服务配置从数据库读取并轮询热更新。
#
# 定位：替代本地 YAML 文件成为服务配置的权威来源。各表对应 YAML 的
# 各块内容（建表与种子数据见 deploy/mysql/init.sql）：
#
#   service_config      单行表：log_level / tick_interval_s
#   haproxy_instances   受管 HAProxy 实例：stats socket 接线
#                       （socket_path 或 host/port，二选一 + 超时）
#   haproxy_frontends   该实例的受管 frontend：监听地址端口、模式、
#                       限额 quota_mbps、maxconn、balance、各项超时
#   haproxy_servers     各 frontend 的后端服务器：地址端口、权重、健康检查
#
# 一个 rl-limiter 实例只管**一台** HAProxy（单 HAProxy 模型，v0.4 起）。
# 多台机器可以共用一个配置库：每台用 RL_NODE_NAME 指定自己是哪个
# instance，只读写属于自己的那些行——集中管理与集中审计因此得以保留。
# （v0.3 及以前的 envs / env_targets 两张"业务环境分组"表已随多节点模型
# 一并移除，见 tag v0.3.0-colocated。）
#
# 复用而不是重写：数据库行先被组装成与 yaml.safe_load 结果**同构**的原始
# dict（rows_to_raw），再走 config.from_raw 的既有解析/校验管线——校验
# 规则只维护一份，两种配置来源的拒绝行为与错误信息完全一致。
#
# 热更新模型（watch）：
#   - 每 poll_interval_s 拉取一次全量配置（各条 SELECT 包在同一个事务里，
#     InnoDB REPEATABLE READ 保证读到的是同一时刻的一致快照）；
#   - 以配置内容的 CRC32 校验和为"版本号"：内容变了校验和必变，直接把
#     组装好的 ControllerConfig 投入主循环的配置队列热生效。frontend 与
#     后端服务器全部可热更——改完即被 enforcer 写进 haproxy.cfg 并
#     reload，这正是 Web 界面能"改完立刻生效"的原因；haproxy_instances
#     的 stats socket 接线在启动时定型，变更记 warning 提示重启；
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

# 快照查询。ORDER BY 让行序稳定：变更检测比较的是序列化后的字符串，
# 行序抖动会被误判成"配置变了"，每轮都触发一次无谓的热应用与 reload。
_SQL_SERVICE = (
    "SELECT log_level, tick_interval_s "
    "FROM service_config WHERE id = 1"
)
# 本实例自己那一行接线。按 name 参数化取，一个配置库可服务多台机器。
_SQL_INSTANCE = (
    "SELECT name, host, port, socket_path, timeout_ms, limit_scope, host_quota_mbps "
    "FROM haproxy_instances WHERE name = %s"
)
_SQL_FRONTENDS = (
    "SELECT name, bind_address, bind_port, mode, quota_mbps, maxconn, balance, "
    "timeout_connect_ms, timeout_client_ms, timeout_server_ms "
    "FROM haproxy_frontends WHERE instance = %s ORDER BY name"
)
_SQL_SERVERS = (
    "SELECT frontend, name, address, port, weight, check_enabled, check_inter_ms "
    "FROM haproxy_servers WHERE instance = %s ORDER BY frontend, name"
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
    instance_row: Sequence[Any] | None,
    frontend_rows: Sequence[Sequence[Any]],
    server_rows: Sequence[Sequence[Any]],
) -> dict[str, Any]:
    """把各表的行组装成与 yaml.safe_load 结果同构的原始 dict。

    纯函数（不碰数据库），是数据库模式复用 config.from_raw 校验管线的
    衔接点。刻意不在这里做任何业务校验：类型/取值/引用完整性全部交给
    config._validate，保证两种配置来源的拒绝行为一字不差。

    service_row / instance_row 为 None 表示对应的行不存在——组装出的
    dict 相应键缺省，由 config 层按默认值处理或报出更易懂的错误。
    """
    raw: dict[str, Any] = {}
    if service_row is not None:
        log_level, tick = service_row
        if log_level:
            raw["log_level"] = str(log_level)
        if tick is not None:
            raw["tick_interval_s"] = tick

    if instance_row is not None:
        (name, host, port, socket_path, timeout_ms,
         limit_scope, host_quota_mbps) = instance_row
        raw["haproxy"] = {
            "name": name,
            # NULL 列统一规整成空/零：同机形态下 host/port 就是空的，
            # 交给 config._validate 判定"二选一"是否成立。
            "host": host or "",
            "port": port or 0,
            "socket_path": socket_path or "",
            "timeout_ms": timeout_ms or 0,
            # 限速范围与整机限额也是实例级配置，随配置轮询热更新——把整机
            # 限额从 1000 调到 500 和改某个 frontend 的限额一样立刻生效。
            # 列是 NOT NULL DEFAULT 'frontend'，这里的兜底只为老库缺列/
            # 值为空的极端情况。兜底成 "frontend" 而不是 "host"：整机范围
            # 缺限额会被校验拒掉，那就成了"库里少个值 → 服务起不来"。
            "limit_scope": limit_scope or "frontend",
            "host_quota_mbps": host_quota_mbps or 0,
        }

    # 先按 frontend 归拢后端服务器，再挂到各 frontend 上。
    servers_by_fe: dict[str, list[dict[str, Any]]] = {}
    for (frontend, name, address, port, weight,
         check_enabled, check_inter_ms) in server_rows:
        servers_by_fe.setdefault(str(frontend), []).append({
            "name": name,
            "address": address or "",
            "port": port or 0,
            "weight": 100 if weight is None else weight,
            # MySQL 的 TINYINT(1) 取回来是 0/1，统一成 bool。
            "check": bool(1 if check_enabled is None else check_enabled),
            "check_inter_ms": check_inter_ms or 2000,
        })

    fronts: list[dict[str, Any]] = []
    for (name, bind_address, bind_port, mode, quota_mbps, maxconn, balance,
         t_connect, t_client, t_server) in frontend_rows:
        fe: dict[str, Any] = {
            "name": name,
            "bind_address": bind_address or "",
            "bind_port": bind_port or 0,
            "mode": mode or "tcp",
            "quota_mbps": quota_mbps or 0,
            "maxconn": maxconn or 0,
            "balance": balance or "roundrobin",
            "servers": servers_by_fe.get(str(name), []),
        }
        # 超时列允许为 NULL：留空即采用 config 层的默认值，不必在库里
        # 为每个 frontend 都填一遍。
        if t_connect:
            fe["timeout_connect_ms"] = t_connect
        if t_client:
            fe["timeout_client_ms"] = t_client
        if t_server:
            fe["timeout_server_ms"] = t_server
        fronts.append(fe)
    raw["frontends"] = fronts
    return raw


def canonical_config(frontends: list[model.FrontendConfig]) -> str:
    """把可热更新部分（受管 frontend 及其后端服务器）序列化为规范化 JSON
    （键排序、紧凑分隔符），作为配置内容的精确身份。

    变更检测必须比较这个字符串本身而不是它的哈希：32 位校验和存在碰撞
    窗口（~2^-32/次，且非密码学哈希对结构化输入可能更差），一旦新旧内容
    碰撞，该次变更会被静默丢弃且永不自愈——watch 手里本就持有全量内容，
    没有理由用有损比较。查询带 ORDER BY，行序稳定，字符串可复现。
    """
    payload = [f.to_dict() for f in frontends]
    return json.dumps(
        payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    )


def config_checksum(frontends: list[model.FrontendConfig]) -> int:
    """内容校验和 = canonical_config 的 CRC32，充当配置版本号。

    数据库没有现成的单调版本号可用（要求运维每次改配置手动 bump 版本，
    既繁琐又容易忘），因此用内容指纹代替。注意角色边界：它只用于
    ControllerConfig.version 的展示/核对（日志、控制台），变更检测一律用
    canonical_config 字符串精确比较（见其 docstring），不依赖此哈希。
    """
    return zlib.crc32(canonical_config(frontends).encode("utf-8"))


async def _fetch_raw(opts: MySQLOptions, instance: str) -> dict[str, Any]:
    """连接数据库，在单个事务里拉取本实例的一致快照并组装为原始 dict。

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
                # 显式事务包住各条 SELECT：InnoDB 默认 REPEATABLE READ 下，
                # 事务内读到的是同一时刻的快照，避免"改到一半的配置"（比如
                # 先插了 frontend 还没插 server）被拼成半新半旧的组合。
                await conn.begin()
                await cur.execute(_SQL_SERVICE)
                service_row = await cur.fetchone()
                await cur.execute(_SQL_INSTANCE, (instance,))
                instance_row = await cur.fetchone()
                await cur.execute(_SQL_FRONTENDS, (instance,))
                frontend_rows = await cur.fetchall()
                await cur.execute(_SQL_SERVERS, (instance,))
                server_rows = await cur.fetchall()
                await conn.commit()
        finally:
            conn.close()
    if instance_row is None:
        # 实例行不存在 = RL_NODE_NAME 与库里对不上。若放任不管，配置会被
        # 解析成"没有接线信息"，报出的错误指向 haproxy 段而非真正的原因。
        raise ValueError(
            f"haproxy_instances 表里没有名为 {instance!r} 的实例——"
            f"请确认 RL_NODE_NAME 与库中登记的实例名一致")
    return rows_to_raw(service_row, instance_row, frontend_rows, server_rows)


async def fetch_service_config(
    opts: MySQLOptions, instance: str,
) -> configmod.ServiceConfig:
    """拉取一次全量配置并走统一校验管线，返回 ServiceConfig。

    rows_to_raw 阶段若抛 ValueError，它发生在 from_raw
    的来源包装之前，这里补上同样的来源前缀——保证"所有配置内容错误都
    带来源描述"的承诺对数据库路径同样成立（多实例对接不同配置库时靠它
    定位是哪个库写坏了）。

    instance 指定读哪一台 HAProxy 的配置（RL_NODE_NAME）——一个配置库
    可以服务多台机器，每台只读写属于自己的那些行。
    """
    try:
        raw = await _fetch_raw(opts, instance)
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
    instance: str,
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
            return await fetch_service_config(opts, instance)
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
    1062: "已存在同名记录（frontend 名或同一 frontend 下的 server 名重复）",
    1452: "引用了不存在的记录（外键约束：instance 或 frontend 不存在）",
    1451: "该记录仍被其它记录引用，无法删除",
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


def _validate_frontend_payload(d: Any) -> dict[str, Any]:
    """把控制台传入的 frontend 载荷规整成可写库的形态。

    这里只做**结构与类型**的规整；取值合法性（端口范围、名字字符集、
    balance 白名单等）交给 config 层——写库后的下一轮轮询会走完整的
    from_raw 校验，坏配置会被 fail-static 拒绝而不会污染数据面。
    但字符集必须在这里就拦住：这些值最终会被渲染进 haproxy.cfg，
    放行任意字符等于允许经控制台往配置文件注入指令。
    """
    if not isinstance(d, dict):
        raise ValueError("请求体必须是 JSON 对象")
    try:
        fe = model.FrontendConfig.from_dict(d)
    except (KeyError, TypeError, ValueError) as e:
        raise ValueError(f"frontend 载荷字段缺失或类型错误: {e}") from None

    # 复用配置层的白名单，保证"经控制台写入"与"直接写库后被加载"两条
    # 路径的接受集合完全一致。
    # 这个临时配置只为跑 frontend 级的校验规则。限速范围固定用 frontend：
    # 整机限速那条"必须给正的 host_quota_mbps"是**实例级**约束，跟"这个
    # frontend 填得对不对"无关，不该在这里把用户的请求拒掉。
    tmp = configmod.ServiceConfig(haproxy=model.NodeConfig(
        name="haproxy", socket_path="/run/haproxy/admin.sock",
        limit_scope="frontend"), frontends=[fe])
    configmod._validate(tmp)     # 不通过则抛 ValueError，调用方按 400 应答
    return {"frontend": fe}


async def upsert_frontend(opts: MySQLOptions, instance: str, payload: Any) -> None:
    """新建或整体更新一个受管 frontend（含它的后端服务器列表）。

    frontend 与它的 server 是一个整体：界面上编辑的是"这个监听端口连同
    它的后端"，所以写库也按整体替换——一个事务里先删旧 server 行再插新的，
    避免出现"改了一半"的中间态被轮询读到。
    """
    fe = _validate_frontend_payload(payload)["frontend"]
    stmts: list[tuple[str, tuple]] = [
        ("INSERT INTO haproxy_frontends "
         "(instance, name, bind_address, bind_port, mode, quota_mbps, maxconn, "
         " balance, timeout_connect_ms, timeout_client_ms, timeout_server_ms) "
         "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
         "ON DUPLICATE KEY UPDATE "
         " bind_address=VALUES(bind_address), bind_port=VALUES(bind_port), "
         " mode=VALUES(mode), quota_mbps=VALUES(quota_mbps), "
         " maxconn=VALUES(maxconn), balance=VALUES(balance), "
         " timeout_connect_ms=VALUES(timeout_connect_ms), "
         " timeout_client_ms=VALUES(timeout_client_ms), "
         " timeout_server_ms=VALUES(timeout_server_ms)",
         (instance, fe.name, fe.bind_address, fe.bind_port, fe.mode,
          fe.quota_mbps, fe.maxconn, fe.balance,
          fe.timeout_connect_ms, fe.timeout_client_ms, fe.timeout_server_ms)),
        ("DELETE FROM haproxy_servers WHERE instance = %s AND frontend = %s",
         (instance, fe.name)),
    ]
    for s in fe.servers:
        stmts.append((
            "INSERT INTO haproxy_servers "
            "(instance, frontend, name, address, port, weight, check_enabled, "
            " check_inter_ms) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
            (instance, fe.name, s.name, s.address, s.port, s.weight,
             1 if s.check else 0, s.check_inter_ms)))
    await _exec_tx(opts, stmts)


async def delete_frontend(opts: MySQLOptions, instance: str, name: str) -> None:
    """删除一个受管 frontend 及其全部后端服务器。

    删掉最后一个 frontend 是被拒绝的：受管区块会因此变空，等于把这台
    HAProxy 的全部监听端口摘掉——那是事故而不是配置操作。
    """
    remaining = await _count_frontends(opts, instance)
    if remaining <= 1:
        raise ValueError(
            "这是最后一个受管 frontend，删除它会摘掉本机全部监听端口；"
            "如确需下线该端口，请先新增替代的 frontend")
    n = (await _exec_tx(opts, [
        ("DELETE FROM haproxy_servers WHERE instance = %s AND frontend = %s",
         (instance, name)),
        ("DELETE FROM haproxy_frontends WHERE instance = %s AND name = %s",
         (instance, name)),
    ]))[1]
    if n == 0:
        raise ValueError(f"frontend {name!r} 不存在于实例 {instance!r}")


async def _count_frontends(opts: MySQLOptions, instance: str) -> int:
    """统计本实例现有的受管 frontend 数量（删除前的安全检查用）。"""
    import aiomysql  # 延迟导入，理由见 _fetch_raw

    async with asyncio.timeout(FETCH_TIMEOUT_S):
        conn = await aiomysql.connect(
            host=opts.host, port=opts.port, user=opts.user,
            password=opts.password, db=opts.database,
            connect_timeout=opts.connect_timeout_s, charset="utf8mb4")
        try:
            async with conn.cursor() as cur:
                await cur.execute(
                    "SELECT COUNT(*) FROM haproxy_frontends WHERE instance = %s",
                    (instance,))
                row = await cur.fetchone()
        finally:
            conn.close()
    return int(row[0]) if row else 0


async def watch(
    opts: MySQLOptions,
    queue: "asyncio.Queue[model.ControllerConfig]",
    boot_cfg: configmod.ServiceConfig,
    log: logging.Logger,
    instance: str,
) -> None:
    """常驻轮询任务：发现配置内容变化就把新 ControllerConfig 投入 queue。

    boot_cfg 是启动时加载并已应用的引导配置，watch 从它自行派生变更检测
    基线——基线与实际已应用的内容出自同一份数据，不存在两个模块各算一份、
    日后悄悄发散的风险。

    - 变更判定：canonical_config 字符串精确比较（不是哈希，见其
      docstring）；首轮轮询读到与引导相同的内容时不会重复触发空应用；
    - 能力边界：frontend 与后端服务器全部可热更（这正是 Web 界面"改完
      立刻生效"的基础）。**stats socket 接线例外**——采样客户端在进程
      启动时构建，haproxy_instances 行变化只记 warning 提示重启；
    - 任何失败（连接断、校验不过）都保留当前配置继续监控（fail-static），
      下一轮再试。
    """
    last_canonical = canonical_config(boot_cfg.frontends)
    last_wiring = boot_cfg.haproxy      # NodeConfig 是 dataclass，逐字段相等
    while True:
        await asyncio.sleep(opts.poll_interval_s)
        try:
            cfg = await fetch_service_config(opts, instance)
        except Exception as e:
            log.warning(
                "轮询数据库配置失败，保留当前配置继续监控（fail-static），下一轮再试 "
                "poll_interval_s=%s err=%s", opts.poll_interval_s, e)
            continue

        if cfg.haproxy != last_wiring:
            log.warning(
                "检测到 haproxy_instances 表的接线信息发生变化：stats socket "
                "客户端在进程启动时构建，热更新不生效，请重启 rl-limiter "
                "endpoint=%s", cfg.haproxy.endpoint())
            last_wiring = cfg.haproxy   # 只在变化那一轮告警一次

        canonical = canonical_config(cfg.frontends)
        if canonical == last_canonical:
            continue

        last_canonical = canonical
        version = zlib.crc32(canonical.encode("utf-8"))
        queue.put_nowait(model.ControllerConfig(
            version=version, frontends=list(cfg.frontends)))
        log.info(
            "检测到数据库配置变化，已提交主循环热生效（随后由 enforcer 写入 "
            "haproxy.cfg 并 reload） version=%s frontends=%d",
            version, len(cfg.frontends))
