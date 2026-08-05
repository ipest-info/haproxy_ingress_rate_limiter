# hapagg.stats —— 解析 HAProxy runtime API 的回包。
#
# 两条命令，两种格式：
#
#   show stat   CSV，首行是以 "# " 开头的列头。每行是一个 proxy 对象：
#               frontend / backend / server / listener，由 `type` 列区分。
#   show info   "Key: value" 的逐行文本，进程级指标（连接数、空闲率…）。
#
# **一律按列名取值，绝不按列序。** HAProxy 各版本之间列会增删（2.4→2.8
# 就多了十几列），按位置取的话换个版本所有数字都会串位——而且串完之后
# 图还是画得出来，只是全错。这是这类工具最典型的静默故障。
#
# 同理，取不到的列返回 None 而不是 0：`None` 在视图里显示成 "-"（这个
# 版本没有这个指标），`0` 会被当成"有这个指标且值为零"。混在一起的话，
# "老版本没有 conn_rate 这一列"和"当前连接速率是 0"就再也分不开了。

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any

# `type` 列的取值。HAProxy 文档里的定义，不是猜的。
TYPE_FRONTEND = 0
TYPE_BACKEND = 1
TYPE_SERVER = 2
TYPE_LISTENER = 3

TYPE_NAMES = {
    TYPE_FRONTEND: "frontend",
    TYPE_BACKEND: "backend",
    TYPE_SERVER: "server",
    TYPE_LISTENER: "listener",
}


class ParseError(ValueError):
    pass


@dataclass
class Row:
    """`show stat` 的一行 = 一个 proxy 对象。

    保留**全部**列（raw），因为这个工具的目的就是"各种监控维度"——预先
    挑一批字段出来，等于替使用者决定他能看什么。取值一律走 num()/text()，
    它们负责列名兼容与 None 语义。
    """

    pxname: str
    svname: str
    type: int
    raw: dict[str, str] = field(default_factory=dict)

    @property
    def type_name(self) -> str:
        return TYPE_NAMES.get(self.type, f"type{self.type}")

    @property
    def key(self) -> tuple[str, str]:
        """跨机合并用的身份：同名 proxy 的同名对象是"同一个东西"。"""
        return (self.pxname, self.svname)

    def text(self, *names: str) -> str | None:
        """取一个字符串列；列不存在或为空返回 None。

        接受多个候选名是因为个别列在不同版本里改过名，调用方给出全部
        候选，这里挑第一个存在的。
        """
        for n in names:
            v = self.raw.get(n)
            if v not in (None, ""):
                return v
        return None

    def num(self, *names: str) -> int | None:
        """取一个整数列。**取不到返回 None，不是 0**（见模块头）。"""
        v = self.text(*names)
        if v is None:
            return None
        try:
            return int(v)
        except ValueError:
            # HAProxy 偶有非整数值（比如 check_duration 空着、weight 带
            # 百分号）。当作"这一格没有可用数字"，而不是 0。
            return None


def parse_stat_csv(out: str) -> list[Row]:
    """解析 `show stat` 的 CSV 回包。"""
    lines = [x for x in out.splitlines() if x.strip()]
    if not lines:
        raise ParseError("show stat 回包为空")
    head = lines[0]
    if not head.startswith("#"):
        raise ParseError(
            f"show stat 回包首行不是列头（应以 '# ' 开头），实际是：{head[:80]!r}")
    # 列头形如 "# pxname,svname,qcur,..."，末尾常有一个空列（行尾逗号）。
    cols = [c.strip() for c in head.lstrip("#").strip().split(",")]

    rows: list[Row] = []
    for line in lines[1:]:
        if line.startswith("#"):
            continue
        vals = line.split(",")
        raw = {c: vals[i] for i, c in enumerate(cols) if i < len(vals) and c}
        pxname = raw.get("pxname", "")
        svname = raw.get("svname", "")
        if not pxname or not svname:
            continue
        try:
            typ = int(raw.get("type", ""))
        except ValueError:
            # 没有 type 列的极老版本：按 svname 的约定回退。FRONTEND/
            # BACKEND 是 HAProxy 固定使用的两个特殊名字。
            typ = {"FRONTEND": TYPE_FRONTEND,
                   "BACKEND": TYPE_BACKEND}.get(svname, TYPE_SERVER)
        rows.append(Row(pxname=pxname, svname=svname, type=typ, raw=raw))
    return rows


_INFO_LINE = re.compile(r"^([A-Za-z][\w .()/-]*):\s*(.*)$")


def parse_info(out: str) -> dict[str, str]:
    """解析 `show info` 的 "Key: value" 回包。"""
    info: dict[str, str] = {}
    for line in out.splitlines():
        m = _INFO_LINE.match(line.strip())
        if m:
            info[m.group(1).strip()] = m.group(2).strip()
    if not info:
        raise ParseError(f"show info 回包解析不出任何字段：{out[:80]!r}")
    return info


def info_num(info: dict[str, str], *names: str) -> int | None:
    for n in names:
        v = info.get(n)
        if v not in (None, ""):
            try:
                return int(float(v))
            except ValueError:
                return None
    return None


# ---------------------------------------------------------------------------
# 监控维度目录
# ---------------------------------------------------------------------------
#
# 这张表是这个工具的核心资产：它同时定义了「有哪些维度」和「跨机怎么合」。
# 合并方式一旦选错，视图会给出一个**看起来合理但完全错误**的数字——
# 比如把 3 台机器的空闲率加起来得到 300%，或者把各机器的历史峰值相加，
# 得出一个从未真实发生过的"总峰值"。
#
# 四种合并方式：
#   sum   累计量与瞬时量：字节、连接数、速率、错误计数。跨机相加有意义。
#   max   峰值与上限观测：各机器的峰值不同时发生，**相加会凭空放大**。
#
#         峰值这一类的标签一律写成「峰值(单机)」。原因是一个会毁掉整张表
#         可信度的现象：当前会话是**求和**（3 台各 60 = 180），会话峰值是
#         **取最大**（单台最高 99），于是表上出现 "当前 180 / 峰值 99"
#         这种自相矛盾的组合。两个数各自都对，错的是让人以为它们是同一
#         口径。真正的"集群同时刻峰值"无法从各台的历史峰值推出来——那
#         需要时间对齐的采样，stats socket 给不了。所以这里不假装能算，
#         只把口径在标签上写死。
#   min   取最差的那台：空闲率这类"越低越危险"的指标，看整体健康要看最差。
#   wavg  按权重加权平均：各类延迟。直接平均会让一台空闲机器把繁忙机器
#         的高延迟稀释掉，看不出问题。权重用该对象的会话数。

SUM, MAX, MIN, WAVG = "sum", "max", "min", "wavg"


@dataclass(frozen=True)
class Dim:
    """一个监控维度。"""

    key: str                 # 输出里的字段名
    cols: tuple[str, ...]    # CSV 里的候选列名（版本兼容）
    how: str                 # 合并方式
    label: str               # 人看的名字
    group: str               # 分组，决定它出现在哪一屏
    unit: str = ""
    # 是不是"默认就该看见"的列。全部维度铺开是 30+ 列，任何终端都放不下，
    # 而且真正每天要看的就那几个。其余的用 --all 或按分组调出来。
    core: bool = False

    def pick(self, row: Row) -> int | None:
        return row.num(*self.cols)


# 按"运维实际关心的问题"分组，而不是按 CSV 的列序。
STAT_DIMS: tuple[Dim, ...] = (
    # ── 流量 ──────────────────────────────────────────────────────────
    Dim("bin", ("bin",), SUM, "入向字节", "流量", "B", core=True),
    Dim("bout", ("bout",), SUM, "出向字节", "流量", "B", core=True),
    # ── 连接与会话 ────────────────────────────────────────────────────
    Dim("scur", ("scur",), SUM, "当前会话", "连接", core=True),
    Dim("smax", ("smax",), MAX, "会话峰值(单机)", "连接"),
    Dim("slim", ("slim",), SUM, "会话上限", "连接"),
    Dim("stot", ("stot",), SUM, "累计会话", "连接", core=True),
    Dim("rate", ("rate",), SUM, "会话速率", "连接", "/s", core=True),
    Dim("rate_max", ("rate_max",), MAX, "会话速率峰值(单机)", "连接", "/s"),
    Dim("conn_tot", ("conn_tot",), SUM, "累计连接", "连接"),
    Dim("conn_rate", ("conn_rate",), SUM, "连接速率", "连接", "/s"),
    Dim("conn_rate_max", ("conn_rate_max",), MAX, "连接速率峰值(单机)", "连接", "/s"),
    # ── 排队（backend/server 才有意义）────────────────────────────────
    Dim("qcur", ("qcur",), SUM, "当前排队", "排队", core=True),
    Dim("qmax", ("qmax",), MAX, "排队峰值(单机)", "排队"),
    # ── 错误与丢弃 ────────────────────────────────────────────────────
    Dim("ereq", ("ereq",), SUM, "请求错误", "错误", core=True),
    Dim("econ", ("econ",), SUM, "连接后端错误", "错误", core=True),
    Dim("eresp", ("eresp",), SUM, "响应错误", "错误", core=True),
    Dim("dreq", ("dreq",), SUM, "拒绝请求", "错误", core=True),
    Dim("dresp", ("dresp",), SUM, "拒绝响应", "错误"),
    Dim("dcon", ("dcon",), SUM, "拒绝连接", "错误", core=True),
    Dim("wretr", ("wretr",), SUM, "重试", "错误"),
    Dim("wredis", ("wredis",), SUM, "重新分发", "错误"),
    Dim("cli_abrt", ("cli_abrt",), SUM, "客户端中断", "错误"),
    Dim("srv_abrt", ("srv_abrt",), SUM, "后端中断", "错误"),
    Dim("chkfail", ("chkfail",), SUM, "健康检查失败", "错误"),
    Dim("chkdown", ("chkdown",), SUM, "被摘除次数", "错误", core=True),
    # ── HTTP ──────────────────────────────────────────────────────────
    Dim("hrsp_1xx", ("hrsp_1xx",), SUM, "1xx", "HTTP"),
    Dim("hrsp_2xx", ("hrsp_2xx",), SUM, "2xx", "HTTP"),
    Dim("hrsp_3xx", ("hrsp_3xx",), SUM, "3xx", "HTTP"),
    Dim("hrsp_4xx", ("hrsp_4xx",), SUM, "4xx", "HTTP", core=True),
    Dim("hrsp_5xx", ("hrsp_5xx",), SUM, "5xx", "HTTP", core=True),
    Dim("hrsp_other", ("hrsp_other",), SUM, "其它响应", "HTTP"),
    Dim("req_rate", ("req_rate",), SUM, "请求速率", "HTTP", "/s", core=True),
    Dim("req_rate_max", ("req_rate_max",), MAX, "请求速率峰值(单机)", "HTTP", "/s"),
    Dim("req_tot", ("req_tot",), SUM, "累计请求", "HTTP"),
    # ── 延迟（毫秒，最近 1024 个请求的平均）───────────────────────────
    # 必须**加权**平均：直接平均会让一台空闲机器把繁忙机器的高延迟稀释掉。
    Dim("qtime", ("qtime",), WAVG, "排队耗时", "延迟", "ms"),
    Dim("ctime", ("ctime",), WAVG, "连接耗时", "延迟", "ms"),
    Dim("rtime", ("rtime",), WAVG, "响应耗时", "延迟", "ms", core=True),
    Dim("ttime", ("ttime",), WAVG, "总耗时", "延迟", "ms", core=True),
    Dim("qtime_max", ("qtime_max",), MAX, "排队耗时峰值(单机)", "延迟", "ms"),
    Dim("ctime_max", ("ctime_max",), MAX, "连接耗时峰值(单机)", "延迟", "ms"),
    Dim("rtime_max", ("rtime_max",), MAX, "响应耗时峰值(单机)", "延迟", "ms"),
    Dim("ttime_max", ("ttime_max",), MAX, "总耗时峰值(单机)", "延迟", "ms"),
    # ── 后端服务器 ────────────────────────────────────────────────────
    Dim("act", ("act",), SUM, "活跃服务器", "后端", core=True),
    Dim("bck", ("bck",), SUM, "备份服务器", "后端"),
    Dim("lbtot", ("lbtot",), SUM, "被选中次数", "后端"),
    Dim("weight", ("weight",), SUM, "权重", "后端"),
)

# 加权平均的权重列：用"累计会话数"。它正比于该对象处理过的请求量，
# 是这几个平均延迟的天然权重。
WAVG_WEIGHT_COLS = ("stot",)

INFO_DIMS: tuple[Dim, ...] = (
    Dim("CurrConns", ("CurrConns",), SUM, "当前连接", "进程"),
    Dim("CumConns", ("CumConns",), SUM, "累计连接", "进程"),
    Dim("CumReq", ("CumReq",), SUM, "累计请求", "进程"),
    Dim("ConnRate", ("ConnRate",), SUM, "连接速率", "进程", "/s"),
    Dim("SessRate", ("SessRate",), SUM, "会话速率", "进程", "/s"),
    Dim("Maxconn", ("Maxconn",), SUM, "连接上限", "进程"),
    Dim("MaxconnReached", ("MaxconnReached",), SUM, "触顶次数", "进程"),
    Dim("CurrSslConns", ("CurrSslConns",), SUM, "当前 SSL 连接", "进程"),
    Dim("Tasks", ("Tasks",), SUM, "任务数", "进程"),
    Dim("Run_queue", ("Run_queue",), SUM, "运行队列", "进程"),
    # 空闲率是"越低越危险"，所以取**最差的那台**。相加会得到 300% 这种
    # 荒唐数字，取平均则会把一台已经跑满的机器藏在平均值后面。
    Dim("Idle_pct", ("Idle_pct",), MIN, "最低空闲率", "进程", "%"),
    # 运行时长取最小 = 最近重启过的那台。整体视图里这个数变小就意味着
    # "有机器重启了"，是个很有用的信号。
    Dim("Uptime_sec", ("Uptime_sec",), MIN, "最短运行时长", "进程", "s"),
)


def merge_values(how: str, values: list[int], weights: list[int] | None = None
                 ) -> int | None:
    """按指定方式把多台机器的同一个指标合成一个。

    空列表返回 None（"没有任何一台提供了这个指标"），而不是 0。
    """
    vals = [v for v in values if v is not None]
    if not vals:
        return None
    if how == SUM:
        return sum(vals)
    if how == MAX:
        return max(vals)
    if how == MIN:
        return min(vals)
    if how == WAVG:
        ws = [w if w else 0 for w in (weights or [])][:len(vals)]
        ws += [0] * (len(vals) - len(ws))
        total = sum(ws)
        if total <= 0:
            # 没有权重信息（都没跑过请求）时退化成简单平均——这时各台的
            # 延迟本来也都是 0 或无意义，不会造成误导。
            return round(sum(vals) / len(vals))
        return round(sum(v * w for v, w in zip(vals, ws)) / total)
    raise ValueError(f"未知的合并方式：{how}")


# ---------------------------------------------------------------------------
# 状态：合并要取"最坏"
# ---------------------------------------------------------------------------
#
# 状态不是数字，不能相加。跨机合并时取**最坏**的那个：3 台里有 1 台
# DOWN，整体就该显示成有问题，而不是被 2 台 UP 平均掉。

# 越靠前越坏。HAProxy 的状态字串还可能带后缀（"UP 1/3"、"DOWN 2/3"），
# 所以比对时只取第一个词。
_STATUS_ORDER = ("DOWN", "MAINT", "DRAIN", "NOLB", "no check", "UP", "OPEN")


def status_rank(status: str | None) -> int:
    if not status:
        return len(_STATUS_ORDER)
    head = status.split()[0]
    for i, s in enumerate(_STATUS_ORDER):
        if head == s.split()[0]:
            return i
    return len(_STATUS_ORDER)


def merge_status(statuses: list[str | None]) -> str | None:
    known = [s for s in statuses if s]
    if not known:
        return None
    return min(known, key=status_rank)


def dims_by_group(dims: tuple[Dim, ...] = STAT_DIMS) -> dict[str, list[Dim]]:
    out: dict[str, list[Dim]] = {}
    for d in dims:
        out.setdefault(d.group, []).append(d)
    return out


def dim_map(dims: tuple[Dim, ...]) -> dict[str, Dim]:
    return {d.key: d for d in dims}


def as_dict(row: Row, dims: tuple[Dim, ...] = STAT_DIMS) -> dict[str, Any]:
    """把一行摊成 {维度名: 值}，供单机视图与合并使用。"""
    return {d.key: d.pick(row) for d in dims}
