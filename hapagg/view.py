# hapagg.view —— 把聚合结果渲染成人看的东西。
#
# 两条硬规则：
#
#   1. **覆盖度永远贴着总量出现。** 少了两台的"总带宽"和齐全的长得一模
#      一样，不写清楚就是在骗人。
#   2. **拿不到的指标显示 "-"，不显示 0。** 老版本 HAProxy 没有某一列
#      与"这个值确实是 0"是两回事，混在一起会让人对着一屏 0 找原因。

from __future__ import annotations

from . import stats
from .aggregate import AggregatedView, MergedObject


def human_bytes(n: int | None) -> str:
    if n is None:
        return "-"
    v = float(n)
    for unit in ("B", "K", "M", "G", "T", "P"):
        if abs(v) < 1024 or unit == "P":
            return f"{v:.0f}{unit}" if unit == "B" else f"{v:.1f}{unit}"
        v /= 1024
    return f"{v:.1f}P"


def human_num(n: int | None) -> str:
    if n is None:
        return "-"
    if abs(n) >= 1_000_000:
        return f"{n / 1_000_000:.1f}M"
    if abs(n) >= 10_000:
        return f"{n / 1000:.1f}k"
    return str(n)


def fmt(key: str, val: int | None, unit: str = "") -> str:
    """格式化一个格子。**没有值时只写 "-"，不带单位。**

    带单位的话会印出 "-ms" / "-/s" 这种东西，看着像某种特殊状态，其实
    只是"这台的这个版本没有这一列"。
    """
    if val is None:
        return "-"
    if key in ("bin", "bout"):
        return human_bytes(val) + unit.replace("B", "")
    return human_num(val) + unit


def table(headers: list[str], rows: list[list[str]], indent: str = "") -> str:
    """等宽表格。列宽按内容算——固定宽度在中文与长 proxy 名下必然错位。"""
    if not rows:
        return indent + "（无）"
    widths = [len(h) for h in headers]
    for r in rows:
        for i, c in enumerate(r):
            if i < len(widths):
                widths[i] = max(widths[i], _w(c))
    out = [indent + "  ".join(_pad(h, widths[i]) for i, h in enumerate(headers))]
    out.append(indent + "  ".join("-" * w for w in widths))
    for r in rows:
        out.append(indent + "  ".join(
            _pad(c, widths[i]) for i, c in enumerate(r) if i < len(widths)))
    return "\n".join(out)


def _w(s: str) -> int:
    """显示宽度：中文按 2 算，否则表格在有中文的列上一定错位。"""
    return sum(2 if ord(ch) > 0x2E80 else 1 for ch in s)


def _pad(s: str, width: int) -> str:
    return s + " " * max(0, width - _w(s))


def render_header(view: AggregatedView) -> str:
    lines = [f"采集覆盖：{view.coverage_note()}"]
    if view.failures:
        lines.append("失败的机器（它们的数据**不在**下面任何总量里）：")
        for label, err in view.failures:
            lines.append(f"  [FAIL] {label}：{err}")
    if len(set(view.versions.values())) > 1:
        # 版本不一致时 CSV 的列会不一样，某些维度会在一部分机器上缺失。
        vs = ", ".join(f"{n}={v}" for n, v in sorted(view.versions.items()))
        lines.append(f"  [warn] 各机器 HAProxy 版本不一致（{vs}）——"
                     f"部分维度可能只有一部分机器提供")
    return "\n".join(lines)


def render_info(view: AggregatedView) -> str:
    if not view.per_node_info:
        return "进程级指标：（没有任何机器提供 show info）"
    dims = {d.key: d for d in stats.INFO_DIMS}
    rows = []
    for key, d in dims.items():
        v = view.info.get(key)
        how = {stats.SUM: "求和", stats.MAX: "取最大",
               stats.MIN: "取最小", stats.WAVG: "加权平均"}[d.how]
        rows.append([d.label, fmt(key, v, d.unit), how])
    return "进程级指标（show info）\n" + table(
        ["指标", "合计", "跨机合并方式"], rows, indent="  ")


def pick_dims(objs: list[MergedObject], groups: list[str],
              all_cols: bool) -> list[stats.Dim]:
    """挑出这张表要显示的列。

    两道筛选，都是为了让表能真的被看：
      1. 默认只留 core 列。全部维度铺开是 30+ 列，任何终端都放不下，
         而横向滚动的表格等于没有表格。--all 才全上。
      2. **丢掉所有对象都取不到的列。** 一整列 "-" 只会占宽度、不带任何
         信息——它的含义（这批机器的版本没有这个指标）在表头写一次就够，
         不需要每行重复一遍。
    """
    dims = [d for d in stats.STAT_DIMS
            if d.group in groups and (all_cols or d.core)]
    return [d for d in dims
            if any(o.values.get(d.key) is not None for o in objs)]


def render_objects(title: str, objs: list[MergedObject], groups: list[str],
                   ok_nodes: int, all_cols: bool = False) -> str:
    """按分组渲染一类对象。分组是为了让一屏只回答一类问题。"""
    if not objs:
        return f"{title}：（无）"
    dims = pick_dims(objs, groups, all_cols)
    headers = ["proxy", "对象", "状态", "台数"] + [d.label for d in dims]
    rows = []
    for o in objs:
        cover = str(len(o.nodes))
        if ok_nodes and len(o.nodes) < ok_nodes:
            # 只在部分机器上存在 —— 多半是某台的 cfg 没同步。
            cover += "!"
        rows.append([o.pxname, o.svname, o.status or "-", cover]
                    + [fmt(d.key, o.values.get(d.key), d.unit) for d in dims])
    hidden = sum(1 for d in stats.STAT_DIMS
                 if d.group in groups and not (all_cols or d.core))
    tail = f"（另有 {hidden} 个维度，加 --all 显示）" if hidden and not all_cols else ""
    return f"{title}{tail}\n" + table(headers, rows, indent="  ")


def render_problems(view: AggregatedView) -> str:
    """把"需要立刻看的东西"单独拎出来。

    聚合视图的价值一半在这里：几十台机器的明细没人逐行看，但"哪个后端
    在哪台上 DOWN 了"必须一眼可见。
    """
    lines: list[str] = []
    down = [o for o in view.objects.values()
            if stats.status_rank(o.status) < stats.status_rank("UP")]
    for o in sorted(down, key=lambda x: (x.pxname, x.svname)):
        who = "、".join(o.bad_nodes()) or "?"
        lines.append(f"  [FAIL] {o.pxname}/{o.svname} 状态 {o.status}"
                     f"（在这些机器上：{who}）")

    odd = view.inconsistent_objects()
    for o in sorted(odd, key=lambda x: (x.pxname, x.svname)):
        lines.append(f"  [warn] {o.pxname}/{o.svname} 只在 {len(o.nodes)}/"
                     f"{view.ok_nodes} 台上存在（{'、'.join(o.nodes)}）"
                     f" —— 各机器的 haproxy.cfg 可能不一致")

    if not lines:
        return "需要注意的：无"
    return "需要注意的：\n" + "\n".join(lines)


def render(view: AggregatedView, detail: bool = False,
           all_cols: bool = False) -> str:
    """完整的一屏。"""
    parts = [
        render_header(view),
        "",
        render_problems(view),
        "",
        render_info(view),
        "",
        render_objects("frontend（入口）", view.frontends(),
                       ["流量", "连接", "HTTP", "错误"], view.ok_nodes, all_cols),
        "",
        render_objects("backend（后端组）", view.backends(),
                       ["流量", "连接", "排队", "延迟", "后端"],
                       view.ok_nodes, all_cols),
    ]
    if detail:
        parts += ["", render_objects("server（后端服务器）", view.servers(),
                                     ["流量", "连接", "排队", "错误", "延迟"],
                                     view.ok_nodes, all_cols)]
    return "\n".join(parts)


def render_drilldown(view: AggregatedView, pxname: str) -> str:
    """某个 proxy 在各台机器上的原值。

    聚合告诉你"有问题"，这里告诉你"在哪台"。没有它的话，排障第一步就
    只能挨台去 ssh。
    """
    objs = [o for o in view.objects.values() if o.pxname == pxname]
    if not objs:
        return f"没有名为 {pxname!r} 的 proxy"
    out = []
    for o in sorted(objs, key=lambda x: (x.type, x.svname)):
        # 每个对象各自挑列：只留**这个对象在这些机器上真的有值**的维度。
        # 下钻表是用来比对各台差异的，整列 "-" 只会把真正有差异的那几列
        # 挤到屏幕外面去。
        dims = [d for d in stats.STAT_DIMS
                if d.group in ("流量", "连接", "错误", "延迟")
                and any(o.per_node.get(n, {}).get(d.key) is not None
                        for n in o.nodes)]
        headers = ["机器", "状态"] + [d.label for d in dims]
        rows = []
        for n in o.nodes:
            vals = o.per_node.get(n, {})
            rows.append([n, o.per_node_status.get(n) or "-"]
                        + [fmt(d.key, vals.get(d.key), d.unit) for d in dims])
        out.append(f"{o.pxname}/{o.svname}（{o.type_name}）\n"
                   + table(headers, rows, indent="  "))
    return "\n\n".join(out)
