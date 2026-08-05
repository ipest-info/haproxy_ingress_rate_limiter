# hapagg.aggregate —— 把多台 HAProxy 合成一个视图。
#
# 合并的身份是 **(pxname, svname)**：几台机器上同名的 proxy 对象被当成
# 同一个东西。这正是集群部署的实情——3 台机器各有一个 `fe_main`，运维
# 关心的是"fe_main 这个入口一共扛了多少"，而不是三份分开的数字。
#
# 每个维度怎么合，由 stats.STAT_DIMS 里的 how 决定（sum/max/min/wavg），
# 那张表的注释解释了每一种的理由。这里只负责把规则执行对，外加三件
# 合并本身必须处理好的事：
#
#   1. **覆盖度要跟着数字走。** 每个合并结果都带 `nodes`（由几台合成）。
#      少了一台的总量和齐全的总量长得一模一样，不带这个数就没法区分。
#   2. **状态取最坏。** 3 台里 1 台 DOWN，整体就是有问题，不能被 2 台 UP
#      平均掉。
#   3. **保留每台的原值。** 聚合视图告诉你"有问题"，per-node 明细告诉你
#      "problem 在哪台"。只有前者的话，排障第一步就卡住。

from __future__ import annotations

from dataclasses import dataclass, field

from . import stats
from .collect import Collection, NodeSnapshot


@dataclass
class MergedObject:
    """跨机合并后的一个 proxy 对象（frontend / backend / server）。"""

    pxname: str
    svname: str
    type: int
    # 维度名 → 合并后的值（None = 没有任何一台提供这个指标）
    values: dict[str, int | None] = field(default_factory=dict)
    # 合并后的状态（取最坏）
    status: str | None = None
    # 这个对象在哪几台机器上出现过。**数量少于成功台数就是有机器缺这个
    # proxy** —— 通常意味着几台的 haproxy.cfg 不一致，是个值得看见的信号。
    nodes: list[str] = field(default_factory=list)
    # 各台的原值，供下钻：机器名 → {维度: 值}
    per_node: dict[str, dict[str, int | None]] = field(default_factory=dict)
    # 各台的状态，供下钻定位是哪台 DOWN
    per_node_status: dict[str, str | None] = field(default_factory=dict)

    @property
    def type_name(self) -> str:
        return stats.TYPE_NAMES.get(self.type, f"type{self.type}")

    @property
    def key(self) -> tuple[str, str]:
        return (self.pxname, self.svname)

    def bad_nodes(self) -> list[str]:
        """状态不正常的机器名。排障时第一个要看的东西。"""
        worst = stats.status_rank(self.status)
        if worst >= stats.status_rank("UP"):
            return []
        return [n for n, s in self.per_node_status.items()
                if stats.status_rank(s) <= worst]


@dataclass
class AggregatedView:
    """一轮采集合成的完整视图。"""

    ts: float
    # 覆盖度：总目标数 / 成功数 / 各失败原因
    total_nodes: int = 0
    ok_nodes: int = 0
    failures: list[tuple[str, str]] = field(default_factory=list)
    # 合并后的对象，按 (pxname, svname) 索引
    objects: dict[tuple[str, str], MergedObject] = field(default_factory=dict)
    # 进程级指标（show info）合并结果
    info: dict[str, int | None] = field(default_factory=dict)
    per_node_info: dict[str, dict[str, int | None]] = field(default_factory=dict)
    versions: dict[str, str] = field(default_factory=dict)

    @property
    def complete(self) -> bool:
        return self.total_nodes > 0 and self.ok_nodes == self.total_nodes

    def coverage_note(self) -> str:
        """挂在每个总量旁边的那句话。**不要省。**"""
        if self.complete:
            return f"{self.ok_nodes}/{self.total_nodes} 台全部采到"
        return (f"**{self.ok_nodes}/{self.total_nodes} 台**"
                f"（{len(self.failures)} 台失败，下面的总量只含成功的那些）")

    def by_type(self, typ: int) -> list[MergedObject]:
        return sorted((o for o in self.objects.values() if o.type == typ),
                      key=lambda o: (o.pxname, o.svname))

    def frontends(self) -> list[MergedObject]:
        return self.by_type(stats.TYPE_FRONTEND)

    def backends(self) -> list[MergedObject]:
        return self.by_type(stats.TYPE_BACKEND)

    def servers(self) -> list[MergedObject]:
        return self.by_type(stats.TYPE_SERVER)

    def inconsistent_objects(self) -> list[MergedObject]:
        """只在部分机器上存在的对象。

        集群里各台的 haproxy.cfg 本该一致；不一致往往意味着某台漏了一次
        发布。这个信号只有在聚合视图里才看得出来——单看任何一台都正常。
        """
        if self.ok_nodes <= 1:
            return []
        return [o for o in self.objects.values() if len(o.nodes) < self.ok_nodes]


def _merge_dim(d: stats.Dim, per_node: dict[str, dict[str, int | None]],
               weights: dict[str, int | None]) -> int | None:
    names = list(per_node)
    vals = [per_node[n].get(d.key) for n in names]
    ws = [weights.get(n) or 0 for n in names] if d.how == stats.WAVG else None
    # 只把有值的那些交给合并函数，权重要跟着对齐。
    pairs = [(v, (ws[i] if ws else 0)) for i, v in enumerate(vals) if v is not None]
    if not pairs:
        return None
    return stats.merge_values(d.how, [p[0] for p in pairs],
                              [p[1] for p in pairs] if ws else None)


def aggregate(coll: Collection,
              dims: tuple[stats.Dim, ...] = stats.STAT_DIMS,
              info_dims: tuple[stats.Dim, ...] = stats.INFO_DIMS
              ) -> AggregatedView:
    """把一轮采集合成视图。"""
    view = AggregatedView(
        ts=coll.ts,
        total_nodes=len(coll.nodes),
        ok_nodes=len(coll.ok_nodes),
        failures=[(n.label, n.error) for n in coll.failed_nodes],
    )

    # ── proxy 对象 ────────────────────────────────────────────────────
    # 先按 (pxname, svname) 把各台的行归拢，再逐个维度合并。分两步是为了
    # 让加权平均能拿到同一个对象在各台上的权重。
    buckets: dict[tuple[str, str], dict[str, stats.Row]] = {}
    for node in coll.ok_nodes:
        for row in node.rows:
            buckets.setdefault(row.key, {})[node.label] = row

    for key, by_node in buckets.items():
        any_row = next(iter(by_node.values()))
        obj = MergedObject(pxname=key[0], svname=key[1], type=any_row.type)
        obj.nodes = sorted(by_node)
        obj.per_node = {n: stats.as_dict(r, dims) for n, r in by_node.items()}
        obj.per_node_status = {n: r.text("status") for n, r in by_node.items()}
        obj.status = stats.merge_status(list(obj.per_node_status.values()))
        weights = {n: r.num(*stats.WAVG_WEIGHT_COLS) for n, r in by_node.items()}
        obj.values = {d.key: _merge_dim(d, obj.per_node, weights) for d in dims}
        view.objects[key] = obj

    # ── 进程级（show info）──────────────────────────────────────────
    per_node_info: dict[str, dict[str, int | None]] = {}
    for node in coll.ok_nodes:
        if not node.info:
            continue
        per_node_info[node.label] = {
            d.key: stats.info_num(node.info, *d.cols) for d in info_dims}
        if node.version:
            view.versions[node.label] = node.version
    view.per_node_info = per_node_info
    for d in info_dims:
        vals = [v.get(d.key) for v in per_node_info.values()]
        vals = [v for v in vals if v is not None]
        view.info[d.key] = stats.merge_values(d.how, vals) if vals else None
    return view


def totals(view: AggregatedView, typ: int = stats.TYPE_FRONTEND
           ) -> dict[str, int | None]:
    """把某一类对象再横向汇总成"整个集群"一行。

    **只对 sum 类维度做二次汇总。** max/min/wavg 再汇总一次没有意义：
    把各 frontend 的峰值相加，得到的是一个从未真实发生过的数字；把各
    frontend 的平均延迟再平均一次，权重信息已经在上一步丢掉了。
    宁可这一行少几列，也不要给一个说不清口径的数。
    """
    out: dict[str, int | None] = {}
    objs = view.by_type(typ)
    for d in stats.STAT_DIMS:
        if d.how != stats.SUM:
            continue
        vals = [o.values.get(d.key) for o in objs]
        vals = [v for v in vals if v is not None]
        out[d.key] = sum(vals) if vals else None
    return out


def node_health(coll: Collection) -> list[tuple[NodeSnapshot, str]]:
    """每台机器一行的健康小结，供视图顶部展示。"""
    out = []
    for n in coll.nodes:
        if not n.ok:
            out.append((n, f"失败：{n.error}"))
        else:
            fe = sum(1 for r in n.rows if r.type == stats.TYPE_FRONTEND)
            be = sum(1 for r in n.rows if r.type == stats.TYPE_BACKEND)
            sv = sum(1 for r in n.rows if r.type == stats.TYPE_SERVER)
            ver = f" {n.version}" if n.version else ""
            out.append((n, f"ok{ver}  {fe} frontend / {be} backend / "
                           f"{sv} server  {n.elapsed_ms}ms"))
    return out
