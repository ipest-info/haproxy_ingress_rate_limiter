# rl_limiter.allocator —— 把控制单元（节点）的整形值按其挂载点拆分的
# 加权分配算法。
#
# 架构位置：控制单元=节点（见 model.py 顶部说明），governor 对节点上
# 受控 frontend 的聚合用量做 AIMD 决策、产出该节点的整形值 bwlim；一台
# 节点可能挂载多个受控 frontend（多个 Target），本模块把 bwlim 拆分到
# 每个 Target，executor 据此写回该节点的 bwlim map。节点只挂一个
# frontend 时退化为恒等映射。注意拆分只发生在**单台节点内部**——
# 节点之间没有任何配额调配。
#
# 算法：
#     保底: floor[i] = bwlim * 5% / N            # 每挂载点保底，防止饿死
#     剩余: rest = bwlim - Σfloor
#     分配: alloc[i] = floor[i] + rest * usage[i] / Σusage
#          (Σusage == 0 时平均分配)
#     约束: Σalloc == bwlim
#
# 为什么要保底（5% 均摊）：分配完全按用量加权时，一个当前没有流量的
# Target 会分到 0——它的 map 值被写成 0 后，新到达的连接立刻被整形到
# 0 速率，永远产生不了用量，也就永远拿不到配额（饿死死锁）。每个
# Target 无条件保底 bwlim × 5% / N，保证空闲挂载点始终留有一条"起步
# 车道"，流量一来就能产生 EWMA、进而在下一轮分配中挣到更大的份额。
#
# 为什么按用量加权（而不是简单均分）：同一节点上多个受控 frontend 的
# 流量并不均匀。若把整形值均分，繁忙 frontend 会在"节点总量并没有超"
# 的情况下被本入口份额卡住（冤枉限速），空闲 frontend 的份额却白白
# 闲置。按各 Target 近 60s EWMA 用量加权，配额自动从空闲挂载点流向
# 繁忙挂载点，限的是"节点总量"而不是"单入口份额"。
#
# 单位口径：输入 bwlim_bps 与 target_ewma 均为 bytes/s（内部统一口径，
# 见 model.py 顶部说明）；输出为 int bytes/s——HAProxy map 值由
# map_str_int 查表消费，必须是整数，逐项向下取整同时保证 Σ分配 ≤ bwlim
# （各项精确值之和恰为 bwlim，floor 只会往下丢，总和绝不超发）。

from __future__ import annotations

import math

from . import model

# 保底比例：聚合整形值的 5% 拿出来在全部 Target 间均摊作保底，其余
# 95% 按用量加权。模块级常量，特殊场景可整体调整。
FLOOR_FRAC = 0.05


def allocate(
    bwlim_bps: float,
    targets: list[model.Target],
    target_ewma: dict[model.Target, float],
) -> dict[model.Target, int]:
    """把节点整形值 bwlim_bps 拆分到该节点的各 Target，返回每个 Target
    的整数 map 值（bytes/s）。

    - 保底：每个 Target 无条件获得 bwlim × FLOOR_FRAC / N；
    - 加权：其余 (1 - FLOOR_FRAC) 按 target_ewma 中的近期用量占比分配，
      缺失/非正的 EWMA 视为 0（只影响加权部分，保底仍在）；
    - Σewma == 0（比如节点刚接入毫无流量）时加权部分退化为均分；
    - 空 targets 返回空 dict（没有挂载点就没有可写的对象）；
    - 保证 Σ分配 ≤ bwlim_bps：各项在取整前的精确值之和恰等于
      bwlim_bps，逐项向下取整只减不增。
    """
    if not targets:
        return {}

    n = len(targets)
    # 负数预算没有物理意义（governor 的 bwlim 永远 ≥ quota×tighten_floor
    # > 0），钳到 0 只是防御性兜底，避免上游异常时输出负 map 值。
    budget = max(0.0, float(bwlim_bps))

    # 保底部分：FLOOR_FRAC 均摊到每个 Target。
    floor_each = budget * FLOOR_FRAC / n
    # 加权部分：剩余 (1 - FLOOR_FRAC) 的预算。
    rest = budget - floor_each * n

    # 权重取该 Target 的近 60s EWMA 用量；缺失或非正一律按 0 计——
    # EWMA 不可能为负，出现负值说明上游有 bug，按 0 处理不放大错误。
    weights = {t: max(0.0, float(target_ewma.get(t, 0.0))) for t in targets}
    total = sum(weights.values())

    out: dict[model.Target, int] = {}
    for t in targets:
        if total <= 0.0:
            # 全部 Target 都没有用量记录：无从加权，退化为均分。
            share = rest / n
        else:
            share = rest * weights[t] / total
        # 向下取整为整数 bytes/s：map_str_int 只认整数，且 floor 保证
        # 各 Target 之和不超过聚合预算（绝不超发）。
        out[t] = int(math.floor(floor_each + share))
    return out
