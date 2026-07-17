# rl_limiter.executor —— 把 governor 的决策落地到各台 HAProxy 的
# per-frontend bwlim map（设计文档 §3.2 "限速执行机制"）。
#
# 架构位置：executor 是核心循环"采集 → 决策 → 分配 → 执行"四段中的
# 最后一段——governor 只产出节点级整形目标值，allocator 把该值按节点
# 内各 Target 近期用量加权拆分，executor 再通过各节点的 HAProxy runtime
# API 更新 map 条目，haproxy 配置里的 filter bwlim 以 map_str_int 查表
# 的方式实时读取该值完成整形。
#
# 结构要点：
#   - 集中式服务控制多台 HAProxy，因此按节点名持有 clients 与
#     map_paths 两张表，每个 Target 用其所在节点的 client 写该节点
#     自己的 map 路径；
#   - 节点整形值到各 Target 的拆分由 allocator 按用量加权完成，apply
#     直接消费分配好的整数值，不在 executor 内自行均分；
#   - 并发约定：本模块运行在单线程 asyncio 事件循环内，不需要锁——
#     但 apply 是协程，会在 await 处让出控制权，set_mode 可能在两个
#     await 之间被其他任务调用。因此采用"先取内存快照 → I/O →
#     末尾统一回写"的三段结构：mode/resync/pending 在第一个 await
#     之前一次性取好，保证本拍语义完全由快照时刻决定，I/O 期间的
#     状态变更只影响下一拍；快照/pending 的回写放在所有 I/O 结束
#     之后一次完成，避免半途让出控制权时暴露不一致的中间状态。
#
# 运行模式可在 dry-run（只记日志，不碰 HAProxy）与 enforce（真实写
# map）之间热切换，且粒度到**单个节点**：全局默认模式 + 按节点覆盖
# （node_modes），生产灰度时可以逐台 HAProxy 打开 enforce、其余节点
# 留在 dry-run 观察。某节点从 dry-run 切到 enforce 时武装该节点的
# 一次性 resync 标志：dry-run 期间该节点 map 里的真实值没有被更新过，
# 可能已与 governor 目标脱节，切换后的第一个非空 apply 必须无条件
# 重写涉及该节点的全部环境，使真实状态一次性收敛。

from __future__ import annotations

import asyncio
import logging
from typing import Any

from . import model

# 分配漂移再平衡阈值（相对变化比例）。
#
# 背景：apply 默认只在 changed=True（节点整形值变化）时写 map。稳态下
# （用量低于限制、bwlim 长期停在弹性上限不变）changed 恒为 False——
# 此时若同一节点上多个受控 frontend 的流量发生倾斜，map 里还是旧的
# 分配值，繁忙 frontend 会被过小的份额冤枉限速，而 governor 看到的
# 节点总量并未超限、永远不会触发重写。因此 executor 必须自己感知
# "分配结果相对上次落地值的漂移"：任一 Target 的分配值相对变化超过
# 该阈值（或 Target 集合变化）即强制重写该节点的整份分配。阈值过小会
# 造成每秒写 map 的抖动，过大会让倾斜迟迟得不到纠正，5% 与 AIStepFrac
# 同量级，实测每次显著倾斜后 1~2 秒内完成再平衡。
REBALANCE_EPSILON = 0.05


class Executor:
    """可热切换 dry-run / enforce 的执行器。

    clients:   节点名 → runtime client。client 只需提供
               `async set_map_entry(map_path, key, value)` 方法
               （鸭子类型，便于测试注入假对象）。
    map_paths: 节点名 → 该节点上 bwlim map 的路径（map 标识）。
    mode:      初始运行模式；非法值告警并降级到 dry-run（安全方向）。
    log:       传入 None 时回退到模块级 logger。
    """

    def __init__(
        self,
        clients: dict[str, Any],
        map_paths: dict[str, str],
        mode: str,
        log: logging.Logger | None = None,
        node_modes: dict[str, str] | None = None,
    ) -> None:
        self._clients = dict(clients)
        self._map_paths = dict(map_paths)
        self._log = log if log is not None else logging.getLogger(__name__)
        # resync 集合：某节点从 dry-run 切到 enforce 时被记入（一次性）。
        # 下一个非空 apply 会把涉及这些节点的决策一律视为"已变化"重写。
        # 语义上它表示"该节点 HAProxy 的真实 map 状态与 governor 目标
        # 状态之间的一致性未知，需要一次全量收敛"。空 apply 不消费——
        # 没有决策可写时"重写"无从谈起，标志必须留到真正有决策的那拍。
        self._resync_nodes: set[str] = set()
        # pending 记录"上一次 enforce 写入失败"的环境集合。它存在的
        # 根本原因是：governor 的 last_emitted 在发射 changed 决策时就
        # 自行推进了，并不关心 executor 是否写成功；如果 executor 不
        # 自己负责重试，写失败的环境会一直挂着旧限速值，直到目标值
        # 下一次漂移超过 epsilon 才有机会被修正——这可能是很久以后。
        # 因此写失败的 env 记入 pending，下一拍即使 changed=False 也
        # 强制重写，直到写成功为止。
        self._pending: set[str] = set()
        # last_applied 是每个 env 最近一次成功落地（enforce）或记录
        # （dry-run）的聚合 bwlim，bytes/s，供 snapshot 对外暴露。
        self._last_applied: dict[str, float] = {}
        # last_alloc 是每个 env 最近一次**成功写入 HAProxy** 的按 Target
        # 分配结果（仅含 enforce 节点上的 Target），用于分配漂移再平衡的
        # 比较基准（见 REBALANCE_EPSILON 注释）。dry-run 不追踪——没有
        # 真实落地值。
        self._last_alloc: dict[str, dict[model.Target, int]] = {}
        self._mode = self._valid_mode(mode)
        # 按节点覆盖（节点名 → 模式）；字典中不存在的节点继承 _mode。
        self._node_modes: dict[str, str] = self._valid_node_modes(node_modes or {})

    def _valid_mode(self, mode: str) -> str:
        """把任意输入收敛到受支持的模式：除两个已知模式外一律降级到
        dry-run 并告警。降级方向选择 dry-run 是出于安全考虑——配置
        写错时宁可"不动 HAProxy"也不能"意外真实写入"。"""
        if mode in (model.MODE_DRY_RUN, model.MODE_ENFORCE):
            return mode
        self._log.warning("运行模式配置非法，已安全降级为 dry-run（只记录不写入 HAProxy） mode=%s", mode)
        return model.MODE_DRY_RUN

    def _valid_node_modes(self, node_modes: dict[str, str]) -> dict[str, str]:
        """逐项收敛节点覆盖，非法值降级 dry-run（与 _valid_mode 同向）。"""
        return {n: self._valid_mode(m) for n, m in node_modes.items()}

    def _effective_mode(self, node: str) -> str:
        """节点的实际生效模式：覆盖优先，未覆盖继承全局默认。"""
        return self._node_modes.get(node, self._mode)

    def set_mode(self, mode: str, node_modes: dict[str, str] | None = None) -> None:
        """应用一份完整的模式期望状态：全局默认 + 按节点覆盖（None 视作
        无覆盖）。按**每个节点的生效模式变化**决定动作：

        - 某节点 dry-run → enforce：把该节点记入 resync 集合，下一拍
          重写涉及它的全部环境（dry-run 期间该节点 map 里可能是陈旧值）；
        - 全部节点都变为 dry-run：清空 pending 重试与落地基准——只记
          日志的模式下没有需要收敛的真实状态，留着反而会在下次回到
          enforce 时造成误重试（届时 resync 会全量覆盖，pending 无意义）；
        - 生效模式完全没变：no-op，不武装任何 resync。
        """
        new_mode = self._valid_mode(mode)
        new_overrides = self._valid_node_modes(node_modes or {})

        # 已知节点全集上的生效模式对比（clients 即受控节点清单）。
        def eff(default: str, ov: dict[str, str], n: str) -> str:
            return ov.get(n, default)

        changed_nodes = {
            n for n in self._clients
            if eff(self._mode, self._node_modes, n)
            != eff(new_mode, new_overrides, n)
        }
        if not changed_nodes and new_mode == self._mode \
                and new_overrides == self._node_modes:
            return

        prev_mode, prev_overrides = self._mode, self._node_modes
        self._mode, self._node_modes = new_mode, new_overrides

        armed = {
            n for n in changed_nodes
            if eff(prev_mode, prev_overrides, n) == model.MODE_DRY_RUN
            and self._effective_mode(n) == model.MODE_ENFORCE
        }
        self._resync_nodes |= armed

        if all(self._effective_mode(n) == model.MODE_DRY_RUN
               for n in self._clients):
            self._pending = set()
            # 落地值基准一并清空：dry-run 期间真实 map 状态会与内存脱节，
            # 回到 enforce 时由 resync 重写并重建基准。
            self._last_alloc = {}
            self._resync_nodes = set()

        self._log.info(
            "执行模式已切换（dry-run→enforce 的节点武装 resync，下一拍重写涉及它的环境） "
            "default_from=%s default_to=%s node_overrides=%s "
            "changed_nodes=%s resync_armed=%s",
            prev_mode, new_mode,
            ";".join(f"{n}={m}" for n, m in sorted(new_overrides.items())) or "-",
            ",".join(sorted(changed_nodes)) or "-",
            ",".join(sorted(armed)) or "-",
        )

    def mode(self) -> str:
        """返回全局默认运行模式（未被覆盖的节点继承它）。"""
        return self._mode

    def node_modes(self) -> dict[str, str]:
        """返回按节点的模式覆盖副本（控制台展示用）。"""
        return dict(self._node_modes)

    def snapshot(self) -> dict[str, float]:
        """返回每个 env 最近一次落地/记录的聚合 bwlim（bytes/s）的
        副本，供状态展示使用。"""
        return dict(self._last_applied)

    async def apply(
        self,
        items: list[tuple[model.Decision, dict[model.Target, int]]],
    ) -> list[Exception]:
        """执行一拍的全部决策。items 的每个元素是 (决策, 分配结果)：
        分配结果由 allocator 产出，给出该环境每个 Target 的整数 map 值
        （bytes/s）。

        每个 Target 按其**所在节点的生效模式**处理：enforce 节点真实
        写 map，dry-run 节点只记演练日志——同一环境可以横跨两种模式的
        节点（生产灰度的常态）。

        跳过规则：changed=False 的决策默认跳过，但有三个例外——
        (a) 该环境有 Target 落在刚切到 enforce 的节点上（节点级一次性
        resync，重写该环境）；
        (b) 该 env 上一次 enforce 写入失败，处于 pending 重试（governor
        发射后即自行推进 last_emitted，不会替 executor 重发，重试责任
        必须由 executor 承担，否则 HAProxy 会一直挂着旧限速值直到目标
        值下次漂移，详见 _pending 字段注释）；
        (c) enforce 侧分配结果相对上次落地值漂移超过阈值（再平衡）。

        错误语义：enforce 写失败不会中断本拍——同一 env 的其余 Target
        与其余决策照常执行，所有异常收集后一并返回（列表，调用方决定
        日志级别）；只有该 env 全部 enforce Target 都写成功才计入
        snapshot，任一失败则整个 env 进入 pending。同一 env 的多个
        Target 用 asyncio.gather 并发写（分布在不同节点，socket 往返
        可重叠），单点失败不阻断其他写入。
        """
        # —— 第一段：取内存快照（第一个 await 之前，原子）——
        # 本拍的行为完全由这一刻的快照决定，I/O 期间其他任务对模式的
        # 修改只影响下一拍。
        default_mode = self._mode
        node_modes = dict(self._node_modes)
        resync_nodes = set(self._resync_nodes)
        if items:
            self._resync_nodes = set()  # 一次性消费；空拍不得消费
        pending = set(self._pending)

        def eff(node: str) -> str:
            return node_modes.get(node, default_mode)

        if resync_nodes and items:
            self._log.info(
                "节点切换到 enforce 后的首拍重写：这些节点真实 map 状态一致性未知，"
                "无条件下发涉及它们的环境整形值 resync_nodes=%s decision_count=%d",
                ",".join(sorted(resync_nodes)), len(items),
            )

        # —— 第二段：逐决策执行 I/O（不触碰共享可变状态）——
        errs: list[Exception] = []
        applied: dict[str, float] = {}
        applied_alloc: dict[str, dict[model.Target, int]] = {}
        failed: set[str] = set()

        for d, alloc in items:
            if not d.targets:
                self._log.warning(
                    "决策不含任何挂载点，无法执行（请检查环境的 targets 配置） env=%s", d.env_id
                )
                continue

            # 按节点生效模式把挂载点分成"真实写入"与"演练"两组。
            enf_targets = [t for t in d.targets if eff(t.node) == model.MODE_ENFORCE]
            dry_targets = [t for t in d.targets if eff(t.node) == model.MODE_DRY_RUN]
            node_resync = any(t.node in resync_nodes for t in enf_targets)

            # 分配漂移再平衡（仅 enforce 侧有意义）：changed/resync/pending
            # 都未触发时，检查本拍 enforce 侧分配结果相对上次成功落地值的
            # 漂移，承接原慢环"节点间配额再平衡"的职责（REBALANCE_EPSILON）。
            # 比较基准 _last_alloc 只含 enforce Target：节点模式翻转会改变
            # 集合构成，_alloc_drift 视集合变化为必然超阈值，自然触发重写。
            drift = 0.0
            if (
                enf_targets
                and not d.changed
                and not node_resync
                and d.env_id not in pending
            ):
                enf_alloc = {t: int(alloc.get(t, 0)) for t in enf_targets}
                drift = self._alloc_drift(d.env_id, enf_alloc)
                if drift > REBALANCE_EPSILON:
                    self._log.info(
                        "挂载点分配相对上次落地值漂移超过阈值，触发节点内再平衡重写 "
                        "env=%s max_drift=%.3f alloc=%s",
                        d.env_id, drift,
                        {str(t): v for t, v in enf_alloc.items()},
                    )
            if (
                not d.changed
                and not node_resync
                and d.env_id not in pending
                and drift <= REBALANCE_EPSILON
            ):
                continue

            # 演练侧：只在决策变化或 resync 时记日志（漂移/pending 是
            # enforce 侧的触发原因，替演练节点刷日志只会制造噪音）。
            if dry_targets and (d.changed or node_resync):
                self._log.info(
                    "【DRY-RUN 演练】本应下发整形值（未真实写入 HAProxy） "
                    "env=%s state=%s bwlim_bytes_per_sec=%s targets=%s alloc=%s",
                    d.env_id, d.state, d.bwlim_bps,
                    [str(t) for t in dry_targets],
                    {str(t): int(alloc.get(t, 0)) for t in dry_targets},
                )

            if not enf_targets:
                # 纯演练环境：记录为"已应用"（含把遗留 pending 洗掉——
                # 没有 enforce 挂载点就没有需要重试的真实写入）。
                applied[d.env_id] = d.bwlim_bps
                continue

            retry = d.env_id in pending
            # 并发写该 env 的全部 enforce Target：return_exceptions=True
            # 保证单点失败不取消其余写入，异常随结果一起返回。
            results = await asyncio.gather(
                *(
                    self._write_target(d.env_id, t, int(alloc.get(t, 0)))
                    for t in enf_targets
                ),
                return_exceptions=True,
            )
            env_errs = [r for r in results if isinstance(r, Exception)]
            errs.extend(env_errs)

            if not env_errs:
                if retry:
                    self._log.info(
                        "重试队列中的环境整形值写入成功，移出队列恢复正常 env=%s bwlim_bytes_per_s=%s",
                        d.env_id, d.bwlim_bps,
                    )
                applied[d.env_id] = d.bwlim_bps
                applied_alloc[d.env_id] = {
                    t: int(alloc.get(t, 0)) for t in enf_targets
                }
            else:
                self._log.warning(
                    "整形值写入失败，该环境已加入重试队列（下一拍强制重写） env=%s failed_targets=%d",
                    d.env_id, len(env_errs),
                )
                failed.add(d.env_id)

        # —— 第三段：回写快照与重试集合（所有 I/O 结束之后一次完成，
        # 不与 I/O 交错，避免在 await 让出点暴露中间状态）：写成功的
        # env 更新快照并移出 pending，写失败的 env 记入 pending 留待
        # 下一拍强制重写。——
        for env_id, v in applied.items():
            self._last_applied[env_id] = v
            self._pending.discard(env_id)
        for env_id, a in applied_alloc.items():
            self._last_alloc[env_id] = a
        for env_id in failed:
            self._pending.add(env_id)
        return errs

    def _alloc_drift(self, env_id: str, alloc: dict[model.Target, int]) -> float:
        """计算本拍分配结果相对上次成功落地值的最大相对漂移。

        返回值语义：0.0 表示无基准或完全一致；Target 集合发生增删视为
        无穷大漂移（返回一个必然超阈值的常数）——集合变化意味着挂载点
        拓扑变了，必须立即重写。相对漂移分母取旧值；旧值为 0 而新值
        非 0 时同样视为必然超阈值（从"无份额"到"有份额"没有比例可言）。
        """
        last = self._last_alloc.get(env_id)
        if last is None:
            # 无基准：首次写入必然由 changed=True（governor 首拍强制发射）
            # 或 resync 触发，这里不重复触发。
            return 0.0
        if set(last.keys()) != set(alloc.keys()):
            return float("inf")
        drift = 0.0
        for t, new in alloc.items():
            old = last[t]
            if old == 0:
                if new != 0:
                    return float("inf")
                continue
            drift = max(drift, abs(new - old) / old)
        return drift

    async def _write_target(self, env_id: str, t: model.Target, value: int) -> None:
        """把单个 Target 的整数 map 值写入其所在节点的 HAProxy。

        节点不在 clients / map_paths 中属于配置不一致（Target 引用了
        未声明的节点），记 error 日志并抛异常——按"该 Target 写失败"
        处理，使整个 env 进入 pending，等待配置修复后由重试路径收敛。
        map 值以十进制字符串下发——runtime API 的 set map 命令按
        文本协议传值。
        """
        client = self._clients.get(t.node)
        map_path = self._map_paths.get(t.node)
        if client is None or map_path is None:
            self._log.error(
                "节点未配置 runtime client 或 map 路径，该挂载点写入失败"
                "（配置不一致，环境将进入重试队列等待配置修复） "
                "env=%s target=%s node=%s",
                env_id, t, t.node,
            )
            raise KeyError(f"set bwlim env={env_id} target={t}: node {t.node!r} not configured")
        try:
            await client.set_map_entry(map_path, t.frontend, str(value))
        except Exception as exc:
            # 保留原始异常链，外层收集后由调用方决定日志级别。
            raise RuntimeError(f"set bwlim env={env_id} target={t}: {exc}") from exc
        self._log.info(
            "整形值已写入节点 map env=%s target=%s value_bytes_per_s=%d map_path=%s",
            env_id, t, value, map_path,
        )
