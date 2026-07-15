# rl_limiter.executor —— 把 governor 的决策落地到各台 HAProxy 的
# per-frontend bwlim map（设计文档 §3.2 "限速执行机制"，自 Go 版
# agent/internal/executor 完整移植）。
#
# 架构位置：executor 是核心循环"采集 → 决策 → 分配 → 执行"四段中的
# 最后一段——governor 只产出环境聚合目标值，allocator 把聚合值按各
# Target 近期用量加权拆分，executor 再通过各节点的 HAProxy runtime API
# 更新 map 条目，haproxy 配置里的 filter bwlim-out 以 map_str_int 查表
# 的方式实时读取该值完成聚合整形。
#
# v2.0 与 Go 版的结构差异：
#   - Go 版单节点单 map（一个 rt + 一个 mapPath）；v2.0 集中式服务
#     控制多台 HAProxy，因此按节点名持有 clients 与 map_paths 两张表，
#     每个 Target 用其所在节点的 client 写该节点自己的 map 路径；
#   - Go 版在 executor 内做"聚合值 ÷ frontend 数"的均分（骨架期策略）；
#     v2.0 的拆分由 allocator 按用量加权完成，apply 直接消费分配好的
#     整数值，不再自行均分；
#   - Go 版用互斥锁保护内存状态（Apply 与 SetMode 来自不同 goroutine）；
#     Python 版运行在单线程 asyncio 事件循环内，无需锁——但 apply 是
#     协程，会在 await 处让出控制权，set_mode 可能在两个 await 之间被
#     其他任务调用。因此沿用 Go 版"先取内存快照、再做 I/O、最后回写"
#     的三段结构：mode/resync/pending 在第一个 await 之前一次性取好，
#     I/O 期间的状态变更不影响本拍语义；快照/pending 的回写放在所有
#     I/O 结束之后一次完成，与 Go 版持锁回写等价。
#
# 运行模式可在 dry-run（只记日志，不碰 HAProxy）与 enforce（真实写
# map）之间热切换。从 dry-run 切到 enforce 时会武装一次性的 resync
# 标志：dry-run 期间 HAProxy 里的真实 map 值没有被更新过，可能已经
# 与 governor 的目标值脱节，因此切换后的第一个非空 apply 必须无条件
# 全量重写，使真实状态一次性收敛到目标状态。

from __future__ import annotations

import asyncio
import logging
from typing import Any

from . import model


class Executor:
    """可热切换 dry-run / enforce 的执行器（Go 版 Switchable 的移植）。

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
    ) -> None:
        self._clients = dict(clients)
        self._map_paths = dict(map_paths)
        self._log = log if log is not None else logging.getLogger(__name__)
        # resync 在切换到 enforce 时被武装（一次性标志）：下一个非空
        # apply 会把所有决策一律视为"已变化"全量重写。语义上它表示
        # "HAProxy 的真实 map 状态与 governor 目标状态之间的一致性
        # 未知，需要一次全量收敛"。空 apply 不消费该标志——没有决策
        # 可写时"重写"无从谈起，标志必须留到真正有决策的那一拍。
        self._resync = False
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
        self._mode = self._valid_mode(mode)

    def _valid_mode(self, mode: str) -> str:
        """把任意输入收敛到受支持的模式：除两个已知模式外一律降级到
        dry-run 并告警。降级方向选择 dry-run 是出于安全考虑——配置
        写错时宁可"不动 HAProxy"也不能"意外真实写入"。"""
        if mode in (model.MODE_DRY_RUN, model.MODE_ENFORCE):
            return mode
        self._log.warning("invalid executor mode; falling back to dry-run mode=%s", mode)
        return model.MODE_DRY_RUN

    def set_mode(self, mode: str) -> None:
        """切换运行模式。真正切到 enforce 时武装 resync 标志，让下一个
        apply 全量重写（dry-run 期间只记了日志，HAProxy 的 map 里可能
        是陈旧值）；切到 dry-run 时清空 pending 重试状态——只记日志的
        模式下没有需要收敛的真实状态，留着重试记录反而会在下次回到
        enforce 时造成误重试（届时 resync 会做一次全量覆盖，pending
        已无意义）。"""
        m = self._valid_mode(mode)
        if m == self._mode:
            return
        prev = self._mode
        self._mode = m
        self._resync = m == model.MODE_ENFORCE
        if m == model.MODE_DRY_RUN:
            self._pending = set()
        self._log.info(
            "executor mode changed mode_from=%s mode_to=%s resync_armed=%s",
            prev, m, self._resync,
        )

    def mode(self) -> str:
        """返回当前运行模式。"""
        return self._mode

    def snapshot(self) -> dict[str, float]:
        """返回每个 env 最近一次落地/记录的聚合 bwlim（bytes/s）的
        副本，供状态上报使用。"""
        return dict(self._last_applied)

    async def apply(
        self,
        items: list[tuple[model.Decision, dict[model.Target, int]]],
    ) -> list[Exception]:
        """执行一拍的全部决策。items 的每个元素是 (决策, 分配结果)：
        分配结果由 allocator 产出，给出该环境每个 Target 的整数 map 值
        （bytes/s）。

        跳过规则：changed=False 的决策默认跳过，但有两个例外——
        (a) 切换到 enforce 后武装的一次性 resync 生效中（全量重写）；
        (b) 该 env 上一次 enforce 写入失败，处于 pending 重试（governor
        发射后即自行推进 last_emitted，不会替 executor 重发，重试责任
        必须由 executor 承担，否则 HAProxy 会一直挂着旧限速值直到目标
        值下次漂移，详见 _pending 字段注释）。

        错误语义：enforce 写失败不会中断本拍——同一 env 的其余 Target
        与其余决策照常执行，所有异常收集后一并返回（列表，调用方决定
        日志级别）；只有该 env 全部 Target 都写成功才计入 snapshot，
        任一失败则整个 env 进入 pending。同一 env 的多个 Target 用
        asyncio.gather 并发写（分布在不同节点，socket 往返可重叠），
        单点失败不阻断其他写入。
        """
        # —— 第一段：取内存快照（第一个 await 之前，原子）——
        # 对应 Go 版持锁读取 mode/resync/pending 后立即放锁的结构：
        # 本拍的行为完全由这一刻的快照决定，I/O 期间其他任务对模式的
        # 修改只影响下一拍。
        mode = self._mode
        resync = self._resync
        if items:
            self._resync = False  # 一次性消费；空拍不得消费（无决策可重写）
        pending = set(self._pending)

        if resync and items:
            self._log.info(
                "resync triggered; rewriting all decisions mode=%s decision_count=%d",
                mode, len(items),
            )

        # —— 第二段：逐决策执行 I/O（不触碰共享可变状态）——
        errs: list[Exception] = []
        applied: dict[str, float] = {}
        failed: set[str] = set()

        for d, alloc in items:
            if not d.changed and not resync and d.env_id not in pending:
                continue
            if mode == model.MODE_DRY_RUN:
                if not d.changed and not resync:
                    # pending 是上一段 enforce 期间留下的陈旧状态，
                    # dry-run 下没有真实写入需要重试，直接跳过。
                    continue
                self._log.info(
                    "DRY-RUN would set bwlim env=%s state=%s bwlim_bytes_per_sec=%s targets=%s",
                    d.env_id, d.state, d.bwlim_bps, [str(t) for t in d.targets],
                )
                applied[d.env_id] = d.bwlim_bps
                continue
            if not d.targets:
                self._log.warning(
                    "decision has no targets; nothing to apply env=%s", d.env_id
                )
                continue

            retry = d.env_id in pending
            # 并发写该 env 的全部 Target：return_exceptions=True 保证
            # 单点失败不取消其余写入，异常随结果一起返回。
            results = await asyncio.gather(
                *(
                    self._write_target(d.env_id, t, int(alloc.get(t, 0)))
                    for t in d.targets
                ),
                return_exceptions=True,
            )
            env_errs = [r for r in results if isinstance(r, Exception)]
            errs.extend(env_errs)

            if not env_errs:
                if retry:
                    self._log.info(
                        "pending bwlim retry succeeded env=%s bwlim_bytes_per_s=%s",
                        d.env_id, d.bwlim_bps,
                    )
                applied[d.env_id] = d.bwlim_bps
            else:
                self._log.warning(
                    "bwlim write failed; env queued for retry env=%s failed_targets=%d",
                    d.env_id, len(env_errs),
                )
                failed.add(d.env_id)

        # —— 第三段：回写快照与重试集合（所有 I/O 结束之后一次完成，
        # 对应 Go 版持锁回写）：写成功的 env 更新快照并移出 pending，
        # 写失败的 env 记入 pending 留待下一拍强制重写。——
        for env_id, v in applied.items():
            self._last_applied[env_id] = v
            self._pending.discard(env_id)
        for env_id in failed:
            self._pending.add(env_id)
        return errs

    async def _write_target(self, env_id: str, t: model.Target, value: int) -> None:
        """把单个 Target 的整数 map 值写入其所在节点的 HAProxy。

        节点不在 clients / map_paths 中属于配置不一致（Target 引用了
        未声明的节点），记 error 日志并抛异常——按"该 Target 写失败"
        处理，使整个 env 进入 pending，等待配置修复后由重试路径收敛。
        map 值以十进制字符串下发（与 Go 版 strconv.FormatInt 一致，
        runtime API 的 set map 命令按文本协议传值）。
        """
        client = self._clients.get(t.node)
        map_path = self._map_paths.get(t.node)
        if client is None or map_path is None:
            self._log.error(
                "no runtime client or map path for node; target write failed "
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
            "bwlim map entry written env=%s target=%s value_bytes_per_s=%d map_path=%s",
            env_id, t, value, map_path,
        )
