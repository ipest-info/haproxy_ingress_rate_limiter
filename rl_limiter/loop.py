# rl_limiter.loop —— 集中式快环（主控制循环，控制单元=节点）。
#
# 每个 tick 按固定流水线执行"采集 → 决策 → 分配 → 执行 → 上报"。
# 其中"分配"一步：决策器产出的是节点级整形值（bwlim_bps），一台节点
# 可能挂载多个受控 frontend，需要按各挂载点（Target = 节点 × frontend）
# 近期用量加权拆分后才能写回该节点的 bwlim map（见 allocator 模块头；
# 拆分只发生在单台节点内部，节点之间没有配额调配）。
#
# 并发模型：整个循环运行在单个 asyncio 任务中，组件间不会并发访问，
# 因此无需任何锁；配置通过 asyncio.Queue 注入，时间通过 tick_interval_s
# 对齐的 asyncio.sleep 推进。

from __future__ import annotations

import asyncio
import logging
import time

from . import allocator, model

# 周期状态汇总日志的输出频率：每 60 个 tick（约每分钟）输出一条 info，
# 便于在正常运行时低成本确认"循环活着、配置版本正确、各环境用量正常"。
STATUS_SUMMARY_EVERY_TICKS = 60


def executor_mode(executor) -> str:
    """兼容两种执行器契约：mode 既可能是属性/property，也可能是方法。

    并行开发的 Executor 模块尚未定版，这里做一次运行时探测以解耦：
    取到可调用对象就调用它，否则按属性值直接使用。
    """
    m = getattr(executor, "mode", "")
    return m() if callable(m) else m


def _summarize_quotas(envs: list[model.EnvQuota]) -> str:
    """把环境配额压缩成单个日志字段，格式 "env1=200000000;env2=..."，
    数值为配置口径的 bits/s。"""
    return ";".join(f"{e.env_id}={e.quota_bits_per_sec}" for e in envs)


def _summarize_usages(usages: list[model.EnvUsage]) -> str:
    """把各环境用量压缩成单个日志字段，格式
    "env1:mean10_bytes_per_s=12345,conn=6;env2:..."。mean10 为计费口径的
    10 秒滑动均值（bytes/s），conn 为当前并发连接数之和。"""
    return ";".join(
        f"{u.env_id}:mean10_bytes_per_s={u.mean10_bps:.0f},conn={u.conn_cur}"
        for u in usages
    )


class ControlLoop:
    """rl-limiter 的集中式快速控制环。

    组件契约（与并行开发的模块约定）：
      - collector.set_mapping(dict[Target, str])；async collector.tick(now)
        -> list[EnvUsage]；collector.degraded_nodes() -> set[str]
      - governor.update_config(list[EnvQuota])；governor.tick(now, usages)
        -> list[Decision]
      - allocator.allocate(bwlim_bps, targets, target_ewma) -> dict[Target, int]
      - async executor.apply(list[tuple[Decision, dict[Target, int]]])
        -> list[Exception]；executor.set_mode(mode, node_modes)
        （mode 为全局默认；node_modes 为按节点覆盖，未覆盖的节点继承默认）
      - sampler 可选回调：sampler(now, usages, decisions)，供上报器缓冲样本。
    """

    def __init__(self, collector, governor, executor, sampler=None, log=None):
        self._collector = collector
        self._governor = governor
        self._executor = executor
        self._sampler = sampler
        self._log = log if log is not None else logging.getLogger("rl_limiter.loop")
        # 最近一次成功应用的 ControllerConfig 版本号；单任务访问，无需原子量。
        self._version: int = 0
        # 已处理的 tick 累计数，用于日志节流与错误定位。
        self._ticks: int = 0

    @property
    def version(self) -> int:
        """最近一次应用的配置版本号（尚未应用任何配置时为 0）。
        上报器在样本/心跳中携带它，供后台核对配置是否推送到位。"""
        return self._version

    def seed(self, cfg: model.ControllerConfig) -> None:
        """在 run 启动之前同步应用一份初始配置，即"引导"语义：让循环从
        第一个 tick 起就带着配额工作，而不是空转等管理后台。

        三个使用场景（见 __main__ 的引导优先级）：
          - 独立运行模式的本地静态配额；
          - 接入后台时的本地 envs 兜底；
          - fail-static 缓存（设计文档 §3.7：断联期间按最后一次下发的
            配置继续限速，绝不放开为不限速）。

        seed 与 run 内的配置应用走同一条 _apply_config 路径，语义完全一致。
        """
        self._apply_config(cfg)

    def _apply_config(self, cfg: model.ControllerConfig) -> None:
        """把一份配置原子地灌入三个组件：先归一化（补默认值、归一非法
        模式），再依次更新决策器的配额、采集器的 Target→env 映射、执行器
        的运行模式，最后记录版本号。调用方保证串行（seed 在 run 之前，
        run 内单任务），组件间不会看到半新半旧的配置。"""
        cfg.normalize()
        self._governor.update_config(cfg.envs)
        self._collector.set_mapping(cfg.target_to_env())
        self._executor.set_mode(cfg.mode, cfg.node_modes)
        self._version = cfg.version
        self._log.info(
            "配置已应用到快环（映射/配额/模式已更新） "
            "version=%s mode=%s node_modes=%s envs=%d env_quotas=%s",
            cfg.version, cfg.mode,
            ";".join(f"{n}={m}" for n, m in sorted(cfg.node_modes.items())) or "-",
            len(cfg.envs), _summarize_quotas(cfg.envs),
        )

    async def run(self, config_queue: asyncio.Queue | None,
                  tick_interval_s: float = 1.0) -> None:
        """驱动循环直至所在任务被取消。

        - config_queue 传递管理后台推送的新配置（Reporter.config_queue）；
          独立运行模式下传 None，循环退化为纯 tick 驱动。
        - 顺序保证：某个 tick 之前已经送达的配置，一定在处理该 tick 之前
          被应用——每拍开头先非阻塞地把队列里排队的配置全部排空再跑流水
          线。理由：若先按旧配置执行本 tick，这一秒就会按旧配额/旧模式做
          决策，对"后台刚下调配额"或"dry-run 切 enforce"这类变更意味着
          多放行一秒流量。
        - 节拍对齐：用"计算下一拍的绝对时刻再 sleep 差值"的方式推进，
          单拍处理耗时不会累积成节拍漂移。
        """
        ev = asyncio.get_running_loop()
        # 第一拍立即执行：seed 已经就位，无需白等一个周期。
        next_at = ev.time()
        while True:
            # 1) 配置优先于 tick：先排空队列中所有待应用配置（非阻塞）。
            if config_queue is not None:
                while True:
                    try:
                        cfg = config_queue.get_nowait()
                    except asyncio.QueueEmpty:
                        break
                    self._apply_config(cfg)
            # 2) 执行一次完整流水线。
            await self._tick(time.time())
            # 3) 对齐下一拍：按绝对时刻推进，防止处理耗时造成漂移。
            next_at += tick_interval_s
            now_m = ev.time()
            if next_at <= now_m:
                # 本拍耗时已超过一个周期：重新对齐到"当前时刻 + 周期"，
                # 跳过错过的拍而不是连续补拍——补拍风暴只会加重下游
                # （HAProxy runtime API）的压力，且对限速精度没有帮助。
                next_at = now_m + tick_interval_s
            await asyncio.sleep(max(0.0, next_at - ev.time()))

    async def _tick(self, now: float) -> None:
        """一次完整的快环流水线：采集 → 决策 → 分配 → 执行 → 上报。"""
        self._ticks += 1

        # 采集：所有节点的 frontend 统计按控制单元（节点）聚合。
        usages = await self._collector.tick(now)
        # 决策：AIMD 三段状态机产出各节点的整形值。
        decisions = self._governor.tick(now, usages)

        # 分配：把每个节点的整形值按其挂载点的 60s EWMA 用量加权拆分
        # 成 per-Target 的整数值（bytes/s），这是写回该节点 map 的最终值。
        ewma_by_env = {u.env_id: u.target_ewma for u in usages}
        batch: list[tuple[model.Decision, dict[model.Target, int]]] = []
        for d in decisions:
            allocations = allocator.allocate(
                d.bwlim_bps, d.targets, ewma_by_env.get(d.env_id, {}))
            batch.append((d, allocations))

        # 执行：执行失败绝不能中断循环——HAProxy 可能正在 reload（设计
        # 文档 §3.7），TCP stats socket 短暂不可用是预期内故障；下一个
        # tick 会带着新决策自然重试，残留在 HAProxy 上的旧整形值维持原样
        # （安全方向）。executor 按契约把逐条错误收集成列表返回而不抛出。
        errs = await self._executor.apply(batch)
        for err in errs or []:
            self._log.error(
                "本拍下发整形值时发生错误（循环继续，不中断限速，下拍自然重试） "
                "tick=%d err=%s", self._ticks, err)

        # 上报：把本 tick 的完整结果交给可选的 sampler 回调（上报器缓冲）。
        if self._sampler is not None:
            self._sampler(now, usages, decisions)

        # 周期状态汇总：每 STATUS_SUMMARY_EVERY_TICKS 个 tick 输出一次，
        # 正常运行时以约 1 条/分钟的成本留下可核对的运行痕迹。
        if self._ticks % STATUS_SUMMARY_EVERY_TICKS == 0:
            degraded = sorted(self._collector.degraded_nodes())
            self._log.info(
                "运行状态周期汇总（每 60 拍输出一次） "
                "tick=%d config_version=%s mode=%s envs=%d "
                "degraded_nodes=%s envs_summary=%s",
                self._ticks, self._version, executor_mode(self._executor),
                len(usages), ",".join(degraded) if degraded else "-",
                _summarize_usages(usages),
            )
