# rl_limiter.reqgov —— 根据带宽动态限制请求接纳（AIMD 控制环）。
#
# ## 它做什么
#
# tc 限的是字节速率（带宽）；本模块在它之上加一个**请求侧**的动态闸：
# 持续对比各 frontend 的 10 秒均值带宽与登记限额（quotas），超限就经
# HAProxy runtime API 把该 frontend 的并发接纳上限（`set maxconn
# frontend`）**乘性收缩**，回落后**加性恢复**——经典 AIMD，收得快、
# 放得稳。效果：带宽顶着限额跑的时候，新请求在 HAProxy 门口被排队/
# 拒绝，而不是放进来再被 tc 慢慢掐。
#
# ## 口径的诚实说明
#
# HAProxy runtime API 没有"每秒请求数"的 per-frontend 旋钮；运行期可调
# 的请求侧闸是 **maxconn（并发接纳上限）**——压并发即压请求接纳速率，
# 两者在长连接场景下不是同一回事，但这是 reload-free 的全部可调面。
# 真要精确的 req/s 限速是 haproxy.cfg 里的 stick-table/http_req_rate，
# 那是静态配置，与本模块互补。
#
# ## 与本仓库"只读 HAProxy"承诺的关系
#
# 主分支的 rl-limiter **不写任何 HAProxy 状态**。本分支是刻意的例外：
# 动态调节本质上就是写（且只写这一个旋钮，不碰权重/上下线）。代价是
# stats socket 需要 **level admin**（实测 HAProxy 2.8：level user 与
# level operator 都会拒绝 set maxconn frontend——首次被拒会告警一次并
# 给出改法）。admin 级 socket 的暴露面大，**务必用本机 unix socket +
# mode 660**，绝不要把 admin 级开在 TCP 上。
#
# ## 失效安全
#
#   - 采样降级（degraded）时**不动作**——按陈旧数据调闸比不调更糟；
#   - 永不压到 min_conns 以下——把入口饿死比超限更糟；
#   - 限额取消/清零时一次性恢复到基准（frontend 自己的 maxconn，取
#     show stat 的 slim 列；cfg 没给 frontend 级 maxconn 则用配置的
#     max_conns 兜底）；
#   - 本进程崩溃/重启不留隐患：maxconn 是 HAProxy 的运行期值，reload
#     即回到 cfg 配置值；本模块启动时也会把受管 frontend 恢复到基准
#     再开始调（上一次进程留下的压缩值不该被继承）。

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass

from .haproxy import CommandError, RuntimeClient


@dataclass
class ReqGovConfig:
    """动态请求限速的控制参数（YAML 的 dynamic_request_limit 段）。"""

    enabled: bool = False
    # 收缩下限：无论超限多严重，接纳上限绝不压到它以下。
    min_conns: int = 50
    # frontend 没配自己的 maxconn（slim=0）时的恢复基准。
    max_conns: int = 100_000
    # mean10 > quota×high_ratio → 收；mean10 < quota×low_ratio → 放。
    # 两者之间是滞回带，不动作——没有滞回会在限额线附近来回抖。
    high_ratio: float = 1.0
    low_ratio: float = 0.85
    # 乘性收缩系数（每次收到 cur×decrease）。
    decrease: float = 0.7
    # 调节节拍（秒）。mean10 本身已平滑，3s 足够快又不抖。
    adjust_interval_s: float = 3.0


class RequestGovernor:
    """带宽驱动的请求接纳调节器（sampler 喂数据，自带任务下发命令）。"""

    def __init__(self, client: RuntimeClient, cfg: ReqGovConfig,
                 frontends_fn, log: logging.Logger):
        self._client = client
        self._cfg = cfg
        # 当前受管清单（名字→FrontendConfig），每轮现取（热更新跟随）。
        self._frontends_fn = frontends_fn
        self._log = log
        # 最近一拍的用量（sampler 回调喂进来；None = 还没采到样）。
        self._latest = None
        self._latest_ts = 0.0
        # frontend 名 → 当前由我们设置的接纳上限（不在表里 = 在基准值）。
        self._cur: dict[str, int] = {}
        # frontend 名 → 恢复基准（slim 或 max_conns），懒加载缓存。
        self._ceilings: dict[str, int] = {}
        self._warned_permission = False

    # ---- sampler 回调（与 hub/mlog 并列的第三个出口）----

    def record(self, now: float, usages, instance=None) -> None:
        self._latest = list(usages)
        self._latest_ts = now

    # ---- 基准值 ----

    async def _refresh_ceilings(self) -> None:
        """从 show stat 的 slim 列取各 frontend 配置的 maxconn。

        **只记首次看到的值**：slim 反映的是运行期 maxconn（实测），我们
        压缩之后它就是压缩值——无条件覆盖会把压缩值当成恢复基准，越恢复
        越矮。已经被我们动过（在 _cur 里）的 frontend 一律不更新基准。
        失败只告警——基准拿不到时先用 max_conns 兜底，不阻塞调节。
        """
        try:
            stats = await self._client.show_stat()
        except Exception as e:
            self._log.debug("读取 frontend maxconn 基准失败，用配置兜底 "
                            "err=%s", e)
            return
        for s in stats:
            if (s.sess_limit > 0 and s.name not in self._ceilings
                    and s.name not in self._cur):
                self._ceilings[s.name] = s.sess_limit

    def _ceiling(self, name: str) -> int:
        return self._ceilings.get(name, self._cfg.max_conns)

    # ---- 命令下发 ----

    async def _set_maxconn(self, name: str, value: int) -> bool:
        try:
            await self._client.exec_cmd(f"set maxconn frontend {name} {value}")
            return True
        except CommandError as e:
            if "Permission denied" in e.reply and not self._warned_permission:
                self._warned_permission = True
                self._log.error(
                    "HAProxy 拒绝 set maxconn（stats socket 权限不足）——"
                    "动态请求限速需要 **level admin**（实测 operator 也不"
                    "够）：把本机 unix stats socket 升为 level admin"
                    "（保持 mode 660，绝不要把 admin 级开在 TCP 上）并 "
                    "reload。在此之前本模块只观测不动作 err=%s", e)
            else:
                self._log.warning("set maxconn 失败，本轮跳过 frontend=%s "
                                  "value=%d err=%s", name, value, e)
            return False
        except Exception as e:
            self._log.warning("set maxconn 失败，本轮跳过 frontend=%s "
                              "value=%d err=%s", name, value, e)
            return False

    async def _restore(self, name: str) -> None:
        """恢复到基准并忘掉状态（限额取消/段消失/启动清场）。"""
        ceiling = self._ceiling(name)
        if await self._set_maxconn(name, ceiling):
            self._log.warning(
                "已把 frontend 的请求接纳上限恢复到基准 frontend=%s "
                "maxconn=%d", name, ceiling)
        self._cur.pop(name, None)

    # ---- 控制环 ----

    async def adjust_once(self) -> None:
        """跑一轮 AIMD 调节（供 run() 周期调用；测试直接调它）。"""
        usages = self._latest
        if usages is None:
            return
        limited = {f.name: f for f in self._frontends_fn() if f.limited}

        # 不再受管（限额删了/段没了）的 frontend：恢复基准。
        for name in [n for n in self._cur if n not in limited]:
            await self._restore(name)

        by_name = {u.name: u for u in usages}
        for name, f in limited.items():
            u = by_name.get(name)
            if u is None or u.degraded:
                continue                     # 没采到/陈旧数据：不动作
            quota = f.quota_bytes_per_sec
            if quota <= 0:
                continue
            if name not in self._ceilings:
                await self._refresh_ceilings()
            ceiling = self._ceiling(name)
            cur = self._cur.get(name, ceiling)

            if u.mean10_bps > quota * self._cfg.high_ratio:
                new = max(self._cfg.min_conns,
                          int(cur * self._cfg.decrease))
                if new < cur:
                    if await self._set_maxconn(name, new):
                        self._cur[name] = new
                        self._log.warning(
                            "带宽超限，已收缩请求接纳上限（乘性收缩） "
                            "frontend=%s mean10=%.0fB/s quota=%.0fB/s "
                            "maxconn=%d→%d", name, u.mean10_bps, quota,
                            cur, new)
            elif (u.mean10_bps < quota * self._cfg.low_ratio
                  and cur < ceiling):
                step = max(1, ceiling // 20)
                new = min(ceiling, cur + step)
                if await self._set_maxconn(name, new):
                    if new >= ceiling:
                        self._cur.pop(name, None)
                    else:
                        self._cur[name] = new
                    self._log.info(
                        "带宽回落，已放宽请求接纳上限（加性恢复） "
                        "frontend=%s maxconn=%d→%d（基准 %d）",
                        name, cur, new, ceiling)

    async def run(self) -> None:
        """常驻任务：启动先清场（恢复基准），然后按节拍调节。"""
        # 上一个进程实例可能把 maxconn 压着就崩了；HAProxy 不会自己
        # 恢复（直到 reload）。启动时把所有受管 frontend 拉回基准，
        # 从干净状态开始。
        await self._refresh_ceilings()
        for f in self._frontends_fn():
            if f.limited:
                await self._set_maxconn(f.name, self._ceiling(f.name))
        while True:
            try:
                await self.adjust_once()
            except asyncio.CancelledError:
                raise
            except Exception as e:          # 常驻任务不能因单轮异常退出
                self._log.error("动态请求限速一轮调节出现未预期异常，"
                                "本轮跳过 err=%s", e)
            await asyncio.sleep(self._cfg.adjust_interval_s)
