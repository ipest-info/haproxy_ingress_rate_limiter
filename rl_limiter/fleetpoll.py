# rl_limiter.fleetpoll —— 总控的节点轮询与下发执行。
#
# 职责：
#   - 维护节点状态表（NodeState，见 fleetgov）：每 poll_interval_s 并发
#     GET 各节点 /api/overview，提取观测（出向/入向/配额/degraded/可执行）；
#   - 在线判定：连续 offline_after 次失败判离线（状态翻转打 WARN）；
#     恢复在线时**重置需求 EWMA**——陈旧均值不配参与首轮分配；
#   - 下发执行：把 fleetgov 的写清单并发 PUT 到各节点
#     /api/instance-quota（带节点令牌；失败只记 write_err，不重试——
#     下一个分配周期自然重试）。
#
# 观测口径与节点闭环同源——**网卡口径**（按量计费计的是网卡字节）：
# 优先 latest.regulation.instance.egress_mbps（节点闭环的观测值 =
# 客户端侧网卡流出 10s 均值）；节点还没在调（无 regulation）时退
# instance.nic_rate_out_bytes_per_s（同一张卡的瞬时差分）；连设备数据
# 都没有（节点 RL_NIC=-）才退 HAProxy 应用层口径
# sum(units[].mean10_bytes_per_s)。入向观测同链：网卡优先。
#
# 并发语义：gather + 单节点超时，一台慢/死节点绝不拖累其余节点的观测。

from __future__ import annotations

import asyncio
import logging
import time

import aiohttp

from .fleetconfig import FleetNode
from .fleetgov import NodeState


class NodePoller:
    """节点轮询器 + 下发执行器（单事件循环内使用，无锁）。"""

    def __init__(self, log: logging.Logger,
                 timeout_s: float = 3.0,
                 offline_after: int = 3):
        self._log = log
        self._timeout_s = timeout_s
        self._offline_after = offline_after
        self._states: "dict[str, NodeState]" = {}
        self._session: "aiohttp.ClientSession | None" = None

    # ---- 节点清单（配置热更的落点） ----

    def set_nodes(self, nodes: "list[FleetNode]") -> None:
        """应用节点清单（启动与热更共用）。

        新节点建状态（首次成功轮询前不参与分配——fleetgov 按 quota
        未知处理）；已有节点就地更新接线；移出清单的节点停止轮询与
        下发，**配额不回收不清零**（移除 ≠ 释放，乱动一台不再归管的
        机器违反最小惊讶）。
        """
        seen = set()
        for n in nodes:
            seen.add(n.name)
            s = self._states.get(n.name)
            if s is None:
                s = NodeState(name=n.name)
                self._states[n.name] = s
                self._log.info("节点加入总控清单 node=%s url=%s%s",
                               n.name, n.url,
                               ""if n.token else "（无令牌，只读观测）")
            s.url = n.url
            s.console_url = n.console_url or n.url
            s.token = n.token
        for name in list(self._states):
            if name not in seen:
                s = self._states.pop(name)
                self._log.warning(
                    "节点移出总控清单：停止轮询与下发，其实例配额保持"
                    "现状（%s），总量守恒不再包含它——需要释放请在该"
                    "节点上自行处理 node=%s",
                    f"{s.quota_mbps:g}Mbps" if s.quota_mbps else "未知/不限",
                    name)

    def states(self) -> "list[NodeState]":
        return list(self._states.values())

    # ---- HTTP ----

    async def _ensure_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=self._timeout_s),
                connector=aiohttp.TCPConnector(limit_per_host=2))
        return self._session

    async def close(self) -> None:
        if self._session is not None and not self._session.closed:
            await self._session.close()

    # ---- 轮询 ----

    async def poll_once(self) -> None:
        """并发轮询全部节点一轮。单节点的失败/超时只影响它自己。"""
        states = self.states()
        if not states:
            return
        await asyncio.gather(*(self._poll_one(s) for s in states))

    async def _poll_one(self, s: NodeState) -> None:
        try:
            session = await self._ensure_session()
            async with session.get(s.url + "/api/overview") as resp:
                if resp.status != 200:
                    raise RuntimeError(f"HTTP {resp.status}")
                ov = await resp.json()
            self._apply_overview(s, ov)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            was_online = s.online
            s.fail_count += 1
            s.error = str(e) or type(e).__name__
            if s.fail_count >= self._offline_after and (was_online
                                                        or s.last_ok_ts == 0):
                if s.online or s.fail_count == self._offline_after:
                    s.online = False
                    self._log.warning(
                        "节点判定离线（配额冻结记账，恢复后自动纳回） "
                        "node=%s fail_count=%d err=%s",
                        s.name, s.fail_count, s.error)

    def _apply_overview(self, s: NodeState, ov: dict) -> None:
        was_offline = not s.online
        s.online = True
        s.fail_count = 0
        s.last_ok_ts = time.time()
        s.error = ""
        if was_offline and s.demand_ewma_mbps is not None:
            # 恢复在线：丢掉离线前的需求均值，用新鲜观测重新起步。
            # （demand_ewma 为 None = 新节点首次联通，无均值可丢。）
            s.demand_ewma_mbps = None
            self._log.warning("节点恢复在线，重新纳入分配 node=%s", s.name)
        hap = ov.get("haproxy") or {}
        s.degraded = bool(hap.get("degraded"))
        s.executable = bool(ov.get("instance_quota_executable", True))
        try:
            s.quota_mbps = float(ov.get("instance_quota_mbps") or 0.0)
        except (TypeError, ValueError):
            s.quota_mbps = 0.0
        latest = ov.get("latest") or {}
        reg = ((latest.get("regulation") or {}).get("instance")
               if isinstance(latest.get("regulation"), dict) else None)
        if reg:
            s.egress_mbps = float(reg.get("egress_mbps") or 0.0)
            ing = reg.get("ingress_mbps")
            s.ingress_mbps = float(ing) if ing is not None else 0.0
            s.landed = bool(reg.get("landed"))
        else:
            inst = latest.get("instance") or {}
            if inst.get("nics") or inst.get("nic"):
                # 网卡口径（与节点闭环、按量计费同源）：nic_* 标量是
                # 节点锚点网卡（客户端侧执行网卡）的镜像。
                s.egress_mbps = float(
                    inst.get("nic_rate_out_bytes_per_s") or 0.0) * 8 / 1e6
                s.ingress_mbps = float(
                    inst.get("nic_rate_in_bytes_per_s") or 0.0) * 8 / 1e6
            else:
                # 节点连设备数据都没有（RL_NIC=-）：退 HAProxy 应用层
                # 口径，分配仍能工作，只是口径与计费略有出入。
                units = latest.get("units") or {}
                s.egress_mbps = sum(
                    float(u.get("mean10_bytes_per_s") or 0.0)
                    for u in units.values()) * 8 / 1e6
                s.ingress_mbps = float(
                    inst.get("rate_in_bytes_per_s") or 0.0) * 8 / 1e6
            s.landed = None

    # ---- 下发 ----

    async def apply(self, writes: list) -> None:
        """并发执行写清单 [(NodeState, Mbps)]。失败记 write_err 不重试
        ——写抑制以节点自报值为基准，没写成的下一轮自然再触发。"""
        if not writes:
            return
        await asyncio.gather(*(self._put_one(s, v) for s, v in writes))

    async def _put_one(self, s: NodeState, mbps: float) -> None:
        try:
            session = await self._ensure_session()
            async with session.put(
                    s.url + "/api/instance-quota",
                    json={"quota_mbps": mbps},
                    headers={"X-API-Token": s.token}) as resp:
                body = {}
                try:
                    body = await resp.json()
                except Exception:
                    pass
                if resp.status != 200:
                    raise RuntimeError(
                        f"HTTP {resp.status}: "
                        f"{body.get('error', '') or '无错误信息'}")
            s.applied_mbps = mbps
            s.write_err = ""
        except asyncio.CancelledError:
            raise
        except Exception as e:
            s.write_err = str(e) or type(e).__name__
            self._log.error(
                "下发实例配额失败（下一个分配周期自动重试）node=%s "
                "quota=%.1fMbps err=%s%s",
                s.name, mbps, s.write_err,
                "——401/403 请核对 fleet.yaml 里该节点的 token 与节点侧"
                " RL_API_TOKEN 是否一致"
                if "401" in s.write_err or "403" in s.write_err else "")
