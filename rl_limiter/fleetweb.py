# rl_limiter.fleetweb —— 总控制台的发布枢纽与 HTTP 面。
#
# 结构照抄节点侧 webconsole（同一套纪律，便于两边对照维护）：
#   - FleetHub ≙ StatusHub：轮询环每拍喂一帧 fleet 快照 → 留存历史 +
#     广播给 SSE 订阅者；overview/history 是 REST 拉取口径；
#   - build_fleet_app ≙ build_app：静态页 + 只读 API + 写 API（总带宽、
#     节点清单的添加/编辑/删除；令牌鉴权，回写 fleet YAML 热生效）；
#   - run_fleet_console ≙ run_console：出厂默认绑 0.0.0.0（fleetmain 的
#     DEFAULT_CONSOLE_BIND），绑非回环打 WARN 提醒防火墙限源。
#
# 安全边界与节点侧相同：读接口无鉴权（节点清单里的 token **绝不出现**
# 在任何读接口里）；写接口必须令牌。

from __future__ import annotations

import asyncio
import collections
import datetime as _dt
import hmac
import json
import logging
import time
from pathlib import Path
from typing import Any, Callable

from aiohttp import web

from . import fleetconfig, fleetmetrics, fleetopenapi, tokenpolicy
from .webconsole import HISTORY_TICKS, LOGS_PAGE_LIMIT, LogBuffer  # noqa: F401

_STATIC_DIR = Path(__file__).parent / "static"

SUBSCRIBER_QUEUE_DEPTH = 5


def fleet_snapshot(now: float, states: list, total_mbps: float) -> dict:
    """把一轮轮询的节点状态摊成 fleet 快照（页面/SSE 的一帧）。

    单位一律 Mbps（总控的量本来就以 Mbps 流转，与节点 regulation 段
    同一口径）。allocated_mbps = Σ分配 + 冻结记账——它 ≈ 总带宽是总量
    守恒的现场证据，页面拿它和 total 对照展示。
    """
    nodes: dict[str, Any] = {}
    egress = ingress = allocated = 0.0
    online = 0
    for s in states:
        if s.online:
            online += 1
            egress += s.egress_mbps
            ingress += s.ingress_mbps
        if s.desired_mbps is not None:
            allocated += s.desired_mbps
        elif s.quota_mbps:
            allocated += s.quota_mbps          # 冻结节点按自报值记账
        util = (round(s.egress_mbps / s.quota_mbps, 3)
                if s.online and s.quota_mbps else None)
        nodes[s.name] = {
            "online": s.online,
            "degraded": s.degraded,
            "executable": s.executable,
            "egress_mbps": round(s.egress_mbps, 2),
            "ingress_mbps": round(s.ingress_mbps, 2),
            "quota_mbps": s.quota_mbps,
            "desired_mbps": s.desired_mbps,
            "util": util,
            "landed": s.landed,
            "console_url": s.console_url,
            "controllable": bool(s.token),
            "error": s.error,
            "write_err": s.write_err,
            # 总量控制已取消、该节点的限速还没解除（离线/下发失败中）。
            "release_pending": s.release_pending,
            # 固定限速（Mbps；0 = 按需分配）。
            "fixed_mbps": s.fixed_mbps,
        }
    return {
        "ts": now,
        "total_quota_mbps": total_mbps,
        "egress_mbps": round(egress, 2),
        "ingress_mbps": round(ingress, 2),
        "allocated_mbps": round(allocated, 1),
        "nodes_online": online,
        "nodes_total": len(states),
        "nodes": nodes,
    }


class FleetHub:
    """总控实时数据的发布枢纽（单事件循环内使用，无锁）。"""

    def __init__(self, service_version: str,
                 total_fn: Callable[[], float],
                 params_fn: Callable[[], dict],
                 nodes_fn: "Callable[[], list] | None" = None):
        self._service_version = service_version
        # 当前总带宽、分配参数与节点清单的延迟求值闭包（配置热更后自动
        # 跟上）。
        self._total_fn = total_fn
        self._params_fn = params_fn
        self._nodes_fn = nodes_fn or (lambda: [])
        self._history: collections.deque = collections.deque(
            maxlen=HISTORY_TICKS)
        self._subs: set = set()
        self._started = time.time()

    def record(self, snap: dict) -> None:
        self._history.append(snap)
        for q in list(self._subs):
            if q.full():
                try:
                    q.get_nowait()
                except asyncio.QueueEmpty:
                    pass
            q.put_nowait(snap)

    def subscribe(self) -> asyncio.Queue:
        q: asyncio.Queue = asyncio.Queue(SUBSCRIBER_QUEUE_DEPTH)
        self._subs.add(q)
        return q

    def unsubscribe(self, q: asyncio.Queue) -> None:
        self._subs.discard(q)

    def close(self) -> None:
        """停机：叫醒并结束全部 SSE 订阅者（同 StatusHub.close）。"""
        for q in list(self._subs):
            if q.full():
                try:
                    q.get_nowait()
                except asyncio.QueueEmpty:
                    pass
            q.put_nowait(None)

    def overview(self) -> dict:
        return {
            "service_version": self._service_version,
            "uptime_s": time.time() - self._started,
            "total_quota_mbps": self._total_fn(),
            # 分配参数的只读视图（页面"算法参数"折叠块）。
            "allocation": self._params_fn(),
            "latest": self._history[-1] if self._history else None,
        }

    def history(self) -> list:
        return list(self._history)

    def nodes(self) -> list:
        """节点清单的只读视图（页面「节点管理」）。令牌**只给有无**，
        绝不给值——与快照同一条安全边界。"""
        return [{"name": n.name, "url": n.url,
                 "console_url": n.console_url,
                 "has_token": bool(n.token),
                 # 配了但强度不足、已当作没有令牌（只读观测）。
                 "token_weak": bool(getattr(n, "token_rejected", "")),
                 # 固定限速（Mbps；0 = 按需分配）。
                 "fixed_mbps": getattr(n, "fixed_mbps", 0.0)}
                for n in self._nodes_fn()]


def _parse_time(v: "str | None", default: float) -> float:
    """反查参数里的时间：unix 秒，或 ISO 8601（不带时区按总控本机时区）。"""
    if v is None or not v.strip():
        return default
    v = v.strip()
    try:
        return float(v)
    except ValueError:
        pass
    try:
        return _dt.datetime.fromisoformat(v).timestamp()
    except ValueError:
        raise ValueError(f"时间 {v!r} 既不是 unix 秒也不是 ISO 8601"
                         f"（如 2026-10-27T08:00:00+08:00）") from None


def build_fleet_app(
    hub: FleetHub,
    logbuf: LogBuffer,
    log: logging.Logger,
    yaml_path: str = "",
    api_token: str = "",
    poke: "asyncio.Event | None" = None,
    metrics_path: str = "",
    keys_fn: "Callable[[], list] | None" = None,
    audit_fn: "Callable[[str, str, dict], None] | None" = None,
    frame_ancestors: "list | tuple" = (),
    cors_origins: "list | tuple" = (),
) -> web.Application:
    """组装总控的 aiohttp 应用（开放 API，接口说明见 docs/12 与
    /api/openapi.json）。

    写接口全部回写 fleet YAML 并即时热更，按范围鉴权：limits（总带宽、
    节点固定限速）、nodes（节点清单增删改与导入）——管理员令牌
    （RL_API_TOKEN）拥有全部范围并且能管理 API 密钥；第三方用按范围
    授权的 API 密钥（keys_fn 给出当前密钥）。每次成功写都经 audit_fn
    留痕（调用者 + 操作）。frame_ancestors 是允许 iframe 嵌入本页面的
    来源（缺省只许同源），cors_origins 是允许浏览器跨域调 /api/ 的来源。
    """

    index_html = (_STATIC_DIR / "fleet.html").read_bytes()

    async def handle_index(_request: web.Request) -> web.Response:
        return web.Response(body=index_html, content_type="text/html",
                            charset="utf-8")

    async def handle_overview(_request: web.Request) -> web.Response:
        return web.json_response(hub.overview())

    async def handle_history(_request: web.Request) -> web.Response:
        return web.json_response({"snapshots": hub.history()})

    async def handle_nodes(_request: web.Request) -> web.Response:
        return web.json_response({"nodes": hub.nodes()})

    async def handle_metrics_log(request: web.Request) -> web.Response:
        """反查落盘数据：?from=&to=（unix 秒或 ISO 8601，缺省最近 1
        小时）&kind=fleet,node,event&node=<名称>&limit=&format=jsonl。"""
        if not metrics_path:
            return web.json_response(
                {"error": "总控未启用监控落盘——在 rl-fleet.env 里设 "
                          "RL_METRICS_LOG=<文件路径> 后重启即可"},
                status=404)
        q = request.query
        now = time.time()
        try:
            end = _parse_time(q.get("to"), now)
            start = _parse_time(q.get("from"), end - 3600)
            kinds = [k for k in (q.get("kind") or "").split(",") if k]
            bad = [k for k in kinds if k not in fleetmetrics.KINDS]
            if bad:
                raise ValueError(f"kind 只能是 {','.join(fleetmetrics.KINDS)}"
                                 f" 的组合，当前 {','.join(bad)}")
            limit = int(q.get("limit") or fleetmetrics.QUERY_LIMIT_DEFAULT)
        except ValueError as e:
            return web.json_response({"error": str(e)}, status=400)
        if start >= end:
            return web.json_response(
                {"error": "from 必须早于 to"}, status=400)
        res = await asyncio.to_thread(
            fleetmetrics.query, metrics_path, start, end, kinds,
            (q.get("node") or "").strip(), limit)
        if q.get("format") == "jsonl":
            body = "".join(json.dumps(r, ensure_ascii=False) + "\n"
                           for r in res["rows"])
            return web.Response(
                text=body, content_type="application/x-ndjson",
                charset="utf-8", headers={
                    "Content-Disposition":
                        f'attachment; filename="fleet-metrics-'
                        f'{int(start)}-{int(end)}.jsonl"'})
        res.update(start=start, end=end)
        return web.json_response(res)

    async def handle_logs(request: web.Request) -> web.Response:
        try:
            after = int(request.query.get("after", "0"))
        except ValueError:
            after = 0
        return web.json_response({"logs": logbuf.since(after)})

    async def handle_stream(request: web.Request) -> web.StreamResponse:
        resp = web.StreamResponse(headers={
            "Content-Type": "text/event-stream; charset=utf-8",
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        })
        await resp.prepare(request)
        q = hub.subscribe()
        try:
            latest = hub.overview()["latest"]
            if latest is not None:
                await resp.write(
                    b"data: " + json.dumps(latest).encode() + b"\n\n")
            while True:
                snap = await q.get()
                if snap is None:
                    break
                await resp.write(
                    b"data: " + json.dumps(snap).encode() + b"\n\n")
        except (ConnectionResetError, ConnectionError, OSError):
            pass
        finally:
            hub.unsubscribe(q)
        return resp

    write_lock = asyncio.Lock()
    # 弱令牌 = 没有令牌：在鉴权层本身兜底，不依赖调用方先查过。
    api_token, token_weak = tokenpolicy.effective(api_token)
    keys_fn = keys_fn or (lambda: [])

    def _no_admin_reason() -> str:
        return (f"RL_API_TOKEN 强度不足（{token_weak}），已视为未配置——用 "
                f"{tokenpolicy.GENERATE_HINT} 生成新令牌，写进 rl-fleet.env "
                f"后重启" if token_weak else
                "未配置 RL_API_TOKEN——在 rl-fleet 的环境变量（rl-fleet.env）"
                "里设置令牌后重启")

    def _auth(request: web.Request,
              scope: "str | None") -> "tuple[str, web.Response | None]":
        """鉴权。scope = "limits"/"nodes"：管理员令牌或带该范围的 API
        密钥都行；None：只认管理员令牌（管理密钥本身）；"any"：任一有效
        凭据。返回 (调用者, 拒绝响应)——调用者 = "admin" 或 "key:<名>"。"""
        keys = list(keys_fn())
        if not api_token and not keys:
            return "", web.json_response(
                {"error": _no_admin_reason() + "，写接口整体禁用"},
                status=403)
        got = ""
        auth = request.headers.get("Authorization", "")
        if auth.startswith("Bearer "):
            got = auth[len("Bearer "):].strip()
        if not got:
            got = request.headers.get("X-API-Token", "").strip()
        bad = web.json_response(
            {"error": "令牌缺失或不正确（请求头 Authorization: Bearer "
                      "<token> 或 X-API-Token: <token>）"}, status=401)
        if not got:
            return "", bad
        # 常数时间比较：令牌校验不能给旁路计时留缝。
        if api_token and hmac.compare_digest(got, api_token):
            return "admin", None
        h = fleetconfig.hash_token(got)
        for k in keys:
            if hmac.compare_digest(h, k.token_sha256):
                if scope is None:
                    return "", web.json_response(
                        {"error": "API 密钥不能管理密钥——需要管理员令牌"
                                  "（rl-fleet 的 RL_API_TOKEN）"}, status=403)
                if scope != "any" and scope not in k.scopes:
                    return "", web.json_response(
                        {"error": f"密钥 {k.name!r} 没有 {scope} 权限（它的"
                                  f"范围：{','.join(k.scopes)}）"}, status=403)
                return f"key:{k.name}", None
        return "", bad

    def _audit(actor: str, op: str, **detail) -> None:
        """每次成功的写操作留痕：谁（admin / key:<名>）做了什么。"""
        log.warning("开放 API 写操作 by=%s op=%s %s", actor, op,
                    " ".join(f"{k}={v}" for k, v in detail.items()))
        if audit_fn is not None:
            try:
                audit_fn(actor, op, detail)
            except Exception as e:                # 记账失败不影响写结果
                log.warning("审计记录失败 err=%s", e)

    async def _json_body(request: web.Request) -> "dict | None":
        try:
            body = await request.json()
        except Exception:
            return None
        return body if isinstance(body, dict) else None

    async def _write(fn, *args) -> "tuple[object, web.Response | None]":
        """回写 YAML 的公共尾巴：串行化写、错误码映射、触发热更。"""
        async with write_lock:
            try:
                res = await asyncio.to_thread(fn, yaml_path, *args)
            except (fleetconfig.NodeExistsError,
                    fleetconfig.KeyExistsError) as e:
                return None, web.json_response({"error": str(e)}, status=409)
            except (fleetconfig.NodeNotFoundError,
                    fleetconfig.KeyNotFoundError) as e:
                return None, web.json_response({"error": str(e)}, status=404)
            except ValueError as e:
                return None, web.json_response({"error": str(e)}, status=400)
        if poke is not None:
            poke.set()
        return res, None

    # ---- 限速：总带宽 + 节点固定限速（scope = limits） ----

    def _mbps(body: "dict | None", key: str, allow_zero: bool
              ) -> "float | None":
        v = body.get(key) if body is not None else None
        if isinstance(v, bool) or not isinstance(v, (int, float)):
            return None
        if v < 0 or (v == 0 and not allow_zero) or v > 1e7:
            return None
        return float(v)

    async def handle_put_total(request: web.Request) -> web.Response:
        actor, denied = _auth(request, "limits")
        if denied is not None:
            return denied
        v = _mbps(await _json_body(request), "quota_mbps", allow_zero=True)
        if v is None:
            return web.json_response(
                {"error": "请求体必须是 JSON {\"quota_mbps\": <≥0 的数字>}"
                          "（Mbps；0 = 取消总量控制，解除各节点实例总限速）"},
                status=400)
        _, err = await _write(fleetconfig.set_total_quota, v)
        if err is not None:
            return err
        _audit(actor, "set_total", total_mbps=v)
        return web.json_response({"ok": True, "total_egress_mbps": v})

    async def handle_delete_total(request: web.Request) -> web.Response:
        actor, denied = _auth(request, "limits")
        if denied is not None:
            return denied
        _, err = await _write(fleetconfig.set_total_quota, 0.0)
        if err is not None:
            return err
        _audit(actor, "cancel_total")
        return web.json_response({"ok": True, "total_egress_mbps": 0})

    async def handle_put_limit(request: web.Request) -> web.Response:
        """节点固定限速：{"mbps": <>0>}。照值下发，不参与按需分配。"""
        actor, denied = _auth(request, "limits")
        if denied is not None:
            return denied
        name = request.match_info["name"]
        v = _mbps(await _json_body(request), "mbps", allow_zero=False)
        if v is None:
            return web.json_response(
                {"error": "请求体必须是 JSON {\"mbps\": <大于 0 的数字>}（Mbps；"
                          "取消固定、回到按需分配用 DELETE）"}, status=400)
        _, err = await _write(fleetconfig.set_node_limit, name, v)
        if err is not None:
            return err
        _audit(actor, "set_node_limit", node=name, mbps=v)
        return web.json_response({"ok": True, "name": name, "fixed_mbps": v})

    async def handle_delete_limit(request: web.Request) -> web.Response:
        actor, denied = _auth(request, "limits")
        if denied is not None:
            return denied
        name = request.match_info["name"]
        _, err = await _write(fleetconfig.set_node_limit, name, 0.0)
        if err is not None:
            return err
        _audit(actor, "clear_node_limit", node=name)
        return web.json_response({"ok": True, "name": name, "fixed_mbps": 0})

    # ---- 节点清单（scope = nodes） ----

    async def handle_add_node(request: web.Request) -> web.Response:
        actor, denied = _auth(request, "nodes")
        if denied is not None:
            return denied
        body = await _json_body(request)
        if body is None or not isinstance(body.get("name"), str):
            return web.json_response(
                {"error": "请求体必须是 JSON：{\"name\": ..., \"url\": "
                          "\"http://<节点>:8090\", \"token\": ..., "
                          "\"console_url\": ...}"}, status=400)
        name = body["name"].strip()
        fields = {k: body[k] for k in fleetconfig.NODE_FIELDS if k in body}
        _, err = await _write(fleetconfig.add_node, name, fields)
        if err is not None:
            return err
        _audit(actor, "add_node", node=name,
               url=str(fields.get("url", "")).strip(),
               token="有" if str(fields.get("token") or "").strip() else "无")
        return web.json_response({"ok": True, "name": name})

    async def handle_update_node(request: web.Request) -> web.Response:
        actor, denied = _auth(request, "nodes")
        if denied is not None:
            return denied
        name = request.match_info["name"]
        body = await _json_body(request)
        fields = ({k: body[k] for k in fleetconfig.NODE_FIELDS if k in body}
                  if body is not None else None)
        if not fields:
            return web.json_response(
                {"error": "请求体必须是 JSON，且至少含 url / token / "
                          "console_url 之一（token 给空串 = 清除令牌；限速用 "
                          "PUT /api/nodes/{name}/limit）"}, status=400)
        _, err = await _write(fleetconfig.update_node, name, fields)
        if err is not None:
            return err
        _audit(actor, "update_node", node=name, fields=",".join(sorted(fields)))
        return web.json_response({"ok": True, "name": name})

    async def handle_import_nodes(request: web.Request) -> web.Response:
        """粘贴接入串批量导入：{"text": "...", "dry_run": true|false}。

        dry_run = 只预览每行会怎样（新增/更新/无变化/错误），不写盘；
        正式导入跳过有错的行，其余一次原子回写。响应里绝不带令牌。
        """
        actor, denied = _auth(request, "nodes")
        if denied is not None:
            return denied
        body = await _json_body(request)
        if body is None or not isinstance(body.get("text"), str):
            return web.json_response(
                {"error": "请求体必须是 JSON：{\"text\": \"<接入串，一行一台>"
                          "\", \"dry_run\": true|false}"}, status=400)
        dry_run = bool(body.get("dry_run"))
        entries, ignored = fleetconfig.parse_join_text(body["text"])
        if not entries:
            return web.json_response(
                {"error": "没找到接入串（形如 rl-node name=... url=... "
                          "token=...，节点装完 install.sh 会打印）"},
                status=400)
        async with write_lock:
            try:
                results = await asyncio.to_thread(
                    fleetconfig.import_nodes, yaml_path, entries, dry_run)
            except ValueError as e:
                return web.json_response({"error": str(e)}, status=400)
        by_line = {e["line"]: e for e in entries}
        for r in results:
            e = by_line[r["line"]]
            r.update(url=e["url"], console_url=e["console_url"],
                     has_token=bool(e["token"]))
        counts = {a: sum(r["action"] == a for r in results)
                  for a in ("add", "update", "same", "error")}
        if not dry_run and (counts["add"] or counts["update"]):
            if poke is not None:
                poke.set()
            _audit(actor, "import_nodes", added=counts["add"],
                   updated=counts["update"], skipped=counts["error"],
                   nodes=",".join(r["name"] for r in results
                                  if r["action"] in ("add", "update")))
        return web.json_response({"dry_run": dry_run, "items": results,
                                  "ignored_lines": ignored, **counts})

    async def handle_delete_node(request: web.Request) -> web.Response:
        actor, denied = _auth(request, "nodes")
        if denied is not None:
            return denied
        name = request.match_info["name"]
        _, err = await _write(fleetconfig.remove_node, name)
        if err is not None:
            return err
        _audit(actor, "remove_node", node=name)
        return web.json_response({"ok": True, "name": name})

    # ---- 开放 API 密钥（只有管理员令牌能管） ----

    async def handle_list_keys(request: web.Request) -> web.Response:
        _, denied = _auth(request, None)
        if denied is not None:
            return denied
        return web.json_response({"keys": [
            {"name": k.name, "scopes": list(k.scopes), "created": k.created}
            for k in keys_fn()], "scopes": list(fleetconfig.API_SCOPES)})

    async def handle_create_key(request: web.Request) -> web.Response:
        actor, denied = _auth(request, None)
        if denied is not None:
            return denied
        body = await _json_body(request)
        if body is None or not isinstance(body.get("name"), str):
            return web.json_response(
                {"error": "请求体必须是 JSON {\"name\": \"<密钥名>\", "
                          "\"scopes\": [\"limits\", \"nodes\"]}"}, status=400)
        name = body["name"].strip()
        token, err = await _write(fleetconfig.add_api_key, name,
                                  body.get("scopes"))
        if err is not None:
            return err
        _audit(actor, "create_key", key=name,
               scopes=",".join(sorted(set(body.get("scopes") or []))))
        return web.json_response({
            "ok": True, "name": name, "scopes": sorted(set(body["scopes"])),
            "token": token,
            "note": "令牌只显示这一次，总控只存它的 sha256——请立刻交给"
                    "第三方系统妥善保存；丢了就吊销重建"})

    async def handle_delete_key(request: web.Request) -> web.Response:
        actor, denied = _auth(request, None)
        if denied is not None:
            return denied
        name = request.match_info["name"]
        _, err = await _write(fleetconfig.remove_api_key, name)
        if err is not None:
            return err
        _audit(actor, "revoke_key", key=name)
        return web.json_response({"ok": True, "name": name})

    async def handle_whoami(request: web.Request) -> web.Response:
        """第三方自检凭据：返回调用者身份与可用范围（不写任何东西）。"""
        actor, denied = _auth(request, "any")
        if denied is not None:
            return denied
        if actor == "admin":
            return web.json_response({"actor": "admin", "scopes": list(
                fleetconfig.API_SCOPES) + ["keys"]})
        name = actor.split(":", 1)[1]
        k = next(k for k in keys_fn() if k.name == name)
        return web.json_response({"actor": actor, "scopes": list(k.scopes)})

    async def handle_openapi(_request: web.Request) -> web.Response:
        return web.json_response(fleetopenapi.spec())

    # ---- 响应头：嵌入白名单（frame-ancestors）+ 跨域（CORS） ----

    cors_set = set(cors_origins or ())
    ancestors = ["'self'"] + list(frame_ancestors or ())

    @web.middleware
    async def preflight_mw(request: web.Request, handler):
        if request.method == "OPTIONS" and request.path.startswith("/api/"):
            if request.headers.get("Origin", "") not in cors_set:
                return web.json_response(
                    {"error": "跨域来源不在 RL_CORS_ORIGINS 白名单里"},
                    status=403)
            return web.Response(status=204, headers={
                "Access-Control-Allow-Methods": "GET, POST, PUT, DELETE",
                "Access-Control-Allow-Headers":
                    "Authorization, X-API-Token, Content-Type",
                "Access-Control-Max-Age": "600"})
        return await handler(request)

    async def on_prepare(request: web.Request,
                         response: web.StreamResponse) -> None:
        h = response.headers
        h["Content-Security-Policy"] = "frame-ancestors " + " ".join(ancestors)
        if not frame_ancestors:
            h["X-Frame-Options"] = "SAMEORIGIN"     # 老浏览器不认 CSP
        h["X-Content-Type-Options"] = "nosniff"
        origin = request.headers.get("Origin", "")
        if origin and origin in cors_set and request.path.startswith("/api/"):
            h["Access-Control-Allow-Origin"] = origin
            h["Vary"] = "Origin"

    app = web.Application(middlewares=[preflight_mw])
    app.on_response_prepare.append(on_prepare)
    app.router.add_get("/", handle_index)
    app.router.add_get("/api/overview", handle_overview)
    app.router.add_get("/api/history", handle_history)
    app.router.add_get("/api/nodes", handle_nodes)
    app.router.add_get("/api/metrics-log", handle_metrics_log)
    app.router.add_get("/api/logs", handle_logs)
    app.router.add_get("/api/stream", handle_stream)
    app.router.add_get("/api/openapi.json", handle_openapi)
    if yaml_path:
        app.router.add_get("/api/whoami", handle_whoami)
        app.router.add_put("/api/total-quota", handle_put_total)
        app.router.add_delete("/api/total-quota", handle_delete_total)
        app.router.add_post("/api/nodes", handle_add_node)
        app.router.add_post("/api/nodes/import", handle_import_nodes)
        app.router.add_put("/api/nodes/{name}", handle_update_node)
        app.router.add_delete("/api/nodes/{name}", handle_delete_node)
        app.router.add_put("/api/nodes/{name}/limit", handle_put_limit)
        app.router.add_delete("/api/nodes/{name}/limit", handle_delete_limit)
        app.router.add_get("/api/keys", handle_list_keys)
        app.router.add_post("/api/keys", handle_create_key)
        app.router.add_delete("/api/keys/{name}", handle_delete_key)
    return app


async def run_fleet_console(
    port: int,
    hub: FleetHub,
    logbuf: LogBuffer,
    log: logging.Logger,
    bind: str = "127.0.0.1",
    yaml_path: str = "",
    api_token: str = "",
    poke: "asyncio.Event | None" = None,
    metrics_path: str = "",
    keys_fn: "Callable[[], list] | None" = None,
    audit_fn: "Callable[[str, str, dict], None] | None" = None,
    frame_ancestors: "list | tuple" = (),
    cors_origins: "list | tuple" = (),
    ssl_context=None,
) -> None:
    """常驻任务：启动总控 HTTP(S) 服务并挂起到被取消（同 run_console）。
    ssl_context 非空 = HTTPS（被 HTTPS 页面 iframe 嵌入时必须，否则浏览器
    按混合内容拦掉）。"""
    runner = web.AppRunner(
        build_fleet_app(hub, logbuf, log, yaml_path=yaml_path,
                        api_token=api_token, poke=poke,
                        metrics_path=metrics_path, keys_fn=keys_fn,
                        audit_fn=audit_fn, frame_ancestors=frame_ancestors,
                        cors_origins=cors_origins),
        access_log=None)
    await runner.setup()
    site = web.TCPSite(runner, bind, port, ssl_context=ssl_context)
    await site.start()
    eff_token, token_weak = tokenpolicy.effective(api_token)
    log.info("总控制台已启动 %s bind=%s port=%d 写API=%s",
             "https" if ssl_context is not None else "http", bind, port,
             ("已启用（令牌鉴权）" if yaml_path and eff_token
              else "已禁用（RL_API_TOKEN 强度不足，视为未配置）"
              if yaml_path and token_weak
              else "已禁用（未配置 RL_API_TOKEN）" if yaml_path
              else "未挂载"))
    if bind not in ("127.0.0.1", "::1", "localhost"):
        log.warning(
            "总控制台绑定在非回环地址上，而**读接口没有任何鉴权**"
            "（暴露全部节点的监控汇总与运行日志）——请确认该地址只在"
            "内网且已由防火墙/安全组限制来源 bind=%s port=%d", bind, port)
    try:
        await asyncio.Event().wait()
    finally:
        hub.close()
        await runner.cleanup()
