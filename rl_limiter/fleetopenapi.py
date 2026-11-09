# rl_limiter.fleetopenapi —— 总控开放 API 的 OpenAPI 3 描述
# （GET /api/openapi.json）。第三方可以直接导入 Postman/Apifox 或生成
# 客户端；人读的说明与 curl 示例在 docs/12-总控开放API.md。
#
# 与 fleetweb 的路由一一对应——新增/改接口时两边一起改（测试会核对
# 路由与这里列的路径一致）。

from __future__ import annotations

try:
    from importlib import metadata as _metadata
    _VERSION = _metadata.version("rl-limiter")
except Exception:                                      # pragma: no cover
    _VERSION = "dev"


def _json(schema: dict, desc: str = "成功") -> dict:
    return {"description": desc,
            "content": {"application/json": {"schema": schema}}}


def _body(schema: dict, example: dict) -> dict:
    return {"required": True, "content": {"application/json": {
        "schema": schema, "example": example}}}


_ERR = {"$ref": "#/components/schemas/Error"}
_OK = {"type": "object", "properties": {"ok": {"type": "boolean"}}}


def _errs(*codes: int) -> dict:
    text = {400: "参数不合法", 401: "令牌缺失或不正确",
            403: "无权限（范围不够，或写接口未启用）", 404: "对象不存在",
            409: "重名"}
    return {str(c): _json(_ERR, text[c]) for c in codes}


def _write(summary: str, scope: str, desc: str = "", body=None,
           ok: "dict | None" = None, errs=(400, 401, 403)) -> dict:
    op = {"summary": summary, "tags": [scope],
          "description": (desc + "\n\n" if desc else "")
          + f"需要范围 **{scope}**（管理员令牌拥有全部范围）。",
          "security": [{"bearer": []}, {"apiKeyHeader": []}],
          "responses": {"200": _json(ok or _OK), **_errs(*errs)}}
    if body is not None:
        op["requestBody"] = body
    return op


def _read(summary: str, schema: dict, desc: str = "") -> dict:
    return {"summary": summary, "tags": ["read"], "description": desc
            or "只读接口，无需鉴权（部署时由防火墙限制来源）。",
            "responses": {"200": _json(schema)}}


_NAME = {"name": "name", "in": "path", "required": True,
         "schema": {"type": "string", "pattern": "^[A-Za-z0-9._-]{1,64}$"}}


def spec() -> dict:
    return {
        "openapi": "3.0.3",
        "info": {
            "title": "rl-fleet 总控开放 API",
            "version": _VERSION,
            "description": (
                "多机总量控制：总带宽、节点固定限速、节点清单。写接口按"
                "范围鉴权——limits（总带宽、节点固定限速）、nodes（节点"
                "清单增删改与接入串导入）；管理员令牌（RL_API_TOKEN）拥有"
                "全部范围并能管理 API 密钥。第三方请用管理员在「开放 API」"
                "页签创建的、按范围授权的密钥。每次成功写都记审计事件"
                "（GET /api/metrics-log?kind=event 里 type=api）。"),
        },
        "components": {
            "securitySchemes": {
                "bearer": {"type": "http", "scheme": "bearer"},
                "apiKeyHeader": {"type": "apiKey", "in": "header",
                                 "name": "X-API-Token"},
            },
            "schemas": {
                "Error": {"type": "object", "required": ["error"],
                          "properties": {"error": {"type": "string"}}},
                "Node": {"type": "object", "properties": {
                    "name": {"type": "string"},
                    "url": {"type": "string"},
                    "console_url": {"type": "string"},
                    "has_token": {"type": "boolean"},
                    "token_weak": {"type": "boolean"}}},
            },
        },
        "paths": {
            "/api/overview": {"get": _read(
                "总控概览：总带宽、分配参数、最新快照（含各节点出入向、"
                "配额、固定限速）", {"type": "object"})},
            "/api/nodes": {
                "get": _read("节点清单（令牌只给有无）", {
                    "type": "object", "properties": {"nodes": {
                        "type": "array",
                        "items": {"$ref": "#/components/schemas/Node"}}}}),
                "post": _write(
                    "添加节点", "nodes",
                    body=_body({"type": "object", "required": ["name", "url"],
                                "properties": {
                                    "name": {"type": "string"},
                                    "url": {"type": "string"},
                                    "token": {"type": "string"},
                                    "console_url": {"type": "string"}}},
                               {"name": "node-sh-03",
                                "url": "http://10.0.1.13:8090",
                                "token": "<该节点的 RL_API_TOKEN>"}),
                    errs=(400, 401, 403, 409)),
            },
            "/api/nodes/import": {"post": _write(
                "粘贴接入串批量导入（dry_run 只预览）", "nodes",
                body=_body({"type": "object", "required": ["text"],
                            "properties": {"text": {"type": "string"},
                                           "dry_run": {"type": "boolean"}}},
                           {"text": "rl-node name=n1 url=http://10.0.1.11:"
                                    "8090 token=<令牌>", "dry_run": True}),
                ok={"type": "object"})},
            "/api/nodes/{name}": {
                "parameters": [_NAME],
                "put": _write(
                    "修改节点接线（url / token / console_url 中给出的字段）",
                    "nodes", body=_body({"type": "object", "properties": {
                        "url": {"type": "string"},
                        "token": {"type": "string"},
                        "console_url": {"type": "string"}}},
                        {"token": "<新令牌>"}),
                    errs=(400, 401, 403, 404)),
                "delete": _write("删除节点（停止轮询与下发，配额保持现状）",
                                 "nodes", errs=(401, 403, 404)),
            },
            "/api/nodes/{name}/limit": {
                "parameters": [_NAME],
                "put": _write(
                    "设置节点固定限速", "limits",
                    desc="该节点的实例出向目标固定为 mbps，照值下发、不参与"
                         "按需分配，但计入总量记账；与总量控制无关（总带宽为 0"
                         " 时同样生效）。",
                    body=_body({"type": "object", "required": ["mbps"],
                                "properties": {"mbps": {
                                    "type": "number", "exclusiveMinimum": 0}}},
                               {"mbps": 50}),
                    errs=(400, 401, 403, 404)),
                "delete": _write(
                    "取消节点固定限速", "limits",
                    desc="在做总量控制 → 回到按需分配；没在做 → 解除该节点"
                         "限速（恢复不限）。", errs=(401, 403, 404)),
            },
            "/api/total-quota": {
                "put": _write(
                    "设置总流出带宽", "limits",
                    desc="全部节点出向合计的目标，立即按需重新分配；0 = 取消"
                         "总量控制。",
                    body=_body({"type": "object", "required": ["quota_mbps"],
                                "properties": {"quota_mbps": {
                                    "type": "number", "minimum": 0}}},
                               {"quota_mbps": 1000})),
                "delete": _write(
                    "取消总量控制", "limits",
                    desc="立即解除各按需节点的实例总限速（固定限速的节点不受"
                         "影响）。", errs=(401, 403)),
            },
            "/api/whoami": {"get": {
                "summary": "检查凭据：返回调用者身份与可用范围",
                "tags": ["auth"],
                "security": [{"bearer": []}, {"apiKeyHeader": []}],
                "responses": {"200": _json({"type": "object", "properties": {
                    "actor": {"type": "string"},
                    "scopes": {"type": "array",
                               "items": {"type": "string"}}}}),
                    **_errs(401, 403)}}},
            "/api/keys": {
                "get": {"summary": "列出 API 密钥（只有名称与范围）",
                        "tags": ["keys"], "description": "只认管理员令牌。",
                        "security": [{"bearer": []}, {"apiKeyHeader": []}],
                        "responses": {"200": _json({"type": "object"}),
                                      **_errs(401, 403)}},
                "post": {"summary": "创建 API 密钥（令牌只在响应里出现这一次）",
                         "tags": ["keys"], "description": "只认管理员令牌。",
                         "security": [{"bearer": []}, {"apiKeyHeader": []}],
                         "requestBody": _body(
                             {"type": "object", "required": ["name", "scopes"],
                              "properties": {
                                  "name": {"type": "string"},
                                  "scopes": {"type": "array", "items": {
                                      "type": "string",
                                      "enum": ["limits", "nodes"]}}}},
                             {"name": "billing", "scopes": ["limits"]}),
                         "responses": {"200": _json({
                             "type": "object", "properties": {
                                 "name": {"type": "string"},
                                 "token": {"type": "string"}}}),
                             **_errs(400, 401, 403, 409)}},
            },
            "/api/keys/{name}": {"parameters": [_NAME], "delete": {
                "summary": "吊销 API 密钥（立即失效）", "tags": ["keys"],
                "description": "只认管理员令牌。",
                "security": [{"bearer": []}, {"apiKeyHeader": []}],
                "responses": {"200": _json(_OK), **_errs(401, 403, 404)}}},
            "/api/metrics-log": {"get": {
                "summary": "反查监控落盘（分钟数据 + 变更/审计事件）",
                "tags": ["read"],
                "parameters": [
                    {"name": "from", "in": "query",
                     "schema": {"type": "string"},
                     "description": "unix 秒或 ISO 8601，缺省最近 1 小时"},
                    {"name": "to", "in": "query",
                     "schema": {"type": "string"}},
                    {"name": "kind", "in": "query",
                     "schema": {"type": "string"},
                     "description": "fleet,node,event 的组合"},
                    {"name": "node", "in": "query",
                     "schema": {"type": "string"}},
                    {"name": "limit", "in": "query",
                     "schema": {"type": "integer"}},
                    {"name": "format", "in": "query",
                     "schema": {"type": "string", "enum": ["jsonl"]}}],
                "responses": {"200": _json({"type": "object"}),
                              **_errs(400, 404)}}},
            "/api/history": {"get": _read("最近约 10 分钟的快照历史",
                                          {"type": "object"})},
            "/api/stream": {"get": {
                "summary": "实时快照流（Server-Sent Events）",
                "tags": ["read"],
                "responses": {"200": {"description": "text/event-stream"}}}},
            "/api/logs": {"get": _read("进程内最近的运行日志",
                                       {"type": "object"})},
            "/api/openapi.json": {"get": _read("本文档", {"type": "object"})},
        },
    }
