# rl_limiter.nodeopenapi —— 节点（rl-limiter）控制台 API 的 OpenAPI 3
# 描述（GET /api/openapi.json）。人读的规范见 docs/13-接口文档.md §3。
#
# 与 webconsole 的路由一一对应——新增/改接口时两边一起改（测试会核对
# 路由与这里列的路径一致）。

from __future__ import annotations

try:
    from importlib import metadata as _metadata
    _VERSION = _metadata.version("rl-limiter")
except Exception:                                      # pragma: no cover
    _VERSION = "dev"

_ERR = {"$ref": "#/components/schemas/Error"}
_SEC = [{"bearer": []}, {"apiKeyHeader": []}]


def _json(schema: dict, desc: str = "成功") -> dict:
    return {"description": desc,
            "content": {"application/json": {"schema": schema}}}


def _errs(*codes: int) -> dict:
    text = {400: "参数不合法（含端口未登记）", 401: "令牌缺失或不正确",
            403: "写接口未启用（未配置令牌或令牌强度不足）",
            409: "本机没有实例总限速的执行者"}
    return {str(c): _json(_ERR, text[c]) for c in codes}


def _read(summary: str, desc: str = "", schema: "dict | None" = None,
          params: "list | None" = None) -> dict:
    op = {"summary": summary, "tags": ["read"],
          "description": desc or "只读接口，无需鉴权（部署时由防火墙限制来源）。",
          "responses": {"200": _json(schema or {"type": "object"})}}
    if params:
        op["parameters"] = params
    return op


def _write(summary: str, desc: str, body=None, ok=None, errs=(400, 401, 403)):
    op = {"summary": summary, "tags": ["write"],
          "description": desc + "\n\n需要节点令牌（RL_API_TOKEN）。修改整份"
                         "校验后原子回写 YAML，数秒内热生效。",
          "security": _SEC,
          "responses": {"200": _json(ok or {"type": "object"}),
                        **_errs(*errs)}}
    if body is not None:
        op["requestBody"] = {"required": True, "content": {
            "application/json": body}}
    return op


_NAME = {"name": "name", "in": "path", "required": True,
         "description": "端口登记名（YAML ports 段的键）",
         "schema": {"type": "string"}}


def spec() -> dict:
    return {
        "openapi": "3.0.3",
        "info": {
            "title": "rl-limiter 节点控制台 API",
            "version": _VERSION,
            "description": (
                "单台服务器的出口限速与监控。速率字段一律 bytes/s（网卡/"
                "应用层口径见各字段说明），调节视图 regulation 段为 Mbps。"
                "读接口无鉴权；写接口需要节点令牌（RL_API_TOKEN，须过强度"
                "规则，不达标等同未配置）。总控（rl-fleet）就是用本 API "
                "轮询节点并下发实例出向目标。"),
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
            },
        },
        "paths": {
            "/api/overview": {"get": _read(
                "服务与配置的当前视图 + 最新一拍快照",
                schema={"type": "object", "properties": {
                    "service_version": {"type": "string"},
                    "config_version": {"type": "integer"},
                    "uptime_s": {"type": "number"},
                    "frontends": {"type": "object"},
                    "request_limits": {"type": "object"},
                    "instance_quota_mbps": {"type": "number"},
                    "instance_quota_bytes_per_s": {"type": "number"},
                    "instance_quota_executable": {"type": "boolean"},
                    "haproxy": {"type": "object"},
                    "latest": {"type": "object", "nullable": True}}})},
            "/api/history": {"get": _read("近 10 分钟、1 秒一拍的快照")},
            "/api/stream": {"get": {
                "summary": "实时快照流（Server-Sent Events）",
                "tags": ["read"],
                "responses": {"200": {"description": "text/event-stream"}}}},
            "/api/logs": {"get": _read(
                "进程内日志环形缓冲的增量拉取",
                params=[{"name": "after", "in": "query",
                         "schema": {"type": "integer"},
                         "description": "上次拿到的最大 seq"}])},
            "/api/tc": {"get": _read("tc 实况：期望配置 + 内核原始输出")},
            "/api/config-view": {"get": _read("配置页数据（只读 kv 与可编辑行）")},
            "/api/openapi.json": {"get": _read("本文档")},
            "/metrics": {"get": {
                "summary": "Prometheus 指标（文本格式，前缀 rl_limiter_）",
                "tags": ["read"],
                "responses": {"200": {"description": "text/plain"}}}},
            "/api/instance-quota": {
                "put": _write(
                    "设置实例出向总目标",
                    "Mbps；0 = 取消。本机没有执行者"
                    "（instance_quota_executable=false）时 409。",
                    body={"schema": {"type": "object",
                                     "required": ["quota_mbps"],
                                     "properties": {"quota_mbps": {
                                         "type": "number", "minimum": 0}}},
                          "example": {"quota_mbps": 40}},
                    errs=(400, 401, 403, 409)),
                "delete": _write("取消实例出向总目标（恢复不限）",
                                 "清理残留配置：即使本机没有执行者也放行。",
                                 errs=(401, 403)),
            },
            "/api/request-limits/{name}": {
                "parameters": [_NAME],
                "put": _write(
                    "登记/修改端口的入向限速（部分更新）",
                    "mbps（≥0 数字，0 = 不限带宽）与 conn_rate（≥0 整数，"
                    "新建连接/秒，0 = 不限）至少给一个，只给一个时另一个保留"
                    "现值。端口必须已在 YAML ports 段登记。",
                    body={"schema": {"type": "object", "properties": {
                        "mbps": {"type": "number", "minimum": 0},
                        "conn_rate": {"type": "integer", "minimum": 0}}},
                        "example": {"mbps": 10, "conn_rate": 150}}),
                "delete": _write("删除端口的入向限速登记",
                                 "响应 removed 表示是否真的删掉了一条。",
                                 errs=(400, 401, 403)),
            },
        },
    }
