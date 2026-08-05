# hap-agg HTTP API 文档（面向第三方开发）

hap-agg 在 Web 页面端口上同时提供一组 HTTP API：**读接口**（聚合监控
数据、SSE 实时流、Prometheus 指标）与**写接口**（目标清单管理，带令牌
鉴权）。页面本身就是这组 API 的第一个消费者——页面能做到的，第三方
程序都能做到。

## 1. 基础信息

| 项 | 值 |
| --- | --- |
| Base URL | `http://<主机>:<端口>`（`--port`，默认 `8100`；默认只绑 `127.0.0.1`） |
| 传输编码 | 一律 UTF-8；JSON 响应 `Content-Type: application/json` |
| 认证 | 读接口无认证；写接口需令牌（§2） |

> **网络边界**：读接口暴露全部目标的监控数据，默认只绑回环。放内网
> 必须 `--bind` 显式指定并用防火墙/安全组限制来源，绝不可暴露公网。

## 2. 认证（仅写接口）

令牌由服务端环境变量 `HAP_AGG_TOKEN` 配置，请求头二选一：

```
Authorization: Bearer <token>
X-API-Token: <token>
```

| 情形 | 状态码 |
| --- | --- |
| 服务端未配置 `HAP_AGG_TOKEN`（写功能整体关闭） | `403` |
| 未带令牌 / 令牌错误 | `401` |

非 2xx 响应的 body 一律为 `{"error": "<中文原因>"}`。

## 3. 通用口径

- 速率字段一律 `bytes/s`（HAProxy 应用层计数器差分）；
- `ts` 为 Unix 秒（浮点）；
- 目标视图里 `ok=false` 时其余数字是**最近一次成功采样的陈旧值**
  （供展示），不计入 `total` 合计；`degraded=true` 表示已连续失败
  ≥ 3 拍；
- `conn`（`show info` 的 CurrConns，整机所有连接）与 `fe_conn`
  （Σ frontend scur）口径不同，两个都给。

## 4. 读接口

### 4.1 `GET /api/overview`

```jsonc
{
  "service_version": "0.1.0",
  "uptime_s": 3600.5,
  "targets": [ {"name": "node-1", "addr": "10.0.0.11:9999"} ],
  "latest": { /* 最新一拍快照，结构见 §4.2；未采到样时为 null */ }
}
```

### 4.2 `GET /api/history` — 最近约 10 分钟快照

```jsonc
{ "snapshots": [ <snapshot>, ... ] }    // 从旧到新；重启即清空
```

每个 `snapshot`：

```jsonc
{
  "ts": 1785723600.0,
  "total": {                            // 全部健康（ok）目标合计
    "targets": 3, "targets_ok": 3,
    "conn": 950,                        // Σ CurrConns（整机口径）
    "fe_conn": 900,                     // Σ frontend scur
    "max_conn": 15000,                  // Σ Maxconn
    "rate_in_bytes_per_s": 1200000.0,
    "rate_out_bytes_per_s": 52000000.0,
    "conn_new_ps": 120.0,
    "denied_ps": 0.0,
    "sess_rate": 118
  },
  "targets": {                          // 每台一项
    "node-1": {
      "name": "node-1", "addr": "10.0.0.11:9999",
      "ok": true, "degraded": false, "error": "", "consec_failures": 0,
      "conn": 320, "max_conn": 5000, "idle_pct": 92,
      "sess_rate": 40, "uptime_s": 86400,
      "fe_conn": 300,
      "rate_in_bytes_per_s": 400000.0,
      "rate_out_bytes_per_s": 17000000.0,
      "conn_new_ps": 40.0, "denied_ps": 0.0,
      "frontends": {                    // 该目标的每个 frontend
        "fe_main": {
          "rate_out_bytes_per_s": 17000000.0,
          "rate_in_bytes_per_s": 400000.0,
          "conn": 300, "conn_new_ps": 40.0, "denied_ps": 0.0,
          "mode": "tcp"
        }
      }
    }
  },
  "frontends": {                        // 同名 frontend 跨机合并
    "fe_main": {
      "rate_out_bytes_per_s": 52000000.0, "rate_in_bytes_per_s": 1200000.0,
      "conn": 900, "conn_new_ps": 120.0, "denied_ps": 0.0,
      "targets": 3,                     // 有这个 frontend 的目标数
      "mode": "tcp"
    }
  }
}
```

### 4.3 `GET /api/stream` — SSE 实时流

每拍推一帧 §4.2 的 snapshot（`data:` 行，一帧一个 JSON）。连接建立后
先补发最新一帧；消费慢时服务端丢最旧帧（队列深度 5）；服务停机时流
正常 EOF，客户端应自动重连。

### 4.4 `GET /metrics` — Prometheus 抓取端点

全部 gauge，取最新一拍：

| 指标 | 说明 |
| --- | --- |
| `hap_agg_info{version}` | 元信息（值恒 1） |
| `hap_agg_targets` / `hap_agg_targets_up` | 登记目标数 / 本拍采样成功数 |
| `hap_agg_total_connections` / `_frontend_connections` | 合计整机并发 / frontend 并发 |
| `hap_agg_total_rate_in_bytes_per_second` / `_out_…` | 聚合上/下行速率 |
| `hap_agg_total_new_connections_per_second` / `_denied_per_second` | 聚合每秒新建/拒绝 |
| `hap_agg_target_up{target,addr}` | 该目标本拍是否成功（0 时其余为陈旧值） |
| `hap_agg_target_connections{target,addr}` 等 | 每台：并发/上限/空闲率/uptime/带宽/新建/拒绝 |
| `hap_agg_frontend_*{frontend}` | 跨机合并的 frontend 维度：带宽/并发/新建/拒绝/实例数 |

## 5. 写接口（目标清单管理）

### 5.1 `POST /api/targets` — 批量导入

请求体：

```json
{ "text": "10.0.0.11:9999\nsg-02 10.0.0.12:9999\n# 注释行" }
```

每行一条：`IP:port`（名字缺省 = 地址）或 `名字 IP:port`；IPv6 写
`[::1]:9999`。规则：

- **一行错整批拒绝**（`400`，error 指出哪行、什么问题），目标集与
  YAML 都不动；
- 同名同址幂等跳过；同址异名视为已导入过；**同名异址 `400`**（静默
  替换会让人以为旧地址还在被监控）；
- 成功后**立即生效**（下一拍开始采样）并原子回写服务端 YAML。

成功 `200`：

```json
{ "ok": true, "added": ["10.0.0.11:9999", "sg-02"], "total": 5 }
```

### 5.2 `DELETE /api/targets/{name}` — 删除目标

`{name}` 为目标名（URL 编码）。幂等：不存在时 `removed: false`。

```json
{ "ok": true, "name": "sg-02", "removed": true }
```

## 6. 兼容性

- 已文档化字段与端点语义保持不变，新版本可能**新增**字段/指标——
  解析 JSON 时应忽略未知字段；
- `/api/history` 与 SSE 是进程内数据，不承诺持久性；长期历史请用
  Prometheus 抓 `/metrics`。
