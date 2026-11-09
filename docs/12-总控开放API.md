# 12 总控开放 API（第三方集成）

rl-fleet 总控对第三方系统（计费、运维门户、CMDB、工单……）开放两类能力：

- **API**：改总带宽、给单个节点设固定限速、维护节点清单，读概览与历史；
- **页面嵌入**：第三方页面用 iframe 引用总控页面（只读精简版）。

机器可读的接口描述：`GET /api/openapi.json`（OpenAPI 3，可直接导入
Postman / Apifox 或生成客户端）。总控页面「开放 API」页签有接口速查与
curl 示例。

## 1. 凭据与权限范围

| 凭据 | 从哪来 | 能做什么 |
| ---- | ---- | ---- |
| **管理员令牌** | rl-fleet 的 `RL_API_TOKEN`（`install.sh --fleet --token auto` 生成） | 全部写接口 + 管理 API 密钥。**不要给第三方** |
| **API 密钥** | 管理员在页面「开放 API」页签创建（或 `POST /api/keys`） | 只能做创建时授权的范围 |

API 密钥的范围（scopes）：

| 范围 | 允许的写接口 |
| ---- | ---- |
| `limits` | 总流出带宽、节点固定限速 |
| `nodes` | 节点清单增删改、接入串导入 |

给计费系统一般只开 `limits`，给 CMDB 只开 `nodes`。

请求头二选一：`X-API-Token: <令牌>` 或 `Authorization: Bearer <令牌>`。

- 密钥的令牌是总控生成的 64 位十六进制高强度随机串，**只在创建时返回
  一次**；`fleet.yaml` 里只存它的 sha256。丢了吊销重建。
- 吊销（页面或 `DELETE /api/keys/{name}`）立即生效。
- 管理员令牌必须过强度规则（[11](11-发布与安装.md) §3.4），不达标等同
  未配置；只配了 API 密钥、没配管理员令牌时，密钥照常可用，但没人能
  再管理密钥（先把管理员令牌配上）。
- 自检凭据：`GET /api/whoami` → `{"actor": "key:billing", "scopes": ["limits"]}`。

读接口（概览、节点清单、历史、审计）**无需鉴权**，与总控页面一致——
部署时由防火墙/安全组限制来源。

## 2. 接口

基址 `http(s)://<总控>:8095`，请求/响应体都是 JSON，错误一律
`{"error": "<中文原因>"}`。

### 2.1 限速（范围 limits）

| 接口 | 请求体 | 说明 |
| ---- | ---- | ---- |
| `PUT /api/total-quota` | `{"quota_mbps": 1000}` | 设总流出带宽（全部节点出向合计的目标），立即按需重新分配；0 = 取消总量控制 |
| `DELETE /api/total-quota` | — | 取消总量控制：立即解除各**按需**节点的实例总限速（固定限速的节点不受影响） |
| `PUT /api/nodes/{name}/limit` | `{"mbps": 50}` | 节点**固定限速**（见 §3） |
| `DELETE /api/nodes/{name}/limit` | — | 取消固定限速 |

```bash
F=https://fleet.example.com:8095; K='<API 密钥>'
curl -s -X PUT $F/api/total-quota -H "X-API-Token: $K" -d '{"quota_mbps": 1000}'
curl -s -X PUT $F/api/nodes/node-sh-01/limit -H "X-API-Token: $K" -d '{"mbps": 50}'
curl -s -X DELETE $F/api/nodes/node-sh-01/limit -H "X-API-Token: $K"
```

### 2.2 节点清单（范围 nodes）

| 接口 | 请求体 | 说明 |
| ---- | ---- | ---- |
| `POST /api/nodes` | `{"name", "url", "token"?, "console_url"?}` | 添加节点；重名 409；节点令牌须过强度规则 |
| `PUT /api/nodes/{name}` | `url` / `token` / `console_url` 中要改的 | 改接线（`token: ""` = 清除，转只读观测） |
| `DELETE /api/nodes/{name}` | — | 删除节点（停止轮询与下发，配额保持现状） |
| `POST /api/nodes/import` | `{"text": "<接入串，一行一台>", "dry_run": true}` | 批量导入接入串（[10](10-总控制台.md) §2）；`dry_run` 只预览 |

### 2.3 密钥管理（只认管理员令牌）

| 接口 | 说明 |
| ---- | ---- |
| `GET /api/keys` | 列出密钥（名称、范围、创建时间；不含令牌） |
| `POST /api/keys` `{"name": "billing", "scopes": ["limits"]}` | 创建，响应里的 `token` 只出现这一次 |
| `DELETE /api/keys/{name}` | 吊销 |

### 2.4 读接口（无需鉴权）

| 接口 | 说明 |
| ---- | ---- |
| `GET /api/overview` | 总带宽、分配参数、最新快照（`latest.nodes.*`：出向/入向、自报配额 `quota_mbps`、本轮分配 `desired_mbps`、固定限速 `fixed_mbps`、在线状态等） |
| `GET /api/nodes` | 节点清单（令牌只给有无） |
| `GET /api/metrics-log` | 历史分钟数据与变更/审计事件（[10](10-总控制台.md) §5） |
| `GET /api/stream` | 实时快照（SSE） |

### 2.5 状态码

| 码 | 含义 |
| ---- | ---- |
| 200 | 成功 |
| 400 | 参数不合法（原因写在 error 里） |
| 401 | 令牌缺失或不正确 |
| 403 | 范围不够；或总控既没配管理员令牌也没有任何密钥（写接口整体禁用）；或用密钥调密钥管理接口 |
| 404 | 节点/密钥不存在 |
| 409 | 重名 |

## 3. 节点固定限速的语义

- 设了固定限速的节点，实例出向目标**就是这个值**：总控照值下发，不参与
  按需分配；但它**计入总量**——按需节点分的是 `总带宽 − Σ固定限速 −
  冻结记账`。固定之和超过总带宽时总控告警"超卖"，按需节点压到保底。
- 固定限速**与总量控制无关**：总带宽为 0（不做总量控制）时同样生效；
  取消总量控制只解除按需节点，不动固定节点。
- 取消固定：在做总量控制 → 回到按需分配；没在做 → 解除该节点的限速
  （恢复不限——那个限速本来就是总控设的）。
- 节点离线时先记着，恢复在线即下发。
- 页面「节点管理」的节点列表有"限速"列（按需分配 / 固定 X Mbps），手工
  表单里可直接填；总览节点表里固定节点带"固定"标签。

## 4. 审计

每次成功的写操作都记一条审计事件（需开监控落盘，默认开）：

```json
{"ts": 1793...,"kind": "event","type": "api","by": "key:billing","op": "set_node_limit","node": "node-sh-01","mbps": 50.0}
```

`by` 是 `admin` 或 `key:<密钥名>`；`op` 为 set_total / cancel_total /
set_node_limit / clear_node_limit / add_node / update_node / remove_node /
import_nodes / create_key / revoke_key。配置因此发生的变化另有事件
（total / limit / nodes / keys）。查询：页面「历史回查」，或

```bash
curl -s "$F/api/metrics-log?kind=event&from=2026-11-01T00:00:00%2B08:00" | jq '.rows[] | select(.type=="api")'
```

令牌（管理员令牌、密钥、节点令牌）的值**从不**出现在审计、日志或任何
读接口里。

## 5. 页面嵌入（iframe）

**默认只许同源嵌入**（`Content-Security-Policy: frame-ancestors 'self'`
+ `X-Frame-Options: SAMEORIGIN`）：总控页面上有带令牌的写操作，放开给
任意网站嵌入就有点击劫持风险。允许第三方嵌入，在 `rl-fleet.env` 里配：

```
RL_FRAME_ANCESTORS=https://portal.example.com,https://*.corp.example.com
```

重启 rl-fleet 生效。第三方页面这样引用（**只读精简版**：没有页头、页签、
令牌输入框和任何写操作，也不读浏览器里存的令牌）：

```html
<iframe src="https://fleet.example.com:8095/?embed=1&tab=overview"
        style="width:100%;height:640px;border:0"></iframe>
```

`tab` 可选 `overview`（总览）/ `nodes`（节点清单）/ `history`（历史回查）/
`logs`（日志）。

**HTTPS**：第三方页面是 HTTPS 时，被嵌入的总控也必须是 HTTPS，否则浏览器
按"混合内容"直接拦掉。两种做法：

- 总控自己开 HTTPS：`rl-fleet.env` 里设 `RL_TLS_CERT` / `RL_TLS_KEY`
  （PEM，服务用户 rl-limiter 可读），重启生效；
- 前面放反向代理（nginx 等）做 TLS——此时代理不要覆盖总控返回的
  `Content-Security-Policy` 头。

## 6. 浏览器跨域调用（CORS）

第三方**后端**调 API 不涉及跨域，什么都不用配。第三方**浏览器前端**直接
调总控 API 时，要把它的来源加入白名单：

```
RL_CORS_ORIGINS=https://ops.example.com
```

只收精确来源（不支持通配）；不在白名单的来源，预检请求返回 403。注意把
令牌放在浏览器前端意味着用这个页面的人都能拿到它——前端集成只建议用
`limits` 等最小范围的密钥，或者经第三方自己的后端转发。

## 7. 配置项汇总（rl-fleet.env）

| 变量 | 默认 | 说明 |
| ---- | ---- | ---- |
| `RL_API_TOKEN` | 未设 | 管理员令牌（高强度，见 [11](11-发布与安装.md) §3.4） |
| `RL_FRAME_ANCESTORS` | 未设 = 只许同源 | 允许 iframe 嵌入的来源，逗号分隔，可 `https://*.example.com`；`*` = 任何网站（启动告警） |
| `RL_CORS_ORIGINS` | 未设 = 不放行 | 允许浏览器跨域调 `/api/` 的精确来源 |
| `RL_TLS_CERT` / `RL_TLS_KEY` | 未设 = HTTP | HTTPS 证书链与私钥（两个一起设） |

配错（来源格式不对、只给了证书没给私钥、证书加载失败）rl-fleet 直接
启动失败并写明原因——不会让人以为放行了、实际被浏览器拦。
