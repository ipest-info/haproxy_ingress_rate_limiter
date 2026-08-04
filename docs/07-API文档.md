# rl-limiter HTTP API 文档（面向第三方开发）

rl-limiter 在 Web 控制台端口上同时提供一组 HTTP API：**读接口**（监控
数据、配置视图、Prometheus 指标、日志、SSE 实时流）与**写接口**（修改
限额，带令牌鉴权）。控制台页面本身就是这组 API 的第一个消费者——页面
能做到的，第三方程序都能做到。

适用版本：与本仓库同版本的 rl-limiter。接口稳定性见 [§8](#8-兼容性与稳定性)。

---

## 1. 基础信息

| 项 | 值 |
| --- | --- |
| Base URL | `http://<主机>:<RL_CONSOLE_PORT>`（默认端口 `8090`，默认只绑 `127.0.0.1`） |
| 传输编码 | 请求/响应一律 UTF-8；JSON 响应 `Content-Type: application/json` |
| 认证 | 读接口无认证；写接口需令牌（见 §2） |
| 部署形态 | **每台 HAProxy 机器各有一个独立实例**，API 只反映/操作本机。跨机汇总由调用方自己做 |

服务未设置 `RL_CONSOLE_PORT` 时整个 HTTP 服务（含全部 API）不启动。

> **网络边界**：读接口暴露全量监控数据与运行日志，默认只绑回环。放到
> 内网必须由运维显式设置 `RL_CONSOLE_BIND` 并用防火墙/安全组限制来源，
> **绝不可暴露公网**。第三方程序应从内网访问。

## 2. 认证（仅写接口）

写接口的令牌由服务端环境变量 `RL_API_TOKEN` 配置。请求时二选一携带：

```
Authorization: Bearer <token>
```

或

```
X-API-Token: <token>
```

| 情形 | 状态码 | 响应 |
| --- | --- | --- |
| 服务端未配置 `RL_API_TOKEN` | `403` | `{"error": "未配置 RL_API_TOKEN，写接口已禁用…"}`（写功能整体关闭） |
| 未带令牌 / 令牌错误 | `401` | `{"error": "令牌缺失或不正确…"}` |

读接口不校验令牌，带了也会被忽略。

## 3. 通用约定

- **单位**：速率字段一律 `bytes/s`（应用层口径，除非字段名带 `nic_`/
  `pkts_`/`drop_` 前缀——那些是网卡/链路层口径）；限额的**配置**单位是
  Mbps（与 YAML 一致），响应里通常两种都给（`quota_mbps` 与换算好的
  `quota_bytes_per_s = quota_mbps × 1e6 ÷ 8`）。
- **时间戳**：`ts` 为 Unix 秒（浮点）。
- **错误响应**：非 2xx 时 body 为 `{"error": "<中文原因>"}`，信息面向
  人类可读，直接展示给使用者即可。
- **`degraded` 字段**：为 `true` 时表示采样失联（HAProxy 挂了/socket
  不可读），各速率字段是**陈旧值**而不是 0——消费方做告警时必须先看它。

---

## 4. 读接口

### 4.1 `GET /api/overview` — 当前状态总览

服务信息 + 配置视图 + 最新一拍监控快照。轮询频率建议 ≤ 1 次/秒（数据
本身 1 秒一拍，更快没有意义）。

```bash
curl -s http://127.0.0.1:8090/api/overview
```

响应（节选注释）：

```jsonc
{
  "service_version": "0.4.0",
  "config_version": 3750930076,     // 当前配置的内容校验和（见 §8）
  "uptime_s": 86400.5,
  "frontends": {                    // 受管 frontend 的配置视图（cfg + YAML 合并结果）
    "fe_main": {
      "name": "fe_main",
      "bind_address": "",           // 空 = 通配
      "bind_port": 8080,
      "quota_mbps": 40.0,           // 0 = 不限速
      "mode": "tcp",
      "quota_bytes_per_s": 5000000.0
    }
  },
  "instance_quota_mbps": 800.0,     // 实例总限速（0 = 不限）
  "instance_quota_bytes_per_s": 100000000.0,
  "haproxy": {
    "name": "haproxy",
    "endpoint": "/run/haproxy/admin.sock",
    "unix": true,                   // true = 本机 unix socket，false = 内网 TCP
    "degraded": false               // true = 采样失联
  },
  "latest": { /* 最新一拍快照，结构见 §4.2；启动后未采到样时为 null */ }
}
```

### 4.2 `GET /api/history` — 最近历史快照

返回内存里的最近约 10 分钟（600 拍）快照，用于回填曲线。重启即清空
——长期历史请用 `/metrics`（Prometheus 抓取）或监控落盘 JSONL。

```jsonc
{ "snapshots": [ <snapshot>, ... ] }   // 按时间从旧到新
```

每个 `snapshot`：

```jsonc
{
  "ts": 1785723600.0,
  "config_version": 3750930076,
  "units": {                        // 按 frontend 名索引
    "fe_main": {
      "rate_bytes_per_s": 4998321.0,    // 实时下行速率（限速的方向）
      "mean10_bytes_per_s": 5003210.0,  // 10 秒滑动均值（计费/超限判定口径）
      "ewma60_bytes_per_s": 4712000.0,  // 60 秒指数均值
      "rate_in_bytes_per_s": 88231.0,   // 实时上行速率
      "conn": 35,                       // 并发连接数
      "active_conns": 20,
      "idle_conns": 15,
      "conn_new_ps": 12.0,              // 每秒新建连接
      "conn_denied_ps": 0.0,            // 每秒被拒绝连接
      "pkts_out_ps": 4300.0,            // tc 队列每秒流出包数（链路层，出方向）
      "drop_out_ps": 2.0,               // tc 每秒丢包（被限速丢弃）
      "overlimit_ps": 130.0,            // tc 每秒触发限速次数（限额吃紧信号）
      "backlog_bytes": 51200,           // tc 队列当前积压字节
      "quota_bytes_per_s": 5000000.0,   // 登记限额（null/0 = 未限速）
      "over": false,                    // 瞬时超限标记（mean10 > 限额）
      "degraded": false                 // 该 frontend 采样是否失联
    }
  },
  "instance": {                     // 整台 HAProxy（可能为 {}：无实例采样）
    "conn": 120, "active_conns": 70, "idle_conns": 50,
    "max_conn": 100000,             // 进程连接上限
    "conn_new_ps": 33.0, "conn_denied_ps": 0.0,
    "rate_in_bytes_per_s": 1000000.0,   // HAProxy 应用层口径
    "rate_out_bytes_per_s": 5000000.0,
    "nic": "eth0",                      // 网卡口径统计取自哪张网卡（"" = 未采集）
    "nic_rate_in_bytes_per_s": 5400000.0,   // 网卡/链路层口径（整机）
    "nic_rate_out_bytes_per_s": 5600000.0,
    "pkts_in_ps": 4400.0, "pkts_out_ps": 4600.0,
    "drop_in_ps": 0.0, "drop_out_ps": 0.0,
    "idle_pct": 87,                 // HAProxy 自报空闲率（%）
    "degraded": false
  },
  "haproxy": { /* 同 overview.haproxy */ }
}
```

> 应用层口径（HAProxy 计数）与链路层口径（tc/网卡计数）**数值本就不
> 相等**（后者含以太网/IP/TCP 头与重传，典型高 3%~8%），字段名前缀刻意
> 区分，请勿混用比较。

### 4.3 `GET /api/stream` — SSE 实时流

Server-Sent Events：每拍（1 秒）推送一帧 §4.2 的 `snapshot`。连接建立
后先补发最新一帧，之后跟拍。

```
Content-Type: text/event-stream; charset=utf-8

data: {"ts": 1785723600.0, "config_version": ..., "units": {...}, ...}

data: ...
```

消费要点：

- 每个 event 只有 `data:` 行（无 event/id 字段），一帧一个 JSON；
- 消费慢时服务端**丢最旧帧**（队列深度 5），不保证每拍必达——需要
  完整序列请轮询 `/api/history` 补齐；
- 服务优雅停机时流会正常结束（EOF），客户端应自动重连（浏览器
  `EventSource` 默认就会）。

### 4.4 `GET /metrics` — Prometheus 抓取端点

文本 exposition 格式，全部为 gauge，与控制台曲线同源（最新一拍）。
抓取间隔建议 ≥ 15s（值本身每秒更新，Prometheus 侧不需要更密）。

主要指标（`{frontend="<名>"}` 为按 frontend 打标签的）：

| 指标 | 说明 |
| --- | --- |
| `rl_limiter_info{version,haproxy}` | 服务元信息（值恒 1） |
| `rl_limiter_config_version` | 配置内容校验和 |
| `rl_limiter_uptime_seconds` | 运行秒数 |
| `rl_limiter_haproxy_degraded` | 采样失联（1=失联，各速率为陈旧值） |
| `rl_limiter_instance_quota_bytes_per_second` | 实例总限速（0=不限，罩全部 frontend 出向流量合计） |
| `rl_limiter_frontend_quota_bytes_per_second{frontend}` | 登记限额（0=不限速） |
| `rl_limiter_frontend_rate_bytes_per_second{frontend}` | 实时下行速率 |
| `rl_limiter_frontend_mean10_bytes_per_second{frontend}` | 10s 均值（计费/超限口径） |
| `rl_limiter_frontend_rate_in_bytes_per_second{frontend}` | 实时上行速率 |
| `rl_limiter_frontend_connections{frontend}` | 并发连接数 |
| `rl_limiter_frontend_active_connections{frontend}` / `_idle_connections` | 活跃/空闲连接 |
| `rl_limiter_frontend_new_connections_per_second{frontend}` | 每秒新建连接 |
| `rl_limiter_frontend_denied_per_second{frontend}` | 每秒被拒绝连接 |
| `rl_limiter_frontend_tc_packets_out_per_second{frontend}` | tc 每秒流出包数 |
| `rl_limiter_frontend_tc_drops_per_second{frontend}` | tc 每秒丢包（被限速丢弃） |
| `rl_limiter_frontend_tc_overlimits_per_second{frontend}` | tc 每秒触发限速次数 |
| `rl_limiter_frontend_over_quota{frontend}` | 瞬时超限（mean10>限额） |
| `rl_limiter_frontend_degraded{frontend}` | 该 frontend 采样失联 |
| `rl_limiter_instance_connections` / `_max_connections` | 整机并发/上限 |
| `rl_limiter_instance_new_connections_per_second` | 整机每秒新建 |
| `rl_limiter_instance_rate_in_bytes_per_second` / `_rate_out_…` | 整机上/下行（HAProxy 口径） |
| `rl_limiter_nic_rate_in_bytes_per_second` / `_out_…` | 网卡入/出向速率（链路层） |
| `rl_limiter_nic_packets_in_per_second` / `rl_limiter_nic_drops_in_per_second` | 网卡入包/入向丢包 |
| `rl_limiter_haproxy_idle_percent` | HAProxy 自报空闲率 |

### 4.5 `GET /api/logs?after=<seq>` — 运行日志（增量）

进程内环形缓冲（最近 1000 条）的增量拉取。`after` 传上次收到的最大
`seq`（首次传 0），单次最多返回 500 条，从旧到新。

```jsonc
{
  "logs": [
    {
      "seq": 1234,                 // 单调递增；重启后从 1 重新计
      "ts": 1785723600.123,
      "level": "WARNING",          // DEBUG/INFO/WARNING/ERROR/CRITICAL
      "logger": "rl_limiter.tc",
      "msg": "已就地调整 tc 限速…"
    }
  ]
}
```

---

## 5. 写接口（修改限额）

四个端点，全部需要令牌（§2）。**能改的只有限额**：单 frontend 的
quota 与实例总限速。负载均衡配置（监听端口、后端服务器）**没有任何写
接口**——那以本机 haproxy.cfg 为唯一权威，只归运维手工编辑。

写入行为（四个端点相同）：

1. 修改后的**整份配置**先过与服务启动完全相同的校验链——不合法直接
   `400`，配置文件一个字节不动，绝不会写出一份服务重启后加载不了的
   YAML；
2. 通过后以"临时文件 + 原子替换"回写到服务的 YAML 配置文件（读者绝
   不会见到半份文件；**文件里的手写注释会丢**，文件头部会注明）；
3. 立即触发配置重读（不等 5s 轮询），限速（tc）与监控基准**数秒内**
   热生效——生效结果可通过 `/api/overview` 的对应字段确认；
4. 服务内并发写请求串行化；与运维手工编辑文件并发时**后写的赢**（不做
   合并）。

### 5.1 `PUT /api/quotas/{name}` — 设置单个 frontend 的限额

`{name}` 为 haproxy.cfg 里 frontend/listen 段的名字（需 URL 编码）。

请求体：

```json
{ "quota_mbps": 20 }
```

`quota_mbps`：数字，≥ 0，单位 Mbps，允许小数。**`0` 有明确语义**：
"显式不限速"——撤掉该端口的 tc 限速类、不再提示"未登记"，与"删除
登记"（§5.2）的区别只在语义标注（0 = 运维确认过的决定）。

```bash
curl -X PUT \
     -H "X-API-Token: $TOKEN" -H "Content-Type: application/json" \
     -d '{"quota_mbps": 20}' \
     http://127.0.0.1:8090/api/quotas/fe_main
```

成功 `200`：

```json
{ "ok": true, "frontend": "fe_main", "quota_mbps": 20.0 }
```

失败：

| 状态码 | 情形 | error 内容 |
| --- | --- | --- |
| `400` | `{name}` 不是 cfg 里现有的段名 | 点名该名字并**列出当前解析到的全部段名**（多半是拼错了） |
| `400` | body 不是 JSON / 缺 `quota_mbps` / 不是数字 / 负数 / 太小（>0 但不足 1 byte/s） | 具体原因 |
| `401`/`403` | 见 §2 | |

### 5.2 `DELETE /api/quotas/{name}` — 删除限额登记

该 frontend 回到"只监控不限速"状态（服务启动时会 warn 提醒未登记，
这是与写 0 的差别）。无请求体。

```bash
curl -X DELETE -H "X-API-Token: $TOKEN" \
     http://127.0.0.1:8090/api/quotas/fe_main
```

成功 `200`（幂等：本来就没登记时 `removed: false`，文件不动）：

```json
{ "ok": true, "frontend": "fe_main", "removed": true }
```

### 5.3 `PUT /api/instance-quota` — 设置实例总限速

本机 HAProxy **全部 frontend** 出向流量合计的总闸（含未在 quotas 里
登记限额、只监控的段——它们也被总闸罩住）；SSH/监控等系统流量**不在**
总闸内、不受影响。各 frontend 自己的限额仍各自生效（单端口上限 =
min(自身限额, 总限速)）。tc 侧切换为层级模式，原理与超卖时的带宽分配
规则见 [06-tc限速方案.md §3.1](06-tc限速方案.md)。

请求体同 §5.1（`quota_mbps`，Mbps，≥ 0；`0` = 取消总限速）。

```bash
curl -X PUT \
     -H "X-API-Token: $TOKEN" -H "Content-Type: application/json" \
     -d '{"quota_mbps": 800}' \
     http://127.0.0.1:8090/api/instance-quota
```

成功 `200`：

```json
{ "ok": true, "instance_quota_mbps": 800.0 }
```

注意：监听在 **65535** 端口的 frontend 无法参与限速（其 tc classid 与
总限速的聚合类冲突，限速下发会整体拒绝并在日志报错）；正常业务端口
不受影响。

### 5.4 `DELETE /api/instance-quota` — 取消实例总限速

等价于 `PUT {"quota_mbps": 0}`：回到平铺模式（只按端口各限各的）。
无请求体。成功 `200`：

```json
{ "ok": true, "instance_quota_mbps": 0 }
```

---

## 6. 状态码速查

| 状态码 | 含义 |
| --- | --- |
| `200` | 成功；写接口的实际生效在数秒内（可轮询 overview 确认） |
| `400` | 请求不合法：body 格式/数值校验不过、frontend 名不存在、或修改会让整份配置非法 |
| `401` | 令牌缺失或不正确 |
| `403` | 服务端未配置 `RL_API_TOKEN`，写功能整体关闭 |
| `404`/`405` | 路径/方法不存在 |

## 7. 集成示例

轮询限速利用率并在持续超限时告警（Python）：

```python
import time, requests

BASE = "http://127.0.0.1:8090"

while True:
    o = requests.get(f"{BASE}/api/overview", timeout=3).json()
    snap = o.get("latest") or {}
    for name, u in (snap.get("units") or {}).items():
        if u.get("degraded"):
            continue                      # 陈旧数据不判超限
        quota = u.get("quota_bytes_per_s")
        if quota and u["mean10_bytes_per_s"] > quota:
            print(f"[over] {name}: mean10 "
                  f"{u['mean10_bytes_per_s']*8/1e6:.1f} Mbps > "
                  f"限额 {quota*8/1e6:.1f} Mbps")
    time.sleep(5)
```

按外部计费系统的结果调整限额（带鉴权写入 + 确认生效）：

```python
import time, requests

BASE, TOKEN = "http://127.0.0.1:8090", "<RL_API_TOKEN>"
H = {"X-API-Token": TOKEN}

r = requests.put(f"{BASE}/api/quotas/fe_main",
                 json={"quota_mbps": 100}, headers=H, timeout=5)
r.raise_for_status()                      # 400/401/403 时 body["error"] 是中文原因

for _ in range(10):                       # 数秒内热生效，轮询确认
    o = requests.get(f"{BASE}/api/overview", timeout=3).json()
    if o["frontends"]["fe_main"]["quota_mbps"] == 100:
        print("已生效")
        break
    time.sleep(1)
```

## 8. 兼容性与稳定性

- **只增不改**：已文档化的字段与端点保持语义不变，新版本可能**新增**
  字段/指标/端点——消费方解析 JSON 时应忽略未知字段；
- `config_version` 是配置内容的**校验和**，只用于"变没变"的判断，
  **不是单调递增的版本号**，不要拿它比大小；
- `/api/logs` 的 `seq` 重启后从头计；`/api/history` 重启即清空——两者
  都是进程内数据，不承诺持久性；
- 日志的 `msg` 是面向人的中文文本，格式**不承诺稳定**，程序化消费请
  用 `/metrics` 或监控落盘 JSONL（见 [03-限速服务运行指南.md](03-限速服务运行指南.md)），
  不要解析日志文本；
- 未文档化的接口（如页面静态资源）不属于兼容性承诺范围。
