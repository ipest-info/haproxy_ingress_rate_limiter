# 07 API 参考

控制台同端口（`RL_CONSOLE_PORT`）提供全部 HTTP API。约定：

- **速率字段一律 bytes/s（应用层口径）**，与配置/页面的 Mbps 换算
  （×8÷1e6）只在显示层做；闭环调节视图（regulation）例外，整段 Mbps；
- 读接口无鉴权（部署边界见 [06](06-控制台与监控.md) §6）；写接口要
  令牌：请求头 `X-API-Token: <token>` 或 `Authorization: Bearer <token>`；
- 错误一律 `{"error": "<中文原因>"}`，错误信息就是给人看的，照着改即可。

## 1. 读接口

### GET /api/overview

服务与配置的当前视图 + 最新一拍快照。

| 字段 | 说明 |
| ---- | ---- |
| `service_version` / `config_version` / `uptime_s` | 版本与运行时长（config_version 是配置内容校验和，变了=热更过） |
| `frontends` | 受管清单的配置视图：`{名: {bind_address, bind_port, mode, quota_mbps, quota_bytes_per_s, limited, ...}}` |
| `request_limits` | 入向限速登记：`{名: {mbps, conn_rate}}` |
| `instance_quota_mbps` / `instance_quota_bytes_per_s` | 实例出向总目标（0=不限） |
| `instance_quota_executable` | 本机是否有实例总限速的执行者（false 时 PUT 会 409） |
| `haproxy` | `{name, endpoint, unix, degraded}`——degraded=true 表示采样失联，各速率为陈旧值 |
| `latest` | 最新一拍快照（结构同 /api/history 的元素），没采到过为 null |

### GET /api/history

`{"snapshots": [...]}`，近 10 分钟、1s 一拍。快照结构：

```jsonc
{
  "ts": 1760580000.0,
  "config_version": 123456,
  "units": {            // 按 frontend
    "fe_main": {
      "rate_bytes_per_s": ..., "rate_in_bytes_per_s": ...,   // 瞬时（出/入）
      "mean10_bytes_per_s": ..., "ewma60_bytes_per_s": ...,  // 10s 均值 / EWMA
      "conn": ..., "active_conns": ..., "idle_conns": ...,
      "conn_new_ps": ..., "conn_denied_ps": ...,
      "pkts_out_ps": ..., "drop_out_ps": ..., "overlimit_ps": ...,  // tc 队列口径
      "quota_bytes_per_s": ..., "over": false, "degraded": false
    }
  },
  "instance": {          // 整机
    "conn": ..., "active_conns": ..., "idle_conns": ..., "max_conn": ...,
    "conn_new_ps": ..., "conn_denied_ps": ...,
    "rate_in_bytes_per_s": ..., "rate_out_bytes_per_s": ...,   // HAProxy 口径
    "nic": "eth1", "pkts_in_ps": ..., "pkts_out_ps": ...,
    "drop_in_ps": ..., "drop_out_ps": ...,
    "nic_rate_in_bytes_per_s": ..., "nic_rate_out_bytes_per_s": ...,  // 网卡口径
    "idle_pct": ..., "degraded": false
  },
  "haproxy": {"name": ..., "endpoint": ..., "unix": true, "degraded": false},
  "regulation": {        // 仅闭环在工作时存在；单位一律 Mbps
    "instance": {
      "target_mbps": 40.0,       // 实例出向目标
      "gate_mbps": 0.58,         // 入向总闸当前值；null = 全开
      "gate_max_mbps": 40.0,     // 总闸上限
      "ingress_mbps": 1.1,       // 最近一轮实测流入；null = 暂无
      "egress_mbps": 39.6,       // 出向总观测（mean10）
      "landed": true             // 是否已按实测流入落位
    },
    "frontends": {               // 受端口级闭环管的端口
      "fe_main": {"target_mbps": 40.0, "max_mbps": 10.0,
                   "cap_mbps": 5.2,    // 当前动态入向上限；null = 全开/静态
                   "egress_mbps": 38.9}
    }
  }
}
```

### GET /api/stream

SSE（`text/event-stream`），每拍推一帧快照（结构同上），连上先补发最新
一帧。页面的实时曲线走它。

### GET /api/logs?after=<seq>

进程内日志环形缓冲的增量拉取：`{"logs": [{seq, ts, level, msg}]}`。

### GET /api/tc

tc 调试面板数据：`{"sections": [{key, title, iface, desired, help[],
raw: [{label, note, text}]}]}`。desired 是期望配置（动态值已标注），
raw 是现跑 `tc ... show` 的原始输出。限速未接线时
`{"sections": [], "disabled_reason": "..."}`。

### GET /api/config-view

配置页数据：`{"yaml_path", "sections": [...]}`。section 两种形态：
只读 kv（`{title, hint, kv: [{label, value}]}`）与可编辑行
（`{title, hint, rows: [{label, sub, fields: [{name, label, value,
api: {method, url, key}, disabled?, disabled_reason?}], del?: {url,
label}}]}`）——可编辑字段带着自己的写 API 描述，客户端照着提交即可，
不必认识具体字段。

### GET /metrics

Prometheus 文本格式，与快照同源同口径。指标名前缀 `rl_limiter_`，
速率一律 `*_bytes_per_second`。

## 2. 写接口（令牌鉴权，回写 YAML 热生效）

统一行为：修改经整份校验后原子回写 YAML（校验不过 400、文件不动），
并立即 poke 配置轮询——数秒内热生效。未配置 `RL_API_TOKEN` 时整体
403；令牌缺失/错误 401；段名不存在 400（会列出当前解析到的段名）。

### PUT /api/quotas/{name}

```bash
curl -X PUT -H "X-API-Token: $TOKEN" -H "Content-Type: application/json" \
  -d '{"quota_mbps": 40}' http://127.0.0.1:8090/api/quotas/fe_main
```

登记/修改该端口的**出向带宽目标**（本分支：闭环判断基准）。0 = 显式
不限速。

### DELETE /api/quotas/{name}

删除登记（回到只监控不限速）。响应 `{"removed": true|false}`。

### PUT /api/instance-quota · DELETE /api/instance-quota

实例出向总目标：`{"quota_mbps": 40}`；0 或 DELETE = 取消。本机没有
执行者（`instance_quota_executable=false`）时 PUT 409 并说明替代方案
（DELETE 仍放行：清掉残留配置是合法清理）。

### PUT /api/request-limits/{name}

```bash
curl -X PUT -H "X-API-Token: $TOKEN" -H "Content-Type: application/json" \
  -d '{"mbps": 10, "conn_rate": 150}' \
  http://127.0.0.1:8090/api/request-limits/fe_main
```

登记/修改入向限速。**部分更新**：`mbps`（≥0 数字）与 `conn_rate`
（≥0 整数）至少给一个，只给一个时另一个保留现值——两个维度各自独立
保存，不互相清零。响应回显写入后的整段
`{"request_limit": {"mbps": ..., "conn_rate": ...}}`。

### DELETE /api/request-limits/{name}

删除入向限速登记（回到不限请求方向）。

## 3. 集成提示

- 轮询场景用 `/api/overview`（自带 latest），曲线场景先 `/api/history`
  补历史再挂 `/api/stream`；
- `config_version` 变化 = 配置热更过，拉一次 overview 刷新配置视图；
- 判断"限速是否真的在执行"别只看配置：对照 `/api/tc` 的原始输出，或
  机器上 `python3 tools/tc_check.py verify`。
