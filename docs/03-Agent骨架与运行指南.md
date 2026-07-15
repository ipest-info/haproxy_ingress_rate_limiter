# Agent 骨架与运行指南

> 对应设计：[01-方案设计.md](01-方案设计.md)。本文只覆盖当前代码骨架的构建、本地演示与配置说明。

## 1. 构建

```bash
make build          # 产出 bin/rl-agent 与 bin/mock-controller
make test           # 全部单元测试
```

单位约定：配置里的 `quota_bps` 一律为 **bit/s**（200000000 = 200 Mbps），Agent 内部统一使用 **bytes/s**。

## 2. 本地 dry-run 演示

mock-controller 是控制面的开发替身：从一个 JSON 文件（[deploy/config/mock-controller-config.json](../deploy/config/mock-controller-config.json)）提供配置长轮询，版本号 = 文件 mtime（秒），改文件即触发下发；metrics / heartbeat 只打日志。

```bash
# 终端 1：控制面替身
./bin/mock-controller -addr :9090 -config deploy/config/mock-controller-config.json

# 终端 2：Agent（先复制示例配置并按本机情况改 stats_socket / cache_path）
cp deploy/config/agent.example.yaml /tmp/agent.yaml
# 建议本地演示改两处：
#   controller.cache_path: /tmp/rl-agent-cache.json
#   haproxy.stats_socket:  指向本机 haproxy 管理 socket
./bin/rl-agent -config /tmp/agent.yaml
```

可观察到的行为：

- rl-agent 侧（stderr，slog 文本格式）：启动即打 `seeded from local envs`（或有缓存时 `seeded from controller cache`），随后 `controller config received version=<mtime>`。
- mock-controller 侧：`config served`、每 10s `heartbeat received`、每 5s `metrics received ... samples=5`。
- 修改（或 `touch`）mock-controller-config.json，1 秒内新版本推送到 Agent。
- **DRY-RUN 日志**：需要本机有真实 HAProxy（用 [deploy/haproxy/bwlim-example.cfg](../deploy/haproxy/bwlim-example.cfg) 的机制）供采集。首个成功采样后，限速目标每次变化都会打：
  `DRY-RUN would set bwlim env=env-a state=normal bwlim_bytes_per_sec=2.75e+07 frontends=[fe_env_a]`
  （200 Mbps = 25,000,000 bytes/s，×1.10 弹性上限 = 27,500,000）。
- 没有 HAProxy 时不会有 DRY-RUN 行：Agent 每秒打 `stats sample failed; holding last rates`，连续 10 次后打 `collector degraded`，控制面链路仍可完整演示。

切到真实执行：`mode: enforce`（写 map 需要 admin socket 与 map 文件已就位，见 §4）。

## 3. 配置参考（/etc/rl-agent/config.yaml）

| 键 | 默认值 | 说明 |
| --- | --- | --- |
| `node_id` | 必填 | 节点唯一标识，与控制面 LB_NODE 对应 |
| `mode` | `dry-run` | `dry-run` 只打日志；`enforce` 真实写 bwlim map |
| `log_level` | `info` | debug / info / warn / error |
| `haproxy.stats_socket` | `/var/run/haproxy/admin.sock` | 管理 socket，须 `level admin` |
| `haproxy.bwlim_map_path` | `/etc/haproxy/maps/bwlim.map` | 与 haproxy 配置中 `map_str_int(...)` 路径一致 |
| `haproxy.timeout_ms` | `500` | 单次 runtime API 调用超时（毫秒） |
| `controller.base_url` | 空 | 空 = standalone，只用本地 `envs`；非空 = 长轮询 + 上报 + 心跳 |
| `controller.cache_path` | `/var/lib/rl-agent/config-cache.json` | fail-static 缓存（systemd `StateDirectory=rl-agent` 自动建目录） |
| `controller.tls.ca_file / cert_file / key_file` | 空 | mTLS 材料，生产必配 |
| `envs[].env_id` | 必填 | 环境 ID |
| `envs[].frontends` | 必填 | 本节点上属于该环境的 frontend 名列表（不允许跨环境重复） |
| `envs[].quota_bps` | 必填 | 约定带宽，**bit/s** |
| `envs[].params` | 缺省用默认 | 快环参数覆盖（§3.3：1.10 / 0.90 / 3s / 5s / ×0.9 / 0.95 / +5%） |

配置优先级：控制面下发 > 本地缓存（fail-static）> `envs` 引导值。

## 4. HAProxy 前置条件

- **版本 ≥ 2.8**（`bwlim` 过滤器 2.8 起正式；无法升级的节点只能退化为 tc 单层硬限）。
- `global` 中开启 admin 级 stats socket：
  `stats socket /var/run/haproxy/admin.sock mode 660 level admin expose-fd listeners`
- map 文件 `/etc/haproxy/maps/bwlim.map` 必须在 HAProxy 启动前存在（启动时加载），每行 `<frontend名> <bytes/s>`；运行期条目归 Agent 所有，通过 runtime API `set map` 更新，改限速不需要 reload。
- frontend 侧的 bwlim 接线（含逐行说明、按流动态限速与共享 stick-table 两种方案的取舍）见 [deploy/haproxy/bwlim-example.cfg](../deploy/haproxy/bwlim-example.cfg)。
- 内核层兜底：[deploy/tc/backstop.sh](../deploy/tc/backstop.sh)，速率取节点配额的 115%（§3.2 L3），如 200 Mbps 配额 → `./backstop.sh eth0 230`。

## 5. 故障行为摘要（骨架已实现部分，对应设计 §3.7）

| 故障 | 行为 |
| --- | --- |
| 控制面断联 / 宕机 | 长轮询按 1~30s 指数退避重试；Agent 按最后一次下发配置继续限速（fail-static），启动时从 `cache_path` 复原，绝不放开为不限速 |
| 采集失败（socket 超时等） | 当秒沿用上一秒速率，窗口/EWMA 继续滚动；连续 10 秒失败进入 degraded：限速值冻结不动，恢复采样后自动解除 |
| 上报失败 | 样本本地缓冲（上限 600 条 ≈ 10 分钟），恢复后补发，超限丢最旧 |
| Agent 崩溃 | systemd `Restart=always` 2 秒拉起；期间 tc 兜底仍在内核生效，HAProxy 残留限速值保持原样（安全方向） |
| dry-run → enforce 切换 | 触发一次全量 resync，把当前所有限速值真实写入 map（dry-run 期间 map 可能已过期） |

## 6. 骨架刻意未包含（后续里程碑）

- **真实 Controller**：配额管理、审批审计、对账、告警都不在；mock-controller 只回放一个静态 JSON。
- **全局慢环**：多节点按用量再分配环境配额（M3）；当前每节点只对本地配额负责。
- **tc 自动化**：backstop.sh 需手工执行，Agent 不校验/不维护 tc 规则。
- **用量数据落库**：metrics 上报后即丢弃，无时序库、无 Prometheus 指标端点、无大盘。
- 其他：HAProxy reload 检测与限速值重放（§3.7）、mTLS 证书签发流程（配置项已留好）。
