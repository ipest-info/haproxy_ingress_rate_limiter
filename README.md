# haproxy_ingress_rate_limiter

带宽账密计费模式下的入口动态限速系统：在 HAProxy 入口层，保证每个环境（env）的**下行带宽**（代理返回给客户端的流量）不超过订单约定带宽。

## 文档

| 文档 | 说明 |
| ---- | ---- |
| [docs/00-需求说明.md](docs/00-需求说明.md) | 原始需求（名词定义、架构图、核心诉求） |
| [docs/01-方案设计.md](docs/01-方案设计.md) | 总体方案设计：架构、限速算法、多节点协调、数据模型、接口、容错、里程碑 |
| [docs/02-建议与讨论点.md](docs/02-建议与讨论点.md) | 对需求本身的改进建议与待拍板的决策点 |

## 系统组成（规划）

```
haproxy_ingress_rate_limiter/
├── agent/        # 部署在每台 HAProxy ECS 上的限速代理（采集 + 执行）
├── controller/   # 后台控制面（配额管理、多节点协调、对账、审计）
├── deploy/       # systemd / 发布脚本 / haproxy 配置片段
└── docs/         # 设计文档
```

## 核心思路一句话

Agent 每秒从 HAProxy stats socket 读取各 frontend 的 `bytes_out` 计算下行速率，按控制面下发的节点配额做**滞回 + 渐进式**的准入控制（动态调 `maxconn` / 会话速率），并以 HAProxy 原生 `bwlim-out` 过滤器（或 tc）作为硬性兜底；控制面按环境聚合各节点用量，周期性再分配节点配额，同时承担带宽计费数据的权威管理与对账告警。
