# hap-agg：HAProxy 多实例监控聚合

把 N 台 HAProxy 合并成**一个监控视图**。经各台机器 haproxy.cfg 里暴露
的内网 TCP stats socket（`stats socket ipv4@*:9999`）每秒批量采样
`show stat` / `show info`，聚合展示在 Web 页面上，并同时提供 JSON API、
SSE 实时流与 Prometheus `/metrics`。

**纯只读**：不写任何 HAProxy 状态，目标机器**零安装**——只要 cfg 里
有一条 stats socket。

## 能看到什么

- **聚合总览**：全部健康目标合计的流入/流出带宽、整机并发、frontend
  并发、每秒新建/拒绝连接 + 实时曲线；
- **实例列表**：每台一行——状态（正常/抖动/失联+原因）、并发、带宽、
  新建/拒绝、HAProxy 空闲率、运行时长；
- **frontend 聚合**：同名 frontend 跨机合并（多台机器配置相同的水平
  扩容形态）：实例数、并发、带宽、新建/拒绝；
- **视图选择**：页头"视图选择"下拉可勾选要显示的板块与曲线（聚合
  总览/实例列表/frontend 聚合/批量导入，及总览里的每张曲线），选择存
  浏览器本地，刷新后保持；
- **目标管理**：页面/API 上批量导入 `IP:port`（每行一条，可选命名），
  立即生效并回写 YAML；带令牌鉴权（`HAP_AGG_TOKEN`）。

## 快速开始

```bash
make install    # pip install -e ".[test]"
make test       # 全量单元测试

make demo-up    # docker compose 一键演示：三台 HAProxy（官方 2.8 镜像，
                # 零安装，只挂了一份带 stats socket 的 cfg）+ 模拟后端
                # + 压测 + 聚合器
                # 页面：http://localhost:8100（写操作令牌 demo-token）
make demo-down
```

生产上对着真实机器跑：

```bash
cp deploy/hap-agg.example.yaml /etc/hap-agg/config.yaml   # 编辑 targets
HAP_AGG_TOKEN=$(openssl rand -hex 24) hap-agg -c /etc/hap-agg/config.yaml
# 页面默认 http://127.0.0.1:8100；放内网用 --bind 并配防火墙
```

前提（每台被聚合的机器，haproxy.cfg 的 global 段）：

```
stats socket ipv4@<内网IP>:9999 level user
```

`level user` 就够（本程序只读）。已有的 `level admin expose-fd
listeners` socket 也能用，但 admin 级 socket 请务必绑内网 IP 并用
防火墙限制来源。

## 文档

| 文档 | 说明 |
| ---- | ---- |
| [docs/01-使用指南.md](docs/01-使用指南.md) | 前提、配置、批量导入规则、视图与口径、容错行为 |
| [docs/02-API文档.md](docs/02-API文档.md) | HTTP API 参考（面向第三方开发）：读接口字段口径、写接口与错误码、Prometheus 指标表 |

## 系统组成

```
hap_agg/          # Python ≥3.9 + asyncio
  haproxy.py      #   HAProxy runtime API 客户端（TCP/unix stats socket，只读）
  model.py        #   采样原始统计类型（FrontendStat / InstanceStat）
  sampler.py      #   多目标并发采样 + 计数器差分 + 合并快照
  config.py       #   YAML 配置（targets 清单 + 原子回写）
  web.py          #   Web 视图 / JSON API / SSE / metrics / 目标管理写接口
  __main__.py     #   服务入口（hap-agg 命令）
  static/         #   单页前端（含视图选择器）
tools/            # fake_haproxy.py（联调假目标）、random_web.py（模拟后端）、
                  # loadgen.py（可调并发压测）——demo/测试用
deploy/
  hap-agg.example.yaml   # 配置示例
  demo/                  # compose 演示用的最小 haproxy.cfg 与聚合器配置
docker-compose.yml       # 一键演示（3×HAProxy 官方镜像 + 后端 + 压测 + 聚合器）
```

## 设计要点

- 速率一律**应用层 bytes/s**（HAProxy 计数器差分），页面显示 Mbps；
  reload 清零按 0 处理（一拍空洞后自动恢复）；
- 单目标失败隔离；连续 3 拍失败标记失联，**不计入聚合合计**（陈旧
  速率混进总数会让总带宽虚高），行仍保留在实例列表里带错误原因；
- 批量导入一行错整批拒绝、幂等合并、原子回写 YAML；
- 读接口无鉴权（默认只绑回环，放内网靠防火墙），写接口必须带令牌，
  未配置 `HAP_AGG_TOKEN` 则写接口整体 403、页面退化为只读。
