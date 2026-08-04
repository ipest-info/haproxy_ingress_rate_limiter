# Docker Compose 一键演示环境（三台 Ubuntu 24.04 节点）

本演示把完整链路装进一个 `docker compose`（`docker-compose-demo.yml`）。
**每个 node 容器 = 生产上的一台 ECS**：基于 **Ubuntu 24.04 LTS**，容器内
同时跑发行版自带的 **HAProxy 2.8**（TCP L4 转发）与**同机的
rl-limiter**——rl-limiter 通过容器内的 unix socket
（`/run/haproxy/admin.sock`）只采本机那台 HAProxy，stats socket
不占任何网络端口、也无需跨容器可达。限速由各节点的**内核 tc（HTB）**
执行（各 40 Mbps）。

配置来源只有两个本地文件（**没有数据库**）：

- **haproxy.cfg**（模板 `deploy/docker/haproxy-base.cfg`）——负载均衡
  配置的**唯一权威**：`listen fe_main` 监听 8080、转发到 `web:9000`
  都直接写在里面；
- **rl-limiter 的 YAML**（模板 `deploy/docker/limiter-node.yaml`）——
  stats socket 接线 + quotas 限额登记（`fe_main: 40`）。

rl-limiter 轮询两份文件的内容（5s），改了即热生效。后端挂一个**每次请求
返回随机大小响应**的模拟 web 服务，再用一个**可在线调节并发数的压测
服务**打散到全部入口。

> 为什么不用 haproxy 官方镜像：官方镜像只有 haproxy 一个进程，而本项目
> 的部署形态是"rl-limiter 与 HAProxy 同机"。用 Ubuntu 基础镜像装两个
> 组件，容器形态才和生产的单台 ECS 一致。镜像构建时会校验 haproxy
> 版本 ≥ 2.8，不满足直接构建失败。

单节点版：仓库根的 `docker-compose.yml` 是同一形态的单节点配置（无
web/loadgen），适合接自己的后端试用。

## 拓扑

```
   HTTP 并发
┌─────────┐   ┌──────────────────────┐ ┌─────────────────┐ ┌────────────────┐
│ loadgen │──►│ node1  (Ubuntu 24.04)│ │ node2           │ │ node3          │
│ :8084   │──►│  haproxy fe_main     │ │  haproxy fe_main │ │ haproxy fe_main │
└─────────┘──►│   :8080              │ │   :8082          │ │  :8083         │
              │   内核 tc 40M        │ │   tc 40M         │ │  tc 40M        │
              │      │ unix socket   │ │                  │ │                │
              │      ▼ (只读采样)     │ │                  │ │                │
              │  rl-limiter          │ │ rl-limiter       │ │ rl-limiter     │
              │   ↻ 轮询本机 cfg+YAML │ │  ↻ 同左          │ │  ↻ 同左        │
              │   控制台 :8090        │ │  控制台 :8092     │ │  控制台 :8093   │
              └──────────┬───────────┘ └────────┬─────────┘ └───────┬────────┘
                         └───────────────────┬──┴───────────────────┘
                                             ▼
                                      web:9000（随机大小响应）
```

| 服务 | 说明 | 宿主机端口 |
| ---- | ---- | ---- |
| `node1` / `node2` / `node3` | **Ubuntu 24.04 + HAProxy 2.8 + 同机 rl-limiter**（根目录 `Dockerfile`，全仓库统一镜像）。挂进去的 `haproxy-base.cfg` 是**完整的** haproxy.cfg（含 fe_main），`limiter-node.yaml` 登记限额 | 代理 `8080`/`8082`/`8083`；控制台 `127.0.0.1:8090`/`8092`/`8093` |
| `web` | 模拟业务后端，每次请求返回 256 KiB～2 MiB 随机大小响应；`/big` 为大文件下载端点（默认 512 MiB，`WEB_BIG_BYTES` 可调）（`tools/random_web.py`） | 无 |
| `loadgen` | 压测服务，打散到全部入口，并发数可在线调节；默认每入口另挂 1 条**长连接大文件下载**（`tools/loadgen.py`） | `8084`（控制口） |

每个 node 容器的进程编排见 `deploy/docker/node-entrypoint.sh`：先做三项
启动前准备（tc 限速自检 → 内核参数调优 → FD 预检），再起 HAProxy 并等
unix socket 就绪，最后起 rl-limiter；任一进程退出即整体退出（对齐生产上
systemd `Restart=always` 的语义），由 compose 拉起。

准备工作必须在 HAProxy **之前**做完：内核参数改晚了对已经建好的监听套接字
不生效（backlog 在 `listen()` 那一刻就定死），FD 不够则 HAProxy 根本起不来。
`docker compose -f docker-compose-demo.yml logs node1` 里能看到逐项结果，
其中"调不动"的那几项是容器里改不了、需要在**宿主机**上设的，日志会直接
给出命令。详见 [08-内核参数调优.md](08-内核参数调优.md)。

聚合限速语义：tc 限的是**该监听端口全部连接的总速率**——与连接数、
单连接快慢无关，存量长连接持续受控；改限额是 `tc class change`，
**不 reload、存量连接立刻按新限额跑**。

## 快速开始

```bash
make demo-up      # = docker compose -f docker-compose-demo.yml up -d --build
                  # 首次构建要装 Ubuntu 包 + Python 依赖，
                  # 比官方 haproxy 镜像慢，属正常
make demo-logs    # 看各入口吞吐被压在 40 Mbps 的过程 + 各节点日志
```

> **`--build` 不能省**。node 镜像里打包了入口脚本与 rl-limiter 代码，
> 拉了新版本后若只 `up -d`，跑的仍是旧镜像——而 compose 里的挂载点/
> 环境变量已经是新的，两边对不上。典型症状是 node 容器无限重启；新版
> 入口脚本会直接给出"镜像与 compose 版本不匹配，请 docker compose up
> -d --build"的提示。

确认每台节点确实是"HAProxy + 同机 rl-limiter"（下面的 `docker compose`
均指 `docker compose -f docker-compose-demo.yml`）：

```bash
docker compose exec node1 ps -eo comm | sort -u | grep -E 'haproxy|rl-limiter'
docker compose exec node1 ls -l /run/haproxy/admin.sock   # 采样用的本机 socket
docker compose exec node1 haproxy -v                      # Ubuntu 24.04 自带 2.8.x
docker compose exec node1 tc class show dev eth0          # tc 速率类（1:1f90 = 8080）
```

`loadgen` 日志每 2 秒一行（`rate_mbps` 与限额同口径）：

```
吞吐观测 concurrency=24 total_mbps=115.8 ...（表格按入口逐行：并发/速率/请求/错误/状态）
  http://node1:8080/  …  ≈40 Mbps  正常   ← 每台节点被内核 tc 精确压在 40M
```

宿主机也可以直接体验：`curl -o /dev/null http://localhost:8080/`。

## Web 控制台（只读实时观测）

**每台节点各有一个控制台**（同机部署形态）：

| 节点 | 控制台 |
| ---- | ---- |
| node1 | http://localhost:8090 |
| node2 | http://localhost:8092 |
| node3 | http://localhost:8093 |

演示里容器内绑 `0.0.0.0`（`RL_CONSOLE_BIND`）、宿主机只映射到
`127.0.0.1`；生产默认就是 `127.0.0.1`。

- **各 frontend 的带宽视图**：每个受管 frontend 一张卡片（实时速率 /
  10s 均值 + 限额虚线，1s 粒度）——曲线被压在限额线下即限速生效的直接
  证据；持续高于限额时卡片出现**超限**徽标；
- **配置视图（只读）**：卡片显示该 frontend 的监听端点、模式与限额
  （来自 haproxy.cfg + YAML 的解析结果）。**改配置 = 进容器改那两份
  文件**（见下节），页面不提供写操作；
- **实例视图**：整台 HAProxy 的连接/带宽/数据包曲线，本机 stats socket
  端点与采样健康（失联标红）；
- **运行日志**：最近 1000 条结构化日志增量流式展示，按级别过滤。

对应的 HTTP API（页面之外也可脚本化调用，全部只读）：

```bash
curl http://localhost:8090/api/overview            # 最新状态 + 配置视图
curl http://localhost:8090/api/history             # 最近 10 分钟逐拍快照
curl -N http://localhost:8090/api/stream           # SSE 实时流（每拍一帧）
curl http://localhost:8090/api/logs?after=0        # 日志增量拉取
curl http://localhost:8090/metrics                 # Prometheus 抓取端点
```

监控数据另有**分钟粒度的本地落盘**（`RL_METRICS_LOG`，compose 里已
开）——历史回查不依赖控制台内存：

```bash
docker compose exec node1 tail -3 /var/log/rl-limiter/metrics.jsonl
# {"ts":1785723600,"kind":"frontend","samples":60,"rate_avg":4998321.0,
#  "rate_max":5312400.0,...,"name":"fe_main","mean10_max":5003210.0,
#  "quota":5000000.0,"over_s":0,"degraded_s":0}
```

安全提示：控制台**读接口无鉴权**（暴露全量监控数据与运行日志）。默认
只绑 `127.0.0.1`（`RL_CONSOLE_BIND`）；要放到内网必须显式设置并配合
防火墙/安全组限制来源，绝不可暴露公网。绑非回环地址时服务会打一条
warning 留痕。写接口（页面/API 改限额）需 `RL_API_TOKEN` 令牌，demo
的节点已预设 `demo-token`——生产上请生成随机值。

## 调节并发（模拟不同强度的客户端群）

```bash
curl http://localhost:8084/status                              # 当前并发与实时吞吐
curl -X PUT http://localhost:8084/concurrency -d '{"concurrency": 32}'
curl -X PUT http://localhost:8084/concurrency -d '2'           # 裸数字也接受
curl -X PUT http://localhost:8084/concurrency -d '0'           # 暂停打流
```

无需重启、秒级生效。初始并发用环境变量：
`LOADGEN_CONCURRENCY=32 docker compose -f docker-compose-demo.yml up -d`。

要点观察：并发调大/调小，每台节点的**总**吞吐都精确贴着自己的 40M
限额——tc 限总量，各连接动态分享额度（新连接加入时存量连接立即让出
份额，无需重连）。

## 长连接大文件下载场景（存量连接持续受控的实证）

默认每个入口挂 1 条长连接大文件下载 worker（同一条 TCP 连接上循环
请求 `/big`，单个响应 512 MiB，与 4 条短请求 worker 分享 40M 限额时
约 8 Mbps、要下 ~9 分钟）——这是触发旧方案生产事故的工作负载，现在
专门作为常驻测试场景：

```bash
curl -X PUT http://localhost:8084/big -d '{"per_target": 2}'   # 每入口 2 条
curl -X PUT http://localhost:8084/big -d '0'                   # 关闭该场景
curl http://localhost:8084/status    # big_downloads[]：每条在途下载的
                                     # 进度/下载时长/连接年龄
```

要点观察：

- loadgen 表格多了"大文件"列；短请求 + 长下载合计仍精确贴 40M，
  长下载拿到公平份额（≈ 限额 ÷ 连接数）——tc 对存量长连接**持续**
  限速，不存在"建连定格"；
- 每个下载完成时打
  `大文件下载完成（同一长连接继续下一个） ... conn_age_s=532`——
  连接年龄数百秒，是真正的长连接；
- 做下面的"改限额 SOP"时**在途下载不断线**：tc 换挡即时生效，曲线
  直接压到新限额继续下——这正是 tc 相对旧 bwlim 方案的核心优势
  （bwlim 改限额要 reload + hard-stop 断连重连）。

## 调整某台节点的限额（完整 SOP 演示）

配置就是文件。入口脚本把两份模板复制成了**容器内各自的**普通文件
（`/etc/haproxy/haproxy.cfg` 与 `/etc/rl-limiter/config.yaml`），进容器
改副本即可演示热生效——改 node1 不会牵动 node2/node3，与生产上"每台
机器一份自己的配置"完全一致。

**改限额（不 reload，存量连接立刻跟上）**：

```bash
# 把 node1 的 fe_main 从 40 Mbps 降到 20 Mbps
docker compose exec node1 sed -i 's/fe_main: 40/fe_main: 20/' \
  /etc/rl-limiter/config.yaml

# ≤5s 内观察它落到数据面：
docker compose logs --tail=5 node1 | grep 配置变化
docker compose exec node1 tc class show dev eth0
#   → class htb 1:1f90 ... rate 20Mbit    （1f90 = 8080 的十六进制）
```

日志里会出现：

```
INFO 检测到 haproxy.cfg / 限额配置变化，已提交监控循环热生效 version=... frontends=1
INFO tc 限速已按配置就地调整（rate-change，未重建队列树） ...
```

**解除限速**：把限额改成 `0`（显式不限速）同样 ≤5s 生效——该端口的
tc 类被撤掉、吞吐立刻放开；全部改 0 时整棵队列树被拆掉（`tc qdisc
show dev eth0` 恢复默认）。

观察：loadgen 表格里 node1 行**立刻**降到 ≈20 Mbps（在途大文件下载
不断线、直接换挡），其余两台不受影响。**反向调大同理**——吞吐立即回升，
无需 reload、更无需重启。

**改监听端口/后端（HAProxy 的固有流程：改 cfg + reload）**：

```bash
docker compose exec node1 sed -i 's/bind :8080/bind :8085/' /etc/haproxy/haproxy.cfg
docker compose exec node1 sh -c 'kill -USR2 $(cat /run/haproxy/master.pid)'
# ≤5s 内 rl-limiter 跟上：监控清单与 tc 分类自动切到 8085
docker compose logs --tail=5 node1 | grep 配置变化
```

（quotas 的键是**段名**不是端口，所以改端口不用动 YAML。）

### 热更新边界

| 改什么 | 改哪个文件 | 生效方式 |
| ---- | ---- | ---- |
| 限额 | YAML 的 `quotas` 段 | **热生效**（≤5s，tc class change，不 reload） |
| 监听端口 / 后端服务器 / 模式 | haproxy.cfg | reload haproxy 后 ≤5s，rl-limiter 自动跟上 |
| log_level / tick_interval_s / haproxy 接线 | YAML 对应字段 | 重启 rl-limiter（检测到变化会记 warning 提醒） |

校验是整份执行的：YAML 写坏（quotas 写 0、接线两个都给）整份被拒绝，
保留当前配置继续监控（fail-static）并打日志；cfg 正在被编辑的瞬间读到
半份同理，下一轮重试。

环境变量（compose 中注入）：

- `RL_CONSOLE_PORT` / `RL_CONSOLE_BIND`：控制台端口与监听地址；
- `RL_TC_IFACE`：限速网卡（容器里不设，自动探测出 `eth0`）；
- `RL_NIC`：实例监控视图的数据包统计要采样的网卡。不设即跟随限速网卡。
  注意容器网络下这份计数是**该容器网络命名空间**的整机口径，含 compose
  内部的东西向流量。口径说明见 [05-监控视图.md](05-监控视图.md)；
- `HAPROXY_MAXCONN` / `HAPROXY_MAXPIPES`：见下。

三台节点还各设了 `ulimits.nofile: 524288`。这不是随手写的余量：HAProxy
需要的文件描述符数 = `maxconn × 2 + maxpipes × 2 + 34`（管道那两项是
splice 用的，本项目开着 splice）。容器默认的 nofile 常常只有 1024/4096，
给不够 HAProxy 会**拒绝启动**并打

```
[ALERT] Cannot raise FD limit to 400034, limit is 4096.
```

**演示跑的是降级量级**：`haproxy-base.cfg` 的默认值是 `maxconn 1000000`
（默认值不该成为限制），对应 400 万 fd，而宿主机 `fs.nr_open` 默认只有
1048576、**容器里改不动**，开发机上直接起不来。所以 compose 用
`HAPROXY_MAXCONN` / `HAPROXY_MAXPIPES` 把它降到 10 万（= 400034 fd）——
入口脚本会就地改写复制出来的 cfg，**模板本身不动**，仍是那份可以直接抄
去生产的配置。生产机器请先按 [08-内核参数调优.md](08-内核参数调优.md)
把 `fs.nr_open` 抬上去，然后不设这两个变量、直接用模板默认值。

改 `maxconn` 时务必同步改 `ulimits`；入口脚本会在启动前预检并直接报出
该改成多少。调优基线的完整说明见
[03-限速服务运行指南.md §5.1](03-限速服务运行指南.md) 与
`deploy/docker/haproxy-base.cfg` 本身的注释（每项都带实测数字）。

## 验证"监控与限速互不牵连"

同机形态下故障域完全按节点隔离，可以直接演示：

```bash
# 只杀 node1 里的 rl-limiter：限速照旧（tc 规则在内核里，loadgen 表格里
# node1 仍是 40M），只是该节点的控制台/告警停了；容器整体退出后被
# compose 拉起。
docker compose exec node1 pkill -f rl-limiter
docker compose logs --tail=20 node1

# 整台 node3 停掉：node1/node2 的监控与限速完全不受影响。
docker compose stop node3
curl -s http://localhost:8090/api/overview | head -c 300
docker compose start node3
```

## 调整模拟后端的响应大小

```bash
WEB_MIN_BYTES=1048576 WEB_MAX_BYTES=8388608 \
  docker compose -f docker-compose-demo.yml up -d web
```

## 收场

```bash
make demo-down    # = docker compose -f docker-compose-demo.yml down
```

（没有数据库，也就没有要清理的数据卷；容器内改过的配置副本随容器一起
消失，下次 `up` 回到模板初始值。）
