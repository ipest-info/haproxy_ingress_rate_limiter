# Docker Compose 一键演示环境（MySQL 配置 + 三台 Ubuntu 24.04 节点）

本演示把完整链路装进一个 `docker compose`。**每个 node 容器 = 生产上的
一台 ECS**：基于 **Ubuntu 24.04 LTS**，容器内同时跑发行版自带的
**HAProxy 2.8**（TCP L4 + shared bwlim 聚合限速）与**同机的
rl-limiter**——rl-limiter 通过容器内的 unix socket
（`/run/haproxy/admin.sock`）只采本机那台 HAProxy，stats socket
不占任何网络端口、也无需跨容器可达。配置存 MySQL（启动加载、轮询热
更新），各节点独立轮询、对照库中登记限额做持续超限告警，各自带一个
Web 控制台。后端挂一个**每次请求返回随机大小响应**的模拟 web 服务，
再用一个**可在线调节并发数的压测服务**打散到全部入口。

> 为什么不用 haproxy 官方镜像：官方镜像只有 haproxy 一个进程，而本项目
> 的部署形态是"rl-limiter 与 HAProxy 同机"。用 Ubuntu 基础镜像装两个
> 组件，容器形态才和生产的单台 ECS 一致。镜像构建时会校验 haproxy
> 版本 ≥ 2.8（shared bwlim 的下限），不满足直接构建失败。

## 拓扑

```
                 ┌────────────┐   各节点独立轮询配置（每 RL_MYSQL_POLL_S 秒）
                 │   mysql    │◄──────────┬───────────┬───────────┐
                 │ (配置四表)  │           │           │           │
                 └────────────┘           │           │           │
   HTTP 并发                               │           │           │
┌─────────┐   ┌──────────────────────┐ ┌──┴───────────┴──┐ ┌──────┴────────┐
│ loadgen │──►│ node1  (Ubuntu 24.04)│ │ node2           │ │ node3         │
│ :8084   │──►│  haproxy fe_env_a    │ │  haproxy fe_env_a│ │ haproxy fe_env_b│
└─────────┘──►│   shared bwlim 40M   │ │   bwlim 40M      │ │  bwlim 40M     │
              │   :8080              │ │   :8082          │ │  :8083         │
              │      │ unix socket   │ │                  │ │                │
              │      ▼ (只读采样)     │ │                  │ │                │
              │  rl-limiter hap-1    │ │ rl-limiter hap-2 │ │ rl-limiter hap-3│
              │   控制台 :8090        │ │  控制台 :8092     │ │  控制台 :8093   │
              └──────────┬───────────┘ └────────┬─────────┘ └───────┬────────┘
                         └───────────────────┬──┴───────────────────┘
                                             ▼
                                      web:9000（随机大小响应）
```

| 服务 | 说明 | 宿主机端口 |
| ---- | ---- | ---- |
| `mysql` | 配置库（表结构与种子数据：`deploy/mysql/init.sql`） | `127.0.0.1:3306` |
| `node1` / `node2` / `node3` | **Ubuntu 24.04 + HAProxy 2.8 + 同机 rl-limiter**（`deploy/docker/Dockerfile.node`）。HAProxy 按环境角色分用 `deploy/docker/haproxy-env-a/b.cfg`，limit 各 5,000,000 bytes/s = 40 Mbps；rl-limiter 用 `RL_NODE_NAME` 锁定本机节点 | 代理 `8080`/`8082`/`8083`；控制台 `127.0.0.1:8090`/`8092`/`8093` |
| `web` | 模拟业务后端，每次请求返回 256 KiB～2 MiB 随机大小响应；`/big` 为大文件下载端点（默认 512 MiB，`WEB_BIG_BYTES` 可调）（`tools/random_web.py`） | 无 |
| `loadgen` | 压测服务，打散到全部入口，并发数可在线调节；默认每入口另挂 1 条**长连接大文件下载**（`tools/loadgen.py`） | `8084`（控制口） |

每个 node 容器的进程编排见 `deploy/docker/node-entrypoint.sh`：先起
HAProxy 并等 unix socket 就绪，再起 rl-limiter；任一进程退出即整体退出
（对齐生产上 systemd `Restart=always` 的语义），由 compose 拉起。

演示种子：hap-1 / hap-2 / hap-3 登记限额各 **40 Mbps**（与各自 cfg 的
limit 一致）；环境是纯分组——env-a = hap-1 + hap-2（聚合视图显示合计
80 Mbps），env-b = hap-3。

聚合限速语义：shared bwlim 限的是**本 frontend 全部连接的总速率**——
与连接数、单连接快慢无关，存量长连接持续受控；限额调整（改 cfg +
reload）后存量连接最迟在 `hard-stop-after`（演示 15s）宽限期结束时
断开重连、进入新限额。

## 快速开始

```bash
docker compose up -d --build        # 或 make demo-up
                                    # 首次构建要装 Ubuntu 包 + Python 依赖，
                                    # 比官方 haproxy 镜像慢，属正常
docker compose logs -f loadgen      # 看各入口吞吐被压在 40 Mbps 的过程
docker compose logs -f node1        # 看 node1 里 haproxy + rl-limiter 的日志
```

确认每台节点确实是"HAProxy + 同机 rl-limiter"：

```bash
docker compose exec node1 ps -eo comm | sort -u | grep -E 'haproxy|rl-limiter'
docker compose exec node1 ls -l /run/haproxy/admin.sock   # 采样用的本机 socket
docker compose exec node1 haproxy -v                      # Ubuntu 24.04 自带 2.8.x
```

`loadgen` 日志每 2 秒一行（`rate_mbps` 与限额同口径）：

```
吞吐观测 concurrency=24 total_mbps=115.8 ...（表格按入口逐行：并发/速率/请求/错误/状态）
  http://node1:8080/  …  ≈40 Mbps  正常   ← 每台节点被 shared bwlim 精确压在 40M
```

宿主机也可以直接体验：`curl -o /dev/null http://localhost:8080/`。

## Web 控制台（实时观测 + 配置管理）

**每台节点各有一个控制台**（同机部署形态）：

| 节点 | 控制台 |
| ---- | ---- |
| `hap-1`（node1） | http://localhost:8090 |
| `hap-2`（node2） | http://localhost:8092 |
| `hap-3`（node3） | http://localhost:8093 |

页面顶部会显示**同机部署模式**横幅，标明本控制台属于哪个节点。演示里
容器内绑 `0.0.0.0`（`RL_CONSOLE_BIND`）、宿主机只映射到 `127.0.0.1`；
生产默认就是 `127.0.0.1`。

- **节点带宽视图**：本机 HAProxy 一张图（实时速率 / 10s 均值 + 登记
  限额虚线，1s 粒度）——曲线被压在限额线下即该节点限速生效的直接
  证据；实测持续高于限额时节点卡出现**超限**徽标（说明 HAProxy 配置
  与库不一致）；登记限额就在节点卡上编辑（写库热生效，页面提示需
  同步 HAProxy limit）；
- **环境聚合视图**：同机形态下**只含本机成员**（横幅已说明这不是环境
  全量带宽）；挂载点管理照常可用——写的是全量配置库，改动会被其它
  节点的 rl-limiter 一并读到；
- **生效证据计数**：超限秒数、利用率（均值/限额）、并发连接数、节点
  失联标记；
- **节点面板**：本机 HAProxy 的 stats socket 端点
  （`/run/haproxy/admin.sock (unix)`）、所属环境、登记限额、采样健康
  （失联标红）；
- **环境与挂载点管理**：环境卡上直接增删挂载点（节点 × frontend）、
  新建环境（环境 ID + 初始挂载点）、删除环境；挂载点迁移 = 原环境
  移除 + 目标环境添加（同一挂载点同时属于两个环境会被拒绝）；
- **运行日志**：最近 1000 条结构化日志增量流式展示，按级别过滤
  （持续超限告警在此可见）。

对应的 HTTP API（页面之外也可脚本化调用）：

```bash
curl http://localhost:8090/api/overview            # 最新状态 + 配置视图
curl http://localhost:8090/api/history             # 最近 10 分钟逐拍快照
curl -N http://localhost:8090/api/stream           # SSE 实时流（每拍一帧）
curl http://localhost:8090/api/logs?after=0        # 日志增量拉取
curl -X PUT http://localhost:8090/api/nodes/hap-1/quota -d '{"quota_bps": 20000000}'  # 登记限额
curl -X PUT http://localhost:8090/api/envs/env-a/targets \
     -d '{"targets": [{"node":"hap-1","frontend":"fe_env_a"},{"node":"hap-2","frontend":"fe_env_a"}]}'
curl -X POST http://localhost:8090/api/envs \
     -d '{"env_id":"env-c","targets":[{"node":"hap-1","frontend":"fe_env_c"}]}'
curl -X DELETE http://localhost:8090/api/envs/env-c
```

> 上面的 `hap-1` / `env-a` 只是示例；每台节点的控制台端口不同（8090 /
> 8092 / 8093），但写接口写的都是同一个配置库，从哪个控制台改都一样。

安全提示：控制台**无鉴权且带写接口**（改限额、改挂载点、删环境）。
默认只绑 `127.0.0.1`（`RL_CONSOLE_BIND`）；要放到内网必须显式设置并
配合防火墙/安全组限制来源，绝不可暴露公网。绑非回环地址时服务会打一条
warning 留痕。

## 调节并发（模拟不同强度的客户端群）

```bash
curl http://localhost:8084/status                              # 当前并发与实时吞吐
curl -X PUT http://localhost:8084/concurrency -d '{"concurrency": 32}'
curl -X PUT http://localhost:8084/concurrency -d '2'           # 裸数字也接受
curl -X PUT http://localhost:8084/concurrency -d '0'           # 暂停打流
```

无需重启、秒级生效。初始并发用环境变量：
`LOADGEN_CONCURRENCY=32 docker compose up -d`。

要点观察：并发调大/调小，每台节点的**总**吞吐都精确贴着自己的 40M
限额——shared bwlim 限总量，各连接动态分享额度（新连接加入时存量
连接立即让出份额，无需重连）。

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
  长下载拿到公平份额（≈ 限额 ÷ 连接数）——shared bwlim 对存量长连接
  **持续**限速，不存在"建连定格"；
- 每个下载完成时打
  `大文件下载完成（同一长连接继续下一个） ... conn_age_s=532`——
  连接年龄数百秒，是真正的长连接（实测 512 MiB / 531.9s / 8.1 Mbps）；
- 做上面的"限额调整 SOP"时，在途下载在 hard-stop 宽限期末被断开，
  loadgen 打 `大文件下载被中断（多半是限额调整 reload 的 hard-stop
  断连…）` 并自动重连重下，随后按**新限额**继续——调低即时压住、
  调高吞吐回升，长连接不再像旧方案那样把旧限速带到天荒地老。

## 调整某台节点的限额（完整 SOP 演示）

限额调整是两步（与生产一致，见 docs/03 §3）：

```bash
# 1) 改库（监控基准，热生效；只做这一步会触发持续超限告警）
docker compose exec mysql mysql -url -prl_pass rl_limiter \
  -e "UPDATE haproxy_nodes SET quota_bps = 20000000 WHERE name = 'hap-1';"

# 2) 改数据面：原地编辑 deploy/docker/haproxy-env-a.cfg，把
#    「limit 5000000」改为「limit 2500000」（bytes/s = bit/s ÷ 8），然后
#    给容器内的 haproxy master 发 SIGUSR2 平滑 reload：
docker compose exec node1 pkill -USR2 -x haproxy
```

> 注意：`haproxy-env-a.cfg` 被 node1 与 node2 共用，改一次会同时影响
> 两台——本例只 reload 了 node1，因此只有 hap-1 的数据面变了，hap-2
> 仍按旧限额跑（要一起改就把两个容器都 reload）。

> 容器单文件挂载的坑：必须**原地修改** cfg（保持 inode 不变，编辑器/
> `python -c` 的 r+ 写法都行）；`sed -i` 会替换文件 inode，容器内看到的
> 还是旧内容。生产环境（配置管理 + systemctl reload）无此问题。

观察：reload 后新连接立即按 20M；存量连接最迟 15s（`hard-stop-after`）
断开重连进入新限额；loadgen 表格里 node1 行降到 ≈20 Mbps，其余两台
不受影响。**反向调大同理**——存量长连接也会在宽限期后进入新限额（这
正是 shared bwlim 相对旧 per-stream 方案的核心修复：调高限额吞吐必然
回升，无需重启）。

### 配置表与热更新边界

| 表 | 内容 | 改表后 |
| ---- | ---- | ---- |
| `haproxy_nodes.quota_bps` | 节点登记限额（监控基准；真实限速在该节点 cfg 的 limit） | **热生效**（一个轮询周期内）；与 cfg 不一致会触发持续超限告警 |
| `envs` / `env_targets` | 环境分组、挂载点归属 | **热生效** |
| `service_config` | log_level / tick_interval_s | 重启生效 |
| `haproxy_nodes` 其余列 | 节点接线（`socket_path` 或 `host`/`port`、超时） | 重启生效（检测到变化会记 warning；引用新增节点的环境会被拒绝热应用） |

校验规则与 YAML 完全一致（同一套管线），且**在全量配置上执行**：任意
一处写坏（比如让两个环境抢同一台节点），三台节点的 rl-limiter 会一起
保留当前配置继续监控（fail-static）并打日志。

同机模式下变更检测是**本机口径**的：改 hap-3 的限额不会让 node1 产生
一次无谓的热应用——`docker compose logs node1` 里只会看到与本机相关的
`检测到数据库配置变化，已提交主循环热生效`。

环境变量（`docker-compose.yml` 中注入）：
- `RL_NODE_NAME`：**本机节点名，同机模式开关**（演示里三台分别是
  `hap-1`/`hap-2`/`hap-3`）。拼错会让该节点启动失败并列出已登记节点名；
- `RL_MYSQL_HOST`（设置即启用数据库模式）、`RL_MYSQL_PORT`、
  `RL_MYSQL_USER`、`RL_MYSQL_PASSWORD`、`RL_MYSQL_DB`、`RL_MYSQL_POLL_S`；
  不设 `RL_MYSQL_HOST` 则回落到 `-c` 指定的本地 YAML；
- `RL_CONSOLE_PORT` / `RL_CONSOLE_BIND`：控制台端口与监听地址。

## 验证"监控与限速互不牵连"

同机形态下故障域完全按节点隔离，可以直接演示：

```bash
# 只杀 node1 里的 rl-limiter：限速照旧（loadgen 表格里 node1 仍是 40M），
# 只是该节点的控制台/告警停了；容器整体退出后被 compose 拉起。
docker compose exec node1 pkill -f rl-limiter
docker compose logs --tail=20 node1

# 整台 node3 停掉：node1/node2 的监控与限速完全不受影响。
docker compose stop node3
curl -s http://localhost:8090/api/overview | head -c 300
docker compose start node3
```

## 调整模拟后端的响应大小

```bash
WEB_MIN_BYTES=1048576 WEB_MAX_BYTES=8388608 docker compose up -d web
```

## 收场

```bash
docker compose down -v    # 或 make demo-down；-v 一并清掉 MySQL 数据卷
```

注意：`down` 不带 `-v` 时 MySQL 数据卷保留，改过的配置（而非 init.sql
种子）在下次 `up` 时继续生效——init.sql 只在数据卷首次初始化时执行。
