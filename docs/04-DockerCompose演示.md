# Docker Compose 一键演示环境（MySQL 配置 + 三台 Ubuntu 24.04 节点）

本演示把完整链路装进一个 `docker compose`。**每个 node 容器 = 生产上的
一台 ECS**：基于 **Ubuntu 24.04 LTS**，容器内同时跑发行版自带的
**HAProxy 2.8**（TCP L4 + shared bwlim 聚合限速）与**同机的
rl-limiter**——rl-limiter 通过容器内的 unix socket
（`/run/haproxy/admin.sock`）只采本机那台 HAProxy，stats socket
不占任何网络端口、也无需跨容器可达。配置存 MySQL（启动加载、轮询热
更新），各节点独立轮询、把配置渲染进本机 haproxy.cfg 的受管区块并
reload，同时对照限额做持续超限告警，各自带一个 Web 控制台。后端挂一个**每次请求返回随机大小响应**的模拟 web 服务，
再用一个**可在线调节并发数的压测服务**打散到全部入口。

> 为什么不用 haproxy 官方镜像：官方镜像只有 haproxy 一个进程，而本项目
> 的部署形态是"rl-limiter 与 HAProxy 同机"。用 Ubuntu 基础镜像装两个
> 组件，容器形态才和生产的单台 ECS 一致。镜像构建时会校验 haproxy
> 版本 ≥ 2.8（shared bwlim 的下限），不满足直接构建失败。

## 拓扑

```
                 ┌────────────┐   各节点独立轮询配置（每 RL_MYSQL_POLL_S 秒）
                 │   mysql    │◄──────────┬───────────┬───────────┐
                 │ (配置三表)  │           │           │           │
                 └────────────┘           │           │           │
   HTTP 并发                               │           │           │
┌─────────┐   ┌──────────────────────┐ ┌──┴───────────┴──┐ ┌──────┴────────┐
│ loadgen │──►│ node1  (Ubuntu 24.04)│ │ node2           │ │ node3         │
│ :8084   │──►│  haproxy fe_main     │ │  haproxy fe_main │ │ haproxy fe_main │
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
| `node1` / `node2` / `node3` | **Ubuntu 24.04 + HAProxy 2.8 + 同机 rl-limiter**（根目录 `Dockerfile`，全仓库统一镜像）。挂进去的 `deploy/docker/haproxy-base.cfg` 只是 global/defaults 骨架，监听端口与后端由各自的 rl-limiter 从配置库渲染进受管区块；`RL_NODE_NAME` 指定它管哪个实例 | 代理 `8080`/`8082`/`8083`；控制台 `127.0.0.1:8090`/`8092`/`8093` |
| `web` | 模拟业务后端，每次请求返回 256 KiB～2 MiB 随机大小响应；`/big` 为大文件下载端点（默认 512 MiB，`WEB_BIG_BYTES` 可调）（`tools/random_web.py`） | 无 |
| `loadgen` | 压测服务，打散到全部入口，并发数可在线调节；默认每入口另挂 1 条**长连接大文件下载**（`tools/loadgen.py`） | `8084`（控制口） |

每个 node 容器的进程编排见 `deploy/docker/node-entrypoint.sh`：先做三项
启动前准备（tc 限速自检 → 内核参数调优 → FD 预检），再起 HAProxy 并等
unix socket 就绪，最后起 rl-limiter；任一进程退出即整体退出（对齐生产上
systemd `Restart=always` 的语义），由 compose 拉起。

准备工作必须在 HAProxy **之前**做完：内核参数改晚了对已经建好的监听套接字
不生效（backlog 在 `listen()` 那一刻就定死），FD 不够则 HAProxy 根本起不来。
`docker compose logs node1` 里能看到逐项结果，其中"调不动"的那几项是容器
里改不了、需要在**宿主机**上设的，日志会直接给出命令。详见
[08-内核参数调优.md](08-内核参数调优.md)。

演示种子：hap-1 / hap-2 / hap-3 各有一个 `fe_main`，监听 8080、限额
**40 Mbps**、后端指向 `web:9000`。这些行由各自机器上的 rl-limiter 渲染成
haproxy.cfg 的受管区块——**下发给 tc 的类速率与库里的 quota_mbps 同源**。

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

> **`--build` 不能省**。node 镜像里打包了入口脚本与 rl-limiter 代码，
> 拉了新版本后若只 `docker compose up -d`，跑的仍是旧镜像 —— 而 compose
> 里的挂载点/环境变量已经是新的，两边对不上。典型症状是 node 容器无限
> 重启并刷：
>
> ```
> [ALERT] config : Cannot open configuration file/directory
>                  /usr/local/etc/haproxy/haproxy.cfg : No such file or directory
> ```
>
> 这是旧镜像在找已经不再挂载的旧路径。新版入口脚本会直接给出"镜像与
> compose 版本不匹配，请 docker compose up -d --build"的提示。

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

演示里
容器内绑 `0.0.0.0`（`RL_CONSOLE_BIND`）、宿主机只映射到 `127.0.0.1`；
生产默认就是 `127.0.0.1`。

- **各 frontend 的带宽视图**：每个受管 frontend 一张图（实时速率 /
  10s 均值 + 限额虚线，1s 粒度）——曲线被压在限额线下即限速生效的直接
  证据；持续高于限额时卡片出现**超限**徽标；
- **标准化配置界面**（本次演示的重点）：卡片上点「编辑」即可改监听地址
  端口、模式（tcp/http）、限额、maxconn、balance、各项超时，以及后端
  服务器列表（增删改地址/端口/权重/健康检查）；页面底部可**新增**
  frontend，卡片上可**删除**。保存即写库，随后自动渲染进 haproxy.cfg
  并 reload；
- **下发状态横幅**：绿色 = 已生效并附最近一次下发时间；红色 = 下发失败
  并附 `haproxy -c` 的原始原因（此时数据面仍按调整前的配置运行）；
- **实例信息**：本机 stats socket 端点（`/run/haproxy/admin.sock`）与
  采样健康（失联标红）；
- **运行日志**：最近 1000 条结构化日志增量流式展示，按级别过滤。

对应的 HTTP API（页面之外也可脚本化调用）：

```bash
curl http://localhost:8090/api/overview            # 最新状态 + 配置视图
curl http://localhost:8090/api/history             # 最近 10 分钟逐拍快照
curl -N http://localhost:8090/api/stream           # SSE 实时流（每拍一帧）
curl http://localhost:8090/api/logs?after=0        # 日志增量拉取
# 新建或整体更新一个 frontend（含它的后端服务器列表）
curl -X PUT http://localhost:8090/api/frontends/fe_main -d '{
  "name": "fe_main", "bind_port": 8080, "quota_mbps": 20,
  "mode": "tcp", "maxconn": 2000, "balance": "roundrobin",
  "servers": [{"name": "web1", "address": "web", "port": 9000}]
}'
curl -X DELETE http://localhost:8090/api/frontends/fe_api
```

> 每台节点的控制台端口不同（8090 / 8092 / 8093），各自只读写属于自己
> 那台 HAProxy 的配置行——在 8090 上改不会影响 hap-2/hap-3。

安全提示：控制台**无鉴权且带写接口**（改监听端口、改限额、改后端、
删 frontend）。
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

演示环境**已启用配置自动下发**（`RL_APPLY_HAPROXY_CFG`），所以配置调整
只有一步——改库（或在控制台上点），剩下的由 node1 里的 rl-limiter 自动
完成。下面以改限额为例，改监听端口/后端服务器完全同理：

```bash
# 唯一一步：改库（也可以直接在控制台的 frontend 卡片上点「编辑」）
docker compose exec mysql mysql -url -prl_pass rl_limiter \
  -e "UPDATE haproxy_frontends SET quota_mbps = 20
      WHERE instance = 'hap-1' AND name = 'fe_main';"

# 几秒内（一个 RL_MYSQL_POLL_S 轮询周期）观察它自动落到数据面：
docker compose logs --tail=5 node1 | grep 已把受管配置
docker compose exec node1 grep -o 'limit [0-9]*' /etc/haproxy/haproxy.cfg
#   → limit 2500000   （20 Mbps ÷ 8）
docker compose exec node1 sed -n '/BEGIN rl-limiter/,/END rl-limiter/p' \
  /etc/haproxy/haproxy.cfg      # 看完整的受管区块
```

日志里会出现一行：

```
WARNING 已把受管配置写入本机 haproxy.cfg 并 reload（数据面已按新配置运行）
        cfg=/etc/haproxy/haproxy.cfg frontends=fe_main::8080:2500000bytes/s:1srv
```

控制台顶部同时会显示绿色的"配置自动下发已启用"横幅与最近一次下发时间；
应用失败时是红色横幅 + 具体原因（那时数据面仍按**调整前**的限额运行）。

**每台节点各有一份自己的 cfg**：`deploy/docker/haproxy-base.cfg` 只是
只读挂进去的**模板**，入口脚本会把它复制成容器内可写的
`/etc/haproxy/haproxy.cfg`。所以改 hap-1 的限额不会牵动 hap-2——与生产
上"每台机器一份自己的 cfg"完全一致。（也正因为要原地改写，cfg 不能是
只读的单文件 bind mount：那种挂载无法被 rename 覆盖，而原子写必须靠
rename。）

**配置漂移自动修复**——手动把 cfg 改回去，看它被拉回来：

```bash
docker compose exec node1 sed -i 's/limit 2500000/limit 5000000/' /etc/haproxy/haproxy.cfg
docker compose exec node1 kill -USR2 "$(docker compose exec -T node1 cat /run/haproxy/master.pid)"
sleep 35   # 等一个兜底 reconcile 周期（RL_APPLY_PERIOD_S，默认 30s）
docker compose exec node1 grep -o 'limit [0-9]*' /etc/haproxy/haproxy.cfg
#   → limit 2500000   ← 已被拉回配置库登记值
```

观察：reload 后新连接立即按 20M；存量连接最迟 15s（`hard-stop-after`）
断开重连进入新限额；loadgen 表格里 node1 行降到 ≈20 Mbps，其余两台
不受影响。**反向调大同理**——存量长连接也会在宽限期后进入新限额（这
正是 shared bwlim 相对旧 per-stream 方案的核心修复：调高限额吞吐必然
回升，无需重启）。

### 配置表与热更新边界

| 表 | 内容 | 改表后 |
| ---- | ---- | ---- |
| `haproxy_frontends` | 监听地址端口、限额、模式、maxconn、balance、超时 | **热生效**：监控基准立刻跟随，且（演示已启用配置下发）自动渲染进本机 cfg 并 reload，数据面即时生效 |
| `haproxy_servers` | 后端服务器 | **热生效**，同上 |
| `service_config` | log_level / tick_interval_s | 重启生效 |
| `haproxy_instances` | stats socket 接线（`socket_path` 或 `host`/`port`、超时） | 重启生效（采样客户端启动时定型；检测到变化会记 warning） |

校验规则与 YAML 完全一致（同一套管线），且**在全量配置上执行**：任意
一处写坏（比如让两个环境抢同一台节点），三台节点的 rl-limiter 会一起
保留当前配置继续监控（fail-static）并打日志。

变更检测是**本机口径**的：改 hap-3 的配置不会让 node1 产生一次无谓的
热应用——`docker compose logs node1` 里只会看到与本机相关的
`检测到数据库配置变化，已提交主循环热生效`。

环境变量（`docker-compose.yml` 中注入）：
- `RL_NODE_NAME`：**本机节点名，同机模式开关**（演示里三台分别是
  `hap-1`/`hap-2`/`hap-3`）。拼错会让该节点启动失败并列出已登记节点名；
- `RL_MYSQL_HOST`（设置即启用数据库模式）、`RL_MYSQL_PORT`、
  `RL_MYSQL_USER`、`RL_MYSQL_PASSWORD`、`RL_MYSQL_DB`、`RL_MYSQL_POLL_S`；
  不设 `RL_MYSQL_HOST` 则回落到 `-c` 指定的本地 YAML；
- `RL_CONSOLE_PORT` / `RL_CONSOLE_BIND`：控制台端口与监听地址；
- `RL_APPLY_HAPROXY_CFG` / `RL_APPLY_RELOAD_CMD`：**配置自动下发**。
  演示里 reload 命令是 `kill -USR2 $(cat /run/haproxy/master.pid)`
  （容器里没有 systemd）；生产上默认 `systemctl reload haproxy`。
  reload 命令只从本机环境变量读、绝不从配置库读——否则拿到库写权限
  就等于在每台 HAProxy 上远程执行任意命令；
- `RL_NIC`：实例监控视图的数据包统计要采样的网卡。不设即自动选默认路由
  的出口网卡（容器里通常是 `eth0`）。注意容器网络下这份计数是**该容器
  网络命名空间**的整机口径，含 compose 内部的东西向流量。口径说明见
  [05-监控视图.md](05-监控视图.md)。

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
去生产的骨架。生产机器请先按 [08-内核参数调优.md](08-内核参数调优.md)
把 `fs.nr_open` 抬上去，然后不设这两个变量、直接用模板默认值。

改 `maxconn` 时务必同步改 `ulimits`；入口脚本会在启动前预检并直接报出
该改成多少。调优基线的完整说明见
[03-限速服务运行指南.md §5.1](03-限速服务运行指南.md) 与
`deploy/docker/haproxy-base.cfg` 本身的注释（每项都带实测数字）。

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
