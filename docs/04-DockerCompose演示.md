# Docker Compose 一键演示环境（MySQL 配置 + 双 HAProxy TCP L4 整形）

本演示把完整链路装进一个 `docker compose`：**配置存 MySQL**（服务启动时
加载、运行期轮询热更新），**两台真实 HAProxy 2.8（TCP L4 负载均衡形态）**
执行 `bwlim-out` 整形，后端挂一个**每次请求返回随机大小响应**的模拟 web
服务，再用一个**可在线调节并发数的压测服务**同时打散到全部入口，肉眼观察
rl-limiter 跨节点聚合限速、逐节点灰度切模式的全过程。

## 拓扑

```
                 ┌────────────┐   轮询配置（每 RL_MYSQL_POLL_S 秒）
                 │   mysql    │◄────────────────┐
                 │ (配置四表)  │                 │
                 └────────────┘          ┌──────┴──────┐   :8090 Web 控制台
                                         │ rl-limiter  │◄── 实时曲线/调参/日志
                 show stat / set map     └──┬───────┬──┘
                 （:9999 admin socket）      │       │
   HTTP 并发        ┌───────────────────────┘       └───────────┐
┌─────────┐         ▼                                           ▼
│ loadgen │──┬─►┌──────────────────────┐      ┌──────────────────────┐
│ :8084   │  │  │ haproxy1 (hap-1, L4) │      │ haproxy2 (hap-2, L4) │
└─────────┘  │  │ :8080 fe_env_a       │      │ :8082→8080 fe_env_a  │
             └─►│ :8081 fe_env_b       │      │ :8083→8081 fe_env_b  │
                └──────────┬───────────┘      └──────────┬───────────┘
                           └──────────────┬──────────────┘
                                          ▼
                                   web:9000（随机大小响应）
```

| 服务 | 说明 | 宿主机端口 |
| ---- | ---- | ---- |
| `mysql` | 配置库（表结构与种子数据：`deploy/mysql/init.sql`） | `127.0.0.1:3306` |
| `haproxy1` / `haproxy2` | 真实 HAProxy 2.8，**TCP L4** 整形入口（配置：`deploy/docker/haproxy.cfg`，两台共用） | `8080/8081`、`8082/8083` |
| `web` | 模拟业务后端，每次请求返回 256 KiB～2 MiB 随机大小响应（`tools/random_web.py`） | 无 |
| `rl-limiter` | 限速服务，配置来自 MySQL；内置 Web 控制台 | `8090`（控制台） |
| `loadgen` | 压测服务，随机打散到 4 个入口，并发数可在线调节（`tools/loadgen.py`） | `8084`（控制口） |

演示种子：**env-a = 80 Mbps**、**env-b = 40 Mbps**，都横跨两台 HAProxy
（同一环境两台的流量**全局聚合**后统一限速）。

TCP L4 语义提示：每连接的限速值在**建连时定格**，runtime map 更新只影响
新连接——AIMD 靠连接自然轮转收敛（演示的 loadgen 用短连接所以秒级生效），
长连接为主的业务需配合 tc 兜底（`deploy/tc/backstop.sh`）。

## 快速开始

```bash
docker compose up -d --build        # 或 make demo-up
docker compose logs -f loadgen      # 看吞吐被压回配额的过程
```

`loadgen` 日志每 2 秒一行（`rate_mbps` 与配额同口径）：

```
吞吐观测 concurrency=8 rate_mbps=87.3 ... ← 冷启动冲到弹性上限（110%）附近
吞吐观测 concurrency=8 rate_mbps=79.8 ... ← AIMD 收紧后稳定在 80 Mbps 配额
```

对应地，`docker compose logs -f rl-limiter` 能看到采样/决策/写 map 的
全过程（收紧时有"整形值已写入节点 map"日志）。宿主机也可以直接体验：
`curl -o /dev/null http://localhost:8080/`。

## Web 控制台（实时观测 + 在线调参）

浏览器打开 **http://localhost:8090**（rl-limiter 内置，`RL_CONSOLE_PORT`
启用，无需额外服务）：

- **实时曲线**：每个环境一张图，实时速率 / 10s 均值（计费口径）/ 整形值
  三条线与配额虚线画在同一条 1s 粒度的时间轴上——曲线被压在配额线下
  即限速生效的直接证据；悬浮显示十字线与各序列数值；窗口可切 1/5/10 分钟；
- **生效证据计数**：AIMD 状态徽标（常态/收紧中/恢复中）、收紧次数、
  超配额秒数、利用率（均值/配额）、并发连接数、节点失联标记；
- **节点面板**：每台 HAProxy 的地址、采样健康（失联标红）与**按节点的
  模式控制**——「继承全局 / dry-run / enforce」三选一。生产灰度的标准
  动作：全局默认留 dry-run，逐台把节点切到 enforce；
- **环境与挂载点管理**：环境卡上直接增删挂载点（节点 × frontend）、
  新建环境（环境 ID + 配额 + 初始挂载点）、删除环境；挂载点迁移 =
  原环境移除 + 目标环境添加（同一挂载点同时属于两个环境会被拒绝）；
- **在线调参**：配额（Mbps）、全局默认模式切换、AIMD 参数覆盖
  （JSON）。所有修改**写入 MySQL**（配置唯一事实源），经既有轮询链路
  在一个轮询周期内热生效——页面显示的参数永远与库一致；
- **运行日志**：最近 1000 条结构化日志增量流式展示，按级别过滤。

对应的 HTTP API（页面之外也可脚本化调用）：

```bash
curl http://localhost:8090/api/overview            # 最新状态 + 配置视图
curl http://localhost:8090/api/history             # 最近 10 分钟逐拍快照
curl -N http://localhost:8090/api/stream           # SSE 实时流（每拍一帧）
curl http://localhost:8090/api/logs?after=0        # 日志增量拉取
curl -X PUT http://localhost:8090/api/envs/env-a/quota -d '{"quota_bps": 40000000}'
curl -X PUT http://localhost:8090/api/mode -d '{"mode": "dry-run"}'          # 全局默认
curl -X PUT http://localhost:8090/api/nodes/hap-2/mode -d '{"mode": "enforce"}'  # 按节点覆盖
curl -X PUT http://localhost:8090/api/nodes/hap-2/mode -d '{"mode": null}'   # 恢复继承全局
curl -X PUT http://localhost:8090/api/envs/env-a/params -d '{"params": {"md_factor": 0.8}}'
curl -X PUT http://localhost:8090/api/envs/env-a/targets \
     -d '{"targets": [{"node":"hap-1","frontend":"fe_env_a"},{"node":"hap-2","frontend":"fe_env_a"}]}'
curl -X POST http://localhost:8090/api/envs \
     -d '{"env_id":"env-c","quota_bps":100000000,"targets":[{"node":"hap-1","frontend":"fe_env_c"}]}'
curl -X DELETE http://localhost:8090/api/envs/env-c
```

安全提示：控制台无鉴权，定位与 HAProxy stats socket 相同——只允许绑定
内网/受防火墙保护的端口，不要暴露公网。

## 调节并发（模拟不同强度的客户端群）

```bash
curl http://localhost:8084/status                              # 当前并发与实时吞吐
curl -X PUT http://localhost:8084/concurrency -d '{"concurrency": 32}'
curl -X PUT http://localhost:8084/concurrency -d '2'           # 裸数字也接受
curl -X PUT http://localhost:8084/concurrency -d '0'           # 暂停打流
```

无需重启、秒级生效。初始并发用环境变量：
`LOADGEN_CONCURRENCY=32 docker compose up -d`。

要点观察：并发从 8 调到 32 后，聚合吞吐仍被压在配额附近——每流限速 =
聚合整形值 / 当前连接数，由 HAProxy 在响应侧动态均分。

## 改配置（MySQL 即配置中心，改表热生效）

```bash
docker compose exec mysql mysql -url -prl_pass rl_limiter
```

```sql
-- 配额 80 Mbps → 40 Mbps（等一个轮询周期≈3s，loadgen 吞吐随即腰斩）
UPDATE envs SET quota_bps = 40000000 WHERE env_id = 'env-a';

-- enforce ↔ dry-run 热切换。注意：dry-run 只是停止更新 map，enforce
-- 期间最后写入的整形值仍留在 HAProxy 里继续限速（fail-static 设计，
-- 服务绝不主动放开限速）；切回 enforce 时会触发一次全量重写（resync）。
UPDATE service_config SET mode = 'dry-run' WHERE id = 1;

-- 按环境覆盖快环参数（字段见 rl_limiter/model.py GovParams）
UPDATE envs SET params_json = '{"md_factor": 0.8, "recover_after_s": 10}'
 WHERE env_id = 'env-a';
```

rl-limiter 侧日志会出现：
`检测到数据库配置变化，已提交主循环热生效 version=... mode=... envs=...`。

### 配置表与热更新边界

| 表 | 内容 | 改表后 |
| ---- | ---- | ---- |
| `envs` / `env_targets` | 环境配额、参数覆盖、挂载点 | **热生效**（一个轮询周期内） |
| `service_config.mode` | 全局默认 dry-run / enforce | **热生效** |
| `haproxy_nodes.mode` | 按节点模式覆盖（NULL=继承全局） | **热生效**（逐节点灰度就改它） |
| `service_config` 其余列 | node_id / log_level / tick_interval_s | 重启生效 |
| `haproxy_nodes` 其余列 | 节点接线（地址/端口/map 路径/超时） | 重启生效（检测到变化会记 warning；引用新增节点的环境会被拒绝热应用） |

校验规则与 YAML 完全一致（同一套管线）：配置写错时 rl-limiter 保留当前
配置继续限速（fail-static）并打日志，绝不因坏配置放开限速。

MySQL 接线环境变量（`docker-compose.yml` 中注入）：`RL_MYSQL_HOST`（设置
即启用数据库模式）、`RL_MYSQL_PORT`、`RL_MYSQL_USER`、`RL_MYSQL_PASSWORD`、
`RL_MYSQL_DB`、`RL_MYSQL_POLL_S`（轮询周期，秒）。不设 `RL_MYSQL_HOST`
则回落到 `-c` 指定的本地 YAML（原部署方式不受影响）。

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
