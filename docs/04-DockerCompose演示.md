# Docker Compose 一键演示环境（MySQL 配置 + 三台 HAProxy TCP L4 按节点限速）

本演示把完整链路装进一个 `docker compose`：**配置存 MySQL**（服务启动时
加载、运行期轮询热更新），**三台真实 HAProxy 2.8（TCP L4 负载均衡形态）**
各自执行 `bwlim-out` 整形——v2.1 起**每台节点自己设置带宽限制、独立
AIMD 调节**（节点间无自动调配），环境是节点分组、控制台聚合查看成员
带宽之和。后端挂一个**每次请求返回随机大小响应**的模拟 web 服务，再用
一个**可在线调节并发数的压测服务**打散到全部入口。

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
│ loadgen │──┬─►┌─────────────────┐ ┌─────────────────┐ ┌─────────────────┐
│ :8084   │  ├─►│ haproxy1(hap-1) │ │ haproxy2(hap-2) │ │ haproxy3(hap-3) │
└─────────┘  └─►│ :8080 fe_env_a  │ │ :8082 fe_env_a  │ │ :8083 fe_env_b  │
                │  40Mbps 独立限速 │ │  40Mbps 独立限速 │ │  40Mbps 独立限速 │
                └────────┬────────┘ └────────┬────────┘ └────────┬────────┘
                         └───────────────────┼───────────────────┘
                                             ▼
                                      web:9000（随机大小响应）
```

| 服务 | 说明 | 宿主机端口 |
| ---- | ---- | ---- |
| `mysql` | 配置库（表结构与种子数据：`deploy/mysql/init.sql`） | `127.0.0.1:3306` |
| `haproxy1` / `haproxy2` / `haproxy3` | 真实 HAProxy 2.8，**TCP L4** 整形入口（按环境角色分用 `deploy/docker/haproxy-env-a/b.cfg`） | `8080`、`8082`、`8083` |
| `web` | 模拟业务后端，每次请求返回 256 KiB～2 MiB 随机大小响应（`tools/random_web.py`） | 无 |
| `rl-limiter` | 限速服务，配置来自 MySQL；内置 Web 控制台 | `8090`（控制台） |
| `loadgen` | 压测服务，随机打散到 4 个入口，并发数可在线调节（`tools/loadgen.py`） | `8084`（控制口） |

演示种子（v2.1：**带宽限制按节点设置**，每台 HAProxy 独立调节，节点间
无自动调配）：hap-1 / hap-2 / hap-3 各 **40 Mbps**；环境是纯分组——
env-a = hap-1 + hap-2（聚合视图显示合计 80 Mbps），env-b = hap-3。

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
吞吐观测 concurrency=24 total_mbps=115.8 ...（表格按入口逐行：并发/速率/请求/错误/状态）
  http://haproxy1:8080/  …  ≈40 Mbps  正常   ← 每台节点各自贴自己的 40M 限制
```

对应地，`docker compose logs -f rl-limiter` 能看到采样/决策/写 map 的
全过程（收紧时有"整形值已写入节点 map"日志）。宿主机也可以直接体验：
`curl -o /dev/null http://localhost:8080/`。

## Web 控制台（实时观测 + 在线调参）

浏览器打开 **http://localhost:8090**（rl-limiter 内置，`RL_CONSOLE_PORT`
启用，无需额外服务）：

- **节点带宽视图**：每台 HAProxy 一张图（实时速率 / 10s 均值 / 整形值
  三线 + 带宽限制虚线，1s 粒度）——曲线被压在限制线下即该节点限速
  生效的直接证据；带宽限制与 AIMD 参数就在节点卡上编辑；
- **环境聚合视图**：每个环境一张只读聚合图（成员节点各序列**求和**），
  以及挂载点管理；环境不再有自己的配额与调节；
- **生效证据计数**：AIMD 状态徽标（常态/收紧中/恢复中）、收紧次数、
  超配额秒数、利用率（均值/配额）、并发连接数、节点失联标记；
- **节点面板**：每台 HAProxy 的地址、采样健康（失联标红）与**按节点的
  模式控制**——「继承全局 / dry-run / enforce」三选一。生产灰度的标准
  动作：全局默认留 dry-run，逐台把节点切到 enforce；
- **环境与挂载点管理**：环境卡上直接增删挂载点（节点 × frontend）、
  新建环境（环境 ID + 配额 + 初始挂载点）、删除环境；挂载点迁移 =
  原环境移除 + 目标环境添加（同一挂载点同时属于两个环境会被拒绝）；
- **在线调参**：节点带宽限制（Mbps）、全局默认模式、按节点模式、按
  节点 AIMD 参数。所有修改**写入 MySQL**（配置唯一事实源），经既有
  轮询链路在一个轮询周期内热生效——页面显示的参数永远与库一致；
- **运行日志**：最近 1000 条结构化日志增量流式展示，按级别过滤。

对应的 HTTP API（页面之外也可脚本化调用）：

```bash
curl http://localhost:8090/api/overview            # 最新状态 + 配置视图
curl http://localhost:8090/api/history             # 最近 10 分钟逐拍快照
curl -N http://localhost:8090/api/stream           # SSE 实时流（每拍一帧）
curl http://localhost:8090/api/logs?after=0        # 日志增量拉取
curl -X PUT http://localhost:8090/api/nodes/hap-1/quota -d '{"quota_bps": 20000000}'  # 节点带宽限制
curl -X PUT http://localhost:8090/api/mode -d '{"mode": "dry-run"}'          # 全局默认
curl -X PUT http://localhost:8090/api/nodes/hap-2/mode -d '{"mode": "enforce"}'  # 按节点覆盖
curl -X PUT http://localhost:8090/api/nodes/hap-2/mode -d '{"mode": null}'   # 恢复继承全局
curl -X PUT http://localhost:8090/api/nodes/hap-1/params -d '{"params": {"md_factor": 0.8}}'
curl -X PUT http://localhost:8090/api/envs/env-a/targets \
     -d '{"targets": [{"node":"hap-1","frontend":"fe_env_a"},{"node":"hap-2","frontend":"fe_env_a"}]}'
curl -X POST http://localhost:8090/api/envs \
     -d '{"env_id":"env-c","targets":[{"node":"hap-1","frontend":"fe_env_c"}]}'
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

要点观察：并发调大后，每台节点的吞吐仍各自贴着自己的带宽限制——
每连接限速 = 节点整形值 / 该节点当前连接数，由 HAProxy 动态均分。

## 改配置（MySQL 即配置中心，改表热生效）

```bash
docker compose exec mysql mysql -url -prl_pass rl_limiter
```

```sql
-- 某台节点 40 Mbps → 20 Mbps（等一个轮询周期≈3s，该节点吞吐随即腰斩）
UPDATE haproxy_nodes SET quota_bps = 20000000 WHERE name = 'hap-1';

-- enforce ↔ dry-run 热切换。注意：dry-run 只是停止更新 map，enforce
-- 期间最后写入的整形值仍留在 HAProxy 里继续限速（fail-static 设计，
-- 服务绝不主动放开限速）；切回 enforce 时会触发一次全量重写（resync）。
UPDATE service_config SET mode = 'dry-run' WHERE id = 1;

-- 按节点覆盖快环参数（字段见 rl_limiter/model.py GovParams）
UPDATE haproxy_nodes SET params_json = '{"md_factor": 0.8, "recover_after_s": 10}'
 WHERE name = 'hap-1';
```

rl-limiter 侧日志会出现：
`检测到数据库配置变化，已提交主循环热生效 version=... mode=... envs=...`。

### 配置表与热更新边界

| 表 | 内容 | 改表后 |
| ---- | ---- | ---- |
| `haproxy_nodes.quota_bps` | 节点带宽限制（v2.1 核心调节入口） | **热生效**（一个轮询周期内） |
| `haproxy_nodes.params_json` | 按节点 AIMD 参数覆盖 | **热生效** |
| `envs` / `env_targets` | 环境分组、挂载点归属 | **热生效** |
| `service_config.mode` | 全局默认 dry-run / enforce | **热生效** |
| `haproxy_nodes.mode` | 按节点模式覆盖（NULL=继承全局） | **热生效**（逐节点灰度就改它） |
| `service_config` 其余列 | log_level / tick_interval_s | 重启生效 |
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
