# Docker Compose 一键演示环境（MySQL 配置 + 真实 HAProxy 整形）

本演示把完整链路装进一个 `docker compose`：**配置存 MySQL**（服务启动时
加载、运行期轮询热更新），**真实 HAProxy 2.8** 执行 `bwlim-out` 整形，
后端挂一个**每次请求返回随机大小响应**的模拟 web 服务，再用一个
**可在线调节并发数的压测服务**模拟并发带宽，肉眼观察 rl-limiter 把吞吐
压回配额的全过程。

## 拓扑

```
                 ┌────────────┐   轮询配置（每 RL_MYSQL_POLL_S 秒）
                 │   mysql    │◄────────────────┐
                 │ (配置四表)  │                 │
                 └────────────┘          ┌──────┴──────┐
                                         │ rl-limiter  │
                 show stat / set map     │  (enforce)  │
                 ┌──────────────────────►└─────────────┘
                 │ :9999 (admin socket)
   HTTP 并发     ▼
┌─────────┐   ┌────────────┐   ┌────────────┐
│ loadgen │──►│  haproxy   │──►│    web     │
│ :8081   │   │ :8080 整形  │   │ 随机大小响应 │
└─────────┘   └────────────┘   └────────────┘
```

| 服务 | 说明 | 宿主机端口 |
| ---- | ---- | ---- |
| `mysql` | 配置库（表结构与种子数据：`deploy/mysql/init.sql`） | `127.0.0.1:3306` |
| `haproxy` | 真实 HAProxy 2.8，`fe_env_a` 整形入口（配置：`deploy/docker/haproxy.cfg`） | `8080` |
| `web` | 模拟业务后端，每次请求返回 256 KiB～2 MiB 随机大小响应（`tools/random_web.py`） | 无 |
| `rl-limiter` | 限速服务，配置来自 MySQL（`RL_MYSQL_*` 环境变量接线），enforce 模式 | 无 |
| `loadgen` | 压测服务，N 个并发 worker 持续打流，并发数可在线调节（`tools/loadgen.py`） | `8081`（控制口） |

演示种子配额：环境 `env-a` = **80 Mbps**（10 MB/s 下行）。

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

## 调节并发（模拟不同强度的客户端群）

```bash
curl http://localhost:8081/status                              # 当前并发与实时吞吐
curl -X PUT http://localhost:8081/concurrency -d '{"concurrency": 32}'
curl -X PUT http://localhost:8081/concurrency -d '2'           # 裸数字也接受
curl -X PUT http://localhost:8081/concurrency -d '0'           # 暂停打流
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
| `service_config.mode` | dry-run / enforce | **热生效** |
| `service_config` 其余列 | node_id / log_level / tick_interval_s | 重启生效 |
| `haproxy_nodes` | 节点接线（地址/端口/map 路径/超时） | 重启生效（检测到变化会记 warning 提醒） |

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
