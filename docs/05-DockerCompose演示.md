# 05 DockerCompose 演示

一键起一套完整的双网卡闭环环境：三台节点（每台 = Ubuntu 24.04 +
任意业务进程示例 random_web + 同机 rl-limiter）、可调并发压测服务。
仅供体验与验证，不是生产形态（生产见 [04 部署指南](04-部署指南.md)）。

## 1. 拓扑

```
loadgen ══ frontnet 10.211.1.0/24（客户端侧）══► node1/2/3 :8080（fe_main）
                                                   │ 入向整形在 eth-front（按网段自动定位）
                                                   ▼
            backnet 10.211.2.0/24（后端侧）═════► web :9000（随机大小响应）
```

- compose 的双网络模拟生产的双物理网卡：frontnet = 客户端侧（闭环整形
  在这侧），backnet = 后端侧（绝不被误伤）；
- 节点容器内网卡名不稳定，入口脚本按 `RL_FRONT_SUBNET=10.211.1.` 定位
  客户端侧网卡并导出 `RL_TC_IN_IFACE`；
- 每个节点：`RL_TC_IFACE=-`（出向不整形）、`RL_TC_IN_MODE=ifb`、
  闭环 `dynamic_ingress.enabled=true`（demo 调节周期 5s）、控制台
  写 API 令牌 `demo-token`。

## 2. 起停

```bash
make demo-up        # = docker compose -f docker-compose-demo.yml up -d --build
make demo-logs      # 跟 node1 + loadgen 日志
make demo-down      # 收场（没有数据库，无需清理数据卷）
```

宿主机入口：

| 地址 | 是什么 |
| ---- | ---- |
| `localhost:8080` / `8082` / `8083` | 三台节点的 fe_main（可直接 curl 体验限速） |
| `127.0.0.1:8090` / `8092` / `8093` | 三台节点各自的控制台 |
| `localhost:8084` | loadgen 控制端口（GET /status、PUT /concurrency、PUT /big） |
| `127.0.0.1:8095` | **总控制台 rl-fleet**（多机总量控制，玩法见 [10 总控制台](10-总控制台.md) §6） |

前提：Linux 主机（内核有 ifb/HTB）。Docker Desktop（Mac/Win）缺 ifb，
节点会**启动即失败**并在日志里写明三条出路——这是刻意行为，见
[09 排障指南](09-排障指南.md)。

## 3. 闭环演示 SOP（看着调节视图压流量）

1. **起环境**，打开 node1 控制台 `http://127.0.0.1:8090`，页头填入
   API 令牌 `demo-token`；
2. **设实例出向目标**：实例页"实例总限速"填 40（Mbps）保存——或在
   "配置"页改，两处等价；此时 demo 默认不登记任何端口限额，总闸上限
   退化为目标值；
3. **加压**：
   ```bash
   curl -X PUT localhost:8084/concurrency -d '{"concurrency": 900}'
   ```
4. **看"调节视图（闭环动态限速）"卡**（设了目标且闭环在跑才出现）：
   - 出向总带宽先冲高（无限制时三节点合计可达数百 Mbps）；
   - 第一轮收紧：总闸**落位**到实测流入×1.1（日志里有"入向总闸落位"
     一行）；
   - 之后小步下调，约 1 分钟出向压到目标附近，稳态在死区
     [0.85, 1.0]×目标内，总闸只做零星微调；
5. **退潮**：把并发改回 60，看总闸按 +5%/次爬升、出向回落后控制器
   逐步松闸直到全开；
6. 顺手看"tc 实况（调试）"面板：期望配置行会标出
   `实例出向目标=40Mbps→入向总闸 X.XMbps（动态）`，与 IFB 上
   `tc class show` 的原始输出对照。

端口级闭环同理：给 fe_main 登记 `quotas`（出向目标）+
`request_limits.mbps`（入向上限）即可，配置页一站式改。

## 4. loadgen 与 web 的调节

```bash
curl localhost:8084/status                                    # 聚合状态（分片进程数、每目标速率）
curl -X PUT localhost:8084/concurrency -d '{"concurrency": N}'  # 并发（上限 10240，自动多进程分片）
curl -X PUT localhost:8084/big -d '{"per_target": 2}'           # 每目标 N 条长连接大文件下载
```

环境变量（compose 里改）：`LOADGEN_CONCURRENCY`（默认 5000）、
`WEB_MIN_BYTES`/`WEB_MAX_BYTES`（响应大小区间）、`WEB_BIG_BYTES`
（/big 大文件大小）、`WEB_PROCS`/`LOADGEN_PROCS`（0=自动按核数分片）。

## 5. demo 与生产的差异（读数时心里有数）

- 三节点 + loadgen + web 挤在一台宿主机上：CPU/带宽互相挤占，绝对
  吞吐数字没有意义，**看收敛行为**（调节视图的曲线形状）；
  生产独占机器删掉这些覆盖；
- 节点/loadgen/web 的 sysctls（somaxconn、syn_backlog、tw_reuse、端口
  区间等）在 compose 里按 netns 声明——5000 并发下不给会出现 accept
  队列溢出、TIME_WAIT 耗尽等假瓶颈（曾实测 75s 溢出 3.1 万次）。节点的
  端口区间下界保持 32768：tc 按源端口分类，下探会撞监听端口；
- 改容器内两份配置文件做实验：`docker exec` 进去直接改，轮询 5s 内热
  生效；容器重建回到模板初始值。

## 6. 构建问题

公司内网/Mac 上构建拉不到 pypi（DNS 被挡）：

```bash
docker compose -f docker-compose-demo.yml build \
  --build-arg PIP_INDEX_URL=https://pypi.tuna.tsinghua.edu.cn/simple
```
