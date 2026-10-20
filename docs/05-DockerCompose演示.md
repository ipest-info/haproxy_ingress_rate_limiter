# 05 DockerCompose 演示

一键起一套完整的 **LVS-NAT 主备**环境：一对 keepalived 主备 director
（每台 = IPVS NAT + 同机 rl-limiter）、模拟业务后端、可调并发压测、
总控。仅供体验与验证，不是生产形态（生产见 [04 部署指南](04-部署指南.md)
与 [11 LVS 主备与 NAT 形态](11-LVS主备与NAT形态.md)）。

## 1. 拓扑

```
loadgen ══ frontnet 10.211.1.0/24 ══► VIP 10.211.1.100:8080（VRRP 漂移）
                                        │ node1(主,.11) / node2(备,.12)
                                        │ IPVS NAT 转发（主备都装表）
                                        ▼
           backnet 10.211.2.0/24 ◄══ web 10.211.2.20:9000
           （web 默认网关 = 后端侧 VIP 10.211.2.100，随 VRRP 一起漂）
```

- keepalived 同时管两个 VIP：客户端入口 + 后端回程网关——这是
  LVS-NAT HA 的教科书形态；virtual_server 主备**都**下发，备机的
  rl-limiter 清单与 tc 规则与主机完全一致（主备对称，docs/11）；
- 每台 director：出向 HTB 在客户端侧网卡直接限速（NAT 双向都过）；
  限额键 `vs_10.211.1.100_8080`；
- **宿主机前提**：`modprobe ip_vs ip_vs_rr`（容器里 modprobe 不了，
  入口自检失败会直接说）。

## 2. 起停

```bash
make demo-up        # = docker compose -f docker-compose-demo.yml up -d --build
make demo-logs      # 跟 node1 + loadgen 日志
make demo-down      # 收场（没有数据库，无需清理数据卷）
```

宿主机入口：

| 地址 | 是什么 |
| ---- | ---- |
| `127.0.0.1:8090` / `8092` | 主/备 director 的控制台 |
| `localhost:8084` | loadgen 控制端口（GET /status、PUT /concurrency、PUT /big） |
| `127.0.0.1:8095` | **总控制台 rl-fleet**（多机总量控制，玩法见 [10 总控制台](10-总控制台.md) §5） |

前提：Linux 主机（内核有 ifb/HTB）。Docker Desktop（Mac/Win）缺 ifb，
节点会**启动即失败**并在日志里写明三条出路——这是刻意行为，见
[09 排障指南](09-排障指南.md)。

## 3. 限速与主备漂移演示 SOP

**限速（出向 HTB 直接整形，NAT 主路径）**：

1. 起环境，打开 node1 控制台 `http://127.0.0.1:8090`，页头填 API 令牌
   `demo-token`；
2. 加压：`curl -X PUT localhost:8084/concurrency -d '{"concurrency": 400}'`
   ——loadgen 打的是 VIP，流量全部落在当前主机（node1）；
3. "虚拟服务"页给 `vs_10.211.1.100_8080` 设限额 40（Mbps）保存——或
   在"配置"页改，两处等价。几秒内热生效：带宽曲线被压到 40 的限额
   参考线下，"tc 实况"面板能看到 HTB 类与 `tc class change` 的痕迹；
4. 改限额走 `tc class change` 原位变更：存量连接立刻按新限额跑。

**主备漂移（本分支的核心卖点，docs/11）**：

1. 看 node2 控制台 `http://127.0.0.1:8092`：清单与 node1 完全一致
   （keepalived 主备都装 IPVS 表）、带宽全为 0（备机没有 VIP 流量）、
   tc 规则同样就位——这就是"备机空转"；
2. `docker compose -f docker-compose-demo.yml stop node1`：
   VRRP 把两个 VIP 漂到 node2，loadgen 流量无缝切过去；node2 的曲线
   立刻出现流量，**限额照样生效（规则早已就位，零窗口期）**；
3. 总控 `http://127.0.0.1:8095`：node1 变离线（配额冻结记账），需求
   与配额转移到 node2；
4. `docker compose -f docker-compose-demo.yml start node1`：node1 以
   更高 priority 抢回 MASTER，流量漂回来。

入向闭环（可选能力）在 LVS demo 里默认不开（`RL_TC_IN_IFACE=-`，
NAT 下出向直接限速已是完整路径）；要体验闭环请看 HAProxy 分支
（claude/dual-nic-ingress-limit）的 demo。

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
- 演示量级：`HAPROXY_MAXCONN` 压到 10 万（宿主机 `fs.nr_open` 默认值
  起不来 100 万的模板默认）、`HAPROXY_NBTHREAD=2`（防三实例线程超配）；
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
