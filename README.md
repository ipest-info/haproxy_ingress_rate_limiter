# haproxy_ingress_rate_limiter

带宽账密计费模式下的入口限速与监控系统。**一个 rl-limiter 实例管一台与
它同机的 HAProxy**：在 Web 界面上配置监听端口、限额与后端服务器，保存即
写库，随后由本机 rl-limiter 渲染进 haproxy.cfg 的受管区块并 reload——
**改完立刻生效**（实测一次下发 ~74 ms）。

限速由**内核 tc（HTB）**执行：按源端口把每个 frontend 的出向流量分到
自己的速率类里，该端口全部连接（含存量长连接）的总速率被硬性压在限额内，
与连接数、单连接快慢无关。改限额走 `tc class change`，**连 reload 都不需要，
存量连接立刻跟上**。rl-limiter 同时每秒经**本机 unix stats socket** 采样
各 frontend 的下行带宽做持续超限告警。

> 之前用的是 HAProxy 的 shared bwlim。换掉是因为实测发现：**只要挂着
> bwlim 滤镜，HAProxy 就会完全关闭内核 splice（零拷贝转发）**，同吞吐下
> HAProxy 的 CPU 要多花一倍（0.59 → 0.33 CPU 秒/GB）。原委、行为差异与
> **尚未验证的部分**见 [docs/06-tc限速方案.md](docs/06-tc限速方案.md)。
>
> 检查限速有没有真的生效：`python3 tools/tc_check.py doctor`（体检环境）、
> `plan`（干跑看命令）、`verify`（核对网卡实况与配置是否一致）。

> **同机部署的首要理由就是"改完立刻生效"**：只有在同一台机器上，服务
> 才有可能直接改本机配置并 reload；跨机的集中服务做不到（要么开 SSH，
> 要么另装 agent）。附带收益：stats socket 不占任何网络端口、无需对内网
> 开放；一台机器的故障不外溢。
>
> **受管区块**：rl-limiter 只重写 cfg 里 `# >>> BEGIN rl-limiter managed`
> 与 `# <<< END rl-limiter managed` 之间的内容，标记之外（global、TLS、
> ACL、手写的其它 backend）一个字节都不碰。

## 文档

| 文档 | 说明 |
| ---- | ---- |
| [docs/00-需求说明.md](docs/00-需求说明.md) | 原始需求（名词定义、架构图、核心诉求） |
| [docs/01-方案设计.md](docs/01-方案设计.md) | 方案设计（聚合限速、限额调整流程、监控与超限告警、MySQL 配置模型、容错） |
| [docs/02-建议与讨论点.md](docs/02-建议与讨论点.md) | 需求改进建议与决策清单 |
| [docs/03-限速服务运行指南.md](docs/03-限速服务运行指南.md) | 安装、配置来源（MySQL/YAML）、限额调整 SOP、HAProxy 侧接线 |
| [docs/04-DockerCompose演示.md](docs/04-DockerCompose演示.md) | docker compose 一键演示（MySQL + 三台 Ubuntu 24.04 节点 + Web 控制台 + 可调并发压测） |
| [docs/05-监控视图.md](docs/05-监控视图.md) | 监控视图：每条曲线的数据来源与口径 |
| [docs/06-tc限速方案.md](docs/06-tc限速方案.md) | **限速为什么从 HAProxy bwlim 换成内核 tc**：实测依据、映射方式、行为差异，以及尚未验证的部分 |
| [docs/07-监控数据回查.md](docs/07-监控数据回查.md) | **90 天回查怎么存怎么查**：分级保留、聚合语义、真实 MariaDB 上的容量与耗时实测 |

## 系统组成

```
rl_limiter/       # Python 3.11 + asyncio 服务（与 HAProxy 同机）
  model.py        #   共享领域类型（单位约定、FrontendConfig/ServerEntry、接线）
  haproxy.py      #   HAProxy runtime API 客户端（unix / TCP stats socket，只读采样）
  window.py       #   滑动窗口 + EWMA
  collector.py    #   每秒采样，按 frontend 产出用量；fail-static 与降级
  dbconfig.py     #   MySQL 配置源（启动加载 + 轮询热更新 + 控制台写回，RL_MYSQL_* 接线）
  enforcer.py     #   配置下发：渲染受管区块 → haproxy -c 校验 → 原子替换 → reload
  tcshaper.py     #   限速下发：把限额落到本机网卡的 tc（HTB），按源端口分类
  config.py       #   配置解析与校验（YAML 与数据库共用同一管线）
  metricstore.py  #   监控数据分级落库与回查（1min×7天 / 5min×90天）
  webconsole.py   #   内置 Web 控制台（带宽曲线 + 端口/限额/后端服务器管理）
  loop.py         #   1s 监控主循环（采集 → 超限判定 → 发布）
tools/            # fake_haproxy.py（联调假节点，支持 unix / TCP）
                  # random_web.py（随机大小响应的模拟后端）、loadgen.py（可调并发压测）
                  # tc_check.py（限速检查：plan 干跑 / doctor 体检 / verify 核对）
deploy/           # systemd（同机形态）、haproxy 骨架配置示例、tc 兜底脚本、
                  # YAML 示例配置、mysql/init.sql（配置库建表+种子）
  docker/         #   node-entrypoint.sh（节点入口：haproxy + 同机 rl-limiter）、
                  #   haproxy-base.cfg（compose 用的 global/defaults 骨架）
docker-compose.yml # 一键演示：MySQL + 三台 Ubuntu 24.04 节点 + 模拟后端 + 压测
```

## 核心思路一句话

配置库是唯一数据源：一个 frontend = 一个监听端口 + 一个 tc 速率类 +
一组后端服务器。rl-limiter 把监听端口与后端渲染进本机 haproxy.cfg 的受管
区块并 reload，把限额下发到本机网卡的 tc 上，同一份限额同时作为超限告警
基准——同源，因此不存在"库改了、数据面忘了改"的漂移。tc 把该端口全部连接
的**总**下行速率硬限在限额内，存量长连接持续受控；上游流量靠 TCP 背压
自然收敛。
监控每秒采 `bytes_out`（前提 `option contstats`）算 10 秒滑动均值；
rl-limiter 宕机不影响已下发的限速。

## 快速开始

```bash
make install    # pip install -e ".[test]"
make test       # 全量单元测试
# 本地最小演示（假 HAProxy unix socket + standalone YAML）见 docs/03

make demo-up    # docker compose 一键演示：MySQL 配置 + 三台 Ubuntu 24.04
                # 节点（每台 = HAProxy TCP L4 转发 + 内核 tc 限速 + 同机 rl-limiter）
                # + 随机大小响应后端 + 可调并发压测
                # 每台节点各一个控制台：http://localhost:8090 / :8092 / :8093
make demo-logs  # 观察各节点监控与 loadgen 分入口吞吐表格
make demo-down  # 收场（含 MySQL 数据卷）
```

**配置来源二选一**：设置 `RL_MYSQL_HOST` 等环境变量时从 **MySQL** 读取
并轮询热更新（Web 控制台写的也是它）；否则回落到 `-c` 指定的本地 YAML
（standalone/开发用，此时控制台的写接口返回 409）。`RL_NODE_NAME` 指定
本实例对应配置库里的哪个 HAProxy 实例——多台机器可共用一个配置库，各自
只读写属于自己的行。

**配置下发**：设 `RL_APPLY_HAPROXY_CFG=<本机 haproxy.cfg 路径>` 启用。
下发逻辑幂等且持续 reconcile（变更即触发 + 30s 兜底），所以手改 cfg 会被
自动拉回——**配置漂移从"被动告警"变成"自动修复"**。不设则完全不写盘、
不 reload，退化为只读监控。所需授权（cfg 目录可写、polkit/sudoers 授权
reload）见 `deploy/systemd/rl-limiter.service` 文件头。

**Web 控制台**：`RL_CONSOLE_PORT` 启用，`RL_CONSOLE_BIND` 指定监听地址
（默认 `127.0.0.1`）。控制台**无鉴权且带写接口**，放到内网必须配合
防火墙/安全组限制来源。三个 tab：**实例**（整台 HAProxy 的连接/带宽/
数据包视图）、**监听端口**（每个 frontend 的监控曲线 + 配置编辑）、
**日志**。数据包与丢包取自本机网卡 `/proc/net/dev`（`RL_NIC` 指定网卡）
——HAProxy 完全不统计数据包，因此它们是**整机口径**且无法按 frontend
拆分；每条曲线的来源与口径见 [docs/05-监控视图.md](docs/05-监控视图.md)。
