# haproxy_ingress_rate_limiter

带宽账密计费模式下的入口限速与监控系统：**限速由各台 HAProxy（TCP L4
负载均衡）自身的 shared bwlim 聚合限速执行**——本机受控 frontend 全部
连接（含存量长连接）的总速率被硬性压在限额内，限额是配置常量，调整走
"改库 + 改 cfg + reload"的发布流程。**rl-limiter（Python）与 HAProxy
同机部署**：每台 HAProxy 服务器上一个实例，做两件事——①通过**本机 unix
stats socket** 只读采样本机下行带宽并做持续超限告警；②**把配置库里的
限额自动落到本机数据面**：限额一改就改写本机 haproxy.cfg 的 bwlim limit
并 reload，实测 ~74 ms 生效。内置 **Web 控制台**（实时曲线、限额登记、
日志）。

> **同机部署的首要理由就是"改完立刻生效"**：只有在同一台机器上，服务
> 才有可能直接改本机配置并 reload；跨机的集中服务做不到（要么开 SSH，
> 要么另装 agent）。附带收益：stats socket 不占任何网络端口、无需对内网
> 开放；一台机器的故障不外溢。
>
> 由 `RL_NODE_NAME=<本机节点名>` 开启同机模式，`RL_APPLY_HAPROXY_CFG`
> 开启限额自动应用（默认关闭 = 只读监控）。不设 `RL_NODE_NAME` 则退回
> **集中监控**形态（一个实例经内网 TCP 采样多台，提供跨节点的环境聚合
> 视图），代码同时支持两者。

## 文档

| 文档 | 说明 |
| ---- | ---- |
| [docs/00-需求说明.md](docs/00-需求说明.md) | 原始需求（名词定义、架构图、核心诉求） |
| [docs/01-方案设计.md](docs/01-方案设计.md) | 方案设计（shared bwlim 聚合限速、限额调整流程、监控与超限告警、MySQL 配置模型、容错） |
| [docs/02-建议与讨论点.md](docs/02-建议与讨论点.md) | 需求改进建议与决策清单 |
| [docs/03-限速服务运行指南.md](docs/03-限速服务运行指南.md) | 安装、配置来源（MySQL/YAML）、限额调整 SOP、HAProxy 侧接线 |
| [docs/04-DockerCompose演示.md](docs/04-DockerCompose演示.md) | docker compose 一键演示（MySQL + 三台 HAProxy L4 聚合限速 + Web 控制台 + 可调并发压测） |

## 系统组成

```
rl_limiter/       # Python 3.11 + asyncio 监控服务（与 HAProxy 同机）
  model.py        #   共享领域类型（单位约定、Target/EnvQuota=监控单元、节点接线）
  haproxy.py      #   HAProxy runtime API 客户端（unix / TCP stats socket，只读采样）
  window.py       #   滑动窗口 + EWMA
  collector.py    #   并发采样、按监控单元（节点）聚合、节点级容错
  dbconfig.py     #   MySQL 配置源（启动加载 + 轮询热更新 + 控制台写回，RL_MYSQL_* 接线）
  enforcer.py     #   限额下发：改本机 haproxy.cfg 的 bwlim limit + 校验 + reload
  config.py       #   配置解析与校验（YAML 与数据库共用同一管线）+ 按本机节点裁剪
  webconsole.py   #   内置 Web 控制台（节点带宽视图/限额登记/日志）
  loop.py         #   1s 监控主循环（采集 → 超限判定 → 发布）
tools/            # fake_haproxy.py（联调假节点，支持 unix / TCP）
                  # random_web.py（随机大小响应的模拟后端）、loadgen.py（可调并发压测）
deploy/           # systemd（同机形态）、haproxy 聚合限速配置示例、tc 兜底脚本、
                  # YAML 示例配置、mysql/init.sql（配置库建表+种子）
  docker/         #   Dockerfile.node（Ubuntu 24.04 + HAProxy + 同机 rl-limiter）、
                  #   node-entrypoint.sh、compose 用的 HAProxy 配置
docker-compose.yml # 一键演示：MySQL + 三台 Ubuntu 24.04 节点 + 模拟后端 + 压测
```

## 核心思路一句话

每台 HAProxy 用 shared bwlim（stick-table 共享速率桶）把本机受控
frontend 的**总**下行速率硬限在限额内——与连接数、单连接快慢无关，
存量长连接持续受控，限额调整（reload + hard-stop-after）对存量连接也
生效；节点之间互不调配、故障互不影响；上游流量靠 TCP 背压自然收敛，
tc 在各节点作硬兜底。同机的 rl-limiter 每秒经本机 unix socket 采样本机
各 frontend 的 `bytes_out`（前提 `option contstats`），聚合成 10 秒滑动
均值，持续高于库中登记限额即告警（配置漂移的兜底检验）；监控进程宕机
不影响限速。

## 快速开始

```bash
make install    # pip install -e ".[test]"
make test       # 全量单元测试
# 本地最小演示（假 HAProxy unix socket + standalone YAML）见 docs/03

make demo-up    # docker compose 一键演示：MySQL 配置 + 三台 Ubuntu 24.04
                # 节点（每台 = HAProxy TCP L4 shared bwlim + 同机 rl-limiter）
                # + 随机大小响应后端 + 可调并发压测
                # 每台节点各一个控制台：http://localhost:8090 / :8092 / :8093
make demo-logs  # 观察各节点监控与 loadgen 分入口吞吐表格
make demo-down  # 收场（含 MySQL 数据卷）
```

**部署形态**：设 `RL_NODE_NAME=<本机节点名>` 进入同机模式（只采本机，
走 `socket_path` 指向的本机 unix stats socket）；不设则为集中监控模式
（采多台，走 `host`/`port` 内网 TCP）。配置文件/配置库**始终写全量**并
按全量校验（节点独占等是跨节点不变量），校验通过后才裁剪到本机——三台
机器可共用同一份配置，只有 `RL_NODE_NAME` 不同。

**配置来源二选一**：设置 `RL_MYSQL_HOST` 等环境变量时从 **MySQL** 读取
并轮询热更新（限额/环境分组改表即生效）；否则回落到 `-c` 指定的本地
YAML（standalone/开发用）。

**限额调整**：启用限额自动应用（`RL_APPLY_HAPROXY_CFG`）后只需改库，
本机 rl-limiter 自动改 cfg + `haproxy -c` 校验 + 原子替换 + reload；
应用逻辑幂等且持续 reconcile，手改 cfg 会被自动拉回——**配置漂移从
"被动告警"变成"自动修复"**。未启用时退回人工两步流程（docs/03 §3）。
所需授权（cfg 目录可写、polkit/sudoers 授权 reload）见
`deploy/systemd/rl-limiter.service` 文件头。

**Web 控制台**：`RL_CONSOLE_PORT` 启用，`RL_CONSOLE_BIND` 指定监听地址
（默认 `127.0.0.1`）。控制台**无鉴权且带写接口**，放到内网必须配合
防火墙/安全组限制来源。
