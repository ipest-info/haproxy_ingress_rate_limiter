# haproxy_ingress_rate_limiter

带宽账密计费模式下的入口动态限速系统（Python 实现）：rl-limiter 服务
独立部署，通过内网 TCP 同时控制多台 HAProxy（TCP L4 负载均衡），**每台
HAProxy 按自己的带宽限制独立整形与调节**；业务环境是节点分组，控制台
提供"环境 = 成员节点带宽之和"的聚合视图。配置存 **MySQL**（启动加载 +
轮询热更新），内置 **Web 控制台**（实时曲线、在线调参、日志）。

## 文档

| 文档 | 说明 |
| ---- | ---- |
| [docs/00-需求说明.md](docs/00-需求说明.md) | 原始需求（名词定义、架构图、核心诉求） |
| [docs/01-方案设计.md](docs/01-方案设计.md) | 方案设计（按节点限速、AIMD 算法、MySQL 配置模型、TCP L4 整形、容错） |
| [docs/02-建议与讨论点.md](docs/02-建议与讨论点.md) | 需求改进建议与决策清单 |
| [docs/03-限速服务运行指南.md](docs/03-限速服务运行指南.md) | 安装、配置来源（MySQL/YAML）、本地演示、HAProxy 侧前置条件 |
| [docs/04-DockerCompose演示.md](docs/04-DockerCompose演示.md) | docker compose 一键演示（MySQL + 三台 HAProxy L4 + Web 控制台 + 可调并发压测） |

## 系统组成

```
rl_limiter/       # Python 3.11 + asyncio 集中限速服务
  model.py        #   共享领域类型（单位约定、Target/EnvQuota=控制单元/Decision 等）
  haproxy.py      #   HAProxy runtime API 客户端（内网 TCP stats socket）
  window.py       #   滑动窗口 + EWMA
  collector.py    #   多节点并发采样、按控制单元（节点）聚合、节点级容错
  governor.py     #   per-node AIMD 状态机
  allocator.py    #   节点整形值在节点内按 frontend 加权拆分（含保底）
  executor.py     #   dry-run/enforce 执行（模式可按节点）、pending 重试、resync
  dbconfig.py     #   MySQL 配置源（启动加载 + 轮询热更新 + 控制台写回，RL_MYSQL_* 接线）
  config.py       #   配置解析与校验（YAML 与数据库共用同一管线）
  webconsole.py   #   内置 Web 控制台（节点带宽视图/环境聚合视图/在线调参/日志）
  loop.py         #   1s 主循环
tools/            # fake_haproxy.py（联调假节点）
                  # random_web.py（随机大小响应的模拟后端）、loadgen.py（可调并发压测）
deploy/           # systemd、haproxy L4 配置示例、tc 兜底脚本、YAML 示例配置
                  # mysql/init.sql（配置库建表+种子）、docker/（compose 用 HAProxy 配置）
docker-compose.yml # 一键演示：MySQL + 三台 HAProxy + 模拟后端 + 压测 + 控制台
```

## 核心思路一句话

rl-limiter 每秒并发采样所有 HAProxy 节点各 frontend 的 `bytes_out`，
按节点聚合成 **10 秒滑动均值**，与该节点自己的带宽限制比较后做 AIMD
调整（弹性上限 110%、急收慢放、不拒绝新建连接、不断开存量连接），经
HAProxy 原生 `bwlim-out` + runtime API map 写回该节点完成整形；节点之间
互不调配、故障互不影响；上游流量靠 TCP 背压自然收敛，tc 在各节点作硬
兜底；配置库断联或服务宕机时全链路 fail-static，绝不放开限速。

## 快速开始

```bash
make install    # pip install -e ".[test]"
make test       # 全量单元测试
# 本地最小演示（假 HAProxy + standalone YAML）见 docs/03

make demo-up    # docker compose 一键演示：MySQL 配置 + 三台真实 HAProxy
                # (TCP L4) + 随机大小响应后端 + 可调并发压测（玩法见 docs/04）
                # 浏览器打开 http://localhost:8090 进入 Web 控制台：
                # 每节点带宽曲线/调参 + 环境聚合视图 + 日志
make demo-logs  # 观察 rl-limiter 决策与 loadgen 分入口吞吐表格
make demo-down  # 收场（含 MySQL 数据卷）
```

配置来源二选一：设置 `RL_MYSQL_HOST` 等环境变量时从 **MySQL** 读取并
轮询热更新（节点带宽限制/模式/参数改表即生效）；否则回落到 `-c` 指定的
本地 YAML（standalone/开发用）。
