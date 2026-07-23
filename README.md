# haproxy_ingress_rate_limiter

带宽账密计费模式下的入口限速与监控系统：**限速由各台 HAProxy（TCP L4
负载均衡）自身的 shared bwlim 聚合限速执行**——本机受控 frontend 全部
连接（含存量长连接）的总速率被硬性压在限额内，限额是配置常量，调整走
"改库 + 改 cfg + reload"的发布流程。**rl-limiter（Python）是独立部署的
集中监控服务**：通过内网 TCP 只读采样多台 HAProxy 的下行带宽，按节点/
环境展示实时视图，对照 MySQL 配置库中登记的限额做**持续超限告警**
（发现配置漂移/漏配限速）。内置 **Web 控制台**（实时曲线、环境聚合、
限额登记、日志）。

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
rl_limiter/       # Python 3.11 + asyncio 集中监控服务
  model.py        #   共享领域类型（单位约定、Target/EnvQuota=监控单元等）
  haproxy.py      #   HAProxy runtime API 客户端（内网 TCP stats socket，只读采样）
  window.py       #   滑动窗口 + EWMA
  collector.py    #   多节点并发采样、按监控单元（节点）聚合、节点级容错
  dbconfig.py     #   MySQL 配置源（启动加载 + 轮询热更新 + 控制台写回，RL_MYSQL_* 接线）
  config.py       #   配置解析与校验（YAML 与数据库共用同一管线）
  webconsole.py   #   内置 Web 控制台（节点带宽视图/环境聚合视图/限额登记/日志）
  loop.py         #   1s 监控主循环（采集 → 超限判定 → 发布）
tools/            # fake_haproxy.py（联调假节点）
                  # random_web.py（随机大小响应的模拟后端）、loadgen.py（可调并发压测）
deploy/           # systemd、haproxy 聚合限速配置示例、tc 兜底脚本、YAML 示例配置
                  # mysql/init.sql（配置库建表+种子）、docker/（compose 用 HAProxy 配置）
docker-compose.yml # 一键演示：MySQL + 三台 HAProxy + 模拟后端 + 压测 + 控制台
```

## 核心思路一句话

每台 HAProxy 用 shared bwlim（stick-table 共享速率桶）把本机受控
frontend 的**总**下行速率硬限在限额内——与连接数、单连接快慢无关，
存量长连接持续受控，限额调整（reload + hard-stop-after）对存量连接也
生效；节点之间互不调配、故障互不影响；上游流量靠 TCP 背压自然收敛，
tc 在各节点作硬兜底。rl-limiter 每秒并发采样所有节点各 frontend 的
`bytes_out`（前提 `option contstats`），按节点聚合成 10 秒滑动均值，
持续高于库中登记限额即告警（配置漂移的兜底检验）；监控服务宕机不影响
限速。

## 快速开始

```bash
make install    # pip install -e ".[test]"
make test       # 全量单元测试
# 本地最小演示（假 HAProxy + standalone YAML）见 docs/03

make demo-up    # docker compose 一键演示：MySQL 配置 + 三台真实 HAProxy
                # (TCP L4 shared bwlim) + 随机大小响应后端 + 可调并发压测
                # 浏览器打开 http://localhost:8090 进入 Web 控制台：
                # 每节点带宽曲线 + 环境聚合视图 + 超限告警 + 日志
make demo-logs  # 观察 rl-limiter 监控与 loadgen 分入口吞吐表格
make demo-down  # 收场（含 MySQL 数据卷）
```

配置来源二选一：设置 `RL_MYSQL_HOST` 等环境变量时从 **MySQL** 读取并
轮询热更新（登记限额/环境分组改表即生效）；否则回落到 `-c` 指定的
本地 YAML（standalone/开发用）。限额的真实执行在各节点 haproxy.cfg 的
shared bwlim `limit`，与库中登记值由发布流程保持一致（docs/03 §3）。
