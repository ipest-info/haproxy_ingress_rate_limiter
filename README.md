# haproxy_ingress_rate_limiter

带宽账密计费模式下的入口限速与监控系统。**一个 rl-limiter 实例管一台与
它同机的 HAProxy**。**haproxy.cfg 是负载均衡配置的唯一权威**：监听端口、
模式、后端服务器由运维直接写在 cfg 里并自行 reload；rl-limiter 只**读**
这份 cfg 解析出 frontend/listen 清单，与本地 YAML 里登记的限额（quotas）
合并后做两件事：

1. **限速**：把限额下发到本机网卡的**内核 tc（HTB）**——按源端口把每个
   frontend 的出向流量分到自己的速率类里，该端口全部连接（含存量长连接）
   的总速率被硬性压在限额内，与连接数、单连接快慢无关。改限额走
   `tc class change`，**不 reload、存量连接立刻跟上**；
2. **监控**：每秒经**本机 unix stats socket** 采样各 frontend 的下行带宽，
   产出实时曲线（内置 Web 控制台 + Prometheus `/metrics`）、按分钟
   落盘到本地 JSONL 日志（历史回查/计费对账），并做持续超限告警。

限额除了直接编辑 YAML，也可在控制台页面上改，或调用带令牌鉴权的写 API
（`RL_API_TOKEN`）：支持每个 frontend 的限额与**实例总限速**
（`instance_quota_mbps`，tc 层级模式，罩全部 frontend 出向流量合计；
SSH/系统流量不受影响），修改回写 YAML 后数秒内热生效。

配置来源只有两个本地文件（**没有数据库**）：haproxy.cfg + rl-limiter 的
YAML（`-c` 指定：stats socket 接线、cfg 路径、quotas 限额）。运行期轮询
两者内容（5s），改了即热生效；文件短暂读不到时按最后一次配置继续跑
（fail-static）。

> 限速之前用的是 HAProxy 的 shared bwlim。换成 tc 是因为实测发现：**只要
> 挂着 bwlim 滤镜，HAProxy 就会完全关闭内核 splice（零拷贝转发）**，同
> 吞吐下 CPU 要多花一倍（0.59 → 0.33 CPU 秒/GB）。原委、行为差异与
> **尚未验证的部分**见 [docs/06-tc限速方案.md](docs/06-tc限速方案.md)。
>
> 检查限速有没有真的生效：`python3 tools/tc_check.py doctor`（体检环境）、
> `plan`（干跑看命令）、`verify`（核对网卡实况与配置是否一致）。

> **同机部署的理由**：tc 只能在流量出口那台机器上做，限速与被限的流量
> 必须同机。附带收益：stats socket 不占任何网络端口、无需对内网开放；
> 一台机器的故障不外溢；rl-limiter 宕机不影响已下发的限速（tc 规则留在
> 内核里）。

## 文档

| 文档 | 说明 |
| ---- | ---- |
| [docs/00-需求说明.md](docs/00-需求说明.md) | 原始需求（名词定义、架构图、核心诉求） |
| [docs/01-方案设计.md](docs/01-方案设计.md) | 方案设计（cfg 为权威的配置模型、tc 限速、监控与超限告警、容错） |
| [docs/02-建议与讨论点.md](docs/02-建议与讨论点.md) | 需求改进建议与决策清单 |
| [docs/03-限速服务运行指南.md](docs/03-限速服务运行指南.md) | 安装、YAML/haproxy.cfg 两个配置文件怎么写、限额调整 SOP、HAProxy 侧接线 |
| [docs/04-DockerCompose演示.md](docs/04-DockerCompose演示.md) | docker compose 一键演示（三台 Ubuntu 24.04 节点 + Web 控制台 + 可调并发压测） |
| [docs/05-监控视图.md](docs/05-监控视图.md) | 监控视图：每条曲线的数据来源与口径 |
| [docs/06-tc限速方案.md](docs/06-tc限速方案.md) | **限速为什么从 HAProxy bwlim 换成内核 tc**：实测依据、映射方式、行为差异，以及尚未验证的部分 |
| [docs/07-API文档.md](docs/07-API文档.md) | **HTTP API 参考（面向第三方开发）**：监控数据读接口（overview/history/SSE//metrics/logs）与限额写接口（令牌鉴权），字段口径、错误码、集成示例 |
| [docs/08-内核参数调优.md](docs/08-内核参数调优.md) | **让瓶颈落在 maxconn 而不是内核默认值上**：初始化阶段的 sysctl 调优与 FD 预检、哪些容器里改不动、怎么验证真的生效 |
| [docs/09-裸机部署.md](docs/09-裸机部署.md) | **HAProxy 已装好的机器上怎么加 rl-limiter**：一键装机脚本、两份配置文件、权限的由来 |
| [docs/10-监控聚合.md](docs/10-监控聚合.md) | **hap-agg：多台 HAProxy 的聚合监控**——经各机内网 TCP stats socket 批量采样，IP:port 批量导入，合并成一个视图（页面 + API + /metrics），目标机器零安装 |

## 系统组成

```
rl_limiter/       # Python 3.11 + asyncio 服务（与 HAProxy 同机）
  model.py        #   共享领域类型（单位约定、FrontendConfig、接线）
  haproxy.py      #   HAProxy runtime API 客户端（unix / TCP stats socket，只读采样）
  window.py       #   滑动窗口 + EWMA
  collector.py    #   每秒采样，按 frontend 产出用量；fail-static 与降级
  cfgparse.py     #   从本机 haproxy.cfg 解析受管 frontend 清单 + 双文件轮询热更新
  tcshaper.py     #   限速下发：把限额落到本机网卡的 tc（HTB），按源端口分类
  config.py       #   YAML 配置解析与校验（接线 + quotas 限额登记）
  webconsole.py   #   内置 Web 控制台（带宽曲线 + 实例视图 + 日志 + 限额编辑/写 API）
  configstore.py  #   写 API 的 YAML 回写（整份校验 + 原子替换，唯一写配置的地方）
  loop.py         #   1s 监控主循环（采集 → 超限判定 → 发布）
  agg.py          #   hap-agg：多目标采样差分与合并（独立入口 hap-agg，见 docs/10）
  aggconfig.py    #   hap-agg 的 YAML 配置（targets 清单 + 回写）
  aggweb.py       #   hap-agg 的 Web 视图/API/metrics
  aggmain.py      #   hap-agg 服务入口
tools/            # fake_haproxy.py（联调假节点，支持 unix / TCP）
                  # random_web.py（随机大小响应的模拟后端）、loadgen.py（可调并发压测）
                  # tc_check.py（限速检查：plan 干跑 / doctor 体检 / verify 核对）
deploy/           # systemd（同机形态）、haproxy 完整配置示例、YAML 示例配置
  docker/         #   node-entrypoint.sh（节点入口：环境预检 + haproxy + rl-limiter）、
                  #   haproxy-base.cfg（compose 用的完整 haproxy.cfg）、
                  #   limiter-node.yaml（compose 用的节点 YAML）
  sysctl/         #   tune-kernel.sh（内核参数调优表：apply 调优 / check 体检 /
                  #   dump 出 sysctl.d 配置。初始化阶段自动跑）
  bare/           #   裸机部署：rl-limiter.sh（一键装机+体检）、systemd unit
                  #   模板、rl-limiter.env 模板
docker-compose.yml      # 单节点环境（HAProxy + 同机 rl-limiter）
docker-compose-demo.yml # 一键演示：三台节点 + 模拟后端 + 压测
```

## 核心思路一句话

haproxy.cfg 定义"有哪些监听端口、转发到哪"，YAML 的 quotas 按段名登记
"每个端口限多少 Mbps"。一个 frontend = 一个监听端口 = 一个 tc 速率类；
tc 把该端口全部连接的**总**下行速率硬限在限额内，存量长连接持续受控，
上游流量靠 TCP 背压自然收敛。同一份限额同时是超限告警基准——同源，因此
不存在"配置改了、数据面忘了改"的漂移。cfg 里有但未登记限额的端口**只
监控不限速**。监控每秒采 `bytes_out`（前提 `option contstats`）算 10 秒
滑动均值；rl-limiter 宕机不影响已下发的限速。

## 快速开始

```bash
make install    # pip install -e ".[test]"
make test       # 全量单元测试
# 本地最小演示（假 HAProxy unix socket + YAML）见 docs/03

make demo-up    # docker compose 一键演示：三台 Ubuntu 24.04 节点（每台 =
                # HAProxy TCP L4 转发 + 内核 tc 限速 + 同机 rl-limiter）
                # + 随机大小响应后端 + 可调并发压测
                # 每台节点各一个控制台：http://localhost:8090 / :8092 / :8093
make demo-logs  # 观察各节点监控与 loadgen 分入口吞吐表格
make demo-down  # 收场
```

**往已经装好 HAProxy 的机器上加 rl-limiter**（全部跑在本机，无任何容器/
数据库依赖）——见 [docs/09-裸机部署.md](docs/09-裸机部署.md)：

```bash
sudo deploy/bare/rl-limiter.sh install
deploy/bare/rl-limiter.sh check     # 体检：只看不改
```

装机脚本**不碰你的 haproxy.cfg**（缺 stats socket / contstats 时只告诉你
该加哪一行），起来之后会跑一遍 `tc_check.py verify` 逐条核对限速真的在
生效。

**配置调整 SOP**：

- **改限额** → 三条等价入口，最终都是 YAML 的 `quotas` 段：直接编辑
  文件；控制台页面（frontend 卡片右上角）；写 API
  `PUT /api/quotas/<段名>`（页面与 API 需 `RL_API_TOKEN`，见 docs/03）。
  5s 内热生效（`tc class change`，不 reload，存量连接立刻按新限额跑；
  经 API 修改会立即触发重读，不等轮询）；**写 0 = 显式不限速**（撤掉该
  端口的 tc 类；全部为 0/清空且未设总限速时整棵限速队列树被拆掉）；
- **改实例总限速** → YAML 的 `instance_quota_mbps` / 控制台实例页 /
  `PUT /api/instance-quota`。设置后 tc 切换为层级模式：本机 HAProxy
  **全部 frontend**（含未登记限额的）出向合计不超过总限速，各端口限额
  仍各自生效，SSH/系统流量不在总闸内；0 = 取消（回到平铺模式）；
- **改端口/后端** → 改 haproxy.cfg → `systemctl reload haproxy`。
  rl-limiter 轮询到 cfg 内容变化后自动更新监控清单与 tc 分类
  （**这一类没有页面/API 入口**：haproxy.cfg 只归运维手工编辑）。

**内核参数**：节点初始化阶段（启动 HAProxy 之前）自动跑
`deploy/sysctl/tune-kernel.sh`，把高并发/高带宽相关的 sysctl 抬到位，
逐项**读回校验**，容器里改不动的明确列出并给出宿主机命令。不做这一步的话
内核默认值会在 HAProxy 下面先成为瓶颈——例如 cfg 里写着 `backlog 65536`，
`net.core.somaxconn` 默认 4096 会把它**静默削到 4096**（实测）。体检用
`tune-kernel.sh check`，宿主机持久化用 `dump`；详见
[docs/08-内核参数调优.md](docs/08-内核参数调优.md)。

**Web 控制台**：`RL_CONSOLE_PORT` 启用，`RL_CONSOLE_BIND` 指定监听地址
（默认 `127.0.0.1`）。**读接口无鉴权**（暴露全量监控数据与运行日志），
放到内网必须配合防火墙/安全组限制来源；**写接口（改限额）必须带令牌**
（`RL_API_TOKEN`，不设则写接口整体 403、页面退化为只读）。三个 tab：
**实例**（整台 HAProxy 的连接/带宽/数据包视图 + 实例总限速编辑）、
**监听端口**（每个 frontend 的监控曲线、配置视图与限额编辑）、**日志**。
每条曲线的来源与口径见 [docs/05-监控视图.md](docs/05-监控视图.md)。

**监控数据的三个出口**（同一拍、同一份数据）：

- **实时曲线**：控制台内存环形缓冲，最近约 10 分钟；
- **Prometheus 抓取**：控制台同端口的 `GET /metrics`（文本 exposition
  格式，frontend/instance 两级 gauge），接进已有的 Prometheus/Grafana
  即得长期存储与告警；
- **本地日志落盘**：设 `RL_METRICS_LOG=<文件路径>` 启用——分钟粒度
  JSONL（avg/max/mean10_max/超限秒数，限额随行），按天轮转、默认保留
  90 天（`RL_METRICS_LOG_DAYS`），重启不丢、可 grep、可被日志采集系统
  直接摄取。

**默认值不设限**：HAProxy 示例配置的 `maxconn` 默认 100 万——**要限并发
时再显式往下调**，而不是让人在排查"为什么这么慢"时最后发现是某个默认值。
100 万连接对应 400 万 fd，宿主机的 `fs.nr_open` 得先抬上去，见
[docs/08](docs/08-内核参数调优.md)。限速的配置单位一律是 **Mbps**
（quotas 里填 40 就是 40 Mbps，允许小数）。
