# haproxy_ingress_rate_limiter

带宽账密计费模式下的入口限速与监控系统。**一个 rl-limiter 实例管一台与
它同机的 HAProxy**：在 Web 界面上配置监听端口、限额与后端服务器，保存即
写库，随后由本机 rl-limiter 渲染进 haproxy.cfg 的受管区块并 reload——
**改完立刻生效**（实测一次下发 ~74 ms）。

限速由**内核 tc（HTB）**执行，**两种范围二选一**（`limit_scope`）：

- **整机限速**（`host`，**默认**）：一个速率类罩住本机网卡的**全部出向
  流量**，不按端口分类。队列树固定两个类，加多少个入口都不变，也没有按
  源端口分类的那一串坑。限额按机器算（一台机器 = 一份带宽）时用它。
- **按 frontend 限速**（`frontend`）：按源端口把每个 frontend 的出向流量
  分到自己的速率类里。一台机器上多个入口各有各的限额时用它。

**默认不限速**：整机限额（`host_quota_mbps`）默认留空，含义就是不限速——
网卡上不会有任何 tc 队列树，新装的机器开箱能跑满，要限的时候再填一个值。
留空与填 0 不是一回事：填 0 直接拒绝（0 Mbps 谁也跑不动，那是配错了），
用 0 兼表"没设"会让打错字**静默变成不限速**。

两种范围下，受限流量（含存量长连接）的**总**速率都被硬性压在限额内，与
连接数、单连接快慢无关。改限额走 `tc class change`，**连 reload 都不需要，
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
| [docs/06-tc限速方案.md](docs/06-tc限速方案.md) | **限速为什么从 HAProxy bwlim 换成内核 tc**：两种限速范围（整机 / 按 frontend）、实测依据、映射方式、行为差异，以及尚未验证的部分 |
| [docs/07-监控数据回查.md](docs/07-监控数据回查.md) | **90 天回查怎么存怎么查**：分级保留、聚合语义、真实 MariaDB 上的容量与耗时实测 |
| [docs/08-内核参数调优.md](docs/08-内核参数调优.md) | **让瓶颈落在 maxconn 而不是内核默认值上**：初始化阶段的 sysctl 调优与 FD 预检、哪些容器里改不动、怎么验证真的生效 |
| [docs/09-裸机部署.md](docs/09-裸机部署.md) | **HAProxy 已装好的机器上怎么加 rl-limiter**：一键装机脚本、只起配置库的 compose、三项权限的由来 |
| [docs/10-LVS-IPVS可行性.md](docs/10-LVS-IPVS可行性.md) | **换成 LVS/IPVS 行不行**：只有 NAT 模式可行的原因、逐项得失、验证计划与证据等级。限速侧的关键前提已在等价内核路径上实测（研究，未动限速/转发代码） |

## 系统组成

```
rl_limiter/       # Python 3.11 + asyncio 服务（与 HAProxy 同机）
  model.py        #   共享领域类型（单位约定、FrontendConfig/ServerEntry、接线）
  haproxy.py      #   HAProxy runtime API 客户端（unix / TCP stats socket，只读采样）
  window.py       #   滑动窗口 + EWMA
  collector.py    #   每秒采样，按 frontend 产出用量；fail-static 与降级
  dbconfig.py     #   MySQL 配置源（启动加载 + 轮询热更新 + 控制台写回，RL_MYSQL_* 接线）
  enforcer.py     #   配置下发：渲染受管区块 → haproxy -c 校验 → 原子替换 → reload
  tcshaper.py     #   限速下发：把限额落到本机网卡的 tc（HTB）。两种范围：
                  #   整机一个总闸门（默认）/ 按源端口分类逐个 frontend
  config.py       #   配置解析与校验（YAML 与数据库共用同一管线）
  metricstore.py  #   监控数据分级落库与回查（1min×7天 / 5min×90天）
  webconsole.py   #   内置 Web 控制台（带宽曲线 + 端口/限额/后端服务器管理）
  loop.py         #   1s 监控主循环（采集 → 超限判定 → 发布）
tools/            # fake_haproxy.py（联调假节点，支持 unix / TCP）
                  # bootstrap_db.py（把本机登记进配置库，幂等、只增不改）
                  # random_web.py（随机大小响应的模拟后端）、loadgen.py（可调并发压测）
                  # tc_check.py（限速检查：plan 干跑 / doctor 体检 / verify 核对）
                  # ipvs_probe.sh（LVS/IPVS 可行性实测：doctor 体检 /
                  # forward 转发路径实测（不需要 ip_vs）/ classify / stats）
deploy/           # systemd（同机形态）、haproxy 骨架配置示例、
                  # YAML 示例配置、mysql/init.sql（配置库建表+种子）
  docker/         #   node-entrypoint.sh（节点入口：环境预检 + haproxy + rl-limiter）、
                  #   haproxy-base.cfg（compose 用的 global/defaults 骨架）
  sysctl/         #   tune-kernel.sh（内核参数调优表：apply 调优 / check 体检 /
                  #   dump 出 sysctl.d 配置。初始化阶段自动跑）
  bare/           #   裸机部署：rl-limiter.sh（一键装机+体检）、systemd unit
                  #   模板、rl-limiter.env 模板、只起 MySQL 的 compose
docker-compose.yml # 一键演示：MySQL + 三台 Ubuntu 24.04 节点 + 模拟后端 + 压测
```

## 核心思路一句话

配置库是唯一数据源：一个 frontend = 一个监听端口 + 一组后端服务器 +
一个限额。rl-limiter 把监听端口与后端渲染进本机 haproxy.cfg 的受管区块并
reload，把限额下发到本机网卡的 tc 上，同一份限额同时作为超限告警基准——
同源，因此不存在"库改了、数据面忘了改"的漂移。tc 把受限流量（整机，或
该端口全部连接）的**总**下行速率硬限在限额内，存量长连接持续受控；
上游流量靠 TCP 背压自然收敛。
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

**往已经装好 HAProxy 的机器上加 rl-limiter**（两个都跑在本机，只有配置库
用 Docker）——见 [docs/09-裸机部署.md](docs/09-裸机部署.md)：

```bash
sudo BOOTSTRAP_BACKEND=10.0.0.21:9000 deploy/bare/rl-limiter.sh install
deploy/bare/rl-limiter.sh check     # 体检：只看不改
```

装机脚本**不碰你的 haproxy.cfg**（缺 stats socket 时只告诉你该加哪一行），
起来之后会跑一遍 `tc_check.py verify` 逐条核对限速真的在生效。

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

**内核参数**：节点初始化阶段（启动 HAProxy 之前）自动跑
`deploy/sysctl/tune-kernel.sh`，把高并发/高带宽相关的 sysctl 抬到位，
逐项**读回校验**，容器里改不动的明确列出并给出宿主机命令。不做这一步的话
内核默认值会在 HAProxy 下面先成为瓶颈——例如 cfg 里写着 `backlog 65536`，
`net.core.somaxconn` 默认 4096 会把它**静默削到 4096**（实测）。体检用
`tune-kernel.sh check`，宿主机持久化用 `dump`；详见
[docs/08-内核参数调优.md](docs/08-内核参数调优.md)。

**限速范围与整机限额在控制台的「实例」页上改**。只改限额是就地生效的
（`tc class change`），不打断任何连接；切换范围、以及在"限速/不限速"
之间来回则会重建队列树。把限额清空 = 关闭本机限速，会拆掉整棵队列树并
在日志里明确记一条，不会悄悄发生。

**Web 控制台**：`RL_CONSOLE_PORT` 启用，`RL_CONSOLE_BIND` 指定监听地址
（默认 `127.0.0.1`）。控制台**无鉴权且带写接口**，放到内网必须配合
防火墙/安全组限制来源。三个 tab：**实例**（整台 HAProxy 的连接/带宽/
数据包视图）、**监听端口**（每个 frontend 的监控曲线 + 配置编辑）、
**日志**。数据包与丢包取自本机网卡 `/proc/net/dev`（`RL_NIC` 指定网卡）
——HAProxy 完全不统计数据包，因此它们是**整机口径**且无法按 frontend
拆分；每条曲线的来源与口径见 [docs/05-监控视图.md](docs/05-监控视图.md)。

**配后端服务器不用一台一台点**：编辑面板里有「批量填充」——把一列地址
粘进去即可，名称按前缀（默认 `srv`）自动编号。地址支持 `10.0.0.5:9000`
与末段区间 `10.0.0.20-24`；端口只填一个就套用到所有地址，也可以给一列
一一对应（数量对不上**直接报错，不猜**）。往表格的「地址」「端口」格子
里直接粘贴多行同样会自动展开成多行。

**默认值不设限**：新建监听端口的限额默认 **50000 Mbps**（限速的配置单位
一律是 Mbps——库、YAML、控制台三处同一个字段同一个单位），frontend 级
`maxconn` 默认不设，HAProxy 骨架的 `maxconn`
默认 100 万——**要限速/限并发时再显式往下调**，而不是让人在排查"为什么这么
慢"时最后发现是某个默认值。100 万连接对应 400 万 fd，宿主机的
`fs.nr_open` 得先抬上去，见 [docs/08](docs/08-内核参数调优.md)。
