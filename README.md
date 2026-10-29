# rl-limiter（通用形态）

带宽按量计费场景下的**服务器出口限速与监控系统**：一台服务器一个
rl-limiter，**不依赖机器上跑的任何服务**——nginx、HAProxy、自研 TCP
服务，或者什么都不跑，一概适用。本分支的形态是**双网卡 + 闭环动态
限速**：

- **监控全部来自内核**：双网卡设备视图（/proc/net/dev：带宽/包/丢包，
  出厂接线 eth0 客户侧 / eth1 后端侧）+ TCP 连接统计（/proc/net/snmp：
  并发连接、每秒新建连接，整机口径）；
- **限速以网卡字节为准**（与计费同口径）：`instance_quota_mbps` 是
  出向带宽目标，出向超标时**动态收紧客户端侧网卡的入向带宽**把出向
  间接压回目标（执行面：ingress 重定向到 IFB，HTB 平稳整形）——请求
  进得慢，响应自然出得少，响应路径零排队；
- 调整刻意**平缓**：死区 + 乘性小步 + 按实测流入落位，宁可收敛慢也不
  振荡；
- 另可按端口登记**静态入向限速**（YAML `ports` + `request_limits`，
  纯 tc，与服务无关）。

限速逻辑整体在**内核 tc**：rl-limiter 进程只做采样、决策与下发，它
短暂宕机不影响已生效的限速。监控侧提供内置 Web 控制台（实时曲线、
调节视图、设备视图、配置页）、Prometheus `/metrics`、分钟粒度 JSONL
落盘。多机总量控制由同包的 rl-fleet 总控完成（docs/10）。

两条铁律：

1. **配置只有一份本地 YAML**（没有数据库），页面/写 API 的修改回写
   YAML 热生效；
2. **宁可起不来，也不要"以为在限速"**：一切会让限速静默失效的条件都在
   启动时大声失败。

## 快速开始

**本地演示**（Linux 主机，内核需有 ifb/HTB；Docker Desktop 不行，原因
见 docs/09）：

```bash
make demo-up          # 两台"任意服务"节点 + 压测 + 总控，双网卡拓扑
open http://127.0.0.1:8090        # node1 控制台（演示令牌见 docs/05）
# 配置页设"实例总限速"=40 → curl -X PUT localhost:8084/concurrency \
#   -d '{"concurrency": 1000}' 加压 → 看"调节视图"收敛
make demo-down
```

**生产**：二进制一键安装（[docs/11](docs/11-发布与安装.md)）——节点
必须双网卡，客户侧/后端侧网卡相斥：

```bash
curl -fL -o install.sh https://github.com/ipest-info/haproxy_ingress_rate_limiter/releases/latest/download/install.sh
sudo bash install.sh --download --token auto --client-nic eth0 --backend-nic eth1   # 节点（装完打印令牌）
sudo bash install.sh --download --fleet --token auto                               # 总控（页面里添加节点）
```

或裸机 `deploy/bare/rl-limiter.sh install`，或容器 host 网络
`docker compose -f docker-compose-host.yml up -d --build`。见
[docs/04 部署指南](docs/04-部署指南.md)。

## 文档

| 文档 | 说明 |
| ---- | ---- |
| [01 总体设计](docs/01-总体设计.md) | 定位与设计原则、组件与数据流、配置分工 |
| [02 闭环限速原理](docs/02-闭环限速原理.md) | 为什么限入向、IFB+HTB 执行面、闭环算法与全部参数、ACK 杠杆 |
| [03 配置参考](docs/03-配置参考.md) | YAML 全字段（热更性标注）、环境变量全表 |
| [04 部署指南](docs/04-部署指南.md) | 二进制/裸机/容器 host 网络、内核前提、双网卡接线、验收清单 |
| [05 DockerCompose 演示](docs/05-DockerCompose演示.md) | 一键 demo：拓扑、闭环演示 SOP、loadgen 控制 |
| [06 控制台与监控](docs/06-控制台与监控.md) | 页面与调节视图、读数口径、落盘、/metrics、安全边界 |
| [07 API 参考](docs/07-API参考.md) | 读/写 HTTP API：字段口径、鉴权、错误码、curl 示例 |
| [08 内核参数调优](docs/08-内核参数调优.md) | tune-kernel.sh：参数表、容器里哪些改不动、怎么验证生效 |
| [09 排障指南](docs/09-排障指南.md) | 按症状查：启动失败、限速没生效、振荡、高并发 |
| [10 总控制台](docs/10-总控制台.md) | **多机总量控制**：rl-fleet 汇总数十台节点并把总流出带宽按需分配为各节点的出向目标；页面「节点管理」增删节点、「历史回查」查总带宽/限速变更 |
| [11 发布与安装](docs/11-发布与安装.md) | 二进制发布包：tag/Release 命名规则、CI 构建、install.sh 一键安装（令牌/网卡选项、双网卡检查） |

## 系统组成

```
rl_limiter/       # Python 3.9+ asyncio 服务（一台服务器一个）
  netsrc.py       #   内核 TCP 计数器采样      netdev.py（/proc/net/dev 设备视图）
  collector.py    #   每秒采样与差分           window.py / loop.py
  cfgparse.py     #   端口登记派生 + YAML 热更  config.py / configstore.py
  ingressgov.py   #   闭环控制器（网卡口径观测 → 入向总闸）
  tcifb.py        #   入向执行面 IFB+HTB       tcingress.py（police 备选 + SYN 闸）
  tcshaper.py     #   出向执行面 HTB（可选）
  webconsole.py   #   控制台 + API + /metrics  metricslog.py（JSONL 落盘）
  fleet*.py       #   rl-fleet 总控：多机汇总 + 总带宽按需分配（docs/10）
tools/            # loadgen.py / random_web.py / tc_check.py
deploy/           # bare/（裸机装机+systemd） docker/（容器入口与模板）
                  # release/（二进制构建与安装） sysctl/tune-kernel.sh
                  # config/limiter.example.yaml（带全量注释的生产示例）
docker-compose-demo.yml   # 演示环境（双网卡拓扑）
docker-compose-host.yml   # 生产形态样例（host 网络）
```

## 测试

```bash
make test         # = python3 -m pytest tests/（纯单元测试，不碰真实 tc/网络）
```
