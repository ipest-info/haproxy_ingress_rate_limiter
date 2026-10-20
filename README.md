# haproxy_ingress_rate_limiter

带宽按量计费场景下的入口限速与监控系统（**LVS 分支**）：一个
rl-limiter 实例管一台与它同机的 **LVS（IPVS）director**（NAT/FULLNAT
模式，双向流量都过本机）。核心能力：

- **按虚拟服务（VIP:端口）限出向带宽**：NAT 下出向包 un-NAT 后源即
  VIP:vport，内核 tc（HTB）按端口分类直接整形，改限额 `tc class
  change` 热生效；另有实例总限速（层级模式总闸）；
- **受管清单 = 内核 IPVS 实况表**：keepalived/ipvsadm 维护虚拟服务，
  rl-limiter 只读（/proc/net/ip_vs + ipvsadm --stats），增删服务热跟进；
- **主备开箱即用**：keepalived 主备都装 IPVS 表、rl-limiter 两台同
  配置部署——备机规则空转、VIP 漂移限速零窗口接管（docs/11）；
- 继承自 HAProxy 分支的可选能力：入向闭环动态限速、多机总控
  （rl-fleet 总量按需分配）。

限速逻辑整体在**内核 tc**：rl-limiter 进程只做采样、决策与下发，它
短暂宕机不影响已生效的限速。监控侧提供内置 Web 控制台（实时曲线、
调节视图、tc 实况、配置页）、Prometheus `/metrics`、持续超限告警与
分钟粒度 JSONL 落盘。

两条铁律：

1. **haproxy.cfg 是负载均衡配置的唯一权威**（监听端口/后端由运维直管，
   rl-limiter 只读、绝不 reload HAProxy）；限额住在 rl-limiter 自己的
   YAML 里，**没有数据库**，页面/写 API 的修改回写 YAML 热生效；
2. **宁可起不来，也不要"以为在限速"**：一切会让限速静默失效的条件都在
   启动时大声失败。

## 快速开始

**本地演示**（Linux 主机，先 `modprobe ip_vs ip_vs_rr`；内核还需
HTB。Docker Desktop 的精简内核多半不行，入口自检会直接说）：

```bash
modprobe ip_vs ip_vs_rr && make demo-up   # 主备 director 对 + 后端 + 压测 + 总控
open http://127.0.0.1:8090        # node1 控制台（令牌 demo-token）
# 配置页给 vs_10.211.1.100_8080 设限额 → 看带宽被压住；
# docker compose -f docker-compose-demo.yml stop node1 → 看 VIP 漂移零窗口接管
make demo-down
```

**生产**：裸机 `deploy/bare/rl-limiter.sh install`，或容器 host 网络
`docker compose -f docker-compose-host.yml up -d --build`。见
[docs/04 部署指南](docs/04-部署指南.md)。

## 文档

| 文档 | 说明 |
| ---- | ---- |
| [01 总体设计](docs/01-总体设计.md) | 设计原则、组件与数据流、两份配置文件的分工、分支形态 |
| [02 闭环限速原理](docs/02-闭环限速原理.md) | 为什么限入向、IFB+HTB 执行面、两级闭环算法与全部参数、ACK 杠杆 |
| [03 配置参考](docs/03-配置参考.md) | YAML 全字段（热更性标注）、环境变量全表、haproxy.cfg 约定 |
| [04 部署指南](docs/04-部署指南.md) | 裸机 systemd / 容器 host 网络 / 内核前提 / 双网卡接线 / 验收清单 |
| [05 DockerCompose 演示](docs/05-DockerCompose演示.md) | 一键 demo：拓扑、闭环演示 SOP、loadgen 控制 |
| [06 控制台与监控](docs/06-控制台与监控.md) | 四个页面与调节视图、读数口径、告警、落盘、/metrics、安全边界 |
| [07 API 参考](docs/07-API参考.md) | 读/写 HTTP API：字段口径、鉴权、错误码、curl 示例 |
| [08 内核参数调优](docs/08-内核参数调优.md) | tune-kernel.sh：参数表、容器里哪些改不动、怎么验证生效 |
| [09 排障指南](docs/09-排障指南.md) | 按症状查：启动失败、限速没生效、振荡、采样失联、高并发 |
| [10 总控制台](docs/10-总控制台.md) | **多机总量控制**：rl-fleet 汇总数十台节点并把总流出带宽按需分配为各节点的出向目标 |
| [11 LVS 主备与 NAT 形态](docs/11-LVS主备与NAT形态.md) | **本分支核心**：NAT 流量路径与限速点、keepalived 主备部署（备机空转/漂移即生效）、已知边界 |

## 系统组成

```
rl_limiter/       # Python 3.9+ asyncio 服务（与 HAProxy 同机）
  haproxy.py      #   stats socket 只读采样    collector.py / window.py / loop.py
  cfgparse.py     #   cfg 解析 + 双文件热更    config.py / configstore.py
  ingressgov.py   #   闭环控制器（两级环）
  tcifb.py        #   入向执行面 IFB+HTB       tcingress.py（police 备选 + SYN 闸）
  tcshaper.py     #   出向执行面 HTB（单网卡形态可选）
  webconsole.py   #   控制台 + API + /metrics  metricslog.py（JSONL 落盘）
  fleet*.py       #   rl-fleet 总控：多机汇总 + 总带宽按需分配（docs/10）
tools/            # loadgen.py / random_web.py / fake_haproxy.py / tc_check.py
deploy/           # bare/（裸机装机+systemd） docker/（容器入口与模板）
                  # sysctl/tune-kernel.sh     config/limiter.example.yaml
docker-compose-demo.yml   # 演示环境（双网卡拓扑）
docker-compose-host.yml   # 生产形态样例（host 网络）
```

## 测试

```bash
make test         # = python3 -m pytest tests/（纯单元测试，不碰真实 tc/网络）
```
