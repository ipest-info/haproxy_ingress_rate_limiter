# rl-limiter

**带宽按量计费场景下的服务器出口限速与监控系统。** 每台服务器装一个节点
（rl-limiter），把本机出向带宽稳稳压在目标内；多台服务器再加一个总控
（rl-fleet），把"所有服务器出向合计"控制在一个总带宽内，并按各台的实际
需求动态分配。

- **与业务零耦合**：不依赖机器上跑的服务（nginx、HAProxy、自研 TCP 服务，
  或什么都不跑），监控和限速全部基于内核网卡计数器与 tc；
- **与账单同口径**：限的就是网卡流出字节——按量计费计的那个数；
- **对客户端温和**：出向超标时平缓收紧客户端侧网卡的**入向**带宽（请求进得
  慢，响应自然出得少），响应路径不排队；调整刻意平缓，不振荡；
- **开箱即用**：一条命令装好节点和总控，节点装完打印一行接入串，粘贴到
  总控即完成登记；
- **可集成**：总控开放 API（按范围授权的密钥、审计）、OpenAPI 描述、
  页面可直接 iframe 嵌入。

---

## 它解决什么问题

| 场景 | 用法 |
| ---- | ---- |
| 单台服务器按流量计费，要保证出向不超过某个带宽 | 节点上设"实例总限速"（Mbps），闭环自动把出向压在目标附近 |
| 一批服务器共享一个总带宽包 | 总控设"总流出带宽"，按需分给各台：忙的多分、闲的少分，合计守恒 |
| 某几台要给固定额度（重点客户、专线） | 总控给节点设"固定限速"，照值执行、计入总量、不参与按需分配 |
| 计费/运维系统要自动调带宽 | 第三方用 API 密钥调总控 API 改总带宽或节点限速，每次操作留审计 |
| 运维门户要展示流量 | iframe 嵌入总控页面（有只读精简版），或拉 Prometheus `/metrics` |
| 事后对账、追查"什么时候谁改了限速" | 分钟粒度监控落盘 + 变更/审计事件，页面「历史回查」或 API 查询 |

## 工作原理

```
                       ┌──────────── rl-fleet 总控（可选，一台管数十台）────────────┐
                       │ 每 2s 轮询各节点流量 → 汇总展示                            │
                       │ 每 15s 把"总流出带宽"按需分配 → 下发各节点的出向目标       │
                       └──────┬─────────────────────┬─────────────────────┬────────┘
                              ▼                     ▼                     ▼
 ┌──────────────────────────── 节点（每台服务器一个 rl-limiter）──────────────────────┐
 │                                                                                  │
 │  客户端 ──▶ eth0（客户端侧）──▶ 业务进程（任意，rl-limiter 不感知）──▶ eth1（后端侧）│
 │               ▲                                                                  │
 │               │ 入向总闸（tc：ingress → IFB → HTB 平稳整形）                       │
 │               │                                                                  │
 │   闭环控制器：每秒读网卡计数器，出向 > 目标 → 小步收紧入向；出向回落 → 逐步放开      │
 │                                                                                  │
 │   Web 控制台 / HTTP API / Prometheus /metrics / 分钟粒度 JSONL 落盘                │
 └──────────────────────────────────────────────────────────────────────────────────┘
```

- **节点（快速内环）**：观测客户端侧网卡的流出字节（10 秒均值），超过目标
  时按"死区 + 乘性小步 + 按实测流入落位"调节入向总闸。限速规则在内核 tc
  里，rl-limiter 进程只做采样、决策与下发——它短暂退出不影响已生效的限速。
  另可按端口登记静态的入向限速（带宽 + 每秒新建连接数）。
- **总控（慢速外环）**：只是节点 API 的客户端，节点侧零改动。可控节点
  "保底 + 按需比例"切分总带宽；离线/失联节点的配额冻结不动、如实记账；
  被自己配额压着的节点按需求放大，避免"配额小→观测小→分得更小"的饿死；
  合计恒等于总带宽。取消总量控制会立即解除各按需节点的限速（固定限速的
  节点不受影响）。

原理与参数：[02 闭环限速原理](docs/02-闭环限速原理.md)、
[10 总控制台](docs/10-总控制台.md) §3。

## 功能一览

**节点 rl-limiter**

- 实例出向总目标（闭环动态入向限速，默认 IFB + HTB；老内核可选 police）；
- 端口级静态入向限速：带宽 + SYN 速率闸（每秒新建连接）；
- 可选出向 HTB 直接整形（单网卡机器的基线形态）；
- 内核口径监控：逐网卡带宽/包/丢包，整机并发连接、每秒新建/拒绝连接；
- Web 控制台：实时曲线、调节视图、设备视图、tc 实况、配置页（在线改限速）；
- Prometheus `/metrics`；分钟粒度 JSONL 落盘（按天轮转，默认保留 90 天）；
- 配置只有一份本地 YAML，页面/API 的修改原子回写、数秒内热生效。

**总控 rl-fleet**

- 汇总所有节点流量（网卡口径），总流出带宽按需动态分配；
- 节点固定限速（照值下发、计入总量、与总量控制无关）；
- 节点管理：手工增删改，或**粘贴接入串**批量登记（先预览再导入）；
- 历史回查：总带宽、各节点出入向与配额的分钟数据，限速变更与 API 审计事件，
  支持按时间/节点筛选和导出 JSONL；
- 开放 API：管理员令牌 + 按范围（`limits` / `nodes`）授权、可吊销的 API 密钥，
  每次写操作记审计；
- 页面不限制 iframe 嵌入，`?embed=1` 只读精简页；CORS 白名单；可选 HTTPS。

## 快速开始

### 生产安装（二进制，Linux x86_64，glibc ≥ 2.17）

```bash
curl -fL -o install.sh https://github.com/ipest-info/haproxy_ingress_rate_limiter/releases/latest/download/install.sh

# 1) 每台服务器装节点（必须双网卡：客户端侧 + 后端侧，两者不能是同一张）
sudo bash install.sh --download --token auto --client-nic eth0 --backend-nic eth1
sudo systemctl enable --now rl-limiter
#    → 装完最后一行打印接入串：rl-node name=… url=http://10.0.1.21:8090 token=…

# 2) 装一台总控（不要求双网卡）
sudo bash install.sh --download --fleet --token auto
sudo systemctl enable --now rl-fleet
#    → 打开 http://<总控>:8095，页头填总控令牌，「节点管理」→「粘贴接入串」
#      （多台的接入串可以攒起来一次粘贴）→ 总览里设总流出带宽
```

- 节点：`http://<节点>:8090`；总控：`http://<总控>:8095`；
- `--token auto` 生成 256 bit 随机令牌。令牌必须是高强度随机串，弱令牌
  一律当作没有令牌（写接口禁用），安装脚本直接拒装；
- 升级 = 重跑 install.sh（二进制覆盖、配置不动）+ `systemctl restart`；
  卸载 `install.sh --uninstall [--fleet] [--purge]`（会清掉网卡上的 tc 规则）；
- 其它装法：venv 裸机脚本 `deploy/bare/rl-limiter.sh install`，或容器 host
  网络 `docker compose -f docker-compose-host.yml up -d --build`——见
  [04 部署指南](docs/04-部署指南.md)、[11 发布与安装](docs/11-发布与安装.md)。

### 本地演示（Docker）

```bash
make demo-up      # 两台节点 + 可调并发压测 + 总控，双网卡拓扑
# 节点控制台 http://127.0.0.1:8090，总控 http://127.0.0.1:8095（演示令牌见 docs/05）
# 配置页设"实例总限速"=40 → 压测加到 1000 并发 → 看"调节视图"收敛
make demo-down
```

需要 Linux 宿主机（内核要有 ifb / HTB）；Docker Desktop（Mac/Win）的
虚拟机内核通常没有 ifb，见 [05 DockerCompose 演示](docs/05-DockerCompose演示.md)。

## 第三方集成

```bash
F=https://fleet.example.com:8095
K='<API 密钥>'      # 管理员在总控「开放 API」页签创建，令牌只显示一次

curl -s -X PUT $F/api/total-quota       -H "X-API-Token: $K" -d '{"quota_mbps": 1000}'  # 总带宽
curl -s -X PUT $F/api/nodes/sh-01/limit -H "X-API-Token: $K" -d '{"mbps": 50}'          # 节点固定限速
curl -s       $F/api/overview                                                           # 总览（读接口无鉴权）
```

```html
<iframe src="https://fleet.example.com:8095/?embed=1&tab=overview"
        style="width:100%;height:640px;border:0"></iframe>
```

- 机器可读描述：节点和总控都提供 `GET /api/openapi.json`（OpenAPI 3.0，可
  导入 Postman / Apifox）；
- 全部接口的参数、响应、错误码，以及安装脚本命令行、数据格式、配置项：
  **[13 接口文档](docs/13-接口文档.md)**；集成指南：
  [12 总控开放 API](docs/12-总控开放API.md)。

## 设计原则

1. **配置只有一份本地 YAML，没有数据库。** 页面和 API 的修改经整份校验后
   原子回写，与手工编辑殊途同归；
2. **宁可起不来，也不要"以为在限速"。** 一切会让限速静默失效的条件（读不到
   内核计数器、建不出 IFB、网卡接错、证书只配了一半……）都在启动时大声失败；
3. **fail-static。** 配置短暂不可读时按上次合法配置运行；采样失联时冻结
   决策；总控绝不乱动失联节点的配额；
4. **令牌只认高强度。** 节点令牌、总控管理员令牌、登记在总控里的节点令牌
   都要过同一套强度规则，不达标即视为未配置；令牌的值从不写进日志、
   落盘或读接口；
5. **单位只在边界换算。** 配置与页面 Mbps，API 速率字段 bytes/s，tc 内部 bit/s。

## 部署须知

- **权限**：节点需要 `CAP_NET_ADMIN`（systemd 单元已配置，以专用用户
  `rl-limiter` 运行）；总控不碰 tc，无需特权；
- **内核**：`sch_htb`、`ifb` + `act_mirred`；端口级新建连接闸另需
  `cls_flower` + `act_police`；
- **网络边界**：读接口（概览、历史、日志）**无鉴权**，控制台端口必须由
  防火墙/安全组限制来源，不要暴露公网；总控 ↔ 节点建议走后端/管理侧网络；
- **已知边界**：动态闸值不落盘，进程重启后几个调节周期内重新收敛；SYN 速率闸
  路径尚未经真机验证。详见 [02](docs/02-闭环限速原理.md) §6、
  [09 排障指南](docs/09-排障指南.md)。

## 文档

| 文档 | 说明 |
| ---- | ---- |
| [01 总体设计](docs/01-总体设计.md) | 定位与设计原则、组件与数据流、配置分工 |
| [02 闭环限速原理](docs/02-闭环限速原理.md) | 为什么限入向、IFB+HTB 执行面、闭环算法与参数、收敛实测、已知边界 |
| [03 配置参考](docs/03-配置参考.md) | YAML 全字段（热更性标注）、环境变量全表 |
| [04 部署指南](docs/04-部署指南.md) | 二进制/裸机/容器 host 网络、内核前提、双网卡接线、验收清单 |
| [05 DockerCompose 演示](docs/05-DockerCompose演示.md) | 一键 demo：拓扑、闭环演示步骤、压测控制 |
| [06 控制台与监控](docs/06-控制台与监控.md) | 页面与调节视图、读数口径、落盘、/metrics、安全边界 |
| [07 API 参考](docs/07-API参考.md) | 节点 API 的字段口径与集成思路 |
| [08 内核参数调优](docs/08-内核参数调优.md) | tune-kernel.sh：参数表、容器里改不动的、怎么验证 |
| [09 排障指南](docs/09-排障指南.md) | 按症状查：启动失败、限速没生效、振荡、高并发 |
| [10 总控制台](docs/10-总控制台.md) | 多机总量控制：汇总、按需分配算法、节点管理、历史回查 |
| [11 发布与安装](docs/11-发布与安装.md) | 发布包命名与 CI、install.sh（令牌/网卡/接入串/卸载）、令牌强度规则 |
| [12 总控开放 API](docs/12-总控开放API.md) | 第三方集成：API 密钥与范围、节点固定限速、审计、iframe 嵌入、CORS、HTTPS |
| [13 接口文档](docs/13-接口文档.md) | 标准接口文档：安装脚本命令行、节点与总控全部 HTTP 接口、数据格式、Prometheus 指标、配置项 |

## 仓库结构

```
rl_limiter/            Python 3.9+ asyncio（aiohttp）服务
  __main__.py          节点入口：环境变量接线、启动校验
  collector.py …       采样：netdev.py（/proc/net/dev）、netsrc.py（/proc/net/snmp）、window.py、loop.py
  config.py …          配置：cfgparse.py（热更轮询）、configstore.py（原子回写）
  ingressgov.py        闭环控制器
  tcifb.py …           tc 执行面：tcingress.py（police + SYN 闸）、tcshaper.py（出向 HTB）
  webconsole.py        节点控制台、HTTP API、/metrics；nodeopenapi.py；metricslog.py（落盘）
  fleet*.py            总控：fleetmain / fleetpoll（轮询下发）/ fleetgov（分配）/ fleetweb /
                       fleetconfig / fleetmetrics / fleetopenapi
  tokenpolicy.py       令牌强度规则（节点与总控共用）
  static/              控制台页面
deploy/
  release/             二进制构建（PyInstaller）与 install.sh
  bare/                venv 裸机安装脚本与 systemd 模板
  config/              带全量注释的配置模板（节点/总控 YAML 与 env）
  docker/  sysctl/     容器入口与模板、内核参数调优脚本
tools/                 loadgen.py（压测）、random_web.py（模拟服务）、tc_check.py（限速体检）
tests/                 单元与集成测试（不碰真实 tc/网络）
docs/                  01–13 文档
```

## 开发与测试

```bash
make install      # pip install -e ".[test]"
make test         # python3 -m pytest -q
make release-build  # 本地构建单文件二进制发布包（正式发布走 CI，见 docs/11）
```

发布：GitHub Actions `release` 工作流按 tag（`v<主>.<次>.<补丁>`）构建
`rl-limiter-<tag>-linux-x86_64.tar.gz`（含 rl-limiter、rl-fleet 两个二进制与
install.sh）及 sha256，`install.sh` 也单独作为 Release 附件。各版本变化见
[13 接口文档](docs/13-接口文档.md) §8 与 GitHub Releases。
