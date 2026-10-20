# 11 LVS 主备与 NAT 形态

本分支把受管对象从"本机 HAProxy"换成"本机 LVS（IPVS）director"。
本篇讲清三件事：NAT 流量路径与限速为什么成立、主备（keepalived VRRP）
怎么部署、已知边界。

## 1. NAT 形态的流量路径与限速点

```
客户端 ──► VIP:vport（客户端侧网卡）
             │ ① ingress（PREROUTING 之前）：入向闭环的挂点（可选）
             │ ② IPVS DNAT → 改写目的为 RS:rport，经后端侧网卡转发
             ▼
           RealServer ──回包──► director（RS 的默认网关必须指回来）
             │ ③ IPVS un-NAT → 源地址改回 VIP:vport
             │ ④ egress qdisc（客户端侧网卡）：出向 HTB 的挂点
             ▼
           客户端
```

两个关键结论（代码注释与测试都钉了）：

- **出向 HTB 按 sport=vport 分类成立**：un-NAT 在 netfilter
  （LOCAL_OUT/FORWARD）完成，egress qdisc 在设备出队前、晚于
  netfilter——tc 看到的出向包源地址**已经是 VIP:vport**，与 HAProxy
  形态逐包等价。自证：`tcpdump -i <客户端侧网卡> -nn src port <vport>`；
- **入向闭环同样成立**：ingress qdisc 先于 IPVS 的 DNAT，按
  dst=VIP:vport 匹配请求流不受 NAT 影响（闭环是可选能力，LVS NAT 的
  主路径是出向直接限速）。

FULLNAT 差异：director→RS 换成 localip 池，但**发往客户端方向的源仍
是 VIP:vport**——出向分类与观测不受影响，本方案同样成立。

DR/TUN 模式**不适用本分支**：响应由 RS 直接回客户端、不经 director，
出向既测不到也限不到——那是另一个形态的问题，别把本分支部署上去。

## 2. 受管清单 = 内核 IPVS 实况表

对应基线里 haproxy.cfg 的角色，但不解析任何配置文件：

- rl-limiter 只读 `/proc/net/ip_vs`（服务清单 + 连接数）和
  `ipvsadm -Ln --stats --exact`（按服务字节数），轮询热生效——
  keepalived 增删 virtual_server、手工 ipvsadm 操作都自动跟上；
- 每个虚拟服务的合成名 `vs_<VIP>_<端口>`（如 `vs_10.0.0.1_8080`）
  就是 quotas / 写 API 的登记键；
- ip_vs 模块未加载 → **启动失败**并指引 `modprobe ip_vs ip_vs_rr`
  （与"读不到 haproxy.cfg 不起"同一铁律：宁可起不来，也不要以为在
  限速）。

## 3. 主备部署（keepalived VRRP）——"一台没流量、一台流量很高"怎么限

答案：**两台都部署 rl-limiter、同一份 YAML，什么都不用联动。**

原理四条（设计依据，也是排障时的预期行为）：

1. **主备的受管清单天然一致**：keepalived 的 virtual_server（IPVS
   表）在主备**都**下发，VRRP 只漂 VIP。备机上 `ipvsadm -Ln` 看到的
   服务与主机相同，rl-limiter 的清单与 tc 规则照常建好；
2. **备机规则空转、漂移即生效**：备机没有 VIP 流量，HTB 类匹配不到
   包（零开销）；VIP 漂过来的那一刻规则早已就位，**限速零窗口期接管**
   ——不存在"切换后要等限速重建"的空档；
3. **动态值重收敛**：入向闭环（如果启用）的动态上限在备机会因出向≈0
   爬回全开；漂移后头几个调节周期内可能短暂超目标、随后按正常步长
   收敛。**静态出向 HTB 不存在这个过渡**——漂移即精确，这也是 NAT
   形态推荐出向直接限速的原因之一；
4. **总控登记两台**：fleet.yaml 把主备都写进 nodes。按需分配天然适配
   不对称流量——备机需求≈0 只占保底份额，漂移后需求转移、配额在一两
   个分配周期内跟过去；双活（两台各挂不同 VIP）同样成立。

部署清单（每台 director 相同）：

```
modprobe ip_vs ip_vs_rr                  # 或写进 /etc/modules-load.d/
keepalived（主备 priority 不同，virtual_server 段相同）
rl-limiter -c /etc/rl-limiter/config.yaml   # 同一份 YAML
环境变量：RL_TC_IFACE=<客户端侧物理网卡>（出向 HTB）
```

demo 的漂移演示：`docker compose stop node1` → 看 loadgen 流量与
node2 控制台曲线无缝接上、总控节点表里 node1 变离线（配额冻结记账）。

## 4. 已知边界（v1）

- **仅 IPv4**：/proc 里的 IPv6 服务行被跳过（只监控不了也不限速）；
- **同端口多 VIP**：tc 按端口分类（classid 次要号=端口），两个 VIP
  共用 80 端口时执行面无法区分——名序第一个服务参与限速，其余被强制
  只监控（启动 error 点名）。**监控不受影响**（IPVS 按服务分开统计）。
  出路：错开端口，或等后续版本的"VIP+端口"分类；
- **FWM（防火墙标记）/SCTP 服务**：跳过不管理；
- **conn_rate（SYN 速率闸）与入向闭环**是从 HAProxy 分支继承的可选
  能力，在 LVS NAT 下机制成立但未经真机验证——首次启用时按
  docs/09 §7 的办法核对；
- **裸机装机脚本（deploy/bare）尚未适配 LVS**：它仍按 HAProxy 形态
  检查环境。LVS 生产部署暂用 docker-compose-host.yml 的容器形态，或
  手工照 §3 清单装（rl-limiter 本体只需 venv + 环境变量 + systemd）。

## 5. 排障速查

| 症状 | 原因与处置 |
| ---- | ---- |
| 启动失败：/proc/net/ip_vs 不存在 | 宿主机 `modprobe ip_vs ip_vs_rr`；容器里 modprobe 不了 |
| ipvsadm 执行失败/超时 | 容器缺 CAP_NET_ADMIN；或 ipvsadm 没装；超时看机器负载，可调 lvs.timeout_ms |
| 页面有服务但限速没生效 | 看启动日志有没有"同端口有多个虚拟服务"的 error（v1 边界）；再看 tc 实况面板 |
| 备机页面带宽全为 0 | 正常——备机没有 VIP 流量；看"在线/清单/配额"即可 |
| 漂移后短暂超目标 | 闭环动态值重收敛的预期行为（§3.3），几个调节周期内恢复；纯出向 HTB 无此现象 |
| NAT 不通（请求有去无回） | RS 的默认网关必须指向 director（demo 里是后端侧 VIP）；`ip_forward`/`vs.conntrack` 必须为 1（入口脚本已自检） |
