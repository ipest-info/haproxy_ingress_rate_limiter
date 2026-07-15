# haproxy_ingress_rate_limiter

带宽账密计费模式下的入口动态限速系统（**v2.0 集中式，Python 实现**）：
rl-limiter 服务独立部署，通过内网 TCP 同时控制多台 HAProxy，保证每个环境
（env）的**下行带宽**（代理返回给客户端的流量，跨节点全局聚合）不超过订单
约定带宽。

## 文档

| 文档 | 说明 |
| ---- | ---- |
| [docs/00-需求说明.md](docs/00-需求说明.md) | 原始需求（名词定义、架构图、核心诉求） |
| [docs/01-方案设计.md](docs/01-方案设计.md) | 总体方案设计（v2.0 集中式架构、AIMD 算法、加权分配、数据模型、接口、容错） |
| [docs/02-建议与讨论点.md](docs/02-建议与讨论点.md) | 需求改进建议与决策清单（5 项已拍板、2 项待定） |
| [docs/03-限速服务运行指南.md](docs/03-限速服务运行指南.md) | 安装、本地演示、配置参考、HAProxy 侧前置条件 |

## 系统组成

```
rl_limiter/       # Python 3.11 + asyncio 集中限速服务
  model.py        #   共享领域类型（单位约定、Target/EnvQuota/Decision 等）
  haproxy.py      #   HAProxy runtime API 客户端（内网 TCP stats socket）
  window.py       #   滑动窗口 + EWMA
  collector.py    #   多节点并发采样、按环境全局聚合、节点级容错
  governor.py     #   per-env AIMD 状态机（全局快环）
  allocator.py    #   整形值按挂载点用量加权分配
  executor.py     #   dry-run/enforce 执行、pending 重试、resync
  reporter.py     #   管理后台长轮询/上报/心跳、fail-static 缓存
  config.py       #   本地 YAML 配置（节点连接信息 + 引导配额）
  loop.py         #   1s 主循环
tools/            # fake_haproxy.py（联调假节点）、mock_backend.py（后台桩）
deploy/           # systemd、haproxy 2.8 配置片段、tc 兜底脚本、示例配置
```

## 核心思路一句话

rl-limiter 每秒并发采样所有 HAProxy 节点各 frontend 的 `bytes_out`，把同一
环境跨节点的流量全局聚合成 **10 秒滑动均值**，与配额比较后做 AIMD 调整
（弹性上限 110%、急收慢放、不拒绝新建连接、不断开存量连接），再把环境聚合
整形值按各挂载点近 60s 用量**加权拆分**，经 HAProxy 原生 `bwlim-out` +
runtime API map 写回各节点完成聚合整形；上游流量靠 TCP 背压自然收敛，tc
在各节点作硬兜底；服务与后台断联或自身宕机时全链路 fail-static，绝不放开
限速。

## 快速开始

```bash
make install    # pip install -e ".[test]"
make test       # 全量单元测试
# 本地三步演示（假 HAProxy ×2 + 后台桩 + dry-run 服务）见 docs/03
```
