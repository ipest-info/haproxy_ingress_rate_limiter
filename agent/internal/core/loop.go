// Package core 实现 rl-agent 的本地快环（设计文档 §2.1：周期 1 秒，
// 只依赖本地数据，断网也能工作）。每个 tick 按固定流水线执行：
//
//	采集（Collector.Tick）→ 决策（Governor.Tick）→ 执行（Executor.Apply）→ 上报（Sampler）
//
// 循环本身是确定性的：时间只通过注入的 tick 通道进入，配置只通过
// 注入的 configs 通道进入，因此测试可以完全控制节奏与输入。
package core

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync/atomic"
	"time"

	"github.com/ipest-info/haproxy_ingress_rate_limiter/internal/model"
)

// statusSummaryEveryTicks 控制周期状态汇总日志的输出频率：
// 每 60 个 tick（约每分钟）输出一条 info，便于在正常运行时低成本
// 确认"循环活着、配置版本正确、各环境用量正常"。
const statusSummaryEveryTicks = 60

// 下面三个本地接口镜像 internal/model 中的组件契约，收窄为循环真正
// 用到的方法集，使测试可以在不引入具体实现包的情况下伪造全部依赖。

// collectorIface 是采集器契约：维护 frontend → env 映射，每个 tick
// 产出按环境聚合的用量，并暴露采样退化（连续失败）状态。
type collectorIface interface {
	SetMapping(frontendToEnv map[string]string)
	Tick(ctx context.Context, now time.Time) []model.EnvUsage
	Degraded() bool
}

// governorIface 是决策器契约：接收配额配置更新，每个 tick 根据用量
// 运行 AIMD 算法（设计文档 §3.3）产出各环境的整形决策。
type governorIface interface {
	UpdateConfig(envs []model.EnvQuota)
	Tick(now time.Time, usages []model.EnvUsage) []model.Decision
}

// executorIface 是执行器契约：把决策写入 HAProxy（或在 dry-run 模式
// 下只记录），并可在运行期切换 dry-run / enforce 模式。
type executorIface interface {
	Apply(ctx context.Context, ds []model.Decision) error
	SetMode(mode string)
	Mode() string
}

// Components 汇集循环的全部依赖。Sampler 是可选回调：设置后每个
// tick 结束时收到本 tick 的完整结果（时间、用量、决策），供上报器
// 缓冲样本，或在独立运行模式下做调试汇总；为 nil 时跳过。
type Components struct {
	Collector collectorIface
	Governor  governorIface
	Executor  executorIface
	Sampler   func(now time.Time, usages []model.EnvUsage, decisions []model.Decision)
}

// Loop 是 agent 的快速控制环。除 version 外的字段只在 Run 所在的
// 单个 goroutine（以及 Run 之前的 Seed 调用）中访问，无需加锁；
// version 用原子量是因为上报器会从其他 goroutine 读取它。
type Loop struct {
	c       Components
	log     *slog.Logger
	version atomic.Int64 // 最近一次成功应用的 ControllerConfig 版本号
	ticks   uint64       // 已处理的 tick 累计数，仅在循环 goroutine 内递增，用于日志节流与错误定位
}

// New 构造 Loop。logger 传 nil 时回退到 slog.Default()。
func New(c Components, log *slog.Logger) *Loop {
	if log == nil {
		log = slog.Default()
	}
	return &Loop{c: c, log: log}
}

// Version 返回最近一次应用的配置版本号（尚未应用任何配置时为 0）。
// 可安全地被其他 goroutine 并发调用（上报器在心跳/样本中携带它）。
func (l *Loop) Version() int64 {
	return l.version.Load()
}

// Seed 在 Run 启动之前同步应用一份初始配置，即"引导"语义：
// 让循环从第一个 tick 起就带着配额工作，而不是空转等控制面。
// 三个使用场景（见 rl-agent main 的引导优先级）：
//   - 独立运行模式的本地静态配额；
//   - 连接控制面时的本地 envs 兜底；
//   - fail-static 缓存（设计文档 §3.7：断联期间按最后一次下发的
//     配置继续限速，绝不放开为不限速）。
//
// 注意 Seed 与 Run 内的配置应用走同一条 applyConfig 路径，语义完全
// 一致；区别仅在于 Seed 是调用方线程同步执行。
func (l *Loop) Seed(cfg model.ControllerConfig) {
	l.applyConfig(cfg)
}

// Run 驱动循环直至 ctx 结束。两个通道均由调用方注入：
//   - tick 通常是 1s 的 time.Ticker 通道，也可以是测试手工驱动的通道；
//   - configs 传递控制面推送的新配置，独立运行模式下可为 nil
//     （nil 通道永不就绪，select 会自然忽略它）。
//
// 顺序保证：某个 tick 之前已经送达的配置，一定在处理该 tick 之前
// 被应用（见 tick 分支内的嵌套 select 说明）。
func (l *Loop) Run(ctx context.Context, tick <-chan time.Time, configs <-chan model.ControllerConfig) {
	for {
		select {
		case <-ctx.Done():
			return
		case cfg, ok := <-configs:
			if !ok {
				// 通道被关闭：置 nil 使后续 select 不再命中此分支，
				// 循环退化为纯 tick 驱动，而不是空转忙读已关闭通道。
				configs = nil
				continue
			}
			l.applyConfig(cfg)
		case now := <-tick:
			// 配置优先于 tick：Go 的 select 在多分支同时就绪时随机
			// 选择，外层 select 可能在配置已到达的情况下先取到 tick。
			// 若直接执行本 tick，这一秒就会按旧配额/旧模式做决策——
			// 对"控制面刚下调配额"或"dry-run 切 enforce"这类变更，
			// 意味着多放行一秒流量。因此这里用带 default 的嵌套
			// select 把已经排队的配置全部排空后再跑流水线，保证本
			// tick 一定基于最新配置执行；default 分支确保没有待处理
			// 配置时立即继续，不会阻塞 tick。
			for configs != nil {
				select {
				case cfg, ok := <-configs:
					if !ok {
						configs = nil
						continue
					}
					l.applyConfig(cfg)
					continue
				default:
				}
				break
			}
			l.tick(ctx, now)
		}
	}
}

// applyConfig 把一份配置原子地灌入三个组件：先归一化（补默认值、
// 归一非法模式），再依次更新决策器的配额、采集器的 frontend→env
// 映射、执行器的运行模式，最后记录版本号。调用方保证串行（Seed 在
// Run 之前，Run 内单 goroutine），组件间不会看到半新半旧的配置。
func (l *Loop) applyConfig(cfg model.ControllerConfig) {
	cfg.Normalize()
	l.c.Governor.UpdateConfig(cfg.Envs)
	l.c.Collector.SetMapping(cfg.FrontendToEnv())
	l.c.Executor.SetMode(cfg.Mode)
	l.version.Store(cfg.Version)
	l.log.Info("config applied",
		"version", cfg.Version,
		"mode", cfg.Mode,
		"envs", len(cfg.Envs),
		"env_quotas", summarizeQuotas(cfg.Envs))
}

// tick 执行一次完整的快环流水线：采集 → 决策 → 执行 → 上报。
func (l *Loop) tick(ctx context.Context, now time.Time) {
	l.ticks++
	usages := l.c.Collector.Tick(ctx, now)
	ds := l.c.Governor.Tick(now, usages)
	// 执行失败绝不能中断循环：HAProxy 可能正在 reload（设计文档
	// §3.7），socket 短暂不可用是预期内故障；下一个 tick 会带着新
	// 决策自然重试，残留在 HAProxy 上的旧整形值维持原样（安全方向）。
	if err := l.c.Executor.Apply(ctx, ds); err != nil {
		l.log.Error("executor apply failed",
			"err", err,
			"tick", l.ticks,
			"degraded", l.c.Collector.Degraded())
	}
	// 周期状态汇总：每 statusSummaryEveryTicks 个 tick 输出一次，
	// 正常运行时以约 1 条/分钟的成本留下可核对的运行痕迹。
	if l.ticks%statusSummaryEveryTicks == 0 {
		l.log.Info("status summary",
			"tick", l.ticks,
			"config_version", l.version.Load(),
			"mode", l.c.Executor.Mode(),
			"envs", len(usages),
			"degraded", l.c.Collector.Degraded(),
			"envs_summary", summarizeUsages(usages))
	}
	if l.c.Sampler != nil {
		l.c.Sampler(now, usages, ds)
	}
}

// summarizeQuotas 把环境配额压缩成单个日志字段，格式：
// "env1=200000000;env2=100000000"，数值为配置口径的 bits/s。
func summarizeQuotas(envs []model.EnvQuota) string {
	var b strings.Builder
	for i, e := range envs {
		if i > 0 {
			b.WriteByte(';')
		}
		fmt.Fprintf(&b, "%s=%d", e.EnvID, e.QuotaBitsPerSec)
	}
	return b.String()
}

// summarizeUsages 把各环境用量压缩成单个日志字段，格式：
// "env1:mean10_bytes_per_s=12345,conn=6;env2:..."，mean10 为计费口径
// 的 10 秒滑动均值（bytes/s），conn 为当前并发连接数。
func summarizeUsages(usages []model.EnvUsage) string {
	var b strings.Builder
	for i, u := range usages {
		if i > 0 {
			b.WriteByte(';')
		}
		fmt.Fprintf(&b, "%s:mean10_bytes_per_s=%.0f,conn=%d", u.EnvID, u.Mean10Bps, u.ConnCur)
	}
	return b.String()
}
