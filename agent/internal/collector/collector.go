// Package collector 是 Agent 快环的"输入级"：每秒对 HAProxy 的 frontend
// 统计做一次采样，把 bytes_out 累计计数差分成每秒速率，并按 frontend→env
// 映射聚合成各环境的用量样本，供 governor（快环限速决策）与 reporter
// （上报慢环）消费。
//
// 采集口径遵循设计文档 §3.1：以 frontend 的 bytes_out（HAProxy 发回客户端
// 的应用层字节数）为准，而非网卡计数——口径与计费一致，且天然按 frontend
// 拆分以支持一台 HAProxy 服务多个环境。在每秒瞬时速率之上，本包维护两条
// 平滑曲线：10 秒滑动窗口均值（承诺口径，快环决策输入）与 60 秒 EWMA
// （慢环配额分配输入），瞬时毛刺不会直接触发任何限速动作。
//
// 容错行为遵循设计文档 §3.7：采样失败或计数差分为负（计数回绕）的那一秒，
// 沿用上一秒的速率值继续喂给窗口与 EWMA；连续 10 秒采样失败则把自身标记为
// degraded，供上层告警并保持当前整形值不动（fail-static，绝不因数据缺失而
// 放开限速）。
package collector

import (
	"context"
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/ipest-info/haproxy_ingress_rate_limiter/internal/model"
)

const (
	// window10Size 是滑动窗口容量（单位：秒）。10 秒滑动均值是已拍板的承诺
	// 口径——"10 秒均值 ≤ 约定带宽，瞬时容忍至 110%"（设计文档 §3.1/§3.3），
	// 因此该值与业务承诺绑定，不是随意可调的平滑参数。
	window10Size = 10
	// ewma60Alpha 按经典 span 公式 α = 2/(N+1) 取 N=60，在 1 秒 tick 节奏下
	// 近似 60 秒 EWMA。EWMA 相比再开一个 60 格窗口只需 O(1) 状态，且对慢环
	// 的配额分配来说"平滑趋势"比"精确窗口语义"更重要。
	ewma60Alpha = 2.0 / (60 + 1)
	// degradedFailureThreshold 是触发降级的连续 ShowStat 失败次数（§3.7：
	// "连续 10s 失败则告警并保持当前整形值不动"）。达到阈值只是打标记与
	// 告警，不清空任何状态——恢复后差分基线仍然可用。
	degradedFailureThreshold = 10
	// absentTickLimit 限制"frontend 从采样结果中消失后其计数基线还保留多少
	// 个成功 tick"。保留一段时间是为了容忍 HAProxy reload 等短暂消失场景
	// （回来后差分依旧连续）；但不能无限保留，否则被真正下线的 frontend
	// 会造成状态泄漏。60 个成功 tick（约 1 分钟）后基线被淘汰，此后若同名
	// frontend 再出现则按首次采样重新建基线。
	absentTickLimit = 60
)

// frontendState 是按 frontend 维护的计数差分基线。它独立于 mapping 存在：
// 即使某个 frontend 当前未映射到任何 env，其基线也持续刷新（见 tickOK），
// 这样 SetMapping 换新映射后，下一个 tick 的差分依然连续，不会因为"刚被
// 映射进来"而出现一次虚高或缺失的速率。
type frontendState struct {
	lastBytesOut uint64
	lastRate     float64 // 上一秒速率（bytes/s）；计数回绕的那一秒沿用该值（§3.7）
	absentTicks  int     // 连续多少个成功 tick 未在采样结果中见到该 frontend
}

// envState 是按环境维护的聚合状态：滑动窗口、EWMA、上一秒速率与上一次
// 输出的完整用量样本（采样失败时以它为底稿继续输出，见 tickFailed）。
type envState struct {
	window    *SlidingWindow
	ewma      *EWMA
	lastRate  float64
	lastUsage model.EnvUsage
}

// Collector 把原始 frontend 统计转换成按环境聚合的用量样本。
//
// 并发约定：Tick 必须由单一 goroutine（Agent 核心循环）调用，Tick 私有的
// 状态（failures/frontends/envs/loggedUnmapped）因此无需加锁；SetMapping 与
// Degraded 可由其他 goroutine 并发调用，mapping 与 degraded 由 mu 保护。
type Collector struct {
	src model.StatSource
	log *slog.Logger

	mu       sync.Mutex        // 保护 mapping 与 degraded
	mapping  map[string]string // frontend 名 -> env id，由 Controller 下发
	degraded bool

	// 以下状态仅由 Tick goroutine 独占访问，不加锁。
	failures       int
	frontends      map[string]*frontendState
	envs           map[string]*envState
	loggedUnmapped map[string]bool // 未映射 frontend 只告警一次，防止每秒刷屏
}

// New 基于给定的统计源构造 Collector。logger 为 nil 时回退到 slog.Default()，
// 保证内部日志调用永不判空。
func New(src model.StatSource, log *slog.Logger) *Collector {
	if log == nil {
		log = slog.Default()
	}
	return &Collector{
		src:            src,
		log:            log,
		frontends:      make(map[string]*frontendState),
		envs:           make(map[string]*envState),
		loggedUnmapped: make(map[string]bool),
	}
}

// SetMapping 整体替换 frontend→env 映射（Controller 配置下发时调用）。
// 输入 map 被拷贝一份，调用方之后可以继续改动自己的副本；Tick 侧读取时
// 也因此只需拿到 map 引用即可，无需在持锁期间遍历。
func (c *Collector) SetMapping(frontendToEnv map[string]string) {
	m := make(map[string]string, len(frontendToEnv))
	for fe, env := range frontendToEnv {
		m[fe] = env
	}
	c.mu.Lock()
	c.mapping = m
	c.mu.Unlock()
	c.log.Debug("frontend mapping replaced", "frontend_count", len(m))
}

// Degraded 报告采样是否已连续失败达到 degradedFailureThreshold 次（§3.7）。
// 上层据此告警并冻结整形值（fail-static）。
func (c *Collector) Degraded() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.degraded
}

// Tick 对统计源采样一次，返回按 EnvID 排序的各环境用量。now 由调用方注入
// 以便测试确定性；核心循环以固定 1 秒节奏调用 Tick，因此计数差分本身就是
// bytes/s 速率，无需再除以真实时间间隔。
func (c *Collector) Tick(ctx context.Context, now time.Time) []model.EnvUsage {
	_ = now // 速率计算依赖固定 1s 节奏而非墙钟差值，now 仅为将来扩展保留

	c.mu.Lock()
	mapping := c.mapping // SetMapping 总是整体替换 map，解锁后继续读该引用是安全的
	c.mu.Unlock()

	stats, err := c.src.ShowStat(ctx)
	if err != nil {
		return c.tickFailed(err)
	}
	return c.tickOK(mapping, stats)
}

// tickFailed 处理采样失败的 tick（§3.7）：对每个已知 env 沿用上一秒的瞬时
// 速率喂给窗口与 EWMA，让 mean10/ewma60 在陈旧数据上继续推进，而不是留下
// 空洞或骤降为零——速率归零会诱导快环误放松限速，方向上不安全。
// 连续失败计数达到阈值时打 degraded 标记并升级为 error 日志。
func (c *Collector) tickFailed(err error) []model.EnvUsage {
	c.failures++
	crossed := c.failures == degradedFailureThreshold // 恰好越线的那一次才发 error，避免每秒重复告警
	degraded := c.failures >= degradedFailureThreshold

	c.mu.Lock()
	c.degraded = degraded
	c.mu.Unlock()

	c.log.Warn("stats sample failed; holding last rates",
		"err", err,
		"consecutive_failures", c.failures,
		"degraded_threshold", degradedFailureThreshold,
		"degraded", degraded,
		"env_count", len(c.envs))
	if crossed {
		c.log.Error("collector degraded: consecutive stats sample failures reached threshold",
			"threshold", degradedFailureThreshold,
			"consecutive_failures", c.failures)
	}

	usages := make([]model.EnvUsage, 0, len(c.envs))
	for envID, st := range c.envs {
		st.window.Push(st.lastRate)
		st.ewma.Update(st.lastRate)
		u := st.lastUsage // 以上一次输出为底稿：ConnCur 等无法重采的字段保持不变
		u.EnvID = envID
		u.RateBps = st.lastRate
		u.Mean10Bps = st.window.Mean()
		u.Ewma60Bps = st.ewma.Value()
		u.Degraded = degraded
		st.lastUsage = u
		usages = append(usages, u)
	}
	sortUsages(usages)
	return usages
}

// tickOK 处理采样成功的 tick：差分各 frontend 的 bytes_out、按 mapping 聚合
// 到 env、维护窗口/EWMA，并做基线与聚合状态的生命周期管理。
func (c *Collector) tickOK(mapping map[string]string, stats []model.FrontendStat) []model.EnvUsage {
	// 任何一次成功采样都会清零失败计数并解除降级——降级只反映"当下是否
	// 连续采不到数据"，不做粘滞。
	if c.failures > 0 {
		prevFailures := c.failures
		wasDegraded := c.failures >= degradedFailureThreshold
		c.failures = 0
		c.mu.Lock()
		c.degraded = false
		c.mu.Unlock()
		if wasDegraded {
			c.log.Info("stats sampling recovered",
				"previous_consecutive_failures", prevFailures,
				"degraded_threshold", degradedFailureThreshold)
		} else {
			c.log.Debug("stats sampling recovered before degradation",
				"previous_consecutive_failures", prevFailures)
		}
	}

	type agg struct {
		rate float64
		conn int64
		// measured：至少一个已映射 frontend 贡献了基于差分的速率。
		// baselined：至少一个已映射 frontend 本 tick 是首次采样（只建基线）。
		// 仅建基线的 tick（baselined && !measured）速率是"未知"而非零：
		// Agent 重启后若把 0 喂进窗口/EWMA，mean10 会被压低约 10 秒、慢环的
		// EWMA 输入被压低数十秒，导致该收紧时收紧变慢——所以未知就跳过，
		// 让窗口只吃真实测量值（见下方 push 处的判断）。
		measured  bool
		baselined bool
	}
	// mapping 中引用到的每个 env 都会被输出，哪怕本 tick 没有任何存活的
	// frontend——下游（governor/reporter）因此看到稳定的 env 集合，无需
	// 处理"env 忽隐忽现"的情况。
	sums := make(map[string]*agg, len(mapping))
	for _, envID := range mapping {
		if _, ok := sums[envID]; !ok {
			sums[envID] = &agg{}
		}
	}

	present := make(map[string]bool, len(stats))
	for _, st := range stats {
		present[st.Name] = true
		envID, mapped := mapping[st.Name]
		fs, known := c.frontends[st.Name]
		if !known {
			if !mapped {
				// 未映射且从未见过的 frontend：不建基线（省状态），只在
				// 首次出现时记一条 debug，避免每秒刷同样的日志。
				if !c.loggedUnmapped[st.Name] {
					c.loggedUnmapped[st.Name] = true
					c.log.Debug("ignoring unmapped frontend",
						"frontend", st.Name, "bytes_out", st.BytesOut, "conn_cur", st.ConnCur)
				}
				continue
			}
			// 首次采样：只有一个累计值、没有前值可差分，速率未知，本 tick
			// 仅记录基线。连接数是瞬时值不依赖差分，可直接计入。
			c.frontends[st.Name] = &frontendState{lastBytesOut: st.BytesOut}
			sums[envID].conn += st.ConnCur
			sums[envID].baselined = true
			c.log.Info("baselined new frontend",
				"frontend", st.Name, "env", envID,
				"bytes_out", st.BytesOut, "conn_cur", st.ConnCur)
			continue
		}
		fs.absentTicks = 0
		var rate float64
		if st.BytesOut >= fs.lastBytesOut {
			rate = float64(st.BytesOut - fs.lastBytesOut)
		} else {
			// 计数回绕（典型场景：HAProxy reload 后计数从零重来）：差分为
			// 负不可用，按 §3.7 沿用上一秒速率顶过这一秒，同时用新累计值
			// 重建基线，下一秒差分即恢复正常。
			rate = fs.lastRate
			c.log.Info("bytes_out counter went backwards; holding previous rate",
				"frontend", st.Name,
				"previous_bytes_out", fs.lastBytesOut,
				"current_bytes_out", st.BytesOut,
				"held_rate_bps", fs.lastRate)
		}
		fs.lastBytesOut = st.BytesOut
		fs.lastRate = rate
		// 已知但当前未映射的 frontend 也在上面刷新了基线：将来被重新映射
		// 进来时，差分从第一秒起就是连续正确的，而不必重走"首采样建基线"
		// 丢掉一秒数据。
		if mapped {
			a := sums[envID]
			a.rate += rate
			a.conn += st.ConnCur
			a.measured = true
		}
	}

	// 基线生命周期：对从采样结果中消失的 frontend 累加缺席计数，超过
	// absentTickLimit（约 1 分钟）后淘汰基线，防止已下线的 frontend 造成
	// 状态泄漏；限期内回归的 frontend（如 reload 抖动）差分保持连续。
	for name, fs := range c.frontends {
		if present[name] {
			continue
		}
		fs.absentTicks++
		if fs.absentTicks >= absentTickLimit {
			delete(c.frontends, name)
			c.log.Info("dropping counter baseline for absent frontend",
				"frontend", name,
				"absent_ticks", fs.absentTicks,
				"absent_tick_limit", absentTickLimit)
		}
	}

	// 聚合状态生命周期：mapping 里不再出现的 env，其窗口/EWMA 一并丢弃——
	// 陈旧的平滑状态若保留，env 将来重新上线时会带着过期历史起步。
	for envID := range c.envs {
		if _, ok := sums[envID]; !ok {
			delete(c.envs, envID)
			c.log.Debug("dropping aggregation state for unmapped env", "env", envID)
		}
	}

	usages := make([]model.EnvUsage, 0, len(sums))
	for envID, a := range sums {
		st, ok := c.envs[envID]
		if !ok {
			st = &envState{
				window: NewSlidingWindow(window10Size),
				ewma:   NewEWMA(ewma60Alpha),
			}
			c.envs[envID] = st
		}
		// 仅建基线的 tick（baselined && !measured）速率未知，跳过窗口/EWMA，
		// 原因见 agg 的注释。而"既无测量也无基线"的 env（映射里有、但本 tick
		// 没有任何存活 frontend）是真实的零：没有 frontend 就没有流量，零值
		// 必须进入窗口，否则 mean10 会停留在旧值上虚高。
		if a.measured || !a.baselined {
			st.window.Push(a.rate)
			st.ewma.Update(a.rate)
			st.lastRate = a.rate
		}
		u := model.EnvUsage{
			EnvID:     envID,
			RateBps:   a.rate,
			Mean10Bps: st.window.Mean(),
			Ewma60Bps: st.ewma.Value(),
			ConnCur:   a.conn,
		}
		st.lastUsage = u
		usages = append(usages, u)
	}
	sortUsages(usages)

	// 每 tick 的 per-env 汇总仅在 debug 级输出（每秒每环境一条，量大）。
	// 先做 Enabled 判断，生产 info 级别下完全零开销。
	if c.log.Enabled(context.Background(), slog.LevelDebug) {
		for _, u := range usages {
			c.log.Debug("tick env usage",
				"env", u.EnvID,
				"rate_bps", u.RateBps,
				"mean10_bps", u.Mean10Bps,
				"ewma60_bps", u.Ewma60Bps,
				"conn_cur", u.ConnCur)
		}
	}
	return usages
}

// sortUsages 按 EnvID 升序排序，保证 Tick 输出顺序确定，便于测试比对与
// 日志/上报的稳定阅读顺序。
func sortUsages(us []model.EnvUsage) {
	sort.Slice(us, func(i, j int) bool { return us[i].EnvID < us[j].EnvID })
}
