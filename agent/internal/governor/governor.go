// Package governor 实现每秒执行一次的快环 AIMD 控制器（设计文档 §3.3
// "本地快环控制算法"）。
//
// 架构位置：governor 位于 Agent 核心循环的"决策"环节——上游是 collector
// 每秒产出的各环境用量（10s 滑动均值等），下游是 executor（设计文档
// §3.2）负责把决策真正写入 HAProxy。governor 本身是纯决策组件：
// 既不接触 HAProxy，也不读系统时钟，因此可以被完全确定性地单测。
//
// 控制目标（承诺口径）：10 秒滑动均值 ≤ 约定配额，瞬时允许冲高到弹性
// 上限（默认 quota × 1.10）。由于整形常驻生效，算法退化为对整形值
// bwlim 的动态微调，采用 AIMD（急收慢放）结构：
//
//   - 超限持续 ≥ TightenAfterS 秒 → 乘性收紧：bwlim ×= MDFactor
//     （下限 quota × TightenFloor）。乘性收缩能在少数几拍内把均值
//     压回配额以内，响应速度与超限幅度成正比——超得越狠收得越快；
//   - 用量低于低水位持续 ≥ RecoverAfterS 秒 → 加性放松：bwlim +=
//     quota × AIStepFrac（上限为弹性 ceiling）。放松刻意走慢速线性
//     步进，避免一放开就立即再次超限。
//
// 这种"乘性收紧、加性放松"的不对称结构是 TCP 拥塞控制验证过的稳定
// 形态，专门用来防止"限速→带宽掉→放开→又超限"的锯齿振荡。
//
// 时间语义：Tick 的时间戳由调用方注入，算法本身不依赖墙钟差值，而是
// 假定 Agent 核心循环固定 1 秒一拍，因此持续性计数器（overSecs /
// underSecs）直接以 tick 计数充当秒数。
package governor

import (
	"log/slog"
	"math"
	"slices"
	"sync"
	"time"

	"github.com/ipest-info/haproxy_ingress_rate_limiter/internal/model"
)

// changedEpsilonFrac 是发射（emit）迟滞阈值：只有当本拍目标值与上一次
// 以 Changed=true 发射的值相差超过配额的 0.1% 时，决策才会被标记为
// Changed。这个 epsilon 的存在有两个目的：
//
//  1. 抑制无意义的抖动写入——浮点运算带来的微小漂移不值得触发一次
//     HAProxy runtime API 写操作；
//  2. 防止漂移被"静默吞掉"——lastEmitted 只在 Changed 时才推进，
//     因此若干拍的亚阈值漂移会持续累积，一旦累积量越过 0.1% 便会
//     整体发射出去，长期看目标值不会因迟滞而系统性偏离。
const changedEpsilonFrac = 0.001

// envState 是单个环境的完整控制状态。所有速率字段单位均为 bytes/s。
type envState struct {
	frontends   []string        // 该环境映射到的 HAProxy frontend 列表
	quotaBytes  float64         // 控制面下发的节点配额，bytes/s
	params      model.GovParams // 快环控制参数（可按环境覆盖）
	bwlim       float64         // 当前整形目标值，bytes/s
	overSecs    int             // mean10 > quota 的连续拍数（收紧持续性计数）
	underSecs   int             // mean10 < quota×LowWatermark 的连续拍数（恢复持续性计数）
	state       model.GovState  // 当前状态机所处状态（Normal/Tightening/Recovering）
	lastEmitted float64         // 上一次以 Changed=true 发射出去的值
	emitted     bool            // 首次 Changed 发射之前为 false，用于强制首拍发射
}

// Governor 持有全部已配置环境的快环控制状态。
// UpdateConfig 与 Tick 可能来自不同 goroutine（配置热更新 vs 核心循环），
// 因此用互斥锁保护整个 envs 表。
type Governor struct {
	log *slog.Logger

	mu   sync.Mutex
	envs map[string]*envState
}

// New 构造一个空的 Governor。传入 nil logger 时回退到 slog.Default()。
func New(log *slog.Logger) *Governor {
	if log == nil {
		log = slog.Default()
	}
	return &Governor{
		log:  log,
		envs: make(map[string]*envState),
	}
}

// UpdateConfig 用新的环境列表整体替换当前配置。
//
//   - 新增环境：初始 bwlim 停靠在弹性上限（quota × ElasticCeiling），
//     状态为 StateNormal——这对应设计文档 §3.3 的"常态"分支；
//   - 已有环境的配额或参数发生变化：控制状态重置——bwlim 回到新的
//     弹性上限、持续性计数清零、状态回到 Normal。重置而非平滑过渡的
//     原因是：旧的收紧/恢复进度是基于旧配额算出来的，换了刻度后继续
//     沿用没有意义，不如从干净状态重新收敛；
//   - 新列表中不存在的环境被直接删除。
//
// 仅 frontend 列表变化不会重置控制状态（配额刻度没变，收敛进度仍然
// 有效），但会清掉 emitted 标志强制下一拍发射 Changed 决策，确保新映射
// 进来的 frontend 能立刻拿到当前整形值，而不是等到目标值下次漂移。
func (g *Governor) UpdateConfig(envs []model.EnvQuota) {
	g.mu.Lock()
	defer g.mu.Unlock()

	seen := make(map[string]bool, len(envs))
	for _, e := range envs {
		if seen[e.EnvID] {
			g.log.Warn("duplicate env in config ignored", "env", e.EnvID)
			continue
		}
		seen[e.EnvID] = true

		quota := e.QuotaBytesPerSec()
		params := e.EffectiveParams()
		frontends := slices.Clone(e.Frontends)
		ceil := quota * params.ElasticCeiling

		st, ok := g.envs[e.EnvID]
		if !ok {
			// 新环境：直接停靠在弹性上限，等待第一拍用量数据。
			g.envs[e.EnvID] = &envState{
				frontends:  frontends,
				quotaBytes: quota,
				params:     params,
				bwlim:      ceil,
				state:      model.StateNormal,
			}
			g.log.Info("env added",
				"env", e.EnvID,
				"quota_bytes_per_s", quota,
				"ceil_bytes_per_s", ceil,
				"bwlim_bytes_per_s", ceil,
				"frontends", frontends,
				"params", paramsGroup(params))
			continue
		}

		if st.quotaBytes != quota || st.params != params {
			// 配额/参数变化：整套控制状态重置到新刻度下的常态。
			oldQuota := st.quotaBytes
			oldBwlim := st.bwlim
			st.quotaBytes = quota
			st.params = params
			st.bwlim = ceil
			st.overSecs = 0
			st.underSecs = 0
			st.state = model.StateNormal
			st.emitted = false // 强制重新发射重置后的值
			g.log.Info("env quota/params changed; bwlim reset to ceiling",
				"env", e.EnvID,
				"old_quota_bytes_per_s", oldQuota,
				"new_quota_bytes_per_s", quota,
				"ceil_bytes_per_s", ceil,
				"bwlim_old", oldBwlim,
				"bwlim_new", st.bwlim,
				"params", paramsGroup(params))
		}
		if !slices.Equal(st.frontends, frontends) {
			// frontend 集合变化：不动控制状态，只强制下一拍发射，
			// 让新 frontend 立即拿到当前整形值。
			st.frontends = frontends
			st.emitted = false
		}
	}

	// 删除新配置中不再出现的环境。
	for id := range g.envs {
		if !seen[id] {
			delete(g.envs, id)
			g.log.Info("env removed", "env", id)
		}
	}
}

// Tick 对每个用量样本推进一步 AIMD，并为每个"已配置且有样本"的环境
// 返回一条决策。未配置的 EnvID 被忽略；已配置但本拍缺样本的环境不产生
// 决策——没有数据时保持现状（hold），绝不凭空猜测。
//
// now 为注入参数，仅为可测试性保留：算法依赖固定 1s 的 tick 节奏，
// 不依赖墙钟差值，因此这里刻意不使用它。
func (g *Governor) Tick(now time.Time, usages []model.EnvUsage) []model.Decision {
	_ = now // 持续性计数器以固定 1s 节奏的 tick 计数充当秒数

	g.mu.Lock()
	defer g.mu.Unlock()

	decisions := make([]model.Decision, 0, len(usages))
	for _, u := range usages {
		st, ok := g.envs[u.EnvID]
		if !ok {
			continue
		}
		decisions = append(decisions, g.step(st, u))
	}
	return decisions
}

// step 将单个环境推进一拍并产出该环境的决策。这里是 AIMD 状态机的
// 全部分支逻辑所在。
func (g *Governor) step(st *envState, u model.EnvUsage) model.Decision {
	if u.Degraded {
		// 降级冻结：采样链路持续失败，collector 送来的是"保持上次
		// 良好值"的陈旧数据。基于陈旧数据做任何调整都可能放大错误
		// （比如误把已经回落的流量继续收紧），因此冻结 bwlim、状态和
		// 持续性计数，Changed=false，等待数据恢复后再继续推进。
		g.log.Debug("tick env degraded; state frozen",
			"env", u.EnvID,
			"state", st.state.String(),
			"bwlim_bytes_per_s", st.bwlim)
		return model.Decision{
			EnvID:     u.EnvID,
			Frontends: st.frontends,
			BwlimBps:  st.bwlim,
			State:     st.state,
			Changed:   false,
		}
	}

	q := st.quotaBytes
	p := st.params
	ceil := q * p.ElasticCeiling
	m := u.Mean10Bps

	oldState := st.state
	oldBwlim := st.bwlim

	switch {
	case m > q:
		// —— 超限分支：10s 均值越过配额（承诺口径被打破）——
		// 先累计超限持续拍数并清空恢复计数（两个计数器互斥，方向
		// 一旦反转就要求对方重新计满，这本身就是一层防抖）。
		st.overSecs++
		st.underSecs = 0
		if st.overSecs >= p.TightenAfterS {
			// 持续超限达到阈值才动手，过滤掉 1~2 秒的瞬时毛刺。
			// 乘性收紧（bwlim ×= MDFactor）：收缩量与当前值成正比，
			// 超限越久收得越快，能以几何速度把均值压回配额内；
			// 下限 quota × TightenFloor 保证不会把用户压到远低于
			// 其付费配额的水平——收紧的目标是"压回配额"，不是惩罚。
			st.bwlim = math.Max(q*p.TightenFloor, st.bwlim*p.MDFactor)
			st.state = model.StateTightening
		}
	case m < q*p.LowWatermark:
		// —— 低水位分支：均值低于 quota × LowWatermark ——
		// 恢复阈值刻意低于配额本身（默认 0.90），与上方的超限阈值
		// (1.0) 之间留出一条死区，避免均值在配额附近来回穿越时
		// 收紧/放松交替触发。
		st.underSecs++
		st.overSecs = 0
		if st.bwlim < ceil && st.underSecs >= p.RecoverAfterS {
			// 加性放松（bwlim += quota × AIStepFrac）：固定小步慢速
			// 归还带宽，比乘性放大稳得多——如果放松也用乘性，刚收
			// 紧完就会被指数级放回去，AIMD 的防振荡结构就失效了。
			// 上限是弹性 ceiling；到顶即回到 Normal（常态），否则
			// 停留在 Recovering 表示"仍在爬坡途中"。
			st.bwlim = math.Min(ceil, st.bwlim+q*p.AIStepFrac)
			if st.bwlim >= ceil {
				st.state = model.StateNormal
			} else {
				st.state = model.StateRecovering
			}
		}
	default:
		// —— 死区分支：quota×LowWatermark ≤ mean10 ≤ quota ——
		// 均值落在承诺口径以内但尚未低到值得放松的程度：保持当前
		// bwlim 不动。两个持续性计数都清零，因为"持续超限/持续
		// 低水位"的判定要求条件连续成立——一旦回到死区，之前的
		// 累计就不再代表一个连续区间，必须重新计数，否则断断续续
		// 的越界会被错误地拼接成"持续越界"。注意非 Normal 状态在
		// 死区中被保留：只有 bwlim 真正爬回 ceiling 才算恢复完成。
		st.overSecs = 0
		st.underSecs = 0
	}

	// 状态变迁日志：Normal/Tightening/Recovering 任意互转都记录一条。
	if st.state != oldState {
		g.log.Info("governor state transition",
			"env", u.EnvID,
			"state_from", oldState.String(),
			"state_to", st.state.String(),
			"mean10_bytes_per_s", m,
			"quota_bytes_per_s", q,
			"utilization", utilization(m, q),
			"bwlim_old", oldBwlim,
			"bwlim_new", st.bwlim,
			"over_secs", st.overSecs,
			"under_secs", st.underSecs)
	}
	// bwlim 实际调整日志：只有数值真的变了才记（触底/到顶后的空转
	// 不算调整），收紧与放松使用不同的消息便于检索。
	if st.bwlim < oldBwlim {
		g.log.Info("bwlim tightened",
			"env", u.EnvID,
			"bwlim_old", oldBwlim,
			"bwlim_new", st.bwlim,
			"mean10_bytes_per_s", m,
			"quota_bytes_per_s", q,
			"utilization", utilization(m, q),
			"over_secs", st.overSecs)
	} else if st.bwlim > oldBwlim {
		g.log.Info("bwlim relaxed",
			"env", u.EnvID,
			"bwlim_old", oldBwlim,
			"bwlim_new", st.bwlim,
			"mean10_bytes_per_s", m,
			"quota_bytes_per_s", q,
			"utilization", utilization(m, q),
			"under_secs", st.underSecs)
	}

	// 发射判定：首次必发（emitted=false），此后仅当与上次发射值的
	// 偏差超过配额的 0.1%（changedEpsilonFrac）才标记 Changed，
	// 详见常量注释中关于迟滞与漂移累积的说明。
	changed := !st.emitted || math.Abs(st.bwlim-st.lastEmitted) > changedEpsilonFrac*q
	if changed {
		st.emitted = true
		st.lastEmitted = st.bwlim
	}

	// 每拍 per-env 摘要（debug 级），用于问题排查时还原完整时间线。
	g.log.Debug("tick env summary",
		"env", u.EnvID,
		"state", st.state.String(),
		"mean10_bytes_per_s", m,
		"quota_bytes_per_s", q,
		"utilization", utilization(m, q),
		"bwlim_bytes_per_s", st.bwlim,
		"over_secs", st.overSecs,
		"under_secs", st.underSecs,
		"changed", changed)

	return model.Decision{
		EnvID:     u.EnvID,
		Frontends: st.frontends,
		BwlimBps:  st.bwlim,
		State:     st.state,
		Changed:   changed,
	}
}

// utilization 计算 mean10/quota 的利用率，四舍五入保留两位小数，
// 仅用于日志展示；quota 非正时返回 0 以避免日志里出现 Inf/NaN。
func utilization(mean10, quota float64) float64 {
	if quota <= 0 {
		return 0
	}
	return math.Round(mean10/quota*100) / 100
}

// paramsGroup 把快环参数打包成一个 slog 分组属性，供配置变更日志
// 输出参数概要，避免在每条日志里平铺七个字段。
func paramsGroup(p model.GovParams) slog.Value {
	return slog.GroupValue(
		slog.Float64("elastic_ceiling", p.ElasticCeiling),
		slog.Float64("low_watermark", p.LowWatermark),
		slog.Int("tighten_after_s", p.TightenAfterS),
		slog.Int("recover_after_s", p.RecoverAfterS),
		slog.Float64("md_factor", p.MDFactor),
		slog.Float64("tighten_floor", p.TightenFloor),
		slog.Float64("ai_step_frac", p.AIStepFrac),
	)
}
