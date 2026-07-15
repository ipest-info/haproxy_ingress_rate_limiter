// Package model 存放 rl-agent 与 controller 之间共享的领域类型，
// 是采集器（collector）、决策器（governor）、执行器（executor）以及
// 控制面协议共同依赖的"词汇表"。
//
// 单位约定（非常重要，混淆会带来 8 倍误差）：
//   - 内部所有速率一律为「字节每秒」（bytes/s，float64）。采集自 HAProxy
//     stats 的 bytes_out 本身就是字节计数，内部保持字节口径可避免换算。
//   - 配置中的配额（本地 YAML 与控制面 JSON 的 quota_bps 字段）一律为
//     「比特每秒」（bits/s），遵循运维习惯：200_000_000 表示 200 Mbps。
//   - 两种口径只在配置入口处转换一次：EnvQuota.QuotaBytesPerSec()
//     负责 bits/s → bytes/s（除以 8），其余代码不得再做单位换算。
package model

import "context"

// 执行器（executor）的两种运行模式：
//   - dry-run：只计算并记录本应写入的整形值，不真正改动 HAProxy，
//     用于灰度观察与新环境验证（安全默认值）；
//   - enforce：把整形值真实写入 HAProxy bwlim map，实际生效限速。
//
// 模式由控制面下发（ControllerConfig.Mode），非法值会被 Normalize
// 归一为 dry-run，保证误配置不会意外放大影响。
const (
	ModeDryRun  = "dry-run"
	ModeEnforce = "enforce"
)

// FrontendStat 是从 HAProxy stats socket（show stat）采样得到的单个
// frontend 行，是快环每秒采集的原始数据（设计文档 §3.1：统计口径用
// frontend 的 bytes_out，而非网卡计数，以排除内网/健康检查流量）。
type FrontendStat struct {
	Name     string // frontend 名称（stats 输出的 pxname 列）
	BytesOut uint64 // 发往客户端（下行方向）的累计字节数，单调递增计数器；速率由相邻两秒差分得出
	ConnCur  int64  // 当前并发连接数（stats 输出的 scur 列），用于资源保护水位观测
}

// EnvUsage 是采集器每个 tick（1s）按环境聚合计算出的用量视图，
// 同一环境可能覆盖多个 frontend，这里已把它们的计数求和。
// 三个速率字段对应三种时间尺度，服务于不同消费方：
type EnvUsage struct {
	EnvID string // 所属环境 ID

	// RateBps 为瞬时速率（bytes/s）：本秒与上一秒 bytes_out 计数器的差分。
	// 噪声最大，仅作观测参考，不直接驱动限速决策。
	RateBps float64

	// Mean10Bps 为 10 秒滑动窗口均值（bytes/s）。这是承诺/计费口径
	// （设计文档 §3.3 已拍板：10 秒滑动均值 ≤ 约定带宽，瞬时容忍至
	// 110%），也是快环收紧/恢复判据的直接输入。
	Mean10Bps float64

	// Ewma60Bps 为约 60 秒的指数加权移动平均（bytes/s），是全局慢环
	// 按节点近期用量加权重新切分环境配额的输入（设计文档 §2.1 的
	// 节点配额分配算法使用"近 60s EWMA 用量"）。
	Ewma60Bps float64

	// ConnCur 为该环境所有 frontend 当前并发连接数之和。
	ConnCur int64

	// Degraded 表示采样已连续失败（stats socket 超时、计数回绕等），
	// 当前各速率值是沿用最后一次成功采样的结果（设计文档 §3.7：
	// 采集失败的秒沿用上一秒速率，连续失败告警并保持整形值不动）。
	Degraded bool
}

// GovState 描述决策器（governor）当前对某个环境所处的 AIMD 状态，
// 对应设计文档 §3.3 的"常态 / 收紧 / 恢复"三段：
type GovState int

const (
	// StateNormal：常态，整形值停在弹性上限（quota × ElasticCeiling），
	// 允许瞬时/短时冲高到 110%。
	StateNormal GovState = iota
	// StateTightening：收紧中，mean10 持续超配额后按 MDFactor 乘性下压。
	StateTightening
	// StateRecovering：恢复中，mean10 持续低于低水位后按 AIStepFrac
	// 加性放松，逐步回到弹性上限。
	StateRecovering
)

// String 返回状态的小写英文名，用于日志与上报字段。
func (s GovState) String() string {
	switch s {
	case StateTightening:
		return "tightening"
	case StateRecovering:
		return "recovering"
	default:
		return "normal"
	}
}

// Decision 是决策器每个 tick 针对单个环境产出的执行指令，
// 由执行器翻译成 HAProxy bwlim map 的写入操作。
type Decision struct {
	EnvID     string   // 目标环境 ID
	Frontends []string // 需要应用该整形值的 frontend 列表（同环境共享聚合整形值）
	BwlimBps  float64  // 目标聚合整形值，单位 bytes/s（内部口径）
	State     GovState // 当前 AIMD 状态，用于观测与上报
	// Changed 为 true 表示 BwlimBps 相比上次产出的值发生了变化；
	// 执行器可据此跳过无变化的 map 写入，减少 runtime API 压力。
	Changed bool
}

// GovParams 是本地快环的控制参数（设计文档 §3.3），全部可由控制面
// 下发，并支持按环境覆盖（EnvQuota.Params）。默认值即 §3.3 中的
// 1.10 / 0.90 / 3s / 5s / ×0.9 / +5% 组合。
type GovParams struct {
	// ElasticCeiling：弹性上限系数，ceil = quota × ElasticCeiling。
	// 默认 1.10，即常态下允许冲高到配额的 110%。
	ElasticCeiling float64 `json:"elastic_ceiling" yaml:"elastic_ceiling"`
	// LowWatermark：恢复判据的低水位系数。mean10 < quota × LowWatermark
	// 持续 RecoverAfterS 秒后开始放松。默认 0.90。
	LowWatermark float64 `json:"low_watermark" yaml:"low_watermark"`
	// TightenAfterS：mean10 > quota 需持续的秒数，达到后触发收紧。
	// 默认 3（秒）。
	TightenAfterS int `json:"tighten_after_s" yaml:"tighten_after_s"`
	// RecoverAfterS：mean10 低于低水位需持续的秒数，达到后触发恢复。
	// 默认 5（秒）。急收慢放：恢复窗口比收紧窗口更长。
	RecoverAfterS int `json:"recover_after_s" yaml:"recover_after_s"`
	// MDFactor：乘性收紧系数（multiplicative decrease），
	// 收紧时 bwlim = max(quota × TightenFloor, bwlim × MDFactor)。默认 0.9。
	MDFactor float64 `json:"md_factor" yaml:"md_factor"`
	// TightenFloor：收紧下限系数，整形值永不低于 quota × TightenFloor，
	// 防止过度惩罚。默认 0.95。
	TightenFloor float64 `json:"tighten_floor" yaml:"tighten_floor"`
	// AIStepFrac：加性恢复步长（additive increase），每秒放松
	// quota × AIStepFrac，直至回到弹性上限。默认 0.05（5%/s）。
	AIStepFrac float64 `json:"ai_step_frac" yaml:"ai_step_frac"`
}

// DefaultGovParams 返回设计文档 §3.3 给出的初始参数组合。
func DefaultGovParams() GovParams {
	return GovParams{
		ElasticCeiling: 1.10,
		LowWatermark:   0.90,
		TightenAfterS:  3,
		RecoverAfterS:  5,
		MDFactor:       0.9,
		TightenFloor:   0.95,
		AIStepFrac:     0.05,
	}
}

// Normalize 把零值/非法字段回填为默认值，使"只覆盖个别参数"的
// 局部配置也能得到一组自洽的完整参数。特别地，MDFactor 必须落在
// (0, 1) 开区间内才有"乘性收紧"的意义，越界值一律回退默认。
func (p *GovParams) Normalize() {
	d := DefaultGovParams()
	if p.ElasticCeiling <= 0 {
		p.ElasticCeiling = d.ElasticCeiling
	}
	if p.LowWatermark <= 0 {
		p.LowWatermark = d.LowWatermark
	}
	if p.TightenAfterS <= 0 {
		p.TightenAfterS = d.TightenAfterS
	}
	if p.RecoverAfterS <= 0 {
		p.RecoverAfterS = d.RecoverAfterS
	}
	if p.MDFactor <= 0 || p.MDFactor >= 1 {
		p.MDFactor = d.MDFactor
	}
	if p.TightenFloor <= 0 {
		p.TightenFloor = d.TightenFloor
	}
	if p.AIStepFrac <= 0 {
		p.AIStepFrac = d.AIStepFrac
	}
}

// EnvQuota 把一个环境绑定到它的 frontend 列表与本节点配额，是配置
// 的最小分发单元（设计文档 §3.5 数据模型）。控制面慢环重新切分
// 配额时，改动的就是各节点收到的 QuotaBitsPerSec。
type EnvQuota struct {
	EnvID     string   `json:"env_id" yaml:"env_id"`       // 环境 ID
	Frontends []string `json:"frontends" yaml:"frontends"` // 该环境在本节点上覆盖的 HAProxy frontend 名单
	// QuotaBitsPerSec 为本节点配额，单位「比特每秒」（bits/s，运维
	// 口径）。注意：这是全代码库唯一以 bits/s 存储的速率字段，进入
	// 内部计算前必须经 QuotaBytesPerSec() 转为 bytes/s。
	QuotaBitsPerSec int64 `json:"quota_bps" yaml:"quota_bps"`
	// Params 为该环境的快环参数覆盖；nil 表示整体使用默认参数。
	Params *GovParams `json:"params,omitempty" yaml:"params,omitempty"`
}

// QuotaBytesPerSec 把配置口径的 bits/s 配额转换为内部口径的 bytes/s。
// 这是 bits/s 与 bytes/s 的唯一换算边界（除以 8）。
func (q EnvQuota) QuotaBytesPerSec() float64 {
	return float64(q.QuotaBitsPerSec) / 8.0
}

// EffectiveParams 返回该环境实际生效的快环参数：无覆盖时用默认值；
// 有覆盖时以覆盖为底、经 Normalize 补齐未填字段。返回副本，不会
// 修改 q.Params 本身。
func (q EnvQuota) EffectiveParams() GovParams {
	if q.Params == nil {
		return DefaultGovParams()
	}
	p := *q.Params
	p.Normalize()
	return p
}

// ControllerConfig 是控制面通过长轮询配置接口（设计文档 §3.6）下发
// 的带版本配置文档，同时也是 fail-static 本地缓存的持久化格式
// （§3.7：断联时按最后一次下发的配置继续限速，恢复后先拉全量）。
// Version 单调比较：agent 以本地版本号发起长轮询，控制面仅在版本
// 不一致时立即返回新配置。
type ControllerConfig struct {
	Version int64      `json:"version"` // 配置版本号，agent 用它做长轮询与幂等判断
	Mode    string     `json:"mode"`    // 执行模式：dry-run | enforce（非法值归一为 dry-run）
	Envs    []EnvQuota `json:"envs"`    // 本节点承载的各环境配额
	// ReportIntervalS：agent 上报用量样本的间隔（秒），默认 5。
	ReportIntervalS int `json:"report_interval_s"`
	// HeartbeatIntervalS：agent 心跳间隔（秒），默认 10。
	HeartbeatIntervalS int `json:"heartbeat_interval_s"`
}

// Normalize 为可选字段填充默认值：模式非法时归一为 dry-run（安全
// 方向），上报/心跳间隔非正时取默认值，并逐个归一各环境的参数覆盖。
// 所有配置入口（控制面下发、本地 YAML、缓存加载）都应先经过这里。
func (c *ControllerConfig) Normalize() {
	if c.Mode != ModeEnforce {
		c.Mode = ModeDryRun
	}
	if c.ReportIntervalS <= 0 {
		c.ReportIntervalS = 5
	}
	if c.HeartbeatIntervalS <= 0 {
		c.HeartbeatIntervalS = 10
	}
	for i := range c.Envs {
		if c.Envs[i].Params != nil {
			c.Envs[i].Params.Normalize()
		}
	}
}

// FrontendToEnv 把环境列表展平为 frontend → env_id 的查找表，供采集
// 器按 frontend 行归属环境做聚合。若同一 frontend 被多个环境声明，
// 后者覆盖前者（配置侧应保证不出现这种情况）。
func (c ControllerConfig) FrontendToEnv() map[string]string {
	m := make(map[string]string)
	for _, e := range c.Envs {
		for _, fe := range e.Frontends {
			m[fe] = e.EnvID
		}
	}
	return m
}

// ---- 组件契约（具体实现位于 agent/internal/...） ----

// StatSource 抽象 frontend 统计的采样来源：生产实现是 HAProxy
// runtime API 客户端（show stat），测试中以假实现替代。
type StatSource interface {
	ShowStat(ctx context.Context) ([]FrontendStat, error)
}

// MapSetter 抽象通过 HAProxy runtime API 更新 map 表项的能力
// （set map <path> <key> <value>），执行器用它写入 bwlim 整形值。
type MapSetter interface {
	SetMapEntry(ctx context.Context, mapPath, key, value string) error
}
