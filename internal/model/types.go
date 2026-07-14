// Package model holds the domain types shared between the rl-agent and the
// controller.
//
// Unit conventions:
//   - All internal rate values are BYTES per second (float64).
//   - Quota values in configuration (local YAML and controller JSON) are BITS
//     per second (ops convention: 200_000_000 = 200 Mbps). They are converted
//     to bytes/s at the config boundary via EnvQuota.QuotaBytesPerSec.
package model

import "context"

// Operating modes for the executor.
const (
	ModeDryRun  = "dry-run"
	ModeEnforce = "enforce"
)

// FrontendStat is one frontend row sampled from the HAProxy stats socket.
type FrontendStat struct {
	Name     string // pxname
	BytesOut uint64 // cumulative bytes sent to clients (downstream)
	ConnCur  int64  // scur: current concurrent connections
}

// EnvUsage is the per-environment usage computed by the collector each tick.
type EnvUsage struct {
	EnvID     string
	RateBps   float64 // instantaneous bytes/s (1s counter diff)
	Mean10Bps float64 // 10s sliding-window mean, bytes/s (billing/commit metric)
	Ewma60Bps float64 // ~60s EWMA, bytes/s (slow-loop reallocation input)
	ConnCur   int64   // sum of frontend concurrent connections
	Degraded  bool    // sampling has been failing; values are held from last good tick
}

// GovState describes what the governor is currently doing for an env.
type GovState int

const (
	StateNormal GovState = iota // bwlim parked at the elastic ceiling
	StateTightening
	StateRecovering
)

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

// Decision is the governor's per-env output for one tick.
type Decision struct {
	EnvID     string
	Frontends []string // frontends the shaping value must be applied to
	BwlimBps  float64  // target aggregate shaping value, bytes/s
	State     GovState
	Changed   bool // true when BwlimBps differs from the previously emitted value
}

// GovParams are the fast-loop control parameters (design doc §3.3). All are
// distributable by the controller and overridable per env.
type GovParams struct {
	ElasticCeiling float64 `json:"elastic_ceiling" yaml:"elastic_ceiling"` // ceil = quota × this (default 1.10)
	LowWatermark   float64 `json:"low_watermark" yaml:"low_watermark"`     // recovery threshold fraction (default 0.90)
	TightenAfterS  int     `json:"tighten_after_s" yaml:"tighten_after_s"` // mean10 > quota for this long → tighten (default 3)
	RecoverAfterS  int     `json:"recover_after_s" yaml:"recover_after_s"` // mean10 < low for this long → relax (default 5)
	MDFactor       float64 `json:"md_factor" yaml:"md_factor"`             // multiplicative decrease (default 0.9)
	TightenFloor   float64 `json:"tighten_floor" yaml:"tighten_floor"`     // never tighten below quota × this (default 0.95)
	AIStepFrac     float64 `json:"ai_step_frac" yaml:"ai_step_frac"`       // additive increase per second, fraction of quota (default 0.05)
}

// DefaultGovParams returns the initial parameter set from the design doc.
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

// Normalize fills zero-valued fields with defaults so a partially specified
// override stays sane.
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

// EnvQuota binds an environment to its frontends and node-level quota.
type EnvQuota struct {
	EnvID           string     `json:"env_id" yaml:"env_id"`
	Frontends       []string   `json:"frontends" yaml:"frontends"`
	QuotaBitsPerSec int64      `json:"quota_bps" yaml:"quota_bps"` // BITS per second
	Params          *GovParams `json:"params,omitempty" yaml:"params,omitempty"`
}

// QuotaBytesPerSec converts the configured bits/s quota to internal bytes/s.
func (q EnvQuota) QuotaBytesPerSec() float64 {
	return float64(q.QuotaBitsPerSec) / 8.0
}

// EffectiveParams returns the per-env override merged over defaults.
func (q EnvQuota) EffectiveParams() GovParams {
	if q.Params == nil {
		return DefaultGovParams()
	}
	p := *q.Params
	p.Normalize()
	return p
}

// ControllerConfig is the versioned document delivered by the controller via
// the long-poll config endpoint, and persisted locally as the fail-static
// cache.
type ControllerConfig struct {
	Version            int64      `json:"version"`
	Mode               string     `json:"mode"` // dry-run | enforce
	Envs               []EnvQuota `json:"envs"`
	ReportIntervalS    int        `json:"report_interval_s"`
	HeartbeatIntervalS int        `json:"heartbeat_interval_s"`
}

// Normalize fills defaults for optional fields.
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

// FrontendToEnv flattens the env list into a frontend → env lookup map.
func (c ControllerConfig) FrontendToEnv() map[string]string {
	m := make(map[string]string)
	for _, e := range c.Envs {
		for _, fe := range e.Frontends {
			m[fe] = e.EnvID
		}
	}
	return m
}

// ---- Component contracts (implemented in agent/internal/...) ----

// StatSource samples frontend statistics (implemented by the HAProxy runtime
// API client; faked in tests).
type StatSource interface {
	ShowStat(ctx context.Context) ([]FrontendStat, error)
}

// MapSetter updates a HAProxy map entry via the runtime API.
type MapSetter interface {
	SetMapEntry(ctx context.Context, mapPath, key, value string) error
}
