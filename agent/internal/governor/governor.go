// Package governor implements the per-second fast-loop AIMD controller
// (design doc §3.3). The commit metric is the 10s sliding mean: while it
// exceeds the quota the shaping value is tightened multiplicatively (fast),
// and once usage falls below the low watermark it is relaxed additively
// (slow) back to the elastic ceiling. The asymmetric AIMD structure prevents
// limit/recover oscillation.
//
// The governor is a pure decision component: it never touches HAProxy and
// never reads the clock. Tick receives an injected timestamp and assumes the
// fixed 1s cadence of the agent core loop, so the persistence counters
// (overSecs/underSecs) are tick counts.
package governor

import (
	"log/slog"
	"math"
	"slices"
	"sync"
	"time"

	"github.com/ipest-info/haproxy_ingress_rate_limiter/internal/model"
)

// changedEpsilonFrac is the emit hysteresis: a decision is flagged Changed
// only when the target differs from the last emitted value by more than this
// fraction of the quota. lastEmitted advances only on Changed decisions so
// sub-threshold drift accumulates instead of being silently absorbed.
const changedEpsilonFrac = 0.001

// envState is the per-environment control state.
type envState struct {
	frontends   []string
	quotaBytes  float64 // bytes/s
	params      model.GovParams
	bwlim       float64 // current shaping target, bytes/s
	overSecs    int     // consecutive ticks with mean10 > quota
	underSecs   int     // consecutive ticks with mean10 < quota × LowWatermark
	state       model.GovState
	lastEmitted float64 // last value emitted with Changed=true
	emitted     bool    // false until the first Changed emission
}

// Governor holds the fast-loop state for every configured environment.
// UpdateConfig and Tick may be called from different goroutines.
type Governor struct {
	log *slog.Logger

	mu   sync.Mutex
	envs map[string]*envState
}

// New builds an empty Governor. A nil logger falls back to slog.Default().
func New(log *slog.Logger) *Governor {
	if log == nil {
		log = slog.Default()
	}
	return &Governor{
		log:  log,
		envs: make(map[string]*envState),
	}
}

// UpdateConfig replaces the configured environment set.
//
//   - A new env starts parked at the elastic ceiling in StateNormal.
//   - An existing env whose quota or params changed is reset to the new
//     ceiling with cleared persistence counters; the change is logged.
//   - Envs absent from the new list are dropped.
//
// A frontend-list change alone does not reset the control state, but forces
// the next decision to be Changed so newly mapped frontends receive the
// current shaping value.
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

		st, ok := g.envs[e.EnvID]
		if !ok {
			g.envs[e.EnvID] = &envState{
				frontends:  frontends,
				quotaBytes: quota,
				params:     params,
				bwlim:      quota * params.ElasticCeiling,
				state:      model.StateNormal,
			}
			g.log.Info("env added",
				"env", e.EnvID,
				"quota_bytes_per_s", quota,
				"bwlim_bytes_per_s", quota*params.ElasticCeiling)
			continue
		}

		if st.quotaBytes != quota || st.params != params {
			oldQuota := st.quotaBytes
			st.quotaBytes = quota
			st.params = params
			st.bwlim = quota * params.ElasticCeiling
			st.overSecs = 0
			st.underSecs = 0
			st.state = model.StateNormal
			st.emitted = false // force re-emit of the reset value
			g.log.Info("env quota/params changed; bwlim reset to ceiling",
				"env", e.EnvID,
				"old_quota_bytes_per_s", oldQuota,
				"new_quota_bytes_per_s", quota,
				"bwlim_bytes_per_s", st.bwlim)
		}
		if !slices.Equal(st.frontends, frontends) {
			st.frontends = frontends
			st.emitted = false // new frontends must receive the current value
		}
	}

	for id := range g.envs {
		if !seen[id] {
			delete(g.envs, id)
			g.log.Info("env removed", "env", id)
		}
	}
}

// Tick runs one AIMD step per usage sample and returns one Decision per
// usage whose EnvID is configured. Unknown env IDs are ignored; configured
// envs missing from usages produce no decision (no data → hold state).
// `now` is injected for determinism; the algorithm relies on the fixed 1s
// tick cadence, not on wall-clock deltas.
func (g *Governor) Tick(now time.Time, usages []model.EnvUsage) []model.Decision {
	_ = now // persistence counters are tick counts at the fixed 1s cadence

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

// step advances one env by one tick and produces its decision.
func (g *Governor) step(st *envState, u model.EnvUsage) model.Decision {
	if u.Degraded {
		// Stale input: freeze bwlim and counters, never apply on guesses.
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

	switch {
	case m > q:
		st.overSecs++
		st.underSecs = 0
		if st.overSecs >= p.TightenAfterS {
			// Multiplicative decrease, floored at quota × TightenFloor.
			st.bwlim = math.Max(q*p.TightenFloor, st.bwlim*p.MDFactor)
			st.state = model.StateTightening
			g.log.Debug("tighten",
				"env", u.EnvID, "mean10", m, "bwlim", st.bwlim)
		}
	case m < q*p.LowWatermark:
		st.underSecs++
		st.overSecs = 0
		if st.bwlim < ceil && st.underSecs >= p.RecoverAfterS {
			// Additive increase, capped at the elastic ceiling.
			st.bwlim = math.Min(ceil, st.bwlim+q*p.AIStepFrac)
			if st.bwlim >= ceil {
				st.state = model.StateNormal
			} else {
				st.state = model.StateRecovering
			}
			g.log.Debug("recover",
				"env", u.EnvID, "mean10", m, "bwlim", st.bwlim)
		}
	default:
		// Deadband quota×LowWatermark ≤ mean10 ≤ quota: hold the current
		// value; a non-normal state is kept until bwlim returns to ceiling.
		st.overSecs = 0
		st.underSecs = 0
	}

	changed := !st.emitted || math.Abs(st.bwlim-st.lastEmitted) > changedEpsilonFrac*q
	if changed {
		st.emitted = true
		st.lastEmitted = st.bwlim
	}
	return model.Decision{
		EnvID:     u.EnvID,
		Frontends: st.frontends,
		BwlimBps:  st.bwlim,
		State:     st.state,
		Changed:   changed,
	}
}
