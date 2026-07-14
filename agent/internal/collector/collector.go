// Package collector samples HAProxy frontend statistics each second,
// differentiates the cumulative bytes_out counters into per-second rates, and
// aggregates them per environment (design doc §3.1). Fault behaviour follows
// §3.7: a failed or negative sample holds the previous instantaneous rate,
// and 10 consecutive sample failures mark the collector degraded.
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
	// window10Size backs the 10s sliding-window mean (billing/commit metric).
	window10Size = 10
	// ewma60Alpha approximates a 60s EWMA at the 1s tick cadence.
	ewma60Alpha = 2.0 / (60 + 1)
	// degradedFailureThreshold is the consecutive ShowStat failure count at
	// which the collector flags itself degraded (§3.7).
	degradedFailureThreshold = 10
	// absentTickLimit bounds baseline retention: a frontend missing from this
	// many consecutive successful samples has its counter baseline dropped.
	absentTickLimit = 60
)

// frontendState is the per-frontend counter baseline.
type frontendState struct {
	lastBytesOut uint64
	lastRate     float64 // bytes/s, reused when the counter resets
	absentTicks  int
}

// envState is the per-environment aggregation state.
type envState struct {
	window    *SlidingWindow
	ewma      *EWMA
	lastRate  float64
	lastUsage model.EnvUsage
}

// Collector turns raw frontend stats into per-env usage samples. Tick must be
// called from a single goroutine (the agent core loop); SetMapping and
// Degraded may be called concurrently from other goroutines.
type Collector struct {
	src model.StatSource
	log *slog.Logger

	mu       sync.Mutex        // guards mapping and degraded
	mapping  map[string]string // frontend name -> env id
	degraded bool

	// State below is owned exclusively by the Tick goroutine.
	failures       int
	frontends      map[string]*frontendState
	envs           map[string]*envState
	loggedUnmapped map[string]bool
}

// New builds a Collector over the given stat source. A nil logger falls back
// to slog.Default().
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

// SetMapping replaces the frontend→env mapping. The input map is copied, so
// the caller may keep mutating its own copy.
func (c *Collector) SetMapping(frontendToEnv map[string]string) {
	m := make(map[string]string, len(frontendToEnv))
	for fe, env := range frontendToEnv {
		m[fe] = env
	}
	c.mu.Lock()
	c.mapping = m
	c.mu.Unlock()
}

// Degraded reports whether sampling has failed for at least
// degradedFailureThreshold consecutive ticks.
func (c *Collector) Degraded() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.degraded
}

// Tick samples the stat source once and returns per-env usage sorted by
// EnvID. `now` is injected for determinism; the core loop calls Tick at a
// fixed 1s cadence, so a counter diff is directly a bytes/s rate.
func (c *Collector) Tick(ctx context.Context, now time.Time) []model.EnvUsage {
	_ = now // rate math relies on the fixed 1s cadence, not wall-clock deltas

	c.mu.Lock()
	mapping := c.mapping // replaced wholesale by SetMapping; safe to read after unlock
	c.mu.Unlock()

	stats, err := c.src.ShowStat(ctx)
	if err != nil {
		return c.tickFailed(err)
	}
	return c.tickOK(mapping, stats)
}

// tickFailed holds the last instantaneous rate for every known env so the
// window mean and EWMA keep moving on stale data (§3.7).
func (c *Collector) tickFailed(err error) []model.EnvUsage {
	c.failures++
	crossed := c.failures == degradedFailureThreshold
	degraded := c.failures >= degradedFailureThreshold

	c.mu.Lock()
	c.degraded = degraded
	c.mu.Unlock()

	c.log.Warn("stats sample failed; holding last rates",
		"err", err, "consecutive_failures", c.failures)
	if crossed {
		c.log.Error("collector degraded: consecutive stats sample failures reached threshold",
			"threshold", degradedFailureThreshold)
	}

	usages := make([]model.EnvUsage, 0, len(c.envs))
	for envID, st := range c.envs {
		st.window.Push(st.lastRate)
		st.ewma.Update(st.lastRate)
		u := st.lastUsage
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

func (c *Collector) tickOK(mapping map[string]string, stats []model.FrontendStat) []model.EnvUsage {
	if c.failures > 0 {
		wasDegraded := c.failures >= degradedFailureThreshold
		c.failures = 0
		c.mu.Lock()
		c.degraded = false
		c.mu.Unlock()
		if wasDegraded {
			c.log.Info("stats sampling recovered")
		}
	}

	type agg struct {
		rate float64
		conn int64
	}
	// Every env referenced by the mapping is emitted, even with no live
	// frontends this tick, so downstream consumers see a stable env set.
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
				if !c.loggedUnmapped[st.Name] {
					c.loggedUnmapped[st.Name] = true
					c.log.Debug("ignoring unmapped frontend", "frontend", st.Name)
				}
				continue
			}
			// First-ever sample: baseline only, rate 0.
			c.frontends[st.Name] = &frontendState{lastBytesOut: st.BytesOut}
			sums[envID].conn += st.ConnCur
			continue
		}
		fs.absentTicks = 0
		var rate float64
		if st.BytesOut >= fs.lastBytesOut {
			rate = float64(st.BytesOut - fs.lastBytesOut)
		} else {
			// Counter reset (HAProxy reload): hold the previous rate for this
			// tick and re-baseline (§3.7).
			rate = fs.lastRate
		}
		fs.lastBytesOut = st.BytesOut
		fs.lastRate = rate
		// A known-but-unmapped frontend still has its baseline refreshed above
		// so a later remap resumes with a correct diff.
		if mapped {
			a := sums[envID]
			a.rate += rate
			a.conn += st.ConnCur
		}
	}

	// Bound baseline retention for frontends gone from the stats output.
	for name, fs := range c.frontends {
		if present[name] {
			continue
		}
		fs.absentTicks++
		if fs.absentTicks >= absentTickLimit {
			delete(c.frontends, name)
		}
	}

	// Drop aggregation state for envs no longer in the mapping.
	for envID := range c.envs {
		if _, ok := sums[envID]; !ok {
			delete(c.envs, envID)
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
		st.window.Push(a.rate)
		st.ewma.Update(a.rate)
		st.lastRate = a.rate
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
	return usages
}

func sortUsages(us []model.EnvUsage) {
	sort.Slice(us, func(i, j int) bool { return us[i].EnvID < us[j].EnvID })
}
