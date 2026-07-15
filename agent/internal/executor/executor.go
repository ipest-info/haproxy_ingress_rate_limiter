// Package executor applies governor decisions to HAProxy's per-frontend
// bwlim map (design doc §3.2). It is switchable between dry-run (log only)
// and enforce (runtime-API map writes); switching from dry-run to enforce
// arms a one-shot resync because dry-run left HAProxy's real map state
// untouched.
package executor

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"strconv"
	"sync"

	"github.com/ipest-info/haproxy_ingress_rate_limiter/internal/model"
)

// Switchable executes decisions in dry-run or enforce mode. Apply is called
// from the agent core loop; SetMode, Mode and Snapshot may be called
// concurrently from other goroutines. The mutex is never held across
// runtime-API I/O.
type Switchable struct {
	rt      model.MapSetter
	mapPath string
	log     *slog.Logger

	mu          sync.Mutex
	mode        string
	resync      bool               // armed on switch to enforce: next non-empty Apply treats every decision as changed
	pending     map[string]bool    // env id -> a previous enforce write failed; retry on the next Apply even if unchanged
	lastApplied map[string]float64 // env id -> last applied (enforce) or logged (dry-run) aggregate bwlim, bytes/s
}

// NewSwitchable builds an executor writing to mapPath through rt. An invalid
// mode falls back to dry-run with a warning. A nil logger falls back to
// slog.Default().
func NewSwitchable(rt model.MapSetter, mapPath string, mode string, log *slog.Logger) *Switchable {
	if log == nil {
		log = slog.Default()
	}
	s := &Switchable{
		rt:          rt,
		mapPath:     mapPath,
		log:         log,
		pending:     make(map[string]bool),
		lastApplied: make(map[string]float64),
	}
	s.mode = s.validMode(mode)
	return s
}

// validMode collapses arbitrary input to a supported mode; anything but the
// two known modes degrades to dry-run (the safe direction) with a warning.
func (s *Switchable) validMode(mode string) string {
	switch mode {
	case model.ModeDryRun, model.ModeEnforce:
		return mode
	default:
		s.log.Warn("invalid executor mode; falling back to dry-run", "mode", mode)
		return model.ModeDryRun
	}
}

// SetMode switches the operating mode. An actual change to enforce arms the
// resync flag so the next Apply re-writes every decision: dry-run only
// logged, so HAProxy's map may hold stale values. A change to dry-run drops
// any pending resync and retry state (nothing real to converge when only
// logging).
func (s *Switchable) SetMode(mode string) {
	m := s.validMode(mode)
	s.mu.Lock()
	defer s.mu.Unlock()
	if m == s.mode {
		return
	}
	prev := s.mode
	s.mode = m
	s.resync = m == model.ModeEnforce
	if m == model.ModeDryRun {
		s.pending = make(map[string]bool)
	}
	s.log.Info("executor mode changed", "from", prev, "to", m)
}

// Mode returns the current operating mode.
func (s *Switchable) Mode() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.mode
}

// Snapshot returns a copy of the last applied/logged aggregate bwlim per env
// (bytes/s).
func (s *Switchable) Snapshot() map[string]float64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]float64, len(s.lastApplied))
	for env, v := range s.lastApplied {
		out[env] = v
	}
	return out
}

// Apply executes one tick's decisions. Decisions with Changed=false are
// skipped unless (a) the one-shot resync after a switch to enforce is armed,
// or (b) the env's previous enforce write failed (pending retry) — the
// governor advances its own emitted value regardless of executor success, so
// the executor must own the retry or HAProxy would keep a stale limit until
// the target next drifts. Enforce failures are joined into the returned error
// while the remaining frontends and decisions are still applied; an env is
// recorded in the snapshot only when all of its frontends were written.
//
// Decision.BwlimBps is the env's AGGREGATE budget on this node. The haproxy
// config applies each frontend's map value as that frontend's aggregate
// (further divided per stream by fe_conn), so the budget is split equally
// across the env's frontends; writing the full aggregate to every frontend
// would allow N× the quota for an env with N frontends. Equal split is the
// skeleton policy — per-frontend usage weighting belongs to the slow loop.
func (s *Switchable) Apply(ctx context.Context, ds []model.Decision) error {
	s.mu.Lock()
	mode := s.mode
	resync := s.resync
	if len(ds) > 0 {
		s.resync = false // one-shot; an empty tick must not consume it
	}
	pending := make(map[string]bool, len(s.pending))
	for env := range s.pending {
		pending[env] = true
	}
	s.mu.Unlock()

	var errs []error
	applied := make(map[string]float64)
	failed := make(map[string]bool)

	for _, d := range ds {
		if !d.Changed && !resync && !pending[d.EnvID] {
			continue
		}
		if mode == model.ModeDryRun {
			if !d.Changed && !resync {
				continue // stale pending state from a previous enforce period
			}
			s.log.Info("DRY-RUN would set bwlim",
				"env", d.EnvID,
				"state", d.State.String(),
				"bwlim_bytes_per_sec", d.BwlimBps,
				"frontends", d.Frontends)
			applied[d.EnvID] = d.BwlimBps
			continue
		}
		if len(d.Frontends) == 0 {
			s.log.Warn("decision has no frontends; nothing to apply", "env", d.EnvID)
			continue
		}
		// The map is consumed by the haproxy config's map_str_int lookup:
		// values must be integer bytes/s. Floor keeps the per-frontend sum at
		// or below the aggregate budget.
		perFrontend := math.Floor(d.BwlimBps / float64(len(d.Frontends)))
		value := strconv.FormatInt(int64(perFrontend), 10)
		ok := true
		for _, fe := range d.Frontends {
			if err := s.rt.SetMapEntry(ctx, s.mapPath, fe, value); err != nil {
				ok = false
				errs = append(errs, fmt.Errorf("set bwlim env=%s frontend=%s: %w", d.EnvID, fe, err))
			}
		}
		if ok {
			applied[d.EnvID] = d.BwlimBps
		} else {
			failed[d.EnvID] = true
		}
	}

	if len(applied) > 0 || len(failed) > 0 {
		s.mu.Lock()
		for env, v := range applied {
			s.lastApplied[env] = v
			delete(s.pending, env)
		}
		for env := range failed {
			s.pending[env] = true
		}
		s.mu.Unlock()
	}
	return errors.Join(errs...)
}
