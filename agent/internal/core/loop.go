// Package core runs the rl-agent 1s control loop: sample usage, decide
// shaping values, apply them, and hand a sample to the reporter. The loop is
// deterministic — time enters only through the injected tick channel.
package core

import (
	"context"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/ipest-info/haproxy_ingress_rate_limiter/internal/model"
)

// Local interfaces mirror the component contracts so tests can fake every
// dependency without importing the concrete packages.

type collectorIface interface {
	SetMapping(frontendToEnv map[string]string)
	Tick(ctx context.Context, now time.Time) []model.EnvUsage
	Degraded() bool
}

type governorIface interface {
	UpdateConfig(envs []model.EnvQuota)
	Tick(now time.Time, usages []model.EnvUsage) []model.Decision
}

type executorIface interface {
	Apply(ctx context.Context, ds []model.Decision) error
	SetMode(mode string)
	Mode() string
}

// Components wires the loop's dependencies. Sampler is optional; when set it
// receives the outcome of every tick (used by the reporter, or for standalone
// debug summaries).
type Components struct {
	Collector collectorIface
	Governor  governorIface
	Executor  executorIface
	Sampler   func(now time.Time, usages []model.EnvUsage, decisions []model.Decision)
}

// Loop is the agent's fast control loop.
type Loop struct {
	c       Components
	log     *slog.Logger
	version atomic.Int64 // last applied ControllerConfig version
}

// New builds a Loop. A nil logger falls back to slog.Default().
func New(c Components, log *slog.Logger) *Loop {
	if log == nil {
		log = slog.Default()
	}
	return &Loop{c: c, log: log}
}

// Version returns the version of the last applied config (0 before any).
func (l *Loop) Version() int64 {
	return l.version.Load()
}

// Seed applies an initial config synchronously, before Run. Used for
// standalone quotas, bootstrap envs, and the fail-static cache.
func (l *Loop) Seed(cfg model.ControllerConfig) {
	l.applyConfig(cfg)
}

// Run drives the loop until ctx is done. Both channels are injected: tick is
// typically a 1s time.Ticker channel; configs delivers controller pushes and
// may be nil in standalone mode (a nil channel never fires). A pending config
// is always applied before the tick that follows it is processed.
func (l *Loop) Run(ctx context.Context, tick <-chan time.Time, configs <-chan model.ControllerConfig) {
	for {
		select {
		case <-ctx.Done():
			return
		case cfg, ok := <-configs:
			if !ok {
				configs = nil // closed: stop selecting on it
				continue
			}
			l.applyConfig(cfg)
		case now := <-tick:
			// Config takes priority when both channels are ready: drain any
			// pending config so this tick runs against the newest state.
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

func (l *Loop) applyConfig(cfg model.ControllerConfig) {
	cfg.Normalize()
	l.c.Governor.UpdateConfig(cfg.Envs)
	l.c.Collector.SetMapping(cfg.FrontendToEnv())
	l.c.Executor.SetMode(cfg.Mode)
	l.version.Store(cfg.Version)
	l.log.Info("config applied", "version", cfg.Version, "mode", cfg.Mode, "envs", len(cfg.Envs))
}

func (l *Loop) tick(ctx context.Context, now time.Time) {
	usages := l.c.Collector.Tick(ctx, now)
	ds := l.c.Governor.Tick(now, usages)
	// Apply failures must never stop the loop: HAProxy may be reloading; the
	// next tick retries with fresh decisions.
	if err := l.c.Executor.Apply(ctx, ds); err != nil {
		l.log.Error("executor apply failed", "err", err, "degraded", l.c.Collector.Degraded())
	}
	if l.c.Sampler != nil {
		l.c.Sampler(now, usages, ds)
	}
}
