package core

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ipest-info/haproxy_ingress_rate_limiter/internal/model"
)

// events is a buffered channel the fakes push named events into; reading from
// it doubles as synchronization with the loop goroutine.
type events chan string

func newEvents() events { return make(events, 64) }

// expect blocks for the next event and asserts its name.
func (e events) expect(t *testing.T, want string) {
	t.Helper()
	select {
	case got := <-e:
		if got != want {
			t.Fatalf("event = %q, want %q", got, want)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for event %q", want)
	}
}

type fakeCollector struct {
	ev       events
	usages   []model.EnvUsage
	mapping  map[string]string
	lastTick time.Time
	degraded bool
}

func (f *fakeCollector) SetMapping(m map[string]string) {
	f.mapping = m
	f.ev <- "collector.SetMapping"
}

func (f *fakeCollector) Tick(_ context.Context, now time.Time) []model.EnvUsage {
	f.lastTick = now
	f.ev <- "collector.Tick"
	return f.usages
}

func (f *fakeCollector) Degraded() bool { return f.degraded }

type fakeGovernor struct {
	ev        events
	decisions []model.Decision
	envs      []model.EnvQuota
	gotUsages []model.EnvUsage
	lastTick  time.Time
}

func (f *fakeGovernor) UpdateConfig(envs []model.EnvQuota) {
	f.envs = envs
	f.ev <- "governor.UpdateConfig"
}

func (f *fakeGovernor) Tick(now time.Time, usages []model.EnvUsage) []model.Decision {
	f.lastTick = now
	f.gotUsages = usages
	f.ev <- "governor.Tick"
	return f.decisions
}

type fakeExecutor struct {
	ev      events
	err     error
	mode    string
	applied [][]model.Decision
}

func (f *fakeExecutor) Apply(_ context.Context, ds []model.Decision) error {
	f.applied = append(f.applied, ds)
	f.ev <- "executor.Apply"
	return f.err
}

func (f *fakeExecutor) SetMode(mode string) {
	f.mode = mode
	f.ev <- "executor.SetMode"
}

func (f *fakeExecutor) Mode() string { return f.mode }

type harness struct {
	ev   events
	col  *fakeCollector
	gov  *fakeGovernor
	exe  *fakeExecutor
	loop *Loop
}

func newHarness(sampler func(time.Time, []model.EnvUsage, []model.Decision)) *harness {
	ev := newEvents()
	h := &harness{
		ev:  ev,
		col: &fakeCollector{ev: ev},
		gov: &fakeGovernor{ev: ev},
		exe: &fakeExecutor{ev: ev},
	}
	h.loop = New(Components{Collector: h.col, Governor: h.gov, Executor: h.exe, Sampler: sampler}, nil)
	return h
}

// expectConfigApplied consumes the three component calls of one config apply.
func (h *harness) expectConfigApplied(t *testing.T) {
	t.Helper()
	h.ev.expect(t, "governor.UpdateConfig")
	h.ev.expect(t, "collector.SetMapping")
	h.ev.expect(t, "executor.SetMode")
}

func TestSeedAppliesConfigSynchronously(t *testing.T) {
	h := newHarness(nil)
	cfg := model.ControllerConfig{
		Version: 42,
		Mode:    model.ModeEnforce,
		Envs: []model.EnvQuota{
			{EnvID: "e1", Frontends: []string{"fe1", "fe2"}, QuotaBitsPerSec: 8000},
		},
	}
	h.loop.Seed(cfg)

	if h.loop.Version() != 42 {
		t.Errorf("Version() = %d, want 42", h.loop.Version())
	}
	if len(h.gov.envs) != 1 || h.gov.envs[0].EnvID != "e1" {
		t.Errorf("governor envs = %+v", h.gov.envs)
	}
	want := map[string]string{"fe1": "e1", "fe2": "e1"}
	if len(h.col.mapping) != 2 || h.col.mapping["fe1"] != "e1" || h.col.mapping["fe2"] != "e1" {
		t.Errorf("collector mapping = %v, want %v", h.col.mapping, want)
	}
	if h.exe.mode != model.ModeEnforce {
		t.Errorf("executor mode = %q, want enforce", h.exe.mode)
	}
	h.expectConfigApplied(t) // events emitted in apply order
}

func TestSeedNormalizesMode(t *testing.T) {
	h := newHarness(nil)
	h.loop.Seed(model.ControllerConfig{Version: 1, Mode: "bogus"})
	if h.exe.mode != model.ModeDryRun {
		t.Errorf("executor mode = %q, want dry-run after Normalize", h.exe.mode)
	}
}

func TestTickPipelineOrderAndSampler(t *testing.T) {
	var sNow time.Time
	var sUsages []model.EnvUsage
	var sDecisions []model.Decision
	ev := make(chan struct{}, 1)
	h := newHarness(func(now time.Time, u []model.EnvUsage, d []model.Decision) {
		sNow, sUsages, sDecisions = now, u, d
		ev <- struct{}{}
	})
	h.col.usages = []model.EnvUsage{{EnvID: "e1", RateBps: 100}}
	h.gov.decisions = []model.Decision{{EnvID: "e1", BwlimBps: 500, Changed: true}}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tick := make(chan time.Time)
	done := make(chan struct{})
	go func() { h.loop.Run(ctx, tick, nil); close(done) }()

	now := time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC)
	tick <- now
	h.ev.expect(t, "collector.Tick")
	h.ev.expect(t, "governor.Tick")
	h.ev.expect(t, "executor.Apply")
	select {
	case <-ev:
	case <-time.After(2 * time.Second):
		t.Fatal("sampler not invoked")
	}

	if !sNow.Equal(now) || !h.col.lastTick.Equal(now) || !h.gov.lastTick.Equal(now) {
		t.Errorf("now not propagated: sampler=%v collector=%v governor=%v", sNow, h.col.lastTick, h.gov.lastTick)
	}
	if len(sUsages) != 1 || sUsages[0].EnvID != "e1" || len(h.gov.gotUsages) != 1 {
		t.Errorf("usages not piped: sampler=%v governor=%v", sUsages, h.gov.gotUsages)
	}
	if len(sDecisions) != 1 || sDecisions[0].BwlimBps != 500 {
		t.Errorf("decisions not piped to sampler: %v", sDecisions)
	}
	if len(h.exe.applied) != 1 || len(h.exe.applied[0]) != 1 {
		t.Errorf("executor applied = %v", h.exe.applied)
	}

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after ctx cancel")
	}
}

func TestConfigDrainedBeforeTick(t *testing.T) {
	h := newHarness(nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Both channels ready before Run starts: config must be applied before
	// the tick pipeline runs, whichever case the outer select picks.
	tick := make(chan time.Time, 1)
	configs := make(chan model.ControllerConfig, 1)
	configs <- model.ControllerConfig{Version: 7, Mode: model.ModeEnforce}
	tick <- time.Unix(1000, 0)

	go h.loop.Run(ctx, tick, configs)

	h.expectConfigApplied(t)
	h.ev.expect(t, "collector.Tick")
	h.ev.expect(t, "governor.Tick")
	h.ev.expect(t, "executor.Apply")

	if h.loop.Version() != 7 {
		t.Errorf("Version() = %d, want 7", h.loop.Version())
	}
	if h.exe.mode != model.ModeEnforce {
		t.Errorf("mode = %q: tick ran before config applied", h.exe.mode)
	}
}

func TestVersionTracksLatestConfig(t *testing.T) {
	h := newHarness(nil)
	h.loop.Seed(model.ControllerConfig{Version: 1})
	h.expectConfigApplied(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	configs := make(chan model.ControllerConfig, 1)
	go h.loop.Run(ctx, nil, configs)

	configs <- model.ControllerConfig{Version: 9, Mode: model.ModeDryRun}
	h.expectConfigApplied(t)
	if h.loop.Version() != 9 {
		t.Errorf("Version() = %d, want 9", h.loop.Version())
	}
}

func TestApplyErrorDoesNotStopLoop(t *testing.T) {
	h := newHarness(nil)
	h.exe.err = errors.New("socket refused")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tick := make(chan time.Time)
	go h.loop.Run(ctx, tick, nil)

	for i := 0; i < 3; i++ {
		tick <- time.Unix(int64(1000+i), 0)
		h.ev.expect(t, "collector.Tick")
		h.ev.expect(t, "governor.Tick")
		h.ev.expect(t, "executor.Apply")
	}
	if len(h.exe.applied) != 3 {
		t.Errorf("Apply called %d times, want 3", len(h.exe.applied))
	}
}

func TestClosedConfigChannelKeepsTicking(t *testing.T) {
	h := newHarness(nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tick := make(chan time.Time)
	configs := make(chan model.ControllerConfig)
	close(configs)
	go h.loop.Run(ctx, tick, configs)

	for i := 0; i < 2; i++ {
		select {
		case tick <- time.Unix(int64(2000+i), 0):
		case <-time.After(2 * time.Second):
			t.Fatal("loop stopped accepting ticks after configs closed")
		}
		h.ev.expect(t, "collector.Tick")
		h.ev.expect(t, "governor.Tick")
		h.ev.expect(t, "executor.Apply")
	}
}

func TestRunReturnsOnContextDone(t *testing.T) {
	h := newHarness(nil)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { h.loop.Run(ctx, nil, nil); close(done) }()
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return")
	}
}
