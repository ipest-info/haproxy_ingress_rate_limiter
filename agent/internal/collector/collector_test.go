package collector_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"math"
	"testing"
	"time"

	"github.com/ipest-info/haproxy_ingress_rate_limiter/agent/internal/collector"
	"github.com/ipest-info/haproxy_ingress_rate_limiter/internal/model"
)

const floatTol = 1e-9

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// fakeSource returns whatever stats/err the test set before the call.
type fakeSource struct {
	stats []model.FrontendStat
	err   error
	calls int
}

func (f *fakeSource) ShowStat(context.Context) ([]model.FrontendStat, error) {
	f.calls++
	if f.err != nil {
		return nil, f.err
	}
	return f.stats, nil
}

func fe(name string, bytesOut uint64, conn int64) model.FrontendStat {
	return model.FrontendStat{Name: name, BytesOut: bytesOut, ConnCur: conn}
}

func almostEqual(a, b float64) bool {
	return math.Abs(a-b) <= floatTol
}

func TestSlidingWindow(t *testing.T) {
	w := collector.NewSlidingWindow(3)
	if got := w.Mean(); got != 0 {
		t.Fatalf("empty Mean = %v, want 0", got)
	}
	w.Push(3)
	if got := w.Mean(); !almostEqual(got, 3) {
		t.Fatalf("Mean after 1 push = %v, want 3", got)
	}
	w.Push(6)
	if got := w.Mean(); !almostEqual(got, 4.5) {
		t.Fatalf("Mean over partial window = %v, want 4.5", got)
	}
	w.Push(9)
	if got := w.Mean(); !almostEqual(got, 6) {
		t.Fatalf("Mean over full window = %v, want 6", got)
	}
	w.Push(12) // evicts 3
	if got := w.Mean(); !almostEqual(got, 9) {
		t.Fatalf("Mean after eviction = %v, want 9", got)
	}
}

func TestEWMA(t *testing.T) {
	e := collector.NewEWMA(2.0 / 61.0)
	if got := e.Value(); got != 0 {
		t.Fatalf("Value before first update = %v, want 0", got)
	}
	e.Update(100) // first update seeds
	if got := e.Value(); !almostEqual(got, 100) {
		t.Fatalf("seeded Value = %v, want 100", got)
	}
	e.Update(161) // 100 + alpha*(161-100) = 100 + 2 = 102
	if got := e.Value(); !almostEqual(got, 102) {
		t.Fatalf("Value after update = %v, want 102", got)
	}
}

// expect is one tick's partial expectation (EWMA is exercised separately).
type expect struct {
	envID   string
	rate    float64
	mean10  float64
	conn    int64
	degrade bool
}

type step struct {
	stats []model.FrontendStat
	err   error
	want  []expect
}

func runSteps(t *testing.T, c *collector.Collector, src *fakeSource, steps []step) {
	t.Helper()
	now := time.Unix(1_700_000_000, 0)
	for i, s := range steps {
		src.stats, src.err = s.stats, s.err
		got := c.Tick(context.Background(), now.Add(time.Duration(i)*time.Second))
		if got == nil && len(s.want) > 0 {
			t.Fatalf("step %d: Tick returned nil, want %d usages", i, len(s.want))
		}
		if len(got) != len(s.want) {
			t.Fatalf("step %d: got %d usages (%+v), want %d", i, len(got), got, len(s.want))
		}
		for j, w := range s.want {
			u := got[j]
			if u.EnvID != w.envID {
				t.Errorf("step %d usage %d: EnvID = %q, want %q", i, j, u.EnvID, w.envID)
			}
			if !almostEqual(u.RateBps, w.rate) {
				t.Errorf("step %d usage %d (%s): RateBps = %v, want %v", i, j, w.envID, u.RateBps, w.rate)
			}
			if !almostEqual(u.Mean10Bps, w.mean10) {
				t.Errorf("step %d usage %d (%s): Mean10Bps = %v, want %v", i, j, w.envID, u.Mean10Bps, w.mean10)
			}
			if u.ConnCur != w.conn {
				t.Errorf("step %d usage %d (%s): ConnCur = %d, want %d", i, j, w.envID, u.ConnCur, w.conn)
			}
			if u.Degraded != w.degrade {
				t.Errorf("step %d usage %d (%s): Degraded = %v, want %v", i, j, w.envID, u.Degraded, w.degrade)
			}
		}
	}
}

func TestCollectorScenarios(t *testing.T) {
	cases := []struct {
		name    string
		mapping map[string]string
		steps   []step
	}{
		{
			name:    "first sample is baseline only",
			mapping: map[string]string{"fe1": "envA"},
			steps: []step{
				{
					stats: []model.FrontendStat{fe("fe1", 1000, 3)},
					want:  []expect{{envID: "envA", rate: 0, mean10: 0, conn: 3}},
				},
			},
		},
		{
			name:    "steady rate and window mean",
			mapping: map[string]string{"fe1": "envA"},
			steps: []step{
				{
					// Baseline-only tick: rate unknown, window untouched.
					stats: []model.FrontendStat{fe("fe1", 0, 1)},
					want:  []expect{{envID: "envA", rate: 0, mean10: 0, conn: 1}},
				},
				{
					stats: []model.FrontendStat{fe("fe1", 100, 1)},
					want:  []expect{{envID: "envA", rate: 100, mean10: 100, conn: 1}},
				},
				{
					stats: []model.FrontendStat{fe("fe1", 200, 1)},
					want:  []expect{{envID: "envA", rate: 100, mean10: 100, conn: 1}},
				},
				{
					stats: []model.FrontendStat{fe("fe1", 300, 1)},
					want:  []expect{{envID: "envA", rate: 100, mean10: 100, conn: 1}},
				},
			},
		},
		{
			name:    "counter reset holds previous rate and re-baselines",
			mapping: map[string]string{"fe1": "envA"},
			steps: []step{
				{
					stats: []model.FrontendStat{fe("fe1", 1000, 1)},
					want:  []expect{{envID: "envA", rate: 0, mean10: 0, conn: 1}},
				},
				{
					stats: []model.FrontendStat{fe("fe1", 1100, 1)},
					want:  []expect{{envID: "envA", rate: 100, mean10: 100, conn: 1}},
				},
				{
					// HAProxy reload: counter drops to 50. Rate held at 100,
					// new baseline stored.
					stats: []model.FrontendStat{fe("fe1", 50, 1)},
					want:  []expect{{envID: "envA", rate: 100, mean10: 100, conn: 1}},
				},
				{
					// Diff resumes from the new baseline: 150-50 = 100.
					stats: []model.FrontendStat{fe("fe1", 150, 1)},
					want:  []expect{{envID: "envA", rate: 100, mean10: 100, conn: 1}},
				},
			},
		},
		{
			name:    "multi-frontend env sums rates and connections",
			mapping: map[string]string{"fe1": "envA", "fe2": "envA"},
			steps: []step{
				{
					stats: []model.FrontendStat{fe("fe1", 0, 2), fe("fe2", 0, 3)},
					want:  []expect{{envID: "envA", rate: 0, mean10: 0, conn: 5}},
				},
				{
					stats: []model.FrontendStat{fe("fe1", 100, 2), fe("fe2", 50, 3)},
					want:  []expect{{envID: "envA", rate: 150, mean10: 150, conn: 5}},
				},
			},
		},
		{
			name:    "output sorted by env id",
			mapping: map[string]string{"fe-b": "envB", "fe-a": "envA"},
			steps: []step{
				{
					stats: []model.FrontendStat{fe("fe-b", 0, 1), fe("fe-a", 0, 2)},
					want: []expect{
						{envID: "envA", rate: 0, mean10: 0, conn: 2},
						{envID: "envB", rate: 0, mean10: 0, conn: 1},
					},
				},
				{
					stats: []model.FrontendStat{fe("fe-b", 200, 1), fe("fe-a", 100, 2)},
					want: []expect{
						{envID: "envA", rate: 100, mean10: 100, conn: 2},
						{envID: "envB", rate: 200, mean10: 200, conn: 1},
					},
				},
			},
		},
		{
			name:    "unmapped frontend ignored",
			mapping: map[string]string{"fe1": "envA"},
			steps: []step{
				{
					stats: []model.FrontendStat{fe("fe1", 0, 1), fe("stray", 0, 9)},
					want:  []expect{{envID: "envA", rate: 0, mean10: 0, conn: 1}},
				},
				{
					stats: []model.FrontendStat{fe("fe1", 100, 1), fe("stray", 5000, 9)},
					want:  []expect{{envID: "envA", rate: 100, mean10: 100, conn: 1}},
				},
			},
		},
		{
			name:    "mapped env with no live frontend reports zero",
			mapping: map[string]string{"fe1": "envA", "ghost": "envB"},
			steps: []step{
				{
					stats: []model.FrontendStat{fe("fe1", 0, 1)},
					want: []expect{
						{envID: "envA", rate: 0, mean10: 0, conn: 1},
						{envID: "envB", rate: 0, mean10: 0, conn: 0},
					},
				},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			src := &fakeSource{}
			c := collector.New(src, quietLogger())
			c.SetMapping(tc.mapping)
			runSteps(t, c, src, tc.steps)
		})
	}
}

func TestCollectorFailureHoldAndDegraded(t *testing.T) {
	src := &fakeSource{}
	c := collector.New(src, quietLogger())
	c.SetMapping(map[string]string{"fe1": "envA"})
	ctx := context.Background()
	now := time.Unix(1_700_000_000, 0)

	// Establish a 100 B/s rate.
	src.stats = []model.FrontendStat{fe("fe1", 0, 2)}
	c.Tick(ctx, now)
	src.stats = []model.FrontendStat{fe("fe1", 100, 2)}
	c.Tick(ctx, now.Add(time.Second))

	// 10 consecutive failures: rate held, means keep moving, degraded at 10.
	src.err = errors.New("socket timeout")
	for i := 1; i <= 10; i++ {
		got := c.Tick(ctx, now.Add(time.Duration(1+i)*time.Second))
		if got == nil {
			t.Fatalf("failure %d: Tick returned nil slice with known envs", i)
		}
		if len(got) != 1 {
			t.Fatalf("failure %d: got %d usages, want 1", i, len(got))
		}
		u := got[0]
		if u.EnvID != "envA" {
			t.Fatalf("failure %d: EnvID = %q, want envA", i, u.EnvID)
		}
		if !almostEqual(u.RateBps, 100) {
			t.Errorf("failure %d: RateBps = %v, want held 100", i, u.RateBps)
		}
		if u.ConnCur != 2 {
			t.Errorf("failure %d: ConnCur = %d, want held 2", i, u.ConnCur)
		}
		wantDegraded := i >= 10
		if u.Degraded != wantDegraded {
			t.Errorf("failure %d: Degraded = %v, want %v", i, u.Degraded, wantDegraded)
		}
		if c.Degraded() != wantDegraded {
			t.Errorf("failure %d: Degraded() = %v, want %v", i, c.Degraded(), wantDegraded)
		}
		if i == 10 && !almostEqual(u.Mean10Bps, 100) {
			// Window: [0, 100] + 10 held pushes of 100; the initial 0 has
			// been evicted, so the mean converged to the held rate.
			t.Errorf("failure %d: Mean10Bps = %v, want 100", i, u.Mean10Bps)
		}
	}

	// Recovery clears failure state and the degraded flag.
	src.err = nil
	src.stats = []model.FrontendStat{fe("fe1", 200, 2)}
	got := c.Tick(ctx, now.Add(12*time.Second))
	if len(got) != 1 {
		t.Fatalf("recovery: got %d usages, want 1", len(got))
	}
	if got[0].Degraded {
		t.Error("recovery: usage still flagged degraded")
	}
	if c.Degraded() {
		t.Error("recovery: Degraded() still true")
	}
	if !almostEqual(got[0].RateBps, 100) {
		t.Errorf("recovery: RateBps = %v, want 100", got[0].RateBps)
	}
}

func TestCollectorFailureBeforeAnySuccess(t *testing.T) {
	src := &fakeSource{err: errors.New("down")}
	c := collector.New(src, quietLogger())
	c.SetMapping(map[string]string{"fe1": "envA"})
	got := c.Tick(context.Background(), time.Unix(1_700_000_000, 0))
	if len(got) != 0 {
		t.Fatalf("got %d usages, want 0 (no envs known yet)", len(got))
	}
}

func TestCollectorMappingSwapMidStream(t *testing.T) {
	src := &fakeSource{}
	c := collector.New(src, quietLogger())
	ctx := context.Background()
	now := time.Unix(1_700_000_000, 0)

	c.SetMapping(map[string]string{"fe1": "envA"})
	src.stats = []model.FrontendStat{fe("fe1", 0, 1)}
	c.Tick(ctx, now)
	src.stats = []model.FrontendStat{fe("fe1", 100, 1)}
	got := c.Tick(ctx, now.Add(time.Second))
	if len(got) != 1 || got[0].EnvID != "envA" || !almostEqual(got[0].RateBps, 100) {
		t.Fatalf("pre-swap: got %+v, want envA at 100 B/s", got)
	}

	// Remap fe1 to envB. The frontend baseline survives the swap, so the
	// very next tick reports the true diff rate, not a fresh baseline zero.
	c.SetMapping(map[string]string{"fe1": "envB"})
	src.stats = []model.FrontendStat{fe("fe1", 200, 1)}
	got = c.Tick(ctx, now.Add(2*time.Second))
	if len(got) != 1 {
		t.Fatalf("post-swap: got %d usages, want 1", len(got))
	}
	if got[0].EnvID != "envB" {
		t.Fatalf("post-swap: EnvID = %q, want envB", got[0].EnvID)
	}
	if !almostEqual(got[0].RateBps, 100) {
		t.Errorf("post-swap: RateBps = %v, want 100 (baseline preserved)", got[0].RateBps)
	}
	// envB aggregation state is new: its window holds a single sample.
	if !almostEqual(got[0].Mean10Bps, 100) {
		t.Errorf("post-swap: Mean10Bps = %v, want 100 (fresh env window)", got[0].Mean10Bps)
	}
}

func TestSetMappingCopiesInput(t *testing.T) {
	src := &fakeSource{}
	c := collector.New(src, quietLogger())
	m := map[string]string{"fe1": "envA"}
	c.SetMapping(m)
	m["fe1"] = "envZ" // caller mutation must not leak in

	src.stats = []model.FrontendStat{fe("fe1", 0, 1)}
	got := c.Tick(context.Background(), time.Unix(1_700_000_000, 0))
	if len(got) != 1 || got[0].EnvID != "envA" {
		t.Fatalf("got %+v, want single usage for envA", got)
	}
}

func TestCollectorBaselineDropAfterLongAbsence(t *testing.T) {
	src := &fakeSource{}
	c := collector.New(src, quietLogger())
	c.SetMapping(map[string]string{"fe1": "envA", "fe2": "envB"})
	ctx := context.Background()
	now := time.Unix(1_700_000_000, 0)
	tick := 0
	next := func(stats ...model.FrontendStat) []model.EnvUsage {
		src.stats = stats
		tick++
		return c.Tick(ctx, now.Add(time.Duration(tick)*time.Second))
	}

	next(fe("fe1", 0, 1), fe("fe2", 1000, 1))
	got := next(fe("fe1", 100, 1), fe("fe2", 1100, 1))
	if !almostEqual(got[1].RateBps, 100) {
		t.Fatalf("setup: envB RateBps = %v, want 100", got[1].RateBps)
	}

	// fe2 vanishes for 60 consecutive successful ticks: its env reads zero
	// and its counter baseline is eventually dropped.
	var b uint64 = 100
	for i := 0; i < 60; i++ {
		b += 100
		got = next(fe("fe1", b, 1))
		if len(got) != 2 {
			t.Fatalf("absence tick %d: got %d usages, want 2", i, len(got))
		}
		if !almostEqual(got[1].RateBps, 0) {
			t.Fatalf("absence tick %d: envB RateBps = %v, want 0", i, got[1].RateBps)
		}
	}

	// fe2 reappears with a huge counter. A retained stale baseline would
	// produce a massive bogus rate; a dropped one yields a fresh baseline.
	got = next(fe("fe1", b+100, 1), fe("fe2", 99_000_000, 1))
	if !almostEqual(got[1].RateBps, 0) {
		t.Errorf("reappearance: envB RateBps = %v, want 0 (baseline was dropped)", got[1].RateBps)
	}
}

func TestCollectorShortAbsenceKeepsBaseline(t *testing.T) {
	src := &fakeSource{}
	c := collector.New(src, quietLogger())
	c.SetMapping(map[string]string{"fe1": "envA"})
	ctx := context.Background()
	now := time.Unix(1_700_000_000, 0)

	src.stats = []model.FrontendStat{fe("fe1", 1000, 1)}
	c.Tick(ctx, now)
	src.stats = []model.FrontendStat{fe("fe1", 1100, 1)}
	c.Tick(ctx, now.Add(time.Second))

	// Absent for a few ticks (below the drop limit).
	src.stats = nil
	for i := 0; i < 5; i++ {
		c.Tick(ctx, now.Add(time.Duration(2+i)*time.Second))
	}

	// Baseline retained: the diff picks up from the old counter value.
	src.stats = []model.FrontendStat{fe("fe1", 1600, 1)}
	got := c.Tick(ctx, now.Add(8*time.Second))
	if len(got) != 1 {
		t.Fatalf("got %d usages, want 1", len(got))
	}
	if !almostEqual(got[0].RateBps, 500) {
		t.Errorf("RateBps = %v, want 500 (baseline kept across short absence)", got[0].RateBps)
	}
}

func TestCollectorEwmaThroughTicks(t *testing.T) {
	src := &fakeSource{}
	c := collector.New(src, quietLogger())
	c.SetMapping(map[string]string{"fe1": "envA"})
	ctx := context.Background()
	now := time.Unix(1_700_000_000, 0)

	// Tick 1 is baseline-only: the EWMA stays unseeded (rate unknown, not 0).
	src.stats = []model.FrontendStat{fe("fe1", 0, 1)}
	got := c.Tick(ctx, now)
	if !almostEqual(got[0].Ewma60Bps, 0) {
		t.Fatalf("tick 1: Ewma60Bps = %v, want 0 (unseeded)", got[0].Ewma60Bps)
	}

	// Tick 2 rate 61 is the first real measurement: it SEEDS the EWMA at 61
	// instead of averaging against a phantom 0 from the baseline tick.
	src.stats = []model.FrontendStat{fe("fe1", 61, 1)}
	got = c.Tick(ctx, now.Add(time.Second))
	if !almostEqual(got[0].Ewma60Bps, 61) {
		t.Fatalf("tick 2: Ewma60Bps = %v, want 61 (seeded by first measurement)", got[0].Ewma60Bps)
	}

	// A failed tick keeps the EWMA moving on the held rate (61 → stays 61).
	src.err = errors.New("timeout")
	got = c.Tick(ctx, now.Add(2*time.Second))
	if !almostEqual(got[0].Ewma60Bps, 61) {
		t.Fatalf("failed tick: Ewma60Bps = %v, want 61 (held rate)", got[0].Ewma60Bps)
	}
}

func TestCollectorNilLoggerDefaults(t *testing.T) {
	src := &fakeSource{stats: []model.FrontendStat{fe("fe1", 0, 1)}}
	c := collector.New(src, nil)
	c.SetMapping(map[string]string{"fe1": "envA"})
	got := c.Tick(context.Background(), time.Unix(1_700_000_000, 0))
	if len(got) != 1 {
		t.Fatalf("got %d usages, want 1", len(got))
	}
}
