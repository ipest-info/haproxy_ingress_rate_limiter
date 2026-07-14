package governor

import (
	"io"
	"log/slog"
	"math"
	"testing"
	"time"

	"github.com/ipest-info/haproxy_ingress_rate_limiter/internal/model"
)

const (
	// 80 Mbit/s quota → 10 MB/s internal.
	quotaBits  = int64(80_000_000)
	quotaBytes = 10_000_000.0
)

func newGovernor() *Governor {
	return New(slog.New(slog.NewTextHandler(io.Discard, nil)))
}

func envQuota(id string, bits int64, params *model.GovParams, frontends ...string) model.EnvQuota {
	return model.EnvQuota{
		EnvID:           id,
		Frontends:       frontends,
		QuotaBitsPerSec: bits,
		Params:          params,
	}
}

func usage(envID string, mean10 float64) model.EnvUsage {
	return model.EnvUsage{EnvID: envID, RateBps: mean10, Mean10Bps: mean10}
}

func approx(a, b float64) bool {
	return math.Abs(a-b) <= 1e-6*math.Max(1, math.Abs(b))
}

// tick runs one Tick with a single usage and requires exactly one decision.
func tick(t *testing.T, g *Governor, now time.Time, u model.EnvUsage) model.Decision {
	t.Helper()
	ds := g.Tick(now, []model.EnvUsage{u})
	if len(ds) != 1 {
		t.Fatalf("Tick returned %d decisions, want 1", len(ds))
	}
	return ds[0]
}

// TestSustainedOverloadConvergesToFloor: mean10 held at 1.3×q must drive
// bwlim from the ceiling down to quota×TightenFloor within TightenAfterS
// plus a few ticks, and never below it.
func TestSustainedOverloadConvergesToFloor(t *testing.T) {
	g := newGovernor()
	g.UpdateConfig([]model.EnvQuota{envQuota("env-a", quotaBits, nil, "fe_a")})

	p := model.DefaultGovParams()
	floor := quotaBytes * p.TightenFloor // 0.95q
	ceil := quotaBytes * p.ElasticCeiling
	now := time.Unix(1_700_000_000, 0)

	reachedFloorAt := 0
	for i := 1; i <= 30; i++ {
		d := tick(t, g, now, usage("env-a", 1.3*quotaBytes))
		now = now.Add(time.Second)

		if d.BwlimBps < floor-1e-6 {
			t.Fatalf("tick %d: bwlim %.1f fell below floor %.1f", i, d.BwlimBps, floor)
		}
		if i < p.TightenAfterS {
			// Persistence not reached yet: still parked at the ceiling.
			if !approx(d.BwlimBps, ceil) {
				t.Fatalf("tick %d: bwlim %.1f, want ceiling %.1f before TightenAfterS", i, d.BwlimBps, ceil)
			}
		}
		if reachedFloorAt == 0 && approx(d.BwlimBps, floor) {
			reachedFloorAt = i
			if d.State != model.StateTightening {
				t.Fatalf("tick %d: state %v, want tightening", i, d.State)
			}
		}
		if reachedFloorAt != 0 && i > reachedFloorAt {
			if !approx(d.BwlimBps, floor) {
				t.Fatalf("tick %d: bwlim %.1f, want to stay at floor %.1f", i, d.BwlimBps, floor)
			}
			if d.Changed {
				t.Fatalf("tick %d: Changed=true while parked at floor", i)
			}
		}
	}
	// ceil×0.9 = 0.99q on tick 3, then floored on tick 4 with defaults.
	if reachedFloorAt == 0 || reachedFloorAt > p.TightenAfterS+3 {
		t.Fatalf("floor reached at tick %d, want within TightenAfterS(%d)+3", reachedFloorAt, p.TightenAfterS)
	}
}

// TestDeadbandHoldsValue: 100 ticks inside the deadband produce exactly one
// Changed decision (the first emit) and no value movement.
func TestDeadbandHoldsValue(t *testing.T) {
	g := newGovernor()
	g.UpdateConfig([]model.EnvQuota{envQuota("env-a", quotaBits, nil, "fe_a")})

	ceil := quotaBytes * model.DefaultGovParams().ElasticCeiling
	now := time.Unix(1_700_000_000, 0)
	changedCount := 0
	for i := 1; i <= 100; i++ {
		d := tick(t, g, now, usage("env-a", 0.95*quotaBytes))
		now = now.Add(time.Second)
		if d.Changed {
			changedCount++
		}
		if !approx(d.BwlimBps, ceil) {
			t.Fatalf("tick %d: bwlim %.1f, want ceiling %.1f", i, d.BwlimBps, ceil)
		}
		if d.State != model.StateNormal {
			t.Fatalf("tick %d: state %v, want normal", i, d.State)
		}
	}
	if changedCount != 1 {
		t.Fatalf("Changed decisions = %d, want exactly 1 (first emit)", changedCount)
	}
}

// TestRecoveryRampsAndParksAtCeiling: after tightening to the floor,
// sustained low usage must first wait RecoverAfterS ticks, then ramp by
// quota×AIStepFrac per tick, and park at the ceiling in StateNormal.
func TestRecoveryRampsAndParksAtCeiling(t *testing.T) {
	g := newGovernor()
	g.UpdateConfig([]model.EnvQuota{envQuota("env-a", quotaBits, nil, "fe_a")})

	p := model.DefaultGovParams()
	floor := quotaBytes * p.TightenFloor
	ceil := quotaBytes * p.ElasticCeiling
	step := quotaBytes * p.AIStepFrac
	now := time.Unix(1_700_000_000, 0)

	// Drive to the floor.
	var d model.Decision
	for i := 0; i < p.TightenAfterS+3; i++ {
		d = tick(t, g, now, usage("env-a", 1.3*quotaBytes))
		now = now.Add(time.Second)
	}
	if !approx(d.BwlimBps, floor) {
		t.Fatalf("setup: bwlim %.1f, want floor %.1f", d.BwlimBps, floor)
	}

	// A deadband interlude holds the tightened value and keeps the state.
	for i := 0; i < 3; i++ {
		d = tick(t, g, now, usage("env-a", 0.95*quotaBytes))
		now = now.Add(time.Second)
		if !approx(d.BwlimBps, floor) || d.Changed {
			t.Fatalf("deadband hold: bwlim %.1f changed=%v, want floor %.1f unchanged", d.BwlimBps, d.Changed, floor)
		}
		if d.State != model.StateTightening {
			t.Fatalf("deadband hold: state %v, want tightening kept", d.State)
		}
	}

	// Low usage: no movement until RecoverAfterS is reached.
	low := 0.5 * quotaBytes
	for i := 1; i < p.RecoverAfterS; i++ {
		d = tick(t, g, now, usage("env-a", low))
		now = now.Add(time.Second)
		if !approx(d.BwlimBps, floor) || d.Changed {
			t.Fatalf("pre-recovery tick %d: bwlim %.1f changed=%v, want floor held", i, d.BwlimBps, d.Changed)
		}
	}

	// Ramp: +step per tick until the ceiling, exact increments.
	prev := floor
	for i := 0; ; i++ {
		if i > 20 {
			t.Fatal("recovery did not reach the ceiling within 20 ticks")
		}
		d = tick(t, g, now, usage("env-a", low))
		now = now.Add(time.Second)
		want := math.Min(ceil, prev+step)
		if !approx(d.BwlimBps, want) {
			t.Fatalf("recovery tick %d: bwlim %.1f, want %.1f", i, d.BwlimBps, want)
		}
		if !d.Changed {
			t.Fatalf("recovery tick %d: Changed=false during ramp", i)
		}
		prev = d.BwlimBps
		if approx(d.BwlimBps, ceil) {
			if d.State != model.StateNormal {
				t.Fatalf("at ceiling: state %v, want normal", d.State)
			}
			break
		}
		if d.State != model.StateRecovering {
			t.Fatalf("recovery tick %d: state %v, want recovering", i, d.State)
		}
	}

	// Parked: further low ticks change nothing.
	for i := 0; i < 5; i++ {
		d = tick(t, g, now, usage("env-a", low))
		now = now.Add(time.Second)
		if !approx(d.BwlimBps, ceil) || d.Changed || d.State != model.StateNormal {
			t.Fatalf("parked tick %d: bwlim %.1f changed=%v state=%v, want ceiling unchanged normal", i, d.BwlimBps, d.Changed, d.State)
		}
	}
}

// TestQuotaChangeResetsToNewCeiling: a quota update resets bwlim to the new
// ceiling, clears persistence counters, and re-emits Changed.
func TestQuotaChangeResetsToNewCeiling(t *testing.T) {
	g := newGovernor()
	g.UpdateConfig([]model.EnvQuota{envQuota("env-a", quotaBits, nil, "fe_a")})

	p := model.DefaultGovParams()
	now := time.Unix(1_700_000_000, 0)

	// Tighten to the floor under the old quota.
	for i := 0; i < p.TightenAfterS+3; i++ {
		tick(t, g, now, usage("env-a", 1.3*quotaBytes))
		now = now.Add(time.Second)
	}
	// Two extra over-quota ticks so overSecs is mid-count when config changes.
	// (Counters are already saturated; the reset proof is below.)

	newQuotaBytes := 2 * quotaBytes
	g.UpdateConfig([]model.EnvQuota{envQuota("env-a", 2*quotaBits, nil, "fe_a")})

	newCeil := newQuotaBytes * p.ElasticCeiling
	// Overload against the new quota: counters must start from zero, so the
	// first TightenAfterS-1 ticks stay at the new ceiling.
	for i := 1; i < p.TightenAfterS; i++ {
		d := tick(t, g, now, usage("env-a", 1.3*newQuotaBytes))
		now = now.Add(time.Second)
		if !approx(d.BwlimBps, newCeil) {
			t.Fatalf("tick %d after quota change: bwlim %.1f, want new ceiling %.1f", i, d.BwlimBps, newCeil)
		}
		if i == 1 {
			if !d.Changed {
				t.Fatal("first decision after quota change must be Changed")
			}
			if d.State != model.StateNormal {
				t.Fatalf("first decision after quota change: state %v, want normal", d.State)
			}
		}
	}
	// Counter reaches TightenAfterS only now → first tighten.
	d := tick(t, g, now, usage("env-a", 1.3*newQuotaBytes))
	want := math.Max(newQuotaBytes*p.TightenFloor, newCeil*p.MDFactor)
	if !approx(d.BwlimBps, want) || d.State != model.StateTightening {
		t.Fatalf("post-reset tighten: bwlim %.1f state=%v, want %.1f tightening", d.BwlimBps, d.State, want)
	}
}

// TestUnchangedConfigKeepsState: re-applying an identical config must not
// reset the tightened value.
func TestUnchangedConfigKeepsState(t *testing.T) {
	g := newGovernor()
	cfg := []model.EnvQuota{envQuota("env-a", quotaBits, nil, "fe_a")}
	g.UpdateConfig(cfg)

	p := model.DefaultGovParams()
	now := time.Unix(1_700_000_000, 0)
	for i := 0; i < p.TightenAfterS+3; i++ {
		tick(t, g, now, usage("env-a", 1.3*quotaBytes))
		now = now.Add(time.Second)
	}

	g.UpdateConfig(cfg)
	d := tick(t, g, now, usage("env-a", 0.95*quotaBytes))
	floor := quotaBytes * p.TightenFloor
	if !approx(d.BwlimBps, floor) || d.Changed {
		t.Fatalf("after identical config: bwlim %.1f changed=%v, want floor %.1f unchanged", d.BwlimBps, d.Changed, floor)
	}
}

// TestDegradedFreezes: degraded usages must not move bwlim, must not advance
// or reset counters, and must emit Changed=false — even on the first tick.
func TestDegradedFreezes(t *testing.T) {
	g := newGovernor()
	g.UpdateConfig([]model.EnvQuota{envQuota("env-a", quotaBits, nil, "fe_a")})

	p := model.DefaultGovParams()
	ceil := quotaBytes * p.ElasticCeiling
	now := time.Unix(1_700_000_000, 0)

	// First-ever tick degraded: no emit of a first value.
	du := usage("env-a", 1.3*quotaBytes)
	du.Degraded = true
	d := tick(t, g, now, du)
	now = now.Add(time.Second)
	if d.Changed || !approx(d.BwlimBps, ceil) {
		t.Fatalf("first degraded tick: bwlim %.1f changed=%v, want ceiling frozen unchanged", d.BwlimBps, d.Changed)
	}

	// TightenAfterS-1 real overload ticks: counter one short of tightening.
	for i := 1; i < p.TightenAfterS; i++ {
		d = tick(t, g, now, usage("env-a", 1.3*quotaBytes))
		now = now.Add(time.Second)
	}
	if !approx(d.BwlimBps, ceil) {
		t.Fatalf("setup: bwlim %.1f moved before TightenAfterS", d.BwlimBps)
	}

	// Degraded interlude: frozen value, frozen counters.
	for i := 0; i < 5; i++ {
		d = tick(t, g, now, du)
		now = now.Add(time.Second)
		if d.Changed || !approx(d.BwlimBps, ceil) || d.State != model.StateNormal {
			t.Fatalf("degraded tick %d: bwlim %.1f changed=%v state=%v, want frozen ceiling", i, d.BwlimBps, d.Changed, d.State)
		}
	}

	// One more real overload tick completes the persistence count exactly:
	// proves the degraded ticks neither reset nor advanced overSecs.
	d = tick(t, g, now, usage("env-a", 1.3*quotaBytes))
	want := math.Max(quotaBytes*p.TightenFloor, ceil*p.MDFactor)
	if !approx(d.BwlimBps, want) || d.State != model.StateTightening || !d.Changed {
		t.Fatalf("post-degraded tick: bwlim %.1f state=%v changed=%v, want tighten to %.1f", d.BwlimBps, d.State, d.Changed, want)
	}
}

// TestPerEnvParamOverride: an env-level GovParams override must drive every
// threshold and step of the algorithm.
func TestPerEnvParamOverride(t *testing.T) {
	over := &model.GovParams{
		ElasticCeiling: 1.2,
		LowWatermark:   0.8,
		TightenAfterS:  1,
		RecoverAfterS:  2,
		MDFactor:       0.5,
		TightenFloor:   0.8,
		AIStepFrac:     0.1,
	}
	g := newGovernor()
	g.UpdateConfig([]model.EnvQuota{envQuota("env-a", quotaBits, over, "fe_a")})
	now := time.Unix(1_700_000_000, 0)

	// Overridden ceiling.
	d := tick(t, g, now, usage("env-a", 0.9*quotaBytes)) // deadband: 0.8q ≤ m ≤ q
	now = now.Add(time.Second)
	if !approx(d.BwlimBps, 1.2*quotaBytes) {
		t.Fatalf("initial bwlim %.1f, want overridden ceiling %.1f", d.BwlimBps, 1.2*quotaBytes)
	}

	// TightenAfterS=1: a single over-quota tick tightens immediately;
	// MDFactor=0.5 undershoots, so TightenFloor=0.8 clamps.
	d = tick(t, g, now, usage("env-a", 1.5*quotaBytes))
	now = now.Add(time.Second)
	if !approx(d.BwlimBps, 0.8*quotaBytes) || d.State != model.StateTightening {
		t.Fatalf("override tighten: bwlim %.1f state=%v, want %.1f tightening", d.BwlimBps, d.State, 0.8*quotaBytes)
	}

	// RecoverAfterS=2 and AIStepFrac=0.1: second low tick ramps by 0.1q.
	low := usage("env-a", 0.5*quotaBytes) // below 0.8q watermark
	d = tick(t, g, now, low)
	now = now.Add(time.Second)
	if !approx(d.BwlimBps, 0.8*quotaBytes) || d.Changed {
		t.Fatalf("override pre-recovery: bwlim %.1f changed=%v, want hold", d.BwlimBps, d.Changed)
	}
	d = tick(t, g, now, low)
	if !approx(d.BwlimBps, 0.9*quotaBytes) || d.State != model.StateRecovering {
		t.Fatalf("override recovery step: bwlim %.1f state=%v, want %.1f recovering", d.BwlimBps, d.State, 0.9*quotaBytes)
	}
}

// TestEnvScoping: unknown usages are ignored, configured envs without usage
// emit nothing, removed envs stop emitting, and decisions carry the
// configured frontends.
func TestEnvScoping(t *testing.T) {
	g := newGovernor()
	g.UpdateConfig([]model.EnvQuota{
		envQuota("env-a", quotaBits, nil, "fe_a1", "fe_a2"),
		envQuota("env-b", quotaBits, nil, "fe_b"),
	})
	now := time.Unix(1_700_000_000, 0)

	// env-b has no usage this tick; env-x is unknown.
	ds := g.Tick(now, []model.EnvUsage{
		usage("env-a", 0.95*quotaBytes),
		usage("env-x", 5*quotaBytes),
	})
	if len(ds) != 1 || ds[0].EnvID != "env-a" {
		t.Fatalf("decisions = %+v, want exactly one for env-a", ds)
	}
	if len(ds[0].Frontends) != 2 || ds[0].Frontends[0] != "fe_a1" || ds[0].Frontends[1] != "fe_a2" {
		t.Fatalf("frontends = %v, want [fe_a1 fe_a2]", ds[0].Frontends)
	}

	// Remove env-a: its usages no longer produce decisions.
	g.UpdateConfig([]model.EnvQuota{envQuota("env-b", quotaBits, nil, "fe_b")})
	ds = g.Tick(now.Add(time.Second), []model.EnvUsage{usage("env-a", 0.95*quotaBytes)})
	if len(ds) != 0 {
		t.Fatalf("decisions after removal = %+v, want none", ds)
	}
}
