package executor

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"

	"github.com/ipest-info/haproxy_ingress_rate_limiter/internal/model"
)

type call struct {
	mapPath, key, value string
}

// fakeSetter records SetMapEntry calls and injects per-key errors.
type fakeSetter struct {
	mu    sync.Mutex
	calls []call
	errs  map[string]error
}

func (f *fakeSetter) SetMapEntry(_ context.Context, mapPath, key, value string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, call{mapPath, key, value})
	if err, ok := f.errs[key]; ok {
		return err
	}
	return nil
}

func (f *fakeSetter) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

func (f *fakeSetter) valuesFor(key string) []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	var vs []string
	for _, c := range f.calls {
		if c.key == key {
			vs = append(vs, c.value)
		}
	}
	return vs
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func decision(env string, frontends []string, bwlim float64, changed bool) model.Decision {
	return model.Decision{
		EnvID:     env,
		Frontends: frontends,
		BwlimBps:  bwlim,
		State:     model.StateNormal,
		Changed:   changed,
	}
}

func TestDryRunLogsWithoutIO(t *testing.T) {
	var buf bytes.Buffer
	log := slog.New(slog.NewTextHandler(&buf, nil))
	f := &fakeSetter{}
	s := NewSwitchable(f, "/etc/haproxy/bwlim.map", model.ModeDryRun, log)

	ds := []model.Decision{
		decision("env-a", []string{"fe_a1", "fe_a2"}, 12_500_000, true),
		decision("env-b", []string{"fe_b1"}, 6_250_000, false),
	}
	if err := s.Apply(context.Background(), ds); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if n := f.callCount(); n != 0 {
		t.Fatalf("dry-run performed %d SetMapEntry calls, want 0", n)
	}
	out := buf.String()
	if !strings.Contains(out, "DRY-RUN would set bwlim") || !strings.Contains(out, "env-a") {
		t.Fatalf("dry-run log missing expected line: %q", out)
	}
	if strings.Contains(out, "env-b") {
		t.Fatalf("unchanged decision was logged: %q", out)
	}
	snap := s.Snapshot()
	if got, ok := snap["env-a"]; !ok || got != 12_500_000 {
		t.Fatalf("snapshot env-a = %v (present=%v), want 12500000", got, ok)
	}
	if _, ok := snap["env-b"]; ok {
		t.Fatalf("unchanged env-b recorded in snapshot")
	}
}

func TestEnforceSplitsAggregateAcrossFrontends(t *testing.T) {
	f := &fakeSetter{}
	s := NewSwitchable(f, "/etc/haproxy/bwlim.map", model.ModeEnforce, discardLogger())

	// BwlimBps is the env AGGREGATE: each of the 2 frontends must receive
	// floor(1048576.9 / 2) = 524288 so the per-frontend sum stays ≤ aggregate.
	ds := []model.Decision{
		decision("env-a", []string{"fe_a1", "fe_a2"}, 1_048_576.9, true),
	}
	if err := s.Apply(context.Background(), ds); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if n := f.callCount(); n != 2 {
		t.Fatalf("call count = %d, want 2", n)
	}
	for _, fe := range []string{"fe_a1", "fe_a2"} {
		vs := f.valuesFor(fe)
		if len(vs) != 1 || vs[0] != "524288" {
			t.Fatalf("values for %s = %v, want [524288]", fe, vs)
		}
	}
	for _, c := range f.calls {
		if c.mapPath != "/etc/haproxy/bwlim.map" {
			t.Fatalf("mapPath = %q, want /etc/haproxy/bwlim.map", c.mapPath)
		}
	}
	if got := s.Snapshot()["env-a"]; got != 1_048_576.9 {
		t.Fatalf("snapshot env-a = %v, want aggregate 1048576.9", got)
	}
}

func TestFailedWriteRetriedOnUnchangedDecision(t *testing.T) {
	boom := errors.New("socket gone")
	f := &fakeSetter{errs: map[string]error{"fe1": boom}}
	s := NewSwitchable(f, "m", model.ModeEnforce, discardLogger())

	changed := []model.Decision{decision("env-a", []string{"fe1"}, 1000, true)}
	unchanged := []model.Decision{decision("env-a", []string{"fe1"}, 1000, false)}

	if err := s.Apply(context.Background(), changed); !errors.Is(err, boom) {
		t.Fatalf("first Apply error = %v, want %v", err, boom)
	}
	if _, ok := s.Snapshot()["env-a"]; ok {
		t.Fatal("failed env recorded in snapshot")
	}

	// The runtime API recovers; the governor emits Changed=false from now on
	// (its own emitted value already advanced). The executor must retry.
	f.mu.Lock()
	delete(f.errs, "fe1")
	f.mu.Unlock()

	if err := s.Apply(context.Background(), unchanged); err != nil {
		t.Fatalf("retry Apply: %v", err)
	}
	if n := f.callCount(); n != 2 {
		t.Fatalf("call count = %d, want 2 (initial failure + one retry)", n)
	}
	if got := s.Snapshot()["env-a"]; got != 1000 {
		t.Fatalf("snapshot env-a = %v, want 1000 after retry", got)
	}

	// Once converged, unchanged decisions are no-ops again.
	if err := s.Apply(context.Background(), unchanged); err != nil {
		t.Fatalf("post-retry Apply: %v", err)
	}
	if n := f.callCount(); n != 2 {
		t.Fatalf("call count = %d, want still 2", n)
	}
}

func TestUnchangedDecisionsSkipped(t *testing.T) {
	f := &fakeSetter{}
	s := NewSwitchable(f, "m", model.ModeEnforce, discardLogger())

	ds := []model.Decision{decision("env-a", []string{"fe1"}, 100, false)}
	if err := s.Apply(context.Background(), ds); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if n := f.callCount(); n != 0 {
		t.Fatalf("unchanged decision caused %d calls, want 0", n)
	}
	if len(s.Snapshot()) != 0 {
		t.Fatalf("snapshot = %v, want empty", s.Snapshot())
	}
}

func TestPartialFailureJoinsErrorsAndKeepsWriting(t *testing.T) {
	errA := errors.New("boom-a")
	errB := errors.New("boom-b")
	f := &fakeSetter{errs: map[string]error{"fe_a2": errA, "fe_b1": errB}}
	s := NewSwitchable(f, "m", model.ModeEnforce, discardLogger())

	ds := []model.Decision{
		decision("env-a", []string{"fe_a1", "fe_a2", "fe_a3"}, 1000, true),
		decision("env-b", []string{"fe_b1", "fe_b2"}, 2000, true),
		decision("env-c", []string{"fe_c1"}, 3000, true),
	}
	err := s.Apply(context.Background(), ds)
	if err == nil {
		t.Fatal("Apply returned nil, want joined error")
	}
	if !errors.Is(err, errA) || !errors.Is(err, errB) {
		t.Fatalf("joined error missing injected errors: %v", err)
	}
	// Every frontend must still have been attempted, including those after a
	// failure within the same decision and in later decisions.
	for _, fe := range []string{"fe_a1", "fe_a2", "fe_a3", "fe_b1", "fe_b2", "fe_c1"} {
		if len(f.valuesFor(fe)) != 1 {
			t.Fatalf("frontend %s not written exactly once: %v", fe, f.valuesFor(fe))
		}
	}
	snap := s.Snapshot()
	if _, ok := snap["env-a"]; ok {
		t.Fatal("env-a recorded despite failed frontend")
	}
	if _, ok := snap["env-b"]; ok {
		t.Fatal("env-b recorded despite failed frontend")
	}
	if got := snap["env-c"]; got != 3000 {
		t.Fatalf("snapshot env-c = %v, want 3000", got)
	}
}

func TestInvalidModeFallsBackToDryRun(t *testing.T) {
	var buf bytes.Buffer
	log := slog.New(slog.NewTextHandler(&buf, nil))
	f := &fakeSetter{}
	s := NewSwitchable(f, "m", "bogus", log)
	if got := s.Mode(); got != model.ModeDryRun {
		t.Fatalf("Mode() = %q after invalid constructor mode, want dry-run", got)
	}
	if !strings.Contains(buf.String(), "invalid executor mode") {
		t.Fatalf("missing warning for invalid mode: %q", buf.String())
	}

	s.SetMode(model.ModeEnforce)
	if got := s.Mode(); got != model.ModeEnforce {
		t.Fatalf("Mode() = %q, want enforce", got)
	}
	s.SetMode("garbage")
	if got := s.Mode(); got != model.ModeDryRun {
		t.Fatalf("Mode() = %q after invalid SetMode, want dry-run", got)
	}
}

func TestSwitchToEnforceResyncsOnce(t *testing.T) {
	f := &fakeSetter{}
	s := NewSwitchable(f, "m", model.ModeDryRun, discardLogger())

	changed := []model.Decision{decision("env-a", []string{"fe1", "fe2"}, 500, true)}
	unchanged := []model.Decision{decision("env-a", []string{"fe1", "fe2"}, 500, false)}

	if err := s.Apply(context.Background(), changed); err != nil {
		t.Fatalf("dry-run Apply: %v", err)
	}
	if f.callCount() != 0 {
		t.Fatalf("dry-run made %d calls", f.callCount())
	}

	s.SetMode(model.ModeEnforce)

	// First Apply after the switch must re-write even unchanged decisions.
	if err := s.Apply(context.Background(), unchanged); err != nil {
		t.Fatalf("resync Apply: %v", err)
	}
	if n := f.callCount(); n != 2 {
		t.Fatalf("resync wrote %d entries, want 2", n)
	}

	// Resync is one-shot: the next unchanged Apply is a no-op.
	if err := s.Apply(context.Background(), unchanged); err != nil {
		t.Fatalf("post-resync Apply: %v", err)
	}
	if n := f.callCount(); n != 2 {
		t.Fatalf("post-resync call count = %d, want still 2", n)
	}
}

func TestResyncNotArmedWithoutModeChange(t *testing.T) {
	f := &fakeSetter{}
	s := NewSwitchable(f, "m", model.ModeEnforce, discardLogger())

	s.SetMode(model.ModeEnforce) // no-op: mode unchanged
	ds := []model.Decision{decision("env-a", []string{"fe1"}, 500, false)}
	if err := s.Apply(context.Background(), ds); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if n := f.callCount(); n != 0 {
		t.Fatalf("redundant SetMode armed resync: %d calls", n)
	}
}

func TestResyncSurvivesEmptyApply(t *testing.T) {
	f := &fakeSetter{}
	s := NewSwitchable(f, "m", model.ModeDryRun, discardLogger())
	s.SetMode(model.ModeEnforce)

	if err := s.Apply(context.Background(), nil); err != nil {
		t.Fatalf("empty Apply: %v", err)
	}
	ds := []model.Decision{decision("env-a", []string{"fe1"}, 500, false)}
	if err := s.Apply(context.Background(), ds); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if n := f.callCount(); n != 1 {
		t.Fatalf("resync consumed by empty Apply: %d calls, want 1", n)
	}
}

func TestSwitchBackToDryRunDropsPendingResync(t *testing.T) {
	f := &fakeSetter{}
	s := NewSwitchable(f, "m", model.ModeDryRun, discardLogger())
	s.SetMode(model.ModeEnforce)
	s.SetMode(model.ModeDryRun) // pending resync must be dropped

	ds := []model.Decision{decision("env-a", []string{"fe1"}, 500, false)}
	if err := s.Apply(context.Background(), ds); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if f.callCount() != 0 {
		t.Fatalf("dry-run made calls: %d", f.callCount())
	}
	if len(s.Snapshot()) != 0 {
		t.Fatalf("unchanged decision recorded after dropped resync: %v", s.Snapshot())
	}
}

func TestConcurrentSetModeApplySnapshot(t *testing.T) {
	f := &fakeSetter{}
	s := NewSwitchable(f, "m", model.ModeEnforce, discardLogger())

	const iters = 200
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		ds := []model.Decision{
			decision("env-a", []string{"fe1", "fe2"}, 1000, true),
			decision("env-b", []string{"fe3"}, 2000, true),
		}
		for i := 0; i < iters; i++ {
			if err := s.Apply(context.Background(), ds); err != nil {
				t.Errorf("Apply: %v", err)
				return
			}
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < iters; i++ {
			if i%2 == 0 {
				s.SetMode(model.ModeDryRun)
			} else {
				s.SetMode(model.ModeEnforce)
			}
			_ = s.Mode()
			_ = s.Snapshot()
		}
	}()
	wg.Wait()
}
