package reporter

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	cryptorand "crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"errors"
	"io"
	"log/slog"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ipest-info/haproxy_ingress_rate_limiter/internal/model"
)

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func newTestReporter(t *testing.T, baseURL, cachePath string) *Reporter {
	t.Helper()
	return New(Options{
		BaseURL:      baseURL,
		NodeID:       "node-1",
		CachePath:    cachePath,
		AgentVersion: "test-0.1",
	}, quietLogger())
}

// startRun runs r.Run in a goroutine and returns a cancel that also waits for
// Run to return.
func startRun(t *testing.T, r *Reporter) (stop func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { r.Run(ctx); close(done) }()
	return func() {
		cancel()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Fatal("Run did not return after cancel")
		}
	}
}

// blockThen204 emulates a controller long-poll with nothing new: hold the
// request until the client goes away (or a cap), then answer 204.
func blockThen204(w http.ResponseWriter, req *http.Request) {
	select {
	case <-req.Context().Done():
	case <-time.After(3 * time.Second):
	}
	w.WriteHeader(http.StatusNoContent)
}

func TestLongPollAdvancesVersionPersistsAndCoalesces(t *testing.T) {
	dir := t.TempDir()
	cachePath := filepath.Join(dir, "config-cache.json")

	var mu sync.Mutex
	var versionsSeen []string
	sawV2 := make(chan struct{})
	var once sync.Once

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.URL.Path != configPath {
			w.WriteHeader(http.StatusOK) // metrics/heartbeat noise
			return
		}
		if got := req.URL.Query().Get("node_id"); got != "node-1" {
			t.Errorf("node_id = %q, want node-1", got)
		}
		v := req.URL.Query().Get("version")
		mu.Lock()
		versionsSeen = append(versionsSeen, v)
		mu.Unlock()
		switch v {
		case "0":
			io.WriteString(w, `{"version":1,"mode":"enforce","envs":[{"env_id":"env-a","frontends":["fe_a"],"quota_bps":80000000}]}`)
		case "1":
			// Bogus mode: Normalize must collapse it to dry-run.
			io.WriteString(w, `{"version":2,"mode":"bogus","envs":[{"env_id":"env-a","frontends":["fe_a"],"quota_bps":160000000}]}`)
		default:
			once.Do(func() { close(sawV2) })
			blockThen204(w, req)
		}
	}))
	defer srv.Close()

	r := newTestReporter(t, srv.URL, cachePath)
	stop := startRun(t, r)
	defer stop()

	// A request carrying version=2 proves both configs were fully applied
	// (persisted + pushed) before the test reads anything.
	select {
	case <-sawV2:
	case <-time.After(3 * time.Second):
		t.Fatal("long-poll never advanced to version 2")
	}

	var cfg model.ControllerConfig
	select {
	case cfg = <-r.Configs():
	case <-time.After(time.Second):
		t.Fatal("no config delivered on Configs()")
	}
	// Coalescing: v1 was never consumed, so the channel must now hold v2 only.
	if cfg.Version != 2 {
		t.Fatalf("delivered config version = %d, want 2 (coalesced)", cfg.Version)
	}
	if cfg.Mode != model.ModeDryRun {
		t.Fatalf("Normalize not applied before push: mode = %q", cfg.Mode)
	}
	if cfg.ReportIntervalS != 5 || cfg.HeartbeatIntervalS != 10 {
		t.Fatalf("Normalize defaults missing: report=%d heartbeat=%d", cfg.ReportIntervalS, cfg.HeartbeatIntervalS)
	}
	select {
	case extra := <-r.Configs():
		t.Fatalf("unexpected second config in channel: version %d", extra.Version)
	default:
	}

	// Cache holds the latest normalized config.
	cached, err := LoadCache(cachePath)
	if err != nil {
		t.Fatalf("LoadCache after poll: %v", err)
	}
	if cached.Version != 2 || cached.Mode != model.ModeDryRun || len(cached.Envs) != 1 {
		t.Fatalf("cached config = %+v, want version 2 dry-run with 1 env", cached)
	}
	if got := cached.Envs[0].QuotaBytesPerSec(); got != 20_000_000 {
		t.Fatalf("cached quota bytes/s = %v, want 2e7", got)
	}

	// Atomic write: no temp files left next to the cache.
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Name() != filepath.Base(cachePath) {
		names := make([]string, 0, len(entries))
		for _, e := range entries {
			names = append(names, e.Name())
		}
		t.Fatalf("cache dir entries = %v, want only %s", names, filepath.Base(cachePath))
	}

	stop()
	mu.Lock()
	defer mu.Unlock()
	if len(versionsSeen) < 3 || versionsSeen[0] != "0" || versionsSeen[1] != "1" || versionsSeen[2] != "2" {
		t.Fatalf("version query progression = %v, want 0,1,2,...", versionsSeen)
	}
}

func TestPollBackoffOnServerErrors(t *testing.T) {
	var polls atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.URL.Path == configPath {
			polls.Add(1)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	r := newTestReporter(t, srv.URL, "")
	ctx, cancel := context.WithTimeout(context.Background(), 2600*time.Millisecond)
	defer cancel()
	r.Run(ctx)

	n := polls.Load()
	// Jittered backoff: first retry within 1s, second within a further 2s,
	// so 2.6s guarantees at least one retry; minimum sleeps (0.5s, 1s, 2s)
	// cap the count well below a hot loop's.
	if n < 2 {
		t.Fatalf("got %d config polls in 2.6s, want at least 2 (one retry)", n)
	}
	if n > 6 {
		t.Fatalf("got %d config polls in 2.6s: backoff not applied", n)
	}
}

func TestAddSampleBufferBound(t *testing.T) {
	r := newTestReporter(t, "http://unused.invalid", "")
	total := maxBufferedSamples + 50
	for i := 0; i < total; i++ {
		r.AddSample(time.Unix(int64(i), 0),
			[]model.EnvUsage{{EnvID: "env-a", RateBps: float64(i)}},
			nil, model.ModeDryRun, 1)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.samples) != maxBufferedSamples {
		t.Fatalf("buffer length = %d, want %d", len(r.samples), maxBufferedSamples)
	}
	if got := r.samples[0].TS; got != 50 {
		t.Fatalf("oldest sample TS = %d, want 50 (oldest dropped first)", got)
	}
	if got := r.samples[len(r.samples)-1].TS; got != int64(total-1) {
		t.Fatalf("newest sample TS = %d, want %d", got, total-1)
	}
}

func TestMetricsFlushPayloadShape(t *testing.T) {
	bodies := make(chan []byte, 4)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		switch req.URL.Path {
		case metricsPath:
			b, _ := io.ReadAll(req.Body)
			select {
			case bodies <- b:
			default:
			}
			w.WriteHeader(http.StatusOK)
		case configPath:
			blockThen204(w, req)
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer srv.Close()

	r := newTestReporter(t, srv.URL, "")
	r.flushInterval = 40 * time.Millisecond
	r.AddSample(time.Unix(1_700_000_000, 0),
		[]model.EnvUsage{{EnvID: "env-a", RateBps: 100, Mean10Bps: 90, Ewma60Bps: 80, ConnCur: 7}},
		[]model.Decision{{EnvID: "env-a", Frontends: []string{"fe_a"}, BwlimBps: 1250, State: model.StateTightening, Changed: true}},
		model.ModeEnforce, 3)

	stop := startRun(t, r)
	defer stop()

	var body []byte
	select {
	case body = <-bodies:
	case <-time.After(2 * time.Second):
		t.Fatal("no metrics POST within 2s")
	}

	var payload metricsPayload
	if err := json.Unmarshal(body, &payload); err != nil {
		t.Fatalf("decode metrics payload: %v", err)
	}
	if payload.NodeID != "node-1" || payload.AgentVersion != "test-0.1" {
		t.Fatalf("payload identity = %q/%q", payload.NodeID, payload.AgentVersion)
	}
	if len(payload.Samples) != 1 {
		t.Fatalf("samples = %d, want 1", len(payload.Samples))
	}
	s := payload.Samples[0]
	if s.TS != 1_700_000_000 || s.Mode != model.ModeEnforce || s.ConfigVersion != 3 {
		t.Fatalf("sample header = %+v", s)
	}
	if len(s.Envs) != 1 {
		t.Fatalf("env samples = %d, want 1", len(s.Envs))
	}
	e := s.Envs[0]
	if e.EnvID != "env-a" || e.RateBps != 100 || e.Mean10Bps != 90 || e.Ewma60Bps != 80 ||
		e.ConnCur != 7 || e.BwlimBps != 1250 || e.State != "tightening" || !e.Changed {
		t.Fatalf("env sample = %+v", e)
	}

	// Wire field names are part of the agent<->controller contract.
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(body, &raw); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"node_id", "agent_version", "samples"} {
		if _, ok := raw[key]; !ok {
			t.Fatalf("payload missing wire key %q; body = %s", key, body)
		}
	}
	var rawSamples []map[string]json.RawMessage
	if err := json.Unmarshal(raw["samples"], &rawSamples); err != nil {
		t.Fatal(err)
	}
	var rawEnvs []map[string]json.RawMessage
	if err := json.Unmarshal(rawSamples[0]["envs"], &rawEnvs); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"env_id", "rate_bps", "mean10_bps", "ewma60_bps", "conn_cur", "bwlim_bps", "state", "changed"} {
		if _, ok := rawEnvs[0][key]; !ok {
			t.Fatalf("env sample missing wire key %q; body = %s", key, body)
		}
	}

	// Successful flush drains the buffer.
	deadline := time.Now().Add(time.Second)
	for {
		r.mu.Lock()
		n := len(r.samples)
		r.mu.Unlock()
		if n == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("buffer not drained after successful flush: %d samples left", n)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestMetricsFlushFailureKeepsSamples(t *testing.T) {
	var attempts atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		switch req.URL.Path {
		case metricsPath:
			attempts.Add(1)
			w.WriteHeader(http.StatusInternalServerError)
		case configPath:
			blockThen204(w, req)
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer srv.Close()

	r := newTestReporter(t, srv.URL, "")
	r.flushInterval = 30 * time.Millisecond
	for i := 0; i < 3; i++ {
		r.AddSample(time.Unix(int64(i), 0),
			[]model.EnvUsage{{EnvID: "env-a", RateBps: 1}}, nil, model.ModeDryRun, 1)
	}

	stop := startRun(t, r)
	defer stop()

	deadline := time.Now().Add(2 * time.Second)
	for attempts.Load() < 2 {
		if time.Now().After(deadline) {
			t.Fatal("fewer than 2 flush attempts within 2s")
		}
		time.Sleep(10 * time.Millisecond)
	}
	// Samples survive failed flushes in order. Poll: a flush may be in
	// flight (buffer momentarily empty) at any single instant.
	deadline = time.Now().Add(time.Second)
	for {
		r.mu.Lock()
		n := len(r.samples)
		ordered := n == 3 && r.samples[0].TS == 0 && r.samples[2].TS == 2
		r.mu.Unlock()
		if ordered {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("buffer after failed flushes: %d samples, want 3 in order", n)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestHeartbeat(t *testing.T) {
	bodies := make(chan []byte, 4)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		switch req.URL.Path {
		case heartbeatPath:
			b, _ := io.ReadAll(req.Body)
			select {
			case bodies <- b:
			default:
			}
			w.WriteHeader(http.StatusOK)
		case configPath:
			blockThen204(w, req)
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer srv.Close()

	r := newTestReporter(t, srv.URL, "")
	r.heartbeatInterval = 50 * time.Millisecond
	// Heartbeat reports the mode/version last observed by the core loop.
	r.AddSample(time.Unix(0, 0), nil, nil, model.ModeEnforce, 7)

	stop := startRun(t, r)
	defer stop()

	var body []byte
	select {
	case body = <-bodies:
	case <-time.After(2 * time.Second):
		t.Fatal("no heartbeat POST within 2s")
	}
	var hb heartbeatPayload
	if err := json.Unmarshal(body, &hb); err != nil {
		t.Fatalf("decode heartbeat: %v", err)
	}
	if hb.NodeID != "node-1" || hb.AgentVersion != "test-0.1" ||
		hb.Mode != model.ModeEnforce || hb.ConfigVersion != 7 {
		t.Fatalf("heartbeat payload = %+v", hb)
	}
}

func TestLoadCacheMissingFile(t *testing.T) {
	cfg, err := LoadCache(filepath.Join(t.TempDir(), "absent.json"))
	if err == nil {
		t.Fatal("expected error for missing cache file")
	}
	if !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("error %v does not wrap os.ErrNotExist", err)
	}
	if cfg.Version != 0 || cfg.Envs != nil {
		t.Fatalf("missing cache must yield zero config, got %+v", cfg)
	}
}

func TestLoadCacheNormalizes(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cache.json")
	if err := os.WriteFile(path, []byte(`{"version":3,"envs":[{"env_id":"e1","frontends":["f1"],"quota_bps":8000}]}`), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadCache(path)
	if err != nil {
		t.Fatalf("LoadCache: %v", err)
	}
	if cfg.Version != 3 || cfg.Mode != model.ModeDryRun || cfg.ReportIntervalS != 5 || cfg.HeartbeatIntervalS != 10 {
		t.Fatalf("normalized cache = %+v", cfg)
	}
	if got := cfg.Envs[0].QuotaBytesPerSec(); got != 1000 {
		t.Fatalf("quota bytes/s = %v, want 1000", got)
	}
}

func TestLoadCacheCorrupt(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cache.json")
	if err := os.WriteFile(path, []byte("{not json"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadCache(path); err == nil {
		t.Fatal("expected decode error for corrupt cache")
	} else if errors.Is(err, os.ErrNotExist) {
		t.Fatal("corrupt cache must not report os.ErrNotExist")
	}
}

// selfSignedPEM generates a throwaway CA-style cert/key pair for TLS
// construction tests.
func selfSignedPEM(t *testing.T) (certPEM, keyPEM []byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), cryptorand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "rl-test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(cryptorand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	certPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM = pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	return certPEM, keyPEM
}

func TestNewTLSClientConstruction(t *testing.T) {
	// Broken TLS material: New must not fail; it logs and keeps the default
	// transport.
	r := New(Options{
		BaseURL:     "https://controller.invalid",
		TLSCertFile: "/nonexistent/cert.pem",
		TLSKeyFile:  "/nonexistent/key.pem",
	}, quietLogger())
	if r == nil {
		t.Fatal("New returned nil")
	}
	if r.client.Transport != nil {
		t.Fatal("broken TLS material must fall back to the default transport")
	}

	// Cert without key is invalid: same fallback.
	r = New(Options{BaseURL: "https://controller.invalid", TLSCertFile: "/nonexistent/cert.pem"}, quietLogger())
	if r.client.Transport != nil {
		t.Fatal("cert-without-key must fall back to the default transport")
	}

	// Valid CA + client pair yields an mTLS-ready transport.
	certPEM, keyPEM := selfSignedPEM(t)
	dir := t.TempDir()
	caFile := filepath.Join(dir, "ca.pem")
	certFile := filepath.Join(dir, "cert.pem")
	keyFile := filepath.Join(dir, "key.pem")
	for _, f := range []struct {
		path string
		data []byte
	}{{caFile, certPEM}, {certFile, certPEM}, {keyFile, keyPEM}} {
		if err := os.WriteFile(f.path, f.data, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	r = New(Options{
		BaseURL:     "https://controller.invalid",
		TLSCAFile:   caFile,
		TLSCertFile: certFile,
		TLSKeyFile:  keyFile,
	}, quietLogger())
	tr, ok := r.client.Transport.(*http.Transport)
	if !ok || tr.TLSClientConfig == nil {
		t.Fatalf("expected TLS transport, got %T", r.client.Transport)
	}
	if tr.TLSClientConfig.RootCAs == nil {
		t.Fatal("RootCAs not set from CA file")
	}
	if len(tr.TLSClientConfig.Certificates) != 1 {
		t.Fatalf("client certificates = %d, want 1", len(tr.TLSClientConfig.Certificates))
	}
	if r.client.Timeout != requestTimeout {
		t.Fatalf("client timeout = %v, want %v", r.client.Timeout, requestTimeout)
	}
}

func TestRunWithoutBaseURLBlocksUntilCancel(t *testing.T) {
	r := New(Options{NodeID: "node-1"}, quietLogger())
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	done := make(chan struct{})
	go func() { r.Run(ctx); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run without BaseURL did not return on ctx cancel")
	}
}
