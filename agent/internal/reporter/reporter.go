// Package reporter is the controller-facing client of the rl-agent (design
// doc §3.6): a config long-poll loop, a batched metrics uploader and a
// heartbeat. Fail-static behaviour (§3.7) is implemented by persisting every
// received config to a local cache file; on startup the agent core seeds
// itself from LoadCache and keeps enforcing the last known quotas while the
// controller is unreachable.
package reporter

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ipest-info/haproxy_ingress_rate_limiter/internal/model"
)

const (
	// requestTimeout bounds every HTTP request; it must exceed the
	// controller's 30s long-poll hold time (§3.6).
	requestTimeout = 40 * time.Second
	// defaultFlushInterval is the metrics batch cadence. The controller can
	// lower ReportIntervalS in the config; this skeleton keeps a fixed ticker
	// and only documents the knob.
	defaultFlushInterval = 5 * time.Second
	// defaultHeartbeatInterval per §3.6.
	defaultHeartbeatInterval = 10 * time.Second
	// maxBufferedSamples bounds the metrics buffer while the controller is
	// unreachable (~10 minutes at 1 sample/s); oldest samples are dropped.
	maxBufferedSamples = 600
	// backoffMin/backoffMax bound the config-poll retry backoff.
	backoffMin = 1 * time.Second
	backoffMax = 30 * time.Second
	// maxResponseBytes caps how much of any controller response is read.
	maxResponseBytes = 8 << 20

	configPath    = "/v1/agent/config"
	metricsPath   = "/v1/agent/metrics"
	heartbeatPath = "/v1/agent/heartbeat"
)

// Options configures a Reporter. BaseURL empty is tolerated defensively (Run
// blocks until ctx is done) but the agent core skips constructing a Reporter
// entirely in standalone mode.
type Options struct {
	BaseURL      string
	NodeID       string
	CachePath    string
	AgentVersion string
	TLSCAFile    string
	TLSCertFile  string
	TLSKeyFile   string
}

// envSample is the per-env slice of one metrics sample.
type envSample struct {
	EnvID     string  `json:"env_id"`
	RateBps   float64 `json:"rate_bps"`
	Mean10Bps float64 `json:"mean10_bps"`
	Ewma60Bps float64 `json:"ewma60_bps"`
	ConnCur   int64   `json:"conn_cur"`
	BwlimBps  float64 `json:"bwlim_bps"`
	State     string  `json:"state"`
	Changed   bool    `json:"changed"`
}

// sample is one core-loop tick recorded by AddSample.
type sample struct {
	TS            int64       `json:"ts"` // unix seconds
	Mode          string      `json:"mode"`
	ConfigVersion int64       `json:"config_version"`
	Envs          []envSample `json:"envs"`
}

type metricsPayload struct {
	NodeID       string   `json:"node_id"`
	AgentVersion string   `json:"agent_version"`
	Samples      []sample `json:"samples"`
}

type heartbeatPayload struct {
	NodeID        string `json:"node_id"`
	AgentVersion  string `json:"agent_version"`
	Mode          string `json:"mode"`
	ConfigVersion int64  `json:"config_version"`
}

// Reporter talks to the controller. AddSample is called from the agent core
// loop; the poll/flush/heartbeat goroutines started by Run share state only
// through the mutex and the atomic version.
type Reporter struct {
	opts    Options
	baseURL string // opts.BaseURL without trailing slash
	log     *slog.Logger
	client  *http.Client

	configs chan model.ControllerConfig // buffered(1); coalesced by pushConfig
	version atomic.Int64                // last config version received via long-poll

	mu                sync.Mutex
	samples           []sample
	lastMode          string // last mode passed to AddSample; reported in heartbeats
	lastConfigVersion int64  // last config version passed to AddSample

	// Tickers are fields so tests can shorten them before Run.
	flushInterval     time.Duration
	heartbeatInterval time.Duration
}

// New builds a Reporter. A nil logger falls back to slog.Default(). When TLS
// material is configured but fails to load, the error is logged and the
// Reporter falls back to a plain default client rather than failing
// construction (the agent must still start and fail-static).
func New(opts Options, log *slog.Logger) *Reporter {
	if log == nil {
		log = slog.Default()
	}
	client := &http.Client{Timeout: requestTimeout}
	if opts.TLSCAFile != "" || opts.TLSCertFile != "" || opts.TLSKeyFile != "" {
		tlsCfg, err := buildTLSConfig(opts)
		if err != nil {
			log.Error("TLS client config failed; falling back to default HTTP client", "err", err)
		} else {
			client.Transport = &http.Transport{TLSClientConfig: tlsCfg}
		}
	}
	return &Reporter{
		opts:              opts,
		baseURL:           strings.TrimRight(opts.BaseURL, "/"),
		log:               log,
		client:            client,
		configs:           make(chan model.ControllerConfig, 1),
		flushInterval:     defaultFlushInterval,
		heartbeatInterval: defaultHeartbeatInterval,
	}
}

// buildTLSConfig assembles an mTLS-ready tls.Config from the configured CA
// bundle and client certificate pair. Each piece is optional, but a client
// cert requires both cert and key files.
func buildTLSConfig(opts Options) (*tls.Config, error) {
	cfg := &tls.Config{}
	if opts.TLSCAFile != "" {
		pem, err := os.ReadFile(opts.TLSCAFile)
		if err != nil {
			return nil, fmt.Errorf("read CA file: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("CA file %s: no valid PEM certificates", opts.TLSCAFile)
		}
		cfg.RootCAs = pool
	}
	if opts.TLSCertFile != "" || opts.TLSKeyFile != "" {
		if opts.TLSCertFile == "" || opts.TLSKeyFile == "" {
			return nil, errors.New("client TLS requires both cert and key files")
		}
		cert, err := tls.LoadX509KeyPair(opts.TLSCertFile, opts.TLSKeyFile)
		if err != nil {
			return nil, fmt.Errorf("load client key pair: %w", err)
		}
		cfg.Certificates = []tls.Certificate{cert}
	}
	return cfg, nil
}

// LoadCache reads the fail-static config cache written by the long-poll loop.
// A missing file yields a zero config and an error wrapping os.ErrNotExist so
// callers can distinguish first boot from a corrupt cache.
func LoadCache(path string) (model.ControllerConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return model.ControllerConfig{}, fmt.Errorf("config cache %s: %w", path, os.ErrNotExist)
		}
		return model.ControllerConfig{}, fmt.Errorf("read config cache: %w", err)
	}
	var cfg model.ControllerConfig
	if err := json.Unmarshal(data, &cfg); err != nil {
		return model.ControllerConfig{}, fmt.Errorf("decode config cache %s: %w", path, err)
	}
	cfg.Normalize()
	return cfg, nil
}

// Configs returns the channel on which received configs are delivered. The
// channel is buffered(1) and coalescing: a slow consumer only ever sees the
// latest config.
func (r *Reporter) Configs() <-chan model.ControllerConfig {
	return r.configs
}

// AddSample records one core-loop tick into the bounded metrics buffer. It is
// non-blocking (mutex-only, no I/O) and safe to call while a flush is in
// flight. When the buffer is full the oldest sample is dropped.
func (r *Reporter) AddSample(now time.Time, usages []model.EnvUsage, decisions []model.Decision, mode string, configVersion int64) {
	byEnv := make(map[string]model.Decision, len(decisions))
	for _, d := range decisions {
		byEnv[d.EnvID] = d
	}
	envs := make([]envSample, 0, len(usages))
	for _, u := range usages {
		es := envSample{
			EnvID:     u.EnvID,
			RateBps:   u.RateBps,
			Mean10Bps: u.Mean10Bps,
			Ewma60Bps: u.Ewma60Bps,
			ConnCur:   u.ConnCur,
		}
		if d, ok := byEnv[u.EnvID]; ok {
			es.BwlimBps = d.BwlimBps
			es.State = d.State.String()
			es.Changed = d.Changed
		}
		envs = append(envs, es)
	}
	s := sample{TS: now.Unix(), Mode: mode, ConfigVersion: configVersion, Envs: envs}

	r.mu.Lock()
	defer r.mu.Unlock()
	r.lastMode = mode
	r.lastConfigVersion = configVersion
	r.samples = append(r.samples, s)
	if len(r.samples) > maxBufferedSamples {
		// Reslicing keeps the backing array; append reallocates and copies once
		// its capacity is exhausted, so retention stays bounded.
		r.samples = r.samples[len(r.samples)-maxBufferedSamples:]
	}
}

// Run starts the long-poll, metrics-flush and heartbeat goroutines and blocks
// until ctx is done. With an empty BaseURL (standalone mode, defensive) it
// only blocks.
func (r *Reporter) Run(ctx context.Context) {
	if r.baseURL == "" {
		r.log.Warn("reporter has no base URL; controller sync disabled")
		<-ctx.Done()
		return
	}
	var wg sync.WaitGroup
	wg.Add(3)
	go func() { defer wg.Done(); r.pollLoop(ctx) }()
	go func() { defer wg.Done(); r.flushLoop(ctx) }()
	go func() { defer wg.Done(); r.heartbeatLoop(ctx) }()
	wg.Wait()
}

// pollLoop long-polls the config endpoint. 200 applies the config, 204 and
// plain long-poll timeouts re-poll immediately, transport/status errors back
// off exponentially (1s..30s, jittered).
func (r *Reporter) pollLoop(ctx context.Context) {
	backoff := time.Duration(0)
	for ctx.Err() == nil {
		cfg, err := r.pollOnce(ctx)
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			if isTimeout(err) {
				// Long-poll expiry on a silent controller: not a failure.
				backoff = 0
				continue
			}
			backoff = nextBackoff(backoff)
			d := withJitter(backoff)
			r.log.Warn("config poll failed", "err", err, "retry_in", d)
			if !sleepCtx(ctx, d) {
				return
			}
			continue
		}
		backoff = 0
		if cfg != nil {
			r.applyConfig(*cfg)
		}
	}
}

// pollOnce performs one long-poll request. It returns (nil, nil) on 204 ("no
// change yet").
func (r *Reporter) pollOnce(ctx context.Context) (*model.ControllerConfig, error) {
	u := fmt.Sprintf("%s%s?node_id=%s&version=%d",
		r.baseURL, configPath, url.QueryEscape(r.opts.NodeID), r.version.Load())
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	resp, err := r.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() {
		io.Copy(io.Discard, io.LimitReader(resp.Body, maxResponseBytes)) //nolint:errcheck
		resp.Body.Close()
	}()
	switch resp.StatusCode {
	case http.StatusOK:
		var cfg model.ControllerConfig
		if err := json.NewDecoder(io.LimitReader(resp.Body, maxResponseBytes)).Decode(&cfg); err != nil {
			return nil, fmt.Errorf("decode config: %w", err)
		}
		return &cfg, nil
	case http.StatusNoContent:
		return nil, nil
	default:
		return nil, fmt.Errorf("config poll: unexpected status %d", resp.StatusCode)
	}
}

// applyConfig normalizes, persists (fail-static cache) and publishes one
// received config, then advances the long-poll version.
func (r *Reporter) applyConfig(cfg model.ControllerConfig) {
	cfg.Normalize()
	if err := r.persistCache(cfg); err != nil {
		// The config is still applied in-memory; only the fail-static cache
		// is stale.
		r.log.Error("persist config cache failed", "err", err, "path", r.opts.CachePath)
	}
	r.pushConfig(cfg)
	r.version.Store(cfg.Version)
	r.log.Info("controller config received",
		"version", cfg.Version, "mode", cfg.Mode, "envs", len(cfg.Envs))
}

// persistCache atomically replaces the cache file (temp file in the same
// directory + rename) so a crash mid-write can never leave a torn cache.
func (r *Reporter) persistCache(cfg model.ControllerConfig) error {
	if r.opts.CachePath == "" {
		return nil
	}
	data, err := json.Marshal(cfg)
	if err != nil {
		return fmt.Errorf("encode config cache: %w", err)
	}
	dir := filepath.Dir(r.opts.CachePath)
	f, err := os.CreateTemp(dir, ".config-cache-*")
	if err != nil {
		return fmt.Errorf("create cache temp file: %w", err)
	}
	tmp := f.Name()
	if _, err := f.Write(data); err == nil {
		err = f.Sync()
	} else {
		f.Close()
		os.Remove(tmp)
		return fmt.Errorf("write cache temp file: %w", err)
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("close cache temp file: %w", err)
	}
	if err := os.Rename(tmp, r.opts.CachePath); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("rename cache file: %w", err)
	}
	return nil
}

// pushConfig delivers cfg on the buffered(1) channel, dropping any stale
// pending config so a slow consumer always reads the latest one. Only the
// poll goroutine produces, so the loop terminates.
func (r *Reporter) pushConfig(cfg model.ControllerConfig) {
	for {
		select {
		case r.configs <- cfg:
			return
		default:
		}
		select {
		case <-r.configs:
		default:
		}
	}
}

// flushLoop uploads buffered samples on a fixed ticker. The controller's
// ReportIntervalS knob is intentionally not wired up yet (skeleton keeps a
// fixed 5s cadence, which matches the config default).
func (r *Reporter) flushLoop(ctx context.Context) {
	t := time.NewTicker(r.flushInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			r.flushOnce(ctx)
		}
	}
}

// flushOnce POSTs the buffered samples. On failure the batch is put back in
// front of anything added meanwhile, re-applying the buffer bound.
func (r *Reporter) flushOnce(ctx context.Context) {
	r.mu.Lock()
	if len(r.samples) == 0 {
		r.mu.Unlock()
		return
	}
	batch := r.samples
	r.samples = nil
	r.mu.Unlock()

	payload := metricsPayload{
		NodeID:       r.opts.NodeID,
		AgentVersion: r.opts.AgentVersion,
		Samples:      batch,
	}
	if err := r.postJSON(ctx, metricsPath, payload); err != nil {
		if ctx.Err() == nil {
			r.log.Warn("metrics flush failed; keeping samples", "err", err, "samples", len(batch))
		}
		r.mu.Lock()
		r.samples = append(batch, r.samples...)
		if len(r.samples) > maxBufferedSamples {
			r.samples = r.samples[len(r.samples)-maxBufferedSamples:]
		}
		r.mu.Unlock()
	}
}

// heartbeatLoop beats immediately on start (controller visibility after
// restart), then every heartbeatInterval.
func (r *Reporter) heartbeatLoop(ctx context.Context) {
	r.heartbeatOnce(ctx)
	t := time.NewTicker(r.heartbeatInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			r.heartbeatOnce(ctx)
		}
	}
}

func (r *Reporter) heartbeatOnce(ctx context.Context) {
	r.mu.Lock()
	payload := heartbeatPayload{
		NodeID:        r.opts.NodeID,
		AgentVersion:  r.opts.AgentVersion,
		Mode:          r.lastMode,
		ConfigVersion: r.lastConfigVersion,
	}
	r.mu.Unlock()
	if err := r.postJSON(ctx, heartbeatPath, payload); err != nil && ctx.Err() == nil {
		r.log.Warn("heartbeat failed", "err", err)
	}
}

func (r *Reporter) postJSON(ctx context.Context, path string, v any) error {
	body, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("encode %s payload: %w", path, err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, r.baseURL+path, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := r.client.Do(req)
	if err != nil {
		return err
	}
	io.Copy(io.Discard, io.LimitReader(resp.Body, maxResponseBytes)) //nolint:errcheck
	resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("POST %s: unexpected status %d", path, resp.StatusCode)
	}
	return nil
}

// nextBackoff doubles the previous backoff within [backoffMin, backoffMax].
func nextBackoff(cur time.Duration) time.Duration {
	if cur <= 0 {
		return backoffMin
	}
	cur *= 2
	if cur > backoffMax {
		cur = backoffMax
	}
	return cur
}

// withJitter maps d to a uniform value in [d/2, d] so restarting agents do
// not synchronize their retries.
func withJitter(d time.Duration) time.Duration {
	half := d / 2
	return half + time.Duration(rand.Int64N(int64(half)+1))
}

// isTimeout reports whether err is a client-side request timeout (the
// long-poll outlived requestTimeout without controller activity).
func isTimeout(err error) bool {
	var ne net.Error
	return errors.As(err, &ne) && ne.Timeout()
}

// sleepCtx sleeps for d; it returns false when ctx ended first.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
