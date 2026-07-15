// mock-controller is a development stand-in for the real control plane. It
// serves the three agent endpoints (design doc §3.6) from a single JSON file:
//
//	GET  /v1/agent/config     long-poll; Version = config file mtime (unix s)
//	POST /v1/agent/metrics    logged and discarded
//	POST /v1/agent/heartbeat  logged and discarded
//
// Editing the config file (any mtime change) releases pending long-polls with
// the new content. No auth, no TLS, no persistence: dev/test only.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/ipest-info/haproxy_ingress_rate_limiter/internal/model"
)

const (
	// longPollHold must stay below the agent's 40s request timeout so a
	// silent hold ends in a clean 204, not a client-side timeout.
	longPollHold = 25 * time.Second
	// reloadInterval is the config file mtime poll cadence.
	reloadInterval = 1 * time.Second
	// maxBodyBytes caps how much of any agent POST body is read.
	maxBodyBytes = 8 << 20
)

// store holds the current config and wakes long-poll waiters on change.
type store struct {
	log *slog.Logger

	mu   sync.Mutex
	cfg  model.ControllerConfig // Version = file mtime unix seconds
	wait chan struct{}          // closed and replaced on every version change
}

// snapshot returns the current config and the channel that will be closed on
// the next change.
func (s *store) snapshot() (model.ControllerConfig, <-chan struct{}) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cfg, s.wait
}

// set installs a new config and releases all pending long-polls.
func (s *store) set(cfg model.ControllerConfig) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cfg = cfg
	close(s.wait)
	s.wait = make(chan struct{})
}

// loadConfig reads path as a version-less model.ControllerConfig and stamps
// Version with the file's mtime in unix seconds.
func loadConfig(path string) (model.ControllerConfig, error) {
	fi, err := os.Stat(path)
	if err != nil {
		return model.ControllerConfig{}, fmt.Errorf("stat config: %w", err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return model.ControllerConfig{}, fmt.Errorf("read config: %w", err)
	}
	var cfg model.ControllerConfig
	if err := json.Unmarshal(data, &cfg); err != nil {
		return model.ControllerConfig{}, fmt.Errorf("parse config %s: %w", path, err)
	}
	cfg.Version = fi.ModTime().Unix()
	cfg.Normalize()
	return cfg, nil
}

// watchConfig polls the file mtime every reloadInterval and reloads on
// change. A file that fails to load keeps the previous config in service.
func watchConfig(ctx context.Context, path string, st *store, log *slog.Logger) {
	t := time.NewTicker(reloadInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		fi, err := os.Stat(path)
		if err != nil {
			log.Warn("config stat failed; keeping current config", "path", path, "err", err)
			continue
		}
		cur, _ := st.snapshot()
		if fi.ModTime().Unix() == cur.Version {
			continue
		}
		cfg, err := loadConfig(path)
		if err != nil {
			log.Error("config reload failed; keeping current config", "path", path, "err", err)
			continue
		}
		st.set(cfg)
		log.Info("config reloaded", "version", cfg.Version, "mode", cfg.Mode, "envs", len(cfg.Envs))
	}
}

// handleConfig implements the long-poll: an up-to-date client is held until a
// change or the hold deadline (then 204); a stale client gets 200 + JSON.
func handleConfig(st *store, log *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		nodeID := r.URL.Query().Get("node_id")
		clientVer, _ := strconv.ParseInt(r.URL.Query().Get("version"), 10, 64)

		deadline := time.NewTimer(longPollHold)
		defer deadline.Stop()
		for {
			cfg, changed := st.snapshot()
			if cfg.Version != clientVer {
				w.Header().Set("Content-Type", "application/json")
				if err := json.NewEncoder(w).Encode(cfg); err != nil {
					log.Warn("config write failed", "node_id", nodeID, "err", err)
					return
				}
				log.Info("config served",
					"node_id", nodeID, "client_version", clientVer,
					"version", cfg.Version, "mode", cfg.Mode, "envs", len(cfg.Envs))
				return
			}
			select {
			case <-changed:
				// Re-read and serve the new config on the next iteration.
			case <-deadline.C:
				w.WriteHeader(http.StatusNoContent)
				return
			case <-r.Context().Done():
				return
			}
		}
	}
}

// decodeLoose reads the request body as an untyped JSON object so the mock
// never breaks when the agent payload evolves.
func decodeLoose(r *http.Request) (map[string]any, error) {
	body, err := io.ReadAll(io.LimitReader(r.Body, maxBodyBytes))
	if err != nil {
		return nil, err
	}
	var m map[string]any
	if err := json.Unmarshal(body, &m); err != nil {
		return nil, err
	}
	return m, nil
}

func handleMetrics(log *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		m, err := decodeLoose(r)
		if err != nil {
			log.Warn("metrics decode failed", "err", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		samples := 0
		if s, ok := m["samples"].([]any); ok {
			samples = len(s)
		}
		log.Info("metrics received", "node_id", m["node_id"], "samples", samples)
		w.WriteHeader(http.StatusNoContent)
	}
}

func handleHeartbeat(log *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		m, err := decodeLoose(r)
		if err != nil {
			log.Warn("heartbeat decode failed", "err", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		log.Info("heartbeat received",
			"node_id", m["node_id"], "agent_version", m["agent_version"],
			"mode", m["mode"], "config_version", m["config_version"])
		w.WriteHeader(http.StatusNoContent)
	}
}

func main() {
	addr := flag.String("addr", ":9090", "listen address")
	configPath := flag.String("config", "", "path to a ControllerConfig JSON file (version-less; version = file mtime)")
	flag.Parse()
	if *configPath == "" {
		fmt.Fprintln(os.Stderr, "mock-controller: -config is required")
		os.Exit(1)
	}

	log := slog.New(slog.NewTextHandler(os.Stderr, nil))

	cfg, err := loadConfig(*configPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, "mock-controller:", err)
		os.Exit(1)
	}
	st := &store{log: log, cfg: cfg, wait: make(chan struct{})}
	log.Info("config loaded", "path", *configPath, "version", cfg.Version, "mode", cfg.Mode, "envs", len(cfg.Envs))

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	go watchConfig(ctx, *configPath, st, log)

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/agent/config", handleConfig(st, log))
	mux.HandleFunc("/v1/agent/metrics", handleMetrics(log))
	mux.HandleFunc("/v1/agent/heartbeat", handleHeartbeat(log))

	// No WriteTimeout: the config handler holds connections for longPollHold.
	srv := &http.Server{
		Addr:              *addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		srv.Shutdown(shutdownCtx) //nolint:errcheck
	}()

	log.Info("mock-controller listening", "addr", *addr, "config", *configPath)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		fmt.Fprintln(os.Stderr, "mock-controller:", err)
		os.Exit(1)
	}
	log.Info("mock-controller stopped")
}
