package config

import (
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ipest-info/haproxy_ingress_rate_limiter/internal/model"
)

func writeConfig(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return path
}

func TestLoadFullConfig(t *testing.T) {
	path := writeConfig(t, `
node_id: node-a1
mode: enforce
log_level: debug
haproxy:
  stats_socket: /run/hap.sock
  bwlim_map_path: /etc/haproxy/maps/custom.map
  timeout_ms: 250
controller:
  base_url: https://ctrl.example.com
  cache_path: /var/cache/rl-agent.json
  tls:
    ca_file: /pki/ca.pem
    cert_file: /pki/cert.pem
    key_file: /pki/key.pem
envs:
  - env_id: env-1
    frontends: [fe_a, fe_b]
    quota_bps: 200000000
    params:
      elastic_ceiling: 1.2
  - env_id: env-2
    frontends: [fe_c]
    quota_bps: 80000000
`)
	f, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if f.NodeID != "node-a1" || f.Mode != model.ModeEnforce || f.LogLevel != "debug" {
		t.Errorf("top-level fields = %q %q %q", f.NodeID, f.Mode, f.LogLevel)
	}
	if f.HAProxy.StatsSocket != "/run/hap.sock" || f.HAProxy.BwlimMapPath != "/etc/haproxy/maps/custom.map" {
		t.Errorf("haproxy section = %+v", f.HAProxy)
	}
	if got := f.HAProxy.Timeout(); got != 250*time.Millisecond {
		t.Errorf("Timeout() = %v, want 250ms", got)
	}
	if f.Controller.BaseURL != "https://ctrl.example.com" || f.Controller.CachePath != "/var/cache/rl-agent.json" {
		t.Errorf("controller section = %+v", f.Controller)
	}
	if f.Controller.TLS.CAFile != "/pki/ca.pem" || f.Controller.TLS.CertFile != "/pki/cert.pem" || f.Controller.TLS.KeyFile != "/pki/key.pem" {
		t.Errorf("tls section = %+v", f.Controller.TLS)
	}
	if len(f.Envs) != 2 {
		t.Fatalf("len(Envs) = %d, want 2", len(f.Envs))
	}
	e := f.Envs[0]
	if e.EnvID != "env-1" || len(e.Frontends) != 2 || e.QuotaBitsPerSec != 200_000_000 {
		t.Errorf("env[0] = %+v", e)
	}
	if got := e.QuotaBytesPerSec(); got != 25_000_000 {
		t.Errorf("QuotaBytesPerSec = %v, want 25e6", got)
	}
	if e.Params == nil || e.Params.ElasticCeiling != 1.2 {
		t.Errorf("env[0].Params = %+v", e.Params)
	}
	if f.Envs[1].Params != nil {
		t.Errorf("env[1].Params = %+v, want nil", f.Envs[1].Params)
	}
}

func TestLoadDefaults(t *testing.T) {
	f, err := Load(writeConfig(t, "node_id: n1\n"))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if f.Mode != model.ModeDryRun {
		t.Errorf("Mode = %q, want dry-run", f.Mode)
	}
	if f.LogLevel != "info" {
		t.Errorf("LogLevel = %q, want info", f.LogLevel)
	}
	if f.HAProxy.StatsSocket != DefaultStatsSocket {
		t.Errorf("StatsSocket = %q", f.HAProxy.StatsSocket)
	}
	if f.HAProxy.BwlimMapPath != DefaultBwlimMapPath {
		t.Errorf("BwlimMapPath = %q", f.HAProxy.BwlimMapPath)
	}
	if f.HAProxy.TimeoutMS != DefaultTimeoutMS {
		t.Errorf("TimeoutMS = %d", f.HAProxy.TimeoutMS)
	}
	if f.Controller.CachePath != DefaultCachePath {
		t.Errorf("CachePath = %q", f.Controller.CachePath)
	}
	if f.Controller.BaseURL != "" || len(f.Envs) != 0 {
		t.Errorf("unexpected non-defaults: %+v", f)
	}
}

func TestLoadMissingFile(t *testing.T) {
	if _, err := Load(filepath.Join(t.TempDir(), "absent.yaml")); err == nil {
		t.Fatal("Load succeeded on missing file")
	}
}

func TestLoadBadYAML(t *testing.T) {
	if _, err := Load(writeConfig(t, "node_id: [unclosed\n")); err == nil {
		t.Fatal("Load succeeded on malformed YAML")
	}
}

func TestValidateErrors(t *testing.T) {
	const validEnv = `
envs:
  - env_id: e1
    frontends: [fe1]
    quota_bps: 1000
`
	cases := []struct {
		name    string
		yaml    string
		wantErr string
	}{
		{"missing node_id", "mode: enforce\n" + validEnv, "node_id"},
		{"bad mode", "node_id: n1\nmode: audit\n", `mode "audit"`},
		{"bad log_level", "node_id: n1\nlog_level: trace\n", "log_level"},
		{"missing env_id", "node_id: n1\nenvs:\n  - frontends: [fe1]\n    quota_bps: 1000\n", "env_id"},
		{"zero quota", "node_id: n1\nenvs:\n  - env_id: e1\n    frontends: [fe1]\n    quota_bps: 0\n", "quota_bps"},
		{"negative quota", "node_id: n1\nenvs:\n  - env_id: e1\n    frontends: [fe1]\n    quota_bps: -5\n", "quota_bps"},
		{"no frontends", "node_id: n1\nenvs:\n  - env_id: e1\n    quota_bps: 1000\n", "frontend"},
		{"empty frontend name", "node_id: n1\nenvs:\n  - env_id: e1\n    frontends: [\"\"]\n    quota_bps: 1000\n", "empty frontend"},
		{
			"duplicate frontend across envs",
			"node_id: n1\nenvs:\n  - env_id: e1\n    frontends: [fe1]\n    quota_bps: 1000\n  - env_id: e2\n    frontends: [fe1]\n    quota_bps: 2000\n",
			`frontend "fe1"`,
		},
		{
			"duplicate frontend within env",
			"node_id: n1\nenvs:\n  - env_id: e1\n    frontends: [fe1, fe1]\n    quota_bps: 1000\n",
			`frontend "fe1"`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Load(writeConfig(t, tc.yaml))
			if err == nil {
				t.Fatal("Load succeeded, want error")
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("error %q does not mention %q", err, tc.wantErr)
			}
		})
	}
}

func TestSlogLevel(t *testing.T) {
	cases := map[string]slog.Level{
		"debug": slog.LevelDebug,
		"info":  slog.LevelInfo,
		"warn":  slog.LevelWarn,
		"error": slog.LevelError,
		"":      slog.LevelInfo, // fallback
	}
	for in, want := range cases {
		f := File{LogLevel: in}
		if got := f.SlogLevel(); got != want {
			t.Errorf("SlogLevel(%q) = %v, want %v", in, got, want)
		}
	}
}
