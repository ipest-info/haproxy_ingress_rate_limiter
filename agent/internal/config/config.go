// Package config loads and validates the rl-agent local YAML configuration.
//
// The local file provides node identity, HAProxy wiring, controller endpoint
// and optional standalone/bootstrap quotas. Quota values follow the ops
// convention: BITS per second (see model.EnvQuota).
package config

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/ipest-info/haproxy_ingress_rate_limiter/internal/model"
)

// Defaults applied by Load when the corresponding key is absent.
const (
	DefaultStatsSocket  = "/var/run/haproxy/admin.sock"
	DefaultBwlimMapPath = "/etc/haproxy/maps/bwlim.map"
	DefaultTimeoutMS    = 500
	DefaultCachePath    = "/var/lib/rl-agent/config-cache.json"
	DefaultLogLevel     = "info"
)

// HAProxy configures access to the local HAProxy runtime API.
type HAProxy struct {
	StatsSocket  string `yaml:"stats_socket"`
	BwlimMapPath string `yaml:"bwlim_map_path"`
	TimeoutMS    int    `yaml:"timeout_ms"`
}

// Timeout returns TimeoutMS as a time.Duration.
func (h HAProxy) Timeout() time.Duration {
	return time.Duration(h.TimeoutMS) * time.Millisecond
}

// TLS holds optional mTLS material for the controller connection.
type TLS struct {
	CAFile   string `yaml:"ca_file"`
	CertFile string `yaml:"cert_file"`
	KeyFile  string `yaml:"key_file"`
}

// Controller configures the control-plane connection. An empty BaseURL means
// standalone mode: the agent runs from local envs only.
type Controller struct {
	BaseURL   string `yaml:"base_url"`
	CachePath string `yaml:"cache_path"`
	TLS       TLS    `yaml:"tls"`
}

// File is the on-disk agent configuration.
type File struct {
	NodeID     string           `yaml:"node_id"`
	Mode       string           `yaml:"mode"`      // dry-run | enforce
	LogLevel   string           `yaml:"log_level"` // debug | info | warn | error
	HAProxy    HAProxy          `yaml:"haproxy"`
	Controller Controller       `yaml:"controller"`
	Envs       []model.EnvQuota `yaml:"envs"` // standalone/bootstrap quotas
}

// Load reads path, unmarshals YAML, applies defaults and validates.
func Load(path string) (*File, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	var f File
	if err := yaml.Unmarshal(data, &f); err != nil {
		return nil, fmt.Errorf("parse config %s: %w", path, err)
	}
	f.applyDefaults()
	if err := f.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config %s: %w", path, err)
	}
	return &f, nil
}

func (f *File) applyDefaults() {
	if f.Mode == "" {
		f.Mode = model.ModeDryRun
	}
	if f.LogLevel == "" {
		f.LogLevel = DefaultLogLevel
	}
	if f.HAProxy.StatsSocket == "" {
		f.HAProxy.StatsSocket = DefaultStatsSocket
	}
	if f.HAProxy.BwlimMapPath == "" {
		f.HAProxy.BwlimMapPath = DefaultBwlimMapPath
	}
	if f.HAProxy.TimeoutMS <= 0 {
		f.HAProxy.TimeoutMS = DefaultTimeoutMS
	}
	if f.Controller.CachePath == "" {
		f.Controller.CachePath = DefaultCachePath
	}
}

// Validate enforces the structural invariants the rest of the agent relies on.
func (f *File) Validate() error {
	if f.NodeID == "" {
		return errors.New("node_id is required")
	}
	if f.Mode != model.ModeDryRun && f.Mode != model.ModeEnforce {
		return fmt.Errorf("mode %q: must be %q or %q", f.Mode, model.ModeDryRun, model.ModeEnforce)
	}
	switch f.LogLevel {
	case "debug", "info", "warn", "error":
	default:
		return fmt.Errorf("log_level %q: must be one of debug, info, warn, error", f.LogLevel)
	}
	frontendOwner := make(map[string]string)
	for i, e := range f.Envs {
		if e.EnvID == "" {
			return fmt.Errorf("envs[%d]: env_id is required", i)
		}
		if e.QuotaBitsPerSec <= 0 {
			return fmt.Errorf("envs[%d] (%s): quota_bps must be > 0", i, e.EnvID)
		}
		if len(e.Frontends) == 0 {
			return fmt.Errorf("envs[%d] (%s): at least one frontend is required", i, e.EnvID)
		}
		for _, fe := range e.Frontends {
			if fe == "" {
				return fmt.Errorf("envs[%d] (%s): empty frontend name", i, e.EnvID)
			}
			if owner, dup := frontendOwner[fe]; dup {
				return fmt.Errorf("frontend %q mapped to both %q and %q", fe, owner, e.EnvID)
			}
			frontendOwner[fe] = e.EnvID
		}
	}
	return nil
}

// SlogLevel maps LogLevel to a slog.Level (unknown values fall back to info).
func (f *File) SlogLevel() slog.Level {
	switch f.LogLevel {
	case "debug":
		return slog.LevelDebug
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
