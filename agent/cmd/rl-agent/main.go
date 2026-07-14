// rl-agent is the per-node rate-limiter agent: it samples HAProxy frontend
// bytes_out every second, runs the AIMD fast loop against controller-assigned
// quotas, and applies bwlim map updates (dry-run or enforce).
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ipest-info/haproxy_ingress_rate_limiter/agent/internal/collector"
	"github.com/ipest-info/haproxy_ingress_rate_limiter/agent/internal/config"
	"github.com/ipest-info/haproxy_ingress_rate_limiter/agent/internal/core"
	"github.com/ipest-info/haproxy_ingress_rate_limiter/agent/internal/executor"
	"github.com/ipest-info/haproxy_ingress_rate_limiter/agent/internal/governor"
	"github.com/ipest-info/haproxy_ingress_rate_limiter/agent/internal/haproxy"
	"github.com/ipest-info/haproxy_ingress_rate_limiter/agent/internal/reporter"
	"github.com/ipest-info/haproxy_ingress_rate_limiter/internal/model"
)

// agentVersion is stamped at build time via -ldflags "-X main.agentVersion=...".
var agentVersion = "dev"

func main() {
	cfgPath := flag.String("config", "/etc/rl-agent/config.yaml", "path to agent config file")
	showVersion := flag.Bool("version", false, "print version and exit")
	flag.Parse()
	if *showVersion {
		fmt.Println("rl-agent", agentVersion)
		return
	}

	cfg, err := config.Load(*cfgPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, "rl-agent:", err)
		os.Exit(1)
	}

	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: cfg.SlogLevel()}))
	slog.SetDefault(log)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	rt := haproxy.New(cfg.HAProxy.StatsSocket, cfg.HAProxy.Timeout())
	col := collector.New(rt, log)
	gov := governor.New(log)
	exe := executor.NewSwitchable(rt, cfg.HAProxy.BwlimMapPath, cfg.Mode, log)

	var rep *reporter.Reporter
	var configs <-chan model.ControllerConfig // nil in standalone mode: never fires
	if cfg.Controller.BaseURL != "" {
		rep = reporter.New(reporter.Options{
			BaseURL:      cfg.Controller.BaseURL,
			NodeID:       cfg.NodeID,
			CachePath:    cfg.Controller.CachePath,
			AgentVersion: agentVersion,
			TLSCAFile:    cfg.Controller.TLS.CAFile,
			TLSCertFile:  cfg.Controller.TLS.CertFile,
			TLSKeyFile:   cfg.Controller.TLS.KeyFile,
		}, log)
		configs = rep.Configs()
	}

	// The sampler closure reads loop.Version(); loop is assigned before Run
	// starts, so the deferred capture is safe.
	var loop *core.Loop
	comps := core.Components{Collector: col, Governor: gov, Executor: exe}
	if rep != nil {
		comps.Sampler = func(now time.Time, usages []model.EnvUsage, ds []model.Decision) {
			rep.AddSample(now, usages, ds, exe.Mode(), loop.Version())
		}
	} else {
		ticks := 0
		comps.Sampler = func(now time.Time, usages []model.EnvUsage, ds []model.Decision) {
			ticks++
			if ticks%60 == 0 {
				log.Debug("standalone summary",
					"ticks", ticks, "envs", len(usages), "mode", exe.Mode(), "degraded", col.Degraded())
			}
		}
	}
	loop = core.New(comps, log)

	// Bootstrap precedence: controller cache (fail-static) > local envs >
	// unlimited until the controller pushes a config.
	seeded := false
	if cfg.Controller.BaseURL != "" {
		if cached, err := reporter.LoadCache(cfg.Controller.CachePath); err == nil {
			loop.Seed(cached)
			seeded = true
			log.Info("seeded from controller cache", "path", cfg.Controller.CachePath, "version", cached.Version)
		} else {
			log.Warn("controller cache unavailable", "path", cfg.Controller.CachePath, "err", err)
		}
	}
	if !seeded && len(cfg.Envs) > 0 {
		loop.Seed(model.ControllerConfig{Version: 0, Mode: cfg.Mode, Envs: cfg.Envs})
		seeded = true
		log.Info("seeded from local envs", "envs", len(cfg.Envs))
	}
	if !seeded {
		log.Warn("no bootstrap config: starting unlimited, waiting for controller")
	}

	if rep != nil {
		go rep.Run(ctx)
	}

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	log.Info("rl-agent started",
		"node_id", cfg.NodeID, "mode", exe.Mode(), "controller", cfg.Controller.BaseURL, "version", agentVersion)
	loop.Run(ctx, ticker.C, configs)
	log.Info("shutting down")
}
