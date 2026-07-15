// rl-agent 是部署在每台 HAProxy 节点上的限速代理，即设计文档 §2.1
// 中的"本地快环"：每秒从 HAProxy stats socket 采样各 frontend 的
// bytes_out，按控制面下发的节点配额运行 AIMD 快环算法（§3.3），把
// 整形值写入 HAProxy 的 bwlim map（dry-run 模式下只记录不写入）。
// 断联时按最后一次配置继续限速（fail-static，§3.7）。
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
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

// agentVersion 在构建时通过 -ldflags "-X main.agentVersion=..." 注入，
// 会随心跳上报给控制面，便于灰度期间核对各节点的 agent 版本。
var agentVersion = "dev"

// summarizeEnvs 把本地静态 envs 压缩成单个日志字段，格式：
// "env_id=e1,quota_bps=200000000,frontends=fe1|fe2;..."。
// quota_bps 为配置口径的 bits/s。
func summarizeEnvs(envs []model.EnvQuota) string {
	var b strings.Builder
	for i, e := range envs {
		if i > 0 {
			b.WriteByte(';')
		}
		fmt.Fprintf(&b, "env_id=%s,quota_bps=%d,frontends=%s",
			e.EnvID, e.QuotaBitsPerSec, strings.Join(e.Frontends, "|"))
	}
	return b.String()
}

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

	// 启动即输出完整配置摘要：现场排障时第一条要看的日志，
	// 可直接核对节点身份、模式、HAProxy 接入点与配额来源。
	log.Info("config loaded",
		"path", *cfgPath,
		"node_id", cfg.NodeID,
		"mode", cfg.Mode,
		"stats_socket", cfg.HAProxy.StatsSocket,
		"bwlim_map_path", cfg.HAProxy.BwlimMapPath,
		"controller_configured", cfg.Controller.BaseURL != "",
		"controller_base_url", cfg.Controller.BaseURL,
		"cache_path", cfg.Controller.CachePath,
		"envs", len(cfg.Envs),
		"envs_detail", summarizeEnvs(cfg.Envs))

	// ctx 在收到 SIGINT/SIGTERM 时取消，驱动快环与上报器退出。
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	// NotifyContext 只提供取消信号、不暴露信号本身；这里额外注册一个
	// 通道，仅用于把触发退出的信号名写进日志（同一信号会同时投递到
	// 两处注册，互不影响；该 goroutine 随进程退出回收）。
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		if sig, ok := <-sigCh; ok {
			log.Info("shutdown signal received", "signal", sig.String())
		}
	}()

	// 组装快环三组件：HAProxy runtime API 客户端同时充当采集来源
	//（show stat）与执行通道（set map）。
	rt := haproxy.New(cfg.HAProxy.StatsSocket, cfg.HAProxy.Timeout())
	col := collector.New(rt, log)
	gov := governor.New(log)
	exe := executor.NewSwitchable(rt, cfg.HAProxy.BwlimMapPath, cfg.Mode, log)

	// 配置了控制面 base_url 才启用上报器与配置长轮询；否则 configs
	// 保持 nil（nil 通道永不就绪），快环进入纯本地的独立运行模式。
	var rep *reporter.Reporter
	var configs <-chan model.ControllerConfig
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

	// Sampler 闭包引用 loop.Version()；loop 在 Run 启动前完成赋值，
	// 而 Sampler 只会在 Run 的 tick 中被调用，因此延迟捕获是安全的。
	var loop *core.Loop
	comps := core.Components{Collector: col, Governor: gov, Executor: exe}
	if rep != nil {
		// 接入控制面：每个 tick 的结果交给上报器缓冲，按下发的间隔
		// 批量上报（慢环据此重新切分配额）。
		comps.Sampler = func(now time.Time, usages []model.EnvUsage, ds []model.Decision) {
			rep.AddSample(now, usages, ds, exe.Mode(), loop.Version())
		}
	} else {
		// 独立运行：无处上报，仅每 60 个 tick 打一条 debug 汇总，
		// 确认循环存活（info 级的周期汇总由 core.Loop 自己输出）。
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

	// 启动引导（Seed）优先级：控制面缓存 > 本地静态 envs > 无限速等待。
	//
	//  1. 配置了控制面时优先用本地缓存的最后一次下发配置引导——这正是
	//     fail-static（§3.7）在"重启后控制面恰好不可达"场景下的延伸：
	//     缓存里的配额比本地静态配置新（是慢环最近一次切分的结果），
	//     用它引导可保证重启前后限速行为连续，绝不放开为不限速。
	//  2. 无缓存（首次部署/缓存被清）时退回本地静态 envs：粗粒度但
	//     安全的兜底配额，同时也是纯独立运行模式的唯一配置来源。
	//  3. 两者皆无时不限速启动，等待控制面首次下发——此时节点尚无
	//     任何配额信息，凭空限速比不限速更危险（可能误伤全部流量），
	//     故记 warn 提醒运维这是一个需要关注的空窗期。
	seeded := false
	if cfg.Controller.BaseURL != "" {
		if cached, err := reporter.LoadCache(cfg.Controller.CachePath); err == nil {
			loop.Seed(cached)
			seeded = true
			log.Info("seeded from controller cache",
				"path", cfg.Controller.CachePath,
				"version", cached.Version,
				"mode", cached.Mode,
				"envs", len(cached.Envs))
		} else {
			log.Warn("controller cache unavailable", "path", cfg.Controller.CachePath, "err", err)
		}
	}
	if !seeded && len(cfg.Envs) > 0 {
		loop.Seed(model.ControllerConfig{Version: 0, Mode: cfg.Mode, Envs: cfg.Envs})
		seeded = true
		log.Info("seeded from local envs",
			"mode", cfg.Mode,
			"envs", len(cfg.Envs),
			"envs_detail", summarizeEnvs(cfg.Envs))
	}
	if !seeded {
		log.Warn("no bootstrap config: starting unlimited, waiting for controller",
			"controller_base_url", cfg.Controller.BaseURL)
	}

	if rep != nil {
		go rep.Run(ctx)
	}

	// 1s ticker 驱动快环；Run 阻塞至 ctx 取消（收到退出信号）。
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	log.Info("rl-agent started",
		"node_id", cfg.NodeID, "mode", exe.Mode(), "controller", cfg.Controller.BaseURL, "version", agentVersion)
	loop.Run(ctx, ticker.C, configs)
	log.Info("shutting down")
}
