// mock-controller 是真实控制面的开发替身：用一个 JSON 文件充当配置
// 源，实现设计文档 §3.6 定义的三个 agent 端点：
//
//	GET  /v1/agent/config     长轮询下发配置；Version = 配置文件 mtime（unix 秒）
//	POST /v1/agent/metrics    仅记日志后丢弃
//	POST /v1/agent/heartbeat  仅记日志后丢弃
//
// 版本机制（mtime 即版本）：真实控制面用数据库里的单调版本号，mock
// 直接借用配置文件的修改时间（unix 秒）——只要编辑/touch 文件，mtime
// 变化即视为新版本，被挂起的长轮询立即拿到新内容返回。代价是同一秒
// 内的多次修改只能算一个版本，且回拨 mtime 也会被当作"变化"——对
// 开发场景足够。无鉴权、无 TLS、无持久化：仅限开发/联调使用。
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
	// longPollHold 是长轮询的最长挂起时间。必须小于 agent 侧 40s 的
	// 请求超时，保证"配置无变化"时以干净的 204 收尾，而不是让
	// agent 观察到客户端超时错误。
	longPollHold = 25 * time.Second
	// reloadInterval 是轮询配置文件 mtime 的周期：mtime 变化最迟
	// 1 秒内被发现并广播给所有挂起的长轮询。
	reloadInterval = 1 * time.Second
	// maxBodyBytes 限制读取 agent POST 请求体的上限，防止异常
	// 大包耗尽内存。
	maxBodyBytes = 8 << 20
)

// store 持有当前配置，并在配置变化时唤醒所有挂起的长轮询。
// 唤醒采用"关闭并替换通道"的经典广播手法：每个等待者持有当前
// wait 通道，set 关闭旧通道（唤醒所有人）并换上新通道供下一轮等待。
type store struct {
	log *slog.Logger

	mu   sync.Mutex
	cfg  model.ControllerConfig // Version = 配置文件 mtime（unix 秒）
	wait chan struct{}          // 每次版本变化时被关闭并替换
}

// snapshot 返回当前配置，以及"下一次变化时会被关闭"的通知通道。
// 二者在同一把锁下取出，保证等待者不会错过紧随其后的变更。
func (s *store) snapshot() (model.ControllerConfig, <-chan struct{}) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cfg, s.wait
}

// set 安装新配置并释放所有挂起的长轮询。
func (s *store) set(cfg model.ControllerConfig) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cfg = cfg
	close(s.wait)
	s.wait = make(chan struct{})
}

// loadConfig 把 path 读取为不含版本号的 model.ControllerConfig，
// 并用文件 mtime（unix 秒）盖章为 Version，随后 Normalize 补默认值。
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

// watchConfig 每 reloadInterval 轮询一次文件 mtime，发现与当前版本
// 不一致就重新加载并广播。加载失败（文件被删、JSON 编辑到一半等）
// 时保留旧配置继续服务，只记日志——mock 也遵循 fail-static 精神。
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

// handleConfig 实现配置长轮询。协议行为：
//   - 客户端带上自己已应用的版本号（?version=...）；
//   - 版本与服务端不一致（含首次请求 version=0）：立即 200 + JSON 全量配置；
//   - 版本一致：挂起等待，直到 (a) 配置变化——被 store.set 关闭的
//     通道唤醒，循环回到顶部重新快照并返回新配置；(b) 挂满
//     longPollHold——返回 204 让客户端立即重发下一轮；(c) 客户端
//     断开——直接返回。
func handleConfig(st *store, log *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		nodeID := r.URL.Query().Get("node_id")
		clientVer, _ := strconv.ParseInt(r.URL.Query().Get("version"), 10, 64)
		start := time.Now()

		deadline := time.NewTimer(longPollHold)
		defer deadline.Stop()
		for {
			cfg, changed := st.snapshot()
			if cfg.Version != clientVer {
				w.Header().Set("Content-Type", "application/json")
				if err := json.NewEncoder(w).Encode(cfg); err != nil {
					log.Warn("config write failed", "node_id", nodeID, "remote_addr", r.RemoteAddr, "err", err)
					return
				}
				log.Info("config served",
					"node_id", nodeID, "remote_addr", r.RemoteAddr,
					"client_version", clientVer,
					"version", cfg.Version, "mode", cfg.Mode, "envs", len(cfg.Envs),
					"waited_ms", time.Since(start).Milliseconds())
				return
			}
			select {
			case <-changed:
				// 配置已变化：回到循环顶部重新快照并返回新配置。
			case <-deadline.C:
				w.WriteHeader(http.StatusNoContent)
				return
			case <-r.Context().Done():
				return
			}
		}
	}
}

// decodeLoose 把请求体解析为无类型的 JSON 对象，agent 上报格式演进
// 时 mock 不需要同步改结构体。
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

// handleMetrics 接收 agent 的用量样本批次：只记录节点与样本条数，
// 数据即弃（真实控制面会以此驱动慢环的配额再切分）。
func handleMetrics(log *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		m, err := decodeLoose(r)
		if err != nil {
			log.Warn("metrics decode failed", "remote_addr", r.RemoteAddr, "err", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		samples := 0
		if s, ok := m["samples"].([]any); ok {
			samples = len(s)
		}
		log.Info("metrics received", "node_id", m["node_id"], "remote_addr", r.RemoteAddr, "samples", samples)
		w.WriteHeader(http.StatusNoContent)
	}
}

// handleHeartbeat 接收 agent 心跳：记录节点身份、agent 版本、运行
// 模式与已应用的配置版本，便于联调时确认配置是否推送到位。
func handleHeartbeat(log *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		m, err := decodeLoose(r)
		if err != nil {
			log.Warn("heartbeat decode failed", "remote_addr", r.RemoteAddr, "err", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		log.Info("heartbeat received",
			"node_id", m["node_id"], "remote_addr", r.RemoteAddr,
			"agent_version", m["agent_version"],
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

	// 刻意不设 WriteTimeout：配置端点要把连接挂起 longPollHold 之久，
	// 全局写超时会把正常的长轮询误杀。
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
