// Package reporter 是 rl-agent 面向 Controller 的客户端（设计文档 §3.6
// "Agent ⇆ Controller 接口"），承担三条独立的通信链路：
//
//  1. 配置长轮询：GET /v1/agent/config 携带本地已应用的 config_version，
//     Controller 有变更立即返回新配置（200），无变更则挂住直至约 30s 超时
//     （204 或客户端侧超时），Agent 随即重新发起轮询——变更秒级可达，
//     且空闲时不产生轮询风暴。
//  2. 指标批量上报：POST /v1/agent/metrics，每 5s 一批（内含每秒明细样本）。
//  3. 心跳：POST /v1/agent/heartbeat，每 10s 上报节点身份/模式/配置版本，
//     供 Controller 判断节点存活与配置是否收敛。
//
// fail-static 容错（设计文档 §3.7）在本包的落点：每次收到新配置都先原子
// 落盘到本地缓存文件，再投递给 agent core；进程重启后 core 通过 LoadCache
// 读回"最后一次下发的配置"，在 Controller 不可达期间继续按既有配额限速，
// 绝不因断联而放开为不限速。
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
	// requestTimeout 是所有 HTTP 请求的客户端超时。必须大于 Controller 的
	// 长轮询挂起时长（§3.6 约定 30s）：否则每次长轮询都会在服务端返回前
	// 被客户端掐断，长轮询退化为纯超时重试。40s 留出 10s 网络余量。
	requestTimeout = 40 * time.Second
	// defaultFlushInterval 是指标批量上报的节拍（§3.6：每 5s 批量）。
	// Controller 下发配置中的 ReportIntervalS 可以调低该值，但当前骨架
	// 使用固定 ticker，仅在此记录该旋钮的存在，尚未接线。
	defaultFlushInterval = 5 * time.Second
	// defaultHeartbeatInterval 是心跳周期，对应 §3.6 的"每 10s"。
	defaultHeartbeatInterval = 10 * time.Second
	// maxBufferedSamples 是 Controller 不可达期间指标缓冲的上限。按核心循环
	// 每秒一个样本计算约可缓存 10 分钟；超出后丢弃最旧样本（新数据比旧数据
	// 更有诊断价值），从而把断联期间的内存占用限制在常数级。
	maxBufferedSamples = 600
	// backoffMin/backoffMax 界定配置轮询失败后的指数退避区间：
	// 首次失败等 1s，之后逐次翻倍，封顶 30s（见 nextBackoff/withJitter）。
	backoffMin = 1 * time.Second
	backoffMax = 30 * time.Second
	// maxResponseBytes 限制读取任何 Controller 响应体的字节数上限（8 MiB），
	// 防止异常/恶意响应把 Agent 内存撑爆。
	maxResponseBytes = 8 << 20

	// Controller 侧的三个接口路径（§3.6），不可变更。
	configPath    = "/v1/agent/config"
	metricsPath   = "/v1/agent/metrics"
	heartbeatPath = "/v1/agent/heartbeat"
)

// dropLogEvery 控制"缓冲满丢弃最旧样本"告警日志的节流粒度：
// 首次丢弃立即告警，之后每累计 dropLogEvery 次丢弃再告警一次，
// 避免长时间断联时每秒刷一条 warn。
const dropLogEvery = 100

// Options 是构造 Reporter 所需的全部参数。
//
// BaseURL 为空是防御性容忍（Run 只阻塞到 ctx 结束，不发任何请求）；
// 正常流程下 agent core 在 standalone 模式会直接跳过 Reporter 的构造。
type Options struct {
	// BaseURL 是 Controller 的根地址（如 https://controller:9090），
	// 末尾斜杠会被剥掉。
	BaseURL string
	// NodeID 是本节点唯一标识，随配置轮询/指标/心跳一起上报，
	// 须与控制面 LB_NODE 记录一致。
	NodeID string
	// CachePath 是 fail-static 配置缓存文件路径；为空则跳过落盘
	// （仅内存应用，重启后失去断联保护）。
	CachePath string
	// AgentVersion 随指标与心跳上报，供控制面掌握节点版本分布。
	AgentVersion string
	// TLSCAFile/TLSCertFile/TLSKeyFile 是可选的 mTLS 材料（§3.6：
	// Agent 与 Controller 双向认证，节点证书与 LB_NODE 绑定）。
	// 客户端证书与私钥必须成对提供。
	TLSCAFile   string
	TLSCertFile string
	TLSKeyFile  string
}

// envSample 是单个指标样本中某一环境（env）的切片：当前速率、两档均值、
// 活跃连接数，以及快环对该环境的最新整形决策。
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

// sample 是 AddSample 记录的一次核心循环 tick：时间戳、当时的运行模式与
// 配置版本，以及各环境的用量/决策快照。
type sample struct {
	TS            int64       `json:"ts"` // Unix 秒
	Mode          string      `json:"mode"`
	ConfigVersion int64       `json:"config_version"`
	Envs          []envSample `json:"envs"`
}

// metricsPayload 是 POST /v1/agent/metrics 的请求体：节点身份 + 一批样本。
type metricsPayload struct {
	NodeID       string   `json:"node_id"`
	AgentVersion string   `json:"agent_version"`
	Samples      []sample `json:"samples"`
}

// heartbeatPayload 是 POST /v1/agent/heartbeat 的请求体，携带节点当前
// 运行模式与已应用的配置版本，供控制面判断配置是否已收敛到各节点。
type heartbeatPayload struct {
	NodeID        string `json:"node_id"`
	AgentVersion  string `json:"agent_version"`
	Mode          string `json:"mode"`
	ConfigVersion int64  `json:"config_version"`
}

// Reporter 封装与 Controller 的全部通信。AddSample 由 agent core 循环调用；
// Run 启动的轮询/上报/心跳三个 goroutine 之间只通过 mu 保护的字段与
// atomic 的 version 共享状态。
type Reporter struct {
	opts    Options
	baseURL string // opts.BaseURL 去掉末尾斜杠后的形式
	log     *slog.Logger
	client  *http.Client

	// configs 是向 agent core 投递新配置的通道：容量 1 且由 pushConfig
	// 实现"合并"（coalescing）语义——消费慢时旧配置被新配置顶掉，
	// core 永远只看到最新一份（中间版本没有单独应用的价值）。
	configs chan model.ControllerConfig
	// version 是长轮询已应用的最新配置版本，随请求上报给 Controller
	// 作为"我已有到哪个版本"的水位（幂等应用的依据，§3.6）。
	version atomic.Int64

	mu                sync.Mutex
	samples           []sample // 有界指标缓冲（上限 maxBufferedSamples）
	lastMode          string   // AddSample 最近一次传入的模式，用于心跳上报
	lastConfigVersion int64    // AddSample 最近一次传入的配置版本，用于心跳上报
	dropEvents        uint64   // 缓冲满丢弃的累计次数（mu 保护），用于告警节流
	droppedTotal      uint64   // 缓冲满丢弃的样本总数（mu 保护）

	// 心跳连续失败计数。仅心跳 goroutine 读写，无需加锁。
	heartbeatFailures int

	// 两个周期以字段形式暴露，便于测试在 Run 之前调短。
	flushInterval     time.Duration
	heartbeatInterval time.Duration
}

// New 构造 Reporter。log 为 nil 时回落到 slog.Default()。
//
// TLS 材料配置了却加载失败时：记录 error 后降级为普通 HTTP 客户端，而不是
// 让构造失败——Agent 必须先启动起来进入 fail-static（按本地缓存继续限速），
// 证书问题留给运维修复，不能因此拖垮数据面。
func New(opts Options, log *slog.Logger) *Reporter {
	if log == nil {
		log = slog.Default()
	}
	client := &http.Client{Timeout: requestTimeout}
	if opts.TLSCAFile != "" || opts.TLSCertFile != "" || opts.TLSKeyFile != "" {
		tlsCfg, err := buildTLSConfig(opts)
		if err != nil {
			log.Error("tls client config failed; falling back to default http client",
				"err", err,
				"ca_file", opts.TLSCAFile,
				"cert_file", opts.TLSCertFile,
				"key_file", opts.TLSKeyFile)
		} else {
			client.Transport = &http.Transport{TLSClientConfig: tlsCfg}
			log.Info("tls client configured",
				"ca_file", opts.TLSCAFile,
				"cert_file", opts.TLSCertFile,
				"mutual_tls", opts.TLSCertFile != "")
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

// buildTLSConfig 用配置的 CA 证书与客户端证书对组装可用于 mTLS 的
// tls.Config。三个文件各自可选，但客户端证书与私钥必须同时提供——
// 只给一半无法完成 TLS 握手，直接报错让运维尽早发现。
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

// LoadCache 读取长轮询循环写下的 fail-static 配置缓存（§3.7）：Agent
// 重启后据此恢复"最后一次下发的配置"，在 Controller 不可达期间维持限速。
//
// 文件不存在时返回零值配置和包裹 os.ErrNotExist 的错误，调用方可以据此
// 区分"首次启动、尚无缓存"（正常，走本地 envs 引导）与"缓存损坏"（异常，
// 需要告警）两种情形。
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

// Configs 返回投递新配置的只读通道。通道容量为 1 且具有合并语义：
// 消费方阻塞期间到达多个版本时，只有最新一份会被读到（见 pushConfig）。
func (r *Reporter) Configs() <-chan model.ControllerConfig {
	return r.configs
}

// AddSample 把一次核心循环 tick 记入有界指标缓冲。它只做加锁的内存操作、
// 不含任何 I/O，因此对每秒调用一次的核心循环是非阻塞的，且与并发进行中的
// flush 安全共存。缓冲已满时丢弃最旧样本（保新弃旧），丢弃告警按
// "首次 + 每 dropLogEvery 次"节流。
func (r *Reporter) AddSample(now time.Time, usages []model.EnvUsage, decisions []model.Decision, mode string, configVersion int64) {
	// 决策按 env_id 建索引，再与用量逐环境拼合成完整样本。
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
		dropped := len(r.samples) - maxBufferedSamples
		// 重切片保留原底层数组；append 在容量耗尽时会重新分配并拷贝一次，
		// 因此旧数据的驻留是有界的，不会无限增长。
		r.samples = r.samples[len(r.samples)-maxBufferedSamples:]
		r.droppedTotal += uint64(dropped)
		r.dropEvents++
		// 断联期间每秒都会触发丢弃，按"首次 + 每 dropLogEvery 次"节流告警。
		if r.dropEvents == 1 || r.dropEvents%dropLogEvery == 0 {
			r.log.Warn("metrics buffer full; dropped oldest samples",
				"dropped_now", dropped,
				"dropped_total", r.droppedTotal,
				"buffer_cap", maxBufferedSamples)
		}
	}
}

// Run 启动配置长轮询、指标上报、心跳三个 goroutine 并阻塞至 ctx 结束。
// BaseURL 为空（standalone 模式的防御分支）时只阻塞、不发任何请求。
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

// pollLoop 是配置长轮询主循环，按结果分三路处理：
//   - 200：应用新配置并立即重新轮询；
//   - 204 / 客户端长轮询超时：Controller 无变更，属于正常静默，
//     立即重连（这正是长轮询"挂住等变更"的设计，不算失败）；
//   - 传输错误 / 非预期状态码：按指数退避重试（1s 起步、逐次翻倍、
//     封顶 30s、带抖动），避免 Controller 故障恢复瞬间被全体 Agent 打爆。
func (r *Reporter) pollLoop(ctx context.Context) {
	backoff := time.Duration(0)
	failures := 0 // 连续失败次数，成功或超时即清零
	for ctx.Err() == nil {
		cfg, err := r.pollOnce(ctx)
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			if isTimeout(err) {
				// 长轮询到期而 Controller 无动静：正常现象，立即重连。
				r.log.Debug("config long poll timed out; re-polling",
					"version", r.version.Load())
				backoff = 0
				failures = 0
				continue
			}
			failures++
			backoff = nextBackoff(backoff)
			d := withJitter(backoff)
			r.log.Warn("config poll failed",
				"err", err,
				"retry_in", d,
				"consecutive_failures", failures)
			if !sleepCtx(ctx, d) {
				return
			}
			continue
		}
		backoff = 0
		failures = 0
		if cfg != nil {
			r.applyConfig(*cfg)
		} else {
			// 204：Controller 明确表示"当前版本即最新"，立即重新挂起等待。
			r.log.Debug("config long poll returned no content; re-polling",
				"version", r.version.Load())
		}
	}
}

// pollOnce 发起一次长轮询请求，query 携带 node_id 与本地已应用的配置版本。
// 返回 (nil, nil) 表示 204（"暂无更新"）。响应体读取受 maxResponseBytes
// 限制；defer 中排空剩余字节以便底层连接可被复用。
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

// applyConfig 处理一份新收到的配置，顺序刻意为"先落盘、再投递、最后推版本"：
//
//  1. Normalize：单位换算/字段兜底，与 LoadCache 读回时的处理保持一致；
//  2. persistCache：先写 fail-static 缓存——即使落盘失败，配置仍在内存中
//     生效（只是断联保护退化到上一份缓存），并以 error 级别告警；
//  3. pushConfig：投递给 agent core（合并语义，见 pushConfig）；
//  4. version.Store：推进长轮询水位，下一次轮询从新版本继续。
func (r *Reporter) applyConfig(cfg model.ControllerConfig) {
	versionOld := r.version.Load()
	cfg.Normalize()
	if err := r.persistCache(cfg); err != nil {
		// 配置仍然在内存中生效；受影响的只有 fail-static 缓存（变旧）。
		r.log.Error("persist config cache failed", "err", err, "path", r.opts.CachePath)
	}
	r.pushConfig(cfg)
	r.version.Store(cfg.Version)
	r.log.Info("controller config received",
		"version_old", versionOld,
		"version_new", cfg.Version,
		"mode", cfg.Mode,
		"envs", len(cfg.Envs))
}

// persistCache 原子替换 fail-static 缓存文件：先写同目录下的临时文件并
// fsync，再 rename 到目标路径。rename 在同一文件系统内是原子操作，因此
// 无论进程在哪一步崩溃，缓存文件要么是完整的旧版本、要么是完整的新版本，
// 绝不会出现"写了一半"的残缺 JSON——这是 fail-static（§3.7）成立的前提：
// 重启后 LoadCache 必须能读到一份可解析的配置。
// 临时文件必须与目标同目录，否则 rename 可能跨文件系统而失去原子性。
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
	r.log.Debug("config cache written",
		"path", r.opts.CachePath,
		"bytes", len(data))
	return nil
}

// pushConfig 把 cfg 投入容量为 1 的通道，并实现合并语义：若通道里还压着
// 一份未被消费的旧配置，先弹掉它再投递新的，保证慢消费者读到的永远是
// 最新版本。中间版本没有必须应用的价值（配置是全量下发、幂等应用的），
// 跳过它们反而避免 core 追着过期配置做无用功。
// 生产者只有轮询 goroutine 一个，因此"弹旧-投新"的循环必然在有限步内结束。
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

// flushLoop 按固定节拍（flushInterval，默认 5s）上报缓冲中的指标样本。
// Controller 配置里的 ReportIntervalS 旋钮当前刻意未接线：骨架阶段保持
// 固定 5s 节拍，与配置默认值一致。
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

// flushOnce 摘走当前缓冲并 POST 给 Controller。
//
// 失败时把这批样本放回缓冲头部（保持时间顺序，排在上报期间新到样本之前），
// 并重新套用缓冲上限——因此断联期间样本不会丢在半路上，只会在缓冲溢出时
// 从最旧的开始被淘汰。摘缓冲/回填都在锁内完成，HTTP 请求在锁外进行，
// 不会阻塞 AddSample。
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
	start := time.Now()
	if err := r.postJSON(ctx, metricsPath, payload); err != nil {
		// 回填批次并重新套用上限，随后记录当前水位便于观察积压程度。
		r.mu.Lock()
		r.samples = append(batch, r.samples...)
		if len(r.samples) > maxBufferedSamples {
			r.samples = r.samples[len(r.samples)-maxBufferedSamples:]
		}
		buffered := len(r.samples)
		r.mu.Unlock()
		if ctx.Err() == nil {
			r.log.Warn("metrics flush failed; keeping samples",
				"err", err,
				"samples", len(batch),
				"buffered", buffered,
				"buffer_cap", maxBufferedSamples)
		}
		return
	}
	r.log.Debug("metrics flushed",
		"samples", len(batch),
		"duration", time.Since(start))
}

// heartbeatLoop 启动后立即先发一次心跳（Agent 重启后让 Controller 第一
// 时间看到节点回归），之后按 heartbeatInterval 周期发送。
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

// heartbeatOnce 发送一次心跳，内容取 AddSample 最近记录的模式与配置版本。
// 失败仅告警不重试（下个周期自然重发），并携带连续失败计数——运维可据此
// 结合 §3.8 的"Agent 失联 > 1 分钟"告警判断断联时长。
func (r *Reporter) heartbeatOnce(ctx context.Context) {
	r.mu.Lock()
	payload := heartbeatPayload{
		NodeID:        r.opts.NodeID,
		AgentVersion:  r.opts.AgentVersion,
		Mode:          r.lastMode,
		ConfigVersion: r.lastConfigVersion,
	}
	r.mu.Unlock()
	if err := r.postJSON(ctx, heartbeatPath, payload); err != nil {
		if ctx.Err() == nil {
			r.heartbeatFailures++
			r.log.Warn("heartbeat failed",
				"err", err,
				"consecutive_failures", r.heartbeatFailures)
		}
		return
	}
	r.heartbeatFailures = 0
}

// postJSON 把 v 编码为 JSON 后 POST 到 baseURL+path，非 2xx 状态码视为
// 错误。响应体在 maxResponseBytes 限制内排空后关闭，保证连接可复用。
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

// nextBackoff 计算下一档退避：首次失败取 backoffMin，之后逐次翻倍，
// 封顶 backoffMax，即 1s → 2s → 4s → ... → 30s。
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

// withJitter 把 d 映射到 [d/2, d] 区间内的均匀随机值。抖动的目的是打散
// 重试相位：Controller 宕机恢复的一瞬间，全体 Agent 若按相同节拍重试
// 会形成同步冲击（thundering herd），随机化后重连压力被摊平。
func withJitter(d time.Duration) time.Duration {
	half := d / 2
	return half + time.Duration(rand.Int64N(int64(half)+1))
}

// isTimeout 判断 err 是否为客户端侧的请求超时——即长轮询挂满
// requestTimeout 而 Controller 始终无变更。这属于长轮询的正常收尾，
// pollLoop 据此跳过退避、立即重连。
func isTimeout(err error) bool {
	var ne net.Error
	return errors.As(err, &ne) && ne.Timeout()
}

// sleepCtx 睡眠 d；若 ctx 先结束则返回 false，调用方据此立即退出循环。
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
