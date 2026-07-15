// Package config 负责加载并校验 rl-agent 的本地 YAML 配置文件
// （示例见 deploy/config/agent.example.yaml，通常安装为
// /etc/rl-agent/config.yaml）。
//
// 本地配置提供四类信息：节点身份（node_id）、HAProxy 接线（stats socket、
// bwlim map 路径）、控制面接入（base_url、fail-static 缓存、mTLS 材料，
// 设计文档 §3.6/§3.7），以及 standalone/首启引导配额（envs）。
//
// 单位约定：配额一律按运维口径的 BIT 每秒（quota_bps，200000000 = 200 Mbps）
// 书写，Agent 内部统一换算为 bytes/s（见 model.EnvQuota）。
//
// 加载流程为 读文件 → 解析 YAML → 补默认值 → 校验；Load 成功与否均不打
// 日志，成功日志由 main 统一输出，失败则通过错误信息精确指出问题字段。
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

// Load 在对应键缺失时补上的默认值。与 deploy/config/agent.example.yaml
// 中的示例保持一致。
const (
	// DefaultStatsSocket 是 HAProxy 管理 socket 的默认路径。该 socket 必须
	// 配置为 level admin：Agent 既要读（show stat 采集用量），也要写
	// （set map 下发限速值）。
	DefaultStatsSocket = "/var/run/haproxy/admin.sock"
	// DefaultBwlimMapPath 是 bwlim map 文件的默认路径，必须与 haproxy 配置
	// 中 map_str_int(...) 引用的路径一致，否则限速值写了也不会生效。
	DefaultBwlimMapPath = "/etc/haproxy/maps/bwlim.map"
	// DefaultTimeoutMS 是单次 HAProxy runtime API 调用的默认超时（毫秒）。
	// 500ms 远大于本机 unix socket 的正常往返，又不至于拖住每秒一次的
	// 核心循环。
	DefaultTimeoutMS = 500
	// DefaultCachePath 是 fail-static 配置缓存的默认落盘路径（设计文档
	// §3.7）：断联时 Agent 按缓存中最后一次下发的配置继续限速。
	DefaultCachePath = "/var/lib/rl-agent/config-cache.json"
	// DefaultLogLevel 是默认日志级别。
	DefaultLogLevel = "info"
)

// HAProxy 描述如何访问本机 HAProxy 的 runtime API。
type HAProxy struct {
	// StatsSocket 是 HAProxy 管理 socket 路径（须为 level admin）。
	// 默认 DefaultStatsSocket。
	StatsSocket string `yaml:"stats_socket"`
	// BwlimMapPath 是 bwlim map 文件路径，须与 haproxy 配置中
	// map_str_int(...) 的路径一致。默认 DefaultBwlimMapPath。
	BwlimMapPath string `yaml:"bwlim_map_path"`
	// TimeoutMS 是单次 runtime API 调用超时，单位毫秒；<= 0 时取
	// DefaultTimeoutMS。
	TimeoutMS int `yaml:"timeout_ms"`
}

// Timeout 把 TimeoutMS 换算为 time.Duration，供拨号/读写超时直接使用。
func (h HAProxy) Timeout() time.Duration {
	return time.Duration(h.TimeoutMS) * time.Millisecond
}

// TLS 是连接控制面用的可选 mTLS 材料（设计文档 §3.6：Agent 与 Controller
// 双向认证，节点证书与 LB_NODE 绑定，防伪造上报/伪造下发）。三个文件
// 均可留空（如内网明文调试），生产环境建议全部配置。
type TLS struct {
	// CAFile 是校验 Controller 服务端证书的 CA 证书路径。
	CAFile string `yaml:"ca_file"`
	// CertFile/KeyFile 是本节点的客户端证书与私钥路径，必须成对提供。
	CertFile string `yaml:"cert_file"`
	KeyFile  string `yaml:"key_file"`
}

// Controller 描述控制面接入方式。BaseURL 留空表示 standalone 模式：
// 不连接控制面，只使用本地 envs 的引导配额运行。
type Controller struct {
	// BaseURL 是控制面根地址（如 http://127.0.0.1:9090）；空串 = standalone。
	BaseURL string `yaml:"base_url"`
	// CachePath 是 fail-static 配置缓存路径（§3.7），默认 DefaultCachePath。
	CachePath string `yaml:"cache_path"`
	// TLS 是可选的 mTLS 材料。
	TLS TLS `yaml:"tls"`
}

// File 是磁盘上完整的 Agent 配置。
type File struct {
	// NodeID 是节点唯一标识，必填，需与控制面 LB_NODE 记录一致——
	// 配置轮询、指标上报、心跳都用它标识本节点。
	NodeID string `yaml:"node_id"`
	// Mode 是运行模式：dry-run（只打日志不写 map，观测模式）或
	// enforce（真实下发限速）。默认 dry-run，确保误部署时不产生
	// 任何数据面影响。
	Mode string `yaml:"mode"`
	// LogLevel 是日志级别：debug | info | warn | error，默认 info。
	LogLevel string `yaml:"log_level"`
	// HAProxy 是本机 HAProxy runtime API 的接线配置。
	HAProxy HAProxy `yaml:"haproxy"`
	// Controller 是控制面接入配置。
	Controller Controller `yaml:"controller"`
	// Envs 是 standalone/首启引导配额：standalone 模式下这是唯一的配额
	// 来源；接入控制面时仅作首启引导，控制面下发配置后以下发为准。
	Envs []model.EnvQuota `yaml:"envs"`
}

// Load 读取 path 指向的 YAML 配置：解析、补默认值、校验，全部通过后返回。
// 任一步失败时错误信息都会带上文件路径与具体原因。Load 本身不打日志，
// 成功日志由 main 统一输出。
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

// applyDefaults 为缺失（或非法的非正超时）的键填入包级默认值。
// 注意 Mode 缺省为 dry-run：新节点必须显式选择 enforce 才会真正限速。
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

// Validate 校验 Agent 其余部分赖以运行的结构性不变量，逐条业务原因如下：
//
//   - node_id 必填：它是控制面识别节点的唯一键，缺失则轮询/上报/心跳
//     全部无法归属；
//   - mode 只能是 dry-run 或 enforce：写错模式的后果不对称（该限不限 /
//     不该限乱限），必须在启动前拦下而不是静默取默认；
//   - log_level 枚举校验：拼错的级别若静默回落会让运维误以为已调级；
//   - env_id 必填、quota_bps 必须 > 0：配额是限速计算的分母/基准，
//     零或负配额无意义且会导致把环境限死；
//   - 每个环境至少一个 frontend：没有 frontend 的环境既采不到用量
//     也无处下发限速，属于配置残缺；
//   - frontend 不得为空串、不得被映射到两个环境：Agent 按
//     frontend → env 归并用量并按 env 写限速值，一对多映射会导致
//     同一份流量被重复计入两个环境、限速值互相覆盖，必须拒绝。
func (f *File) Validate() error {
	if f.NodeID == "" {
		return errors.New("node_id is required (unique node identity, must match the controller's LB_NODE record)")
	}
	if f.Mode != model.ModeDryRun && f.Mode != model.ModeEnforce {
		return fmt.Errorf("mode %q: must be %q or %q", f.Mode, model.ModeDryRun, model.ModeEnforce)
	}
	switch f.LogLevel {
	case "debug", "info", "warn", "error":
	default:
		return fmt.Errorf("log_level %q: must be one of debug, info, warn, error", f.LogLevel)
	}
	// frontendOwner 记录 frontend → env_id 的归属，用于检出跨环境
	// （或同环境重复书写）的 frontend 冲突。
	frontendOwner := make(map[string]string)
	for i, e := range f.Envs {
		if e.EnvID == "" {
			return fmt.Errorf("envs[%d]: env_id is required", i)
		}
		if e.QuotaBitsPerSec <= 0 {
			return fmt.Errorf("envs[%d] (%s): quota_bps must be > 0, got %v (bits per second, e.g. 200000000 for 200 Mbps)", i, e.EnvID, e.QuotaBitsPerSec)
		}
		if len(e.Frontends) == 0 {
			return fmt.Errorf("envs[%d] (%s): at least one frontend is required", i, e.EnvID)
		}
		for _, fe := range e.Frontends {
			if fe == "" {
				return fmt.Errorf("envs[%d] (%s): empty frontend name", i, e.EnvID)
			}
			if owner, dup := frontendOwner[fe]; dup {
				return fmt.Errorf("frontend %q mapped to both %q and %q: each frontend must belong to exactly one env (usage accounting and bwlim writes are keyed by env)", fe, owner, e.EnvID)
			}
			frontendOwner[fe] = e.EnvID
		}
	}
	return nil
}

// SlogLevel 把 LogLevel 映射为 slog.Level。未知值回落到 info——但正常
// 流程走不到这一步：非法值已在 Validate 中被拒绝，此处只是防御性兜底。
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
