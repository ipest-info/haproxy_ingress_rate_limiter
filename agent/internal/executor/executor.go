// Package executor 负责把 governor 的决策落地到 HAProxy 的 per-frontend
// bwlim map（设计文档 §3.2 "限速执行机制"）。
//
// 架构位置：executor 是 Agent 核心循环"采集 → 决策 → 执行"三段中的
// 最后一段——governor 只产出目标值，executor 通过 HAProxy runtime API
// 更新 map 条目，haproxy 配置里的 filter bwlim-out 以 map_str_int 查表
// 的方式实时读取该值完成聚合整形。
//
// 运行模式可在 dry-run（只记日志，不碰 HAProxy）与 enforce（真实写
// map）之间热切换。从 dry-run 切到 enforce 时会武装一次性的 resync
// 标志：dry-run 期间 HAProxy 里的真实 map 值没有被更新过，可能已经
// 与 governor 的目标值脱节，因此切换后的第一个非空 Apply 必须无条件
// 全量重写，使真实状态一次性收敛到目标状态。
package executor

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"strconv"
	"sync"

	"github.com/ipest-info/haproxy_ingress_rate_limiter/internal/model"
)

// Switchable 是可热切换 dry-run / enforce 的执行器。
//
// 并发约定：Apply 只由 Agent 核心循环调用；SetMode、Mode、Snapshot
// 可能被其他 goroutine（配置热更新、状态上报）并发调用。互斥锁只保护
// 内存状态，绝不跨 runtime API 的 I/O 持有，避免慢速 socket 写阻塞
// 其他调用方。
type Switchable struct {
	rt      model.MapSetter // HAProxy runtime API 的 map 写入抽象
	mapPath string          // bwlim map 在 HAProxy 侧的路径（map 标识）
	log     *slog.Logger

	mu sync.Mutex
	// mode 是当前运行模式（model.ModeDryRun / model.ModeEnforce）。
	mode string
	// resync 在切换到 enforce 时被武装（一次性标志）：下一个非空
	// Apply 会把所有决策一律视为"已变化"全量重写。语义上它表示
	// "HAProxy 的真实 map 状态与 governor 目标状态之间的一致性
	// 未知，需要一次全量收敛"。空 Apply 不消费该标志——没有决策
	// 可写时"重写"无从谈起，标志必须留到真正有决策的那一拍。
	resync bool
	// pending 记录"上一次 enforce 写入失败"的环境集合。它存在的
	// 根本原因是：governor 的 lastEmitted 在发射 Changed 决策时就
	// 自行推进了，并不关心 executor 是否写成功；如果 executor 不
	// 自己负责重试，写失败的环境会一直挂着旧限速值，直到目标值
	// 下一次漂移超过 epsilon 才有机会被修正——这可能是很久以后。
	// 因此写失败的 env 记入 pending，下一拍即使 Changed=false 也
	// 强制重写，直到写成功为止。
	pending map[string]bool
	// lastApplied 是每个 env 最近一次成功落地（enforce）或记录
	// （dry-run）的聚合 bwlim，bytes/s，供 Snapshot 对外暴露。
	lastApplied map[string]float64
}

// NewSwitchable 构造一个通过 rt 向 mapPath 写入的执行器。非法 mode
// 会告警并回退到 dry-run；nil logger 回退到 slog.Default()。
func NewSwitchable(rt model.MapSetter, mapPath string, mode string, log *slog.Logger) *Switchable {
	if log == nil {
		log = slog.Default()
	}
	s := &Switchable{
		rt:          rt,
		mapPath:     mapPath,
		log:         log,
		pending:     make(map[string]bool),
		lastApplied: make(map[string]float64),
	}
	s.mode = s.validMode(mode)
	return s
}

// validMode 把任意输入收敛到受支持的模式：除两个已知模式外一律降级
// 到 dry-run 并告警。降级方向选择 dry-run 是出于安全考虑——配置写错
// 时宁可"不动 HAProxy"也不能"意外真实写入"。
func (s *Switchable) validMode(mode string) string {
	switch mode {
	case model.ModeDryRun, model.ModeEnforce:
		return mode
	default:
		s.log.Warn("invalid executor mode; falling back to dry-run", "mode", mode)
		return model.ModeDryRun
	}
}

// SetMode 切换运行模式。真正切到 enforce 时武装 resync 标志，让下一
// 个 Apply 全量重写（dry-run 期间只记了日志，HAProxy 的 map 里可能
// 是陈旧值）；切到 dry-run 时清空 pending 重试状态——只记日志的模式
// 下没有需要收敛的真实状态，留着重试记录反而会在下次回到 enforce 时
// 造成误重试（届时 resync 会做一次全量覆盖，pending 已无意义）。
func (s *Switchable) SetMode(mode string) {
	m := s.validMode(mode)
	s.mu.Lock()
	defer s.mu.Unlock()
	if m == s.mode {
		return
	}
	prev := s.mode
	s.mode = m
	s.resync = m == model.ModeEnforce
	if m == model.ModeDryRun {
		s.pending = make(map[string]bool)
	}
	s.log.Info("executor mode changed",
		"mode_from", prev,
		"mode_to", m,
		"resync_armed", s.resync)
}

// Mode 返回当前运行模式。
func (s *Switchable) Mode() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.mode
}

// Snapshot 返回每个 env 最近一次落地/记录的聚合 bwlim（bytes/s）的
// 副本，供状态上报使用。
func (s *Switchable) Snapshot() map[string]float64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]float64, len(s.lastApplied))
	for env, v := range s.lastApplied {
		out[env] = v
	}
	return out
}

// Apply 执行一拍的全部决策。
//
// 跳过规则：Changed=false 的决策默认跳过，但有两个例外——
// (a) 切换到 enforce 后武装的一次性 resync 生效中（全量重写）；
// (b) 该 env 上一次 enforce 写入失败，处于 pending 重试（governor
// 发射后即自行推进 lastEmitted，不会替 executor 重发，重试责任必须
// 由 executor 承担，否则 HAProxy 会一直挂着旧限速值直到目标值下次
// 漂移，详见 pending 字段注释）。
//
// 错误语义：enforce 写失败不会中断本拍——同一 env 的其余 frontend
// 与其余决策照常执行，所有错误 Join 后一并返回；只有该 env 全部
// frontend 都写成功才计入 Snapshot，任一失败则整个 env 进入 pending。
//
// 均分语义：Decision.BwlimBps 是该 env 在本节点的"聚合"预算。
// haproxy 配置把每个 frontend 的 map 值当作该 frontend 的聚合额度
// （再由 fe_conn 按流数进一步均分），因此这里必须把预算按 frontend
// 数量等分——如果把完整聚合值原样写给每个 frontend，一个有 N 个
// frontend 的 env 实际可用带宽就是配额的 N 倍，超发 N 倍。等分是
// 骨架期策略；按各 frontend 实际用量加权的分配属于慢环的职责。
func (s *Switchable) Apply(ctx context.Context, ds []model.Decision) error {
	s.mu.Lock()
	mode := s.mode
	resync := s.resync
	if len(ds) > 0 {
		s.resync = false // 一次性消费；空拍不得消费（无决策可重写）
	}
	pending := make(map[string]bool, len(s.pending))
	for env := range s.pending {
		pending[env] = true
	}
	s.mu.Unlock()

	if resync && len(ds) > 0 {
		s.log.Info("resync triggered; rewriting all decisions",
			"mode", mode,
			"decision_count", len(ds))
	}

	var errs []error
	applied := make(map[string]float64)
	failed := make(map[string]bool)

	for _, d := range ds {
		if !d.Changed && !resync && !pending[d.EnvID] {
			continue
		}
		if mode == model.ModeDryRun {
			if !d.Changed && !resync {
				// pending 是上一段 enforce 期间留下的陈旧状态，
				// dry-run 下没有真实写入需要重试，直接跳过。
				continue
			}
			s.log.Info("DRY-RUN would set bwlim",
				"env", d.EnvID,
				"state", d.State.String(),
				"bwlim_bytes_per_sec", d.BwlimBps,
				"frontends", d.Frontends)
			applied[d.EnvID] = d.BwlimBps
			continue
		}
		if len(d.Frontends) == 0 {
			s.log.Warn("decision has no frontends; nothing to apply", "env", d.EnvID)
			continue
		}
		// map 值由 haproxy 配置的 map_str_int 查表消费，必须是整数
		// bytes/s；向下取整保证各 frontend 之和不超过聚合预算。
		retry := pending[d.EnvID]
		perFrontend := math.Floor(d.BwlimBps / float64(len(d.Frontends)))
		value := strconv.FormatInt(int64(perFrontend), 10)
		ok := true
		for _, fe := range d.Frontends {
			if err := s.rt.SetMapEntry(ctx, s.mapPath, fe, value); err != nil {
				ok = false
				errs = append(errs, fmt.Errorf("set bwlim env=%s frontend=%s: %w", d.EnvID, fe, err))
				continue
			}
			s.log.Info("bwlim map entry written",
				"env", d.EnvID,
				"frontend", fe,
				"value_bytes_per_s", perFrontend,
				"map_path", s.mapPath)
		}
		if ok {
			if retry {
				s.log.Info("pending bwlim retry succeeded",
					"env", d.EnvID,
					"value_bytes_per_s", perFrontend,
					"map_path", s.mapPath)
			}
			applied[d.EnvID] = d.BwlimBps
		} else {
			s.log.Warn("bwlim write failed; env queued for retry",
				"env", d.EnvID,
				"value_bytes_per_s", perFrontend,
				"map_path", s.mapPath)
			failed[d.EnvID] = true
		}
	}

	// 回写快照与重试集合：写成功的 env 更新快照并移出 pending，
	// 写失败的 env 记入 pending 留待下一拍强制重写。
	if len(applied) > 0 || len(failed) > 0 {
		s.mu.Lock()
		for env, v := range applied {
			s.lastApplied[env] = v
			delete(s.pending, env)
		}
		for env := range failed {
			s.pending[env] = true
		}
		s.mu.Unlock()
	}
	return errors.Join(errs...)
}
