// Package haproxy 实现基于 unix stats socket 的 HAProxy runtime API 客户端，
// 是整个 Agent 与数据面（HAProxy 进程）之间唯一的交互通道：
//
//   - 采集侧：collector 每秒通过 ShowStat 拉取各 frontend 的 bytes_out 累计值
//     与当前并发连接数，作为计费口径的原始输入（设计文档 §3.1：选用 frontend
//     bytes_out 而非网卡计数，保证口径与"HAProxy 发回客户端的字节数"精确一致，
//     且天然按 frontend 拆分以支持一台 HAProxy 服务多个环境）；
//   - 执行侧：executor 通过 SetMapEntry 把快环算出的整形值写入 runtime map，
//     驱动 bwlim-out 过滤器动态调整聚合限速（设计文档 §3.2/§3.3）。
//
// 协议约束：HAProxy runtime socket 在非交互模式下"一次连接只服务一条命令"，
// 命令执行完即由服务端关闭连接。因此本包的 Exec 每次调用都重新拨号，
// 而不是复用长连接——这不是性能疏忽，而是协议要求。
package haproxy

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/ipest-info/haproxy_ingress_rate_limiter/internal/model"
)

// showStatCmd 是采集用的固定命令。三个参数依次为 "<proxy_id> <type> <server_id>"：
// proxy_id = -1 表示全部代理；type = 1 是类型掩码"仅 frontend"（我们只关心
// frontend 的下行字节，backend/server 行既冗余又会显著增大回包体积）；
// server_id = -1 表示全部 server（对 frontend 行无实际筛选作用，按惯例传 -1）。
const showStatCmd = "show stat -1 1 -1"

// errorReplyPrefixes 列举"回包首行以此开头即可断定命令失败"的前缀。
// runtime socket 的失败回包没有统一格式，只能靠已知前缀识别：
// 命令不存在（"Unknown command"）、socket 权限级别不足（"Permission denied"）、
// 以及 "[ALERT]"/"[CFGERR]" 这类方括号包裹的诊断信息。
// 不在此列的回包一律原样返回，由调用方按各自命令的语义解释——
// 例如 "show stat" 的正常回包是 CSV，"set map" 成功时回包为空。
var errorReplyPrefixes = []string{
	"Unknown command",
	"Permission denied",
	"[", // 例如 "[ALERT]"、"[CFGERR]" 等方括号风格的诊断输出
}

// Client 与单个 HAProxy 进程的 stats socket 通信，同时实现 model.StatSource
// （供 collector 采样）与 model.MapSetter（供 executor 下发限速值）两个接口。
// Client 自身无状态（不缓存连接），可被多个 goroutine 并发使用。
type Client struct {
	socketPath string
	timeout    time.Duration // 单条命令的端到端预算，覆盖拨号 + 写入 + 读取全过程
	log        *slog.Logger  // 仅用于调试观测（命令、耗时、回退事件），不参与控制逻辑
}

// New 返回指向 socketPath 处 runtime socket 的客户端。timeout 约束每条命令的
// 端到端耗时；timeout <= 0 表示关闭客户端侧预算，完全交由调用方 context 的
// 截止时间控制（测试场景常用）。日志固定走 slog.Default()，保持构造签名精简。
func New(socketPath string, timeout time.Duration) *Client {
	return &Client{socketPath: socketPath, timeout: timeout, log: slog.Default()}
}

// Exec 发送一条命令并返回去除首尾空白后的回包。
//
// 每次调用都新建连接：HAProxy 在非交互模式下执行完一条命令就会关闭 runtime
// socket，长连接复用在协议上不可行。回包若命中已知错误前缀，则连同非 nil
// error 一起返回（回包本身仍返回，便于上层记录原文）；其余回包原样返回，
// 由调用方按命令语义解析。
func (c *Client) Exec(ctx context.Context, cmd string) (string, error) {
	if c.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.timeout)
		defer cancel()
	}

	start := time.Now()

	var d net.Dialer
	conn, err := d.DialContext(ctx, "unix", c.socketPath)
	if err != nil {
		return "", fmt.Errorf("haproxy: dial %s: %w", c.socketPath, err)
	}
	defer conn.Close()

	// 把 context 的截止时间同步到连接上，让阻塞中的读写按时返回。
	if dl, ok := ctx.Deadline(); ok {
		if err := conn.SetDeadline(dl); err != nil {
			return "", fmt.Errorf("haproxy: set deadline: %w", err)
		}
	}
	// 上面的 deadline 只覆盖"超时到期"这一种情况；若调用方显式 cancel 了
	// context（例如 Agent 收到退出信号），阻塞中的读操作并不会被唤醒。
	// 这里用一个哨兵 goroutine 监听 ctx.Done()，触发时把 deadline 置为当前
	// 时刻，强制在途读写立即以超时错误返回，避免退出被 socket 读挂住。
	watchDone := make(chan struct{})
	defer close(watchDone)
	go func() {
		select {
		case <-ctx.Done():
			conn.SetDeadline(time.Now())
		case <-watchDone:
		}
	}()

	if _, err := io.WriteString(conn, cmd+"\n"); err != nil {
		return "", fmt.Errorf("haproxy: write %q: %w", cmd, err)
	}
	// 读到 EOF 为止：服务端执行完命令即关闭连接，EOF 就是"回包结束"的信号，
	// 无需（也无法）依赖长度前缀或分隔符。
	raw, err := io.ReadAll(conn)
	if err != nil {
		return "", fmt.Errorf("haproxy: read reply to %q: %w", cmd, err)
	}

	out := strings.TrimSpace(string(raw))
	c.log.Debug("haproxy runtime command executed",
		"cmd", cmd,
		"duration_ms", time.Since(start).Milliseconds(),
		"reply_bytes", len(raw))
	if isErrorReply(out) {
		return out, fmt.Errorf("haproxy: command %q rejected: %s", cmd, firstLine(out))
	}
	return out, nil
}

// ShowStat 执行一次 "show stat" 采样并返回全部 frontend 行。列位置按 CSV
// 表头中的列名解析（原因见 parseShowStat）；只保留 svname 为 "FRONTEND"
// 的汇总行，且剔除 HAProxy 内建的 "stats" 管理 frontend——那是运维自身的
// 流量，不属于计费口径（设计文档 §3.1）。
func (c *Client) ShowStat(ctx context.Context) ([]model.FrontendStat, error) {
	out, err := c.Exec(ctx, showStatCmd)
	if err != nil {
		return nil, err
	}
	return parseShowStat(out)
}

// SetMapEntry 更新 runtime map 中 key 对应的条目。
//
// HAProxy 对 "set map" 成功时回包为空；若 key 尚不存在（例如 map 文件初始
// 为空、或 HAProxy reload 后 map 被重建），"set map" 会返回 "not found" 类
// 回包。此时自动回退一次 "add map"，让首次出现的 key 被透明创建——调用方
// （executor）因此无需关心"该 key 是否已存在"，两条路径对外语义一致。
// 回退只做一次：若 "add map" 仍失败，说明是 map 路径错误等真实故障，直接上抛。
func (c *Client) SetMapEntry(ctx context.Context, mapPath, key, value string) error {
	out, err := c.Exec(ctx, fmt.Sprintf("set map %s %s %s", mapPath, key, value))
	if isMissingEntryReply(out) {
		// key 不存在不算错误，是"首次写入"的正常路径；记 info 便于确认
		// map 冷启动/重建后的首次填充时点。
		c.log.Info("set map entry missing, falling back to add map",
			"map_path", mapPath, "key", key, "value", value)
		out, err = c.Exec(ctx, fmt.Sprintf("add map %s %s %s", mapPath, key, value))
		if err != nil {
			return fmt.Errorf("haproxy: add map %s %s: %w", mapPath, key, err)
		}
		if out != "" {
			// "add map" 成功时同样应回空包；任何非空回包都是未知情况，
			// 宁可报错也不能假装写入成功（限速值未生效属于危险方向）。
			return fmt.Errorf("haproxy: add map %s %s: unexpected reply: %s", mapPath, key, firstLine(out))
		}
		return nil
	}
	if err != nil {
		return fmt.Errorf("haproxy: set map %s %s: %w", mapPath, key, err)
	}
	if out != "" {
		return fmt.Errorf("haproxy: set map %s %s: unexpected reply: %s", mapPath, key, firstLine(out))
	}
	return nil
}

// isErrorReply 判断回包首行是否命中已知错误前缀。只看首行：错误回包的
// 后续行（若有）是补充说明，不影响成败判定。
func isErrorReply(out string) bool {
	line := firstLine(out)
	for _, p := range errorReplyPrefixes {
		if strings.HasPrefix(line, p) {
			return true
		}
	}
	return false
}

// isMissingEntryReply 识别 "set map" 的"条目不存在"回包。不同 HAProxy 版本
// 的措辞不完全一致（"entry not found" / "unable to find ..."），因此用小写
// 子串匹配兜住两种已知变体，而不是精确比对。
func isMissingEntryReply(out string) bool {
	l := strings.ToLower(out)
	return strings.Contains(l, "not found") || strings.Contains(l, "unable to find")
}

// firstLine 返回 s 的第一行（无换行符时返回原串），用于把多行回包压缩成
// 可读的单行错误信息。
func firstLine(s string) string {
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		return s[:i]
	}
	return s
}

// parseShowStat 解析 "show stat" 的 CSV 回包。
//
// 为什么按列名而不是列下标解析：HAProxy 的 stat 列集合随版本增删（2.x 各
// 小版本都有变化），列的绝对位置完全不可依赖；唯一稳定的契约是表头行
// （以 "# " 开头）中的列名。因此先从表头建立 名字→下标 索引，再取行内
// 字段。"bytes_out" 被接受为 HAProxy 原生列名 "bout" 的别名，以兼容测试
// 桩及可能的代理层改写。
//
// 只保留 svname == "FRONTEND" 的汇总行：type 掩码虽已请求"仅 frontend"，
// 但按行再校验一次可以防御掩码语义变化或桩数据混入其他行。内建 "stats"
// frontend 被剔除，理由见 ShowStat 的注释。
func parseShowStat(out string) ([]model.FrontendStat, error) {
	var (
		stats   []model.FrontendStat
		colIdx  map[string]int
		iPxname = -1
		iSvname = -1
		iScur   = -1
		iBytes  = -1
	)

	for _, line := range strings.Split(out, "\n") {
		line = strings.TrimRight(line, "\r")
		if line == "" {
			continue
		}
		if strings.HasPrefix(line, "#") {
			// 表头行：重建列索引。理论上回包只有一个表头，但循环内处理
			// 使解析器天然容忍"多段 CSV 拼接"的输入。
			colIdx = make(map[string]int)
			for i, name := range strings.Split(strings.TrimSpace(strings.TrimPrefix(line, "#")), ",") {
				colIdx[strings.TrimSpace(name)] = i
			}
			iPxname = indexOf(colIdx, "pxname")
			iSvname = indexOf(colIdx, "svname")
			iScur = indexOf(colIdx, "scur")
			iBytes = indexOf(colIdx, "bytes_out")
			if iBytes < 0 {
				iBytes = indexOf(colIdx, "bout") // HAProxy 原生列名
			}
			if iPxname < 0 || iSvname < 0 || iScur < 0 || iBytes < 0 {
				// 缺任何一列都无法给出正确口径，必须整体失败而不是带着
				// 残缺数据继续——collector 的容错逻辑（§3.7）会兜住这次失败。
				return nil, fmt.Errorf("haproxy: show stat header missing required columns (pxname/svname/scur/bytes_out|bout): %s", line)
			}
			continue
		}
		if colIdx == nil {
			// 数据行先于表头出现：回包不是合法的 show stat CSV。
			return nil, fmt.Errorf("haproxy: show stat output has no CSV header: %s", firstLine(out))
		}

		fields := strings.Split(line, ",")
		pxname := fieldAt(fields, iPxname)
		svname := fieldAt(fields, iSvname)
		if svname != "FRONTEND" || pxname == "" {
			continue
		}
		if pxname == "stats" { // 内建管理 frontend，不属于计费流量（§3.1）
			continue
		}

		bytesOut, err := parseUintField(fieldAt(fields, iBytes))
		if err != nil {
			return nil, fmt.Errorf("haproxy: frontend %s: bad bytes_out: %w", pxname, err)
		}
		scur, err := parseIntField(fieldAt(fields, iScur))
		if err != nil {
			return nil, fmt.Errorf("haproxy: frontend %s: bad scur: %w", pxname, err)
		}
		stats = append(stats, model.FrontendStat{
			Name:     pxname,
			BytesOut: bytesOut,
			ConnCur:  scur,
		})
	}

	if colIdx == nil {
		// 连表头都没有：空回包或完全非预期的输出，按失败处理。
		return nil, fmt.Errorf("haproxy: empty show stat output")
	}
	return stats, nil
}

// indexOf 返回列名在索引表中的下标，不存在时返回 -1（与"缺列"判定配合）。
func indexOf(m map[string]int, name string) int {
	if i, ok := m[name]; ok {
		return i
	}
	return -1
}

// fieldAt 取第 i 个字段并去除首尾空白。HAProxy 某些行的尾部字段可能缺省，
// 导致行内字段数少于表头列数；这里把越界读取容忍为返回空串，交由数值
// 解析函数按"空即为 0"处理，而不是让整次采样崩掉。
func fieldAt(fields []string, i int) string {
	if i < 0 || i >= len(fields) {
		return ""
	}
	return strings.TrimSpace(fields[i])
}

// parseUintField 解析无符号计数列。空字段视为 0：stat 输出中"该指标不适用"
// 就表现为空串，语义上等价于零值。
func parseUintField(s string) (uint64, error) {
	if s == "" {
		return 0, nil
	}
	return strconv.ParseUint(s, 10, 64)
}

// parseIntField 解析有符号数值列，空字段同样视为 0（理由同上）。
func parseIntField(s string) (int64, error) {
	if s == "" {
		return 0, nil
	}
	return strconv.ParseInt(s, 10, 64)
}
