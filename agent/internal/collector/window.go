package collector

// 本文件提供 collector 的两个平滑原语（设计文档 §3.1）：
//   - SlidingWindow：10 秒滑动窗口均值的载体，对应"10 秒均值 ≤ 约定带宽"
//     的承诺口径，是快环限速决策的输入；
//   - EWMA：60 秒指数加权移动平均，是慢环配额分配的输入。
// 两者都由 Tick goroutine 独占使用，因此刻意不做并发保护，避免无谓的锁开销。

// SlidingWindow 是固定容量的环形缓冲，保存最近 capacity 个样本。
// 窗口填满之前，均值只在已推入的样本上计算——冷启动阶段不用零值凑满
// 窗口，否则 mean10 会被人为压低，延迟快环的收紧动作。
// 非并发安全：仅供单一 goroutine 使用。
type SlidingWindow struct {
	buf   []float64
	next  int // 下一次写入的位置（环形推进）
	count int // 已持有的有效样本数，最多为 len(buf)
}

// NewSlidingWindow 返回保存最近 capacity 个样本的窗口。
// capacity 必须为正；<= 0 的值被钳制为 1，保证 Push/Mean 永不因空缓冲崩溃。
func NewSlidingWindow(capacity int) *SlidingWindow {
	if capacity <= 0 {
		capacity = 1
	}
	return &SlidingWindow{buf: make([]float64, capacity)}
}

// Push 追加一个样本；窗口已满时按环形位置覆盖最旧的样本。
func (w *SlidingWindow) Push(v float64) {
	w.buf[w.next] = v
	w.next = (w.next + 1) % len(w.buf)
	if w.count < len(w.buf) {
		w.count++
	}
}

// Mean 返回当前持有样本的算术平均（空窗口返回 0）。
//
// 为什么每次全量重算而不是维护滚动和（push 时加、evict 时减）：滚动和的
// 浮点误差会随运行时间无界累积，Agent 是常驻进程、每秒推一个样本，跑上
// 数周后滚动和可能明显偏离真值；而窗口只有 10 格，O(n) 重算的成本可以
// 忽略，换来的是长期运行下的数值稳定。
func (w *SlidingWindow) Mean() float64 {
	if w.count == 0 {
		return 0
	}
	// 首次绕回之前有效样本是 buf[0:count]；绕回之后整个缓冲都有效且
	// count == len(buf)，因此统一按 buf[0:count] 求和即可，无需区分两种形态。
	var sum float64
	for i := 0; i < w.count; i++ {
		sum += w.buf[i]
	}
	return sum / float64(w.count)
}

// EWMA 是指数加权移动平均。首个样本直接作为初值（seed），而不是从 0 开始
// 衰减——否则 Agent 启动后 EWMA 要花约一个时间常数（这里约 60 秒）才能爬到
// 真实水平，慢环在这段时间会看到系统性偏低的用量。
// 非并发安全：仅供单一 goroutine 使用。
type EWMA struct {
	alpha  float64 // 平滑系数，越大对新样本越敏感
	value  float64
	seeded bool // 是否已用首个样本完成初始化
}

// NewEWMA 返回平滑系数为 alpha（0 < alpha <= 1）的 EWMA。
// collector 以 α = 2/(N+1)、N=60 构造，在 1 秒采样节奏下近似 60 秒均线。
func NewEWMA(alpha float64) *EWMA {
	return &EWMA{alpha: alpha}
}

// Update 把一个新样本折入均值：首个样本直接落位（理由见类型注释），
// 之后按标准递推 value = α·v + (1-α)·value。
func (e *EWMA) Update(v float64) {
	if !e.seeded {
		e.value = v
		e.seeded = true
		return
	}
	e.value = e.alpha*v + (1-e.alpha)*e.value
}

// Value 返回当前均值（首次 Update 之前为 0）。
func (e *EWMA) Value() float64 {
	return e.value
}
