package collector

// SlidingWindow is a fixed-capacity ring buffer of the most recent samples.
// Until the window is full the mean is taken over the pushed count only.
// Not safe for concurrent use.
type SlidingWindow struct {
	buf   []float64
	next  int
	count int
}

// NewSlidingWindow returns a window holding the last `capacity` samples.
// Capacity must be positive; values <= 0 are clamped to 1.
func NewSlidingWindow(capacity int) *SlidingWindow {
	if capacity <= 0 {
		capacity = 1
	}
	return &SlidingWindow{buf: make([]float64, capacity)}
}

// Push appends a sample, evicting the oldest one once the window is full.
func (w *SlidingWindow) Push(v float64) {
	w.buf[w.next] = v
	w.next = (w.next + 1) % len(w.buf)
	if w.count < len(w.buf) {
		w.count++
	}
}

// Mean returns the average of the currently held samples (0 when empty).
// It recomputes from the buffer so no floating-point drift accumulates over
// long runtimes.
func (w *SlidingWindow) Mean() float64 {
	if w.count == 0 {
		return 0
	}
	// Before the first wrap the valid entries are buf[0:count]; after it the
	// whole buffer is valid and count == len(buf).
	var sum float64
	for i := 0; i < w.count; i++ {
		sum += w.buf[i]
	}
	return sum / float64(w.count)
}

// EWMA is an exponentially weighted moving average. The first Update seeds
// the value directly. Not safe for concurrent use.
type EWMA struct {
	alpha  float64
	value  float64
	seeded bool
}

// NewEWMA returns an EWMA with the given smoothing factor (0 < alpha <= 1).
func NewEWMA(alpha float64) *EWMA {
	return &EWMA{alpha: alpha}
}

// Update folds a new sample into the average.
func (e *EWMA) Update(v float64) {
	if !e.seeded {
		e.value = v
		e.seeded = true
		return
	}
	e.value = e.alpha*v + (1-e.alpha)*e.value
}

// Value returns the current average (0 before the first Update).
func (e *EWMA) Value() float64 {
	return e.value
}
