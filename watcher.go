// Package goroutinewatch watches a process's goroutine count over time and
// reports sustained growth — the pattern a goroutine leak produces — while
// trying hard not to fire on the growth a healthy server produces under
// rising load.
//
// It is a runtime companion to test-time tools like uber-go/goleak, not a
// replacement for one. See the README for how the two differ and why both
// are worth having.
package goroutinewatch

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"
)

// Config controls how a Watcher samples goroutine counts, when it decides
// growth is sustained rather than incidental, and what it does about it.
type Config struct {
	// Interval is how often the goroutine count is sampled. Sampling itself
	// is just runtime.NumGoroutine(), a cheap atomic read, so a short
	// interval is safe; the expensive part (stack capture) only happens on
	// a detected growth transition, never on this schedule. Defaults to 2s.
	Interval time.Duration

	// WindowSize is how many recent samples are kept and fed to the trend
	// estimator. A larger window smooths over more noise but takes longer
	// (WindowSize * Interval) to notice real growth and longer to notice
	// growth has stopped. Defaults to 30.
	WindowSize int

	// MinSamples is how many samples must be collected before the trend is
	// evaluated at all. Defaults to 8, and is clamped to at least 2.
	MinSamples int

	// Threshold is the slope, in goroutines per sample, above which growth
	// is reported. There is no universally correct default — it depends on
	// Interval and on how noisy your own workload's goroutine count is.
	// Defaults to 1.0 (on average, one more goroutine every sample than the
	// last, sustained across the whole window).
	Threshold float64

	// OnGrowth is called when the trend crosses Threshold, having not been
	// over it on the previous sample (an edge trigger — see the README for
	// why this package only ever captures stacks on that transition, not on
	// every sample while growth continues). It receives a Report with the
	// current count, the estimated slope, and, if capture succeeds,
	// attribution grouped by top stack frame. OnGrowth is called
	// synchronously from the watcher's own background goroutine, so it must
	// not block or call back into the Watcher.
	OnGrowth func(Report)

	// DisableStackCapture turns off attribution: when true, OnGrowth's
	// Report never includes Groups, and the expensive runtime.Stack call
	// (see the README's measured numbers) never happens. Attribution is
	// enabled by default (this is false unless set); disable it if you
	// already know its stop-the-world cost is unacceptable at your
	// process's typical goroutine count and only want count/slope
	// information.
	DisableStackCapture bool

	// MaxGroups caps how many attribution groups a Report carries. Defaults
	// to 10.
	MaxGroups int
}

func (c *Config) setDefaults() {
	if c.Interval <= 0 {
		c.Interval = 2 * time.Second
	}
	if c.WindowSize <= 0 {
		c.WindowSize = 30
	}
	if c.MinSamples <= 0 {
		c.MinSamples = 8
	}
	if c.MinSamples < 2 {
		c.MinSamples = 2
	}
	if c.MinSamples > c.WindowSize {
		c.MinSamples = c.WindowSize
	}
	if c.Threshold == 0 {
		c.Threshold = 1.0
	}
	if c.MaxGroups <= 0 {
		c.MaxGroups = 10
	}
}

// Report describes one detected growth episode.
type Report struct {
	// Time is when the episode was detected.
	Time time.Time
	// Count is the goroutine count at detection time.
	Count int
	// Slope is the Theil-Sen slope, in goroutines per sample, that
	// triggered detection.
	Slope float64
	// Groups is the attribution: goroutines at capture time, grouped by top
	// frame, largest group first. Empty if DisableStackCapture was set or
	// capture failed.
	Groups []Group
	// StackCaptureDuration is how long the runtime.Stack call itself took.
	// It is reported on every episode so a Watcher's cost in your own
	// process is visible, not just the numbers this package's own README
	// measured on its author's machine.
	StackCaptureDuration time.Duration
	// Truncated is true if the stack dump had to be cut off at
	// maxStackBufBytes rather than growing further to fit every goroutine.
	Truncated bool
}

// String renders a one-line-per-group human-readable summary, e.g. for
// logging.
func (r Report) String() string {
	s := fmt.Sprintf("goroutinewatch: sustained growth detected, %d goroutines, slope=%.2f/sample", r.Count, r.Slope)
	for _, g := range r.Groups {
		s += fmt.Sprintf("\n  %d in %s at %s [%s]", g.Count, g.Func, g.Location, g.State)
	}
	return s
}

// Stats is a snapshot of a Watcher's current state, meant to be exposed on a
// metrics endpoint. It marshals to JSON.
type Stats struct {
	Current    int       `json:"current"`
	Slope      float64   `json:"slope"`
	Growing    bool      `json:"growing"`
	Samples    int       `json:"samples"`
	LastSample time.Time `json:"last_sample"`
}

// Watcher samples a process's goroutine count on a timer, watches for
// sustained growth using a robust trend estimator, and — only when growth is
// actually detected — captures and attributes stacks so the report says
// where the goroutines are, not just that there are more of them.
//
// A zero Watcher is not usable; construct one with New.
type Watcher struct {
	cfg Config

	mu      sync.Mutex
	samples []sample
	nextIdx int
	growing bool
	stats   Stats

	cancel context.CancelFunc
	done   chan struct{}
}

// New creates a Watcher with the given configuration. Zero-valued fields in
// cfg are replaced with defaults (see Config's field docs). The Watcher does
// nothing until Start is called.
func New(cfg Config) *Watcher {
	cfg.setDefaults()
	return &Watcher{cfg: cfg}
}

// Start begins sampling in a background goroutine. Calling Start more than
// once, or calling it after Stop, panics — a Watcher is meant to be started
// once and stopped once; construct a new one to restart.
func (w *Watcher) Start() {
	w.mu.Lock()
	if w.cancel != nil {
		w.mu.Unlock()
		panic("goroutinewatch: Start called twice on the same Watcher")
	}
	ctx, cancel := context.WithCancel(context.Background())
	w.cancel = cancel
	w.done = make(chan struct{})
	w.mu.Unlock()

	go w.run(ctx)
}

// run is the background sampling loop.
func (w *Watcher) run(ctx context.Context) {
	defer close(w.done)
	ticker := time.NewTicker(w.cfg.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case t := <-ticker.C:
			w.tick(t)
		}
	}
}

// Stop halts sampling and blocks until the background goroutine has fully
// exited. After Stop returns, the Watcher has left nothing running — no
// goroutine, no ticker — which is the property this package's own tests
// check for, since a leak detector that leaks would be an embarrassment.
// Stop is safe to call once; calling it before Start, or more than once, is
// a no-op.
func (w *Watcher) Stop() {
	w.mu.Lock()
	cancel := w.cancel
	done := w.done
	w.mu.Unlock()
	if cancel == nil {
		return
	}
	cancel()
	<-done
}

// Stats returns a snapshot of the Watcher's current state, safe to call
// concurrently with sampling.
func (w *Watcher) Stats() Stats {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.stats
}

// ServeHTTP writes the current Stats as JSON, so a Watcher can be mounted
// directly on a metrics/debug mux, e.g.
// mux.Handle("/debug/goroutinewatch", watcher).
func (w *Watcher) ServeHTTP(rw http.ResponseWriter, _ *http.Request) {
	rw.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(rw).Encode(w.Stats())
}

// tick takes one sample, updates the trend, and — on a transition into
// sustained growth — captures attribution and invokes OnGrowth.
func (w *Watcher) tick(t time.Time) {
	n := numGoroutine()

	w.mu.Lock()
	idx := w.nextIdx
	w.nextIdx++
	w.samples = append(w.samples, sample{index: idx, count: n})
	if len(w.samples) > w.cfg.WindowSize {
		w.samples = w.samples[len(w.samples)-w.cfg.WindowSize:]
	}
	windowCopy := append([]sample(nil), w.samples...)
	w.mu.Unlock()

	if len(windowCopy) < w.cfg.MinSamples {
		w.mu.Lock()
		w.stats = Stats{Current: n, Samples: len(windowCopy), LastSample: t}
		w.mu.Unlock()
		return
	}

	slope := theilSenSlope(windowCopy)
	growing := slope > w.cfg.Threshold

	w.mu.Lock()
	wasGrowing := w.growing
	w.growing = growing
	w.stats = Stats{Current: n, Slope: slope, Growing: growing, Samples: len(windowCopy), LastSample: t}
	w.mu.Unlock()

	if growing && !wasGrowing && w.cfg.OnGrowth != nil {
		w.cfg.OnGrowth(w.buildReport(t, n, slope))
	}
}

func (w *Watcher) buildReport(t time.Time, count int, slope float64) Report {
	r := Report{Time: t, Count: count, Slope: slope}
	if w.cfg.DisableStackCapture {
		return r
	}
	selfID := currentGoroutineID() // this goroutine, mid-call-stack, is the one about to dump every stack including its own
	start := time.Now()
	dump, truncated := captureAllStacks()
	r.StackCaptureDuration = time.Since(start)
	r.Truncated = truncated
	r.Groups = attribute(dump, selfID, w.cfg.MaxGroups)
	return r
}
