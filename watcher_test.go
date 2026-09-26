package goroutinewatch

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

// --- Stop() leaves nothing behind ---------------------------------------

func TestWatcher_StopLeavesNoGoroutineBehind(t *testing.T) {
	before := runtime.NumGoroutine()

	w := New(Config{Interval: 5 * time.Millisecond})
	w.Start()
	time.Sleep(50 * time.Millisecond) // let it sample a handful of times
	w.Stop()

	// The background goroutine is torn down synchronously inside Stop, but
	// give the scheduler a couple of ticks to actually reclaim its stack
	// before comparing counts — NumGoroutine can lag a scheduler-cycle or
	// two behind a goroutine returning.
	ok := waitUntil(t, time.Second, func() bool {
		return runtime.NumGoroutine() <= before
	})
	if !ok {
		t.Fatalf("goroutine count after Stop = %d, before Start = %d; Watcher left something running", runtime.NumGoroutine(), before)
	}
}

func TestWatcher_StopBeforeStartIsNoop(t *testing.T) {
	w := New(Config{})
	w.Stop() // must not panic or block
}

func TestWatcher_StartTwicePanics(t *testing.T) {
	w := New(Config{Interval: time.Second})
	w.Start()
	defer w.Stop()
	defer func() {
		if recover() == nil {
			t.Fatalf("expected Start called twice to panic")
		}
	}()
	w.Start()
}

// --- Stats / metrics endpoint ---------------------------------------------

func TestWatcher_StatsAndServeHTTP(t *testing.T) {
	w := New(Config{Interval: 5 * time.Millisecond, MinSamples: 2})
	w.Start()
	defer w.Stop()

	waitUntil(t, time.Second, func() bool { return w.Stats().Samples >= 2 })

	stats := w.Stats()
	if stats.Current <= 0 {
		t.Fatalf("Stats().Current = %d, want > 0", stats.Current)
	}

	srv := httptest.NewServer(w)
	defer srv.Close()
	resp, err := http.Get(srv.URL)
	if err != nil {
		t.Fatalf("GET metrics endpoint: %v", err)
	}
	defer resp.Body.Close()
	if resp.Header.Get("Content-Type") != "application/json" {
		t.Fatalf("Content-Type = %q, want application/json", resp.Header.Get("Content-Type"))
	}
}

// --- The core claim: distinguishing a real leak from a matched non-leak --

// runScenario spawns goroutines at a steady rate for up to trialDuration:
// if leaking, each one blocks forever on an unbuffered channel receive
// (released only at the very end of the trial, so the test process itself
// doesn't actually leak them); if not, each one does a small, bounded amount
// of work and returns, at the same spawn rate. It reports whether the
// Watcher fired OnGrowth before trialDuration elapsed.
func runScenario(t *testing.T, leaking bool, trialDuration time.Duration) bool {
	t.Helper()

	release := make(chan struct{}) // only ever used by the leaking case
	var wg sync.WaitGroup

	stopSpawn := make(chan struct{})
	spawnerDone := make(chan struct{})

	spawnTicker := time.NewTicker(4 * time.Millisecond)
	defer spawnTicker.Stop()

	go func() {
		defer close(spawnerDone)
		for {
			select {
			case <-stopSpawn:
				return
			case <-spawnTicker.C:
				for i := 0; i < 2; i++ {
					wg.Add(1)
					if leaking {
						go func() {
							defer wg.Done()
							<-release // deliberately never sent/closed until trial end: a real leak
						}()
					} else {
						go func() {
							defer wg.Done()
							time.Sleep(2 * time.Millisecond) // matched: starts and finishes at the same rate
						}()
					}
				}
			}
		}
	}()

	detected := make(chan struct{}, 1)
	w := New(Config{
		Interval:            10 * time.Millisecond,
		WindowSize:          8,
		MinSamples:          5,
		Threshold:           2.5,
		DisableStackCapture: true, // isolate the count/trend claim from stack-capture cost
		OnGrowth: func(Report) {
			select {
			case detected <- struct{}{}:
			default:
			}
		},
	})
	w.Start()

	var fired bool
	select {
	case <-detected:
		fired = true
	case <-time.After(trialDuration):
		fired = false
	}

	w.Stop()

	// Teardown must be strictly ordered: stop the spawner and wait for it to
	// actually exit *before* closing release or waiting on wg. Otherwise the
	// spawner can still be mid-tick — adding to wg and spawning one more
	// worker — concurrently with (or after) a Wait() call that already saw
	// the counter hit zero, which is the sync.WaitGroup misuse the race
	// detector flagged the first time this test was written: Add with a
	// positive delta must happen-before the Wait it's meant to be counted
	// by, and closing over a channel that's already closed is not a
	// substitute for that ordering.
	close(stopSpawn)
	<-spawnerDone
	close(release)
	wg.Wait()

	return fired
}

func TestWatcher_DistinguishesLeakFromNonLeak(t *testing.T) {
	const trials = 15
	const trialDuration = 400 * time.Millisecond

	var leakHits, nonLeakFalsePositives int

	t.Run("leak", func(t *testing.T) {
		for i := 0; i < trials; i++ {
			i := i
			t.Run(fmt.Sprintf("trial-%d", i), func(t *testing.T) {
				if runScenario(t, true, trialDuration) {
					leakHits++
				} else {
					t.Errorf("trial %d: leaking scenario was NOT flagged within %s", i, trialDuration)
				}
			})
		}
	})

	t.Run("non-leak", func(t *testing.T) {
		for i := 0; i < trials; i++ {
			i := i
			t.Run(fmt.Sprintf("trial-%d", i), func(t *testing.T) {
				if runScenario(t, false, trialDuration) {
					nonLeakFalsePositives++
					t.Errorf("trial %d: non-leaking scenario was falsely flagged as growth", i)
				}
			})
		}
	})

	t.Logf("hit rate (leak correctly flagged):      %d/%d", leakHits, trials)
	t.Logf("false-positive rate (non-leak flagged):  %d/%d", nonLeakFalsePositives, trials)
}

// --- End-to-end: detection plus attribution on a real leak ----------------

func TestWatcher_AttributesRealLeak(t *testing.T) {
	release := make(chan struct{})
	stopSpawn := make(chan struct{})
	spawnerDone := make(chan struct{})
	var wg sync.WaitGroup

	spawnTicker := time.NewTicker(5 * time.Millisecond)
	defer spawnTicker.Stop()
	go func() {
		defer close(spawnerDone)
		for {
			select {
			case <-stopSpawn:
				return
			case <-spawnTicker.C:
				wg.Add(1)
				go func() {
					defer wg.Done()
					<-release // the leak: this goroutine never returns until the test releases it
				}()
			}
		}
	}()
	// Teardown must be strictly ordered (see runScenario's comment above for
	// why): stop spawning, wait for the spawner itself to exit, only then
	// release the blocked goroutines and wait for them.
	defer func() {
		close(stopSpawn)
		<-spawnerDone
		close(release)
		wg.Wait()
	}()

	reportCh := make(chan Report, 1)
	w := New(Config{
		Interval:   10 * time.Millisecond,
		WindowSize: 8,
		MinSamples: 5,
		Threshold:  1.5,
		// DisableStackCapture left false: attribution is on by default, and
		// that default is exactly what this test verifies end-to-end.
		OnGrowth: func(r Report) {
			select {
			case reportCh <- r:
			default:
			}
		},
	})
	w.Start()
	defer w.Stop()

	select {
	case r := <-reportCh:
		if len(r.Groups) == 0 {
			t.Fatalf("report has no attribution groups: %+v", r)
		}
		top := r.Groups[0]
		if !strings.Contains(top.Func, "TestWatcher_AttributesRealLeak") {
			t.Errorf("top group func = %q, want it to name the leaking test function", top.Func)
		}
		if !strings.Contains(top.State, "chan receive") {
			t.Errorf("top group state = %q, want chan receive", top.State)
		}
		if top.Count < 2 {
			t.Errorf("top group count = %d, want several accumulated goroutines", top.Count)
		}
		t.Logf("report: %s", r.String())
	case <-time.After(2 * time.Second):
		t.Fatalf("Watcher never reported growth for a real, deliberate leak")
	}
}
