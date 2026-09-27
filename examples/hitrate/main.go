// Command hitrate runs a real, deliberately leaking goroutine scenario and a
// matched non-leaking scenario against goroutinewatch.Watcher repeatedly,
// and reports the hit rate and false-positive rate — the same shape of
// evidence the Node sibling (leak-check) publishes for memory leaks.
//
//	go run ./examples/hitrate
package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	goroutinewatch "github.com/agentscope-ai-java/goroutinewatch"
)

const (
	trials        = 30
	trialDuration = 400 * time.Millisecond
)

// runScenario spawns goroutines at a steady rate: leaking ones block forever
// on an unbuffered channel receive until the trial ends; non-leaking ones do
// a small bounded amount of work and return, at the same spawn rate. It
// reports whether the Watcher fired OnGrowth before trialDuration elapsed.
func runScenario(leaking bool) bool {
	release := make(chan struct{})
	var wg sync.WaitGroup

	ctx, cancel := context.WithTimeout(context.Background(), trialDuration)
	defer cancel()

	spawnTicker := time.NewTicker(4 * time.Millisecond)
	defer spawnTicker.Stop()

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-spawnTicker.C:
				for i := 0; i < 2; i++ {
					wg.Add(1)
					if leaking {
						go func() {
							defer wg.Done()
							<-release
						}()
					} else {
						go func() {
							defer wg.Done()
							time.Sleep(2 * time.Millisecond)
						}()
					}
				}
			}
		}
	}()

	detected := make(chan struct{}, 1)
	w := goroutinewatch.New(goroutinewatch.Config{
		Interval:            10 * time.Millisecond,
		WindowSize:          8,
		MinSamples:          5,
		Threshold:           2.5,
		DisableStackCapture: true, // isolate the count/trend claim from stack-capture cost
		OnGrowth: func(goroutinewatch.Report) {
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
	cancel()
	close(release)
	wg.Wait()

	return fired
}

func main() {
	fmt.Printf("Running Watcher %dx against a real leak and %dx against a matched non-leak.\n", trials, trials)
	fmt.Printf("(trialDuration=%s, Interval=10ms, WindowSize=8, MinSamples=5, Threshold=2.5)\n\n", trialDuration)

	hits := 0
	for i := 0; i < trials; i++ {
		if runScenario(true) {
			hits++
		}
	}

	falsePositives := 0
	for i := 0; i < trials; i++ {
		if runScenario(false) {
			falsePositives++
		}
	}

	fmt.Printf("  hit rate (leak correctly flagged):       %d/%d (%.0f%%)\n", hits, trials, 100*float64(hits)/float64(trials))
	fmt.Printf("  false-positive rate (non-leak flagged):  %d/%d (%.0f%%)\n", falsePositives, trials, 100*float64(falsePositives)/float64(trials))
}
