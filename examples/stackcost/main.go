// Command stackcost measures the real cost of runtime.Stack(buf, true) — the
// stop-the-world call goroutinewatch uses for attribution — at realistic
// goroutine counts. Run it directly:
//
//	go run ./examples/stackcost
//
// It spins up N goroutines parked on an unbuffered channel receive (the same
// shape as an idle worker pool), captures all stacks several times, and
// reports the time taken and the size of the resulting dump. The numbers
// this prints on the author's machine are pasted into the README; run it
// yourself to see the numbers on yours — they will differ by CPU and by Go
// version.
package main

import (
	"fmt"
	"runtime"
	"time"
)

func park(release <-chan struct{}) {
	<-release
}

func captureAllStacks() (dump []byte, took time.Duration) {
	buf := make([]byte, 64*1024)
	start := time.Now()
	for {
		n := runtime.Stack(buf, true)
		if n < len(buf) {
			return buf[:n], time.Since(start)
		}
		buf = make([]byte, 2*len(buf))
	}
}

func measure(n int, captures int) {
	release := make(chan struct{})
	for i := 0; i < n; i++ {
		go park(release)
	}
	defer close(release)

	// Let the scheduler actually park everyone before measuring.
	time.Sleep(200 * time.Millisecond)
	runtime.GC()

	var total time.Duration
	var dumpLen int
	for i := 0; i < captures; i++ {
		dump, took := captureAllStacks()
		total += took
		dumpLen = len(dump)
	}
	avg := total / time.Duration(captures)
	fmt.Printf("%9d goroutines   avg capture: %12s   dump size: %10d bytes (%d captures)\n",
		n, avg, dumpLen, captures)
}

func main() {
	fmt.Println("Measuring runtime.Stack(buf, true) cost at increasing goroutine counts.")
	fmt.Println("Each row parks N goroutines on a channel receive, then captures all stacks.")
	fmt.Println()

	measure(1_000, 20)
	measure(10_000, 10)
	measure(100_000, 5)
}
