# goroutinewatch

A production-safe watcher that catches goroutine leaks while the process is
still running, by watching whether the goroutine count trends upward and
never recovers — and, when it does, capturing and grouping stacks so the
report says which call site is accumulating, not just that something is.

```
Running Watcher 30x against a real leak and 30x against a matched non-leak.
(trialDuration=400ms, Interval=10ms, WindowSize=8, MinSamples=5, Threshold=2.5)

  hit rate (leak correctly flagged):       30/30 (100%)
  false-positive rate (non-leak flagged):  0/30 (0%)
```

That's `go run ./examples/hitrate`, measured on this machine (macOS, Apple
Silicon, Go 1.27.1) — the leak is a goroutine spawned every 4ms that blocks
forever on an unbuffered channel receive; the matched non-leak is spawned at
the same rate and does a small bounded amount of work before returning. See
[What it does not do](#what-it-does-not-do) for what this number does and
doesn't tell you.

## The problem

[uber-go/goleak](https://github.com/uber-go/goleak) is the standard tool for
goroutine leaks, and it's excellent — but it is **test-time only**. Its
entire surface (`goleak.VerifyNone(t)`, `goleak.VerifyTestMain(m)`) asserts
that no unexpected goroutines are running at the end of a test. That catches
a huge class of leaks before they ship. It catches nothing after that: once
the binary is running in production, goleak has no presence there at all.

Nothing widely adopted runs *continuously in a live process*, watching
goroutine count the way memory-leak detectors watch heap size. A goroutine
leak in production shows up as slow RSS growth (each leaked goroutine holds
a stack, at minimum 2 KB, plus whatever it's retaining) and eventual OOM-kill
or degraded latency, with nothing in the symptom pointing at the cause. By
the time someone notices, the process has probably already been restarted
once or twice, taking the evidence with it.

This package is not a goleak replacement — it can't be, since goleak's job
(fail this specific test if this specific code path leaked) is a different
question from this package's job (tell me, while the server is up, that
something out there is leaking, and where). Use both: goleak in CI to catch
what it can before merge, goroutinewatch in production to catch what
inevitably still gets through — leaks that only manifest under production
traffic patterns, third-party code goleak's suite never exercised, races that
only lose under real contention.

### The obvious workaround doesn't work either

Checking `runtime.NumGoroutine()` once and alerting past a fixed threshold
doesn't work, for the same reason a single `process.memoryUsage()` reading
doesn't catch a memory leak (see the Node sibling, `leak-check`, for that
argument in full): **a single reading can't tell "this server is busy" from
"this server is leaking."** A web server's goroutine count legitimately
tracks load — it grows under a traffic spike and shrinks back down once the
spike passes. A fixed threshold either fires on every legitimate spike (and
gets muted) or is set so high it never fires on a real leak until the process
is already in trouble. The distinction that actually matters is *does it
recover*, and a single number, or even two numbers five minutes apart,
cannot show that — only a trend across many samples can.

## Install

```sh
go get github.com/agentscope-ai-java/goroutinewatch
```

Go >= 1.27 (see `go.mod`). No runtime dependencies.

## Use

```go
w := goroutinewatch.New(goroutinewatch.Config{
    Interval:   5 * time.Second,
    WindowSize: 24,   // 2 minutes of history at Interval=5s
    Threshold:  2.0,  // goroutines/sample sustained before it's "growth"
    OnGrowth: func(r goroutinewatch.Report) {
        log.Print(r.String())
        // r.Groups[0].Func, r.Groups[0].Location, r.Groups[0].Count, ...
    },
})
w.Start()
defer w.Stop()

mux.Handle("/debug/goroutinewatch", w) // Stats() as JSON
```

A detected episode's `Report.String()` looks like:

```
goroutinewatch: sustained growth detected, 412 goroutines, slope=6.10/sample
  398 in main.(*Pool).worker at /app/worker.go:88 [chan receive]
  9 in net/http.(*persistConn).readLoop at /usr/local/go/src/net/http/transport.go:2261 [select]
  5 in main.(*Server).handleConn at /app/server.go:41 [IO wait]
```

That is the point of this package: not "goroutine count is up," but "412 of
them are blocked in `chan receive` at `worker.go:88`" — a line you can go
look at.

## How it decides "growth," not "a spike"

Every `Interval`, the Watcher reads `runtime.NumGoroutine()` — a cheap atomic
read, safe to do often — and keeps the last `WindowSize` samples. Once at
least `MinSamples` samples exist, it fits a trend across the whole window
using the **Theil-Sen estimator**: the median of the slopes between every
pair of samples, not an ordinary least-squares line through all of them. This
is the same choice the Node sibling (`leak-check`) makes for memory, for the
same reason: a single outlier sample — a burst of short-lived goroutines
answering one batch of requests, landing right when a sample is taken — can
only ever be one endpoint of a minority of the pairwise slopes, so the median
outvotes it instead of letting it drag a least-squares fit off course.
`trend_test.go` demonstrates this: a single spurious 5000-goroutine spike
injected into an otherwise flat window moves the Theil-Sen slope by less than
1 goroutine/sample.

When that slope exceeds `Threshold`, and it wasn't already over `Threshold`
on the previous sample (an edge trigger — see below), the Watcher captures
attribution and calls `OnGrowth`.

### Why an edge trigger, and why that still isn't "detects a leak the instant it starts"

`OnGrowth` fires once per transition into sustained growth, not on every
sample while growth continues. Two reasons: it keeps the expensive stack
capture (below) from firing repeatedly while one leak is ongoing, and it
matches what you'd actually want paged about — "growth has started," not
"growth is still happening" every `Interval` for as long as the leak runs.

The window inherently trades detection latency for stability: with
`WindowSize=24` and `Interval=5s`, the Watcher needs `MinSamples` samples
(2–3 minutes, by default) of sustained upward trend before it calls this
growth rather than noise. A leak that adds one goroutine every ten minutes
will eventually cross the threshold, but slowly; a leak that adds one every
second crosses it almost immediately. Tune `Interval`/`WindowSize`/`Threshold`
to your own workload's baseline noise — there's no universal default, the
same way there's no universal `thresholdBytesPerIteration` in the Node
sibling.

## The real cost of attribution: measured, not assumed

`runtime.Stack(buf, true)` **stops every other goroutine in the process**
while it walks each one's stack — the same "stop the world" cost as a memory
profile snapshot, and it gets more expensive as goroutine count grows, not
just as a constant tax. Measured on this machine (macOS, Apple Silicon, Go
1.27.1; each row parks N goroutines on a channel receive, then captures —
`go run ./examples/stackcost`):

```
     1000 goroutines   avg capture:   2.127568ms   dump size:     267335 bytes (20 captures)
    10000 goroutines   avg capture:  35.058720ms   dump size:    2681455 bytes (10 captures)
   100000 goroutines   avg capture: 503.866424ms   dump size:   26911413 bytes (5 captures)
```

Roughly linear in goroutine count, at about 2-5 microseconds per goroutine on
this machine (the per-goroutine cost rises somewhat as the count grows,
rather than staying perfectly flat) — but "stop the world for half a second"
at 100k goroutines is a real production event in its own right (every other
goroutine, including whatever's serving requests, is frozen for that half
second), not a rounding error. **This is exactly why the Watcher only ever
calls `runtime.Stack` on a detected growth transition, never on a
schedule.** Sampling the count itself
(`runtime.NumGoroutine()`) is a cheap atomic read and safe at any `Interval`;
the stop-the-world cost only happens once per growth episode. If your process
regularly runs at goroutine counts where even that occasional cost is
unacceptable, set `DisableStackCapture: true` and use `Stats()` for count/slope
only — you lose attribution, not detection.

Run `go run ./examples/stackcost` yourself before trusting these numbers in
your environment — they will differ by CPU, by goroutine stack size (a
goroutine holding more on its stack costs more to dump), and by Go version.

## Options

| Option | Default | What it does |
| --- | --- | --- |
| `Interval` | 2s | How often the goroutine count is sampled. |
| `WindowSize` | 30 | How many recent samples feed the trend estimator. |
| `MinSamples` | 8 | Samples required before the trend is evaluated at all. |
| `Threshold` | 1.0 | Slope (goroutines/sample) above which growth is reported. |
| `OnGrowth` | — | Called once per transition into sustained growth, with a `Report`. |
| `DisableStackCapture` | false | Turns off stack attribution (and its stop-the-world cost) entirely. |
| `MaxGroups` | 10 | How many attribution groups a `Report` carries, largest first. |

`Watcher.Stats()` returns the current count, slope, growing flag and sample
count; `Watcher` itself implements `http.Handler`, writing `Stats()` as JSON,
so it can be mounted directly on a debug/metrics mux.

## What it does not do

- **A trend distinguishes "sustained, one-directional growth" from "a spike
  that recovered before the window closed." It cannot always distinguish
  "sustained growth" from "a spike that hasn't recovered yet."** If a
  legitimate traffic surge lasts longer than `WindowSize * Interval` before
  it recedes, the Watcher cannot yet tell that from a leak — both look
  identical from inside one window. It will fire. The only way to tell them
  apart from there is what happens next: a real leak's count keeps climbing
  in the following window too; a recovered spike's count comes back down.
  This package reports the first window that crosses `Threshold` — it does
  not itself wait for a second window to confirm the trend didn't reverse.
  If your workload has traffic surges longer than a few minutes, widen
  `WindowSize` accordingly, and treat one `OnGrowth` call as "worth a look,"
  not "confirmed leak," until you've seen whether the next episode's report
  shows the same call site still climbing.
- **Attribution groups by top stack frame only, not the full call stack.**
  Two structurally different leaks that happen to share a helper — a
  `doRequest` wrapper both a leaking poller and a leaking retry loop call
  into, say — will be merged into one group if that helper is where both
  happen to be blocked. The `Example` field on each `Group` carries one full
  stack trace from that group, specifically so you can tell the two apart by
  eye when the top frame alone is ambiguous.
- **It is a statistical trend check, not a proof.** Like the Node sibling's
  `assertNoLeak`, its verdict is "the trend across N samples does/doesn't
  look like sustained growth above this threshold," not a guarantee about
  every possible leak shape or rate.
- **We measured one leak shape at one rate, not a general reliability
  number.** The 100%/0% figure above is real, but it's for one leak rate (2
  goroutines every 4ms, a deliberately obvious leak) against one threshold,
  on one machine. We have not sweep-tested behavior near the threshold
  boundary, under real production noise, or with leaks that grow far more
  slowly than the 30-trial demo's.
- **A leak slower than `Threshold`, or one that stops accumulating within
  your `WindowSize`, will not trip.** That is the definition you gave it via
  `Threshold`/`WindowSize`, and it may not be the definition you actually
  want for a very slow leak — lower `Threshold` and widen `WindowSize`
  accordingly, at the cost of a slower reaction time and more sensitivity to
  legitimate long surges (see above).
- **Stack capture stops the world, with a real, measured, goroutine-count-
  dependent cost** — see the numbers above. This package never schedules
  that cost; it only ever pays it once per detected growth transition. If
  that single occasional cost is still unacceptable at your process's
  typical goroutine count, disable it with `DisableStackCapture: true`.
- **The stack-dump text format is not a documented, versioned Go contract.**
  `parseStacks` is written against the "goroutine N [state]:" header format
  Go has used across recent releases; a future Go release changing that
  format could make parsing silently return fewer or malformed groups. It
  fails soft (skips anything it doesn't recognize) rather than panicking.
- **It only watches goroutine count.** It says nothing about what those
  goroutines are holding in memory, file descriptors, or other resources
  leaking alongside them — only that there are, or aren't, more goroutines
  over time than there were before.

## Develop

```sh
go build ./...
go vet ./...
go test -race ./...
gofmt -l .

go run ./examples/stackcost   # measures the numbers quoted above
go run ./examples/hitrate     # measures the hit-rate numbers quoted above
```

## License

MIT
