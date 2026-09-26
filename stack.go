package goroutinewatch

import (
	"bytes"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
)

// maxStackBufBytes caps how far captureAllStacks will grow its buffer while
// retrying runtime.Stack. 512 MiB comfortably covers well over a million
// goroutines' worth of dumped stacks; beyond that we give up and return a
// truncated (but still useful) dump rather than let a pathological process
// force an unbounded allocation on top of the world it just stopped.
const maxStackBufBytes = 512 << 20

// captureAllStacks calls runtime.Stack(buf, true) to dump every goroutine's
// stack, growing the buffer and retrying until the dump fits (the standard
// pattern for this API: runtime.Stack fills as much as it can and returns
// len(buf) when it was truncated, with no other signal that it was cut off).
//
// This is the expensive, world-stopping call in this package — see the cost
// numbers measured in examples/stackcost and quoted in the README. Watcher
// only ever calls this after its trend check has already flagged sustained
// growth, never on a fixed schedule.
func captureAllStacks() (dump []byte, truncated bool) {
	buf := make([]byte, 64*1024)
	for {
		n := runtime.Stack(buf, true)
		if n < len(buf) {
			return buf[:n], false
		}
		if len(buf) >= maxStackBufBytes {
			return buf[:n], true
		}
		buf = make([]byte, 2*len(buf))
	}
}

// goroutineHeaderRe matches the first line of each goroutine's block in a
// runtime.Stack(buf, true) dump, e.g. "goroutine 34 [chan receive]:".
var goroutineHeaderRe = regexp.MustCompile(`^goroutine (\d+) \[([^\]]*)\]:$`)

// goroutineStack is one parsed goroutine from a stack dump: its id, the
// state runtime.Stack reported for it (e.g. "chan receive", "running"), its
// top user frame (the function and file:line it was executing or blocked
// in), and the full text of its block for use as a representative example.
type goroutineStack struct {
	id      int
	state   string
	topFunc string
	topLoc  string
	full    string
}

// parseStacks splits a runtime.Stack(buf, true) dump into its individual
// goroutines. It is intentionally tolerant of anything it doesn't recognize
// (skips the block) rather than erroring, since the dump format is not a
// documented, versioned contract — this parses the shape Go has used across
// recent releases, keyed on the "goroutine N [state]:" header line that has
// been stable for a long time.
func parseStacks(dump []byte) []goroutineStack {
	blocks := strings.Split(string(dump), "\n\n")
	stacks := make([]goroutineStack, 0, len(blocks))
	for _, block := range blocks {
		block = strings.TrimRight(block, "\n")
		if block == "" {
			continue
		}
		lines := strings.Split(block, "\n")
		if len(lines) < 3 {
			continue
		}
		m := goroutineHeaderRe.FindStringSubmatch(lines[0])
		if m == nil {
			continue
		}
		id, err := strconv.Atoi(m[1])
		if err != nil {
			continue
		}
		funcLine := strings.TrimSpace(lines[1])
		locLine := strings.TrimSpace(lines[2])

		funcName := funcLine
		if idx := strings.IndexByte(funcLine, '('); idx >= 0 {
			funcName = funcLine[:idx]
		}
		loc := locLine
		if idx := strings.Index(loc, " +0x"); idx >= 0 {
			loc = loc[:idx]
		}

		stacks = append(stacks, goroutineStack{
			id:      id,
			state:   m[2],
			topFunc: funcName,
			topLoc:  loc,
			full:    block,
		})
	}
	return stacks
}

// currentGoroutineID returns the id runtime.Stack assigns to the calling
// goroutine, by taking a single-goroutine (non-stop-the-world) trace of just
// itself and parsing the header. This is cheap — it is not the expensive,
// all-goroutines call — and is used to identify and exclude the Watcher's
// own background goroutine from its own attribution.
//
// Excluding by id, rather than by matching (*Watcher).run's function name,
// is deliberate: runtime.Stack(buf, true) documents that the calling
// goroutine's trace comes first, but that goroutine is captured wherever it
// currently is on its own call stack — inside captureAllStacks or tick or
// buildReport, not inside run itself, since run is what called down into
// all of those. A function-name filter for "run" therefore never actually
// matches the real self-capture, and a broader package-prefix filter
// over-matches: it would also swallow a real leak whose goroutines happen to
// live in the same package as the Watcher construction site (this package's
// own tests hit exactly that, with a deliberately leaking test goroutine
// defined in package goroutinewatch). Matching the exact, numeric id of the
// one goroutine actually doing the capturing has neither problem.
func currentGoroutineID() int {
	buf := make([]byte, 256)
	for {
		n := runtime.Stack(buf, false)
		if n < len(buf) {
			buf = buf[:n]
			break
		}
		buf = make([]byte, 2*len(buf))
	}
	nl := bytes.IndexByte(buf, '\n')
	if nl < 0 {
		nl = len(buf)
	}
	m := goroutineHeaderRe.FindSubmatch(buf[:nl])
	if m == nil {
		return -1
	}
	id, err := strconv.Atoi(string(m[1]))
	if err != nil {
		return -1
	}
	return id
}

// Group is a set of goroutines that share a top frame, i.e. that were all
// executing (or blocked in) the same function at the same file:line when
// growth was detected.
type Group struct {
	// Func is the top user frame's function name, e.g. "main.worker".
	Func string
	// Location is the top user frame's file:line, e.g. "/app/worker.go:88".
	Location string
	// State is the most common runtime state among this group's goroutines
	// at capture time, e.g. "chan receive". Goroutines in one group can have
	// mixed states if they share a top frame but were sampled mid-transition;
	// States records the full breakdown.
	State string
	// States is every distinct state seen in this group, mapped to how many
	// goroutines reported it.
	States map[string]int
	// Count is how many goroutines share this group's top frame.
	Count int
	// Example is the full stack trace text of one representative goroutine
	// from this group.
	Example string
}

// attribute groups parsed goroutine stacks by their top frame and returns
// the groups sorted by size, largest first, with at most topN groups
// returned (0 means no limit). The goroutine whose id equals excludeID (a
// negative value excludes nothing) is left out entirely — used to drop the
// Watcher's own goroutine, which is always the one actually calling
// runtime.Stack, from its own attribution; see currentGoroutineID's doc for
// why that's an id match rather than a function-name match. Grouping by top
// frame only — not the full stack — is a deliberate, documented
// simplification: two structurally different leaks that happen to share a
// helper function (a shared "doRequest" wrapper, say) will be merged into
// one group. See the README's honesty section.
func attribute(dump []byte, excludeID int, topN int) []Group {
	stacks := parseStacks(dump)
	order := make([]string, 0, len(stacks))
	groups := make(map[string]*Group, len(stacks))

	for _, s := range stacks {
		if s.id == excludeID {
			continue
		}
		key := s.topFunc + "@" + s.topLoc
		g, ok := groups[key]
		if !ok {
			g = &Group{
				Func:     s.topFunc,
				Location: s.topLoc,
				States:   make(map[string]int),
				Example:  s.full,
			}
			groups[key] = g
			order = append(order, key)
		}
		g.Count++
		g.States[s.state]++
	}

	result := make([]Group, 0, len(order))
	for _, key := range order {
		g := groups[key]
		best, bestN := "", 0
		for state, n := range g.States {
			if n > bestN {
				best, bestN = state, n
			}
		}
		g.State = best
		result = append(result, *g)
	}

	sort.Slice(result, func(i, j int) bool {
		if result[i].Count != result[j].Count {
			return result[i].Count > result[j].Count
		}
		return result[i].Func < result[j].Func
	})

	if topN > 0 && len(result) > topN {
		result = result[:topN]
	}
	return result
}
