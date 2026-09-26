package goroutinewatch

import (
	"strings"
	"testing"
	"time"
)

// waitUntil polls cond every few milliseconds until it returns true or the
// deadline passes, returning whether it succeeded. Goroutine scheduling and
// stack-dump timing are not instantaneous, so tests that depend on a
// goroutine having reached a particular state poll rather than sleep a fixed
// guess.
func waitUntil(t *testing.T, timeout time.Duration, cond func() bool) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if cond() {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(2 * time.Millisecond)
	}
}

func TestAttribute_GroupsByTopFrame(t *testing.T) {
	stop := make(chan struct{})
	const n = 7
	for i := 0; i < n; i++ {
		go func() {
			<-stop // parks in "chan receive", all at the same source line
		}()
	}
	t.Cleanup(func() { close(stop) })

	// Give the scheduler a moment to actually park them before dumping.
	time.Sleep(20 * time.Millisecond)

	dump, truncated := captureAllStacks()
	if truncated {
		t.Fatalf("unexpected truncation for a tiny dump")
	}
	groups := attribute(dump, -1, 0)

	var found *Group
	for i := range groups {
		if groups[i].Count == n && strings.Contains(groups[i].Func, "TestAttribute_GroupsByTopFrame") {
			found = &groups[i]
			break
		}
	}
	if found == nil {
		t.Fatalf("no group of size %d found for the spawned goroutines; groups=%+v", n, groups)
	}
	if !strings.Contains(found.Location, "stack_test.go") {
		t.Fatalf("group location = %q, want it to reference stack_test.go", found.Location)
	}
	if found.State == "" || !strings.Contains(found.State, "chan receive") {
		t.Fatalf("group state = %q, want it to mention chan receive", found.State)
	}
	if found.Example == "" {
		t.Fatalf("group example stack is empty")
	}
}

func TestCurrentGoroutineID_MatchesSelfInFullDump(t *testing.T) {
	id := currentGoroutineID()
	if id <= 0 {
		t.Fatalf("currentGoroutineID() = %d, want a positive id", id)
	}

	dump, _ := captureAllStacks()
	stacks := parseStacks(dump)

	var self *goroutineStack
	for i := range stacks {
		if stacks[i].id == id {
			self = &stacks[i]
			break
		}
	}
	if self == nil {
		t.Fatalf("id %d from currentGoroutineID() not found in a full dump taken by the same goroutine", id)
	}
	if self.state != "running" {
		t.Fatalf("self goroutine state = %q, want %q (it's the one doing the capturing)", self.state, "running")
	}
}

func TestAttribute_ExcludeIDDropsOnlyThatGoroutine(t *testing.T) {
	// A deliberate leak living in this very package (this is an internal
	// test file, package goroutinewatch) must never be filtered out just
	// because it shares a package with the Watcher's own machinery — only
	// the exact excluded id should be dropped. This is the regression this
	// package's own attribution logic needed: an earlier version filtered by
	// function-name prefix, which also (wrongly) swallowed leaks like this
	// one.
	stop := make(chan struct{})
	defer close(stop)
	const n = 4
	for i := 0; i < n; i++ {
		go func() { <-stop }()
	}
	time.Sleep(20 * time.Millisecond)

	dump, _ := captureAllStacks()
	self := currentGoroutineID()

	before := attribute(dump, -1, 0)
	after := attribute(dump, self, 0)

	find := func(groups []Group) *Group {
		for i := range groups {
			if strings.Contains(groups[i].Func, "TestAttribute_ExcludeIDDropsOnlyThatGoroutine") && groups[i].Count >= n {
				return &groups[i]
			}
		}
		return nil
	}

	if find(before) == nil {
		t.Fatalf("leak group not present even before excluding anything; groups=%+v", before)
	}
	if find(after) == nil {
		t.Fatalf("excluding the calling goroutine's own id also dropped an unrelated same-package leak; groups=%+v", after)
	}

	for _, g := range after {
		if g.Count == 0 {
			continue
		}
		// The excluded goroutine's own top frame (inside captureAllStacks at
		// the moment it call runtime.Stack) must not appear at all.
		if strings.Contains(g.Func, "captureAllStacks") {
			t.Fatalf("excluded goroutine's frame still present: %+v", g)
		}
	}
}

func TestParseStacks_TopLevelSanity(t *testing.T) {
	dump, _ := captureAllStacks()
	stacks := parseStacks(dump)
	if len(stacks) == 0 {
		t.Fatalf("parsed zero goroutines from a real dump")
	}
	for _, s := range stacks {
		if s.id <= 0 {
			t.Fatalf("goroutine with non-positive id: %+v", s)
		}
		if s.topFunc == "" {
			t.Fatalf("goroutine with empty top func: %+v", s)
		}
	}
}
