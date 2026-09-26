package goroutinewatch

import "runtime"

// numGoroutine is runtime.NumGoroutine, indirected through a package
// variable purely so tests can substitute a deterministic sequence without
// needing to actually spawn thousands of goroutines to exercise the trend
// logic in isolation. Production code always uses the real thing; it is
// never reassigned outside tests.
var numGoroutine = runtime.NumGoroutine
