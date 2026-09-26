package goroutinewatch

import "sort"

// sample is one goroutine-count observation, tagged with its position in the
// sliding window (not wall-clock time) so the slope below comes out in units
// of goroutines-per-sample, independent of how Interval is configured.
type sample struct {
	index int
	count int
}

// theilSenSlope estimates the trend of goroutine count across samples using
// the Theil-Sen estimator: the median of the slopes between every pair of
// samples, rather than an ordinary least-squares fit through all of them.
//
// The reason to prefer the median-of-pairwise-slopes over OLS is the same
// reason the Node sibling (leak-check) uses it for memory: with a handful of
// samples, a single outlier — a GC pause, a scheduler hiccup, a burst of
// short-lived goroutines from an unrelated request — can be one endpoint of
// only a minority of the pairwise slopes, so it gets outvoted by the median
// instead of dragging a least-squares line off course.
//
// It returns 0 if fewer than two samples are given.
func theilSenSlope(samples []sample) float64 {
	n := len(samples)
	if n < 2 {
		return 0
	}
	slopes := make([]float64, 0, n*(n-1)/2)
	for i := 0; i < n; i++ {
		for j := i + 1; j < n; j++ {
			dx := float64(samples[j].index - samples[i].index)
			if dx == 0 {
				continue
			}
			dy := float64(samples[j].count - samples[i].count)
			slopes = append(slopes, dy/dx)
		}
	}
	if len(slopes) == 0 {
		return 0
	}
	sort.Float64s(slopes)
	mid := len(slopes) / 2
	if len(slopes)%2 == 0 {
		return (slopes[mid-1] + slopes[mid]) / 2
	}
	return slopes[mid]
}
