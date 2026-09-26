package goroutinewatch

import (
	"math"
	"testing"
)

func TestTheilSenSlope_LinearTrend(t *testing.T) {
	// A perfectly linear trend of +5 goroutines per sample.
	samples := make([]sample, 10)
	for i := range samples {
		samples[i] = sample{index: i, count: 100 + 5*i}
	}
	got := theilSenSlope(samples)
	if math.Abs(got-5.0) > 1e-9 {
		t.Fatalf("slope = %v, want 5.0", got)
	}
}

func TestTheilSenSlope_Flat(t *testing.T) {
	samples := make([]sample, 10)
	for i := range samples {
		samples[i] = sample{index: i, count: 100}
	}
	got := theilSenSlope(samples)
	if got != 0 {
		t.Fatalf("slope = %v, want 0", got)
	}
}

func TestTheilSenSlope_RobustToOneOutlier(t *testing.T) {
	// Otherwise-flat trend (no leak) with one huge, one-off spike injected
	// in the middle — e.g. a burst of legitimate short-lived goroutines
	// answering a batch of requests. A least-squares fit through these
	// points would be dragged hard toward a positive slope by that single
	// point; Theil-Sen should barely move, since the spike can only be one
	// endpoint of a minority of the pairwise slopes.
	samples := make([]sample, 11)
	for i := range samples {
		samples[i] = sample{index: i, count: 100}
	}
	samples[5].count = 100 + 5000 // one spurious spike

	got := theilSenSlope(samples)
	if math.Abs(got) > 5 {
		t.Fatalf("slope = %v, want close to 0 despite the outlier", got)
	}
}

func TestTheilSenSlope_DetectsGrowthDespiteNoise(t *testing.T) {
	// A real, smaller leak (+2/sample) with noise layered on top: every
	// third sample is off by a few goroutines in either direction. The
	// slope should still land close to the true rate.
	base := []int{0, 2, -1, 3, 1, -2, 4, 0, 2, -1, 3}
	samples := make([]sample, len(base))
	count := 100
	for i, noise := range base {
		count += 2 + noise
		samples[i] = sample{index: i, count: count}
	}
	got := theilSenSlope(samples)
	if got < 1.0 || got > 4.0 {
		t.Fatalf("slope = %v, want roughly 2.0 (allowing noise slack)", got)
	}
}

func TestTheilSenSlope_TooFewSamples(t *testing.T) {
	if got := theilSenSlope(nil); got != 0 {
		t.Fatalf("slope of nil = %v, want 0", got)
	}
	if got := theilSenSlope([]sample{{index: 0, count: 5}}); got != 0 {
		t.Fatalf("slope of one sample = %v, want 0", got)
	}
}
