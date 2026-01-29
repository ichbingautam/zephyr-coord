package server

import (
	"bytes"
	"strings"
	"testing"
)

func TestCounter(t *testing.T) {
	c := NewCounter("test_counter", "A test counter", nil)

	if c.Value() != 0 {
		t.Errorf("expected 0, got %d", c.Value())
	}

	c.Inc()
	if c.Value() != 1 {
		t.Errorf("expected 1, got %d", c.Value())
	}

	c.Add(5)
	if c.Value() != 6 {
		t.Errorf("expected 6, got %d", c.Value())
	}
}

func TestGauge(t *testing.T) {
	g := NewGauge("test_gauge", "A test gauge", nil)

	if g.Value() != 0 {
		t.Errorf("expected 0, got %d", g.Value())
	}

	g.Set(100)
	if g.Value() != 100 {
		t.Errorf("expected 100, got %d", g.Value())
	}

	g.Inc()
	if g.Value() != 101 {
		t.Errorf("expected 101, got %d", g.Value())
	}

	g.Dec()
	if g.Value() != 100 {
		t.Errorf("expected 100, got %d", g.Value())
	}

	g.Add(-50)
	if g.Value() != 50 {
		t.Errorf("expected 50, got %d", g.Value())
	}
}

func TestHistogram(t *testing.T) {
	h := NewHistogram("test_histogram", "A test histogram", nil, []float64{0.1, 0.5, 1.0})

	h.Observe(0.05)
	h.Observe(0.3)
	h.Observe(0.7)
	h.Observe(2.0)

	h.mu.Lock()
	defer h.mu.Unlock()

	if h.count != 4 {
		t.Errorf("expected count 4, got %d", h.count)
	}

	// Sum should be 0.05 + 0.3 + 0.7 + 2.0 = 3.05
	if h.sum != 3.05 {
		t.Errorf("expected sum 3.05, got %f", h.sum)
	}

	// Bucket 0.1 should have 1 (0.05)
	if h.bucketVals[0] != 1 {
		t.Errorf("bucket 0.1: expected 1, got %d", h.bucketVals[0])
	}

	// Bucket 0.5 should have 2 (0.05, 0.3)
	if h.bucketVals[1] != 2 {
		t.Errorf("bucket 0.5: expected 2, got %d", h.bucketVals[1])
	}

	// Bucket 1.0 should have 3 (0.05, 0.3, 0.7)
	if h.bucketVals[2] != 3 {
		t.Errorf("bucket 1.0: expected 3, got %d", h.bucketVals[2])
	}
}

func TestMetricsRegistry(t *testing.T) {
	r := NewMetricsRegistry()

	// Test pre-defined metrics exist
	if r.RequestsTotal == nil {
		t.Error("RequestsTotal should not be nil")
	}

	if r.ActiveConnections == nil {
		t.Error("ActiveConnections should not be nil")
	}

	if r.RequestDuration == nil {
		t.Error("RequestDuration should not be nil")
	}

	// Use the metrics
	r.RequestsTotal.Inc()
	r.RequestsTotal.Inc()
	r.ActiveConnections.Set(5)
	r.RequestDuration.Observe(0.01)

	if r.RequestsTotal.Value() != 2 {
		t.Errorf("expected 2, got %d", r.RequestsTotal.Value())
	}

	if r.ActiveConnections.Value() != 5 {
		t.Errorf("expected 5, got %d", r.ActiveConnections.Value())
	}
}

func TestMetricsRegistry_WritePrometheus(t *testing.T) {
	r := NewMetricsRegistry()

	r.RequestsTotal.Add(100)
	r.ActiveConnections.Set(10)
	r.RequestDuration.Observe(0.05)

	var buf bytes.Buffer
	err := r.WritePrometheus(&buf)
	if err != nil {
		t.Fatalf("WritePrometheus failed: %v", err)
	}

	output := buf.String()

	// Check for expected metrics
	if !strings.Contains(output, "zephyr_requests_total") {
		t.Error("output should contain zephyr_requests_total")
	}

	if !strings.Contains(output, "zephyr_active_connections") {
		t.Error("output should contain zephyr_active_connections")
	}

	if !strings.Contains(output, "zephyr_request_duration_seconds") {
		t.Error("output should contain zephyr_request_duration_seconds")
	}

	if !strings.Contains(output, "go_goroutines") {
		t.Error("output should contain go_goroutines")
	}

	if !strings.Contains(output, "# TYPE") {
		t.Error("output should contain TYPE declarations")
	}

	if !strings.Contains(output, "# HELP") {
		t.Error("output should contain HELP text")
	}
}

func TestFormatLabels(t *testing.T) {
	tests := []struct {
		labels   map[string]string
		expected string
	}{
		{nil, ""},
		{map[string]string{}, ""},
		{map[string]string{"method": "get"}, `{method="get"}`},
	}

	for _, tc := range tests {
		result := formatLabels(tc.labels)
		if result != tc.expected {
			t.Errorf("formatLabels(%v) = %s, expected %s", tc.labels, result, tc.expected)
		}
	}
}

func TestTimer(t *testing.T) {
	h := NewHistogram("latency", "", nil, []float64{0.001, 0.01, 0.1, 1})

	timer := NewTimer(h)
	// Do some work
	for i := 0; i < 1000; i++ {
		_ = i * i
	}
	timer.ObserveDuration()

	h.mu.Lock()
	defer h.mu.Unlock()

	if h.count != 1 {
		t.Errorf("expected 1 observation, got %d", h.count)
	}
}

func BenchmarkCounter_Inc(b *testing.B) {
	c := NewCounter("bench", "", nil)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Inc()
	}
}

func BenchmarkGauge_Set(b *testing.B) {
	g := NewGauge("bench", "", nil)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		g.Set(int64(i))
	}
}

func BenchmarkHistogram_Observe(b *testing.B) {
	h := NewHistogram("bench", "", nil, DefaultBuckets())
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h.Observe(float64(i) * 0.001)
	}
}
