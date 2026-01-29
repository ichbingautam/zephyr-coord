// Package server - Prometheus-compatible metrics.
package server

import (
	"fmt"
	"io"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// MetricType represents the type of metric.
type MetricType string

const (
	MetricCounter   MetricType = "counter"
	MetricGauge     MetricType = "gauge"
	MetricHistogram MetricType = "histogram"
)

// Metric represents a single metric.
type Metric struct {
	Name   string
	Help   string
	Type   MetricType
	Labels map[string]string
	Value  atomic.Int64
}

// Counter is a monotonically increasing counter.
type Counter struct {
	name   string
	help   string
	labels map[string]string
	value  atomic.Int64
}

// NewCounter creates a new counter.
func NewCounter(name, help string, labels map[string]string) *Counter {
	return &Counter{
		name:   name,
		help:   help,
		labels: labels,
	}
}

// Inc increments the counter by 1.
func (c *Counter) Inc() {
	c.value.Add(1)
}

// Add adds the given value to the counter.
func (c *Counter) Add(delta int64) {
	c.value.Add(delta)
}

// Value returns the current counter value.
func (c *Counter) Value() int64 {
	return c.value.Load()
}

// Gauge is a metric that can go up and down.
type Gauge struct {
	name   string
	help   string
	labels map[string]string
	value  atomic.Int64
}

// NewGauge creates a new gauge.
func NewGauge(name, help string, labels map[string]string) *Gauge {
	return &Gauge{
		name:   name,
		help:   help,
		labels: labels,
	}
}

// Set sets the gauge value.
func (g *Gauge) Set(value int64) {
	g.value.Store(value)
}

// Inc increments the gauge by 1.
func (g *Gauge) Inc() {
	g.value.Add(1)
}

// Dec decrements the gauge by 1.
func (g *Gauge) Dec() {
	g.value.Add(-1)
}

// Add adds the given value to the gauge.
func (g *Gauge) Add(delta int64) {
	g.value.Add(delta)
}

// Value returns the current gauge value.
func (g *Gauge) Value() int64 {
	return g.value.Load()
}

// Histogram tracks the distribution of values.
type Histogram struct {
	name    string
	help    string
	labels  map[string]string
	buckets []float64

	mu         sync.Mutex
	count      int64
	sum        float64
	bucketVals []int64
}

// NewHistogram creates a new histogram.
func NewHistogram(name, help string, labels map[string]string, buckets []float64) *Histogram {
	sort.Float64s(buckets)
	return &Histogram{
		name:       name,
		help:       help,
		labels:     labels,
		buckets:    buckets,
		bucketVals: make([]int64, len(buckets)),
	}
}

// Observe records a value in the histogram.
func (h *Histogram) Observe(value float64) {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.count++
	h.sum += value

	for i, bound := range h.buckets {
		if value <= bound {
			h.bucketVals[i]++
		}
	}
}

// DefaultBuckets returns default histogram buckets for latency.
func DefaultBuckets() []float64 {
	return []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10}
}

// MetricsRegistry holds all registered metrics.
type MetricsRegistry struct {
	mu         sync.RWMutex
	counters   map[string]*Counter
	gauges     map[string]*Gauge
	histograms map[string]*Histogram

	// Pre-defined ZephyrCoord metrics
	RequestsTotal     *Counter
	RequestDuration   *Histogram
	ActiveConnections *Gauge
	NodeCount         *Gauge
	WatchCount        *Gauge
	SessionCount      *Gauge
	BytesReceived     *Counter
	BytesSent         *Counter
	WalWrites         *Counter
	SnapshotsTaken    *Counter
	ElectionCount     *Counter
	ProposalCount     *Counter
	CommitCount       *Counter
	LeadershipChanges *Counter
}

// NewMetricsRegistry creates a new metrics registry with pre-defined metrics.
func NewMetricsRegistry() *MetricsRegistry {
	r := &MetricsRegistry{
		counters:   make(map[string]*Counter),
		gauges:     make(map[string]*Gauge),
		histograms: make(map[string]*Histogram),
	}

	// Initialize pre-defined metrics
	r.RequestsTotal = r.NewCounter("zephyr_requests_total", "Total number of requests processed", nil)
	r.RequestDuration = r.NewHistogram("zephyr_request_duration_seconds", "Request latency histogram", nil, DefaultBuckets())
	r.ActiveConnections = r.NewGauge("zephyr_active_connections", "Number of active client connections", nil)
	r.NodeCount = r.NewGauge("zephyr_node_count", "Number of znodes in the data tree", nil)
	r.WatchCount = r.NewGauge("zephyr_watch_count", "Number of registered watches", nil)
	r.SessionCount = r.NewGauge("zephyr_session_count", "Number of active sessions", nil)
	r.BytesReceived = r.NewCounter("zephyr_bytes_received_total", "Total bytes received", nil)
	r.BytesSent = r.NewCounter("zephyr_bytes_sent_total", "Total bytes sent", nil)
	r.WalWrites = r.NewCounter("zephyr_wal_writes_total", "Total WAL writes", nil)
	r.SnapshotsTaken = r.NewCounter("zephyr_snapshots_total", "Total snapshots taken", nil)
	r.ElectionCount = r.NewCounter("zephyr_elections_total", "Total leader elections", nil)
	r.ProposalCount = r.NewCounter("zephyr_proposals_total", "Total proposals made", nil)
	r.CommitCount = r.NewCounter("zephyr_commits_total", "Total commits processed", nil)
	r.LeadershipChanges = r.NewCounter("zephyr_leadership_changes_total", "Total leadership changes", nil)

	return r
}

// NewCounter registers and returns a new counter.
func (r *MetricsRegistry) NewCounter(name, help string, labels map[string]string) *Counter {
	c := NewCounter(name, help, labels)
	r.mu.Lock()
	r.counters[name] = c
	r.mu.Unlock()
	return c
}

// NewGauge registers and returns a new gauge.
func (r *MetricsRegistry) NewGauge(name, help string, labels map[string]string) *Gauge {
	g := NewGauge(name, help, labels)
	r.mu.Lock()
	r.gauges[name] = g
	r.mu.Unlock()
	return g
}

// NewHistogram registers and returns a new histogram.
func (r *MetricsRegistry) NewHistogram(name, help string, labels map[string]string, buckets []float64) *Histogram {
	h := NewHistogram(name, help, labels, buckets)
	r.mu.Lock()
	r.histograms[name] = h
	r.mu.Unlock()
	return h
}

// WritePrometheus writes metrics in Prometheus text format.
func (r *MetricsRegistry) WritePrometheus(w io.Writer) error {
	r.mu.RLock()
	defer r.mu.RUnlock()

	// Write counters
	for name, c := range r.counters {
		if c.help != "" {
			fmt.Fprintf(w, "# HELP %s %s\n", name, c.help)
		}
		fmt.Fprintf(w, "# TYPE %s counter\n", name)
		fmt.Fprintf(w, "%s%s %d\n", name, formatLabels(c.labels), c.Value())
	}

	// Write gauges
	for name, g := range r.gauges {
		if g.help != "" {
			fmt.Fprintf(w, "# HELP %s %s\n", name, g.help)
		}
		fmt.Fprintf(w, "# TYPE %s gauge\n", name)
		fmt.Fprintf(w, "%s%s %d\n", name, formatLabels(g.labels), g.Value())
	}

	// Write histograms
	for name, h := range r.histograms {
		h.mu.Lock()
		if h.help != "" {
			fmt.Fprintf(w, "# HELP %s %s\n", name, h.help)
		}
		fmt.Fprintf(w, "# TYPE %s histogram\n", name)

		var cumulative int64
		for i, bound := range h.buckets {
			cumulative += h.bucketVals[i]
			fmt.Fprintf(w, "%s_bucket{le=\"%g\"} %d\n", name, bound, cumulative)
		}
		fmt.Fprintf(w, "%s_bucket{le=\"+Inf\"} %d\n", name, h.count)
		fmt.Fprintf(w, "%s_sum %g\n", name, h.sum)
		fmt.Fprintf(w, "%s_count %d\n", name, h.count)
		h.mu.Unlock()
	}

	// Write Go runtime metrics
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	fmt.Fprintf(w, "# HELP go_goroutines Number of goroutines\n")
	fmt.Fprintf(w, "# TYPE go_goroutines gauge\n")
	fmt.Fprintf(w, "go_goroutines %d\n", runtime.NumGoroutine())

	fmt.Fprintf(w, "# HELP go_heap_alloc_bytes Bytes allocated and still in use\n")
	fmt.Fprintf(w, "# TYPE go_heap_alloc_bytes gauge\n")
	fmt.Fprintf(w, "go_heap_alloc_bytes %d\n", m.HeapAlloc)

	fmt.Fprintf(w, "# HELP go_heap_sys_bytes Bytes of heap obtained from system\n")
	fmt.Fprintf(w, "# TYPE go_heap_sys_bytes gauge\n")
	fmt.Fprintf(w, "go_heap_sys_bytes %d\n", m.HeapSys)

	fmt.Fprintf(w, "# HELP go_gc_pause_ns_total Total GC pause time in nanoseconds\n")
	fmt.Fprintf(w, "# TYPE go_gc_pause_ns_total counter\n")
	fmt.Fprintf(w, "go_gc_pause_ns_total %d\n", m.PauseTotalNs)

	return nil
}

// formatLabels formats labels for Prometheus output.
func formatLabels(labels map[string]string) string {
	if len(labels) == 0 {
		return ""
	}

	var parts []string
	for k, v := range labels {
		parts = append(parts, fmt.Sprintf(`%s="%s"`, k, v))
	}
	sort.Strings(parts)
	return "{" + strings.Join(parts, ",") + "}"
}

// Timer is a helper for measuring request duration.
type Timer struct {
	start     time.Time
	histogram *Histogram
}

// NewTimer creates a timer that will record to the given histogram.
func NewTimer(h *Histogram) *Timer {
	return &Timer{
		start:     time.Now(),
		histogram: h,
	}
}

// ObserveDuration records the elapsed time since timer creation.
func (t *Timer) ObserveDuration() {
	duration := time.Since(t.start).Seconds()
	t.histogram.Observe(duration)
}
