package agent

import (
	"github.com/prometheus/client_golang/prometheus"

	"github.com/nodebeat/agent/internal/supervise"
)

// statsCollector exposes supervisor child state as Prometheus metrics,
// read live from the supervisor on every scrape.
type statsCollector struct {
	stats    func() []supervise.Stats
	up       *prometheus.Desc
	restarts *prometheus.Desc
	failures *prometheus.Desc
}

func newStatsCollector(stats func() []supervise.Stats) *statsCollector {
	return &statsCollector{
		stats: stats,
		up: prometheus.NewDesc(
			"nodebeat_child_up",
			"1 when the supervised child process is running.",
			[]string{"child"}, nil,
		),
		restarts: prometheus.NewDesc(
			"nodebeat_child_restarts_total",
			"Total child process restarts since agent start.",
			[]string{"child"}, nil,
		),
		failures: prometheus.NewDesc(
			"nodebeat_child_consecutive_failures",
			"Current consecutive crash count for the child.",
			[]string{"child"}, nil,
		),
	}
}

func (c *statsCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.up
	ch <- c.restarts
	ch <- c.failures
}

func (c *statsCollector) Collect(ch chan<- prometheus.Metric) {
	for _, s := range c.stats() {
		up := 0.0
		if s.Running {
			up = 1.0
		}
		ch <- prometheus.MustNewConstMetric(c.up, prometheus.GaugeValue, up, s.Name)
		ch <- prometheus.MustNewConstMetric(c.restarts, prometheus.CounterValue, float64(s.Restarts), s.Name)
		ch <- prometheus.MustNewConstMetric(c.failures, prometheus.GaugeValue, float64(s.Failures), s.Name)
	}
}
