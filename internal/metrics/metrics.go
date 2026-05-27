// Package metrics owns the oracle-service Prometheus registry.
//
// Service-owned registry (not the default global one) so tests can spin up
// new instances without colliding on Register, and so we don't accidentally
// scrape some other library's globals on /metrics.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

// Metrics bundles every counter / gauge / histogram the service emits.
type Metrics struct {
	Registry *prometheus.Registry

	SubmissionsTotal      *prometheus.CounterVec
	SubmissionDuration    *prometheus.HistogramVec
	SignatureSetTotal     *prometheus.CounterVec
	GasUsed               prometheus.Histogram
	ReporterBalance       *prometheus.GaugeVec
	StreamEventsReceived  *prometheus.CounterVec
	StreamReconnectTotal  prometheus.Counter
	StreamLagSeconds      prometheus.Gauge
	HeartbeatSkippedTotal *prometheus.CounterVec
}

// New constructs and registers every metric on a fresh registry.
func New() *Metrics {
	reg := prometheus.NewRegistry()

	m := &Metrics{
		Registry: reg,
		SubmissionsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "oracle_submissions_total",
			Help: "Number of fulfillPrice submissions, keyed by asset and terminal status.",
		}, []string{"asset", "status"}),
		SubmissionDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "oracle_submission_duration_seconds",
			Help:    "Wall-clock seconds from submit() entry to terminal state.",
			Buckets: prometheus.DefBuckets,
		}, []string{"asset"}),
		SignatureSetTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "oracle_signature_set_total",
			Help: "Number of signatures produced per reporter.",
		}, []string{"reporter_address"}),
		GasUsed: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "oracle_gas_used",
			Help:    "Gas used per confirmed fulfillPrice tx.",
			Buckets: []float64{50_000, 100_000, 200_000, 300_000, 500_000, 1_000_000},
		}),
		ReporterBalance: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "oracle_reporter_balance_eth",
			Help: "Reporter wallet native-token balance (ETH).",
		}, []string{"address"}),
		StreamEventsReceived: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "oracle_stream_events_received_total",
			Help: "Number of events received from indexer.StreamEvents, by kind.",
		}, []string{"kind"}),
		StreamReconnectTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "oracle_stream_reconnect_total",
			Help: "Number of times the indexer stream reconnected after an error.",
		}),
		StreamLagSeconds: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "oracle_stream_lag_seconds",
			Help: "now() - meta.observed_at on the last delivered event.",
		}),
		HeartbeatSkippedTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "oracle_heartbeat_skipped_total",
			Help: "Heartbeat ticks that decided not to fire (within tolerance + within interval).",
		}, []string{"symbol"}),
	}

	reg.MustRegister(
		m.SubmissionsTotal,
		m.SubmissionDuration,
		m.SignatureSetTotal,
		m.GasUsed,
		m.ReporterBalance,
		m.StreamEventsReceived,
		m.StreamReconnectTotal,
		m.StreamLagSeconds,
		m.HeartbeatSkippedTotal,
	)
	return m
}
