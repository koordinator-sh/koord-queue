package metrics

import (
	"context"
	"net/url"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	clientgometrics "k8s.io/client-go/tools/metrics"
	"k8s.io/component-base/metrics"
	// _ "k8s.io/component-base/metrics/prometheus/restclient"
)

var (
	QueueUnitsInActiveQueue = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "queueunits_in_active_queue",
		Help: "The total number of queueunits in active queues",
	}, []string{"queue"})
	QueueUnitsInBackoffQueue = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "queueunits_in_backoff_queue",
		Help: "The total number of queueunits in backoff queue",
	}, []string{"queue"})
	QuotaUsageByQuota = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "dequeued_quota_usage_by_quota",
		Help: "Sum resources of dequeued job",
	}, []string{"quota", "resource"})
	QuotaUsageByNamespace = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "dequeued_quota_usage_by_namespace",
		Help: "Sum resources of dequeued job",
	}, []string{"namespace", "resource"})
	JobSchedulingAlgorithmLatency = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "job_scheduling_algorithm_latency",
		Help:    "Latency of job scheduling algorithm",
		Buckets: metrics.ExponentialBuckets(0.01, 2, 15),
	})
	// JobSchedulingE2ELatency = promauto.NewHistogram(prometheus.HistogramOpts{
	// 	Name:    "job_scheduling_e2e_latency",
	// 	Help:    "Latency of job scheduling e2e process",
	// 	Buckets: metrics.ExponentialBuckets(0.01, 2, 15),
	// })
	JobScheduleAttempts = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "job_schedule_attempts",
	}, []string{"queue", "result"})
	QueueUnitsByJobType = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "queueunits_by_job_type",
		Help: "total queueunits",
	}, []string{"namespace", "type"})
	QueueUnitsComingRate = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "queueunits_coming_rate_by_job_type",
		Help: "total queueunits",
	}, []string{"namespace", "type", "status"})
	// RequestLatency = promauto.NewHistogramVec(prometheus.HistogramOpts{
	// 	Name:    "rest_client_request_duration_seconds",
	// 	Help:    "Request latency in seconds. Broken down by verb, and host.",
	// 	Buckets: []float64{0.005, 0.025, 0.1, 0.25, 0.5, 1.0, 2.0, 4.0, 8.0, 15.0, 30.0, 60.0},
	// }, []string{"verb", "host"})
)

type latencyAdapter struct {
	m *prometheus.HistogramVec
}

func (l *latencyAdapter) Observe(ctx context.Context, verb string, u url.URL, latency time.Duration) {
	l.m.WithLabelValues(verb, u.Host).Observe(latency.Seconds())
}

type resultAdapter struct {
	m *prometheus.CounterVec
}

func (r *resultAdapter) Increment(ctx context.Context, code, method, host string) {
	r.m.WithLabelValues(code, method, host).Inc()
}

func init() {
	// requestLatency is a Prometheus Histogram metric type partitioned by
	// "verb", and "host" labels. It is used for the rest client latency metrics.
	requestLatency := promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "rest_client_request_duration_seconds",
		Help:    "Request latency in seconds. Broken down by verb, and host.",
		Buckets: []float64{0.005, 0.025, 0.1, 0.25, 0.5, 1.0, 2.0, 4.0, 8.0, 15.0, 30.0, 60.0},
	}, []string{"verb", "host"})
	requestResult := promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "rest_client_requests_total",
		Help: "Number of HTTP requests, partitioned by status code, method, and host.",
	}, []string{"code", "method", "host"})
	// prometheus.MustRegister(requestLatency.HistogramVec)
	clientgometrics.Register(clientgometrics.RegisterOpts{
		RequestLatency: &latencyAdapter{m: requestLatency},
		RequestResult:  &resultAdapter{m: requestResult},
	})
}
