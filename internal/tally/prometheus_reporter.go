package tally

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"math"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/uber-go/tally/v4"
	"go.uber.org/fx"
	"go.uber.org/zap"

	"github.com/coinbase/chainstorage/internal/config"
)

var defaultBuckets = []float64{
	0.1, 0.2, 0.3, 0.5, 0.7, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
	12, 15, 20, 25, 30, 40, 50, 75, 100, 200, 300, 500, 750,
	1000, 2000, 3000, 5000, 7000, 10000,
}

type prometheusReporter struct {
	stats *prometheusStats
}

func newPrometheusReporter(
	cfg *config.PrometheusConfig,
	lifecycle fx.Lifecycle,
	logger *zap.Logger,
) tally.StatsReporter {
	opts := []prometheusStatsOption{}
	if cfg.Namespace != "" {
		opts = append(opts, withPrometheusNamespace(cfg.Namespace))
	}
	if len(cfg.GlobalLabels) > 0 {
		opts = append(opts, withPrometheusLabels(cfg.GlobalLabels))
	}
	if len(cfg.DefaultHistogramBuckets) > 0 {
		opts = append(opts, withDefaultPrometheusHistogramBuckets(defaultBuckets))
	}
	if len(cfg.HistogramBuckets) > 0 {
		opts = append(opts, withPrometheusHistogramBuckets(cfg.HistogramBuckets))
	}

	s := newPrometheusStats(logger, opts...)

	mux := http.NewServeMux()

	metricsPath := "/metrics"
	if cfg.MetricsPath != "" {
		metricsPath = cfg.MetricsPath
	}
	mux.Handle(metricsPath, s.MetricsHandler())

	addr := fmt.Sprintf(":%d", cfg.Port)
	srv := &http.Server{
		Addr:    addr,
		Handler: mux,
	}

	lifecycle.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			logger.Info("prometheus metrics server starting", zap.String("address", addr))

			go func() {
				if err := srv.ListenAndServe(); err != nil {
					if err != http.ErrServerClosed {
						logger.Error("prometheus metrics server failed to start", zap.Error(err))
					}
				}
			}()

			return nil
		},
		OnStop: func(ctx context.Context) error {
			logger.Info("prometheus metrics server stopping", zap.String("address", addr))
			return srv.Shutdown(ctx)
		},
	})

	return &prometheusReporter{
		stats: s,
	}
}

func (p *prometheusReporter) labels() prometheus.Labels {
	return p.stats.Labels()
}

func (p *prometheusReporter) Capabilities() tally.Capabilities {
	return p
}

func (p *prometheusReporter) Reporting() bool {
	return true
}

func (p *prometheusReporter) Tagging() bool {
	return true
}

func (p *prometheusReporter) Flush() {
	// no-op
}

func (p *prometheusReporter) ReportCounter(name string, tags map[string]string, value int64) {
	p.stats.Count(name, value, p.tags(tags))
}

func (p *prometheusReporter) ReportGauge(name string, tags map[string]string, value float64) {
	p.stats.Gauge(name, value, p.tags(tags))
}

func (p *prometheusReporter) ReportHistogramDurationSamples(
	name string,
	tags map[string]string,
	buckets tally.Buckets,
	bucketLowerBound time.Duration,
	bucketUpperBound time.Duration,
	samples int64,
) {
	panic("unimplemented")
}

func (p *prometheusReporter) ReportHistogramValueSamples(
	name string,
	tags map[string]string,
	buckets tally.Buckets,
	bucketLowerBound float64,
	bucketUpperBound float64,
	samples int64,
) {
	panic("unimplemented")
}

func (p *prometheusReporter) ReportTimer(
	name string,
	tags map[string]string,
	interval time.Duration,
) {
	p.stats.Timing(name, interval, p.tags(tags))
}

func (p *prometheusReporter) tags(tags map[string]string) map[string]string {
	if len(tags) == 0 {
		return p.labels()
	}

	m := make(map[string]string)
	maps.Copy(m, p.labels())
	maps.Copy(m, tags)

	return m
}

type prometheusStats struct {
	mux    sync.RWMutex
	logger *zap.Logger

	counters         map[string]*prometheus.CounterVec
	gauges           map[string]*prometheus.GaugeVec
	histograms       map[string]*prometheus.HistogramVec
	histogramBuckets map[string][]float64
	defaultBuckets   []float64
	reg              *prometheus.Registry

	globalLabels prometheus.Labels
	namespace    string
}

type prometheusStatsOption func(*prometheusStats)

func withPrometheusNamespace(namespace string) prometheusStatsOption {
	return func(s *prometheusStats) {
		s.namespace = namespace
	}
}

func withPrometheusLabels(labels map[string]string) prometheusStatsOption {
	return func(s *prometheusStats) {
		for k, v := range labels {
			s.globalLabels[k] = v
		}
	}
}

func withPrometheusHistogramBuckets(buckets map[string][]float64) prometheusStatsOption {
	return func(s *prometheusStats) {
		for k, v := range buckets {
			s.histogramBuckets[k] = v
		}
	}
}

func withDefaultPrometheusHistogramBuckets(buckets []float64) prometheusStatsOption {
	return func(s *prometheusStats) {
		s.defaultBuckets = buckets
	}
}

func newPrometheusStats(logger *zap.Logger, opts ...prometheusStatsOption) *prometheusStats {
	s := &prometheusStats{
		logger:           logger,
		counters:         make(map[string]*prometheus.CounterVec),
		gauges:           make(map[string]*prometheus.GaugeVec),
		histograms:       make(map[string]*prometheus.HistogramVec),
		histogramBuckets: make(map[string][]float64),
		defaultBuckets:   defaultBuckets,
		globalLabels:     make(prometheus.Labels),
		namespace:        "",
		reg:              prometheus.NewRegistry(),
	}

	// Add go runtime metrics and process collectors.
	s.reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)

	for _, opt := range opts {
		opt(s)
	}

	return s
}

func (c *prometheusStats) Labels() prometheus.Labels {
	return c.globalLabels
}

func (c *prometheusStats) MetricsHandler() http.Handler {
	return promhttp.HandlerFor(c.reg, promhttp.HandlerOpts{Registry: c.reg})
}

func (c *prometheusStats) Count(key string, n interface{}, tags map[string]string) {
	v, err := toFloat64(n)
	if err != nil {
		return
	}

	op, err := c.loadCount(key, tags)
	if err != nil {
		c.logger.Warn("prometheus.count.error", zap.Error(err))
		return
	}
	op.With(labels(tags)).Add(v)
}

func (c *prometheusStats) Inc(key string, tags map[string]string) {
	op, err := c.loadGauge(key, tags)
	if err != nil {
		c.logger.Warn("prometheus.inc.error", zap.Error(err))
		return
	}
	op.With(labels(tags)).Inc()
}

func (c *prometheusStats) Dec(key string, tags map[string]string) {
	op, err := c.loadGauge(key, tags)
	if err != nil {
		c.logger.Warn("prometheus.dec.error", zap.Error(err))
		return
	}
	op.With(labels(tags)).Dec()
}

func (c *prometheusStats) Gauge(key string, n interface{}, tags map[string]string) {
	v, err := toFloat64(n)
	if err != nil {
		return
	}

	op, err := c.loadGauge(key, tags)
	if err != nil {
		c.logger.Warn("prometheus.gauge.error", zap.Error(err))
		return
	}
	op.With(labels(tags)).Set(v)
}

func (c *prometheusStats) Histogram(key string, n interface{}, tags map[string]string) {
	v, err := toFloat64(n)
	if err != nil {
		return
	}

	op, err := c.loadHistogram(key, tags)
	if err != nil {
		c.logger.Warn("prometheus.histogram.error", zap.Error(err))
		return
	}
	op.With(labels(tags)).Observe(v)
}

func (c *prometheusStats) Timing(key string, t time.Duration, tags map[string]string) {
	op, err := c.loadHistogram(key, tags)
	if err != nil {
		c.logger.Warn("prometheus.timing.error", zap.Error(err))
		return
	}

	op.With(labels(tags)).Observe(float64(t) / float64(time.Millisecond))
}

func (c *prometheusStats) loadGauge(key string, tags map[string]string) (*prometheus.GaugeVec, error) {
	key = c.key(key)
	id, labelNames := labelKey(key, tags)

	c.mux.RLock()
	gauge, ok := c.gauges[id]
	c.mux.RUnlock()
	if ok {
		return gauge, nil
	}

	c.mux.Lock()
	gauge, err := registerMetric(c.reg, prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace:   c.namespace,
		Name:        key,
		ConstLabels: c.globalLabels,
	}, labelNames))
	if err != nil {
		c.mux.Unlock()
		return nil, err
	}
	c.gauges[id] = gauge
	c.mux.Unlock()

	return gauge, nil
}

func (c *prometheusStats) loadCount(key string, tags map[string]string) (*prometheus.CounterVec, error) {
	key = c.key(key)
	id, labelNames := labelKey(key, tags)

	c.mux.RLock()
	counter, ok := c.counters[id]
	c.mux.RUnlock()
	if ok {
		return counter, nil
	}

	c.mux.Lock()
	counter, err := registerMetric(c.reg, prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace:   c.namespace,
		Name:        key,
		ConstLabels: c.globalLabels,
	}, labelNames))
	if err != nil {
		c.mux.Unlock()
		return nil, err
	}
	c.counters[id] = counter
	c.mux.Unlock()

	return counter, nil
}

func labelKey(key string, tags map[string]string) (id string, labelNames []string) {
	for k := range labels(tags) {
		labelNames = append(labelNames, k)
	}

	sort.Strings(labelNames)
	newKey := strings.Join(append([]string{key}, labelNames...), ".")

	return newKey, labelNames
}

func (c *prometheusStats) loadHistogram(key string, tags map[string]string) (*prometheus.HistogramVec, error) {
	key = c.key(key)
	id, labelNames := labelKey(key, tags)

	c.mux.RLock()
	histogram, registered := c.histograms[id]
	histogramBuckets, hasBuckets := c.histogramBuckets[key]
	c.mux.RUnlock()

	if registered {
		return histogram, nil
	}

	if !hasBuckets {
		histogramBuckets = defaultBuckets
	}

	c.mux.Lock()
	histogram, err := registerMetric(c.reg, prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace:   c.namespace,
		Name:        key,
		ConstLabels: c.globalLabels,
		Buckets:     histogramBuckets,
	}, labelNames))
	if err != nil {
		c.mux.Unlock()
		return nil, err
	}
	c.histograms[id] = histogram
	c.mux.Unlock()

	return histogram, nil
}

func (c *prometheusStats) key(key string) string {
	return strings.ReplaceAll(key, ".", "_")
}

func labels(tags map[string]string) prometheus.Labels {
	if len(tags) > 0 {
		return prometheus.Labels(tags)
	}
	return prometheus.Labels{}
}

func registerMetric[T prometheus.Collector](
	reg prometheus.Registerer,
	metric T,
) (T, error) {
	var err error
	if reg != nil {
		err = reg.Register(metric)
	} else {
		err = prometheus.Register(metric)
	}
	if err != nil {
		if are, ok := err.(prometheus.AlreadyRegisteredError); ok {
			existing, ok := are.ExistingCollector.(T)
			if !ok {
				return metric, fmt.Errorf("metric with different type already exists")
			}

			return existing, nil
		}
	}

	return metric, err
}

func toFloat64(n interface{}) (float64, error) {
	var v float64
	switch n := n.(type) {
	case float64:
		v = n
	case float32:
		v = float64(n)
	case int:
		v = float64(n)
	case int8:
		v = float64(n)
	case int16:
		v = float64(n)
	case int32:
		v = float64(n)
	case int64:
		v = float64(n)
	default:
		// NaN
		return math.NaN(), errors.New("failed to convert value to float64")
	}
	return v, nil
}
