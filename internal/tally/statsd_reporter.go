package tally

import (
	"context"
	"time"

	smirastatsd "github.com/smira/go-statsd"
	"github.com/uber-go/tally/v4"
	"go.uber.org/fx"
	"go.uber.org/zap"

	"github.com/coinbase/chainstorage/internal/config"
)

type statsDReporter struct {
	client *smirastatsd.Client
}

func newStatsDReporter(
	cfg *config.StatsDConfig,
	lifecycle fx.Lifecycle,
	logger *zap.Logger,
) tally.StatsReporter {
	// hardcoding this to be datadog format
	// we need think about whats the best way to set it up in config such that
	// when we switch reporter impl, config will still be backward compatible
	tagFormat := smirastatsd.TagFormatDatadog

	client := smirastatsd.NewClient(
		cfg.Address,
		smirastatsd.MetricPrefix(cfg.Prefix),
		smirastatsd.TagStyle(tagFormat),
		smirastatsd.ReportInterval(reportingInterval),
	)
	logger.Info("initialized statsd client")
	lifecycle.Append(fx.Hook{
		OnStop: func(ctx context.Context) error {
			return client.Close()
		},
	})

	return &statsDReporter{
		client: client,
	}
}

func (r *statsDReporter) convertTags(tagsMap map[string]string) []smirastatsd.Tag {
	tags := make([]smirastatsd.Tag, 0, len(tagsMap))
	for key, value := range tagsMap {
		tags = append(tags, smirastatsd.StringTag(key, value))
	}
	return tags
}

func (r *statsDReporter) ReportCounter(name string, tags map[string]string, value int64) {
	r.client.Incr(name, value, r.convertTags(tags)...)
}

func (r *statsDReporter) ReportGauge(name string, tags map[string]string, value float64) {
	r.client.FGauge(name, value, r.convertTags(tags)...)
}

func (r *statsDReporter) ReportTimer(name string, tags map[string]string, value time.Duration) {
	r.client.PrecisionTiming(name, value, r.convertTags(tags)...)
}

func (r *statsDReporter) ReportHistogramValueSamples(
	name string,
	tags map[string]string,
	buckets tally.Buckets,
	bucketLowerBound,
	bucketUpperBound float64,
	samples int64,
) {
	panic("no implemented")
}

func (r *statsDReporter) ReportHistogramDurationSamples(
	name string,
	tags map[string]string,
	buckets tally.Buckets,
	bucketLowerBound,
	bucketUpperBound time.Duration,
	samples int64,
) {
	panic("no implemented")
}

func (r *statsDReporter) Capabilities() tally.Capabilities {
	return r
}

func (r *statsDReporter) Reporting() bool {
	return true
}

func (r *statsDReporter) Tagging() bool {
	return true
}

func (r *statsDReporter) Flush() {
	// no-op
}
