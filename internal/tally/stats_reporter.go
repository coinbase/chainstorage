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

type (
	StatsReporterParams struct {
		fx.In
		Lifecycle fx.Lifecycle
		Logger    *zap.Logger
		Config    *config.Config
	}

	reporter struct {
		client *smirastatsd.Client
	}
)

const (
	reportingInterval = time.Second
)

var (
	// hardcoding this to be datadog format
	// we need think about whats the best way to set it up in config such that
	// when we switch reporter impl, config will still be backward compatible
	tagFormat = smirastatsd.TagFormatDatadog
)

func NewStatsReporter(params StatsReporterParams) tally.StatsReporter {
	if params.Config.StatsD == nil {
		return tally.NullStatsReporter
	}
	cfg := params.Config.StatsD
	client := smirastatsd.NewClient(
		cfg.Address,
		smirastatsd.MetricPrefix(cfg.Prefix),
		smirastatsd.TagStyle(tagFormat),
		smirastatsd.ReportInterval(reportingInterval),
	)
	params.Logger.Info("initialized statsd client")
	params.Lifecycle.Append(fx.Hook{
		OnStop: func(ctx context.Context) error {
			return client.Close()
		},
	})
	return &reporter{
		client: client,
	}
}

func convertTags(tagsMap map[string]string) []smirastatsd.Tag {
	tags := make([]smirastatsd.Tag, 0, len(tagsMap))
	for key, value := range tagsMap {
		tags = append(tags, smirastatsd.StringTag(key, value))
	}
	return tags
}

func (r *reporter) ReportCounter(name string, tags map[string]string, value int64) {
	r.client.Incr(name, value, convertTags(tags)...)
}

func (r *reporter) ReportGauge(name string, tags map[string]string, value float64) {
	r.client.FGauge(name, value, convertTags(tags)...)
}

func (r *reporter) ReportTimer(name string, tags map[string]string, value time.Duration) {
	r.client.PrecisionTiming(name, value, convertTags(tags)...)
}

func (r *reporter) ReportHistogramValueSamples(
	name string,
	tags map[string]string,
	buckets tally.Buckets,
	bucketLowerBound,
	bucketUpperBound float64,
	samples int64) {
	panic("no implemented")
}

func (r *reporter) ReportHistogramDurationSamples(
	name string,
	tags map[string]string,
	buckets tally.Buckets,
	bucketLowerBound,
	bucketUpperBound time.Duration,
	samples int64) {
	panic("no implemented")
}

func (r *reporter) Capabilities() tally.Capabilities {
	return r
}

func (r *reporter) Reporting() bool {
	return true
}

func (r *reporter) Tagging() bool {
	return true
}

func (r *reporter) Flush() {
	// no-op
}
