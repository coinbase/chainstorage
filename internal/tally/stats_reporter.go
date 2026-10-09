package tally

import (
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
)

func NewStatsReporter(params StatsReporterParams) tally.StatsReporter {
	switch {
	case params.Config.StatsD != nil:
		return newStatsDReporter(params.Config.StatsD, params.Lifecycle, params.Logger)
	case params.Config.Prometheus != nil:
		return newPrometheusReporter(params.Config.Prometheus, params.Lifecycle, params.Logger)
	default:
		return tally.NullStatsReporter
	}
}
