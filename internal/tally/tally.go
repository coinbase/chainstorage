package tally

import (
	"context"
	"time"

	"github.com/uber-go/tally/v4"
	"go.uber.org/fx"

	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/utils/consts"
)

const (
	reportingInterval = time.Second
)

type (
	MetricParams struct {
		fx.In
		Lifecycle fx.Lifecycle
		Config    *config.Config
		Reporter  tally.StatsReporter
	}
)

func NewRootScope(params MetricParams) tally.Scope {
	opts := tally.ScopeOptions{
		Prefix:   consts.ServiceName,
		Reporter: params.Reporter,
		Tags:     params.Config.GetCommonTags(),
	}
	// report interval will be set on reporter
	scope, closer := tally.NewRootScope(opts, reportingInterval)
	params.Lifecycle.Append(fx.Hook{
		OnStop: func(ctx context.Context) error {
			return closer.Close()
		},
	})

	return scope
}
