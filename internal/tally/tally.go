package tally

import (
	"context"
	"time"

	"github.com/uber-go/tally"
	"go.uber.org/fx"

	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/utils/consts"
)

type (
	MetricParams struct {
		fx.In
		Lifecycle fx.Lifecycle
		Config    *config.Config
		Reporter  tally.StatsReporter `optional:"true"`
	}
)

const (
	reportingInterval = time.Second
)

func NewRootScope(params MetricParams) tally.Scope {
	// XXX: Inject your own reporter here.
	reporter := params.Reporter
	if reporter == nil {
		reporter = tally.NullStatsReporter
	}

	opts := tally.ScopeOptions{
		Prefix:   consts.ServiceName,
		Reporter: reporter,
		Tags:     params.Config.GetCommonTags(),
	}
	scope, closer := tally.NewRootScope(opts, reportingInterval)
	params.Lifecycle.Append(fx.Hook{
		OnStop: func(ctx context.Context) error {
			return closer.Close()
		},
	})

	return scope
}
