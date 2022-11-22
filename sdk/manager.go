package sdk

import (
	"context"

	"go.uber.org/zap"

	"github.com/coinbase/chainstorage/internal/services"
)

type (
	SystemManager = services.SystemManager
	ManagerOption = services.ManagerOption
)

func NewManager(opts ...ManagerOption) SystemManager {
	return services.NewManager(opts...)
}

// WithLogger allows the logger to be injected into the manager.
func WithLogger(logger *zap.Logger) ManagerOption {
	return services.WithLogger(logger)
}

// WithLogSampling will enable sampling on the default logger if you didn't
// provide one yourself. This option will have no effect if used with the
// WithLogger option.
func WithLogSampling(sampling *zap.SamplingConfig) ManagerOption {
	return services.WithLogSampling(sampling)
}

// WithContext allows to set root context instead of
// using context.Background() by default
func WithContext(ctx context.Context) ManagerOption {
	return services.WithContext(ctx)
}
