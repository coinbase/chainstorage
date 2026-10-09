package postgres

import (
	"context"

	"go.uber.org/fx"
	"go.uber.org/zap"
)

var Module = fx.Options(
	fx.Provide(fx.Annotated{
		Name:   "metastorage/postgres",
		Target: NewFactory,
	}),
	fx.Invoke(registerClosePoolsHook),
)

func registerClosePoolsHook(lc fx.Lifecycle, logger *zap.Logger) {
	lc.Append(fx.Hook{
		OnStart: func(context.Context) error { return nil },
		OnStop: func(ctx context.Context) error {
			if err := CloseAllConnectionPools(); err != nil {
				logger.Error("failed to close PostgreSQL connection pools", zap.Error(err))
				return err
			}
			logger.Info("PostgreSQL connection pools closed successfully")
			return nil
		},
	})
}
