package main

import (
	"go.uber.org/fx"
	"go.uber.org/zap"

	"github.com/coinbase/chainstorage/internal/aws"
	"github.com/coinbase/chainstorage/internal/blockchain"
	"github.com/coinbase/chainstorage/internal/cadence"
	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/dlq"
	"github.com/coinbase/chainstorage/internal/s3"
	"github.com/coinbase/chainstorage/internal/storage"
	"github.com/coinbase/chainstorage/internal/tally"
	"github.com/coinbase/chainstorage/internal/tracer"
	"github.com/coinbase/chainstorage/internal/utils/fxparams"
	"github.com/coinbase/chainstorage/internal/workflow"
	"github.com/coinbase/chainstorage/sdk/services"
)

func main() {
	manager := startManager()
	manager.WaitForInterrupt()
}

func startManager(opts ...fx.Option) services.SystemManager {
	manager := services.NewManager()
	ctx := manager.Context()
	logger := manager.Logger()

	opts = append(
		opts,
		aws.Module,
		blockchain.Module,
		cadence.Module,
		config.Module,
		dlq.Module,
		fxparams.Module,
		s3.Module,
		storage.Module,
		tally.Module,
		tracer.Module,
		workflow.Module,
		fx.NopLogger,
		fx.Provide(func() services.SystemManager { return manager }),
		fx.Provide(func() *zap.Logger { return logger }),
		fx.Invoke(workflow.NewManager),
	)

	app := fx.New(opts...)

	if err := app.Start(ctx); err != nil {
		logger.Fatal("failed to start app", zap.Error(err))
	}
	manager.AddPreShutdownHook(func() {
		logger.Info("shutting down worker")
		if err := app.Stop(ctx); err != nil {
			logger.Error("failed to stop app", zap.Error(err))
		}
	})

	logger.Info("started worker")
	return manager
}
