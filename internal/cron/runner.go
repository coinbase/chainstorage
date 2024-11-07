package cron

import (
	"context"
	"fmt"
	"time"

	"github.com/robfig/cron/v3"
	"go.uber.org/fx"
	"go.uber.org/zap"

	"github.com/coinbase/chainstorage/internal/utils/fxparams"
	"github.com/coinbase/chainstorage/internal/utils/log"
	"github.com/coinbase/chainstorage/sdk/services"
)

type (
	RunnerParams struct {
		fx.In
		fxparams.Params
		Lifecycle fx.Lifecycle
		Manager   services.SystemManager
		Tasks     []Task `group:"task"`
	}
)

const (
	subScope    = "cron"
	stopTimeout = time.Second * 5
)

func RegisterRunner(params RunnerParams) error {
	logger := log.WithPackage(params.Logger)
	metrics := params.Metrics.SubScope(subScope)

	c := cron.New()
	jobCtx, cancel := context.WithCancel(params.Manager.ServiceContext())

	params.Lifecycle.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			logger.Info("starting cron", zap.Int("num_jobs", len(params.Tasks)))
			c.Start()
			return nil
		},
		OnStop: func(ctx context.Context) error {
			logger.Info("stopping cron")
			timer := time.After(stopTimeout)
			cancel()
			ctx = c.Stop()
			select {
			case <-ctx.Done():
				logger.Info("stopped cron")
			case <-timer:
				logger.Error("timed out while stopping cron")
			}
			return nil
		},
	})

	for _, task := range params.Tasks {
		taskName := task.Name()
		if !task.Enabled() {
			logger.Warn("task is disabled", zap.String("task", taskName))
			continue
		}

		job, err := NewJob(jobCtx, params.Config, logger, metrics, task)
		if err != nil {
			return fmt.Errorf("failed to create job %v: %w", taskName, err)
		}

		if _, err := c.AddJob(task.Spec(), job); err != nil {
			return fmt.Errorf("failed to add job %v: %w", taskName, err)
		}
	}

	return nil
}
