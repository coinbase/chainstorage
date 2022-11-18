package cron

import (
	"context"
	"time"
)

type (
	Task interface {
		Name() string
		Spec() string
		Parallelism() int64
		DelayStartDuration() time.Duration
		Run(ctx context.Context) error
		Enabled() bool
	}
)
