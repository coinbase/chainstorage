package cron

import (
	"go.uber.org/fx"
)

var Module = fx.Options(
	fx.Provide(fx.Annotated{
		Group:  "task",
		Target: NewDLQProcessor,
	}),
	fx.Provide(fx.Annotated{
		Group:  "task",
		Target: NewPollingCanary,
	}),
	fx.Provide(fx.Annotated{
		Group:  "task",
		Target: NewStreamingCanary,
	}),
	fx.Provide(fx.Annotated{
		Group:  "task",
		Target: NewNodeCanary,
	}),
	fx.Provide(fx.Annotated{
		Group:  "task",
		Target: NewWorkflowStatus,
	}),
)
