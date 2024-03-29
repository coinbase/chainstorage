package activity

import (
	"context"

	"go.temporal.io/sdk/activity"
)

type (
	Heartbeater interface {
		RecordHeartbeat(ctx context.Context, details ...any)
	}

	heartbeaterImpl struct{}

	heartbeaterNop struct{}
)

func NewHeartbeater() Heartbeater {
	return heartbeaterImpl{}
}

func (h heartbeaterImpl) RecordHeartbeat(ctx context.Context, details ...any) {
	activity.RecordHeartbeat(ctx, details...)
}

func NewNopHeartbeater() Heartbeater {
	return heartbeaterNop{}
}

func (h heartbeaterNop) RecordHeartbeat(_ context.Context, _ ...any) {
}
