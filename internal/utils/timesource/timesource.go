package timesource

import (
	"sync/atomic"
	"time"

	"go.temporal.io/sdk/workflow"
)

type (
	// TimeSource is an interface for any entity that provides the current time.
	TimeSource interface {
		Now() time.Time
	}

	// realTimeSource serves the real wall-clock time.
	realTimeSource struct{}

	// workflowTimeSource serves the replay-aware time.
	workflowTimeSource struct {
		ctx workflow.Context
	}

	// TickingTimeSource serves the auto-incrementing time.
	tickingTimeSource struct {
		now int64
	}

	// EventTimeSource serves the fake controlled time.
	EventTimeSource struct {
		now int64
	}
)

func NewRealTimeSource() TimeSource {
	return &realTimeSource{}
}

func (s *realTimeSource) Now() time.Time {
	return time.Now()
}

func NewWorkflowTimeSource(ctx workflow.Context) TimeSource {
	return &workflowTimeSource{
		ctx: ctx,
	}
}

func (s *workflowTimeSource) Now() time.Time {
	return workflow.Now(s.ctx)
}

func NewTickingTimeSource() TimeSource {
	return &tickingTimeSource{}
}

func (s *tickingTimeSource) Now() time.Time {
	return time.Unix(0, atomic.AddInt64(&s.now, int64(time.Second))).UTC()
}

func NewEventTimeSource() *EventTimeSource {
	return &EventTimeSource{}
}

func (s *EventTimeSource) Now() time.Time {
	return time.Unix(0, atomic.LoadInt64(&s.now)).UTC()
}

func (s *EventTimeSource) Update(now time.Time) *EventTimeSource {
	atomic.StoreInt64(&s.now, now.UnixNano())
	return s
}
