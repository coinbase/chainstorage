package activity

import (
	"context"
	"fmt"

	"go.temporal.io/sdk/workflow"
	"go.uber.org/fx"
	"go.uber.org/zap"

	"github.com/coinbase/chainstorage/internal/cadence"
	"github.com/coinbase/chainstorage/internal/storage/metastorage"
	"github.com/coinbase/chainstorage/internal/storage/metastorage/model"
)

type (
	EventLoader struct {
		baseActivity
		metaStorage metastorage.MetaStorage
	}

	EventLoaderParams struct {
		fx.In
		Runtime     cadence.Runtime
		MetaStorage metastorage.MetaStorage
	}

	EventLoaderRequest struct {
		EventTag uint32 `validate:"required"`
		Events   []*model.EventEntry
	}

	EventLoaderResponse struct {
	}
)

func NewEventLoader(params EventLoaderParams) *EventLoader {
	a := &EventLoader{
		baseActivity: newBaseActivity(ActivityEventLoader, params.Runtime),
		metaStorage:  params.MetaStorage,
	}
	a.register(a.execute)
	return a
}

func (a *EventLoader) Execute(ctx workflow.Context, request *EventLoaderRequest) (*EventLoaderResponse, error) {
	var response EventLoaderResponse
	err := a.executeActivity(ctx, request, &response)
	return &response, err
}

func (a *EventLoader) execute(ctx context.Context, request *EventLoaderRequest) (*EventLoaderResponse, error) {
	if err := a.validateRequest(request); err != nil {
		return nil, err
	}

	logger := a.getLogger(ctx)

	if err := a.metaStorage.AddEventEntries(ctx, request.EventTag, request.Events); err != nil {
		return nil, fmt.Errorf("failed to add events to metaStorage: %w", err)
	}

	logger.Info(
		"added events into storage",
		zap.Int("num_events", len(request.Events)),
	)
	return &EventLoaderResponse{}, nil
}
