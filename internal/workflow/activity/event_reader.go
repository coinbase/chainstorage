package activity

import (
	"context"

	"go.temporal.io/sdk/workflow"
	"go.uber.org/fx"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/cadence"
	"github.com/coinbase/chainstorage/internal/storage"
	"github.com/coinbase/chainstorage/internal/storage/metastorage"
	"github.com/coinbase/chainstorage/internal/storage/metastorage/model"
)

type (
	EventReader struct {
		baseActivity
		metaStorage metastorage.MetaStorage
	}

	EventReaderParams struct {
		fx.In
		Runtime     cadence.Runtime
		MetaStorage metastorage.MetaStorage
	}

	EventReaderRequest struct {
		EventTag      uint32
		StartSequence uint64
		EndSequence   uint64
		LatestEvent   bool
	}

	EventReaderResponse struct {
		// Eventdata is set to nil if event does not exist in meta storage
		Eventdata []*model.EventEntry
	}
)

func NewEventReader(params EventReaderParams) *EventReader {
	a := &EventReader{
		baseActivity: newBaseActivity(ActivityEventReader, params.Runtime),
		metaStorage:  params.MetaStorage,
	}
	a.register(a.execute)
	return a
}

func (a *EventReader) Execute(ctx workflow.Context, request *EventReaderRequest) (*EventReaderResponse, error) {
	var response EventReaderResponse
	err := a.executeActivity(ctx, request, &response)
	return &response, err
}

func (a *EventReader) execute(ctx context.Context, request *EventReaderRequest) (*EventReaderResponse, error) {
	if err := a.validateRequest(request); err != nil {
		return nil, err
	}

	eventData, err := a.metaStorage.GetEventsByEventIdRange(ctx, request.EventTag, int64(request.StartSequence), int64(request.EndSequence))
	if err != nil {
		if xerrors.Is(err, storage.ErrItemNotFound) {
			// Convert not-found error into an empty response.
			return &EventReaderResponse{}, nil
		}

		return nil, xerrors.Errorf("failed to read event from meta storage: %w", err)
	}

	return &EventReaderResponse{
		Eventdata: eventData,
	}, nil
}
