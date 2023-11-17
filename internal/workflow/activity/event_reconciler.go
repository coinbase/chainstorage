package activity

import (
	"context"

	"go.temporal.io/sdk/workflow"
	"go.uber.org/fx"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/cadence"
	"github.com/coinbase/chainstorage/internal/storage/metastorage"
	"github.com/coinbase/chainstorage/internal/storage/metastorage/model"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

type (
	EventReconciler struct {
		baseActivity
		metaStorage metastorage.MetaStorage
	}

	EventReconcilerParams struct {
		fx.In
		Runtime     cadence.Runtime
		MetaStorage metastorage.MetaStorage
	}

	EventReconcilerRequest struct {
		Tag                 uint32 `validate:"required"`
		EventTag            uint32 `validate:"required"`
		UpgradeFromEventTag uint32
		UpgradeFromEvents   []*model.EventEntry
	}

	EventReconcilerResponse struct {
		Eventdata []*model.EventEntry
	}
)

func NewEventReconciler(params EventReconcilerParams) *EventReconciler {
	a := &EventReconciler{
		baseActivity: newBaseActivity(ActivityEventReconciler, params.Runtime),
		metaStorage:  params.MetaStorage,
	}
	a.register(a.execute)
	return a
}

func (a *EventReconciler) Execute(ctx workflow.Context, request *EventReconcilerRequest) (*EventReconcilerResponse, error) {
	var response EventReconcilerResponse
	err := a.executeActivity(ctx, request, &response)
	return &response, err
}

func (a *EventReconciler) execute(ctx context.Context, request *EventReconcilerRequest) (*EventReconcilerResponse, error) {
	if err := a.validateRequest(request); err != nil {
		return nil, err
	}

	events := request.UpgradeFromEvents
	newBlockTag, newEventTag := request.Tag, request.EventTag
	for _, event := range events {
		event.EventTag = newEventTag
		// 1. Check if existing old event is an orphaned event(block), we will mark it as a skipped event
		//
		// Note: we are also checking on BLOCK_REMOVED events here as well since there's couple corner cases that
		// we could have false positive reorg events, e.g. +A, +B, +C, -C, -B, +B, +C, so this change will make
		// the first +B, +C, -C, -B have exact same data(block tag) and does not impact on the data correctness.
		// See this for more details: https://docs.google.com/document/d/1X7Nfld9AsW_fuKmPoz0Siz46ZNBGHsfXD3FWxqNfE4w/edit?usp=sharing
		blockMetadata, err := a.readBlock(ctx, newBlockTag, event.BlockHeight)
		if err != nil {
			return nil, err
		}
		if blockMetadata.Hash != event.BlockHash {
			// 2. If block hashes are not equal, mark event as BlockSkipped since it's not on the canonical chain
			event.BlockSkipped = true
		} else {
			// 3. Bump the event block tag to new block tag and set BlockSkipped to false after confirming it's on canonical chain.
			event.Tag = newBlockTag
			event.BlockSkipped = false
		}
	}

	return &EventReconcilerResponse{
		Eventdata: events,
	}, nil
}

func (a *EventReconciler) readBlock(ctx context.Context, tag uint32, height uint64) (*api.BlockMetadata, error) {
	blockMetadata, err := a.metaStorage.GetBlockByHeight(ctx, tag, height)
	if err != nil {
		return nil, xerrors.Errorf("failed to get block(tag=%v) by height=%v: %w", tag, height, err)
	}

	return blockMetadata, nil
}
