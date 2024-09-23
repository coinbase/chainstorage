package workflow

import (
	"container/list"
	"context"
	"strconv"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/workflow"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/cadence"
	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/storage/metastorage/model"
	"github.com/coinbase/chainstorage/internal/utils/fxparams"
	"github.com/coinbase/chainstorage/internal/workflow/activity"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

type (
	EventBackfiller struct {
		baseWorkflow
		eventReader     *activity.EventReader
		eventReconciler *activity.EventReconciler
		eventLoader     *activity.EventLoader
	}

	EventBackfillerParams struct {
		fx.In
		fxparams.Params
		Runtime         cadence.Runtime
		EventReader     *activity.EventReader
		EventReconciler *activity.EventReconciler
		EventLoader     *activity.EventLoader
	}

	EventBackfillerRequest struct {
		Tag                 uint32
		EventTag            uint32
		UpgradeFromEventTag uint32
		StartSequence       uint64 `validate:"gt=0"`
		EndSequence         uint64 `validate:"gt=0,gtfield=StartSequence"`
		BatchSize           uint64 // Optional. If not specified, it is read from the workflow config.
		CheckpointSize      uint64 // Optional. If not specified, it is read from the workflow config.
	}
)

var (
	_ InstrumentedRequest = (*EventBackfillerRequest)(nil)
)

const (
	// event backfiller metrics. need to have `workflow.event_backfiller` as prefix
	eventBackfillerHeightGauge = "workflow.event_backfiller.height"
)

func NewEventBackfiller(params EventBackfillerParams) *EventBackfiller {
	w := &EventBackfiller{
		baseWorkflow:    newBaseWorkflow(&params.Config.Workflows.EventBackfiller, params.Runtime),
		eventReader:     params.EventReader,
		eventReconciler: params.EventReconciler,
		eventLoader:     params.EventLoader,
	}
	w.registerWorkflow(w.execute)
	return w
}

func (w *EventBackfiller) Execute(ctx context.Context, request *EventBackfillerRequest) (client.WorkflowRun, error) {
	return w.startWorkflow(ctx, w.name, request)
}

func (w *EventBackfiller) execute(ctx workflow.Context, request *EventBackfillerRequest) error {
	return w.executeWorkflow(ctx, request, func() error {
		if err := w.validateRequest(request); err != nil {
			return err
		}
		ctx = w.withActivityOptions(ctx)

		var cfg config.EventBackfillerWorkflowConfig
		if err := w.readConfig(ctx, &cfg); err != nil {
			return xerrors.Errorf("failed to read config: %w", err)
		}

		batchSize := cfg.BatchSize
		if request.BatchSize > 0 {
			batchSize = request.BatchSize
		}

		checkpointSize := cfg.CheckpointSize
		if request.CheckpointSize > 0 {
			checkpointSize = request.CheckpointSize
		}

		tag := cfg.GetEffectiveBlockTag(request.Tag)
		eventTag := cfg.GetEffectiveEventTag(request.EventTag)
		metrics := w.getMetricsHandler(ctx).WithTags(map[string]string{
			tagBlockTag: strconv.Itoa(int(tag)),
			tagEventTag: strconv.Itoa(int(eventTag)),
		})
		logger := w.getLogger(ctx).With(
			zap.Reflect("request", request),
			zap.Reflect("config", cfg),
		)

		lastEvent, err := w.readLastEvent(ctx, eventTag, request.StartSequence)
		if err != nil {
			logger.Error("failed to read last event", zap.Error(err))
			return xerrors.Errorf("failed to read last event: %w", err)
		}
		logger.Info("last event", zap.Reflect("event data", lastEvent))

		logger.Info("workflow started",
			zap.Uint64("startSequence", request.StartSequence),
			zap.Uint64("endSequence", request.EndSequence))

		for batchStart := request.StartSequence; batchStart < request.EndSequence; batchStart += batchSize {
			if batchStart-request.StartSequence >= checkpointSize {
				newRequest := *request
				newRequest.StartSequence = batchStart
				logger.Info(
					"checkpoint reached",
					zap.Reflect("newRequest", newRequest))
				return w.continueAsNew(ctx, request)
			}

			batchEnd := batchStart + batchSize
			if batchEnd > request.EndSequence {
				batchEnd = request.EndSequence
			}

			processedBatch, err := w.processBatch(
				ctx,
				tag,
				eventTag,
				request.UpgradeFromEventTag,
				batchStart,
				batchEnd,
				lastEvent,
			)
			if err != nil {
				logger.Error(
					"failed to process batch",
					zap.Uint64("batchStart", batchStart),
					zap.Uint64("batchEnd", batchEnd),
					zap.Error(err),
				)
				return xerrors.Errorf("failed to process batch [%v, %v): %w", batchStart, batchEnd, err)
			}
			lastEvent = processedBatch[len(processedBatch)-1]

			metrics.Gauge(eventBackfillerHeightGauge).Update(float64(batchEnd - 1))
			logger.Info(
				"processed batch",
				zap.Uint64("batchStart", batchStart),
				zap.Uint64("batchEnd", batchEnd),
			)
		}

		logger.Info("workflow finished",
			zap.Uint64("startSequence", request.StartSequence),
			zap.Uint64("endSequence", request.EndSequence))

		return nil
	})
}

func (w *EventBackfiller) processBatch(
	ctx workflow.Context,
	tag uint32,
	eventTag uint32,
	upgradeFromEventTag uint32,
	batchStart uint64,
	batchEnd uint64,
	lastEvent *model.EventEntry) ([]*model.EventEntry, error) {
	readerRequest := &activity.EventReaderRequest{
		EventTag:      upgradeFromEventTag,
		StartSequence: batchStart,
		EndSequence:   batchEnd,
	}
	eventReaderResponse, err := w.eventReader.Execute(ctx, readerRequest)
	if err != nil || eventReaderResponse.Eventdata == nil || len(eventReaderResponse.Eventdata) == 0 {
		return nil, xerrors.Errorf("failed to read events with range[startSequence=%v, endSequence=%v) and eventTag=%v: %w", batchStart, batchEnd, upgradeFromEventTag, err)
	}
	if int(batchEnd-batchStart) != len(eventReaderResponse.Eventdata) {
		return nil, xerrors.Errorf("unmatched length=%v of events data with batch range[startSequence=%v, endSequence=%v) and eventTag=%v", len(eventReaderResponse.Eventdata), batchStart, batchEnd, upgradeFromEventTag)
	}

	reconcilerRequest := &activity.EventReconcilerRequest{
		Tag:                 tag,
		EventTag:            eventTag,
		UpgradeFromEventTag: upgradeFromEventTag,
		UpgradeFromEvents:   eventReaderResponse.Eventdata,
	}
	reconcilerResponse, err := w.eventReconciler.Execute(ctx, reconcilerRequest)
	if err != nil {
		return nil, xerrors.Errorf("failed to execute event reconciler (request=%+v): %w", reconcilerRequest, err)
	}

	eventDDBEntries := reconcilerResponse.Eventdata
	if err := validateEventBlocks(eventDDBEntries, lastEvent); err != nil {
		return nil, err
	}

	loaderRequest := &activity.EventLoaderRequest{
		EventTag: eventTag,
		Events:   eventDDBEntries,
	}
	if _, err := w.eventLoader.Execute(ctx, loaderRequest); err != nil {
		return nil, xerrors.Errorf("failed to execute event loader (request=%+v): %w", loaderRequest, err)
	}

	return eventDDBEntries, nil
}

func (w *EventBackfiller) readLastEvent(ctx workflow.Context, eventTag uint32, sequence uint64) (*model.EventEntry, error) {
	if sequence == 1 {
		return nil, nil
	}

	readerRequest := &activity.EventReaderRequest{
		EventTag:      eventTag,
		StartSequence: sequence - 1,
		EndSequence:   sequence,
	}
	eventReaderResponse, err := w.eventReader.Execute(ctx, readerRequest)
	if err != nil || eventReaderResponse.Eventdata == nil || len(eventReaderResponse.Eventdata) == 0 {
		return nil, xerrors.Errorf("failed to read events with range[startSequence=%v, endSequence=%v) and eventTag=%v: %w", sequence-1, sequence, eventTag, err)
	}

	return eventReaderResponse.Eventdata[0], nil
}

// ValidateEventBlocks checks if the block data associated with sequence of events is continuous.
// 1. We will use a stack to populate the continuous event(block) sequence
// 2. This validation does not cover the events that end from previous batch to begin of current batch and only checks lastEvent + current batch
func validateEventBlocks(events []*model.EventEntry, lastEvent *model.EventEntry) error {
	eventsValidationStack := list.New()
	if lastEvent != nil && lastEvent.EventType != api.BlockchainEvent_BLOCK_REMOVED {
		eventsValidationStack.PushBack(lastEvent)
	}

	for _, event := range events {
		stackSize := eventsValidationStack.Len()
		if stackSize == 0 {
			if event.EventType == api.BlockchainEvent_BLOCK_ADDED {
				eventsValidationStack.PushBack(event)
			}
			continue
		}

		prevItem := eventsValidationStack.Back()
		prevEvent, ok := model.CastItemToEventEntry(prevItem.Value)
		if !ok {
			return xerrors.Errorf("failed to cast {%+v} to *model.EventDDBEntry", prevItem.Value)
		}

		if event.EventType == api.BlockchainEvent_BLOCK_ADDED {
			// Skip the check if last event is unavailable, skipped OR parent hash is unavailable.
			if prevEvent != nil && !prevEvent.BlockSkipped && prevEvent.ParentHash != "" {
				if prevEvent.BlockHeight+1 != event.BlockHeight || prevEvent.BlockHash != event.ParentHash {
					return xerrors.Errorf("chain with the event sequence is not continuous (last={%+v}, curr={%+v})", prevEvent, event)
				}
			}
			eventsValidationStack.PushBack(event)
		} else {
			if prevEvent.BlockHeight != event.BlockHeight || prevEvent.BlockHash != event.BlockHash || prevEvent.Tag != event.Tag || prevEvent.BlockSkipped != event.BlockSkipped {
				return xerrors.Errorf("reorg events (BLOCK_ADDED={%+v}, BLOCK_REMOVED={%+v}) are not matching", prevEvent, event)
			}
			eventsValidationStack.Remove(eventsValidationStack.Back())
		}
	}

	return nil
}

func (w EventBackfillerRequest) GetTags() map[string]string {
	return map[string]string{
		tagBlockTag: strconv.Itoa(int(w.Tag)),
		tagEventTag: strconv.Itoa(int(w.EventTag)),
	}
}
