package activity

import (
	"container/list"
	"context"
	"time"

	"github.com/uber-go/tally/v4"
	"go.temporal.io/sdk/workflow"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/blockchain/parser"
	"github.com/coinbase/chainstorage/internal/cadence"
	"github.com/coinbase/chainstorage/internal/storage"
	"github.com/coinbase/chainstorage/internal/storage/metastorage"
	"github.com/coinbase/chainstorage/internal/storage/metastorage/model"
	"github.com/coinbase/chainstorage/internal/utils/fxparams"
	"github.com/coinbase/chainstorage/internal/utils/instrument"
	"github.com/coinbase/chainstorage/internal/utils/utils"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

const (
	defaultMaxAllowedReorgHeight        = 100
	streamerBatchGetSize                = 50
	streamerMaxAllowedContinuousFetches = 100
)

var (
	errorChainCompletelyDifferent = xerrors.New("chain constructed with events completely disagrees with block metadata")
)

type (
	Streamer struct {
		baseActivity
		blockStartHeight uint64
		metaStorage      metastorage.MetaStorage
		metrics          *StreamerMetrics
		parallelism      int
	}

	StreamerParams struct {
		fx.In
		fxparams.Params
		Runtime     cadence.Runtime
		MetaStorage metastorage.MetaStorage
	}

	StreamerRequest struct {
		BatchSize             uint64 `validate:"required"`
		Tag                   uint32 `validate:"required"`
		MaxAllowedReorgHeight uint64
		EventTag              uint32
	}

	StreamerResponse struct {
		LatestStreamedHeight uint64
		Gap                  uint64
		EventTag             uint32
		TimeSinceLastBlock   time.Duration
	}

	StreamerMetrics struct {
		instrumentHandleReorg               instrument.InstrumentWithResult[*streamerReorgResult]
		reorgCounter                        tally.Counter
		reorgDistanceExceedThresholdCounter tally.Counter
		reorgDistanceWithinThresholdCounter tally.Counter
		reorgDistanceGauge                  tally.Gauge
	}

	streamerReorgResult struct {
		forkBlock               *api.BlockMetadata
		canonicalChainTipHeight uint64
		updateEvents            []*model.BlockEvent
		eventTag                uint32
	}
)

func NewStreamer(params StreamerParams) *Streamer {
	s := &Streamer{
		baseActivity:     newBaseActivity(ActivityStreamer, params.Runtime),
		blockStartHeight: params.Config.Chain.BlockStartHeight,
		metaStorage:      params.MetaStorage,
		metrics:          newStreamerMetrics(params.Metrics),
		parallelism:      params.Config.Workflows.Poller.Parallelism,
	}
	s.register(s.execute)
	return s
}

func newStreamerMetrics(scope tally.Scope) *StreamerMetrics {
	scope = scope.SubScope(ActivityStreamer)
	return &StreamerMetrics{
		instrumentHandleReorg:               instrument.NewWithResult[*streamerReorgResult](scope, "handle_reorg"),
		reorgCounter:                        scope.Counter("reorg"),
		reorgDistanceExceedThresholdCounter: scope.Tagged(errTags).Counter(reorgDistanceCheckMetric),
		reorgDistanceWithinThresholdCounter: scope.Tagged(successTags).Counter(reorgDistanceCheckMetric),
		reorgDistanceGauge:                  scope.Gauge("reorg_distance"),
	}
}

func (s *Streamer) Execute(ctx workflow.Context, request *StreamerRequest) (*StreamerResponse, error) {
	var response StreamerResponse
	err := s.executeActivity(ctx, request, &response)
	return &response, err
}

func (s *Streamer) execute(ctx context.Context, request *StreamerRequest) (*StreamerResponse, error) {
	if err := s.validateRequest(request); err != nil {
		return nil, err
	}

	tag := request.Tag
	eventTag := request.EventTag
	logger := s.getLogger(ctx).With(zap.Reflect("request", request), zap.Uint32("tag", tag), zap.Uint32("event_tag", eventTag))
	result, err := s.handleReorg(ctx, logger, request)

	var start uint64
	var startParentBlock *api.BlockMetadata
	var metaTipHeight uint64
	if err != nil && xerrors.Is(err, storage.ErrNoEventHistory) {
		start = s.blockStartHeight
		metaLatest, err := s.metaStorage.GetLatestBlock(ctx, tag)
		if err != nil {
			return nil, xerrors.Errorf("failed to get latest block from meta storage: %w", err)
		}
		metaTipHeight = metaLatest.Height
	} else if err == nil {
		reorgDistance := len(result.updateEvents)
		s.metrics.reorgDistanceGauge.Update(float64(reorgDistance))
		if reorgDistance > 0 {
			s.metrics.reorgCounter.Inc(1)
			logger.Info("detected reorg",
				zap.Uint64("canonical_chain_tip_height", result.canonicalChainTipHeight),
				zap.Uint64("fork_block_height", result.forkBlock.Height),
				zap.Int("reorg_distance", reorgDistance))
			err = s.metaStorage.AddEvents(ctx, eventTag, result.updateEvents)
			if err != nil {
				return nil, xerrors.Errorf("failed to add events for reorg: %w", err)
			}
		}
		start = result.forkBlock.Height + 1
		startParentBlock = result.forkBlock
		metaTipHeight = result.canonicalChainTipHeight
	} else {
		return nil, xerrors.Errorf("failed to handle reorg: %w", err)
	}

	end := metaTipHeight + 1
	if end > start+request.BatchSize {
		end = start + request.BatchSize
	}

	var timeSinceLastBlock time.Duration
	if start > end {
		return nil, xerrors.Errorf("run into unexpected start (%d) bigger than end (%d)", start, end)
	} else if start == end {
		// already caught up, no need to do anything
	} else {
		events := make([]*model.BlockEvent, 0, end-start)
		blocks, err := s.metaStorage.GetBlocksByHeightRange(ctx, tag, start, end)
		if err != nil {
			return nil, xerrors.Errorf("failed to get blocks in range [%d, %d): %w", start, end, err)
		}

		if err = parser.ValidateChain(blocks, startParentBlock); err != nil {
			return nil, xerrors.Errorf("metastore returns inconsistent data due to race condition: %w", err)
		}

		for _, block := range blocks {
			event := model.NewBlockEventWithBlockMeta(api.BlockchainEvent_BLOCK_ADDED, block)
			events = append(events, event)
		}

		if err = s.metaStorage.AddEvents(ctx, eventTag, events); err != nil {
			return nil, xerrors.Errorf("failed to add events: %w", err)
		}

		timeSinceLastBlock = utils.SinceTimestamp(blocks[0].GetTimestamp())
		logger.Info(
			"streamer finished",
			zap.Uint64("start", start),
			zap.Uint64("end", end),
			zap.Uint64("meta_tip_height", metaTipHeight),
			zap.Uint32("event_tag", eventTag),
			zap.String("time_since_last_block", timeSinceLastBlock.String()),
		)
	}

	return &StreamerResponse{
		LatestStreamedHeight: end - 1,
		Gap:                  metaTipHeight - start + 1,
		TimeSinceLastBlock:   timeSinceLastBlock,
		EventTag:             eventTag,
	}, nil
}

func (s *Streamer) populateEventsQueue(ctx context.Context, eventTag uint32, minEventIdFetched *int64, eventsToChainAdaptor *metastorage.EventsToChainAdaptor) error {
	maxEventId := *minEventIdFetched
	minEventId := maxEventId - streamerBatchGetSize
	if minEventId < metastorage.EventIdStartValue {
		minEventId = metastorage.EventIdStartValue
	}
	if minEventId == maxEventId {
		return nil
	}
	events, err := s.metaStorage.GetEventsByEventIdRange(ctx, eventTag, minEventId, maxEventId)
	if err != nil {
		return xerrors.Errorf("failed to fetch events from metaStorage (minEventId=%d, maxEventId=%d): %w", minEventId, maxEventId, err)
	}
	err = eventsToChainAdaptor.AppendEvents(events)
	if err != nil {
		return xerrors.Errorf("failed to append events to adaptor: %w", err)
	}
	*minEventIdFetched = events[0].EventId
	return nil
}

func (s *Streamer) getEventForTailBlock(ctx context.Context, eventTag uint32, minEventIdFetched *int64, eventsToChainAdaptor *metastorage.EventsToChainAdaptor) (*model.EventEntry, error) {
	numFetches := 0
	for {
		headEvent, err := eventsToChainAdaptor.PopEventForTailBlock()
		if err != nil {
			if !xerrors.Is(err, storage.ErrNoEventAvailable) {
				return nil, xerrors.Errorf("failed to get event for tail block: %w", err)
			}
		} else {
			return headEvent, nil
		}
		if numFetches >= streamerMaxAllowedContinuousFetches {
			return nil, xerrors.Errorf("still trying to fetch after %d times", streamerMaxAllowedContinuousFetches)
		}
		numFetches += 1
		if *minEventIdFetched <= metastorage.EventIdStartValue {
			return nil, xerrors.Errorf("trying to get more events with event id below %d", metastorage.EventIdStartValue)
		}
		err = s.populateEventsQueue(ctx, eventTag, minEventIdFetched, eventsToChainAdaptor)
		if err != nil {
			return nil, xerrors.Errorf("failed to populate event queue: %w", err)
		}
	}
}

func (s *Streamer) populateBlocksQueue(ctx context.Context, tag uint32, minBlockHeightFetched *uint64, minBlockFetchedParentHash *string, blockList *list.List) error {
	maxHeight := *minBlockHeightFetched
	var minHeight uint64
	if maxHeight >= s.blockStartHeight+streamerBatchGetSize {
		minHeight = maxHeight - streamerBatchGetSize
	} else {
		minHeight = s.blockStartHeight
	}
	if minHeight == maxHeight {
		return nil
	}
	blocks, err := s.metaStorage.GetBlocksByHeightRange(ctx, tag, minHeight, maxHeight)
	if err != nil {
		return xerrors.Errorf("failed to fetch blocks from metaStorage (minHeight=%d, maxHeight=%d): %w", minHeight, maxHeight, err)
	}
	for i := len(blocks) - 1; i >= 0; i-- {
		block := blocks[i]
		*minBlockHeightFetched = block.Height
		if *minBlockFetchedParentHash != "" {
			if !block.Skipped && block.GetHash() != *minBlockFetchedParentHash {
				return xerrors.Errorf(
					"metastore returns inconsistent data due to race condition (expected_hash=%v, current_block={%+v}",
					*minBlockFetchedParentHash, block,
				)
			}
		}
		*minBlockFetchedParentHash = block.GetParentHash()
		blockList.PushBack(block)
	}

	return nil
}

func (s *Streamer) getTailBlock(ctx context.Context, tag uint32, minBlockHeightFetched *uint64, minBlockFetchedParentHash *string, blockList *list.List) (*api.BlockMetadata, error) {
	for {
		headItem := blockList.Front()
		if headItem != nil {
			headBlock, ok := headItem.Value.(*api.BlockMetadata)
			if !ok {
				return nil, xerrors.Errorf("failed to cast %v to *api.BlockMetadata", headItem.Value)
			}
			blockList.Remove(headItem)
			return headBlock, nil
		}

		if *minBlockHeightFetched <= s.blockStartHeight {
			return nil, xerrors.Errorf("trying to get more blocks with height below %d", s.blockStartHeight)
		}
		err := s.populateBlocksQueue(ctx, tag, minBlockHeightFetched, minBlockFetchedParentHash, blockList)
		if err != nil {
			return nil, xerrors.Errorf("failed to populate block queue: %w", err)
		}
	}
}

// algo is described here: https://docs.google.com/document/d/1_wVacdTtoSz-Gr24AADUGmVjLpu7d43KleBEvc8JV5g/edit#heading=h.4d0br9f39hl8
func (s *Streamer) handleReorg(ctx context.Context, logger *zap.Logger, req *StreamerRequest) (*streamerReorgResult, error) {
	eventTag := req.EventTag
	return s.metrics.instrumentHandleReorg.Instrument(ctx, func(ctx context.Context) (*streamerReorgResult, error) {
		maxEventId, err := s.metaStorage.GetMaxEventId(ctx, eventTag)
		if err != nil {
			return nil, xerrors.Errorf("failed to get max event id for eventTag=%d: %w", eventTag, err)
		}
		metaLatest, err := s.metaStorage.GetLatestBlock(ctx, req.Tag)
		if err != nil {
			return nil, xerrors.Errorf("failed to get latest block from meta storage: %w", err)
		}

		logger.Info("checking for chain reorg",
			zap.Uint64("max_block_meta_height", metaLatest.Height),
			zap.Uint32("max_block_meta_tag", metaLatest.Tag),
			zap.Int64("max_event_id", maxEventId),
			zap.Uint32("event_tag", eventTag))

		eventsToChainAdaptor := metastorage.NewEventsToChainAdaptor()
		blocksList := list.New()
		var minBlockHeightFetched *uint64
		var minBlockFetchedParentHash string
		var eventWatermarkHeight *uint64
		minEventIdFetched := maxEventId + 1
		var headEvent *model.EventEntry
		updateEvents := make([]*model.BlockEvent, 0)
		for {
			headEvent, err = s.getEventForTailBlock(ctx, eventTag, &minEventIdFetched, eventsToChainAdaptor)
			if err != nil {
				return nil, xerrors.Errorf("failed to get next event: %w", err)
			}
			if eventWatermarkHeight == nil {
				height := headEvent.BlockHeight
				eventWatermarkHeight = &height
			}
			if headEvent.BlockHeight <= metaLatest.Height {
				// only compare with block meta when event watermark height is no higher than metaLast.Height
				// metaLast.Height could be smaller when meta tip is set to lower value
				if minBlockHeightFetched == nil {
					height := headEvent.BlockHeight + 1
					minBlockHeightFetched = &height
				}
				var headBlock *api.BlockMetadata
				headBlock, err = s.getTailBlock(ctx, req.Tag, minBlockHeightFetched, &minBlockFetchedParentHash, blocksList)
				if err != nil {
					return nil, xerrors.Errorf("failed to get next block meta: %w", err)
				}
				if headEvent.BlockHeight != headBlock.Height {
					return nil, xerrors.Errorf("expect head event and head block to have the same height (headEvent = %v, headBlock=%v)", headEvent.BlockHeight, headBlock.Height)
				}
				if headEvent.BlockHash == headBlock.Hash {
					logger.Info("found fork block",
						zap.Uint64("event_watermark_height", *eventWatermarkHeight),
						zap.Uint64("fork_block_height", headEvent.BlockHeight),
						zap.Int("reorg_distance", len(updateEvents)),
					)

					maxAllowedReorgHeight := req.MaxAllowedReorgHeight
					reorgDistance := len(updateEvents)
					// Check reorg explanation in https://docs.google.com/document/d/18DhoFKh2lt7uJIg57XfiSHAFpnr-f0MVswT8wsZ6qgw/edit?usp=sharing
					// Note: when maxAllowedReorgHeight = 0, it means NO reorg validation in streamer.
					if maxAllowedReorgHeight > 0 && uint64(reorgDistance) >= maxAllowedReorgHeight {
						logger.Error("reorg distance should be less than maxAllowedReorgHeight",
							zap.Int("reorg_distance", reorgDistance),
							zap.Uint64("max_allowed_reorg_height", maxAllowedReorgHeight),
						)
						s.metrics.reorgDistanceExceedThresholdCounter.Inc(1)
					} else {
						s.metrics.reorgDistanceWithinThresholdCounter.Inc(1)
					}

					result := &streamerReorgResult{
						forkBlock:               headBlock,
						canonicalChainTipHeight: metaLatest.Height,
						updateEvents:            updateEvents,
						eventTag:                eventTag,
					}
					return result, nil
				}
				// block hash does not match
				if headEvent.EventId == metastorage.EventIdStartValue || headBlock.GetHeight() == s.blockStartHeight {
					return nil, errorChainCompletelyDifferent
				}
			}
			// detected diff
			newEvent := model.NewBlockEventFromEventEntry(api.BlockchainEvent_BLOCK_REMOVED, headEvent)
			updateEvents = append(updateEvents, newEvent)
		}
	})
}
