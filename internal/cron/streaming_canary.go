package cron

import (
	"context"
	"time"

	"go.uber.org/fx"
	"go.uber.org/zap"
	"golang.org/x/xerrors"
	"google.golang.org/grpc/codes"

	"github.com/coinbase/chainstorage/internal/config"

	"github.com/coinbase/chainstorage/internal/gateway"
	"github.com/coinbase/chainstorage/internal/storage"
	"github.com/coinbase/chainstorage/internal/utils/fxparams"
	"github.com/coinbase/chainstorage/internal/utils/log"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

type (
	StreamingCanaryTaskParams struct {
		fx.In
		fxparams.Params
		Client       gateway.Client
		EventStorage storage.EventStorage
		Config       *config.Config
	}

	streamingCanaryTask struct {
		enabled      bool
		client       gateway.Client
		logger       *zap.Logger
		eventStorage storage.EventStorage
		eventTag     uint32
	}
)

const (
	streamingPadding   = 7
	streamingNumEvents = 10
)

func NewStreamingCanary(params StreamingCanaryTaskParams) (Task, error) {
	return &streamingCanaryTask{
		enabled:      !params.Config.Cron.DisableStreamingCanary,
		client:       params.Client,
		logger:       log.WithPackage(params.Logger),
		eventStorage: params.EventStorage,
		eventTag:     params.Config.GetStableEventTag(),
	}, nil
}

func (t *streamingCanaryTask) Name() string {
	return "streaming_canary"
}

func (t *streamingCanaryTask) Spec() string {
	return "@every 2m"
}

func (t *streamingCanaryTask) Parallelism() int64 {
	return 4
}

func (t *streamingCanaryTask) Enabled() bool {
	return t.enabled
}

func (t *streamingCanaryTask) DelayStartDuration() time.Duration {
	// delay for 10 minutes in case there is new API being added
	return 10 * time.Minute
}

func (t *streamingCanaryTask) Run(ctx context.Context) error {
	eventTag := t.eventTag
	maxEventId, err := t.eventStorage.GetMaxEventId(ctx, eventTag)
	if err != nil {
		if xerrors.Is(err, storage.ErrNoEventHistory) {
			t.logger.Info("no event history")
			return nil
		}

		return xerrors.Errorf("failed to get max event id: %w", err)
	}

	if maxEventId < streamingPadding+storage.EventIdStartValue {
		t.logger.Info("insufficient event history")
		return nil
	}

	sequenceNum := maxEventId - streamingPadding
	t.logger.Info(
		"running streaming canary task",
		zap.Reflect("maxEventId", maxEventId),
		zap.Int64("sequenceNum", sequenceNum),
		zap.Uint32("eventTag", eventTag),
	)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	stream, err := t.client.StreamChainEvents(ctx, &api.ChainEventsRequest{
		SequenceNum: sequenceNum,
		EventTag:    eventTag,
	})
	if err != nil {
		if t.isAbortedError(err) {
			return nil
		}

		return xerrors.Errorf("failed to call StreamChainEvents (sequenceNum=%v, eventTag=%v): %w", sequenceNum, eventTag, err)
	}

	for i := 0; i < streamingNumEvents; i++ {
		resp, err := stream.Recv()
		if err != nil {
			if t.isAbortedError(err) {
				return nil
			}

			return xerrors.Errorf("failed to receive from event stream (sequenceNum=%v, i=%v): %w", sequenceNum, i, err)
		}

		event := resp.GetEvent()
		if event == nil {
			return xerrors.Errorf("received null event (sequenceNum=%v, i=%v)", sequenceNum, i)
		}

		t.logger.Debug("received event", zap.Int("i", i), zap.Reflect("event", event))
		// The block file should exist regardless of the event type.
		if _, err := t.client.GetBlockFile(ctx, &api.GetBlockFileRequest{
			Tag:    event.Block.Tag,
			Height: event.Block.Height,
			Hash:   event.Block.Hash,
		}); err != nil {
			return xerrors.Errorf("failed to get block file (sequenceNum=%v, i=%v, event={%+v}): %w", sequenceNum, i, event, err)
		}
	}

	return nil
}

func (t *streamingCanaryTask) isAbortedError(err error) bool {
	// When there is no new block for a long time, the server side returns Aborted and
	// the client side should retry after a little while.
	// Since the SLA alerts are already monitoring the data freshness,
	// this particular error should be ignored by the canary task.
	var grpcerr gateway.GrpcError
	return xerrors.As(err, &grpcerr) && grpcerr.GRPCStatus().Code() == codes.Aborted
}
