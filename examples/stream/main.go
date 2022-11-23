package main

import (
	"context"
	"time"

	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/protos/coinbase/c3/common"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
	"github.com/coinbase/chainstorage/sdk"
)

const (
	blockchain    = common.Blockchain_BLOCKCHAIN_POLYGON
	network       = common.Network_NETWORK_POLYGON_MAINNET
	batchSize     = 20
	startDistance = 100
)

type (
	Worker struct {
		manager              sdk.SystemManager
		logger               *zap.Logger
		session              sdk.Session
		irreversibleDistance uint64 // N
		backoffInterval      time.Duration
		checkpoint           int64
		numTransactions      int
		numBatches           int
		blocks               []*api.NativeBlock // Last N blocks in the canonical chain.
	}
)

func main() {
	manager := sdk.NewManager()
	defer manager.Shutdown()

	worker, err := NewWorker(manager)
	if err != nil {
		panic(err)
	}

	if err := worker.Run(); err != nil {
		panic(err)
	}
}

func NewWorker(manager sdk.SystemManager) (*Worker, error) {
	logger := manager.Logger()
	session, err := sdk.New(manager, &sdk.Config{
		Blockchain: blockchain,
		Network:    network,
		Env:        sdk.EnvProduction,
	})
	if err != nil {
		return nil, xerrors.Errorf("failed to create session: %w", err)
	}

	ctx := manager.ServiceContext()
	client := session.Client()

	chainMetadata, err := client.GetChainMetadata(ctx, &api.GetChainMetadataRequest{})
	if err != nil {
		return nil, xerrors.Errorf("failed to get chain metadata: %w", err)
	}

	irreversibleDistance := chainMetadata.IrreversibleDistance
	backoffInterval, err := time.ParseDuration(chainMetadata.BlockTime)
	if err != nil {
		return nil, xerrors.Errorf("failed to parse block time: %w", err)
	}

	events, err := client.GetChainEvents(ctx, &api.GetChainEventsRequest{
		InitialPositionInStream: sdk.InitialPositionLatest,
		MaxNumEvents:            1,
	})
	if err != nil {
		return nil, xerrors.Errorf("failed to get chain events: %w", err)
	}

	if len(events) != 1 {
		return nil, xerrors.Errorf("got unexpected number of events: %v", len(events))
	}

	checkpoint := events[0].SequenceNum - startDistance
	blocks := make([]*api.NativeBlock, 0, irreversibleDistance)

	return &Worker{
		manager:              manager,
		logger:               logger,
		session:              session,
		irreversibleDistance: irreversibleDistance,
		backoffInterval:      backoffInterval,
		checkpoint:           checkpoint,
		numTransactions:      0,
		numBatches:           0,
		blocks:               blocks,
	}, nil
}

func (w *Worker) Run() error {
	ctx := w.manager.ServiceContext()

	w.logger.Info(
		"running worker",
		zap.Int64("checkpoint", w.checkpoint),
	)

	for {
		sequence := w.checkpoint
		nextSequence, err := w.processBatch(ctx, sequence, batchSize)
		if err != nil {
			return xerrors.Errorf("failed to process batch [%v, %v): %w", sequence, sequence+batchSize, err)
		}

		// Checkpoint is typically persisted in the database.
		// In this example, we simply store the checkpoint in memory.
		w.checkpoint = nextSequence
	}
}

func (w *Worker) processBatch(ctx context.Context, sequence int64, batchSize uint64) (int64, error) {
	events, err := w.session.Client().GetChainEvents(ctx, &api.GetChainEventsRequest{
		SequenceNum:  sequence,
		MaxNumEvents: batchSize,
	})
	if err != nil {
		return 0, xerrors.Errorf("failed to get chain events: %w", err)
	}

	if len(events) == 0 {
		// Note that the above API is non-blocking, and it returns an empty slice when no event is available.
		w.logger.Info(
			"waiting for new events",
			zap.Int64("sequence", sequence),
			zap.String("backoffInterval", w.backoffInterval.String()),
		)
		time.Sleep(w.backoffInterval)
		return sequence, nil
	}

	w.logger.Info(
		"processing batch",
		zap.Int64("sequence", sequence),
		zap.Uint64("batchSize", batchSize),
	)

	blocks := make([]*api.NativeBlock, len(events))
	group, ctx := errgroup.WithContext(ctx)
	for i := range events {
		i := i
		group.Go(func() error {
			event := events[i]
			blockID := event.Block
			if blockID.Skipped {
				return nil
			}

			block, err := w.session.Client().GetBlockWithTag(ctx, blockID.Tag, blockID.Height, blockID.Hash)
			if err != nil {
				return xerrors.Errorf("failed to get block {%+v}: %w", blockID, err)
			}

			nativeBlock, err := w.session.Parser().ParseNativeBlock(ctx, block)
			if err != nil {
				return xerrors.Errorf("failed to parse block {%+v}: %w", blockID, err)
			}

			blocks[i] = nativeBlock
			return nil
		})
	}

	if err := group.Wait(); err != nil {
		return 0, xerrors.Errorf("failed to get blocks: %w", err)
	}

	w.numBatches += 1
	for i, event := range events {
		if event.Block.Skipped {
			continue
		}

		block := blocks[i]
		if event.Type == api.BlockchainEvent_BLOCK_ADDED {
			w.blocks = append(w.blocks, block)
			w.numTransactions += len(block.GetEthereum().Transactions)
		} else if event.Type == api.BlockchainEvent_BLOCK_REMOVED {
			if len(w.blocks) > 0 {
				// ChainStorage guarantees the ordering of the events.
				// +1, +2, +3, -3, -2, +2', +3'
				lastBlock := w.blocks[len(w.blocks)-1]
				if block.Height != lastBlock.Height {
					return 0, xerrors.Errorf("invalid chain: block=%v, lastBlock=%v", block.Height, lastBlock.Height)
				}

				// Remove the orphaned block.
				w.blocks = w.blocks[:len(w.blocks)-1]
			}

			w.logger.Info(
				"removing orphaned block",
				zap.Reflect("height", block.Height),
				zap.Reflect("hash", block.Hash),
			)
			w.numTransactions -= len(block.GetEthereum().Transactions)
		} else {
			return 0, xerrors.Errorf("unknown event type: %v", event.Type)
		}
	}

	lastEvent := events[len(events)-1]
	nextSequence := lastEvent.SequenceNum
	if n := int(w.irreversibleDistance); len(w.blocks) > n {
		// Keep last n blocks only.
		w.blocks = w.blocks[len(w.blocks)-n:]
	}

	var lastBlock *api.NativeBlock
	if len(w.blocks) > 0 {
		lastBlock = w.blocks[len(w.blocks)-1]
	}

	w.logger.Info(
		"finished batch",
		zap.Int64("sequence", sequence),
		zap.Int64("nextSequence", nextSequence),
		zap.Int("numBatches", w.numBatches),
		zap.Int("numTransactions", w.numTransactions),
		zap.Int("numBlocks", len(w.blocks)),
		zap.Uint64("lastBlock", lastBlock.Height),
	)
	return nextSequence, nil
}
