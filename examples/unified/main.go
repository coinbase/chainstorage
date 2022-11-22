package main

import (
	"context"
	"sort"
	"sync"
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
	// KeyValueStore is basic in-memory implementation of time-based versioning.
	// The mono-increasing sequence number is used as the virtual clock.
	// Ref: https://aws.amazon.com/blogs/database/implementing-version-control-using-amazon-dynamodb/
	KeyValueStore struct {
		lock           sync.Mutex
		checkpoint     int64
		blocksByHeight map[uint64][]*Block
	}

	Block struct {
		Event *api.BlockchainEvent
		Block *api.NativeBlock
	}

	Worker struct {
		manager         sdk.SystemManager
		logger          *zap.Logger
		session         sdk.Session
		backoffInterval time.Duration
		numBatches      int
		store           *KeyValueStore
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

func NewKeyValueStore(checkpoint int64) *KeyValueStore {
	return &KeyValueStore{
		lock:           sync.Mutex{},
		checkpoint:     checkpoint,
		blocksByHeight: make(map[uint64][]*Block),
	}
}

func (s *KeyValueStore) GetCheckpoint() int64 {
	s.lock.Lock()
	defer s.lock.Unlock()

	return s.checkpoint
}

func (s *KeyValueStore) SetCheckpoint(checkpoint int64) {
	s.lock.Lock()
	defer s.lock.Unlock()

	s.checkpoint = checkpoint
}

func (s *KeyValueStore) GetBlock(height uint64) (*Block, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	// This is equivalent to the following DynamoDB query:
	// table.query(
	//   # Partition key is the block height.
	//   KeyConditionExpression = Key('PK').eq(height) & Key('SK').le(checkpoint),
	//   # Strongly consistent read.
	//   ConsistentRead = True,
	//   # Sort items in descending order
	//   ScanIndexForward = False,
	//   # Specifies the maximum number of items to evaluate
	//   Limit = 1,
	// )
	blocks, ok := s.blocksByHeight[height]
	if !ok {
		return nil, xerrors.Errorf("not found: %v", height)
	}

	for i := len(blocks) - 1; i >= 0; i-- {
		block := blocks[i]
		if block.Event.SequenceNum <= s.checkpoint {
			return block, nil
		}
	}

	return nil, xerrors.Errorf("not found: %v", height)
}

func (s *KeyValueStore) SetBlock(height uint64, block *Block) {
	s.lock.Lock()
	defer s.lock.Unlock()

	blocks, ok := s.blocksByHeight[height]
	if !ok {
		blocks = make([]*Block, 0, 1)
	}

	// The blocks are processed out of order.
	// Sort it so that we can find the latest block (based on sequence number).
	blocks = append(blocks, block)
	if len(blocks) > 1 {
		sort.Slice(blocks, func(i, j int) bool {
			return blocks[i].Event.SequenceNum < blocks[j].Event.SequenceNum
		})
	}

	s.blocksByHeight[height] = blocks
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
	store := NewKeyValueStore(checkpoint)

	return &Worker{
		manager:         manager,
		logger:          logger,
		session:         session,
		backoffInterval: backoffInterval,
		numBatches:      0,
		store:           store,
	}, nil
}

func (w *Worker) Run() error {
	ctx := w.manager.ServiceContext()

	w.logger.Info(
		"running worker",
		zap.Int64("checkpoint", w.store.GetCheckpoint()),
	)

	for {
		sequence := w.store.GetCheckpoint()
		nextSequence, err := w.processBatch(ctx, sequence, batchSize)
		if err != nil {
			return xerrors.Errorf("failed to process batch [%v, %v): %w", sequence, sequence+batchSize, err)
		}

		w.store.SetCheckpoint(nextSequence)
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

	// When the requested amount is more than the available amount,
	// ChainStorage simply returns the available events.
	w.logger.Info(
		"processing batch",
		zap.Int64("sequence", sequence),
		zap.Uint64("batchSize", batchSize),
	)

	// This example differs from examples/stream in that
	// the blocks are persisted in the key-value store in parallel and out of order.
	group, ctx := errgroup.WithContext(ctx)
	for i := range events {
		i := i
		group.Go(func() error {
			event := events[i]
			blockID := event.Block
			if blockID.Skipped {
				return nil
			}

			if event.Type != api.BlockchainEvent_BLOCK_ADDED {
				// In this example, we only care about the newly added blocks.
				// The block with the maximum sequence number is the one on the canonical chain.
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

			w.store.SetBlock(blockID.Height, &Block{
				Event: event,
				Block: nativeBlock,
			})

			return nil
		})
	}

	if err := group.Wait(); err != nil {
		return 0, xerrors.Errorf("failed to get blocks: %w", err)
	}

	w.numBatches += 1
	lastEvent := events[len(events)-1]
	nextSequence := lastEvent.SequenceNum

	w.logger.Info(
		"finished batch",
		zap.Int64("sequence", sequence),
		zap.Int64("nextSequence", nextSequence),
		zap.Int("numBatches", w.numBatches),
	)
	return nextSequence, nil
}
