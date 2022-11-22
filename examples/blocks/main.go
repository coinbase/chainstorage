package main

import (
	"context"
	"time"

	"go.uber.org/zap"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/services"
	"github.com/coinbase/chainstorage/protos/coinbase/c3/common"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
	"github.com/coinbase/chainstorage/sdk"
)

const (
	blockchain    = common.Blockchain_BLOCKCHAIN_ETHEREUM
	network       = common.Network_NETWORK_ETHEREUM_GOERLI
	batchSize     = 20
	startDistance = 100
)

type (
	Worker struct {
		manager              services.SystemManager
		logger               *zap.Logger
		session              sdk.Session
		batchSize            uint64
		checkpoint           uint64
		irreversibleDistance uint64
		backoffInterval      time.Duration
		numTransactions      int
		numBatches           int
	}
)

func main() {
	manager := services.NewManager()
	defer manager.Shutdown()

	worker, err := NewWorker(manager)
	if err != nil {
		panic(err)
	}

	if err := worker.Run(); err != nil {
		panic(err)
	}
}

func NewWorker(manager services.SystemManager) (*Worker, error) {
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

	latestHeight, err := client.GetLatestBlock(ctx)
	if err != nil {
		return nil, xerrors.Errorf("failed to get latest block: %w", err)
	}

	// Checkpoint is typically persisted in the database.
	// In this example, we simply derive the checkpoint from the latest height.
	checkpoint := latestHeight - startDistance

	backoffInterval, err := time.ParseDuration(chainMetadata.BlockTime)
	if err != nil {
		return nil, xerrors.Errorf("failed to parse block time: %w", err)
	}

	return &Worker{
		manager:              manager,
		logger:               logger,
		session:              session,
		batchSize:            batchSize,
		checkpoint:           checkpoint,
		irreversibleDistance: chainMetadata.IrreversibleDistance,
		backoffInterval:      backoffInterval,
		numTransactions:      0,
		numBatches:           0,
	}, nil
}

func (w *Worker) Run() error {
	ctx := w.manager.ServiceContext()

	w.logger.Info(
		"running worker",
		zap.Uint64("checkpoint", w.checkpoint),
		zap.Uint64("irreversibleDistance", w.irreversibleDistance),
	)

	for {
		startHeight := w.checkpoint
		latestHeight, err := w.session.Client().GetLatestBlock(ctx)
		if err != nil {
			return xerrors.Errorf("failed to get latest block: %w", err)
		}

		endHeight := latestHeight - w.irreversibleDistance
		if endHeight > w.checkpoint+w.batchSize {
			endHeight = w.checkpoint + w.batchSize
		}

		if err := w.processBatch(ctx, startHeight, endHeight); err != nil {
			return xerrors.Errorf("failed to process batch [%v, %v): %w", startHeight, endHeight, err)
		}

		// Checkpoint is typically persisted in the database.
		// In this example, we simply store the checkpoint in memory.
		w.checkpoint = endHeight
	}
}

func (w *Worker) processBatch(ctx context.Context, startHeight uint64, endHeight uint64) error {
	if startHeight >= endHeight {
		w.logger.Info(
			"waiting for new blocks",
			zap.Uint64("startHeight", startHeight),
			zap.Uint64("endHeight", endHeight),
		)
		time.Sleep(w.backoffInterval)
		return nil
	}

	w.logger.Info(
		"processing batch",
		zap.Uint64("startHeight", startHeight),
		zap.Uint64("endHeight", endHeight),
	)

	blocks, err := w.session.Client().GetBlocksByRange(ctx, startHeight, endHeight)
	if err != nil {
		return xerrors.Errorf("failed to get blocks: %w", err)
	}

	parser := w.session.Parser()
	for _, block := range blocks {
		nativeBlock, err := parser.ParseNativeBlock(ctx, block)
		if err != nil {
			return xerrors.Errorf("failed to parse block {%+v}: %w", block.Metadata, err)
		}

		ethereumBlock := nativeBlock.GetEthereum()
		w.numTransactions += len(ethereumBlock.Header.Transactions)
	}

	w.numBatches += 1
	w.logger.Info(
		"finished batch",
		zap.Uint64("startHeight", startHeight),
		zap.Uint64("endHeight", endHeight),
		zap.Int("numBatches", w.numBatches),
		zap.Int("numTransactions", w.numTransactions),
	)
	return nil
}
