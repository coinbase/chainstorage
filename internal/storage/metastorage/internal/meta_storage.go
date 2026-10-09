package internal

import (
	"context"
	"time"

	"go.uber.org/fx"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/storage/metastorage/model"
	"github.com/coinbase/chainstorage/internal/utils/fxparams"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

type (
	BlockStorage interface {
		PersistBlockMetas(ctx context.Context, updateWatermark bool, blocks []*api.BlockMetadata, lastBlock *api.BlockMetadata) error
		GetLatestBlock(ctx context.Context, tag uint32) (*api.BlockMetadata, error)
		GetBlockByHash(ctx context.Context, tag uint32, height uint64, blockHash string) (*api.BlockMetadata, error)
		GetBlockByHeight(ctx context.Context, tag uint32, height uint64) (*api.BlockMetadata, error)
		GetBlocksByHeightRange(ctx context.Context, tag uint32, startHeight, endHeight uint64) ([]*api.BlockMetadata, error)
		// GetBlocksByHeights gets blocks by heights. Results is an ordered array that matches the order in `heights` array
		// i.e. if the heights is [100,2,3], it will return the metadata in order: [block 100, block 2, block 3]
		GetBlocksByHeights(ctx context.Context, tag uint32, heights []uint64) ([]*api.BlockMetadata, error)
		// GetBlockByTimestamp gets the latest block before or at the given timestamp
		GetBlockByTimestamp(ctx context.Context, tag uint32, timestamp uint64) (*api.BlockMetadata, error)
		GetBlockConsolidationShadow(ctx context.Context, block *api.BlockMetadata) (*api.BlockMetadata, error)
		GetBlocksConsolidationShadow(ctx context.Context, blocks []*api.BlockMetadata) ([]*api.BlockMetadata, error)
		GetBlocksMissingConsolidationShadow(ctx context.Context, tag uint32, startHeight, endHeight uint64, limit uint64) ([]*BlockMetadataRecord, error)
		GetBlockConsolidationShadowStats(ctx context.Context, tag uint32, startHeight, endHeight uint64) (*ConsolidationShadowStats, error)
		GetFirstBlockMissingConsolidationShadow(ctx context.Context, tag uint32, startHeight, endHeight uint64) (uint64, bool, error)
		GetFirstPromotableBlockConsolidationShadow(ctx context.Context, tag uint32, startHeight, endHeight uint64) (uint64, bool, error)
		GetBlockConsolidationCursor(ctx context.Context, name string, tag uint32) (uint64, bool, error)
		SetBlockConsolidationCursor(ctx context.Context, name string, tag uint32, height uint64) error
		// ResetBlockConsolidationCursor writes the cursor exactly, allowing it
		// to move DOWN. SetBlockConsolidationCursor is monotonic by design (a
		// consolidation frontier never regresses); the retention floor
		// watermark (INF-1571) must be lowered when reconciliation finds an
		// undeleted row below it, which is the one legitimate backwards move.
		ResetBlockConsolidationCursor(ctx context.Context, name string, tag uint32, height uint64) error
		PersistBlockConsolidationShadows(ctx context.Context, placements []*ConsolidationShadowPlacement) error
		PromoteBlockConsolidationShadows(ctx context.Context, tag uint32, startHeight, endHeight uint64, limit uint64, singleBlockObjectRetention time.Duration) (*ConsolidationPromotionResult, error)
	}

	EventStorage interface {
		AddEvents(ctx context.Context, eventTag uint32, events []*model.BlockEvent) error
		AddEventEntries(ctx context.Context, eventTag uint32, eventEntries []*model.EventEntry) error
		GetEventByEventId(ctx context.Context, eventTag uint32, eventId int64) (*model.EventEntry, error)
		GetEventsAfterEventId(ctx context.Context, eventTag uint32, eventId int64, maxEvents uint64) ([]*model.EventEntry, error)
		GetEventsByEventIdRange(ctx context.Context, eventTag uint32, minEventId int64, maxEventId int64) ([]*model.EventEntry, error)
		GetMaxEventId(ctx context.Context, eventTag uint32) (int64, error)
		SetMaxEventId(ctx context.Context, eventTag uint32, maxEventId int64) error
		GetFirstEventIdByBlockHeight(ctx context.Context, eventTag uint32, blockHeight uint64) (int64, error)
		GetEventsByBlockHeight(ctx context.Context, eventTag uint32, blockHeight uint64) ([]*model.EventEntry, error)
	}

	TransactionStorage interface {
		// AddTransactions adds or updates a transaction to block number and hash mapping to the storage.
		AddTransactions(ctx context.Context, transaction []*model.Transaction, parallelism int) error

		// GetTransaction returns a slice of model.Transaction objects, representing the blocks'
		// information for the specific transaction.
		//
		// There are two reasons that a transaction could map to multiple blocks:
		// 1. blockchain reorgs
		// 2. protocol by design, e.g. NEAR
		GetTransaction(ctx context.Context, tag uint32, transactionHash string) ([]*model.Transaction, error)
	}

	MetaStorage interface {
		BlockStorage
		EventStorage
		TransactionStorage
		SingleBlockUploadGuardStorage
	}

	// SingleBlockUploadGuard serializes a single-block PUT with retirement
	// preparation, including when its metadata row does not exist yet.
	SingleBlockUploadGuard interface {
		RetirementFenced() bool
		Release() error
	}

	SingleBlockUploadGuardStorage interface {
		AcquireSingleBlockUploadGuard(ctx context.Context, tag uint32, height uint64, hash string) (SingleBlockUploadGuard, error)
	}

	Result struct {
		fx.Out
		BlockStorage       BlockStorage
		EventStorage       EventStorage
		MetaStorage        MetaStorage
		TransactionStorage TransactionStorage
	}

	BlockMetadataRecord struct {
		ID       int64
		Metadata *api.BlockMetadata
	}

	ConsolidationShadowPlacement struct {
		BlockMetadataID               int64
		Tag                           uint32
		Height                        uint64
		Hash                          string
		SingleBlockObjectKeyMain      string
		SingleBlockStorageGeneration  string
		ConsolidatedObjectKeyMain     string
		ConsolidatedStorageGeneration string
		ObjectFormat                  api.BlockObjectFormat
		ByteOffset                    uint64
		ByteLength                    uint64
		UncompressedLength            uint64
	}

	ConsolidationShadowStats struct {
		Objects        uint64
		Blocks         uint64
		EligibleBlocks uint64
	}

	ConsolidationPromotionResult struct {
		Blocks uint64
	}

	MetaStorageFactory interface {
		Create() (Result, error)
	}

	MetaStorageFactoryParams struct {
		fx.In
		fxparams.Params
		DynamoDB  MetaStorageFactory `name:"metastorage/dynamodb"`
		Firestore MetaStorageFactory `name:"metastorage/firestore"`
		Postgres  MetaStorageFactory `name:"metastorage/postgres"`
	}
)

type unfencedSingleBlockUploadGuard struct{}

func NewUnfencedSingleBlockUploadGuard() SingleBlockUploadGuard {
	return unfencedSingleBlockUploadGuard{}
}

func (unfencedSingleBlockUploadGuard) RetirementFenced() bool {
	return false
}

func (unfencedSingleBlockUploadGuard) Release() error {
	return nil
}

const (
	BatchConsolidatorAutoConsolidateCursor = "batch_consolidator_auto_consolidate_processed_exclusive"
	// SingleBlockRetentionFloorWatermarkCursor persists the retention cron's
	// floor watermark: everything below it was found retired by a walk, so
	// the next probe may start there instead of re-walking retired history
	// from approved_start_height (INF-1571). Raised via
	// SetBlockConsolidationCursor after each probe, lowered via
	// ResetBlockConsolidationCursor when reconciliation finds a stray.
	SingleBlockRetentionFloorWatermarkCursor = "single_block_retention_floor_watermark"
)

func WithMetaStorageFactory(params MetaStorageFactoryParams) (Result, error) {
	var factory MetaStorageFactory
	storageType := params.Config.StorageType.MetaStorageType
	switch storageType {
	case config.MetaStorageType_UNSPECIFIED, config.MetaStorageType_DYNAMODB:
		factory = params.DynamoDB
	case config.MetaStorageType_FIRESTORE:
		factory = params.Firestore
	case config.MetaStorageType_POSTGRES:
		factory = params.Postgres
	}
	if factory == nil {
		return Result{}, xerrors.Errorf("meta storage type is not implemented: %v", storageType)
	}
	result, err := factory.Create()
	if err != nil {
		return Result{}, xerrors.Errorf("failed to create meta storage of type %v, error: %w", storageType, err)
	}
	return result, nil
}
