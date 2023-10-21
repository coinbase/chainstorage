package internal

import (
	"context"

	"go.uber.org/fx"

	"github.com/coinbase/chainstorage/internal/storage/metastorage/model"
	"github.com/coinbase/chainstorage/internal/utils/fxparams"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

type (
	EventEntry = model.EventEntry
	BlockEvent = model.BlockEvent

	BlockStorage interface {
		PersistBlockMetas(ctx context.Context, updateWatermark bool, blocks []*api.BlockMetadata, lastBlock *api.BlockMetadata) error
		GetLatestBlock(ctx context.Context, tag uint32) (*api.BlockMetadata, error)
		GetBlockByHash(ctx context.Context, tag uint32, height uint64, blockHash string) (*api.BlockMetadata, error)
		GetBlockByHeight(ctx context.Context, tag uint32, height uint64) (*api.BlockMetadata, error)
		GetBlocksByHeightRange(ctx context.Context, tag uint32, startHeight, endHeight uint64) ([]*api.BlockMetadata, error)
	}

	EventStorage interface {
		AddEvents(ctx context.Context, eventTag uint32, events []*BlockEvent) error
		GetEventByEventId(ctx context.Context, eventTag uint32, eventId int64) (*EventEntry, error)
		GetEventsAfterEventId(ctx context.Context, eventTag uint32, eventId int64, maxEvents uint64) ([]*EventEntry, error)
		GetEventsByEventIdRange(ctx context.Context, eventTag uint32, minEventId int64, maxEventId int64) ([]*EventEntry, error)
		GetMaxEventId(ctx context.Context, eventTag uint32) (int64, error)
		SetMaxEventId(ctx context.Context, eventTag uint32, maxEventId int64) error
		GetFirstEventIdByBlockHeight(ctx context.Context, eventTag uint32, blockHeight uint64) (int64, error)
		GetEventsByBlockHeight(ctx context.Context, eventTag uint32, blockHeight uint64) ([]*EventEntry, error)
	}

	MetaStorage interface {
		BlockStorage
		EventStorage
	}

	Result struct {
		fx.Out
		MetaStorage  MetaStorage
		BlockStorage BlockStorage
		EventStorage EventStorage
	}

	MetaStorageFactory interface {
		Create() (Result, error)
	}

	MetaStorageFactoryParams struct {
		fx.In
		fxparams.Params
		DynamoDB MetaStorageFactory `name:"metastorage/dynamodb"`
	}
)

const (
	DefaultBlockTag   = uint32(1)
	EventIdStartValue = int64(1)
	EventIdDeleted    = int64(0)
)

func NewBlockEvent(eventType api.BlockchainEvent_Type, blockHash string, parentHash string, blockHeight uint64, tag uint32, skipped bool, blockTimestamp int64) *BlockEvent {
	return &BlockEvent{
		EventType:      eventType,
		BlockHash:      blockHash,
		BlockHeight:    blockHeight,
		ParentHash:     parentHash,
		Tag:            tag,
		Skipped:        skipped,
		BlockTimestamp: blockTimestamp,
	}
}

func NewEventEntry(eventTag uint32, eventId int64, inputEvent *BlockEvent) *EventEntry {
	return &EventEntry{
		EventId:        eventId,
		EventType:      inputEvent.EventType,
		BlockHeight:    inputEvent.BlockHeight,
		BlockHash:      inputEvent.BlockHash,
		Tag:            inputEvent.Tag,
		ParentHash:     inputEvent.ParentHash,
		BlockSkipped:   inputEvent.Skipped,
		EventTag:       eventTag,
		BlockTimestamp: inputEvent.BlockTimestamp,
	}
}

func NewBlockEventWithBlockMeta(eventType api.BlockchainEvent_Type, block *api.BlockMetadata) *BlockEvent {
	return &BlockEvent{
		EventType:      eventType,
		BlockHash:      block.GetHash(),
		BlockHeight:    block.GetHeight(),
		ParentHash:     block.GetParentHash(),
		Tag:            block.GetTag(),
		Skipped:        block.GetSkipped(),
		BlockTimestamp: block.GetTimestamp().GetSeconds(),
	}
}
