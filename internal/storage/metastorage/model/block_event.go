package model

import (
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

const (
	DefaultBlockTag = uint32(1)
)

type (
	EventDDBEntry struct {
		EventId        int64                    `dynamodbav:"event_id"`
		EventType      api.BlockchainEvent_Type `dynamodbav:"type"`
		BlockHeight    uint64                   `dynamodbav:"height"` // together with tag & hash, we will be able to use them to locate the corresponding metadata entry
		BlockHash      string                   `dynamodbav:"hash"`
		Tag            uint32                   `dynamodbav:"tag"`
		ParentHash     string                   `dynamodbav:"parent_hash"`
		MaxEventId     int64                    `dynamodbav:"max_event_id"`
		BlockSkipped   bool                     `dynamodbav:"skipped"`
		EventTag       uint32                   `dynamodbav:"event_tag"`
		BlockTimestamp int64                    `dynamodbav:"block_timestamp"`
	}
	VersionedEventDDBEntry struct {
		EventId        string                   `dynamodbav:"event_id"` // will be formatted as {EventTag}-{Sequence}
		Sequence       int64                    `dynamodbav:"sequence"`
		BlockId        string                   `dynamodbav:"block_id"` // will be formatted as {EventTag}-{BlockHeight}
		BlockHeight    uint64                   `dynamodbav:"height"`
		BlockHash      string                   `dynamodbav:"hash"`
		EventType      api.BlockchainEvent_Type `dynamodbav:"type"`
		Tag            uint32                   `dynamodbav:"tag"`
		ParentHash     string                   `dynamodbav:"parent_hash"`
		BlockSkipped   bool                     `dynamodbav:"skipped"`
		EventTag       uint32                   `dynamodbav:"event_tag"`
		BlockTimestamp int64                    `dynamodbav:"block_timestamp"`
	}
	BlockEvent struct {
		eventType      api.BlockchainEvent_Type
		blockHash      string
		blockHeight    uint64
		parentHash     string
		tag            uint32
		skipped        bool
		blockTimestamp int64
	}
)

func NewBlockEvent(eventType api.BlockchainEvent_Type, blockHash string, parentHash string, blockHeight uint64, tag uint32, skipped bool, blockTimestamp int64) *BlockEvent {
	return &BlockEvent{
		eventType:      eventType,
		blockHash:      blockHash,
		blockHeight:    blockHeight,
		parentHash:     parentHash,
		tag:            tag,
		skipped:        skipped,
		blockTimestamp: blockTimestamp,
	}
}

func NewBlockEventFromAnotherDDBEntry(eventType api.BlockchainEvent_Type, ddbEntry *EventDDBEntry) *BlockEvent {
	return &BlockEvent{
		eventType:      eventType,
		blockHash:      ddbEntry.BlockHash,
		blockHeight:    ddbEntry.BlockHeight,
		parentHash:     ddbEntry.ParentHash,
		tag:            ddbEntry.Tag,
		skipped:        ddbEntry.BlockSkipped,
		blockTimestamp: ddbEntry.BlockTimestamp,
	}
}

func NewBlockEventWithBlockMeta(eventType api.BlockchainEvent_Type, block *api.BlockMetadata) *BlockEvent {
	return &BlockEvent{
		eventType:      eventType,
		blockHash:      block.GetHash(),
		blockHeight:    block.GetHeight(),
		parentHash:     block.GetParentHash(),
		tag:            block.GetTag(),
		skipped:        block.GetSkipped(),
		blockTimestamp: block.GetTimestamp().GetSeconds(),
	}
}

func NewEventDDBEntry(eventTag uint32, eventId int64, inputEvent *BlockEvent) *EventDDBEntry {
	return &EventDDBEntry{
		EventId:        eventId,
		EventType:      inputEvent.eventType,
		BlockHeight:    inputEvent.blockHeight,
		BlockHash:      inputEvent.blockHash,
		Tag:            inputEvent.tag,
		ParentHash:     inputEvent.parentHash,
		BlockSkipped:   inputEvent.skipped,
		EventTag:       eventTag,
		BlockTimestamp: inputEvent.blockTimestamp,
	}
}

func CastItemToDDBEntry(outputItem interface{}) (*EventDDBEntry, bool) {
	eventDDBEntry, ok := outputItem.(*EventDDBEntry)
	if !ok {
		return nil, ok
	}
	// switch to defaultTag is not set
	if eventDDBEntry.Tag == 0 {
		eventDDBEntry.Tag = DefaultBlockTag
	}
	return eventDDBEntry, true
}

func CastVersionedItemToVersionedDDBEntry(outputItem interface{}) (*VersionedEventDDBEntry, bool) {
	versionedEventDDBEntry, ok := outputItem.(*VersionedEventDDBEntry)
	if !ok {
		return nil, ok
	}

	// switch to defaultTag is not set
	if versionedEventDDBEntry.Tag == 0 {
		versionedEventDDBEntry.Tag = DefaultBlockTag
	}
	return versionedEventDDBEntry, true
}

func (e *BlockEvent) GetBlockHeight() uint64 {
	return e.blockHeight
}

func (e *BlockEvent) GetBlockSkipped() bool {
	return e.skipped
}
