package model

import (
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

const (
	DefaultBlockTag   = uint32(1)
	EventIdStartValue = int64(1)
	EventIdDeleted    = int64(0)
)

type (
	EventEntry struct {
		EventId        int64
		EventType      api.BlockchainEvent_Type
		BlockHeight    uint64
		BlockHash      string
		Tag            uint32
		ParentHash     string
		MaxEventId     int64
		BlockSkipped   bool
		EventTag       uint32
		BlockTimestamp int64
	}

	BlockEvent struct {
		EventType      api.BlockchainEvent_Type
		BlockHash      string
		BlockHeight    uint64
		ParentHash     string
		Tag            uint32
		Skipped        bool
		BlockTimestamp int64
	}
)

func (e *BlockEvent) GetBlockHeight() uint64 {
	return e.BlockHeight
}

func (e *BlockEvent) GetBlockTimestamp() int64 {
	return e.BlockTimestamp
}

func (e *BlockEvent) GetBlockSkipped() bool {
	return e.Skipped
}

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

func NewBlockEventFromEventEntry(eventType api.BlockchainEvent_Type, entry *EventEntry) *BlockEvent {
	return &BlockEvent{
		EventType:      eventType,
		BlockHash:      entry.BlockHash,
		BlockHeight:    entry.BlockHeight,
		ParentHash:     entry.ParentHash,
		Tag:            entry.Tag,
		Skipped:        entry.BlockSkipped,
		BlockTimestamp: entry.BlockTimestamp,
	}
}

func CastItemToEventEntry(outputItem any) (*EventEntry, bool) {
	eventEntry, ok := outputItem.(*EventEntry)
	if !ok {
		return nil, ok
	}
	// switch to defaultTag is not set
	if eventEntry.Tag == 0 {
		eventEntry.Tag = DefaultBlockTag
	}
	return eventEntry, true
}
