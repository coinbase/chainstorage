package model

import (
	"github.com/coinbase/chainstorage/internal/storage/metastorage/internal"
	"github.com/coinbase/chainstorage/internal/storage/metastorage/model"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
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
)

func newEventDDBEntry(eventTag uint32, eventId int64, inputEvent *model.BlockEvent) *EventDDBEntry {
	return &EventDDBEntry{
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

func ConvertBlockEventsToEventDDBEntries(blockEvents []*internal.BlockEvent, eventTag uint32, eventId int64) []*EventDDBEntry {
	eventDDBEntries := make([]*EventDDBEntry, len(blockEvents))
	if len(blockEvents) == 0 {
		return eventDDBEntries
	}
	for i, inputEvent := range blockEvents {
		event := newEventDDBEntry(eventTag, eventId, inputEvent)
		eventDDBEntries[i] = event
		eventId += 1
	}

	return eventDDBEntries
}

func CastItemToDDBEntry(outputItem interface{}) (*EventDDBEntry, bool) {
	eventDDBEntry, ok := outputItem.(*EventDDBEntry)
	if !ok {
		return nil, ok
	}
	// switch to defaultTag is not set
	if eventDDBEntry.Tag == 0 {
		eventDDBEntry.Tag = internal.DefaultBlockTag
	}
	return eventDDBEntry, true
}

func CastEventDDBEntryToEventEntry(entry *EventDDBEntry) *model.EventEntry {
	return &model.EventEntry{
		EventId:        entry.EventId,
		EventType:      entry.EventType,
		BlockHash:      entry.BlockHash,
		BlockHeight:    entry.BlockHeight,
		ParentHash:     entry.ParentHash,
		Tag:            entry.Tag,
		BlockTimestamp: entry.BlockTimestamp,
		MaxEventId:     entry.MaxEventId,
		BlockSkipped:   entry.BlockSkipped,
		EventTag:       entry.EventTag,
	}
}

func CastEventEntryToEventDDBEntry(entry *internal.EventEntry) *EventDDBEntry {
	return &EventDDBEntry{
		EventId:        entry.EventId,
		EventType:      entry.EventType,
		BlockHash:      entry.BlockHash,
		BlockHeight:    entry.BlockHeight,
		ParentHash:     entry.ParentHash,
		Tag:            entry.Tag,
		BlockTimestamp: entry.BlockTimestamp,
		MaxEventId:     entry.MaxEventId,
		BlockSkipped:   entry.BlockSkipped,
		EventTag:       entry.EventTag,
	}
}

func CastVersionedItemToVersionedDDBEntry(outputItem interface{}) (*VersionedEventDDBEntry, bool) {
	versionedEventDDBEntry, ok := outputItem.(*VersionedEventDDBEntry)
	if !ok {
		return nil, ok
	}

	// switch to defaultTag is not set
	if versionedEventDDBEntry.Tag == 0 {
		versionedEventDDBEntry.Tag = internal.DefaultBlockTag
	}
	return versionedEventDDBEntry, true
}
