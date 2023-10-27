package model

import (
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
