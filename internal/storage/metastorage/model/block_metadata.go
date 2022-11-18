package model

import (
	"github.com/coinbase/chainstorage/internal/utils/utils"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

type BlockMetaDataDDBEntry struct {
	BlockPid      string `dynamodbav:"block_pid"`
	BlockRid      string `dynamodbav:"block_rid"`
	Tag           uint32 `dynamodbav:"tag"`
	Hash          string `dynamodbav:"block_hash"`
	ParentHash    string `dynamodbav:"parent_hash"`
	Height        uint64 `dynamodbav:"height"`
	ParentHeight  uint64 `dynamodbav:"parent_height"`
	ObjectKeyMain string `dynamodbav:"object_key_main"`
	Skipped       bool   `dynamodbav:"skipped"`
	Timestamp     int64  `dynamodbav:"timestamp"`
}

func BlockMetadataToProto(bm *BlockMetaDataDDBEntry) *api.BlockMetadata {
	v := &api.BlockMetadata{
		Tag:           bm.Tag,
		Hash:          bm.Hash,
		ParentHash:    bm.ParentHash,
		Height:        bm.Height,
		ParentHeight:  bm.ParentHeight,
		ObjectKeyMain: bm.ObjectKeyMain,
		Skipped:       bm.Skipped,
		Timestamp:     utils.ToTimestamp(bm.Timestamp),
	}

	// Set parent height if it is not present,
	// except for the genesis and skipped block.
	if v.ParentHeight == 0 && v.Height != 0 && !v.Skipped {
		v.ParentHeight = v.Height - 1
	}

	return v
}
