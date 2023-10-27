package dynamodb

import (
	"context"
	"fmt"
	"reflect"
	"sort"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/blockchain/parser"
	"github.com/coinbase/chainstorage/internal/storage/internal/errors"
	"github.com/coinbase/chainstorage/internal/storage/metastorage/dynamodb/model"
	"github.com/coinbase/chainstorage/internal/storage/metastorage/internal"
	"github.com/coinbase/chainstorage/internal/utils/instrument"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

const (
	blockPidFormat     = "%d-%v"
	latestBlockHeight  = "latest"
	canonicalBlockHash = "canonical"
	nullBlockHash      = "null"
	blockPidKeyName    = "block_pid"
	blockRidKeyName    = "block_rid"
	maxBlocksWriteSize = (maxWriteItemsSize - 1) / 2 // for each block we write two entries, plus one watermark entry
)

type (
	blockStorageImpl struct {
		blockTable                       ddbTable
		blockStartHeight                 uint64
		instrumentPersistBlockMetas      instrument.Call
		instrumentGetLatestBlock         instrument.Call
		instrumentGetBlockByHash         instrument.Call
		instrumentGetBlockByHeight       instrument.Call
		instrumentGetBlocksByHeightRange instrument.Call
	}
)

func newBlockStorage(params Params) (internal.BlockStorage, error) {
	attrDefs := []*dynamodb.AttributeDefinition{
		{
			AttributeName: aws.String(blockPidKeyName),
			AttributeType: awsStringType,
		},
		{
			AttributeName: aws.String(blockRidKeyName),
			AttributeType: awsStringType,
		},
	}
	keySchema := []*dynamodb.KeySchemaElement{
		{
			AttributeName: aws.String(blockPidKeyName),
			KeyType:       hashKeyType,
		},
		{
			AttributeName: aws.String(blockRidKeyName),
			KeyType:       rangeKeyType,
		},
	}
	var globalSecondaryIndexes []*dynamodb.GlobalSecondaryIndex
	metadataTable, err := newDDBTable(
		params.Config.AWS.DynamoDB.BlockTable,
		reflect.TypeOf(model.BlockMetaDataDDBEntry{}),
		keySchema, attrDefs, globalSecondaryIndexes,
		params,
	)
	if err != nil {
		return nil, xerrors.Errorf("failed to create metadata accessor: %w", err)
	}

	metrics := params.Metrics.SubScope("block_storage")
	accessor := blockStorageImpl{
		blockTable:                       metadataTable,
		blockStartHeight:                 params.Config.Chain.BlockStartHeight,
		instrumentPersistBlockMetas:      instrument.NewCall(metrics, "persist_block_metas"),
		instrumentGetLatestBlock:         instrument.NewCall(metrics, "get_latest_block"),
		instrumentGetBlockByHash:         instrument.NewCall(metrics, "get_block_by_hash"),
		instrumentGetBlockByHeight:       instrument.NewCall(metrics, "get_block_by_height"),
		instrumentGetBlocksByHeightRange: instrument.NewCall(metrics, "get_blocks_by_height_range"),
	}
	return &accessor, nil
}

func getBlockKeyMap(blockPid string, blockRid string) StringMap {
	return StringMap{
		blockPidKeyName: blockPid,
		blockRidKeyName: blockRid,
	}
}

func getBlockPidForHeight(tag uint32, height uint64) string {
	return fmt.Sprintf(blockPidFormat, tag, height)
}

func getBlockPidForLatest(tag uint32) string {
	return fmt.Sprintf(blockPidFormat, tag, latestBlockHeight)
}

func getCanonicalBlockRid() string {
	return canonicalBlockHash
}

func getBlockRidWithBlockHash(blockHash string) string {
	if blockHash == "" {
		// Note that blockHash is empty for those skipped blocks,
		// yet DynamoDB doesn't allow empty range key.
		return nullBlockHash
	}
	return blockHash
}

func (a *blockStorageImpl) getBlockByKeys(
	ctx context.Context, blockPid string, blockRid string) (*api.BlockMetadata, error) {

	blockKeyMap := getBlockKeyMap(blockPid, blockRid)
	outputItem, err := a.blockTable.GetItem(ctx, blockKeyMap)
	if err != nil {
		return nil, xerrors.Errorf("failed to get block metadata with GetBlock: %w", err)
	}
	blockDDBEntry, ok := outputItem.(*model.BlockMetaDataDDBEntry)
	if !ok {
		return nil, xerrors.Errorf("failed to cast to blockDDBEntry: %v", outputItem)
	}
	return model.BlockMetadataToProto(blockDDBEntry), nil
}

func (a *blockStorageImpl) getBlocksByKeys(ctx context.Context, blockPids []string, blockRid string) (
	[]*api.BlockMetadata, error) {
	keys := make([]StringMap, len(blockPids))
	for i, blockPid := range blockPids {
		blockKeyMap := getBlockKeyMap(blockPid, blockRid)
		keys[i] = blockKeyMap
	}

	outputEntries, err := a.blockTable.GetItems(ctx, keys)
	if err != nil {
		return nil, xerrors.Errorf("failed to get block metadata with GetItems: %w", err)
	}

	blocks := make([]*api.BlockMetadata, len(outputEntries))
	for i := 0; i < len(outputEntries); i++ {
		outputEntry, ok := outputEntries[i].(*model.BlockMetaDataDDBEntry)
		if !ok {
			return nil, xerrors.Errorf("failed to cast to blockDDBEntry: %v", outputEntries[i])
		}
		blocks[i] = model.BlockMetadataToProto(outputEntry)
	}
	return blocks, nil
}

func (a *blockStorageImpl) GetLatestBlock(
	ctx context.Context, tag uint32) (*api.BlockMetadata, error) {
	var block *api.BlockMetadata
	if err := a.instrumentGetLatestBlock.Instrument(ctx, func(ctx context.Context) error {
		var err error
		blockPid := getBlockPidForLatest(tag)
		blockRid := getCanonicalBlockRid()
		block, err = a.getBlockByKeys(ctx, blockPid, blockRid)
		return err
	}); err != nil {
		return nil, err
	}

	return block, nil
}

func (a *blockStorageImpl) GetBlockByHeight(
	ctx context.Context, tag uint32, height uint64) (*api.BlockMetadata, error) {
	if height < a.blockStartHeight {
		return nil, xerrors.Errorf(
			"height(%d) should be no less than blockStartHeight(%d): %w",
			height, a.blockStartHeight, errors.ErrInvalidHeight)
	}
	var block *api.BlockMetadata
	if err := a.instrumentGetBlockByHeight.Instrument(ctx, func(ctx context.Context) error {
		var err error
		blockPid := getBlockPidForHeight(tag, height)
		blockRid := getCanonicalBlockRid()
		block, err = a.getBlockByKeys(ctx, blockPid, blockRid)
		return err
	}); err != nil {
		return nil, err
	}

	return block, nil
}

func (a *blockStorageImpl) GetBlocksByHeightRange(
	ctx context.Context, tag uint32, startHeight, endHeight uint64) ([]*api.BlockMetadata, error) {
	if startHeight >= endHeight || startHeight < a.blockStartHeight {
		return nil, xerrors.Errorf(
			"startHeight(%d) should be less than endHeight(%d) and startHeight should also be no less than blockStartHeight(%d): %w",
			startHeight, endHeight, a.blockStartHeight, errors.ErrOutOfRange)
	}

	blockPids := make([]string, endHeight-startHeight)
	for i := uint64(0); startHeight+i < endHeight; i++ {
		blockPids[i] = getBlockPidForHeight(tag, startHeight+i)
	}

	var blocks []*api.BlockMetadata
	if err := a.instrumentGetBlocksByHeightRange.Instrument(ctx, func(ctx context.Context) error {
		var err error
		blockRid := getCanonicalBlockRid()
		blocks, err = a.getBlocksByKeys(ctx, blockPids, blockRid)
		if err != nil {
			return xerrors.Errorf("failed to get blocks by keys: %w", err)
		}

		sort.Slice(blocks, func(i, j int) bool {
			return blocks[i].Height < blocks[j].Height
		})
		if err = parser.ValidateChain(blocks, nil); err != nil {
			return xerrors.Errorf("failed to validate chain: %w", err)
		}

		return nil
	}); err != nil {
		return nil, err
	}

	return blocks, nil
}

func (a *blockStorageImpl) GetBlockByHash(
	ctx context.Context, tag uint32, height uint64, blockHash string) (*api.BlockMetadata, error) {
	var block *api.BlockMetadata
	if err := a.instrumentGetBlockByHash.Instrument(ctx, func(ctx context.Context) error {
		var err error
		if blockHash == "" {
			blockHash = canonicalBlockHash
		}
		blockPid := getBlockPidForHeight(tag, height)
		blockRid := getBlockRidWithBlockHash(blockHash)
		block, err = a.getBlockByKeys(ctx, blockPid, blockRid)
		return err
	}); err != nil {
		return nil, err
	}

	return block, nil
}

func makeBlockMetaDataDDBEntry(block *api.BlockMetadata) *model.BlockMetaDataDDBEntry {
	blockMetaDataDDBEntry := model.BlockMetaDataDDBEntry{
		BlockPid:      getBlockPidForHeight(block.Tag, block.Height),
		BlockRid:      getBlockRidWithBlockHash(block.Hash),
		Tag:           block.Tag,
		Hash:          block.Hash,
		ParentHash:    block.ParentHash,
		Height:        block.Height,
		ParentHeight:  block.ParentHeight,
		ObjectKeyMain: block.ObjectKeyMain,
		Skipped:       block.Skipped,
		Timestamp:     block.GetTimestamp().GetSeconds(),
	}
	return &blockMetaDataDDBEntry
}

func (a *blockStorageImpl) PersistBlockMetas(
	ctx context.Context, updateWatermark bool, blocks []*api.BlockMetadata, lastBlock *api.BlockMetadata) error {
	return a.instrumentPersistBlockMetas.Instrument(ctx, func(ctx context.Context) error {
		if len(blocks) == 0 {
			return nil
		}
		sort.Slice(blocks, func(i, j int) bool {
			return blocks[i].Height < blocks[j].Height
		})
		if err := parser.ValidateChain(blocks, lastBlock); err != nil {
			return xerrors.Errorf("failed to validate chain: %w", err)
		}
		// cut blocks into two chunks, the second chunk is updated with TransactWriteItems such that the watermark will be
		// updated within it
		startIndex := 0
		if len(blocks) > maxBlocksWriteSize {
			startIndex = len(blocks) - maxBlocksWriteSize
			blockEntries := a.makeBlockMetaDataDDBEntries(false, blocks[:startIndex])
			err := a.blockTable.WriteItems(ctx, blockEntries)
			if err != nil {
				return xerrors.Errorf("failed to persist block metadatas with WriteItems: %w", err)
			}
		}

		blockEntries := a.makeBlockMetaDataDDBEntries(updateWatermark, blocks[startIndex:])
		err := a.blockTable.TransactWriteItems(ctx, blockEntries)
		if err != nil {
			return xerrors.Errorf("failed to persist block metadatas with TransactWriteItems: %w", err)
		}

		return nil
	})
}

func (a *blockStorageImpl) makeBlockMetaDataDDBEntries(includeWatermark bool, blocks []*api.BlockMetadata) []interface{} {
	blockEntries := make([]interface{}, 0, len(blocks)*2+1)
	for _, block := range blocks {
		// for each block append two entries, one under block hash, one under canonical alias
		blockMetaDataDDBEntry := makeBlockMetaDataDDBEntry(block)
		blockEntries = append(blockEntries, blockMetaDataDDBEntry)

		blockMetaDataDDBEntry = makeBlockMetaDataDDBEntry(block)
		blockMetaDataDDBEntry.BlockRid = getCanonicalBlockRid()
		blockEntries = append(blockEntries, blockMetaDataDDBEntry)
	}
	if includeWatermark {
		// update watermark item
		watermarkBlock := blocks[len(blocks)-1]
		blockMetaDataDDBEntry := makeBlockMetaDataDDBEntry(watermarkBlock)
		blockMetaDataDDBEntry.BlockPid = getBlockPidForLatest(watermarkBlock.Tag)
		blockMetaDataDDBEntry.BlockRid = getCanonicalBlockRid()
		blockEntries = append(blockEntries, blockMetaDataDDBEntry)
	}
	return blockEntries
}
