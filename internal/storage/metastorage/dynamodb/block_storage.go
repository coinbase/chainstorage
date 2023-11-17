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
		instrumentPersistBlockMetas      instrument.Instrument
		instrumentGetLatestBlock         instrument.InstrumentWithResult[*api.BlockMetadata]
		instrumentGetBlockByHash         instrument.InstrumentWithResult[*api.BlockMetadata]
		instrumentGetBlockByHeight       instrument.InstrumentWithResult[*api.BlockMetadata]
		instrumentGetBlocksByHeightRange instrument.InstrumentWithResult[[]*api.BlockMetadata]
		instrumentGetBlocksByHeights     instrument.InstrumentWithResult[[]*api.BlockMetadata]
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
		instrumentPersistBlockMetas:      instrument.New(metrics, "persist_block_metas"),
		instrumentGetLatestBlock:         instrument.NewWithResult[*api.BlockMetadata](metrics, "get_latest_block"),
		instrumentGetBlockByHash:         instrument.NewWithResult[*api.BlockMetadata](metrics, "get_block_by_hash"),
		instrumentGetBlockByHeight:       instrument.NewWithResult[*api.BlockMetadata](metrics, "get_block_by_height"),
		instrumentGetBlocksByHeightRange: instrument.NewWithResult[[]*api.BlockMetadata](metrics, "get_blocks_by_height_range"),
		instrumentGetBlocksByHeights:     instrument.NewWithResult[[]*api.BlockMetadata](metrics, "get_blocks_by_heights"),
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
	return a.instrumentGetLatestBlock.Instrument(ctx, func(ctx context.Context) (*api.BlockMetadata, error) {
		blockPid := getBlockPidForLatest(tag)
		blockRid := getCanonicalBlockRid()
		return a.getBlockByKeys(ctx, blockPid, blockRid)
	})
}

func (a *blockStorageImpl) GetBlockByHeight(
	ctx context.Context, tag uint32, height uint64) (*api.BlockMetadata, error) {
	if err := a.validateHeight(height); err != nil {
		return nil, err
	}

	return a.instrumentGetBlockByHeight.Instrument(ctx, func(ctx context.Context) (*api.BlockMetadata, error) {
		blockPid := getBlockPidForHeight(tag, height)
		blockRid := getCanonicalBlockRid()
		return a.getBlockByKeys(ctx, blockPid, blockRid)
	})
}

func (a *blockStorageImpl) GetBlocksByHeightRange(
	ctx context.Context, tag uint32, startHeight, endHeight uint64) ([]*api.BlockMetadata, error) {
	if startHeight >= endHeight {
		return nil, xerrors.Errorf(
			"startHeight(%d) should be less than endHeight(%d): %w",
			startHeight, endHeight, errors.ErrOutOfRange)
	}
	if err := a.validateHeight(startHeight); err != nil {
		return nil, err
	}

	blockPids := make([]string, endHeight-startHeight)
	for i := uint64(0); startHeight+i < endHeight; i++ {
		blockPids[i] = getBlockPidForHeight(tag, startHeight+i)
	}

	return a.instrumentGetBlocksByHeightRange.Instrument(ctx, func(ctx context.Context) ([]*api.BlockMetadata, error) {
		blockRid := getCanonicalBlockRid()
		blocks, err := a.getBlocksByKeys(ctx, blockPids, blockRid)
		if err != nil {
			return nil, xerrors.Errorf("failed to get blocks by keys: %w", err)
		}

		sort.Slice(blocks, func(i, j int) bool {
			return blocks[i].Height < blocks[j].Height
		})
		if err = parser.ValidateChain(blocks, nil); err != nil {
			return nil, xerrors.Errorf("failed to validate chain: %w", err)
		}

		return blocks, nil
	})
}

func (a *blockStorageImpl) GetBlocksByHeights(ctx context.Context, tag uint32, heights []uint64) ([]*api.BlockMetadata, error) {
	if len(heights) == 0 {
		return nil, nil
	}

	blockPids := make([]string, len(heights))
	for i, height := range heights {
		if err := a.validateHeight(height); err != nil {
			return nil, err
		}
		blockPids[i] = getBlockPidForHeight(tag, height)
	}

	blocks, err := a.instrumentGetBlocksByHeights.Instrument(ctx, func(ctx context.Context) ([]*api.BlockMetadata, error) {
		blockRid := getCanonicalBlockRid()
		return a.getBlocksByKeys(ctx, blockPids, blockRid)
	})
	if err != nil {
		return nil, err
	}

	heightToBlockMap := make(map[uint64]*api.BlockMetadata)
	for _, block := range blocks {
		heightToBlockMap[block.Height] = block
	}

	// results is ordered based on input array `heights`
	results := make([]*api.BlockMetadata, len(heights))
	for i, height := range heights {
		results[i] = heightToBlockMap[height]
	}

	return results, nil
}

func (a *blockStorageImpl) GetBlockByHash(
	ctx context.Context, tag uint32, height uint64, blockHash string) (*api.BlockMetadata, error) {
	if err := a.validateHeight(height); err != nil {
		return nil, err
	}

	return a.instrumentGetBlockByHash.Instrument(ctx, func(ctx context.Context) (*api.BlockMetadata, error) {
		if blockHash == "" {
			blockHash = canonicalBlockHash
		}
		blockPid := getBlockPidForHeight(tag, height)
		blockRid := getBlockRidWithBlockHash(blockHash)
		return a.getBlockByKeys(ctx, blockPid, blockRid)
	})
}

func (a *blockStorageImpl) validateHeight(height uint64) error {
	if height < a.blockStartHeight {
		return xerrors.Errorf(
			"height(%d) should be no less than blockStartHeight(%d): %w",
			height, a.blockStartHeight, errors.ErrInvalidHeight)
	}
	return nil
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

func (a *blockStorageImpl) makeBlockMetaDataDDBEntries(includeWatermark bool, blocks []*api.BlockMetadata) []any {
	blockEntries := make([]any, 0, len(blocks)*2+1)
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
