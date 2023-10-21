package testutil

import (
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/ethereum/go-ethereum/common/hexutil"

	metastorage "github.com/coinbase/chainstorage/internal/storage/metastorage/model"
	"github.com/coinbase/chainstorage/internal/utils/utils"
	"github.com/coinbase/chainstorage/protos/coinbase/c3/common"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

type (
	Option func(*builderOptions)

	builderOptions struct {
		blockHashFormat string
		dataCompression api.Compression
		blockSkipped    bool
	}

	Header struct {
		Hash       string `json:"hash"`
		ParentHash string `json:"parentHash"`
		Number     string `json:"number"`
		Timestamp  string `json:"timestamp"`
	}
)

const (
	blockTimestamp = 0x5fbd2fb9
)

func MakeBlockEvent(eventType api.BlockchainEvent_Type, height uint64, tag uint32, opts ...Option) *metastorage.BlockEvent {
	block := getBlock(height, tag, opts...)
	return metastorage.NewBlockEvent(eventType, block.Hash, block.ParentHash, height, tag, block.Skipped, block.GetTimestamp().GetSeconds())
}

func MakeBlockEvents(eventType api.BlockchainEvent_Type, startHeight uint64, endHeight uint64, tag uint32, opts ...Option) []*metastorage.BlockEvent {
	events := make([]*metastorage.BlockEvent, 0, endHeight-startHeight)
	for i := startHeight; i < endHeight; i++ {
		events = append(events, MakeBlockEvent(eventType, i, tag, opts...))
	}
	return events
}

func MakeBlockEventEntries(eventType api.BlockchainEvent_Type, eventTag uint32, endEventId int64, startHeight uint64, endHeight uint64, tag uint32, opts ...Option) []*metastorage.EventEntry {
	events := MakeBlockEvents(eventType, startHeight, endHeight, tag, opts...)
	entries := make([]*metastorage.EventEntry, len(events))
	for i, event := range events {
		entries[i] = metastorage.NewEventEntry(eventTag, endEventId+int64(i+1-len(events)), event)
	}
	return entries
}

func getBlock(height uint64, tag uint32, opts ...Option) *api.BlockMetadata {
	options := &builderOptions{
		blockHashFormat: "0x%s",
	}
	for _, opt := range opts {
		opt(options)
	}

	if options.blockSkipped {
		return &api.BlockMetadata{
			Tag:     tag,
			Height:  height,
			Skipped: true,
		}
	}

	hash := fmt.Sprintf(options.blockHashFormat, strconv.FormatInt(int64(height), 16))
	parentHash := fmt.Sprintf(options.blockHashFormat, strconv.FormatInt(int64(height-1), 16))
	objectKeyMain := fmt.Sprintf("%v/%v/%v", tag, height, hash)
	if options.dataCompression == api.Compression_GZIP {
		objectKeyMain += ".gzip"
	}

	return &api.BlockMetadata{
		Tag:           tag,
		Hash:          hash,
		ParentHash:    parentHash,
		Height:        height,
		ParentHeight:  height - 1,
		ObjectKeyMain: objectKeyMain,
		Timestamp:     utils.ToTimestamp(blockTimestamp),
	}
}

func MakeBlockMetadata(height uint64, tag uint32, opts ...Option) *api.BlockMetadata {
	block := getBlock(height, tag, opts...)
	return block
}

func MakeBlockMetadatas(size int, tag uint32, opts ...Option) []*api.BlockMetadata {
	return MakeBlockMetadatasFromStartHeight(0, size, tag, opts...)
}

func MakeBlockMetadatasFromStartHeight(startHeight uint64, size int, tag uint32, opts ...Option) []*api.BlockMetadata {
	blocks := make([]*api.BlockMetadata, size)
	for i := 0; i < size; i++ {
		height := startHeight + uint64(i)
		blocks[i] = MakeBlockMetadata(height, tag, opts...)
	}
	return blocks
}

func MakeBlock(height uint64, tag uint32, opts ...Option) *api.Block {
	blocks := MakeBlocksFromStartHeight(height, 1, tag, opts...)
	return blocks[0]
}

func MakeBlocksFromStartHeight(startHeight uint64, size int, tag uint32, opts ...Option) []*api.Block {
	metadata := MakeBlockMetadatasFromStartHeight(startHeight, size, tag, opts...)
	blocks := make([]*api.Block, size)
	for i := 0; i < size; i++ {
		header := Header{
			Hash:       metadata[i].Hash,
			ParentHash: metadata[i].ParentHash,
			Number:     hexutil.EncodeUint64(metadata[i].Height),
			Timestamp:  hexutil.EncodeUint64(blockTimestamp),
		}
		headerData, err := json.Marshal(header)
		if err != nil {
			panic(err)
		}

		blocks[i] = &api.Block{
			Blockchain: common.Blockchain_BLOCKCHAIN_ETHEREUM,
			Network:    common.Network_NETWORK_ETHEREUM_MAINNET,
			Metadata:   metadata[i],
			Blobdata: &api.Block_Ethereum{
				Ethereum: &api.EthereumBlobdata{
					Header: headerData,
				},
			},
		}
	}

	return blocks
}

func WithBlockHashFormat(format string) Option {
	return func(opts *builderOptions) {
		opts.blockHashFormat = format
	}
}

func WithDataCompression(compression api.Compression) Option {
	return func(opts *builderOptions) {
		opts.dataCompression = compression
	}
}

func WithBlockSkipped() Option {
	return func(opts *builderOptions) {
		opts.blockSkipped = true
	}
}
