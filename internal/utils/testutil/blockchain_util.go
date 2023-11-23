package testutil

import (
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/ethereum/go-ethereum/common/hexutil"

	"github.com/coinbase/chainstorage/internal/storage/metastorage/model"
	"github.com/coinbase/chainstorage/internal/utils/utils"
	"github.com/coinbase/chainstorage/protos/coinbase/c3/common"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
	rosetta "github.com/coinbase/chainstorage/protos/coinbase/crypto/rosetta/types"
)

type (
	Option func(*builderOptions)

	builderOptions struct {
		blockHashFormat string
		dataCompression api.Compression
		blockSkipped    bool
		timestamp       int64
	}

	Header struct {
		Hash       string `json:"hash"`
		ParentHash string `json:"parentHash"`
		Number     string `json:"number"`
		Timestamp  string `json:"timestamp"`
	}
)

const (
	blockTimestamp  = 0x5fbd2fb9
	transactionHash = "fake_txn_hash"
)

func MakeBlockEvent(eventType api.BlockchainEvent_Type, height uint64, tag uint32, opts ...Option) *model.BlockEvent {
	block := getBlock(height, tag, opts...)
	return model.NewBlockEvent(eventType, block.Hash, block.ParentHash, height, tag, block.Skipped, block.GetTimestamp().GetSeconds())
}

func MakeBlockEvents(eventType api.BlockchainEvent_Type, startHeight uint64, endHeight uint64, tag uint32, opts ...Option) []*model.BlockEvent {
	events := make([]*model.BlockEvent, 0, endHeight-startHeight)
	for i := startHeight; i < endHeight; i++ {
		events = append(events, MakeBlockEvent(eventType, i, tag, opts...))
	}
	return events
}

func MakeBlockEventEntries(eventType api.BlockchainEvent_Type, eventTag uint32, endEventId int64, startHeight uint64, endHeight uint64, tag uint32, opts ...Option) []*model.EventEntry {
	events := MakeBlockEvents(eventType, startHeight, endHeight, tag, opts...)
	entries := make([]*model.EventEntry, len(events))
	for i, event := range events {
		entries[i] = model.NewEventEntry(eventTag, endEventId+int64(i+1-len(events)), event)
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
	timestamp := utils.ToTimestamp(blockTimestamp)
	if options.timestamp != 0 {
		timestamp = utils.ToTimestamp(options.timestamp)
	}

	return &api.BlockMetadata{
		Tag:           tag,
		Hash:          hash,
		ParentHash:    parentHash,
		Height:        height,
		ParentHeight:  height - 1,
		ObjectKeyMain: objectKeyMain,
		Timestamp:     timestamp,
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

func MakeBlocksWithTransactionsFromStartHeight(startHeight uint64, size int, tag uint32, transactionSize int, opts ...Option) []*api.Block {
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
			TransactionMetadata: &api.TransactionMetadata{
				Transactions: MakeTransactionHashList(transactionSize),
			},
			Blobdata: &api.Block_Ethereum{
				Ethereum: &api.EthereumBlobdata{
					Header: headerData,
				},
			},
		}
	}

	return blocks
}

func MakeTransactionHashList(size int) []string {
	transactions := make([]string, size)
	for i := 0; i < size; i++ {
		transactionHash := fmt.Sprintf("transactionHash%d", i)
		transactions[i] = transactionHash
	}

	return transactions
}

func MakeTransactionsFromStartHeight(startHeight int, size int, tag uint32) []*model.Transaction {
	transactions := make([]*model.Transaction, size)
	for i := startHeight; i < size; i++ {
		transactionHash := fmt.Sprintf("transactionHash%d", i)
		blockHash := fmt.Sprintf("blockHash%d", i)

		transaction := &model.Transaction{
			Hash:        transactionHash,
			BlockHash:   blockHash,
			BlockNumber: uint64(i),
			BlockTag:    tag,
		}
		transactions[i] = transaction
	}

	return transactions
}

func MakeNativeBlock(height uint64, tag uint32, opts ...Option) *api.NativeBlock {
	meta := getBlock(height, tag, opts...)
	return &api.NativeBlock{
		Blockchain:      common.Blockchain_BLOCKCHAIN_ETHEREUM,
		Network:         common.Network_NETWORK_ETHEREUM_MAINNET,
		Tag:             meta.Tag,
		Hash:            meta.Hash,
		ParentHash:      meta.ParentHash,
		Height:          meta.Height,
		Timestamp:       meta.Timestamp,
		NumTransactions: 1,
		ParentHeight:    meta.ParentHeight,
		Skipped:         meta.Skipped,
		Block: &api.NativeBlock_Ethereum{
			Ethereum: &api.EthereumBlock{
				Header: &api.EthereumHeader{},
				Transactions: []*api.EthereumTransaction{
					{
						Hash: transactionHash,
					},
				},
			},
		},
	}
}

func MakeRosettaBlock(height uint64, tag uint32, opts ...Option) *api.RosettaBlock {
	meta := getBlock(height, tag, opts...)
	return &api.RosettaBlock{
		Block: &rosetta.Block{
			BlockIdentifier: &rosetta.BlockIdentifier{
				Index: int64(meta.Height),
				Hash:  meta.Hash,
			},
			ParentBlockIdentifier: &rosetta.BlockIdentifier{
				Index: int64(meta.ParentHeight),
				Hash:  meta.ParentHash,
			},
			Timestamp: meta.Timestamp,
			Transactions: []*rosetta.Transaction{
				{
					TransactionIdentifier: &rosetta.TransactionIdentifier{
						Hash: transactionHash,
					},
				},
			},
		},
	}
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

func WithTimestamp(timestamp int64) Option {
	return func(opts *builderOptions) {
		opts.timestamp = timestamp
	}
}
