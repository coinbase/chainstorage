package parser

import (
	"context"
	"encoding/json"

	"go.uber.org/zap"
	"golang.org/x/xerrors"

	sdk "github.com/coinbase/rosetta-sdk-go/types"

	"github.com/coinbase/chainstorage/internal/utils/log"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
	rosetta "github.com/coinbase/chainstorage/protos/coinbase/crypto/rosetta/types"
)

type (
	rosettaNativeParserImpl struct {
		logger *zap.Logger
	}
)

func NewRosettaNativeParser(params ParserParams, opts ...ParserFactoryOption) (NativeParser, error) {
	return &rosettaNativeParserImpl{
		logger: log.WithPackage(params.Logger),
	}, nil
}

func (p *rosettaNativeParserImpl) ParseBlock(ctx context.Context, rawBlock *api.Block) (*api.NativeBlock, error) {
	metadata := rawBlock.GetMetadata()
	if metadata == nil {
		return nil, xerrors.New("metadata not found")
	}

	blobdata := rawBlock.GetRosetta()
	if blobdata == nil {
		return nil, xerrors.New("blobdata not found for rosetta")
	}

	rosettaBlock, err := p.parseRosettaBlock(blobdata.Header)
	if err != nil {
		return nil, xerrors.Errorf("failed to parse rosetta block: %w", err)
	}

	otherTransactions, err := p.parseOtherTransactions(blobdata.OtherTransactions)
	if err != nil {
		return nil, xerrors.Errorf("failed to parse rosetta other transactions: %w", err)
	}

	if len(otherTransactions) > 0 {
		if rosettaBlock == nil {
			return nil, xerrors.Errorf("rosetta block is nil while having other transactions")
		}
		rosettaBlock.Transactions = append(rosettaBlock.Transactions, otherTransactions...)
	}

	return &api.NativeBlock{
		Blockchain:      rawBlock.Blockchain,
		Network:         rawBlock.Network,
		Tag:             metadata.Tag,
		Hash:            metadata.Hash,
		ParentHash:      metadata.ParentHash,
		Height:          metadata.Height,
		ParentHeight:    metadata.ParentHeight,
		Timestamp:       rosettaBlock.GetTimestamp(),
		NumTransactions: uint64(len(rosettaBlock.GetTransactions())),
		Block: &api.NativeBlock_Rosetta{
			Rosetta: rosettaBlock,
		},
	}, nil
}

func (p *rosettaNativeParserImpl) parseRosettaBlock(data []byte) (*rosetta.Block, error) {
	var blockResponse sdk.BlockResponse
	if err := json.Unmarshal(data, &blockResponse); err != nil {
		return nil, xerrors.Errorf("failed to parse block response: %w", err)
	}

	block := blockResponse.Block
	if block == nil {
		return nil, nil
	}

	rosettaBlock, err := rosetta.FromSDKBlock(block)
	if err != nil {
		return nil, xerrors.Errorf("failed to parse rosetta sdk block: %w", err)
	}

	return rosettaBlock, nil
}

func (p *rosettaNativeParserImpl) parseOtherTransactions(data [][]byte) ([]*rosetta.Transaction, error) {
	txs := make([]*rosetta.Transaction, len(data))
	for i, rawTx := range data {
		var transactionResponse sdk.BlockTransactionResponse
		if err := json.Unmarshal(rawTx, &transactionResponse); err != nil {
			return nil, xerrors.Errorf("failed to parse block transaction response: %w", err)
		}

		transaction := transactionResponse.Transaction
		tx, err := rosetta.FromSDKTransaction(transaction)
		if err != nil {
			return nil, xerrors.Errorf("failed to parse transaction of other transactions: %w", err)
		}

		txs[i] = tx
	}
	return txs, nil
}
