package parser

import (
	"context"
	"encoding/json"
	"math/big"

	"github.com/golang/protobuf/ptypes/any"
	"go.uber.org/zap"
	"golang.org/x/xerrors"

	rosetta "github.com/coinbase/chainstorage/protos/coinbase/crypto/rosetta/types"

	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/utils/log"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

const (
	// https://burn.polygon.technology/
	burntContract = "0x70bca57f4579f58670ab2d18ef16e02c17553c38"
)

type (
	polygonRosettaParserImpl struct {
		logger       *zap.Logger
		config       *config.Config
		nativeParser NativeParser
	}
)

var (
	polygonRosettaCurrency = rosetta.Currency{
		Symbol:   "MATIC",
		Decimals: 18,
	}
)

func NewPolygonRosettaParser(params ParserParams, nativeParser NativeParser, opts ...ParserFactoryOption) (RosettaParser, error) {
	return &polygonRosettaParserImpl{
		logger:       log.WithPackage(params.Logger),
		config:       params.Config,
		nativeParser: nativeParser,
	}, nil
}

func (p *polygonRosettaParserImpl) ParseBlock(ctx context.Context, rawBlock *api.Block) (*api.RosettaBlock, error) {
	nativeBlock, err := p.nativeParser.ParseBlock(ctx, rawBlock)
	if err != nil {
		return nil, xerrors.Errorf("failed to parse block into native format: %w", err)
	}

	block := nativeBlock.GetEthereum()
	if block == nil {
		return nil, xerrors.New("failed to find polygon block")
	}

	blockIdentifier := &rosetta.BlockIdentifier{
		Index: int64(rawBlock.GetMetadata().GetHeight()),
		Hash:  rawBlock.GetMetadata().GetHash(),
	}

	parentBlockIndentifier := &rosetta.BlockIdentifier{
		Index: int64(rawBlock.GetMetadata().GetParentHeight()),
		Hash:  rawBlock.GetMetadata().GetParentHash(),
	}

	transactions, err := p.getRosettaTransactions(block, rawBlock.GetEthereum())
	if err != nil {
		return nil, xerrors.Errorf("failed to parse block transactions: %w", err)
	}

	return &api.RosettaBlock{
		Block: &rosetta.Block{
			BlockIdentifier:       blockIdentifier,
			ParentBlockIdentifier: parentBlockIndentifier,
			Timestamp:             block.GetHeader().Timestamp,
			Transactions:          transactions,
			Metadata:              nil,
		},
	}, nil
}

func (p *polygonRosettaParserImpl) getRosettaTransactions(block *api.EthereumBlock, blobData *api.EthereumBlobdata) ([]*rosetta.Transaction, error) {
	var author string
	if err := json.Unmarshal(blobData.GetPolygon().GetAuthor(), &author); err != nil {
		return nil, xerrors.Errorf("unexpected author: %w", err)
	}

	return getRosettaTransactionsFromCSBlock(block, blobData, author, &polygonRosettaCurrency, p.feeOps)
}

// Ref: https://github.com/maticnetwork/polygon-rosetta/blob/0b60290550521cce0088ad68e857b752d01be869/polygon/client.go#L974
func (p *polygonRosettaParserImpl) feeOps(transaction *api.EthereumTransaction, author string, block *api.EthereumBlock) ([]*rosetta.Operation, error) {
	feeDetails, err := getFeeDetails(transaction, block)
	if err != nil {
		return nil, xerrors.Errorf("failed to calculate polygon fee details: %w", err)
	}

	var feeBurned *big.Int
	var minerEarnedAmount *big.Int
	if feeDetails.maxFeePerGas != nil {
		minerEarnedAmount = new(big.Int).Mul(feeDetails.priorityFeePerGas, feeDetails.gasUsed)
		feeBurned = new(big.Int).Sub(feeDetails.feeAmount, minerEarnedAmount)
	} else {
		minerEarnedAmount = feeDetails.feeAmount
	}

	rOps := []*rosetta.Operation{
		{
			OperationIdentifier: &rosetta.OperationIdentifier{
				Index: 0,
			},
			Type:   opTypeFee,
			Status: opStatusSuccess,
			Account: &rosetta.AccountIdentifier{
				Address: transaction.From,
			},
			Amount: &rosetta.Amount{
				Value:    new(big.Int).Neg(minerEarnedAmount).String(),
				Currency: &polygonRosettaCurrency,
			},
			Metadata: map[string]*any.Any{},
		},
		{
			OperationIdentifier: &rosetta.OperationIdentifier{
				Index: 1,
			},
			RelatedOperations: []*rosetta.OperationIdentifier{
				{
					Index: 0,
				},
			},
			Type:   opTypeFee,
			Status: opStatusSuccess,
			Account: &rosetta.AccountIdentifier{
				Address: author,
			},
			Amount: &rosetta.Amount{
				Value:    minerEarnedAmount.String(),
				Currency: &polygonRosettaCurrency,
			},
			Metadata: map[string]*any.Any{},
		},
	}

	if feeBurned == nil {
		return rOps, nil
	}

	debitOp := &rosetta.Operation{
		OperationIdentifier: &rosetta.OperationIdentifier{
			Index: 2,
		},
		Type:   opTypeFee,
		Status: opStatusSuccess,
		Account: &rosetta.AccountIdentifier{
			Address: transaction.From,
		},
		Amount: &rosetta.Amount{
			Value:    new(big.Int).Neg(feeBurned).String(),
			Currency: &polygonRosettaCurrency,
		},
	}

	creditOp := &rosetta.Operation{
		OperationIdentifier: &rosetta.OperationIdentifier{
			Index: 3,
		},
		RelatedOperations: []*rosetta.OperationIdentifier{
			{
				Index: 2,
			},
		},
		Type:   opTypeFee,
		Status: opStatusSuccess,
		Account: &rosetta.AccountIdentifier{
			Address: burntContract,
		},
		Amount: &rosetta.Amount{
			Value:    feeBurned.String(),
			Currency: &polygonRosettaCurrency,
		},
	}

	return append(rOps, debitOp, creditOp), nil
}
