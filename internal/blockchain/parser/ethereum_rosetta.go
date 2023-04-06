package parser

import (
	"context"
	"fmt"
	"math/big"
	"strconv"
	"strings"

	"github.com/golang/protobuf/ptypes/any"
	"go.uber.org/zap"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/blockchain/bootstrap"

	"github.com/coinbase/chainstorage/internal/blockchain/types"
	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/utils/log"
	"github.com/coinbase/chainstorage/protos/coinbase/c3/common"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
	rosetta "github.com/coinbase/chainstorage/protos/coinbase/crypto/rosetta/types"
)

const (
	// UnclesRewardMultiplier is the uncle reward multiplier.
	// Ref: https://github.com/coinbase/rosetta-ethereum/blob/79a9b97d0a3ed08ae151a3f313aa57b88dd128a4/ethereum/types.go#L108
	UnclesRewardMultiplier = 32
	// MaxUncleDepth is the maximum depth for
	// an uncle to be rewarded.
	// Ref: https://github.com/coinbase/rosetta-ethereum/blob/79a9b97d0a3ed08ae151a3f313aa57b88dd128a4/ethereum/types.go#L112
	MaxUncleDepth = int64(8)
)

type (
	ethereumRosettaParserImpl struct {
		logger       *zap.Logger
		config       *config.Config
		nativeParser NativeParser
	}
)

var (
	ethereumRosettaCurrency = rosetta.Currency{
		Symbol:   "ETH",
		Decimals: 18,
	}

	FrontierBlockReward       = int64(5e+18) // Block reward in wei for successfully mining a block
	ByzantiumBlockReward      = int64(3e+18) // Block reward in wei for successfully mining a block upward from Byzantium
	ConstantinopleBlockReward = int64(2e+18) // Block reward in wei for successfully mining a block upward from Constantinople
)

func NewEthereumRosettaParser(params ParserParams, nativeParser NativeParser, opts ...ParserFactoryOption) (RosettaParser, error) {
	// TODO: add support for more node types so that other chains can build rosetta parsers backed by this parser
	options := &ethereumParserOptions{
		nodeType: types.EthereumNodeType_ARCHIVAL,
	}
	for _, opt := range opts {
		opt(options)
	}

	return &ethereumRosettaParserImpl{
		logger:       log.WithPackage(params.Logger),
		config:       params.Config,
		nativeParser: nativeParser,
	}, nil
}

func (p *ethereumRosettaParserImpl) ParseBlock(ctx context.Context, rawBlock *api.Block) (*api.RosettaBlock, error) {
	nativeBlock, err := p.nativeParser.ParseBlock(ctx, rawBlock)

	if err != nil {
		return nil, xerrors.Errorf("failed to parse block into native format: %w", err)
	}

	block := nativeBlock.GetEthereum()
	if block == nil {
		return nil, xerrors.New("failed to find ethereum block")
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

	if nativeBlock.Height == 0 {
		// For ethereum mainnet, we use this allocation file as reference:
		// https://github.com/coinbase/rosetta-ethereum/blob/master/rosetta-cli-conf/mainnet/bootstrap_balances.json
		genesisAllocation, err := bootstrap.GenerateGenesisAllocations(p.config.Chain.Network)
		if err != nil {
			return nil, err
		}

		genesisTransactions, err := p.getGenesisTransactions(genesisAllocation)
		if err != nil {
			return nil, xerrors.Errorf("failed to generate genesis transactions: %w", err)
		}
		transactions = append(transactions, genesisTransactions...)
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

func (p *ethereumRosettaParserImpl) getRosettaTransactions(block *api.EthereumBlock, blobData *api.EthereumBlobdata) ([]*rosetta.Transaction, error) {
	rosettaTxs, err := getRosettaTransactionsFromCSBlock(block, blobData, block.Header.Miner, &ethereumRosettaCurrency, p.feeOps, p.tokenTransferOpsFn)
	if err != nil {
		return nil, err
	}

	rosettaTransactions := make([]*rosetta.Transaction, len(rosettaTxs)+1)

	// compute the reward transaction
	rosettaTransactions[0] = p.createBlockRewardTransaction(block)

	for i := range rosettaTxs {
		rosettaTransactions[i+1] = rosettaTxs[i]
	}

	return rosettaTransactions, nil
}

// Ref: https://github.com/coinbase/rosetta-ethereum/blob/3a9db2f08ab5fae90cd8d08876ba69a9097e29f5/ethereum/client.go#L948
func (p *ethereumRosettaParserImpl) miningReward(blockHeight uint64) int64 {
	if blockHeight == 0 {
		return 0
	}

	mergeHeight, ok := mergeHeights[p.config.Chain.Network]
	if !ok {
		return 0
	}

	var blockReward int64

	switch p.config.Chain.Network {
	case common.Network_NETWORK_ETHEREUM_MAINNET:
		blockReward = FrontierBlockReward

		if blockHeight >= ethByzantiumHardForkHeight {
			blockReward = ByzantiumBlockReward
		}

		if blockHeight >= ethConstantinopleForkHeight {
			blockReward = ConstantinopleBlockReward
		}

		// No more mining reward after ETH merge
		if blockHeight >= mergeHeight {
			blockReward = 0
		}
	case common.Network_NETWORK_ETHEREUM_GOERLI:
		// for goerli, initial block reward is 2 ETH
		blockReward = ConstantinopleBlockReward

		// No more mining reward after ETH merge
		if blockHeight >= mergeHeight {
			blockReward = 0
		}
	}

	return blockReward
}

// Ref: https://github.com/coinbase/rosetta-ethereum/blob/3a9db2f08ab5fae90cd8d08876ba69a9097e29f5/ethereum/client.go#L967
func (p *ethereumRosettaParserImpl) createBlockRewardTransaction(block *api.EthereumBlock) *rosetta.Transaction {
	var ops []*rosetta.Operation
	miningReward := p.miningReward(block.Header.Number)

	// Calculate miner rewards
	minerReward := miningReward
	numUncles := len(block.Uncles)
	if numUncles > 0 {
		reward := new(big.Float)
		uncleReward := float64(numUncles) / UnclesRewardMultiplier
		rewardFloat := reward.Mul(big.NewFloat(uncleReward), big.NewFloat(float64(miningReward)))
		rewardInt, _ := rewardFloat.Int64()
		minerReward += rewardInt
	}

	miningRewardOp := &rosetta.Operation{
		OperationIdentifier: &rosetta.OperationIdentifier{
			Index: 0,
		},
		Type:   opTypeMinerReward,
		Status: opStatusSuccess,
		Account: &rosetta.AccountIdentifier{
			Address: block.GetHeader().GetMiner(),
		},
		Amount: &rosetta.Amount{
			Value:    strconv.FormatInt(minerReward, 10),
			Currency: &ethereumRosettaCurrency,
		},
		Metadata: map[string]*any.Any{},
	}

	ops = append(ops, miningRewardOp)

	// Calculate uncle rewards
	for _, b := range block.Uncles {
		uncleMiner := b.Miner
		uncleBlock := int64(b.Number)
		uncleRewardBlock := new(
			big.Int,
		).Mul(
			big.NewInt(uncleBlock+MaxUncleDepth-int64(block.Header.Number)),
			big.NewInt(miningReward/MaxUncleDepth),
		)

		uncleRewardOp := &rosetta.Operation{
			OperationIdentifier: &rosetta.OperationIdentifier{
				Index: int64(len(ops)),
			},
			Type:   opTypeUncleReward,
			Status: opStatusSuccess,
			Account: &rosetta.AccountIdentifier{
				Address: uncleMiner,
			},
			Amount: &rosetta.Amount{
				Value:    uncleRewardBlock.String(),
				Currency: &ethereumRosettaCurrency,
			},
			Metadata: map[string]*any.Any{},
		}
		ops = append(ops, uncleRewardOp)
	}

	return &rosetta.Transaction{
		TransactionIdentifier: &rosetta.TransactionIdentifier{
			Hash: block.Header.Hash,
		},
		Operations: ops,
		Metadata:   map[string]*any.Any{},
	}
}

func (p *ethereumRosettaParserImpl) feeOps(transaction *api.EthereumTransaction, miner string, block *api.EthereumBlock) ([]*rosetta.Operation, error) {
	feeDetails, err := getFeeDetails(transaction, block)
	if err != nil {
		return nil, xerrors.Errorf("failed to calculate ethereum fee details: %w", err)
	}
	minerEarnedAmount := feeDetails.feeAmount
	if feeDetails.feeBurned != nil {
		minerEarnedAmount = new(big.Int).Sub(feeDetails.feeAmount, feeDetails.feeBurned)
	}

	ops := []*rosetta.Operation{
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
				Currency: &ethereumRosettaCurrency,
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
				Address: miner,
			},
			Amount: &rosetta.Amount{
				Value:    minerEarnedAmount.String(),
				Currency: &ethereumRosettaCurrency,
			},
			Metadata: map[string]*any.Any{},
		},
	}

	if feeDetails.feeBurned == nil {
		return ops, nil
	}

	burntOp := &rosetta.Operation{
		OperationIdentifier: &rosetta.OperationIdentifier{
			Index: 2,
		},
		Type:   opTypeFee,
		Status: opStatusSuccess,
		Account: &rosetta.AccountIdentifier{
			Address: transaction.From,
		},
		Amount: &rosetta.Amount{
			Value:    new(big.Int).Neg(feeDetails.feeBurned).String(),
			Currency: &ethereumRosettaCurrency,
		},
	}
	ops = append(ops, burntOp)

	return ops, nil
}

func (p *ethereumRosettaParserImpl) tokenTransferOpsFn(transaction *api.EthereumTransaction, startIndex int) ([]*rosetta.Operation, error) {
	return []*rosetta.Operation{}, nil
}

func (p *ethereumRosettaParserImpl) getGenesisTransactions(allocations []*bootstrap.GenesisAllocation) ([]*rosetta.Transaction, error) {
	var genesisTxns []*rosetta.Transaction
	for _, allo := range allocations {
		address := allo.AccountIdentifier.Address
		_, err := checksumAddress(address)
		if err != nil {
			return nil, xerrors.Errorf("%s is not a valid address", address)
		}
		addressLower := strings.ToLower(address)

		genesisOp := &rosetta.Operation{
			OperationIdentifier: &rosetta.OperationIdentifier{
				Index: 0,
			},
			Type:   opTypeGenesis,
			Status: opStatusSuccess,
			Account: &rosetta.AccountIdentifier{
				Address: addressLower,
			},
			Amount: &rosetta.Amount{
				Value:    allo.Value,
				Currency: &ethereumRosettaCurrency,
			},
			Metadata: map[string]*any.Any{},
		}

		txnIdentifier := fmt.Sprintf("GENESIS_%s", addressLower[2:])
		txn := &rosetta.Transaction{
			TransactionIdentifier: &rosetta.TransactionIdentifier{
				Hash: txnIdentifier,
			},
			Operations: []*rosetta.Operation{genesisOp},
			Metadata:   map[string]*any.Any{},
		}

		genesisTxns = append(genesisTxns, txn)
	}

	return genesisTxns, nil
}
