package ethereum

import (
	"context"
	"errors"
	"fmt"
	"math/big"

	"go.uber.org/zap"

	"github.com/coinbase/chainstorage/internal/blockchain/parser/ethereum/types"
	"github.com/coinbase/chainstorage/internal/blockchain/parser/internal"
	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/utils/log"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
	rosetta "github.com/coinbase/chainstorage/protos/coinbase/crypto/rosetta/types"
)

type (
	baseRosettaParserImpl struct {
		logger       *zap.Logger
		config       *config.Config
		nativeParser internal.NativeParser
	}
)

const (
	baseL1ToL2DepositType = uint64(126)
	MintOpType            = "MINT"
)

// Optimism Predeploy Addresses (represented as 0x-prefixed hex string)
// See [PredeployedContracts] for more information.
//
// [PredeployedContracts]: https://github.com/ethereum-optimism/optimism/blob/develop/specs/predeploys.md
var (
	// The BaseFeeVault predeploy receives the basefees on L2.
	// The basefee is not burnt on L2 like it is on L1.
	// Once the contract has received a certain amount of fees,
	// the ETH can be permissionlessly withdrawn to an immutable address on L1.
	BaseFeeVault = "0x4200000000000000000000000000000000000019"

	// The L1FeeVault predeploy receives the L1 portion of the transaction fees.
	// Once the contract has received a certain amount of fees,
	// the ETH can be permissionlessly withdrawn to an immutable address on L1.
	L1FeeVault = "0x420000000000000000000000000000000000001a"
)

func NewBaseRosettaParser(params internal.ParserParams, nativeParser internal.NativeParser, opts ...internal.ParserFactoryOption) (internal.RosettaParser, error) {
	// Base shares the same data schema as Ethereum since it is an internal EVM chain.
	options := &ethereumParserOptions{
		nodeType: types.EthereumNodeType_ARCHIVAL,
	}
	for _, opt := range opts {
		opt(options)
	}

	return &baseRosettaParserImpl{
		logger:       log.WithPackage(params.Logger),
		config:       params.Config,
		nativeParser: nativeParser,
	}, nil
}

func (p *baseRosettaParserImpl) ParseBlock(ctx context.Context, rawBlock *api.Block) (*api.RosettaBlock, error) {
	nativeBlock, err := p.nativeParser.ParseBlock(ctx, rawBlock)

	if err != nil {
		return nil, fmt.Errorf("failed to parse block into native format: %w", err)
	}

	block := nativeBlock.GetEthereum()
	if block == nil {
		return nil, errors.New("failed to find ethereum block")
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
		return nil, fmt.Errorf("failed to parse block transactions: %w", err)
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

func (p *baseRosettaParserImpl) getRosettaTransactions(block *api.EthereumBlock, blobData *api.EthereumBlobdata) ([]*rosetta.Transaction, error) {
	return getRosettaTransactionsFromCSBlock(block, blobData, block.Header.Miner, &ethereumRosettaCurrency, p.feeOps, p.MintOps)
}

// feeOps generates the fee operations for an transaction.
// In summary:
// * Deposit transaction pays no fees
// * the from address pays the L2 execution fee + L1 data fee
// * the priority fee is paid to the miner, i.e. the SequencerFeeVault
// * there's no burn fee, the burn fee is paid to the BaseFeeValut
// * the L1 data fee is paid to the L1FeeValut
// https://docs.google.com/document/d/1K8vcGV-mTgwhWW_UAIZ-X5z_v7fbaptbSR4B89wGvcY/edit#
// See https://community.optimism.io/docs/developers/build/transaction-fees/#understanding-the-basics
// See https://community.optimism.io/docs/developers/bedrock/differences/#eip-1559
// see https://github.com/ethereum-optimism/optimism/blob/develop/specs/predeploys.md
func (p *baseRosettaParserImpl) feeOps(transaction *api.EthereumTransaction, miner string, block *api.EthereumBlock) ([]*rosetta.Operation, error) {
	if IsDepositTx(transaction) {
		return nil, nil
	}

	feeDetails, err := getFeeDetails(transaction, block)
	if err != nil {
		return nil, fmt.Errorf("failed to calculate base fee details: %w", err)
	}

	sequencerFeeAmount := feeDetails.feeAmount
	if feeDetails.feeBurned != nil {
		sequencerFeeAmount = new(big.Int).Sub(feeDetails.feeAmount, feeDetails.feeBurned)
	}

	fromFeeAmount := new(big.Int).Neg(feeDetails.feeAmount)
	l1FeeAmount := ExtractL1Fee(transaction)
	if l1FeeAmount != nil {
		fromFeeAmount = fromFeeAmount.Sub(fromFeeAmount, l1FeeAmount)
	}

	relatedOps := []*rosetta.OperationIdentifier{
		{
			Index: 0,
		},
	}

	ops := []*rosetta.Operation{
		generateOp(0, nil, opTypeFee, transaction.From, fromFeeAmount),
		generateOp(1, relatedOps, opTypeFee, miner, sequencerFeeAmount),
	}

	if feeDetails.feeBurned != nil {
		ops = append(ops, generateOp(int64(len(ops)), relatedOps, opTypeFee, BaseFeeVault, feeDetails.feeBurned))
	}

	if l1FeeAmount != nil {
		ops = append(ops, generateOp(int64(len(ops)), relatedOps, opTypeFee, L1FeeVault, l1FeeAmount))
	}

	return ops, nil
}

// https://github.com/ethereum-optimism/optimism/blob/develop/specs/deposits.md
// Ref: https://github.com/Inphi/optimism-rosetta/pull/77/files#diff-97c9722cf92f6dbff3f3280715701b484d2b69d0b67ba9b497c08e3a6d120bdcR34
// The following code is inspired from the optimism rosetta pr with some modifications. The deposits.md explains the L1 deposit operations.
// MintOps constructs a list of [RosettaTypes.Operation]s for a Deposit (tx_type=126) or "mint" transaction.
func (p *baseRosettaParserImpl) MintOps(tx *api.EthereumTransaction, startIndex int) ([]*rosetta.Operation, error) {
	if !IsDepositTx(tx) || tx.GetMint() == "0" || tx.GetMint() == "" {
		return nil, nil
	}

	mintAmount, err := internal.BigInt(tx.GetMint())
	if err != nil {
		return nil, err
	}

	return []*rosetta.Operation{generateOp(int64(startIndex), nil, MintOpType, tx.From, mintAmount)}, nil
}

// IsDepositTx returns true if the transaction is a deposit tx type.
func IsDepositTx(tx *api.EthereumTransaction) bool {
	return tx.GetType() == baseL1ToL2DepositType
}

// ExtractL1Fee attempts to extract L1Fee from the transaction
// L1Fee = L1GasPrice * ( L1GasUsed + 2100) * L1FeeScalar
// See https://community.optimism.io/docs/developers/build/transaction-fees/#the-l1-data-fee
func ExtractL1Fee(transaction *api.EthereumTransaction) *big.Int {
	l1FeeInfo := transaction.GetReceipt().GetL1FeeInfo()
	if l1FeeInfo != nil {
		return new(big.Int).SetUint64(l1FeeInfo.L1Fee)
	}
	return nil
}

func generateOp(opIndex int64, relatedOps []*rosetta.OperationIdentifier, opType string, address string, value *big.Int) *rosetta.Operation {
	return &rosetta.Operation{
		OperationIdentifier: &rosetta.OperationIdentifier{
			Index: opIndex,
		},
		RelatedOperations: relatedOps,
		Type:              opType,
		Status:            opStatusSuccess,
		Account: &rosetta.AccountIdentifier{
			Address: address,
		},
		Amount: &rosetta.Amount{
			Value:    value.String(),
			Currency: &ethereumRosettaCurrency,
		},
		Metadata: nil,
	}
}
