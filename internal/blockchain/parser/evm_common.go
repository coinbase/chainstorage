package parser

import (
	"encoding/json"
	"math/big"

	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/golang/protobuf/ptypes/any"
	"golang.org/x/xerrors"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/coinbase/chainstorage/protos/coinbase/c3/common"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
	rosetta "github.com/coinbase/chainstorage/protos/coinbase/crypto/rosetta/types"
)

const (
	// EIP-2718 transaction types.
	legacyTxType  = uint64(0)
	eip1559TxType = uint64(2)

	// ethByzantiumHardForkHeight is the Byzantium hard fork height which changed the tx receipt status format
	ethByzantiumHardForkHeight  = 4_370_000
	ethConstantinopleForkHeight = 7_280_000

	opStatusFailure       = "FAILURE"
	opStatusSuccess       = "SUCCESS"
	opTypeUncleReward     = "UNCLE_REWARD"
	opTypeDestruct        = "DESTRUCT"
	opTypeFee             = "FEE"
	opTypeMinerReward     = "MINER_REWARD"
	opTypePayment         = "PAYMENT"
	traceTypeCall         = "CALL"
	traceTypeSelfDestruct = "SELFDESTRUCT"
	traceTypeCreate       = "CREATE"

	// ContractAddressKey is the key used to denote the contract address
	// for a token, provided via Currency metadata.
	ContractAddressKey string = "token_address"
)

var (
	mergeHeights = map[common.Network]uint64{
		common.Network_NETWORK_ETHEREUM_MAINNET: 15_537_394,
		common.Network_NETWORK_ETHEREUM_GOERLI:  7_382_819, // https://goerli.etherscan.io/block/7382819
	}
)

type feeDetails struct {
	maxPriorityFeePerGas *big.Int
	maxFeePerGas         *big.Int
	priorityFeePerGas    *big.Int
	effectiveFeePerGas   *big.Int
	feeAmount            *big.Int
	gasUsed              *big.Int
	feeBurned            *big.Int
}

func getFeeDetails(transaction *api.EthereumTransaction, block *api.EthereumBlock) (*feeDetails, error) {
	// 1559 transactions use the block base fee, the priority fee, and the max priority fee
	// to compute the effective fee per gas.
	var maxPriorityFeePerGas, maxFeePerGas, priorityFeePerGas, effectiveFeePerGas, feeBurn *big.Int
	feeAmount := new(big.Int)
	gasUsed := big.NewInt(int64(transaction.Receipt.GasUsed))
	txType := transaction.GetType()

	if txType == eip1559TxType {
		if transaction.GetOptionalMaxPriorityFeePerGas() == nil {
			return nil, xerrors.Errorf("Miss maxPriorityFeePerGas for transaction %v", transaction.Hash)
		}
		maxPriorityFeePerGas = big.NewInt(int64(transaction.GetMaxPriorityFeePerGas()))

		if transaction.GetOptionalMaxFeePerGas() == nil {
			return nil, xerrors.Errorf("Miss maxFeePerGas for transaction %v", transaction.Hash)
		}
		maxFeePerGas = big.NewInt(int64(transaction.GetMaxFeePerGas()))

		if transaction.GetOptionalPriorityFeePerGas() == nil {
			return nil, xerrors.Errorf("Miss priorityFeePerGas for transaction %v", transaction.Hash)
		}
		priorityFeePerGas = big.NewInt(int64(transaction.GetPriorityFeePerGas()))

		effectiveFeePerGas = big.NewInt(int64(transaction.Receipt.EffectiveGasPrice))
		feeAmount.Mul(gasUsed, effectiveFeePerGas)
	} else {
		gasPrice := big.NewInt(int64(transaction.GasPrice))
		feeAmount.Mul(gasUsed, gasPrice)
	}

	if block.GetHeader().GetOptionalBaseFeePerGas() != nil {
		baseFeePerGas := new(big.Int).SetUint64(block.GetHeader().GetBaseFeePerGas())
		feeBurn = new(big.Int).Mul(gasUsed, baseFeePerGas)
	}

	return &feeDetails{
		maxPriorityFeePerGas: maxPriorityFeePerGas,
		maxFeePerGas:         maxFeePerGas,
		priorityFeePerGas:    priorityFeePerGas,
		effectiveFeePerGas:   effectiveFeePerGas,
		feeAmount:            feeAmount,
		gasUsed:              gasUsed,
		feeBurned:            feeBurn,
	}, nil
}

// Ref: https://github.com/coinbase/rosetta-ethereum/blob/3a9db2f08ab5fae90cd8d08876ba69a9097e29f5/ethereum/client.go#L569
func convertCSTransactionToRosettaOps(transaction *api.EthereumTransaction, startIndex int, currency *rosetta.Currency) ([]*rosetta.Operation, error) {
	var ops []*rosetta.Operation
	flattenedTraces := transaction.FlattenedTraces
	if len(flattenedTraces) == 0 {
		return ops, nil
	}

	destroyedAccounts := map[string]*big.Int{}

	for _, trace := range flattenedTraces {
		metadata := map[string]*any.Any{}
		opStatus := opStatusSuccess
		if len(trace.Error) > 0 {
			opStatus = opStatusFailure
			anyError, err := anypb.New(structpb.NewStringValue(trace.Error))
			if err != nil {
				return nil, xerrors.Errorf("failed to convert trace error to proto-any: %w", err)
			}
			metadata["error"] = anyError
		}

		traceValue, ok := new(big.Int).SetString(trace.Value, 10)
		if !ok {
			return nil, xerrors.Errorf("invalid trace value [%s]", trace.Value)
		}

		zeroValue := traceValue.Sign() == 0
		shouldAdd := !zeroValue || trace.TraceType != traceTypeCall

		if shouldAdd {
			var amount *rosetta.Amount
			if zeroValue {
				amount = nil
			} else {
				amount = &rosetta.Amount{
					Value:    new(big.Int).Neg(traceValue).String(),
					Currency: currency,
				}
				_, destroyed := destroyedAccounts[trace.From]
				if destroyed && opStatus == opStatusSuccess {
					destroyedAccounts[trace.From] = new(big.Int).Sub(destroyedAccounts[trace.From], traceValue)
				}
			}

			fromOp := &rosetta.Operation{
				OperationIdentifier: &rosetta.OperationIdentifier{
					Index: int64(len(ops) + startIndex),
				},
				Type:   trace.Type,
				Status: opStatus,
				Account: &rosetta.AccountIdentifier{
					Address: trace.From,
				},
				Amount:   amount,
				Metadata: metadata,
			}

			ops = append(ops, fromOp)
		}

		// Add to destroyed accounts if SELFDESTRUCT
		// and overwrite existing balance.
		if trace.TraceType == traceTypeSelfDestruct {
			destroyedAccounts[trace.From] = new(big.Int)

			// If destination of of SELFDESTRUCT is self,
			// we should skip. In the EVM, the balance is reset
			// after the balance is increased on the destination so this is a no-op.
			if trace.From == trace.To {
				continue
			}
		}

		// Skip empty to addresses (this may not actually occur but leaving it as a sanity check)
		if len(trace.To) == 0 {
			continue
		}

		// If the account is resurrected, we remove it from the destroyed accounts map.
		if trace.TraceType == traceTypeCreate {
			delete(destroyedAccounts, trace.To)
		}

		if shouldAdd {
			var amount *rosetta.Amount
			if zeroValue {
				amount = nil
			} else {
				amount = &rosetta.Amount{
					Value:    traceValue.String(),
					Currency: currency,
				}
				_, destroyed := destroyedAccounts[trace.To]
				if destroyed && opStatus == opStatusSuccess {
					destroyedAccounts[trace.To] = new(big.Int).Add(destroyedAccounts[trace.To], traceValue)
				}
			}

			lastOpIndex := ops[len(ops)-1].OperationIdentifier.Index

			toOp := &rosetta.Operation{
				OperationIdentifier: &rosetta.OperationIdentifier{
					Index: lastOpIndex + 1,
				},
				RelatedOperations: []*rosetta.OperationIdentifier{
					{
						Index: lastOpIndex,
					},
				},
				Type:   trace.Type,
				Status: opStatus,
				Account: &rosetta.AccountIdentifier{
					Address: trace.To,
				},
				Amount:   amount,
				Metadata: metadata,
			}

			ops = append(ops, toOp)
		}
	}

	// Zero-out all destroyed accounts that are removed during transaction finalization.
	for account, value := range destroyedAccounts {
		if value.Sign() == 0 {
			continue
		}

		if value.Sign() < 0 {
			return nil, xerrors.Errorf("there is a negative balance [%s] for a suicided account [%s]", value.String(), account)
		}

		lastOpIndex := ops[len(ops)-1].OperationIdentifier.Index
		ops = append(ops, &rosetta.Operation{
			OperationIdentifier: &rosetta.OperationIdentifier{
				Index: lastOpIndex + 1,
			},
			Type:   opTypeDestruct,
			Status: opStatusSuccess,
			Account: &rosetta.AccountIdentifier{
				Address: account,
			},
			Amount: &rosetta.Amount{
				Value:    new(big.Int).Neg(value).String(),
				Currency: currency,
			},
			Metadata: map[string]*any.Any{},
		})
	}

	return ops, nil
}

func getRosettaTransactionsFromCSBlock(
	block *api.EthereumBlock,
	blobData *api.EthereumBlobdata,
	rewardRecipient string,
	currency *rosetta.Currency,
	feeOpsFn func(*api.EthereumTransaction, string, *api.EthereumBlock) ([]*rosetta.Operation, error),
) ([]*rosetta.Transaction, error) {
	rosettaTransactions := make([]*rosetta.Transaction, len(block.Transactions))

	for i, ethTxn := range block.Transactions {
		feeOps, err := feeOpsFn(ethTxn, rewardRecipient, block)
		if err != nil {
			return nil, xerrors.Errorf("failed to parse fee operations: %w", err)
		}

		traceOps, err := convertCSTransactionToRosettaOps(ethTxn, len(feeOps), currency)
		if err != nil {
			return nil, xerrors.Errorf("failed to parse trace operations: %w", err)
		}

		var receiptMap map[string]interface{}
		if len(blobData.TransactionReceipts) > i {
			if err := json.Unmarshal(blobData.TransactionReceipts[i], &receiptMap); err != nil {
				return nil, xerrors.Errorf("failed to unmarshal transaction receipts: %w", err)
			}
		}

		var traceMap map[string]interface{}
		// TODO: Utilize https://github.com/coinbase/chainstorage/blob/masterinternal/blockchain/client/ethereum.go#L1190
		// when calculating trace length is fixed on production
		if len(blobData.TransactionTraces) > i {
			if err := json.Unmarshal(blobData.TransactionTraces[i], &traceMap); err != nil {
				return nil, xerrors.Errorf("failed to unmarshal transaction trace: %w", err)
			}
		}

		metadata, err := rosetta.FromSDKMetadata(map[string]interface{}{
			"gas_limit": hexutil.EncodeUint64(ethTxn.GetGas()),
			"gas_price": hexutil.EncodeUint64(ethTxn.GetGasPrice()),
			"receipt":   receiptMap,
			"trace":     traceMap,
		})
		if err != nil {
			return nil, xerrors.Errorf("failed to convert transaction metadata to rosetta proto: %w", err)
		}

		rosettaTransactions[i] = &rosetta.Transaction{
			TransactionIdentifier: &rosetta.TransactionIdentifier{
				Hash: ethTxn.Hash,
			},
			Operations:          append(feeOps, traceOps...),
			RelatedTransactions: nil,
			Metadata:            metadata,
		}
	}

	return rosettaTransactions, nil
}
