package ethereum

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"strings"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/coinbase/chainstorage/internal/blockchain/parser/internal"
	c3common "github.com/coinbase/chainstorage/protos/coinbase/c3/common"
	rosetta "github.com/coinbase/chainstorage/protos/coinbase/crypto/rosetta/types"

	"github.com/coinbase/chainstorage/internal/blockchain/bootstrap"
	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/utils/log"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

const (
	// https://burn.polygon.technology/
	burntContract = "0x70bca57f4579f58670ab2d18ef16e02c17553c38"

	maticDepositedTopic   = "0xec3afb067bce33c5a294470ec5b29e6759301cd3928550490c6d48816cdc2f5d"
	maticDepositedAddress = "0x0000000000000000000000000000000000000000000000000000000000001010"

	// WMatic
	wmaticAddress         = "0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270"
	wmaticWithdrawalTopic = "0x7fcf532c15f0a6db0bd6d0e038bea71d30d808c7d98cb3bf7268a95bf5081b65"
	wmaticDepositTopic    = "0xe1fffcc4923d04b559f4d29a8bfc6cda04eb5b0d3c460751c2402c5c5cc9109c"

	// Hop Protocol
	hopL2PolygonBridgeAddress       = "0x553bc791d746767166fa3888432038193ceed5e2"
	hopSwapAddress                  = "0x3d4cc8a61c7528fd86c55cfe061a78dcba48edd1"
	hopSwapTopic                    = "0xc6c1e0630dbe9130cc068028486c0d118ddcea348550819defd5cb8c257f8a38"
	hopTransferFromL1CompletedTopic = "0x320958176930804eb66c2343c7343fc0367dc16249590c0f195783bee199d094"

	logTransferTopic = "0xe6497e3ee548a3372136af2fcb0696db31fc6cf20260707645068bd3fe97f3c4"
	maticAddress     = "0x0000000000000000000000000000000000001010"

	valueLength      = 64
	topicLength      = 66
	addressHexLength = 40

	defaultERC20Decimals = 0
	defaultERC20Symbol   = "UNKNOWN"
)

type (
	polygonRosettaParserImpl struct {
		logger       *zap.Logger
		config       *config.Config
		nativeParser internal.NativeParser
	}
)

var (
	polygonRosettaCurrency = rosetta.Currency{
		Symbol:   "MATIC",
		Decimals: 18,
	}

	borReceiptPrefix = []byte("matic-bor-receipt-") // borReceiptPrefix + number + block hash -> bor block receipt
)

func NewPolygonRosettaParser(params internal.ParserParams, nativeParser internal.NativeParser, opts ...internal.ParserFactoryOption) (internal.RosettaParser, error) {
	return &polygonRosettaParserImpl{
		logger:       log.WithPackage(params.Logger),
		config:       params.Config,
		nativeParser: nativeParser,
	}, nil
}

func (p *polygonRosettaParserImpl) ParseBlock(ctx context.Context, rawBlock *api.Block) (*api.RosettaBlock, error) {
	nativeBlock, err := p.nativeParser.ParseBlock(ctx, rawBlock)
	if err != nil {
		return nil, fmt.Errorf("failed to parse block into native format: %w", err)
	}

	block := nativeBlock.GetEthereum()
	if block == nil {
		return nil, errors.New("failed to find polygon block")
	}

	blockIdentifier := &rosetta.BlockIdentifier{
		Index: int64(rawBlock.GetMetadata().GetHeight()),
		Hash:  rawBlock.GetMetadata().GetHash(),
	}

	parentBlockIdentifier := blockIdentifier
	if blockIdentifier.Index != internal.GenesisBlockIndex {
		parentBlockIdentifier = &rosetta.BlockIdentifier{
			Index: int64(rawBlock.GetMetadata().GetParentHeight()),
			Hash:  rawBlock.GetMetadata().GetParentHash(),
		}
	}

	transactions, err := p.getRosettaTransactions(block, rawBlock.GetEthereum(), nativeBlock.Tag, nativeBlock.Network)
	if err != nil {
		return nil, fmt.Errorf("failed to parse block transactions: %w", err)
	}

	if nativeBlock.Height == 0 {
		genesisAllocation, err := bootstrap.GenerateGenesisAllocations(p.config.Chain.Network)
		if err != nil {
			return nil, err
		}

		genesisTransactions, err := p.getGenesisTransactions(genesisAllocation)
		if err != nil {
			return nil, fmt.Errorf("failed to generate genesis transactions: %w", err)
		}
		transactions = append(transactions, genesisTransactions...)
	}

	return &api.RosettaBlock{
		Block: &rosetta.Block{
			BlockIdentifier:       blockIdentifier,
			ParentBlockIdentifier: parentBlockIdentifier,
			Timestamp:             block.GetHeader().Timestamp,
			Transactions:          transactions,
			Metadata:              nil,
		},
	}, nil
}

func (p *polygonRosettaParserImpl) getRosettaTransactions(block *api.EthereumBlock, blobData *api.EthereumBlobdata, blockTag uint32, network c3common.Network) ([]*rosetta.Transaction, error) {
	var author string

	// For backward compatibility in polygon-mainnet, since author data is not ingested For historical blocks with tag = 1
	if (network == c3common.Network_NETWORK_POLYGON_MAINNET && blockTag > 1) || network == c3common.Network_NETWORK_POLYGON_TESTNET {
		if err := json.Unmarshal(blobData.GetPolygon().GetAuthor(), &author); err != nil {
			return nil, fmt.Errorf("unexpected author: %w", err)
		}
	}

	return getRosettaTransactionsFromCSBlock(
		block,
		blobData,
		author,
		&polygonRosettaCurrency,
		p.feeOps,
		p.tokenTransferOpsFn,
		p.getLogTransferTxOps,
	)
}

// Ref: https://github.com/maticnetwork/polygon-rosetta/blob/0b60290550521cce0088ad68e857b752d01be869/polygon/client.go#L974
func (p *polygonRosettaParserImpl) feeOps(transaction *api.EthereumTransaction, author string, block *api.EthereumBlock) ([]*rosetta.Operation, error) {
	feeDetails, err := getFeeDetails(transaction, block)
	if err != nil {
		return nil, fmt.Errorf("failed to calculate polygon fee details: %w", err)
	}

	if feeDetails.feeAmount.Cmp(new(big.Int)) == 0 {
		// This can happen for state sync transactions
		return []*rosetta.Operation{}, nil
	}

	var minerEarnedAmount *big.Int
	if feeDetails.feeBurned != nil {
		minerEarnedAmount = new(big.Int).Sub(feeDetails.feeAmount, feeDetails.feeBurned)
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
			Metadata: map[string]*anypb.Any{},
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
			Metadata: map[string]*anypb.Any{},
		},
	}

	if feeDetails.feeBurned == nil {
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
			Value:    new(big.Int).Neg(feeDetails.feeBurned).String(),
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
			Value:    feeDetails.feeBurned.String(),
			Currency: &polygonRosettaCurrency,
		},
	}

	return append(rOps, debitOp, creditOp), nil
}

func (p *polygonRosettaParserImpl) tokenTransferOpsFn(transaction *api.EthereumTransaction, startIndex int) ([]*rosetta.Operation, error) {
	var ops []*rosetta.Operation

	var status string
	if transaction.GetReceipt().GetOptionalStatus() == nil {
		return nil, fmt.Errorf("receipt status not exist for transaction=%v", transaction.GetHash())
	}
	if transaction.GetReceipt().GetStatus() == 1 {
		status = opStatusSuccess
	} else {
		status = opStatusFailure
	}

	for _, tokenTransfer := range transaction.TokenTransfers {
		// If this isn't an ERC20 transfer, skip
		if tokenTransfer.GetErc20() == nil {
			continue
		}

		// If value <= 0, skip to the next receiptLog. Otherwise, proceed to generate the debit + credit operations.
		value, err := internal.BigInt(tokenTransfer.Value)
		if err != nil {
			return nil, fmt.Errorf("unable to parse value of tokenTransfer %s due to %w", tokenTransfer.Value, err)
		}
		if value.Cmp(big.NewInt(0)) < 1 {
			continue
		}

		contractAddress := tokenTransfer.TokenAddress
		_, err = internal.ChecksumAddress(contractAddress)
		if err != nil {
			return nil, fmt.Errorf("%s is not a valid address", contractAddress)
		}

		fromAddress := tokenTransfer.FromAddress
		_, err = internal.ChecksumAddress(fromAddress)
		if err != nil {
			return nil, fmt.Errorf("%s is not a valid address", fromAddress)
		}

		toAddress := tokenTransfer.ToAddress
		_, err = internal.ChecksumAddress(toAddress)
		if err != nil {
			return nil, fmt.Errorf("%s is not a valid address", toAddress)
		}

		// Use default currency as there is no way to fetch currency based on contract address
		metadata, err := rosetta.FromSDKMetadata(map[string]any{
			ContractAddressKey: contractAddress,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to marshal currency metadata for contractAddress %v: %w", contractAddress, err)
		}
		currency := &rosetta.Currency{
			Symbol:   defaultERC20Symbol,
			Decimals: defaultERC20Decimals,
			Metadata: metadata,
		}

		fromOp := &rosetta.Operation{
			OperationIdentifier: &rosetta.OperationIdentifier{
				Index: int64(len(ops) + startIndex),
			},
			Type:   opTypePayment,
			Status: status,
			Account: &rosetta.AccountIdentifier{
				Address: fromAddress,
			},
			Amount: &rosetta.Amount{
				Value:    new(big.Int).Neg(value).String(),
				Currency: currency,
			},
		}

		ops = append(ops, fromOp)

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
			Type:   opTypePayment,
			Status: status,
			Account: &rosetta.AccountIdentifier{
				Address: toAddress,
			},
			Amount: &rosetta.Amount{
				Value:    value.String(),
				Currency: currency,
			},
		}

		ops = append(ops, toOp)
	}

	return ops, nil
}

func (p *polygonRosettaParserImpl) getLogTransferTxOps(transaction *api.EthereumTransaction, startIndex int) ([]*rosetta.Operation, error) {
	var operations []*rosetta.Operation

	if transaction.From == ethereumZeroAddress && transaction.To == ethereumZeroAddress {
		for _, eventLog := range transaction.Receipt.Logs {
			if eventLog.Address == maticAddress && len(eventLog.Topics) >= 4 {
				if eventLog.Topics[0] != logTransferTopic || eventLog.Topics[1] != maticDepositedAddress {
					continue
				}

				fromAddress, err := getAddressFromTopic(eventLog.Topics[2])
				if err != nil {
					return nil, err
				}

				toAddress, err := getAddressFromTopic(eventLog.Topics[3])
				if err != nil {
					return nil, err
				}

				value, err := getValueFromHex(eventLog.Data[2:], valueLength)
				if err != nil {
					return nil, err
				}

				operations = append(operations, &rosetta.Operation{
					OperationIdentifier: &rosetta.OperationIdentifier{
						Index: int64(len(operations) + startIndex),
					},
					Type:   traceTypeCall,
					Status: opStatusSuccess,
					Account: &rosetta.AccountIdentifier{
						Address: fromAddress,
					},
					Amount: &rosetta.Amount{
						Value:    new(big.Int).Neg(value).String(),
						Currency: &polygonRosettaCurrency,
					},
					Metadata: map[string]*anypb.Any{},
				})

				operations = append(operations, &rosetta.Operation{
					OperationIdentifier: &rosetta.OperationIdentifier{
						Index: int64(len(operations) + startIndex),
					},
					Type:   traceTypeCall,
					Status: opStatusSuccess,
					Account: &rosetta.AccountIdentifier{
						Address: toAddress,
					},
					Amount: &rosetta.Amount{
						Value:    value.String(),
						Currency: &polygonRosettaCurrency,
					},
					Metadata: map[string]*anypb.Any{},
				})
			}
		}
	}

	return operations, nil
}

func (p *polygonRosettaParserImpl) getHopProtocolTxOps(transaction *api.EthereumTransaction, startIndex int) ([]*rosetta.Operation, error) {
	var value *big.Int
	var toAddress string
	var err error

	for _, eventLog := range transaction.Receipt.Logs {
		// Look for TokenSwap event
		if eventLog.Address == hopSwapAddress && len(eventLog.Topics) >= 2 && eventLog.Topics[0] == hopSwapTopic {
			// tokensSold + tokensBought + soldId + boughtId
			if len(eventLog.Data) > 130 {
				value, err = getValueFromHex(eventLog.Data[66:], valueLength)
				if err != nil {
					return nil, err
				}
			}

			continue
		}

		// Look for TransferFromL1Completed event
		if eventLog.Address == hopL2PolygonBridgeAddress && len(eventLog.Topics) >= 2 && eventLog.Topics[0] == hopTransferFromL1CompletedTopic {
			toAddress, err = getAddressFromTopic(eventLog.Topics[1])
			if err != nil {
				return nil, err
			}
		}
	}

	var operations []*rosetta.Operation

	if value != nil && len(toAddress) > 0 {
		operations = append(operations, &rosetta.Operation{
			OperationIdentifier: &rosetta.OperationIdentifier{
				Index: int64(len(operations) + startIndex),
			},
			Type:   opTypePayment,
			Status: opStatusSuccess,
			Account: &rosetta.AccountIdentifier{
				Address: toAddress,
			},
			Amount: &rosetta.Amount{
				Value:    value.String(),
				Currency: &polygonRosettaCurrency,
			},
			Metadata: map[string]*anypb.Any{},
		},
		)
	}

	return operations, nil
}

// https://github.com/maticnetwork/polygon-rosetta/blob/0b60290550521cce0088ad68e857b752d01be869/polygon/client.go#L417
func (p *polygonRosettaParserImpl) getStateSyncTracesOpsFn(transaction *api.EthereumTransaction, startIndex int) ([]*rosetta.Operation, error) {
	if transaction.Hash != getDerivedBorTxHash(borReceiptKey(transaction.BlockNumber, transaction.BlockHash)) {
		return nil, nil
	}

	var operations []*rosetta.Operation

	// Get TokenDeposited events from the receipt Logs and convert to expected call traces
	for _, eventLog := range transaction.Receipt.Logs {
		// 0xec3a... is the topic hash for the token deposited event
		// TokenDeposited (index_topic_1 address rootToken, index_topic_2 address childToken,
		//                 index_topic_3 address user, uint256 amount, uint256 depositCount)
		if eventLog.Topics[0] == maticDepositedTopic {
			childTokenAddress := eventLog.Topics[2]
			// Data is amount + depositCount. We don't care about deposit count so just convert first 32 bytes to bigint
			value, err := getValueFromHex(eventLog.Data, valueLength)
			if err != nil {
				return nil, err
			}

			toAddress, err := getAddressFromTopic(eventLog.Topics[3])
			if err != nil {
				return nil, err
			}

			// We only care about MATIC deposits for now. 0x1010 is the matic token address
			if childTokenAddress == maticDepositedAddress {
				operation := &rosetta.Operation{
					OperationIdentifier: &rosetta.OperationIdentifier{
						Index: int64(len(operations) + startIndex),
					},
					Type:   opTypePayment,
					Status: opStatusSuccess,
					Account: &rosetta.AccountIdentifier{
						Address: toAddress,
					},
					Amount: &rosetta.Amount{
						Value:    value.String(),
						Currency: &polygonRosettaCurrency,
					},
					Metadata: map[string]*anypb.Any{},
				}

				operations = append(operations, operation)
			}
		}
	}

	return operations, nil
}

func (p *polygonRosettaParserImpl) getGenesisTransactions(allocations []*bootstrap.GenesisAllocation) ([]*rosetta.Transaction, error) {
	var genesisTxns []*rosetta.Transaction
	for _, allo := range allocations {
		address := allo.AccountIdentifier.Address
		_, err := internal.ChecksumAddress(address)
		if err != nil {
			return nil, fmt.Errorf("%s is not a valid address", address)
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
				Currency: &polygonRosettaCurrency,
			},
			Metadata: map[string]*anypb.Any{},
		}

		txnIdentifier := fmt.Sprintf("GENESIS_%s", addressLower[2:])
		txn := &rosetta.Transaction{
			TransactionIdentifier: &rosetta.TransactionIdentifier{
				Hash: txnIdentifier,
			},
			Operations: []*rosetta.Operation{genesisOp},
			Metadata:   map[string]*anypb.Any{},
		}

		genesisTxns = append(genesisTxns, txn)
	}

	return genesisTxns, nil
}

// borReceiptKey = borReceiptPrefix + num (uint64 big endian) + hash
func borReceiptKey(number uint64, txHash string) []byte {
	enc := make([]byte, 8)
	binary.BigEndian.PutUint64(enc, number)
	return append(append(borReceiptPrefix, enc...), common.HexToHash(txHash).Bytes()...)
}

// getDerivedBorTxHash get derived tx hash from receipt key
func getDerivedBorTxHash(receiptKey []byte) string {
	return common.BytesToHash(crypto.Keccak256(receiptKey)).String()
}

// getAddressFromTopic returns an address from a given topic
func getAddressFromTopic(topic string) (string, error) {
	if len(topic) < topicLength {
		return "", fmt.Errorf("topic is too short: %v", topic)
	}

	return prepend0x(topic[len(topic)-addressHexLength:]), nil
}

// getValueFromHex returns a value from a given topic
func getValueFromHex(hexStr string, length int) (*big.Int, error) {
	strippedHex := strip0x(hexStr)

	if len(strippedHex) < length {
		return nil, fmt.Errorf("value is too short: %v", hexStr)
	}

	result, ok := big.NewInt(0).SetString(strippedHex[:length], 16)
	if !ok {
		return nil, fmt.Errorf("invalid hex string: %v", hexStr)
	}

	return result, nil
}

// strip0x removes 0x/0X prefix from the given string if it exists.
func strip0x(s string) string {
	if internal.Has0xPrefix(s) {
		return s[2:]
	}
	return s
}

// prepend0x adds the 0x/0X prefix to the given string if it doesn't exist.
func prepend0x(s string) string {
	if internal.Has0xPrefix(s) {
		return s
	}
	return "0x" + s
}
