package bitcoin

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"

	"go.uber.org/zap"
	"golang.org/x/xerrors"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/coinbase/chainstorage/internal/blockchain/parser/internal"
	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/utils/log"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
	rosetta "github.com/coinbase/chainstorage/protos/coinbase/crypto/rosetta/types"
)

const (
	// CoinbaseOpType is used to describe
	// Coinbase.
	coinbaseOpType = "COINBASE"
	// InputOpType is used to describe
	// INPUT.
	inputOpType = "INPUT"
	// outputOpType is used to describe
	// OUTPUT.
	outputOpType = "OUTPUT"

	// SuccessStatus is the status of all
	// Bitcoin operations because anything
	// on-chain is considered successful.
	successStatus = "SUCCESS"
	// SkippedStatus is the status of all
	// operations that are skipped because
	// of BIP-30. You can read more about these
	// types of operations in BIP-30.
	SkippedStatus = "SKIPPED"

	// NullData is returned by bitcoind
	// as the ScriptPubKey.Type for OP_RETURN
	// locking scripts.
	NullData = "nulldata"
)

type (
	bitcoinRosettaParserImpl struct {
		logger       *zap.Logger
		config       *config.Config
		nativeParser internal.NativeParser
	}
)

var (
	bitcoinRosettaCurrency = rosetta.Currency{
		Symbol:   "BTC",
		Decimals: 8,
	}
)

func NewBitcoinRosettaParser(params internal.ParserParams, nativeParser internal.NativeParser, opts ...internal.ParserFactoryOption) (internal.RosettaParser, error) {
	return &bitcoinRosettaParserImpl{
		logger:       log.WithPackage(params.Logger),
		config:       params.Config,
		nativeParser: nativeParser,
	}, nil
}

// Ref: https://github.com/coinbase/rosetta-bitcoin/blob/master/bitcoin/client.go
func (p *bitcoinRosettaParserImpl) ParseBlock(ctx context.Context, rawBlock *api.Block) (*api.RosettaBlock, error) {
	nativeBlock, err := p.nativeParser.ParseBlock(ctx, rawBlock)

	if err != nil {
		return nil, xerrors.Errorf("failed to parse block into native format: %w", err)
	}

	block := nativeBlock.GetBitcoin()
	if block == nil {
		return nil, xerrors.New("failed to find bitcoin block")
	}

	blockIdentifier := &rosetta.BlockIdentifier{
		Index: int64(rawBlock.GetMetadata().GetHeight()),
		Hash:  rawBlock.GetMetadata().GetHash(),
	}

	// for genesis block, get {0, ""} for parent block identifier
	parentBlockIndentifier := &rosetta.BlockIdentifier{
		Index: int64(rawBlock.GetMetadata().GetParentHeight()),
		Hash:  rawBlock.GetMetadata().GetParentHash(),
	}

	transactions, err := p.getRosettaTransactions(block, rawBlock.GetBitcoin())
	if err != nil {
		return nil, xerrors.Errorf("failed to parse block transactions: %w", err)
	}

	metadata, err := rosetta.FromSDKMetadata(map[string]any{
		"nonce":      block.GetHeader().GetNonce(),
		"merkleroot": block.GetHeader().GetMerkleRoot(),
		"version":    block.GetHeader().GetVersion(),
		"size":       block.GetHeader().GetSize(),
		"weight":     block.GetHeader().GetWeight(),
		"mediantime": block.GetHeader().GetMedianTime(),
		"bits":       block.GetHeader().GetBits(),
		"difficulty": block.GetHeader().GetDifficulty(),
	})

	if err != nil {
		return nil, xerrors.Errorf("failed to convert block metadata to rosetta proto: %w", err)
	}

	return &api.RosettaBlock{
		Block: &rosetta.Block{
			BlockIdentifier:       blockIdentifier,
			ParentBlockIdentifier: parentBlockIndentifier,
			Timestamp:             block.GetHeader().Timestamp,
			Transactions:          transactions,
			Metadata:              metadata,
		},
	}, nil
}

func (p *bitcoinRosettaParserImpl) getRosettaTransactions(block *api.BitcoinBlock, blobData *api.BitcoinBlobdata) ([]*rosetta.Transaction, error) {
	rosettaTransactions := make([]*rosetta.Transaction, len(block.Transactions))
	blockHeight := int64(block.GetHeader().GetHeight())
	blockHash := block.GetHeader().GetHash()
	for idx, transaction := range block.GetTransactions() {
		operations, err := p.getRosettaOperations(transaction)

		if err != nil {
			return nil, xerrors.Errorf(
				"failed to get operation for transaction %s: %w",
				transaction.GetHash(),
				err)
		}

		if skipTransactionOperations(
			blockHeight,
			blockHash,
			transaction.GetHash()) {
			for _, op := range operations {
				op.Status = SkippedStatus
			}
		}

		metadata, err := rosetta.FromSDKMetadata(map[string]any{
			"size":            transaction.GetSize(),
			"vsize":           transaction.GetVirtualSize(),
			"version":         transaction.GetVersion(),
			"locktime":        transaction.GetLockTime(),
			"weight":          transaction.GetWeight(),
			"block_timestamp": block.GetHeader().GetTimestamp().AsTime().Unix(),
		})

		if err != nil {
			return nil, xerrors.Errorf(
				"failed to convert transaction metadata to rosetta proto for transaction %s: %w",
				transaction.GetHash(),
				err)
		}

		rosettaTransactions[idx] = &rosetta.Transaction{
			TransactionIdentifier: &rosetta.TransactionIdentifier{
				Hash: transaction.GetTransactionId(),
			},
			Operations:          operations,
			RelatedTransactions: nil,
			Metadata:            metadata,
		}
	}

	return rosettaTransactions, nil
}

// parseTransactions returns the transaction operations for a specified transaction.
// It uses a map of previous transactions to properly hydrate the input operations.
func (p *bitcoinRosettaParserImpl) getRosettaOperations(
	tx *api.BitcoinTransaction,
) ([]*rosetta.Operation, error) {
	txOps := []*rosetta.Operation{}

	for inputIdx, input := range tx.Inputs {
		metadata, err := getInputMetadata(input)

		if err != nil {
			return nil, xerrors.Errorf("failed to convert input metadata to rosetta proto: %w", err)
		}

		// This reflects an input that does not correspond to a previous output.
		if bitcoinIsCoinbaseInput(input, tx.GetIsCoinbase()) {
			txOp := &rosetta.Operation{
				OperationIdentifier: &rosetta.OperationIdentifier{
					Index:        int64(len(txOps)),
					NetworkIndex: int64(inputIdx),
				},
				Type:     coinbaseOpType,
				Status:   successStatus,
				Metadata: metadata,
			}
			txOps = append(txOps, txOp)
			break
		}

		// If we are unable to parse the output account (i.e. bitcoind
		// returns a blank/nonstandard/multisig ScriptPubKey), we create an address as the
		// concatenation of the tx hash and index.
		//
		// Example: 4852fe372ff7534c16713b3146bbc1e86379c70bea4d5c02fb1fa0112980a081:1
		// on testnet
		account := &rosetta.AccountIdentifier{
			Address: input.GetFromOutput().GetScriptPublicKey().GetAddress(),
		}
		if len(account.Address) == 0 {
			account.Address = fmt.Sprintf("%s:%d", input.GetTransactionId(), input.GetFromOutputIndex())
		}

		// Parse the input transaction operation
		txOp := &rosetta.Operation{
			OperationIdentifier: &rosetta.OperationIdentifier{
				Index:        int64(len(txOps)),
				NetworkIndex: int64(inputIdx),
			},
			Type:    inputOpType,
			Status:  successStatus,
			Account: account,
			Amount: &rosetta.Amount{
				Value:    new(big.Int).Neg(new(big.Int).SetUint64(input.GetFromOutput().GetValue())).String(),
				Currency: &bitcoinRosettaCurrency,
			},
			CoinChange: &rosetta.CoinChange{
				CoinIdentifier: &rosetta.CoinIdentifier{
					Identifier: fmt.Sprintf("%s:%d", input.GetTransactionId(), input.GetFromOutputIndex()),
				},
				CoinAction: rosetta.CoinChange_COIN_SPENT,
			},
			Metadata: metadata,
		}

		txOps = append(txOps, txOp)
	}

	for outputIdx, output := range tx.Outputs {
		metadata, err := getOutputMetadata(output)
		if err != nil {
			return nil, xerrors.Errorf("failed to convert output metadata to rosetta proto: %w", err)
		}

		coinChange := &rosetta.CoinChange{
			CoinIdentifier: &rosetta.CoinIdentifier{
				Identifier: fmt.Sprintf("%s:%d", tx.GetTransactionId(), outputIdx),
			},
			CoinAction: rosetta.CoinChange_COIN_CREATED,
		}

		// If this is an OP_RETURN locking script,
		// we don't create a coin because it is provably unspendable.
		if output.GetScriptPublicKey().GetType() == NullData {
			coinChange = nil
		}

		// If we are unable to parse the output account (i.e. bitcoind
		// returns a blank/nonstandard ScriptPubKey), we create an address as the
		// concatenation of the tx hash and index.
		//
		// Example: 4852fe372ff7534c16713b3146bbc1e86379c70bea4d5c02fb1fa0112980a081:1
		// on testnet
		account := &rosetta.AccountIdentifier{
			Address: output.GetScriptPublicKey().GetAddress(),
		}
		if len(account.Address) == 0 {
			account.Address = fmt.Sprintf("%s:%d", tx.GetTransactionId(), outputIdx)
		}

		txOp := &rosetta.Operation{
			OperationIdentifier: &rosetta.OperationIdentifier{
				Index:        int64(len(txOps)),
				NetworkIndex: int64(outputIdx),
			},
			Type:    outputOpType,
			Status:  successStatus,
			Account: account,
			Amount: &rosetta.Amount{
				Value:    new(big.Int).SetUint64(output.GetValue()).String(),
				Currency: &bitcoinRosettaCurrency,
			},
			CoinChange: coinChange,
			Metadata:   metadata,
		}

		txOps = append(txOps, txOp)
	}

	return txOps, nil
}

// bitcoinIsCoinbaseInput returns whether the specified input is
// the coinbase input. The coinbase input is always the first input in the first
// transaction, and does not contain a previous transaction hash.
func bitcoinIsCoinbaseInput(input *api.BitcoinTransactionInput, txIsCoinbase bool) bool {
	return input.GetCoinbase() != "" || txIsCoinbase
}

// skipTransactionOperations is used to skip operations on transactions that
// contain duplicate UTXOs (which are no longer possible after BIP-30). This
// function mirrors the behavior of a similar commit in bitcoin-core.
//
// Source: https://github.com/bitcoin/bitcoin/commit/ab91bf39b7c11e9c86bb2043c24f0f377f1cf514
// See BIP30, CVE-2012-1909, and http://r6.ca/blog/20120206T005236Z.html for more information.
func skipTransactionOperations(blockNumber int64, blockHash string, transactionHash string) bool {
	if blockNumber == 91842 && blockHash == "00000000000a4d0a398161ffc163c503763b1f4360639393e0e4c8e300e0caec" &&
		transactionHash == "d5d27987d2a3dfc724e359870c6644b40e497bdc0589a033220fe15429d88599" {
		return true
	}

	if blockNumber == 91880 && blockHash == "00000000000743f190a18c5577a3c2d2a1f610ae9601ac046a38084ccb7cd721" &&
		transactionHash == "e3bf3d07d4b0375638d5f1db5255fe07ba2c4cb067cd81b84ee974b6585fb468" {
		return true
	}

	return false
}

func getInputMetadata(input *api.BitcoinTransactionInput) (map[string]*anypb.Any, error) {
	metaMap := map[string]any{
		"sequence": input.GetSequence(),
		"coinbase": input.GetCoinbase(),
	}

	if input.GetScriptSignature() != nil {
		scriptSig, err := json.Marshal(input.GetScriptSignature())
		if err != nil {
			return nil, err
		}
		metaMap["scriptsig"] = string(scriptSig)
	}
	if input.GetTransactionInputWitnesses() != nil {
		witness, err := json.Marshal(input.GetTransactionInputWitnesses())
		if err != nil {
			return nil, err
		}
		metaMap["txinwitness"] = string(witness)
	}

	return rosetta.FromSDKMetadata(metaMap)
}

func getOutputMetadata(output *api.BitcoinTransactionOutput) (map[string]*anypb.Any, error) {
	metadataMap, err := json.Marshal(output.GetScriptPublicKey())
	if err != nil {
		return nil, err
	}

	return rosetta.FromSDKMetadata(map[string]any{
		"scriptPubKey": string(metadataMap),
	})
}
