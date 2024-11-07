package rosetta

import (
	"encoding/json"
	"fmt"

	sdk "github.com/coinbase/rosetta-sdk-go/types"

	rosetta "github.com/coinbase/chainstorage/protos/coinbase/crypto/rosetta/types"
)

func ParseRosettaBlock(data []byte) (*rosetta.Block, error) {
	var blockResponse sdk.BlockResponse
	if err := json.Unmarshal(data, &blockResponse); err != nil {
		return nil, fmt.Errorf("failed to parse block response: %w", err)
	}

	block := blockResponse.Block
	if block == nil {
		return nil, nil
	}

	rosettaBlock, err := rosetta.FromSDKBlock(block)
	if err != nil {
		return nil, fmt.Errorf("failed to parse rosetta sdk block: %w", err)
	}

	return rosettaBlock, nil
}

func ParseOtherTransactions(data [][]byte) ([]*rosetta.Transaction, error) {
	txs := make([]*rosetta.Transaction, len(data))
	for i, rawTx := range data {
		var transactionResponse sdk.BlockTransactionResponse
		if err := json.Unmarshal(rawTx, &transactionResponse); err != nil {
			return nil, fmt.Errorf("failed to parse block transaction response: %w", err)
		}

		transaction := transactionResponse.Transaction
		tx, err := rosetta.FromSDKTransaction(transaction)
		if err != nil {
			return nil, fmt.Errorf("failed to parse transaction of other transactions: %w", err)
		}

		txs[i] = tx
	}
	return txs, nil
}
