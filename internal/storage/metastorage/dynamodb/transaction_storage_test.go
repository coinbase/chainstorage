package dynamodb

import (
	"testing"

	ddbmodel "github.com/coinbase/chainstorage/internal/storage/metastorage/dynamodb/model"
	"github.com/coinbase/chainstorage/internal/storage/metastorage/model"
	"github.com/coinbase/chainstorage/internal/utils/testutil"
)

func TestTransformToTransaction_Success(t *testing.T) {
	require := testutil.Require(t)

	ddbEntry := &ddbmodel.TransactionDDBEntry{
		TransactionPid: "1#transactionHash",
		Hash:           "transactionHash",
		BlockHash:      "blockHash",
		BlockNumber:    123,
		BlockTag:       1,
	}

	expectedTransactions := &model.Transaction{
		Hash:        "transactionHash",
		BlockNumber: 123,
		BlockHash:   "blockHash",
		BlockTag:    1,
	}

	transaction, err := ddbmodel.TransformToTransaction(ddbEntry)

	require.NoError(err)
	require.NotNil(transaction)
	require.Equal(expectedTransactions, transaction)
}

func TestTransformToTransactions_Err_EmptyTransactionHash(t *testing.T) {
	require := testutil.Require(t)

	ddbEntry := &ddbmodel.TransactionDDBEntry{
		TransactionPid: "1#",
		Hash:           "",
		BlockHash:      "blockHash",
		BlockNumber:    123,
		BlockTag:       1,
	}

	_, err := ddbmodel.TransformToTransaction(ddbEntry)

	require.Error(err)
	require.ErrorContains(err, "transaction hash is empty")
}

func TestTransformToTransactions_Err_EmptyBlockHash(t *testing.T) {
	require := testutil.Require(t)

	ddbEntry := &ddbmodel.TransactionDDBEntry{
		TransactionPid: "1#transactionHash",
		Hash:           "transactionHash",
		BlockHash:      "",
		BlockNumber:    123,
		BlockTag:       1,
	}

	_, err := ddbmodel.TransformToTransaction(ddbEntry)

	require.Error(err)
	require.ErrorContains(err, "block hash is empty")
}
