package dynamodb

import (
	"context"
	"testing"

	"github.com/stretchr/testify/suite"
	"go.uber.org/fx"

	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/storage/internal/errors"
	"github.com/coinbase/chainstorage/internal/storage/metastorage/internal"
	"github.com/coinbase/chainstorage/internal/storage/metastorage/model"
	"github.com/coinbase/chainstorage/internal/utils/testapp"
	"github.com/coinbase/chainstorage/internal/utils/testutil"
)

type transactionStorageTestSuite struct {
	suite.Suite
	storage internal.MetaStorage
	config  *config.Config
}

func TestIntegrationTransactionStorageTestSuite(t *testing.T) {
	// TODO: speed up the tests before re-enabling TestAllEnvs.
	// testapp.TestAllEnvs(t, func(t *testing.T, cfg *config.Config) {
	// 	suite.Run(t, &transactionStorageTestSuite{config: cfg})
	// })

	require := testutil.Require(t)
	cfg, err := config.New()
	require.NoError(err)
	suite.Run(t, &transactionStorageTestSuite{config: cfg})
}

func (s *transactionStorageTestSuite) SetupTest() {
	require := testutil.Require(s.T())

	var storage internal.MetaStorage
	cfg, err := config.New()
	require.NoError(err)
	cfg.Chain.BlockStartHeight = 10
	s.config = cfg
	app := testapp.New(
		s.T(),
		Module,
		testapp.WithIntegration(),
		testapp.WithConfig(s.config),
		fx.Populate(&storage),
	)
	defer app.Close()
	s.storage = storage
}

func (s *transactionStorageTestSuite) TestAddAndGetTransaction() {
	require := testutil.Require(s.T())
	ctx := context.Background()

	transaction1 := &model.Transaction{
		Hash:        "transactionHash",
		BlockNumber: 123,
		BlockHash:   "blockHash",
		BlockTag:    1,
	}

	// first update attempt
	err := s.storage.AddTransactions(ctx, []*model.Transaction{transaction1}, 2)
	require.NoError(err)

	persisted, err := s.storage.GetTransaction(ctx, 1, "transactionHash")
	require.NoError(err)
	require.Len(persisted, 1)

	s.validateExpectedAndActualTxn(transaction1, persisted[0])

	transaction2 := &model.Transaction{
		Hash:        "transactionHash2",
		BlockNumber: 124,
		BlockHash:   "blockHash2",
		BlockTag:    1,
	}

	// second update attempt with different key
	err = s.storage.AddTransactions(ctx, []*model.Transaction{transaction2}, 2)
	require.NoError(err)

	persisted, err = s.storage.GetTransaction(ctx, 1, "transactionHash2")
	require.NoError(err)
	require.Len(persisted, 1)

	s.validateExpectedAndActualTxn(transaction2, persisted[0])

	transaction3 := &model.Transaction{
		Hash:        "transactionHash",
		BlockNumber: 125,
		BlockHash:   "blockHash3",
		BlockTag:    1,
	}

	// third update attempt with same hash key but different block hash
	err = s.storage.AddTransactions(ctx, []*model.Transaction{transaction3}, 2)
	require.NoError(err)

	persisted, err = s.storage.GetTransaction(ctx, 1, "transactionHash")
	require.NoError(err)
	require.Len(persisted, 2)

	if transaction1.BlockNumber == persisted[0].BlockNumber {
		s.validateExpectedAndActualTxn(transaction1, persisted[0])
		s.validateExpectedAndActualTxn(transaction3, persisted[1])
	} else {
		s.validateExpectedAndActualTxn(transaction1, persisted[1])
		s.validateExpectedAndActualTxn(transaction3, persisted[0])
	}
}

func (s *transactionStorageTestSuite) TestAddAndGetTransaction_Batch() {
	require := testutil.Require(s.T())
	ctx := context.Background()

	transactions := testutil.MakeTransactionsFromStartHeight(0, 25, 1)

	err := s.storage.AddTransactions(ctx, transactions, 2)
	require.NoError(err)

	persisted, err := s.storage.GetTransaction(ctx, 1, "transactionHash0")
	require.NoError(err)
	require.Len(persisted, 1)
	actual := persisted[0]
	require.Equal(uint32(1), actual.BlockTag)
	require.Equal("transactionHash0", actual.Hash)
	require.Equal("blockHash0", actual.BlockHash)
	require.Equal(uint64(0), actual.BlockNumber)

	persisted, err = s.storage.GetTransaction(ctx, 1, "transactionHash10")
	require.NoError(err)
	require.Len(persisted, 1)
	actual = persisted[0]
	require.Equal(uint32(1), actual.BlockTag)
	require.Equal("transactionHash10", actual.Hash)
	require.Equal("blockHash10", actual.BlockHash)
	require.Equal(uint64(10), actual.BlockNumber)

	persisted, err = s.storage.GetTransaction(ctx, 1, "transactionHash24")
	require.NoError(err)
	require.Len(persisted, 1)
	actual = persisted[0]
	require.Equal(uint32(1), actual.BlockTag)
	require.Equal("transactionHash24", actual.Hash)
	require.Equal("blockHash24", actual.BlockHash)
	require.Equal(uint64(24), actual.BlockNumber)
}

func (s *transactionStorageTestSuite) TestAddAndGetTransaction_Dedup() {
	require := testutil.Require(s.T())
	ctx := context.Background()

	transaction1 := &model.Transaction{
		Hash:        "transactionHash",
		BlockNumber: 123,
		BlockHash:   "blockHash",
		BlockTag:    1,
	}

	// first update attempt
	err := s.storage.AddTransactions(ctx, []*model.Transaction{transaction1}, 2)
	require.NoError(err)

	persisted, err := s.storage.GetTransaction(ctx, 1, "transactionHash")
	require.NoError(err)
	require.Len(persisted, 1)

	// second update attempt with same transaction
	err = s.storage.AddTransactions(ctx, []*model.Transaction{transaction1}, 2)
	require.NoError(err)

	persisted, err = s.storage.GetTransaction(ctx, 1, "transactionHash")
	require.NoError(err)
	require.Len(persisted, 1)

	s.validateExpectedAndActualTxn(transaction1, persisted[0])

}

func (s *transactionStorageTestSuite) TestGetTransaction_NotExists() {
	require := testutil.Require(s.T())

	ctx := context.Background()
	persisted, err := s.storage.GetTransaction(ctx, 1, "transactionHash")
	require.Error(err)
	require.ErrorIs(err, errors.ErrItemNotFound)
	require.Nil(persisted)
}

func (s *transactionStorageTestSuite) validateExpectedAndActualTxn(expected *model.Transaction, actual *model.Transaction) {
	require := testutil.Require(s.T())

	require.Equal(expected.BlockTag, actual.BlockTag)
	require.Equal(expected.Hash, actual.Hash)
	require.Equal(expected.BlockHash, actual.BlockHash)
	require.Equal(expected.BlockNumber, actual.BlockNumber)
}
