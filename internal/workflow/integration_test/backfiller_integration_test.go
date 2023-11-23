package integration_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"go.temporal.io/sdk/testsuite"
	"go.uber.org/fx"
	"go.uber.org/zap"

	"github.com/coinbase/chainstorage/internal/blockchain/client"
	"github.com/coinbase/chainstorage/internal/blockchain/jsonrpc"
	"github.com/coinbase/chainstorage/internal/blockchain/parser"
	"github.com/coinbase/chainstorage/internal/blockchain/restapi"
	"github.com/coinbase/chainstorage/internal/cadence"
	"github.com/coinbase/chainstorage/internal/dlq"
	"github.com/coinbase/chainstorage/internal/s3"
	"github.com/coinbase/chainstorage/internal/storage"
	"github.com/coinbase/chainstorage/internal/storage/blobstorage"
	"github.com/coinbase/chainstorage/internal/storage/metastorage"
	storage_utils "github.com/coinbase/chainstorage/internal/storage/utils"
	"github.com/coinbase/chainstorage/internal/utils/testapp"
	"github.com/coinbase/chainstorage/internal/utils/testutil"
	"github.com/coinbase/chainstorage/internal/workflow"
	"github.com/coinbase/chainstorage/protos/coinbase/c3/common"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

type BackfillerIntegrationTestSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite
}

func TestIntegrationBackfillerTestSuite(t *testing.T) {
	suite.Run(t, new(BackfillerIntegrationTestSuite))
}

func (s *BackfillerIntegrationTestSuite) TestBackfiller() {
	const (
		timeout     = 30 * time.Minute
		tag         = uint32(1)
		startHeight = uint64(17035140)
		endHeight   = uint64(17035160)
		parallelism = 4
	)
	minTimestamp := testutil.MustTimestamp("2023-04-12T23:32:47Z") // timestamp of 17035140
	maxTimestamp := testutil.MustTimestamp("2023-04-12T23:37:35Z") // timestamp of 17035160

	require := testutil.Require(s.T())

	var deps struct {
		fx.In
		Backfiller  *workflow.Backfiller
		BlobStorage blobstorage.BlobStorage
		MetaStorage metastorage.MetaStorage
		Parser      parser.Parser
	}

	env := cadence.NewTestEnv(s)
	env.SetTestTimeout(timeout)
	app := testapp.New(
		s.T(),
		testapp.WithFunctional(),
		cadence.WithTestEnv(env),
		workflow.Module,
		client.Module,
		jsonrpc.Module,
		restapi.Module,
		s3.Module,
		storage.Module,
		parser.Module,
		dlq.Module,
		fx.Populate(&deps),
	)
	defer app.Close()

	_, err := deps.Backfiller.Execute(context.Background(), &workflow.BackfillerRequest{
		Tag:                     tag,
		StartHeight:             startHeight,
		EndHeight:               endHeight,
		NumConcurrentExtractors: parallelism,
	})
	require.NoError(err)

	for i := startHeight; i < endHeight; i++ {
		app.Logger().Info("verifying blocks", zap.Uint64("height", i))
		metadata, err := deps.MetaStorage.GetBlockByHeight(context.Background(), tag, i)
		require.NoError(err)

		require.Equal(tag, metadata.Tag)
		require.Equal(i, metadata.Height)
		require.Equal(i-1, metadata.ParentHeight)
		require.NotEmpty(metadata.Hash)
		require.NotEmpty(metadata.ParentHash)
		require.NotEmpty(metadata.ObjectKeyMain)
		require.Equal(storage_utils.GetCompressionType(metadata.ObjectKeyMain), api.Compression_GZIP)
		require.False(metadata.Skipped)
		require.NotNil(metadata.Timestamp)
		require.LessOrEqual(minTimestamp.Seconds, metadata.Timestamp.Seconds)
		require.GreaterOrEqual(maxTimestamp.Seconds, metadata.Timestamp.Seconds)

		rawBlock, err := deps.BlobStorage.Download(context.Background(), metadata)
		require.NoError(err)
		require.Equal(metadata.Tag, rawBlock.Metadata.Tag)
		require.Equal(metadata.Hash, rawBlock.Metadata.Hash)
		require.Equal(metadata.ParentHash, rawBlock.Metadata.ParentHash)
		require.Equal(metadata.Height, rawBlock.Metadata.Height)
		require.Equal(metadata.ParentHeight, rawBlock.Metadata.ParentHeight)
		require.NotEmpty(rawBlock.Metadata.ObjectKeyMain)
		require.Equal(storage_utils.GetCompressionType(rawBlock.Metadata.ObjectKeyMain), api.Compression_GZIP)
		require.False(rawBlock.Metadata.Skipped)
		require.NotNil(rawBlock.Metadata.Timestamp)
		require.LessOrEqual(minTimestamp.Seconds, rawBlock.Metadata.Timestamp.Seconds)
		require.GreaterOrEqual(maxTimestamp.Seconds, rawBlock.Metadata.Timestamp.Seconds)
	}
}

func (s *BackfillerIntegrationTestSuite) TestBackfiller_MiniBatch() {
	const (
		timeout       = 30 * time.Minute
		tag           = uint32(2)
		startHeight   = uint64(194000000)
		endHeight     = uint64(194000100)
		parallelism   = 20
		miniBatchSize = 5
	)
	require := testutil.Require(s.T())

	var deps struct {
		fx.In
		Backfiller  *workflow.Backfiller
		BlobStorage blobstorage.BlobStorage
		MetaStorage metastorage.MetaStorage
		Parser      parser.Parser
	}

	env := cadence.NewTestEnv(s)
	env.SetTestTimeout(timeout)
	app := testapp.New(
		s.T(),
		testapp.WithFunctional(),
		testapp.WithBlockchainNetwork(common.Blockchain_BLOCKCHAIN_SOLANA, common.Network_NETWORK_SOLANA_MAINNET),
		cadence.WithTestEnv(env),
		workflow.Module,
		client.Module,
		jsonrpc.Module,
		restapi.Module,
		s3.Module,
		storage.Module,
		parser.Module,
		dlq.Module,
		fx.Populate(&deps),
	)
	defer app.Close()

	_, err := deps.Backfiller.Execute(context.Background(), &workflow.BackfillerRequest{
		Tag:                     tag,
		StartHeight:             startHeight,
		EndHeight:               endHeight,
		NumConcurrentExtractors: parallelism,
		MiniBatchSize:           miniBatchSize,
	})
	require.NoError(err)

	for i := startHeight; i < endHeight; i++ {
		app.Logger().Info("verifying blocks", zap.Uint64("height", i))
		metadata, err := deps.MetaStorage.GetBlockByHeight(context.Background(), tag, i)
		require.NoError(err)

		require.Equal(tag, metadata.Tag)
		require.Equal(i, metadata.Height)
		require.Equal(i-1, metadata.ParentHeight)
		require.NotEmpty(metadata.Hash)
		require.NotEmpty(metadata.ParentHash)
		require.NotEmpty(metadata.ObjectKeyMain)
		require.Equal(storage_utils.GetCompressionType(metadata.ObjectKeyMain), api.Compression_GZIP)
		require.False(metadata.Skipped)
		require.NotNil(metadata.Timestamp)

		rawBlock, err := deps.BlobStorage.Download(context.Background(), metadata)
		require.NoError(err)
		require.Equal(metadata.Tag, rawBlock.Metadata.Tag)
		require.Equal(metadata.Hash, rawBlock.Metadata.Hash)
		require.Equal(metadata.ParentHash, rawBlock.Metadata.ParentHash)
		require.Equal(metadata.Height, rawBlock.Metadata.Height)
		require.Equal(metadata.ParentHeight, rawBlock.Metadata.ParentHeight)
		require.NotEmpty(rawBlock.Metadata.ObjectKeyMain)
		require.Equal(storage_utils.GetCompressionType(rawBlock.Metadata.ObjectKeyMain), api.Compression_GZIP)
		require.False(rawBlock.Metadata.Skipped)
		require.NotNil(rawBlock.Metadata.Timestamp)

		nativeBlock, err := deps.Parser.ParseNativeBlock(context.Background(), rawBlock)
		require.NoError(err)
		require.Equal(metadata.Tag, nativeBlock.Tag)
		require.Equal(metadata.Hash, nativeBlock.Hash)
		require.Equal(metadata.ParentHash, nativeBlock.ParentHash)
		require.Equal(metadata.Height, nativeBlock.Height)
		require.Equal(metadata.ParentHeight, nativeBlock.ParentHeight)
	}
}
