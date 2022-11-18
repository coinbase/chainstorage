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
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

type BackfillerIntegrationTestSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite
}

func TestIntegrationBackfillerTestSuite(t *testing.T) {
	suite.Run(t, new(BackfillerIntegrationTestSuite))
}

func (s *BackfillerIntegrationTestSuite) TestBackfillerIntegration() {
	const (
		timeout     = 30 * time.Minute
		tag         = uint32(1)
		startHeight = uint64(15373483)
		endHeight   = uint64(15373503)
		parallelism = 4
	)
	minTimestamp := testutil.MustTimestamp("2022-08-19T20:43:25Z") // timestamp of 15373483
	maxTimestamp := testutil.MustTimestamp("2022-08-19T20:48:51Z") // timestamp of 15373503

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
