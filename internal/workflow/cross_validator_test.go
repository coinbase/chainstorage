package workflow

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/suite"
	"go.temporal.io/sdk/testsuite"
	"go.uber.org/fx"
	"go.uber.org/mock/gomock"

	"github.com/coinbase/chainstorage/internal/blockchain/client"
	clientmocks "github.com/coinbase/chainstorage/internal/blockchain/client/mocks"
	"github.com/coinbase/chainstorage/internal/blockchain/jsonrpc"
	"github.com/coinbase/chainstorage/internal/blockchain/parser"
	"github.com/coinbase/chainstorage/internal/cadence"
	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/storage/blobstorage"
	blobstoragemocks "github.com/coinbase/chainstorage/internal/storage/blobstorage/mocks"
	"github.com/coinbase/chainstorage/internal/storage/metastorage"
	metastoragemocks "github.com/coinbase/chainstorage/internal/storage/metastorage/mocks"
	"github.com/coinbase/chainstorage/internal/utils/testapp"
	"github.com/coinbase/chainstorage/internal/utils/testutil"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

const (
	validatorTag                = uint32(1)
	validatorStartHeight        = uint64(100)
	validatorClientLatestHeight = uint64(700)
	validatorMaxHeightIngested  = uint64(430)
	validatorMaxReorgDistance   = uint64(100)
	validatorHeightPadding      = validatorMaxReorgDistance * validationHeightPaddingMultiplier
	validatorBatchSize          = 290
	validatorCheckpointSize     = 2
	validatorEndHeight          = validatorClientLatestHeight - validatorHeightPadding
	workflowStartHeight         = uint64(50)
)

type crossValidatorTestSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite
	env             *cadence.TestEnv
	ctrl            *gomock.Controller
	validatorClient *clientmocks.MockClient
	metaStorage     *metastoragemocks.MockMetaStorage
	blobStorage     *blobstoragemocks.MockBlobStorage
	crossValidator  *CrossValidator
	parser          parser.Parser
	app             testapp.TestApp
	cfg             *config.Config
}

func TestCrossValidatorTestSuite(t *testing.T) {
	suite.Run(t, new(crossValidatorTestSuite))
}

func (s *crossValidatorTestSuite) SetupTest() {
	require := testutil.Require(s.T())

	// Override config to speed up the test.
	cfg, err := config.New()
	require.NoError(err)
	cfg.Workflows.CrossValidator.BatchSize = validatorBatchSize
	cfg.Workflows.CrossValidator.CheckpointSize = validatorCheckpointSize
	cfg.Workflows.CrossValidator.IrreversibleDistance = validatorMaxReorgDistance
	cfg.Workflows.CrossValidator.ValidationStartHeight = workflowStartHeight
	cfg.Workflows.CrossValidator.ValidationPercentage = 100
	s.cfg = cfg

	s.env = cadence.NewTestEnv(s)
	s.ctrl = gomock.NewController(s.T())
	s.validatorClient = clientmocks.NewMockClient(s.ctrl)
	s.metaStorage = metastoragemocks.NewMockMetaStorage(s.ctrl)
	s.blobStorage = blobstoragemocks.NewMockBlobStorage(s.ctrl)
	s.app = testapp.New(
		s.T(),
		Module,
		testapp.WithConfig(cfg),
		cadence.WithTestEnv(s.env),
		parser.Module,
		fx.Provide(func() blobstorage.BlobStorage {
			return s.blobStorage
		}),
		fx.Provide(fx.Annotated{
			Name: "validator",
			Target: func() client.Client {
				return s.validatorClient
			},
		}),
		fx.Provide(func() metastorage.MetaStorage {
			return s.metaStorage
		}),
		fx.Populate(&s.crossValidator),
		fx.Populate(&s.parser),
	)
}

func (s *crossValidatorTestSuite) TearDownTest() {
	s.app.Close()
	s.ctrl.Finish()
	s.env.AssertExpectations(s.T())
}

func (s *crossValidatorTestSuite) TestCrossValidator() {
	require := testutil.Require(s.T())

	seen := struct {
		validatorClient sync.Map
		blobStorage     sync.Map
		metaStorage     sync.Map
	}{}

	// Get the latest block
	s.metaStorage.EXPECT().
		GetLatestBlock(gomock.Any(), validatorTag).
		Times(2).
		Return(testutil.MakeBlockMetadata(validatorMaxHeightIngested, validatorTag), nil)
	s.validatorClient.EXPECT().
		GetLatestHeight(gomock.Any()).
		Times(2).
		Return(validatorClientLatestHeight, nil)

	// Validate range
	s.metaStorage.EXPECT().GetBlockByHeight(gomock.Any(), validatorTag, gomock.Any()).
		Times(int(validatorEndHeight - validatorStartHeight)).
		DoAndReturn(func(ctx context.Context, tag uint32, height uint64) (*api.BlockMetadata, error) {
			seen.metaStorage.LoadOrStore(height, true)
			return testutil.MakeBlockMetadata(height, tag), nil
		})
	s.blobStorage.EXPECT().Download(gomock.Any(), gomock.Any()).
		Times(int(validatorEndHeight - validatorStartHeight)).
		DoAndReturn(func(ctx context.Context, metadata *api.BlockMetadata) (*api.Block, error) {
			height := metadata.Height
			seen.blobStorage.LoadOrStore(height, true)
			return testutil.MakeBlock(height, metadata.Tag), nil
		})
	s.validatorClient.EXPECT().GetBlockByHash(gomock.Any(), validatorTag, gomock.Any(), gomock.Any()).
		Times(int(validatorEndHeight - validatorStartHeight)).
		DoAndReturn(func(ctx context.Context, tag uint32, height uint64, hash string, opts ...jsonrpc.Option) (*api.Block, error) {
			seen.validatorClient.LoadOrStore(height, true)
			return testutil.MakeBlock(height, tag), nil
		})

	_, err := s.crossValidator.Execute(context.Background(), &CrossValidatorRequest{
		StartHeight: validatorStartHeight,
		Tag:         validatorTag,
	})
	require.Error(err)
	require.True(IsContinueAsNewError(err))

	for i := validatorStartHeight; i < validatorEndHeight; i++ {
		v, ok := seen.validatorClient.Load(i)
		require.True(ok)
		require.True(v.(bool))
		v, ok = seen.blobStorage.Load(i)
		require.True(ok)
		require.True(v.(bool))
		v, ok = seen.metaStorage.Load(i)
		require.True(ok)
		require.True(v.(bool))
	}
}

func (s *crossValidatorTestSuite) TestCrossValidator_InvalidStartHeight() {
	require := testutil.Require(s.T())

	_, err := s.crossValidator.Execute(context.Background(), &CrossValidatorRequest{
		StartHeight: workflowStartHeight - 1,
		Tag:         validatorTag,
	})
	require.Error(err)
	require.Contains(err.Error(), "invalid startHeight")
}
