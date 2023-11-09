package activity

import (
	"testing"

	"go.temporal.io/sdk/testsuite"
	"go.uber.org/fx"
	"go.uber.org/mock/gomock"

	"github.com/stretchr/testify/suite"

	"github.com/coinbase/chainstorage/internal/blockchain/client"
	clientmocks "github.com/coinbase/chainstorage/internal/blockchain/client/mocks"
	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/storage/blobstorage"
	"github.com/coinbase/chainstorage/internal/storage/metastorage"
	"github.com/coinbase/chainstorage/internal/utils/testutil"

	"github.com/coinbase/chainstorage/internal/cadence"
	blobstoragemocks "github.com/coinbase/chainstorage/internal/storage/blobstorage/mocks"
	metastoragemocks "github.com/coinbase/chainstorage/internal/storage/metastorage/mocks"
	"github.com/coinbase/chainstorage/internal/utils/testapp"
)

type crossValidatorTestSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite
	ctrl            *gomock.Controller
	metaStorage     *metastoragemocks.MockMetaStorage
	blobStorage     *blobstoragemocks.MockBlobStorage
	validatorClient *clientmocks.MockClient
	crossValidator  *CrossValidator
	cfg             *config.Config
	app             testapp.TestApp
	env             *cadence.TestEnv
}

func TestCrossValidatorTestSuite(t *testing.T) {
	suite.Run(t, new(crossValidatorTestSuite))
}

func (s *crossValidatorTestSuite) SetupTest() {
	require := testutil.Require(s.T())

	s.ctrl = gomock.NewController(s.T())
	s.blobStorage = blobstoragemocks.NewMockBlobStorage(s.ctrl)
	s.metaStorage = metastoragemocks.NewMockMetaStorage(s.ctrl)
	s.validatorClient = clientmocks.NewMockClient(s.ctrl)
	cfg, err := config.New()
	require.NoError(err)
	cfg.Workflows.CrossValidator.ValidationPercentage = 100
	s.cfg = cfg
	s.env = cadence.NewTestActivityEnv(s)
	s.app = testapp.New(
		s.T(),
		Module,
		cadence.WithTestEnv(s.env),
		testapp.WithConfig(cfg),
		fx.Provide(func() blobstorage.BlobStorage { return s.blobStorage }),
		fx.Provide(func() metastorage.MetaStorage { return s.metaStorage }),
		fx.Provide(fx.Annotated{
			Name: "validator",
			Target: func() client.Client {
				return s.validatorClient
			},
		}),
		fx.Populate(&s.crossValidator),
	)
}

func (s *crossValidatorTestSuite) TearDownTest() {
	s.app.Close()
	s.ctrl.Finish()
	s.env.AssertExpectations(s.T())
}

func (s *crossValidatorTestSuite) TestValidator() {
	const (
		blockTag            = uint32(1)
		startHeight         = uint64(15_000_000)
		ourHeight           = uint64(15_001_000)
		theirHeight         = uint64(15_001_005)
		heightPadding       = uint64(1000)
		maxBlocksToValidate = uint64(50)
		maxReorgDistance    = uint64(35)
		parallelism         = 1
	)

	require := testutil.Require(s.T())
	endHeight := theirHeight - heightPadding

	// Get the latest block
	s.metaStorage.EXPECT().
		GetLatestBlock(gomock.Any(), blockTag).
		Return(testutil.MakeBlockMetadata(ourHeight, blockTag), nil)
	s.validatorClient.EXPECT().
		GetLatestHeight(gomock.Any()).
		Return(theirHeight, nil)

	// Validate range
	for i := startHeight; i < endHeight; i++ {
		blockMetadata := testutil.MakeBlockMetadata(i, blockTag)
		block := testutil.MakeBlock(i, blockTag)
		s.metaStorage.EXPECT().
			GetBlockByHeight(gomock.Any(), blockTag, i).
			Return(blockMetadata, nil)
		s.blobStorage.EXPECT().
			Download(gomock.Any(), blockMetadata).
			Return(block, nil)
		s.validatorClient.EXPECT().
			GetBlockByHash(gomock.Any(), blockTag, i, gomock.Any()).
			Return(block, nil)
	}

	response, err := s.crossValidator.Execute(s.env.BackgroundContext(), &CrossValidatorRequest{
		Tag:                     blockTag,
		StartHeight:             startHeight,
		ValidationHeightPadding: heightPadding,
		MaxHeightsToValidate:    maxBlocksToValidate,
		Parallelism:             parallelism,
		MaxReorgDistance:        maxReorgDistance,
	})
	require.NoError(err)
	require.Equal(&CrossValidatorResponse{
		EndHeight: endHeight,
		BlockGap:  theirHeight - endHeight,
	}, response)
}
