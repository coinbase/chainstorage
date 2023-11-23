package activity

import (
	"testing"

	"go.temporal.io/sdk/testsuite"
	"go.uber.org/fx"
	"go.uber.org/mock/gomock"

	"github.com/stretchr/testify/suite"

	"github.com/coinbase/chainstorage/internal/blockchain/client"
	clientmocks "github.com/coinbase/chainstorage/internal/blockchain/client/mocks"
	"github.com/coinbase/chainstorage/internal/blockchain/parser"
	parsermocks "github.com/coinbase/chainstorage/internal/blockchain/parser/mocks"
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

type crossValidatorTestSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite
	ctrl            *gomock.Controller
	metaStorage     *metastoragemocks.MockMetaStorage
	blobStorage     *blobstoragemocks.MockBlobStorage
	validatorClient *clientmocks.MockClient
	crossValidator  *CrossValidator
	parser          *parsermocks.MockParser
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
	s.parser = parsermocks.NewMockParser(s.ctrl)
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
		fx.Provide(func() parser.Parser { return s.parser }),
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
		s.parser.EXPECT().
			ParseNativeBlock(gomock.Any(), block).
			Return(&api.NativeBlock{}, nil).Times(2)
		s.parser.EXPECT().
			CompareNativeBlocks(gomock.Any(), i, gomock.Any(), gomock.Any()).
			Return(nil)
	}

	response, err := s.crossValidator.Execute(s.env.BackgroundContext(), &CrossValidatorRequest{
		Tag:                     blockTag,
		StartHeight:             startHeight,
		ValidationHeightPadding: heightPadding,
		MaxHeightsToValidate:    maxBlocksToValidate,
		Parallelism:             parallelism,
	})
	require.NoError(err)
	require.Equal(&CrossValidatorResponse{
		EndHeight: endHeight,
		BlockGap:  theirHeight - endHeight,
	}, response)
}

func (s *crossValidatorTestSuite) TestValidator_Reorg() {
	const (
		blockTag            = uint32(1)
		startHeight         = uint64(15_000_200)
		ourHeight           = uint64(15_002_000)
		theirHeight         = uint64(15_001_005)
		heightPadding       = uint64(1000)
		maxBlocksToValidate = uint64(50)
		parallelism         = 1
	)

	require := testutil.Require(s.T())

	// Get the latest block
	s.metaStorage.EXPECT().
		GetLatestBlock(gomock.Any(), blockTag).
		Return(testutil.MakeBlockMetadata(ourHeight, blockTag), nil)
	s.validatorClient.EXPECT().
		GetLatestHeight(gomock.Any()).
		Return(theirHeight, nil)

	// Validation block range:
	// endHeight = theirHeight - heightPadding < startHeight
	// Thus crossValidator will return directly with `response.EndHeight = startHeight`
	response, err := s.crossValidator.Execute(s.env.BackgroundContext(), &CrossValidatorRequest{
		Tag:                     blockTag,
		StartHeight:             startHeight,
		ValidationHeightPadding: heightPadding,
		MaxHeightsToValidate:    maxBlocksToValidate,
		Parallelism:             parallelism,
	})
	require.NoError(err)
	require.Equal(startHeight, response.EndHeight)
	require.Equal(theirHeight-startHeight, response.BlockGap)
}
