package activity

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/suite"
	"go.temporal.io/sdk/testsuite"
	"go.uber.org/fx"
	"go.uber.org/mock/gomock"

	"github.com/coinbase/chainstorage/internal/blockchain/client"
	clientmocks "github.com/coinbase/chainstorage/internal/blockchain/client/mocks"
	"github.com/coinbase/chainstorage/internal/blockchain/endpoints"
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

type ValidatorTestSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite
	ctrl                   *gomock.Controller
	metaStorage            *metastoragemocks.MockMetaStorage
	blobStorage            *blobstoragemocks.MockBlobStorage
	masterClient           *clientmocks.MockClient
	slaveClient            *clientmocks.MockClient
	validatorClient        *clientmocks.MockClient
	consensusClient        *clientmocks.MockClient
	masterEndpointProvider endpoints.EndpointProvider
	slaveEndpointProvider  endpoints.EndpointProvider
	cfg                    *config.Config
	parser                 *parsermocks.MockParser
	app                    testapp.TestApp
	validator              *Validator
	env                    *cadence.TestEnv
}

const (
	blockTag            = uint32(2)
	eventTag            = uint32(1)
	startHeight         = uint64(15_000_000)
	startEventId        = int64(15_002_000)
	currentEventId      = int64(15_002_010)
	ourHeight           = uint64(15_001_000)
	theirHeight         = uint64(15_001_005)
	heightPadding       = uint64(1000)
	maxBlocksToValidate = uint64(50)
	maxEventsToValidate = uint64(50)
	parallelism         = 1
)

func TestValidatorTestSuite(t *testing.T) {
	suite.Run(t, new(ValidatorTestSuite))
}

func (s *ValidatorTestSuite) SetupTest() {
	require := testutil.Require(s.T())

	var deps struct {
		fx.In
		MasterEndpoints endpoints.EndpointProvider `name:"master"`
		SlaveEndpoints  endpoints.EndpointProvider `name:"slave"`
	}

	endpointGroup := &config.EndpointGroup{
		Endpoints: []config.Endpoint{
			{
				Name:   "foo",
				Weight: 1,
			},
		},
		EndpointsFailover: []config.Endpoint{
			{
				Name:   "bar",
				Weight: 1,
			},
		},
	}

	s.ctrl = gomock.NewController(s.T())
	s.blobStorage = blobstoragemocks.NewMockBlobStorage(s.ctrl)
	s.metaStorage = metastoragemocks.NewMockMetaStorage(s.ctrl)
	s.masterClient = clientmocks.NewMockClient(s.ctrl)
	s.slaveClient = clientmocks.NewMockClient(s.ctrl)
	s.validatorClient = clientmocks.NewMockClient(s.ctrl)
	s.consensusClient = clientmocks.NewMockClient(s.ctrl)
	s.parser = parsermocks.NewMockParser(s.ctrl)
	cfg, err := config.New()
	require.NoError(err)
	cfg.Chain.Client.Master.EndpointGroup = *endpointGroup
	cfg.Chain.Client.Slave.EndpointGroup = *endpointGroup
	s.cfg = cfg
	s.env = cadence.NewTestActivityEnv(s)
	s.app = testapp.New(
		s.T(),
		fx.Provide(NewValidator),
		cadence.WithTestEnv(s.env),
		testapp.WithConfig(cfg),
		fx.Provide(func() blobstorage.BlobStorage { return s.blobStorage }),
		fx.Provide(func() metastorage.MetaStorage { return s.metaStorage }),
		fx.Provide(fx.Annotated{
			Name: "master",
			Target: func() client.Client {
				return s.masterClient
			},
		}),
		fx.Provide(fx.Annotated{
			Name: "slave",
			Target: func() client.Client {
				return s.slaveClient
			},
		}),
		fx.Provide(fx.Annotated{
			Name: "validator",
			Target: func() client.Client {
				return s.validatorClient
			},
		}),
		fx.Provide(fx.Annotated{
			Name: "consensus",
			Target: func() client.Client {
				return s.consensusClient
			},
		}),
		fx.Provide(func() parser.Parser { return s.parser }),
		fx.Populate(&s.validator),
		fx.Populate(&deps),
	)
	s.masterEndpointProvider = deps.MasterEndpoints
	s.slaveEndpointProvider = deps.SlaveEndpoints
}

func (s *ValidatorTestSuite) TearDownTest() {
	s.app.Close()
	s.ctrl.Finish()
	s.env.AssertExpectations(s.T())
}

func (s *ValidatorTestSuite) TestValidator() {
	require := testutil.Require(s.T())
	endHeight := theirHeight - heightPadding

	// validateLatestBlock
	s.metaStorage.EXPECT().
		GetLatestBlock(gomock.Any(), blockTag).
		Return(testutil.MakeBlockMetadata(ourHeight, blockTag), nil)
	s.blobStorage.EXPECT().
		Download(gomock.Any(), testutil.MakeBlockMetadata(ourHeight, blockTag)).
		Return(testutil.MakeBlock(ourHeight, blockTag), nil)
	s.masterClient.EXPECT().
		GetLatestHeight(gomock.Any()).
		DoAndReturn(func(ctx context.Context) (uint64, error) {
			require.False(s.masterEndpointProvider.HasFailoverContext(ctx))
			require.False(s.slaveEndpointProvider.HasFailoverContext(ctx))
			return theirHeight, nil
		})

	// validateRange
	s.metaStorage.EXPECT().
		GetBlocksByHeightRange(gomock.Any(), blockTag, startHeight, endHeight+1).
		Return(testutil.MakeBlockMetadatasFromStartHeight(startHeight, int(endHeight+1-startHeight), blockTag), nil)

	for i := startHeight; i <= endHeight; i++ {
		s.metaStorage.EXPECT().
			GetBlockByHeight(gomock.Any(), blockTag, i).
			Return(testutil.MakeBlockMetadata(i, blockTag), nil)
		s.blobStorage.EXPECT().
			Download(gomock.Any(), testutil.MakeBlockMetadata(i, blockTag)).
			Return(testutil.MakeBlock(i, blockTag), nil)
	}
	s.slaveClient.EXPECT().
		BatchGetBlockMetadata(gomock.Any(), blockTag, startHeight, endHeight+1).
		Return(testutil.MakeBlockMetadatasFromStartHeight(startHeight, int(endHeight+1-startHeight), blockTag), nil)

	// validateEvents
	s.metaStorage.EXPECT().
		GetMaxEventId(gomock.Any(), eventTag).
		Return(currentEventId, nil)
	s.metaStorage.EXPECT().
		GetEventsAfterEventId(gomock.Any(), eventTag, startEventId-numOfExtraPrevEventsToValidate, maxEventsToValidate+uint64(numOfExtraPrevEventsToValidate)).
		Return(testutil.MakeBlockEventEntries(api.BlockchainEvent_BLOCK_ADDED, eventTag, currentEventId, startHeight, endHeight+1, blockTag), nil)

	// parser
	s.parser.EXPECT().
		ParseNativeBlock(gomock.Any(), gomock.Any()).
		Return(&api.NativeBlock{}, nil).
		AnyTimes()
	s.parser.EXPECT().
		ValidateBlock(gomock.Any(), gomock.Any()).
		Return(nil).
		AnyTimes()
	s.parser.EXPECT().
		ParseRosettaBlock(gomock.Any(), gomock.Any()).
		Return(&api.RosettaBlock{}, nil).
		AnyTimes()
	s.parser.EXPECT().
		ValidateRosettaBlock(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil).
		AnyTimes()

	response, err := s.validator.Execute(s.env.BackgroundContext(), &ValidatorRequest{
		Tag:                     blockTag,
		StartHeight:             startHeight,
		ValidationHeightPadding: heightPadding,
		MaxHeightsToValidate:    maxBlocksToValidate,
		StartEventId:            startEventId,
		MaxEventsToValidate:     maxEventsToValidate,
		Parallelism:             parallelism,
		EventTag:                eventTag,
	})
	require.NoError(err)
	require.Equal(&ValidatorResponse{
		LastValidatedHeight:      endHeight,
		BlockGap:                 theirHeight - endHeight,
		LastValidatedEventId:     currentEventId,
		LastValidatedEventHeight: endHeight,
		EventGap:                 0,
		EventTag:                 eventTag,
	}, response)
}

func (s *ValidatorTestSuite) TestValidator_WithFailover() {
	require := testutil.Require(s.T())
	endHeight := theirHeight - heightPadding

	// validateLatestBlock
	s.metaStorage.EXPECT().
		GetLatestBlock(gomock.Any(), blockTag).
		Return(testutil.MakeBlockMetadata(ourHeight, blockTag), nil)
	s.blobStorage.EXPECT().
		Download(gomock.Any(), testutil.MakeBlockMetadata(ourHeight, blockTag)).
		Return(testutil.MakeBlock(ourHeight, blockTag), nil)
	s.masterClient.EXPECT().
		GetLatestHeight(gomock.Any()).
		DoAndReturn(func(ctx context.Context) (uint64, error) {
			require.True(s.masterEndpointProvider.HasFailoverContext(ctx))
			require.True(s.slaveEndpointProvider.HasFailoverContext(ctx))
			return theirHeight, nil
		})

	// validateRange
	s.metaStorage.EXPECT().
		GetBlocksByHeightRange(gomock.Any(), blockTag, startHeight, endHeight+1).
		Return(testutil.MakeBlockMetadatasFromStartHeight(startHeight, int(endHeight+1-startHeight), blockTag), nil)

	for i := startHeight; i <= endHeight; i++ {
		s.metaStorage.EXPECT().
			GetBlockByHeight(gomock.Any(), blockTag, i).
			Return(testutil.MakeBlockMetadata(i, blockTag), nil)
		s.blobStorage.EXPECT().
			Download(gomock.Any(), testutil.MakeBlockMetadata(i, blockTag)).
			Return(testutil.MakeBlock(i, blockTag), nil)
	}
	s.slaveClient.EXPECT().
		BatchGetBlockMetadata(gomock.Any(), blockTag, startHeight, endHeight+1).
		Return(testutil.MakeBlockMetadatasFromStartHeight(startHeight, int(endHeight+1-startHeight), blockTag), nil)

	// validateEvents
	s.metaStorage.EXPECT().
		GetMaxEventId(gomock.Any(), eventTag).
		Return(currentEventId, nil)
	s.metaStorage.EXPECT().
		GetEventsAfterEventId(gomock.Any(), eventTag, startEventId-numOfExtraPrevEventsToValidate, maxEventsToValidate+uint64(numOfExtraPrevEventsToValidate)).
		Return(testutil.MakeBlockEventEntries(api.BlockchainEvent_BLOCK_ADDED, eventTag, currentEventId, startHeight, endHeight+1, blockTag), nil)

	// parser
	s.parser.EXPECT().
		ParseNativeBlock(gomock.Any(), gomock.Any()).
		Return(&api.NativeBlock{}, nil).
		AnyTimes()
	s.parser.EXPECT().
		ValidateBlock(gomock.Any(), gomock.Any()).
		Return(nil).
		AnyTimes()
	s.parser.EXPECT().
		ParseRosettaBlock(gomock.Any(), gomock.Any()).
		Return(&api.RosettaBlock{}, nil).
		AnyTimes()
	s.parser.EXPECT().
		ValidateRosettaBlock(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil).
		AnyTimes()

	response, err := s.validator.Execute(s.env.BackgroundContext(), &ValidatorRequest{
		Tag:                     blockTag,
		StartHeight:             startHeight,
		ValidationHeightPadding: heightPadding,
		MaxHeightsToValidate:    maxBlocksToValidate,
		StartEventId:            startEventId,
		MaxEventsToValidate:     maxEventsToValidate,
		Parallelism:             parallelism,
		EventTag:                eventTag,
		Failover:                true,
	})
	require.NoError(err)
	require.Equal(&ValidatorResponse{
		LastValidatedHeight:      endHeight,
		BlockGap:                 theirHeight - endHeight,
		LastValidatedEventId:     currentEventId,
		LastValidatedEventHeight: endHeight,
		EventGap:                 0,
		EventTag:                 eventTag,
	}, response)
}

func (s *ValidatorTestSuite) TestValidator_Reorg() {
	require := testutil.Require(s.T())
	startHeightReorg := startHeight + 200

	s.metaStorage.EXPECT().
		GetLatestBlock(gomock.Any(), blockTag).
		Return(testutil.MakeBlockMetadata(ourHeight, blockTag), nil)
	s.masterClient.EXPECT().
		GetLatestHeight(gomock.Any()).
		DoAndReturn(func(ctx context.Context) (uint64, error) {
			require.False(s.masterEndpointProvider.HasFailoverContext(ctx))
			require.False(s.slaveEndpointProvider.HasFailoverContext(ctx))
			return theirHeight, nil
		})
	// validateLatestBlock
	s.blobStorage.EXPECT().
		Download(gomock.Any(), testutil.MakeBlockMetadata(ourHeight, blockTag)).
		Return(testutil.MakeBlock(ourHeight, blockTag), nil)

	// validateEvents
	s.metaStorage.EXPECT().
		GetMaxEventId(gomock.Any(), eventTag).
		Return(currentEventId, nil)
	s.metaStorage.EXPECT().
		GetEventsAfterEventId(gomock.Any(), eventTag, startEventId-numOfExtraPrevEventsToValidate, maxEventsToValidate+uint64(numOfExtraPrevEventsToValidate)).
		Return(testutil.MakeBlockEventEntries(api.BlockchainEvent_BLOCK_ADDED, eventTag, currentEventId, startHeight, startHeight+1, blockTag), nil)

	// parser
	s.parser.EXPECT().
		ParseNativeBlock(gomock.Any(), gomock.Any()).
		Return(&api.NativeBlock{}, nil).
		AnyTimes()
	s.parser.EXPECT().
		ValidateBlock(gomock.Any(), gomock.Any()).
		Return(nil).
		AnyTimes()
	s.parser.EXPECT().
		ParseRosettaBlock(gomock.Any(), gomock.Any()).
		Return(&api.RosettaBlock{}, nil).
		AnyTimes()
	s.parser.EXPECT().
		ValidateRosettaBlock(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil).
		AnyTimes()

	// Validation block range:
	// endHeight = theirHeight - heightPadding < startHeightReorg
	// Thus validator will return directly with `response.LastValidatedHeight = startHeightReorg`
	response, err := s.validator.Execute(s.env.BackgroundContext(), &ValidatorRequest{
		Tag:                     blockTag,
		StartHeight:             startHeightReorg,
		ValidationHeightPadding: heightPadding,
		MaxHeightsToValidate:    maxBlocksToValidate,
		StartEventId:            startEventId,
		MaxEventsToValidate:     maxEventsToValidate,
		Parallelism:             parallelism,
		EventTag:                eventTag,
	})
	require.NoError(err)
	require.Equal(&ValidatorResponse{
		LastValidatedHeight:      startHeightReorg,
		BlockGap:                 theirHeight - startHeightReorg,
		LastValidatedEventId:     currentEventId,
		LastValidatedEventHeight: startHeight,
		EventGap:                 0,
		EventTag:                 eventTag,
	}, response)
}

func (s *ValidatorTestSuite) TestValidator_BlockValidation_Failure() {
	require := testutil.Require(s.T())
	endHeight := theirHeight - heightPadding

	// validateLatestBlock
	s.metaStorage.EXPECT().
		GetLatestBlock(gomock.Any(), blockTag).
		Return(testutil.MakeBlockMetadata(ourHeight, blockTag), nil)
	s.masterClient.EXPECT().
		GetLatestHeight(gomock.Any()).
		DoAndReturn(func(ctx context.Context) (uint64, error) {
			require.False(s.masterEndpointProvider.HasFailoverContext(ctx))
			require.False(s.slaveEndpointProvider.HasFailoverContext(ctx))
			return theirHeight, nil
		})
	s.blobStorage.EXPECT().
		Download(gomock.Any(), testutil.MakeBlockMetadata(ourHeight, blockTag)).
		Return(testutil.MakeBlock(ourHeight, blockTag), nil)
	s.parser.EXPECT().
		ParseNativeBlock(gomock.Any(), gomock.Any()).
		Return(&api.NativeBlock{}, nil)
	s.parser.EXPECT().
		ValidateBlock(gomock.Any(), gomock.Any()).
		Return(nil)
	s.parser.EXPECT().
		ParseRosettaBlock(gomock.Any(), gomock.Any()).
		Return(&api.RosettaBlock{}, nil)
	s.parser.EXPECT().
		ValidateRosettaBlock(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil)

	// validateRange
	s.metaStorage.EXPECT().
		GetBlocksByHeightRange(gomock.Any(), blockTag, startHeight, endHeight+1).
		Return(testutil.MakeBlockMetadatasFromStartHeight(startHeight, int(endHeight+1-startHeight), blockTag), nil)

	for i := startHeight; i <= endHeight; i++ {
		s.metaStorage.EXPECT().
			GetBlockByHeight(gomock.Any(), blockTag, i).
			Return(testutil.MakeBlockMetadata(i, blockTag), nil)
		s.blobStorage.EXPECT().
			Download(gomock.Any(), testutil.MakeBlockMetadata(i, blockTag)).
			Return(testutil.MakeBlock(i, blockTag), nil)
		s.parser.EXPECT().
			ParseNativeBlock(gomock.Any(), gomock.Any()).
			Return(&api.NativeBlock{}, nil).
			AnyTimes()
		s.parser.EXPECT().
			ValidateBlock(gomock.Any(), gomock.Any()).
			Return(fmt.Errorf("mock error"))
	}
	s.slaveClient.EXPECT().
		BatchGetBlockMetadata(gomock.Any(), blockTag, startHeight, endHeight+1).
		Return(testutil.MakeBlockMetadatasFromStartHeight(startHeight, int(endHeight+1-startHeight), blockTag), nil)

	// validateEvents
	s.metaStorage.EXPECT().
		GetMaxEventId(gomock.Any(), eventTag).
		Return(currentEventId, nil)
	s.metaStorage.EXPECT().
		GetEventsAfterEventId(gomock.Any(), eventTag, startEventId-numOfExtraPrevEventsToValidate, maxEventsToValidate+uint64(numOfExtraPrevEventsToValidate)).
		Return(testutil.MakeBlockEventEntries(api.BlockchainEvent_BLOCK_ADDED, eventTag, currentEventId, startHeight, endHeight+1, blockTag), nil)

	response, err := s.validator.Execute(s.env.BackgroundContext(), &ValidatorRequest{
		Tag:                     blockTag,
		StartHeight:             startHeight,
		ValidationHeightPadding: heightPadding,
		MaxHeightsToValidate:    maxBlocksToValidate,
		StartEventId:            startEventId,
		MaxEventsToValidate:     maxEventsToValidate,
		Parallelism:             parallelism,
		EventTag:                eventTag,
	})
	require.NoError(err)
	require.Equal(&ValidatorResponse{
		LastValidatedHeight:      endHeight,
		BlockGap:                 theirHeight - endHeight,
		LastValidatedEventId:     currentEventId,
		LastValidatedEventHeight: endHeight,
		EventGap:                 0,
		EventTag:                 eventTag,
	}, response)
}

func (s *ValidatorTestSuite) TestValidator_BlockValidation_NotImplemented() {
	require := testutil.Require(s.T())
	endHeight := theirHeight - heightPadding

	// validateLatestBlock
	s.metaStorage.EXPECT().
		GetLatestBlock(gomock.Any(), blockTag).
		Return(testutil.MakeBlockMetadata(ourHeight, blockTag), nil)
	s.masterClient.EXPECT().
		GetLatestHeight(gomock.Any()).
		DoAndReturn(func(ctx context.Context) (uint64, error) {
			require.False(s.masterEndpointProvider.HasFailoverContext(ctx))
			require.False(s.slaveEndpointProvider.HasFailoverContext(ctx))
			return theirHeight, nil
		})
	s.blobStorage.EXPECT().
		Download(gomock.Any(), testutil.MakeBlockMetadata(ourHeight, blockTag)).
		Return(testutil.MakeBlock(ourHeight, blockTag), nil)
	s.parser.EXPECT().
		ParseNativeBlock(gomock.Any(), gomock.Any()).
		Return(&api.NativeBlock{}, nil)
	s.parser.EXPECT().
		ValidateBlock(gomock.Any(), gomock.Any()).
		Return(parser.ErrNotImplemented)
	s.parser.EXPECT().
		ParseRosettaBlock(gomock.Any(), gomock.Any()).
		Return(&api.RosettaBlock{}, nil)
	s.parser.EXPECT().
		ValidateRosettaBlock(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil)

	// validateRange
	s.metaStorage.EXPECT().
		GetBlocksByHeightRange(gomock.Any(), blockTag, startHeight, endHeight+1).
		Return(testutil.MakeBlockMetadatasFromStartHeight(startHeight, int(endHeight+1-startHeight), blockTag), nil)

	for i := startHeight; i <= endHeight; i++ {
		s.metaStorage.EXPECT().
			GetBlockByHeight(gomock.Any(), blockTag, i).
			Return(testutil.MakeBlockMetadata(i, blockTag), nil)
		s.blobStorage.EXPECT().
			Download(gomock.Any(), testutil.MakeBlockMetadata(i, blockTag)).
			Return(testutil.MakeBlock(i, blockTag), nil)
		s.parser.EXPECT().
			ParseNativeBlock(gomock.Any(), gomock.Any()).
			Return(&api.NativeBlock{}, nil)
		s.parser.EXPECT().
			ValidateBlock(gomock.Any(), gomock.Any()).
			Return(parser.ErrNotImplemented)
		s.parser.EXPECT().
			ParseRosettaBlock(gomock.Any(), gomock.Any()).
			Return(&api.RosettaBlock{}, nil)
		s.parser.EXPECT().
			ValidateRosettaBlock(gomock.Any(), gomock.Any(), gomock.Any()).
			Return(nil)
	}
	s.slaveClient.EXPECT().
		BatchGetBlockMetadata(gomock.Any(), blockTag, startHeight, endHeight+1).
		Return(testutil.MakeBlockMetadatasFromStartHeight(startHeight, int(endHeight+1-startHeight), blockTag), nil)

	// validateEvents
	s.metaStorage.EXPECT().
		GetMaxEventId(gomock.Any(), eventTag).
		Return(currentEventId, nil)
	s.metaStorage.EXPECT().
		GetEventsAfterEventId(gomock.Any(), eventTag, startEventId-numOfExtraPrevEventsToValidate, maxEventsToValidate+uint64(numOfExtraPrevEventsToValidate)).
		Return(testutil.MakeBlockEventEntries(api.BlockchainEvent_BLOCK_ADDED, eventTag, currentEventId, startHeight, endHeight+1, blockTag), nil)

	response, err := s.validator.Execute(s.env.BackgroundContext(), &ValidatorRequest{
		Tag:                     blockTag,
		StartHeight:             startHeight,
		ValidationHeightPadding: heightPadding,
		MaxHeightsToValidate:    maxBlocksToValidate,
		StartEventId:            startEventId,
		MaxEventsToValidate:     maxEventsToValidate,
		Parallelism:             parallelism,
		EventTag:                eventTag,
	})
	require.NoError(err)
	require.Equal(&ValidatorResponse{
		LastValidatedHeight:      endHeight,
		BlockGap:                 theirHeight - endHeight,
		LastValidatedEventId:     currentEventId,
		LastValidatedEventHeight: endHeight,
		EventGap:                 0,
		EventTag:                 eventTag,
	}, response)
}

func (s *ValidatorTestSuite) TestValidator_RosettaBlockValidation_NotImplemented() {
	require := testutil.Require(s.T())
	endHeight := theirHeight - heightPadding

	// validateLatestBlock
	s.metaStorage.EXPECT().
		GetLatestBlock(gomock.Any(), blockTag).
		Return(testutil.MakeBlockMetadata(ourHeight, blockTag), nil)
	s.masterClient.EXPECT().
		GetLatestHeight(gomock.Any()).
		DoAndReturn(func(ctx context.Context) (uint64, error) {
			require.False(s.masterEndpointProvider.HasFailoverContext(ctx))
			require.False(s.slaveEndpointProvider.HasFailoverContext(ctx))
			return theirHeight, nil
		})
	s.blobStorage.EXPECT().
		Download(gomock.Any(), testutil.MakeBlockMetadata(ourHeight, blockTag)).
		Return(testutil.MakeBlock(ourHeight, blockTag), nil)
	s.parser.EXPECT().
		ParseNativeBlock(gomock.Any(), gomock.Any()).
		Return(&api.NativeBlock{}, nil)
	s.parser.EXPECT().
		ValidateBlock(gomock.Any(), gomock.Any()).
		Return(nil)
	s.parser.EXPECT().
		ParseRosettaBlock(gomock.Any(), gomock.Any()).
		Return(&api.RosettaBlock{}, nil)
	s.parser.EXPECT().
		ValidateRosettaBlock(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(parser.ErrNotImplemented)

	// validateRange
	s.metaStorage.EXPECT().
		GetBlocksByHeightRange(gomock.Any(), blockTag, startHeight, endHeight+1).
		Return(testutil.MakeBlockMetadatasFromStartHeight(startHeight, int(endHeight+1-startHeight), blockTag), nil)

	for i := startHeight; i <= endHeight; i++ {
		s.metaStorage.EXPECT().
			GetBlockByHeight(gomock.Any(), blockTag, i).
			Return(testutil.MakeBlockMetadata(i, blockTag), nil)
		s.blobStorage.EXPECT().
			Download(gomock.Any(), testutil.MakeBlockMetadata(i, blockTag)).
			Return(testutil.MakeBlock(i, blockTag), nil)
		s.parser.EXPECT().
			ParseNativeBlock(gomock.Any(), gomock.Any()).
			Return(&api.NativeBlock{}, nil)
		s.parser.EXPECT().
			ValidateBlock(gomock.Any(), gomock.Any()).
			Return(nil)
		s.parser.EXPECT().
			ParseRosettaBlock(gomock.Any(), gomock.Any()).
			Return(&api.RosettaBlock{}, nil)
		s.parser.EXPECT().
			ValidateRosettaBlock(gomock.Any(), gomock.Any(), gomock.Any()).
			Return(parser.ErrNotImplemented)
	}
	s.slaveClient.EXPECT().
		BatchGetBlockMetadata(gomock.Any(), blockTag, startHeight, endHeight+1).
		Return(testutil.MakeBlockMetadatasFromStartHeight(startHeight, int(endHeight+1-startHeight), blockTag), nil)

	// validateEvents
	s.metaStorage.EXPECT().
		GetMaxEventId(gomock.Any(), eventTag).
		Return(currentEventId, nil)
	s.metaStorage.EXPECT().
		GetEventsAfterEventId(gomock.Any(), eventTag, startEventId-numOfExtraPrevEventsToValidate, maxEventsToValidate+uint64(numOfExtraPrevEventsToValidate)).
		Return(testutil.MakeBlockEventEntries(api.BlockchainEvent_BLOCK_ADDED, eventTag, currentEventId, startHeight, endHeight+1, blockTag), nil)

	response, err := s.validator.Execute(s.env.BackgroundContext(), &ValidatorRequest{
		Tag:                     blockTag,
		StartHeight:             startHeight,
		ValidationHeightPadding: heightPadding,
		MaxHeightsToValidate:    maxBlocksToValidate,
		StartEventId:            startEventId,
		MaxEventsToValidate:     maxEventsToValidate,
		Parallelism:             parallelism,
		EventTag:                eventTag,
	})
	require.NoError(err)
	require.Equal(&ValidatorResponse{
		LastValidatedHeight:      endHeight,
		BlockGap:                 theirHeight - endHeight,
		LastValidatedEventId:     currentEventId,
		LastValidatedEventHeight: endHeight,
		EventGap:                 0,
		EventTag:                 eventTag,
	}, response)
}

func (s *ValidatorTestSuite) TestValidator_ParseRosetta_NotImplemented() {
	require := testutil.Require(s.T())
	endHeight := theirHeight - heightPadding

	// validateLatestBlock
	s.metaStorage.EXPECT().
		GetLatestBlock(gomock.Any(), blockTag).
		Return(testutil.MakeBlockMetadata(ourHeight, blockTag), nil)
	s.blobStorage.EXPECT().
		Download(gomock.Any(), testutil.MakeBlockMetadata(ourHeight, blockTag)).
		Return(testutil.MakeBlock(ourHeight, blockTag), nil)
	s.masterClient.EXPECT().
		GetLatestHeight(gomock.Any()).
		DoAndReturn(func(ctx context.Context) (uint64, error) {
			require.False(s.masterEndpointProvider.HasFailoverContext(ctx))
			require.False(s.slaveEndpointProvider.HasFailoverContext(ctx))
			return theirHeight, nil
		})

	// validateRange
	s.metaStorage.EXPECT().
		GetBlocksByHeightRange(gomock.Any(), blockTag, startHeight, endHeight+1).
		Return(testutil.MakeBlockMetadatasFromStartHeight(startHeight, int(endHeight+1-startHeight), blockTag), nil)

	for i := startHeight; i <= endHeight; i++ {
		s.metaStorage.EXPECT().
			GetBlockByHeight(gomock.Any(), blockTag, i).
			Return(testutil.MakeBlockMetadata(i, blockTag), nil)
		s.blobStorage.EXPECT().
			Download(gomock.Any(), testutil.MakeBlockMetadata(i, blockTag)).
			Return(testutil.MakeBlock(i, blockTag), nil)
	}
	s.slaveClient.EXPECT().
		BatchGetBlockMetadata(gomock.Any(), blockTag, startHeight, endHeight+1).
		Return(testutil.MakeBlockMetadatasFromStartHeight(startHeight, int(endHeight+1-startHeight), blockTag), nil)

	// validateEvents
	s.metaStorage.EXPECT().
		GetMaxEventId(gomock.Any(), eventTag).
		Return(currentEventId, nil)
	s.metaStorage.EXPECT().
		GetEventsAfterEventId(gomock.Any(), eventTag, startEventId-numOfExtraPrevEventsToValidate, maxEventsToValidate+uint64(numOfExtraPrevEventsToValidate)).
		Return(testutil.MakeBlockEventEntries(api.BlockchainEvent_BLOCK_ADDED, eventTag, currentEventId, startHeight, endHeight+1, blockTag), nil)

	// parser
	s.parser.EXPECT().
		ParseNativeBlock(gomock.Any(), gomock.Any()).
		Return(&api.NativeBlock{}, nil).
		AnyTimes()
	s.parser.EXPECT().
		ValidateBlock(gomock.Any(), gomock.Any()).
		Return(nil).
		AnyTimes()
	s.parser.EXPECT().
		ParseRosettaBlock(gomock.Any(), gomock.Any()).
		Return(nil, parser.ErrNotImplemented).
		AnyTimes()

	response, err := s.validator.Execute(s.env.BackgroundContext(), &ValidatorRequest{
		Tag:                     blockTag,
		StartHeight:             startHeight,
		ValidationHeightPadding: heightPadding,
		MaxHeightsToValidate:    maxBlocksToValidate,
		StartEventId:            startEventId,
		MaxEventsToValidate:     maxEventsToValidate,
		Parallelism:             parallelism,
		EventTag:                eventTag,
	})
	require.NoError(err)
	require.Equal(&ValidatorResponse{
		LastValidatedHeight:      endHeight,
		BlockGap:                 theirHeight - endHeight,
		LastValidatedEventId:     currentEventId,
		LastValidatedEventHeight: endHeight,
		EventGap:                 0,
		EventTag:                 eventTag,
	}, response)
}

func (s *ValidatorTestSuite) TestValidator_WithSkipBlock() {
	// This test verifies the following case:
	// 15_000_000-15_000_003 (non-skipped), 15_000_004-15_000_005 (skipped)

	require := testutil.Require(s.T())
	endHeight := theirHeight - heightPadding
	lastNonSkipBlock := uint64(15_000_003)

	// validateLatestBlock
	s.metaStorage.EXPECT().
		GetLatestBlock(gomock.Any(), blockTag).
		Return(testutil.MakeBlockMetadata(ourHeight, blockTag), nil)
	s.blobStorage.EXPECT().
		Download(gomock.Any(), testutil.MakeBlockMetadata(ourHeight, blockTag)).
		Return(testutil.MakeBlock(ourHeight, blockTag), nil)
	s.masterClient.EXPECT().
		GetLatestHeight(gomock.Any()).
		DoAndReturn(func(ctx context.Context) (uint64, error) {
			require.False(s.masterEndpointProvider.HasFailoverContext(ctx))
			require.False(s.slaveEndpointProvider.HasFailoverContext(ctx))
			return theirHeight, nil
		})

	// validateRange
	blocks := testutil.MakeBlockMetadatasFromStartHeight(startHeight, int(lastNonSkipBlock-startHeight+1), blockTag)
	blocks = append(blocks, testutil.MakeBlockMetadatasFromStartHeight(lastNonSkipBlock+1, int(endHeight-lastNonSkipBlock), blockTag, blockSkippedOption)...)
	s.metaStorage.EXPECT().
		GetBlocksByHeightRange(gomock.Any(), blockTag, startHeight, endHeight+1).
		Return(blocks, nil)

	for i := startHeight; i <= lastNonSkipBlock; i++ {
		s.metaStorage.EXPECT().
			GetBlockByHeight(gomock.Any(), blockTag, i).
			Return(testutil.MakeBlockMetadata(i, blockTag), nil)
		s.blobStorage.EXPECT().
			Download(gomock.Any(), testutil.MakeBlockMetadata(i, blockTag)).
			Return(testutil.MakeBlock(i, blockTag), nil)
	}
	s.slaveClient.EXPECT().
		BatchGetBlockMetadata(gomock.Any(), blockTag, startHeight, lastNonSkipBlock+1).
		Return(testutil.MakeBlockMetadatasFromStartHeight(startHeight, int(lastNonSkipBlock+1-startHeight), blockTag), nil)

	// validateEvents
	s.metaStorage.EXPECT().
		GetMaxEventId(gomock.Any(), eventTag).
		Return(currentEventId, nil)
	s.metaStorage.EXPECT().
		GetEventsAfterEventId(gomock.Any(), eventTag, startEventId-numOfExtraPrevEventsToValidate, maxEventsToValidate+uint64(numOfExtraPrevEventsToValidate)).
		Return(testutil.MakeBlockEventEntries(api.BlockchainEvent_BLOCK_ADDED, eventTag, currentEventId, startHeight, endHeight+1, blockTag), nil)

	// parser
	s.parser.EXPECT().
		ParseNativeBlock(gomock.Any(), gomock.Any()).
		Return(&api.NativeBlock{}, nil).
		AnyTimes()
	s.parser.EXPECT().
		ValidateBlock(gomock.Any(), gomock.Any()).
		Return(nil).
		AnyTimes()
	s.parser.EXPECT().
		ParseRosettaBlock(gomock.Any(), gomock.Any()).
		Return(&api.RosettaBlock{}, nil).
		AnyTimes()
	s.parser.EXPECT().
		ValidateRosettaBlock(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil).
		AnyTimes()

	response, err := s.validator.Execute(s.env.BackgroundContext(), &ValidatorRequest{
		Tag:                     blockTag,
		StartHeight:             startHeight,
		ValidationHeightPadding: heightPadding,
		MaxHeightsToValidate:    maxBlocksToValidate,
		StartEventId:            startEventId,
		MaxEventsToValidate:     maxEventsToValidate,
		Parallelism:             parallelism,
		EventTag:                eventTag,
	})
	require.NoError(err)
	require.Equal(&ValidatorResponse{
		LastValidatedHeight:      lastNonSkipBlock,
		BlockGap:                 theirHeight - lastNonSkipBlock,
		LastValidatedEventId:     currentEventId,
		LastValidatedEventHeight: endHeight,
		EventGap:                 0,
		EventTag:                 eventTag,
	}, response)
}
