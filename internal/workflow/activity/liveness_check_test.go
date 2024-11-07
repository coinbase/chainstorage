package activity

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"go.temporal.io/sdk/testsuite"
	"go.uber.org/fx"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap"

	"github.com/coinbase/chainstorage/internal/blockchain/client"
	clientmocks "github.com/coinbase/chainstorage/internal/blockchain/client/mocks"
	"github.com/coinbase/chainstorage/internal/blockchain/endpoints"
	"github.com/coinbase/chainstorage/internal/cadence"
	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/utils/testapp"
	"github.com/coinbase/chainstorage/internal/utils/testutil"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

type LivenessCheckTestSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite
	ctrl                   *gomock.Controller
	masterBlockchainClient *clientmocks.MockClient
	masterEndpointProvider endpoints.EndpointProvider
	cfg                    *config.Config
	app                    testapp.TestApp
	livenessCheck          *LivenessCheck
	logger                 *zap.Logger
	env                    *cadence.TestEnv
}

func TestLivenessCheckTestSuite(t *testing.T) {
	suite.Run(t, new(LivenessCheckTestSuite))
}

func (s *LivenessCheckTestSuite) SetupTest() {
	require := testutil.Require(s.T())

	var deps struct {
		fx.In
		MasterEndpoints endpoints.EndpointProvider `name:"master"`
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
	s.masterBlockchainClient = clientmocks.NewMockClient(s.ctrl)
	cfg, err := config.New()
	require.NoError(err)
	cfg.Chain.Client.Master.EndpointGroup = *endpointGroup
	cfg.Chain.Client.Slave.EndpointGroup = *endpointGroup
	s.cfg = cfg
	s.env = cadence.NewTestActivityEnv(s)
	s.app = testapp.New(
		s.T(),
		fx.Provide(NewLivenessCheck),
		cadence.WithTestEnv(s.env),
		testapp.WithConfig(cfg),
		fx.Provide(fx.Annotated{
			Name: "master",
			Target: func() client.Client {
				return s.masterBlockchainClient
			},
		}),
		fx.Populate(&s.livenessCheck),
		fx.Populate(&s.logger),
		fx.Populate(&deps),
	)
	s.masterEndpointProvider = deps.MasterEndpoints
}

func (s *LivenessCheckTestSuite) TearDownTest() {
	s.app.Close()
	s.ctrl.Finish()
	s.env.AssertExpectations(s.T())
}

func (s *LivenessCheckTestSuite) TestLivenessCheck_WithinSLA() {
	require := testutil.Require(s.T())

	latestHeight := uint64(100)
	now := time.Now().Unix()
	timeSinceLastBlock := 10 * time.Second
	livenessCheckThreshold := 2 * time.Minute

	s.masterBlockchainClient.EXPECT().
		GetLatestHeight(gomock.Any()).
		DoAndReturn(func(ctx context.Context) (uint64, error) {
			require.False(s.masterEndpointProvider.HasFailoverContext(ctx))
			return latestHeight, nil
		})

	blocks := testutil.MakeBlockMetadatasFromStartHeight(latestHeight, 1, tag, testutil.WithTimestamp(now-int64(timeSinceLastBlock.Seconds())))
	s.masterBlockchainClient.EXPECT().
		BatchGetBlockMetadata(gomock.Any(), tag, gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, tag uint32, from uint64, to uint64) ([]*api.BlockMetadata, error) {
			require.Equal(latestHeight, from)
			require.Equal(latestHeight+1, to)
			return blocks, nil
		})

	request := &LivenessCheckRequest{
		Tag:                    tag,
		LivenessCheckThreshold: livenessCheckThreshold,
	}
	response, err := s.livenessCheck.Execute(s.env.BackgroundContext(), request)
	require.NoError(err)
	require.Equal(false, response.LivenessCheckViolation)
}

func (s *LivenessCheckTestSuite) TestLivenessCheck_OutOfSLA() {
	require := testutil.Require(s.T())

	latestHeight := uint64(100)
	now := time.Now().Unix()
	timeSinceLastBlock := 5 * time.Minute
	livenessCheckThreshold := 2 * time.Minute

	s.masterBlockchainClient.EXPECT().
		GetLatestHeight(gomock.Any()).
		DoAndReturn(func(ctx context.Context) (uint64, error) {
			require.False(s.masterEndpointProvider.HasFailoverContext(ctx))
			return latestHeight, nil
		})

	blocks := testutil.MakeBlockMetadatasFromStartHeight(latestHeight, 1, tag, testutil.WithTimestamp(now-int64(timeSinceLastBlock.Seconds())))
	s.masterBlockchainClient.EXPECT().
		BatchGetBlockMetadata(gomock.Any(), tag, gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, tag uint32, from uint64, to uint64) ([]*api.BlockMetadata, error) {
			require.Equal(latestHeight, from)
			require.Equal(latestHeight+1, to)
			return blocks, nil
		})

	request := &LivenessCheckRequest{
		Tag:                    tag,
		LivenessCheckThreshold: livenessCheckThreshold,
	}
	response, err := s.livenessCheck.Execute(s.env.BackgroundContext(), request)
	require.NoError(err)
	require.Equal(true, response.LivenessCheckViolation)
}

func (s *LivenessCheckTestSuite) TestLivenessCheck_Failure() {
	require := testutil.Require(s.T())

	livenessCheckThreshold := 2 * time.Minute

	s.masterBlockchainClient.EXPECT().
		GetLatestHeight(gomock.Any()).
		Return(uint64(0), fmt.Errorf("master client failure"))

	request := &LivenessCheckRequest{
		Tag:                    tag,
		LivenessCheckThreshold: livenessCheckThreshold,
	}
	_, err := s.livenessCheck.Execute(s.env.BackgroundContext(), request)
	require.Error(err)
	require.Contains(err.Error(), "failed to get canonical chain tip height")
}
