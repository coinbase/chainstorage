package workflow

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"go.uber.org/fx"
	"go.uber.org/mock/gomock"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/blockchain/client"
	clientmocks "github.com/coinbase/chainstorage/internal/blockchain/client/mocks"
	"github.com/coinbase/chainstorage/internal/blockchain/endpoints"
	"github.com/coinbase/chainstorage/internal/cadence"
	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/storage/blobstorage"
	blobstoragemocks "github.com/coinbase/chainstorage/internal/storage/blobstorage/mocks"
	"github.com/coinbase/chainstorage/internal/storage/metastorage"
	metastoragemocks "github.com/coinbase/chainstorage/internal/storage/metastorage/mocks"
	"github.com/coinbase/chainstorage/internal/utils/testapp"
	"github.com/coinbase/chainstorage/internal/utils/testutil"
	"github.com/coinbase/chainstorage/internal/workflow/activity"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

const (
	pollerCheckpointSize       = 10
	parallelism          uint8 = 2
)

type pollerTestSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite
	env                       *cadence.TestEnv
	metaStorage               *metastoragemocks.MockMetaStorage
	blobStorage               *blobstoragemocks.MockBlobStorage
	masterBlockchainClient    *clientmocks.MockClient
	slaveBlockchainClient     *clientmocks.MockClient
	validatorBlockchainClient *clientmocks.MockClient
	masterEndpointProvider    endpoints.EndpointProvider
	slaveEndpointProvider     endpoints.EndpointProvider
	poller                    *Poller
	app                       testapp.TestApp
	ctrl                      *gomock.Controller
	cfg                       *config.Config
}

func TestPollerTestSuite(t *testing.T) {
	suite.Run(t, new(pollerTestSuite))
}

func (s *pollerTestSuite) SetupTest() {
	require := testutil.Require(s.T())

	var deps struct {
		fx.In
		MasterEndpoints    endpoints.EndpointProvider `name:"master"`
		SlaveEndpoints     endpoints.EndpointProvider `name:"slave"`
		ValidatorEndpoints endpoints.EndpointProvider `name:"validator"`
	}

	endpointGroup := &config.EndpointGroup{
		Endpoints: []config.Endpoint{
			{
				Name:   "foo",
				Weight: parallelism,
			},
		},
		EndpointsFailover: []config.Endpoint{
			{
				Name:   "bar",
				Weight: parallelism,
			},
		},
	}

	s.ctrl = gomock.NewController(s.T())

	// Override config to speed up the test.
	cfg, err := config.New()
	require.NoError(err)
	cfg.Workflows.Poller.CheckpointSize = pollerCheckpointSize
	cfg.Chain.Client.Master.EndpointGroup = *endpointGroup
	cfg.Chain.Client.Slave.EndpointGroup = *endpointGroup
	s.cfg = cfg

	s.env = cadence.NewTestEnv(s)
	s.env.SetWorkerOptions(worker.Options{
		EnableSessionWorker: true,
	})
	s.metaStorage = metastoragemocks.NewMockMetaStorage(s.ctrl)
	s.blobStorage = blobstoragemocks.NewMockBlobStorage(s.ctrl)
	s.masterBlockchainClient = clientmocks.NewMockClient(s.ctrl)
	s.slaveBlockchainClient = clientmocks.NewMockClient(s.ctrl)
	s.validatorBlockchainClient = clientmocks.NewMockClient(s.ctrl)
	s.app = testapp.New(
		s.T(),
		Module,
		cadence.WithTestEnv(s.env),
		testapp.WithConfig(cfg),
		fx.Provide(func() blobstorage.BlobStorage {
			return s.blobStorage
		}),
		fx.Provide(func() metastorage.MetaStorage {
			return s.metaStorage
		}),
		fx.Provide(fx.Annotated{
			Name: "master",
			Target: func() client.Client {
				return s.masterBlockchainClient
			},
		}),
		fx.Provide(fx.Annotated{
			Name: "slave",
			Target: func() client.Client {
				return s.slaveBlockchainClient
			},
		}),
		fx.Provide(fx.Annotated{
			Name: "validator",
			Target: func() client.Client {
				return s.validatorBlockchainClient
			},
		}),
		fx.Populate(&s.poller),
		fx.Populate(&deps),
	)

	s.masterEndpointProvider = deps.MasterEndpoints
	s.slaveEndpointProvider = deps.SlaveEndpoints
}

func (s *pollerTestSuite) TearDownTest() {
	s.app.Close()
	s.ctrl.Finish()
}

func (s *pollerTestSuite) TestPollerSuccess() {
	require := testutil.Require(s.T())
	seen := struct {
		blocks         sync.Map
		blockMetadatas sync.Map
	}{}
	heightGap := uint64(10)
	startHeight := uint64(100)
	endHeight := uint64(startHeight + pollerCheckpointSize*heightGap)
	for i := startHeight; i < endHeight; i += heightGap { // each poller cycle
		start := i + 1
		end := i + heightGap + 1
		s.masterBlockchainClient.EXPECT().
			GetLatestHeight(gomock.Any()).
			Return(i+heightGap, nil)
		forkBlock := testutil.MakeBlockMetadata(i, tag)
		s.metaStorage.EXPECT().
			GetLatestBlock(gomock.Any(), gomock.Any()).
			Return(forkBlock, nil)
		prevBlocks := testutil.MakeBlockMetadatasFromStartHeight(i, 1, tag)
		s.masterBlockchainClient.EXPECT().
			BatchGetBlockMetadata(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
			Return(prevBlocks, nil)
		s.metaStorage.EXPECT().
			GetBlocksByHeightRange(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
			Return(prevBlocks, nil)
		blocks := testutil.MakeBlockMetadatasFromStartHeight(start, int(heightGap), tag)
		s.masterBlockchainClient.EXPECT().
			BatchGetBlockMetadata(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
			Return(blocks, nil)
		for k := start; k < end; k++ {
			s.slaveBlockchainClient.EXPECT().
				GetBlockByHash(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
				Return(&api.Block{Metadata: &api.BlockMetadata{Height: k}}, nil)
			s.blobStorage.EXPECT().
				Upload(gomock.Any(), gomock.Any(), gomock.Any()).
				DoAndReturn(func(ctx context.Context, block *api.Block, compression api.Compression) (string, error) {
					require.Equal(api.Compression_GZIP, compression)
					seen.blocks.LoadOrStore(block.Metadata.Height, true)
					return "someObjectKey", nil
				})
		}
		s.metaStorage.EXPECT().
			PersistBlockMetas(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
			DoAndReturn(func(ctx context.Context, updateWatermark bool, blocks []*api.BlockMetadata, lastBlock *api.BlockMetadata) error {
				require.Equal(forkBlock, lastBlock)
				for _, block := range blocks {
					require.True(updateWatermark)
					require.Equal("someObjectKey", block.ObjectKeyMain)
					seen.blockMetadatas.LoadOrStore(block.Height, true)
				}
				return nil
			})
	}

	_, err := s.poller.Execute(context.Background(), &PollerRequest{
		Tag: tag,
	})
	require.Error(err)

	for i := startHeight + 1; i <= endHeight; i++ {
		v, ok := seen.blocks.Load(i)
		require.True(ok)
		require.True(v.(bool))
		v, ok = seen.blockMetadatas.Load(i)
		require.True(ok)
		require.True(v.(bool))
	}

	require.True(IsContinueAsNewError(err))
}

func (s *pollerTestSuite) TestPollerSuccess_FastSync() {
	require := testutil.Require(s.T())
	seen := struct {
		blocks         sync.Map
		blockMetadatas sync.Map
	}{}
	heightGap := uint64(10)
	startHeight := uint64(100)
	endHeight := startHeight + pollerCheckpointSize*heightGap
	for i := startHeight; i < endHeight; i += heightGap { // each poller cycle
		start := i + 1
		end := i + heightGap + 1
		s.masterBlockchainClient.EXPECT().
			GetLatestHeight(gomock.Any()).
			Return(i+heightGap, nil)
		forkBlock := testutil.MakeBlockMetadata(i, tag)
		s.metaStorage.EXPECT().
			GetLatestBlock(gomock.Any(), gomock.Any()).
			Return(forkBlock, nil)
		for k := start; k < end; k++ {
			s.slaveBlockchainClient.EXPECT().
				GetBlockByHeight(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
				Return(&api.Block{Metadata: &api.BlockMetadata{Height: k}}, nil)
			s.blobStorage.EXPECT().
				Upload(gomock.Any(), gomock.Any(), gomock.Any()).
				DoAndReturn(func(ctx context.Context, block *api.Block, compression api.Compression) (string, error) {
					require.Equal(api.Compression_GZIP, compression)
					seen.blocks.LoadOrStore(block.Metadata.Height, true)
					return "someObjectKey", nil
				})
		}
		s.metaStorage.EXPECT().
			PersistBlockMetas(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
			DoAndReturn(func(ctx context.Context, updateWatermark bool, blocks []*api.BlockMetadata, lastBlock *api.BlockMetadata) error {
				require.Equal(forkBlock, lastBlock)
				for _, block := range blocks {
					require.True(updateWatermark)
					require.Equal("someObjectKey", block.ObjectKeyMain)
					seen.blockMetadatas.LoadOrStore(block.Height, true)
				}
				return nil
			})
	}

	_, err := s.poller.Execute(context.Background(), &PollerRequest{
		Tag:      tag,
		FastSync: true,
	})
	require.Error(err)

	for i := startHeight + 1; i <= endHeight; i++ {
		v, ok := seen.blocks.Load(i)
		require.True(ok)
		require.True(v.(bool))
		v, ok = seen.blockMetadatas.Load(i)
		require.True(ok)
		require.True(v.(bool))
	}

	require.True(IsContinueAsNewError(err))
}

func (s *pollerTestSuite) TestPollerSuccess_FailoverEnabled() {
	require := testutil.Require(s.T())

	// Enable failover feature
	s.cfg.Workflows.Poller.FailoverEnabled = true
	seen := struct {
		blocks         sync.Map
		blockMetadatas sync.Map
	}{}
	heightGap := uint64(10)
	startHeight := uint64(100)
	endHeight := uint64(startHeight + pollerCheckpointSize*heightGap)
	for i := startHeight; i < endHeight; i += heightGap { // each poller cycle
		start := i + 1
		end := i + heightGap + 1
		s.masterBlockchainClient.EXPECT().
			GetLatestHeight(gomock.Any()).
			DoAndReturn(func(ctx context.Context) (uint64, error) {
				require.True(s.masterEndpointProvider.HasFailoverContext(ctx))
				require.True(s.slaveEndpointProvider.HasFailoverContext(ctx))
				return i + heightGap, nil
			})
		forkBlock := testutil.MakeBlockMetadata(i, tag)
		s.metaStorage.EXPECT().
			GetLatestBlock(gomock.Any(), gomock.Any()).
			Return(&api.BlockMetadata{Height: i}, nil)
		prevBlocks := testutil.MakeBlockMetadatasFromStartHeight(i, 1, tag)
		s.masterBlockchainClient.EXPECT().
			BatchGetBlockMetadata(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
			DoAndReturn(func(ctx context.Context, tag uint32, from uint64, to uint64) ([]*api.BlockMetadata, error) {
				require.True(s.masterEndpointProvider.HasFailoverContext(ctx))
				require.True(s.slaveEndpointProvider.HasFailoverContext(ctx))
				return prevBlocks, nil
			})
		s.metaStorage.EXPECT().
			GetBlocksByHeightRange(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
			Return(prevBlocks, nil)
		blocks := testutil.MakeBlockMetadatasFromStartHeight(start, int(heightGap), tag)
		s.masterBlockchainClient.EXPECT().
			BatchGetBlockMetadata(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
			DoAndReturn(func(ctx context.Context, tag uint32, from uint64, to uint64) ([]*api.BlockMetadata, error) {
				require.True(s.masterEndpointProvider.HasFailoverContext(ctx))
				require.True(s.slaveEndpointProvider.HasFailoverContext(ctx))
				return blocks, nil
			})
		for k := start; k < end; k++ {
			curHeight := k
			s.slaveBlockchainClient.EXPECT().
				GetBlockByHash(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
				DoAndReturn(func(ctx context.Context, tag uint32, height uint64, hash string, opts ...client.ClientOption) (*api.Block, error) {
					require.True(s.masterEndpointProvider.HasFailoverContext(ctx))
					require.True(s.slaveEndpointProvider.HasFailoverContext(ctx))
					return testutil.MakeBlocksFromStartHeight(curHeight, 1, tag)[0], nil
				})
			s.blobStorage.EXPECT().
				Upload(gomock.Any(), gomock.Any(), gomock.Any()).
				DoAndReturn(func(ctx context.Context, block *api.Block, compression api.Compression) (string, error) {
					require.Equal(api.Compression_GZIP, compression)
					seen.blocks.LoadOrStore(block.Metadata.Height, true)
					return "someObjectKey", nil
				})
		}
		s.metaStorage.EXPECT().
			PersistBlockMetas(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
			DoAndReturn(func(ctx context.Context, updateWatermark bool, blocks []*api.BlockMetadata, lastBlock *api.BlockMetadata) error {
				require.Equal(forkBlock, lastBlock)
				for _, block := range blocks {
					require.True(updateWatermark)
					require.Equal("someObjectKey", block.ObjectKeyMain)
					seen.blockMetadatas.LoadOrStore(block.Height, true)
				}
				return nil
			})
	}

	_, err := s.poller.Execute(context.Background(), &PollerRequest{
		Tag:      tag,
		Failover: true,
	})
	require.Error(err)

	for i := startHeight + 1; i <= endHeight; i++ {
		v, ok := seen.blocks.Load(i)
		require.True(ok)
		require.True(v.(bool))
		v, ok = seen.blockMetadatas.Load(i)
		require.True(ok)
		require.True(v.(bool))
	}

	require.True(IsContinueAsNewError(err))
}

func (s *pollerTestSuite) TestPollerSuccessAfterMasterFallback() {
	require := testutil.Require(s.T())
	seen := struct {
		blocks         sync.Map
		blockMetadatas sync.Map
	}{}
	heightGap := uint64(10)
	startHeight := uint64(100)
	endHeight := uint64(startHeight + pollerCheckpointSize*heightGap)
	for i := startHeight; i < endHeight; i += heightGap { // each poller cycle
		start := i + 1
		end := i + heightGap + 1
		s.masterBlockchainClient.EXPECT().
			GetLatestHeight(gomock.Any()).
			Return(i+heightGap, nil)
		forkBlock := testutil.MakeBlockMetadata(i, tag)
		s.metaStorage.EXPECT().
			GetLatestBlock(gomock.Any(), gomock.Any()).
			Return(forkBlock, nil)
		prevBlocks := testutil.MakeBlockMetadatasFromStartHeight(i, 1, tag)
		s.masterBlockchainClient.EXPECT().
			BatchGetBlockMetadata(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
			Return(prevBlocks, nil)
		s.metaStorage.EXPECT().
			GetBlocksByHeightRange(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
			Return(prevBlocks, nil)
		blocks := testutil.MakeBlockMetadatasFromStartHeight(start, int(heightGap), tag)
		s.masterBlockchainClient.EXPECT().
			BatchGetBlockMetadata(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
			Return(blocks, nil)
		for k := start; k < end; k++ {
			s.slaveBlockchainClient.EXPECT().
				GetBlockByHash(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
				Return(nil, client.ErrBlockNotFound)
			s.masterBlockchainClient.EXPECT().
				GetBlockByHash(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
				Return(&api.Block{Metadata: &api.BlockMetadata{Height: k}}, nil)
		}
		for k := start; k < end; k++ {
			s.blobStorage.EXPECT().
				Upload(gomock.Any(), gomock.Any(), gomock.Any()).
				DoAndReturn(func(ctx context.Context, block *api.Block, compression api.Compression) (string, error) {
					require.Equal(api.Compression_GZIP, compression)
					seen.blocks.LoadOrStore(block.Metadata.Height, true)
					return "someObjectKey", nil
				})
		}
		s.metaStorage.EXPECT().
			PersistBlockMetas(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
			DoAndReturn(func(ctx context.Context, updateWatermark bool, blocks []*api.BlockMetadata, lastBlock *api.BlockMetadata) error {
				require.Equal(forkBlock, lastBlock)
				for _, block := range blocks {
					require.True(updateWatermark)
					require.Equal("someObjectKey", block.ObjectKeyMain)
					seen.blockMetadatas.LoadOrStore(block.Height, true)
				}
				return nil
			})
	}

	_, err := s.poller.Execute(context.Background(), &PollerRequest{
		Tag: tag,
	})
	require.Error(err)

	for i := startHeight + 1; i <= endHeight; i++ {
		v, ok := seen.blocks.Load(i)
		require.True(ok)
		require.True(v.(bool))
		v, ok = seen.blockMetadatas.Load(i)
		require.True(ok)
		require.True(v.(bool))
	}

	require.True(IsContinueAsNewError(err))
}

func (s *pollerTestSuite) TestPollerSuccessAfterReprocessing() {
	require := testutil.Require(s.T())
	seen := struct {
		blocks         sync.Map
		blockMetadatas sync.Map
	}{}
	heightGap := uint64(10)
	startHeight := uint64(100)
	endHeight := uint64(startHeight + pollerCheckpointSize*heightGap)
	for i := startHeight; i < endHeight; i += heightGap { // each poller cycle
		start := i + 1
		end := i + heightGap + 1
		s.masterBlockchainClient.EXPECT().
			GetLatestHeight(gomock.Any()).
			Return(i+heightGap, nil)
		forkBlock := testutil.MakeBlockMetadata(i, tag)
		s.metaStorage.EXPECT().
			GetLatestBlock(gomock.Any(), gomock.Any()).
			Return(forkBlock, nil)
		prevBlocks := testutil.MakeBlockMetadatasFromStartHeight(i, 1, tag)
		s.masterBlockchainClient.EXPECT().
			BatchGetBlockMetadata(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
			Return(prevBlocks, nil)
		s.metaStorage.EXPECT().
			GetBlocksByHeightRange(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
			Return(prevBlocks, nil)
		blocks := testutil.MakeBlockMetadatasFromStartHeight(start, int(heightGap), tag)
		s.masterBlockchainClient.EXPECT().
			BatchGetBlockMetadata(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
			Return(blocks, nil)
		for k := start; k < end; k++ {
			s.slaveBlockchainClient.EXPECT().
				GetBlockByHash(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
				Return(nil, xerrors.Errorf("slave client failure"))
		}
		for k := start; k < end; k++ {
			s.masterBlockchainClient.EXPECT().
				GetBlockByHash(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
				Return(&api.Block{Metadata: &api.BlockMetadata{Height: k}}, nil)
			s.blobStorage.EXPECT().
				Upload(gomock.Any(), gomock.Any(), gomock.Any()).
				DoAndReturn(func(ctx context.Context, block *api.Block, compression api.Compression) (string, error) {
					require.Equal(api.Compression_GZIP, compression)
					seen.blocks.LoadOrStore(block.Metadata.Height, true)
					return "someObjectKey", nil
				})
		}
		s.metaStorage.EXPECT().
			PersistBlockMetas(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
			DoAndReturn(func(ctx context.Context, updateWatermark bool, blocks []*api.BlockMetadata, lastBlock *api.BlockMetadata) error {
				require.Equal(forkBlock, lastBlock)
				for _, block := range blocks {
					require.True(updateWatermark)
					require.Equal("someObjectKey", block.ObjectKeyMain)
					seen.blockMetadatas.LoadOrStore(block.Height, true)
				}
				return nil
			})
	}

	_, err := s.poller.Execute(context.Background(), &PollerRequest{
		Tag: tag,
	})
	require.Error(err)

	for i := startHeight + 1; i <= endHeight; i++ {
		v, ok := seen.blocks.Load(i)
		require.True(ok)
		require.True(v.(bool))
		v, ok = seen.blockMetadatas.Load(i)
		require.True(ok)
		require.True(v.(bool))
	}

	require.True(IsContinueAsNewError(err))
}

func (s *pollerTestSuite) TestPollerFailure() {
	require := testutil.Require(s.T())
	// Disable poller failover feature
	s.cfg.Workflows.Poller.FailoverEnabled = false

	localHeight := uint64(100)
	heightGap := uint64(10)
	s.masterBlockchainClient.EXPECT().
		GetLatestHeight(gomock.Any()).
		AnyTimes().
		Return(localHeight+heightGap, nil)
	s.metaStorage.EXPECT().
		GetLatestBlock(gomock.Any(), gomock.Any()).
		AnyTimes().
		Return(&api.BlockMetadata{Height: localHeight}, nil)
	blocks := testutil.MakeBlockMetadatasFromStartHeight(localHeight-heightGap+1, int(heightGap), tag)
	s.metaStorage.EXPECT().
		GetBlocksByHeightRange(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		AnyTimes().
		Return(blocks, nil)
	s.masterBlockchainClient.EXPECT().
		BatchGetBlockMetadata(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		AnyTimes().
		Return(nil, xerrors.Errorf("master client failure"))
	_, err := s.poller.Execute(context.Background(), &PollerRequest{
		Tag: tag,
	})
	require.Error(err)
	require.Contains(err.Error(), "failed to execute workflow")
	require.False(IsContinueAsNewError(err))
}

func (s *pollerTestSuite) TestPollerFailure_AutomateFailover() {
	require := testutil.Require(s.T())

	// Enable failover feature
	s.cfg.Workflows.Poller.FailoverEnabled = true
	localHeight := uint64(100)
	heightGap := uint64(10)

	s.masterBlockchainClient.EXPECT().
		GetLatestHeight(gomock.Any()).
		AnyTimes().
		Return(localHeight+heightGap, nil)
	s.metaStorage.EXPECT().
		GetLatestBlock(gomock.Any(), gomock.Any()).
		AnyTimes().
		Return(&api.BlockMetadata{Height: localHeight}, nil)
	blocks := testutil.MakeBlockMetadatasFromStartHeight(localHeight-heightGap+1, int(heightGap), tag)
	s.metaStorage.EXPECT().
		GetBlocksByHeightRange(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		AnyTimes().
		Return(blocks, nil)
	s.masterBlockchainClient.EXPECT().
		BatchGetBlockMetadata(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		AnyTimes().
		Return(nil, xerrors.Errorf("master client failure"))
	_, err := s.poller.Execute(context.Background(), &PollerRequest{
		Tag: tag,
	})
	require.Error(err)
	require.True(IsContinueAsNewError(err))
}

func (s *pollerTestSuite) TestPollerFailureTooManySessionErr() {
	require := testutil.Require(s.T())

	s.env.OnActivity(activity.ActivitySyncer, mock.Anything, mock.Anything).Return(&activity.SyncerResponse{}, workflow.ErrSessionFailed)

	_, err := s.poller.Execute(context.Background(), &PollerRequest{
		Tag:                 tag,
		RetryableErrorCount: RetryableErrorLimit,
	})
	require.Error(err)
	require.False(IsContinueAsNewError(err))
}

func (s *pollerTestSuite) TestPollerFailureWithSessionErr() {
	require := testutil.Require(s.T())

	s.env.OnActivity(activity.ActivitySyncer, mock.Anything, mock.Anything).Return(&activity.SyncerResponse{}, workflow.ErrSessionFailed)

	_, err := s.poller.Execute(context.Background(), &PollerRequest{
		Tag:                 tag,
		RetryableErrorCount: RetryableErrorLimit - 1,
	})
	require.Error(err)
	require.True(IsContinueAsNewError(err))
}

func (s *pollerTestSuite) TestPollerFailureWithScheduleToStartTimeout() {
	require := testutil.Require(s.T())

	s.env.OnActivity(activity.ActivitySyncer, mock.Anything, mock.Anything).Return(&activity.SyncerResponse{}, temporal.NewTimeoutError(enums.TIMEOUT_TYPE_SCHEDULE_TO_START, nil))

	_, err := s.poller.Execute(context.Background(), &PollerRequest{
		Tag:                 tag,
		RetryableErrorCount: RetryableErrorLimit - 1,
	})
	require.Error(err)
	require.True(IsContinueAsNewError(err))
}
