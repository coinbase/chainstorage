package workflow

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/sdk/testsuite"
	"go.uber.org/fx"
	"go.uber.org/mock/gomock"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/blockchain/client"
	clientmocks "github.com/coinbase/chainstorage/internal/blockchain/client/mocks"
	"github.com/coinbase/chainstorage/internal/blockchain/endpoints"
	"github.com/coinbase/chainstorage/internal/blockchain/jsonrpc"
	"github.com/coinbase/chainstorage/internal/cadence"
	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/dlq"
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
	tag                     = uint32(1)
	startHeight             = uint64(12000000)
	endHeight               = uint64(12000113)
	numConcurrentExtractors = 4
	batchSize               = 12
	checkpointSize          = 240
)

type backfillerTestSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite
	env                    *cadence.TestEnv
	ctrl                   *gomock.Controller
	blockchainClient       *clientmocks.MockClient
	metadataAccessor       *metastoragemocks.MockMetaStorage
	blobStorage            *blobstoragemocks.MockBlobStorage
	masterEndpointProvider endpoints.EndpointProvider
	slaveEndpointProvider  endpoints.EndpointProvider
	backfiller             *Backfiller
	app                    testapp.TestApp
	cfg                    *config.Config
}

func TestBackfillerTestSuite(t *testing.T) {
	suite.Run(t, new(backfillerTestSuite))
}

func (s *backfillerTestSuite) SetupTest() {
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

	// Override config to speed up the test.
	cfg, err := config.New()
	require.NoError(err)
	cfg.Workflows.Backfiller.BatchSize = batchSize
	cfg.Workflows.Backfiller.CheckpointSize = checkpointSize
	cfg.Chain.Client.Master.EndpointGroup = *endpointGroup
	cfg.Chain.Client.Slave.EndpointGroup = *endpointGroup
	s.cfg = cfg

	s.env = cadence.NewTestEnv(s)
	s.ctrl = gomock.NewController(s.T())
	s.blockchainClient = clientmocks.NewMockClient(s.ctrl)
	s.metadataAccessor = metastoragemocks.NewMockMetaStorage(s.ctrl)
	s.blobStorage = blobstoragemocks.NewMockBlobStorage(s.ctrl)
	s.app = testapp.New(
		s.T(),
		Module,
		testapp.WithConfig(cfg),
		cadence.WithTestEnv(s.env),
		fx.Provide(func() blobstorage.BlobStorage {
			return s.blobStorage
		}),
		fx.Provide(fx.Annotated{
			Name: "slave",
			Target: func() client.Client {
				return s.blockchainClient
			},
		}),
		fx.Provide(func() metastorage.MetaStorage {
			return s.metadataAccessor
		}),
		fx.Provide(dlq.NewNop),
		fx.Populate(&s.backfiller),
		fx.Populate(&deps),
	)
	s.masterEndpointProvider = deps.MasterEndpoints
	s.slaveEndpointProvider = deps.SlaveEndpoints

	require.Greater(endHeight-startHeight, s.app.Config().Workflows.Backfiller.BatchSize)
}

func (s *backfillerTestSuite) TearDownTest() {
	s.app.Close()
	s.ctrl.Finish()
	s.env.AssertExpectations(s.T())
}

func (s *backfillerTestSuite) TestBackfiller() {
	require := testutil.Require(s.T())

	seen := struct {
		blockchainClient sync.Map
		blobStorage      sync.Map
		metadataAccessor sync.Map
	}{}
	s.metadataAccessor.EXPECT().GetBlockByHeight(gomock.Any(), gomock.Any(), gomock.Any()).
		Times(1).
		DoAndReturn(func(ctx context.Context, tag_ uint32, height uint64) (*api.BlockMetadata, error) {
			require.Equal(tag, tag_)
			return s.getMockMetadata(height), nil
		})
	s.blockchainClient.EXPECT().GetBlockByHeight(gomock.Any(), gomock.Any(), gomock.Any()).
		Times(int(endHeight - startHeight)).
		DoAndReturn(func(ctx context.Context, tag_ uint32, height uint64) (*api.Block, error) {
			require.False(s.masterEndpointProvider.HasFailoverContext(ctx))
			require.False(s.slaveEndpointProvider.HasFailoverContext(ctx))
			require.Equal(tag, tag_)
			_, ok := seen.blockchainClient.LoadOrStore(height, true)
			require.False(ok)
			return &api.Block{
				Metadata: s.getMockMetadata(height),
			}, nil
		})

	s.blobStorage.EXPECT().Upload(gomock.Any(), gomock.Any(), gomock.Any()).
		Times(int(endHeight - startHeight)).
		DoAndReturn(func(ctx context.Context, block *api.Block, compression api.Compression) (string, error) {
			require.Equal(api.Compression_GZIP, compression)
			_, ok := seen.blobStorage.LoadOrStore(block.Metadata.Height, true)
			require.False(ok)
			return strconv.Itoa(int(block.Metadata.Height)), nil
		})

	s.metadataAccessor.EXPECT().PersistBlockMetas(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, updateWatermark bool, blocks []*api.BlockMetadata, lastBlock *api.BlockMetadata) error {
			require.Equal(lastBlock.Hash, blocks[0].ParentHash)
			for _, block := range blocks {
				require.False(updateWatermark)
				_, ok := seen.metadataAccessor.LoadOrStore(block.Height, true)
				require.False(ok)
			}
			return nil
		}).AnyTimes()

	_, err := s.backfiller.Execute(context.Background(), &BackfillerRequest{
		Tag:                     tag,
		StartHeight:             startHeight,
		EndHeight:               endHeight,
		NumConcurrentExtractors: numConcurrentExtractors,
	})
	require.NoError(err)

	for i := startHeight; i < endHeight; i++ {
		v, ok := seen.blockchainClient.Load(i)
		require.True(ok)
		require.True(v.(bool))
		v, ok = seen.blobStorage.Load(i)
		require.True(ok)
		require.True(v.(bool))
		v, ok = seen.metadataAccessor.Load(i)
		require.True(ok)
		require.True(v.(bool))
	}
}

func (s *backfillerTestSuite) TestBackfiller_MiniBatch() {
	require := testutil.Require(s.T())

	seen := struct {
		blockchainClient sync.Map
		blobStorage      sync.Map
		metadataAccessor sync.Map
	}{}
	s.metadataAccessor.EXPECT().GetBlockByHeight(gomock.Any(), gomock.Any(), gomock.Any()).
		Times(1).
		DoAndReturn(func(ctx context.Context, tag_ uint32, height uint64) (*api.BlockMetadata, error) {
			require.Equal(tag, tag_)
			return s.getMockMetadata(height), nil
		})
	s.blockchainClient.EXPECT().GetBlockByHeight(gomock.Any(), gomock.Any(), gomock.Any()).
		Times(int(endHeight - startHeight)).
		DoAndReturn(func(ctx context.Context, tag_ uint32, height uint64) (*api.Block, error) {
			require.False(s.masterEndpointProvider.HasFailoverContext(ctx))
			require.False(s.slaveEndpointProvider.HasFailoverContext(ctx))
			require.Equal(tag, tag_)
			_, ok := seen.blockchainClient.LoadOrStore(height, true)
			require.False(ok)
			return &api.Block{
				Metadata: s.getMockMetadata(height),
			}, nil
		})

	s.blobStorage.EXPECT().Upload(gomock.Any(), gomock.Any(), gomock.Any()).
		Times(int(endHeight - startHeight)).
		DoAndReturn(func(ctx context.Context, block *api.Block, compression api.Compression) (string, error) {
			require.Equal(api.Compression_GZIP, compression)
			_, ok := seen.blobStorage.LoadOrStore(block.Metadata.Height, true)
			require.False(ok)
			return strconv.Itoa(int(block.Metadata.Height)), nil
		})

	s.metadataAccessor.EXPECT().PersistBlockMetas(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, updateWatermark bool, blocks []*api.BlockMetadata, lastBlock *api.BlockMetadata) error {
			require.Equal(lastBlock.Hash, blocks[0].ParentHash)
			for _, block := range blocks {
				require.False(updateWatermark)
				_, ok := seen.metadataAccessor.LoadOrStore(block.Height, true)
				require.False(ok)
			}
			return nil
		}).AnyTimes()

	_, err := s.backfiller.Execute(context.Background(), &BackfillerRequest{
		Tag:                     tag,
		StartHeight:             startHeight,
		EndHeight:               endHeight,
		NumConcurrentExtractors: numConcurrentExtractors,
		MiniBatchSize:           3,
	})
	require.NoError(err)

	for i := startHeight; i < endHeight; i++ {
		v, ok := seen.blockchainClient.Load(i)
		require.True(ok)
		require.True(v.(bool))
		v, ok = seen.blobStorage.Load(i)
		require.True(ok)
		require.True(v.(bool))
		v, ok = seen.metadataAccessor.Load(i)
		require.True(ok)
		require.True(v.(bool))
	}
}

func (s *backfillerTestSuite) TestBackfiller_Failover() {
	require := testutil.Require(s.T())
	s.cfg.Workflows.Backfiller.FailoverEnabled = true

	seen := struct {
		blockchainClient sync.Map
		blobStorage      sync.Map
		metadataAccessor sync.Map
	}{}
	s.metadataAccessor.EXPECT().GetBlockByHeight(gomock.Any(), gomock.Any(), gomock.Any()).
		Times(1).
		DoAndReturn(func(ctx context.Context, tag_ uint32, height uint64) (*api.BlockMetadata, error) {
			require.Equal(tag, tag_)
			return s.getMockMetadata(height), nil
		})
	s.blockchainClient.EXPECT().GetBlockByHeight(gomock.Any(), gomock.Any(), gomock.Any()).
		Times(int(endHeight - startHeight)).
		DoAndReturn(func(ctx context.Context, tag_ uint32, height uint64, opts ...jsonrpc.Option) (*api.Block, error) {
			require.True(s.masterEndpointProvider.HasFailoverContext(ctx))
			require.True(s.slaveEndpointProvider.HasFailoverContext(ctx))
			require.Equal(tag, tag_)
			_, ok := seen.blockchainClient.LoadOrStore(height, true)
			require.False(ok)
			return &api.Block{
				Metadata: s.getMockMetadata(height),
			}, nil
		})

	s.blobStorage.EXPECT().Upload(gomock.Any(), gomock.Any(), gomock.Any()).
		Times(int(endHeight - startHeight)).
		DoAndReturn(func(ctx context.Context, block *api.Block, compression api.Compression) (string, error) {
			require.Equal(api.Compression_GZIP, compression)
			_, ok := seen.blobStorage.LoadOrStore(block.Metadata.Height, true)
			require.False(ok)
			return strconv.Itoa(int(block.Metadata.Height)), nil
		})

	s.metadataAccessor.EXPECT().PersistBlockMetas(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, updateWatermark bool, blocks []*api.BlockMetadata, lastBlock *api.BlockMetadata) error {
			require.Equal(lastBlock.Hash, blocks[0].ParentHash)
			for _, block := range blocks {
				require.False(updateWatermark)
				_, ok := seen.metadataAccessor.LoadOrStore(block.Height, true)
				require.False(ok)
			}
			return nil
		}).AnyTimes()

	_, err := s.backfiller.Execute(context.Background(), &BackfillerRequest{
		Tag:                     tag,
		StartHeight:             startHeight,
		EndHeight:               endHeight,
		NumConcurrentExtractors: numConcurrentExtractors,
		Failover:                true,
	})
	require.NoError(err)

	for i := startHeight; i < endHeight; i++ {
		v, ok := seen.blockchainClient.Load(i)
		require.True(ok)
		require.True(v.(bool))
		v, ok = seen.blobStorage.Load(i)
		require.True(ok)
		require.True(v.(bool))
		v, ok = seen.metadataAccessor.Load(i)
		require.True(ok)
		require.True(v.(bool))
	}
}

func (s *backfillerTestSuite) TestBackfiller_Checkpoint() {
	require := testutil.Require(s.T())

	s.env.OnActivity(activity.ActivityReader, mock.Anything, mock.Anything).
		Return(&activity.ReaderResponse{}, nil)

	s.env.OnActivity(activity.ActivityExtractor, mock.Anything, mock.Anything).
		Return(func(ctx context.Context, request *activity.ExtractorRequest) (*activity.ExtractorResponse, error) {
			response := &activity.ExtractorResponse{
				Metadatas: []*api.BlockMetadata{
					s.getMockMetadata(request.Heights[0]),
				},
			}
			return response, nil

		})

	s.env.OnActivity(activity.ActivityLoader, mock.Anything, mock.Anything).
		Return(&activity.LoaderResponse{}, nil)

	_, err := s.backfiller.Execute(context.Background(), &BackfillerRequest{
		Tag:                     tag,
		StartHeight:             startHeight,
		EndHeight:               startHeight + checkpointSize + 1,
		NumConcurrentExtractors: numConcurrentExtractors,
	})
	require.Error(err)
	require.True(IsContinueAsNewError(err))
}

func (s *backfillerTestSuite) TestBackfiller_NonContinuousChainWithinBatch() {
	require := testutil.Require(s.T())

	const invalidHeight = 12000067
	require.True(startHeight < invalidHeight && invalidHeight < endHeight)

	s.env.OnActivity(activity.ActivityReader, mock.Anything, mock.Anything).
		Return(&activity.ReaderResponse{}, nil)

	s.env.OnActivity(activity.ActivityExtractor, mock.Anything, mock.Anything).
		Return(func(ctx context.Context, request *activity.ExtractorRequest) (*activity.ExtractorResponse, error) {
			response := &activity.ExtractorResponse{
				Metadatas: []*api.BlockMetadata{
					s.getMockMetadata(request.Heights[0]),
				},
			}
			if request.Heights[0] == invalidHeight {
				response.Metadatas[0].ParentHash = "unexpected_hash"
			}
			return response, nil

		})

	s.env.OnActivity(activity.ActivityLoader, mock.Anything, mock.Anything).
		Return(&activity.LoaderResponse{}, nil)

	_, err := s.backfiller.Execute(context.Background(), &BackfillerRequest{
		Tag:                     tag,
		StartHeight:             startHeight,
		EndHeight:               endHeight,
		NumConcurrentExtractors: numConcurrentExtractors,
	})
	require.Error(err)
	require.Contains(err.Error(), "chain is not continuous")
}

func (s *backfillerTestSuite) TestBackfiller_NonContinuousChainAcrossBatch() {
	require := testutil.Require(s.T())

	s.env.OnActivity(activity.ActivityReader, mock.Anything, mock.Anything).
		Return(&activity.ReaderResponse{}, nil)

	s.env.OnActivity(activity.ActivityExtractor, mock.Anything, mock.Anything).
		Return(func(ctx context.Context, request *activity.ExtractorRequest) (*activity.ExtractorResponse, error) {
			response := &activity.ExtractorResponse{
				Metadatas: []*api.BlockMetadata{
					s.getMockMetadata(request.Heights[0]),
				},
			}
			if request.Heights[0] == startHeight+2*s.app.Config().Workflows.Backfiller.BatchSize {
				response.Metadatas[0].ParentHash = "unexpected_hash"
			}
			return response, nil

		})

	s.env.OnActivity(activity.ActivityLoader, mock.Anything, mock.Anything).
		Return(&activity.LoaderResponse{}, nil)

	_, err := s.backfiller.Execute(context.Background(), &BackfillerRequest{
		Tag:                     tag,
		StartHeight:             startHeight,
		EndHeight:               endHeight,
		NumConcurrentExtractors: numConcurrentExtractors,
	})
	require.Error(err)
	require.Contains(err.Error(), "chain is not continuous")
}

func (s *backfillerTestSuite) TestBackfiller_NonContinuousChainAcrossCheckpoint() {
	require := testutil.Require(s.T())

	s.env.OnActivity(activity.ActivityReader, mock.Anything, mock.Anything).
		Return(&activity.ReaderResponse{
			Metadata: &api.BlockMetadata{
				Tag:    tag,
				Height: startHeight - 1,
				Hash:   "unexpected_hash",
			},
		}, nil)

	s.env.OnActivity(activity.ActivityExtractor, mock.Anything, mock.Anything).
		Return(func(ctx context.Context, request *activity.ExtractorRequest) (*activity.ExtractorResponse, error) {
			response := &activity.ExtractorResponse{
				Metadatas: []*api.BlockMetadata{
					s.getMockMetadata(request.Heights[0]),
				},
			}
			return response, nil

		})

	_, err := s.backfiller.Execute(context.Background(), &BackfillerRequest{
		Tag:                     tag,
		StartHeight:             startHeight,
		EndHeight:               endHeight,
		NumConcurrentExtractors: numConcurrentExtractors,
	})
	require.Error(err)
	require.Contains(err.Error(), "chain is not continuous")
}

func (s *backfillerTestSuite) TestBackfiller_Reprocess() {
	require := testutil.Require(s.T())

	const blockNumber = 12000067
	maximumAttempts := s.app.Config().Workflows.Backfiller.ActivityRetryMaximumAttempts

	s.env.OnActivity(activity.ActivityReader, mock.Anything, mock.Anything).Return(&activity.ReaderResponse{}, nil)

	var numCallsForReprocessedBlock int32
	s.env.OnActivity(activity.ActivityExtractor, mock.Anything, mock.Anything).
		Return(func(ctx context.Context, request *activity.ExtractorRequest) (*activity.ExtractorResponse, error) {
			response := &activity.ExtractorResponse{
				Metadatas: []*api.BlockMetadata{
					s.getMockMetadata(request.Heights[0]),
				},
			}

			if request.Heights[0] == blockNumber {
				numCallsForReprocessedBlock += 1
				if numCallsForReprocessedBlock <= maximumAttempts+1 {
					return nil, xerrors.New("transient error")
				}

				require.True(request.WithBestEffort)
			}

			return response, nil
		})

	s.env.OnActivity(activity.ActivityLoader, mock.Anything, mock.Anything).Return(&activity.LoaderResponse{}, nil)

	_, err := s.backfiller.Execute(context.Background(), &BackfillerRequest{
		Tag:                     tag,
		StartHeight:             startHeight,
		EndHeight:               endHeight,
		NumConcurrentExtractors: numConcurrentExtractors,
	})
	require.NoError(err)
	require.Equal(maximumAttempts+2, numCallsForReprocessedBlock)
}

func (s *backfillerTestSuite) TestBackfiller_UpdateWatermarkIfSet() {
	require := testutil.Require(s.T())

	s.env.OnActivity(activity.ActivityReader, mock.Anything, mock.Anything).Return(&activity.ReaderResponse{}, nil)

	s.env.OnActivity(activity.ActivityExtractor, mock.Anything, mock.Anything).
		Return(func(ctx context.Context, request *activity.ExtractorRequest) (*activity.ExtractorResponse, error) {
			response := &activity.ExtractorResponse{
				Metadatas: []*api.BlockMetadata{
					s.getMockMetadata(request.Heights[0]),
				},
			}
			return response, nil

		})

	s.env.OnActivity(activity.ActivityLoader, mock.Anything, mock.Anything).
		Return(func(ctx context.Context, request *activity.LoaderRequest) (*activity.LoaderResponse, error) {
			require.True(request.UpdateWatermark)
			return &activity.LoaderResponse{}, nil
		})

	_, err := s.backfiller.Execute(context.Background(), &BackfillerRequest{
		StartHeight:             startHeight,
		EndHeight:               endHeight,
		NumConcurrentExtractors: numConcurrentExtractors,
		UpdateWatermark:         true,
	})
	require.NoError(err)
}

func (s *backfillerTestSuite) TestBackfiller_ValidateRequest() {
	require := testutil.Require(s.T())

	// requires start <= end
	_, err := s.backfiller.Execute(context.Background(), &BackfillerRequest{
		Tag:                     tag,
		StartHeight:             endHeight,
		EndHeight:               startHeight,
		NumConcurrentExtractors: numConcurrentExtractors,
	})
	require.Error(err)
	require.Contains(err.Error(), "invalid workflow request")
	require.Contains(err.Error(), "Field validation for 'EndHeight' failed on the 'gtfield' tag")

	// start == end is also wrong
	_, err = s.backfiller.Execute(context.Background(), &BackfillerRequest{
		Tag:                     tag,
		StartHeight:             startHeight,
		EndHeight:               startHeight,
		NumConcurrentExtractors: numConcurrentExtractors,
	})
	require.Error(err)
	require.Contains(err.Error(), "invalid workflow request")
	require.Contains(err.Error(), "Field validation for 'EndHeight' failed on the 'gtfield' tag")
}

func (s *backfillerTestSuite) getMockMetadata(height uint64) *api.BlockMetadata {
	return &api.BlockMetadata{
		Tag:          tag,
		Height:       height,
		ParentHeight: height - 1,
		Hash:         fmt.Sprintf("fake_hash_%v", height),
		ParentHash:   fmt.Sprintf("fake_hash_%v", height-1),
	}
}
