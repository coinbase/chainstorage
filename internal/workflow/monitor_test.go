package workflow

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/suite"
	"go.temporal.io/sdk/testsuite"
	"go.uber.org/fx"
	"go.uber.org/mock/gomock"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/blockchain/client"
	clientmocks "github.com/coinbase/chainstorage/internal/blockchain/client/mocks"
	"github.com/coinbase/chainstorage/internal/blockchain/endpoints"
	"github.com/coinbase/chainstorage/internal/blockchain/parser"
	"github.com/coinbase/chainstorage/internal/cadence"
	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/storage"
	"github.com/coinbase/chainstorage/internal/storage/blobstorage"
	blobstoragemocks "github.com/coinbase/chainstorage/internal/storage/blobstorage/mocks"
	"github.com/coinbase/chainstorage/internal/storage/metastorage"
	metastoragemocks "github.com/coinbase/chainstorage/internal/storage/metastorage/mocks"
	"github.com/coinbase/chainstorage/internal/storage/metastorage/model"
	"github.com/coinbase/chainstorage/internal/utils/pointer"
	"github.com/coinbase/chainstorage/internal/utils/testapp"
	"github.com/coinbase/chainstorage/internal/utils/testutil"
	"github.com/coinbase/chainstorage/protos/coinbase/c3/common"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

const (
	monitorStartHeight               = uint64(0)
	monitorTag                       = uint32(1)
	monitorMaxHeightNativeClient     = uint64(1300)
	monitorMaxHeightIngested         = uint64(330)
	monitorValidatorMaxReorgDistance = uint64(500)
	monitorValidationHeightPadding   = monitorValidatorMaxReorgDistance * validationHeightPaddingMultiplier
	monitorBatchSize                 = 290
	monitorCheckpointSize            = 2
	monitorMaxEventId                = int64(10000)
	monitorEventTag                  = uint32(0)
	totalBlocksToBeValidated         = int(monitorMaxHeightNativeClient - monitorValidationHeightPadding + 1)
)

type monitorTestSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite
	env                    *cadence.TestEnv
	masterClient           *clientmocks.MockClient
	slaveClient            *clientmocks.MockClient
	validatorClient        *clientmocks.MockClient
	masterEndpointProvider endpoints.EndpointProvider
	slaveEndpointProvider  endpoints.EndpointProvider
	metadataAccessor       *metastoragemocks.MockMetaStorage
	blobStorage            *blobstoragemocks.MockBlobStorage
	parser                 parser.Parser
	monitor                *Monitor
	app                    testapp.TestApp
	seen                   *seenMap
	ctrl                   *gomock.Controller
	cfg                    *config.Config
}

type seenMap struct {
	blockchainClient                 sync.Map
	blobStorage                      sync.Map
	metastorageGetBlockByHeight      sync.Map
	metastorageGetEventsAfterEventId int
}

func TestMonitorTestSuite(t *testing.T) {
	suite.Run(t, new(monitorTestSuite))
}

func (s *monitorTestSuite) SetupTest() {
	s.ctrl = gomock.NewController(s.T())
	s.masterClient = clientmocks.NewMockClient(s.ctrl)
	s.slaveClient = clientmocks.NewMockClient(s.ctrl)
	s.validatorClient = clientmocks.NewMockClient(s.ctrl)
	s.metadataAccessor = metastoragemocks.NewMockMetaStorage(s.ctrl)
	s.blobStorage = blobstoragemocks.NewMockBlobStorage(s.ctrl)
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

	s.seen = &seenMap{
		blockchainClient:            sync.Map{},
		blobStorage:                 sync.Map{},
		metastorageGetBlockByHeight: sync.Map{},
	}

	// Override config to speed up the test.
	cfg, err := config.New()
	require.NoError(err)
	cfg.Workflows.Monitor.BatchSize = monitorBatchSize
	cfg.Workflows.Monitor.CheckpointSize = monitorCheckpointSize
	cfg.Workflows.Monitor.IrreversibleDistance = monitorValidatorMaxReorgDistance
	cfg.Chain.Client.Master.EndpointGroup = *endpointGroup
	cfg.Chain.Client.Slave.EndpointGroup = *endpointGroup
	s.cfg = cfg

	s.metadataAccessor.EXPECT().GetLatestBlock(gomock.Any(), gomock.Any()).
		AnyTimes().
		DoAndReturn(func(ctx context.Context, tag uint32) (*api.BlockMetadata, error) {
			return testutil.MakeBlockMetadata(monitorMaxHeightIngested, tag), nil
		})
	s.metadataAccessor.EXPECT().GetBlocksByHeightRange(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		AnyTimes().
		DoAndReturn(func(ctx context.Context, tag uint32, from uint64, to uint64) ([]*api.BlockMetadata, error) {
			require.Equal(monitorTag, tag)
			return testutil.MakeBlockMetadatasFromStartHeight(from, int(to-from), tag), nil
		})
	s.metadataAccessor.EXPECT().GetBlockByHeight(gomock.Any(), gomock.Any(), gomock.Any()).
		AnyTimes().
		DoAndReturn(func(ctx context.Context, tag uint32, height uint64) (*api.BlockMetadata, error) {
			require.Equal(monitorTag, tag)
			count := getValue(&s.seen.metastorageGetBlockByHeight, height)
			s.seen.metastorageGetBlockByHeight.Store(height, pointer.Int(count+1))
			return testutil.MakeBlockMetadata(height, tag), nil
		})
	s.slaveClient.EXPECT().BatchGetBlockMetadata(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		AnyTimes().
		DoAndReturn(func(ctx context.Context, tag uint32, from uint64, to uint64) ([]*api.BlockMetadata, error) {
			require.Equal(monitorTag, tag)
			numBlocks := int(to - from)
			blocks := make([]*api.BlockMetadata, numBlocks)
			for i := 0; i < numBlocks; i++ {
				height := from + uint64(i)
				count := getValue(&s.seen.blockchainClient, height)
				s.seen.blockchainClient.Store(height, pointer.Int(count+1))
				blocks[i] = testutil.MakeBlockMetadata(height, tag)
			}
			return blocks, nil
		})
	s.blobStorage.EXPECT().Download(gomock.Any(), gomock.Any()).
		AnyTimes().
		DoAndReturn(func(ctx context.Context, metadata *api.BlockMetadata) (*api.Block, error) {
			height := metadata.Height
			count := getValue(&s.seen.blobStorage, height)
			s.seen.blobStorage.Store(height, pointer.Int(count+1))
			return s.getMockBlock(height, monitorTag), nil
		})

	s.env = cadence.NewTestEnv(s)
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
		fx.Provide(func() metastorage.MetaStorage {
			return s.metadataAccessor
		}),
		fx.Populate(&s.monitor),
		fx.Populate(&s.parser),
		fx.Populate(&deps),
	)

	s.masterEndpointProvider = deps.MasterEndpoints
	s.slaveEndpointProvider = deps.SlaveEndpoints
}

func (s *monitorTestSuite) TearDownTest() {
	s.app.Close()
	s.ctrl.Finish()
}

func (s *monitorTestSuite) TestMonitor() {
	require := testutil.Require(s.T())

	s.masterClient.EXPECT().GetLatestHeight(gomock.Any()).
		AnyTimes().
		DoAndReturn(func(ctx context.Context) (uint64, error) {
			return monitorMaxHeightNativeClient, nil
		})
	s.metadataAccessor.EXPECT().GetMaxEventId(gomock.Any(), monitorEventTag).
		AnyTimes().
		Return(monitorMaxEventId, nil)
	s.metadataAccessor.EXPECT().GetEventsAfterEventId(gomock.Any(), monitorEventTag, gomock.Any(), gomock.Any()).
		AnyTimes().
		DoAndReturn(func(ctx context.Context, eventTag uint32, eventId int64, maxEvents uint64) ([]*model.EventEntry, error) {
			s.seen.metastorageGetEventsAfterEventId++
			return testutil.MakeBlockEventEntries(
				api.BlockchainEvent_BLOCK_ADDED,
				monitorEventTag, eventId+int64(maxEvents-1), uint64(eventId), uint64(eventId)+maxEvents, monitorTag,
			), nil
		})

	s.env.ExecuteWorkflow(s.monitor.execute, &MonitorRequest{
		StartHeight: monitorStartHeight,
		Tag:         monitorTag,
	})
	s.True(s.env.IsWorkflowCompleted())
	err := s.env.GetWorkflowError()
	require.Error(err)
	require.True(IsContinueAsNewError(err))

	for i := monitorStartHeight; i < uint64(totalBlocksToBeValidated)+monitorStartHeight; i++ {
		blockchainClientCount := getValue(&s.seen.blockchainClient, i)
		blobstorageCount := getValue(&s.seen.blobStorage, i)
		metastorageGetBlockByHeightCount := getValue(&s.seen.metastorageGetBlockByHeight, i)
		if i == monitorBatchSize-1 {
			require.Equal(blockchainClientCount, 2)
			require.Equal(blobstorageCount, 2)
			require.Equal(metastorageGetBlockByHeightCount, 2)
		} else {
			require.Equal(blockchainClientCount, 1)
			require.Equal(blobstorageCount, 1)
			if i == monitorBatchSize-2 {
				require.Equal(metastorageGetBlockByHeightCount, 2)
			} else {
				require.Equal(metastorageGetBlockByHeightCount, 1)
			}
		}
	}
	require.Equal(s.seen.metastorageGetEventsAfterEventId, monitorCheckpointSize)
}

func (s *monitorTestSuite) TestMonitorNoEventHistory() {
	require := testutil.Require(s.T())

	s.masterClient.EXPECT().GetLatestHeight(gomock.Any()).
		AnyTimes().
		DoAndReturn(func(ctx context.Context) (uint64, error) {
			return monitorMaxHeightNativeClient, nil
		})
	s.metadataAccessor.EXPECT().GetMaxEventId(gomock.Any(), monitorEventTag).
		AnyTimes().
		Return(int64(0), storage.ErrNoEventHistory)
	s.env.ExecuteWorkflow(s.monitor.execute, &MonitorRequest{
		StartHeight: monitorStartHeight,
		Tag:         monitorTag,
		EventTag:    monitorEventTag,
	})
	s.True(s.env.IsWorkflowCompleted())
	err := s.env.GetWorkflowError()
	require.Error(err)
	require.True(IsContinueAsNewError(err))
}

func (s *monitorTestSuite) TestMonitorInvalidStartHeight() {
	require := testutil.Require(s.T())

	s.masterClient.EXPECT().GetLatestHeight(gomock.Any()).
		AnyTimes().
		DoAndReturn(func(ctx context.Context) (uint64, error) {
			return monitorMaxHeightNativeClient, nil
		})
	startHeight := monitorMaxHeightNativeClient - monitorValidationHeightPadding + monitorValidatorMaxReorgDistance
	s.env.ExecuteWorkflow(s.monitor.execute, &MonitorRequest{
		StartHeight: startHeight,
		Tag:         monitorTag,
	})
	s.True(s.env.IsWorkflowCompleted())
	err := s.env.GetWorkflowError()
	require.Error(err)
	require.Contains(err.Error(), "InvalidStartHeight")
	require.Equal(0, getValue(&s.seen.blobStorage, startHeight))
	require.Equal(0, getValue(&s.seen.metastorageGetBlockByHeight, startHeight))
	require.Equal(0, getValue(&s.seen.blockchainClient, startHeight))
}

func (s *monitorTestSuite) TestMonitor_AutomatedTriggerFailover() {
	require := testutil.Require(s.T())

	// Enable failover feature
	s.cfg.Workflows.Monitor.FailoverEnabled = true
	s.masterClient.EXPECT().GetLatestHeight(gomock.Any()).AnyTimes().
		DoAndReturn(func(ctx context.Context) (uint64, error) {
			return 0, xerrors.New("received http error: HTTPError 503")
		})

	s.env.ExecuteWorkflow(s.monitor.execute, &MonitorRequest{
		StartHeight: monitorStartHeight,
		Tag:         monitorTag,
	})

	s.True(s.env.IsWorkflowCompleted())
	err := s.env.GetWorkflowError()
	require.Error(err)
	require.True(IsContinueAsNewError(err))
}

func (s *monitorTestSuite) TestMonitor_WithFailoverEnabled() {
	require := testutil.Require(s.T())

	// Enable failover feature
	s.cfg.Workflows.Monitor.FailoverEnabled = true
	s.masterClient.EXPECT().GetLatestHeight(gomock.Any()).
		AnyTimes().
		DoAndReturn(func(ctx context.Context) (uint64, error) {
			require.True(s.masterEndpointProvider.HasFailoverContext(ctx))
			require.True(s.slaveEndpointProvider.HasFailoverContext(ctx))
			return monitorMaxHeightNativeClient, nil
		})
	s.metadataAccessor.EXPECT().GetMaxEventId(gomock.Any(), monitorEventTag).
		AnyTimes().
		Return(monitorMaxEventId, nil)
	s.metadataAccessor.EXPECT().GetEventsAfterEventId(gomock.Any(), monitorEventTag, gomock.Any(), gomock.Any()).
		AnyTimes().
		DoAndReturn(func(ctx context.Context, eventTag uint32, eventId int64, maxEvents uint64) ([]*model.EventEntry, error) {
			s.seen.metastorageGetEventsAfterEventId++
			return testutil.MakeBlockEventEntries(
				api.BlockchainEvent_BLOCK_ADDED,
				monitorEventTag, eventId+int64(maxEvents-1), uint64(eventId), uint64(eventId)+maxEvents, monitorTag,
			), nil
		})

	s.env.ExecuteWorkflow(s.monitor.execute, &MonitorRequest{
		StartHeight: monitorStartHeight,
		Tag:         monitorTag,
		Failover:    true,
	})
	s.True(s.env.IsWorkflowCompleted())
	err := s.env.GetWorkflowError()
	require.Error(err)
	require.True(IsContinueAsNewError(err))

	for i := monitorStartHeight; i < uint64(totalBlocksToBeValidated)+monitorStartHeight; i++ {
		blockchainClientCount := getValue(&s.seen.blockchainClient, i)
		blobstorageCount := getValue(&s.seen.blobStorage, i)
		metastorageGetBlockByHeightCount := getValue(&s.seen.metastorageGetBlockByHeight, i)
		if i == monitorBatchSize-1 {
			require.Equal(blockchainClientCount, 2)
			require.Equal(blobstorageCount, 2)
			require.Equal(metastorageGetBlockByHeightCount, 2)
		} else {
			require.Equal(blockchainClientCount, 1)
			require.Equal(blobstorageCount, 1)
			if i == monitorBatchSize-2 {
				require.Equal(metastorageGetBlockByHeightCount, 2)
			} else {
				require.Equal(metastorageGetBlockByHeightCount, 1)
			}
		}
	}
	require.Equal(s.seen.metastorageGetEventsAfterEventId, monitorCheckpointSize)
}

func (s *monitorTestSuite) getMockBlock(height uint64, tag uint32) *api.Block {
	return &api.Block{
		Blockchain: common.Blockchain_BLOCKCHAIN_ETHEREUM,
		Network:    common.Network_NETWORK_ETHEREUM_MAINNET,
		Metadata:   testutil.MakeBlockMetadata(height, tag),
		Blobdata: &api.Block_Ethereum{
			Ethereum: &api.EthereumBlobdata{
				Header: []byte(`
					{
					   "difficulty":"0x153886c1bbd",
					   "extraData":"0x657468706f6f6c2e6f7267",
					   "gasLimit":"0x520b",
					   "gasUsed":"0x5208",
					   "hash":"0x4e3a3754410177e6937ef1f84bba68ea139e8d1a2258c5f85db9f1cd715a1bdd",
					   "logsBloom":"0x00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000",
					   "miner":"0xe6a7a1d47ff21b6321162aea7c6cb457d5476bca",
					   "mixHash":"0xb48c515a9dde8d346c3337ea520aa995a4738bb595495506125449c1149d6cf4",
					   "nonce":"0xba4f8ecd18aab215",
					   "number":"0xb443",
					   "parentHash":"0x5a41d0e66b4120775176c09fcf39e7c0520517a13d2b57b18d33d342df038bfc",
					   "receiptsRoot":"0xfe2bf2a941abf41d72637e5b91750332a30283efd40c424dc522b77e6f0ed8c4",
					   "sha3Uncles":"0x1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347",
					   "size":"0x27a",
					   "stateRoot":"0x0e0df2706b0a4fb8bd08c9246d472abbe850af446405d9eba1db41db18b4a169",
					   "timestamp":"0x55c42659",
					   "totalDifficulty":"0x97a50222edba99",
					   "transactions":[
						  {
							 "blockHash":"0x4e3a3754410177e6937ef1f84bba68ea139e8d1a2258c5f85db9f1cd715a1bdd",
							 "blockNumber":"0xb443",
							 "from":"0xa1e4380a3b1f749673e270229993ee55f35663b4",
							 "gas":"0x5208",
							 "gasPrice":"0x2d79883d2000",
							 "hash":"0x5c504ed432cb51138bcf09aa5e8a410dd4a1e204ef84bfed1be16dfba1b22060",
							 "input":"0x",
							 "nonce":"0x0",
							 "to":"0x5df9b87991262f6ba471f09758cde1c0fc1de734",
							 "transactionIndex":"0x0",
							 "value":"0x7a69",
							 "type":"0x0",
							 "v":"0x1c",
							 "r":"0x88ff6cf0fefd94db46111149ae4bfc179e9b94721fffd821d38d16464b3f71d0",
							 "s":"0x45e0aff800961cfce805daef7016b9b675c137a6a41a548f7b60a3484c06a33a"
						  }
					   ],
					   "transactionsRoot":"0x4513310fcb9f6f616972a3b948dc5d547f280849a87ebb5af0191f98b87be598",
					   "uncles":[
						  
					   ]
					}
				`),
				TransactionReceipts: [][]byte{
					[]byte(`
						{
						   "blockHash":"0x4e3a3754410177e6937ef1f84bba68ea139e8d1a2258c5f85db9f1cd715a1bdd",
						   "blockNumber":"0xb443",
						   "contractAddress":null,
						   "cumulativeGasUsed":"0x5208",
						   "from":"0xa1e4380a3b1f749673e270229993ee55f35663b4",
						   "gasUsed":"0x5208",
						   "logs":[
							  
						   ],
						   "logsBloom":"0x00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000",
						   "root":"0x96a8e009d2b88b1483e6941e6812e32263b05683fac202abc622a3e31aed1957",
						   "to":"0x5df9b87991262f6ba471f09758cde1c0fc1de734",
						   "transactionHash":"0x5c504ed432cb51138bcf09aa5e8a410dd4a1e204ef84bfed1be16dfba1b22060",
						   "transactionIndex":"0x0",
						   "type":"0x0"
						}
					`),
				},
				TransactionTraces: [][]byte{
					[]byte(`
						{
						   "type":"CALL",
						   "from":"0xa1e4380a3b1f749673e270229993ee55f35663b4",
						   "to":"0x5df9b87991262f6ba471f09758cde1c0fc1de734",
						   "value":"0x7a69",
						   "gas":"0x0",
						   "gasUsed":"0x0",
						   "input":"0x",
						   "output":"0x",
						   "time":"699ns"
						}
					`),
				},
			},
		},
	}
}

func getValue(m *sync.Map, key uint64) int {
	var c *int
	count, ok := m.Load(key)
	if ok {
		c = count.(*int)
	}
	return pointer.IntDeref(c)
}
