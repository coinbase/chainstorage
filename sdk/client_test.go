package sdk

import (
	"context"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"
	"go.uber.org/fx"
	"golang.org/x/xerrors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/coinbase/chainstorage/internal/blockchain/parser"
	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/gateway"
	"github.com/coinbase/chainstorage/internal/storage/blobstorage/downloader"
	downloadermocks "github.com/coinbase/chainstorage/internal/storage/blobstorage/downloader/mocks"
	"github.com/coinbase/chainstorage/internal/utils/testapp"
	"github.com/coinbase/chainstorage/internal/utils/testutil"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
	apimocks "github.com/coinbase/chainstorage/protos/coinbase/chainstorage/mocks"
)

type clientTestSuite struct {
	suite.Suite
	ctrl             *gomock.Controller
	app              testapp.TestApp
	streamClient     *apimocks.MockChainStorage_StreamChainEventsClient
	gatewayClient    *apimocks.MockChainStorageClient
	downloaderClient *downloadermocks.MockBlockDownloader
	client           Client
	config           *config.Config
	require          *testutil.Assertions
}

func TestClientTestSuite(t *testing.T) {
	suite.Run(t, new(clientTestSuite))
}

func (s *clientTestSuite) SetupTest() {
	s.ctrl = gomock.NewController(s.T())
	s.require = testutil.Require(s.T())

	s.streamClient = apimocks.NewMockChainStorage_StreamChainEventsClient(s.ctrl)
	s.gatewayClient = apimocks.NewMockChainStorageClient(s.ctrl)
	s.downloaderClient = downloadermocks.NewMockBlockDownloader(s.ctrl)

	var err error
	s.config, err = config.New()
	s.require.NoError(err)

	s.app = testapp.New(
		s.T(),
		Module,
		parser.Module,
		testapp.WithConfig(s.config),
		fx.Provide(func() downloader.BlockDownloader { return s.downloaderClient }),
		fx.Provide(func() gateway.Client { return s.gatewayClient }),
		fx.Populate(&s.client),
	)
	s.require.NotNil(s.client)
}

func (s *clientTestSuite) TearDownTest() {
	s.app.Close()
	s.ctrl.Finish()
}

func (s *clientTestSuite) TestTag() {
	s.require.Equal(uint32(0), s.client.GetTag())

	s.client.SetTag(2)
	s.require.Equal(uint32(2), s.client.GetTag())
}

func (s *clientTestSuite) TestGetLatestBlock() {
	const expectedHeight = uint64(12345)
	s.gatewayClient.EXPECT().GetLatestBlock(gomock.Any(), gomock.Any()).Return(&api.GetLatestBlockResponse{
		Height: expectedHeight,
	}, nil)
	actualHeight, err := s.client.GetLatestBlock(context.Background())
	s.require.NoError(err)
	s.require.Equal(expectedHeight, actualHeight)
}

func (s *clientTestSuite) TestGetBlockWithTag() {
	const (
		tag    = uint32(2)
		height = uint64(12345)
		hash   = "0xabcde"
	)

	block := testutil.MakeBlocksFromStartHeight(height, 1, tag)[0]
	blockFile := &api.BlockFile{
		Tag:    block.Metadata.Tag,
		Height: block.Metadata.Height,
		Hash:   block.Metadata.Hash,
	}
	s.downloaderClient.EXPECT().Download(gomock.Any(), blockFile).Return(block, nil)
	s.gatewayClient.EXPECT().GetBlockFile(gomock.Any(), &api.GetBlockFileRequest{
		Tag:    tag,
		Height: height,
		Hash:   hash,
	}).Return(&api.GetBlockFileResponse{
		File: blockFile,
	}, nil)

	fetchedBlock, err := s.client.GetBlockWithTag(context.Background(), tag, height, hash)
	s.require.NoError(err)
	s.require.Equal(block, fetchedBlock)
}

func (s *clientTestSuite) TestGetBlocksByRange() {
	const (
		tag         = uint32(0)
		startHeight = uint64(12345)
		endHeight   = uint64(12350)
		numBlocks   = int(endHeight - startHeight)
	)

	blocks := testutil.MakeBlocksFromStartHeight(startHeight, numBlocks, tag)
	blockFiles := make([]*api.BlockFile, numBlocks)
	for i := range blockFiles {
		metadata := blocks[i].Metadata
		blockFile := &api.BlockFile{
			Tag:    metadata.Tag,
			Height: metadata.Height,
		}
		blockFiles[i] = blockFile
		s.downloaderClient.EXPECT().Download(gomock.Any(), blockFile).Return(blocks[i], nil)
	}
	s.gatewayClient.EXPECT().GetBlockFilesByRange(gomock.Any(), &api.GetBlockFilesByRangeRequest{
		StartHeight: startHeight,
		EndHeight:   endHeight,
	}).Return(&api.GetBlockFilesByRangeResponse{
		Files: blockFiles,
	}, nil)

	fetchedBlocks, err := s.client.GetBlocksByRange(context.Background(), startHeight, endHeight)
	s.require.NoError(err)
	s.require.Equal(numBlocks, len(fetchedBlocks))
	s.require.Equal(blocks, fetchedBlocks)
}

func (s *clientTestSuite) TestGetBlocksByRangeWithTag() {
	const (
		tag         = uint32(2)
		startHeight = uint64(12345)
		endHeight   = uint64(12350)
		numBlocks   = int(endHeight - startHeight)
	)

	blocks := testutil.MakeBlocksFromStartHeight(startHeight, numBlocks, tag)
	blockFiles := make([]*api.BlockFile, numBlocks)
	for i := range blockFiles {
		metadata := blocks[i].Metadata
		blockFile := &api.BlockFile{
			Tag:    metadata.Tag,
			Height: metadata.Height,
		}
		blockFiles[i] = blockFile
		s.downloaderClient.EXPECT().Download(gomock.Any(), blockFile).Return(blocks[i], nil)
	}
	s.gatewayClient.EXPECT().GetBlockFilesByRange(gomock.Any(), &api.GetBlockFilesByRangeRequest{
		Tag:         tag,
		StartHeight: startHeight,
		EndHeight:   endHeight,
	}).Return(&api.GetBlockFilesByRangeResponse{
		Files: blockFiles,
	}, nil)

	fetchedBlocks, err := s.client.GetBlocksByRangeWithTag(context.Background(), tag, startHeight, endHeight)
	s.require.NoError(err)
	s.require.Equal(numBlocks, len(fetchedBlocks))
	s.require.Equal(blocks, fetchedBlocks)
}

func (s *clientTestSuite) TestStreamBlocks() {
	s.gatewayClient.EXPECT().StreamChainEvents(gomock.Any(), gomock.Any()).Return(s.streamClient, nil)

	numberOfEvents := uint64(10)
	blocks := testutil.MakeBlocksFromStartHeight(0, int(numberOfEvents), 1)
	events := make([]*api.BlockchainEvent, numberOfEvents)
	for i := uint64(0); i < numberOfEvents; i++ {
		block := blocks[i]
		metadata := block.Metadata
		events[i] = &api.BlockchainEvent{
			Block: &api.BlockIdentifier{
				Height: i,
			},
		}
		blockFile := &api.BlockFile{
			Tag:    metadata.Tag,
			Height: metadata.Height,
		}
		s.streamClient.EXPECT().Recv().Return(&api.ChainEventsResponse{
			Event: events[i],
		}, nil)
		s.gatewayClient.EXPECT().GetBlockFile(gomock.Any(), gomock.Any()).Return(&api.GetBlockFileResponse{
			File: blockFile,
		}, nil)
		s.downloaderClient.EXPECT().Download(gomock.Any(), blockFile).Return(block, nil)
	}

	ch, err := s.client.StreamChainEvents(context.Background(), StreamingConfiguration{
		ChainEventsRequest: &api.ChainEventsRequest{},
		NumberOfEvents:     numberOfEvents,
	})
	s.require.NoError(err)

	count := uint64(0)
	for result := range ch {
		s.require.NotNil(result)
		s.require.NoError(result.Error)
		s.require.Equal(blocks[count], result.Block)
		s.require.Equal(events[count], result.BlockchainEvent)
		count += 1
	}

	// make sure all events are called
	s.require.Equal(numberOfEvents, count)
}

func (s *clientTestSuite) TestStreamBlocks_EventOnly() {
	s.gatewayClient.EXPECT().StreamChainEvents(gomock.Any(), gomock.Any()).Return(s.streamClient, nil)

	numberOfEvents := uint64(10)
	events := make([]*api.BlockchainEvent, numberOfEvents)
	for i := uint64(0); i < numberOfEvents; i++ {
		events[i] = &api.BlockchainEvent{
			Block: &api.BlockIdentifier{
				Height: i,
			},
		}
		s.streamClient.EXPECT().Recv().Return(&api.ChainEventsResponse{
			Event: events[i],
		}, nil)
	}

	ch, err := s.client.StreamChainEvents(context.Background(), StreamingConfiguration{
		ChainEventsRequest: &api.ChainEventsRequest{},
		NumberOfEvents:     numberOfEvents,
		EventOnly:          true,
	})
	s.require.NoError(err)

	count := uint64(0)
	for result := range ch {
		s.require.NotNil(result)
		s.require.NoError(result.Error)
		s.require.Equal(events[count], result.BlockchainEvent)
		s.require.Nil(result.Block)
		count += 1
	}

	// make sure all events are called
	s.require.Equal(numberOfEvents, count)
}

func (s *clientTestSuite) TestStreamBlocks_WithSkippedBlocks() {
	s.gatewayClient.EXPECT().StreamChainEvents(gomock.Any(), gomock.Any()).Return(s.streamClient, nil)

	numberOfEvents := uint64(10)
	blocks := testutil.MakeBlocksFromStartHeight(0, int(numberOfEvents), 1)
	events := make([]*api.BlockchainEvent, numberOfEvents)

	skipped := map[uint64]bool{1: true, 2: true, 6: true, 7: true}

	for i := uint64(0); i < numberOfEvents; i++ {
		block := blocks[i]
		events[i] = &api.BlockchainEvent{
			Block: &api.BlockIdentifier{
				Height: i,
			},
		}
		if skipped[i] {
			block.Metadata.Skipped = true
			events[i].Block.Skipped = true
		}

		metadata := block.Metadata
		blockFile := &api.BlockFile{
			Tag:    metadata.Tag,
			Height: metadata.Height,
		}
		s.streamClient.EXPECT().Recv().Return(&api.ChainEventsResponse{
			Event: events[i],
		}, nil)
		s.gatewayClient.EXPECT().GetBlockFile(gomock.Any(), gomock.Any()).Return(&api.GetBlockFileResponse{
			File: blockFile,
		}, nil)
		s.downloaderClient.EXPECT().Download(gomock.Any(), blockFile).Return(block, nil)
	}

	ch, err := s.client.StreamChainEvents(context.Background(), StreamingConfiguration{
		ChainEventsRequest: &api.ChainEventsRequest{},
		NumberOfEvents:     numberOfEvents,
	})
	s.require.NoError(err)

	count := uint64(0)
	for result := range ch {
		s.require.NotNil(result)
		s.require.NoError(result.Error)
		s.require.Equal(blocks[count], result.Block)
		s.require.Equal(events[count], result.BlockchainEvent)
		if skipped[count] {
			s.require.True(result.Block.Metadata.Skipped)
			s.require.True(result.BlockchainEvent.Block.Skipped)
		}

		count += 1
	}

	// make sure all events are called
	s.require.Equal(numberOfEvents, count)
}

func (s *clientTestSuite) TestStreamBlocks_ErrStreamChainEvents() {
	mockError := xerrors.New("some mock error")
	s.gatewayClient.EXPECT().StreamChainEvents(gomock.Any(), gomock.Any()).Return(nil, mockError)
	ch, err := s.client.StreamChainEvents(context.Background(), StreamingConfiguration{
		ChainEventsRequest: &api.ChainEventsRequest{},
		NumberOfEvents:     2,
	})
	s.require.Error(err)
	s.require.Contains(err.Error(), "failed to call StreamChainEvents")
	s.require.Contains(err.Error(), mockError.Error())

	// make sure channel is nil
	s.require.Nil(ch)
}

func (s *clientTestSuite) TestStreamBlocks_ErrRecv() {
	mockError := xerrors.New("some mock error")
	s.gatewayClient.EXPECT().StreamChainEvents(gomock.Any(), gomock.Any()).Return(s.streamClient, nil)
	s.streamClient.EXPECT().Recv().Return(nil, mockError)

	ch, err := s.client.StreamChainEvents(context.Background(), StreamingConfiguration{
		ChainEventsRequest: &api.ChainEventsRequest{},
		NumberOfEvents:     2,
	})
	s.require.NoError(err)

	count := uint64(0)
	for result := range ch {
		s.require.NotNil(result)
		s.require.Nil(result.Block)
		s.require.Error(result.Error)
		s.require.Contains(result.Error.Error(), "failed to receive from event stream")
		s.require.Contains(result.Error.Error(), mockError.Error())
		count += 1
	}

	// make sure second event not called
	s.require.Equal(uint64(1), count)
}

func (s *clientTestSuite) TestStreamBlocks_RecvTransientErr() {
	const (
		expectedEvents  = 20
		initialSequence = int64(100)
	)
	gomock.InOrder(
		s.gatewayClient.EXPECT().StreamChainEvents(gomock.Any(), &api.ChainEventsRequest{
			SequenceNum: 100,
		}).Return(s.streamClient, nil),
		s.gatewayClient.EXPECT().StreamChainEvents(gomock.Any(), &api.ChainEventsRequest{
			SequenceNum: 109,
		}).Return(s.streamClient, nil),
		s.gatewayClient.EXPECT().StreamChainEvents(gomock.Any(), &api.ChainEventsRequest{
			SequenceNum: 113,
		}).Return(s.streamClient, nil),
	)
	s.gatewayClient.EXPECT().GetBlockFile(gomock.Any(), gomock.Any()).Return(&api.GetBlockFileResponse{}, nil).Times(expectedEvents)
	s.downloaderClient.EXPECT().Download(gomock.Any(), gomock.Any()).Return(&api.Block{}, nil).Times(expectedEvents)

	sequenceNum := initialSequence
	recvCounter := 0
	s.streamClient.EXPECT().Recv().AnyTimes().DoAndReturn(func() (*api.ChainEventsResponse, error) {
		recvCounter += 1
		if recvCounter == 10 {
			return nil, status.Error(codes.Internal, "stream terminated by RST_STREAM with error code: INTERNAL_ERROR")
		}

		if recvCounter == 15 {
			return nil, status.Error(codes.Internal, "unexpected EOF")
		}

		sequenceNum += 1
		return &api.ChainEventsResponse{
			Event: &api.BlockchainEvent{
				SequenceNum: sequenceNum,
				Type:        api.BlockchainEvent_BLOCK_ADDED,
				Block: &api.BlockIdentifier{
					Height: uint64(sequenceNum),
				},
			},
		}, nil
	})

	ch, err := s.client.StreamChainEvents(context.Background(), StreamingConfiguration{
		ChainEventsRequest: &api.ChainEventsRequest{
			SequenceNum: initialSequence,
		},
		NumberOfEvents: expectedEvents,
	})
	s.require.NoError(err)

	actualEvents := 0
	for result := range ch {
		actualEvents += 1
		s.require.NotNil(result)
		s.require.NoError(result.Error)
		s.require.NotNil(result.Block)
		expectedSequence := initialSequence + int64(actualEvents)
		s.require.Equal(expectedSequence, result.BlockchainEvent.SequenceNum)
	}
	s.require.Equal(expectedEvents, actualEvents)
}

func (s *clientTestSuite) TestStreamBlocks_RecvPermanentErr() {
	const (
		expectedEvents  = 10
		initialSequence = int64(100)
	)
	gomock.InOrder(
		s.gatewayClient.EXPECT().StreamChainEvents(gomock.Any(), &api.ChainEventsRequest{
			SequenceNum: 100,
		}).Return(s.streamClient, nil),
		s.gatewayClient.EXPECT().StreamChainEvents(gomock.Any(), &api.ChainEventsRequest{
			SequenceNum: 109,
		}).Return(s.streamClient, xerrors.New("some mock error")),
	)
	s.gatewayClient.EXPECT().GetBlockFile(gomock.Any(), gomock.Any()).Return(&api.GetBlockFileResponse{}, nil).AnyTimes()
	s.downloaderClient.EXPECT().Download(gomock.Any(), gomock.Any()).Return(&api.Block{}, nil).AnyTimes()

	sequenceNum := initialSequence
	recvCounter := 0
	s.streamClient.EXPECT().Recv().AnyTimes().DoAndReturn(func() (*api.ChainEventsResponse, error) {
		recvCounter += 1
		if recvCounter == 10 {
			return nil, status.Error(codes.Internal, "stream terminated by RST_STREAM with error code: INTERNAL_ERROR")
		}

		sequenceNum += 1
		return &api.ChainEventsResponse{
			Event: &api.BlockchainEvent{
				SequenceNum: sequenceNum,
				Type:        api.BlockchainEvent_BLOCK_ADDED,
				Block: &api.BlockIdentifier{
					Height: uint64(sequenceNum),
				},
			},
		}, nil
	})

	ch, err := s.client.StreamChainEvents(context.Background(), StreamingConfiguration{
		ChainEventsRequest: &api.ChainEventsRequest{
			SequenceNum: initialSequence,
		},
		NumberOfEvents: expectedEvents,
	})
	s.require.NoError(err)

	actualEvents := 0
	for result := range ch {
		actualEvents += 1
		if actualEvents < expectedEvents {
			s.T().Log("actualEvents", actualEvents)
			s.require.NotNil(result)
			s.require.NoError(result.Error)
			s.require.NotNil(result.Block)
			expectedSequence := initialSequence + int64(actualEvents)
			s.require.Equal(expectedSequence, result.BlockchainEvent.SequenceNum)
		} else {
			// Last result should be an error.
			s.require.NotNil(result)
			s.require.Error(result.Error)
			s.require.Contains(result.Error.Error(), "stream terminated by RST_STREAM with error code: INTERNAL_ERROR")
		}
	}
	s.require.Equal(expectedEvents, actualEvents)
}

func (s *clientTestSuite) TestStreamBlocks_ErrGetEvent() {
	s.gatewayClient.EXPECT().StreamChainEvents(gomock.Any(), gomock.Any()).Return(s.streamClient, nil)
	s.streamClient.EXPECT().Recv().Return(&api.ChainEventsResponse{}, nil)

	ch, err := s.client.StreamChainEvents(context.Background(), StreamingConfiguration{
		ChainEventsRequest: &api.ChainEventsRequest{},
		NumberOfEvents:     2,
	})
	s.require.NoError(err)

	count := uint64(0)
	for result := range ch {
		s.require.NotNil(result)
		s.require.Nil(result.Block)
		s.require.Error(result.Error)
		s.require.Contains(result.Error.Error(), "received null event")
		count += 1
	}

	// make sure second event not called
	s.require.Equal(uint64(1), count)
}

func (s *clientTestSuite) TestStreamBlocks_CtxDone() {
	s.gatewayClient.EXPECT().StreamChainEvents(gomock.Any(), gomock.Any()).Return(s.streamClient, nil)
	s.streamClient.EXPECT().Recv().Return(&api.ChainEventsResponse{
		Event: &api.BlockchainEvent{
			Block: &api.BlockIdentifier{},
		},
	}, nil).AnyTimes()
	s.gatewayClient.EXPECT().GetBlockFile(gomock.Any(), gomock.Any()).Return(&api.GetBlockFileResponse{}, nil).AnyTimes()
	s.downloaderClient.EXPECT().Download(gomock.Any(), gomock.Any()).Return(&api.Block{}, nil).AnyTimes()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	select {
	case <-ctx.Done():
	default:
		s.require.Fail("ctx should be done")
	}

	// number of events are unbounded so we keep writing
	ch, err := s.client.StreamChainEvents(ctx, StreamingConfiguration{
		ChainEventsRequest: &api.ChainEventsRequest{},
	})
	s.require.NoError(err)

	for range ch {
	}
}

func (s *clientTestSuite) TestStreamBlocks_InvalidConfig() {
	_, err := s.client.StreamChainEvents(context.Background(), StreamingConfiguration{})
	s.require.Error(err)
	s.require.Contains(err.Error(), "Error:Field validation for 'ChainEventsRequest' failed on the 'required' tag")
}

func (s *clientTestSuite) TestGetChainEvents() {
	events := []*api.BlockchainEvent{{SequenceNum: 1}}
	s.gatewayClient.EXPECT().GetChainEvents(gomock.Any(), gomock.Any()).Return(&api.GetChainEventsResponse{
		Events: events,
	}, nil)

	eventsOutput, err := s.client.GetChainEvents(context.Background(), &api.GetChainEventsRequest{
		SequenceNum:  1,
		MaxNumEvents: 3,
	})
	s.require.NoError(err)
	s.require.Equal(events, eventsOutput)
}

func (s *clientTestSuite) TestGetChainMetadata() {
	s.gatewayClient.EXPECT().GetChainMetadata(gomock.Any(), gomock.Any()).Return(&api.GetChainMetadataResponse{
		LatestBlockTag:       1,
		StableBlockTag:       2,
		LatestEventTag:       3,
		StableEventTag:       4,
		BlockStartHeight:     11_000_000,
		IrreversibleDistance: 30,
		BlockTime:            "13s",
	}, nil)

	resp, err := s.client.GetChainMetadata(context.Background(), &api.GetChainMetadataRequest{})
	s.require.NoError(err)
	s.require.Equal(uint32(1), resp.LatestBlockTag)
	s.require.Equal(uint32(2), resp.StableBlockTag)
	s.require.Equal(uint32(3), resp.LatestEventTag)
	s.require.Equal(uint32(4), resp.StableEventTag)
	s.require.Equal(uint64(11_000_000), resp.BlockStartHeight)
	s.require.Equal(uint64(30), resp.IrreversibleDistance)
	s.require.Equal("13s", resp.BlockTime)
}

func (s *clientTestSuite) TestGetStaticChainMetadata() {
	s.config.Chain.BlockTag.Latest = 1
	s.config.Chain.BlockTag.Stable = 2
	s.config.Chain.EventTag.Latest = 3
	s.config.Chain.EventTag.Stable = 4
	s.config.Chain.BlockStartHeight = 100
	s.config.Chain.IrreversibleDistance = 10
	s.config.Chain.BlockTime = 3 * time.Second

	resp, err := s.client.GetStaticChainMetadata(context.Background(), &api.GetChainMetadataRequest{})
	s.require.NoError(err)
	s.require.Equal(uint32(1), resp.LatestBlockTag)
	s.require.Equal(uint32(2), resp.StableBlockTag)
	s.require.Equal(uint32(3), resp.LatestEventTag)
	s.require.Equal(uint32(4), resp.StableEventTag)
	s.require.Equal(uint64(100), resp.BlockStartHeight)
	s.require.Equal(uint64(10), resp.IrreversibleDistance)
	s.require.Equal("3s", resp.BlockTime)
}
