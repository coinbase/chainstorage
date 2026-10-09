package sdk_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/coinbase/chainstorage/internal/utils/retry"
	"github.com/coinbase/chainstorage/internal/utils/testutil"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
	"github.com/coinbase/chainstorage/sdk"
	sdkmocks "github.com/coinbase/chainstorage/sdk/mocks"
)

type (
	clientInterceptorTestSuite struct {
		suite.Suite
		ctrl        *gomock.Controller
		client      *sdkmocks.MockClient
		interceptor sdk.Client
	}

	readSourceClientForTest struct {
		sdk.Client
		getBlockWithTagAndReadSource         func(context.Context, uint32, uint64, string, api.BlockReadSource) (*api.Block, error)
		getBlocksByRangeWithTagAndReadSource func(context.Context, uint32, uint64, uint64, api.BlockReadSource) ([]*api.Block, error)
	}
)

func (c *readSourceClientForTest) GetBlockWithTagAndReadSource(ctx context.Context, tag uint32, height uint64, hash string, readSource api.BlockReadSource) (*api.Block, error) {
	return c.getBlockWithTagAndReadSource(ctx, tag, height, hash, readSource)
}

func (c *readSourceClientForTest) GetBlocksByRangeWithTagAndReadSource(ctx context.Context, tag uint32, startHeight uint64, endHeight uint64, readSource api.BlockReadSource) ([]*api.Block, error) {
	return c.getBlocksByRangeWithTagAndReadSource(ctx, tag, startHeight, endHeight, readSource)
}

func TestClientInterceptor(t *testing.T) {
	suite.Run(t, new(clientInterceptorTestSuite))
}

func (s *clientInterceptorTestSuite) SetupTest() {
	s.ctrl = gomock.NewController(s.T())
	s.client = sdkmocks.NewMockClient(s.ctrl)
	s.interceptor = sdk.WithTimeoutableClientInterceptor(s.client, zap.NewNop())
}

func (s *clientInterceptorTestSuite) TearDownTest() {
	s.ctrl.Finish()
}

func (s *clientInterceptorTestSuite) TestGetLatestBlockWithTag() {
	var (
		expected uint64 = 123
		tag      uint32 = 3
	)

	require := testutil.Require(s.T())
	ctx := context.Background()
	attempts := 0
	s.client.EXPECT().
		GetLatestBlockWithTag(gomock.Any(), tag).
		DoAndReturn(func(ctx context.Context, tag uint32) (uint64, error) {
			attempts += 1
			if attempts < retry.DefaultMaxAttempts {
				return 0, context.DeadlineExceeded
			}

			return expected, nil
		}).
		Times(retry.DefaultMaxAttempts)
	actual, err := s.interceptor.GetLatestBlockWithTag(ctx, tag)
	require.NoError(err)
	require.Equal(expected, actual)
}

func (s *clientInterceptorTestSuite) TestGetLatestBlockWithTag_RetryLimitExceeded() {
	var (
		tag uint32 = 3
	)

	require := testutil.Require(s.T())
	ctx := context.Background()
	s.client.EXPECT().
		GetLatestBlockWithTag(gomock.Any(), tag).
		DoAndReturn(func(ctx context.Context, tag uint32) (uint64, error) {
			return 0, context.DeadlineExceeded
		}).
		Times(retry.DefaultMaxAttempts)
	_, err := s.interceptor.GetLatestBlockWithTag(ctx, tag)
	require.ErrorIs(err, context.DeadlineExceeded)
}

func (s *clientInterceptorTestSuite) TestGetLatestBlockWithTag_NonRetryableError() {
	var (
		tag uint32 = 3
	)

	require := testutil.Require(s.T())
	ctx := context.Background()
	s.client.EXPECT().
		GetLatestBlockWithTag(gomock.Any(), tag).
		DoAndReturn(func(ctx context.Context, tag uint32) (uint64, error) {
			return 0, context.Canceled
		}).
		Times(1)
	_, err := s.interceptor.GetLatestBlockWithTag(ctx, tag)
	require.ErrorIs(err, context.Canceled)
}

func (s *clientInterceptorTestSuite) TestGetBlockWithTag() {
	var (
		tag      uint32 = 2
		height   uint64 = 123
		hash            = "0xabc"
		expected        = testutil.MakeBlock(height, tag)
	)

	require := testutil.Require(s.T())
	ctx := context.Background()
	attempts := 0
	s.client.EXPECT().
		GetBlockWithTag(gomock.Any(), tag, height, hash).
		DoAndReturn(func(ctx context.Context, tag uint32, height uint64, hash string) (*api.Block, error) {
			attempts += 1
			if attempts < retry.DefaultMaxAttempts {
				return nil, status.Error(codes.DeadlineExceeded, "fake timeout")
			}

			return expected, nil
		}).
		Times(retry.DefaultMaxAttempts)
	actual, err := s.interceptor.GetBlockWithTag(ctx, tag, height, hash)
	require.NoError(err)
	require.Equal(expected, actual)
}

func (s *clientInterceptorTestSuite) TestGetBlocksByRangeWithTagAndReadSource_DefaultEndHeightUsesMediumTimeout() {
	var (
		tag         uint32 = 2
		startHeight uint64 = 123
		endHeight   uint64
		readSource  = api.BlockReadSource_BLOCK_READ_SOURCE_CONSOLIDATED
		expected    = []*api.Block{testutil.MakeBlock(startHeight, tag)}
	)

	require := testutil.Require(s.T())
	delegate := &readSourceClientForTest{
		Client: s.client,
		getBlockWithTagAndReadSource: func(context.Context, uint32, uint64, string, api.BlockReadSource) (*api.Block, error) {
			return nil, nil
		},
		getBlocksByRangeWithTagAndReadSource: func(ctx context.Context, tag_ uint32, startHeight_ uint64, endHeight_ uint64, readSource_ api.BlockReadSource) ([]*api.Block, error) {
			require.Equal(tag, tag_)
			require.Equal(startHeight, startHeight_)
			require.Equal(endHeight, endHeight_)
			require.Equal(readSource, readSource_)

			deadline, ok := ctx.Deadline()
			require.True(ok)
			require.Less(time.Until(deadline), 8*time.Second)
			return expected, nil
		},
	}
	interceptor := sdk.WithTimeoutableClientInterceptor(delegate, zap.NewNop())
	readSourceInterceptor, ok := interceptor.(sdk.ReadSourceClient)
	require.True(ok)

	actual, err := readSourceInterceptor.GetBlocksByRangeWithTagAndReadSource(context.Background(), tag, startHeight, endHeight, readSource)
	require.NoError(err)
	require.Equal(expected, actual)
}

func (s *clientInterceptorTestSuite) TestGetBlockWithTagAndReadSource_FallsBackAfterConsolidatedTimeout() {
	var (
		tag      uint32 = 2
		height   uint64 = 123
		hash            = "0xabc"
		expected        = testutil.MakeBlock(height, tag)
	)

	require := testutil.Require(s.T())
	var consolidatedAttempts int
	var singleBlockAttempts int
	delegate := &readSourceClientForTest{
		Client: s.client,
		getBlockWithTagAndReadSource: func(ctx context.Context, tag_ uint32, height_ uint64, hash_ string, readSource_ api.BlockReadSource) (*api.Block, error) {
			require.Equal(tag, tag_)
			require.Equal(height, height_)
			require.Equal(hash, hash_)
			switch readSource_ {
			case api.BlockReadSource_BLOCK_READ_SOURCE_CONSOLIDATED:
				consolidatedAttempts++
				<-ctx.Done()
				return nil, ctx.Err()
			case api.BlockReadSource_BLOCK_READ_SOURCE_SINGLE_BLOCK:
				singleBlockAttempts++
				require.NoError(ctx.Err())
				return expected, nil
			default:
				require.FailNow("unexpected read source", readSource_.String())
			}
			return nil, nil
		},
		getBlocksByRangeWithTagAndReadSource: func(context.Context, uint32, uint64, uint64, api.BlockReadSource) ([]*api.Block, error) {
			return nil, nil
		},
	}
	interceptor := sdk.WithTimeoutableClientInterceptor(delegate, zap.NewNop())
	timeoutClient := interceptor.(interface{ SetClientTimeout(time.Duration) })
	timeoutClient.SetClientTimeout(10 * time.Millisecond)
	readSourceInterceptor, ok := interceptor.(sdk.ReadSourceClient)
	require.True(ok)

	actual, err := readSourceInterceptor.GetBlockWithTagAndReadSource(context.Background(), tag, height, hash, api.BlockReadSource_BLOCK_READ_SOURCE_CONSOLIDATED)
	require.NoError(err)
	require.Equal(expected, actual)
	require.Equal(retry.DefaultMaxAttempts, consolidatedAttempts)
	require.Equal(1, singleBlockAttempts)
}

func (s *clientInterceptorTestSuite) TestGetBlocksByRangeWithTagAndReadSource_FallsBackAfterConsolidatedTimeout() {
	var (
		tag         uint32 = 2
		startHeight uint64 = 123
		endHeight   uint64 = 125
		expected           = []*api.Block{testutil.MakeBlock(startHeight, tag), testutil.MakeBlock(startHeight+1, tag)}
	)

	require := testutil.Require(s.T())
	var consolidatedAttempts int
	var singleBlockAttempts int
	delegate := &readSourceClientForTest{
		Client: s.client,
		getBlockWithTagAndReadSource: func(context.Context, uint32, uint64, string, api.BlockReadSource) (*api.Block, error) {
			return nil, nil
		},
		getBlocksByRangeWithTagAndReadSource: func(ctx context.Context, tag_ uint32, startHeight_ uint64, endHeight_ uint64, readSource_ api.BlockReadSource) ([]*api.Block, error) {
			require.Equal(tag, tag_)
			require.Equal(startHeight, startHeight_)
			require.Equal(endHeight, endHeight_)
			switch readSource_ {
			case api.BlockReadSource_BLOCK_READ_SOURCE_CONSOLIDATED:
				consolidatedAttempts++
				<-ctx.Done()
				return nil, ctx.Err()
			case api.BlockReadSource_BLOCK_READ_SOURCE_SINGLE_BLOCK:
				singleBlockAttempts++
				require.NoError(ctx.Err())
				return expected, nil
			default:
				require.FailNow("unexpected read source", readSource_.String())
			}
			return nil, nil
		},
	}
	interceptor := sdk.WithTimeoutableClientInterceptor(delegate, zap.NewNop())
	timeoutClient := interceptor.(interface{ SetClientTimeout(time.Duration) })
	timeoutClient.SetClientTimeout(10 * time.Millisecond)
	readSourceInterceptor, ok := interceptor.(sdk.ReadSourceClient)
	require.True(ok)

	actual, err := readSourceInterceptor.GetBlocksByRangeWithTagAndReadSource(context.Background(), tag, startHeight, endHeight, api.BlockReadSource_BLOCK_READ_SOURCE_CONSOLIDATED)
	require.NoError(err)
	require.Equal(expected, actual)
	require.Equal(retry.DefaultMaxAttempts, consolidatedAttempts)
	require.Equal(1, singleBlockAttempts)
}

func (s *clientInterceptorTestSuite) TestGetChainEvents() {
	var (
		req      = &api.GetChainEventsRequest{}
		expected = []*api.BlockchainEvent{{
			SequenceNum: 123,
		}}
	)

	require := testutil.Require(s.T())
	ctx := context.Background()
	attempts := 0
	s.client.EXPECT().
		GetChainEvents(gomock.Any(), req).
		DoAndReturn(func(ctx context.Context, req *api.GetChainEventsRequest) ([]*api.BlockchainEvent, error) {
			attempts += 1
			if attempts < retry.DefaultMaxAttempts {
				return nil, status.Error(codes.DeadlineExceeded, "fake timeout")
			}

			return expected, nil
		}).
		Times(retry.DefaultMaxAttempts)
	actual, err := s.interceptor.GetChainEvents(ctx, req)
	require.NoError(err)
	require.Equal(expected, actual)
}
