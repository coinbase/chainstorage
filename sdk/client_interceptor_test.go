package sdk_test

import (
	"context"
	"testing"

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
)

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
