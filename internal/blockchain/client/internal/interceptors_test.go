package internal_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/uber-go/tally/v4"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/blockchain/client/internal"
	clientmocks "github.com/coinbase/chainstorage/internal/blockchain/client/mocks"
	parsermocks "github.com/coinbase/chainstorage/internal/blockchain/parser/mocks"
	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/utils/testutil"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

var (
	errParser          = xerrors.New("parser error")
	errBlockValidation = xerrors.New("block validation failed")
)

func TestParserInterceptor(t *testing.T) {
	const (
		tag    uint32 = 1
		newTag uint32 = 2
		height uint64 = 12345
		hash   string = "0xabcde"
	)

	require := testutil.Require(t)

	expectedMetadata := &api.BlockMetadata{
		Tag:    tag,
		Height: height,
		Hash:   hash,
	}
	expectedBlock := &api.Block{Metadata: expectedMetadata}
	expectedMetadatas := []*api.BlockMetadata{expectedMetadata}
	upgradedBlock := &api.Block{Metadata: &api.BlockMetadata{Tag: newTag}}

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	clt := clientmocks.NewMockClient(ctrl)
	clt.EXPECT().BatchGetBlockMetadata(gomock.Any(), tag, height, height+1).Return(expectedMetadatas, nil)
	clt.EXPECT().GetBlockByHeight(gomock.Any(), tag, height).Return(expectedBlock, nil)
	clt.EXPECT().GetBlockByHash(gomock.Any(), tag, height, hash).Return(expectedBlock, nil)
	clt.EXPECT().GetLatestHeight(gomock.Any()).Return(height, nil)
	clt.EXPECT().UpgradeBlock(gomock.Any(), expectedBlock, newTag).Return(upgradedBlock, nil)
	clt.EXPECT().CanReprocess(tag, height).Return(true)

	parser := parsermocks.NewMockParser(ctrl)
	parser.EXPECT().ParseNativeBlock(gomock.Any(), expectedBlock).AnyTimes().Return(&api.NativeBlock{}, nil)

	cfg, err := config.New()
	require.NoError(err)
	cfg.Chain.Feature.BlockValidationEnabled = false

	logger := zaptest.NewLogger(t)
	newclt := internal.WithParserInterceptor(clt, parser, cfg, logger)
	ctx := context.Background()

	actualMetadatas, err := newclt.BatchGetBlockMetadata(ctx, tag, height, height+1)
	require.NoError(err)
	require.Equal(expectedMetadatas, actualMetadatas)

	actualBlock, err := newclt.GetBlockByHeight(ctx, tag, height)
	require.NoError(err)
	require.Equal(expectedBlock, actualBlock)

	actualBlock, err = newclt.GetBlockByHash(ctx, tag, height, hash)
	require.NoError(err)
	require.Equal(expectedBlock, actualBlock)

	actualHeight, err := newclt.GetLatestHeight(ctx)
	require.NoError(err)
	require.Equal(height, actualHeight)

	actualBlock, err = newclt.UpgradeBlock(ctx, expectedBlock, newTag)
	require.NoError(err)
	require.Equal(upgradedBlock, actualBlock)

	ok := newclt.CanReprocess(tag, height)
	require.True(ok)
}

func TestParserInterceptor_ParserError(t *testing.T) {
	const (
		tag    uint32 = 1
		newTag uint32 = 2
		height uint64 = 12345
		hash   string = "0xabcde"
	)

	require := testutil.Require(t)

	expected := &api.Block{
		Metadata: &api.BlockMetadata{
			Tag:    tag,
			Height: height,
			Hash:   hash,
		},
	}

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	clt := clientmocks.NewMockClient(ctrl)
	clt.EXPECT().GetBlockByHeight(gomock.Any(), tag, height).Return(expected, nil)
	clt.EXPECT().GetBlockByHash(gomock.Any(), tag, height, hash).Return(expected, nil)
	clt.EXPECT().UpgradeBlock(gomock.Any(), expected, newTag).Return(expected, nil)

	parser := parsermocks.NewMockParser(ctrl)
	parser.EXPECT().ParseNativeBlock(gomock.Any(), expected).AnyTimes().Return(nil, errParser)

	cfg, err := config.New()
	require.NoError(err)
	cfg.Chain.Feature.BlockValidationEnabled = false

	logger := zaptest.NewLogger(t)
	newclt := internal.WithParserInterceptor(clt, parser, cfg, logger)
	ctx := context.Background()
	_, err = newclt.GetBlockByHeight(ctx, tag, height)
	require.Error(err)
	require.True(xerrors.Is(err, errParser))

	_, err = newclt.GetBlockByHash(ctx, tag, height, hash)
	require.Error(err)
	require.True(xerrors.Is(err, errParser))

	_, err = newclt.UpgradeBlock(ctx, expected, newTag)
	require.Error(err)
}

func TestParserInterceptor_WrongTag(t *testing.T) {
	const (
		tag    uint32 = 1
		height uint64 = 12345
		hash   string = "0xabcde"
	)

	require := testutil.Require(t)

	block := &api.Block{
		Metadata: &api.BlockMetadata{
			Height: height,
			Hash:   hash,
		},
	}

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	clt := clientmocks.NewMockClient(ctrl)
	clt.EXPECT().GetBlockByHeight(gomock.Any(), tag, height).Return(block, nil)

	parser := parsermocks.NewMockParser(ctrl)

	cfg, err := config.New()
	require.NoError(err)
	cfg.Chain.Feature.BlockValidationEnabled = false

	logger := zaptest.NewLogger(t)
	newclt := internal.WithParserInterceptor(clt, parser, cfg, logger)
	ctx := context.Background()
	_, err = newclt.GetBlockByHeight(ctx, tag, height)
	require.Error(err)
	require.Contains(err.Error(), "expected tag 1 in metadata")
}

func TestParserInterceptor_WrongHeight(t *testing.T) {
	const (
		tag    uint32 = 1
		height uint64 = 12345
		hash   string = "0xabcde"
	)

	require := testutil.Require(t)

	block := &api.Block{
		Metadata: &api.BlockMetadata{
			Tag:  tag,
			Hash: hash,
		},
	}

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	clt := clientmocks.NewMockClient(ctrl)
	clt.EXPECT().GetBlockByHeight(gomock.Any(), tag, height).Return(block, nil)

	parser := parsermocks.NewMockParser(ctrl)

	cfg, err := config.New()
	require.NoError(err)
	cfg.Chain.Feature.BlockValidationEnabled = false

	logger := zaptest.NewLogger(t)
	newclt := internal.WithParserInterceptor(clt, parser, cfg, logger)
	ctx := context.Background()
	_, err = newclt.GetBlockByHeight(ctx, tag, height)
	require.Error(err)
	require.Contains(err.Error(), "expected height 12345 in metadata")
}

func TestParserInterceptor_WrongHash(t *testing.T) {
	const (
		tag    uint32 = 1
		height uint64 = 12345
		hash   string = "0xabcde"
	)

	require := testutil.Require(t)

	block := &api.Block{
		Metadata: &api.BlockMetadata{
			Tag:    tag,
			Height: height,
		},
	}

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	clt := clientmocks.NewMockClient(ctrl)
	clt.EXPECT().GetBlockByHash(gomock.Any(), tag, height, hash).Return(block, nil)

	parser := parsermocks.NewMockParser(ctrl)

	cfg, err := config.New()
	require.NoError(err)
	cfg.Chain.Feature.BlockValidationEnabled = false

	logger := zaptest.NewLogger(t)
	newclt := internal.WithParserInterceptor(clt, parser, cfg, logger)
	ctx := context.Background()
	_, err = newclt.GetBlockByHash(ctx, tag, height, hash)
	require.Error(err)
	require.Contains(err.Error(), "expected hash 0xabcde in metadata")
}

func TestParserInterceptor_BlockValidation_Success(t *testing.T) {
	const (
		tag    uint32 = 1
		newTag uint32 = 2
		height uint64 = 12345
		hash   string = "0xabcde"
	)

	require := testutil.Require(t)

	expectedMetadata := &api.BlockMetadata{
		Tag:    tag,
		Height: height,
		Hash:   hash,
	}
	expectedBlock := &api.Block{Metadata: expectedMetadata}
	expectedMetadatas := []*api.BlockMetadata{expectedMetadata}
	upgradedBlock := &api.Block{Metadata: &api.BlockMetadata{Tag: newTag}}

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	clt := clientmocks.NewMockClient(ctrl)
	clt.EXPECT().BatchGetBlockMetadata(gomock.Any(), tag, height, height+1).Return(expectedMetadatas, nil)
	clt.EXPECT().GetBlockByHeight(gomock.Any(), tag, height).Return(expectedBlock, nil)
	clt.EXPECT().GetBlockByHash(gomock.Any(), tag, height, hash).Return(expectedBlock, nil)
	clt.EXPECT().GetLatestHeight(gomock.Any()).Return(height, nil)
	clt.EXPECT().UpgradeBlock(gomock.Any(), expectedBlock, newTag).Return(upgradedBlock, nil)
	clt.EXPECT().CanReprocess(tag, height).Return(true)

	expectedNativeBlock := &api.NativeBlock{}
	parser := parsermocks.NewMockParser(ctrl)
	parser.EXPECT().ParseNativeBlock(gomock.Any(), expectedBlock).Return(expectedNativeBlock, nil).Times(2)
	parser.EXPECT().ValidateBlock(gomock.Any(), expectedNativeBlock).Return(nil).Times(2)

	cfg, err := config.New()
	require.NoError(err)
	cfg.Chain.Feature.BlockValidationEnabled = true
	cfg.Chain.Feature.BlockValidationMuted = false

	logger := zaptest.NewLogger(t)
	newclt := internal.WithParserInterceptor(clt, parser, cfg, logger)
	ctx := context.Background()

	actualMetadatas, err := newclt.BatchGetBlockMetadata(ctx, tag, height, height+1)
	require.NoError(err)
	require.Equal(expectedMetadatas, actualMetadatas)

	actualBlock, err := newclt.GetBlockByHeight(ctx, tag, height)
	require.NoError(err)
	require.Equal(expectedBlock, actualBlock)

	actualBlock, err = newclt.GetBlockByHash(ctx, tag, height, hash)
	require.NoError(err)
	require.Equal(expectedBlock, actualBlock)

	actualHeight, err := newclt.GetLatestHeight(ctx)
	require.NoError(err)
	require.Equal(height, actualHeight)

	actualBlock, err = newclt.UpgradeBlock(ctx, expectedBlock, newTag)
	require.NoError(err)
	require.Equal(upgradedBlock, actualBlock)

	ok := newclt.CanReprocess(tag, height)
	require.True(ok)
}

func TestParserInterceptor_BlockValidation_Fail(t *testing.T) {
	const (
		tag    uint32 = 1
		height uint64 = 12345
		hash   string = "0xabcde"
	)

	require := testutil.Require(t)

	expectedMetadata := &api.BlockMetadata{
		Tag:    tag,
		Height: height,
		Hash:   hash,
	}
	expectedBlock := &api.Block{Metadata: expectedMetadata}

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	clt := clientmocks.NewMockClient(ctrl)
	clt.EXPECT().GetBlockByHeight(gomock.Any(), tag, height).Return(expectedBlock, nil)
	clt.EXPECT().GetBlockByHash(gomock.Any(), tag, height, hash).Return(expectedBlock, nil)

	expectedNativeBlock := &api.NativeBlock{}
	parser := parsermocks.NewMockParser(ctrl)
	parser.EXPECT().ParseNativeBlock(gomock.Any(), expectedBlock).Return(expectedNativeBlock, nil).Times(2)
	parser.EXPECT().ValidateBlock(gomock.Any(), expectedNativeBlock).Return(errBlockValidation).Times(2)

	cfg, err := config.New()
	require.NoError(err)
	cfg.Chain.Feature.BlockValidationEnabled = true
	cfg.Chain.Feature.BlockValidationMuted = false

	logger := zaptest.NewLogger(t)
	newclt := internal.WithParserInterceptor(clt, parser, cfg, logger)
	ctx := context.Background()

	actualBlock, err := newclt.GetBlockByHeight(ctx, tag, height)
	require.Error(err)
	require.ErrorIs(err, errBlockValidation)
	require.Nil(actualBlock)

	actualBlock, err = newclt.GetBlockByHash(ctx, tag, height, hash)
	require.Error(err)
	require.ErrorIs(err, errBlockValidation)
	require.Nil(actualBlock)
}

func TestParserInterceptor_BlockValidation_Muted(t *testing.T) {
	const (
		tag    uint32 = 1
		height uint64 = 12345
		hash   string = "0xabcde"
	)

	require := testutil.Require(t)

	expectedMetadata := &api.BlockMetadata{
		Tag:    tag,
		Height: height,
		Hash:   hash,
	}
	expectedBlock := &api.Block{Metadata: expectedMetadata}

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	clt := clientmocks.NewMockClient(ctrl)
	clt.EXPECT().GetBlockByHeight(gomock.Any(), tag, height).Return(expectedBlock, nil)
	clt.EXPECT().GetBlockByHash(gomock.Any(), tag, height, hash).Return(expectedBlock, nil)

	expectedNativeBlock := &api.NativeBlock{}
	parser := parsermocks.NewMockParser(ctrl)
	parser.EXPECT().ParseNativeBlock(gomock.Any(), expectedBlock).Return(expectedNativeBlock, nil).Times(2)
	parser.EXPECT().ValidateBlock(gomock.Any(), expectedNativeBlock).Return(errBlockValidation).Times(2)

	cfg, err := config.New()
	require.NoError(err)
	cfg.Chain.Feature.BlockValidationEnabled = true
	cfg.Chain.Feature.BlockValidationMuted = true

	logger := zaptest.NewLogger(t)
	newclt := internal.WithParserInterceptor(clt, parser, cfg, logger)
	ctx := context.Background()

	actualBlock, err := newclt.GetBlockByHeight(ctx, tag, height)
	require.NoError(err)
	require.Equal(expectedBlock, actualBlock)

	actualBlock, err = newclt.GetBlockByHash(ctx, tag, height, hash)
	require.NoError(err)
	require.Equal(expectedBlock, actualBlock)
}

func TestBlockValidation_Skip_WithoutParserInterceptor(t *testing.T) {
	const (
		tag    uint32 = 1
		height uint64 = 12345
		hash   string = "0xabcde"
	)

	require := testutil.Require(t)

	expectedMetadata := &api.BlockMetadata{
		Tag:    tag,
		Height: height,
		Hash:   hash,
	}
	expectedBlock := &api.Block{Metadata: expectedMetadata}

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	clt := clientmocks.NewMockClient(ctrl)
	clt.EXPECT().GetBlockByHeight(gomock.Any(), tag, height).Return(expectedBlock, nil)
	clt.EXPECT().GetBlockByHash(gomock.Any(), tag, height, hash).Return(expectedBlock, nil)

	ctx := context.Background()

	actualBlock, err := clt.GetBlockByHeight(ctx, tag, height)
	require.NoError(err)
	require.Equal(expectedBlock, actualBlock)

	actualBlock, err = clt.GetBlockByHash(ctx, tag, height, hash)
	require.NoError(err)
	require.Equal(expectedBlock, actualBlock)
}

func TestInstrumentInterceptor(t *testing.T) {
	const (
		tag    uint32 = 1
		newTag uint32 = 2
		height uint64 = 12345
		hash   string = "0xabcde"
	)

	require := testutil.Require(t)

	expectedMetadata := &api.BlockMetadata{
		Tag:    tag,
		Height: height,
		Hash:   hash,
	}
	expectedBlock := &api.Block{Metadata: expectedMetadata}
	expectedMetadatas := []*api.BlockMetadata{expectedMetadata}
	upgradedBlock := &api.Block{Metadata: &api.BlockMetadata{Tag: newTag}}

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	clt := clientmocks.NewMockClient(ctrl)
	clt.EXPECT().BatchGetBlockMetadata(gomock.Any(), tag, height, height+1).Return(expectedMetadatas, nil)
	clt.EXPECT().GetBlockByHeight(gomock.Any(), tag, height).Return(expectedBlock, nil)
	clt.EXPECT().GetBlockByHash(gomock.Any(), tag, height, hash).Return(expectedBlock, nil)
	clt.EXPECT().GetLatestHeight(gomock.Any()).Return(height, nil)
	clt.EXPECT().UpgradeBlock(gomock.Any(), expectedBlock, newTag).Return(upgradedBlock, nil)
	clt.EXPECT().CanReprocess(tag, height).Return(true)

	parser := parsermocks.NewMockParser(ctrl)
	parser.EXPECT().ParseNativeBlock(gomock.Any(), expectedBlock).AnyTimes().Return(&api.NativeBlock{}, nil)

	scope := tally.NewTestScope("chainstorage", nil)
	newclt := internal.WithInstrumentInterceptor(clt, scope, zap.NewNop())
	ctx := context.Background()

	actualMetadatas, err := newclt.BatchGetBlockMetadata(ctx, tag, height, height+1)
	require.NoError(err)
	require.Equal(expectedMetadatas, actualMetadatas)

	actualBlock, err := newclt.GetBlockByHeight(ctx, tag, height)
	require.NoError(err)
	require.Equal(expectedBlock, actualBlock)

	actualBlock, err = newclt.GetBlockByHash(ctx, tag, height, hash)
	require.NoError(err)
	require.Equal(expectedBlock, actualBlock)

	actualHeight, err := newclt.GetLatestHeight(ctx)
	require.NoError(err)
	require.Equal(height, actualHeight)

	actualBlock, err = newclt.UpgradeBlock(ctx, expectedBlock, newTag)
	require.NoError(err)
	require.Equal(upgradedBlock, actualBlock)

	ok := newclt.CanReprocess(tag, height)
	require.True(ok)

	snapshot := scope.Snapshot()

	for _, metric := range []string{
		"batch_get_block_metadata",
		"get_block_by_height",
		"get_block_by_hash",
		"get_latest_height",
		"upgrade_block",
	} {
		successCounter := snapshot.Counters()[fmt.Sprintf("chainstorage.client.%v+result_type=success", metric)]
		require.NotNil(successCounter)
		require.Equal(int64(1), successCounter.Value())

		errorCounter := snapshot.Counters()[fmt.Sprintf("chainstorage.client.%v+result_type=error", metric)]
		require.NotNil(errorCounter)
		require.Equal(int64(0), errorCounter.Value())

		latency := snapshot.Timers()[fmt.Sprintf("chainstorage.client.%v.latency+", metric)]
		require.NotNil(latency)
		require.Equal(1, len(latency.Values()))
	}
}
