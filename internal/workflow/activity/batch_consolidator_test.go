package activity

import (
	"context"
	"fmt"
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/suite"
	"go.temporal.io/sdk/testsuite"
	"go.uber.org/fx"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/proto"

	"github.com/coinbase/chainstorage/internal/cadence"
	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/dlq"
	"github.com/coinbase/chainstorage/internal/storage/blobstorage"
	blobstoragemocks "github.com/coinbase/chainstorage/internal/storage/blobstorage/mocks"
	"github.com/coinbase/chainstorage/internal/storage/metastorage"
	metastoragemocks "github.com/coinbase/chainstorage/internal/storage/metastorage/mocks"
	storageutils "github.com/coinbase/chainstorage/internal/storage/utils"
	"github.com/coinbase/chainstorage/internal/utils/testapp"
	"github.com/coinbase/chainstorage/internal/utils/testutil"
	"github.com/coinbase/chainstorage/protos/coinbase/c3/common"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

type BatchConsolidatorTestSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite
	env               *cadence.TestEnv
	ctrl              *gomock.Controller
	metaStorage       *metastoragemocks.MockMetaStorage
	blobStorage       *blobstoragemocks.MockBlobStorage
	app               testapp.TestApp
	batchConsolidator *BatchConsolidator
}

func TestBatchConsolidatorTestSuite(t *testing.T) {
	suite.Run(t, new(BatchConsolidatorTestSuite))
}

func (s *BatchConsolidatorTestSuite) SetupTest() {
	require := testutil.Require(s.T())
	cfg, err := config.New()
	require.NoError(err)
	cfg.AWS.Storage.Consolidation.Enabled = true
	cfg.AWS.Storage.Consolidation.Mode = config.ConsolidationModeShadowDualWrite

	s.env = cadence.NewTestActivityEnv(s)
	s.ctrl = gomock.NewController(s.T())
	s.metaStorage = metastoragemocks.NewMockMetaStorage(s.ctrl)
	s.blobStorage = blobstoragemocks.NewMockBlobStorage(s.ctrl)
	s.app = testapp.New(
		s.T(),
		Module,
		testapp.WithConfig(cfg),
		cadence.WithTestEnv(s.env),
		fx.Provide(func() metastorage.MetaStorage {
			return s.metaStorage
		}),
		fx.Provide(func() blobstorage.BlobStorage {
			return s.blobStorage
		}),
		fx.Provide(dlq.NewNop),
		fx.Populate(&s.batchConsolidator),
	)
}

func (s *BatchConsolidatorTestSuite) TearDownTest() {
	s.app.Close()
	s.ctrl.Finish()
	s.env.AssertExpectations(s.T())
}

func (s *BatchConsolidatorTestSuite) TestEmptyScanNoOps() {
	require := testutil.Require(s.T())
	request := &BatchConsolidatorRequest{
		Tag:         2,
		StartHeight: 100,
		EndHeight:   200,
		MaxBlocks:   100,
	}
	s.metaStorage.EXPECT().
		GetBlocksMissingConsolidationShadow(gomock.Any(), request.Tag, request.StartHeight, request.EndHeight, request.MaxBlocks).
		Return(nil, nil)

	response, err := s.batchConsolidator.Execute(s.env.BackgroundContext(), request)
	require.NoError(err)
	require.Equal(&BatchConsolidatorResponse{
		StartHeight: request.StartHeight,
		EndHeight:   request.EndHeight,
	}, response)
}

func (s *BatchConsolidatorTestSuite) TestAutoConsolidateEmptyScanNoOpsWhenShadowsAlreadyExist() {
	require := testutil.Require(s.T())
	request := &BatchConsolidatorRequest{
		Mode:        config.ConsolidationModeAutoConsolidate,
		Tag:         2,
		StartHeight: 100,
		EndHeight:   200,
		MaxBlocks:   100,
	}
	s.metaStorage.EXPECT().
		PromoteBlockConsolidationShadows(gomock.Any(), request.Tag, request.StartHeight, request.EndHeight, request.MaxBlocks, config.DefaultSingleBlockObjectRetention).
		Return(&metastorage.ConsolidationPromotionResult{}, nil)
	s.metaStorage.EXPECT().
		GetBlocksMissingConsolidationShadow(gomock.Any(), request.Tag, request.StartHeight, request.EndHeight, request.MaxBlocks).
		Return(nil, nil)
	s.metaStorage.EXPECT().
		GetBlocksByHeightRange(gomock.Any(), request.Tag, request.StartHeight, request.EndHeight).
		Return(makeCanonicalWindow(100, 100), nil)

	response, err := s.batchConsolidator.Execute(s.env.BackgroundContext(), request)
	require.NoError(err)
	require.Equal(&BatchConsolidatorResponse{
		StartHeight: request.StartHeight,
		EndHeight:   request.EndHeight,
	}, response)
}

func (s *BatchConsolidatorTestSuite) TestAutoConsolidateEmptyScanPromotesExistingShadowsForRetryCleanup() {
	require := testutil.Require(s.T())
	request := &BatchConsolidatorRequest{
		Mode:        config.ConsolidationModeAutoConsolidate,
		Tag:         2,
		StartHeight: 100,
		EndHeight:   200,
		MaxBlocks:   100,
	}
	s.metaStorage.EXPECT().
		PromoteBlockConsolidationShadows(gomock.Any(), request.Tag, request.StartHeight, request.EndHeight, request.MaxBlocks, config.DefaultSingleBlockObjectRetention).
		Return(&metastorage.ConsolidationPromotionResult{Blocks: 12}, nil)
	s.metaStorage.EXPECT().
		GetBlocksMissingConsolidationShadow(gomock.Any(), request.Tag, request.StartHeight, request.EndHeight, request.MaxBlocks).
		Return(nil, nil)
	s.metaStorage.EXPECT().
		GetBlocksByHeightRange(gomock.Any(), request.Tag, request.StartHeight, request.EndHeight).
		Return(makeCanonicalWindow(100, 100), nil)

	response, err := s.batchConsolidator.Execute(s.env.BackgroundContext(), request)
	require.NoError(err)
	require.Equal(&BatchConsolidatorResponse{
		StartHeight:    request.StartHeight,
		EndHeight:      request.EndHeight,
		PromotedBlocks: 12,
	}, response)
}

func (s *BatchConsolidatorTestSuite) TestAutoConsolidateRejectsMissingCoverageBeforePromotion() {
	require := testutil.Require(s.T())
	request := &BatchConsolidatorRequest{
		Mode:        config.ConsolidationModeAutoConsolidate,
		Tag:         2,
		StartHeight: 100,
		EndHeight:   200,
		MaxBlocks:   100,
	}
	s.metaStorage.EXPECT().
		PromoteBlockConsolidationShadows(gomock.Any(), request.Tag, request.StartHeight, request.EndHeight, request.MaxBlocks, config.DefaultSingleBlockObjectRetention).
		Times(0)
	s.metaStorage.EXPECT().
		GetBlocksMissingConsolidationShadow(gomock.Any(), request.Tag, request.StartHeight, request.EndHeight, request.MaxBlocks).
		Times(0)
	s.metaStorage.EXPECT().
		GetBlocksByHeightRange(gomock.Any(), request.Tag, request.StartHeight, request.EndHeight).
		Return(makeCanonicalWindow(99, 100), nil)

	response, err := s.batchConsolidator.Execute(s.env.BackgroundContext(), request)
	require.Error(err)
	require.Equal(&BatchConsolidatorResponse{}, response)
	require.Contains(err.Error(), "auto_consolidate requires a full canonical consolidation window")
}

func (s *BatchConsolidatorTestSuite) TestDeprecatedHistoricalBackfillAliasRemainsAccepted() {
	require := testutil.Require(s.T())
	request := &BatchConsolidatorRequest{
		Mode:        config.ConsolidationModeHistoricalBackfill,
		Tag:         2,
		StartHeight: 100,
		EndHeight:   200,
		MaxBlocks:   100,
	}
	s.metaStorage.EXPECT().
		PromoteBlockConsolidationShadows(gomock.Any(), request.Tag, request.StartHeight, request.EndHeight, request.MaxBlocks, config.DefaultSingleBlockObjectRetention).
		Return(&metastorage.ConsolidationPromotionResult{}, nil)
	s.metaStorage.EXPECT().
		GetBlocksMissingConsolidationShadow(gomock.Any(), request.Tag, request.StartHeight, request.EndHeight, request.MaxBlocks).
		Return(nil, nil)

	response, err := s.batchConsolidator.Execute(s.env.BackgroundContext(), request)
	require.NoError(err)
	require.Equal(&BatchConsolidatorResponse{
		StartHeight: request.StartHeight,
		EndHeight:   request.EndHeight,
	}, response)
}

func (s *BatchConsolidatorTestSuite) TestConsolidatesAndPersistsShadowPlacements() {
	require := testutil.Require(s.T())
	records, blocks := makeConsolidatorFixture(2, 1000)
	request := &BatchConsolidatorRequest{
		Mode:        config.ConsolidationModeHistoricalBackfill,
		Tag:         records[0].Metadata.GetTag(),
		StartHeight: 1000,
		EndHeight:   1002,
		MaxBlocks:   100,
	}
	objectKey := "consolidated/v=000000000001/shard=000000001000-000000001999/000000001000-000000001002-sha.zstd"
	capturedPayloadPaths := make([]string, 0, len(records))
	placements := []blobstorage.BlockPlacement{
		{
			MetadataID:         records[0].ID,
			Height:             records[0].Metadata.GetHeight(),
			Hash:               records[0].Metadata.GetHash(),
			ObjectFormat:       api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_CSCB_BATCH,
			ByteOffset:         0,
			ByteLength:         100,
			UncompressedLength: 100,
		},
		{
			MetadataID:         records[1].ID,
			Height:             records[1].Metadata.GetHeight(),
			Hash:               records[1].Metadata.GetHash(),
			ObjectFormat:       api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_CSCB_BATCH,
			ByteOffset:         100,
			ByteLength:         110,
			UncompressedLength: 110,
		},
	}
	expectedShadows := []*metastorage.ConsolidationShadowPlacement{
		{
			BlockMetadataID:           records[0].ID,
			Tag:                       records[0].Metadata.GetTag(),
			Height:                    records[0].Metadata.GetHeight(),
			Hash:                      records[0].Metadata.GetHash(),
			SingleBlockObjectKeyMain:  records[0].Metadata.GetObjectKeyMain(),
			ConsolidatedObjectKeyMain: objectKey,
			ObjectFormat:              api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_CSCB_BATCH,
			ByteOffset:                0,
			ByteLength:                100,
			UncompressedLength:        100,
		},
		{
			BlockMetadataID:           records[1].ID,
			Tag:                       records[1].Metadata.GetTag(),
			Height:                    records[1].Metadata.GetHeight(),
			Hash:                      records[1].Metadata.GetHash(),
			SingleBlockObjectKeyMain:  records[1].Metadata.GetObjectKeyMain(),
			ConsolidatedObjectKeyMain: objectKey,
			ObjectFormat:              api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_CSCB_BATCH,
			ByteOffset:                100,
			ByteLength:                110,
			UncompressedLength:        110,
		},
	}
	s.batchConsolidator.config.AWS.Storage.Consolidation.LocalSpillDir = s.T().TempDir()

	s.metaStorage.EXPECT().
		PromoteBlockConsolidationShadows(gomock.Any(), request.Tag, request.StartHeight, request.EndHeight, request.MaxBlocks, config.DefaultSingleBlockObjectRetention).
		Return(&metastorage.ConsolidationPromotionResult{}, nil)
	s.metaStorage.EXPECT().
		GetBlocksMissingConsolidationShadow(gomock.Any(), request.Tag, request.StartHeight, request.EndHeight, request.MaxBlocks).
		Return(records, nil)
	for i, record := range records {
		s.blobStorage.EXPECT().Download(gomock.Any(), record.Metadata).Return(blocks[i], nil)
	}
	s.blobStorage.EXPECT().
		UploadConsolidated(gomock.Any(), consolidatedPayloadsMatchAndCapturePaths(blocks, []int64{records[0].ID, records[1].ID}, &capturedPayloadPaths)).
		Return(objectKey, placements, nil)
	s.metaStorage.EXPECT().
		PersistBlockConsolidationShadows(gomock.Any(), expectedShadows).
		Return(nil)
	s.metaStorage.EXPECT().
		PromoteBlockConsolidationShadows(gomock.Any(), request.Tag, request.StartHeight, request.EndHeight, request.MaxBlocks, config.DefaultSingleBlockObjectRetention).
		Return(&metastorage.ConsolidationPromotionResult{Blocks: 2}, nil)

	response, err := s.batchConsolidator.Execute(s.env.BackgroundContext(), request)
	require.NoError(err)
	require.Equal(&BatchConsolidatorResponse{
		StartHeight:        request.StartHeight,
		EndHeight:          request.EndHeight,
		ScannedBlocks:      2,
		ConsolidatedBlocks: 2,
		PromotedBlocks:     2,
		ObjectKey:          objectKey,
	}, response)
	require.Len(capturedPayloadPaths, len(records))
	for _, path := range capturedPayloadPaths {
		_, statErr := os.Stat(path)
		require.ErrorIs(statErr, os.ErrNotExist)
	}
}

func (s *BatchConsolidatorTestSuite) TestAutoConsolidateAllowsPartialRetryWindow() {
	require := testutil.Require(s.T())
	records, blocks := makeConsolidatorFixture(3, 1000)
	records = records[:2]
	blocks = blocks[:2]
	request := &BatchConsolidatorRequest{
		Mode:        config.ConsolidationModeAutoConsolidate,
		Tag:         records[0].Metadata.GetTag(),
		StartHeight: 1000,
		EndHeight:   1003,
		MaxBlocks:   3,
	}

	s.metaStorage.EXPECT().
		PromoteBlockConsolidationShadows(gomock.Any(), request.Tag, request.StartHeight, request.EndHeight, request.MaxBlocks, config.DefaultSingleBlockObjectRetention).Times(2).
		Return(&metastorage.ConsolidationPromotionResult{}, nil)
	s.metaStorage.EXPECT().
		GetBlocksMissingConsolidationShadow(gomock.Any(), request.Tag, request.StartHeight, request.EndHeight, request.MaxBlocks).
		Return(records, nil)
	s.metaStorage.EXPECT().
		GetBlocksByHeightRange(gomock.Any(), request.Tag, request.StartHeight, request.EndHeight).
		Return(makeCanonicalWindow(3, 1000), nil)
	for i := range records {
		s.blobStorage.EXPECT().Download(gomock.Any(), records[i].Metadata).Return(blocks[i], nil)
	}
	s.blobStorage.EXPECT().
		UploadConsolidated(gomock.Any(), consolidatedPayloadsMatch(blocks, []int64{records[0].ID, records[1].ID})).
		Return("object-key", makeConsolidatorPlacements(records), nil)
	s.metaStorage.EXPECT().
		PersistBlockConsolidationShadows(gomock.Any(), gomock.Any()).
		Return(nil)

	response, err := s.batchConsolidator.Execute(s.env.BackgroundContext(), request)
	require.NoError(err)
	require.Equal(&BatchConsolidatorResponse{
		StartHeight:        request.StartHeight,
		EndHeight:          request.EndHeight,
		ScannedBlocks:      2,
		ConsolidatedBlocks: 2,
		ObjectKey:          "object-key",
	}, response)
}

func (s *BatchConsolidatorTestSuite) TestAutoConsolidateAllowsGappedMissingShadowScanWhenCanonicalWindowIsFull() {
	require := testutil.Require(s.T())
	allRecords, allBlocks := makeConsolidatorFixture(3, 1000)
	records := []*metastorage.BlockMetadataRecord{allRecords[0], allRecords[2]}
	blocks := []*api.Block{allBlocks[0], allBlocks[2]}
	canonicalWindow := makeCanonicalWindow(3, 1000)
	canonicalWindow[1].Skipped = true
	request := &BatchConsolidatorRequest{
		Mode:        config.ConsolidationModeAutoConsolidate,
		Tag:         records[0].Metadata.GetTag(),
		StartHeight: 1000,
		EndHeight:   1003,
		MaxBlocks:   3,
	}

	s.metaStorage.EXPECT().
		PromoteBlockConsolidationShadows(gomock.Any(), request.Tag, request.StartHeight, request.EndHeight, request.MaxBlocks, config.DefaultSingleBlockObjectRetention).Times(2).
		Return(&metastorage.ConsolidationPromotionResult{}, nil)
	s.metaStorage.EXPECT().
		GetBlocksMissingConsolidationShadow(gomock.Any(), request.Tag, request.StartHeight, request.EndHeight, request.MaxBlocks).
		Return(records, nil)
	s.metaStorage.EXPECT().
		GetBlocksByHeightRange(gomock.Any(), request.Tag, request.StartHeight, request.EndHeight).
		Return(canonicalWindow, nil)
	for i := range records {
		s.blobStorage.EXPECT().Download(gomock.Any(), records[i].Metadata).Return(blocks[i], nil)
	}
	s.blobStorage.EXPECT().
		UploadConsolidated(gomock.Any(), consolidatedPayloadsMatch(blocks, []int64{records[0].ID, records[1].ID})).
		Return("object-key", makeConsolidatorPlacements(records), nil)
	s.metaStorage.EXPECT().
		PersistBlockConsolidationShadows(gomock.Any(), gomock.Any()).
		Return(nil)

	response, err := s.batchConsolidator.Execute(s.env.BackgroundContext(), request)
	require.NoError(err)
	require.Equal(&BatchConsolidatorResponse{
		StartHeight:        request.StartHeight,
		EndHeight:          request.EndHeight,
		ScannedBlocks:      2,
		ConsolidatedBlocks: 2,
		ObjectKey:          "object-key",
	}, response)
}

func (s *BatchConsolidatorTestSuite) TestDownloadedBlockMetadataMismatchFailsBeforeUpload() {
	require := testutil.Require(s.T())
	records, blocks := makeConsolidatorFixture(1, 1000)
	blocks[0].Metadata.Hash = "wrong-hash"
	request := &BatchConsolidatorRequest{
		Tag:         records[0].Metadata.GetTag(),
		StartHeight: 1000,
		EndHeight:   1001,
		MaxBlocks:   100,
	}
	s.metaStorage.EXPECT().
		GetBlocksMissingConsolidationShadow(gomock.Any(), request.Tag, request.StartHeight, request.EndHeight, request.MaxBlocks).
		Return(records, nil)
	s.blobStorage.EXPECT().Download(gomock.Any(), records[0].Metadata).Return(blocks[0], nil)

	_, err := s.batchConsolidator.Execute(s.env.BackgroundContext(), request)
	require.Error(err)
	require.Contains(err.Error(), "downloaded block metadata mismatch")
}

func (s *BatchConsolidatorTestSuite) TestUploadFailureDoesNotPersistShadowPlacements() {
	require := testutil.Require(s.T())
	records, blocks := makeConsolidatorFixture(1, 1000)
	request := &BatchConsolidatorRequest{
		Mode:        config.ConsolidationModeAutoConsolidate,
		Tag:         records[0].Metadata.GetTag(),
		StartHeight: 1000,
		EndHeight:   1001,
		MaxBlocks:   1,
	}

	s.metaStorage.EXPECT().
		PromoteBlockConsolidationShadows(gomock.Any(), request.Tag, request.StartHeight, request.EndHeight, request.MaxBlocks, config.DefaultSingleBlockObjectRetention).
		Return(&metastorage.ConsolidationPromotionResult{}, nil)
	s.metaStorage.EXPECT().
		GetBlocksMissingConsolidationShadow(gomock.Any(), request.Tag, request.StartHeight, request.EndHeight, request.MaxBlocks).
		Return(records, nil)
	s.metaStorage.EXPECT().
		GetBlocksByHeightRange(gomock.Any(), request.Tag, request.StartHeight, request.EndHeight).
		Return(makeCanonicalWindow(1, 1000), nil)
	s.blobStorage.EXPECT().Download(gomock.Any(), records[0].Metadata).Return(blocks[0], nil)
	s.blobStorage.EXPECT().
		UploadConsolidated(gomock.Any(), consolidatedPayloadsMatch(blocks, []int64{records[0].ID})).
		Return("", nil, fmt.Errorf("CSCB compressed payload length exceeds max_compressed_bytes"))

	_, err := s.batchConsolidator.Execute(s.env.BackgroundContext(), request)
	require.Error(err)
	require.Contains(err.Error(), "failed to upload consolidated block object")
}

func (s *BatchConsolidatorTestSuite) TestGetShadowStatsReturnsPersistedShadowStats() {
	require := testutil.Require(s.T())
	request := &BatchConsolidatorStatsRequest{
		Tag:         2,
		StartHeight: 1000,
		EndHeight:   2000,
	}
	s.metaStorage.EXPECT().
		GetBlockConsolidationShadowStats(gomock.Any(), request.Tag, request.StartHeight, request.EndHeight).
		Return(&metastorage.ConsolidationShadowStats{Objects: 3, Blocks: 2750}, nil)

	response, err := s.batchConsolidator.GetShadowStats(s.env.BackgroundContext(), request)
	require.NoError(err)
	require.Equal(&BatchConsolidatorStatsResponse{
		StartHeight:   request.StartHeight,
		EndHeight:     request.EndHeight,
		ShadowObjects: 3,
		ShadowBlocks:  2750,
	}, response)
}

func (s *BatchConsolidatorTestSuite) TestGetLatestBlockReturnsMetastoreLatest() {
	require := testutil.Require(s.T())
	request := &BatchConsolidatorLatestBlockRequest{Tag: 2}
	s.metaStorage.EXPECT().
		GetLatestBlock(gomock.Any(), request.Tag).
		Return(&api.BlockMetadata{Tag: request.Tag, Height: 1234}, nil)

	response, err := s.batchConsolidator.GetLatestBlock(s.env.BackgroundContext(), request)
	require.NoError(err)
	require.Equal(&BatchConsolidatorLatestBlockResponse{
		Tag:    request.Tag,
		Height: 1234,
	}, response)
}

func (s *BatchConsolidatorTestSuite) TestGetLatestBlockRejectsNilMetastoreLatest() {
	require := testutil.Require(s.T())
	request := &BatchConsolidatorLatestBlockRequest{Tag: 2}
	s.metaStorage.EXPECT().
		GetLatestBlock(gomock.Any(), request.Tag).
		Return(nil, nil)

	response, err := s.batchConsolidator.GetLatestBlock(s.env.BackgroundContext(), request)
	require.Error(err)
	require.Nil(response)
	require.Contains(err.Error(), "latest block not found")
}

func (s *BatchConsolidatorTestSuite) TestUpdateAutoConsolidateCursorPersistsExclusiveHeight() {
	require := testutil.Require(s.T())
	request := &BatchConsolidatorCursorRequest{
		Tag:    2,
		Height: 11_000,
	}
	s.metaStorage.EXPECT().
		SetBlockConsolidationCursor(gomock.Any(), metastorage.BatchConsolidatorAutoConsolidateCursor, request.Tag, request.Height).
		Return(nil)

	response, err := s.batchConsolidator.UpdateAutoConsolidateCursor(s.env.BackgroundContext(), request)
	require.NoError(err)
	require.Equal(&BatchConsolidatorCursorResponse{
		Tag:    request.Tag,
		Height: request.Height,
	}, response)
}

func (s *BatchConsolidatorTestSuite) TestPromoteFinalizedPromotesShadows() {
	require := testutil.Require(s.T())
	gateHeight := uint64(2_000)
	safeLag := uint64(100)
	s.batchConsolidator.config.AWS.Storage.Consolidation.Mode = config.ConsolidationModePromoteFinalized
	s.batchConsolidator.config.AWS.Storage.Consolidation.PromotionGateHeight = &gateHeight
	s.batchConsolidator.config.AWS.Storage.Consolidation.SafePromotionLag = &safeLag
	request := &BatchConsolidatorRequest{
		Tag:         2,
		StartHeight: 1_000,
		EndHeight:   1_100,
		MaxBlocks:   25,
	}
	s.metaStorage.EXPECT().
		GetLatestBlock(gomock.Any(), request.Tag).
		Return(&api.BlockMetadata{Tag: request.Tag, Height: 1_300}, nil)
	s.metaStorage.EXPECT().
		PromoteBlockConsolidationShadows(gomock.Any(), request.Tag, request.StartHeight, request.EndHeight, request.MaxBlocks, config.DefaultSingleBlockObjectRetention).
		Return(&metastorage.ConsolidationPromotionResult{Blocks: 12}, nil)

	response, err := s.batchConsolidator.Execute(s.env.BackgroundContext(), request)
	require.NoError(err)
	require.Equal(&BatchConsolidatorResponse{
		StartHeight:        request.StartHeight,
		EndHeight:          request.EndHeight,
		ScannedBlocks:      12,
		ConsolidatedBlocks: 12,
		PromotedBlocks:     12,
	}, response)
}

func (s *BatchConsolidatorTestSuite) TestPromoteFinalizedExecuteCapsAtGateAndSafeHeight() {
	require := testutil.Require(s.T())
	gateHeight := uint64(1_050)
	safeLag := uint64(10)
	s.batchConsolidator.config.AWS.Storage.Consolidation.Mode = config.ConsolidationModePromoteFinalized
	s.batchConsolidator.config.AWS.Storage.Consolidation.PromotionGateHeight = &gateHeight
	s.batchConsolidator.config.AWS.Storage.Consolidation.SafePromotionLag = &safeLag
	request := &BatchConsolidatorRequest{
		Tag:         2,
		StartHeight: 1_000,
		EndHeight:   1_200,
		MaxBlocks:   25,
	}
	s.metaStorage.EXPECT().
		GetLatestBlock(gomock.Any(), request.Tag).
		Return(&api.BlockMetadata{Tag: request.Tag, Height: 1_080}, nil)
	s.metaStorage.EXPECT().
		PromoteBlockConsolidationShadows(gomock.Any(), request.Tag, request.StartHeight, gateHeight, request.MaxBlocks, config.DefaultSingleBlockObjectRetention).
		Return(&metastorage.ConsolidationPromotionResult{Blocks: 12}, nil)

	response, err := s.batchConsolidator.Execute(s.env.BackgroundContext(), request)
	require.NoError(err)
	require.Equal(&BatchConsolidatorResponse{
		StartHeight:        request.StartHeight,
		EndHeight:          gateHeight,
		ScannedBlocks:      12,
		ConsolidatedBlocks: 12,
		PromotedBlocks:     12,
	}, response)
}

func (s *BatchConsolidatorTestSuite) TestPromoteFinalizedExecuteAllowsMissingPromotionGate() {
	require := testutil.Require(s.T())
	safeLag := uint64(10)
	s.batchConsolidator.config.AWS.Storage.Consolidation.Mode = config.ConsolidationModePromoteFinalized
	s.batchConsolidator.config.AWS.Storage.Consolidation.PromotionGateHeight = nil
	s.batchConsolidator.config.AWS.Storage.Consolidation.SafePromotionLag = &safeLag
	request := &BatchConsolidatorRequest{
		Tag:         2,
		StartHeight: 1_000,
		EndHeight:   1_200,
		MaxBlocks:   25,
	}
	s.metaStorage.EXPECT().
		GetLatestBlock(gomock.Any(), request.Tag).
		Return(&api.BlockMetadata{Tag: request.Tag, Height: 1_080}, nil)
	s.metaStorage.EXPECT().
		PromoteBlockConsolidationShadows(gomock.Any(), request.Tag, request.StartHeight, uint64(1_071), request.MaxBlocks, config.DefaultSingleBlockObjectRetention).
		Return(&metastorage.ConsolidationPromotionResult{Blocks: 12}, nil)

	response, err := s.batchConsolidator.Execute(s.env.BackgroundContext(), request)
	require.NoError(err)
	require.Equal(&BatchConsolidatorResponse{
		StartHeight:        request.StartHeight,
		EndHeight:          1_071,
		ScannedBlocks:      12,
		ConsolidatedBlocks: 12,
		PromotedBlocks:     12,
	}, response)
}

func (s *BatchConsolidatorTestSuite) TestPromoteFinalizedPlanCapsAtGateAndSafeHeight() {
	require := testutil.Require(s.T())
	gateHeight := uint64(1_050)
	safeLag := uint64(10)
	s.batchConsolidator.config.AWS.Storage.Consolidation.Mode = config.ConsolidationModePromoteFinalized
	s.batchConsolidator.config.AWS.Storage.Consolidation.PromotionGateHeight = &gateHeight
	s.batchConsolidator.config.AWS.Storage.Consolidation.SafePromotionLag = &safeLag
	request := &BatchConsolidatorPlanRequest{
		Tag:         2,
		StartHeight: 1_000,
		EndHeight:   1_200,
	}
	s.metaStorage.EXPECT().
		GetLatestBlock(gomock.Any(), request.Tag).
		Return(&api.BlockMetadata{Tag: request.Tag, Height: 1_080}, nil)

	response, err := s.batchConsolidator.GetPromotionPlan(s.env.BackgroundContext(), request)
	require.NoError(err)
	require.Equal(&BatchConsolidatorPlanResponse{
		StartHeight:         request.StartHeight,
		EndHeight:           gateHeight,
		LatestHeight:        1_080,
		SafePromotionHeight: 1_070,
		PromotionGateHeight: gateHeight,
	}, response)
}

func (s *BatchConsolidatorTestSuite) TestPromoteFinalizedPlanAllowsMissingPromotionGate() {
	require := testutil.Require(s.T())
	safeLag := uint64(10)
	s.batchConsolidator.config.AWS.Storage.Consolidation.Mode = config.ConsolidationModePromoteFinalized
	s.batchConsolidator.config.AWS.Storage.Consolidation.PromotionGateHeight = nil
	s.batchConsolidator.config.AWS.Storage.Consolidation.SafePromotionLag = &safeLag
	request := &BatchConsolidatorPlanRequest{
		Tag:         2,
		StartHeight: 1_000,
		EndHeight:   1_200,
	}
	s.metaStorage.EXPECT().
		GetLatestBlock(gomock.Any(), request.Tag).
		Return(&api.BlockMetadata{Tag: request.Tag, Height: 1_080}, nil)

	response, err := s.batchConsolidator.GetPromotionPlan(s.env.BackgroundContext(), request)
	require.NoError(err)
	require.Equal(&BatchConsolidatorPlanResponse{
		StartHeight:         request.StartHeight,
		EndHeight:           1_071,
		LatestHeight:        1_080,
		SafePromotionHeight: 1_070,
		PromotionGateHeight: request.EndHeight,
	}, response)
}

func (s *BatchConsolidatorTestSuite) TestPromoteFinalizedPlanNoOpsWhenLatestBelowSafeLag() {
	require := testutil.Require(s.T())
	gateHeight := uint64(1_050)
	safeLag := uint64(100)
	s.batchConsolidator.config.AWS.Storage.Consolidation.Mode = config.ConsolidationModePromoteFinalized
	s.batchConsolidator.config.AWS.Storage.Consolidation.PromotionGateHeight = &gateHeight
	s.batchConsolidator.config.AWS.Storage.Consolidation.SafePromotionLag = &safeLag
	request := &BatchConsolidatorPlanRequest{
		Tag:         2,
		StartHeight: 1_000,
		EndHeight:   1_200,
	}
	s.metaStorage.EXPECT().
		GetLatestBlock(gomock.Any(), request.Tag).
		Return(&api.BlockMetadata{Tag: request.Tag, Height: 50}, nil)

	response, err := s.batchConsolidator.GetPromotionPlan(s.env.BackgroundContext(), request)
	require.NoError(err)
	require.Equal(&BatchConsolidatorPlanResponse{
		StartHeight:         request.StartHeight,
		EndHeight:           request.StartHeight,
		LatestHeight:        50,
		SafePromotionHeight: 0,
		PromotionGateHeight: gateHeight,
	}, response)
}

type consolidatedPayloadsMatcher struct {
	blocks        []*api.Block
	metadataIDs   []int64
	capturedPaths *[]string
}

func consolidatedPayloadsMatch(blocks []*api.Block, metadataIDs []int64) gomock.Matcher {
	return consolidatedPayloadsMatchAndCapturePaths(blocks, metadataIDs, nil)
}

func consolidatedPayloadsMatchAndCapturePaths(blocks []*api.Block, metadataIDs []int64, capturedPaths *[]string) gomock.Matcher {
	return &consolidatedPayloadsMatcher{
		blocks:        blocks,
		metadataIDs:   metadataIDs,
		capturedPaths: capturedPaths,
	}
}

func (m *consolidatedPayloadsMatcher) Matches(x any) bool {
	payloads, ok := x.([]blobstorage.ConsolidatedBlockPayload)
	if !ok || len(payloads) != len(m.blocks) || len(payloads) != len(m.metadataIDs) {
		return false
	}
	paths := make([]string, 0, len(payloads))
	for i, payload := range payloads {
		if payload.MetadataID != m.metadataIDs[i] {
			return false
		}
		if !proto.Equal(payload.Metadata, m.blocks[i].GetMetadata()) {
			return false
		}
		if m.capturedPaths != nil {
			fileSource, ok := payload.RawBlockPayload.(blobstorage.FilePayloadSource)
			if !ok {
				return false
			}
			if _, err := os.Stat(fileSource.Path()); err != nil {
				return false
			}
			paths = append(paths, fileSource.Path())
		}
		reader, err := payload.RawBlockPayload.Open(context.Background())
		if err != nil {
			return false
		}
		raw, err := io.ReadAll(reader)
		_ = reader.Close()
		if err != nil || uint64(len(raw)) != payload.UncompressedLength {
			return false
		}
		var block api.Block
		if err := proto.Unmarshal(raw, &block); err != nil {
			return false
		}
		if !proto.Equal(&block, storageutils.CloneBlockWithoutStoragePlacement(m.blocks[i])) {
			return false
		}
	}
	if m.capturedPaths != nil {
		*m.capturedPaths = paths
	}
	return true
}

func (m *consolidatedPayloadsMatcher) String() string {
	return fmt.Sprintf("matches %d consolidated block payloads", len(m.blocks))
}

func makeConsolidatorFixture(count int, startHeight uint64) ([]*metastorage.BlockMetadataRecord, []*api.Block) {
	records := make([]*metastorage.BlockMetadataRecord, count)
	blocks := make([]*api.Block, count)
	for i := 0; i < count; i++ {
		height := startHeight + uint64(i)
		metadata := &api.BlockMetadata{
			Tag:           2,
			Height:        height,
			Hash:          fmt.Sprintf("hash-%d", height),
			ParentHash:    fmt.Sprintf("hash-%d", height-1),
			ParentHeight:  height - 1,
			ObjectKeyMain: fmt.Sprintf("single-block/%d.gzip", height),
		}
		records[i] = &metastorage.BlockMetadataRecord{
			ID:       int64(10 + i),
			Metadata: metadata,
		}
		blocks[i] = &api.Block{
			Blockchain: common.Blockchain_BLOCKCHAIN_ETHEREUM,
			Network:    common.Network_NETWORK_ETHEREUM_MAINNET,
			Metadata:   proto.Clone(metadata).(*api.BlockMetadata),
		}
	}
	return records, blocks
}

func makeCanonicalWindow(count int, startHeight uint64) []*api.BlockMetadata {
	records, _ := makeConsolidatorFixture(count, startHeight)
	blocks := make([]*api.BlockMetadata, len(records))
	for i, record := range records {
		blocks[i] = proto.Clone(record.Metadata).(*api.BlockMetadata)
	}
	return blocks
}

func makeConsolidatorPlacements(records []*metastorage.BlockMetadataRecord) []blobstorage.BlockPlacement {
	placements := make([]blobstorage.BlockPlacement, len(records))
	for i, record := range records {
		placements[i] = blobstorage.BlockPlacement{
			MetadataID:         record.ID,
			Height:             record.Metadata.GetHeight(),
			Hash:               record.Metadata.GetHash(),
			ObjectFormat:       api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_CSCB_BATCH,
			ByteOffset:         uint64(i * 100),
			ByteLength:         100,
			UncompressedLength: 100,
		}
	}
	return placements
}
