package activity

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"time"

	sdkactivity "go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/workflow"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"golang.org/x/xerrors"
	"google.golang.org/protobuf/proto"

	"github.com/coinbase/chainstorage/internal/cadence"
	"github.com/coinbase/chainstorage/internal/config"
	chains3 "github.com/coinbase/chainstorage/internal/s3"
	"github.com/coinbase/chainstorage/internal/storage/blobstorage"
	"github.com/coinbase/chainstorage/internal/storage/cscbrepair"
	"github.com/coinbase/chainstorage/internal/storage/metastorage"
	metapostgres "github.com/coinbase/chainstorage/internal/storage/metastorage/postgres"
	"github.com/coinbase/chainstorage/internal/storage/retirement"
	storageutils "github.com/coinbase/chainstorage/internal/storage/utils"
	"github.com/coinbase/chainstorage/internal/utils/fxparams"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

type (
	BatchConsolidator struct {
		baseActivity
		statsActivity       baseActivity
		latestBlockActivity baseActivity
		planActivity        baseActivity
		cursorActivity      baseActivity
		candidateActivity   baseActivity
		config              *config.Config
		metaStorage         metastorage.MetaStorage
		blobStorage         blobstorage.BlobStorage
		s3Client            chains3.Client
		repairerMu          sync.Mutex
		repairer            cscbrepair.Repairer
	}

	BatchConsolidatorParams struct {
		fx.In
		fxparams.Params
		Runtime     cadence.Runtime
		MetaStorage metastorage.MetaStorage
		BlobStorage blobstorage.BlobStorage
		S3Client    chains3.Client `optional:"true"`
	}

	BatchConsolidatorRequest struct {
		Mode               config.ConsolidationMode `validate:"omitempty,oneof=shadow_dual_write auto_consolidate historical_backfill repair_existing_cscb"`
		Tag                uint32
		StartHeight        uint64
		EndHeight          uint64 `validate:"gtfield=StartHeight"`
		MaxBlocks          uint64 `validate:"required"`
		RepairExecutionKey string
		RepairObjectKey    string
	}

	BatchConsolidatorResponse struct {
		StartHeight        uint64
		EndHeight          uint64
		ScannedBlocks      uint64
		ConsolidatedBlocks uint64
		PromotedBlocks     uint64
		ObjectKey          string
		OldObjectKey       string
		RepairedObjects    uint64
	}

	BatchConsolidatorStatsRequest struct {
		Mode        config.ConsolidationMode `validate:"omitempty,oneof=shadow_dual_write auto_consolidate historical_backfill"`
		Tag         uint32
		StartHeight uint64
		EndHeight   uint64 `validate:"gtfield=StartHeight"`
	}

	BatchConsolidatorStatsResponse struct {
		StartHeight   uint64
		EndHeight     uint64
		ShadowObjects uint64
		ShadowBlocks  uint64
	}

	BatchConsolidatorLatestBlockRequest struct {
		Tag uint32
	}

	BatchConsolidatorLatestBlockResponse struct {
		Tag    uint32
		Height uint64
	}

	BatchConsolidatorPlanRequest struct {
		Tag         uint32
		StartHeight uint64
		EndHeight   uint64 `validate:"gtfield=StartHeight"`
	}

	BatchConsolidatorPlanResponse struct {
		StartHeight         uint64
		EndHeight           uint64
		LatestHeight        uint64
		SafePromotionHeight uint64
		PromotionGateHeight uint64
	}

	BatchConsolidatorCursorRequest struct {
		Tag    uint32
		Height uint64
	}

	BatchConsolidatorCursorResponse struct {
		Tag    uint32
		Height uint64
	}

	BatchConsolidatorRepairCandidatesRequest struct {
		Tag         uint32
		StartHeight uint64
		EndHeight   uint64 `validate:"gtfield=StartHeight"`
		Limit       int    `validate:"required,gt=0,lte=20"`
	}

	BatchConsolidatorRepairCandidatesResponse struct {
		ObjectKeys []string
	}
)

func NewBatchConsolidator(params BatchConsolidatorParams) *BatchConsolidator {
	a := &BatchConsolidator{
		baseActivity:        newBaseActivity(ActivityBatchConsolidator, params.Runtime),
		statsActivity:       newBaseActivity(ActivityBatchConsolidatorStats, params.Runtime),
		latestBlockActivity: newBaseActivity(ActivityBatchConsolidatorLatestBlock, params.Runtime),
		planActivity:        newBaseActivity(ActivityBatchConsolidatorPlan, params.Runtime),
		cursorActivity:      newBaseActivity(ActivityBatchConsolidatorCursor, params.Runtime),
		candidateActivity:   newBaseActivity(ActivityBatchConsolidatorRepairCandidates, params.Runtime),
		config:              params.Config,
		metaStorage:         params.MetaStorage,
		blobStorage:         params.BlobStorage,
		s3Client:            params.S3Client,
	}
	a.register(a.execute)
	a.statsActivity.register(a.executeStats)
	a.latestBlockActivity.register(a.executeLatestBlock)
	a.planActivity.register(a.executePlan)
	a.cursorActivity.register(a.executeCursor)
	a.candidateActivity.register(a.executeRepairCandidates)
	return a
}

func (a *BatchConsolidator) Execute(ctx workflow.Context, request *BatchConsolidatorRequest) (*BatchConsolidatorResponse, error) {
	var response BatchConsolidatorResponse
	err := a.executeActivity(ctx, request, &response)
	return &response, err
}

func (a *BatchConsolidator) GetShadowStats(ctx workflow.Context, request *BatchConsolidatorStatsRequest) (*BatchConsolidatorStatsResponse, error) {
	var response BatchConsolidatorStatsResponse
	err := a.statsActivity.executeActivity(ctx, request, &response)
	return &response, err
}

func (a *BatchConsolidator) GetLatestBlock(ctx workflow.Context, request *BatchConsolidatorLatestBlockRequest) (*BatchConsolidatorLatestBlockResponse, error) {
	var response BatchConsolidatorLatestBlockResponse
	err := a.latestBlockActivity.executeActivity(ctx, request, &response)
	if err != nil {
		return nil, err
	}
	return &response, nil
}

func (a *BatchConsolidator) GetPromotionPlan(ctx workflow.Context, request *BatchConsolidatorPlanRequest) (*BatchConsolidatorPlanResponse, error) {
	var response BatchConsolidatorPlanResponse
	err := a.planActivity.executeActivity(ctx, request, &response)
	return &response, err
}

func (a *BatchConsolidator) UpdateAutoConsolidateCursor(ctx workflow.Context, request *BatchConsolidatorCursorRequest) (*BatchConsolidatorCursorResponse, error) {
	var response BatchConsolidatorCursorResponse
	err := a.cursorActivity.executeActivity(ctx, request, &response)
	return &response, err
}

func (a *BatchConsolidator) GetRepairCandidates(ctx workflow.Context, request *BatchConsolidatorRepairCandidatesRequest) (*BatchConsolidatorRepairCandidatesResponse, error) {
	var response BatchConsolidatorRepairCandidatesResponse
	err := a.candidateActivity.executeActivity(ctx, request, &response)
	return &response, err
}

func (a *BatchConsolidator) executeStats(ctx context.Context, request *BatchConsolidatorStatsRequest) (*BatchConsolidatorStatsResponse, error) {
	if err := a.statsActivity.validateRequest(request); err != nil {
		return nil, err
	}
	if err := a.validateShadowStatsMode(request.Mode); err != nil {
		return nil, err
	}
	logger := a.statsActivity.getLogger(ctx).With(zap.Reflect("request", request))
	statsStart := time.Now()
	logger.Info("started consolidation shadow stats")
	sdkactivity.RecordHeartbeat(ctx, "batch_consolidator.stats.started", request.Tag, request.StartHeight, request.EndHeight)
	stats, err := a.getConsolidationShadowStats(ctx, request.Tag, request.StartHeight, request.EndHeight)
	if err != nil {
		logger.Error("failed consolidation shadow stats", zap.Duration("duration", time.Since(statsStart)), zap.Error(err))
		return nil, err
	}
	statsDuration := time.Since(statsStart)
	logger.Info(
		"finished consolidation shadow stats",
		zap.Uint64("shadow_objects", stats.Objects),
		zap.Uint64("shadow_blocks", stats.Blocks),
		zap.Duration("duration", statsDuration),
	)
	sdkactivity.RecordHeartbeat(ctx, "batch_consolidator.stats.completed", request.Tag, request.StartHeight, request.EndHeight, stats.Objects, stats.Blocks)
	return &BatchConsolidatorStatsResponse{
		StartHeight:   request.StartHeight,
		EndHeight:     request.EndHeight,
		ShadowObjects: stats.Objects,
		ShadowBlocks:  stats.Blocks,
	}, nil
}

func (a *BatchConsolidator) executeLatestBlock(ctx context.Context, request *BatchConsolidatorLatestBlockRequest) (*BatchConsolidatorLatestBlockResponse, error) {
	if err := a.latestBlockActivity.validateRequest(request); err != nil {
		return nil, err
	}
	latestBlock, err := a.metaStorage.GetLatestBlock(ctx, request.Tag)
	if err != nil {
		return nil, xerrors.Errorf("failed to get latest block for auto_consolidate safety check: %w", err)
	}
	if latestBlock == nil {
		return nil, xerrors.New("latest block not found for auto_consolidate safety check")
	}
	return &BatchConsolidatorLatestBlockResponse{
		Tag:    request.Tag,
		Height: latestBlock.GetHeight(),
	}, nil
}

func (a *BatchConsolidator) executePlan(ctx context.Context, request *BatchConsolidatorPlanRequest) (*BatchConsolidatorPlanResponse, error) {
	if err := a.planActivity.validateRequest(request); err != nil {
		return nil, err
	}
	return a.planPromoteFinalized(ctx, request.Tag, request.StartHeight, request.EndHeight)
}

func (a *BatchConsolidator) executeCursor(ctx context.Context, request *BatchConsolidatorCursorRequest) (*BatchConsolidatorCursorResponse, error) {
	if err := a.cursorActivity.validateRequest(request); err != nil {
		return nil, err
	}
	if err := a.metaStorage.SetBlockConsolidationCursor(ctx, metastorage.BatchConsolidatorAutoConsolidateCursor, request.Tag, request.Height); err != nil {
		return nil, xerrors.Errorf("failed to update auto_consolidate cursor: %w", err)
	}
	return &BatchConsolidatorCursorResponse{
		Tag:    request.Tag,
		Height: request.Height,
	}, nil
}

func (a *BatchConsolidator) executeRepairCandidates(
	ctx context.Context,
	request *BatchConsolidatorRepairCandidatesRequest,
) (*BatchConsolidatorRepairCandidatesResponse, error) {
	if err := a.candidateActivity.validateRequest(request); err != nil {
		return nil, err
	}
	if err := a.validateConsolidationEnabled(); err != nil {
		return nil, err
	}
	repairer, err := a.getCSCBRepairer(ctx)
	if err != nil {
		return nil, err
	}
	objectKeys, err := repairer.ListCandidates(
		ctx,
		request.Tag,
		request.StartHeight,
		request.EndHeight,
		request.Limit,
	)
	if err != nil {
		return nil, xerrors.Errorf("failed to list CSCB repair candidates: %w", err)
	}
	return &BatchConsolidatorRepairCandidatesResponse{ObjectKeys: objectKeys}, nil
}

func (a *BatchConsolidator) execute(ctx context.Context, request *BatchConsolidatorRequest) (*BatchConsolidatorResponse, error) {
	if err := a.validateRequest(request); err != nil {
		return nil, err
	}
	mode := request.Mode
	if mode == "" {
		mode = a.config.AWS.Storage.Consolidation.Mode
	}
	if mode.IsRepairExistingCSCB() {
		return a.executeRepairExistingCSCB(ctx, request)
	}
	if mode.IsShadowWrite() {
		return a.executeShadowDualWrite(ctx, request)
	}
	switch mode {
	case config.ConsolidationModePromoteFinalized:
		return a.executePromoteFinalized(ctx, request)
	default:
		if err := a.validateConsolidationEnabled(); err != nil {
			return nil, err
		}
		return nil, xerrors.Errorf(
			"batch consolidator requires consolidation mode %q, %q, or %q, got %q",
			config.ConsolidationModeShadowDualWrite,
			config.ConsolidationModeAutoConsolidate,
			config.ConsolidationModePromoteFinalized,
			mode,
		)
	}
}

func (a *BatchConsolidator) executeRepairExistingCSCB(
	ctx context.Context,
	request *BatchConsolidatorRequest,
) (*BatchConsolidatorResponse, error) {
	if err := a.validateConsolidationEnabled(); err != nil {
		return nil, err
	}
	repairer, err := a.getCSCBRepairer(ctx)
	if err != nil {
		return nil, err
	}
	progress := func(stage string, completed int, total int, height uint64) {
		sdkactivity.RecordHeartbeat(ctx, "batch_consolidator.repair."+stage, completed, total, height)
	}
	var manifest *cscbrepair.Manifest
	if request.RepairObjectKey == "" {
		manifest, err = repairer.PrepareNext(
			ctx,
			request.RepairExecutionKey,
			request.Tag,
			request.StartHeight,
			request.EndHeight,
			request.MaxBlocks,
			progress,
		)
	} else {
		manifest, err = repairer.PrepareObject(
			ctx,
			request.RepairExecutionKey,
			request.Tag,
			request.StartHeight,
			request.EndHeight,
			request.MaxBlocks,
			request.RepairObjectKey,
			progress,
		)
	}
	if err != nil {
		return nil, xerrors.Errorf("failed to prepare CSCB repair: %w", err)
	}
	if manifest == nil {
		return &BatchConsolidatorResponse{
			StartHeight: request.StartHeight,
			EndHeight:   request.EndHeight,
		}, nil
	}

	logger := a.getLogger(ctx).With(
		zap.Int64("repair_id", manifest.ID),
		zap.String("repair_state", string(manifest.State)),
		zap.String("old_object_key", manifest.OldConsolidatedObjectKey),
		zap.Uint64("repair_start_height", manifest.StartHeight),
		zap.Uint64("repair_end_height", manifest.EndHeight),
		zap.Uint64("repair_blocks", manifest.TotalBlockCount),
		zap.Uint64("repair_canonical_blocks", manifest.CanonicalBlockCount),
	)
	logger.Info("resuming CSCB repair")

	if manifest.State == cscbrepair.StatePrepared && manifest.CanonicalBlockCount == 0 {
		manifest, err = repairer.Restore(ctx, manifest.ID, progress)
		if err != nil {
			return nil, xerrors.Errorf("failed to restore CSCB repair to single-block metadata: %w", err)
		}
	}
	if (manifest.State == cscbrepair.StatePrepared || manifest.State == cscbrepair.StateRestored) &&
		manifest.CanonicalBlockCount > 0 {
		payloads, cleanup, err := a.buildPinnedRepairPayloads(ctx, repairer, manifest, progress)
		if err != nil {
			return nil, err
		}
		defer cleanup()
		uploadCtx := blobstorage.WithConsolidatedUploadProgress(ctx, func(stage string, details ...any) {
			heartbeatDetails := append([]any{"batch_consolidator.repair." + stage}, details...)
			sdkactivity.RecordHeartbeat(ctx, heartbeatDetails...)
		})
		objectKey, uploadedPlacements, err := a.blobStorage.UploadConsolidated(uploadCtx, payloads)
		if err != nil {
			return nil, xerrors.Errorf("failed to upload rebuilt normalized CSCB: %w", err)
		}
		placements := make([]cscbrepair.RebuiltPlacement, len(uploadedPlacements))
		for i, placement := range uploadedPlacements {
			placements[i] = cscbrepair.RebuiltPlacement{
				BlockMetadataID:    placement.MetadataID,
				Height:             placement.Height,
				Hash:               placement.Hash,
				ObjectFormat:       placement.ObjectFormat,
				StorageGeneration:  placement.StorageGeneration,
				ByteOffset:         placement.ByteOffset,
				ByteLength:         placement.ByteLength,
				UncompressedLength: placement.UncompressedLength,
			}
		}
		manifest, err = repairer.VerifyAndPromote(
			ctx,
			manifest.ID,
			objectKey,
			placements,
			a.config.AWS.Storage.Consolidation.SingleBlockObjectRetention,
			progress,
		)
		if err != nil {
			return nil, xerrors.Errorf("failed to verify and atomically promote rebuilt CSCB: %w", err)
		}
	}
	if manifest.State == cscbrepair.StateRestored || manifest.State == cscbrepair.StateVerified {
		manifest, err = repairer.Complete(ctx, manifest.ID, progress)
		if err != nil {
			return nil, xerrors.Errorf("failed to complete CSCB repair: %w", err)
		}
	}
	if manifest.State != cscbrepair.StateCompleted {
		return nil, xerrors.Errorf("CSCB repair made no terminal progress: id=%d state=%s", manifest.ID, manifest.State)
	}

	logger.Info(
		"processed CSCB repair",
		zap.String("final_state", string(manifest.State)),
		zap.String("new_object_key", manifest.NewConsolidatedObjectKey),
		zap.String("old_object_outcome", manifest.Outcome),
	)
	consolidatedBlocks := manifest.TotalBlockCount
	promotedBlocks := manifest.TotalBlockCount
	objectKey := manifest.NewConsolidatedObjectKey
	if objectKey == "" {
		consolidatedBlocks = 0
		promotedBlocks = 0
	}
	if manifest.Outcome == cscbrepair.OutcomeAlreadyCleanStorageNeutral {
		consolidatedBlocks = 0
		promotedBlocks = 0
		objectKey = manifest.OldConsolidatedObjectKey
	}
	return &BatchConsolidatorResponse{
		StartHeight:        manifest.StartHeight,
		EndHeight:          manifest.EndHeight,
		ScannedBlocks:      manifest.TotalBlockCount,
		ConsolidatedBlocks: consolidatedBlocks,
		PromotedBlocks:     promotedBlocks,
		ObjectKey:          objectKey,
		OldObjectKey:       manifest.OldConsolidatedObjectKey,
		RepairedObjects:    1,
	}, nil
}

func (a *BatchConsolidator) getCSCBRepairer(ctx context.Context) (cscbrepair.Repairer, error) {
	a.repairerMu.Lock()
	defer a.repairerMu.Unlock()
	if a.repairer != nil {
		return a.repairer, nil
	}
	if a.config.StorageType.MetaStorageType != config.MetaStorageType_POSTGRES || a.config.AWS.Postgres == nil {
		return nil, xerrors.New("repair_existing_cscb requires Postgres meta storage")
	}
	if a.config.StorageType.BlobStorageType != config.BlobStorageType_UNSPECIFIED &&
		a.config.StorageType.BlobStorageType != config.BlobStorageType_S3 {
		return nil, xerrors.New("repair_existing_cscb requires S3 blob storage")
	}
	if a.s3Client == nil {
		return nil, xerrors.New("repair_existing_cscb requires an S3 client")
	}
	storageGeneration, err := a.config.WriteBlockStorageGeneration()
	if err != nil {
		return nil, xerrors.Errorf("repair_existing_cscb failed to resolve write block storage generation: %w", err)
	}
	if storageGeneration != "" {
		return nil, xerrors.New("repair_existing_cscb is only supported for the legacy block storage generation")
	}
	pool, err := metapostgres.GetConnectionPool(ctx, a.config.AWS.Postgres)
	if err != nil {
		return nil, xerrors.Errorf("failed to get CSCB repair Postgres pool: %w", err)
	}
	db := pool.DB()
	if db == nil {
		return nil, xerrors.New("CSCB repair Postgres pool returned a nil database")
	}
	a.repairer = cscbrepair.NewRepairer(
		cscbrepair.NewPostgresRepository(db),
		retirement.NewS3ObjectStore(a.s3Client),
		a.config.AWS.Bucket,
	)
	return a.repairer, nil
}

func (a *BatchConsolidator) executeShadowDualWrite(ctx context.Context, request *BatchConsolidatorRequest) (*BatchConsolidatorResponse, error) {
	return a.executeShadowDualWriteWithExpected(ctx, request, nil)
}

func (a *BatchConsolidator) executeShadowDualWriteWithExpected(
	ctx context.Context,
	request *BatchConsolidatorRequest,
	expectedRecords map[int64]cscbrepair.Block,
) (*BatchConsolidatorResponse, error) {
	if err := a.validateShadowWriteMode(request.Mode); err != nil {
		return nil, err
	}
	sdkactivity.RecordHeartbeat(ctx, "batch_consolidator.started")

	logger := a.getLogger(ctx).With(zap.Reflect("request", request))
	totalStart := time.Now()
	mode := request.Mode
	if mode == "" {
		mode = a.config.AWS.Storage.Consolidation.Mode
	}
	if mode.IsAutoConsolidate() && !isFullAutoConsolidationRequest(request) {
		return nil, xerrors.Errorf(
			"auto_consolidate requires exactly one full consolidation window: start_height=%d end_height=%d max_blocks=%d",
			request.StartHeight,
			request.EndHeight,
			request.MaxBlocks,
		)
	}
	if mode.IsAutoConsolidate() {
		if err := a.validateFullAutoConsolidationWindow(ctx, request); err != nil {
			return nil, err
		}
	}
	var prePromotedBlocks uint64
	if expectedRecords == nil {
		var err error
		prePromotedBlocks, err = a.promoteConsolidatedBlocks(ctx, mode, request)
		if err != nil {
			return nil, err
		}
	}
	scanStart := time.Now()
	records, err := a.metaStorage.GetBlocksMissingConsolidationShadow(ctx, request.Tag, request.StartHeight, request.EndHeight, request.MaxBlocks)
	if err != nil {
		return nil, xerrors.Errorf("failed to scan missing consolidation shadows: %w", err)
	}
	scanDuration := time.Since(scanStart)
	logger.Info("scanned missing consolidation shadows", zap.Int("records", len(records)), zap.Duration("duration", scanDuration))
	if len(records) == 0 {
		if expectedRecords != nil {
			postPromotedBlocks, err := a.promoteConsolidatedBlocks(ctx, mode, request)
			if err != nil {
				return nil, err
			}
			prePromotedBlocks += postPromotedBlocks
		}
		return &BatchConsolidatorResponse{
			StartHeight:    request.StartHeight,
			EndHeight:      request.EndHeight,
			PromotedBlocks: prePromotedBlocks,
		}, nil
	}
	if expectedRecords != nil {
		if err := validateExactRepairRecords(records, expectedRecords); err != nil {
			return nil, err
		}
	}

	buildStart := time.Now()
	payloads, recordsByID, cleanup, err := a.buildPayloads(ctx, records)
	if err != nil {
		return nil, err
	}
	defer cleanup()
	buildDuration := time.Since(buildStart)
	rawBytes := consolidatedPayloadBytes(payloads)
	logger.Info("built consolidated payloads", zap.Int("blocks", len(payloads)), zap.Uint64("raw_bytes", rawBytes), zap.Duration("duration", buildDuration))
	sdkactivity.RecordHeartbeat(ctx, "batch_consolidator.payloads_built", len(records))

	uploadCtx := blobstorage.WithConsolidatedUploadProgress(ctx, func(stage string, details ...any) {
		heartbeatDetails := append([]any{"batch_consolidator." + stage}, details...)
		sdkactivity.RecordHeartbeat(ctx, heartbeatDetails...)
	})
	uploadStart := time.Now()
	objectKey, placements, err := a.blobStorage.UploadConsolidated(uploadCtx, payloads)
	if err != nil {
		return nil, xerrors.Errorf("failed to upload consolidated block object: %w", err)
	}
	uploadDuration := time.Since(uploadStart)
	logger.Info("uploaded consolidated block object", zap.String("object_key", objectKey), zap.Int("placements", len(placements)), zap.Duration("duration", uploadDuration))
	sdkactivity.RecordHeartbeat(ctx, "batch_consolidator.object_uploaded", objectKey, len(placements))
	shadowPlacements, err := makeShadowPlacements(recordsByID, objectKey, placements)
	if err != nil {
		return nil, err
	}
	persistStart := time.Now()
	if err := a.metaStorage.PersistBlockConsolidationShadows(ctx, shadowPlacements); err != nil {
		return nil, xerrors.Errorf("failed to persist consolidation shadow placements: %w", err)
	}
	persistDuration := time.Since(persistStart)
	logger.Info("persisted consolidation shadow placements", zap.Int("placements", len(shadowPlacements)), zap.Duration("duration", persistDuration))
	sdkactivity.RecordHeartbeat(ctx, "batch_consolidator.shadows_persisted", objectKey, len(shadowPlacements))
	postPromotedBlocks, err := a.promoteConsolidatedBlocks(ctx, mode, request)
	if err != nil {
		return nil, err
	}
	promotedBlocks := prePromotedBlocks + postPromotedBlocks

	response := &BatchConsolidatorResponse{
		StartHeight:        request.StartHeight,
		EndHeight:          request.EndHeight,
		ScannedBlocks:      uint64(len(records)),
		ConsolidatedBlocks: uint64(len(shadowPlacements)),
		PromotedBlocks:     promotedBlocks,
		ObjectKey:          objectKey,
	}
	logger.Info(
		"consolidated shadow blocks",
		zap.Int("scanned_blocks", len(records)),
		zap.Int("consolidated_blocks", len(shadowPlacements)),
		zap.Uint64("promoted_blocks", promotedBlocks),
		zap.String("object_key", objectKey),
		zap.Uint64("raw_bytes", rawBytes),
		zap.Duration("scan_duration", scanDuration),
		zap.Duration("build_duration", buildDuration),
		zap.Duration("upload_duration", uploadDuration),
		zap.Duration("persist_duration", persistDuration),
		zap.Duration("total_duration", time.Since(totalStart)),
	)
	return response, nil
}

func validateExactRepairRecords(
	records []*metastorage.BlockMetadataRecord,
	expected map[int64]cscbrepair.Block,
) error {
	if len(records) != len(expected) {
		return xerrors.Errorf("CSCB repair rebuild row count mismatch before upload: expected=%d actual=%d", len(expected), len(records))
	}
	seen := make(map[int64]struct{}, len(records))
	for _, record := range records {
		if record == nil || record.Metadata == nil {
			return xerrors.New("CSCB repair rebuild returned missing block metadata record")
		}
		block, ok := expected[record.ID]
		if !ok {
			return xerrors.Errorf("CSCB repair rebuild returned unexpected metadata_id=%d", record.ID)
		}
		if _, ok := seen[record.ID]; ok {
			return xerrors.Errorf("CSCB repair rebuild returned duplicate metadata_id=%d", record.ID)
		}
		seen[record.ID] = struct{}{}
		metadata := record.Metadata
		if metadata.GetTag() != block.Tag || metadata.GetHeight() != block.Height || metadata.GetHash() != block.Hash {
			return xerrors.Errorf(
				"CSCB repair rebuild identity changed for metadata_id=%d: expected=(%d,%d,%q) actual=(%d,%d,%q)",
				record.ID,
				block.Tag,
				block.Height,
				block.Hash,
				metadata.GetTag(),
				metadata.GetHeight(),
				metadata.GetHash(),
			)
		}
		if metadata.GetObjectKeyMain() != block.SingleBlockObjectKey ||
			metadata.GetObjectFormat() != api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_SINGLE_BLOCK ||
			metadata.GetByteOffset() != 0 || metadata.GetByteLength() != 0 || metadata.GetUncompressedLength() != 0 {
			return xerrors.Errorf("CSCB repair rebuild metadata is not restored to the pinned single-block placement for metadata_id=%d", record.ID)
		}
	}
	return nil
}

func (a *BatchConsolidator) executePromoteFinalized(ctx context.Context, request *BatchConsolidatorRequest) (*BatchConsolidatorResponse, error) {
	if err := a.validatePromoteFinalizedMode(); err != nil {
		return nil, err
	}
	sdkactivity.RecordHeartbeat(ctx, "batch_consolidator.promote_finalized.started")

	plan, err := a.planPromoteFinalized(ctx, request.Tag, request.StartHeight, request.EndHeight)
	if err != nil {
		return nil, err
	}
	if plan.EndHeight <= request.StartHeight {
		return &BatchConsolidatorResponse{
			StartHeight: request.StartHeight,
			EndHeight:   request.StartHeight,
		}, nil
	}
	result, err := a.metaStorage.PromoteBlockConsolidationShadows(
		ctx,
		request.Tag,
		request.StartHeight,
		plan.EndHeight,
		request.MaxBlocks,
		a.config.AWS.Storage.Consolidation.SingleBlockObjectRetention,
	)
	if err != nil {
		return nil, xerrors.Errorf("failed to promote consolidation shadows: %w", err)
	}
	promotedBlocks := uint64(0)
	if result != nil {
		promotedBlocks = result.Blocks
	}
	sdkactivity.RecordHeartbeat(ctx, "batch_consolidator.promote_finalized.promoted", promotedBlocks)
	a.getLogger(ctx).Info(
		"promoted finalized consolidation shadows",
		zap.Reflect("request", request),
		zap.Uint64("promoted_blocks", promotedBlocks),
	)
	return &BatchConsolidatorResponse{
		StartHeight:        request.StartHeight,
		EndHeight:          plan.EndHeight,
		ScannedBlocks:      promotedBlocks,
		ConsolidatedBlocks: promotedBlocks,
		PromotedBlocks:     promotedBlocks,
	}, nil
}

func (a *BatchConsolidator) promoteConsolidatedBlocks(ctx context.Context, mode config.ConsolidationMode, request *BatchConsolidatorRequest) (uint64, error) {
	if mode != config.ConsolidationModeAutoConsolidate && mode != config.ConsolidationModeHistoricalBackfill {
		return 0, nil
	}
	result, err := a.metaStorage.PromoteBlockConsolidationShadows(
		ctx,
		request.Tag,
		request.StartHeight,
		request.EndHeight,
		request.MaxBlocks,
		a.config.AWS.Storage.Consolidation.SingleBlockObjectRetention,
	)
	if err != nil {
		return 0, xerrors.Errorf("failed to promote consolidated block metadata: %w", err)
	}
	if result == nil {
		return 0, nil
	}
	if result.Blocks > 0 {
		sdkactivity.RecordHeartbeat(ctx, "batch_consolidator.metadata_promoted", result.Blocks)
	}
	return result.Blocks, nil
}

func (a *BatchConsolidator) validateFullAutoConsolidationWindow(ctx context.Context, request *BatchConsolidatorRequest) error {
	blocks, err := a.metaStorage.GetBlocksByHeightRange(ctx, request.Tag, request.StartHeight, request.EndHeight)
	if err != nil {
		return xerrors.Errorf("failed to verify auto_consolidate canonical window: %w", err)
	}
	if request.MaxBlocks == 0 || uint64(len(blocks)) != request.MaxBlocks {
		return xerrors.Errorf(
			"auto_consolidate requires a full canonical consolidation window: start_height=%d end_height=%d max_blocks=%d canonical_blocks=%d",
			request.StartHeight,
			request.EndHeight,
			request.MaxBlocks,
			len(blocks),
		)
	}
	for i, block := range blocks {
		if block == nil {
			return xerrors.Errorf(
				"auto_consolidate requires a full canonical consolidation window: start_height=%d end_height=%d max_blocks=%d nil_index=%d",
				request.StartHeight,
				request.EndHeight,
				request.MaxBlocks,
				i,
			)
		}
		expectedHeight := request.StartHeight + uint64(i)
		if expectedHeight < request.StartHeight || block.GetHeight() != expectedHeight {
			return xerrors.Errorf(
				"auto_consolidate requires a full canonical consolidation window: start_height=%d end_height=%d max_blocks=%d expected_height=%d actual_height=%d",
				request.StartHeight,
				request.EndHeight,
				request.MaxBlocks,
				expectedHeight,
				block.GetHeight(),
			)
		}
	}
	return nil
}

func (a *BatchConsolidator) planPromoteFinalized(ctx context.Context, tag uint32, startHeight uint64, endHeight uint64) (*BatchConsolidatorPlanResponse, error) {
	if err := a.validatePromoteFinalizedMode(); err != nil {
		return nil, err
	}
	latest, err := a.metaStorage.GetLatestBlock(ctx, tag)
	if err != nil {
		return nil, xerrors.Errorf("failed to get latest block for consolidation promotion: %w", err)
	}

	consolidation := a.config.AWS.Storage.Consolidation
	safeLag := *consolidation.SafePromotionLag
	gateHeight := endHeight
	if consolidation.PromotionGateHeight != nil {
		gateHeight = *consolidation.PromotionGateHeight
	}
	safeEnd, safeHeight, ok := promotionSafeEndHeight(latest.GetHeight(), safeLag)
	if !ok {
		return &BatchConsolidatorPlanResponse{
			StartHeight:         startHeight,
			EndHeight:           startHeight,
			LatestHeight:        latest.GetHeight(),
			SafePromotionHeight: 0,
			PromotionGateHeight: gateHeight,
		}, nil
	}

	effectiveEndHeight := endHeight
	if gateHeight < effectiveEndHeight {
		effectiveEndHeight = gateHeight
	}
	if safeEnd < effectiveEndHeight {
		effectiveEndHeight = safeEnd
	}
	if effectiveEndHeight < startHeight {
		effectiveEndHeight = startHeight
	}

	return &BatchConsolidatorPlanResponse{
		StartHeight:         startHeight,
		EndHeight:           effectiveEndHeight,
		LatestHeight:        latest.GetHeight(),
		SafePromotionHeight: safeHeight,
		PromotionGateHeight: gateHeight,
	}, nil
}

func (a *BatchConsolidator) getConsolidationShadowStats(
	ctx context.Context,
	tag uint32,
	startHeight uint64,
	endHeight uint64,
) (*metastorage.ConsolidationShadowStats, error) {
	stats, err := a.metaStorage.GetBlockConsolidationShadowStats(ctx, tag, startHeight, endHeight)
	if err != nil {
		return nil, xerrors.Errorf("failed to get consolidation shadow stats: %w", err)
	}
	if stats == nil {
		return &metastorage.ConsolidationShadowStats{}, nil
	}
	return stats, nil
}

func (a *BatchConsolidator) validateConsolidationEnabled() error {
	consolidation := a.config.AWS.Storage.Consolidation
	if !consolidation.Enabled {
		return xerrors.New("batch consolidator requires aws.storage.consolidation.enabled=true")
	}
	return nil
}

func (a *BatchConsolidator) validateShadowWriteMode(mode config.ConsolidationMode) error {
	if err := a.validateConsolidationEnabled(); err != nil {
		return err
	}
	if mode == "" {
		mode = a.config.AWS.Storage.Consolidation.Mode
	}
	if mode.IsShadowWrite() {
		return nil
	}
	return xerrors.Errorf(
		"batch consolidator requires consolidation mode %q, %q, or %q, got %q",
		config.ConsolidationModeShadowDualWrite,
		config.ConsolidationModeAutoConsolidate,
		config.ConsolidationModeHistoricalBackfill,
		mode,
	)
}

func (a *BatchConsolidator) validateShadowStatsMode(mode config.ConsolidationMode) error {
	return a.validateShadowWriteMode(mode)
}

func isFullAutoConsolidationRequest(request *BatchConsolidatorRequest) bool {
	if request == nil || request.MaxBlocks == 0 || request.EndHeight <= request.StartHeight {
		return false
	}
	return request.EndHeight-request.StartHeight == request.MaxBlocks
}

func (a *BatchConsolidator) validatePromoteFinalizedMode() error {
	if err := a.validateConsolidationEnabled(); err != nil {
		return err
	}
	consolidation := a.config.AWS.Storage.Consolidation
	if consolidation.Mode != config.ConsolidationModePromoteFinalized {
		return xerrors.Errorf("batch consolidator requires consolidation mode %q, got %q", config.ConsolidationModePromoteFinalized, consolidation.Mode)
	}
	if consolidation.SafePromotionLag == nil {
		return xerrors.New("batch consolidator promote_finalized requires safe_promotion_lag")
	}
	return nil
}

func promotionSafeEndHeight(latestHeight uint64, safePromotionLag uint64) (uint64, uint64, bool) {
	if latestHeight < safePromotionLag {
		return 0, 0, false
	}
	safeHeight := latestHeight - safePromotionLag
	if safeHeight == ^uint64(0) {
		return safeHeight, safeHeight, true
	}
	return safeHeight + 1, safeHeight, true
}

func (a *BatchConsolidator) buildPinnedRepairPayloads(
	ctx context.Context,
	repairer cscbrepair.Repairer,
	manifest *cscbrepair.Manifest,
	progress cscbrepair.Progress,
) ([]blobstorage.ConsolidatedBlockPayload, func(), error) {
	payloads := make([]blobstorage.ConsolidatedBlockPayload, 0, manifest.TotalBlockCount)
	tempFiles := make([]string, 0, manifest.TotalBlockCount)
	cleanup := func() {
		for _, path := range tempFiles {
			if path != "" {
				_ = os.Remove(path)
			}
		}
	}
	err := repairer.VisitPinnedPayloads(ctx, manifest.ID, progress, func(pinned cscbrepair.PinnedPayload) error {
		source, path, length, err := writeRawBlockPayloadTempFile(
			pinned.RawBlockPayload,
			a.config.AWS.Storage.Consolidation.LocalSpillDir,
		)
		if err != nil {
			return err
		}
		tempFiles = append(tempFiles, path)
		payloads = append(payloads, blobstorage.ConsolidatedBlockPayload{
			Metadata:           pinned.Metadata,
			MetadataID:         pinned.BlockMetadataID,
			RawBlockPayload:    source,
			UncompressedLength: length,
		})
		return nil
	})
	if err != nil {
		cleanup()
		return nil, nil, xerrors.Errorf("failed to build normalized CSCB from pinned versions: %w", err)
	}
	if uint64(len(payloads)) != manifest.TotalBlockCount {
		cleanup()
		return nil, nil, xerrors.Errorf(
			"CSCB repair pinned payload count mismatch: expected=%d actual=%d",
			manifest.TotalBlockCount,
			len(payloads),
		)
	}
	return payloads, cleanup, nil
}

func (a *BatchConsolidator) buildPayloads(
	ctx context.Context,
	records []*metastorage.BlockMetadataRecord,
) ([]blobstorage.ConsolidatedBlockPayload, map[int64]*api.BlockMetadata, func(), error) {
	payloads := make([]blobstorage.ConsolidatedBlockPayload, len(records))
	recordsByID := make(map[int64]*api.BlockMetadata, len(records))
	for i, record := range records {
		if record == nil || record.Metadata == nil {
			return nil, nil, nil, xerrors.New("missing block metadata record")
		}
		recordsByID[record.ID] = record.Metadata
		payloads[i].MetadataID = record.ID
		payloads[i].Metadata = record.Metadata
	}

	tempFiles := make([]string, len(records))
	cleanup := func() {
		for _, path := range tempFiles {
			if path != "" {
				_ = os.Remove(path)
			}
		}
	}

	parallelism := int(a.config.AWS.Storage.Consolidation.MaxInflightRawBlocks)
	if parallelism <= 0 {
		parallelism = 1
	}
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(parallelism)
	for i := range records {
		i := i
		group.Go(func() error {
			record := records[i]
			metadata := record.Metadata
			block, err := a.blobStorage.Download(groupCtx, metadata)
			if err != nil {
				return xerrors.Errorf("failed to download single-block object (height=%d, hash=%s): %w", metadata.GetHeight(), metadata.GetHash(), err)
			}
			if err := validateDownloadedBlock(metadata, block); err != nil {
				return err
			}
			storageNeutralBlock := storageutils.CloneBlockWithoutStoragePlacement(block)
			rawBlockPayload, err := proto.Marshal(storageNeutralBlock)
			if err != nil {
				return xerrors.Errorf("failed to marshal block (height=%d, hash=%s): %w", metadata.GetHeight(), metadata.GetHash(), err)
			}
			source, path, length, err := writeRawBlockPayloadTempFile(rawBlockPayload, a.config.AWS.Storage.Consolidation.LocalSpillDir)
			if err != nil {
				return xerrors.Errorf("failed to stage raw block payload (height=%d, hash=%s): %w", metadata.GetHeight(), metadata.GetHash(), err)
			}
			tempFiles[i] = path
			payloads[i].RawBlockPayload = source
			payloads[i].UncompressedLength = length
			sdkactivity.RecordHeartbeat(ctx, "batch_consolidator.block_downloaded", metadata.GetHeight())
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		cleanup()
		return nil, nil, nil, err
	}
	for _, payload := range payloads {
		if payload.RawBlockPayload == nil {
			cleanup()
			return nil, nil, nil, xerrors.Errorf("missing consolidated payload for metadata_id=%d", payload.MetadataID)
		}
	}
	return payloads, recordsByID, cleanup, nil
}

func writeRawBlockPayloadTempFile(raw []byte, dir string) (blobstorage.PayloadSource, string, uint64, error) {
	if len(raw) == 0 {
		return nil, "", 0, xerrors.New("raw block payload cannot be empty")
	}
	spillDir := os.TempDir()
	if dir != "" {
		spillDir = filepath.Clean(dir)
	}
	if err := os.MkdirAll(spillDir, 0o700); err != nil {
		return nil, "", 0, err
	}
	file, err := os.CreateTemp(spillDir, "chainstorage-cscb-raw-block-*.pb")
	if err != nil {
		return nil, "", 0, err
	}
	path := file.Name()
	success := false
	closed := false
	defer func() {
		if !closed {
			_ = file.Close()
		}
		if !success {
			_ = os.Remove(path)
		}
	}()
	if _, err := file.Write(raw); err != nil {
		return nil, "", 0, err
	}
	if err := file.Close(); err != nil {
		return nil, "", 0, err
	}
	closed = true
	success = true
	length := uint64(len(raw))
	return blobstorage.NewFilePayloadSource(path, length), path, length, nil
}

func consolidatedPayloadBytes(payloads []blobstorage.ConsolidatedBlockPayload) uint64 {
	var total uint64
	for _, payload := range payloads {
		total += payload.UncompressedLength
	}
	return total
}

func validateDownloadedBlock(expected *api.BlockMetadata, block *api.Block) error {
	if block == nil || block.Metadata == nil {
		return xerrors.Errorf("downloaded block is missing metadata (height=%d, hash=%s)", expected.GetHeight(), expected.GetHash())
	}
	actual := block.Metadata
	if actual.GetTag() != expected.GetTag() || actual.GetHeight() != expected.GetHeight() || actual.GetHash() != expected.GetHash() {
		return xerrors.Errorf(
			"downloaded block metadata mismatch (expected tag=%d height=%d hash=%s, got tag=%d height=%d hash=%s)",
			expected.GetTag(),
			expected.GetHeight(),
			expected.GetHash(),
			actual.GetTag(),
			actual.GetHeight(),
			actual.GetHash(),
		)
	}
	return nil
}

func makeShadowPlacements(
	recordsByID map[int64]*api.BlockMetadata,
	objectKey string,
	placements []blobstorage.BlockPlacement,
) ([]*metastorage.ConsolidationShadowPlacement, error) {
	shadowPlacements := make([]*metastorage.ConsolidationShadowPlacement, 0, len(placements))
	for _, placement := range placements {
		metadata, ok := recordsByID[placement.MetadataID]
		if !ok {
			return nil, xerrors.Errorf("missing source metadata for placement metadata_id=%d", placement.MetadataID)
		}
		if placement.Height != metadata.GetHeight() || placement.Hash != metadata.GetHash() {
			return nil, xerrors.Errorf(
				"placement metadata mismatch (metadata_id=%d expected height=%d hash=%s, got height=%d hash=%s)",
				placement.MetadataID,
				metadata.GetHeight(),
				metadata.GetHash(),
				placement.Height,
				placement.Hash,
			)
		}
		shadowPlacements = append(shadowPlacements, &metastorage.ConsolidationShadowPlacement{
			BlockMetadataID:               placement.MetadataID,
			Tag:                           metadata.GetTag(),
			Height:                        metadata.GetHeight(),
			Hash:                          metadata.GetHash(),
			SingleBlockObjectKeyMain:      metadata.GetObjectKeyMain(),
			SingleBlockStorageGeneration:  metadata.GetStorageGeneration(),
			ConsolidatedObjectKeyMain:     objectKey,
			ConsolidatedStorageGeneration: placement.StorageGeneration,
			ObjectFormat:                  placement.ObjectFormat,
			ByteOffset:                    placement.ByteOffset,
			ByteLength:                    placement.ByteLength,
			UncompressedLength:            placement.UncompressedLength,
		})
	}
	return shadowPlacements, nil
}
