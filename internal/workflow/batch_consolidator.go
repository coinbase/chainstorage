package workflow

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"strconv"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/cadence"
	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/utils/fxparams"
	"github.com/coinbase/chainstorage/internal/workflow/activity"
)

type (
	BatchConsolidator struct {
		baseWorkflow
		batchConsolidator *activity.BatchConsolidator
		workflowConfig    *config.BatchConsolidatorWorkflowConfig
	}

	BatchConsolidatorParams struct {
		fx.In
		fxparams.Params
		Runtime           cadence.Runtime
		BatchConsolidator *activity.BatchConsolidator
	}

	BatchConsolidatorRequest struct {
		Mode           config.ConsolidationMode `validate:"omitempty,oneof=shadow_dual_write auto_consolidate historical_backfill repair_existing_cscb"`
		Tag            uint32
		StartHeight    uint64
		EndHeight      uint64 `validate:"gtfield=StartHeight"`
		BatchSize      uint64
		CheckpointSize uint64
		MaxBlocks      uint64
		Parallelism    int `validate:"omitempty,gt=0"`
	}

	batchConsolidatorPendingActivity struct {
		request *activity.BatchConsolidatorRequest
		future  workflow.Future
	}
)

var (
	_ InstrumentedRequest = (*BatchConsolidatorRequest)(nil)
)

const (
	tagConsolidationMode                           = "mode"
	batchConsolidatorHeightGauge                   = "workflow.batch_consolidator.height"
	batchConsolidatorCompletedCounter              = "workflow.batch_consolidator.completed"
	batchConsolidatorObjectCounter                 = "workflow.batch_consolidator.object"
	batchConsolidatorConsolidatedBlockCounter      = "workflow.batch_consolidator.consolidated_block"
	batchConsolidatorEmptyBatchCounter             = "workflow.batch_consolidator.empty_batch"
	batchConsolidatorShadowStatsChangeID           = "batch-consolidator-shadow-stats"
	batchConsolidatorShadowStatsVersion            = 1
	batchConsolidatorHistoricalParallelismChangeID = "batch-consolidator-historical-parallelism"
	batchConsolidatorHistoricalParallelismVersion  = 1
	batchConsolidatorAutoSerialChangeID            = "batch-consolidator-auto-serial"
	batchConsolidatorAutoSerialVersion             = 1
	batchConsolidatorAutoCursorChangeID            = "batch-consolidator-auto-cursor"
	batchConsolidatorAutoCursorVersion             = 1
	batchConsolidatorAutoParallelismChangeID       = "batch-consolidator-auto-parallelism"
	batchConsolidatorAutoParallelismVersion        = 1
	batchConsolidatorRepairParallelismChangeID     = "batch-consolidator-repair-parallelism"
	batchConsolidatorRepairParallelismVersion      = 1
	batchConsolidatorPreviousMaxParallelism        = 10
	batchConsolidatorMaxParallelism                = 50
	maxUint64                                      = ^uint64(0)
)

func NewBatchConsolidator(params BatchConsolidatorParams) *BatchConsolidator {
	w := &BatchConsolidator{
		baseWorkflow:      newBaseWorkflow(&params.Config.Workflows.BatchConsolidator, params.Runtime),
		batchConsolidator: params.BatchConsolidator,
		workflowConfig:    &params.Config.Workflows.BatchConsolidator,
	}
	w.registerWorkflow(w.execute)
	return w
}

func (w *BatchConsolidator) Execute(ctx context.Context, request *BatchConsolidatorRequest) (client.WorkflowRun, error) {
	workflowID := w.name
	if override, ok := workflowIDFromContext(ctx); ok {
		workflowID = override
	}
	taskList := w.workflowConfig.TaskList
	mode := w.workflowConfig.Storage.Consolidation.Mode
	if request != nil && request.Mode != "" {
		mode = request.Mode
	}
	if mode == config.ConsolidationModeHistoricalBackfill && w.workflowConfig.HistoricalTaskList != "" {
		taskList = w.workflowConfig.HistoricalTaskList
	}
	return w.startWorkflowOnTaskList(ctx, workflowID, taskList, request)
}

func (r *BatchConsolidatorRequest) GetTags() map[string]string {
	return map[string]string{
		tagBlockTag:          strconv.Itoa(int(r.Tag)),
		tagConsolidationMode: string(r.Mode),
	}
}

func (w *BatchConsolidator) execute(ctx workflow.Context, request *BatchConsolidatorRequest) error {
	return w.executeWorkflow(ctx, request, func() error {
		var cfg config.BatchConsolidatorWorkflowConfig
		if err := w.readConfig(ctx, &cfg); err != nil {
			return xerrors.Errorf("failed to read config: %w", err)
		}

		batchSize := cfg.BatchSize
		if request.BatchSize > 0 {
			batchSize = request.BatchSize
		}
		checkpointSize := cfg.CheckpointSize
		if request.CheckpointSize > 0 {
			checkpointSize = request.CheckpointSize
		}
		maxBlocks := cfg.MaxBlocks
		if request.MaxBlocks > 0 {
			maxBlocks = request.MaxBlocks
		}
		parallelism := 1
		if request.Parallelism > 0 {
			parallelism = request.Parallelism
		}
		if parallelism > batchConsolidatorMaxParallelism {
			return xerrors.Errorf("batch_consolidator parallelism(%d) exceeds max(%d)", parallelism, batchConsolidatorMaxParallelism)
		}
		mode, err := batchConsolidatorMode(request.Mode, cfg.Storage.Consolidation.Mode)
		if err != nil {
			return err
		}
		storageMaxBlocks := cfg.Storage.Consolidation.MaxBlocks
		if storageMaxBlocks == 0 {
			return xerrors.New("batch_consolidator storage consolidation max_blocks must be positive")
		}
		if maxBlocks == 0 || maxBlocks > storageMaxBlocks {
			maxBlocks = storageMaxBlocks
		}
		shardSize := cfg.Storage.Consolidation.ShardSize
		if batchSize == 0 {
			return xerrors.New("batch_consolidator batch_size must be positive")
		}
		if checkpointSize <= batchSize {
			return xerrors.Errorf("batch_consolidator checkpoint_size(%d) must be greater than batch_size(%d)", checkpointSize, batchSize)
		}
		if maxBlocks == 0 {
			return xerrors.New("batch_consolidator max_blocks must be positive")
		}
		if shardSize == 0 {
			return xerrors.New("batch_consolidator storage consolidation shard_size must be positive")
		}
		tag := cfg.GetEffectiveBlockTag(request.Tag)
		metrics := w.getMetricsHandler(ctx).WithTags(map[string]string{
			tagBlockTag:          strconv.Itoa(int(tag)),
			tagConsolidationMode: string(mode),
		})
		logger := w.getLogger(ctx).With(
			zap.Reflect("request", request),
			zap.Reflect("config", cfg),
			zap.Uint32("effective_tag", tag),
			zap.Uint64("batch_size", batchSize),
			zap.Uint64("checkpoint_size", checkpointSize),
			zap.Uint64("max_blocks", maxBlocks),
			zap.Uint64("storage_max_blocks", storageMaxBlocks),
			zap.Uint64("shard_size", shardSize),
			zap.Int("parallelism", parallelism),
			zap.String("mode", string(mode)),
		)
		logger.Info("workflow started")
		activityTaskList := cfg.TaskList
		if mode == config.ConsolidationModeHistoricalBackfill && cfg.HistoricalTaskList != "" {
			activityTaskList = cfg.HistoricalTaskList
		}
		ctx = w.withActivityOptionsOnTaskList(ctx, activityTaskList)
		statsCtx := w.withShadowStatsActivityOptions(ctx, cfg, activityTaskList)
		cursorCtx := w.withCursorActivityOptions(ctx, cfg, activityTaskList)
		if mode.IsRepairExistingCSCB() {
			repairParallelismVersion := workflow.GetVersion(
				ctx,
				batchConsolidatorRepairParallelismChangeID,
				workflow.DefaultVersion,
				batchConsolidatorRepairParallelismVersion,
			)
			if repairParallelismVersion == workflow.DefaultVersion {
				if parallelism != 1 {
					return xerrors.Errorf("legacy repair_existing_cscb execution requires parallelism=1, got %d", parallelism)
				}
			}
			if err := w.validateHistoricalBackfillRange(ctx, logger, tag, request.StartHeight, request.EndHeight, cfg.IrreversibleDistance); err != nil {
				return err
			}
			if repairParallelismVersion == workflow.DefaultVersion {
				return w.executeRepairExistingCSCB(
					ctx,
					logger,
					metrics,
					request,
					tag,
					checkpointSize,
					maxBlocks,
				)
			}
			return w.executeRepairExistingCSCBParallel(
				ctx,
				logger,
				metrics,
				request,
				tag,
				checkpointSize,
				maxBlocks,
				parallelism,
			)
		}
		usePersistedShadowStats := workflow.GetVersion(
			ctx,
			batchConsolidatorShadowStatsChangeID,
			workflow.DefaultVersion,
			batchConsolidatorShadowStatsVersion,
		) != workflow.DefaultVersion && mode != config.ConsolidationModePromoteFinalized
		historicalBackfillVersion := workflow.DefaultVersion
		if mode == config.ConsolidationModeHistoricalBackfill {
			historicalBackfillVersion = workflow.GetVersion(
				ctx,
				batchConsolidatorHistoricalParallelismChangeID,
				workflow.DefaultVersion,
				batchConsolidatorHistoricalParallelismVersion,
			)
		}
		autoConsolidateVersion := workflow.DefaultVersion
		if mode.IsAutoConsolidate() {
			autoConsolidateVersion = workflow.GetVersion(
				ctx,
				batchConsolidatorAutoSerialChangeID,
				workflow.DefaultVersion,
				batchConsolidatorAutoSerialVersion,
			)
		}
		autoConsolidateCursorVersion := workflow.DefaultVersion
		if mode.IsAutoConsolidate() {
			autoConsolidateCursorVersion = workflow.GetVersion(
				ctx,
				batchConsolidatorAutoCursorChangeID,
				workflow.DefaultVersion,
				batchConsolidatorAutoCursorVersion,
			)
		}
		autoConsolidateParallelismVersion := workflow.DefaultVersion
		if mode.IsAutoConsolidate() {
			autoConsolidateParallelismVersion = workflow.GetVersion(
				ctx,
				batchConsolidatorAutoParallelismChangeID,
				workflow.DefaultVersion,
				batchConsolidatorAutoParallelismVersion,
			)
		}

		if historicalBackfillVersion != workflow.DefaultVersion {
			if err := w.validateHistoricalBackfillRange(ctx, logger, tag, request.StartHeight, request.EndHeight, cfg.IrreversibleDistance); err != nil {
				return err
			}
		}
		if mode.IsAutoConsolidate() {
			if err := w.validateAutoConsolidateRange(ctx, logger, tag, request.StartHeight, request.EndHeight, cfg.IrreversibleDistance); err != nil {
				return err
			}
			if autoConsolidateCursorVersion != workflow.DefaultVersion &&
				!batchConsolidatorAutoRangeHasOnlyFullObjectWindows(request.StartHeight, request.EndHeight, batchSize, maxBlocks, shardSize) {
				return xerrors.Errorf(
					"auto_consolidate range [%d, %d) must contain only full object windows of max_blocks=%d",
					request.StartHeight,
					request.EndHeight,
					maxBlocks,
				)
			}
		}

		workflowEndHeight := request.EndHeight
		if mode == config.ConsolidationModePromoteFinalized {
			plan, err := w.batchConsolidator.GetPromotionPlan(ctx, &activity.BatchConsolidatorPlanRequest{
				Tag:         tag,
				StartHeight: request.StartHeight,
				EndHeight:   request.EndHeight,
			})
			if err != nil {
				return xerrors.Errorf("failed to plan promote_finalized range: %w", err)
			}
			workflowEndHeight = plan.EndHeight
			logger.Info(
				"planned promote_finalized range",
				zap.Uint64("requested_end_height", request.EndHeight),
				zap.Uint64("effective_end_height", workflowEndHeight),
				zap.Uint64("latest_height", plan.LatestHeight),
				zap.Uint64("safe_promotion_height", plan.SafePromotionHeight),
				zap.Uint64("promotion_gate_height", plan.PromotionGateHeight),
			)
			if workflowEndHeight <= request.StartHeight {
				logger.Info("promote_finalized range has no currently safe heights")
				return nil
			}
		}

		// Keep default shadow activity payloads unchanged for existing workflow history replay.
		activityMode := config.ConsolidationMode("")
		if request.Mode != "" {
			activityMode = mode
		}

		for batchStart := request.StartHeight; batchStart < workflowEndHeight; {
			if batchStart-request.StartHeight >= checkpointSize {
				newRequest := *request
				newRequest.StartHeight = batchStart
				logger.Info("checkpoint reached", zap.Reflect("newRequest", newRequest))
				return w.continueAsNew(ctx, &newRequest)
			}

			batchEnd := batchConsolidatorWindowEnd(batchStart, workflowEndHeight, batchSize, shardSize)
			if mode == config.ConsolidationModeHistoricalBackfill && parallelism > batchConsolidatorPreviousMaxParallelism {
				// Parallelism above the previous hard limit cannot exist in an old history.
				// Queue work across shards while keeping each CSCB object shard-bounded below.
				batchEnd = batchConsolidatorBatchEnd(batchStart, workflowEndHeight, batchSize)
			}
			if batchEnd <= batchStart {
				return xerrors.Errorf("batch_consolidator made no height progress from %d to %d", batchStart, batchEnd)
			}

			if historicalBackfillVersion != workflow.DefaultVersion && parallelism > 1 {
				objectsInBatch, blocksInBatch, err := w.processHistoricalBackfillBatchParallel(
					ctx,
					statsCtx,
					logger,
					metrics,
					tag,
					activityMode,
					batchStart,
					batchEnd,
					maxBlocks,
					shardSize,
					parallelism,
					usePersistedShadowStats,
				)
				if err != nil {
					return err
				}
				metrics.Gauge(batchConsolidatorHeightGauge).Update(float64(batchEnd - 1))
				logger.Info(
					"processed shadow batch",
					zap.Uint64("batch_start", batchStart),
					zap.Uint64("batch_end", batchEnd),
					zap.Uint64("objects", objectsInBatch),
					zap.Uint64("consolidated_blocks", blocksInBatch),
				)
				batchStart = batchEnd
				continue
			}

			if mode.IsAutoConsolidate() &&
				(autoConsolidateCursorVersion != workflow.DefaultVersion || autoConsolidateVersion == workflow.DefaultVersion) {
				autoConsolidateParallelism := 1
				if autoConsolidateCursorVersion == workflow.DefaultVersion ||
					autoConsolidateParallelismVersion != workflow.DefaultVersion {
					autoConsolidateParallelism = parallelism
				}
				objectsInBatch, blocksInBatch, err := w.processAutoConsolidateBatchParallel(
					ctx,
					statsCtx,
					logger,
					metrics,
					tag,
					activityMode,
					batchStart,
					batchEnd,
					maxBlocks,
					autoConsolidateParallelism,
					usePersistedShadowStats,
				)
				if err != nil {
					return err
				}
				metrics.Gauge(batchConsolidatorHeightGauge).Update(float64(batchEnd - 1))
				logger.Info(
					"processed shadow batch",
					zap.Uint64("batch_start", batchStart),
					zap.Uint64("batch_end", batchEnd),
					zap.Uint64("objects", objectsInBatch),
					zap.Uint64("consolidated_blocks", blocksInBatch),
				)
				batchStart = batchEnd
				continue
			}

			objectsInBatch := uint64(0)
			blocksInBatch := uint64(0)
			lastShadowObjects := uint64(0)
			lastShadowBlocks := uint64(0)
			if usePersistedShadowStats {
				baseline, err := w.batchConsolidator.GetShadowStats(statsCtx, &activity.BatchConsolidatorStatsRequest{
					Mode:        activityMode,
					Tag:         tag,
					StartHeight: batchStart,
					EndHeight:   batchEnd,
				})
				if err != nil {
					return xerrors.Errorf("failed to get consolidation shadow stats for batch [%d, %d): %w", batchStart, batchEnd, err)
				}
				lastShadowObjects = baseline.ShadowObjects
				lastShadowBlocks = baseline.ShadowBlocks
			}
			for {
				response, err := w.batchConsolidator.Execute(ctx, &activity.BatchConsolidatorRequest{
					Mode:        activityMode,
					Tag:         tag,
					StartHeight: batchStart,
					EndHeight:   batchEnd,
					MaxBlocks:   maxBlocks,
				})
				if err != nil {
					return xerrors.Errorf("failed to consolidate shadow batch [%d, %d): %w", batchStart, batchEnd, err)
				}
				newObjects := uint64(0)
				newBlocks := uint64(0)
				if usePersistedShadowStats {
					stats, err := w.batchConsolidator.GetShadowStats(statsCtx, &activity.BatchConsolidatorStatsRequest{
						Mode:        activityMode,
						Tag:         tag,
						StartHeight: batchStart,
						EndHeight:   batchEnd,
					})
					if err != nil {
						return xerrors.Errorf("failed to get consolidation shadow stats for batch [%d, %d): %w", batchStart, batchEnd, err)
					}
					if stats.ShadowObjects < lastShadowObjects || stats.ShadowBlocks < lastShadowBlocks {
						return xerrors.Errorf(
							"batch_consolidator shadow stats regressed for batch [%d, %d): objects %d -> %d, blocks %d -> %d",
							batchStart,
							batchEnd,
							lastShadowObjects,
							stats.ShadowObjects,
							lastShadowBlocks,
							stats.ShadowBlocks,
						)
					}
					newObjects = stats.ShadowObjects - lastShadowObjects
					newBlocks = stats.ShadowBlocks - lastShadowBlocks
					lastShadowObjects = stats.ShadowObjects
					lastShadowBlocks = stats.ShadowBlocks
				}
				if response.ScannedBlocks == 0 {
					if newObjects > 0 {
						objectsInBatch += newObjects
						metrics.Counter(batchConsolidatorObjectCounter).Inc(int64(newObjects))
					}
					if newBlocks > 0 {
						blocksInBatch += newBlocks
						metrics.Counter(batchConsolidatorConsolidatedBlockCounter).Inc(int64(newBlocks))
					}
					metrics.Counter(batchConsolidatorEmptyBatchCounter).Inc(1)
					break
				}
				if response.ConsolidatedBlocks == 0 {
					return xerrors.Errorf("batch_consolidator made no progress for non-empty shadow scan [%d, %d)", batchStart, batchEnd)
				}
				if !usePersistedShadowStats {
					newObjects = 1
					newBlocks = response.ConsolidatedBlocks
				}
				if newObjects > 0 {
					objectsInBatch += newObjects
					metrics.Counter(batchConsolidatorObjectCounter).Inc(int64(newObjects))
				}
				if newBlocks > 0 {
					blocksInBatch += newBlocks
					metrics.Counter(batchConsolidatorConsolidatedBlockCounter).Inc(int64(newBlocks))
				}
				logger.Info(
					"processed shadow object",
					zap.Uint64("batch_start", batchStart),
					zap.Uint64("batch_end", batchEnd),
					zap.Uint64("scanned_blocks", response.ScannedBlocks),
					zap.Uint64("consolidated_blocks", response.ConsolidatedBlocks),
					zap.Uint64("new_objects", newObjects),
					zap.Uint64("new_consolidated_blocks", newBlocks),
					zap.Uint64("shadow_objects", lastShadowObjects),
					zap.Uint64("shadow_blocks", lastShadowBlocks),
					zap.String("object_key", response.ObjectKey),
				)
				if response.ScannedBlocks < maxBlocks {
					break
				}
			}
			metrics.Gauge(batchConsolidatorHeightGauge).Update(float64(batchEnd - 1))
			logger.Info(
				"processed shadow batch",
				zap.Uint64("batch_start", batchStart),
				zap.Uint64("batch_end", batchEnd),
				zap.Uint64("objects", objectsInBatch),
				zap.Uint64("consolidated_blocks", blocksInBatch),
			)
			batchStart = batchEnd
		}

		if mode.IsAutoConsolidate() && autoConsolidateCursorVersion != workflow.DefaultVersion {
			if _, err := w.batchConsolidator.UpdateAutoConsolidateCursor(cursorCtx, &activity.BatchConsolidatorCursorRequest{
				Tag:    tag,
				Height: workflowEndHeight,
			}); err != nil {
				return xerrors.Errorf("failed to update auto_consolidate cursor to height %d: %w", workflowEndHeight, err)
			}
			logger.Info("updated auto_consolidate cursor", zap.Uint32("tag", tag), zap.Uint64("height", workflowEndHeight))
		}

		metrics.Counter(batchConsolidatorCompletedCounter).Inc(1)
		logger.Info("workflow finished")
		return nil
	})
}

func (w *BatchConsolidator) executeRepairExistingCSCB(
	ctx workflow.Context,
	logger *zap.Logger,
	metrics client.MetricsHandler,
	request *BatchConsolidatorRequest,
	tag uint32,
	checkpointSize uint64,
	maxBlocks uint64,
) error {
	processedBlocks := uint64(0)
	repairIteration := uint64(0)
	for {
		response, err := w.batchConsolidator.Execute(ctx, &activity.BatchConsolidatorRequest{
			Mode:               config.ConsolidationModeRepairExistingCSCB,
			Tag:                tag,
			StartHeight:        request.StartHeight,
			EndHeight:          request.EndHeight,
			MaxBlocks:          maxBlocks,
			RepairExecutionKey: makeRepairExecutionKey(ctx, repairIteration),
		})
		if err != nil {
			return xerrors.Errorf(
				"failed to repair existing CSCB range [%d, %d): %w",
				request.StartHeight,
				request.EndHeight,
				err,
			)
		}
		if response.RepairedObjects == 0 {
			metrics.Counter(batchConsolidatorEmptyBatchCounter).Inc(1)
			logger.Info("CSCB repair range is complete")
			return nil
		}
		if response.ScannedBlocks == 0 {
			return xerrors.Errorf(
				"repair_existing_cscb made no block progress for old object %q",
				response.OldObjectKey,
			)
		}
		processedBlocks += response.ScannedBlocks
		metrics.Counter(batchConsolidatorObjectCounter).Inc(int64(response.RepairedObjects))
		metrics.Counter(batchConsolidatorConsolidatedBlockCounter).Inc(int64(response.ConsolidatedBlocks))
		metrics.Gauge(batchConsolidatorHeightGauge).Update(float64(response.EndHeight - 1))
		logger.Info(
			"repaired existing CSCB object",
			zap.String("old_object_key", response.OldObjectKey),
			zap.String("new_object_key", response.ObjectKey),
			zap.Uint64("start_height", response.StartHeight),
			zap.Uint64("end_height", response.EndHeight),
			zap.Uint64("blocks", response.ScannedBlocks),
			zap.Uint64("canonical_blocks", response.ConsolidatedBlocks),
			zap.Bool("rebuilt_cscb", response.ObjectKey != "" && response.ObjectKey != response.OldObjectKey),
		)
		repairIteration++
		if processedBlocks >= checkpointSize {
			newRequest := *request
			logger.Info(
				"CSCB repair checkpoint reached",
				zap.Uint64("processed_blocks", processedBlocks),
				zap.Reflect("new_request", newRequest),
			)
			return w.continueAsNew(ctx, &newRequest)
		}
	}
}

func (w *BatchConsolidator) executeRepairExistingCSCBParallel(
	ctx workflow.Context,
	logger *zap.Logger,
	metrics client.MetricsHandler,
	request *BatchConsolidatorRequest,
	tag uint32,
	checkpointSize uint64,
	maxBlocks uint64,
	parallelism int,
) error {
	processedBlocks := uint64(0)
	repairIteration := uint64(0)
	for {
		candidates, err := w.batchConsolidator.GetRepairCandidates(ctx, &activity.BatchConsolidatorRepairCandidatesRequest{
			Tag:         tag,
			StartHeight: request.StartHeight,
			EndHeight:   request.EndHeight,
			Limit:       parallelism,
		})
		if err != nil {
			return xerrors.Errorf(
				"failed to list existing CSCB repair candidates in range [%d, %d): %w",
				request.StartHeight,
				request.EndHeight,
				err,
			)
		}
		if len(candidates.ObjectKeys) == 0 {
			metrics.Counter(batchConsolidatorEmptyBatchCounter).Inc(1)
			logger.Info("CSCB repair range is complete")
			return nil
		}
		if len(candidates.ObjectKeys) > parallelism {
			return xerrors.Errorf(
				"CSCB repair candidate list exceeds parallelism: candidates=%d parallelism=%d",
				len(candidates.ObjectKeys),
				parallelism,
			)
		}

		pending := make([]batchConsolidatorPendingActivity, 0, len(candidates.ObjectKeys))
		seen := make(map[string]struct{}, len(candidates.ObjectKeys))
		for _, objectKey := range candidates.ObjectKeys {
			if objectKey == "" {
				return xerrors.New("CSCB repair candidate list contains an empty object key")
			}
			if _, ok := seen[objectKey]; ok {
				return xerrors.Errorf("CSCB repair candidate list contains duplicate object key %q", objectKey)
			}
			seen[objectKey] = struct{}{}
			activityRequest := &activity.BatchConsolidatorRequest{
				Mode:               config.ConsolidationModeRepairExistingCSCB,
				Tag:                tag,
				StartHeight:        request.StartHeight,
				EndHeight:          request.EndHeight,
				MaxBlocks:          maxBlocks,
				RepairExecutionKey: makeRepairExecutionKey(ctx, repairIteration),
				RepairObjectKey:    objectKey,
			}
			pending = append(pending, batchConsolidatorPendingActivity{
				request: activityRequest,
				future:  workflow.ExecuteActivity(ctx, activity.ActivityBatchConsolidator, activityRequest),
			})
			repairIteration++
		}

		var firstErr error
		for _, pendingActivity := range pending {
			var response activity.BatchConsolidatorResponse
			if err := pendingActivity.future.Get(ctx, &response); err != nil {
				if firstErr == nil {
					firstErr = xerrors.Errorf(
						"failed to repair existing CSCB object %q: %w",
						pendingActivity.request.RepairObjectKey,
						err,
					)
				}
				continue
			}
			if response.RepairedObjects != 1 {
				if firstErr == nil {
					firstErr = xerrors.Errorf(
						"repair_existing_cscb returned repaired_objects=%d for object %q",
						response.RepairedObjects,
						pendingActivity.request.RepairObjectKey,
					)
				}
				continue
			}
			if response.OldObjectKey != pendingActivity.request.RepairObjectKey {
				if firstErr == nil {
					firstErr = xerrors.Errorf(
						"repair_existing_cscb returned unexpected old object: expected=%q actual=%q",
						pendingActivity.request.RepairObjectKey,
						response.OldObjectKey,
					)
				}
				continue
			}
			if response.ScannedBlocks == 0 {
				if firstErr == nil {
					firstErr = xerrors.Errorf(
						"repair_existing_cscb made no block progress for old object %q",
						response.OldObjectKey,
					)
				}
				continue
			}

			processedBlocks += response.ScannedBlocks
			metrics.Counter(batchConsolidatorObjectCounter).Inc(int64(response.RepairedObjects))
			metrics.Counter(batchConsolidatorConsolidatedBlockCounter).Inc(int64(response.ConsolidatedBlocks))
			metrics.Gauge(batchConsolidatorHeightGauge).Update(float64(response.EndHeight - 1))
			logger.Info(
				"repaired existing CSCB object",
				zap.String("old_object_key", response.OldObjectKey),
				zap.String("new_object_key", response.ObjectKey),
				zap.Uint64("start_height", response.StartHeight),
				zap.Uint64("end_height", response.EndHeight),
				zap.Uint64("blocks", response.ScannedBlocks),
				zap.Uint64("canonical_blocks", response.ConsolidatedBlocks),
				zap.Bool("rebuilt_cscb", response.ObjectKey != "" && response.ObjectKey != response.OldObjectKey),
			)
		}
		if firstErr != nil {
			return firstErr
		}
		if processedBlocks >= checkpointSize {
			newRequest := *request
			logger.Info(
				"CSCB repair checkpoint reached",
				zap.Uint64("processed_blocks", processedBlocks),
				zap.Reflect("new_request", newRequest),
			)
			return w.continueAsNew(ctx, &newRequest)
		}
	}
}

func makeRepairExecutionKey(ctx workflow.Context, iteration uint64) string {
	info := workflow.GetInfo(ctx)
	payload := info.WorkflowExecution.ID + "\x00" +
		info.WorkflowExecution.RunID + "\x00" +
		strconv.FormatUint(iteration, 10)
	digest := sha256.Sum256([]byte(payload))
	return hex.EncodeToString(digest[:])
}

func (w *BatchConsolidator) processAutoConsolidateBatchParallel(
	ctx workflow.Context,
	statsCtx workflow.Context,
	logger *zap.Logger,
	metrics client.MetricsHandler,
	tag uint32,
	activityMode config.ConsolidationMode,
	batchStart uint64,
	batchEnd uint64,
	maxBlocks uint64,
	parallelism int,
	usePersistedShadowStats bool,
) (uint64, uint64, error) {
	lastShadowObjects := uint64(0)
	lastShadowBlocks := uint64(0)
	if usePersistedShadowStats {
		baseline, err := w.batchConsolidator.GetShadowStats(statsCtx, &activity.BatchConsolidatorStatsRequest{
			Mode:        activityMode,
			Tag:         tag,
			StartHeight: batchStart,
			EndHeight:   batchEnd,
		})
		if err != nil {
			return 0, 0, xerrors.Errorf("failed to get consolidation shadow stats for batch [%d, %d): %w", batchStart, batchEnd, err)
		}
		lastShadowObjects = baseline.ShadowObjects
		lastShadowBlocks = baseline.ShadowBlocks
	}

	objectsInBatch := uint64(0)
	blocksInBatch := uint64(0)
	for windowStart := batchStart; windowStart < batchEnd; {
		pending := make([]batchConsolidatorPendingActivity, 0, parallelism)
		for len(pending) < parallelism && windowStart < batchEnd {
			windowEnd, fullWindow := batchConsolidatorFullObjectWindowEnd(windowStart, batchEnd, maxBlocks)
			if !fullWindow {
				logger.Info(
					"auto consolidate waiting for a full object window",
					zap.Uint64("window_start", windowStart),
					zap.Uint64("batch_end", batchEnd),
					zap.Uint64("max_blocks", maxBlocks),
				)
				break
			}
			request := &activity.BatchConsolidatorRequest{
				Mode:        activityMode,
				Tag:         tag,
				StartHeight: windowStart,
				EndHeight:   windowEnd,
				MaxBlocks:   maxBlocks,
			}
			pending = append(pending, batchConsolidatorPendingActivity{
				request: request,
				future:  workflow.ExecuteActivity(ctx, activity.ActivityBatchConsolidator, request),
			})
			windowStart = windowEnd
		}
		if len(pending) == 0 {
			break
		}

		var firstErr error
		type successResult struct {
			request  *activity.BatchConsolidatorRequest
			response activity.BatchConsolidatorResponse
		}
		successes := make([]successResult, 0, len(pending))
		for _, item := range pending {
			var response activity.BatchConsolidatorResponse
			request := item.request
			if err := item.future.Get(ctx, &response); err != nil {
				if firstErr == nil {
					firstErr = xerrors.Errorf("failed to consolidate shadow batch [%d, %d): %w", request.StartHeight, request.EndHeight, err)
				}
				continue
			}
			successes = append(successes, successResult{request: request, response: response})
		}
		if firstErr != nil {
			return 0, 0, firstErr
		}

		for _, success := range successes {
			request := success.request
			response := success.response
			if response.ScannedBlocks == 0 {
				metrics.Counter(batchConsolidatorEmptyBatchCounter).Inc(1)
				continue
			}
			if response.ConsolidatedBlocks == 0 {
				return 0, 0, xerrors.Errorf(
					"batch_consolidator made no progress for non-empty shadow scan [%d, %d)",
					request.StartHeight,
					request.EndHeight,
				)
			}

			newObjects := uint64(0)
			newBlocks := uint64(0)
			if !usePersistedShadowStats {
				newObjects = 1
				newBlocks = response.ConsolidatedBlocks
				objectsInBatch += newObjects
				blocksInBatch += newBlocks
				metrics.Counter(batchConsolidatorObjectCounter).Inc(int64(newObjects))
				metrics.Counter(batchConsolidatorConsolidatedBlockCounter).Inc(int64(newBlocks))
			}
			logger.Info(
				"processed shadow object",
				zap.Uint64("batch_start", batchStart),
				zap.Uint64("batch_end", batchEnd),
				zap.Uint64("window_start", request.StartHeight),
				zap.Uint64("window_end", request.EndHeight),
				zap.Uint64("scanned_blocks", response.ScannedBlocks),
				zap.Uint64("consolidated_blocks", response.ConsolidatedBlocks),
				zap.Uint64("new_objects", newObjects),
				zap.Uint64("new_consolidated_blocks", newBlocks),
				zap.Uint64("shadow_objects", lastShadowObjects),
				zap.Uint64("shadow_blocks", lastShadowBlocks),
				zap.String("object_key", response.ObjectKey),
			)
		}
	}

	if usePersistedShadowStats {
		stats, err := w.batchConsolidator.GetShadowStats(statsCtx, &activity.BatchConsolidatorStatsRequest{
			Mode:        activityMode,
			Tag:         tag,
			StartHeight: batchStart,
			EndHeight:   batchEnd,
		})
		if err != nil {
			return 0, 0, xerrors.Errorf("failed to get consolidation shadow stats for batch [%d, %d): %w", batchStart, batchEnd, err)
		}
		if stats.ShadowObjects < lastShadowObjects || stats.ShadowBlocks < lastShadowBlocks {
			return 0, 0, xerrors.Errorf(
				"batch_consolidator shadow stats regressed for batch [%d, %d): objects %d -> %d, blocks %d -> %d",
				batchStart,
				batchEnd,
				lastShadowObjects,
				stats.ShadowObjects,
				lastShadowBlocks,
				stats.ShadowBlocks,
			)
		}
		objectsInBatch = stats.ShadowObjects - lastShadowObjects
		blocksInBatch = stats.ShadowBlocks - lastShadowBlocks
		if objectsInBatch > 0 {
			metrics.Counter(batchConsolidatorObjectCounter).Inc(int64(objectsInBatch))
		}
		if blocksInBatch > 0 {
			metrics.Counter(batchConsolidatorConsolidatedBlockCounter).Inc(int64(blocksInBatch))
		}
	}

	return objectsInBatch, blocksInBatch, nil
}

func (w *BatchConsolidator) processHistoricalBackfillBatchParallel(
	ctx workflow.Context,
	statsCtx workflow.Context,
	logger *zap.Logger,
	metrics client.MetricsHandler,
	tag uint32,
	activityMode config.ConsolidationMode,
	batchStart uint64,
	batchEnd uint64,
	maxBlocks uint64,
	shardSize uint64,
	parallelism int,
	usePersistedShadowStats bool,
) (uint64, uint64, error) {
	lastShadowObjects := uint64(0)
	lastShadowBlocks := uint64(0)
	if usePersistedShadowStats {
		baseline, err := w.batchConsolidator.GetShadowStats(statsCtx, &activity.BatchConsolidatorStatsRequest{
			Mode:        activityMode,
			Tag:         tag,
			StartHeight: batchStart,
			EndHeight:   batchEnd,
		})
		if err != nil {
			return 0, 0, xerrors.Errorf("failed to get consolidation shadow stats for batch [%d, %d): %w", batchStart, batchEnd, err)
		}
		lastShadowObjects = baseline.ShadowObjects
		lastShadowBlocks = baseline.ShadowBlocks
	}

	objectsInBatch := uint64(0)
	blocksInBatch := uint64(0)
	for windowStart := batchStart; windowStart < batchEnd; {
		pending := make([]batchConsolidatorPendingActivity, 0, parallelism)
		for len(pending) < parallelism && windowStart < batchEnd {
			windowEnd := batchConsolidatorWindowEnd(windowStart, batchEnd, maxBlocks, shardSize)
			if windowEnd <= windowStart {
				return 0, 0, xerrors.Errorf("batch_consolidator made no parallel window progress from %d to %d", windowStart, windowEnd)
			}
			request := &activity.BatchConsolidatorRequest{
				Mode:        activityMode,
				Tag:         tag,
				StartHeight: windowStart,
				EndHeight:   windowEnd,
				MaxBlocks:   maxBlocks,
			}
			pending = append(pending, batchConsolidatorPendingActivity{
				request: request,
				future:  workflow.ExecuteActivity(ctx, activity.ActivityBatchConsolidator, request),
			})
			windowStart = windowEnd
		}
		if len(pending) == 0 {
			break
		}

		var firstErr error
		type successResult struct {
			request  *activity.BatchConsolidatorRequest
			response activity.BatchConsolidatorResponse
		}
		successes := make([]successResult, 0, len(pending))
		for _, item := range pending {
			var response activity.BatchConsolidatorResponse
			request := item.request
			if err := item.future.Get(ctx, &response); err != nil {
				if firstErr == nil {
					firstErr = xerrors.Errorf("failed to consolidate shadow batch [%d, %d): %w", request.StartHeight, request.EndHeight, err)
				}
				continue
			}
			successes = append(successes, successResult{request: request, response: response})
		}
		if firstErr != nil {
			return 0, 0, firstErr
		}

		for _, success := range successes {
			request := success.request
			response := success.response
			if response.ScannedBlocks == 0 {
				metrics.Counter(batchConsolidatorEmptyBatchCounter).Inc(1)
				continue
			}
			if response.ConsolidatedBlocks == 0 {
				return 0, 0, xerrors.Errorf(
					"batch_consolidator made no progress for non-empty shadow scan [%d, %d)",
					request.StartHeight,
					request.EndHeight,
				)
			}

			newObjects := uint64(0)
			newBlocks := uint64(0)
			if !usePersistedShadowStats {
				newObjects = 1
				newBlocks = response.ConsolidatedBlocks
				objectsInBatch += newObjects
				blocksInBatch += newBlocks
				metrics.Counter(batchConsolidatorObjectCounter).Inc(int64(newObjects))
				metrics.Counter(batchConsolidatorConsolidatedBlockCounter).Inc(int64(newBlocks))
			}
			logger.Info(
				"processed shadow object",
				zap.Uint64("batch_start", batchStart),
				zap.Uint64("batch_end", batchEnd),
				zap.Uint64("window_start", request.StartHeight),
				zap.Uint64("window_end", request.EndHeight),
				zap.Uint64("scanned_blocks", response.ScannedBlocks),
				zap.Uint64("consolidated_blocks", response.ConsolidatedBlocks),
				zap.Uint64("new_objects", newObjects),
				zap.Uint64("new_consolidated_blocks", newBlocks),
				zap.Uint64("shadow_objects", lastShadowObjects),
				zap.Uint64("shadow_blocks", lastShadowBlocks),
				zap.String("object_key", response.ObjectKey),
			)
		}
	}

	if usePersistedShadowStats {
		stats, err := w.batchConsolidator.GetShadowStats(statsCtx, &activity.BatchConsolidatorStatsRequest{
			Mode:        activityMode,
			Tag:         tag,
			StartHeight: batchStart,
			EndHeight:   batchEnd,
		})
		if err != nil {
			return 0, 0, xerrors.Errorf("failed to get consolidation shadow stats for batch [%d, %d): %w", batchStart, batchEnd, err)
		}
		if stats.ShadowObjects < lastShadowObjects || stats.ShadowBlocks < lastShadowBlocks {
			return 0, 0, xerrors.Errorf(
				"batch_consolidator shadow stats regressed for batch [%d, %d): objects %d -> %d, blocks %d -> %d",
				batchStart,
				batchEnd,
				lastShadowObjects,
				stats.ShadowObjects,
				lastShadowBlocks,
				stats.ShadowBlocks,
			)
		}
		objectsInBatch = stats.ShadowObjects - lastShadowObjects
		blocksInBatch = stats.ShadowBlocks - lastShadowBlocks
		if objectsInBatch > 0 {
			metrics.Counter(batchConsolidatorObjectCounter).Inc(int64(objectsInBatch))
		}
		if blocksInBatch > 0 {
			metrics.Counter(batchConsolidatorConsolidatedBlockCounter).Inc(int64(blocksInBatch))
		}
	}

	return objectsInBatch, blocksInBatch, nil
}

func batchConsolidatorMode(
	requestMode config.ConsolidationMode,
	configMode config.ConsolidationMode,
) (config.ConsolidationMode, error) {
	mode := requestMode
	if mode == "" {
		mode = configMode
	}
	switch mode {
	case config.ConsolidationModeShadowDualWrite,
		config.ConsolidationModeAutoConsolidate,
		config.ConsolidationModeHistoricalBackfill,
		config.ConsolidationModePromoteFinalized,
		config.ConsolidationModeRepairExistingCSCB:
		return mode, nil
	default:
		return "", xerrors.Errorf(
			"batch_consolidator requires mode %q, %q, %q, %q, or %q, got %q",
			config.ConsolidationModeShadowDualWrite,
			config.ConsolidationModeAutoConsolidate,
			config.ConsolidationModeHistoricalBackfill,
			config.ConsolidationModePromoteFinalized,
			config.ConsolidationModeRepairExistingCSCB,
			mode,
		)
	}
}

func (w *BatchConsolidator) validateHistoricalBackfillRange(
	ctx workflow.Context,
	logger *zap.Logger,
	tag uint32,
	startHeight uint64,
	endHeight uint64,
	irreversibleDistance uint64,
) error {
	latest, err := w.batchConsolidator.GetLatestBlock(ctx, &activity.BatchConsolidatorLatestBlockRequest{
		Tag: tag,
	})
	if err != nil {
		return xerrors.Errorf("failed to get latest block for historical backfill range validation: %w", err)
	}
	if latest == nil {
		return xerrors.New("latest block response is nil for historical backfill range validation")
	}
	safeEndHeight, ok := batchConsolidatorHistoricalSafeEndHeight(latest.Height, irreversibleDistance)
	if !ok {
		return xerrors.Errorf(
			"historical_backfill range [%d, %d) is unsafe: latest height %d is below irreversible_distance %d",
			startHeight,
			endHeight,
			latest.Height,
			irreversibleDistance,
		)
	}
	if endHeight > safeEndHeight {
		return xerrors.Errorf(
			"historical_backfill range [%d, %d) is unsafe: end_height must be <= %d for latest_height=%d irreversible_distance=%d",
			startHeight,
			endHeight,
			safeEndHeight,
			latest.Height,
			irreversibleDistance,
		)
	}
	logger.Info(
		"validated historical backfill range",
		zap.Uint32("tag", tag),
		zap.Uint64("start_height", startHeight),
		zap.Uint64("end_height", endHeight),
		zap.Uint64("latest_height", latest.Height),
		zap.Uint64("irreversible_distance", irreversibleDistance),
		zap.Uint64("safe_end_height", safeEndHeight),
	)
	return nil
}

func batchConsolidatorHistoricalSafeEndHeight(latestHeight uint64, irreversibleDistance uint64) (uint64, bool) {
	return batchConsolidatorSafeEndHeight(latestHeight, irreversibleDistance)
}

func (w *BatchConsolidator) validateAutoConsolidateRange(
	ctx workflow.Context,
	logger *zap.Logger,
	tag uint32,
	startHeight uint64,
	endHeight uint64,
	irreversibleDistance uint64,
) error {
	latest, err := w.batchConsolidator.GetLatestBlock(ctx, &activity.BatchConsolidatorLatestBlockRequest{
		Tag: tag,
	})
	if err != nil {
		return xerrors.Errorf("failed to get latest block for auto_consolidate range validation: %w", err)
	}
	if latest == nil {
		return xerrors.New("latest block response is nil for auto_consolidate range validation")
	}
	safeEndHeight, ok := batchConsolidatorAutoConsolidateSafeEndHeight(latest.Height, irreversibleDistance)
	if !ok {
		return xerrors.Errorf(
			"auto_consolidate range [%d, %d) is unsafe: latest height %d is below irreversible_distance %d",
			startHeight,
			endHeight,
			latest.Height,
			irreversibleDistance,
		)
	}
	if endHeight > safeEndHeight {
		return xerrors.Errorf(
			"auto_consolidate range [%d, %d) is unsafe: end_height must be <= %d for latest_height=%d irreversible_distance=%d",
			startHeight,
			endHeight,
			safeEndHeight,
			latest.Height,
			irreversibleDistance,
		)
	}
	logger.Info(
		"validated auto consolidate range",
		zap.Uint32("tag", tag),
		zap.Uint64("start_height", startHeight),
		zap.Uint64("end_height", endHeight),
		zap.Uint64("latest_height", latest.Height),
		zap.Uint64("irreversible_distance", irreversibleDistance),
		zap.Uint64("safe_end_height", safeEndHeight),
	)
	return nil
}

func batchConsolidatorAutoConsolidateSafeEndHeight(latestHeight uint64, irreversibleDistance uint64) (uint64, bool) {
	return batchConsolidatorSafeEndHeight(latestHeight, irreversibleDistance)
}

func batchConsolidatorSafeEndHeight(latestHeight uint64, irreversibleDistance uint64) (uint64, bool) {
	if irreversibleDistance == 0 {
		if latestHeight == maxUint64 {
			return maxUint64, true
		}
		return latestHeight + 1, true
	}
	if latestHeight < irreversibleDistance-1 {
		return 0, false
	}
	latestFinalizedHeight := latestHeight - irreversibleDistance + 1
	if latestFinalizedHeight == maxUint64 {
		return maxUint64, true
	}
	return latestFinalizedHeight + 1, true
}

func (w *BatchConsolidator) withShadowStatsActivityOptions(
	ctx workflow.Context,
	cfg config.BatchConsolidatorWorkflowConfig,
	taskList string,
) workflow.Context {
	base := cfg.Base()
	retryPolicy := w.getShadowStatsActivityRetryPolicy(base.ActivityRetry)
	return workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		TaskQueue:              taskList,
		StartToCloseTimeout:    base.ActivityStartToCloseTimeout,
		ScheduleToCloseTimeout: base.ActivityScheduleToCloseTimeout,
		HeartbeatTimeout:       base.ActivityHeartbeatTimeout,
		RetryPolicy:            retryPolicy,
	})
}

func (w *BatchConsolidator) getShadowStatsActivityRetryPolicy(cfg *config.RetryPolicy) *temporal.RetryPolicy {
	retryPolicy := w.getRetryPolicy(cfg)
	if retryPolicy != nil {
		retryPolicyCopy := *retryPolicy
		// Stats reads are side-effect-free and cheap. Unlimited attempts prevent
		// old workers on the shared task queue from exhausting retries during a
		// rolling deploy before a new worker polls the new stats activity name.
		retryPolicyCopy.MaximumAttempts = 0
		retryPolicy = &retryPolicyCopy
	}
	return retryPolicy
}

func (w *BatchConsolidator) withCursorActivityOptions(
	ctx workflow.Context,
	cfg config.BatchConsolidatorWorkflowConfig,
	taskList string,
) workflow.Context {
	base := cfg.Base()
	retryPolicy := w.getCursorActivityRetryPolicy(base.ActivityRetry)
	return workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		TaskQueue:              taskList,
		StartToCloseTimeout:    base.ActivityStartToCloseTimeout,
		ScheduleToCloseTimeout: base.ActivityScheduleToCloseTimeout,
		HeartbeatTimeout:       base.ActivityHeartbeatTimeout,
		RetryPolicy:            retryPolicy,
	})
}

func (w *BatchConsolidator) getCursorActivityRetryPolicy(cfg *config.RetryPolicy) *temporal.RetryPolicy {
	retryPolicy := w.getRetryPolicy(cfg)
	if retryPolicy != nil {
		retryPolicyCopy := *retryPolicy
		// Cursor writes are monotonic and idempotent. Unlimited attempts prevent
		// old workers on the shared task queue from exhausting retries during a
		// rolling deploy before a new worker polls the new cursor activity name.
		retryPolicyCopy.MaximumAttempts = 0
		retryPolicy = &retryPolicyCopy
	}
	return retryPolicy
}

func batchConsolidatorWindowEnd(startHeight uint64, requestedEndHeight uint64, batchSize uint64, shardSize uint64) uint64 {
	batchEnd := batchConsolidatorBatchEnd(startHeight, requestedEndHeight, batchSize)
	shardEnd := batchConsolidatorShardEnd(startHeight, shardSize)
	if shardEnd < batchEnd {
		return shardEnd
	}
	return batchEnd
}

func batchConsolidatorBatchEnd(startHeight uint64, requestedEndHeight uint64, batchSize uint64) uint64 {
	batchEnd := startHeight + batchSize
	if batchEnd < startHeight || batchEnd > requestedEndHeight {
		batchEnd = requestedEndHeight
	}
	return batchEnd
}

func batchConsolidatorObjectWindowEnd(startHeight uint64, batchEnd uint64, maxBlocks uint64) uint64 {
	windowEnd := startHeight + maxBlocks
	if windowEnd < startHeight || windowEnd > batchEnd {
		return batchEnd
	}
	return windowEnd
}

func batchConsolidatorFullObjectWindowEnd(startHeight uint64, batchEnd uint64, maxBlocks uint64) (uint64, bool) {
	windowEnd := startHeight + maxBlocks
	if windowEnd < startHeight || windowEnd > batchEnd {
		return batchEnd, false
	}
	return windowEnd, true
}

func batchConsolidatorAutoRangeHasOnlyFullObjectWindows(startHeight uint64, endHeight uint64, batchSize uint64, maxBlocks uint64, shardSize uint64) bool {
	if endHeight <= startHeight || batchSize == 0 || maxBlocks == 0 || shardSize == 0 {
		return false
	}
	for batchStart := startHeight; batchStart < endHeight; {
		batchEnd := batchConsolidatorWindowEnd(batchStart, endHeight, batchSize, shardSize)
		if batchEnd <= batchStart {
			return false
		}
		for windowStart := batchStart; windowStart < batchEnd; {
			windowEnd, fullWindow := batchConsolidatorFullObjectWindowEnd(windowStart, batchEnd, maxBlocks)
			if !fullWindow || windowEnd <= windowStart {
				return false
			}
			windowStart = windowEnd
		}
		batchStart = batchEnd
	}
	return true
}

func batchConsolidatorShardEnd(height uint64, shardSize uint64) uint64 {
	return (height/shardSize + 1) * shardSize
}
