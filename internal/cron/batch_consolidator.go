package cron

import (
	"context"
	"fmt"
	"strings"
	"time"

	"go.temporal.io/api/serviceerror"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/cadence"
	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/storage/metastorage"
	"github.com/coinbase/chainstorage/internal/utils/fxparams"
	"github.com/coinbase/chainstorage/internal/utils/log"
	"github.com/coinbase/chainstorage/internal/workflow"
)

type (
	BatchConsolidatorTaskParams struct {
		fx.In
		fxparams.Params
		Config            *config.Config
		Runtime           cadence.Runtime
		MetaStorage       metastorage.MetaStorage
		BatchConsolidator *workflow.BatchConsolidator
	}

	batchConsolidatorTask struct {
		config            *config.Config
		logger            *zap.Logger
		runtime           cadence.Runtime
		metaStorage       metastorage.MetaStorage
		batchConsolidator *workflow.BatchConsolidator
	}
)

const (
	autoConsolidateSuffix                   = "auto_consolidate"
	batchConsolidatorOpenPageSize           = 1000
	defaultBatchConsolidatorCronSpec        = "@every 30m"
	maxBatchConsolidatorWorkflowParallelism = 20
)

func NewBatchConsolidator(params BatchConsolidatorTaskParams) (Task, error) {
	return &batchConsolidatorTask{
		config:            params.Config,
		logger:            log.WithPackage(params.Logger),
		runtime:           params.Runtime,
		metaStorage:       params.MetaStorage,
		batchConsolidator: params.BatchConsolidator,
	}, nil
}

func (t *batchConsolidatorTask) Name() string {
	return "batch_consolidator"
}

func (t *batchConsolidatorTask) Spec() string {
	spec := t.config.Cron.BatchConsolidator.Spec
	if spec == "" {
		return defaultBatchConsolidatorCronSpec
	}
	return spec
}

func (t *batchConsolidatorTask) Parallelism() int64 {
	parallelism := t.config.Cron.BatchConsolidator.Parallelism
	if parallelism <= 0 {
		return 1
	}
	return parallelism
}

func (t *batchConsolidatorTask) Enabled() bool {
	return t.config.Cron.BatchConsolidator.Enabled
}

func (t *batchConsolidatorTask) DelayStartDuration() time.Duration {
	return t.config.Cron.BatchConsolidator.DelayStartDuration
}

func (t *batchConsolidatorTask) Run(ctx context.Context) error {
	consolidation := t.config.AWS.Storage.Consolidation
	if !consolidation.Enabled {
		return xerrors.New("batch_consolidator cron requires aws.storage.consolidation.enabled=true")
	}
	if consolidation.Mode != config.ConsolidationModeAutoConsolidate {
		return xerrors.Errorf("batch_consolidator cron requires consolidation mode %q, got %q", config.ConsolidationModeAutoConsolidate, consolidation.Mode)
	}
	if consolidation.MaxBlocks == 0 {
		return xerrors.New("batch_consolidator cron requires aws.storage.consolidation.max_blocks to be positive")
	}
	if consolidation.ShardSize == 0 {
		return xerrors.New("batch_consolidator cron requires aws.storage.consolidation.shard_size to be positive")
	}
	if consolidation.MaxBlocks > consolidation.ShardSize {
		return xerrors.Errorf(
			"batch_consolidator cron consolidation max_blocks(%d) must be <= shard_size(%d)",
			consolidation.MaxBlocks,
			consolidation.ShardSize,
		)
	}

	cronConfig := t.config.Cron.BatchConsolidator
	if cronConfig.WorkflowParallelism < 0 {
		return xerrors.Errorf("batch_consolidator cron workflow_parallelism(%d) must be non-negative", cronConfig.WorkflowParallelism)
	}
	if cronConfig.WorkflowParallelism > maxBatchConsolidatorWorkflowParallelism {
		return xerrors.Errorf(
			"batch_consolidator cron workflow_parallelism(%d) exceeds max(%d)",
			cronConfig.WorkflowParallelism,
			maxBatchConsolidatorWorkflowParallelism,
		)
	}

	workflowID := t.autoConsolidateWorkflowID()
	openWorkflowID, open, err := t.openBatchConsolidatorWorkflow(ctx)
	if err != nil {
		return err
	}
	if open {
		t.logger.Info(
			"batch_consolidator cron skipped because a batch_consolidator workflow is already open",
			zap.String("open_workflow_id", openWorkflowID),
			zap.String("auto_workflow_id", workflowID),
		)
		return nil
	}

	tag := t.config.GetEffectiveBlockTag(0)
	searchStart := cronConfig.StartHeight
	cursorHeight, cursorFound, err := t.metaStorage.GetBlockConsolidationCursor(ctx, metastorage.BatchConsolidatorAutoConsolidateCursor, tag)
	if err != nil {
		return xerrors.Errorf("failed to get auto_consolidate cursor: %w", err)
	}
	if cursorFound && cursorHeight > searchStart {
		searchStart = cursorHeight
	}
	latest, err := t.metaStorage.GetLatestBlock(ctx, tag)
	if err != nil {
		return xerrors.Errorf("failed to get latest block for batch_consolidator cron: %w", err)
	}
	if latest == nil {
		return xerrors.New("latest block not found for batch_consolidator cron")
	}
	searchEnd, safeHeight, ok := batchConsolidatorCronSafeEndHeight(latest.GetHeight(), t.config.Chain.IrreversibleDistance)
	if !ok {
		t.logger.Info(
			"batch_consolidator cron has no irreversible consolidation range",
			zap.Uint32("tag", tag),
			zap.Uint64("latest_height", latest.GetHeight()),
			zap.Uint64("irreversible_distance", t.config.Chain.IrreversibleDistance),
		)
		return nil
	}
	if searchEnd <= searchStart {
		t.logger.Info(
			"batch_consolidator cron safe range is below configured start height",
			zap.Uint32("tag", tag),
			zap.Uint64("start_height", searchStart),
			zap.Uint64("configured_start_height", cronConfig.StartHeight),
			zap.Bool("cursor_found", cursorFound),
			zap.Uint64("cursor_height", cursorHeight),
			zap.Uint64("safe_end_height", searchEnd),
			zap.Uint64("latest_height", latest.GetHeight()),
			zap.Uint64("safe_consolidation_height", safeHeight),
		)
		return nil
	}

	if cursorFound {
		repairScanStart := cronConfig.StartHeight
		if cursorHeight > consolidation.ShardSize && cursorHeight-consolidation.ShardSize > repairScanStart {
			repairScanStart = cursorHeight - consolidation.ShardSize
		}
		if repairScanStart < searchStart {
			repairStart, repairFound, err := t.firstAutoConsolidateWindowStart(
				ctx,
				tag,
				repairScanStart,
				searchStart,
				cronConfig.StartHeight,
				consolidation.MaxBlocks,
				consolidation.ShardSize,
			)
			if err != nil {
				return err
			}
			if repairFound {
				searchStart = repairStart
			}
		}
	} else {
		bootstrapStart, bootstrapFound, err := t.firstAutoConsolidateWindowStart(
			ctx,
			tag,
			cronConfig.StartHeight,
			searchEnd,
			cronConfig.StartHeight,
			consolidation.MaxBlocks,
			consolidation.ShardSize,
		)
		if err != nil {
			return err
		}
		if !bootstrapFound {
			cursorEnd, cursorEndFound := batchConsolidatorCronRangeEnd(
				cronConfig.StartHeight,
				searchEnd,
				consolidation.MaxBlocks,
				consolidation.ShardSize,
			)
			if !cursorEndFound {
				t.logger.Info(
					"batch_consolidator cron found no missing consolidation shadow and no full window to bootstrap cursor",
					zap.Uint32("tag", tag),
					zap.Uint64("configured_start_height", cronConfig.StartHeight),
					zap.Uint64("safe_end_height", searchEnd),
					zap.Uint64("latest_height", latest.GetHeight()),
					zap.Uint64("safe_consolidation_height", safeHeight),
					zap.Uint64("consolidation_max_blocks", consolidation.MaxBlocks),
				)
				return nil
			}
			if err := t.verifyAutoConsolidateCoverage(ctx, tag, cronConfig.StartHeight, cursorEnd); err != nil {
				t.logger.Info(
					"batch_consolidator cron skipped cursor bootstrap because already consolidated range did not pass coverage verification",
					zap.Uint32("tag", tag),
					zap.Uint64("configured_start_height", cronConfig.StartHeight),
					zap.Uint64("cursor_height", cursorEnd),
					zap.Error(err),
				)
				return nil
			}
			if err := t.metaStorage.SetBlockConsolidationCursor(ctx, metastorage.BatchConsolidatorAutoConsolidateCursor, tag, cursorEnd); err != nil {
				return xerrors.Errorf("failed to bootstrap auto_consolidate cursor to height %d: %w", cursorEnd, err)
			}
			t.logger.Info(
				"batch_consolidator cron bootstrapped cursor from already consolidated range",
				zap.Uint32("tag", tag),
				zap.Uint64("configured_start_height", cronConfig.StartHeight),
				zap.Uint64("cursor_height", cursorEnd),
				zap.Uint64("safe_end_height", searchEnd),
				zap.Uint64("latest_height", latest.GetHeight()),
				zap.Uint64("safe_consolidation_height", safeHeight),
			)
			return nil
		}
		searchStart = bootstrapStart
	}

	normalizedStart := batchConsolidatorCronFullWindowStartAtOrAfter(searchStart, consolidation.MaxBlocks, consolidation.ShardSize)
	if normalizedStart != searchStart {
		t.logger.Info(
			"batch_consolidator cron advanced start height to next full consolidation window",
			zap.Uint32("tag", tag),
			zap.Uint64("start_height", searchStart),
			zap.Uint64("normalized_start_height", normalizedStart),
			zap.Uint64("consolidation_max_blocks", consolidation.MaxBlocks),
			zap.Uint64("shard_size", consolidation.ShardSize),
		)
		searchStart = normalizedStart
	}

	startHeight := searchStart
	endHeight, found := batchConsolidatorCronRangeEnd(
		searchStart,
		searchEnd,
		consolidation.MaxBlocks,
		consolidation.ShardSize,
	)
	if !found {
		t.logger.Info(
			"batch_consolidator cron found no full consolidation window",
			zap.Uint32("tag", tag),
			zap.Uint64("start_height", searchStart),
			zap.Uint64("configured_start_height", cronConfig.StartHeight),
			zap.Bool("cursor_found", cursorFound),
			zap.Uint64("cursor_height", cursorHeight),
			zap.Uint64("end_height", searchEnd),
			zap.Uint64("latest_height", latest.GetHeight()),
			zap.Uint64("safe_consolidation_height", safeHeight),
			zap.Uint64("consolidation_max_blocks", consolidation.MaxBlocks),
		)
		return nil
	}

	request := &workflow.BatchConsolidatorRequest{
		Mode:        config.ConsolidationModeAutoConsolidate,
		Tag:         tag,
		StartHeight: startHeight,
		EndHeight:   endHeight,
	}
	if cronConfig.WorkflowParallelism > 0 {
		request.Parallelism = cronConfig.WorkflowParallelism
	}
	workflowCtx := workflow.WithWorkflowID(ctx, workflowID)
	run, err := t.batchConsolidator.Execute(workflowCtx, request)
	if err != nil {
		if isWorkflowAlreadyStarted(err) {
			t.logger.Info("batch_consolidator cron skipped because auto consolidation workflow was already started", zap.String("workflow_id", workflowID))
			return nil
		}
		return xerrors.Errorf("failed to start batch_consolidator cron workflow: %w", err)
	}
	t.logger.Info(
		"started batch_consolidator cron workflow",
		zap.String("workflow_id", workflowID),
		zap.String("run_id", run.GetRunID()),
		zap.Uint32("tag", tag),
		zap.Uint64("start_height", startHeight),
		zap.Uint64("end_height", endHeight),
		zap.Uint64("configured_start_height", cronConfig.StartHeight),
		zap.Bool("cursor_found", cursorFound),
		zap.Uint64("cursor_height", cursorHeight),
		zap.Uint64("latest_height", latest.GetHeight()),
		zap.Uint64("safe_consolidation_height", safeHeight),
		zap.Int("workflow_parallelism", request.Parallelism),
	)
	return nil
}

func (t *batchConsolidatorTask) autoConsolidateWorkflowID() string {
	return fmt.Sprintf("%s/%s", t.config.Workflows.BatchConsolidator.WorkflowIdentity, autoConsolidateSuffix)
}

func (t *batchConsolidatorTask) firstAutoConsolidateWindowStart(
	ctx context.Context,
	tag uint32,
	searchStart uint64,
	searchEnd uint64,
	lowerBound uint64,
	consolidationWindowBlocks uint64,
	shardSize uint64,
) (uint64, bool, error) {
	if searchEnd <= searchStart {
		return 0, false, nil
	}
	missingHeight, found, err := t.metaStorage.GetFirstBlockMissingConsolidationShadow(ctx, tag, searchStart, searchEnd)
	if err != nil {
		return 0, false, xerrors.Errorf("failed to get first block missing consolidation shadow: %w", err)
	}
	if !found {
		return 0, false, nil
	}
	windowStart := batchConsolidatorCronObjectWindowStart(missingHeight, consolidationWindowBlocks, shardSize)
	if windowStart < lowerBound {
		windowStart = batchConsolidatorCronFullWindowStartAtOrAfter(lowerBound, consolidationWindowBlocks, shardSize)
	}
	if windowStart >= searchEnd {
		return 0, false, nil
	}
	return windowStart, true, nil
}

func (t *batchConsolidatorTask) verifyAutoConsolidateCoverage(ctx context.Context, tag uint32, startHeight uint64, endHeight uint64) error {
	if endHeight <= startHeight {
		return xerrors.Errorf("invalid auto_consolidate coverage range [%d, %d)", startHeight, endHeight)
	}
	stats, err := t.metaStorage.GetBlockConsolidationShadowStats(ctx, tag, startHeight, endHeight)
	if err != nil {
		return xerrors.Errorf("failed to get auto_consolidate coverage stats for range [%d, %d): %w", startHeight, endHeight, err)
	}
	eligibleBlocks := uint64(0)
	shadowBlocks := uint64(0)
	if stats != nil {
		eligibleBlocks = stats.EligibleBlocks
		shadowBlocks = stats.Blocks
	}
	if shadowBlocks != eligibleBlocks {
		return xerrors.Errorf("auto_consolidate coverage mismatch for range [%d, %d): expected %d eligible blocks, got %d shadow blocks", startHeight, endHeight, eligibleBlocks, shadowBlocks)
	}
	return nil
}

func (t *batchConsolidatorTask) openBatchConsolidatorWorkflow(ctx context.Context) (string, bool, error) {
	workflowIdentity := t.config.Workflows.BatchConsolidator.WorkflowIdentity
	defaultTaskList := t.config.Workflows.BatchConsolidator.TaskList
	historicalTaskList := t.config.Workflows.BatchConsolidator.HistoricalTaskList
	hasIsolatedHistoricalTaskList := historicalTaskList != "" && historicalTaskList != defaultTaskList
	openWorkflows, err := t.runtime.ListOpenWorkflows(ctx, t.config.Cadence.Domain, batchConsolidatorOpenPageSize, workflowIdentity)
	if err != nil {
		return "", false, xerrors.Errorf("failed to list open workflows for batch_consolidator cron: %w", err)
	}
	if openWorkflows == nil {
		return "", false, nil
	}
	for _, wf := range openWorkflows.Executions {
		if wf.GetType().GetName() == workflowIdentity {
			// Only the dedicated historical queue is resource-isolated. Missing or
			// shared task-queue metadata remains blocking so the guard fails closed.
			if hasIsolatedHistoricalTaskList && wf.GetTaskQueue() == historicalTaskList {
				continue
			}
			workflowID := wf.GetExecution().GetWorkflowId()
			return workflowID, true, nil
		}
	}
	return "", false, nil
}

func batchConsolidatorCronSafeEndHeight(latestHeight uint64, irreversibleDistance uint64) (uint64, uint64, bool) {
	if latestHeight < irreversibleDistance {
		return 0, 0, false
	}
	safeHeight := latestHeight - irreversibleDistance
	if safeHeight == ^uint64(0) {
		return safeHeight, safeHeight, true
	}
	return safeHeight + 1, safeHeight, true
}

func batchConsolidatorCronRangeEnd(startHeight uint64, safeEndHeight uint64, consolidationWindowBlocks uint64, shardSize uint64) (uint64, bool) {
	if safeEndHeight <= startHeight || consolidationWindowBlocks == 0 || shardSize == 0 {
		return 0, false
	}
	rangeLimit := safeEndHeight
	endHeight := startHeight
	for {
		nextEnd := endHeight + consolidationWindowBlocks
		if nextEnd < endHeight || nextEnd > rangeLimit || nextEnd > batchConsolidatorCronShardEnd(endHeight, shardSize) {
			break
		}
		endHeight = nextEnd
	}
	if endHeight == startHeight {
		return 0, false
	}
	return endHeight, true
}

func batchConsolidatorCronObjectWindowStart(height uint64, consolidationWindowBlocks uint64, shardSize uint64) uint64 {
	if consolidationWindowBlocks == 0 || shardSize == 0 {
		return height
	}
	shardStart := (height / shardSize) * shardSize
	offset := height - shardStart
	return shardStart + (offset/consolidationWindowBlocks)*consolidationWindowBlocks
}

func batchConsolidatorCronFullWindowStartAtOrAfter(startHeight uint64, consolidationWindowBlocks uint64, shardSize uint64) uint64 {
	if consolidationWindowBlocks == 0 || shardSize == 0 {
		return startHeight
	}
	windowEnd := startHeight + consolidationWindowBlocks
	if windowEnd >= startHeight && windowEnd <= batchConsolidatorCronShardEnd(startHeight, shardSize) {
		return startHeight
	}
	return batchConsolidatorCronShardEnd(startHeight, shardSize)
}

func batchConsolidatorCronShardEnd(height uint64, shardSize uint64) uint64 {
	return (height/shardSize + 1) * shardSize
}

func isWorkflowAlreadyStarted(err error) bool {
	var alreadyStarted *serviceerror.WorkflowExecutionAlreadyStarted
	if xerrors.As(err, &alreadyStarted) {
		return true
	}
	return strings.Contains(strings.ToLower(err.Error()), "already started")
}
