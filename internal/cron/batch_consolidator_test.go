package cron

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally/v4"
	commonpb "go.temporal.io/api/common/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/workflow"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"

	"github.com/coinbase/chainstorage/internal/cadence"
	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/storage/metastorage"
	metastoragemocks "github.com/coinbase/chainstorage/internal/storage/metastorage/mocks"
	"github.com/coinbase/chainstorage/internal/utils/fxparams"
	"github.com/coinbase/chainstorage/internal/utils/timesource"
	workflowpkg "github.com/coinbase/chainstorage/internal/workflow"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

type (
	batchConsolidatorCronRuntime struct {
		logger                *zap.Logger
		openWorkflowID        []string
		openWorkflowTaskQueue map[string]string
		requestedWorkflowType string
		executions            []batchConsolidatorCronExecution
	}

	batchConsolidatorCronExecution struct {
		options client.StartWorkflowOptions
		request any
	}

	batchConsolidatorCronRun struct {
		id    string
		runID string
	}
)

var _ cadence.Runtime = (*batchConsolidatorCronRuntime)(nil)

func TestBatchConsolidatorCronStartsFullWindowAutoConsolidateWorkflow(t *testing.T) {
	task, runtime, metaStorage, cfg, ctrl := newBatchConsolidatorCronTask(t)
	defer ctrl.Finish()
	cfg.Cron.BatchConsolidator.StartHeight = 1_000

	tag := cfg.GetEffectiveBlockTag(0)
	metaStorage.EXPECT().
		GetBlockConsolidationCursor(gomock.Any(), metastorage.BatchConsolidatorAutoConsolidateCursor, tag).
		Return(uint64(0), false, nil)
	metaStorage.EXPECT().
		GetLatestBlock(gomock.Any(), tag).
		Return(&api.BlockMetadata{Tag: tag, Height: 5_000}, nil)
	metaStorage.EXPECT().
		GetFirstBlockMissingConsolidationShadow(gomock.Any(), tag, uint64(1_000), uint64(4_901)).
		Return(uint64(3_500), true, nil)

	require.NoError(t, task.Run(context.Background()))
	require.Len(t, runtime.executions, 1)
	require.Equal(t, "workflow.batch_consolidator/auto_consolidate", runtime.executions[0].options.ID)
	require.Equal(t, "default", runtime.executions[0].options.TaskQueue)
	require.Equal(t, &workflowpkg.BatchConsolidatorRequest{
		Mode:        config.ConsolidationModeAutoConsolidate,
		Tag:         tag,
		StartHeight: 3_000,
		EndHeight:   4_000,
	}, runtime.executions[0].request)
}

func TestBatchConsolidatorRoutesHistoricalWorkflowToDedicatedTaskQueue(t *testing.T) {
	task, runtime, _, cfg, ctrl := newBatchConsolidatorCronTask(t)
	defer ctrl.Finish()
	cfg.Workflows.BatchConsolidator.TaskList = "batch_consolidator"
	cfg.Workflows.BatchConsolidator.HistoricalTaskList = "batch_consolidator_backfill"

	_, err := task.batchConsolidator.Execute(context.Background(), &workflowpkg.BatchConsolidatorRequest{
		Mode:        config.ConsolidationModeHistoricalBackfill,
		StartHeight: 100,
		EndHeight:   200,
	})
	require.NoError(t, err)
	require.Len(t, runtime.executions, 1)
	require.Equal(t, "batch_consolidator_backfill", runtime.executions[0].options.TaskQueue)

	_, err = task.batchConsolidator.Execute(context.Background(), &workflowpkg.BatchConsolidatorRequest{
		Mode:        config.ConsolidationModeAutoConsolidate,
		StartHeight: 200,
		EndHeight:   300,
	})
	require.NoError(t, err)
	require.Len(t, runtime.executions, 2)
	require.Equal(t, "batch_consolidator", runtime.executions[1].options.TaskQueue)
}

func TestBatchConsolidatorCronRunsToNearestSafeFullWindow(t *testing.T) {
	task, runtime, metaStorage, cfg, ctrl := newBatchConsolidatorCronTask(t)
	defer ctrl.Finish()
	cfg.Cron.BatchConsolidator.StartHeight = 7_000
	cfg.Chain.IrreversibleDistance = 32

	tag := cfg.GetEffectiveBlockTag(0)
	metaStorage.EXPECT().
		GetBlockConsolidationCursor(gomock.Any(), metastorage.BatchConsolidatorAutoConsolidateCursor, tag).
		Return(uint64(0), false, nil)
	metaStorage.EXPECT().
		GetLatestBlock(gomock.Any(), tag).
		Return(&api.BlockMetadata{Tag: tag, Height: 20_500}, nil)
	metaStorage.EXPECT().
		GetFirstBlockMissingConsolidationShadow(gomock.Any(), tag, uint64(7_000), uint64(20_469)).
		Return(uint64(7_000), true, nil)

	require.NoError(t, task.Run(context.Background()))
	require.Len(t, runtime.executions, 1)
	require.Equal(t, &workflowpkg.BatchConsolidatorRequest{
		Mode:        config.ConsolidationModeAutoConsolidate,
		Tag:         tag,
		StartHeight: 7_000,
		EndHeight:   20_000,
	}, runtime.executions[0].request)
}

func TestBatchConsolidatorCronPassesWorkflowParallelism(t *testing.T) {
	task, runtime, metaStorage, cfg, ctrl := newBatchConsolidatorCronTask(t)
	defer ctrl.Finish()
	cfg.Cron.BatchConsolidator.StartHeight = 1_000
	cfg.Cron.BatchConsolidator.WorkflowParallelism = 20

	tag := cfg.GetEffectiveBlockTag(0)
	metaStorage.EXPECT().
		GetBlockConsolidationCursor(gomock.Any(), metastorage.BatchConsolidatorAutoConsolidateCursor, tag).
		Return(uint64(0), false, nil)
	metaStorage.EXPECT().
		GetLatestBlock(gomock.Any(), tag).
		Return(&api.BlockMetadata{Tag: tag, Height: 5_000}, nil)
	metaStorage.EXPECT().
		GetFirstBlockMissingConsolidationShadow(gomock.Any(), tag, uint64(1_000), uint64(4_901)).
		Return(uint64(3_500), true, nil)

	require.NoError(t, task.Run(context.Background()))
	require.Len(t, runtime.executions, 1)
	require.Equal(t, &workflowpkg.BatchConsolidatorRequest{
		Mode:        config.ConsolidationModeAutoConsolidate,
		Tag:         tag,
		StartHeight: 3_000,
		EndHeight:   4_000,
		Parallelism: 20,
	}, runtime.executions[0].request)
}

func TestBatchConsolidatorCronWaitsForFullConsolidationWindow(t *testing.T) {
	task, runtime, metaStorage, cfg, ctrl := newBatchConsolidatorCronTask(t)
	defer ctrl.Finish()
	cfg.Cron.BatchConsolidator.StartHeight = 1_000

	tag := cfg.GetEffectiveBlockTag(0)
	metaStorage.EXPECT().
		GetBlockConsolidationCursor(gomock.Any(), metastorage.BatchConsolidatorAutoConsolidateCursor, tag).
		Return(uint64(0), false, nil)
	metaStorage.EXPECT().
		GetLatestBlock(gomock.Any(), tag).
		Return(&api.BlockMetadata{Tag: tag, Height: 2_098}, nil)
	metaStorage.EXPECT().
		GetFirstBlockMissingConsolidationShadow(gomock.Any(), tag, uint64(1_000), uint64(1_999)).
		Return(uint64(1_000), true, nil)

	require.NoError(t, task.Run(context.Background()))
	require.Empty(t, runtime.executions)
}

func TestBatchConsolidatorCronUsesPersistedCursor(t *testing.T) {
	task, runtime, metaStorage, cfg, ctrl := newBatchConsolidatorCronTask(t)
	defer ctrl.Finish()
	cfg.Cron.BatchConsolidator.StartHeight = 1_000

	tag := cfg.GetEffectiveBlockTag(0)
	metaStorage.EXPECT().
		GetBlockConsolidationCursor(gomock.Any(), metastorage.BatchConsolidatorAutoConsolidateCursor, tag).
		Return(uint64(10_000), true, nil)
	metaStorage.EXPECT().
		GetLatestBlock(gomock.Any(), tag).
		Return(&api.BlockMetadata{Tag: tag, Height: 12_000}, nil)
	metaStorage.EXPECT().
		GetFirstBlockMissingConsolidationShadow(gomock.Any(), tag, uint64(1_000), uint64(10_000)).
		Return(uint64(0), false, nil)

	require.NoError(t, task.Run(context.Background()))
	require.Len(t, runtime.executions, 1)
	require.Equal(t, &workflowpkg.BatchConsolidatorRequest{
		Mode:        config.ConsolidationModeAutoConsolidate,
		Tag:         tag,
		StartHeight: 10_000,
		EndHeight:   11_000,
	}, runtime.executions[0].request)
}

func TestBatchConsolidatorCronNormalizesCursorShardTail(t *testing.T) {
	task, runtime, metaStorage, cfg, ctrl := newBatchConsolidatorCronTask(t)
	defer ctrl.Finish()
	cfg.Cron.BatchConsolidator.StartHeight = 1_000

	tag := cfg.GetEffectiveBlockTag(0)
	metaStorage.EXPECT().
		GetBlockConsolidationCursor(gomock.Any(), metastorage.BatchConsolidatorAutoConsolidateCursor, tag).
		Return(uint64(9_500), true, nil)
	metaStorage.EXPECT().
		GetLatestBlock(gomock.Any(), tag).
		Return(&api.BlockMetadata{Tag: tag, Height: 12_000}, nil)
	metaStorage.EXPECT().
		GetFirstBlockMissingConsolidationShadow(gomock.Any(), tag, uint64(1_000), uint64(9_500)).
		Return(uint64(0), false, nil)

	require.NoError(t, task.Run(context.Background()))
	require.Len(t, runtime.executions, 1)
	require.Equal(t, &workflowpkg.BatchConsolidatorRequest{
		Mode:        config.ConsolidationModeAutoConsolidate,
		Tag:         tag,
		StartHeight: 10_000,
		EndHeight:   11_000,
	}, runtime.executions[0].request)
}

func TestBatchConsolidatorCronRepairsWithinCursorLookback(t *testing.T) {
	task, runtime, metaStorage, cfg, ctrl := newBatchConsolidatorCronTask(t)
	defer ctrl.Finish()
	cfg.Cron.BatchConsolidator.StartHeight = 1_000

	tag := cfg.GetEffectiveBlockTag(0)
	metaStorage.EXPECT().
		GetBlockConsolidationCursor(gomock.Any(), metastorage.BatchConsolidatorAutoConsolidateCursor, tag).
		Return(uint64(20_000), true, nil)
	metaStorage.EXPECT().
		GetLatestBlock(gomock.Any(), tag).
		Return(&api.BlockMetadata{Tag: tag, Height: 25_000}, nil)
	metaStorage.EXPECT().
		GetFirstBlockMissingConsolidationShadow(gomock.Any(), tag, uint64(10_000), uint64(20_000)).
		Return(uint64(18_500), true, nil)

	require.NoError(t, task.Run(context.Background()))
	require.Len(t, runtime.executions, 1)
	require.Equal(t, &workflowpkg.BatchConsolidatorRequest{
		Mode:        config.ConsolidationModeAutoConsolidate,
		Tag:         tag,
		StartHeight: 18_000,
		EndHeight:   24_000,
	}, runtime.executions[0].request)
}

func TestBatchConsolidatorCronDoesNotCapAutoConsolidateAtPromotionGate(t *testing.T) {
	task, runtime, metaStorage, cfg, ctrl := newBatchConsolidatorCronTask(t)
	defer ctrl.Finish()
	gateHeight := uint64(1_600)
	cfg.AWS.Storage.Consolidation.PromotionGateHeight = &gateHeight
	cfg.Cron.BatchConsolidator.StartHeight = 1_000

	tag := cfg.GetEffectiveBlockTag(0)
	metaStorage.EXPECT().
		GetBlockConsolidationCursor(gomock.Any(), metastorage.BatchConsolidatorAutoConsolidateCursor, tag).
		Return(uint64(0), false, nil)
	metaStorage.EXPECT().
		GetLatestBlock(gomock.Any(), tag).
		Return(&api.BlockMetadata{Tag: tag, Height: 5_000}, nil)
	metaStorage.EXPECT().
		GetFirstBlockMissingConsolidationShadow(gomock.Any(), tag, uint64(1_000), uint64(4_901)).
		Return(uint64(1_000), true, nil)

	require.NoError(t, task.Run(context.Background()))
	require.Len(t, runtime.executions, 1)
	require.Equal(t, &workflowpkg.BatchConsolidatorRequest{
		Mode:        config.ConsolidationModeAutoConsolidate,
		Tag:         tag,
		StartHeight: 1_000,
		EndHeight:   4_000,
	}, runtime.executions[0].request)
}

func TestBatchConsolidatorCronWaitsWhenCursorHasNoFullNewWindow(t *testing.T) {
	task, runtime, metaStorage, cfg, ctrl := newBatchConsolidatorCronTask(t)
	defer ctrl.Finish()
	cfg.Cron.BatchConsolidator.StartHeight = 1_000

	tag := cfg.GetEffectiveBlockTag(0)
	metaStorage.EXPECT().
		GetBlockConsolidationCursor(gomock.Any(), metastorage.BatchConsolidatorAutoConsolidateCursor, tag).
		Return(uint64(1_000), true, nil)
	metaStorage.EXPECT().
		GetLatestBlock(gomock.Any(), tag).
		Return(&api.BlockMetadata{Tag: tag, Height: 2_000}, nil)
	metaStorage.EXPECT().
		GetFirstBlockMissingConsolidationShadow(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Times(0)

	require.NoError(t, task.Run(context.Background()))
	require.Empty(t, runtime.executions)
}

func TestBatchConsolidatorCronBootstrapsCursorWhenNoMissingShadow(t *testing.T) {
	task, runtime, metaStorage, cfg, ctrl := newBatchConsolidatorCronTask(t)
	defer ctrl.Finish()
	cfg.Cron.BatchConsolidator.StartHeight = 1_000

	tag := cfg.GetEffectiveBlockTag(0)
	metaStorage.EXPECT().
		GetBlockConsolidationCursor(gomock.Any(), metastorage.BatchConsolidatorAutoConsolidateCursor, tag).
		Return(uint64(0), false, nil)
	metaStorage.EXPECT().
		GetLatestBlock(gomock.Any(), tag).
		Return(&api.BlockMetadata{Tag: tag, Height: 12_000}, nil)
	metaStorage.EXPECT().
		GetFirstBlockMissingConsolidationShadow(gomock.Any(), tag, uint64(1_000), uint64(11_901)).
		Return(uint64(0), false, nil)
	metaStorage.EXPECT().
		GetBlockConsolidationShadowStats(gomock.Any(), tag, uint64(1_000), uint64(11_000)).
		Return(&metastorage.ConsolidationShadowStats{Objects: 10, Blocks: 10_000, EligibleBlocks: 10_000}, nil)
	metaStorage.EXPECT().
		SetBlockConsolidationCursor(gomock.Any(), metastorage.BatchConsolidatorAutoConsolidateCursor, tag, uint64(11_000)).
		Return(nil)

	require.NoError(t, task.Run(context.Background()))
	require.Empty(t, runtime.executions)
}

func TestBatchConsolidatorCronBootstrapsCursorWithSkippedBlocks(t *testing.T) {
	task, runtime, metaStorage, cfg, ctrl := newBatchConsolidatorCronTask(t)
	defer ctrl.Finish()
	cfg.Cron.BatchConsolidator.StartHeight = 1_000

	tag := cfg.GetEffectiveBlockTag(0)
	metaStorage.EXPECT().
		GetBlockConsolidationCursor(gomock.Any(), metastorage.BatchConsolidatorAutoConsolidateCursor, tag).
		Return(uint64(0), false, nil)
	metaStorage.EXPECT().
		GetLatestBlock(gomock.Any(), tag).
		Return(&api.BlockMetadata{Tag: tag, Height: 12_000}, nil)
	metaStorage.EXPECT().
		GetFirstBlockMissingConsolidationShadow(gomock.Any(), tag, uint64(1_000), uint64(11_901)).
		Return(uint64(0), false, nil)
	metaStorage.EXPECT().
		GetBlockConsolidationShadowStats(gomock.Any(), tag, uint64(1_000), uint64(11_000)).
		Return(&metastorage.ConsolidationShadowStats{Objects: 10, Blocks: 9_990, EligibleBlocks: 9_990}, nil)
	metaStorage.EXPECT().
		SetBlockConsolidationCursor(gomock.Any(), metastorage.BatchConsolidatorAutoConsolidateCursor, tag, uint64(11_000)).
		Return(nil)

	require.NoError(t, task.Run(context.Background()))
	require.Empty(t, runtime.executions)
}

func TestBatchConsolidatorCronDoesNotBootstrapCursorWithoutFullCoverage(t *testing.T) {
	task, runtime, metaStorage, cfg, ctrl := newBatchConsolidatorCronTask(t)
	defer ctrl.Finish()
	cfg.Cron.BatchConsolidator.StartHeight = 1_000

	tag := cfg.GetEffectiveBlockTag(0)
	metaStorage.EXPECT().
		GetBlockConsolidationCursor(gomock.Any(), metastorage.BatchConsolidatorAutoConsolidateCursor, tag).
		Return(uint64(0), false, nil)
	metaStorage.EXPECT().
		GetLatestBlock(gomock.Any(), tag).
		Return(&api.BlockMetadata{Tag: tag, Height: 12_000}, nil)
	metaStorage.EXPECT().
		GetFirstBlockMissingConsolidationShadow(gomock.Any(), tag, uint64(1_000), uint64(11_901)).
		Return(uint64(0), false, nil)
	metaStorage.EXPECT().
		GetBlockConsolidationShadowStats(gomock.Any(), tag, uint64(1_000), uint64(11_000)).
		Return(&metastorage.ConsolidationShadowStats{Objects: 9, Blocks: 9_999, EligibleBlocks: 10_000}, nil)
	metaStorage.EXPECT().
		SetBlockConsolidationCursor(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Times(0)

	require.NoError(t, task.Run(context.Background()))
	require.Empty(t, runtime.executions)
}

func TestBatchConsolidatorCronSkipsWhenBatchConsolidatorWorkflowIsAlreadyOpen(t *testing.T) {
	task, runtime, metaStorage, cfg, ctrl := newBatchConsolidatorCronTask(t)
	defer ctrl.Finish()
	runtime.openWorkflowID = []string{"custom-manual-promote-workflow-id"}
	cfg.Cron.BatchConsolidator.StartHeight = 1_000

	metaStorage.EXPECT().GetBlockConsolidationCursor(gomock.Any(), gomock.Any(), gomock.Any()).Times(0)
	metaStorage.EXPECT().GetLatestBlock(gomock.Any(), gomock.Any()).Times(0)
	metaStorage.EXPECT().GetFirstBlockMissingConsolidationShadow(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(0)

	require.NoError(t, task.Run(context.Background()))
	require.Equal(t, "workflow.batch_consolidator", runtime.requestedWorkflowType)
	require.Empty(t, runtime.executions)
}

func TestBatchConsolidatorCronRunsWhileHistoricalBackfillIsOnDedicatedTaskQueue(t *testing.T) {
	task, runtime, metaStorage, cfg, ctrl := newBatchConsolidatorCronTask(t)
	defer ctrl.Finish()
	cfg.Workflows.BatchConsolidator.TaskList = "batch_consolidator"
	cfg.Workflows.BatchConsolidator.HistoricalTaskList = "batch_consolidator_backfill"
	cfg.Cron.BatchConsolidator.StartHeight = 1_000
	runtime.openWorkflowID = []string{"workflow.batch_consolidator/historical_backfill"}
	runtime.openWorkflowTaskQueue = map[string]string{
		"workflow.batch_consolidator/historical_backfill": "batch_consolidator_backfill",
	}

	tag := cfg.GetEffectiveBlockTag(0)
	metaStorage.EXPECT().
		GetBlockConsolidationCursor(gomock.Any(), metastorage.BatchConsolidatorAutoConsolidateCursor, tag).
		Return(uint64(0), false, nil)
	metaStorage.EXPECT().
		GetLatestBlock(gomock.Any(), tag).
		Return(&api.BlockMetadata{Tag: tag, Height: 5_000}, nil)
	metaStorage.EXPECT().
		GetFirstBlockMissingConsolidationShadow(gomock.Any(), tag, uint64(1_000), uint64(4_901)).
		Return(uint64(3_500), true, nil)

	require.NoError(t, task.Run(context.Background()))
	require.Len(t, runtime.executions, 1)
	require.Equal(t, "batch_consolidator", runtime.executions[0].options.TaskQueue)
}

func TestBatchConsolidatorCronBlocksHistoricalBackfillOnSharedTaskQueue(t *testing.T) {
	task, runtime, metaStorage, cfg, ctrl := newBatchConsolidatorCronTask(t)
	defer ctrl.Finish()
	cfg.Workflows.BatchConsolidator.TaskList = "batch_consolidator"
	cfg.Workflows.BatchConsolidator.HistoricalTaskList = "batch_consolidator_backfill"
	runtime.openWorkflowID = []string{"workflow.batch_consolidator/historical_backfill"}
	runtime.openWorkflowTaskQueue = map[string]string{
		"workflow.batch_consolidator/historical_backfill": "batch_consolidator",
	}

	metaStorage.EXPECT().GetBlockConsolidationCursor(gomock.Any(), gomock.Any(), gomock.Any()).Times(0)
	metaStorage.EXPECT().GetLatestBlock(gomock.Any(), gomock.Any()).Times(0)
	metaStorage.EXPECT().GetFirstBlockMissingConsolidationShadow(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(0)

	require.NoError(t, task.Run(context.Background()))
	require.Empty(t, runtime.executions)
}

func TestBatchConsolidatorCronStillBlocksAutoWorkflowAfterIgnoringDedicatedBackfill(t *testing.T) {
	task, runtime, metaStorage, cfg, ctrl := newBatchConsolidatorCronTask(t)
	defer ctrl.Finish()
	cfg.Workflows.BatchConsolidator.TaskList = "batch_consolidator"
	cfg.Workflows.BatchConsolidator.HistoricalTaskList = "batch_consolidator_backfill"
	runtime.openWorkflowID = []string{
		"workflow.batch_consolidator/historical_backfill",
		"workflow.batch_consolidator/auto_consolidate",
	}
	runtime.openWorkflowTaskQueue = map[string]string{
		"workflow.batch_consolidator/historical_backfill": "batch_consolidator_backfill",
		"workflow.batch_consolidator/auto_consolidate":    "batch_consolidator",
	}

	metaStorage.EXPECT().GetBlockConsolidationCursor(gomock.Any(), gomock.Any(), gomock.Any()).Times(0)
	metaStorage.EXPECT().GetLatestBlock(gomock.Any(), gomock.Any()).Times(0)
	metaStorage.EXPECT().GetFirstBlockMissingConsolidationShadow(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(0)

	require.NoError(t, task.Run(context.Background()))
	require.Empty(t, runtime.executions)
}

func TestBatchConsolidatorCronRequiresAutoConsolidateMode(t *testing.T) {
	task, runtime, metaStorage, cfg, ctrl := newBatchConsolidatorCronTask(t)
	defer ctrl.Finish()
	cfg.AWS.Storage.Consolidation.Mode = config.ConsolidationModePromoteFinalized

	metaStorage.EXPECT().GetBlockConsolidationCursor(gomock.Any(), gomock.Any(), gomock.Any()).Times(0)
	metaStorage.EXPECT().GetLatestBlock(gomock.Any(), gomock.Any()).Times(0)
	metaStorage.EXPECT().GetFirstBlockMissingConsolidationShadow(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(0)

	err := task.Run(context.Background())
	require.Error(t, err)
	require.Contains(t, err.Error(), `batch_consolidator cron requires consolidation mode "auto_consolidate", got "promote_finalized"`)
	require.Empty(t, runtime.executions)
}

func TestBatchConsolidatorCronRejectsInvalidWorkflowParallelism(t *testing.T) {
	task, runtime, metaStorage, cfg, ctrl := newBatchConsolidatorCronTask(t)
	defer ctrl.Finish()
	cfg.Cron.BatchConsolidator.WorkflowParallelism = maxBatchConsolidatorWorkflowParallelism + 1

	metaStorage.EXPECT().GetBlockConsolidationCursor(gomock.Any(), gomock.Any(), gomock.Any()).Times(0)
	metaStorage.EXPECT().GetLatestBlock(gomock.Any(), gomock.Any()).Times(0)
	metaStorage.EXPECT().GetFirstBlockMissingConsolidationShadow(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(0)

	err := task.Run(context.Background())
	require.Error(t, err)
	require.Contains(t, err.Error(), "workflow_parallelism(21) exceeds max(20)")
	require.Empty(t, runtime.executions)
}

func newBatchConsolidatorCronTask(t *testing.T) (*batchConsolidatorTask, *batchConsolidatorCronRuntime, *metastoragemocks.MockMetaStorage, *config.Config, *gomock.Controller) {
	t.Helper()
	cfg, err := config.New()
	require.NoError(t, err)
	cfg.Chain.BlockTag.Stable = 2
	cfg.Chain.BlockTag.Latest = 2
	cfg.Chain.IrreversibleDistance = 100
	cfg.Cron.BatchConsolidator.Enabled = true
	cfg.AWS.Storage.Consolidation.Enabled = true
	cfg.AWS.Storage.Consolidation.Mode = config.ConsolidationModeAutoConsolidate
	cfg.AWS.Storage.Consolidation.MaxBlocks = 1_000
	safeLag := uint64(100)
	cfg.AWS.Storage.Consolidation.SafePromotionLag = &safeLag

	logger := zaptest.NewLogger(t)
	runtime := &batchConsolidatorCronRuntime{logger: logger}
	batchConsolidator := workflowpkg.NewBatchConsolidator(workflowpkg.BatchConsolidatorParams{
		Params: fxparams.Params{
			Config:  cfg,
			Logger:  logger,
			Metrics: tally.NoopScope,
		},
		Runtime: runtime,
	})
	ctrl := gomock.NewController(t)
	metaStorage := metastoragemocks.NewMockMetaStorage(ctrl)
	task, err := NewBatchConsolidator(BatchConsolidatorTaskParams{
		Params: fxparams.Params{
			Config:  cfg,
			Logger:  logger,
			Metrics: tally.NoopScope,
		},
		Config:            cfg,
		Runtime:           runtime,
		MetaStorage:       metaStorage,
		BatchConsolidator: batchConsolidator,
	})
	require.NoError(t, err)
	return task.(*batchConsolidatorTask), runtime, metaStorage, cfg, ctrl
}

func (r *batchConsolidatorCronRuntime) RegisterWorkflow(w any, options workflow.RegisterOptions) {}

func (r *batchConsolidatorCronRuntime) RegisterActivity(a any, options activity.RegisterOptions) {}

func (r *batchConsolidatorCronRuntime) ExecuteWorkflow(ctx context.Context, options client.StartWorkflowOptions, workflow any, request any) (client.WorkflowRun, error) {
	r.executions = append(r.executions, batchConsolidatorCronExecution{
		options: options,
		request: request,
	})
	return batchConsolidatorCronRun{id: options.ID, runID: "test-run-id"}, nil
}

func (r *batchConsolidatorCronRuntime) ExecuteActivity(ctx workflow.Context, activity any, request any, response any) error {
	panic("ExecuteActivity is not used by batch_consolidator cron tests")
}

func (r *batchConsolidatorCronRuntime) GetLogger(ctx workflow.Context) *zap.Logger {
	return r.logger
}

func (r *batchConsolidatorCronRuntime) GetMetricsHandler(ctx workflow.Context) client.MetricsHandler {
	return client.MetricsNopHandler
}

func (r *batchConsolidatorCronRuntime) GetActivityLogger(ctx context.Context) *zap.Logger {
	return r.logger
}

func (r *batchConsolidatorCronRuntime) GetTimeSource(ctx workflow.Context) timesource.TimeSource {
	return timesource.NewRealTimeSource()
}

func (r *batchConsolidatorCronRuntime) TerminateWorkflow(ctx context.Context, workflowID string, runID string, reason string) error {
	return nil
}

func (r *batchConsolidatorCronRuntime) OnStart(ctx context.Context) error {
	return nil
}

func (r *batchConsolidatorCronRuntime) OnStop(ctx context.Context) error {
	return nil
}

func (r *batchConsolidatorCronRuntime) ListOpenWorkflows(ctx context.Context, namespace string, maxPageSize int32, workflowType string) (*workflowservice.ListOpenWorkflowExecutionsResponse, error) {
	r.requestedWorkflowType = workflowType
	executions := make([]*workflowpb.WorkflowExecutionInfo, 0, len(r.openWorkflowID))
	for _, workflowID := range r.openWorkflowID {
		executions = append(executions, &workflowpb.WorkflowExecutionInfo{
			Execution: &commonpb.WorkflowExecution{WorkflowId: workflowID},
			Type:      &commonpb.WorkflowType{Name: workflowType},
			TaskQueue: r.openWorkflowTaskQueue[workflowID],
		})
	}
	return &workflowservice.ListOpenWorkflowExecutionsResponse{Executions: executions}, nil
}

func (r batchConsolidatorCronRun) GetID() string {
	return r.id
}

func (r batchConsolidatorCronRun) GetRunID() string {
	return r.runID
}

func (r batchConsolidatorCronRun) Get(ctx context.Context, valuePtr any) error {
	return nil
}

func (r batchConsolidatorCronRun) GetWithOptions(ctx context.Context, valuePtr any, options client.WorkflowRunGetOptions) error {
	return nil
}
