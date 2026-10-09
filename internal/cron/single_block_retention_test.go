package cron

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally/v4"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap/zaptest"

	"github.com/coinbase/chainstorage/internal/cadence"
	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/storage/metastorage"
	metastoragemocks "github.com/coinbase/chainstorage/internal/storage/metastorage/mocks"
	"github.com/coinbase/chainstorage/internal/storage/retirement"
	"github.com/coinbase/chainstorage/internal/utils/fxparams"
	workflowpkg "github.com/coinbase/chainstorage/internal/workflow"
	workflowactivity "github.com/coinbase/chainstorage/internal/workflow/activity"
)

type retentionCronCohortRepository struct {
	pending []retirement.RetentionCohort
	due     []retirement.RetentionCohort
	err     error

	requestedStartHeight uint64
	requestedEndHeight   uint64
	requestedLimit       int

	// watermark drives the probe floor. found=false makes the selector fall
	// back to the operator's approved_start_height, which is the behaviour
	// every pre-existing case in this file expects.
	watermark          uint64
	watermarkFound     bool
	watermarkErr       error
	watermarkMinHeight uint64

	// undeletedHeights are the undeleted rows the BOUNDED walks can find —
	// the persisted-floor walk above the floor and the reconciliation walk
	// below it; inRangeCalls records every [start, end) chunk either asked
	// for, in order (INF-1571). inRangeErrAtCall fails the Nth call (1-based).
	undeletedHeights []uint64
	inRangeErrAtCall int
	inRangeCalls     [][2]uint64

	// The due/pending cohort fixtures are the SELECTABLE universe: what
	// production's ListRetentionCohorts could return. rawDueHeights models due
	// shadow rows that the due-floor candidate sees but the cohort query
	// excludes (active repair, canonical or metadata mismatch) — the gap
	// between the two is exactly what the cron's advance loop exists for.
	//
	// This double honors range semantics UNCONDITIONALLY, in both the floor
	// and the cohort listing. An earlier version returned every fixture
	// regardless of the requested window, so a probe searching entirely the
	// wrong range still looked correct and the INF-1416 starvation bug was
	// invisible to every test in this file.
	rawDueHeights        []uint64
	nextCursor           retirement.DueCohortCursor
	afterCursors         []retirement.DueCohortCursor
	selectCalls          [][2]uint64
	dueFloorErr          error
	dueFloorMinArg       uint64
	dueFloorMinArgs      []uint64
	dueFloorEndArg       uint64
	dueFloorCalls        int
	earliestDeadline     time.Time
	earliestNoCandidates bool
	earliestErr          error
	earliestCalls        int
	earliestMinArg       uint64
	earliestEndArg       uint64
}

// dueFloorFixtures returns every height the production due-floor candidate
// would see: raw due rows plus the start height of every due-eligible or
// pending cohort (pending rows carry a passed delete_after, so the raw floor
// sees them too).
func (r *retentionCronCohortRepository) dueFloorFixtures(eligibilityCutoff time.Time) []uint64 {
	heights := append([]uint64{}, r.rawDueHeights...)
	for _, cohort := range r.due {
		if !cohort.EligibleAt.After(eligibilityCutoff) {
			heights = append(heights, cohort.StartHeight)
		}
	}
	for _, cohort := range r.pending {
		heights = append(heights, cohort.StartHeight)
	}
	return heights
}

// RetentionEarliestDeadline defaults to "a candidate exists and its deadline
// is the zero time", i.e. year 1 — already past any cutoff a test will use, so
// the cron's deadline gate opens and every pre-existing test still exercises
// the due-floor path it was written for. A test that wants the gate CLOSED
// sets earliestDeadline into the future, or earliestNoCandidates.
func (r *retentionCronCohortRepository) RetentionEarliestDeadline(
	_ context.Context,
	_ string,
	_ uint32,
	minHeight uint64,
	endHeight uint64,
) (time.Time, bool, error) {
	r.earliestCalls++
	r.earliestMinArg = minHeight
	r.earliestEndArg = endHeight
	if r.earliestErr != nil {
		return time.Time{}, false, r.earliestErr
	}
	if r.earliestNoCandidates {
		return time.Time{}, false, nil
	}
	return r.earliestDeadline, true, nil
}

func (r *retentionCronCohortRepository) RetentionDueFloor(
	_ context.Context,
	_ string,
	_ uint32,
	minHeight uint64,
	endHeight uint64,
	eligibilityCutoff time.Time,
) (uint64, bool, error) {
	r.dueFloorCalls++
	r.dueFloorMinArg = minHeight
	r.dueFloorMinArgs = append(r.dueFloorMinArgs, minHeight)
	r.dueFloorEndArg = endHeight
	if r.dueFloorErr != nil {
		return 0, false, r.dueFloorErr
	}
	found := false
	lowest := uint64(0)
	for _, height := range r.dueFloorFixtures(eligibilityCutoff) {
		if height < minHeight || height >= endHeight {
			continue
		}
		if !found || height < lowest {
			found = true
			lowest = height
		}
	}
	return lowest, found, nil
}

func (r *retentionCronCohortRepository) RetentionFloorWatermark(
	_ context.Context,
	_ string,
	_ uint32,
	minHeight uint64,
) (uint64, bool, error) {
	r.watermarkMinHeight = minHeight
	if r.watermarkErr != nil {
		return 0, false, r.watermarkErr
	}
	return r.watermark, r.watermarkFound, nil
}

func (r *retentionCronCohortRepository) RetentionFloorWatermarkInRange(
	_ context.Context,
	_ string,
	_ uint32,
	minHeight uint64,
	endHeight uint64,
) (uint64, bool, error) {
	r.inRangeCalls = append(r.inRangeCalls, [2]uint64{minHeight, endHeight})
	if r.inRangeErrAtCall != 0 && len(r.inRangeCalls) == r.inRangeErrAtCall {
		return 0, false, errors.New("statement timeout")
	}
	found := false
	lowest := uint64(0)
	for _, height := range r.undeletedHeights {
		if height < minHeight || height >= endHeight {
			continue
		}
		if !found || height < lowest {
			found = true
			lowest = height
		}
	}
	return lowest, found, nil
}

func (r *retentionCronCohortRepository) ListRetentionCohorts(
	_ context.Context,
	_ string,
	_ string,
	_ uint32,
	startHeight uint64,
	endHeight uint64,
	_ time.Time,
	limit int,
	after retirement.DueCohortCursor,
) ([]retirement.RetentionCohort, []retirement.RetentionCohort, retirement.DueCohortCursor, error) {
	r.afterCursors = append(r.afterCursors, after)
	r.requestedStartHeight = startHeight
	r.requestedEndHeight = endHeight
	r.requestedLimit = limit
	r.selectCalls = append(r.selectCalls, [2]uint64{startHeight, endHeight})
	if r.err != nil {
		return nil, nil, retirement.DueCohortCursor{}, r.err
	}
	// Honour the requested height range unconditionally. Without this the
	// double returns due cohorts no matter what window was asked for, so a
	// probe that searches the wrong window still looks correct and the bug is
	// invisible to tests.
	//
	// resumeAfter models a budget-truncated selection: non-zero means real
	// candidates in this window were never examined, so the cron must resume
	// there rather than step past the window.
	return filterCohortsToRange(r.pending, startHeight, endHeight),
		filterCohortsToRange(r.due, startHeight, endHeight),
		r.nextCursor,
		nil
}

func filterCohortsToRange(cohorts []retirement.RetentionCohort, startHeight, endHeight uint64) []retirement.RetentionCohort {
	filtered := make([]retirement.RetentionCohort, 0, len(cohorts))
	for _, cohort := range cohorts {
		if cohort.StartHeight >= startHeight && cohort.StartHeight < endHeight {
			filtered = append(filtered, cohort)
		}
	}
	return filtered
}

func newSingleBlockRetentionCronTask(t *testing.T, configOpts ...config.ConfigOption) (
	*singleBlockRetentionTask,
	*batchConsolidatorCronRuntime,
	*retentionCronCohortRepository,
	*metastoragemocks.MockMetaStorage,
	*config.Config,
	*gomock.Controller,
) {
	t.Helper()
	cfg, err := config.New(configOpts...)
	require.NoError(t, err)
	cfg.Chain.BlockTag.Stable = 2
	cfg.Chain.BlockTag.Latest = 2
	cfg.StorageType.MetaStorageType = config.MetaStorageType_POSTGRES
	cfg.AWS.Postgres = &config.PostgresConfig{}
	cfg.Cron.SingleBlockRetention = config.SingleBlockRetentionCronConfig{
		Enabled:                     true,
		MaxObjectRanges:             250,
		WorkflowParallelism:         10,
		WindowBlocks:                1_000_000,
		ApprovedChain:               "solana-mainnet",
		ApprovedStartHeight:         423_300_000,
		ApprovedEndHeight:           437_068_000,
		DirectStorageClientsGuarded: true,
		SingleBlockWritersGuarded:   true,
		FallbackReadsValidated:      true,
		ProductionDeleteEnabled:     true,
	}

	logger := zaptest.NewLogger(t)
	runtime := &batchConsolidatorCronRuntime{logger: logger}
	activity := workflowactivity.NewSingleBlockRetention(workflowactivity.SingleBlockRetentionParams{
		Params: fxparams.Params{
			Config:  cfg,
			Logger:  logger,
			Metrics: tally.NoopScope,
		},
		Runtime: runtime,
	})
	retentionWorkflow := workflowpkg.NewSingleBlockRetention(workflowpkg.SingleBlockRetentionParams{
		Params: fxparams.Params{
			Config:  cfg,
			Logger:  logger,
			Metrics: tally.NoopScope,
		},
		Runtime:  runtime,
		Activity: activity,
	})
	ctrl := gomock.NewController(t)
	metaStorage := metastoragemocks.NewMockMetaStorage(ctrl)
	task, err := NewSingleBlockRetention(SingleBlockRetentionTaskParams{
		Params: fxparams.Params{
			Config:  cfg,
			Logger:  logger,
			Metrics: tally.NoopScope,
		},
		Config:               cfg,
		Runtime:              runtime,
		MetaStorage:          metaStorage,
		SingleBlockRetention: retentionWorkflow,
	})
	require.NoError(t, err)
	retentionTask := task.(*singleBlockRetentionTask)
	cohortRepository := &retentionCronCohortRepository{}
	retentionTask.selectorFactory = func(ctx context.Context) (*retirement.Selector, error) {
		return retirement.NewSelector(cohortRepository), nil
	}
	return retentionTask, runtime, cohortRepository, metaStorage, cfg, ctrl
}

var _ cadence.Runtime = (*batchConsolidatorCronRuntime)(nil)

func TestSingleBlockRetentionCronLaunchesWindowAnchoredAtOldestDueCohort(t *testing.T) {
	task, runtime, cohortRepository, _, cfg, ctrl := newSingleBlockRetentionCronTask(t)
	defer ctrl.Finish()
	cohortRepository.due = []retirement.RetentionCohort{{
		ConsolidatedObjectKey: "consolidated/a.cscb.zstd",
		StartHeight:           430_000_000,
		EndHeight:             430_001_000,
		RowCount:              1_000,
		EligibleAt:            time.Now().UTC().Add(-2 * time.Hour),
	}}

	require.NoError(t, task.Run(context.Background()))
	require.Len(t, runtime.executions, 1)
	require.Equal(t, "workflow.single_block_retention/auto_retain", runtime.executions[0].options.ID)
	request, ok := runtime.executions[0].request.(*workflowpkg.SingleBlockRetentionRequest)
	require.True(t, ok)
	require.True(t, request.Execute)
	require.Equal(t, uint64(430_000_000), request.StartHeight)
	require.Equal(t, uint64(431_000_000), request.EndHeight)
	require.Equal(t, cfg.Cron.SingleBlockRetention.ApprovedChain, request.ApprovedChain)
	require.Equal(t, cfg.Cron.SingleBlockRetention.ApprovedStartHeight, request.ApprovedStartHeight)
	require.Equal(t, cfg.Cron.SingleBlockRetention.ApprovedEndHeight, request.ApprovedEndHeight)
	require.Equal(t, 250, request.MaxObjectRanges)
	require.Equal(t, 10, request.Parallelism)
	require.True(t, request.ProductionDeleteEnabled)
	require.True(t, request.DirectStorageClientsGuarded)
	require.True(t, request.SingleBlockWritersGuarded)
	require.True(t, request.FallbackReadsValidated)
	require.Zero(t, request.FallbackErrorCount)
	require.False(t, request.EligibilityCutoff.IsZero())
	require.WithinDuration(t, time.Now().UTC(), request.EligibilityCutoff, time.Minute)
	require.Nil(t, request.Checkpoint)
	require.Equal(t, cfg.GetEffectiveBlockTag(0), request.Tag)
	// The probe walks ONE WINDOW anchored at the due floor — not the whole
	// envelope (that is what times out, INF-1416) and not the watermark (a
	// not-yet-due row there would hide due work above the window) — and asks
	// for a full workflow batch so pending-cohort ordering cannot mask the
	// oldest due work.
	require.Equal(t, uint64(430_000_000), cohortRepository.requestedStartHeight)
	require.Equal(t, uint64(431_000_000), cohortRepository.requestedEndHeight,
		"the probe must be clipped to one window above the due floor")
	require.Equal(t, cfg.Cron.SingleBlockRetention.ApprovedStartHeight, cohortRepository.dueFloorMinArg,
		"the floor candidate search is bounded below by the approved floor")
	require.Equal(t, cfg.Cron.SingleBlockRetention.ApprovedEndHeight, cohortRepository.dueFloorEndArg,
		"the floor candidate search is bounded above by the approved end")
	require.Equal(t, retirement.MaxRetentionCohortsPerWorkflow+1, cohortRepository.requestedLimit)
}

func TestSingleBlockRetentionCronAnchorsBelowPendingCohorts(t *testing.T) {
	task, runtime, cohortRepository, _, _, ctrl := newSingleBlockRetentionCronTask(t)
	defer ctrl.Finish()
	// A stuck in-flight cohort near the envelope tail sorts ahead of due
	// cohorts in the selector merge; the window must still anchor at the
	// lowest height in the probe set.
	cohortRepository.pending = []retirement.RetentionCohort{{
		ConsolidatedObjectKey: "consolidated/stuck.cscb.zstd",
		StartHeight:           436_900_000,
		EndHeight:             436_901_000,
		RowCount:              1_000,
		EligibleAt:            time.Now().UTC().Add(-30 * time.Minute),
	}}
	cohortRepository.due = []retirement.RetentionCohort{{
		ConsolidatedObjectKey: "consolidated/oldest.cscb.zstd",
		StartHeight:           424_000_000,
		EndHeight:             424_001_000,
		RowCount:              1_000,
		EligibleAt:            time.Now().UTC().Add(-48 * time.Hour),
	}}

	require.NoError(t, task.Run(context.Background()))
	require.Len(t, runtime.executions, 1)
	request := runtime.executions[0].request.(*workflowpkg.SingleBlockRetentionRequest)
	require.Equal(t, uint64(424_000_000), request.StartHeight)
	require.Equal(t, uint64(425_000_000), request.EndHeight)
}

func TestSingleBlockRetentionCronClipsWindowToApprovedEnd(t *testing.T) {
	task, runtime, cohortRepository, _, cfg, ctrl := newSingleBlockRetentionCronTask(t)
	defer ctrl.Finish()
	cfg.Cron.SingleBlockRetention.WindowBlocks = 1_000_000
	cohortRepository.due = []retirement.RetentionCohort{{
		ConsolidatedObjectKey: "consolidated/tail.cscb.zstd",
		StartHeight:           437_000_000,
		EndHeight:             437_001_000,
		RowCount:              1_000,
		EligibleAt:            time.Now().UTC().Add(-time.Hour),
	}}

	require.NoError(t, task.Run(context.Background()))
	require.Len(t, runtime.executions, 1)
	request := runtime.executions[0].request.(*workflowpkg.SingleBlockRetentionRequest)
	require.Equal(t, uint64(437_000_000), request.StartHeight)
	require.Equal(t, cfg.Cron.SingleBlockRetention.ApprovedEndHeight, request.EndHeight)
}

func TestSingleBlockRetentionCronIdlesWhenNothingIsDue(t *testing.T) {
	task, runtime, _, _, _, ctrl := newSingleBlockRetentionCronTask(t)
	defer ctrl.Finish()

	require.NoError(t, task.Run(context.Background()))
	require.Empty(t, runtime.executions)
}

func TestSingleBlockRetentionCronSkipsWhenRetentionWorkflowIsOpen(t *testing.T) {
	task, runtime, cohortRepository, _, _, ctrl := newSingleBlockRetentionCronTask(t)
	defer ctrl.Finish()
	runtime.openWorkflowID = []string{"workflow.single_block_retention/manual_run"}
	cohortRepository.due = []retirement.RetentionCohort{{
		ConsolidatedObjectKey: "consolidated/a.cscb.zstd",
		StartHeight:           430_000_000,
		EndHeight:             430_001_000,
		RowCount:              1_000,
		EligibleAt:            time.Now().UTC().Add(-time.Hour),
	}}

	require.NoError(t, task.Run(context.Background()))
	require.Empty(t, runtime.executions)
	require.Equal(t, "workflow.single_block_retention", runtime.requestedWorkflowType)
}

func TestSingleBlockRetentionCronResolvesOpenEndedApprovalFromConsolidationCursor(t *testing.T) {
	task, runtime, cohortRepository, metaStorage, cfg, ctrl := newSingleBlockRetentionCronTask(t)
	defer ctrl.Finish()
	cfg.Cron.SingleBlockRetention.ApprovedEndHeight = 0
	cfg.Cron.SingleBlockRetention.AllowOpenEndedApproval = true
	tag := cfg.GetEffectiveBlockTag(0)
	metaStorage.EXPECT().
		GetBlockConsolidationCursor(gomock.Any(), metastorage.BatchConsolidatorAutoConsolidateCursor, tag).
		Return(uint64(438_000_000), true, nil)
	cohortRepository.due = []retirement.RetentionCohort{{
		ConsolidatedObjectKey: "consolidated/a.cscb.zstd",
		StartHeight:           430_000_000,
		EndHeight:             430_001_000,
		RowCount:              1_000,
		EligibleAt:            time.Now().UTC().Add(-time.Hour),
	}}

	require.NoError(t, task.Run(context.Background()))
	require.Len(t, runtime.executions, 1)
	request := runtime.executions[0].request.(*workflowpkg.SingleBlockRetentionRequest)
	require.Equal(t, uint64(438_000_000), request.ApprovedEndHeight)
	require.Equal(t, uint64(430_000_000), request.StartHeight)
	require.Equal(t, uint64(431_000_000), request.EndHeight)
}

func TestSingleBlockRetentionCronIdlesWithoutConsolidationCursor(t *testing.T) {
	task, runtime, _, metaStorage, cfg, ctrl := newSingleBlockRetentionCronTask(t)
	defer ctrl.Finish()
	cfg.Cron.SingleBlockRetention.ApprovedEndHeight = 0
	cfg.Cron.SingleBlockRetention.AllowOpenEndedApproval = true
	metaStorage.EXPECT().
		GetBlockConsolidationCursor(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(uint64(0), false, nil)

	require.NoError(t, task.Run(context.Background()))
	require.Empty(t, runtime.executions)
}

func TestSingleBlockRetentionCronFailsClosedOnIncompleteStandingApproval(t *testing.T) {
	testCases := []struct {
		name     string
		mutate   func(cronConfig *config.SingleBlockRetentionCronConfig)
		expected string
	}{
		{
			name: "missing approved chain",
			mutate: func(cronConfig *config.SingleBlockRetentionCronConfig) {
				cronConfig.ApprovedChain = ""
			},
			expected: "approved_chain",
		},
		{
			name: "open-ended without opt-in",
			mutate: func(cronConfig *config.SingleBlockRetentionCronConfig) {
				cronConfig.ApprovedEndHeight = 0
				cronConfig.AllowOpenEndedApproval = false
			},
			expected: "allow_open_ended_approval",
		},
		{
			name: "inverted approved range",
			mutate: func(cronConfig *config.SingleBlockRetentionCronConfig) {
				cronConfig.ApprovedEndHeight = cronConfig.ApprovedStartHeight
			},
			expected: "is invalid",
		},
		{
			name: "direct storage clients unguarded",
			mutate: func(cronConfig *config.SingleBlockRetentionCronConfig) {
				cronConfig.DirectStorageClientsGuarded = false
			},
			expected: "direct_storage_clients_guarded",
		},
		{
			name: "single-block writers unguarded",
			mutate: func(cronConfig *config.SingleBlockRetentionCronConfig) {
				cronConfig.SingleBlockWritersGuarded = false
			},
			expected: "single_block_writers_guarded",
		},
		{
			name: "fallback reads unvalidated",
			mutate: func(cronConfig *config.SingleBlockRetentionCronConfig) {
				cronConfig.FallbackReadsValidated = false
			},
			expected: "fallback_reads_validated",
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			task, runtime, _, _, cfg, ctrl := newSingleBlockRetentionCronTask(t)
			defer ctrl.Finish()
			testCase.mutate(&cfg.Cron.SingleBlockRetention)

			err := task.Run(context.Background())
			require.Error(t, err)
			require.Contains(t, err.Error(), testCase.expected)
			require.Empty(t, runtime.executions)
		})
	}
}

func TestSingleBlockRetentionCronRequiresProductionDeleteEnablementInProduction(t *testing.T) {
	task, runtime, _, _, cfg, ctrl := newSingleBlockRetentionCronTask(t, config.WithEnvironment(config.EnvProduction))
	defer ctrl.Finish()
	cfg.Cron.SingleBlockRetention.ProductionDeleteEnabled = false

	err := task.Run(context.Background())
	require.Error(t, err)
	require.Contains(t, err.Error(), "production_delete_enabled")
	require.Empty(t, runtime.executions)
}

func TestSingleBlockRetentionCronPropagatesProbeErrors(t *testing.T) {
	task, runtime, cohortRepository, _, cfg, ctrl := newSingleBlockRetentionCronTask(t)
	defer ctrl.Finish()
	// A due row must exist for the cohort probe to run at all; without one the
	// floor candidate reports nothing due and the tick idles before Select.
	cohortRepository.due = []retirement.RetentionCohort{{
		ConsolidatedObjectKey: "consolidated/cold.cscb.zstd",
		StartHeight:           cfg.Cron.SingleBlockRetention.ApprovedStartHeight,
		EndHeight:             cfg.Cron.SingleBlockRetention.ApprovedStartHeight + 1_000,
		RowCount:              1_000,
		EligibleAt:            time.Now().UTC().Add(-time.Hour),
	}}
	cohortRepository.err = errors.New("connection reset")

	err := task.Run(context.Background())
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to probe due retention cohorts")
	require.Empty(t, runtime.executions)
}

func TestSingleBlockRetentionCronDefaults(t *testing.T) {
	task, _, _, _, cfg, ctrl := newSingleBlockRetentionCronTask(t)
	defer ctrl.Finish()

	require.Equal(t, "single_block_retention", task.Name())
	require.Equal(t, defaultSingleBlockRetentionCronSpec, task.Spec())
	require.Equal(t, int64(1), task.Parallelism())
	require.True(t, task.Enabled())
	cfg.Cron.SingleBlockRetention.Enabled = false
	require.False(t, task.Enabled())
}

// TestSingleBlockRetentionCronProbesFromTheFloorWatermark is the regression
// guard for INF-1330. The probe must start at the watermark, not the operator's
// approved floor: the due-cohort query's cohort-expansion join is bounded only
// by the height range, and past roughly 1.5M heights of width the planner drops
// idx_block_consolidation_shadow_tag_height for a sequential scan and the probe
// stops finishing inside its statement timeout.
func TestSingleBlockRetentionCronProbesFromTheFloorWatermark(t *testing.T) {
	task, runtime, cohortRepository, _, cfg, ctrl := newSingleBlockRetentionCronTask(t)
	defer ctrl.Finish()
	cohortRepository.watermark = 436_000_000
	cohortRepository.watermarkFound = true
	cohortRepository.due = []retirement.RetentionCohort{{
		ConsolidatedObjectKey: "consolidated/a.cscb.zstd",
		StartHeight:           436_000_000,
		EndHeight:             436_001_000,
		RowCount:              1_000,
		EligibleAt:            time.Now().UTC().Add(-2 * time.Hour),
	}}

	require.NoError(t, task.Run(context.Background()))

	// The watermark bounds the probe...
	require.Equal(t, uint64(436_000_000), cohortRepository.requestedStartHeight)
	require.Equal(t, cfg.Cron.SingleBlockRetention.ApprovedStartHeight, cohortRepository.watermarkMinHeight,
		"the approved floor must bound the watermark lookup so it stays cheap")

	// ...but authorization is unchanged. The workflow still carries the
	// operator's approved envelope, so narrowing the probe can never widen what
	// the sweep is permitted to delete.
	require.Len(t, runtime.executions, 1)
	request, ok := runtime.executions[0].request.(*workflowpkg.SingleBlockRetentionRequest)
	require.True(t, ok)
	require.Equal(t, cfg.Cron.SingleBlockRetention.ApprovedStartHeight, request.ApprovedStartHeight)
	require.Equal(t, cfg.Cron.SingleBlockRetention.ApprovedEndHeight, request.ApprovedEndHeight)
}

// TestSingleBlockRetentionCronKeepsApprovedFloorWhenWatermarkIsLower pins that
// a watermark below the approval floor never drags the probe underneath it.
// Retention may not delete there, so work found below is not the probe's
// business.
func TestSingleBlockRetentionCronKeepsApprovedFloorWhenWatermarkIsLower(t *testing.T) {
	task, _, cohortRepository, _, cfg, ctrl := newSingleBlockRetentionCronTask(t)
	defer ctrl.Finish()
	cohortRepository.watermark = 1_000
	cohortRepository.watermarkFound = true
	cohortRepository.due = []retirement.RetentionCohort{{
		ConsolidatedObjectKey: "consolidated/floor.cscb.zstd",
		StartHeight:           cfg.Cron.SingleBlockRetention.ApprovedStartHeight,
		EndHeight:             cfg.Cron.SingleBlockRetention.ApprovedStartHeight + 1_000,
		RowCount:              1_000,
		EligibleAt:            time.Now().UTC().Add(-time.Hour),
	}}

	require.NoError(t, task.Run(context.Background()))
	require.Equal(t, cfg.Cron.SingleBlockRetention.ApprovedStartHeight, cohortRepository.dueFloorMinArg,
		"a watermark below the approved floor must not lower the floor search")
	require.Equal(t, cfg.Cron.SingleBlockRetention.ApprovedStartHeight, cohortRepository.requestedStartHeight)
}

// TestSingleBlockRetentionCronIdlesWhenWatermarkPassesApprovedEnd covers the
// finished-sweep case: with no outstanding work at or below the approved end
// there is nothing to probe for, so the tick must idle instead of launching a
// workflow over an empty range.
func TestSingleBlockRetentionCronIdlesWhenWatermarkPassesApprovedEnd(t *testing.T) {
	task, runtime, cohortRepository, _, cfg, ctrl := newSingleBlockRetentionCronTask(t)
	defer ctrl.Finish()
	cohortRepository.watermark = cfg.Cron.SingleBlockRetention.ApprovedEndHeight
	cohortRepository.watermarkFound = true
	cohortRepository.due = []retirement.RetentionCohort{{
		ConsolidatedObjectKey: "consolidated/a.cscb.zstd",
		StartHeight:           430_000_000,
		EndHeight:             430_001_000,
		RowCount:              1_000,
		EligibleAt:            time.Now().UTC().Add(-2 * time.Hour),
	}}

	require.NoError(t, task.Run(context.Background()))
	require.Empty(t, runtime.executions)
	require.Zero(t, cohortRepository.requestedStartHeight, "the probe must not run at all")
}

// TestSingleBlockRetentionCronPropagatesWatermarkErrors keeps a failing
// watermark lookup loud. Falling back to the approved floor would work today
// and quietly reintroduce the unbounded range this mechanism exists to prevent.
func TestSingleBlockRetentionCronPropagatesWatermarkErrors(t *testing.T) {
	task, runtime, cohortRepository, _, _, ctrl := newSingleBlockRetentionCronTask(t)
	defer ctrl.Finish()
	cohortRepository.watermarkErr = errors.New("watermark unavailable")

	err := task.Run(context.Background())
	require.Error(t, err)
	require.Contains(t, err.Error(), "watermark")
	require.Empty(t, runtime.executions)
}

// recordingScope captures gauge writes so the probe_failed contract can be
// asserted directly rather than inferred.
type recordingScope struct {
	tally.Scope
	mu     sync.Mutex
	gauges map[string]float64
}

func newRecordingScope() *recordingScope {
	return &recordingScope{Scope: tally.NoopScope, gauges: make(map[string]float64)}
}

func (s *recordingScope) SubScope(string) tally.Scope { return s }

func (s *recordingScope) Gauge(name string) tally.Gauge {
	return &recordingGauge{scope: s, name: name}
}

func (s *recordingScope) get(name string) (float64, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.gauges[name]
	return v, ok
}

type recordingGauge struct {
	scope *recordingScope
	name  string
}

func (g *recordingGauge) Update(v float64) {
	g.scope.mu.Lock()
	defer g.scope.mu.Unlock()
	g.scope.gauges[g.name] = v
}

// TestSingleBlockRetentionCronProbeFailedGaugeTracksOutcome pins the signal the
// failure alert depends on.
//
// It must be a gauge written on every exit path, not the instrument's
// result_type="error" counter: tally suppresses zero counter deltas, so that
// counter emits nothing until the first failure and then goes silent, leaving a
// single isolated sample that PromQL increase() cannot turn into a delta. The
// first outage would be missed — the one case the alert exists for.
func TestSingleBlockRetentionCronProbeFailedGaugeTracksOutcome(t *testing.T) {
	t.Run("failure reports 1", func(t *testing.T) {
		task, runtime, cohortRepository, _, _, ctrl := newSingleBlockRetentionCronTask(t)
		defer ctrl.Finish()
		scope := newRecordingScope()
		task.metrics = scope
		cohortRepository.watermarkErr = errors.New("probe exploded")

		require.Error(t, task.Run(context.Background()))
		require.Empty(t, runtime.executions)

		got, ok := scope.get("probe_failed")
		require.True(t, ok, "a failed tick must still write the gauge")
		require.Equal(t, float64(1), got)
	})

	t.Run("idle tick reports 0", func(t *testing.T) {
		// Nothing due is a success, not a failure — otherwise the alert would
		// fire every time retention has simply caught up.
		task, runtime, _, _, _, ctrl := newSingleBlockRetentionCronTask(t)
		defer ctrl.Finish()
		scope := newRecordingScope()
		task.metrics = scope

		require.NoError(t, task.Run(context.Background()))
		require.Empty(t, runtime.executions)

		got, ok := scope.get("probe_failed")
		require.True(t, ok, "an idle tick must write a 0 baseline, not stay silent")
		require.Equal(t, float64(0), got)
	})

	t.Run("successful sweep reports 0", func(t *testing.T) {
		task, runtime, cohortRepository, _, _, ctrl := newSingleBlockRetentionCronTask(t)
		defer ctrl.Finish()
		scope := newRecordingScope()
		task.metrics = scope
		cohortRepository.due = []retirement.RetentionCohort{{
			ConsolidatedObjectKey: "consolidated/a.cscb.zstd",
			StartHeight:           430_000_000,
			EndHeight:             430_001_000,
			RowCount:              1_000,
			EligibleAt:            time.Now().UTC().Add(-2 * time.Hour),
		}}

		require.NoError(t, task.Run(context.Background()))
		require.Len(t, runtime.executions, 1)

		got, ok := scope.get("probe_failed")
		require.True(t, ok)
		require.Equal(t, float64(0), got)
	})
}

// TestSingleBlockRetentionCronBoundsProbeByWindowBlocks is the regression guard
// for INF-1416. WindowBlocks used to bound only the launched sweep, so the probe
// spanned [watermark, approvedEnd] however far behind retention had fallen and
// its cost grew with the backlog instead of with one tick's work.
//
// That is not a tuning nit. Past roughly 2-3M blocks of width the planner
// abandons the index path for the due-cohort expansion join and sequentially
// scans the whole block_metadata table; measured on robinhood-mainnet prod, 250k
// of width ran in 20.7s, 1M in 49.2s, and 6.74M did not finish inside the 60s
// statement timeout. Nor can it recover on its own: the watermark only advances
// once deletes land, deletes need the probe to return cohorts, and the frontier
// keeps widening the range meanwhile.
func TestSingleBlockRetentionCronBoundsProbeByWindowBlocks(t *testing.T) {
	task, _, cohortRepository, _, cfg, ctrl := newSingleBlockRetentionCronTask(t)
	defer ctrl.Finish()
	cfg.Cron.SingleBlockRetention.WindowBlocks = 200_000
	// A cold backlog: the watermark sits at the approved floor while the
	// approved end is ~13.7M blocks above it, and due work starts right at
	// the floor.
	cohortRepository.watermark = cfg.Cron.SingleBlockRetention.ApprovedStartHeight
	cohortRepository.watermarkFound = true
	cohortRepository.rawDueHeights = []uint64{cfg.Cron.SingleBlockRetention.ApprovedStartHeight}

	require.NoError(t, task.Run(context.Background()))

	require.Equal(t, cfg.Cron.SingleBlockRetention.ApprovedStartHeight, cohortRepository.requestedStartHeight)
	require.Equal(t,
		cfg.Cron.SingleBlockRetention.ApprovedStartHeight+200_000,
		cohortRepository.requestedEndHeight,
		"probe must span exactly one window regardless of how large the backlog is")
	require.Less(t,
		cohortRepository.requestedEndHeight-cohortRepository.requestedStartHeight,
		cfg.Cron.SingleBlockRetention.ApprovedEndHeight-cfg.Cron.SingleBlockRetention.ApprovedStartHeight,
		"the whole point is that the probe is narrower than the approved envelope")
}

// TestSingleBlockRetentionCronProbesWholeRangeWhenNarrowerThanWindow pins the
// other side: when the remaining range is already smaller than one window the
// probe must not be padded out past the approved end.
func TestSingleBlockRetentionCronProbesWholeRangeWhenNarrowerThanWindow(t *testing.T) {
	task, _, cohortRepository, _, cfg, ctrl := newSingleBlockRetentionCronTask(t)
	defer ctrl.Finish()
	cfg.Cron.SingleBlockRetention.WindowBlocks = 1_000_000
	cohortRepository.watermark = cfg.Cron.SingleBlockRetention.ApprovedEndHeight - 50_000
	cohortRepository.watermarkFound = true
	cohortRepository.due = []retirement.RetentionCohort{{
		ConsolidatedObjectKey: "consolidated/tail.cscb.zstd",
		StartHeight:           cfg.Cron.SingleBlockRetention.ApprovedEndHeight - 50_000,
		EndHeight:             cfg.Cron.SingleBlockRetention.ApprovedEndHeight - 50_000 + 1_000,
		RowCount:              1_000,
		EligibleAt:            time.Now().UTC().Add(-time.Hour),
	}}

	require.NoError(t, task.Run(context.Background()))

	require.Equal(t, cfg.Cron.SingleBlockRetention.ApprovedEndHeight-50_000, cohortRepository.requestedStartHeight)
	require.Equal(t, cfg.Cron.SingleBlockRetention.ApprovedEndHeight, cohortRepository.requestedEndHeight,
		"a range narrower than the window must not be widened past the approved end")
}

// TestSingleBlockRetentionCronKeepsSweepAuthorizationAtApprovedEnd pins that
// narrowing the probe never narrows -- or widens -- what the sweep is authorized
// to delete. An earlier revision of the INF-1416 fix clipped the sweep window to
// the probe end too; that underflowed when the anchor sat above the probe end
// and silently pushed the window PAST the approved envelope.
func TestSingleBlockRetentionCronKeepsSweepAuthorizationAtApprovedEnd(t *testing.T) {
	task, runtime, cohortRepository, _, cfg, ctrl := newSingleBlockRetentionCronTask(t)
	defer ctrl.Finish()
	cfg.Cron.SingleBlockRetention.WindowBlocks = 200_000
	cohortRepository.due = []retirement.RetentionCohort{{
		ConsolidatedObjectKey: "consolidated/tail.cscb.zstd",
		StartHeight:           cfg.Cron.SingleBlockRetention.ApprovedEndHeight - 1_000,
		EndHeight:             cfg.Cron.SingleBlockRetention.ApprovedEndHeight,
		RowCount:              1_000,
		EligibleAt:            time.Now().UTC().Add(-time.Hour),
	}}

	require.NoError(t, task.Run(context.Background()))

	require.Len(t, runtime.executions, 1)
	request := runtime.executions[0].request.(*workflowpkg.SingleBlockRetentionRequest)
	require.LessOrEqual(t, request.EndHeight, cfg.Cron.SingleBlockRetention.ApprovedEndHeight,
		"the sweep window must never extend past the approved end")
	require.Equal(t, cfg.Cron.SingleBlockRetention.ApprovedStartHeight, request.ApprovedStartHeight)
	require.Equal(t, cfg.Cron.SingleBlockRetention.ApprovedEndHeight, request.ApprovedEndHeight)
}

// TestSingleBlockRetentionCronFindsDueWorkBeyondAnEmptyFirstWindow is the
// regression guard for the review finding on INF-1416: a windowed probe must not
// be able to hide due work behind a not-yet-due row at its floor.
//
// RetentionFloorWatermark is the lowest UNDELETED height and deliberately does
// not filter on due time, so it pins to a row that is not due yet. Anchoring a
// bounded probe there searches [watermark, watermark+window), finds nothing, and
// idles -- and because the watermark is recomputed identically every tick, due
// work above the window is never discovered until the low row matures. The
// pre-windowed probe spanned the whole approved range and could not hide it.
//
// The interleaving is reachable in production: out-of-order backfill and repair
// promotion set each row's retention clock independently.
//
// This case is range-aware on purpose. A double that returns due cohorts
// regardless of the requested window cannot observe this bug at all -- it would
// pass while the probe searched entirely the wrong range.
func TestSingleBlockRetentionCronFindsDueWorkBeyondAnEmptyFirstWindow(t *testing.T) {
	task, runtime, cohortRepository, _, cfg, ctrl := newSingleBlockRetentionCronTask(t)
	defer ctrl.Finish()
	cfg.Cron.SingleBlockRetention.WindowBlocks = 200

	floor := cfg.Cron.SingleBlockRetention.ApprovedStartHeight
	// The watermark pins here: undeleted, but NOT due for another hour.
	cohortRepository.watermark = floor
	cohortRepository.watermarkFound = true
	cohortRepository.due = []retirement.RetentionCohort{{
		// Due, but 300 blocks up — outside a 200-block window anchored at the
		// watermark.
		ConsolidatedObjectKey: "consolidated/later.cscb.zstd",
		StartHeight:           floor + 300,
		EndHeight:             floor + 400,
		RowCount:              100,
		EligibleAt:            time.Now().UTC().Add(-2 * time.Hour),
	}}

	require.NoError(t, task.Run(context.Background()))

	require.Len(t, runtime.executions, 1,
		"due work 300 blocks above a 200-block window must still be found; anchoring the window on the not-yet-due watermark hides it forever")
	request := runtime.executions[0].request.(*workflowpkg.SingleBlockRetentionRequest)
	require.Equal(t, floor+300, request.StartHeight)
	// The probe anchored on the due floor, not the watermark.
	require.Equal(t, floor+300, cohortRepository.requestedStartHeight)
	require.Equal(t, floor+500, cohortRepository.requestedEndHeight)
	// Authorization is still the operator's envelope, unchanged.
	require.Equal(t, cfg.Cron.SingleBlockRetention.ApprovedStartHeight, request.ApprovedStartHeight)
	require.Equal(t, cfg.Cron.SingleBlockRetention.ApprovedEndHeight, request.ApprovedEndHeight)
}

// TestSingleBlockRetentionCronIdlesWhenNothingIsDueAnywhere pins the other
// reading of an empty probe: with the window anchored on the due floor, "no due
// floor" genuinely means nothing is due in the whole approved range, and idling
// is then correct rather than a hidden backlog.
func TestSingleBlockRetentionCronIdlesWhenNothingIsDueAnywhere(t *testing.T) {
	task, runtime, cohortRepository, _, _, ctrl := newSingleBlockRetentionCronTask(t)
	defer ctrl.Finish()

	require.NoError(t, task.Run(context.Background()))

	require.Empty(t, runtime.executions)
	require.Zero(t, cohortRepository.requestedEndHeight,
		"with nothing due the cron must not run the expensive due-cohort probe at all")
}

// TestSingleBlockRetentionCronAdvancesPastUnselectableWindow is the regression
// guard for the second-round review finding on INF-1416. The due-floor
// candidate deliberately does not evaluate join-level selectability, so a due
// row covered by an active repair (or missing canonical membership) can sit at
// the floor. Anchoring one window there and idling when it selects nothing
// would pin the search on that row forever — the deeper version of the same
// starvation the first fix addressed. The cron must step past such a window
// and find the selectable cohort above it, in the same tick.
func TestSingleBlockRetentionCronAdvancesPastUnselectableWindow(t *testing.T) {
	task, runtime, cohortRepository, _, cfg, ctrl := newSingleBlockRetentionCronTask(t)
	defer ctrl.Finish()
	cfg.Cron.SingleBlockRetention.WindowBlocks = 200
	floor := cfg.Cron.SingleBlockRetention.ApprovedStartHeight
	// A due shadow row the cohort query cannot return (e.g. repair-covered):
	// visible to the floor candidate, absent from the selectable fixtures.
	cohortRepository.rawDueHeights = []uint64{floor}
	// The selectable cohort sits beyond the first 200-block window.
	cohortRepository.due = []retirement.RetentionCohort{{
		ConsolidatedObjectKey: "consolidated/selectable.cscb.zstd",
		StartHeight:           floor + 300,
		EndHeight:             floor + 400,
		RowCount:              100,
		EligibleAt:            time.Now().UTC().Add(-2 * time.Hour),
	}}

	require.NoError(t, task.Run(context.Background()))

	require.Len(t, runtime.executions, 1,
		"a due-but-unselectable row at the floor must not hide the selectable cohort above the window")
	request := runtime.executions[0].request.(*workflowpkg.SingleBlockRetentionRequest)
	require.Equal(t, floor+300, request.StartHeight)
	require.Equal(t, floor+300, cohortRepository.requestedStartHeight,
		"the second probe attempt must anchor on the next due floor, past the dead window")
	require.GreaterOrEqual(t, cohortRepository.dueFloorCalls, 2,
		"stepping past a dead window requires a fresh floor lookup")
}

// TestSingleBlockRetentionCronAlarmsWhenAdvancesAreExhausted pins that a tick
// which steps past its full advance budget without finding selectable work
// raises probe_advance_exhausted instead of idling silently. Due work exists
// and nothing was launched; the next tick will walk the same dead windows, so
// this state needs a human, not quiet.
func TestSingleBlockRetentionCronAlarmsWhenAdvancesAreExhausted(t *testing.T) {
	task, runtime, cohortRepository, _, cfg, ctrl := newSingleBlockRetentionCronTask(t)
	defer ctrl.Finish()
	scope := newRecordingScope()
	task.metrics = scope
	cfg.Cron.SingleBlockRetention.WindowBlocks = 200
	floor := cfg.Cron.SingleBlockRetention.ApprovedStartHeight
	// Unselectable due rows in every window the advance budget can reach.
	for i := uint64(0); i <= uint64(maxRetentionProbeAdvances); i++ {
		cohortRepository.rawDueHeights = append(cohortRepository.rawDueHeights, floor+i*200)
	}

	require.NoError(t, task.Run(context.Background()))

	require.Empty(t, runtime.executions)
	got, ok := scope.get("probe_advance_exhausted")
	require.True(t, ok, "an exhausted tick must write the gauge")
	require.Equal(t, float64(1), got)
	require.Equal(t, maxRetentionProbeAdvances, cohortRepository.dueFloorCalls)
}

// TestSingleBlockRetentionCronResetsProbeGaugesOnNoProbeTicks is the two-tick
// regression for the gauge-staleness review warning: after a tick that probed a
// real window, a later tick that never probes (approved range fully retired)
// must not keep exposing the previous tick's scanned width or floor — a stale
// probe_window_blocks claims a scan that never ran.
func TestSingleBlockRetentionCronResetsProbeGaugesOnNoProbeTicks(t *testing.T) {
	task, runtime, cohortRepository, _, cfg, ctrl := newSingleBlockRetentionCronTask(t)
	defer ctrl.Finish()
	scope := newRecordingScope()
	task.metrics = scope
	floor := cfg.Cron.SingleBlockRetention.ApprovedStartHeight
	cohortRepository.due = []retirement.RetentionCohort{{
		ConsolidatedObjectKey: "consolidated/a.cscb.zstd",
		StartHeight:           floor,
		EndHeight:             floor + 1_000,
		RowCount:              1_000,
		EligibleAt:            time.Now().UTC().Add(-time.Hour),
	}}

	// Tick 1: a real probe over a real window.
	require.NoError(t, task.Run(context.Background()))
	require.Len(t, runtime.executions, 1)
	width, ok := scope.get("probe_window_blocks")
	require.True(t, ok)
	require.Greater(t, width, float64(0))

	// Tick 2: everything approved is retired — the completed-range early
	// return, which never probes.
	cohortRepository.watermark = cfg.Cron.SingleBlockRetention.ApprovedEndHeight
	cohortRepository.watermarkFound = true
	runtime.openWorkflowID = nil
	require.NoError(t, task.Run(context.Background()))

	width, ok = scope.get("probe_window_blocks")
	require.True(t, ok)
	require.Zero(t, width, "a no-probe tick must not expose the previous tick's scanned width")
	dueFloorHeight, ok := scope.get("due_floor_height")
	require.True(t, ok)
	require.Zero(t, dueFloorHeight)
}

// TestSingleBlockRetentionCronResumesBeyondDeadPrefixAcrossTicks is the
// regression guard for the third-round review finding on INF-1416: exhausting
// the advance budget must not permanently starve selectable work beyond the
// dead prefix. Without the resume cursor every tick restarts from the
// watermark, re-walks the same four dead windows, and the selectable cohort in
// the fifth window is never probed — alarmed, but starved forever. With it,
// consecutive ticks ratchet through the dead zone and launch.
func TestSingleBlockRetentionCronResumesBeyondDeadPrefixAcrossTicks(t *testing.T) {
	task, runtime, cohortRepository, _, cfg, ctrl := newSingleBlockRetentionCronTask(t)
	defer ctrl.Finish()
	scope := newRecordingScope()
	task.metrics = scope
	cfg.Cron.SingleBlockRetention.WindowBlocks = 200
	floor := cfg.Cron.SingleBlockRetention.ApprovedStartHeight
	// Five consecutive dead windows: due rows the cohort query cannot return.
	for i := uint64(0); i <= uint64(maxRetentionProbeAdvances); i++ {
		cohortRepository.rawDueHeights = append(cohortRepository.rawDueHeights, floor+i*200)
	}
	// The selectable cohort sits in the window after the dead zone.
	target := floor + uint64(maxRetentionProbeAdvances+1)*200
	cohortRepository.due = []retirement.RetentionCohort{{
		ConsolidatedObjectKey: "consolidated/beyond.cscb.zstd",
		StartHeight:           target,
		EndHeight:             target + 100,
		RowCount:              100,
		EligibleAt:            time.Now().UTC().Add(-2 * time.Hour),
	}}

	// Tick 1: walks the first four dead windows, exhausts, launches nothing.
	require.NoError(t, task.Run(context.Background()))
	require.Empty(t, runtime.executions)
	exhausted, ok := scope.get("probe_advance_exhausted")
	require.True(t, ok)
	require.Equal(t, float64(1), exhausted)

	// Tick 2: resumes where tick 1 stopped, steps past the last dead window,
	// and launches on the selectable cohort.
	require.NoError(t, task.Run(context.Background()))
	require.Len(t, runtime.executions, 1,
		"selectable work beyond the advance budget must be reached by the next tick, not starved")
	request := runtime.executions[0].request.(*workflowpkg.SingleBlockRetentionRequest)
	require.Equal(t, target, request.StartHeight)
	exhausted, ok = scope.get("probe_advance_exhausted")
	require.True(t, ok)
	require.Zero(t, exhausted, "a launching tick clears the exhaustion alarm")

	// Tick 3: the launch reset the ring — the floor search starts from the
	// watermark again, so dead-prefix rows that later become selectable are
	// re-inspected rather than skipped forever.
	runtime.executions = nil
	cohortRepository.dueFloorMinArgs = nil
	require.NoError(t, task.Run(context.Background()))
	require.NotEmpty(t, cohortRepository.dueFloorMinArgs)
	require.Equal(t, floor, cohortRepository.dueFloorMinArgs[0],
		"after a launch the walk must restart from the watermark, not the old cursor")
}

// TestSingleBlockRetentionCronWrapsRingWhenNothingDueBeyondDeadZone pins the
// other end of the ring: when the walk resumes past the dead zone and finds
// nothing due above it, the cursor resets so the next tick re-inspects the
// dead prefix from the watermark — its rows mature (repairs complete, clocks
// pass) and must not be skipped forever either.
func TestSingleBlockRetentionCronWrapsRingWhenNothingDueBeyondDeadZone(t *testing.T) {
	task, runtime, cohortRepository, _, cfg, ctrl := newSingleBlockRetentionCronTask(t)
	defer ctrl.Finish()
	cfg.Cron.SingleBlockRetention.WindowBlocks = 200
	floor := cfg.Cron.SingleBlockRetention.ApprovedStartHeight
	// A dead zone one window wider than the advance budget, nothing beyond it.
	for i := uint64(0); i <= uint64(maxRetentionProbeAdvances); i++ {
		cohortRepository.rawDueHeights = append(cohortRepository.rawDueHeights, floor+i*200)
	}

	// Tick 1 exhausts inside the dead zone.
	require.NoError(t, task.Run(context.Background()))
	require.Empty(t, runtime.executions)

	// Tick 2 resumes, clears the final dead window, finds nothing due above,
	// and wraps.
	require.NoError(t, task.Run(context.Background()))
	require.Empty(t, runtime.executions)

	// Tick 3 starts from the watermark again.
	cohortRepository.dueFloorMinArgs = nil
	require.NoError(t, task.Run(context.Background()))
	require.NotEmpty(t, cohortRepository.dueFloorMinArgs)
	require.Equal(t, floor, cohortRepository.dueFloorMinArgs[0],
		"after the ring wraps, the walk must restart from the watermark")
}

// TestSingleBlockRetentionCronResumesAtTruncatedSelectionHeight is the round-7
// regression guard: when due selection stops on its expansion budget with
// candidates still unexamined, it reports the first unexamined height, and the
// cron must resume its search THERE rather than stepping past the whole window.
// Stepping past would skip cohorts nobody looked at, stranding them exactly as
// the old silent page cap did.
func TestSingleBlockRetentionCronResumesAtTruncatedSelectionHeight(t *testing.T) {
	task, runtime, cohortRepository, _, cfg, ctrl := newSingleBlockRetentionCronTask(t)
	defer ctrl.Finish()
	cfg.Cron.SingleBlockRetention.WindowBlocks = 1_000
	floor := cfg.Cron.SingleBlockRetention.ApprovedStartHeight
	// Due rows exist across the window, but selection returns no cohorts and
	// reports that it only got as far as floor+400.
	// Due rows at both the floor and the resume point, so the floor lookup
	// still finds work after the search advances.
	cohortRepository.rawDueHeights = []uint64{floor, floor + 400}
	cohortRepository.nextCursor = retirement.DueCohortCursor{
		StartHeight: floor + 400,
		ObjectKey:   "consolidated/truncated.cscb.zstd",
	}

	require.NoError(t, task.Run(context.Background()))
	require.Empty(t, runtime.executions)

	require.GreaterOrEqual(t, len(cohortRepository.selectCalls), 2,
		"the cron should probe again after a truncated selection")
	// The re-probe must cover the SAME window — moving it would skip candidates
	// nobody examined, and a height bound could discard an unexamined candidate
	// overlapping the examined ones.
	first := cohortRepository.selectCalls[0]
	second := cohortRepository.selectCalls[1]
	require.Equal(t, first, second,
		"a budget-truncated selection must re-probe the same window, not move it")
	// ...and it must carry the returned keyset cursor, which is what actually
	// skips the examined candidates.
	require.GreaterOrEqual(t, len(cohortRepository.afterCursors), 2)
	require.True(t, cohortRepository.afterCursors[0].IsZero(),
		"the first probe of a window starts from the beginning")
	require.Equal(t, cohortRepository.nextCursor, cohortRepository.afterCursors[1],
		"the re-probe must carry the exact cursor selection returned")
}

// TestSingleBlockRetentionCronStepsPastGenuinelyDeadWindow pins the other side:
// when selection reports NO continuation (candidates exhausted), the window
// really is dead and the cron must step past it. Conflating the two directions
// is what this pair exists to prevent.
func TestSingleBlockRetentionCronStepsPastGenuinelyDeadWindow(t *testing.T) {
	task, runtime, cohortRepository, _, cfg, ctrl := newSingleBlockRetentionCronTask(t)
	defer ctrl.Finish()
	cfg.Cron.SingleBlockRetention.WindowBlocks = 1_000
	floor := cfg.Cron.SingleBlockRetention.ApprovedStartHeight
	cohortRepository.rawDueHeights = []uint64{floor, floor + 1_400}
	cohortRepository.nextCursor = retirement.DueCohortCursor{} // exhausted, not truncated

	require.NoError(t, task.Run(context.Background()))
	require.Empty(t, runtime.executions)

	require.GreaterOrEqual(t, len(cohortRepository.selectCalls), 2)
	first := cohortRepository.selectCalls[0]
	second := cohortRepository.selectCalls[1]
	// The cron anchors each probe on the due floor at or above its search
	// cursor, so the second window starts at the next due height — what matters
	// is that it is at or beyond the first window's end, i.e. the dead window
	// was stepped past rather than re-probed.
	require.GreaterOrEqual(t, second[0], first[1],
		"an exhausted window must be stepped past, not re-probed")
	require.GreaterOrEqual(t, len(cohortRepository.afterCursors), 2)
	require.True(t, cohortRepository.afterCursors[1].IsZero(),
		"stepping past a dead window must not carry a stale cursor into the next one")
}

// TestSingleBlockRetentionCronKeepsCursorAcrossLaunch is the round-9 regression
// guard: a non-empty selection does NOT mean the window is finished. Selection
// can return an early selectable cohort AND report a budget-truncated
// continuation because the dead prefix behind it exhausted the expansion
// budget. Clearing the cursor on launch restarts the next tick at the head of
// the window, so if the launched cohort stays due — a deferred or failed sweep
// leaves it due — every tick re-selects it and work behind the prefix is never
// reached.
func TestSingleBlockRetentionCronKeepsCursorAcrossLaunch(t *testing.T) {
	task, runtime, cohortRepository, _, cfg, ctrl := newSingleBlockRetentionCronTask(t)
	defer ctrl.Finish()
	floor := cfg.Cron.SingleBlockRetention.ApprovedStartHeight
	// An early selectable cohort...
	cohortRepository.due = []retirement.RetentionCohort{{
		ConsolidatedObjectKey: "consolidated/early.cscb.zstd",
		StartHeight:           floor,
		EndHeight:             floor + 1_000,
		RowCount:              1_000,
		EligibleAt:            time.Now().UTC().Add(-2 * time.Hour),
	}}
	// ...and a budget-truncated continuation pointing past the dead prefix.
	truncated := retirement.DueCohortCursor{
		StartHeight: floor + 5_000,
		ObjectKey:   "consolidated/dead-last.cscb.zstd",
	}
	cohortRepository.nextCursor = truncated

	// Tick 1 launches on the early cohort while selection was still truncated.
	require.NoError(t, task.Run(context.Background()))
	require.Len(t, runtime.executions, 1)
	require.GreaterOrEqual(t, len(cohortRepository.afterCursors), 1)
	require.True(t, cohortRepository.afterCursors[0].IsZero())

	// Tick 2 models the launched cohort remaining due (deferred/failed sweep):
	// the fixtures are unchanged. It must resume from the persisted cursor
	// rather than re-selecting the same early cohort from the window head.
	runtime.executions = nil
	runtime.openWorkflowID = nil
	cohortRepository.afterCursors = nil
	require.NoError(t, task.Run(context.Background()))

	require.NotEmpty(t, cohortRepository.afterCursors)
	require.Equal(t, truncated, cohortRepository.afterCursors[0],
		"the next tick must resume from the truncation cursor, not restart at the window head")
}

// TestSingleBlockRetentionCronClearsCursorWhenSelectionExhausts pins the other
// direction: when selection reports no continuation, a launch must leave no
// stale cursor behind, so the following tick walks the window from its head.
func TestSingleBlockRetentionCronClearsCursorWhenSelectionExhausts(t *testing.T) {
	task, runtime, cohortRepository, _, cfg, ctrl := newSingleBlockRetentionCronTask(t)
	defer ctrl.Finish()
	floor := cfg.Cron.SingleBlockRetention.ApprovedStartHeight
	cohortRepository.due = []retirement.RetentionCohort{{
		ConsolidatedObjectKey: "consolidated/only.cscb.zstd",
		StartHeight:           floor,
		EndHeight:             floor + 1_000,
		RowCount:              1_000,
		EligibleAt:            time.Now().UTC().Add(-2 * time.Hour),
	}}
	cohortRepository.nextCursor = retirement.DueCohortCursor{} // exhausted

	require.NoError(t, task.Run(context.Background()))
	require.Len(t, runtime.executions, 1)

	runtime.executions = nil
	runtime.openWorkflowID = nil
	cohortRepository.afterCursors = nil
	require.NoError(t, task.Run(context.Background()))

	require.NotEmpty(t, cohortRepository.afterCursors)
	require.True(t, cohortRepository.afterCursors[0].IsZero(),
		"an exhausted selection must not leave a cursor that skips the window head")
}

// --- INF-1571: persisted floor watermark + bounded reconciliation ---

func enablePersistedFloor(cfg *config.Config, chunk uint64) {
	cfg.Cron.SingleBlockRetention.PersistFloorWatermark = true
	cfg.Cron.SingleBlockRetention.FloorWalkChunkBlocks = chunk
}

func dueCohortAt(height uint64) retirement.RetentionCohort {
	return retirement.RetentionCohort{
		ConsolidatedObjectKey: "consolidated/a.cscb.zstd",
		StartHeight:           height,
		EndHeight:             height + 1_000,
		RowCount:              1_000,
		EligibleAt:            time.Now().UTC().Add(-2 * time.Hour),
	}
}

const (
	testApprovedStart = uint64(423_300_000)
	testApprovedEnd   = uint64(437_068_000)
)

func expectFloorCursor(metaStorage *metastoragemocks.MockMetaStorage, height uint64, found bool) *gomock.Call {
	return metaStorage.EXPECT().
		GetBlockConsolidationCursor(gomock.Any(), metastorage.SingleBlockRetentionFloorWatermarkCursor, uint32(2)).
		Return(height, found, nil)
}

func expectFloorSet(metaStorage *metastoragemocks.MockMetaStorage, height uint64) *gomock.Call {
	return metaStorage.EXPECT().
		SetBlockConsolidationCursor(gomock.Any(), metastorage.SingleBlockRetentionFloorWatermarkCursor, uint32(2), height).
		Return(nil)
}

// TestSingleBlockRetentionCronWalksFromPersistedFloor pins the point of the
// mechanism: the floor walk starts at the persisted floor rather than at
// approved_start_height, so its cost stops growing with retired history —
// while the sweep's authorization envelope is untouched.
func TestSingleBlockRetentionCronWalksFromPersistedFloor(t *testing.T) {
	task, runtime, cohortRepository, metaStorage, cfg, ctrl := newSingleBlockRetentionCronTask(t)
	defer ctrl.Finish()
	enablePersistedFloor(cfg, 0)
	const persisted = uint64(436_500_000)
	cohortRepository.undeletedHeights = []uint64{436_600_000}
	cohortRepository.due = []retirement.RetentionCohort{dueCohortAt(436_600_000)}

	expectFloorCursor(metaStorage, persisted, true).Times(2) // probe + reconciliation
	expectFloorSet(metaStorage, 436_600_000)

	require.NoError(t, task.Run(context.Background()))

	require.Len(t, runtime.executions, 1)
	request, ok := runtime.executions[0].request.(*workflowpkg.SingleBlockRetentionRequest)
	require.True(t, ok)
	require.Equal(t, testApprovedStart, request.ApprovedStartHeight,
		"authorization still carries the operator's approved floor")
	require.Equal(t, [][2]uint64{
		{persisted, testApprovedEnd},                       // the walk: one chunk from the persisted floor, clipped to approvedEnd
		{testApprovedStart, testApprovedStart + 1_000_000}, // reconciliation: first chunk below it
	}, cohortRepository.inRangeCalls, "the walk must start at the persisted floor, not at approved_start_height")
}

// TestSingleBlockRetentionCronBootstrapsInBoundedChunks: with nothing
// persisted the walk starts at the approval floor as before, but bounded —
// every empty chunk raises the floor before the next runs, so the first
// rollout from an old approval floor never runs the unbounded query.
func TestSingleBlockRetentionCronBootstrapsInBoundedChunks(t *testing.T) {
	task, runtime, cohortRepository, metaStorage, cfg, ctrl := newSingleBlockRetentionCronTask(t)
	defer ctrl.Finish()
	const chunk = uint64(5_000_000)
	enablePersistedFloor(cfg, chunk)
	cohortRepository.undeletedHeights = []uint64{436_000_000}
	cohortRepository.due = []retirement.RetentionCohort{dueCohortAt(436_000_000)}

	gomock.InOrder(
		expectFloorCursor(metaStorage, 0, false),
		expectFloorSet(metaStorage, testApprovedStart+chunk),
		expectFloorSet(metaStorage, testApprovedStart+2*chunk),
		expectFloorSet(metaStorage, 436_000_000),
		expectFloorCursor(metaStorage, 436_000_000, true), // reconciliation reads the floor the walk just wrote
	)

	require.NoError(t, task.Run(context.Background()))
	require.Len(t, runtime.executions, 1)
	require.Equal(t, [][2]uint64{
		{testApprovedStart, testApprovedStart + chunk},
		{testApprovedStart + chunk, testApprovedStart + 2*chunk},
		{testApprovedStart + 2*chunk, testApprovedEnd},
		{testApprovedStart, testApprovedStart + chunk}, // reconciliation
	}, cohortRepository.inRangeCalls)
}

// TestSingleBlockRetentionCronBootstrapSpansTicksAtTheChunkBudget: a walk
// that exhausts its per-tick budget launches nothing, keeps its progress, and
// resumes from it next tick — the bootstrap cost is spread, never repeated.
func TestSingleBlockRetentionCronBootstrapSpansTicksAtTheChunkBudget(t *testing.T) {
	task, runtime, cohortRepository, metaStorage, cfg, ctrl := newSingleBlockRetentionCronTask(t)
	defer ctrl.Finish()
	const chunk = uint64(1_000_000)
	enablePersistedFloor(cfg, chunk)
	cohortRepository.undeletedHeights = []uint64{430_000_000}
	cohortRepository.due = []retirement.RetentionCohort{dueCohortAt(430_000_000)}
	budgetEnd := testApprovedStart + uint64(maxSingleBlockRetentionFloorWalkChunksPerTick)*chunk // 427.3M

	gomock.InOrder(
		// tick 1: four empty chunks, each persisted; budget exhausted
		expectFloorCursor(metaStorage, 0, false),
		expectFloorSet(metaStorage, testApprovedStart+chunk),
		expectFloorSet(metaStorage, testApprovedStart+2*chunk),
		expectFloorSet(metaStorage, testApprovedStart+3*chunk),
		expectFloorSet(metaStorage, budgetEnd),
		expectFloorCursor(metaStorage, budgetEnd, true), // reconciliation
		// tick 2: resumes at the persisted progress and finds the row in its third chunk
		expectFloorCursor(metaStorage, budgetEnd, true),
		expectFloorSet(metaStorage, budgetEnd+chunk),
		expectFloorSet(metaStorage, budgetEnd+2*chunk),
		expectFloorSet(metaStorage, 430_000_000),
		expectFloorCursor(metaStorage, 430_000_000, true), // reconciliation
	)

	require.NoError(t, task.Run(context.Background()))
	require.Empty(t, runtime.executions, "no sweep launches without a resolved floor")
	require.Len(t, cohortRepository.inRangeCalls, maxSingleBlockRetentionFloorWalkChunksPerTick+1)

	require.NoError(t, task.Run(context.Background()))
	require.Len(t, runtime.executions, 1)
	walk := cohortRepository.inRangeCalls[maxSingleBlockRetentionFloorWalkChunksPerTick+1:]
	require.Equal(t, [2]uint64{budgetEnd, budgetEnd + chunk}, walk[0], "tick 2 resumes where tick 1 stopped")
	require.Equal(t, [2]uint64{budgetEnd + 2*chunk, budgetEnd + 3*chunk}, walk[2])
}

// TestSingleBlockRetentionCronNothingUndeletedAdvancesTheFloorToApprovedEnd:
// when nothing undeleted remains at or above the floor the walk reaches
// approvedEnd, proves every approved row retired, and moves the floor there,
// so later ticks walk only what the envelope gained — not the retired tail.
func TestSingleBlockRetentionCronNothingUndeletedAdvancesTheFloorToApprovedEnd(t *testing.T) {
	task, runtime, cohortRepository, metaStorage, cfg, ctrl := newSingleBlockRetentionCronTask(t)
	defer ctrl.Finish()
	enablePersistedFloor(cfg, 0)
	const persisted = uint64(436_500_000)

	gomock.InOrder(
		expectFloorCursor(metaStorage, persisted, true),
		expectFloorSet(metaStorage, testApprovedEnd),
		expectFloorCursor(metaStorage, testApprovedEnd, true), // reconciliation
		// next tick: the walk is empty and nothing is written
		expectFloorCursor(metaStorage, testApprovedEnd, true),
		expectFloorCursor(metaStorage, testApprovedEnd, true),
	)

	require.NoError(t, task.Run(context.Background()))
	require.Empty(t, runtime.executions)
	require.NoError(t, task.Run(context.Background()))
	require.Equal(t, [][2]uint64{
		{persisted, testApprovedEnd},
		{testApprovedStart, testApprovedStart + 1_000_000},             // reconciliation, tick 1
		{testApprovedStart + 1_000_000, testApprovedStart + 2_000_000}, // reconciliation, tick 2
	}, cohortRepository.inRangeCalls, "the second tick's walk is empty: the floor already sits at approvedEnd")
}

// TestSingleBlockRetentionCronReconciliationLowersPersistedFloor is the
// safety property the persisted floor depends on: an undeleted row below the
// persisted floor is found by the bounded re-walk and the floor is lowered to
// it, so the next walk starts there and the row is retired instead of being
// stranded.
func TestSingleBlockRetentionCronReconciliationLowersPersistedFloor(t *testing.T) {
	task, _, cohortRepository, metaStorage, cfg, ctrl := newSingleBlockRetentionCronTask(t)
	defer ctrl.Finish()
	enablePersistedFloor(cfg, 20_000_000) // one chunk covers the whole retired range
	const persisted = uint64(436_500_000)
	const stray = uint64(430_000_000)
	cohortRepository.undeletedHeights = []uint64{stray}

	gomock.InOrder(
		expectFloorCursor(metaStorage, persisted, true),
		expectFloorSet(metaStorage, testApprovedEnd), // nothing undeleted above the floor
		expectFloorCursor(metaStorage, testApprovedEnd, true),
		metaStorage.EXPECT().
			ResetBlockConsolidationCursor(gomock.Any(), metastorage.SingleBlockRetentionFloorWatermarkCursor, uint32(2), stray).
			Return(nil),
	)

	require.NoError(t, task.Run(context.Background()))
	require.Equal(t, [][2]uint64{{persisted, testApprovedEnd}, {testApprovedStart, testApprovedEnd}}, cohortRepository.inRangeCalls)
}

// TestSingleBlockRetentionCronReconciliationChunksAndWraps pins the per-tick
// cost bound: each tick walks one chunk below the floor, consecutive ticks
// continue where the last stopped, and reaching the floor wraps to the
// approval floor so the retired range is re-inspected continuously.
func TestSingleBlockRetentionCronReconciliationChunksAndWraps(t *testing.T) {
	task, _, cohortRepository, metaStorage, cfg, ctrl := newSingleBlockRetentionCronTask(t)
	defer ctrl.Finish()
	const chunk = uint64(5_000_000)
	enablePersistedFloor(cfg, chunk)
	// The floor already sits at approvedEnd, so the walk above it is empty
	// and every inRangeCall below is reconciliation.
	expectFloorCursor(metaStorage, testApprovedEnd, true).AnyTimes()

	for i := 0; i < 4; i++ {
		require.NoError(t, task.Run(context.Background()))
	}
	require.Equal(t, [][2]uint64{
		{testApprovedStart, testApprovedStart + chunk},
		{testApprovedStart + chunk, testApprovedStart + 2*chunk},
		{testApprovedStart + 2*chunk, testApprovedEnd}, // clipped to the floor
		{testApprovedStart, testApprovedStart + chunk}, // wrapped
	}, cohortRepository.inRangeCalls)
}

// TestSingleBlockRetentionCronReconcilesOnSkippedTicks: most ticks skip because
// a sweep is open, and those are the ticks the database is otherwise idle for
// — reconciliation must run there too or it would starve.
func TestSingleBlockRetentionCronReconcilesOnSkippedTicks(t *testing.T) {
	task, runtime, cohortRepository, metaStorage, cfg, ctrl := newSingleBlockRetentionCronTask(t)
	defer ctrl.Finish()
	enablePersistedFloor(cfg, 0)
	runtime.openWorkflowID = []string{"workflow.single_block_retention/manual_run"}

	expectFloorCursor(metaStorage, 436_500_000, true) // reconciliation only; the walk never ran

	require.NoError(t, task.Run(context.Background()))
	require.Empty(t, runtime.executions)
	require.Equal(t, [][2]uint64{{testApprovedStart, testApprovedStart + 1_000_000}}, cohortRepository.inRangeCalls)
}

// TestSingleBlockRetentionCronPersistFailureIsNotFatal: the walk's result is
// valid regardless of whether the floor could be written, so a failed write
// must not fail the tick or block the launch.
func TestSingleBlockRetentionCronPersistFailureIsNotFatal(t *testing.T) {
	task, runtime, cohortRepository, metaStorage, cfg, ctrl := newSingleBlockRetentionCronTask(t)
	defer ctrl.Finish()
	enablePersistedFloor(cfg, 20_000_000)
	cohortRepository.undeletedHeights = []uint64{436_000_000}
	cohortRepository.due = []retirement.RetentionCohort{dueCohortAt(436_000_000)}

	expectFloorCursor(metaStorage, 0, false).Times(2)
	metaStorage.EXPECT().
		SetBlockConsolidationCursor(gomock.Any(), metastorage.SingleBlockRetentionFloorWatermarkCursor, uint32(2), uint64(436_000_000)).
		Return(errors.New("write failed"))

	require.NoError(t, task.Run(context.Background()))
	require.Len(t, runtime.executions, 1)
}

// TestSingleBlockRetentionCronWalkChunkErrorFailsTheProbe: an error inside the
// walk above the floor is a probe failure like the unbounded walk's was —
// progress from earlier chunks is already persisted, so the retry is cheap.
func TestSingleBlockRetentionCronWalkChunkErrorFailsTheProbe(t *testing.T) {
	task, runtime, cohortRepository, metaStorage, cfg, ctrl := newSingleBlockRetentionCronTask(t)
	defer ctrl.Finish()
	enablePersistedFloor(cfg, 1_000_000)
	cohortRepository.inRangeErrAtCall = 2

	gomock.InOrder(
		expectFloorCursor(metaStorage, 0, false),
		expectFloorSet(metaStorage, testApprovedStart+1_000_000),
	)

	require.Error(t, task.Run(context.Background()))
	require.Empty(t, runtime.executions)
	require.Len(t, cohortRepository.inRangeCalls, 2, "the tick stops at the failed chunk and reconciliation does not run")
}

// TestSingleBlockRetentionCronReconcileErrorIsNotFatal: a failed reconciliation
// chunk is reported on its own gauge and retried next tick; it never fails
// the tick.
func TestSingleBlockRetentionCronReconcileErrorIsNotFatal(t *testing.T) {
	task, _, cohortRepository, metaStorage, cfg, ctrl := newSingleBlockRetentionCronTask(t)
	defer ctrl.Finish()
	enablePersistedFloor(cfg, 0)
	cohortRepository.inRangeErrAtCall = 1 // the walk above the floor is empty; the first call is reconciliation

	expectFloorCursor(metaStorage, testApprovedEnd, true).Times(2)

	require.NoError(t, task.Run(context.Background()))
	require.Len(t, cohortRepository.inRangeCalls, 1)
}

// TestSingleBlockRetentionCronFlagOffKeepsTheUnboundedWalk: with the flag off
// nothing touches the cursor and the walk is the pre-existing unbounded one
// from approved_start_height.
func TestSingleBlockRetentionCronFlagOffKeepsTheUnboundedWalk(t *testing.T) {
	task, runtime, cohortRepository, _, _, ctrl := newSingleBlockRetentionCronTask(t)
	defer ctrl.Finish()
	cohortRepository.watermark = 436_000_000
	cohortRepository.watermarkFound = true
	cohortRepository.due = []retirement.RetentionCohort{dueCohortAt(436_000_000)}

	require.NoError(t, task.Run(context.Background()))
	require.Len(t, runtime.executions, 1)
	require.Equal(t, testApprovedStart, cohortRepository.watermarkMinHeight)
	require.Empty(t, cohortRepository.inRangeCalls)
}
