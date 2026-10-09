package cron

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/coinbase/chainstorage/internal/storage/retirement"
)

// The deadline gate lets a tick answer "nothing due" without the due-floor
// walk. These tests pin the three behaviours that make that safe rather than
// merely fast; each one, if broken, produces a stalled sweep and no error.

// A future deadline is the whole point: the walk must not run at all.
func TestSingleBlockRetentionCronSkipsTheWalkWhenNothingIsDueYet(t *testing.T) {
	task, runtime, cohortRepository, _, _, ctrl := newSingleBlockRetentionCronTask(t)
	defer ctrl.Finish()
	cohortRepository.earliestDeadline = time.Now().UTC().Add(48 * time.Hour)
	// Due work is staged deliberately. If the gate were advisory rather than
	// authoritative the walk would find this and launch, and the test would
	// fail — which is what makes it a real assertion about skipping.
	cohortRepository.due = []retirement.RetentionCohort{{
		ConsolidatedObjectKey: "consolidated/a.cscb.zstd",
		StartHeight:           430_000_000,
		EndHeight:             430_001_000,
		RowCount:              1_000,
		EligibleAt:            time.Now().UTC().Add(-2 * time.Hour),
	}}

	require.NoError(t, task.Run(context.Background()))
	require.Empty(t, runtime.executions, "a tick with nothing due must not launch a sweep")
	require.Equal(t, 1, cohortRepository.earliestCalls, "the gate runs exactly once per tick")
	require.Zero(t, cohortRepository.dueFloorCalls,
		"the due-floor walk is what the gate exists to avoid; running it anyway keeps the cost the "+
			"change was made to remove")
}

// The gate must be re-asked every tick. Caching the deadline and sleeping until
// it would strand work, because a deadline can be LOWERED between ticks by
// re-consolidation restamping or by shortening the retention window.
func TestSingleBlockRetentionCronReAsksTheGateEveryTick(t *testing.T) {
	task, runtime, cohortRepository, _, _, ctrl := newSingleBlockRetentionCronTask(t)
	defer ctrl.Finish()
	cohortRepository.earliestDeadline = time.Now().UTC().Add(48 * time.Hour)

	require.NoError(t, task.Run(context.Background()))
	require.NoError(t, task.Run(context.Background()))
	require.Equal(t, 2, cohortRepository.earliestCalls,
		"the second tick must re-ask rather than reuse the first tick's answer: a deadline can move "+
			"earlier between ticks, and a cached one would hide it")
	require.Empty(t, runtime.executions)

	// Once the deadline passes, the very next tick must proceed to the walk
	// with no further prompting.
	cohortRepository.earliestDeadline = time.Now().UTC().Add(-time.Minute)
	require.NoError(t, task.Run(context.Background()))
	require.Equal(t, 3, cohortRepository.earliestCalls)
	require.Positive(t, cohortRepository.dueFloorCalls,
		"a deadline that has passed must reopen the walk on the next tick")
}

// MIN over an empty candidate set is NULL, which the repository reports as
// found=false. That is "nothing left to retire", not "due now" — a caller that
// folded absence into a timestamp comparison would get the opposite.
func TestSingleBlockRetentionCronTreatsNoCandidatesAsNothingDue(t *testing.T) {
	task, runtime, cohortRepository, _, _, ctrl := newSingleBlockRetentionCronTask(t)
	defer ctrl.Finish()
	cohortRepository.earliestNoCandidates = true
	cohortRepository.due = []retirement.RetentionCohort{{
		ConsolidatedObjectKey: "consolidated/a.cscb.zstd",
		StartHeight:           430_000_000,
		EndHeight:             430_001_000,
		RowCount:              1_000,
		EligibleAt:            time.Now().UTC().Add(-2 * time.Hour),
	}}

	require.NoError(t, task.Run(context.Background()))
	require.Empty(t, runtime.executions)
	require.Zero(t, cohortRepository.dueFloorCalls)
}

// The gate is asked about the WIDEST window the tick can reach. The advance
// loop only ever raises its search start, so gating on the initial window is a
// superset of every advance and no due row can sit above it.
func TestSingleBlockRetentionCronGatesOverTheFullEnvelope(t *testing.T) {
	task, _, cohortRepository, _, cfg, ctrl := newSingleBlockRetentionCronTask(t)
	defer ctrl.Finish()
	cohortRepository.earliestDeadline = time.Now().UTC().Add(48 * time.Hour)

	require.NoError(t, task.Run(context.Background()))
	require.Equal(t, cfg.Cron.SingleBlockRetention.ApprovedStartHeight, cohortRepository.earliestMinArg,
		"the gate starts at the floor watermark, the lowest height this tick could probe")
	require.Greater(t, cohortRepository.earliestEndArg, cohortRepository.earliestMinArg)
	require.GreaterOrEqual(t, cohortRepository.earliestEndArg, cohortRepository.dueFloorEndArg,
		"the gate's window must not be narrower than any window the walk would search, or work could "+
			"sit above the gate and below the walk's reach")
}
