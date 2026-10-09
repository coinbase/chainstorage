package retirement

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const selectorTestBucket = "selector-test-bucket"

type fakeCohortRepository struct {
	pending       []RetentionCohort
	due           []RetentionCohort
	pendingErr    error
	dueErr        error
	pendingCutoff time.Time
	dueCutoff     time.Time

	watermark        uint64
	watermarkFound   bool
	watermarkErr     error
	watermarkMinArg  uint64
	watermarkGenArg  string
	watermarkTagArg  uint32
	watermarkCallCnt int

	inRangeHeights []uint64
	inRangeCalls   [][2]uint64

	dueFloor             uint64
	dueFloorFound        bool
	dueFloorErr          error
	dueFloorMinArg       uint64
	dueFloorEndArg       uint64
	nextCursor           DueCohortCursor
	afterCursor          DueCohortCursor
	dueFloorCutoff       time.Time
	dueFloorCallCnt      int
	earliestDeadline     time.Time
	earliestNoCandidates bool
	earliestErr          error
	earliestCalls        int
	earliestMinArg       uint64
	earliestEndArg       uint64
}

// RetentionEarliestDeadline defaults to "a candidate exists and its deadline
// is the zero time", i.e. year 1 — already past any cutoff a test will use, so
// the cron's deadline gate opens and every pre-existing test still exercises
// the due-floor path it was written for. A test that wants the gate CLOSED
// sets earliestDeadline into the future, or earliestNoCandidates.
func (r *fakeCohortRepository) RetentionEarliestDeadline(
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

func (r *fakeCohortRepository) RetentionDueFloor(
	_ context.Context,
	_ string,
	_ uint32,
	minHeight uint64,
	endHeight uint64,
	eligibilityCutoff time.Time,
) (uint64, bool, error) {
	r.dueFloorCallCnt++
	r.dueFloorMinArg = minHeight
	r.dueFloorEndArg = endHeight
	r.dueFloorCutoff = eligibilityCutoff
	if r.dueFloorErr != nil {
		return 0, false, r.dueFloorErr
	}
	return r.dueFloor, r.dueFloorFound, nil
}

func (r *fakeCohortRepository) RetentionFloorWatermark(
	_ context.Context,
	storageGeneration string,
	tag uint32,
	minHeight uint64,
) (uint64, bool, error) {
	r.watermarkCallCnt++
	r.watermarkGenArg = storageGeneration
	r.watermarkTagArg = tag
	r.watermarkMinArg = minHeight
	if r.watermarkErr != nil {
		return 0, false, r.watermarkErr
	}
	return r.watermark, r.watermarkFound, nil
}

func (r *fakeCohortRepository) ListRetentionCohorts(
	_ context.Context,
	bucket string,
	storageGeneration string,
	_ uint32,
	_ uint64,
	_ uint64,
	eligibilityCutoff time.Time,
	_ int,
	after DueCohortCursor,
) ([]RetentionCohort, []RetentionCohort, DueCohortCursor, error) {
	r.afterCursor = after
	if bucket != selectorTestBucket || storageGeneration != "" {
		return nil, nil, DueCohortCursor{}, errors.New("unexpected storage target")
	}
	r.pendingCutoff = eligibilityCutoff
	r.dueCutoff = eligibilityCutoff
	if r.pendingErr != nil {
		return nil, nil, DueCohortCursor{}, r.pendingErr
	}
	if r.dueErr != nil {
		return nil, nil, DueCohortCursor{}, r.dueErr
	}
	return r.pending, r.due, r.nextCursor, nil
}

func TestSelectorPrioritizesPendingAndMergesDueRange(t *testing.T) {
	now := time.Date(2026, 7, 23, 0, 0, 0, 0, time.UTC)
	repo := &fakeCohortRepository{
		pending: []RetentionCohort{{
			ConsolidatedObjectKey: "consolidated/a.cscb.zstd",
			StartHeight:           102,
			EndHeight:             104,
			RowCount:              2,
			EligibleAt:            now.Add(-time.Hour),
		}},
		due: []RetentionCohort{
			{
				ConsolidatedObjectKey: "consolidated/a.cscb.zstd",
				StartHeight:           100,
				EndHeight:             110,
				RowCount:              8,
				EligibleAt:            now.Add(-2 * time.Hour),
			},
			{
				ConsolidatedObjectKey: "consolidated/b.cscb.zstd",
				StartHeight:           200,
				EndHeight:             210,
				RowCount:              10,
				EligibleAt:            now,
			},
		},
	}

	cohorts, hasMore, _, err := NewSelector(repo).Select(context.Background(), selectorTestBucket, "", 2, 0, 0, now, 2, DueCohortCursor{})
	require.NoError(t, err)
	require.False(t, hasMore)
	require.Equal(t, now, repo.pendingCutoff)
	require.Equal(t, now, repo.dueCutoff)
	require.Equal(t, []RetentionCohort{
		{
			ConsolidatedObjectKey: "consolidated/a.cscb.zstd",
			StartHeight:           100,
			EndHeight:             110,
			RowCount:              10,
			EligibleAt:            now.Add(-time.Hour),
			Pending:               true,
		},
		{
			ConsolidatedObjectKey: "consolidated/b.cscb.zstd",
			StartHeight:           200,
			EndHeight:             210,
			RowCount:              10,
			EligibleAt:            now,
		},
	}, cohorts)
}

func TestSelectorRejectsInvalidLimitAndCohort(t *testing.T) {
	now := time.Now().UTC()
	_, _, _, err := NewSelector(&fakeCohortRepository{}).Select(context.Background(), "", "", 2, 0, 0, now, 1, DueCohortCursor{})
	require.ErrorContains(t, err, "bucket is required")
	_, _, _, err = NewSelector(&fakeCohortRepository{}).Select(context.Background(), selectorTestBucket, "future", 2, 0, 0, now, 1, DueCohortCursor{})
	require.ErrorContains(t, err, "unsupported")
	_, _, _, err = NewSelector(&fakeCohortRepository{}).Select(context.Background(), selectorTestBucket, "", 2, 0, 0, now, 0, DueCohortCursor{})
	require.ErrorContains(t, err, "between 1 and")

	repo := &fakeCohortRepository{
		due: []RetentionCohort{{
			ConsolidatedObjectKey: "consolidated/a.cscb.zstd",
			StartHeight:           100,
			EndHeight:             100,
			RowCount:              1,
			EligibleAt:            time.Now(),
		}},
	}
	_, _, _, err = NewSelector(repo).Select(context.Background(), selectorTestBucket, "", 2, 0, 0, now, 1, DueCohortCursor{})
	require.ErrorContains(t, err, "invalid retention cohort")
}

func TestSelectorPropagatesRepositoryErrors(t *testing.T) {
	repo := &fakeCohortRepository{pendingErr: errors.New("database unavailable")}
	_, _, _, err := NewSelector(repo).Select(context.Background(), selectorTestBucket, "", 2, 0, 0, time.Now().UTC(), 1, DueCohortCursor{})
	require.ErrorContains(t, err, "database unavailable")
}

func TestSelectorValidatesOptionalHeightRange(t *testing.T) {
	selector := NewSelector(&fakeCohortRepository{})
	now := time.Now().UTC()

	_, _, _, err := selector.Select(context.Background(), selectorTestBucket, "", 2, 100, 0, now, 1, DueCohortCursor{})
	require.ErrorContains(t, err, "end height is required")

	_, _, _, err = selector.Select(context.Background(), selectorTestBucket, "", 2, 100, 100, now, 1, DueCohortCursor{})
	require.ErrorContains(t, err, "invalid retention selection range")

	_, _, _, err = selector.Select(context.Background(), selectorTestBucket, "", 2, 100, 200, now, 1, DueCohortCursor{})
	require.NoError(t, err)

	_, _, _, err = selector.Select(context.Background(), selectorTestBucket, "", 2, 100, 200, time.Time{}, 1, DueCohortCursor{})
	require.ErrorContains(t, err, "eligibility cutoff is required")
}

func TestSelectorReportsRemainingBacklog(t *testing.T) {
	now := time.Now().UTC()
	repo := &fakeCohortRepository{
		due: []RetentionCohort{
			{
				ConsolidatedObjectKey: "consolidated/a.cscb.zstd",
				StartHeight:           100,
				EndHeight:             110,
				RowCount:              10,
				EligibleAt:            now,
			},
			{
				ConsolidatedObjectKey: "consolidated/b.cscb.zstd",
				StartHeight:           200,
				EndHeight:             210,
				RowCount:              10,
				EligibleAt:            now,
			},
		},
	}

	cohorts, hasMore, _, err := NewSelector(repo).Select(context.Background(), selectorTestBucket, "", 2, 0, 0, now, 1, DueCohortCursor{})
	require.NoError(t, err)
	require.True(t, hasMore)
	require.Len(t, cohorts, 1)
	require.Equal(t, "consolidated/a.cscb.zstd", cohorts[0].ConsolidatedObjectKey)
}

func TestDueRetentionCandidateOrdering(t *testing.T) {
	require.Equal(t, dueRetentionCandidateOrderingByHeight, dueRetentionCandidateOrdering(100))
	require.Equal(t, dueRetentionCandidateOrderingByDueTime, dueRetentionCandidateOrdering(0))
}

// TestSortDueRetentionCohorts pins the final re-sort that runs on ENUMERABLE
// bounds: candidate order derives from raw shadow rows, and a drifted lower
// edge can move a cohort's real start past a neighbor's.
func TestSortDueRetentionCohorts(t *testing.T) {
	byHeight := []RetentionCohort{
		{ConsolidatedObjectKey: "b", StartHeight: 300},
		{ConsolidatedObjectKey: "a", StartHeight: 100},
	}
	sortDueRetentionCohorts(byHeight, 1_000)
	require.Equal(t, uint64(100), byHeight[0].StartHeight)

	early := time.Date(2026, 7, 23, 0, 0, 0, 0, time.UTC)
	byDue := []RetentionCohort{
		{ConsolidatedObjectKey: "b", StartHeight: 100, EligibleAt: early.Add(time.Hour)},
		{ConsolidatedObjectKey: "a", StartHeight: 300, EligibleAt: early},
	}
	sortDueRetentionCohorts(byDue, 0)
	require.Equal(t, "a", byDue[0].ConsolidatedObjectKey)
}

func TestSelectorLookaheadKeysReturnsDistinctKeysBeyondWorkflowCap(t *testing.T) {
	now := time.Date(2026, 7, 23, 0, 0, 0, 0, time.UTC)
	repo := &fakeCohortRepository{
		pending: []RetentionCohort{{
			ConsolidatedObjectKey: "consolidated/a.cscb.zstd",
			StartHeight:           100,
			EndHeight:             110,
			RowCount:              10,
			EligibleAt:            now,
		}},
		due: []RetentionCohort{
			{
				ConsolidatedObjectKey: "consolidated/a.cscb.zstd",
				StartHeight:           110,
				EndHeight:             120,
				RowCount:              10,
				EligibleAt:            now,
			},
			{
				ConsolidatedObjectKey: "consolidated/b.cscb.zstd",
				StartHeight:           200,
				EndHeight:             210,
				RowCount:              10,
				EligibleAt:            now,
			},
		},
	}
	selector := NewSelector(repo)

	keys, err := selector.LookaheadKeys(context.Background(), selectorTestBucket, "", 2, 0, 0, now, MaxRetentionPrimingLookahead)
	require.NoError(t, err)
	require.Equal(t, []string{"consolidated/a.cscb.zstd", "consolidated/b.cscb.zstd"}, keys)

	_, err = selector.LookaheadKeys(context.Background(), selectorTestBucket, "", 2, 0, 0, now, MaxRetentionPrimingLookahead+1)
	require.Error(t, err)
	_, err = selector.LookaheadKeys(context.Background(), selectorTestBucket, "", 2, 0, 0, time.Time{}, 10)
	require.Error(t, err)
}

func (r *fakeCohortRepository) RetentionFloorWatermarkInRange(
	_ context.Context,
	_ string,
	_ uint32,
	minHeight uint64,
	endHeight uint64,
) (uint64, bool, error) {
	r.inRangeCalls = append(r.inRangeCalls, [2]uint64{minHeight, endHeight})
	found := false
	lowest := uint64(0)
	for _, height := range r.inRangeHeights {
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
