package retirement

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// watermarkIndexAddMigration created a partial index for the watermark lookup;
// watermarkIndexDropMigration removed it again after production measurement
// showed the planner also applied it to the due-cohort probe and made that 2.5x
// slower. Both are pinned here so the pair cannot be quietly re-added.
const (
	watermarkIndexAddMigration  = "../metastorage/postgres/db/migrations/20260818000001_add_retention_floor_watermark_index.sql"
	watermarkIndexDropMigration = "../metastorage/postgres/db/migrations/20260818000002_drop_retention_floor_watermark_index.sql"
)

// TestFloorWatermarkNeverDropsBelowTheApprovedFloor pins the safety property
// that separates a performance floor from an authorization floor. Retention may
// not delete below approved_start_height, so work discovered underneath it —
// a repair promoted into an old height, for instance — must not drag the probe
// down there.
func TestFloorWatermarkNeverDropsBelowTheApprovedFloor(t *testing.T) {
	require := require.New(t)
	ctx := context.Background()

	repo := &fakeCohortRepository{watermark: 100, watermarkFound: true}
	selector := NewSelector(repo)

	got, err := selector.FloorWatermark(ctx, "", 2, 500)
	require.NoError(err)
	require.Equal(uint64(500), got, "a watermark below the approved floor must not lower the probe start")
	require.Equal(uint64(500), repo.watermarkMinArg, "the approved floor bounds the lookup")
}

// TestFloorWatermarkAdvancesPastRetiredHistory is the whole point of the
// mechanism: everything under the watermark is already retired, so the probe
// starts there and the height range stops growing with the chain.
func TestFloorWatermarkAdvancesPastRetiredHistory(t *testing.T) {
	require := require.New(t)
	ctx := context.Background()

	repo := &fakeCohortRepository{watermark: 439_331_000, watermarkFound: true}
	selector := NewSelector(repo)

	got, err := selector.FloorWatermark(ctx, "", 2, 439_000_000)
	require.NoError(err)
	require.Equal(uint64(439_331_000), got)
}

// TestFloorWatermarkHoldsWhenNoWorkRemains guards the stranding case. With no
// outstanding work the floor stays put rather than jumping to some higher
// height, because rows consolidated moments ago are not due yet and advancing
// past them would put them permanently below the floor.
func TestFloorWatermarkHoldsWhenNoWorkRemains(t *testing.T) {
	require := require.New(t)
	ctx := context.Background()

	repo := &fakeCohortRepository{watermarkFound: false}
	selector := NewSelector(repo)

	got, err := selector.FloorWatermark(ctx, "", 2, 439_000_000)
	require.NoError(err)
	require.Equal(uint64(439_000_000), got)
}

func TestFloorWatermarkRejectsBadInput(t *testing.T) {
	require := require.New(t)
	ctx := context.Background()

	repo := &fakeCohortRepository{watermarkErr: errors.New("boom")}
	_, err := NewSelector(repo).FloorWatermark(ctx, "", 2, 1)
	require.Error(err)
	require.Contains(err.Error(), "boom")

	var nilSelector *Selector
	_, err = nilSelector.FloorWatermark(ctx, "", 2, 1)
	require.Error(err)

	_, err = NewSelector(&fakeCohortRepository{}).FloorWatermark(ctx, "nope", 2, 1)
	require.Error(err, "an unsupported generation must not silently return the approved floor")
}

// TestRetentionFloorWatermarkSQLIsBoundedAndIndexable pins the two properties
// that keep this lookup affordable without a dedicated index. The height bound
// is load-bearing: unbounded, the same query took 58s on production against 97ms
// bounded at the approval floor.
func TestRetentionFloorWatermarkSQLIsBoundedAndIndexable(t *testing.T) {
	require := require.New(t)
	ctx := context.Background()

	recorder := &recordingCohortQuerier{}
	_, _, err := retentionFloorWatermark(ctx, recorder, "v2", 2, 439_000_000)
	require.ErrorIs(err, errRecordingQuerier)

	// 1. The height bound must be present. Removing it is the one change that
	//    turns this from a 97ms lookup into a full walk of retired history.
	require.Contains(recorder.query, "shadow.height >= $2")

	// 2. Generation is matched with equality, never the null-safe form that
	//    cannot use a btree index at all.
	require.Contains(recorder.query, "shadow.single_block_storage_generation = $3")
	require.NotContains(recorder.query, "IS NOT DISTINCT FROM")

	require.Equal([]any{uint32(2), uint64(439_000_000), "v2"}, recorder.args)
}

// TestRetentionFloorWatermarkSQLLegacyGenerationBindsNoArgument mirrors the
// selector's handling of the legacy generation, which is stored as NULL. It
// binds no generation argument, so a mismatch here surfaces as a driver-level
// "bind message supplies N parameters" error rather than a wrong result.
func TestRetentionFloorWatermarkSQLLegacyGenerationBindsNoArgument(t *testing.T) {
	require := require.New(t)
	ctx := context.Background()

	recorder := &recordingCohortQuerier{}
	_, _, err := retentionFloorWatermark(ctx, recorder, "", 2, 10)
	require.ErrorIs(err, errRecordingQuerier)

	require.Contains(recorder.query, "shadow.single_block_storage_generation IS NULL")
	require.Equal([]any{uint32(2), uint64(10)}, recorder.args)

	highest := 0
	for _, match := range regexp.MustCompile(`\$(\d+)`).FindAllStringSubmatch(recorder.query, -1) {
		if n := len(match[1]); n > 0 {
			switch match[1] {
			case "1":
				highest = max(highest, 1)
			case "2":
				highest = max(highest, 2)
			case "3":
				highest = max(highest, 3)
			}
		}
	}
	require.Equal(highest, len(recorder.args),
		"the legacy form must bind exactly the placeholders it references")
}

// TestWatermarkIndexWasAddedAndDropped is a tripwire, not a schema check. The
// partial index for this query was added in 20260818000001 and dropped in
// 20260818000002 after production measurement: the planner also applied it to
// the due-cohort probe, which went from 14.4s to 36.8s in bare execution while
// its estimated cost *fell* tenfold. Disabling nested loops made it 91.6s, so
// the nested-loop plan was already the best available while the index existed —
// statistics tuning could not have saved it.
//
// This test exists so a future change cannot quietly re-add that index and
// reintroduce the regression without reading why it went away.
func TestWatermarkIndexWasAddedAndDropped(t *testing.T) {
	require := require.New(t)

	add, err := os.ReadFile(filepath.Clean(watermarkIndexAddMigration))
	require.NoError(err)
	drop, err := os.ReadFile(filepath.Clean(watermarkIndexDropMigration))
	require.NoError(err)

	const indexName = "idx_block_consolidation_shadow_retention_watermark"
	require.Contains(string(add), "CREATE INDEX CONCURRENTLY IF NOT EXISTS "+indexName)
	require.Contains(string(drop), "DROP INDEX CONCURRENTLY IF EXISTS "+indexName)
	require.Contains(string(drop), "-- +goose NO TRANSACTION",
		"DROP INDEX CONCURRENTLY cannot run inside goose's transaction")

	// The drop must be the later migration, or a fresh database would end up
	// with the index still in place.
	require.Less(
		filepath.Base(watermarkIndexAddMigration),
		filepath.Base(watermarkIndexDropMigration),
		"the drop migration must sort after the add migration",
	)

	// No live migration may leave this index behind. Scan every migration for a
	// CREATE of it other than the original and the down-section of the drop.
	dir := filepath.Dir(watermarkIndexAddMigration)
	entries, err := os.ReadDir(dir)
	require.NoError(err)
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sql") {
			continue
		}
		if entry.Name() == filepath.Base(watermarkIndexAddMigration) ||
			entry.Name() == filepath.Base(watermarkIndexDropMigration) {
			continue
		}
		body, err := os.ReadFile(filepath.Clean(filepath.Join(dir, entry.Name())))
		require.NoError(err)
		require.NotContains(string(body), indexName,
			"%s references %s; see 20260818000002 for why it was dropped", entry.Name(), indexName)
	}
}

// TestRetentionFloorWatermarkQueryNeedsNoIndex documents the consequence of the
// drop: the query must not assume any index exists beyond the pre-existing
// (tag, height) one it now rides.
func TestRetentionFloorWatermarkQueryNeedsNoIndex(t *testing.T) {
	require := require.New(t)

	recorder := &recordingCohortQuerier{}
	_, _, err := retentionFloorWatermark(context.Background(), recorder, "v2", 2, 439_000_000)
	require.ErrorIs(err, errRecordingQuerier)

	// The clause that existed only to match the dropped partial index is gone.
	require.NotContains(recorder.query, "single_block_storage_generation IS NOT NULL")

	// The watermark must not filter on due time or validation state. Both are
	// reasons a row is not deleted *yet*, so including them would let the floor
	// step over live work.
	require.NotContains(recorder.query, "single_block_delete_after")
	require.NotContains(recorder.query, "validated_at")
}

// TestRetentionFloorWatermarkInRangeSQLIsBoundedBothEnds pins the bounded
// form used by floor reconciliation (INF-1571): both bounds present on the
// indexed height column, generation matched with equality, and the
// placeholders renumbered so the generation argument still binds.
func TestRetentionFloorWatermarkInRangeSQLIsBoundedBothEnds(t *testing.T) {
	require := require.New(t)
	ctx := context.Background()

	recorder := &recordingCohortQuerier{}
	_, _, err := retentionFloorWatermarkInRange(ctx, recorder, "v2", 2, 439_000_000, 440_000_000)
	require.ErrorIs(err, errRecordingQuerier)

	require.Contains(recorder.query, "shadow.height >= $2")
	require.Contains(recorder.query, "shadow.height < $3")
	require.Contains(recorder.query, "shadow.single_block_storage_generation = $4")
	require.NotContains(recorder.query, "IS NOT DISTINCT FROM")
	require.Equal([]any{uint32(2), uint64(439_000_000), uint64(440_000_000), "v2"}, recorder.args)
}

func TestRetentionFloorWatermarkInRangeLegacyGenerationBindsNoArgument(t *testing.T) {
	require := require.New(t)
	ctx := context.Background()

	recorder := &recordingCohortQuerier{}
	_, _, err := retentionFloorWatermarkInRange(ctx, recorder, "", 2, 10, 20)
	require.ErrorIs(err, errRecordingQuerier)
	require.Contains(recorder.query, "shadow.single_block_storage_generation IS NULL")
	require.Equal([]any{uint32(2), uint64(10), uint64(20)}, recorder.args)
}

// TestRetentionFloorWatermarkInRangeEmptyRangeDoesNotQuery: an empty or
// inverted range is a no-op rather than a query with a contradictory bound.
func TestRetentionFloorWatermarkInRangeEmptyRangeDoesNotQuery(t *testing.T) {
	require := require.New(t)
	recorder := &recordingCohortQuerier{}
	_, found, err := retentionFloorWatermarkInRange(context.Background(), recorder, "v2", 2, 20, 20)
	require.NoError(err)
	require.False(found)
	require.Empty(recorder.query)
}

// TestRetentionFloorWatermarkUnboundedSQLUnchanged guards that adding the
// bounded form did not alter the unbounded query's placeholders, which the
// cron's main walk still relies on.
func TestRetentionFloorWatermarkUnboundedSQLUnchanged(t *testing.T) {
	require := require.New(t)
	recorder := &recordingCohortQuerier{}
	_, _, err := retentionFloorWatermark(context.Background(), recorder, "v2", 2, 439_000_000)
	require.ErrorIs(err, errRecordingQuerier)
	require.NotContains(recorder.query, "shadow.height < ")
	require.Contains(recorder.query, "shadow.single_block_storage_generation = $3")
	require.Equal([]any{uint32(2), uint64(439_000_000), "v2"}, recorder.args)
}

// TestFloorWatermarkInRangeDoesNotClamp: unlike FloorWatermark, a found row is
// returned as-is — the caller is checking a range it believed retired, and
// the row itself is the finding.
func TestFloorWatermarkInRangeDoesNotClamp(t *testing.T) {
	require := require.New(t)
	ctx := context.Background()

	repo := &fakeCohortRepository{inRangeHeights: []uint64{430_000_000, 435_000_000}}
	selector := NewSelector(repo)

	got, found, err := selector.FloorWatermarkInRange(ctx, "v2", 2, 423_000_000, 436_000_000)
	require.NoError(err)
	require.True(found)
	require.Equal(uint64(430_000_000), got)
	require.Equal([][2]uint64{{423_000_000, 436_000_000}}, repo.inRangeCalls)

	_, found, err = selector.FloorWatermarkInRange(ctx, "v2", 2, 436_000_000, 436_000_000)
	require.NoError(err)
	require.False(found, "an empty range has nothing to find")
	require.Len(repo.inRangeCalls, 1, "an empty range must not reach the repository")
}
