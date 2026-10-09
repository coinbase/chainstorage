package retirement

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// generationDueIndexMigration defines the generation-aware due index that the
// equality form below is what makes reachable.
const generationDueIndexMigration = "../metastorage/postgres/db/migrations/20260817000001_add_generation_aware_retention_due_index.sql"

func TestStorageGenerationMatchRendersIndexablePredicates(t *testing.T) {
	require := require.New(t)

	require.Equal(
		"metadata.storage_generation = $7",
		storageGenerationMatch("v2", "$7", "metadata.storage_generation"),
	)
	require.True(storageGenerationIsBound("v2"))

	// The legacy generation is stored as NULL, so it needs an IS NULL test and
	// binds no argument.
	require.Equal(
		"metadata.storage_generation IS NULL",
		storageGenerationMatch("", "$7", "metadata.storage_generation"),
	)
	require.False(storageGenerationIsBound(""))

	// Several columns share one placeholder.
	require.Equal(
		"a = $7\n\t\t\tAND b = $7",
		storageGenerationMatch("v2", "$7", "a", "b"),
	)
}

// TestSelectorQueriesAvoidIsNotDistinctFrom fails if a generation filter
// regresses to the null-safe form. `col IS NOT DISTINCT FROM ...` cannot use a
// btree index — verified against production, where the identical predicate
// written as `=` plans an Index Only Scan and the IS NOT DISTINCT FROM form
// falls back to a parallel sequential scan — which silently makes the
// generation-aware due index useless (INF-1330).
func TestSelectorQueriesAvoidIsNotDistinctFrom(t *testing.T) {
	require := require.New(t)
	source, err := os.ReadFile(filepath.Clean("selector_postgres.go"))
	require.NoError(err)

	// Scan code only: the doc comment on storageGenerationMatch names the
	// anti-pattern in order to explain it.
	offenders := make([]int, 0)
	for number, line := range strings.Split(string(source), "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "//") {
			continue
		}
		if strings.Contains(line, "IS NOT DISTINCT FROM") {
			offenders = append(offenders, number+1)
		}
	}
	require.Empty(
		offenders,
		"selector_postgres.go must match storage generation with = or IS NULL so the due index stays usable; offending lines: %v",
		offenders,
	)
}

// TestGenerationDueIndexMatchesQueryShape pins the migration's index to the
// columns the probe actually filters, so the index and the query cannot drift
// apart silently.
func TestGenerationDueIndexMatchesQueryShape(t *testing.T) {
	require := require.New(t)
	source, err := os.ReadFile(filepath.Clean(generationDueIndexMigration))
	require.NoError(err)
	definition := regexp.MustCompile(`(?s)CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_block_consolidation_shadow_retention_due_generation.*?\((.*?)\)\s*WHERE`).
		FindStringSubmatch(string(source))
	require.Len(definition, 2, "index definition not found in %s", generationDueIndexMigration)

	columns := make([]string, 0, 4)
	for _, field := range strings.Split(definition[1], ",") {
		if trimmed := strings.TrimSpace(field); trimmed != "" {
			columns = append(columns, trimmed)
		}
	}
	require.Equal(
		[]string{"tag", "single_block_storage_generation", "single_block_delete_after", "height"},
		columns,
		"generation must lead the due index so the probe can seek past superseded generations",
	)
}

// TestDueCohortQueryBindsGenerationOnlyWhenReferenced guards the argument list
// against the placeholder/argument mismatch that conditional SQL invites: a
// bound-but-unreferenced parameter is a runtime error from the driver.
func TestDueCohortQueryBindsGenerationOnlyWhenReferenced(t *testing.T) {
	require := require.New(t)
	for _, generation := range []string{"v2", ""} {
		recorder := &recordingCohortQuerier{}
		_, _, err := listDueRetentionCohorts(
			context.Background(), recorder, generation, 2, 0, 0, time.Now().UTC(), 10, DueCohortCursor{},
		)
		// The recorder never returns rows; the query text and bound arguments
		// are what this asserts on.
		require.Error(err)
		placeholders := regexp.MustCompile(`\$(\d+)`).FindAllStringSubmatch(recorder.query, -1)
		highest := 0
		for _, match := range placeholders {
			index := 0
			for _, digit := range match[1] {
				index = index*10 + int(digit-'0')
			}
			if index > highest {
				highest = index
			}
		}
		require.Equal(highest, len(recorder.args),
			"generation=%q must bind exactly the placeholders the query references", generation)
	}
}

type recordingCohortQuerier struct {
	query string
	args  []any
}

var errRecordingQuerier = errors.New("recording querier does not execute")

func (r *recordingCohortQuerier) QueryContext(_ context.Context, query string, args ...any) (*sql.Rows, error) {
	r.query = query
	r.args = args
	return nil, errRecordingQuerier
}
