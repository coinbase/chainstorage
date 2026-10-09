package retirement

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// pendingStateIndexMigration defines idx_block_single_block_retention_pending,
// the partial index every pending-retirement lookup depends on.
const pendingStateIndexMigration = "../metastorage/postgres/db/migrations/20260713000002_harden_single_block_retention.sql"

// TestPendingRetirementStatesSQLMatchesPartialIndexPredicate pins the inlined
// state literals to the migration's index predicate. Postgres only uses a
// partial index when it can prove the query filter implies the index
// predicate, so any drift here silently degrades pending lookups to a full
// scan of a table that only grows (INF-1330).
func TestPendingRetirementStatesSQLMatchesPartialIndexPredicate(t *testing.T) {
	require := require.New(t)
	require.Equal(
		"'eligible', 'deleting', 'deleted_pending_verification'",
		pendingRetirementStatesSQL,
	)

	source, err := os.ReadFile(filepath.Clean(pendingStateIndexMigration))
	require.NoError(err)
	predicate := regexp.MustCompile(
		`CREATE INDEX idx_block_single_block_retention_pending\s+ON block_single_block_retention \(tag, height, block_metadata_id\)\s+WHERE state IN \(([^)]*)\);`,
	).FindStringSubmatch(string(source))
	require.Len(predicate, 2, "index definition not found in %s", pendingStateIndexMigration)
	require.Equal(
		normalizeStateList(predicate[1]),
		normalizeStateList(pendingRetirementStatesSQL),
		"inlined states must match the partial index predicate exactly",
	)
}

// TestPendingRetirementQueriesInlineStateLiterals fails if a pending-state
// filter regresses to bind parameters, which is the exact form the planner
// cannot match against the partial index.
func TestPendingRetirementQueriesInlineStateLiterals(t *testing.T) {
	require := require.New(t)
	for _, path := range []string{"postgres_repository.go", "selector_postgres.go"} {
		source, err := os.ReadFile(filepath.Clean(path))
		require.NoError(err)
		matches := regexp.MustCompile(`(?i)retention\.state IN \(\s*\$`).FindAllString(string(source), -1)
		require.Empty(
			matches,
			"%s filters retention.state with bind parameters; inline pendingRetirementStatesSQL so the partial index stays usable",
			path,
		)
	}
}

func TestRenderRetirementStateLiteralsRejectsUnsafeStates(t *testing.T) {
	require := require.New(t)
	require.Equal("'eligible'", renderRetirementStateLiterals(RetirementStateEligible))
	require.Panics(func() { renderRetirementStateLiterals("deleted'; DROP TABLE") })
	require.Panics(func() { renderRetirementStateLiterals("") })
}

func normalizeStateList(value string) string {
	fields := strings.Split(value, ",")
	normalized := make([]string, 0, len(fields))
	for _, field := range fields {
		normalized = append(normalized, strings.TrimSpace(field))
	}
	return strings.Join(normalized, ",")
}
