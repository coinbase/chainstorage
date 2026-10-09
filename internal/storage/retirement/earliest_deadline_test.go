package retirement

import (
	"context"
	"database/sql"
	"errors"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// sqlRecorder captures the SQL a query helper builds. QueryContext returns a
// concrete *sql.Rows that cannot be fabricated, so it records and fails: these
// tests assert on the STATEMENT, never on rows.
type sqlRecorder struct {
	query string
	args  []any
}

var errRecorded = errors.New("recorded")

func (r *sqlRecorder) QueryContext(_ context.Context, query string, args ...any) (*sql.Rows, error) {
	r.query = query
	r.args = args
	return nil, errRecorded
}

// The deadline gate is only safe because its candidate set is IDENTICAL to the
// due-floor walk's. With the same rows, MIN(single_block_delete_after) > cutoff
// is the definition of "no row has single_block_delete_after <= cutoff", so the
// cron can skip the walk without risking a missed sweep.
//
// That equivalence lives in two SQL literals that nothing else forces to agree.
// A predicate added to the walk and not the gate makes the gate blind to rows
// the walk would have found, and retention stalls silently until someone
// notices objects accumulating — the failure mode is a gap, not an error. These
// tests are the only thing holding the two in lockstep.

// Alternation is longest-first: <= and >= must precede < and =, or the shorter
// branch matches their prefix and the predicate is silently misparsed — which
// would make this test vacuously green, the one outcome it must never have.
var placeholderRE = regexp.MustCompile(`\$\d+`)

var sqlPredicateRE = regexp.MustCompile(`(?m)^\s*(?:AND\s+)?(shadow\.[a-z_]+(?:\s+IS\s+(?:NOT\s+)?NULL|\s*<>\s*''|\s*<=\s*\$\d+|\s*>=\s*\$\d+|\s*<\s*\$\d+|\s*=\s*\$\d+))`)

// predicatesOf extracts the shadow-column predicates from a query body so the
// two can be compared as sets rather than as text.
func predicatesOf(query string) map[string]bool {
	out := map[string]bool{}
	for _, m := range sqlPredicateRE.FindAllStringSubmatch(query, -1) {
		// Normalise whitespace, then placeholder ORDINALS: the gate binds one
		// fewer parameter than the walk (it takes no cutoff), so the same
		// predicate legitimately reads $4 in one and $5 in the other. The
		// operator still distinguishes them, so >= $N and < $N stay distinct.
		pred := strings.Join(strings.Fields(m[1]), " ")
		out[placeholderRE.ReplaceAllString(pred, "$$N")] = true
	}
	return out
}

func TestEarliestDeadlineAndDueFloorShareACandidateSet(t *testing.T) {
	require := require.New(t)

	gate := captureEarliestDeadlineSQL(t)
	walk := captureDueFloorSQL(t)

	gatePreds := predicatesOf(gate)
	walkPreds := predicatesOf(walk)
	require.NotEmpty(gatePreds, "failed to parse any predicate out of the gate query")
	require.NotEmpty(walkPreds, "failed to parse any predicate out of the due-floor query")

	// The walk carries exactly one predicate the gate must NOT have: the
	// eligibility comparison itself, which is the thing the gate replaces with
	// MIN(). Everything else must match, in both directions.
	const cutoffPredicate = "shadow.single_block_delete_after <= $N"
	require.True(walkPreds[cutoffPredicate],
		"due-floor query no longer compares single_block_delete_after to the cutoff parameter; "+
			"the gate's equivalence argument assumed it did")
	delete(walkPreds, cutoffPredicate)

	for pred := range walkPreds {
		require.True(gatePreds[pred],
			"due-floor walk filters on %q but the deadline gate does not: the gate is now BLIND to rows "+
				"the walk would select, so a tick can report nothing due while work waits. Add it to "+
				"retentionEarliestDeadline.", pred)
	}
	for pred := range gatePreds {
		require.True(walkPreds[pred],
			"deadline gate filters on %q but the due-floor walk does not: the gate can now hide rows the "+
				"walk would have found. Remove it from retentionEarliestDeadline.", pred)
	}
}

// The gate must not consult the database clock. The cron compares the returned
// deadline against an eligibilityCutoff computed with time.Now() in the
// process, and hands that same value to the walk. A gate that used SQL now()
// would put the two decisions on different clocks: harmless while the margin is
// days, and a skipped sweep at the transition, where the margin is seconds.
func TestEarliestDeadlineDoesNotUseTheDatabaseClock(t *testing.T) {
	query := captureEarliestDeadlineSQL(t)
	lowered := strings.ToLower(query)
	for _, banned := range []string{"now()", "current_timestamp", "localtimestamp", "transaction_timestamp", "statement_timestamp", "clock_timestamp"} {
		require.NotContains(t, lowered, banned,
			"the deadline gate must take no clock of its own: the cron compares its result against the same "+
				"eligibilityCutoff it passes to RetentionDueFloor, so app/database skew cannot open a gap")
	}
}

func captureEarliestDeadlineSQL(t *testing.T) string {
	t.Helper()
	rec := &sqlRecorder{}
	_, _, err := retentionEarliestDeadline(context.Background(), rec, "v2", 2, 100, 200)
	require.ErrorIs(t, err, errRecorded)
	require.NotEmpty(t, rec.query, "retentionEarliestDeadline issued no query")
	return rec.query
}

func captureDueFloorSQL(t *testing.T) string {
	t.Helper()
	rec := &sqlRecorder{}
	_, _, err := retentionDueFloor(context.Background(), rec, "v2", 2, 100, 200, time.Unix(0, 0).UTC())
	require.ErrorIs(t, err, errRecorded)
	require.NotEmpty(t, rec.query, "retentionDueFloor issued no query")
	return rec.query
}
