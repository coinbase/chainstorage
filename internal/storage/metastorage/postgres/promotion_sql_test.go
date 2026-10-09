package postgres

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestPromotionStatementsBoundOnlyBlockMetadata pins the shape that unwedged
// robinhood-mainnet's promotion (INF-2023). The selects must bound
// block_metadata by the window so idx_block_metadata_unconsolidated is read by
// (tag, height), must not bound block_consolidation_shadow by the same constant
// range (that range becomes the shadow index condition and turns each shadow
// lookup into a range scan), and the updated CTE must not re-check
// byte_length IS NULL (that predicate lets the unbounded partial index drive
// the update). TestPromotionPlansBoundTheUnconsolidatedIndexByHeight checks the
// resulting plans against a database.
func TestPromotionStatementsBoundOnlyBlockMetadata(t *testing.T) {
	require := require.New(t)
	require.Contains(promotionQuery, "updated AS (")
	require.Contains(promotionQuery, "retired AS (")
	candidates := promotionQuery[:strings.Index(promotionQuery, "updated AS (")]
	updated := promotionQuery[strings.Index(promotionQuery, "updated AS ("):strings.Index(promotionQuery, "retired AS (")]

	for name, statement := range map[string]string{
		"promotionInvalidShadowQuery": promotionInvalidShadowQuery,
		"promotionQuery candidates":   candidates,
	} {
		for _, bound := range []string{"bm.tag = $1", "bm.height >= $2", "bm.height < $3"} {
			require.Contains(statement, bound, "%s must bound block_metadata by the window", name)
		}
		require.NotRegexp(`shadow\.height\s*[<>]=?\s*\$[23]`, statement,
			"%s must not bound block_consolidation_shadow by the constant window", name)
	}
	require.NotContains(updated, "byte_length IS NULL",
		"the updated CTE must reach candidates by primary key, not re-check byte_length")
}
