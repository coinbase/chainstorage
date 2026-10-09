package retirementlock

import (
	"context"
	"database/sql"
	"fmt"
)

// Acquire serializes single-block object uploads with retirement preparation even
// before block metadata exists. Hash collisions only cause safe over-serialization.
func Acquire(ctx context.Context, tx *sql.Tx, tag uint32, height uint64, hash string) error {
	key := fmt.Sprintf("%d\x1f%d\x1f%s", tag, height, hash)
	if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, key); err != nil {
		return fmt.Errorf("failed to acquire single-block object retirement lock: %w", err)
	}
	return nil
}
