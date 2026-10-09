package cscbrepairlock

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
)

// AcquireTag takes the exclusive repair-transition fence for a tag.
// Repair and rehome callers use this before locking metadata or shadow rows.
func AcquireTag(ctx context.Context, tx *sql.Tx, tag uint32) error {
	key := fmt.Sprintf("cscb_repair_tag/%d", tag)
	if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 2))`, key); err != nil {
		return fmt.Errorf("failed to acquire CSCB repair tag lock: %w", err)
	}
	return nil
}

// AcquireTagShared takes the placement-writer side of the repair fence.
// Shared writer locks may coexist, but conflict with AcquireTag.
func AcquireTagShared(ctx context.Context, tx *sql.Tx, tag uint32) error {
	key := fmt.Sprintf("cscb_repair_tag/%d", tag)
	if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock_shared(hashtextextended($1, 2))`, key); err != nil {
		return fmt.Errorf("failed to acquire shared CSCB repair tag lock: %w", err)
	}
	return nil
}

// AcquireTags takes exclusive tag locks in ascending order.
func AcquireTags(ctx context.Context, tx *sql.Tx, tags []uint32) error {
	return acquireTags(ctx, tx, tags, AcquireTag)
}

// AcquireTagsShared takes shared tag locks in ascending order.
func AcquireTagsShared(ctx context.Context, tx *sql.Tx, tags []uint32) error {
	return acquireTags(ctx, tx, tags, AcquireTagShared)
}

func acquireTags(
	ctx context.Context,
	tx *sql.Tx,
	tags []uint32,
	acquire func(context.Context, *sql.Tx, uint32) error,
) error {
	unique := make(map[uint32]struct{}, len(tags))
	ordered := make([]uint32, 0, len(tags))
	for _, tag := range tags {
		if _, ok := unique[tag]; ok {
			continue
		}
		unique[tag] = struct{}{}
		ordered = append(ordered, tag)
	}
	sort.Slice(ordered, func(i, j int) bool { return ordered[i] < ordered[j] })
	for _, tag := range ordered {
		if err := acquire(ctx, tx, tag); err != nil {
			return err
		}
	}
	return nil
}
