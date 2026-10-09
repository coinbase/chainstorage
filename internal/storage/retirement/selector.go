package retirement

import (
	"context"
	"math"
	"time"

	"golang.org/x/xerrors"
)

const MaxRetentionCohortsPerWorkflow = 250

type (
	RetentionCohort struct {
		ConsolidatedObjectKey string    `json:"consolidated_object_key"`
		StartHeight           uint64    `json:"start_height"`
		EndHeight             uint64    `json:"end_height"`
		RowCount              uint64    `json:"row_count"`
		EligibleAt            time.Time `json:"eligible_at"`
		Pending               bool      `json:"pending"`
	}

	// DueCohortCursor marks exactly how far due-candidate enumeration got when
	// a pass stopped on its expansion budget. It is a KEYSET cursor, not a
	// height watermark, because candidate groups are not height-disjoint: a
	// reorg can leave several consolidated objects covering the same or
	// overlapping heights while canonical_blocks points at only one. Resuming
	// from "last examined end height + 1" would filter out an unexamined
	// candidate that happens to start at or below that height, and if no due
	// work exists above the bound the ring resets and re-walks the same dead
	// prefix, starving that candidate permanently.
	//
	// The fields mirror the two candidate orderings exactly: bounded selection
	// compares (StartHeight, ObjectKey); open-ended selection compares
	// (EligibleAt, StartHeight, ObjectKey). A zero cursor starts from the
	// beginning of the requested range.
	DueCohortCursor struct {
		EligibleAt  time.Time
		StartHeight uint64
		ObjectKey   string
	}

	// CohortRepository must return row-disjoint pending and due aggregates.
	// Both slices must come from one database snapshot. Selector merges
	// aggregates for the same consolidated object by summing their row counts.
	//
	// ListRetentionCohorts takes an `after` cursor and returns the next one: a
	// non-zero returned cursor means due selection stopped on its expansion
	// budget with candidates still unexamined, and names exactly where to
	// resume. Callers that advance a search window on an empty result must
	// instead re-probe the SAME window with that cursor, or work behind a large
	// dead prefix is stranded. A zero returned cursor means candidates were
	// exhausted and the window really is done.
	CohortRepository interface {
		ListRetentionCohorts(
			ctx context.Context,
			bucket string,
			storageGeneration string,
			tag uint32,
			startHeight uint64,
			endHeight uint64,
			eligibilityCutoff time.Time,
			limit int,
			after DueCohortCursor,
		) ([]RetentionCohort, []RetentionCohort, DueCohortCursor, error)

		// RetentionFloorWatermark returns the lowest height at or above
		// minHeight that still holds an undeleted single-block object, and
		// whether such a row exists. It is part of this interface rather than
		// an optional one the implementation may satisfy: a missing watermark
		// silently stops the probe floor from advancing, and the resulting
		// range growth is exactly the failure this mechanism exists to
		// prevent. Compile-time enforcement is cheaper than discovering it in
		// production.
		RetentionFloorWatermark(
			ctx context.Context,
			storageGeneration string,
			tag uint32,
			minHeight uint64,
		) (uint64, bool, error)

		// RetentionFloorWatermarkInRange is RetentionFloorWatermark bounded
		// above: the lowest height in [minHeight, endHeight) that still holds
		// an undeleted single-block object. The cron uses it to reconcile a
		// persisted floor one bounded chunk at a time (INF-1571): a stray
		// undeleted row below the persisted floor is exactly what an
		// unbounded walk would find and a persisted hint would skip, so the
		// bound is what keeps that check affordable per tick.
		RetentionFloorWatermarkInRange(
			ctx context.Context,
			storageGeneration string,
			tag uint32,
			minHeight uint64,
			endHeight uint64,
		) (uint64, bool, error)

		// RetentionDueFloor returns the lowest height in [minHeight, endHeight)
		// that is actually DUE at eligibilityCutoff, and whether such a row
		// exists.
		//
		// This is not the same question as RetentionFloorWatermark, and the
		// difference is load-bearing once the probe is windowed. The watermark
		// is deliberately conservative by omission and pins to any undeleted
		// row, due or not. A windowed probe anchored on the watermark can
		// therefore search [notDueRow, notDueRow+window) forever: it returns no
		// cohorts, the tick idles, the watermark is recomputed to the same
		// not-due row next tick, and due work above the window is never
		// discovered until the low row matures. Anchoring the window here
		// instead keeps the probe on due work.
		//
		// The returned floor is a CANDIDATE: never above the earliest
		// selectable row, but possibly below it, because join-level
		// selectability (canonical membership, metadata match, pending and
		// repair exclusion) is deliberately not evaluated here — see the
		// implementation for why folding it in reintroduces the timeout. The
		// cron advances the search window past floors that select nothing.
		RetentionDueFloor(
			ctx context.Context,
			storageGeneration string,
			tag uint32,
			minHeight uint64,
			endHeight uint64,
			eligibilityCutoff time.Time,
		) (uint64, bool, error)

		// RetentionEarliestDeadline returns the earliest
		// single_block_delete_after among the SAME candidate rows
		// RetentionDueFloor considers in [minHeight, endHeight), and whether
		// any such row exists.
		//
		// It exists so a caller can decide "is anything due at all" without
		// paying for the due-floor walk. Because both carry an identical
		// predicate set, deadline > cutoff is exactly "RetentionDueFloor would
		// report nothing due at cutoff" — not a heuristic, but MIN's defining
		// property. The predicate sets must therefore stay in lockstep;
		// TestEarliestDeadlineAndDueFloorShareACandidateSet in
		// earliest_deadline_test.go fails if they drift.
		//
		// No cutoff is taken deliberately: the caller compares against the
		// cutoff it will hand to RetentionDueFloor, so one clock decides both.
		RetentionEarliestDeadline(
			ctx context.Context,
			storageGeneration string,
			tag uint32,
			minHeight uint64,
			endHeight uint64,
		) (time.Time, bool, error)
	}

	Selector struct {
		repo CohortRepository
	}
)

func NewSelector(repo CohortRepository) *Selector {
	return &Selector{repo: repo}
}

func (s *Selector) Select(
	ctx context.Context,
	bucket string,
	storageGeneration string,
	tag uint32,
	startHeight uint64,
	endHeight uint64,
	eligibilityCutoff time.Time,
	limit int,
	after DueCohortCursor,
) ([]RetentionCohort, bool, DueCohortCursor, error) {
	if s == nil || s.repo == nil {
		return nil, false, DueCohortCursor{}, xerrors.New("retention cohort repository is required")
	}
	if bucket == "" {
		return nil, false, DueCohortCursor{}, xerrors.New("retention cohort bucket is required")
	}
	if !isValidStorageGeneration(storageGeneration) {
		return nil, false, DueCohortCursor{}, xerrors.Errorf("unsupported retention cohort storage generation %q", storageGeneration)
	}
	if limit <= 0 || limit > MaxRetentionCohortsPerWorkflow {
		return nil, false, DueCohortCursor{}, xerrors.Errorf(
			"retention cohort limit must be between 1 and %d: %d",
			MaxRetentionCohortsPerWorkflow,
			limit,
		)
	}
	if endHeight == 0 && startHeight != 0 {
		return nil, false, DueCohortCursor{}, xerrors.New("retention selection end height is required when start height is set")
	}
	if endHeight != 0 && endHeight <= startHeight {
		return nil, false, DueCohortCursor{}, xerrors.Errorf("invalid retention selection range [%d, %d)", startHeight, endHeight)
	}
	if eligibilityCutoff.IsZero() {
		return nil, false, DueCohortCursor{}, xerrors.New("retention selection eligibility cutoff is required")
	}

	queryLimit := limit + 1
	pending, due, nextCursor, err := s.repo.ListRetentionCohorts(
		ctx,
		bucket,
		storageGeneration,
		tag,
		startHeight,
		endHeight,
		eligibilityCutoff,
		queryLimit,
		after,
	)
	if err != nil {
		return nil, false, DueCohortCursor{}, xerrors.Errorf("failed to list retention cohorts: %w", err)
	}

	result := make([]RetentionCohort, 0, queryLimit)
	positions := make(map[string]int, queryLimit)
	appendOrMerge := func(cohort RetentionCohort) error {
		if cohort.ConsolidatedObjectKey == "" || cohort.EndHeight <= cohort.StartHeight || cohort.RowCount == 0 || cohort.EligibleAt.IsZero() {
			return xerrors.Errorf("invalid retention cohort: %+v", cohort)
		}
		if position, ok := positions[cohort.ConsolidatedObjectKey]; ok {
			existing := &result[position]
			if cohort.StartHeight < existing.StartHeight {
				existing.StartHeight = cohort.StartHeight
			}
			if cohort.EndHeight > existing.EndHeight {
				existing.EndHeight = cohort.EndHeight
			}
			if cohort.RowCount > math.MaxUint64-existing.RowCount {
				return xerrors.Errorf(
					"retention cohort row count overflow for object %q",
					cohort.ConsolidatedObjectKey,
				)
			}
			existing.RowCount += cohort.RowCount
			if cohort.EligibleAt.After(existing.EligibleAt) {
				existing.EligibleAt = cohort.EligibleAt
			}
			existing.Pending = existing.Pending || cohort.Pending
			return nil
		}
		if len(result) >= queryLimit {
			return nil
		}
		positions[cohort.ConsolidatedObjectKey] = len(result)
		result = append(result, cohort)
		return nil
	}

	for _, cohort := range pending {
		cohort.Pending = true
		if err := appendOrMerge(cohort); err != nil {
			return nil, false, DueCohortCursor{}, err
		}
	}
	for _, cohort := range due {
		if err := appendOrMerge(cohort); err != nil {
			return nil, false, DueCohortCursor{}, err
		}
	}
	hasMore := len(result) > limit
	if hasMore {
		result = result[:limit]
	}
	// A non-zero cursor means selection stopped on its expansion budget with
	// candidates still unexamined, so there is more work regardless of how many
	// cohorts this page produced. Reporting hasMore only from the page length
	// would tell the caller "that is everything" while unexamined candidates
	// remain.
	if !nextCursor.IsZero() {
		hasMore = true
	}
	return result, hasMore, nextCursor, nil
}

// IsZero reports whether the cursor is unset, meaning enumeration should start
// at the beginning of the requested range (or, when returned, that candidates
// were exhausted).
func (c DueCohortCursor) IsZero() bool { return c.ObjectKey == "" }

// MaxRetentionPrimingLookahead bounds how many upcoming cohorts one Select pass
// primes safety observations for. Two workflow batches of lookahead lets the
// next continue-as-new run pass its quiescence gate on the first attempt.
const MaxRetentionPrimingLookahead = 2 * MaxRetentionCohortsPerWorkflow

// LookaheadKeys returns the distinct consolidated object keys of upcoming
// retention cohorts, beyond the per-workflow selection cap, so their safety
// quiescence clocks can be primed ahead of processing. It is advisory only:
// selection and execution never trust its output.
func (s *Selector) LookaheadKeys(
	ctx context.Context,
	bucket string,
	storageGeneration string,
	tag uint32,
	startHeight uint64,
	endHeight uint64,
	eligibilityCutoff time.Time,
	limit int,
) ([]string, error) {
	if s == nil || s.repo == nil {
		return nil, xerrors.New("retention cohort repository is required")
	}
	if bucket == "" || eligibilityCutoff.IsZero() {
		return nil, xerrors.New("retention priming lookahead requires a bucket and eligibility cutoff")
	}
	if !isValidStorageGeneration(storageGeneration) {
		return nil, xerrors.Errorf("unsupported retention cohort storage generation %q", storageGeneration)
	}
	if limit <= 0 || limit > MaxRetentionPrimingLookahead {
		return nil, xerrors.Errorf(
			"retention priming lookahead limit must be between 1 and %d: %d",
			MaxRetentionPrimingLookahead,
			limit,
		)
	}
	// Lookahead only primes safety observations for upcoming keys; it never
	// advances a search window, so the continuation height is not its concern.
	pending, due, _, err := s.repo.ListRetentionCohorts(
		ctx,
		bucket,
		storageGeneration,
		tag,
		startHeight,
		endHeight,
		eligibilityCutoff,
		limit,
		DueCohortCursor{},
	)
	if err != nil {
		return nil, xerrors.Errorf("failed to list retention cohorts for priming: %w", err)
	}
	keys := make([]string, 0, len(pending)+len(due))
	seen := make(map[string]struct{}, len(pending)+len(due))
	for _, cohort := range append(pending, due...) {
		if cohort.ConsolidatedObjectKey == "" {
			continue
		}
		if _, duplicate := seen[cohort.ConsolidatedObjectKey]; duplicate {
			continue
		}
		seen[cohort.ConsolidatedObjectKey] = struct{}{}
		keys = append(keys, cohort.ConsolidatedObjectKey)
		if len(keys) >= limit {
			break
		}
	}
	return keys, nil
}

// FloorWatermark resolves the height the next probe should start from, given
// the operator's approved floor.
//
// The returned height is never below approvedStartHeight: that value is the
// authorization boundary and retention may not delete underneath it, so work
// found below it — a repair promoted into an old height, say — must not pull
// the probe down there. Everything between approvedStartHeight and the
// watermark has already been retired, so skipping it loses nothing.
//
// When the generation has no outstanding work at all, the approved floor is
// returned unchanged rather than some higher value. Advancing past the end of
// known work would put freshly consolidated rows below the floor before their
// retention delay expires, which is precisely how a floor strands data.
func (s *Selector) FloorWatermark(
	ctx context.Context,
	storageGeneration string,
	tag uint32,
	approvedStartHeight uint64,
) (uint64, error) {
	if s == nil || s.repo == nil {
		return 0, xerrors.New("retention cohort repository is required")
	}
	if !isValidStorageGeneration(storageGeneration) {
		return 0, xerrors.Errorf("unsupported retention cohort storage generation %q", storageGeneration)
	}
	watermark, found, err := s.repo.RetentionFloorWatermark(ctx, storageGeneration, tag, approvedStartHeight)
	if err != nil {
		return 0, xerrors.Errorf("failed to resolve retention floor watermark: %w", err)
	}
	if !found || watermark < approvedStartHeight {
		return approvedStartHeight, nil
	}
	return watermark, nil
}

// FloorWatermarkInRange reports the lowest undeleted single-block height in
// [minHeight, endHeight), and whether one exists. Unlike FloorWatermark it
// does not clamp: the caller is checking a range it already believes to be
// fully retired, so a found row is the finding.
func (s *Selector) FloorWatermarkInRange(
	ctx context.Context,
	storageGeneration string,
	tag uint32,
	minHeight uint64,
	endHeight uint64,
) (uint64, bool, error) {
	if s == nil || s.repo == nil {
		return 0, false, xerrors.New("retention cohort repository is required")
	}
	if !isValidStorageGeneration(storageGeneration) {
		return 0, false, xerrors.Errorf("unsupported retention cohort storage generation %q", storageGeneration)
	}
	if endHeight <= minHeight {
		return 0, false, nil
	}
	height, found, err := s.repo.RetentionFloorWatermarkInRange(ctx, storageGeneration, tag, minHeight, endHeight)
	if err != nil {
		return 0, false, xerrors.Errorf("failed to resolve bounded retention floor watermark: %w", err)
	}
	return height, found, nil
}

// EarliestDeadline reports when the next candidate row in
// [minHeight, endHeight) falls due, and whether any candidate exists at all.
// found=false means the range holds no undeleted single-block object; it is
// never an error and never means "due now".
func (s *Selector) EarliestDeadline(
	ctx context.Context,
	storageGeneration string,
	tag uint32,
	minHeight uint64,
	endHeight uint64,
) (time.Time, bool, error) {
	if s == nil || s.repo == nil {
		return time.Time{}, false, xerrors.New("retention cohort repository is required")
	}
	if !isValidStorageGeneration(storageGeneration) {
		return time.Time{}, false, xerrors.Errorf("unsupported retention cohort storage generation %q", storageGeneration)
	}
	if endHeight <= minHeight {
		return time.Time{}, false, nil
	}
	deadline, found, err := s.repo.RetentionEarliestDeadline(ctx, storageGeneration, tag, minHeight, endHeight)
	if err != nil {
		return time.Time{}, false, xerrors.Errorf("failed to resolve retention earliest deadline: %w", err)
	}
	return deadline, found, nil
}

// DueFloor resolves the lowest height in [floorHeight, endHeight) that is due
// at eligibilityCutoff. found is false when nothing is due there, which is the
// caller's signal to idle rather than to probe a window that cannot contain
// work. The result is clamped into [floorHeight, endHeight): a repository
// answer outside the requested range is a bug, and failing closed to
// "nothing due" beats anchoring a window outside the approved envelope.
func (s *Selector) DueFloor(
	ctx context.Context,
	storageGeneration string,
	tag uint32,
	floorHeight uint64,
	endHeight uint64,
	eligibilityCutoff time.Time,
) (uint64, bool, error) {
	if s == nil || s.repo == nil {
		return 0, false, xerrors.New("retention cohort repository is required")
	}
	if !isValidStorageGeneration(storageGeneration) {
		return 0, false, xerrors.Errorf("unsupported retention cohort storage generation %q", storageGeneration)
	}
	if endHeight <= floorHeight {
		return 0, false, nil
	}
	dueFloor, found, err := s.repo.RetentionDueFloor(ctx, storageGeneration, tag, floorHeight, endHeight, eligibilityCutoff)
	if err != nil {
		return 0, false, xerrors.Errorf("failed to resolve retention due floor: %w", err)
	}
	if !found || dueFloor >= endHeight {
		return 0, false, nil
	}
	if dueFloor < floorHeight {
		return floorHeight, true, nil
	}
	return dueFloor, true, nil
}
