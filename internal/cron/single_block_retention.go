package cron

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/uber-go/tally/v4"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/cadence"
	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/storage/metastorage"
	metapostgres "github.com/coinbase/chainstorage/internal/storage/metastorage/postgres"
	"github.com/coinbase/chainstorage/internal/storage/retirement"
	"github.com/coinbase/chainstorage/internal/utils/fxparams"
	"github.com/coinbase/chainstorage/internal/utils/log"
	"github.com/coinbase/chainstorage/internal/workflow"
)

type (
	SingleBlockRetentionTaskParams struct {
		fx.In
		fxparams.Params
		Config               *config.Config
		Runtime              cadence.Runtime
		MetaStorage          metastorage.MetaStorage
		SingleBlockRetention *workflow.SingleBlockRetention
	}

	singleBlockRetentionTask struct {
		config               *config.Config
		logger               *zap.Logger
		metrics              tally.Scope
		runtime              cadence.Runtime
		metaStorage          metastorage.MetaStorage
		singleBlockRetention *workflow.SingleBlockRetention

		selectorFactory func(ctx context.Context) (*retirement.Selector, error)

		// probeResumeHeight is where the next tick's advance walk continues
		// after a tick exhausts maxRetentionProbeAdvances without finding
		// selectable work. Without it every tick restarts from the watermark
		// and re-walks the same dead windows, so selectable work beyond the
		// advance budget is starved permanently; with it consecutive exhausted
		// ticks ratchet through the dead zone, budget-sized step by step, and
		// reach whatever lies beyond.
		//
		// Deliberately in-memory, not persisted: a stored cursor would need
		// its own reconciliation against repairs and completed sweeps (the
		// same reason the watermark is recomputed every tick), while losing
		// this one on restart merely restarts the walk from the watermark —
		// the safe direction, re-inspecting windows rather than skipping any.
		// It is cleared whenever a tick launches, finds nothing due, or finds
		// the approved range complete, so dead-prefix rows that later become
		// selectable are re-inspected on the next full pass of the ring.
		// Atomic only as insurance against a misconfigured parallelism > 1;
		// the cron runs this task single-flight.
		probeResumeHeight atomic.Uint64

		// reconcileNextHeight is where the next tick's reconciliation chunk
		// starts when PersistFloorWatermark is on: the walk below the persisted
		// floor proceeds one bounded chunk per tick and wraps back to the
		// approval floor once it reaches the persisted value. In-memory for the
		// same reason as probeResumeHeight — losing it restarts the sweep from
		// the approval floor, which re-inspects rather than skips.
		reconcileNextHeight atomic.Uint64

		// dueCursorMu guards the budget-truncation cursor and the window it
		// belongs to. Like probeResumeHeight this is deliberately in-memory:
		// losing it on restart merely restarts the walk from the watermark,
		// which re-examines candidates rather than skipping any.
		dueCursorMu     sync.Mutex
		dueCursor       retirement.DueCohortCursor
		dueCursorWindow uint64
	}
)

const (
	autoRetainSuffix                        = "auto_retain"
	defaultSingleBlockRetentionCronSpec     = "@every 1h"
	defaultSingleBlockRetentionWindowBlocks = uint64(250_000)

	// maxRetentionProbeAdvances bounds how many consecutive probe windows one
	// tick may step past when a window's due rows all turn out unselectable
	// (covered by an active repair, missing canonical membership, and so on —
	// the due-floor candidate deliberately does not evaluate those; see
	// RetentionDueFloor). Each advance costs one cheap floor lookup plus one
	// bounded probe over a window that selects nothing, so the cap bounds
	// per-tick cost, not correctness: at 4 advances a tick steps over up to
	// 4 x window_blocks of solidly unselectable due rows, and a dead zone
	// larger than that (hundreds of consecutive broken consolidated objects)
	// is an incident to alarm on — probe_advance_exhausted goes to 1 — but the
	// walk also RESUMES where it stopped (probeResumeHeight), so consecutive
	// ticks ratchet through a dead zone of any width instead of retrying the
	// same prefix forever.
	maxRetentionProbeAdvances        = 4
	singleBlockRetentionOpenPageSize = 1000

	// defaultSingleBlockRetentionFloorWalkChunkBlocks bounds one chunk of the
	// persisted-floor walk, above and below the floor. Measured on
	// robinhood-mainnet prod: a bounded walk over 3M fully retired rows takes
	// ~11s on the reader and ~10-18s per million cold on the writer, so 1M
	// stays an order of magnitude inside the 180s statement timeout even when
	// a sweep is running concurrently.
	defaultSingleBlockRetentionFloorWalkChunkBlocks = uint64(1_000_000)
	// maxSingleBlockRetentionFloorWalkChunksPerTick caps the chunks the
	// walk ABOVE the floor runs in one tick. Bounds a tick's cost at a few
	// minutes worst case (4M cold rows on the writer) while a bootstrap from
	// an old approval floor — 13M retired rows on robinhood today — completes
	// in a handful of ticks. Every empty chunk is persisted before the next
	// runs, so the cap only spreads the walk over ticks; it never repeats it.
	maxSingleBlockRetentionFloorWalkChunksPerTick = 4
)

func NewSingleBlockRetention(params SingleBlockRetentionTaskParams) (Task, error) {
	task := &singleBlockRetentionTask{
		config:               params.Config,
		logger:               log.WithPackage(params.Logger),
		metrics:              params.Metrics.SubScope("cron").SubScope("single_block_retention"),
		runtime:              params.Runtime,
		metaStorage:          params.MetaStorage,
		singleBlockRetention: params.SingleBlockRetention,
	}
	task.selectorFactory = task.newPostgresSelector
	return task, nil
}

// takeDueCursor returns the persisted budget-truncation cursor when it belongs
// to windowStart, and the zero cursor otherwise. A cursor from a different
// window must never be applied: its keyset position would skip candidates the
// current window has not examined.
func (t *singleBlockRetentionTask) takeDueCursor(windowStart uint64) retirement.DueCohortCursor {
	t.dueCursorMu.Lock()
	defer t.dueCursorMu.Unlock()
	if t.dueCursorWindow != windowStart || t.dueCursor.IsZero() {
		return retirement.DueCohortCursor{}
	}
	return t.dueCursor
}

// storeDueCursor persists a truncation cursor for windowStart, or clears it
// when passed a zero cursor.
func (t *singleBlockRetentionTask) storeDueCursor(windowStart uint64, cursor retirement.DueCohortCursor) {
	t.dueCursorMu.Lock()
	defer t.dueCursorMu.Unlock()
	if cursor.IsZero() {
		t.dueCursorWindow = 0
		t.dueCursor = retirement.DueCohortCursor{}
		return
	}
	t.dueCursorWindow = windowStart
	t.dueCursor = cursor
}

func (t *singleBlockRetentionTask) Name() string {
	return "single_block_retention"
}

func (t *singleBlockRetentionTask) Spec() string {
	spec := t.config.Cron.SingleBlockRetention.Spec
	if spec == "" {
		return defaultSingleBlockRetentionCronSpec
	}
	return spec
}

func (t *singleBlockRetentionTask) Parallelism() int64 {
	parallelism := t.config.Cron.SingleBlockRetention.Parallelism
	if parallelism <= 0 {
		return 1
	}
	return parallelism
}

func (t *singleBlockRetentionTask) Enabled() bool {
	return t.config.Cron.SingleBlockRetention.Enabled
}

func (t *singleBlockRetentionTask) DelayStartDuration() time.Duration {
	return t.config.Cron.SingleBlockRetention.DelayStartDuration
}

// Run launches at most one bounded execute sweep anchored at the oldest due
// retention work. Eligibility is database state (a row leaves the due set only
// when its retirement is finalized as deleted-and-verified), so anchoring every
// window at the due minimum guarantees deferred, failed, or repair-re-created
// rows are re-selected by a later tick instead of skipped past.
func (t *singleBlockRetentionTask) Run(ctx context.Context) (err error) {
	// probe_failed is the only signal that reliably detects a failed tick, and
	// it has to be a gauge written on EVERY exit path rather than a counter.
	//
	// Tally suppresses zero deltas for counters (stats.go: `if delta == 0 {
	// return }`), so the instrument's result_type="error" counter emits nothing
	// until the first failure and then falls silent again. A single isolated
	// sample gives PromQL increase() no delta to compute, so an
	// increase(...) > 0 alert on it never fires — the first outage, which is
	// exactly the one worth catching, is missed entirely.
	//
	// Gauges report whenever they were updated since the last flush, so writing
	// 0 on every successful tick keeps a continuous baseline and a plain
	// `> 0` threshold fires on the first failure with no delta involved.
	//
	// Deferred on the named return so every path is covered: the early
	// non-failure exits (workflow already open, no approved range, nothing due)
	// report 0, and anything returning an error reports 1.
	defer func() {
		failed := float64(0)
		if err != nil {
			failed = 1
		}
		t.metrics.Gauge("probe_failed").Update(failed)
	}()

	if err = t.runTick(ctx); err != nil {
		return err
	}
	// Reconciliation runs after every successful tick, including the ones
	// that skipped because a sweep was open — those are most ticks, and they
	// are otherwise idle for the database. It never fails the tick: a
	// reconciliation error is reported on its own gauge, because the probe
	// result the tick already acted on is not invalidated by it.
	if t.config.Cron.SingleBlockRetention.PersistFloorWatermark {
		t.reconcileFloorWatermark(ctx)
	}
	return nil
}

func (t *singleBlockRetentionTask) runTick(ctx context.Context) error {
	cronConfig := t.config.Cron.SingleBlockRetention
	if err := t.validateStandingApproval(cronConfig); err != nil {
		return err
	}

	workflowID := t.autoRetainWorkflowID()
	// The open-workflow guard is best-effort dedup: workflow visibility is
	// eventually consistent, so a manual run started moments before this tick
	// can slip past it. Correctness never depends on exclusivity — per-row
	// retirement claims and manifest conflict checks serialize destructive
	// work — the guard only avoids wasted contention, and the fixed auto
	// workflow ID makes duplicate auto launches impossible.
	openWorkflowID, open, err := t.openSingleBlockRetentionWorkflow(ctx)
	if err != nil {
		return err
	}
	if open {
		// A no-probe tick writes the probe-only gauges as zero rather than
		// leaving them holding the previous tick's values: a stale
		// probe_window_blocks would claim this tick scanned a window it never
		// ran. oldest_due_age_seconds is deliberately NOT reset here — the
		// backlog it measures still exists while the open sweep works it, and
		// zeroing it would silence the age alarm exactly when a sweep is stuck.
		t.metrics.Gauge("probe_window_blocks").Update(0)
		t.metrics.Gauge("due_floor_height").Update(0)
		t.metrics.Gauge("probe_advance_exhausted").Update(0)
		t.logger.Info(
			"single_block_retention cron skipped because a retention workflow is already open",
			zap.String("open_workflow_id", openWorkflowID),
			zap.String("auto_workflow_id", workflowID),
		)
		return nil
	}

	tag := t.config.GetEffectiveBlockTag(0)
	approvedEnd, err := t.resolveApprovedEndHeight(ctx, cronConfig, tag)
	if err != nil {
		return err
	}
	if approvedEnd <= cronConfig.ApprovedStartHeight {
		t.logger.Info(
			"single_block_retention cron has no approved range yet",
			zap.Uint32("tag", tag),
			zap.Uint64("approved_start_height", cronConfig.ApprovedStartHeight),
			zap.Uint64("approved_end_height", approvedEnd),
		)
		return nil
	}

	bucket, err := t.config.WriteBlockStorageBucket()
	if err != nil {
		return xerrors.Errorf("failed to resolve write block storage bucket: %w", err)
	}
	storageGeneration, err := t.config.WriteBlockStorageGeneration()
	if err != nil {
		return xerrors.Errorf("failed to resolve write block storage generation: %w", err)
	}
	selector, err := t.getSelector(ctx)
	if err != nil {
		return err
	}
	eligibilityCutoff := time.Now().UTC()
	// Probe from the watermark rather than the operator's approved floor.
	//
	// The probe's cost is NOT bounded by the undeleted backlog alone, which is
	// what an earlier version of this comment claimed and what justified
	// dropping the floor to 0 in production. The due-cohort query joins its
	// due_keys CTE back to block_consolidation_shadow to expand each cohort,
	// and that join is bounded only by the height range: measured on
	// solana-mainnet prod, past roughly 1.5M heights of width the planner
	// abandons idx_block_consolidation_shadow_tag_height for a sequential scan
	// of the whole table and the probe stops finishing inside its 60s statement
	// timeout. A fixed floor widens at the chain's block rate and reaches that
	// point unaided (INF-1330).
	//
	// The watermark is the lowest height still holding an undeleted single-block
	// object, so it tracks live work and holds the range at the retention delay
	// expressed in blocks. It is recomputed every tick rather than persisted:
	// a stored floor would need its own reconciliation for repairs and failed
	// sweeps, and a stale one strands data silently.
	//
	// With PersistFloorWatermark on, the walk is BOUNDED and its progress is
	// persisted (INF-1571). The unbounded walk from approved_start_height
	// re-scans every row retention has already retired, and that count grows
	// at the drain rate — on robinhood-mainnet it crossed the 60s statement
	// timeout at ~6.6M rows and stalled retention for 12h twice (INF-1569).
	// Instead the walk starts from the persisted floor (the approval floor
	// when there is none) and proceeds one bounded chunk at a time, raising
	// the persisted floor past every chunk that holds no undeleted row, up to
	// a per-tick chunk budget. Each chunk is a fixed cost inside the statement
	// timeout and each empty chunk's result is durable before the next one
	// runs, so a first rollout from an old approval floor, or a lost cursor,
	// is a walk that spans a few ticks instead of a query that can never
	// finish, and a tick cut off mid-walk resumes where it stopped. The walk
	// is capped at approvedEnd: rows above it are outside the sweep's range,
	// and a walk that reaches it having found nothing proves every approved
	// row retired, so the floor moves to approvedEnd and the next tick walks
	// only what the envelope gained since — the fully-retired state costs a
	// constant, never a re-scan of the retired tail.
	//
	// The persisted floor is only ever a starting point for the same walk,
	// never a substitute for it: reconcileFloorWatermark re-checks the range
	// beneath it one bounded chunk per tick, so a stray row that appears
	// below (a repair, a re-ingest at an old height) lowers the floor within
	// one pass instead of being skipped forever. approved_start_height keeps
	// its meaning as the authorisation floor throughout.
	var probeStart uint64
	if cronConfig.PersistFloorWatermark {
		var complete bool
		probeStart, complete, err = t.walkPersistedFloor(ctx, selector, storageGeneration, tag, cronConfig, approvedEnd)
		if err != nil {
			return err
		}
		if !complete {
			// The chunk budget ran out before the walk found an undeleted row
			// or reached approvedEnd. Its progress is persisted and the next
			// tick continues from there; no sweep launches without a floor.
			// Probe-only gauges are zeroed as on a skipped tick.
			t.metrics.Gauge("probe_window_blocks").Update(0)
			t.metrics.Gauge("due_floor_height").Update(0)
			t.metrics.Gauge("probe_advance_exhausted").Update(0)
			return nil
		}
	} else {
		probeStart, err = selector.FloorWatermark(ctx, storageGeneration, tag, cronConfig.ApprovedStartHeight)
		if err != nil {
			return xerrors.Errorf("failed to resolve retention floor watermark: %w", err)
		}
	}
	if probeStart >= approvedEnd {
		// Everything approved has been retired. Report a zero-width range so the
		// width alarm cannot mistake a finished sweep for unbounded growth.
		// Every probe-only gauge is reset here as well: a gauge left holding the
		// previous tick's value would report a scanned window on a tick that
		// never probed, contradicting the gauge's meaning.
		t.metrics.Gauge("floor_watermark_height").Update(float64(probeStart))
		t.metrics.Gauge("probe_range_blocks").Update(0)
		t.metrics.Gauge("probe_window_blocks").Update(0)
		t.metrics.Gauge("due_floor_height").Update(0)
		t.metrics.Gauge("probe_advance_exhausted").Update(0)
		t.metrics.Gauge("oldest_due_age_seconds").Update(0)
		t.metrics.Gauge("probe_backlog_truncated").Update(0)
		t.probeResumeHeight.Store(0)
		t.logger.Info(
			"single_block_retention cron found no outstanding single-block work",
			zap.Uint32("tag", tag),
			zap.String("bucket", bucket),
			zap.String("storage_generation", storageGeneration),
			zap.Uint64("approved_start_height", cronConfig.ApprovedStartHeight),
			zap.Uint64("approved_end_height", approvedEnd),
			zap.Uint64("floor_watermark_height", probeStart),
		)
		return nil
	}
	// probe_range_blocks is the leading indicator for the plan flip described
	// above: it is the one number that predicts the timeout before it happens,
	// where oldest_due_age_seconds cannot — that gauge is derived from this very
	// probe, so it reads zero exactly when the probe fails.
	t.metrics.Gauge("floor_watermark_height").Update(float64(probeStart))
	t.metrics.Gauge("probe_range_blocks").Update(float64(approvedEnd - probeStart))

	// probe_duration_seconds covers the whole search — the deadline gate below,
	// floor lookups, probes, and advances — so a slow tick is visible no matter
	// which stage is slow. Declared here rather than at the walk so the gate,
	// which is a floor lookup, is inside the measurement it belongs to.
	probeStartedAt := time.Now()

	// Gate the due-floor walk on the cheapest question that can answer it:
	// when does the next candidate row fall due?
	//
	// The walk below asks for the lowest DUE height, and MIN(height) is
	// rewritten by Postgres into an ordered scan of the tag+height index, so
	// the envelope WIDTH becomes a row count. Under a 7-day retention window
	// the floor legitimately stops advancing for days between tranches while
	// approvedEnd keeps climbing with consolidation, so that width grows
	// without bound and the walk re-derives "nothing due" from over a million
	// rows every tick. Measured on solana-mainnet prod across a 1.91M-height
	// envelope: 1,240,520 rows filtered, 668,980 buffers, 3.6s cold. The
	// deadline seek covers the identical candidate set in 10,221 buffers and
	// 76ms, because the deadline is a key column of the generation index while
	// height is not.
	//
	// This is an equivalence, not a shortcut. Both queries carry the same
	// predicate set, so "earliest deadline is after the cutoff" IS "no row is
	// due at the cutoff" — MIN's defining property. Three things keep it that
	// way, and each is a silent gap if dropped:
	//
	//   - eligibilityCutoff is passed to BOTH, so one clock decides both. The
	//     cutoff is time.Now() in this process, not the database's: gating on
	//     the database clock instead would let app/RDS skew hide work. The
	//     margin is days for most of a retention window and SECONDS at the
	//     transition, which is exactly the tick that matters.
	//   - the answer is never cached across ticks. A deadline can be lowered
	//     between ticks by re-consolidation restamping or by a shortened
	//     retention window, so sleeping until it would strand work. Re-asking
	//     costs 76ms and keeps this tick's staleness identical to before.
	//   - the range is [probeStart, approvedEnd), the widest window this tick
	//     can reach. The advance loop only ever raises searchStart, so gating
	//     on the initial window is a superset of every advance and nothing can
	//     hide above it.
	nextDeadline, hasCandidates, err := selector.EarliestDeadline(ctx, storageGeneration, tag, probeStart, approvedEnd)
	if err != nil {
		t.metrics.Gauge("probe_duration_seconds").Update(time.Since(probeStartedAt).Seconds())
		return xerrors.Errorf("failed to resolve retention earliest deadline: %w", err)
	}
	if !hasCandidates || nextDeadline.After(eligibilityCutoff) {
		// Absence of candidates and a future deadline are the same outcome for
		// this tick, but they are NOT the same fact, so the gauge separates
		// them: zero means "nothing left to retire here", positive means
		// "work exists and is waiting". Reset every probe-only gauge for the
		// same reason the walk's own idle path does — a gauge holding the
		// previous tick's value would report a scan that never happened.
		var waitSeconds float64
		if hasCandidates {
			waitSeconds = nextDeadline.Sub(eligibilityCutoff).Seconds()
		}
		t.metrics.Gauge("next_due_in_seconds").Update(waitSeconds)
		t.metrics.Gauge("probe_duration_seconds").Update(time.Since(probeStartedAt).Seconds())
		t.metrics.Gauge("probe_window_blocks").Update(0)
		t.metrics.Gauge("due_floor_height").Update(0)
		t.metrics.Gauge("probe_advance_exhausted").Update(0)
		t.metrics.Gauge("oldest_due_age_seconds").Update(0)
		t.metrics.Gauge("probe_backlog_truncated").Update(0)
		t.probeResumeHeight.Store(0)
		t.logger.Info(
			"single_block_retention cron found nothing due",
			zap.Uint32("tag", tag),
			zap.String("bucket", bucket),
			zap.String("storage_generation", storageGeneration),
			zap.Uint64("approved_start_height", cronConfig.ApprovedStartHeight),
			zap.Uint64("approved_end_height", approvedEnd),
			zap.Uint64("floor_watermark_height", probeStart),
			zap.Bool("has_candidates", hasCandidates),
			zap.Float64("next_due_in_seconds", waitSeconds),
			zap.String("gated_by", "earliest_deadline"),
		)
		return nil
	}
	// Something is due. The walk below still runs, and still decides WHICH
	// height anchors the window: this gate only proves the walk is worth it.
	t.metrics.Gauge("next_due_in_seconds").Update(0)

	// Bound the PROBE by the same window that bounds the sweep.
	//
	// Without this the probe spans [watermark, approvedEnd] no matter how far
	// behind retention is, and its cost grows with the backlog rather than with
	// the work one tick can do. Past roughly 2-3M blocks of width the planner
	// abandons the index path for the due-cohort expansion join and sequentially
	// scans the whole block_metadata table, so the probe stops finishing inside
	// its statement timeout. Measured on robinhood-mainnet prod: 250k blocks of
	// width runs in 20.7s, 1M in 49.2s, and 6.74M does not complete in 60s.
	//
	// That failure cannot clear itself. The watermark is the lowest height still
	// holding an undeleted single-block object, so it only advances once deletes
	// land, deletes need the probe to return cohorts, and the frontier keeps
	// widening the range in the meantime. Enabling retention against a cold
	// backlog therefore lands past the flip on the very first tick and stays
	// there (INF-1416; same plan flip as INF-1330 on solana-mainnet).
	//
	// The window is anchored on the earliest DUE height, not on the watermark:
	// the watermark pins to any undeleted row, due or not, and a window anchored
	// on a not-yet-due row can be empty while due work sits above it — recomputed
	// identically every tick, forever. The due floor is a candidate rather than a
	// selectability proof (see RetentionDueFloor), so a window whose due rows all
	// turn out unselectable advances instead of idling; the advance count is
	// bounded and its exhaustion is alarmed, never silent.
	//
	// probe_range_blocks above deliberately still reports the FULL approved
	// range, so the backlog remains visible to alerting; probe_window_blocks
	// reports what was actually scanned.
	windowBlocks := cronConfig.WindowBlocks
	if windowBlocks == 0 {
		windowBlocks = defaultSingleBlockRetentionWindowBlocks
	}

	var (
		cohorts    []retirement.RetentionCohort
		hasMore    bool
		nextCursor retirement.DueCohortCursor
		probeEnd   uint64
	)
	// dueCursor carries a budget-truncated selection forward. It is a keyset
	// cursor into candidate enumeration, so re-probing the SAME window with it
	// resumes exactly where the last pass stopped — no window movement, and no
	// height filter that could discard an unexamined candidate overlapping the
	// examined ones. It is seeded from the previous tick once the window is
	// known, and persisted again if this tick also runs out of budget.
	var dueCursor retirement.DueCohortCursor
	found := false
	searchStart := probeStart
	// Resume an exhausted walk. A cursor at or below the watermark is stale
	// (the watermark caught up past it) and one at or beyond the approved end
	// has finished the ring; both restart the walk from the watermark so
	// dead-prefix rows that have since become selectable are re-inspected.
	if resume := t.probeResumeHeight.Load(); resume > searchStart && resume < approvedEnd {
		searchStart = resume
	} else {
		t.probeResumeHeight.Store(0)
	}
	for attempt := 0; attempt < maxRetentionProbeAdvances; attempt++ {
		dueFloor, dueFound, err := selector.DueFloor(ctx, storageGeneration, tag, searchStart, approvedEnd, eligibilityCutoff)
		if err != nil {
			t.metrics.Gauge("probe_duration_seconds").Update(time.Since(probeStartedAt).Seconds())
			return xerrors.Errorf("failed to resolve retention due floor: %w", err)
		}
		if !dueFound {
			// Nothing due in [searchStart, approvedEnd). Because the floor
			// candidate matches a superset of selectable rows, this is proof
			// there is nothing to select, and idling is correct. Reset every
			// probe gauge so this tick cannot exhibit the previous tick's
			// values.
			t.metrics.Gauge("probe_duration_seconds").Update(time.Since(probeStartedAt).Seconds())
			t.metrics.Gauge("due_floor_height").Update(0)
			t.metrics.Gauge("probe_window_blocks").Update(0)
			t.metrics.Gauge("probe_advance_exhausted").Update(0)
			t.metrics.Gauge("oldest_due_age_seconds").Update(0)
			t.metrics.Gauge("probe_backlog_truncated").Update(0)
			// Wrap the ring: nothing due above searchStart, so the next tick
			// walks from the watermark again and re-inspects any dead prefix
			// whose rows may have become selectable since.
			t.probeResumeHeight.Store(0)
			t.logger.Info(
				"single_block_retention cron found nothing due",
				zap.Uint32("tag", tag),
				zap.String("bucket", bucket),
				zap.String("storage_generation", storageGeneration),
				zap.Uint64("approved_start_height", cronConfig.ApprovedStartHeight),
				zap.Uint64("approved_end_height", approvedEnd),
				zap.Uint64("floor_watermark_height", probeStart),
				zap.Uint64("search_start_height", searchStart),
				zap.Int("probe_advances", attempt),
			)
			return nil
		}
		probeStart = dueFloor
		probeEnd = approvedEnd
		if probeEnd-probeStart > windowBlocks {
			probeEnd = probeStart + windowBlocks
		}
		t.metrics.Gauge("due_floor_height").Update(float64(probeStart))
		t.metrics.Gauge("probe_window_blocks").Update(float64(probeEnd - probeStart))
		if attempt == 0 {
			// Resume a previous tick's truncated walk, but only if it belongs
			// to this exact window: a cursor from a different window would skip
			// candidates this one has never examined.
			dueCursor = t.takeDueCursor(probeStart)
		}

		// Asks for a full workflow batch because the selector sorts pending
		// (in-flight) cohorts by prepared_at ahead of height-ordered due cohorts:
		// anchoring on the first cohort alone could hide older due work behind a
		// stuck pending cohort indefinitely.
		cohorts, hasMore, nextCursor, err = selector.Select(
			ctx,
			bucket,
			storageGeneration,
			tag,
			probeStart,
			probeEnd,
			eligibilityCutoff,
			retirement.MaxRetentionCohortsPerWorkflow,
			dueCursor,
		)
		if err != nil {
			t.metrics.Gauge("probe_duration_seconds").Update(time.Since(probeStartedAt).Seconds())
			return xerrors.Errorf("failed to probe due retention cohorts: %w", err)
		}
		if len(cohorts) > 0 {
			found = true
			break
		}
		// The probe selected nothing here. There are two very different reasons
		// for that and they must not be conflated:
		//
		//   - candidates were EXHAUSTED: every due row in this window is
		//     excluded at the join level (active repair, pending manifest,
		//     canonical or metadata mismatch), so the window really is dead and
		//     the search steps past it. Idling instead would pin the search
		//     here forever, hiding selectable work above.
		//   - selection stopped on its expansion BUDGET with candidates still
		//     unexamined (resumeAfter > 0). Stepping past the window would skip
		//     cohorts nobody looked at, stranding them; the search resumes at
		//     the first unexamined height instead, so a dead prefix of any size
		//     is walked with bounded work per tick.
		// The probe selected nothing here. There are two very different reasons
		// for that and they must not be conflated:
		//
		//   - selection stopped on its expansion BUDGET with candidates still
		//     unexamined (non-zero cursor). Moving the window would skip
		//     cohorts nobody looked at; instead the SAME window is re-probed
		//     with the cursor, which resumes exactly past the examined
		//     candidates and cannot filter out an overlapping unexamined one.
		//   - candidates were EXHAUSTED: every due row in this window is
		//     excluded at the join level (pending manifest, canonical or
		//     metadata mismatch), so the window really is dead and the search
		//     steps past it. Idling instead would pin the search here forever.
		if !nextCursor.IsZero() {
			dueCursor = nextCursor
			t.storeDueCursor(probeStart, nextCursor)
			t.logger.Info(
				"single_block_retention cron re-probing a window after a budget-truncated selection",
				zap.Uint32("tag", tag),
				zap.Uint64("window_start_height", probeStart),
				zap.Uint64("window_end_height", probeEnd),
				zap.Uint64("resume_candidate_height", nextCursor.StartHeight),
				zap.Int("probe_advances", attempt+1),
			)
			continue
		}
		dueCursor = retirement.DueCohortCursor{}
		t.storeDueCursor(0, retirement.DueCohortCursor{})
		t.logger.Info(
			"single_block_retention cron advancing past a window with no selectable cohorts",
			zap.Uint32("tag", tag),
			zap.Uint64("window_start_height", probeStart),
			zap.Uint64("window_end_height", probeEnd),
			zap.Uint64("next_search_height", probeEnd),
			zap.Int("probe_advances", attempt+1),
		)
		searchStart = probeEnd
	}
	t.metrics.Gauge("probe_duration_seconds").Update(time.Since(probeStartedAt).Seconds())
	if !found {
		// Every window we were willing to inspect this tick holds only
		// unselectable due rows. This is an alarm state, not an idle: due work
		// exists, nothing was launched, and the next tick will walk the same
		// windows. A dead zone this wide means a mass of repair-covered or
		// inconsistent objects that needs a human.
		t.metrics.Gauge("probe_advance_exhausted").Update(1)
		t.metrics.Gauge("oldest_due_age_seconds").Update(0)
		t.metrics.Gauge("probe_backlog_truncated").Update(0)
		// Resume here next tick instead of re-walking the same dead prefix —
		// searchStart is the end of the last window inspected. Consecutive
		// exhausted ticks therefore ratchet forward budget-by-budget until
		// they reach selectable work, nothing due, or the approved end.
		t.probeResumeHeight.Store(searchStart)
		t.logger.Warn(
			"single_block_retention cron exhausted probe advances without selectable work",
			zap.Uint32("tag", tag),
			zap.String("bucket", bucket),
			zap.String("storage_generation", storageGeneration),
			zap.Uint64("approved_end_height", approvedEnd),
			zap.Uint64("last_window_end_height", probeEnd),
			zap.Uint64("resume_height", searchStart),
			zap.Int("probe_advances", maxRetentionProbeAdvances),
		)
		return nil
	}
	t.metrics.Gauge("probe_advance_exhausted").Update(0)
	t.probeResumeHeight.Store(0)
	// Preserve a budget-truncated continuation ACROSS the launch. A non-empty
	// page does NOT mean the window is finished: selection can return an early
	// selectable cohort and still have spent its budget on the dead prefix
	// behind it. Clearing here would restart the next tick at the head of this
	// window, and if the cohort just launched stays due — a deferred or failed
	// sweep leaves it due — every subsequent tick would re-select that same
	// cohort and never reach work behind the prefix.
	//
	// storeDueCursor clears when handed a zero cursor, so the exhausted case
	// still resets to a fresh walk. The cursor is keyed to this window, so once
	// the launched cohort is actually retired and the floor moves, it is
	// discarded rather than misapplied.
	t.storeDueCursor(probeStart, nextCursor)
	// Anchor at the minimum start height and age the gauge from the oldest
	// eligibility across the whole probe set, so neither is masked by
	// pending-cohort ordering. Stuck pending cohorts are themselves overdue,
	// so they keep the age alarm honest rather than silencing it.
	anchor := cohorts[0]
	oldestEligibleAt := cohorts[0].EligibleAt
	for _, cohort := range cohorts[1:] {
		if cohort.StartHeight < anchor.StartHeight {
			anchor = cohort
		}
		if cohort.EligibleAt.Before(oldestEligibleAt) {
			oldestEligibleAt = cohort.EligibleAt
		}
	}
	oldestDueAge := eligibilityCutoff.Sub(oldestEligibleAt)
	t.metrics.Gauge("oldest_due_age_seconds").Update(oldestDueAge.Seconds())
	backlogTruncated := float64(0)
	if hasMore {
		backlogTruncated = 1
	}
	t.metrics.Gauge("probe_backlog_truncated").Update(backlogTruncated)

	// The selected cohort lies inside the approved range, so the window is
	// always non-empty; it is clipped to keep each sweep bounded.
	//
	// This deliberately still clips to approvedEnd, NOT to probeEnd. The sweep's
	// authorization is the operator's approved envelope and narrowing the probe
	// must never change it. Clipping to probeEnd also underflows when the anchor
	// sits above it, which silently widened the window past approvedEnd.
	windowStart := anchor.StartHeight
	windowEnd := approvedEnd
	if windowEnd-windowStart > windowBlocks {
		windowEnd = windowStart + windowBlocks
	}
	if windowEnd <= windowStart {
		return xerrors.Errorf(
			"single_block_retention cron derived an invalid window [%d, %d)",
			windowStart,
			windowEnd,
		)
	}

	request := &workflow.SingleBlockRetentionRequest{
		// Pass the resolved tag so the sweep executes under the same tag the
		// probe and window derivation used, even if a rolling deploy bumps the
		// stable tag between launch and activity execution.
		Tag:                         tag,
		StartHeight:                 windowStart,
		EndHeight:                   windowEnd,
		EligibilityCutoff:           eligibilityCutoff,
		MaxObjectRanges:             cronConfig.MaxObjectRanges,
		Parallelism:                 cronConfig.WorkflowParallelism,
		Execute:                     true,
		ProductionDeleteEnabled:     cronConfig.ProductionDeleteEnabled,
		DirectStorageClientsGuarded: cronConfig.DirectStorageClientsGuarded,
		SingleBlockWritersGuarded:   cronConfig.SingleBlockWritersGuarded,
		FallbackReadsValidated:      cronConfig.FallbackReadsValidated,
		FallbackErrorCount:          0,
		ApprovedChain:               cronConfig.ApprovedChain,
		ApprovedStartHeight:         cronConfig.ApprovedStartHeight,
		ApprovedEndHeight:           approvedEnd,
	}
	workflowCtx := workflow.WithWorkflowID(ctx, workflowID)
	run, err := t.singleBlockRetention.Execute(workflowCtx, request)
	if err != nil {
		if isWorkflowAlreadyStarted(err) {
			t.logger.Info(
				"single_block_retention cron skipped because a retention workflow was already started",
				zap.String("workflow_id", workflowID),
			)
			return nil
		}
		return xerrors.Errorf("failed to start single_block_retention cron workflow: %w", err)
	}
	t.metrics.Counter("launched").Inc(1)
	t.logger.Info(
		"started single_block_retention cron workflow",
		zap.String("workflow_id", workflowID),
		zap.String("run_id", run.GetRunID()),
		zap.Uint32("tag", tag),
		zap.String("bucket", bucket),
		zap.String("storage_generation", storageGeneration),
		zap.Uint64("window_start_height", windowStart),
		zap.Uint64("window_end_height", windowEnd),
		zap.Uint64("approved_start_height", cronConfig.ApprovedStartHeight),
		zap.Uint64("approved_end_height", approvedEnd),
		zap.Uint64("floor_watermark_height", probeStart),
		zap.Uint64("probe_range_blocks", approvedEnd-probeStart),
		zap.Time("eligibility_cutoff", eligibilityCutoff),
		zap.Duration("oldest_due_age", oldestDueAge),
		zap.Int("max_object_ranges", cronConfig.MaxObjectRanges),
		zap.Int("workflow_parallelism", cronConfig.WorkflowParallelism),
	)
	return nil
}

func floorWalkChunkBlocks(cronConfig config.SingleBlockRetentionCronConfig) uint64 {
	if cronConfig.FloorWalkChunkBlocks != 0 {
		return cronConfig.FloorWalkChunkBlocks
	}
	return defaultSingleBlockRetentionFloorWalkChunkBlocks
}

// walkPersistedFloor resolves the tick's floor with the bounded, persisted
// walk described at its call site in runTick. It returns the floor and
// complete=true when the walk found the first undeleted row at or above the
// persisted floor, or reached approvedEnd without finding one (the floor is
// then approvedEnd); complete=false when the per-tick chunk budget ran out
// first, with every empty chunk already persisted so the next tick resumes
// from the last one.
func (t *singleBlockRetentionTask) walkPersistedFloor(
	ctx context.Context,
	selector *retirement.Selector,
	storageGeneration string,
	tag uint32,
	cronConfig config.SingleBlockRetentionCronConfig,
	approvedEnd uint64,
) (uint64, bool, error) {
	persistedFloor, persistedFloorFound, err := t.metaStorage.GetBlockConsolidationCursor(
		ctx,
		metastorage.SingleBlockRetentionFloorWatermarkCursor,
		tag,
	)
	if err != nil {
		return 0, false, xerrors.Errorf("failed to read persisted retention floor watermark: %w", err)
	}
	t.metrics.Gauge("persisted_floor_height").Update(float64(persistedFloor))
	start := cronConfig.ApprovedStartHeight
	if persistedFloorFound && persistedFloor > start {
		start = persistedFloor
	}
	chunk := floorWalkChunkBlocks(cronConfig)
	// raise persists height as the floor when it is above the persisted one.
	// Raise only: lowering is reconciliation's job. A failed write is
	// reported, not fatal — the walk's result stands for this tick and the
	// next tick re-walks whatever was not recorded.
	raise := func(height uint64) {
		if persistedFloorFound && height <= persistedFloor {
			return
		}
		if err := t.metaStorage.SetBlockConsolidationCursor(
			ctx,
			metastorage.SingleBlockRetentionFloorWatermarkCursor,
			tag,
			height,
		); err != nil {
			t.metrics.Gauge("floor_persist_failed").Update(1)
			t.logger.Warn(
				"single_block_retention cron failed to persist the retention floor watermark",
				zap.Uint32("tag", tag),
				zap.Uint64("floor_watermark_height", height),
				zap.Error(err),
			)
			return
		}
		persistedFloor, persistedFloorFound = height, true
		t.metrics.Gauge("floor_persist_failed").Update(0)
		t.metrics.Gauge("persisted_floor_height").Update(float64(height))
	}
	chunks := 0
	for start < approvedEnd && chunks < maxSingleBlockRetentionFloorWalkChunksPerTick {
		end := approvedEnd
		if end-start > chunk {
			end = start + chunk
		}
		chunks++
		height, found, err := selector.FloorWatermarkInRange(ctx, storageGeneration, tag, start, end)
		if err != nil {
			t.metrics.Gauge("floor_walk_chunks").Update(float64(chunks))
			return 0, false, xerrors.Errorf("failed to resolve retention floor watermark in [%d, %d): %w", start, end, err)
		}
		if found {
			t.metrics.Gauge("floor_walk_chunks").Update(float64(chunks))
			t.metrics.Gauge("floor_walk_incomplete").Update(0)
			raise(height)
			return height, true, nil
		}
		// Every row in [start, end) is retired: the floor may move to end.
		start = end
		raise(start)
	}
	t.metrics.Gauge("floor_walk_chunks").Update(float64(chunks))
	if start >= approvedEnd {
		t.metrics.Gauge("floor_walk_incomplete").Update(0)
		return approvedEnd, true, nil
	}
	t.metrics.Gauge("floor_walk_incomplete").Update(1)
	t.metrics.Gauge("floor_watermark_height").Update(float64(start))
	t.metrics.Gauge("probe_range_blocks").Update(float64(approvedEnd - start))
	t.logger.Info(
		"single_block_retention cron floor walk paused at its chunk budget; it continues next tick",
		zap.Uint32("tag", tag),
		zap.Uint64("walk_height", start),
		zap.Uint64("approved_end_height", approvedEnd),
		zap.Int("chunks", chunks),
		zap.Uint64("chunk_blocks", chunk),
	)
	return start, false, nil
}

// reconcileFloorWatermark re-walks one bounded chunk of the range below the
// persisted floor looking for an undeleted single-block row, and lowers the
// floor to it when one exists (INF-1571).
//
// This is what makes a persisted floor safe. The cron's own earlier design note
// rejected persistence because "a stored floor would need its own
// reconciliation for repairs and failed sweeps, and a stale one strands data
// silently" — and it was right about the need, not about the cost. The
// unbounded walk that persistence replaces IS that reconciliation, paid in full
// on every tick; this walks the same rows one chunk per tick instead, so a
// stray is found within one pass over the retired range rather than instantly,
// at a per-tick cost that stays fixed while the retired range grows. The
// direction of every failure is the safe one: a lost cursor restarts the pass
// from the approval floor, a found stray lowers the persisted floor so the
// very next probe walks from it, and a chunk that errors is retried next tick.
func (t *singleBlockRetentionTask) reconcileFloorWatermark(ctx context.Context) {
	cronConfig := t.config.Cron.SingleBlockRetention
	tag := t.config.GetEffectiveBlockTag(0)
	storageGeneration, err := t.config.WriteBlockStorageGeneration()
	if err != nil {
		t.reportReconcileFailure(err)
		return
	}
	persistedFloor, found, err := t.metaStorage.GetBlockConsolidationCursor(
		ctx,
		metastorage.SingleBlockRetentionFloorWatermarkCursor,
		tag,
	)
	if err != nil {
		t.reportReconcileFailure(err)
		return
	}
	if !found || persistedFloor <= cronConfig.ApprovedStartHeight {
		// Nothing is skipped below a floor that is absent or at the approval
		// floor, so there is nothing to reconcile.
		t.metrics.Gauge("reconcile_failed").Update(0)
		t.metrics.Gauge("reconcile_chunk_start_height").Update(0)
		t.metrics.Gauge("reconcile_chunk_end_height").Update(0)
		t.reconcileNextHeight.Store(0)
		return
	}
	chunk := floorWalkChunkBlocks(cronConfig)
	start := cronConfig.ApprovedStartHeight
	if resume := t.reconcileNextHeight.Load(); resume > start && resume < persistedFloor {
		start = resume
	}
	end := persistedFloor
	if end-start > chunk {
		end = start + chunk
	}
	selector, err := t.getSelector(ctx)
	if err != nil {
		t.reportReconcileFailure(err)
		return
	}
	startedAt := time.Now()
	stray, strayFound, err := selector.FloorWatermarkInRange(ctx, storageGeneration, tag, start, end)
	t.metrics.Gauge("reconcile_duration_seconds").Update(time.Since(startedAt).Seconds())
	if err != nil {
		t.reportReconcileFailure(err)
		return
	}
	t.metrics.Gauge("reconcile_chunk_start_height").Update(float64(start))
	t.metrics.Gauge("reconcile_chunk_end_height").Update(float64(end))
	if strayFound {
		if err := t.metaStorage.ResetBlockConsolidationCursor(
			ctx,
			metastorage.SingleBlockRetentionFloorWatermarkCursor,
			tag,
			stray,
		); err != nil {
			t.reportReconcileFailure(err)
			return
		}
		t.metrics.Counter("reconcile_strays").Inc(1)
		t.metrics.Gauge("persisted_floor_height").Update(float64(stray))
		t.logger.Warn(
			"single_block_retention cron found an undeleted single-block row below the persisted floor; floor lowered",
			zap.Uint32("tag", tag),
			zap.String("storage_generation", storageGeneration),
			zap.Uint64("stray_height", stray),
			zap.Uint64("previous_floor_height", persistedFloor),
			zap.Uint64("chunk_start_height", start),
			zap.Uint64("chunk_end_height", end),
		)
	}
	t.metrics.Gauge("reconcile_failed").Update(0)
	// Continue from the end of this chunk next tick; wrap once the pass has
	// reached the floor so the whole retired range is re-inspected again.
	if end >= persistedFloor {
		t.reconcileNextHeight.Store(0)
	} else {
		t.reconcileNextHeight.Store(end)
	}
}

func (t *singleBlockRetentionTask) reportReconcileFailure(err error) {
	t.metrics.Gauge("reconcile_failed").Update(1)
	t.logger.Warn("single_block_retention cron floor reconciliation failed", zap.Error(err))
}

// validateStandingApproval fails closed unless the reviewed configuration
// carries the complete operator approval a manual launch would have supplied.
func (t *singleBlockRetentionTask) validateStandingApproval(cronConfig config.SingleBlockRetentionCronConfig) error {
	if t.config.StorageType.MetaStorageType != config.MetaStorageType_POSTGRES || t.config.AWS.Postgres == nil {
		return xerrors.New("single_block_retention cron requires Postgres meta storage")
	}
	if cronConfig.ApprovedChain == "" {
		return xerrors.New("single_block_retention cron requires cron.single_block_retention.approved_chain")
	}
	if cronConfig.ApprovedEndHeight == 0 && !cronConfig.AllowOpenEndedApproval {
		return xerrors.New("single_block_retention cron requires approved_end_height or the explicit allow_open_ended_approval opt-in")
	}
	if cronConfig.ApprovedEndHeight != 0 && cronConfig.ApprovedEndHeight <= cronConfig.ApprovedStartHeight {
		return xerrors.Errorf(
			"single_block_retention cron approved range [%d, %d) is invalid",
			cronConfig.ApprovedStartHeight,
			cronConfig.ApprovedEndHeight,
		)
	}
	if !cronConfig.DirectStorageClientsGuarded {
		return xerrors.New("single_block_retention cron requires direct_storage_clients_guarded")
	}
	if !cronConfig.SingleBlockWritersGuarded {
		return xerrors.New("single_block_retention cron requires single_block_writers_guarded")
	}
	if !cronConfig.FallbackReadsValidated {
		return xerrors.New("single_block_retention cron requires fallback_reads_validated")
	}
	if isProductionRetentionCronEnvironment(t.config.Env()) && !cronConfig.ProductionDeleteEnabled {
		return xerrors.New("single_block_retention cron requires production_delete_enabled in production")
	}
	return nil
}

// resolveApprovedEndHeight resolves an open-ended standing approval to the
// consolidation frontier: everything below the auto-consolidate cursor is
// consolidated, and rows only become due after promotion stamps their
// retention deadline, so the cursor is a safe, monotonic envelope end.
func (t *singleBlockRetentionTask) resolveApprovedEndHeight(
	ctx context.Context,
	cronConfig config.SingleBlockRetentionCronConfig,
	tag uint32,
) (uint64, error) {
	if cronConfig.ApprovedEndHeight != 0 {
		return cronConfig.ApprovedEndHeight, nil
	}
	cursorHeight, cursorFound, err := t.metaStorage.GetBlockConsolidationCursor(
		ctx,
		metastorage.BatchConsolidatorAutoConsolidateCursor,
		tag,
	)
	if err != nil {
		return 0, xerrors.Errorf("failed to resolve consolidation cursor for open-ended retention approval: %w", err)
	}
	if !cursorFound {
		t.logger.Info(
			"single_block_retention cron found no consolidation cursor for open-ended approval",
			zap.Uint32("tag", tag),
		)
		return 0, nil
	}
	return cursorHeight, nil
}

func (t *singleBlockRetentionTask) autoRetainWorkflowID() string {
	return fmt.Sprintf("%s/%s", t.config.Workflows.SingleBlockRetention.WorkflowIdentity, autoRetainSuffix)
}

func (t *singleBlockRetentionTask) openSingleBlockRetentionWorkflow(ctx context.Context) (string, bool, error) {
	workflowIdentity := t.config.Workflows.SingleBlockRetention.WorkflowIdentity
	openWorkflows, err := t.runtime.ListOpenWorkflows(
		ctx,
		t.config.Cadence.Domain,
		singleBlockRetentionOpenPageSize,
		workflowIdentity,
	)
	if err != nil {
		return "", false, xerrors.Errorf("failed to list open workflows for single_block_retention cron: %w", err)
	}
	if openWorkflows == nil {
		return "", false, nil
	}
	for _, wf := range openWorkflows.Executions {
		if wf.GetType().GetName() == workflowIdentity {
			return wf.GetExecution().GetWorkflowId(), true, nil
		}
	}
	return "", false, nil
}

// getSelector resolves per tick instead of caching: the postgres factory rides
// the process-global connection-pool cache, and re-resolving keeps the cron
// healthy if the pool is ever closed or recycled underneath it.
func (t *singleBlockRetentionTask) getSelector(ctx context.Context) (*retirement.Selector, error) {
	return t.selectorFactory(ctx)
}

func (t *singleBlockRetentionTask) newPostgresSelector(ctx context.Context) (*retirement.Selector, error) {
	pool, err := metapostgres.GetConnectionPool(ctx, t.config.AWS.Postgres)
	if err != nil {
		return nil, xerrors.Errorf("failed to get single_block_retention cron Postgres pool: %w", err)
	}
	db := pool.DB()
	if db == nil {
		return nil, xerrors.New("single_block_retention cron Postgres pool returned a nil database")
	}
	return retirement.NewSelector(retirement.NewPostgresRepository(db)), nil
}

func isProductionRetentionCronEnvironment(env config.Env) bool {
	value := strings.ToLower(string(env))
	return value == "production" || value == "prod"
}
