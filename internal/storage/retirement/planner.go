package retirement

import (
	"context"
	cryptorand "crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"
	"golang.org/x/xerrors"

	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

const (
	sha256HexLength                   = sha256.Size * 2
	cscbFormatMetadataKey             = "chainstorage-format"
	cscbCompressionScopeMetadataKey   = "chainstorage-compression-scope"
	cscbUncompressedLengthMetadataKey = "chainstorage-uncompressed-length"
	cscbFormatMetadataValue           = "cscb"
	cscbCompressionScopeMetadataValue = "batch-chunked"
	retirementClaimTokenBytes         = 16
	RetirementClaimLease              = 15 * time.Minute
	RetentionSafetyQuiescencePeriod   = 15 * time.Minute
	// RetirementClaimCleanupTimeout bounds EACH detached cleanup write (the
	// outcome record, then the claim release) that follows a failed claimed
	// operation. The writes are detached from the operation's context so a
	// cancellation still records its trace and releases the row, but they
	// run inside the Temporal worker's stop timeout, after which the process
	// exits: a stalled database must fail them fast rather than hold the
	// shutdown past the point where the release could still land. Each write
	// gets its own deadline, started when that write starts, so an outcome
	// write that runs to its deadline cannot leave the release with an
	// already-expired context — that would keep the row leased for the full
	// RetirementClaimLease, the exact outage the release exists to prevent.
	RetirementClaimCleanupTimeout = 4 * time.Second
	// RetirementClaimCleanupBudget is the worst-case wall time of the whole
	// cleanup: the two writes run in sequence, each on its own timeout.
	// internal/cadence pins it below the worker stop timeout.
	RetirementClaimCleanupBudget = 2 * RetirementClaimCleanupTimeout
	// retentionSafetyRevalidationInterval bounds how stale a successful
	// bucket-safety verification may be before a destructive row re-verifies it
	// against live S3. The preflight verifies every cohort's configuration at
	// the start of an Apply pass; rows within this interval reuse that result
	// instead of re-issuing two control-plane calls per row.
	retentionSafetyRevalidationInterval = time.Minute
	// retirementClaimRenewalFraction renews a claim once the elapsed lease
	// fraction exceeds 1/2. The claim cannot be taken over until the full lease
	// expires, so renewing at half-life preserves the fencing guarantee while
	// removing a database write per destructive step.
	retirementClaimRenewalFraction = 2
	// maxRetirementRowParallelism also bounds memory: each Apply worker holds
	// two payload verifiers, and each verifier pins one decompressed CSCB
	// chunk (tens of MB for Solana), so the cap must fit worker pod limits.
	maxRetirementRowParallelism = 16
	// defaultRetirementRowMemoryBudgetBytes caps the decompressed CSCB chunk
	// working set when no budget is configured. 1 GiB keeps a comfortable
	// margin inside the smallest retention worker reservation while still
	// allowing real concurrency on chains with modest chunks.
	defaultRetirementRowMemoryBudgetBytes = uint64(1) << 30
	s3MutableNullVersionID                = "null"
	versionedDeleteMode                   = "exact_pinned_versions_and_delete_markers"
)

var unverifiedRetentionSafetySHA256 = keySHA256("unverified-retention-safety-configuration")

type Planner struct {
	repo            Repository
	store           ObjectStore
	verifierFactory func() blockPayloadVerifier
	heartbeat       func(ctx context.Context, details ...any)
	rowParallelism  int
	// retentionSafetyClock feeds the safety cache's staleness checks; tests
	// override it to exercise revalidation-interval expiry.
	retentionSafetyClock func() time.Time
	// claimCleanupTimeout overrides RetirementClaimCleanupTimeout per cleanup
	// write; zero means the production constant. Tests shrink it to exercise
	// deadline exhaustion without waiting out the real budget.
	claimCleanupTimeout time.Duration
}

func (p *Planner) claimCleanupWriteTimeout() time.Duration {
	if p.claimCleanupTimeout > 0 {
		return p.claimCleanupTimeout
	}
	return RetirementClaimCleanupTimeout
}

type PlannerOption func(*Planner)

// WithRowParallelism bounds how many independent rows Plan and Apply process
// concurrently. Zero or one preserves the serial default for existing callers.
func WithRowParallelism(parallelism int) PlannerOption {
	return func(p *Planner) {
		p.rowParallelism = parallelism
	}
}

// retirementClaim carries the claim token together with its renewal clock so
// destructive steps can renew on lease half-life instead of before every
// operation.
type retirementClaim struct {
	token         string
	lastRenewedAt time.Time
}

// retentionSafetyOutcome memoizes one bucket-safety verification for a CSCB
// key so an Apply or Reconcile pass re-verifies at most once per
// retentionSafetyRevalidationInterval rather than once per row.
type retentionSafetyOutcome struct {
	reason     string
	retryAfter time.Duration
	err        error
	verifiedAt time.Time
}

type retentionSafetyCache struct {
	mu       sync.Mutex
	now      func() time.Time
	outcomes map[string]retentionSafetyOutcome
	// maxQuiescenceRetry keeps the longest quiescence wait ever observed by
	// this pass, so a later successful re-verification of the same key cannot
	// erase the retry hint the deferred rows still need.
	maxQuiescenceRetry time.Duration
}

func (p *Planner) newRetentionSafetyCache() *retentionSafetyCache {
	now := p.retentionSafetyClock
	if now == nil {
		now = func() time.Time { return time.Now().UTC() }
	}
	return &retentionSafetyCache{
		now:      now,
		outcomes: make(map[string]retentionSafetyOutcome),
	}
}

// planShared holds the per-plan caches that independent row workers share.
// Lookups and stores are mutex-guarded; a cache miss may fetch the same
// immutable object from S3 on more than one worker, which is benign.
type planShared struct {
	mu         sync.Mutex
	heads      map[string]ObjectHead
	topologies map[string]ObjectVersionTopology
	policies   map[string]bool
}

func newPlanShared() *planShared {
	return &planShared{
		heads:      make(map[string]ObjectHead),
		topologies: make(map[string]ObjectVersionTopology),
		policies:   make(map[string]bool),
	}
}

// WithHeartbeat wires a liveness callback invoked from the long-running
// row/version loops, so a Temporal activity heartbeat timeout can bound the
// planner and deliver cancellation between destructive steps.
func WithHeartbeat(heartbeat func(ctx context.Context, details ...any)) PlannerOption {
	return func(p *Planner) {
		p.heartbeat = heartbeat
	}
}

func NewPlanner(repo Repository, store ObjectStore, opts ...PlannerOption) *Planner {
	p := &Planner{
		repo:  repo,
		store: store,
		verifierFactory: func() blockPayloadVerifier {
			return newPayloadVerifier(store)
		},
	}
	for _, opt := range opts {
		opt(p)
	}
	return p
}

func (p *Planner) recordHeartbeat(ctx context.Context, details ...any) {
	if p.heartbeat != nil {
		p.heartbeat(ctx, details...)
	}
}

func (p *Planner) Plan(ctx context.Context, req PlanRequest) (*Report, error) {
	if err := normalizeRequest(&req); err != nil {
		return nil, err
	}
	rows, err := p.repo.ListMetadataRows(ctx, req.Tag, req.StartHeight, req.EndHeight, req.Limit)
	if err != nil {
		return nil, err
	}

	report := newReport(req)
	shared := newPlanShared()
	items := make([]Candidate, len(rows))
	// Plan runs one verifier per worker, so it pins one chunk per worker. The
	// sizing HEAD is skipped entirely when the planner is serial, so a serial
	// pass still touches object storage only for rows that reach inspection.
	parallelism := p.configuredRowParallelism()
	if parallelism > 1 {
		parallelism = p.budgetedRowParallelism(
			req,
			p.cohortConsolidatedChunkBytes(ctx, req, shared, firstConsolidatedKey(rows)),
			1,
		)
	}
	group, groupCtx := errgroup.WithContext(ctx)
	// Contiguous segments keep each worker's verifier chunk cache local to
	// height-adjacent rows, which share CSCB chunks.
	for _, segment := range rowSegments(len(rows), parallelism) {
		group.Go(func() error {
			verifier := p.verifierFactory()
			for i := segment.start; i < segment.end; i++ {
				row := rows[i]
				if err := groupCtx.Err(); err != nil {
					return xerrors.Errorf("single-block retention plan canceled at height %d: %w", row.Height, err)
				}
				p.recordHeartbeat(groupCtx, "retirement.plan.row", row.Height)
				items[i] = p.planRow(groupCtx, req, row, shared, verifier)
			}
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		return nil, err
	}
	report.Items = append(report.Items, items...)
	report.SafetyGates.CSCBWriteOncePolicyVerified = writeOncePolicyGateVerified(report.Items)
	report.Summary = summarize(report.Items)
	return report, nil
}

type rowSegment struct {
	start int
	end   int
}

func rowSegments(total int, parallelism int) []rowSegment {
	if total <= 0 {
		return nil
	}
	if parallelism > total {
		parallelism = total
	}
	segments := make([]rowSegment, 0, parallelism)
	size := (total + parallelism - 1) / parallelism
	for start := 0; start < total; start += size {
		end := start + size
		if end > total {
			end = total
		}
		segments = append(segments, rowSegment{start: start, end: end})
	}
	return segments
}

func (p *Planner) configuredRowParallelism() int {
	if p.rowParallelism <= 1 {
		return 1
	}
	if p.rowParallelism > maxRetirementRowParallelism {
		return maxRetirementRowParallelism
	}
	return p.rowParallelism
}

// budgetedRowParallelism lowers the configured worker count until the pinned
// decompressed-chunk working set fits the memory budget.
//
// Each worker's payload verifier caches one decompressed CSCB chunk, and Apply
// runs two verifiers per worker (pre- and post-delete), so the pinned set is
// workers x verifiersPerWorker x chunk size. Chunk size is chain-specific and
// large: a Solana 1000-block CSCB is ~11.1 GB uncompressed across 100 chunks,
// so one chunk is ~111 MB and a hand-set parallelism of 8 pinned ~1.8 GB in
// Apply — enough to OOM-kill a 6 G worker mid-activity, which surfaces only as
// an opaque Temporal heartbeat timeout (INF-1330).
//
// The size is knowable before any row runs: the pinned CSCB index records each
// chunk's decompressed length, so the largest of those is the true ceiling on
// what one verifier's chunk cache can hold. Deriving the worker count from that
// means a chain with larger blocks quietly runs fewer workers instead of dying.
//
// chunkBytes of 0 means sizing failed, and this fails closed to serial: an
// unmeasurable cohort must not run at a width nobody has bounded.
func (p *Planner) budgetedRowParallelism(req PlanRequest, chunkBytes uint64, verifiersPerWorker int) int {
	configured := p.configuredRowParallelism()
	if configured <= 1 {
		return configured
	}
	if chunkBytes == 0 || verifiersPerWorker <= 0 {
		return 1
	}
	// The budget defaults rather than disabling: an operator who raises
	// parallelism without also setting a budget must still not be able to
	// configure the worker into an OOM.
	budget := req.RowMemoryBudgetBytes
	if budget == 0 {
		budget = defaultRetirementRowMemoryBudgetBytes
	}
	// A parseable but absurd declared length must not wrap this product into a
	// small number (or zero, which would divide by zero) and over-admit
	// workers, so overflow fails closed too.
	perWorker := chunkBytes * uint64(verifiersPerWorker)
	if perWorker/uint64(verifiersPerWorker) != chunkBytes {
		return 1
	}
	affordable := budget / perWorker
	if affordable < 1 {
		return 1
	}
	if affordable < uint64(configured) {
		return int(affordable)
	}
	return configured
}

// cohortConsolidatedChunkBytes reads the cohort's pinned CSCB index once and
// returns its largest decompressed chunk. The HEAD goes through the shared plan
// cache so the per-row path reuses it. Any failure returns 0, which callers
// treat as "unmeasurable" and reduce to serial; the per-row path re-heads and
// fails those rows closed on its own terms.
func (p *Planner) cohortConsolidatedChunkBytes(
	ctx context.Context,
	req PlanRequest,
	shared *planShared,
	consolidatedKey string,
) uint64 {
	if consolidatedKey == "" {
		return 0
	}
	head, err := shared.headFor(ctx, func(ctx context.Context) (ObjectHead, error) {
		return p.store.HeadObject(ctx, req.Bucket, consolidatedKey)
	}, consolidatedKey)
	if err != nil || !head.Exists || !isImmutableVersionID(head.VersionID) {
		return 0
	}
	if _, ok := cscbLogicalPayloadBytes(head); !ok {
		return 0
	}
	index, err := readCSCBIndex(ctx, p.store, req.Bucket, consolidatedKey, head.VersionID)
	if err != nil {
		return 0
	}
	return maxChunkUncompressedBytes(index)
}

// firstConsolidatedKey returns the cohort's CSCB key from the first row that
// carries one. Every row in a cohort shares the same consolidated object.
func firstConsolidatedKey(rows []MetadataRow) string {
	for _, row := range rows {
		if row.Shadow != nil && row.Shadow.ConsolidatedObjectKey != "" {
			return row.Shadow.ConsolidatedObjectKey
		}
	}
	return ""
}

func (p *Planner) Apply(ctx context.Context, req PlanRequest, report *Report) error {
	if report == nil {
		return xerrors.New("report is required")
	}
	if err := normalizeRequest(&req); err != nil {
		return err
	}
	if !req.Execute {
		return xerrors.New("execute must be enabled to apply a retirement report")
	}
	if !req.SingleBlockWritersGuarded {
		return xerrors.New("retirement execution requires all single-block writers to honor the retirement fence")
	}
	if isProduction(req.Environment) && !req.ProductionDeleteEnabled {
		return xerrors.New("production deletion requires explicit production-delete confirmation")
	}
	if err := validateApplyReport(req, report); err != nil {
		return err
	}
	safetyCache := p.newRetentionSafetyCache()
	if err := p.preflightApplyRetentionSafety(ctx, report, safetyCache); err != nil {
		report.QuiescenceRetryAfter = safetyCache.maxQuiescenceRetryAfter()
		report.SafetyGates.CSCBWriteOncePolicyVerified = writeOncePolicyGateVerified(report.Items)
		report.Summary = summarize(report.Items)
		return err
	}

	deleteIndexes := make([]int, 0, len(report.Items))
	for i := range report.Items {
		if report.Items[i].Action == ActionDeleteObjectVersion {
			deleteIndexes = append(deleteIndexes, i)
		}
	}
	// Apply runs a pre- and a post-delete verifier per worker, so it pins two
	// chunks per worker — the heaviest phase, and the one to budget against.
	parallelism := p.configuredRowParallelism()
	if parallelism > 1 && len(deleteIndexes) > 0 {
		parallelism = p.budgetedRowParallelism(
			req,
			p.cohortConsolidatedChunkBytes(
				ctx,
				req,
				newPlanShared(),
				report.Items[deleteIndexes[0]].ConsolidatedKey,
			),
			2,
		)
	}
	group, groupCtx := errgroup.WithContext(ctx)
	// Contiguous segments keep each worker's verifiers local to height-adjacent
	// rows. Rows are independent single-block objects; the first error cancels
	// the pass and every not-yet-started row is marked not-attempted, matching
	// the serial halt-on-failure contract.
	for _, segment := range rowSegments(len(deleteIndexes), parallelism) {
		group.Go(func() error {
			preVerifier := p.verifierFactory()
			postVerifier := p.verifierFactory()
			markNotAttempted := func(itemIndexes []int) {
				for _, itemIndex := range itemIndexes {
					report.Items[itemIndex].Action = ActionSkip
					report.Items[itemIndex].SkipReason = SkipNotAttemptedAfterFailure
				}
			}
			indexes := deleteIndexes[segment.start:segment.end]
			for position, itemIndex := range indexes {
				item := &report.Items[itemIndex]
				if groupCtx.Err() != nil {
					markNotAttempted(indexes[position:])
					if ctxErr := ctx.Err(); ctxErr != nil {
						return xerrors.Errorf("single-block retirement apply canceled: %w", ctxErr)
					}
					return nil
				}
				p.recordHeartbeat(groupCtx, "retirement.apply.row", item.Height)
				reason, err := p.applyCandidate(groupCtx, req, item, preVerifier, postVerifier, safetyCache)
				if reason != "" {
					if item.RetirementState != RetirementStateDeletedPendingVerification {
						item.Action = ActionSkip
					}
					item.SkipReason = reason
					item.RetirementOutcome = reason
				}
				if err != nil {
					markNotAttempted(indexes[position+1:])
					return err
				}
			}
			return nil
		})
	}
	firstErr := group.Wait()
	report.QuiescenceRetryAfter = safetyCache.maxQuiescenceRetryAfter()
	report.SafetyGates.CSCBWriteOncePolicyVerified = writeOncePolicyGateVerified(report.Items)
	report.Summary = summarize(report.Items)
	return firstErr
}

func (p *Planner) Reconcile(ctx context.Context, req PlanRequest) (*Report, error) {
	if err := normalizeRequest(&req); err != nil {
		return nil, err
	}
	if req.Execute && isProduction(req.Environment) && !req.ProductionDeleteEnabled {
		return nil, xerrors.New("production reconciliation requires explicit production-delete confirmation")
	}
	if req.Execute && !req.SingleBlockWritersGuarded {
		return nil, xerrors.New("retirement reconciliation requires all single-block writers to honor the retirement fence")
	}
	manifests, err := p.repo.ListPendingRetirements(
		ctx,
		req.Tag,
		req.StartHeight,
		req.EndHeight,
		req.EligibilityCutoff,
		req.Limit,
	)
	if err != nil {
		return nil, err
	}
	report := newReport(req)
	safetyCache := p.newRetentionSafetyCache()
	preVerifier := p.verifierFactory()
	postVerifier := p.verifierFactory()
	var firstErr error
	halted := false
	for _, manifest := range manifests {
		item := candidateFromManifest(manifest)
		if !halted {
			if ctxErr := ctx.Err(); ctxErr != nil {
				if firstErr == nil {
					firstErr = xerrors.Errorf("single-block retirement reconciliation canceled: %w", ctxErr)
				}
				halted = true
			}
		}
		if halted {
			item.Action = ActionSkip
			item.SkipReason = SkipNotAttemptedAfterFailure
			report.Items = append(report.Items, item)
			continue
		}
		p.recordHeartbeat(ctx, "retirement.reconcile.manifest", item.Height)
		if manifest.Bucket != req.Bucket || !requestAllowsCandidate(req, item) ||
			manifest.State != RetirementStateEligible &&
				manifest.State != RetirementStateDeleting &&
				manifest.State != RetirementStateDeletedPendingVerification ||
			!isSHA256Hex(manifest.SingleBlockObjectKeySHA256) || !isSHA256Hex(manifest.PayloadSHA256) ||
			!validPinnedVersionIDs(
				firstString(manifest.SingleBlockObjectVersionIDs),
				manifest.SingleBlockObjectVersionIDs,
				manifest.SingleBlockDeleteMarkerVersionIDs,
			) {
			markCandidateBlocked(&item, SkipMetadataChanged)
			report.Items = append(report.Items, item)
			continue
		}
		row, err := p.repo.GetMetadataRow(ctx, manifest.BlockMetadataID)
		if err != nil || !storageGenerationsMatchRequest(req, row) {
			markCandidateBlocked(&item, SkipMetadataChanged)
			report.Items = append(report.Items, item)
			if err != nil {
				if firstErr == nil {
					firstErr = err
				}
				halted = true
			}
			continue
		}
		if approvalGateBlocked(req) {
			markCandidateBlocked(&item, SkipChainRangeNotApproved)
			report.Items = append(report.Items, item)
			continue
		}
		if req.FallbackErrorCount > 0 {
			markCandidateBlocked(&item, SkipActiveFallbackOrReadErrors)
			report.Items = append(report.Items, item)
			continue
		}
		if req.Execute && !req.DirectStorageClientsGuarded {
			markCandidateBlocked(&item, SkipDirectStorageClientsNotGuarded)
			report.Items = append(report.Items, item)
			continue
		}
		if !req.Execute {
			if reason, err := p.inspectCSCBRetentionSafety(ctx, &item); err != nil {
				markCandidateBlocked(&item, reason)
				report.Items = append(report.Items, item)
				continue
			}
			if item.RetirementState == RetirementStateDeletedPendingVerification {
				item.Action = ActionDeletedObjectVersion
				item.SkipReason = SkipRetirementVerificationPending
			} else {
				item.Action = ActionReportOnly
			}
			report.Items = append(report.Items, item)
			continue
		}
		if reason, err := p.verifyCSCBRetentionSafetyCached(ctx, &item, safetyCache); err != nil {
			markCandidateBlocked(&item, reason)
			item.RetirementOutcome = reason
			report.Items = append(report.Items, item)
			if firstErr == nil {
				firstErr = err
			}
			if reason != SkipCSCBSafetyQuiescenceActive {
				halted = true
			}
			continue
		}
		reason, err := p.reconcileManifest(ctx, req, &item, manifest, preVerifier, postVerifier, safetyCache)
		if reason != "" {
			if item.RetirementState != RetirementStateDeletedPendingVerification {
				item.Action = ActionSkip
			}
			item.SkipReason = reason
			item.RetirementOutcome = reason
		}
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			halted = true
		}
		report.Items = append(report.Items, item)
	}
	report.QuiescenceRetryAfter = safetyCache.maxQuiescenceRetryAfter()
	report.SafetyGates.CSCBWriteOncePolicyVerified = writeOncePolicyGateVerified(report.Items)
	report.Summary = summarize(report.Items)
	return report, firstErr
}

// headFor and policyFor hold the shared mutex across the fetch: every row of a
// cohort shares one CSCB key, so a check-then-fetch cache would stampede up to
// row-parallelism duplicate bucket calls at pass start. Serializing the
// singleton loads is the point; per-row topology lookups stay un-serialized.
func (s *planShared) headFor(
	ctx context.Context,
	load func(ctx context.Context) (ObjectHead, error),
	key string,
) (ObjectHead, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if head, ok := s.heads[key]; ok {
		return head, nil
	}
	head, err := load(ctx)
	if err != nil {
		return ObjectHead{}, err
	}
	s.heads[key] = head
	return head, nil
}

func (s *planShared) policyFor(
	ctx context.Context,
	load func(ctx context.Context) bool,
	key string,
) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if verified, ok := s.policies[key]; ok {
		return verified
	}
	verified := load(ctx)
	s.policies[key] = verified
	return verified
}

func (s *planShared) cachedTopology(key string) (ObjectVersionTopology, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	topology, ok := s.topologies[key]
	return topology, ok
}

func (s *planShared) storeTopology(key string, topology ObjectVersionTopology) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.topologies[key] = topology
}

func (p *Planner) planRow(
	ctx context.Context,
	req PlanRequest,
	row MetadataRow,
	shared *planShared,
	verifier blockPayloadVerifier,
) Candidate {
	item := Candidate{
		Bucket:             req.Bucket,
		Key:                row.SingleBlockObjectKey,
		BlockMetadataID:    row.BlockMetadataID,
		Tag:                row.Tag,
		Height:             row.Height,
		Hash:               row.Hash,
		ByteOffset:         row.PrimaryByteOffset,
		ByteLength:         row.PrimaryByteLength,
		UncompressedLength: row.PrimaryUncompressedLength,
		Action:             ActionSkip,
	}
	if row.Retirement != nil {
		item.RetirementState = row.Retirement.State
		item.RetirementAttempts = row.Retirement.AttemptCount
		item.RetirementOutcome = row.Retirement.Outcome
		if row.Retirement.Bucket != req.Bucket || !storageGenerationsMatchRequest(req, row) {
			item.SkipReason = SkipStorageGenerationMismatch
			return item
		}
		if row.Retirement.State == RetirementStateDeletedVerified {
			if !finalizedRetirementMatchesRow(row) {
				item.SkipReason = SkipMetadataChanged
				return item
			}
			item.Key = ""
			item.VersionID = firstString(row.Retirement.SingleBlockObjectVersionIDs)
			item.VersionIDs = append([]string(nil), row.Retirement.SingleBlockObjectVersionIDs...)
			item.DeleteMarkerVersionIDs = append([]string(nil), row.Retirement.SingleBlockDeleteMarkerVersionIDs...)
			item.SingleBlockVersions = len(item.VersionIDs)
			item.DeleteMarkers = len(item.DeleteMarkerVersionIDs)
			item.SingleBlockETag = row.Retirement.SingleBlockObjectETag
			item.SingleBlockBytes = row.Retirement.SingleBlockObjectBytes
			item.ConsolidatedKey = row.Retirement.ConsolidatedObjectKey
			item.CSCBVersionID = row.Retirement.ConsolidatedObjectVersionID
			item.CSCBETag = row.Retirement.ConsolidatedObjectETag
			item.PayloadSHA256 = row.Retirement.PayloadSHA256
			item.SingleBlockDeletedAt = row.Retirement.DeletedAt
			item.RetirementVerifiedAt = row.Retirement.VerifiedAt
			item.Action = ActionAlreadyDeleted
			item.SkipReason = SkipRetirementAlreadyFinalized
			return item
		}
		if row.Retirement.State == RetirementStateDeletedPendingVerification {
			if !pendingVerificationRetirementMatchesRow(row) {
				item.SkipReason = SkipMetadataChanged
				return item
			}
			item.Key = ""
			item.VersionID = firstString(row.Retirement.SingleBlockObjectVersionIDs)
			item.VersionIDs = append([]string(nil), row.Retirement.SingleBlockObjectVersionIDs...)
			item.DeleteMarkerVersionIDs = append([]string(nil), row.Retirement.SingleBlockDeleteMarkerVersionIDs...)
			item.SingleBlockVersions = len(item.VersionIDs)
			item.DeleteMarkers = len(item.DeleteMarkerVersionIDs)
			item.SingleBlockBytes = row.Retirement.SingleBlockObjectBytes
			item.ConsolidatedKey = row.Retirement.ConsolidatedObjectKey
			item.CSCBVersionID = row.Retirement.ConsolidatedObjectVersionID
			item.CSCBETag = row.Retirement.ConsolidatedObjectETag
			item.PayloadSHA256 = row.Retirement.PayloadSHA256
			item.SingleBlockDeletedAt = row.Retirement.DeletedAt
			item.Action = ActionDeletedObjectVersion
			item.SkipReason = SkipRetirementVerificationPending
			return item
		}
	}
	if row.Skipped {
		item.SkipReason = SkipSkippedBlock
		return item
	}
	if row.Shadow == nil {
		item.SkipReason = SkipMissingConsolidationShadow
		return item
	}
	if row.SingleBlockObjectKey == "" {
		item.SkipReason = SkipMissingSingleBlockKey
		return item
	}
	shadow := row.Shadow
	if !storageGenerationsMatchRequest(req, row) {
		item.SkipReason = SkipStorageGenerationMismatch
		return item
	}
	item.ConsolidatedKey = shadow.ConsolidatedObjectKey
	item.ValidatedAt = shadow.ValidatedAt
	item.RetiredAt = shadow.SingleBlockRetentionStartedAt
	if shadow.SingleBlockDeleteAfter != nil {
		eligibleAt := *shadow.SingleBlockDeleteAfter
		item.EligibleAt = &eligibleAt
	}
	if shadow.ValidatedAt == nil {
		item.SkipReason = SkipValidationNotPassed
		return item
	}
	if !isPrimaryConsolidated(row) {
		item.SkipReason = SkipActiveMetadataStillSingleBlock
		return item
	}
	if !validShadowReference(row, shadow) {
		item.SkipReason = SkipInvalidMetadataReference
		return item
	}
	if shadow.SingleBlockRetentionStartedAt == nil || shadow.SingleBlockDeleteAfter == nil {
		item.SkipReason = SkipMissingRetirementMarker
		return item
	}
	if item.EligibleAt != nil && req.EligibilityCutoff.Before(*item.EligibleAt) {
		item.SkipReason = SkipRetentionPeriodActive
		return item
	}
	if approvalGateBlocked(req) {
		item.SkipReason = SkipChainRangeNotApproved
		return item
	}
	if req.FallbackErrorCount > 0 {
		item.SkipReason = SkipActiveFallbackOrReadErrors
		return item
	}
	if req.Execute && !req.DirectStorageClientsGuarded {
		item.SkipReason = SkipDirectStorageClientsNotGuarded
		return item
	}
	if req.Execute && !req.SingleBlockWritersGuarded {
		item.SkipReason = SkipSingleBlockWritersNotGuarded
		return item
	}

	topology, ok := shared.cachedTopology(row.SingleBlockObjectKey)
	if !ok {
		var err error
		topology, err = p.store.ListObjectVersions(ctx, req.Bucket, row.SingleBlockObjectKey)
		if err != nil {
			item.SkipReason = SkipObjectInspectionFailed
			return item
		}
		shared.storeTopology(row.SingleBlockObjectKey, topology)
	}
	item.SingleBlockVersions = len(topology.Versions)
	item.DeleteMarkers = len(topology.DeleteMarkers)
	if len(topology.Versions) == 0 && len(topology.DeleteMarkers) == 0 {
		item.SkipReason = SkipSingleBlockObjectMissing
		return item
	}
	version, versionIDs, deleteMarkerVersionIDs, reason := pinSingleBlockTopology(topology)
	if reason != "" {
		item.SkipReason = reason
		return item
	}
	item.VersionID = version.VersionID
	item.VersionIDs = versionIDs
	item.DeleteMarkerVersionIDs = deleteMarkerVersionIDs
	item.SingleBlockETag = version.ETag
	item.SingleBlockBytes = version.Bytes

	head, err := shared.headFor(ctx, func(ctx context.Context) (ObjectHead, error) {
		return p.store.HeadObject(ctx, req.Bucket, shadow.ConsolidatedObjectKey)
	}, shadow.ConsolidatedObjectKey)
	if err != nil {
		item.SkipReason = SkipObjectInspectionFailed
		return item
	}
	if !head.Exists || head.DeleteMark || !isImmutableVersionID(head.VersionID) || head.ETag == "" {
		item.SkipReason = SkipMissingCSCBObject
		return item
	}
	if head.Expiration != "" {
		item.SkipReason = SkipCSCBLifecycleExpirationActive
		return item
	}
	item.CSCBVersionID = head.VersionID
	item.CSCBETag = head.ETag
	logicalBytes, ok := cscbLogicalPayloadBytes(head)
	if head.Bytes == 0 || !ok || shadow.ByteOffset > logicalBytes || shadow.ByteLength > logicalBytes-shadow.ByteOffset {
		item.SkipReason = SkipInvalidMetadataReference
		return item
	}
	item.SingleBlockKeySHA256 = keySHA256(item.Key)
	payloadSHA256, err := verifier.Verify(ctx, item)
	if err != nil {
		item.SkipReason = SkipSingleBlockPayloadMismatch
		return item
	}
	item.PayloadSHA256 = payloadSHA256
	writeOncePolicyVerified := shared.policyFor(ctx, func(ctx context.Context) bool {
		_, policyErr := p.store.InspectObjectRetentionSafety(ctx, req.Bucket, shadow.ConsolidatedObjectKey)
		return policyErr == nil
	}, shadow.ConsolidatedObjectKey)
	item.CSCBWriteOncePolicy = writeOncePolicyVerified
	if !writeOncePolicyVerified {
		item.SkipReason = SkipCSCBWriteOncePolicyUnverified
		return item
	}

	if req.Execute {
		if isProduction(req.Environment) && !req.ProductionDeleteEnabled {
			item.SkipReason = SkipProductionDeletionDisabled
			return item
		}
		item.Action = ActionDeleteObjectVersion
		item.SkipReason = ""
		return item
	}
	item.Action = ActionReportOnly
	item.SkipReason = ""
	return item
}

func (p *Planner) applyCandidate(
	ctx context.Context,
	req PlanRequest,
	item *Candidate,
	preVerifier blockPayloadVerifier,
	postVerifier blockPayloadVerifier,
	safetyCache *retentionSafetyCache,
) (string, error) {
	item.SingleBlockKeySHA256 = keySHA256(item.Key)
	// PrepareRetirement stamps the irreversible single-block upload fence, so
	// the row must re-prove its canonical metadata and its live pinned CSCB
	// object (fresh current + versioned HEADs) before fencing. Only the
	// payload re-read moves after the claim: the pinned version is immutable,
	// so the digest check there re-proves the same bytes Plan verified.
	if _, reason, err := p.revalidateMetadataAndCSCB(ctx, req, *item, true); err != nil {
		return reason, err
	}
	// The manifest pins Plan's verified digest; the post-claim revalidation
	// below must reproduce it from live S3 before anything is deleted.
	manifest := manifestFromCandidate(*item, req.Now)
	if err := p.repo.PrepareRetirement(ctx, manifest, req.StorageGeneration); err != nil {
		return SkipMetadataChanged, err
	}
	return p.withRetirementClaim(ctx, item, func(claim *retirementClaim) (string, error) {
		// One full revalidation after the claim covers the plan-to-delete
		// window: canonical metadata, the pinned CSCB object, the pinned
		// version topology, and the payload digest must all still match what
		// Plan verified. The pinned S3 version is immutable, so a second
		// content read between here and the delete would re-prove the same
		// bytes.
		preDeleteSHA256, reason, err := p.revalidateCandidate(ctx, req, *item, preVerifier)
		if err != nil {
			return reason, err
		}
		if preDeleteSHA256 != item.PayloadSHA256 {
			return SkipSingleBlockPayloadMismatch, xerrors.Errorf("payload digest changed immediately before retirement: block_metadata_id=%d", item.BlockMetadataID)
		}
		if err := p.maybeRenewRetirementClaim(ctx, item.BlockMetadataID, claim); err != nil {
			return SkipRetirementClaimActive, err
		}
		if reason, err := p.verifyCSCBRetentionSafetyCached(ctx, item, safetyCache); err != nil {
			return reason, err
		}
		if reason, err := p.deletePinnedSingleBlockTopology(ctx, item, claim); err != nil {
			return reason, err
		}
		return p.completeObjectDeletion(ctx, req, item, claim, item.PayloadSHA256, postVerifier)
	})
}

func (p *Planner) reconcileManifest(
	ctx context.Context,
	req PlanRequest,
	item *Candidate,
	manifest RetirementManifest,
	preVerifier blockPayloadVerifier,
	postVerifier blockPayloadVerifier,
	safetyCache *retentionSafetyCache,
) (string, error) {
	if manifest.State == RetirementStateDeletedPendingVerification {
		if manifest.SingleBlockObjectKey != "" || manifest.SingleBlockObjectETag != "" || manifest.DeletedAt == nil || manifest.VerifiedAt != nil {
			return SkipMetadataChanged, xerrors.Errorf("pending-verification retirement manifest is inconsistent: block_metadata_id=%d", manifest.BlockMetadataID)
		}
		return p.withRetirementClaim(ctx, item, func(claim *retirementClaim) (string, error) {
			if reason, err := p.verifyCSCBRetentionSafetyCached(ctx, item, safetyCache); err != nil {
				return reason, err
			}
			return p.finalizeRecordedDeletion(ctx, req, item, claim, manifest.PayloadSHA256, postVerifier)
		})
	}
	if manifest.SingleBlockObjectKey == "" || keySHA256(manifest.SingleBlockObjectKey) != manifest.SingleBlockObjectKeySHA256 {
		return SkipMetadataChanged, xerrors.Errorf("retirement manifest single-block key hash mismatch: block_metadata_id=%d", manifest.BlockMetadataID)
	}
	if manifest.State == RetirementStateEligible {
		topology, err := p.store.ListObjectVersions(ctx, manifest.Bucket, manifest.SingleBlockObjectKey)
		if err != nil {
			return SkipObjectInspectionFailed, err
		}
		if len(topology.Versions) == 0 && len(topology.DeleteMarkers) == 0 {
			return SkipMetadataChanged, xerrors.Errorf("single-block object is absent before any delete attempt: block_metadata_id=%d", manifest.BlockMetadataID)
		}
		if !topologyMatchesManifest(topology, manifest) {
			return SkipUnsafeSingleBlockVersionTopology, xerrors.Errorf("eligible retirement topology changed: block_metadata_id=%d", manifest.BlockMetadataID)
		}
	}
	return p.withRetirementClaim(ctx, item, func(claim *retirementClaim) (string, error) {
		topology, err := p.store.ListObjectVersions(ctx, manifest.Bucket, manifest.SingleBlockObjectKey)
		if err != nil {
			return SkipObjectInspectionFailed, err
		}
		if len(topology.Versions) == 0 && len(topology.DeleteMarkers) == 0 {
			if manifest.State != RetirementStateDeleting {
				return SkipMetadataChanged, xerrors.Errorf("single-block object is absent while retirement state is %s", manifest.State)
			}
			if reason, err := p.verifyCSCBRetentionSafetyCached(ctx, item, safetyCache); err != nil {
				return reason, err
			}
			return p.completeObjectDeletion(ctx, req, item, claim, manifest.PayloadSHA256, postVerifier)
		}
		if manifest.State == RetirementStateEligible && !topologyMatchesManifest(topology, manifest) ||
			manifest.State == RetirementStateDeleting && !topologyIsPinnedSubset(topology, manifest) {
			return SkipUnsafeSingleBlockVersionTopology, xerrors.Errorf("pending retirement topology changed: block_metadata_id=%d", manifest.BlockMetadataID)
		}
		verify := p.revalidateCandidate
		if manifest.State == RetirementStateDeleting {
			verify = p.revalidateConsolidatedCandidate
		}
		// One post-claim verification against the persisted manifest digest
		// covers the crash-to-resume window; the pinned version is immutable,
		// so an immediate second read would re-prove the same bytes.
		preDeleteSHA256, reason, err := verify(ctx, req, *item, preVerifier)
		if err != nil {
			return reason, err
		}
		if preDeleteSHA256 != manifest.PayloadSHA256 {
			return SkipSingleBlockPayloadMismatch, xerrors.Errorf("pending retirement payload digest changed immediately before deletion: block_metadata_id=%d", manifest.BlockMetadataID)
		}
		if err := p.maybeRenewRetirementClaim(ctx, item.BlockMetadataID, claim); err != nil {
			return SkipRetirementClaimActive, err
		}
		if reason, err := p.verifyCSCBRetentionSafetyCached(ctx, item, safetyCache); err != nil {
			return reason, err
		}
		if reason, err := p.deletePinnedSingleBlockTopology(ctx, item, claim); err != nil {
			return reason, err
		}
		return p.completeObjectDeletion(ctx, req, item, claim, manifest.PayloadSHA256, postVerifier)
	})
}

func (p *Planner) verifyCSCBRetentionSafety(ctx context.Context, item *Candidate) (string, time.Duration, error) {
	item.CSCBWriteOncePolicy = false
	snapshot, inspectionErr := p.store.InspectObjectRetentionSafety(ctx, item.Bucket, item.ConsolidatedKey)
	configurationSHA256 := snapshot.ConfigurationSHA256
	if inspectionErr != nil || !isSHA256Hex(configurationSHA256) {
		configurationSHA256 = unverifiedRetentionSafetySHA256
	}
	firstObservedAt, observedAt, observationErr := p.repo.ObserveRetentionSafety(
		ctx,
		item.Bucket,
		item.ConsolidatedKey,
		configurationSHA256,
	)
	if observationErr != nil {
		return SkipCSCBWriteOncePolicyUnverified, 0, errors.Join(inspectionErr, observationErr)
	}
	if inspectionErr != nil {
		return SkipCSCBWriteOncePolicyUnverified, 0, inspectionErr
	}
	item.CSCBWriteOncePolicy = true
	if elapsed := observedAt.Sub(firstObservedAt); elapsed < RetentionSafetyQuiescencePeriod {
		return SkipCSCBSafetyQuiescenceActive, RetentionSafetyQuiescencePeriod - elapsed, xerrors.Errorf(
			"CSCB retention safety configuration has not remained stable for %s: bucket=%s key=%s first_observed_at=%s observed_at=%s",
			RetentionSafetyQuiescencePeriod,
			item.Bucket,
			item.ConsolidatedKey,
			firstObservedAt.Format(time.RFC3339Nano),
			observedAt.Format(time.RFC3339Nano),
		)
	}
	return "", 0, nil
}

// verifyCSCBRetentionSafetyCached memoizes bucket-safety verification per CSCB
// key. The verified configuration is bucket-level and the quiescence clock is
// persisted in the database, so within retentionSafetyRevalidationInterval a
// cached success (or failure) stands in for a fresh pair of S3 control-plane
// calls. PrimeRetentionSafety and the Apply preflight seed the cache before any
// destructive row runs.
func (p *Planner) verifyCSCBRetentionSafetyCached(
	ctx context.Context,
	item *Candidate,
	cache *retentionSafetyCache,
) (string, error) {
	if cache == nil {
		reason, _, err := p.verifyCSCBRetentionSafety(ctx, item)
		return reason, err
	}
	cache.mu.Lock()
	outcome, ok := cache.outcomes[item.ConsolidatedKey]
	if ok && cache.now().Sub(outcome.verifiedAt) < retentionSafetyRevalidationInterval {
		cache.mu.Unlock()
		if outcome.err == nil {
			item.CSCBWriteOncePolicy = true
		} else {
			item.CSCBWriteOncePolicy = outcome.reason == SkipCSCBSafetyQuiescenceActive
		}
		return outcome.reason, outcome.err
	}
	cache.mu.Unlock()
	reason, retryAfter, err := p.verifyCSCBRetentionSafety(ctx, item)
	cache.mu.Lock()
	cache.outcomes[item.ConsolidatedKey] = retentionSafetyOutcome{
		reason:     reason,
		retryAfter: retryAfter,
		err:        err,
		verifiedAt: cache.now(),
	}
	if reason == SkipCSCBSafetyQuiescenceActive && retryAfter > cache.maxQuiescenceRetry {
		cache.maxQuiescenceRetry = retryAfter
	}
	cache.mu.Unlock()
	return reason, err
}

// PrimeRetentionSafety records safety observations for upcoming CSCB keys so
// their quiescence clocks start before any execute attempt reaches them. The
// inspected configuration (bucket policy + lifecycle) is bucket-level, but the
// pass/fail evaluation matches Resource ARNs and lifecycle prefixes against
// the specific key, so the keys[0] result is fanned out on the assumption that
// consolidated keys share one pattern. That assumption is only an
// optimization: the destructive path re-inspects every key through
// verifyCSCBRetentionSafety, and a key whose own inspection disagrees records
// a different configuration digest there, which resets its quiescence clock
// and fails closed.
func (p *Planner) PrimeRetentionSafety(ctx context.Context, bucket string, keys []string) (int, error) {
	if len(keys) == 0 {
		return 0, nil
	}
	snapshot, err := p.store.InspectObjectRetentionSafety(ctx, bucket, keys[0])
	if err != nil {
		return 0, xerrors.Errorf("failed to inspect retention safety for priming: %w", err)
	}
	if !isSHA256Hex(snapshot.ConfigurationSHA256) {
		return 0, xerrors.New("retention safety inspection returned an invalid configuration digest")
	}
	primed := 0
	seen := make(map[string]struct{}, len(keys))
	for _, key := range keys {
		if key == "" {
			continue
		}
		if _, duplicate := seen[key]; duplicate {
			continue
		}
		seen[key] = struct{}{}
		if err := ctx.Err(); err != nil {
			return primed, xerrors.Errorf("retention safety priming canceled: %w", err)
		}
		p.recordHeartbeat(ctx, "retirement.prime.key", key)
		if _, _, err := p.repo.ObserveRetentionSafety(ctx, bucket, key, snapshot.ConfigurationSHA256); err != nil {
			return primed, xerrors.Errorf("failed to prime retention safety observation: %w", err)
		}
		primed++
	}
	return primed, nil
}

func (c *retentionSafetyCache) maxQuiescenceRetryAfter() time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.maxQuiescenceRetry
}

func (p *Planner) inspectCSCBRetentionSafety(ctx context.Context, item *Candidate) (string, error) {
	item.CSCBWriteOncePolicy = false
	if _, err := p.store.InspectObjectRetentionSafety(ctx, item.Bucket, item.ConsolidatedKey); err != nil {
		return SkipCSCBWriteOncePolicyUnverified, err
	}
	item.CSCBWriteOncePolicy = true
	return "", nil
}

func (p *Planner) preflightApplyRetentionSafety(
	ctx context.Context,
	report *Report,
	cache *retentionSafetyCache,
) error {
	var firstErr error
	for i := range report.Items {
		item := &report.Items[i]
		if item.Action != ActionDeleteObjectVersion {
			continue
		}
		reason, err := p.verifyCSCBRetentionSafetyCached(ctx, item, cache)
		if err == nil {
			continue
		}
		item.Action = ActionSkip
		item.SkipReason = reason
		item.RetirementOutcome = reason
		if firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (p *Planner) withRetirementClaim(
	ctx context.Context,
	item *Candidate,
	operation func(claim *retirementClaim) (string, error),
) (reason string, err error) {
	claimToken, err := newRetirementClaimToken()
	if err != nil {
		return SkipMetadataChanged, err
	}
	claimedAt := time.Now().UTC()
	if err := p.repo.ClaimRetirement(ctx, item.BlockMetadataID, claimToken, claimedAt, claimedAt.Add(RetirementClaimLease)); err != nil {
		if errors.Is(err, ErrRetirementClaimUnavailable) {
			return SkipRetirementClaimActive, nil
		}
		return SkipMetadataChanged, err
	}
	claim := &retirementClaim{token: claimToken, lastRenewedAt: claimedAt}
	if item.RetirementState == RetirementStateDeletedPendingVerification {
		item.RetirementOutcome = "verification_started"
	} else {
		item.RetirementState = RetirementStateDeleting
		item.RetirementOutcome = "delete_started"
	}
	item.RetirementAttempts++

	claimOwned := true
	defer func() {
		if err == nil || !claimOwned {
			return
		}
		operationErr := err
		outcome := reason
		if outcome == "" {
			outcome = SkipMetadataChanged
		}
		// Record the interruption outcome even when the failure is a
		// cancellation, so a stopped run leaves a durable trace instead of an
		// unexplained claim that only expires by lease.
		detached := context.WithoutCancel(ctx)
		outcomeCtx, cancelOutcome := context.WithTimeout(detached, p.claimCleanupWriteTimeout())
		outcomeErr := p.repo.RecordRetirementOutcome(outcomeCtx, item.BlockMetadataID, claimToken, outcome, time.Now().UTC())
		cancelOutcome()
		if outcomeErr != nil {
			err = errors.Join(err, outcomeErr)
		}
		// A canceled run is going away — the worker is stopping, the pod is
		// terminating — and Temporal reschedules the activity on a live
		// worker within seconds. Left alone, this claim would hold the row
		// against that attempt for the rest of the lease (15 minutes), and
		// a sweep whose retry budget was already spent fails on it
		// (INF-1603). Release it so the next attempt starts at once. Only
		// cancellation releases: a row that failed for any other reason keeps
		// its lease as the cool-down before it is retried.
		// The release runs on its own deadline that starts only now, so an
		// outcome write that consumed its whole budget above cannot hand the
		// release an expired context; the cancellation and deadline checks
		// below read the operation's error, not the cleanup writes', so a
		// timed-out outcome write never blocks the release.
		if ctx.Err() != nil || errors.Is(operationErr, context.Canceled) || errors.Is(operationErr, context.DeadlineExceeded) {
			releaseCtx, cancelRelease := context.WithTimeout(detached, p.claimCleanupWriteTimeout())
			defer cancelRelease()
			if releaseErr := p.repo.ReleaseRetirementClaim(releaseCtx, item.BlockMetadataID, claimToken); releaseErr != nil {
				err = errors.Join(err, releaseErr)
			}
		}
	}()

	reason, err = operation(claim)
	if err == nil {
		claimOwned = false
	}
	return reason, err
}

// maybeRenewRetirementClaim renews once the claim has consumed half of its
// lease. The claim cannot be taken over before the full lease expires, so a
// half-life cadence keeps exclusive ownership provable on non-destructive
// steps; renewRetirementClaim below remains the unconditional fence
// immediately before a row's destructive sequence.
func (p *Planner) maybeRenewRetirementClaim(ctx context.Context, blockMetadataID int64, claim *retirementClaim) error {
	if time.Now().UTC().Sub(claim.lastRenewedAt) < RetirementClaimLease/retirementClaimRenewalFraction {
		return nil
	}
	return p.renewRetirementClaim(ctx, blockMetadataID, claim)
}

// renewRetirementClaim unconditionally renews and thereby re-asserts exclusive
// claim ownership against the database. It is the fence before destructive S3
// work: a claim that was taken over after a long stall fails here instead of
// deleting unfenced.
func (p *Planner) renewRetirementClaim(ctx context.Context, blockMetadataID int64, claim *retirementClaim) error {
	renewedAt := time.Now().UTC()
	if err := p.repo.RenewRetirementClaim(ctx, blockMetadataID, claim.token, renewedAt, renewedAt.Add(RetirementClaimLease)); err != nil {
		return err
	}
	claim.lastRenewedAt = renewedAt
	return nil
}

func (p *Planner) deletePinnedSingleBlockTopology(
	ctx context.Context,
	item *Candidate,
	claim *retirementClaim,
) (string, error) {
	topology, err := p.store.ListObjectVersions(ctx, item.Bucket, item.Key)
	if err != nil {
		return SkipObjectInspectionFailed, err
	}
	if !topologyIsPinnedCandidateSubset(topology, *item) {
		return SkipUnsafeSingleBlockVersionTopology, xerrors.Errorf(
			"single-block object version topology changed before exact deletion: block_metadata_id=%d",
			item.BlockMetadataID,
		)
	}

	// Unconditional fence: prove exclusive claim ownership against the
	// database immediately before this row's destructive sequence, regardless
	// of how recently the claim was renewed.
	if err := p.renewRetirementClaim(ctx, item.BlockMetadataID, claim); err != nil {
		return SkipRetirementClaimActive, err
	}

	remainingVersions := make(map[string]struct{}, len(topology.Versions))
	for _, version := range topology.Versions {
		remainingVersions[version.VersionID] = struct{}{}
	}
	remainingMarkers := make(map[string]struct{}, len(topology.DeleteMarkers))
	for _, marker := range topology.DeleteMarkers {
		remainingMarkers[marker.VersionID] = struct{}{}
	}
	deleteVersion := func(versionID string) (string, error) {
		if err := ctx.Err(); err != nil {
			return SkipRetentionRunCanceled, xerrors.Errorf(
				"single-block version deletion canceled before version %s: %w",
				versionID,
				err,
			)
		}
		p.recordHeartbeat(ctx, "retirement.delete.version", item.Height, versionID)
		if err := p.maybeRenewRetirementClaim(ctx, item.BlockMetadataID, claim); err != nil {
			return SkipRetirementClaimActive, err
		}
		if err := p.store.DeleteObjectVersion(ctx, item.Bucket, item.Key, versionID); err != nil {
			return SkipVersionedDeleteFailed, err
		}
		return "", nil
	}

	// Keep the payload-validation version available until every older data
	// version is gone, then remove delete markers only after all data versions.
	for _, versionID := range item.VersionIDs[1:] {
		if _, ok := remainingVersions[versionID]; !ok {
			continue
		}
		if reason, err := deleteVersion(versionID); err != nil {
			return reason, err
		}
	}
	if _, ok := remainingVersions[item.VersionID]; ok {
		if reason, err := deleteVersion(item.VersionID); err != nil {
			return reason, err
		}
	}
	for _, versionID := range item.DeleteMarkerVersionIDs {
		if _, ok := remainingMarkers[versionID]; !ok {
			continue
		}
		if reason, err := deleteVersion(versionID); err != nil {
			return reason, err
		}
	}
	return "", nil
}

func (p *Planner) completeObjectDeletion(
	ctx context.Context,
	req PlanRequest,
	item *Candidate,
	claim *retirementClaim,
	expectedPayloadSHA256 string,
	postVerifier blockPayloadVerifier,
) (string, error) {
	if err := p.verifySingleBlockDeleted(ctx, item.Bucket, item.Key); err != nil {
		return SkipPostDeleteVerificationFailed, err
	}
	deletedAt, err := p.repo.RecordRetirementObjectDeleted(
		ctx,
		item.BlockMetadataID,
		claim.token,
		ActionDeletedObjectVersion,
	)
	if err != nil {
		return SkipPostDeleteVerificationFailed, err
	}
	item.Action = ActionDeletedObjectVersion
	item.SkipReason = ""
	item.RetirementState = RetirementStateDeletedPendingVerification
	item.RetirementOutcome = ActionDeletedObjectVersion
	item.SingleBlockDeletedAt = &deletedAt
	item.Key = ""
	item.SingleBlockETag = ""
	return p.finalizeRecordedDeletion(ctx, req, item, claim, expectedPayloadSHA256, postVerifier)
}

func (p *Planner) finalizeRecordedDeletion(
	ctx context.Context,
	req PlanRequest,
	item *Candidate,
	claim *retirementClaim,
	expectedPayloadSHA256 string,
	postVerifier blockPayloadVerifier,
) (string, error) {
	if err := p.maybeRenewRetirementClaim(ctx, item.BlockMetadataID, claim); err != nil {
		return SkipRetirementClaimActive, err
	}
	// postVerifier is scoped to the pass and used only after deletions, so its
	// first read of each CSCB chunk happens post-delete and is then shared by
	// height-adjacent rows in the same chunk. Every row still re-heads the
	// pinned CSCB object freshly inside revalidateConsolidatedCandidate, so a
	// vanished or changed CSCB fails this row even on a cached chunk. Residual
	// window accepted by this reuse: an S3 data-loss event that keeps the
	// pinned version's HEAD metadata intact while its bytes become unreadable
	// would pass later rows in the same chunk against the cached read; content
	// verification at the exact instant of finalize was never atomic with the
	// delete either, this widens that instant to at most one chunk of rows.
	payloadSHA256, reason, err := p.revalidateConsolidatedCandidate(ctx, req, *item, postVerifier)
	if err != nil {
		return reason, err
	}
	if payloadSHA256 != expectedPayloadSHA256 {
		return SkipSingleBlockPayloadMismatch, xerrors.Errorf("pinned CSCB payload digest changed after single-block deletion: block_metadata_id=%d", item.BlockMetadataID)
	}
	verifiedAt, err := p.repo.FinalizeRetirement(ctx, item.BlockMetadataID, claim.token, ActionDeletedVerified)
	if err != nil {
		return SkipPostDeleteVerificationFailed, err
	}
	item.Action = ActionDeletedVerified
	item.SkipReason = ""
	item.RetirementState = RetirementStateDeletedVerified
	item.RetirementOutcome = ActionDeletedVerified
	item.RetirementVerifiedAt = &verifiedAt
	return "", nil
}

func (p *Planner) revalidateCandidate(ctx context.Context, req PlanRequest, candidate Candidate, verifier blockPayloadVerifier) (string, string, error) {
	if _, reason, err := p.revalidateMetadataAndCSCB(ctx, req, candidate, true); err != nil {
		return "", reason, err
	}
	topology, err := p.store.ListObjectVersions(ctx, candidate.Bucket, candidate.Key)
	if err != nil {
		return "", SkipObjectInspectionFailed, err
	}
	if !topologyMatchesCandidate(topology, candidate) {
		return "", SkipUnsafeSingleBlockVersionTopology, xerrors.Errorf("single-block object version topology changed before retirement: block_metadata_id=%d", candidate.BlockMetadataID)
	}
	digest, err := verifier.Verify(ctx, candidate)
	if err != nil {
		return "", SkipSingleBlockPayloadMismatch, err
	}
	return digest, "", nil
}

func (p *Planner) revalidateConsolidatedCandidate(ctx context.Context, req PlanRequest, candidate Candidate, verifier blockPayloadVerifier) (string, string, error) {
	if _, reason, err := p.revalidateMetadataAndCSCB(ctx, req, candidate, false); err != nil {
		return "", reason, err
	}
	digest, err := verifier.VerifyConsolidated(ctx, candidate)
	if err != nil {
		return "", SkipSingleBlockPayloadMismatch, err
	}
	return digest, "", nil
}

func (p *Planner) revalidateMetadataAndCSCB(ctx context.Context, req PlanRequest, candidate Candidate, requireCanonical bool) (MetadataRow, string, error) {
	row, err := p.repo.GetMetadataRow(ctx, candidate.BlockMetadataID)
	if err != nil {
		return MetadataRow{}, SkipMetadataChanged, err
	}
	if !candidateMatchesRow(req, candidate, row, requireCanonical) {
		return MetadataRow{}, SkipMetadataChanged, xerrors.Errorf("canonical, shadow, or retirement metadata changed before retirement: block_metadata_id=%d", candidate.BlockMetadataID)
	}
	current, err := p.store.HeadObject(ctx, candidate.Bucket, candidate.ConsolidatedKey)
	if err != nil {
		return MetadataRow{}, SkipObjectInspectionFailed, err
	}
	pinned, err := p.store.HeadObjectVersion(ctx, candidate.Bucket, candidate.ConsolidatedKey, candidate.CSCBVersionID)
	if err != nil {
		return MetadataRow{}, SkipObjectInspectionFailed, err
	}
	if !current.Exists || current.DeleteMark || !isImmutableVersionID(current.VersionID) || current.VersionID != candidate.CSCBVersionID || current.ETag != candidate.CSCBETag ||
		!pinned.Exists || pinned.DeleteMark || pinned.VersionID != candidate.CSCBVersionID || pinned.ETag != candidate.CSCBETag {
		return MetadataRow{}, SkipCSCBObjectChanged, xerrors.Errorf("pinned CSCB object changed before retirement: block_metadata_id=%d", candidate.BlockMetadataID)
	}
	if current.Expiration != "" || pinned.Expiration != "" {
		return MetadataRow{}, SkipCSCBLifecycleExpirationActive, xerrors.Errorf("pinned CSCB object has active lifecycle expiration: block_metadata_id=%d", candidate.BlockMetadataID)
	}
	logicalBytes, ok := cscbLogicalPayloadBytes(pinned)
	if pinned.Bytes == 0 || !ok || candidate.ByteOffset > logicalBytes || candidate.ByteLength > logicalBytes-candidate.ByteOffset {
		return MetadataRow{}, SkipCSCBObjectChanged, xerrors.Errorf("pinned CSCB metadata is invalid before retirement: block_metadata_id=%d", candidate.BlockMetadataID)
	}
	return row, "", nil
}

func (p *Planner) verifySingleBlockDeleted(ctx context.Context, bucket string, key string) error {
	topology, err := p.store.ListObjectVersions(ctx, bucket, key)
	if err != nil {
		return err
	}
	if len(topology.Versions) != 0 || len(topology.DeleteMarkers) != 0 {
		return xerrors.Errorf("single-block key still has versions after delete: versions=%d markers=%d", len(topology.Versions), len(topology.DeleteMarkers))
	}
	head, err := p.store.HeadObject(ctx, bucket, key)
	if err != nil {
		return err
	}
	if head.Exists {
		return xerrors.New("single-block key still resolves after deleting its pinned version")
	}
	return nil
}

func candidateMatchesRow(req PlanRequest, candidate Candidate, row MetadataRow, requireCanonical bool) bool {
	if !storageGenerationsMatchRequest(req, row) {
		return false
	}
	if candidate.RetirementState == RetirementStateDeletedPendingVerification {
		return pendingVerificationCandidateMatchesRow(req, candidate, row, requireCanonical)
	}
	if row.Shadow == nil || row.Shadow.ValidatedAt == nil || row.Shadow.SingleBlockRetentionStartedAt == nil ||
		row.Shadow.SingleBlockDeleteAfter == nil || row.Shadow.SingleBlockObjectDeletedAt != nil ||
		req.EligibilityCutoff.Before(*row.Shadow.SingleBlockDeleteAfter) {
		return false
	}
	retirementMatches := row.Retirement == nil || pendingRetirementMatchesCandidate(row, candidate)
	return (!requireCanonical || row.Canonical) &&
		!row.Skipped &&
		approvalMatches(req) &&
		req.FallbackErrorCount == 0 &&
		req.DirectStorageClientsGuarded &&
		req.SingleBlockWritersGuarded &&
		requestAllowsCandidate(req, candidate) &&
		row.BlockMetadataID == candidate.BlockMetadataID &&
		row.Tag == candidate.Tag &&
		row.Height == candidate.Height &&
		row.Hash == candidate.Hash &&
		row.SingleBlockObjectKey == candidate.Key &&
		row.PrimaryObjectKey == candidate.ConsolidatedKey &&
		row.PrimaryObjectFormat == api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_CSCB_BATCH &&
		row.PrimaryByteOffset == candidate.ByteOffset &&
		row.PrimaryByteLength == candidate.ByteLength &&
		row.PrimaryUncompressedLength == candidate.UncompressedLength &&
		retirementMatches &&
		validShadowReference(row, row.Shadow)
}

func storageGenerationsMatchRequest(req PlanRequest, row MetadataRow) bool {
	return row.Shadow != nil &&
		row.PrimaryStorageGeneration == req.StorageGeneration &&
		row.Shadow.SingleBlockStorageGeneration == req.StorageGeneration &&
		row.Shadow.ConsolidatedStorageGeneration == req.StorageGeneration
}

func pendingVerificationCandidateMatchesRow(req PlanRequest, candidate Candidate, row MetadataRow, requireCanonical bool) bool {
	manifest := row.Retirement
	if manifest == nil || manifest.State != RetirementStateDeletedPendingVerification ||
		row.Shadow == nil || row.RetirementFencedAt == nil ||
		row.SingleBlockObjectKey != "" || row.Shadow.SingleBlockObjectKey != "" ||
		row.Shadow.ValidatedAt == nil || row.Shadow.SingleBlockRetentionStartedAt == nil ||
		row.Shadow.SingleBlockDeleteAfter == nil || row.Shadow.SingleBlockObjectDeletedAt == nil ||
		manifest.SingleBlockObjectKey != "" || manifest.SingleBlockObjectETag != "" ||
		manifest.DeletedAt == nil || manifest.VerifiedAt != nil ||
		!row.Shadow.SingleBlockObjectDeletedAt.Equal(*manifest.DeletedAt) {
		return false
	}
	return (!requireCanonical || row.Canonical) &&
		!row.Skipped &&
		approvalMatches(req) &&
		req.FallbackErrorCount == 0 &&
		req.DirectStorageClientsGuarded &&
		req.SingleBlockWritersGuarded &&
		requestAllowsCandidate(req, candidate) &&
		row.BlockMetadataID == candidate.BlockMetadataID &&
		row.Tag == candidate.Tag &&
		row.Height == candidate.Height &&
		row.Hash == candidate.Hash &&
		row.PrimaryObjectKey == candidate.ConsolidatedKey &&
		row.PrimaryObjectFormat == api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_CSCB_BATCH &&
		row.PrimaryByteOffset == candidate.ByteOffset &&
		row.PrimaryByteLength == candidate.ByteLength &&
		row.PrimaryUncompressedLength == candidate.UncompressedLength &&
		pendingRetirementMatchesCandidate(row, candidate) &&
		validShadowReference(row, row.Shadow)
}

func pendingRetirementMatchesCandidate(row MetadataRow, candidate Candidate) bool {
	manifest := row.Retirement
	if manifest == nil || row.RetirementFencedAt == nil ||
		manifest.State != RetirementStateEligible && manifest.State != RetirementStateDeleting && manifest.State != RetirementStateDeletedPendingVerification {
		return false
	}
	return manifest.BlockMetadataID == candidate.BlockMetadataID &&
		manifest.Tag == candidate.Tag &&
		manifest.Height == candidate.Height &&
		manifest.Hash == candidate.Hash &&
		manifest.Bucket == candidate.Bucket &&
		manifest.SingleBlockObjectKey == candidate.Key &&
		manifest.SingleBlockObjectKeySHA256 == candidate.SingleBlockKeySHA256 &&
		sameStrings(manifest.SingleBlockObjectVersionIDs, candidate.VersionIDs) &&
		sameStrings(manifest.SingleBlockDeleteMarkerVersionIDs, candidate.DeleteMarkerVersionIDs) &&
		firstString(manifest.SingleBlockObjectVersionIDs) == candidate.VersionID &&
		manifest.SingleBlockObjectETag == candidate.SingleBlockETag &&
		manifest.SingleBlockObjectBytes == candidate.SingleBlockBytes &&
		manifest.ConsolidatedObjectKey == candidate.ConsolidatedKey &&
		manifest.ConsolidatedObjectVersionID == candidate.CSCBVersionID &&
		manifest.ConsolidatedObjectETag == candidate.CSCBETag &&
		manifest.ConsolidatedByteOffset == candidate.ByteOffset &&
		manifest.ConsolidatedByteLength == candidate.ByteLength &&
		manifest.ConsolidatedUncompressedLength == candidate.UncompressedLength &&
		manifest.PayloadSHA256 == candidate.PayloadSHA256
}

func pendingVerificationRetirementMatchesRow(row MetadataRow) bool {
	manifest := row.Retirement
	return manifest != nil &&
		manifest.State == RetirementStateDeletedPendingVerification &&
		manifest.SingleBlockObjectKey == "" &&
		manifest.SingleBlockObjectETag == "" &&
		manifest.DeletedAt != nil &&
		manifest.VerifiedAt == nil &&
		manifest.ClaimToken != "" &&
		manifest.ClaimExpiresAt != nil &&
		row.Shadow != nil &&
		row.RetirementFencedAt != nil &&
		row.SingleBlockObjectKey == "" &&
		row.Shadow.SingleBlockObjectKey == "" &&
		row.Shadow.SingleBlockObjectDeletedAt != nil &&
		row.Shadow.SingleBlockObjectDeletedAt.Equal(*manifest.DeletedAt) &&
		manifest.BlockMetadataID == row.BlockMetadataID &&
		manifest.Tag == row.Tag &&
		manifest.Height == row.Height &&
		manifest.Hash == row.Hash &&
		manifest.ConsolidatedObjectKey == row.PrimaryObjectKey &&
		manifest.ConsolidatedByteOffset == row.PrimaryByteOffset &&
		manifest.ConsolidatedByteLength == row.PrimaryByteLength &&
		manifest.ConsolidatedUncompressedLength == row.PrimaryUncompressedLength &&
		validShadowReference(row, row.Shadow)
}

func finalizedRetirementMatchesRow(row MetadataRow) bool {
	manifest := row.Retirement
	if manifest == nil || row.Shadow == nil || row.RetirementFencedAt == nil ||
		manifest.State != RetirementStateDeletedVerified ||
		manifest.SingleBlockObjectKey != "" || manifest.SingleBlockObjectETag != "" ||
		manifest.ClaimToken != "" || manifest.ClaimExpiresAt != nil ||
		manifest.DeletedAt == nil || manifest.VerifiedAt == nil || manifest.Outcome == "" ||
		row.SingleBlockObjectKey != "" || row.Shadow.SingleBlockObjectKey != "" ||
		row.Shadow.SingleBlockObjectDeletedAt == nil ||
		!row.Shadow.SingleBlockObjectDeletedAt.Equal(*manifest.DeletedAt) {
		return false
	}
	return manifest.BlockMetadataID == row.BlockMetadataID &&
		manifest.Tag == row.Tag &&
		manifest.Height == row.Height &&
		manifest.Hash == row.Hash &&
		manifest.ConsolidatedObjectKey == row.PrimaryObjectKey &&
		manifest.ConsolidatedByteOffset == row.PrimaryByteOffset &&
		manifest.ConsolidatedByteLength == row.PrimaryByteLength &&
		manifest.ConsolidatedUncompressedLength == row.PrimaryUncompressedLength &&
		validShadowReference(row, row.Shadow)
}

func requestAllowsCandidate(req PlanRequest, candidate Candidate) bool {
	return candidate.Bucket == req.Bucket &&
		candidate.Tag == req.Tag &&
		candidate.Height >= req.StartHeight &&
		candidate.Height < req.EndHeight
}

func validateApplyReport(req PlanRequest, report *Report) error {
	if report.DryRun ||
		report.Environment != req.Environment ||
		report.Blockchain != req.Blockchain ||
		report.Network != req.Network ||
		report.Sidechain != req.Sidechain ||
		report.Bucket != req.Bucket ||
		report.Tag != req.Tag ||
		report.StartHeight != req.StartHeight ||
		report.EndHeight != req.EndHeight ||
		report.Approval != req.Approval ||
		report.SafetyGates.DirectStorageClientsGuarded != req.DirectStorageClientsGuarded ||
		report.SafetyGates.SingleBlockWritersGuarded != req.SingleBlockWritersGuarded ||
		report.SafetyGates.FallbackReadErrors != req.FallbackErrorCount ||
		report.SafetyGates.ProductionDeleteEnabled != req.ProductionDeleteEnabled ||
		report.SafetyGates.VersionedDeleteMode != versionedDeleteMode ||
		report.SafetyGates.CSCBWriteOncePolicyMode != cscbWriteOncePolicyMode ||
		!report.SafetyGates.CSCBWriteOncePolicyVerified {
		return xerrors.New("retirement report does not match the execution request")
	}
	if !approvalMatches(req) || req.FallbackErrorCount != 0 || !req.DirectStorageClientsGuarded || !req.SingleBlockWritersGuarded {
		return xerrors.New("retirement execution safety gates are not satisfied")
	}
	for _, item := range report.Items {
		if item.Action != ActionDeleteObjectVersion {
			continue
		}
		if !requestAllowsCandidate(req, item) || item.BlockMetadataID <= 0 || item.Key == "" ||
			item.SingleBlockKeySHA256 != keySHA256(item.Key) ||
			!validPinnedVersionIDs(item.VersionID, item.VersionIDs, item.DeleteMarkerVersionIDs) || item.SingleBlockETag == "" ||
			item.SingleBlockBytes == 0 || item.SingleBlockVersions != len(item.VersionIDs) || item.DeleteMarkers != len(item.DeleteMarkerVersionIDs) ||
			item.ConsolidatedKey == "" || !isImmutableVersionID(item.CSCBVersionID) || item.CSCBETag == "" ||
			!item.CSCBWriteOncePolicy ||
			item.ByteLength == 0 || item.UncompressedLength == 0 || !isSHA256Hex(item.PayloadSHA256) {
			return xerrors.Errorf("retirement report contains an invalid delete candidate: block_metadata_id=%d", item.BlockMetadataID)
		}
	}
	return nil
}

func topologyMatchesCandidate(topology ObjectVersionTopology, candidate Candidate) bool {
	version, versionIDs, markerIDs, reason := pinSingleBlockTopology(topology)
	return reason == "" &&
		version.VersionID == candidate.VersionID &&
		version.ETag == candidate.SingleBlockETag &&
		version.Bytes == candidate.SingleBlockBytes &&
		sameStrings(versionIDs, candidate.VersionIDs) &&
		sameStrings(markerIDs, candidate.DeleteMarkerVersionIDs)
}

func topologyMatchesManifest(topology ObjectVersionTopology, manifest RetirementManifest) bool {
	return topologyMatchesCandidate(topology, candidateFromManifest(manifest))
}

func topologyIsPinnedSubset(topology ObjectVersionTopology, manifest RetirementManifest) bool {
	return topologyIsPinnedCandidateSubset(topology, candidateFromManifest(manifest))
}

func topologyIsPinnedCandidateSubset(topology ObjectVersionTopology, candidate Candidate) bool {
	if !validPinnedVersionIDs(candidate.VersionID, candidate.VersionIDs, candidate.DeleteMarkerVersionIDs) {
		return false
	}
	versionIDs := make(map[string]struct{}, len(candidate.VersionIDs))
	for _, versionID := range candidate.VersionIDs {
		versionIDs[versionID] = struct{}{}
	}
	markerIDs := make(map[string]struct{}, len(candidate.DeleteMarkerVersionIDs))
	for _, versionID := range candidate.DeleteMarkerVersionIDs {
		markerIDs[versionID] = struct{}{}
	}
	seen := make(map[string]struct{}, len(topology.Versions)+len(topology.DeleteMarkers))
	for _, version := range topology.Versions {
		if !isImmutableVersionID(version.VersionID) {
			return false
		}
		if _, ok := versionIDs[version.VersionID]; !ok {
			return false
		}
		if _, duplicate := seen[version.VersionID]; duplicate {
			return false
		}
		seen[version.VersionID] = struct{}{}
		if version.VersionID == candidate.VersionID &&
			(version.ETag != candidate.SingleBlockETag || version.Bytes != candidate.SingleBlockBytes) {
			return false
		}
	}
	for _, marker := range topology.DeleteMarkers {
		if !isImmutableVersionID(marker.VersionID) {
			return false
		}
		if _, ok := markerIDs[marker.VersionID]; !ok {
			return false
		}
		if _, duplicate := seen[marker.VersionID]; duplicate {
			return false
		}
		seen[marker.VersionID] = struct{}{}
	}
	return true
}

func pinSingleBlockTopology(topology ObjectVersionTopology) (ObjectVersion, []string, []string, string) {
	if len(topology.Versions) == 0 {
		return ObjectVersion{}, nil, nil, SkipUnsafeSingleBlockVersionTopology
	}
	seen := make(map[string]struct{}, len(topology.Versions)+len(topology.DeleteMarkers))
	latestVersion := -1
	latestEntries := 0
	for i, version := range topology.Versions {
		if !isImmutableVersionID(version.VersionID) {
			return ObjectVersion{}, nil, nil, SkipSingleBlockVersionIDUnavailable
		}
		if _, duplicate := seen[version.VersionID]; duplicate {
			return ObjectVersion{}, nil, nil, SkipUnsafeSingleBlockVersionTopology
		}
		seen[version.VersionID] = struct{}{}
		if version.IsLatest {
			latestEntries++
			latestVersion = i
		}
	}
	for _, marker := range topology.DeleteMarkers {
		if !isImmutableVersionID(marker.VersionID) {
			return ObjectVersion{}, nil, nil, SkipSingleBlockVersionIDUnavailable
		}
		if _, duplicate := seen[marker.VersionID]; duplicate {
			return ObjectVersion{}, nil, nil, SkipUnsafeSingleBlockVersionTopology
		}
		seen[marker.VersionID] = struct{}{}
		if marker.IsLatest {
			latestEntries++
		}
	}
	if latestEntries != 1 {
		return ObjectVersion{}, nil, nil, SkipUnsafeSingleBlockVersionTopology
	}
	if latestVersion < 0 {
		// ListObjectVersions returns data versions newest first. When a delete
		// marker is current, the first data version is the last readable payload.
		latestVersion = 0
	}
	selected := topology.Versions[latestVersion]
	if selected.ETag == "" || selected.Bytes == 0 {
		return ObjectVersion{}, nil, nil, SkipSingleBlockVersionIDUnavailable
	}

	versionIDs := make([]string, 0, len(topology.Versions))
	versionIDs = append(versionIDs, selected.VersionID)
	remainder := make([]string, 0, len(topology.Versions)-1)
	for i, version := range topology.Versions {
		if i != latestVersion {
			remainder = append(remainder, version.VersionID)
		}
	}
	sort.Strings(remainder)
	versionIDs = append(versionIDs, remainder...)
	markerIDs := make([]string, 0, len(topology.DeleteMarkers))
	for _, marker := range topology.DeleteMarkers {
		markerIDs = append(markerIDs, marker.VersionID)
	}
	sort.Strings(markerIDs)
	return selected, versionIDs, markerIDs, ""
}

func validPinnedVersionIDs(validationVersionID string, versionIDs []string, markerIDs []string) bool {
	if len(versionIDs) == 0 || versionIDs[0] != validationVersionID || !isImmutableVersionID(validationVersionID) ||
		!sort.StringsAreSorted(versionIDs[1:]) || !sort.StringsAreSorted(markerIDs) {
		return false
	}
	seen := make(map[string]struct{}, len(versionIDs)+len(markerIDs))
	for _, versionID := range versionIDs {
		if !isImmutableVersionID(versionID) {
			return false
		}
		if _, duplicate := seen[versionID]; duplicate {
			return false
		}
		seen[versionID] = struct{}{}
	}
	for _, markerID := range markerIDs {
		if !isImmutableVersionID(markerID) {
			return false
		}
		if _, duplicate := seen[markerID]; duplicate {
			return false
		}
		seen[markerID] = struct{}{}
	}
	return true
}

func isImmutableVersionID(versionID string) bool {
	return versionID != "" && versionID != s3MutableNullVersionID
}

func manifestFromCandidate(candidate Candidate, preparedAt time.Time) RetirementManifest {
	return RetirementManifest{
		BlockMetadataID:                   candidate.BlockMetadataID,
		Tag:                               candidate.Tag,
		Height:                            candidate.Height,
		Hash:                              candidate.Hash,
		State:                             RetirementStateEligible,
		Bucket:                            candidate.Bucket,
		SingleBlockObjectKey:              candidate.Key,
		SingleBlockObjectKeySHA256:        keySHA256(candidate.Key),
		SingleBlockObjectVersionIDs:       append([]string(nil), candidate.VersionIDs...),
		SingleBlockDeleteMarkerVersionIDs: append([]string(nil), candidate.DeleteMarkerVersionIDs...),
		SingleBlockObjectETag:             candidate.SingleBlockETag,
		SingleBlockObjectBytes:            candidate.SingleBlockBytes,
		ConsolidatedObjectKey:             candidate.ConsolidatedKey,
		ConsolidatedObjectVersionID:       candidate.CSCBVersionID,
		ConsolidatedObjectETag:            candidate.CSCBETag,
		ConsolidatedByteOffset:            candidate.ByteOffset,
		ConsolidatedByteLength:            candidate.ByteLength,
		ConsolidatedUncompressedLength:    candidate.UncompressedLength,
		PayloadSHA256:                     candidate.PayloadSHA256,
		PreparedAt:                        preparedAt,
	}
}

func candidateFromManifest(manifest RetirementManifest) Candidate {
	candidate := Candidate{
		Bucket:                 manifest.Bucket,
		Key:                    manifest.SingleBlockObjectKey,
		VersionID:              firstString(manifest.SingleBlockObjectVersionIDs),
		VersionIDs:             append([]string(nil), manifest.SingleBlockObjectVersionIDs...),
		DeleteMarkerVersionIDs: append([]string(nil), manifest.SingleBlockDeleteMarkerVersionIDs...),
		BlockMetadataID:        manifest.BlockMetadataID,
		Tag:                    manifest.Tag,
		Height:                 manifest.Height,
		Hash:                   manifest.Hash,
		SingleBlockBytes:       manifest.SingleBlockObjectBytes,
		SingleBlockETag:        manifest.SingleBlockObjectETag,
		SingleBlockKeySHA256:   manifest.SingleBlockObjectKeySHA256,
		SingleBlockVersions:    len(manifest.SingleBlockObjectVersionIDs),
		DeleteMarkers:          len(manifest.SingleBlockDeleteMarkerVersionIDs),
		ConsolidatedKey:        manifest.ConsolidatedObjectKey,
		CSCBVersionID:          manifest.ConsolidatedObjectVersionID,
		CSCBETag:               manifest.ConsolidatedObjectETag,
		ByteOffset:             manifest.ConsolidatedByteOffset,
		ByteLength:             manifest.ConsolidatedByteLength,
		UncompressedLength:     manifest.ConsolidatedUncompressedLength,
		PayloadSHA256:          manifest.PayloadSHA256,
		RetirementState:        manifest.State,
		RetirementAttempts:     manifest.AttemptCount,
		RetirementOutcome:      manifest.Outcome,
		SingleBlockDeletedAt:   manifest.DeletedAt,
		RetirementVerifiedAt:   manifest.VerifiedAt,
		Action:                 ActionDeleteObjectVersion,
	}
	if manifest.State == RetirementStateDeletedPendingVerification {
		candidate.Action = ActionDeletedObjectVersion
	}
	return candidate
}

func markCandidateBlocked(candidate *Candidate, reason string) {
	if candidate.RetirementState == RetirementStateDeletedPendingVerification {
		candidate.Action = ActionDeletedObjectVersion
	} else {
		candidate.Action = ActionSkip
	}
	candidate.SkipReason = reason
}

func newRetirementClaimToken() (string, error) {
	value := make([]byte, retirementClaimTokenBytes)
	if _, err := cryptorand.Read(value); err != nil {
		return "", xerrors.Errorf("failed to generate retirement claim token: %w", err)
	}
	return hex.EncodeToString(value), nil
}

func keySHA256(key string) string {
	digest := sha256.Sum256([]byte(key))
	return hex.EncodeToString(digest[:])
}

func isSHA256Hex(value string) bool {
	if len(value) != sha256HexLength {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

func firstString(values []string) string {
	if len(values) == 0 {
		return ""
	}
	return values[0]
}

func newReport(req PlanRequest) *Report {
	return &Report{
		GeneratedAt: req.Now,
		DryRun:      !req.Execute,
		Environment: req.Environment,
		Blockchain:  req.Blockchain,
		Network:     req.Network,
		Sidechain:   req.Sidechain,
		Bucket:      req.Bucket,
		Tag:         req.Tag,
		StartHeight: req.StartHeight,
		EndHeight:   req.EndHeight,
		Approval:    req.Approval,
		SafetyGates: SafetyGates{
			DirectStorageClientsGuarded: req.DirectStorageClientsGuarded,
			SingleBlockWritersGuarded:   req.SingleBlockWritersGuarded,
			FallbackReadErrors:          req.FallbackErrorCount,
			VersionedDeleteMode:         versionedDeleteMode,
			CSCBWriteOncePolicyMode:     cscbWriteOncePolicyMode,
			ProductionDeleteEnabled:     req.ProductionDeleteEnabled,
		},
		Items: make([]Candidate, 0),
	}
}

func writeOncePolicyGateVerified(items []Candidate) bool {
	eligible := false
	for _, item := range items {
		if item.Action != ActionReportOnly && item.Action != ActionDeleteObjectVersion &&
			item.Action != ActionDeletedObjectVersion && item.Action != ActionDeletedVerified {
			continue
		}
		eligible = true
		if !item.CSCBWriteOncePolicy {
			return false
		}
	}
	return eligible
}

func normalizeRequest(req *PlanRequest) error {
	if req == nil {
		return xerrors.New("request is required")
	}
	if req.EndHeight <= req.StartHeight {
		return xerrors.Errorf("end height must be greater than start height: start=%d end=%d", req.StartHeight, req.EndHeight)
	}
	if req.Bucket == "" {
		return xerrors.New("bucket is required")
	}
	if req.Now.IsZero() {
		req.Now = time.Now().UTC()
	}
	if req.EligibilityCutoff.IsZero() {
		req.EligibilityCutoff = req.Now
	}
	return nil
}

func validShadowReference(row MetadataRow, shadow *ConsolidationShadow) bool {
	if shadow == nil {
		return false
	}
	if shadow.Tag != row.Tag || shadow.Height != row.Height || shadow.Hash != row.Hash {
		return false
	}
	if shadow.SingleBlockObjectKey != row.SingleBlockObjectKey {
		return false
	}
	if shadow.ConsolidatedStorageGeneration != row.PrimaryStorageGeneration {
		return false
	}
	if !isPrimaryConsolidated(row) {
		return false
	}
	if row.PrimaryObjectKey != shadow.ConsolidatedObjectKey ||
		row.PrimaryObjectFormat != shadow.ObjectFormat ||
		row.PrimaryByteOffset != shadow.ByteOffset ||
		row.PrimaryByteLength != shadow.ByteLength ||
		row.PrimaryUncompressedLength != shadow.UncompressedLength {
		return false
	}
	if shadow.ConsolidatedObjectKey == "" {
		return false
	}
	if shadow.ObjectFormat != api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_CSCB_BATCH {
		return false
	}
	return shadow.ByteLength != 0
}

func cscbLogicalPayloadBytes(head ObjectHead) (uint64, bool) {
	format, ok := objectMetadataValue(head.Metadata, cscbFormatMetadataKey)
	if !ok || !strings.EqualFold(strings.TrimSpace(format), cscbFormatMetadataValue) {
		return 0, false
	}
	scope, ok := objectMetadataValue(head.Metadata, cscbCompressionScopeMetadataKey)
	if !ok || !strings.EqualFold(strings.TrimSpace(scope), cscbCompressionScopeMetadataValue) {
		return 0, false
	}
	value, ok := objectMetadataValue(head.Metadata, cscbUncompressedLengthMetadataKey)
	if !ok {
		return 0, false
	}
	logicalBytes, err := strconv.ParseUint(strings.TrimSpace(value), 10, 64)
	if err != nil || logicalBytes == 0 {
		return 0, false
	}
	return logicalBytes, true
}

func objectMetadataValue(metadata map[string]string, name string) (string, bool) {
	value, ok := metadata[name]
	if ok {
		return value, true
	}
	for key, candidate := range metadata {
		if strings.EqualFold(key, name) {
			return candidate, true
		}
	}
	return "", false
}

func isPrimaryConsolidated(row MetadataRow) bool {
	return row.PrimaryObjectFormat == api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_CSCB_BATCH &&
		row.PrimaryObjectKey != "" &&
		row.PrimaryByteLength > 0
}

func approvalMatches(req PlanRequest) bool {
	if normalizeApprovalChain(req.Approval.Chain) != normalizeApprovalChain(actualChain(req)) {
		return false
	}
	if req.Approval.AllowContainingRange {
		return req.Approval.StartHeight <= req.StartHeight &&
			req.Approval.EndHeight >= req.EndHeight
	}
	return req.Approval.StartHeight == req.StartHeight &&
		req.Approval.EndHeight == req.EndHeight
}

// approvalGateBlocked reports whether the operator approval blocks this
// request. Direct planner callers require an exact chain/range approval. The
// retention workflow may explicitly opt into a containing envelope after it
// independently validates the selected cohort against that operator input. A
// read-only run may omit the approval entirely so it can still reach the S3
// safety inspection, but a supplied approval is validated even on dry runs so
// the report predicts the execute-time gate.
func approvalGateBlocked(req PlanRequest) bool {
	if !req.Execute && req.Approval == (Approval{}) {
		return false
	}
	return !approvalMatches(req)
}

func actualChain(req PlanRequest) string {
	parts := []string{req.Blockchain, req.Network}
	if req.Sidechain != "" {
		parts = append(parts, req.Sidechain)
	}
	return strings.Join(parts, "-")
}

func normalizeApprovalChain(value string) string {
	return strings.ToLower(strings.ReplaceAll(value, "_", "-"))
}

// NormalizeApprovalChain exposes the approval chain normalization so callers
// can fail fast on an operator approval that names the wrong chain before a
// plan request is even built.
func NormalizeApprovalChain(value string) string {
	return normalizeApprovalChain(value)
}

func isProduction(env string) bool {
	env = strings.ToLower(env)
	return env == "production" || env == "prod"
}

func summarize(items []Candidate) Summary {
	var summary Summary
	summary.TotalRows = len(items)
	for _, item := range items {
		summary.SingleBlockBytes += item.SingleBlockBytes
		if item.DeleteMarkers > 0 || item.SkipReason == SkipSingleBlockCurrentDeleteMarker {
			summary.DeleteMarkerRows++
		}
		switch item.Action {
		case ActionReportOnly, ActionDeleteObjectVersion:
			summary.EligibleRows++
			summary.EligibleBytes += item.SingleBlockBytes
		case ActionDeletedObjectVersion, ActionDeletedVerified, ActionAlreadyDeleted:
			summary.DeletedRows++
		default:
			summary.SkippedRows++
		}
	}
	return summary
}
