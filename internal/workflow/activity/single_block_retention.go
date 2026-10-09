package activity

import (
	"context"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	sdkactivity "go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/workflow"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/cadence"
	"github.com/coinbase/chainstorage/internal/config"
	chains3 "github.com/coinbase/chainstorage/internal/s3"
	metapostgres "github.com/coinbase/chainstorage/internal/storage/metastorage/postgres"
	"github.com/coinbase/chainstorage/internal/storage/retirement"
	"github.com/coinbase/chainstorage/internal/utils/fxparams"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

type (
	SingleBlockRetention struct {
		selectActivity  baseActivity
		processActivity baseActivity
		config          *config.Config
		s3Client        chains3.Client
		componentsMu    sync.Mutex
		selector        *retirement.Selector
		planner         *retirement.Planner
	}

	SingleBlockRetentionParams struct {
		fx.In
		fxparams.Params
		Runtime  cadence.Runtime
		S3Client chains3.Client `optional:"true"`
	}

	SingleBlockRetentionSelectRequest struct {
		Tag         uint32
		StartHeight uint64
		EndHeight   uint64
		// A zero cutoff is accepted only for activity payloads scheduled by the
		// pre-sweep workflow and is resolved to activity start time.
		EligibilityCutoff time.Time
		Limit             int `validate:"required,gt=0,lte=250"`
	}

	SingleBlockRetentionSelectResponse struct {
		Cohorts []retirement.RetentionCohort `json:"cohorts"`
		HasMore bool                         `json:"has_more"`
	}

	SingleBlockRetentionProcessRequest struct {
		Tag    uint32
		Cohort retirement.RetentionCohort
		// A zero cutoff preserves compatibility with activity payloads
		// scheduled before range sweeps froze their destructive set.
		EligibilityCutoff           time.Time
		Execute                     bool
		ProductionDeleteEnabled     bool
		DirectStorageClientsGuarded bool
		SingleBlockWritersGuarded   bool
		FallbackReadsValidated      bool
		FallbackErrorCount          uint64
		// Approved* carry the operator's explicit deletion envelope verbatim.
		// They are never derived from the selected cohort; execution fails
		// closed unless they name this chain and contain the cohort.
		ApprovedChain       string
		ApprovedStartHeight uint64
		ApprovedEndHeight   uint64
	}

	SingleBlockRetentionReasonCount struct {
		Reason string `json:"reason"`
		Rows   uint64 `json:"rows"`
	}

	SingleBlockRetentionRangeResult struct {
		Cohort retirement.RetentionCohort `json:"cohort"`
		// QuiescenceRetryAfter carries the longest remaining safety-quiescence
		// wait the planner observed, so a deferred cohort retries when the
		// window actually elapses instead of a full period later.
		QuiescenceRetryAfter     time.Duration                     `json:"quiescence_retry_after,omitempty"`
		ScannedRows              uint64                            `json:"scanned_rows"`
		PlannedRows              uint64                            `json:"planned_rows"`
		DeletedVerifiedRows      uint64                            `json:"deleted_verified_rows"`
		AlreadyRetiredRows       uint64                            `json:"already_retired_rows"`
		SkippedSlots             uint64                            `json:"skipped_slots"`
		DeferredRows             uint64                            `json:"deferred_rows"`
		FailedRows               uint64                            `json:"failed_rows"`
		DeletedVersions          uint64                            `json:"deleted_versions"`
		DeletedMarkers           uint64                            `json:"deleted_markers"`
		RetiredBytes             uint64                            `json:"retired_bytes"`
		VerifiedThroughExclusive uint64                            `json:"verified_through_exclusive"`
		FirstIncompleteHeight    *uint64                           `json:"first_incomplete_height,omitempty"`
		Terminal                 bool                              `json:"terminal"`
		RetryAfter               time.Duration                     `json:"retry_after,omitempty"`
		RetryReason              string                            `json:"retry_reason,omitempty"`
		FailureMessage           string                            `json:"failure_message,omitempty"`
		Blockers                 []SingleBlockRetentionReasonCount `json:"blockers,omitempty"`
	}
)

func NewSingleBlockRetention(params SingleBlockRetentionParams) *SingleBlockRetention {
	a := &SingleBlockRetention{
		selectActivity:  newBaseActivity(ActivitySingleBlockRetentionSelect, params.Runtime),
		processActivity: newBaseActivity(ActivitySingleBlockRetentionProcess, params.Runtime),
		config:          params.Config,
		s3Client:        params.S3Client,
	}
	a.selectActivity.register(a.executeSelect)
	a.processActivity.register(a.executeProcess)
	return a
}

func (a *SingleBlockRetention) Select(
	ctx workflow.Context,
	request *SingleBlockRetentionSelectRequest,
) (*SingleBlockRetentionSelectResponse, error) {
	var response SingleBlockRetentionSelectResponse
	err := a.selectActivity.executeActivity(ctx, request, &response)
	return &response, err
}

func (a *SingleBlockRetention) Process(
	ctx workflow.Context,
	request *SingleBlockRetentionProcessRequest,
) (*SingleBlockRetentionRangeResult, error) {
	var response SingleBlockRetentionRangeResult
	err := a.processActivity.executeActivity(ctx, request, &response)
	return &response, err
}

func (a *SingleBlockRetention) executeSelect(
	ctx context.Context,
	request *SingleBlockRetentionSelectRequest,
) (*SingleBlockRetentionSelectResponse, error) {
	if err := a.selectActivity.validateRequest(request); err != nil {
		return nil, err
	}
	selector, planner, err := a.getComponents(ctx)
	if err != nil {
		return nil, err
	}
	tag := a.config.GetEffectiveBlockTag(request.Tag)
	eligibilityCutoff := resolveSingleBlockRetentionEligibilityCutoff(
		request.EligibilityCutoff,
		time.Now(),
	)
	bucket, err := a.config.WriteBlockStorageBucket()
	if err != nil {
		return nil, xerrors.Errorf("failed to resolve write block storage bucket: %w", err)
	}
	storageGeneration, err := a.config.WriteBlockStorageGeneration()
	if err != nil {
		return nil, xerrors.Errorf("failed to resolve write block storage generation: %w", err)
	}
	sdkactivity.RecordHeartbeat(ctx, "single_block_retention.select.started", tag, request.Limit)
	// The sweep selects inside a window the cron already approved and does not
	// advance a search cursor of its own; a budget-truncated selection simply
	// does less work this run and the next run re-selects.
	cohorts, hasMore, _, err := selector.Select(
		ctx,
		bucket,
		storageGeneration,
		tag,
		request.StartHeight,
		request.EndHeight,
		eligibilityCutoff,
		request.Limit,
		retirement.DueCohortCursor{},
	)
	if err != nil {
		return nil, err
	}
	a.selectActivity.getLogger(ctx).Info(
		"selected single-block retention cohorts",
		zap.Uint32("tag", tag),
		zap.Uint64("start_height", request.StartHeight),
		zap.Uint64("end_height", request.EndHeight),
		zap.Time("eligibility_cutoff", eligibilityCutoff),
		zap.String("bucket", bucket),
		zap.String("storage_generation", storageGeneration),
		zap.Int("cohorts", len(cohorts)),
		zap.Bool("has_more", hasMore),
		zap.Int("limit", request.Limit),
	)
	a.primeRetentionSafety(ctx, selector, planner, primeRetentionSafetyRequest{
		bucket:            bucket,
		storageGeneration: storageGeneration,
		tag:               tag,
		startHeight:       request.StartHeight,
		endHeight:         request.EndHeight,
		eligibilityCutoff: eligibilityCutoff,
		limit:             request.Limit,
	})
	sdkactivity.RecordHeartbeat(ctx, "single_block_retention.select.completed", tag, len(cohorts))
	return &SingleBlockRetentionSelectResponse{
		Cohorts: cohorts,
		HasMore: hasMore,
	}, nil
}

type primeRetentionSafetyRequest struct {
	bucket            string
	storageGeneration string
	tag               uint32
	startHeight       uint64
	endHeight         uint64
	eligibilityCutoff time.Time
	limit             int
}

// retentionPrimingLookaheadWindow extends the priming cutoff beyond the frozen
// selection cutoff so cohorts becoming due shortly after this run also have
// their quiescence clocks running before they are first selected.
const retentionPrimingLookaheadWindow = 2 * time.Hour

// primeRetentionSafety starts the safety-quiescence clocks for the cohorts this
// run and its successors will process. Priming is advisory: failures are logged
// and never fail the selection, and execution still re-verifies live safety
// configuration before deleting.
func (a *SingleBlockRetention) primeRetentionSafety(
	ctx context.Context,
	selector *retirement.Selector,
	planner *retirement.Planner,
	request primeRetentionSafetyRequest,
) {
	logger := a.selectActivity.getLogger(ctx)
	lookahead := request.limit * 2
	if lookahead > retirement.MaxRetentionPrimingLookahead {
		lookahead = retirement.MaxRetentionPrimingLookahead
	}
	if lookahead <= 0 {
		return
	}
	keys, err := selector.LookaheadKeys(
		ctx,
		request.bucket,
		request.storageGeneration,
		request.tag,
		request.startHeight,
		request.endHeight,
		request.eligibilityCutoff.Add(retentionPrimingLookaheadWindow),
		lookahead,
	)
	if err != nil {
		logger.Warn("failed to look ahead for retention safety priming", zap.Error(err))
		return
	}
	primed, err := planner.PrimeRetentionSafety(ctx, request.bucket, keys)
	if err != nil {
		logger.Warn(
			"failed to prime retention safety observations",
			zap.Int("primed", primed),
			zap.Int("keys", len(keys)),
			zap.Error(err),
		)
		return
	}
	logger.Info(
		"primed retention safety observations",
		zap.Int("primed", primed),
		zap.Int("keys", len(keys)),
	)
}

// watchWorkerStop derives a context that is canceled when the Temporal
// worker begins stopping — a pod terminating — so the planner unwinds inside
// the worker's stop timeout and releases the row claims it holds, instead of
// dying with them and leaving the rows leased for up to 15 minutes against
// the attempt Temporal reschedules within seconds (INF-1603). stopped reports
// whether that happened; the activity turns it into an error rather than a
// result so the cohort is retried on a live worker at once instead of being
// recorded as a failed sweep.
func watchWorkerStop(ctx context.Context) (context.Context, func() bool, context.CancelFunc) {
	if !sdkactivity.IsActivity(ctx) {
		return ctx, func() bool { return false }, func() {}
	}
	return watchStopChannel(ctx, sdkactivity.GetWorkerStopChannel(ctx))
}

func watchStopChannel(ctx context.Context, stopCh <-chan struct{}) (context.Context, func() bool, context.CancelFunc) {
	ctx, cancel := context.WithCancel(ctx)
	var stopped atomic.Bool
	go func() {
		select {
		case <-stopCh:
			stopped.Store(true)
			cancel()
		case <-ctx.Done():
		}
	}()
	return ctx, stopped.Load, cancel
}

func workerStopError(ctx context.Context, err error) error {
	return xerrors.Errorf("single-block retention activity interrupted by worker stop (%v): %w", ctx.Err(), err)
}

func (a *SingleBlockRetention) executeProcess(
	ctx context.Context,
	request *SingleBlockRetentionProcessRequest,
) (*SingleBlockRetentionRangeResult, error) {
	if err := a.processActivity.validateRequest(request); err != nil {
		return nil, err
	}
	if err := validateSingleBlockRetentionProcessRequest(a.config, request); err != nil {
		return nil, err
	}
	ctx, stopped, cancel := watchWorkerStop(ctx)
	defer cancel()
	_, planner, err := a.getComponents(ctx)
	if err != nil {
		return nil, err
	}

	req, err := a.planRequest(request)
	if err != nil {
		return nil, err
	}
	logger := a.processActivity.getLogger(ctx).With(
		zap.Uint32("tag", req.Tag),
		zap.String("consolidated_object_key", request.Cohort.ConsolidatedObjectKey),
		zap.Uint64("start_height", req.StartHeight),
		zap.Uint64("end_height", req.EndHeight),
		zap.Bool("execute", req.Execute),
	)
	sdkactivity.RecordHeartbeat(
		ctx,
		"single_block_retention.process.started",
		req.Tag,
		req.StartHeight,
		req.EndHeight,
		req.Execute,
	)

	if request.Execute {
		reconcileReport, reconcileErr := planner.Reconcile(ctx, req)
		if reconcileErr != nil && stopped() {
			return nil, workerStopError(ctx, reconcileErr)
		}
		var reconcileResult *SingleBlockRetentionRangeResult
		if reconcileReport != nil {
			reconcileResult, err = summarizeSingleBlockRetentionReport(request.Cohort, reconcileReport)
			if err != nil {
				return nil, err
			}
			if setSingleBlockRetentionRetry(reconcileResult) {
				return reconcileResult, nil
			}
		}
		if reconcileErr != nil {
			if reconcileResult == nil {
				return nil, xerrors.Errorf("failed to reconcile retention cohort: %w", reconcileErr)
			}
			reconcileResult.FailureMessage = reconcileErr.Error()
			logger.Error("failed to reconcile single-block retention cohort", zap.Error(reconcileErr))
			return reconcileResult, nil
		}
	}

	report, err := planner.Plan(ctx, req)
	if err != nil {
		if stopped() {
			return nil, workerStopError(ctx, err)
		}
		return nil, xerrors.Errorf("failed to plan retention cohort: %w", err)
	}
	if request.Execute {
		if err := validateSingleBlockRetentionExecutionPlan(request.Cohort, report); err != nil {
			return nil, err
		}
		if applyErr := planner.Apply(ctx, req, report); applyErr != nil {
			if stopped() {
				return nil, workerStopError(ctx, applyErr)
			}
			result, summaryErr := summarizeSingleBlockRetentionReport(request.Cohort, report)
			if summaryErr != nil {
				return nil, summaryErr
			}
			if setSingleBlockRetentionRetry(result) {
				return result, nil
			}
			result.FailureMessage = applyErr.Error()
			logger.Error("failed to apply single-block retention cohort", zap.Error(applyErr))
			return result, nil
		}
	}

	result, err := summarizeSingleBlockRetentionReport(request.Cohort, report)
	if err != nil {
		return nil, err
	}
	if request.Execute && setSingleBlockRetentionRetry(result) {
		return result, nil
	}
	// Every bucket the row accounting classifies into, in the order
	// validateSingleBlockRetentionResult subtracts them, so the line reconciles
	// by eye: scanned_rows == planned + deleted_verified + already_retired +
	// skipped_slots + deferred + failed. Omitting planned_rows and skipped_slots
	// made a correct cohort look broken: a solana sweep logging
	// scanned_rows=1000 deleted_verified_rows=996 with every other logged bucket
	// at zero reads as four silently undeleted rows, when they are skipped slots
	// that never had a block to delete; and a dry run (execute=false) logs
	// deleted_verified_rows=0 with its whole result parked in planned_rows.
	logger.Info(
		"processed single-block retention cohort",
		zap.Uint64("scanned_rows", result.ScannedRows),
		zap.Uint64("planned_rows", result.PlannedRows),
		zap.Uint64("deleted_verified_rows", result.DeletedVerifiedRows),
		zap.Uint64("already_retired_rows", result.AlreadyRetiredRows),
		zap.Uint64("skipped_slots", result.SkippedSlots),
		zap.Uint64("deferred_rows", result.DeferredRows),
		zap.Uint64("failed_rows", result.FailedRows),
		zap.Bool("terminal", result.Terminal),
	)
	sdkactivity.RecordHeartbeat(
		ctx,
		"single_block_retention.process.completed",
		req.Tag,
		req.StartHeight,
		req.EndHeight,
		result.DeletedVerifiedRows,
		result.Terminal,
	)
	return result, nil
}

func (a *SingleBlockRetention) getComponents(
	ctx context.Context,
) (*retirement.Selector, *retirement.Planner, error) {
	a.componentsMu.Lock()
	defer a.componentsMu.Unlock()
	if a.selector != nil && a.planner != nil {
		return a.selector, a.planner, nil
	}
	if a.config.StorageType.MetaStorageType != config.MetaStorageType_POSTGRES || a.config.AWS.Postgres == nil {
		return nil, nil, xerrors.New("single-block retention workflow requires Postgres meta storage")
	}
	if a.config.StorageType.BlobStorageType != config.BlobStorageType_UNSPECIFIED &&
		a.config.StorageType.BlobStorageType != config.BlobStorageType_S3 {
		return nil, nil, xerrors.New("single-block retention workflow requires S3 blob storage")
	}
	if a.s3Client == nil {
		return nil, nil, xerrors.New("single-block retention workflow requires an S3 client")
	}
	pool, err := metapostgres.GetConnectionPool(ctx, a.config.AWS.Postgres)
	if err != nil {
		return nil, nil, xerrors.Errorf("failed to get single-block retention Postgres pool: %w", err)
	}
	db := pool.DB()
	if db == nil {
		return nil, nil, xerrors.New("single-block retention Postgres pool returned a nil database")
	}
	repo := retirement.NewPostgresRepository(db)
	a.selector = retirement.NewSelector(repo)
	// Heartbeat from the planner's row/version loops so the configured
	// activity heartbeat timeout bounds destructive work and Temporal can
	// deliver cancellation between steps instead of only at the end.
	a.planner = retirement.NewPlanner(
		repo,
		retirement.NewS3ObjectStore(a.s3Client),
		retirement.WithHeartbeat(func(ctx context.Context, details ...any) {
			sdkactivity.RecordHeartbeat(ctx, details...)
		}),
		retirement.WithRowParallelism(a.config.Workflows.SingleBlockRetention.RowParallelism),
	)
	return a.selector, a.planner, nil
}

func (a *SingleBlockRetention) planRequest(request *SingleBlockRetentionProcessRequest) (retirement.PlanRequest, error) {
	blockchain, network, sidechain := singleBlockRetentionChainNames(a.config)
	tag := a.config.GetEffectiveBlockTag(request.Tag)
	now := time.Now().UTC()
	eligibilityCutoff := resolveSingleBlockRetentionEligibilityCutoff(request.EligibilityCutoff, now)
	approval := retirement.Approval{
		Chain:       request.ApprovedChain,
		StartHeight: request.ApprovedStartHeight,
		EndHeight:   request.ApprovedEndHeight,
	}
	if request.ApprovedChain != "" {
		approval.AllowContainingRange = true
	}
	bucket, err := a.config.WriteBlockStorageBucket()
	if err != nil {
		return retirement.PlanRequest{}, xerrors.Errorf("failed to resolve write block storage bucket: %w", err)
	}
	storageGeneration, err := a.config.WriteBlockStorageGeneration()
	if err != nil {
		return retirement.PlanRequest{}, xerrors.Errorf("failed to resolve write block storage generation: %w", err)
	}
	return retirement.PlanRequest{
		Environment:                 string(a.config.Env()),
		Blockchain:                  blockchain,
		Network:                     network,
		Sidechain:                   sidechain,
		Bucket:                      bucket,
		StorageGeneration:           storageGeneration,
		Tag:                         tag,
		StartHeight:                 request.Cohort.StartHeight,
		EndHeight:                   request.Cohort.EndHeight,
		Now:                         now,
		EligibilityCutoff:           eligibilityCutoff,
		Execute:                     request.Execute,
		ProductionDeleteEnabled:     request.ProductionDeleteEnabled,
		DirectStorageClientsGuarded: request.DirectStorageClientsGuarded,
		SingleBlockWritersGuarded:   request.SingleBlockWritersGuarded,
		FallbackErrorCount:          request.FallbackErrorCount,
		// The budget lets the planner lower row parallelism on chains whose
		// decompressed CSCB chunks are large.
		RowMemoryBudgetBytes: a.config.Workflows.SingleBlockRetention.RowMemoryBudgetBytes,
		// The approval is the operator's assertion passed through unchanged.
		// The planner independently re-verifies the actual chain and requires
		// the cohort under deletion to be contained by this immutable envelope.
		Approval: approval,
	}, nil
}

func resolveSingleBlockRetentionEligibilityCutoff(cutoff time.Time, now time.Time) time.Time {
	if cutoff.IsZero() {
		return now.UTC()
	}
	return cutoff.UTC()
}

func singleBlockRetentionExpectedChain(cfg *config.Config) string {
	blockchain, network, sidechain := singleBlockRetentionChainNames(cfg)
	parts := []string{blockchain, network}
	if sidechain != "" {
		parts = append(parts, sidechain)
	}
	return strings.Join(parts, "-")
}

func singleBlockRetentionChainNames(cfg *config.Config) (string, string, string) {
	blockchain := cfg.Blockchain().GetName()
	network := strings.TrimPrefix(cfg.Network().GetName(), blockchain+"-")
	sidechain := ""
	if cfg.Sidechain() != api.SideChain_SIDECHAIN_NONE {
		sidechain = strings.TrimPrefix(
			cfg.Sidechain().GetName(),
			blockchain+"-"+network+"-",
		)
	}
	return blockchain, network, sidechain
}

func validateSingleBlockRetentionProcessRequest(
	cfg *config.Config,
	request *SingleBlockRetentionProcessRequest,
) error {
	if request == nil {
		return xerrors.New("single-block retention process request is required")
	}
	cohort := request.Cohort
	if cohort.ConsolidatedObjectKey == "" || cohort.EndHeight <= cohort.StartHeight ||
		cohort.RowCount == 0 || cohort.EligibleAt.IsZero() {
		return xerrors.Errorf("invalid single-block retention cohort: %+v", cohort)
	}
	if !request.Execute {
		return nil
	}
	if !request.DirectStorageClientsGuarded {
		return xerrors.New("retention execution requires direct storage clients to be guarded or out of scope")
	}
	if !request.SingleBlockWritersGuarded {
		return xerrors.New("retention execution requires every single-block writer to honor the retirement fence")
	}
	if !request.FallbackReadsValidated {
		return xerrors.New("retention execution requires explicit fallback-disabled read validation")
	}
	if request.FallbackErrorCount != 0 {
		return xerrors.Errorf("retention execution requires zero fallback read errors, got %d", request.FallbackErrorCount)
	}
	if request.ApprovedChain == "" {
		return xerrors.New("retention execution requires an explicit operator approval chain")
	}
	if cfg != nil {
		expectedChain := singleBlockRetentionExpectedChain(cfg)
		if retirement.NormalizeApprovalChain(request.ApprovedChain) != retirement.NormalizeApprovalChain(expectedChain) {
			return xerrors.Errorf(
				"retention execution approval names chain %q, expected %q",
				request.ApprovedChain,
				expectedChain,
			)
		}
	}
	if request.ApprovedStartHeight > cohort.StartHeight || request.ApprovedEndHeight < cohort.EndHeight {
		return xerrors.Errorf(
			"retention execution cohort %q [%d, %d) is outside approval envelope [%d, %d)",
			cohort.ConsolidatedObjectKey,
			cohort.StartHeight,
			cohort.EndHeight,
			request.ApprovedStartHeight,
			request.ApprovedEndHeight,
		)
	}
	if cfg != nil && isProductionRetentionEnvironment(cfg.Env()) && !request.ProductionDeleteEnabled {
		return xerrors.New("production retention execution requires explicit production-delete enablement")
	}
	return nil
}

func isProductionRetentionEnvironment(env config.Env) bool {
	value := strings.ToLower(string(env))
	return value == "production" || value == "prod"
}

func summarizeSingleBlockRetentionReport(
	cohort retirement.RetentionCohort,
	report *retirement.Report,
) (*SingleBlockRetentionRangeResult, error) {
	if report == nil {
		return nil, xerrors.New("single-block retention report is required")
	}
	if report.StartHeight != cohort.StartHeight || report.EndHeight != cohort.EndHeight {
		return nil, xerrors.Errorf(
			"retention report range [%d, %d) does not match cohort [%d, %d)",
			report.StartHeight,
			report.EndHeight,
			cohort.StartHeight,
			cohort.EndHeight,
		)
	}
	result := &SingleBlockRetentionRangeResult{
		Cohort:                   cohort,
		QuiescenceRetryAfter:     report.QuiescenceRetryAfter,
		ScannedRows:              uint64(len(report.Items)),
		VerifiedThroughExclusive: cohort.StartHeight,
	}
	reasons := make(map[string]uint64)
	items := append([]retirement.Candidate(nil), report.Items...)
	sort.Slice(items, func(i, j int) bool {
		return items[i].Height < items[j].Height
	})
	for _, item := range items {
		if item.SkipReason != retirement.SkipSkippedBlock &&
			item.ConsolidatedKey != cohort.ConsolidatedObjectKey {
			return nil, xerrors.Errorf(
				"retention report row at height %d belongs to consolidated object %q, expected %q",
				item.Height,
				item.ConsolidatedKey,
				cohort.ConsolidatedObjectKey,
			)
		}
		switch item.Action {
		case retirement.ActionReportOnly, retirement.ActionDeleteObjectVersion:
			result.PlannedRows++
		case retirement.ActionDeletedVerified:
			result.DeletedVerifiedRows++
			result.DeletedVersions += uint64(len(item.VersionIDs))
			result.DeletedMarkers += uint64(len(item.DeleteMarkerVersionIDs))
			result.RetiredBytes += item.SingleBlockBytes
		case retirement.ActionAlreadyDeleted:
			result.AlreadyRetiredRows++
		case retirement.ActionDeletedObjectVersion:
			result.DeferredRows++
		default:
			if item.SkipReason == retirement.SkipSkippedBlock {
				result.SkippedSlots++
			} else if isDeferredRetentionReason(item.SkipReason) {
				result.DeferredRows++
			} else {
				result.FailedRows++
			}
		}
		if item.SkipReason != "" && item.SkipReason != retirement.SkipRetirementAlreadyFinalized &&
			item.SkipReason != retirement.SkipSkippedBlock {
			reasons[item.SkipReason]++
		}
	}
	reasonNames := make([]string, 0, len(reasons))
	for reason := range reasons {
		reasonNames = append(reasonNames, reason)
	}
	sort.Strings(reasonNames)
	for _, reason := range reasonNames {
		result.Blockers = append(result.Blockers, SingleBlockRetentionReasonCount{
			Reason: reason,
			Rows:   reasons[reason],
		})
	}

	nextHeight := cohort.StartHeight
	for _, item := range items {
		if item.Height < nextHeight {
			return nil, xerrors.Errorf("retention report contains duplicate or out-of-order height %d", item.Height)
		}
		if item.Height > nextHeight {
			result.FirstIncompleteHeight = uint64Pointer(nextHeight)
			break
		}
		if !terminalRetentionCandidate(item) {
			result.FirstIncompleteHeight = uint64Pointer(item.Height)
			break
		}
		nextHeight++
		result.VerifiedThroughExclusive = nextHeight
	}
	if result.FirstIncompleteHeight == nil && nextHeight < cohort.EndHeight {
		result.FirstIncompleteHeight = uint64Pointer(nextHeight)
	}
	result.Terminal = result.FirstIncompleteHeight == nil &&
		result.VerifiedThroughExclusive == cohort.EndHeight &&
		result.ScannedRows > 0
	return result, nil
}

func validateSingleBlockRetentionExecutionPlan(
	cohort retirement.RetentionCohort,
	report *retirement.Report,
) error {
	if report == nil {
		return xerrors.New("single-block retention execution report is required")
	}
	if report.StartHeight != cohort.StartHeight || report.EndHeight != cohort.EndHeight {
		return xerrors.Errorf(
			"retention execution report range [%d, %d) does not match cohort [%d, %d)",
			report.StartHeight,
			report.EndHeight,
			cohort.StartHeight,
			cohort.EndHeight,
		)
	}
	items := append([]retirement.Candidate(nil), report.Items...)
	sort.Slice(items, func(i, j int) bool {
		return items[i].Height < items[j].Height
	})
	nextHeight := cohort.StartHeight
	for _, item := range items {
		if item.Height != nextHeight {
			return xerrors.Errorf(
				"retention execution plan has non-contiguous height: expected=%d actual=%d",
				nextHeight,
				item.Height,
			)
		}
		if item.SkipReason == retirement.SkipSkippedBlock {
			if item.Action != retirement.ActionSkip {
				return xerrors.Errorf(
					"skipped retention slot at height %d has invalid action %q",
					item.Height,
					item.Action,
				)
			}
		} else {
			if item.ConsolidatedKey != cohort.ConsolidatedObjectKey {
				return xerrors.Errorf(
					"retention execution plan row at height %d belongs to consolidated object %q, expected %q",
					item.Height,
					item.ConsolidatedKey,
					cohort.ConsolidatedObjectKey,
				)
			}
			if item.Action != retirement.ActionDeleteObjectVersion &&
				item.Action != retirement.ActionAlreadyDeleted {
				return xerrors.Errorf(
					"retention execution plan row at height %d is not executable: action=%q reason=%q",
					item.Height,
					item.Action,
					item.SkipReason,
				)
			}
		}
		nextHeight++
	}
	if nextHeight != cohort.EndHeight {
		return xerrors.Errorf(
			"retention execution plan does not cover cohort [%d, %d): verified through %d",
			cohort.StartHeight,
			cohort.EndHeight,
			nextHeight,
		)
	}
	return nil
}

func terminalRetentionCandidate(item retirement.Candidate) bool {
	return item.Action == retirement.ActionDeletedVerified ||
		item.Action == retirement.ActionAlreadyDeleted ||
		item.Action == retirement.ActionSkip && item.SkipReason == retirement.SkipSkippedBlock
}

func isDeferredRetentionReason(reason string) bool {
	switch reason {
	case retirement.SkipRetentionPeriodActive,
		retirement.SkipCSCBSafetyQuiescenceActive,
		retirement.SkipRetirementVerificationPending,
		retirement.SkipRetirementClaimActive:
		return true
	default:
		return false
	}
}

// retentionQuiescenceRetryGrace pads a remaining-quiescence retry so the retry
// attempt lands after the window has actually closed.
const retentionQuiescenceRetryGrace = 15 * time.Second

func setSingleBlockRetentionRetry(result *SingleBlockRetentionRangeResult) bool {
	if result == nil || result.FailedRows != 0 || result.DeferredRows == 0 || len(result.Blockers) != 1 {
		return false
	}
	switch result.Blockers[0].Reason {
	case retirement.SkipCSCBSafetyQuiescenceActive:
		result.RetryAfter = retirement.RetentionSafetyQuiescencePeriod
		if remaining := result.QuiescenceRetryAfter; remaining > 0 &&
			remaining+retentionQuiescenceRetryGrace < result.RetryAfter {
			result.RetryAfter = remaining + retentionQuiescenceRetryGrace
		}
	case retirement.SkipRetirementClaimActive:
		result.RetryAfter = retirement.RetirementClaimLease
	default:
		return false
	}
	result.RetryReason = result.Blockers[0].Reason
	return true
}

func uint64Pointer(value uint64) *uint64 {
	return &value
}
