package workflow

import (
	"context"
	"math"
	"sort"
	"strconv"
	"time"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/cadence"
	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/storage/retirement"
	"github.com/coinbase/chainstorage/internal/utils/fxparams"
	"github.com/coinbase/chainstorage/internal/workflow/activity"
)

type (
	SingleBlockRetention struct {
		baseWorkflow
		activity *activity.SingleBlockRetention
	}

	SingleBlockRetentionParams struct {
		fx.In
		fxparams.Params
		Runtime  cadence.Runtime
		Activity *activity.SingleBlockRetention
	}

	SingleBlockRetentionRequest struct {
		Tag         uint32
		StartHeight uint64
		EndHeight   uint64
		// EligibilityCutoff freezes the destructive set. Dry runs may omit it
		// and return the captured cutoff; new execute sweeps must reuse that
		// exact dry-run cutoff across every continuation.
		EligibilityCutoff time.Time
		// MaxObjectRanges bounds each workflow run. Execute runs continue as new
		// with the same approved envelope while more eligible cohorts remain.
		MaxObjectRanges int `validate:"omitempty,gt=0,lte=250"`
		// Parallelism bounds concurrent cohort lifecycles. Each lifecycle owns
		// one CSCB object, including its optional safety-quiescence retry.
		// Zero preserves the serial default for existing callers.
		Parallelism                 int `validate:"omitempty,gt=0"`
		Execute                     bool
		ProductionDeleteEnabled     bool
		DirectStorageClientsGuarded bool
		SingleBlockWritersGuarded   bool
		FallbackReadsValidated      bool
		FallbackErrorCount          uint64
		// Approved* are the operator's separate deletion approval envelope.
		// Execution requires them, requires the selection range to be fully
		// contained by the approved envelope, and passes them through unchanged
		// across every continuation. Selected cohorts must also be contained by
		// this envelope; approval is never derived from selector output. Read-only
		// runs may omit them.
		ApprovedChain       string
		ApprovedStartHeight uint64
		ApprovedEndHeight   uint64

		// Checkpoint is internal continue-as-new state. Initial workflow
		// executions reject caller-supplied checkpoints.
		Checkpoint *SingleBlockRetentionCheckpoint
	}

	SingleBlockRetentionCheckpoint struct {
		StartedAt                 time.Time
		EligibilityCutoff         time.Time
		EffectiveTag              uint32
		ContinueAsNewCount        uint64
		SelectedObjectRanges      uint64
		ProcessedObjectRanges     uint64
		CompletedObjectRangeCount uint64
		ScannedRows               uint64
		PlannedRows               uint64
		DeletedVerifiedRows       uint64
		AlreadyRetiredRows        uint64
		SkippedSlots              uint64
		DeferredRows              uint64
		FailedRows                uint64
		DeletedVersions           uint64
		DeletedMarkers            uint64
		RetiredBytes              uint64
		LastCompletedObjectRange  *SingleBlockRetentionCompletedRange
	}

	SingleBlockRetentionCompletedRange struct {
		ConsolidatedObjectKey string `json:"consolidated_object_key"`
		StartHeight           uint64 `json:"start_height"`
		EndHeight             uint64 `json:"end_height"`
		EligibleRows          uint64 `json:"eligible_rows"`
	}

	SingleBlockRetentionResult struct {
		StartedAt                 time.Time                           `json:"started_at"`
		CompletedAt               time.Time                           `json:"completed_at"`
		EligibilityCutoff         time.Time                           `json:"eligibility_cutoff"`
		Tag                       uint32                              `json:"tag"`
		SelectionStartHeight      uint64                              `json:"selection_start_height"`
		SelectionEndHeight        uint64                              `json:"selection_end_height"`
		Execute                   bool                                `json:"execute"`
		ApprovedChain             string                              `json:"approved_chain,omitempty"`
		ApprovedStartHeight       uint64                              `json:"approved_start_height,omitempty"`
		ApprovedEndHeight         uint64                              `json:"approved_end_height,omitempty"`
		FallbackReadsValidated    bool                                `json:"fallback_reads_validated"`
		FallbackErrorCount        uint64                              `json:"fallback_error_count"`
		Parallelism               int                                 `json:"parallelism"`
		ContinueAsNewCount        uint64                              `json:"continue_as_new_count"`
		SweepCompleted            bool                                `json:"sweep_completed"`
		SelectedObjectRanges      uint64                              `json:"selected_object_ranges"`
		MoreEligibleRanges        bool                                `json:"more_eligible_ranges"`
		ProcessedObjectRanges     uint64                              `json:"processed_object_ranges"`
		CompletedObjectRangeCount uint64                              `json:"completed_object_range_count"`
		LastCompletedObjectRange  *SingleBlockRetentionCompletedRange `json:"last_completed_object_range,omitempty"`
		// CompletedObjectRanges and RangeResults contain only the current
		// Temporal run. The scalar counters above are cumulative across the
		// full continue-as-new chain.
		CompletedObjectRanges []SingleBlockRetentionCompletedRange        `json:"completed_object_ranges,omitempty"`
		RangeResults          []*activity.SingleBlockRetentionRangeResult `json:"range_results,omitempty"`
		ScannedRows           uint64                                      `json:"scanned_rows"`
		PlannedRows           uint64                                      `json:"planned_rows"`
		DeletedVerifiedRows   uint64                                      `json:"deleted_verified_rows"`
		AlreadyRetiredRows    uint64                                      `json:"already_retired_rows"`
		SkippedSlots          uint64                                      `json:"skipped_slots"`
		DeferredRows          uint64                                      `json:"deferred_rows"`
		FailedRows            uint64                                      `json:"failed_rows"`
		DeletedVersions       uint64                                      `json:"deleted_versions"`
		DeletedMarkers        uint64                                      `json:"deleted_markers"`
		RetiredBytes          uint64                                      `json:"retired_bytes"`
		FailureMessage        string                                      `json:"failure_message,omitempty"`
	}

	SingleBlockRetentionFailureDetails struct {
		FailureMessage                 string                                     `json:"failure_message"`
		Parallelism                    int                                        `json:"parallelism"`
		CurrentRunSelectedObjectRanges uint64                                     `json:"current_run_selected_object_ranges"`
		CurrentRunLaunchedObjectRanges uint64                                     `json:"current_run_launched_object_ranges"`
		SelectedObjectRanges           uint64                                     `json:"selected_object_ranges"`
		ProcessedObjectRanges          uint64                                     `json:"processed_object_ranges"`
		CompletedObjectRangeCount      uint64                                     `json:"completed_object_range_count"`
		DeletedVerifiedRows            uint64                                     `json:"deleted_verified_rows"`
		FailedRows                     uint64                                     `json:"failed_rows"`
		DeferredRows                   uint64                                     `json:"deferred_rows"`
		CohortOutcomes                 []SingleBlockRetentionCohortFailureOutcome `json:"cohort_outcomes"`
	}

	SingleBlockRetentionCohortFailureOutcome struct {
		Cohort         retirement.RetentionCohort                `json:"cohort"`
		Result         *activity.SingleBlockRetentionRangeResult `json:"result,omitempty"`
		FailureMessage string                                    `json:"failure_message,omitempty"`
	}

	singleBlockRetentionCohortOutcome struct {
		cohort retirement.RetentionCohort
		result *activity.SingleBlockRetentionRangeResult
		err    error
	}
)

var _ InstrumentedRequest = (*SingleBlockRetentionRequest)(nil)

const (
	maxSingleBlockRetentionRetryDelay  = 30 * time.Minute
	singleBlockRetentionMaxParallelism = 20

	singleBlockRetentionRangeSweepChangeID  = "single_block_retention.range_sweep"
	singleBlockRetentionRangeSweepVersion   = 1
	singleBlockRetentionParallelismChangeID = "single_block_retention.parallelism"
	singleBlockRetentionParallelismVersion  = 1
	// singleBlockRetentionDeferralRetriesChangeID gates the per-cohort
	// deferral retry budget (INF-1603). Runs started before it carry
	// DefaultVersion and keep the single retry their history already
	// recorded; new runs get maxSingleBlockRetentionDeferralRetries.
	singleBlockRetentionDeferralRetriesChangeID = "single_block_retention.deferral_retries"
	singleBlockRetentionDeferralRetriesVersion  = 1
	// maxSingleBlockRetentionDeferralRetries bounds how many times a deferred
	// cohort is slept-and-retried before the sweep fails. One retry was not
	// reason-aware: a cohort deferred for the CSCB safety quiescence and then,
	// on its retry, for a retirement claim a restarted worker left behind —
	// two unrelated transient causes — failed the whole sweep. Three covers a
	// quiescence wait, a claim wait, and one more, while still bounding a
	// cohort that stays deferred to ~45 minutes of lease waits.
	maxSingleBlockRetentionDeferralRetries = 3
	singleBlockRetentionPartialFailureType = "single_block_retention_partial_failure"
)

func NewSingleBlockRetention(params SingleBlockRetentionParams) *SingleBlockRetention {
	w := &SingleBlockRetention{
		baseWorkflow: newBaseWorkflow(&params.Config.Workflows.SingleBlockRetention, params.Runtime),
		activity:     params.Activity,
	}
	w.registerWorkflow(w.execute)
	return w
}

func (w *SingleBlockRetention) Execute(
	ctx context.Context,
	request *SingleBlockRetentionRequest,
) (client.WorkflowRun, error) {
	workflowID := w.name
	if override, ok := workflowIDFromContext(ctx); ok {
		workflowID = override
	}
	return w.startWorkflow(ctx, workflowID, request)
}

func (r *SingleBlockRetentionRequest) GetTags() map[string]string {
	tag := uint32(0)
	if r != nil {
		tag = r.Tag
	}
	return map[string]string{
		tagBlockTag: strconv.Itoa(int(tag)),
	}
}

func (w *SingleBlockRetention) execute(
	ctx workflow.Context,
	request *SingleBlockRetentionRequest,
) (*SingleBlockRetentionResult, error) {
	result := newSingleBlockRetentionResult(request, workflow.Now(ctx).UTC())
	failureDetailsEnabled := false
	currentRunSelectedObjectRanges := uint64(0)
	cohortOutcomes := make([]SingleBlockRetentionCohortFailureOutcome, 0)
	err := w.executeWorkflow(ctx, request, func() error {
		var cfg config.SingleBlockRetentionWorkflowConfig
		if err := w.readConfig(ctx, &cfg); err != nil {
			return xerrors.Errorf("failed to read config: %w", err)
		}
		rangeSweepEnabled := workflow.GetVersion(
			ctx,
			singleBlockRetentionRangeSweepChangeID,
			workflow.DefaultVersion,
			singleBlockRetentionRangeSweepVersion,
		) != workflow.DefaultVersion
		if err := validateSingleBlockRetentionExecutionRequest(request, rangeSweepEnabled); err != nil {
			return err
		}
		isContinuation := workflow.GetInfo(ctx).ContinuedExecutionRunID != ""
		if request.Execute && rangeSweepEnabled && request.EligibilityCutoff.IsZero() {
			return xerrors.New("single-block retention range execution requires the eligibility cutoff from its approved dry run")
		}
		if result.EligibilityCutoff.After(workflow.Now(ctx).UTC()) {
			return xerrors.Errorf(
				"single-block retention eligibility cutoff %s is in the future",
				result.EligibilityCutoff,
			)
		}

		maxObjectRanges := cfg.MaxObjectRanges
		if request.MaxObjectRanges > 0 {
			maxObjectRanges = request.MaxObjectRanges
		}
		if maxObjectRanges <= 0 || maxObjectRanges > retirement.MaxRetentionCohortsPerWorkflow {
			return xerrors.Errorf(
				"single_block_retention max_object_ranges must be between 1 and %d: %d",
				retirement.MaxRetentionCohortsPerWorkflow,
				maxObjectRanges,
			)
		}
		parallelism := 1
		if request.Parallelism > 0 {
			parallelism = request.Parallelism
		}
		if parallelism > singleBlockRetentionMaxParallelism {
			return xerrors.Errorf(
				"single_block_retention parallelism(%d) exceeds max(%d)",
				parallelism,
				singleBlockRetentionMaxParallelism,
			)
		}
		parallelismVersion := workflow.GetVersion(
			ctx,
			singleBlockRetentionParallelismChangeID,
			workflow.DefaultVersion,
			singleBlockRetentionParallelismVersion,
		)
		failureDetailsEnabled = parallelismVersion != workflow.DefaultVersion
		deferralRetryBudget := 1
		if workflow.GetVersion(
			ctx,
			singleBlockRetentionDeferralRetriesChangeID,
			workflow.DefaultVersion,
			singleBlockRetentionDeferralRetriesVersion,
		) != workflow.DefaultVersion {
			deferralRetryBudget = maxSingleBlockRetentionDeferralRetries
		}
		if parallelismVersion == workflow.DefaultVersion && parallelism != 1 {
			return xerrors.Errorf(
				"legacy single_block_retention execution requires parallelism=1, got %d",
				parallelism,
			)
		}
		tag := cfg.GetEffectiveBlockTag(request.Tag)
		// Preserve the deployed workflow's resolved numeric tag on the legacy
		// version path so replayed activity commands remain compatible.
		activityTag := tag
		activityEligibilityCutoff := time.Time{}
		if rangeSweepEnabled {
			// Pin both values before crossing the workflow/activity boundary.
			// Workers may observe a newer stable tag during a rolling deploy.
			activityTag = encodeSingleBlockRetentionEffectiveTag(tag)
			activityEligibilityCutoff = result.EligibilityCutoff
		}
		result.Tag = tag
		result.Parallelism = parallelism
		result.SelectionStartHeight = request.StartHeight
		result.SelectionEndHeight = request.EndHeight
		if err := validateSingleBlockRetentionCheckpoint(
			request,
			isContinuation,
			rangeSweepEnabled,
			tag,
		); err != nil {
			return err
		}

		logger := w.getLogger(ctx).With(
			zap.Uint32("effective_tag", tag),
			zap.Int("max_object_ranges", maxObjectRanges),
			zap.Int("parallelism", parallelism),
			zap.Uint64("selection_start_height", request.StartHeight),
			zap.Uint64("selection_end_height", request.EndHeight),
			zap.Time("eligibility_cutoff", result.EligibilityCutoff),
			zap.Bool("execute", request.Execute),
		)
		logger.Info("single-block retention workflow started")
		activityCtx := w.withActivityOptions(ctx)
		selected, err := w.activity.Select(activityCtx, &activity.SingleBlockRetentionSelectRequest{
			Tag:               activityTag,
			StartHeight:       request.StartHeight,
			EndHeight:         request.EndHeight,
			EligibilityCutoff: activityEligibilityCutoff,
			Limit:             maxObjectRanges,
		})
		if err != nil {
			return xerrors.Errorf("failed to select retention cohorts: %w", err)
		}
		if len(selected.Cohorts) > maxObjectRanges {
			return xerrors.Errorf(
				"single-block retention selector returned %d cohorts above limit %d",
				len(selected.Cohorts),
				maxObjectRanges,
			)
		}
		if selected.HasMore && len(selected.Cohorts) == 0 {
			return xerrors.New("single-block retention selector reported a backlog without returning a cohort")
		}
		result.SelectedObjectRanges += uint64(len(selected.Cohorts))
		currentRunSelectedObjectRanges = uint64(len(selected.Cohorts))
		result.MoreEligibleRanges = selected.HasMore

		if parallelismVersion != workflow.DefaultVersion {
			if err := validateSingleBlockRetentionCohorts(
				selected.Cohorts,
				request,
				rangeSweepEnabled,
			); err != nil {
				return err
			}
		}
		if parallelismVersion != workflow.DefaultVersion && parallelism > 1 {
			outcomes, err := w.processSingleBlockRetentionCohortsParallel(
				ctx,
				activityCtx,
				logger,
				request,
				activityTag,
				activityEligibilityCutoff,
				selected.Cohorts,
				parallelism,
				rangeSweepEnabled,
				failureDetailsEnabled,
				deferralRetryBudget,
				result,
			)
			cohortOutcomes = append(cohortOutcomes, outcomes...)
			if err != nil {
				return err
			}
		} else {
			for _, cohort := range selected.Cohorts {
				rangeResult, err := w.processSingleBlockRetentionCohort(
					ctx,
					activityCtx,
					logger,
					request,
					activityTag,
					activityEligibilityCutoff,
					cohort,
					rangeSweepEnabled,
					failureDetailsEnabled,
					deferralRetryBudget,
				)
				result.addRangeResult(rangeResult)
				if failureDetailsEnabled {
					cohortOutcomes = append(
						cohortOutcomes,
						newSingleBlockRetentionCohortFailureOutcome(cohort, rangeResult, err),
					)
				}
				if err != nil {
					return err
				}
			}
		}
		if request.Execute && rangeSweepEnabled && !result.MoreEligibleRanges && len(selected.Cohorts) > 0 {
			remaining, err := w.activity.Select(
				activityCtx,
				&activity.SingleBlockRetentionSelectRequest{
					Tag:               activityTag,
					StartHeight:       request.StartHeight,
					EndHeight:         request.EndHeight,
					EligibilityCutoff: activityEligibilityCutoff,
					Limit:             1,
				},
			)
			if err != nil {
				return xerrors.Errorf("failed to confirm retention sweep completion: %w", err)
			}
			if len(remaining.Cohorts) > 1 {
				return xerrors.Errorf(
					"single-block retention completion selector returned %d cohorts above limit 1",
					len(remaining.Cohorts),
				)
			}
			if remaining.HasMore && len(remaining.Cohorts) == 0 {
				return xerrors.New("single-block retention completion selector reported a backlog without returning a cohort")
			}
			result.MoreEligibleRanges = remaining.HasMore || len(remaining.Cohorts) > 0
		}
		if request.Execute && rangeSweepEnabled && result.MoreEligibleRanges {
			nextRequest := *request
			nextRequest.Tag = encodeSingleBlockRetentionEffectiveTag(tag)
			nextRequest.Checkpoint = result.checkpoint()
			nextRequest.Checkpoint.ContinueAsNewCount++
			logger.Info(
				"single-block retention workflow continuing as new",
				zap.Uint64("continue_as_new_count", nextRequest.Checkpoint.ContinueAsNewCount),
				zap.Uint64("selected_object_ranges", result.SelectedObjectRanges),
				zap.Uint64("processed_object_ranges", result.ProcessedObjectRanges),
				zap.Uint64("completed_object_range_count", result.CompletedObjectRangeCount),
				zap.Uint64("deleted_verified_rows", result.DeletedVerifiedRows),
				zap.Uint64("retired_bytes", result.RetiredBytes),
			)
			return w.continueAsNew(ctx, &nextRequest)
		}
		result.SweepCompleted = request.Execute && !result.MoreEligibleRanges
		logger.Info(
			"single-block retention workflow completed",
			zap.Bool("sweep_completed", result.SweepCompleted),
			zap.Uint64("continue_as_new_count", result.ContinueAsNewCount),
			zap.Uint64("selected_object_ranges", result.SelectedObjectRanges),
			zap.Bool("more_eligible_ranges", result.MoreEligibleRanges),
			zap.Uint64("processed_object_ranges", result.ProcessedObjectRanges),
			zap.Uint64("completed_object_range_count", result.CompletedObjectRangeCount),
			zap.Uint64("deleted_verified_rows", result.DeletedVerifiedRows),
			zap.Uint64("retired_bytes", result.RetiredBytes),
		)
		return nil
	})
	result.CompletedAt = workflow.Now(ctx).UTC()
	if err != nil {
		result.FailureMessage = err.Error()
		if failureDetailsEnabled && len(cohortOutcomes) > 0 && !IsContinueAsNewError(err) {
			failureDetails := SingleBlockRetentionFailureDetails{
				FailureMessage:                 err.Error(),
				Parallelism:                    result.Parallelism,
				CurrentRunSelectedObjectRanges: currentRunSelectedObjectRanges,
				CurrentRunLaunchedObjectRanges: uint64(len(cohortOutcomes)),
				SelectedObjectRanges:           result.SelectedObjectRanges,
				ProcessedObjectRanges:          result.ProcessedObjectRanges,
				CompletedObjectRangeCount:      result.CompletedObjectRangeCount,
				DeletedVerifiedRows:            result.DeletedVerifiedRows,
				FailedRows:                     result.FailedRows,
				DeferredRows:                   result.DeferredRows,
				CohortOutcomes:                 cohortOutcomes,
			}
			return result, temporal.NewApplicationErrorWithOptions(
				err.Error(),
				singleBlockRetentionPartialFailureType,
				temporal.ApplicationErrorOptions{
					Cause:   err,
					Details: []interface{}{failureDetails},
				},
			)
		}
	}
	return result, err
}

func (w *SingleBlockRetention) processSingleBlockRetentionCohortsParallel(
	ctx workflow.Context,
	activityCtx workflow.Context,
	logger *zap.Logger,
	request *SingleBlockRetentionRequest,
	activityTag uint32,
	activityEligibilityCutoff time.Time,
	cohorts []retirement.RetentionCohort,
	parallelism int,
	rangeSweepEnabled bool,
	resultValidationEnabled bool,
	deferralRetryBudget int,
	result *SingleBlockRetentionResult,
) ([]SingleBlockRetentionCohortFailureOutcome, error) {
	cohortOutcomes := make([]SingleBlockRetentionCohortFailureOutcome, 0, len(cohorts))
	for start := 0; start < len(cohorts); start += parallelism {
		end := start + parallelism
		if end > len(cohorts) {
			end = len(cohorts)
		}

		futures := make([]workflow.Future, 0, end-start)
		for i := start; i < end; i++ {
			cohort := cohorts[i]
			future, settable := workflow.NewFuture(ctx)
			workflow.Go(activityCtx, func(cohortCtx workflow.Context) {
				rangeResult, err := w.processSingleBlockRetentionCohort(
					cohortCtx,
					cohortCtx,
					logger,
					request,
					activityTag,
					activityEligibilityCutoff,
					cohort,
					rangeSweepEnabled,
					resultValidationEnabled,
					deferralRetryBudget,
				)
				settable.Set(&singleBlockRetentionCohortOutcome{
					cohort: cohort,
					result: rangeResult,
					err:    err,
				}, nil)
			})
			futures = append(futures, future)
		}

		var firstErr error
		for i, future := range futures {
			cohort := cohorts[start+i]
			var outcome *singleBlockRetentionCohortOutcome
			if err := future.Get(ctx, &outcome); err != nil {
				cohortOutcomes = append(
					cohortOutcomes,
					newSingleBlockRetentionCohortFailureOutcome(cohort, nil, err),
				)
				if firstErr == nil {
					firstErr = xerrors.Errorf("failed to await parallel retention cohort: %w", err)
				}
				continue
			}
			if outcome == nil {
				outcomeErr := xerrors.New("parallel retention cohort returned no outcome")
				cohortOutcomes = append(
					cohortOutcomes,
					newSingleBlockRetentionCohortFailureOutcome(cohort, nil, outcomeErr),
				)
				if firstErr == nil {
					firstErr = outcomeErr
				}
				continue
			}
			result.addRangeResult(outcome.result)
			cohortOutcomes = append(
				cohortOutcomes,
				newSingleBlockRetentionCohortFailureOutcome(outcome.cohort, outcome.result, outcome.err),
			)
			if outcome.err != nil && firstErr == nil {
				firstErr = outcome.err
			}
		}
		if firstErr != nil {
			return cohortOutcomes, firstErr
		}
	}
	return cohortOutcomes, nil
}

func (w *SingleBlockRetention) processSingleBlockRetentionCohort(
	ctx workflow.Context,
	activityCtx workflow.Context,
	logger *zap.Logger,
	request *SingleBlockRetentionRequest,
	activityTag uint32,
	activityEligibilityCutoff time.Time,
	cohort retirement.RetentionCohort,
	rangeSweepEnabled bool,
	resultValidationEnabled bool,
	deferralRetryBudget int,
) (*activity.SingleBlockRetentionRangeResult, error) {
	if err := validateSelectedSingleBlockRetentionCohort(
		cohort,
		request.StartHeight,
		request.EndHeight,
	); err != nil {
		return nil, err
	}
	if request.Execute {
		if err := validateApprovedSingleBlockRetentionCohort(
			cohort,
			request,
			rangeSweepEnabled,
		); err != nil {
			return nil, err
		}
	}

	processRequest := &activity.SingleBlockRetentionProcessRequest{
		Tag:                         activityTag,
		Cohort:                      cohort,
		EligibilityCutoff:           activityEligibilityCutoff,
		Execute:                     request.Execute,
		ProductionDeleteEnabled:     request.ProductionDeleteEnabled,
		DirectStorageClientsGuarded: request.DirectStorageClientsGuarded,
		SingleBlockWritersGuarded:   request.SingleBlockWritersGuarded,
		FallbackReadsValidated:      request.FallbackReadsValidated,
		FallbackErrorCount:          request.FallbackErrorCount,
		ApprovedChain:               request.ApprovedChain,
		ApprovedStartHeight:         request.ApprovedStartHeight,
		ApprovedEndHeight:           request.ApprovedEndHeight,
	}
	rangeResult, err := w.activity.Process(activityCtx, processRequest)
	if err != nil {
		return nil, xerrors.Errorf(
			"failed to process retention cohort %q [%d, %d): %w",
			cohort.ConsolidatedObjectKey,
			cohort.StartHeight,
			cohort.EndHeight,
			err,
		)
	}
	if resultValidationEnabled {
		if err := validateSingleBlockRetentionRangeResult(cohort, rangeResult); err != nil {
			return nil, err
		}
	}
	latestValidResult := rangeResult
	// A deferred cohort is slept-and-retried up to deferralRetryBudget times.
	// Each deferral names its own transient cause and delay (the CSCB safety
	// quiescence, an active retirement claim), so one retry per cohort was
	// not enough when two different causes followed each other (INF-1603);
	// the budget bounds the total wait rather than the number of causes.
	for retries := 0; request.Execute && rangeResult.RetryAfter > 0 && retries < deferralRetryBudget; retries++ {
		if rangeResult.RetryAfter > maxSingleBlockRetentionRetryDelay {
			return latestValidResult, xerrors.Errorf(
				"retention cohort %q requested retry delay %s above maximum %s",
				cohort.ConsolidatedObjectKey,
				rangeResult.RetryAfter,
				maxSingleBlockRetentionRetryDelay,
			)
		}
		logger.Info(
			"single-block retention cohort deferred for bounded retry",
			zap.String("consolidated_object_key", cohort.ConsolidatedObjectKey),
			zap.Uint64("start_height", cohort.StartHeight),
			zap.Uint64("end_height", cohort.EndHeight),
			zap.Duration("retry_after", rangeResult.RetryAfter),
			zap.String("retry_reason", rangeResult.RetryReason),
			zap.Int("retry", retries+1),
			zap.Int("retry_budget", deferralRetryBudget),
		)
		if err := workflow.Sleep(ctx, rangeResult.RetryAfter); err != nil {
			return latestValidResult, xerrors.Errorf("failed to wait before retention retry: %w", err)
		}
		retryResult, err := w.activity.Process(activityCtx, processRequest)
		if err != nil {
			return latestValidResult, xerrors.Errorf(
				"failed to retry retention cohort %q [%d, %d): %w",
				cohort.ConsolidatedObjectKey,
				cohort.StartHeight,
				cohort.EndHeight,
				err,
			)
		}
		if resultValidationEnabled {
			if err := validateSingleBlockRetentionRangeResult(cohort, retryResult); err != nil {
				return latestValidResult, err
			}
		}
		rangeResult = retryResult
		latestValidResult = retryResult
	}
	if !request.Execute {
		return rangeResult, nil
	}
	if rangeResult.RetryAfter > 0 {
		return rangeResult, xerrors.Errorf(
			"retention cohort %q remained deferred after bounded retry (%d): %s",
			cohort.ConsolidatedObjectKey,
			deferralRetryBudget,
			rangeResult.RetryReason,
		)
	}
	if rangeResult.FailureMessage != "" {
		return rangeResult, xerrors.Errorf(
			"retention cohort %q failed: %s",
			cohort.ConsolidatedObjectKey,
			rangeResult.FailureMessage,
		)
	}
	if rangeResult.FailedRows > 0 || rangeResult.DeferredRows > 0 || !rangeResult.Terminal {
		return rangeResult, xerrors.Errorf(
			"retention cohort %q did not finish: failed_rows=%d deferred_rows=%d verified_through_exclusive=%d",
			cohort.ConsolidatedObjectKey,
			rangeResult.FailedRows,
			rangeResult.DeferredRows,
			rangeResult.VerifiedThroughExclusive,
		)
	}
	return rangeResult, nil
}

func newSingleBlockRetentionCohortFailureOutcome(
	cohort retirement.RetentionCohort,
	result *activity.SingleBlockRetentionRangeResult,
	err error,
) SingleBlockRetentionCohortFailureOutcome {
	outcome := SingleBlockRetentionCohortFailureOutcome{
		Cohort: cohort,
		Result: result,
	}
	if err != nil {
		outcome.FailureMessage = err.Error()
	}
	return outcome
}

func validateSingleBlockRetentionCohorts(
	cohorts []retirement.RetentionCohort,
	request *SingleBlockRetentionRequest,
	rangeSweepEnabled bool,
) error {
	ordered := append([]retirement.RetentionCohort(nil), cohorts...)
	seenKeys := make(map[string]struct{}, len(ordered))
	for _, cohort := range ordered {
		if err := validateSelectedSingleBlockRetentionCohort(
			cohort,
			request.StartHeight,
			request.EndHeight,
		); err != nil {
			return err
		}
		if request.Execute {
			if err := validateApprovedSingleBlockRetentionCohort(
				cohort,
				request,
				rangeSweepEnabled,
			); err != nil {
				return err
			}
		}
		if _, ok := seenKeys[cohort.ConsolidatedObjectKey]; ok {
			return xerrors.Errorf(
				"retention selection contains duplicate CSCB object %q",
				cohort.ConsolidatedObjectKey,
			)
		}
		seenKeys[cohort.ConsolidatedObjectKey] = struct{}{}
	}
	sort.Slice(ordered, func(i, j int) bool {
		if ordered[i].StartHeight != ordered[j].StartHeight {
			return ordered[i].StartHeight < ordered[j].StartHeight
		}
		return ordered[i].EndHeight < ordered[j].EndHeight
	})
	for i := 1; i < len(ordered); i++ {
		previous := ordered[i-1]
		current := ordered[i]
		if current.StartHeight < previous.EndHeight {
			return xerrors.Errorf(
				"retention CSCB ranges overlap: %q [%d, %d) and %q [%d, %d)",
				previous.ConsolidatedObjectKey,
				previous.StartHeight,
				previous.EndHeight,
				current.ConsolidatedObjectKey,
				current.StartHeight,
				current.EndHeight,
			)
		}
	}
	return nil
}

func validateSingleBlockRetentionRangeResult(
	expected retirement.RetentionCohort,
	result *activity.SingleBlockRetentionRangeResult,
) error {
	if result == nil {
		return xerrors.Errorf(
			"retention cohort %q [%d, %d) returned no result",
			expected.ConsolidatedObjectKey,
			expected.StartHeight,
			expected.EndHeight,
		)
	}
	actual := result.Cohort
	if actual.ConsolidatedObjectKey != expected.ConsolidatedObjectKey ||
		actual.StartHeight != expected.StartHeight ||
		actual.EndHeight != expected.EndHeight ||
		actual.RowCount != expected.RowCount ||
		!actual.EligibleAt.Equal(expected.EligibleAt) ||
		actual.Pending != expected.Pending {
		return xerrors.Errorf(
			"retention result cohort does not match request: expected=%+v actual=%+v",
			expected,
			actual,
		)
	}
	if result.VerifiedThroughExclusive < expected.StartHeight ||
		result.VerifiedThroughExclusive > expected.EndHeight {
		return xerrors.Errorf(
			"retention result verified-through height %d is outside cohort [%d, %d)",
			result.VerifiedThroughExclusive,
			expected.StartHeight,
			expected.EndHeight,
		)
	}
	if result.FirstIncompleteHeight == nil {
		if result.VerifiedThroughExclusive != expected.EndHeight ||
			result.ScannedRows == 0 ||
			!result.Terminal {
			return xerrors.Errorf(
				"retention result has inconsistent terminal progress: terminal=%t scanned_rows=%d verified_through_exclusive=%d first_incomplete_height=nil expected_end=%d",
				result.Terminal,
				result.ScannedRows,
				result.VerifiedThroughExclusive,
				expected.EndHeight,
			)
		}
	} else {
		firstIncompleteHeight := *result.FirstIncompleteHeight
		if firstIncompleteHeight < expected.StartHeight ||
			firstIncompleteHeight >= expected.EndHeight ||
			firstIncompleteHeight != result.VerifiedThroughExclusive ||
			result.Terminal {
			return xerrors.Errorf(
				"retention result has inconsistent incomplete progress: terminal=%t verified_through_exclusive=%d first_incomplete_height=%d cohort=[%d, %d)",
				result.Terminal,
				result.VerifiedThroughExclusive,
				firstIncompleteHeight,
				expected.StartHeight,
				expected.EndHeight,
			)
		}
	}
	classifiedRows := []struct {
		name  string
		value uint64
	}{
		{name: "planned_rows", value: result.PlannedRows},
		{name: "deleted_verified_rows", value: result.DeletedVerifiedRows},
		{name: "already_retired_rows", value: result.AlreadyRetiredRows},
		{name: "skipped_slots", value: result.SkippedSlots},
		{name: "deferred_rows", value: result.DeferredRows},
		{name: "failed_rows", value: result.FailedRows},
	}
	remainingRows := result.ScannedRows
	for _, classified := range classifiedRows {
		if classified.value > remainingRows {
			return xerrors.Errorf(
				"retention result row accounting exceeds scanned rows at %s: value=%d remaining=%d scanned_rows=%d",
				classified.name,
				classified.value,
				remainingRows,
				result.ScannedRows,
			)
		}
		remainingRows -= classified.value
	}
	if remainingRows != 0 {
		return xerrors.Errorf(
			"retention result row accounting is incomplete: unclassified_rows=%d scanned_rows=%d",
			remainingRows,
			result.ScannedRows,
		)
	}
	if result.RetryAfter < 0 || (result.RetryAfter > 0) != (result.RetryReason != "") {
		return xerrors.Errorf(
			"retention result has inconsistent retry state: retry_after=%s retry_reason=%q",
			result.RetryAfter,
			result.RetryReason,
		)
	}
	if result.Terminal && (result.PlannedRows != 0 ||
		result.DeferredRows != 0 ||
		result.FailedRows != 0 ||
		result.RetryAfter != 0 ||
		result.FailureMessage != "") {
		return xerrors.Errorf(
			"terminal retention result contains nonterminal accounting: planned_rows=%d deferred_rows=%d failed_rows=%d retry_after=%s failure_message=%q",
			result.PlannedRows,
			result.DeferredRows,
			result.FailedRows,
			result.RetryAfter,
			result.FailureMessage,
		)
	}
	return nil
}

func encodeSingleBlockRetentionEffectiveTag(tag uint32) uint32 {
	if tag == 0 {
		return math.MaxUint32
	}
	return tag
}

func newSingleBlockRetentionResult(
	request *SingleBlockRetentionRequest,
	now time.Time,
) *SingleBlockRetentionResult {
	result := &SingleBlockRetentionResult{
		StartedAt:         now,
		EligibilityCutoff: now,
	}
	if request == nil {
		return result
	}

	result.Execute = request.Execute
	result.ApprovedChain = request.ApprovedChain
	result.ApprovedStartHeight = request.ApprovedStartHeight
	result.ApprovedEndHeight = request.ApprovedEndHeight
	result.FallbackReadsValidated = request.FallbackReadsValidated
	result.FallbackErrorCount = request.FallbackErrorCount
	if !request.EligibilityCutoff.IsZero() {
		result.EligibilityCutoff = request.EligibilityCutoff.UTC()
	}

	checkpoint := request.Checkpoint
	if checkpoint == nil {
		return result
	}
	result.StartedAt = checkpoint.StartedAt
	result.EligibilityCutoff = checkpoint.EligibilityCutoff
	result.ContinueAsNewCount = checkpoint.ContinueAsNewCount
	result.SelectedObjectRanges = checkpoint.SelectedObjectRanges
	result.ProcessedObjectRanges = checkpoint.ProcessedObjectRanges
	result.CompletedObjectRangeCount = checkpoint.CompletedObjectRangeCount
	result.ScannedRows = checkpoint.ScannedRows
	result.PlannedRows = checkpoint.PlannedRows
	result.DeletedVerifiedRows = checkpoint.DeletedVerifiedRows
	result.AlreadyRetiredRows = checkpoint.AlreadyRetiredRows
	result.SkippedSlots = checkpoint.SkippedSlots
	result.DeferredRows = checkpoint.DeferredRows
	result.FailedRows = checkpoint.FailedRows
	result.DeletedVersions = checkpoint.DeletedVersions
	result.DeletedMarkers = checkpoint.DeletedMarkers
	result.RetiredBytes = checkpoint.RetiredBytes
	result.LastCompletedObjectRange = cloneSingleBlockRetentionCompletedRange(
		checkpoint.LastCompletedObjectRange,
	)
	return result
}

func (r *SingleBlockRetentionResult) checkpoint() *SingleBlockRetentionCheckpoint {
	return &SingleBlockRetentionCheckpoint{
		StartedAt:                 r.StartedAt,
		EligibilityCutoff:         r.EligibilityCutoff,
		EffectiveTag:              r.Tag,
		ContinueAsNewCount:        r.ContinueAsNewCount,
		SelectedObjectRanges:      r.SelectedObjectRanges,
		ProcessedObjectRanges:     r.ProcessedObjectRanges,
		CompletedObjectRangeCount: r.CompletedObjectRangeCount,
		ScannedRows:               r.ScannedRows,
		PlannedRows:               r.PlannedRows,
		DeletedVerifiedRows:       r.DeletedVerifiedRows,
		AlreadyRetiredRows:        r.AlreadyRetiredRows,
		SkippedSlots:              r.SkippedSlots,
		DeferredRows:              r.DeferredRows,
		FailedRows:                r.FailedRows,
		DeletedVersions:           r.DeletedVersions,
		DeletedMarkers:            r.DeletedMarkers,
		RetiredBytes:              r.RetiredBytes,
		LastCompletedObjectRange: cloneSingleBlockRetentionCompletedRange(
			r.LastCompletedObjectRange,
		),
	}
}

func cloneSingleBlockRetentionCompletedRange(
	completedRange *SingleBlockRetentionCompletedRange,
) *SingleBlockRetentionCompletedRange {
	if completedRange == nil {
		return nil
	}
	cloned := *completedRange
	return &cloned
}

func (r *SingleBlockRetentionResult) addRangeResult(result *activity.SingleBlockRetentionRangeResult) {
	if result == nil {
		return
	}
	r.ProcessedObjectRanges++
	r.RangeResults = append(r.RangeResults, result)
	r.ScannedRows += result.ScannedRows
	r.PlannedRows += result.PlannedRows
	r.DeletedVerifiedRows += result.DeletedVerifiedRows
	r.AlreadyRetiredRows += result.AlreadyRetiredRows
	r.SkippedSlots += result.SkippedSlots
	r.DeferredRows += result.DeferredRows
	r.FailedRows += result.FailedRows
	r.DeletedVersions += result.DeletedVersions
	r.DeletedMarkers += result.DeletedMarkers
	r.RetiredBytes += result.RetiredBytes
	if result.Terminal {
		completedRange := SingleBlockRetentionCompletedRange{
			ConsolidatedObjectKey: result.Cohort.ConsolidatedObjectKey,
			StartHeight:           result.Cohort.StartHeight,
			EndHeight:             result.Cohort.EndHeight,
			EligibleRows:          result.Cohort.RowCount,
		}
		r.CompletedObjectRanges = append(r.CompletedObjectRanges, completedRange)
		r.CompletedObjectRangeCount++
		r.LastCompletedObjectRange = &completedRange
	}
}

func validateSingleBlockRetentionCheckpoint(
	request *SingleBlockRetentionRequest,
	isContinuation bool,
	rangeSweepEnabled bool,
	effectiveTag uint32,
) error {
	if request == nil || !rangeSweepEnabled {
		return nil
	}
	checkpoint := request.Checkpoint
	if !isContinuation {
		if checkpoint != nil {
			return xerrors.New("single-block retention checkpoint is internal and cannot be supplied on an initial execution")
		}
		return nil
	}
	if !request.Execute {
		return xerrors.New("single-block retention continuation requires execution mode")
	}
	if checkpoint == nil {
		return xerrors.New("single-block retention continuation is missing its checkpoint")
	}
	if checkpoint.StartedAt.IsZero() ||
		checkpoint.EligibilityCutoff.IsZero() ||
		checkpoint.EligibilityCutoff.After(checkpoint.StartedAt) ||
		checkpoint.ContinueAsNewCount == 0 {
		return xerrors.New("single-block retention continuation checkpoint is invalid")
	}
	if !checkpoint.EligibilityCutoff.Equal(request.EligibilityCutoff) {
		return xerrors.Errorf(
			"single-block retention eligibility cutoff changed across continuation: checkpoint=%s current=%s",
			checkpoint.EligibilityCutoff,
			request.EligibilityCutoff,
		)
	}
	if checkpoint.EffectiveTag != effectiveTag {
		return xerrors.Errorf(
			"single-block retention effective tag changed across continuation: checkpoint=%d current=%d",
			checkpoint.EffectiveTag,
			effectiveTag,
		)
	}
	if checkpoint.ProcessedObjectRanges > checkpoint.SelectedObjectRanges ||
		checkpoint.CompletedObjectRangeCount > checkpoint.ProcessedObjectRanges {
		return xerrors.Errorf(
			"single-block retention continuation checkpoint counters are invalid: selected=%d processed=%d completed=%d",
			checkpoint.SelectedObjectRanges,
			checkpoint.ProcessedObjectRanges,
			checkpoint.CompletedObjectRangeCount,
		)
	}
	if checkpoint.CompletedObjectRangeCount > 0 && checkpoint.LastCompletedObjectRange == nil {
		return xerrors.New("single-block retention continuation checkpoint is missing its last completed range")
	}
	return nil
}

func validateSingleBlockRetentionExecutionRequest(
	request *SingleBlockRetentionRequest,
	allowContainingApproval bool,
) error {
	if request == nil {
		return xerrors.New("single-block retention request is required")
	}
	if !request.Execute {
		return validateSingleBlockRetentionSelectionRange(request.StartHeight, request.EndHeight)
	}
	if err := validateSingleBlockRetentionSelectionRange(request.StartHeight, request.EndHeight); err != nil {
		return err
	}
	if request.EndHeight == 0 {
		return xerrors.New("retention execution requires an explicit exact selection range; unbounded execution is not allowed")
	}
	if request.ApprovedChain == "" {
		return xerrors.New("retention execution requires an explicit operator approval chain")
	}
	if request.ApprovedEndHeight <= request.ApprovedStartHeight {
		return xerrors.Errorf(
			"retention execution requires a valid approved range, got [%d, %d)",
			request.ApprovedStartHeight,
			request.ApprovedEndHeight,
		)
	}
	if !allowContainingApproval &&
		(request.ApprovedStartHeight != request.StartHeight || request.ApprovedEndHeight != request.EndHeight) {
		return xerrors.Errorf(
			"retention execution approved range [%d, %d) must exactly match the selection range [%d, %d)",
			request.ApprovedStartHeight,
			request.ApprovedEndHeight,
			request.StartHeight,
			request.EndHeight,
		)
	}
	if allowContainingApproval &&
		(request.ApprovedStartHeight > request.StartHeight || request.ApprovedEndHeight < request.EndHeight) {
		return xerrors.Errorf(
			"retention execution selection range [%d, %d) is outside the approved envelope [%d, %d)",
			request.StartHeight,
			request.EndHeight,
			request.ApprovedStartHeight,
			request.ApprovedEndHeight,
		)
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
	return nil
}

func validateSingleBlockRetentionSelectionRange(startHeight uint64, endHeight uint64) error {
	if endHeight == 0 && startHeight != 0 {
		return xerrors.New("single-block retention end height is required when start height is set")
	}
	if endHeight != 0 && endHeight <= startHeight {
		return xerrors.Errorf("invalid single-block retention range [%d, %d)", startHeight, endHeight)
	}
	return nil
}

// validateApprovedSingleBlockRetentionCohort preserves the exact-cohort check
// for workflows started before range sweeps were introduced. New workflows
// allow each selected cohort only when it is fully contained by the immutable
// operator-approved envelope.
func validateApprovedSingleBlockRetentionCohort(
	cohort retirement.RetentionCohort,
	request *SingleBlockRetentionRequest,
	rangeSweepEnabled bool,
) error {
	if !rangeSweepEnabled {
		if cohort.StartHeight == request.ApprovedStartHeight &&
			cohort.EndHeight == request.ApprovedEndHeight {
			return nil
		}
		return xerrors.Errorf(
			"selected retention cohort %q [%d, %d) does not exactly match the approved range [%d, %d); approve the exact cohort range before execution",
			cohort.ConsolidatedObjectKey,
			cohort.StartHeight,
			cohort.EndHeight,
			request.ApprovedStartHeight,
			request.ApprovedEndHeight,
		)
	}
	if cohort.StartHeight < request.ApprovedStartHeight || cohort.EndHeight > request.ApprovedEndHeight {
		return xerrors.Errorf(
			"selected retention cohort %q [%d, %d) is outside the approved envelope [%d, %d)",
			cohort.ConsolidatedObjectKey,
			cohort.StartHeight,
			cohort.EndHeight,
			request.ApprovedStartHeight,
			request.ApprovedEndHeight,
		)
	}
	return nil
}

func validateSelectedSingleBlockRetentionCohort(
	cohort retirement.RetentionCohort,
	selectionStartHeight uint64,
	selectionEndHeight uint64,
) error {
	if cohort.ConsolidatedObjectKey == "" || cohort.EndHeight <= cohort.StartHeight ||
		cohort.RowCount == 0 || cohort.EligibleAt.IsZero() {
		return xerrors.Errorf("invalid selected single-block retention cohort: %+v", cohort)
	}
	if selectionEndHeight != 0 &&
		(cohort.StartHeight < selectionStartHeight || cohort.EndHeight > selectionEndHeight) {
		return xerrors.Errorf(
			"selected retention cohort %q [%d, %d) is outside requested range [%d, %d)",
			cohort.ConsolidatedObjectKey,
			cohort.StartHeight,
			cohort.EndHeight,
			selectionStartHeight,
			selectionEndHeight,
		)
	}
	return nil
}
