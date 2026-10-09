package workflow

import (
	"context"
	"errors"
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
	temporalworkflow "go.temporal.io/sdk/workflow"
	"go.uber.org/fx"

	"github.com/coinbase/chainstorage/internal/cadence"
	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/storage/blobstorage"
	"github.com/coinbase/chainstorage/internal/storage/metastorage"
	"github.com/coinbase/chainstorage/internal/storage/retirement"
	"github.com/coinbase/chainstorage/internal/utils/testapp"
	"github.com/coinbase/chainstorage/internal/workflow/activity"
)

type singleBlockRetentionTestSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite
	env      *cadence.TestEnv
	workflow *SingleBlockRetention
	app      testapp.TestApp
	cfg      *config.Config
}

var testSingleBlockRetentionEligibilityCutoff = time.Date(2026, 7, 23, 0, 0, 0, 0, time.UTC)

func TestSingleBlockRetentionWorkflowTestSuite(t *testing.T) {
	suite.Run(t, new(singleBlockRetentionTestSuite))
}

func TestValidateSingleBlockRetentionSelectionRange(t *testing.T) {
	require.ErrorContains(t, validateSingleBlockRetentionSelectionRange(100, 0), "end height is required")
	require.ErrorContains(t, validateSingleBlockRetentionSelectionRange(100, 100), "invalid")
	require.NoError(t, validateSingleBlockRetentionSelectionRange(0, 0))
	require.NoError(t, validateSingleBlockRetentionSelectionRange(100, 200))
}

func TestValidateSelectedSingleBlockRetentionCohort(t *testing.T) {
	cohort := testRetentionCohort("consolidated/100-110.cscb.zstd", 100, 110)

	require.NoError(t, validateSelectedSingleBlockRetentionCohort(cohort, 100, 110))
	require.NoError(t, validateSelectedSingleBlockRetentionCohort(cohort, 0, 0))
	require.ErrorContains(
		t,
		validateSelectedSingleBlockRetentionCohort(cohort, 101, 110),
		"outside requested range",
	)
	require.ErrorContains(
		t,
		validateSelectedSingleBlockRetentionCohort(cohort, 100, 109),
		"outside requested range",
	)
	cohort.EligibleAt = time.Time{}
	require.ErrorContains(t, validateSelectedSingleBlockRetentionCohort(cohort, 0, 0), "invalid selected")
}

func (s *singleBlockRetentionTestSuite) SetupTest() {
	cfg, err := config.New()
	require.NoError(s.T(), err)
	cfg.Workflows.SingleBlockRetention.MaxObjectRanges = 2
	s.cfg = cfg
	s.env = cadence.NewTestEnv(s)
	s.app = testapp.New(
		s.T(),
		Module,
		testapp.WithConfig(cfg),
		cadence.WithTestEnv(s.env),
		fx.Provide(func() metastorage.MetaStorage {
			return nil
		}),
		fx.Provide(func() blobstorage.BlobStorage {
			return nil
		}),
		fx.Populate(&s.workflow),
	)
}

func (s *singleBlockRetentionTestSuite) TearDownTest() {
	s.app.Close()
	s.env.AssertExpectations(s.T())
}

func (s *singleBlockRetentionTestSuite) TestDryRunReturnsPlannedRangesWithoutDeleting() {
	cohort := testRetentionCohort("consolidated/100-110.cscb.zstd", 100, 110)
	firstIncompleteHeight := cohort.StartHeight
	var selectRequest *activity.SingleBlockRetentionSelectRequest
	s.env.OnActivity(activity.ActivitySingleBlockRetentionSelect, mock.Anything, mock.Anything).
		Return(func(_ context.Context, request *activity.SingleBlockRetentionSelectRequest) (*activity.SingleBlockRetentionSelectResponse, error) {
			selectRequest = request
			return &activity.SingleBlockRetentionSelectResponse{
				Cohorts: []retirement.RetentionCohort{cohort},
				HasMore: true,
			}, nil
		})
	s.env.OnActivity(activity.ActivitySingleBlockRetentionProcess, mock.Anything, mock.Anything).
		Return(&activity.SingleBlockRetentionRangeResult{
			Cohort:                   cohort,
			ScannedRows:              10,
			PlannedRows:              10,
			VerifiedThroughExclusive: 100,
			FirstIncompleteHeight:    &firstIncompleteHeight,
		}, nil)

	_, err := s.workflow.Execute(context.Background(), &SingleBlockRetentionRequest{
		Tag:             2,
		StartHeight:     100,
		EndHeight:       110,
		MaxObjectRanges: 1,
	})
	require.NoError(s.T(), err)
	require.Equal(s.T(), uint32(2), selectRequest.Tag)
	require.Equal(s.T(), uint64(100), selectRequest.StartHeight)
	require.Equal(s.T(), uint64(110), selectRequest.EndHeight)
	require.False(s.T(), selectRequest.EligibilityCutoff.IsZero())
	require.Equal(s.T(), 1, selectRequest.Limit)

	var result SingleBlockRetentionResult
	require.NoError(s.T(), s.env.GetWorkflowResult(&result))
	require.False(s.T(), result.Execute)
	require.Equal(s.T(), uint32(2), result.Tag)
	require.Equal(s.T(), selectRequest.EligibilityCutoff, result.EligibilityCutoff)
	require.Equal(s.T(), uint64(100), result.SelectionStartHeight)
	require.Equal(s.T(), uint64(110), result.SelectionEndHeight)
	require.Equal(s.T(), uint64(1), result.SelectedObjectRanges)
	require.True(s.T(), result.MoreEligibleRanges)
	require.Equal(s.T(), uint64(1), result.ProcessedObjectRanges)
	require.Equal(s.T(), uint64(10), result.PlannedRows)
	require.False(s.T(), result.SweepCompleted)
	require.Empty(s.T(), result.CompletedObjectRanges)
	require.Empty(s.T(), result.FailureMessage)
}

func (s *singleBlockRetentionTestSuite) TestExecuteReturnsExactCompletedRanges() {
	cohort := testRetentionCohort("consolidated/100-110.cscb.zstd", 100, 110)
	s.env.OnGetVersion(
		singleBlockRetentionParallelismChangeID,
		temporalworkflow.DefaultVersion,
		singleBlockRetentionParallelismVersion,
	).Return(temporalworkflow.DefaultVersion)
	s.env.OnActivity(activity.ActivitySingleBlockRetentionSelect, mock.Anything, mock.Anything).
		Return(&activity.SingleBlockRetentionSelectResponse{
			Cohorts: []retirement.RetentionCohort{cohort},
		}, nil).
		Once()
	s.env.OnActivity(activity.ActivitySingleBlockRetentionSelect, mock.Anything, mock.Anything).
		Return(&activity.SingleBlockRetentionSelectResponse{}, nil).
		Once()
	s.env.OnActivity(activity.ActivitySingleBlockRetentionProcess, mock.Anything, mock.Anything).
		Return(func(_ context.Context, request *activity.SingleBlockRetentionProcessRequest) (*activity.SingleBlockRetentionRangeResult, error) {
			require.True(s.T(), request.FallbackReadsValidated)
			require.Equal(s.T(), testSingleBlockRetentionEligibilityCutoff, request.EligibilityCutoff)
			// The operator approval must reach the activity verbatim, never
			// rewritten to whatever cohort was selected.
			require.Equal(s.T(), "solana-mainnet", request.ApprovedChain)
			require.Equal(s.T(), uint64(100), request.ApprovedStartHeight)
			require.Equal(s.T(), uint64(110), request.ApprovedEndHeight)
			return &activity.SingleBlockRetentionRangeResult{
				Cohort:                   request.Cohort,
				ScannedRows:              request.Cohort.RowCount,
				DeletedVerifiedRows:      request.Cohort.RowCount,
				DeletedVersions:          request.Cohort.RowCount,
				RetiredBytes:             request.Cohort.RowCount * 100,
				VerifiedThroughExclusive: request.Cohort.EndHeight,
				Terminal:                 true,
			}, nil
		})

	_, err := s.workflow.Execute(context.Background(), &SingleBlockRetentionRequest{
		Tag:                         2,
		StartHeight:                 100,
		EndHeight:                   110,
		EligibilityCutoff:           testSingleBlockRetentionEligibilityCutoff,
		Execute:                     true,
		DirectStorageClientsGuarded: true,
		SingleBlockWritersGuarded:   true,
		FallbackReadsValidated:      true,
		ApprovedChain:               "solana-mainnet",
		ApprovedStartHeight:         100,
		ApprovedEndHeight:           110,
	})
	require.NoError(s.T(), err)

	var result SingleBlockRetentionResult
	require.NoError(s.T(), s.env.GetWorkflowResult(&result))
	require.True(s.T(), result.Execute)
	require.Equal(s.T(), "solana-mainnet", result.ApprovedChain)
	require.Equal(s.T(), uint64(100), result.ApprovedStartHeight)
	require.Equal(s.T(), uint64(110), result.ApprovedEndHeight)
	require.Equal(s.T(), uint64(1), result.SelectedObjectRanges)
	require.Equal(s.T(), 1, result.Parallelism)
	require.Equal(s.T(), uint64(1), result.ProcessedObjectRanges)
	require.Equal(s.T(), uint64(1), result.CompletedObjectRangeCount)
	require.Equal(s.T(), uint64(10), result.DeletedVerifiedRows)
	require.Equal(s.T(), uint64(10), result.DeletedVersions)
	require.Equal(s.T(), uint64(1000), result.RetiredBytes)
	require.True(s.T(), result.SweepCompleted)
	require.Equal(s.T(), &SingleBlockRetentionCompletedRange{
		ConsolidatedObjectKey: cohort.ConsolidatedObjectKey,
		StartHeight:           100,
		EndHeight:             110,
		EligibleRows:          10,
	}, result.LastCompletedObjectRange)
	require.Equal(s.T(), []SingleBlockRetentionCompletedRange{
		{
			ConsolidatedObjectKey: cohort.ConsolidatedObjectKey,
			StartHeight:           100,
			EndHeight:             110,
			EligibleRows:          10,
		},
	}, result.CompletedObjectRanges)
}

func (s *singleBlockRetentionTestSuite) TestExecuteProcessesCohortsInsideApprovedEnvelope() {
	first := testRetentionCohort("consolidated/100-110.cscb.zstd", 100, 110)
	second := testRetentionCohort("consolidated/200-210.cscb.zstd", 200, 210)
	s.env.OnActivity(activity.ActivitySingleBlockRetentionSelect, mock.Anything, mock.Anything).
		Return(&activity.SingleBlockRetentionSelectResponse{
			Cohorts: []retirement.RetentionCohort{first, second},
		}, nil).
		Once()
	s.env.OnActivity(activity.ActivitySingleBlockRetentionSelect, mock.Anything, mock.Anything).
		Return(&activity.SingleBlockRetentionSelectResponse{}, nil).
		Once()
	s.env.OnActivity(activity.ActivitySingleBlockRetentionProcess, mock.Anything, mock.Anything).
		Return(func(_ context.Context, request *activity.SingleBlockRetentionProcessRequest) (*activity.SingleBlockRetentionRangeResult, error) {
			require.Equal(s.T(), uint64(100), request.ApprovedStartHeight)
			require.Equal(s.T(), uint64(210), request.ApprovedEndHeight)
			return &activity.SingleBlockRetentionRangeResult{
				Cohort:                   request.Cohort,
				ScannedRows:              request.Cohort.RowCount,
				DeletedVerifiedRows:      request.Cohort.RowCount,
				VerifiedThroughExclusive: request.Cohort.EndHeight,
				Terminal:                 true,
			}, nil
		}).Twice()

	_, err := s.workflow.Execute(context.Background(), &SingleBlockRetentionRequest{
		Tag:                         2,
		StartHeight:                 100,
		EndHeight:                   210,
		EligibilityCutoff:           testSingleBlockRetentionEligibilityCutoff,
		Execute:                     true,
		DirectStorageClientsGuarded: true,
		SingleBlockWritersGuarded:   true,
		FallbackReadsValidated:      true,
		ApprovedChain:               "solana-mainnet",
		ApprovedStartHeight:         100,
		ApprovedEndHeight:           210,
	})
	require.NoError(s.T(), err)

	var result SingleBlockRetentionResult
	require.NoError(s.T(), s.env.GetWorkflowResult(&result))
	require.True(s.T(), result.SweepCompleted)
	require.Equal(s.T(), uint64(2), result.SelectedObjectRanges)
	require.Equal(s.T(), uint64(2), result.ProcessedObjectRanges)
	require.Equal(s.T(), uint64(2), result.CompletedObjectRangeCount)
	require.Equal(s.T(), uint64(20), result.DeletedVerifiedRows)
	require.Equal(s.T(), second.EndHeight, result.LastCompletedObjectRange.EndHeight)
}

func (s *singleBlockRetentionTestSuite) TestExecuteProcessesCohortsInParallelBatches() {
	cohorts := []retirement.RetentionCohort{
		testRetentionCohort("consolidated/100-110.cscb.zstd", 100, 110),
		testRetentionCohort("consolidated/110-120.cscb.zstd", 110, 120),
		testRetentionCohort("consolidated/120-130.cscb.zstd", 120, 130),
	}
	s.env.OnGetVersion(
		singleBlockRetentionParallelismChangeID,
		temporalworkflow.DefaultVersion,
		singleBlockRetentionParallelismVersion,
	).Return(temporalworkflow.Version(singleBlockRetentionParallelismVersion))
	s.env.OnActivity(activity.ActivitySingleBlockRetentionSelect, mock.Anything, mock.Anything).
		Return(&activity.SingleBlockRetentionSelectResponse{Cohorts: cohorts}, nil).
		Once()
	s.env.OnActivity(activity.ActivitySingleBlockRetentionSelect, mock.Anything, mock.Anything).
		Return(&activity.SingleBlockRetentionSelectResponse{}, nil).
		Once()

	var active int32
	var maxActive int32
	var mu sync.Mutex
	processedKeys := make([]string, 0, len(cohorts))
	s.env.OnActivity(activity.ActivitySingleBlockRetentionProcess, mock.Anything, mock.Anything).
		Return(func(_ context.Context, request *activity.SingleBlockRetentionProcessRequest) (*activity.SingleBlockRetentionRangeResult, error) {
			current := atomic.AddInt32(&active, 1)
			for {
				previous := atomic.LoadInt32(&maxActive)
				if current <= previous || atomic.CompareAndSwapInt32(&maxActive, previous, current) {
					break
				}
			}
			time.Sleep(50 * time.Millisecond)
			atomic.AddInt32(&active, -1)
			mu.Lock()
			processedKeys = append(processedKeys, request.Cohort.ConsolidatedObjectKey)
			mu.Unlock()
			return terminalSingleBlockRetentionRangeResult(request.Cohort), nil
		}).
		Times(len(cohorts))

	_, err := s.workflow.Execute(context.Background(), &SingleBlockRetentionRequest{
		Tag:                         2,
		StartHeight:                 100,
		EndHeight:                   130,
		EligibilityCutoff:           testSingleBlockRetentionEligibilityCutoff,
		MaxObjectRanges:             len(cohorts),
		Parallelism:                 2,
		Execute:                     true,
		DirectStorageClientsGuarded: true,
		SingleBlockWritersGuarded:   true,
		FallbackReadsValidated:      true,
		ApprovedChain:               "solana-mainnet",
		ApprovedStartHeight:         100,
		ApprovedEndHeight:           130,
	})
	require.NoError(s.T(), err)
	require.Equal(s.T(), int32(2), atomic.LoadInt32(&maxActive))
	require.ElementsMatch(s.T(), []string{
		cohorts[0].ConsolidatedObjectKey,
		cohorts[1].ConsolidatedObjectKey,
		cohorts[2].ConsolidatedObjectKey,
	}, processedKeys)

	var result SingleBlockRetentionResult
	require.NoError(s.T(), s.env.GetWorkflowResult(&result))
	require.Equal(s.T(), 2, result.Parallelism)
	require.Equal(s.T(), uint64(3), result.CompletedObjectRangeCount)
	require.Len(s.T(), result.CompletedObjectRanges, 3)
	for i, completed := range result.CompletedObjectRanges {
		require.Equal(s.T(), cohorts[i].ConsolidatedObjectKey, completed.ConsolidatedObjectKey)
	}
}

func (s *singleBlockRetentionTestSuite) TestExecuteRetriesParallelCohortsConcurrently() {
	cohorts := []retirement.RetentionCohort{
		testRetentionCohort("consolidated/100-110.cscb.zstd", 100, 110),
		testRetentionCohort("consolidated/110-120.cscb.zstd", 110, 120),
	}
	s.env.OnActivity(activity.ActivitySingleBlockRetentionSelect, mock.Anything, mock.Anything).
		Return(&activity.SingleBlockRetentionSelectResponse{Cohorts: cohorts}, nil).
		Once()
	s.env.OnActivity(activity.ActivitySingleBlockRetentionSelect, mock.Anything, mock.Anything).
		Return(&activity.SingleBlockRetentionSelectResponse{}, nil).
		Once()

	attempts := make(map[string]int, len(cohorts))
	var mu sync.Mutex
	s.env.OnActivity(activity.ActivitySingleBlockRetentionProcess, mock.Anything, mock.Anything).
		Return(func(_ context.Context, request *activity.SingleBlockRetentionProcessRequest) (*activity.SingleBlockRetentionRangeResult, error) {
			mu.Lock()
			attempts[request.Cohort.ConsolidatedObjectKey]++
			attempt := attempts[request.Cohort.ConsolidatedObjectKey]
			mu.Unlock()
			if attempt == 1 {
				firstIncompleteHeight := request.Cohort.StartHeight
				return &activity.SingleBlockRetentionRangeResult{
					Cohort:                   request.Cohort,
					ScannedRows:              request.Cohort.RowCount,
					DeferredRows:             request.Cohort.RowCount,
					VerifiedThroughExclusive: request.Cohort.StartHeight,
					FirstIncompleteHeight:    &firstIncompleteHeight,
					RetryAfter:               time.Minute,
					RetryReason:              retirement.SkipCSCBSafetyQuiescenceActive,
				}, nil
			}
			return terminalSingleBlockRetentionRangeResult(request.Cohort), nil
		}).
		Times(4)

	_, err := s.workflow.Execute(context.Background(), &SingleBlockRetentionRequest{
		Tag:                         2,
		StartHeight:                 100,
		EndHeight:                   120,
		EligibilityCutoff:           testSingleBlockRetentionEligibilityCutoff,
		MaxObjectRanges:             len(cohorts),
		Parallelism:                 2,
		Execute:                     true,
		DirectStorageClientsGuarded: true,
		SingleBlockWritersGuarded:   true,
		FallbackReadsValidated:      true,
		ApprovedChain:               "solana-mainnet",
		ApprovedStartHeight:         100,
		ApprovedEndHeight:           120,
	})
	require.NoError(s.T(), err)
	require.Equal(s.T(), 2, attempts[cohorts[0].ConsolidatedObjectKey])
	require.Equal(s.T(), 2, attempts[cohorts[1].ConsolidatedObjectKey])

	var result SingleBlockRetentionResult
	require.NoError(s.T(), s.env.GetWorkflowResult(&result))
	require.Less(s.T(), result.CompletedAt.Sub(result.StartedAt), 2*time.Minute)
	require.Equal(s.T(), uint64(2), result.CompletedObjectRangeCount)
}

func (s *singleBlockRetentionTestSuite) TestExecuteRejectsOverlappingParallelCohortsBeforeProcessing() {
	cohorts := []retirement.RetentionCohort{
		testRetentionCohort("consolidated/100-115.cscb.zstd", 100, 115),
		testRetentionCohort("consolidated/110-120.cscb.zstd", 110, 120),
	}
	s.env.OnActivity(activity.ActivitySingleBlockRetentionSelect, mock.Anything, mock.Anything).
		Return(&activity.SingleBlockRetentionSelectResponse{Cohorts: cohorts}, nil).
		Once()

	_, err := s.workflow.Execute(context.Background(), &SingleBlockRetentionRequest{
		Tag:                         2,
		StartHeight:                 100,
		EndHeight:                   120,
		EligibilityCutoff:           testSingleBlockRetentionEligibilityCutoff,
		MaxObjectRanges:             len(cohorts),
		Parallelism:                 2,
		Execute:                     true,
		DirectStorageClientsGuarded: true,
		SingleBlockWritersGuarded:   true,
		FallbackReadsValidated:      true,
		ApprovedChain:               "solana-mainnet",
		ApprovedStartHeight:         100,
		ApprovedEndHeight:           120,
	})
	require.ErrorContains(s.T(), err, "retention CSCB ranges overlap")
}

func (s *singleBlockRetentionTestSuite) TestExecuteRejectsDuplicateSerialCohortsBeforeProcessing() {
	first := testRetentionCohort("consolidated/100-110.cscb.zstd", 100, 110)
	second := testRetentionCohort(first.ConsolidatedObjectKey, 110, 120)
	s.env.OnGetVersion(
		singleBlockRetentionParallelismChangeID,
		temporalworkflow.DefaultVersion,
		singleBlockRetentionParallelismVersion,
	).Return(temporalworkflow.Version(singleBlockRetentionParallelismVersion))
	s.env.OnActivity(activity.ActivitySingleBlockRetentionSelect, mock.Anything, mock.Anything).
		Return(&activity.SingleBlockRetentionSelectResponse{
			Cohorts: []retirement.RetentionCohort{first, second},
		}, nil).
		Once()

	_, err := s.workflow.Execute(context.Background(), &SingleBlockRetentionRequest{
		Tag:             2,
		StartHeight:     100,
		EndHeight:       120,
		MaxObjectRanges: 2,
		Parallelism:     1,
	})
	require.ErrorContains(s.T(), err, "retention selection contains duplicate CSCB object")
	s.env.AssertActivityNotCalled(
		s.T(),
		activity.ActivitySingleBlockRetentionProcess,
		mock.Anything,
		mock.Anything,
	)
}

func (s *singleBlockRetentionTestSuite) TestExecuteRejectsOverlappingSerialCohortsBeforeProcessing() {
	first := testRetentionCohort("consolidated/100-115.cscb.zstd", 100, 115)
	second := testRetentionCohort("consolidated/110-120.cscb.zstd", 110, 120)
	s.env.OnGetVersion(
		singleBlockRetentionParallelismChangeID,
		temporalworkflow.DefaultVersion,
		singleBlockRetentionParallelismVersion,
	).Return(temporalworkflow.Version(singleBlockRetentionParallelismVersion))
	s.env.OnActivity(activity.ActivitySingleBlockRetentionSelect, mock.Anything, mock.Anything).
		Return(&activity.SingleBlockRetentionSelectResponse{
			Cohorts: []retirement.RetentionCohort{first, second},
		}, nil).
		Once()

	_, err := s.workflow.Execute(context.Background(), &SingleBlockRetentionRequest{
		Tag:             2,
		StartHeight:     100,
		EndHeight:       120,
		MaxObjectRanges: 2,
		Parallelism:     1,
	})
	require.ErrorContains(s.T(), err, "retention CSCB ranges overlap")
	s.env.AssertActivityNotCalled(
		s.T(),
		activity.ActivitySingleBlockRetentionProcess,
		mock.Anything,
		mock.Anything,
	)
}

func (s *singleBlockRetentionTestSuite) TestExecuteParallelFailureStopsBeforeNextBatch() {
	cohorts := []retirement.RetentionCohort{
		testRetentionCohort("consolidated/100-110.cscb.zstd", 100, 110),
		testRetentionCohort("consolidated/110-120.cscb.zstd", 110, 120),
		testRetentionCohort("consolidated/120-130.cscb.zstd", 120, 130),
	}
	s.env.OnActivity(activity.ActivitySingleBlockRetentionSelect, mock.Anything, mock.Anything).
		Return(&activity.SingleBlockRetentionSelectResponse{Cohorts: cohorts}, nil).
		Once()

	var mu sync.Mutex
	processedKeys := make([]string, 0, 2)
	s.env.OnActivity(activity.ActivitySingleBlockRetentionProcess, mock.Anything, mock.Anything).
		Return(func(_ context.Context, request *activity.SingleBlockRetentionProcessRequest) (*activity.SingleBlockRetentionRangeResult, error) {
			mu.Lock()
			processedKeys = append(processedKeys, request.Cohort.ConsolidatedObjectKey)
			mu.Unlock()
			if request.Cohort.ConsolidatedObjectKey == cohorts[0].ConsolidatedObjectKey {
				firstIncompleteHeight := request.Cohort.StartHeight
				return &activity.SingleBlockRetentionRangeResult{
					Cohort:                   request.Cohort,
					ScannedRows:              request.Cohort.RowCount,
					AlreadyRetiredRows:       request.Cohort.RowCount - 1,
					FailedRows:               1,
					VerifiedThroughExclusive: request.Cohort.StartHeight,
					FirstIncompleteHeight:    &firstIncompleteHeight,
				}, nil
			}
			return terminalSingleBlockRetentionRangeResult(request.Cohort), nil
		}).
		Twice()

	_, err := s.workflow.Execute(context.Background(), &SingleBlockRetentionRequest{
		Tag:                         2,
		StartHeight:                 100,
		EndHeight:                   130,
		EligibilityCutoff:           testSingleBlockRetentionEligibilityCutoff,
		MaxObjectRanges:             len(cohorts),
		Parallelism:                 2,
		Execute:                     true,
		DirectStorageClientsGuarded: true,
		SingleBlockWritersGuarded:   true,
		FallbackReadsValidated:      true,
		ApprovedChain:               "solana-mainnet",
		ApprovedStartHeight:         100,
		ApprovedEndHeight:           130,
	})
	require.ErrorContains(s.T(), err, "did not finish")
	require.ElementsMatch(s.T(), []string{
		cohorts[0].ConsolidatedObjectKey,
		cohorts[1].ConsolidatedObjectKey,
	}, processedKeys)

	var applicationErr *temporal.ApplicationError
	require.ErrorAs(s.T(), s.env.GetWorkflowError(), &applicationErr)
	require.Equal(s.T(), singleBlockRetentionPartialFailureType, applicationErr.Type())
	require.True(s.T(), applicationErr.HasDetails())
	var failureDetails SingleBlockRetentionFailureDetails
	require.NoError(s.T(), applicationErr.Details(&failureDetails))
	require.ErrorContains(s.T(), errors.New(failureDetails.FailureMessage), "did not finish")
	require.Equal(s.T(), 2, failureDetails.Parallelism)
	require.Equal(s.T(), uint64(3), failureDetails.CurrentRunSelectedObjectRanges)
	require.Equal(s.T(), uint64(2), failureDetails.CurrentRunLaunchedObjectRanges)
	require.Equal(s.T(), uint64(3), failureDetails.SelectedObjectRanges)
	require.Equal(s.T(), uint64(2), failureDetails.ProcessedObjectRanges)
	require.Equal(s.T(), uint64(1), failureDetails.CompletedObjectRangeCount)
	require.Len(s.T(), failureDetails.CohortOutcomes, 2)
	require.Equal(s.T(), cohorts[0], failureDetails.CohortOutcomes[0].Cohort)
	require.ErrorContains(
		s.T(),
		errors.New(failureDetails.CohortOutcomes[0].FailureMessage),
		"did not finish",
	)
	require.NotNil(s.T(), failureDetails.CohortOutcomes[0].Result)
	require.Equal(s.T(), cohorts[1], failureDetails.CohortOutcomes[1].Cohort)
	require.Empty(s.T(), failureDetails.CohortOutcomes[1].FailureMessage)
	require.True(s.T(), failureDetails.CohortOutcomes[1].Result.Terminal)
}

func (s *singleBlockRetentionTestSuite) TestExecuteParallelFailureDetailsIncludeEarlierWaves() {
	cohorts := []retirement.RetentionCohort{
		testRetentionCohort("consolidated/100-110.cscb.zstd", 100, 110),
		testRetentionCohort("consolidated/110-120.cscb.zstd", 110, 120),
		testRetentionCohort("consolidated/120-130.cscb.zstd", 120, 130),
	}
	s.env.OnActivity(activity.ActivitySingleBlockRetentionSelect, mock.Anything, mock.Anything).
		Return(&activity.SingleBlockRetentionSelectResponse{Cohorts: cohorts}, nil).
		Once()
	s.env.OnActivity(activity.ActivitySingleBlockRetentionProcess, mock.Anything, mock.Anything).
		Return(func(_ context.Context, request *activity.SingleBlockRetentionProcessRequest) (*activity.SingleBlockRetentionRangeResult, error) {
			if request.Cohort.ConsolidatedObjectKey == cohorts[2].ConsolidatedObjectKey {
				firstIncompleteHeight := request.Cohort.StartHeight
				return &activity.SingleBlockRetentionRangeResult{
					Cohort:                   request.Cohort,
					ScannedRows:              request.Cohort.RowCount,
					AlreadyRetiredRows:       request.Cohort.RowCount - 1,
					FailedRows:               1,
					VerifiedThroughExclusive: request.Cohort.StartHeight,
					FirstIncompleteHeight:    &firstIncompleteHeight,
				}, nil
			}
			return terminalSingleBlockRetentionRangeResult(request.Cohort), nil
		}).
		Times(len(cohorts))

	_, err := s.workflow.Execute(context.Background(), &SingleBlockRetentionRequest{
		Tag:                         2,
		StartHeight:                 100,
		EndHeight:                   130,
		EligibilityCutoff:           testSingleBlockRetentionEligibilityCutoff,
		MaxObjectRanges:             len(cohorts),
		Parallelism:                 2,
		Execute:                     true,
		DirectStorageClientsGuarded: true,
		SingleBlockWritersGuarded:   true,
		FallbackReadsValidated:      true,
		ApprovedChain:               "solana-mainnet",
		ApprovedStartHeight:         100,
		ApprovedEndHeight:           130,
	})
	require.ErrorContains(s.T(), err, "did not finish")

	var applicationErr *temporal.ApplicationError
	require.ErrorAs(s.T(), s.env.GetWorkflowError(), &applicationErr)
	var failureDetails SingleBlockRetentionFailureDetails
	require.NoError(s.T(), applicationErr.Details(&failureDetails))
	require.Equal(s.T(), uint64(3), failureDetails.CurrentRunSelectedObjectRanges)
	require.Equal(s.T(), uint64(3), failureDetails.CurrentRunLaunchedObjectRanges)
	require.Equal(s.T(), uint64(3), failureDetails.ProcessedObjectRanges)
	require.Equal(s.T(), uint64(2), failureDetails.CompletedObjectRangeCount)
	require.Len(s.T(), failureDetails.CohortOutcomes, 3)
	for i, cohort := range cohorts {
		require.Equal(s.T(), cohort, failureDetails.CohortOutcomes[i].Cohort)
	}
	require.Empty(s.T(), failureDetails.CohortOutcomes[0].FailureMessage)
	require.Empty(s.T(), failureDetails.CohortOutcomes[1].FailureMessage)
	require.ErrorContains(
		s.T(),
		errors.New(failureDetails.CohortOutcomes[2].FailureMessage),
		"did not finish",
	)
}

func (s *singleBlockRetentionTestSuite) TestExecuteParallelRetryFailurePreservesLatestValidResult() {
	cohorts := []retirement.RetentionCohort{
		testRetentionCohort("consolidated/100-110.cscb.zstd", 100, 110),
		testRetentionCohort("consolidated/110-120.cscb.zstd", 110, 120),
	}
	s.env.OnActivity(activity.ActivitySingleBlockRetentionSelect, mock.Anything, mock.Anything).
		Return(&activity.SingleBlockRetentionSelectResponse{Cohorts: cohorts}, nil).
		Once()

	var mu sync.Mutex
	attempts := make(map[string]int)
	s.env.OnActivity(activity.ActivitySingleBlockRetentionProcess, mock.Anything, mock.Anything).
		Return(func(_ context.Context, request *activity.SingleBlockRetentionProcessRequest) (*activity.SingleBlockRetentionRangeResult, error) {
			mu.Lock()
			attempts[request.Cohort.ConsolidatedObjectKey]++
			attempt := attempts[request.Cohort.ConsolidatedObjectKey]
			mu.Unlock()
			if request.Cohort.ConsolidatedObjectKey != cohorts[0].ConsolidatedObjectKey {
				return terminalSingleBlockRetentionRangeResult(request.Cohort), nil
			}
			if attempt == 1 {
				firstIncompleteHeight := request.Cohort.StartHeight
				return &activity.SingleBlockRetentionRangeResult{
					Cohort:                   request.Cohort,
					ScannedRows:              request.Cohort.RowCount,
					DeferredRows:             request.Cohort.RowCount,
					VerifiedThroughExclusive: request.Cohort.StartHeight,
					FirstIncompleteHeight:    &firstIncompleteHeight,
					RetryAfter:               time.Minute,
					RetryReason:              retirement.SkipCSCBSafetyQuiescenceActive,
				}, nil
			}
			return nil, temporal.NewNonRetryableApplicationError(
				"retry unavailable",
				"test_retry_failure",
				nil,
			)
		}).
		Times(3)

	_, err := s.workflow.Execute(context.Background(), &SingleBlockRetentionRequest{
		Tag:                         2,
		StartHeight:                 100,
		EndHeight:                   120,
		EligibilityCutoff:           testSingleBlockRetentionEligibilityCutoff,
		MaxObjectRanges:             len(cohorts),
		Parallelism:                 2,
		Execute:                     true,
		DirectStorageClientsGuarded: true,
		SingleBlockWritersGuarded:   true,
		FallbackReadsValidated:      true,
		ApprovedChain:               "solana-mainnet",
		ApprovedStartHeight:         100,
		ApprovedEndHeight:           120,
	})
	require.ErrorContains(s.T(), err, "failed to retry retention cohort")

	var applicationErr *temporal.ApplicationError
	require.ErrorAs(s.T(), s.env.GetWorkflowError(), &applicationErr)
	var failureDetails SingleBlockRetentionFailureDetails
	require.NoError(s.T(), applicationErr.Details(&failureDetails))
	require.Equal(s.T(), uint64(2), failureDetails.CurrentRunLaunchedObjectRanges)
	require.Equal(s.T(), uint64(2), failureDetails.ProcessedObjectRanges)
	require.Equal(s.T(), uint64(1), failureDetails.CompletedObjectRangeCount)
	require.Equal(s.T(), cohorts[0].RowCount, failureDetails.DeferredRows)
	require.Len(s.T(), failureDetails.CohortOutcomes, 2)
	require.Equal(s.T(), cohorts[0], failureDetails.CohortOutcomes[0].Cohort)
	require.NotNil(s.T(), failureDetails.CohortOutcomes[0].Result)
	require.Equal(s.T(), cohorts[0].RowCount, failureDetails.CohortOutcomes[0].Result.DeferredRows)
	require.ErrorContains(
		s.T(),
		errors.New(failureDetails.CohortOutcomes[0].FailureMessage),
		"failed to retry retention cohort",
	)
	require.Equal(s.T(), cohorts[1], failureDetails.CohortOutcomes[1].Cohort)
	require.True(s.T(), failureDetails.CohortOutcomes[1].Result.Terminal)
}

func (s *singleBlockRetentionTestSuite) TestExecuteCompletionProbeFailureIncludesProcessedCohorts() {
	cohorts := []retirement.RetentionCohort{
		testRetentionCohort("consolidated/100-110.cscb.zstd", 100, 110),
		testRetentionCohort("consolidated/110-120.cscb.zstd", 110, 120),
	}
	s.env.OnActivity(activity.ActivitySingleBlockRetentionSelect, mock.Anything, mock.Anything).
		Return(&activity.SingleBlockRetentionSelectResponse{Cohorts: cohorts}, nil).
		Once()
	s.env.OnActivity(activity.ActivitySingleBlockRetentionProcess, mock.Anything, mock.Anything).
		Return(func(_ context.Context, request *activity.SingleBlockRetentionProcessRequest) (*activity.SingleBlockRetentionRangeResult, error) {
			return terminalSingleBlockRetentionRangeResult(request.Cohort), nil
		}).
		Twice()
	s.env.OnActivity(activity.ActivitySingleBlockRetentionSelect, mock.Anything, mock.Anything).
		Return((*activity.SingleBlockRetentionSelectResponse)(nil), errors.New("completion probe unavailable"))

	_, err := s.workflow.Execute(context.Background(), &SingleBlockRetentionRequest{
		Tag:                         2,
		StartHeight:                 100,
		EndHeight:                   120,
		EligibilityCutoff:           testSingleBlockRetentionEligibilityCutoff,
		MaxObjectRanges:             len(cohorts),
		Parallelism:                 2,
		Execute:                     true,
		DirectStorageClientsGuarded: true,
		SingleBlockWritersGuarded:   true,
		FallbackReadsValidated:      true,
		ApprovedChain:               "solana-mainnet",
		ApprovedStartHeight:         100,
		ApprovedEndHeight:           120,
	})
	require.ErrorContains(s.T(), err, "failed to confirm retention sweep completion")

	var applicationErr *temporal.ApplicationError
	require.ErrorAs(s.T(), s.env.GetWorkflowError(), &applicationErr)
	var failureDetails SingleBlockRetentionFailureDetails
	require.NoError(s.T(), applicationErr.Details(&failureDetails))
	require.ErrorContains(
		s.T(),
		errors.New(failureDetails.FailureMessage),
		"completion probe unavailable",
	)
	require.Equal(s.T(), uint64(2), failureDetails.CurrentRunSelectedObjectRanges)
	require.Equal(s.T(), uint64(2), failureDetails.CurrentRunLaunchedObjectRanges)
	require.Equal(s.T(), uint64(2), failureDetails.CompletedObjectRangeCount)
	require.Len(s.T(), failureDetails.CohortOutcomes, 2)
}

func (s *singleBlockRetentionTestSuite) TestLegacyExecutionRejectsParallelism() {
	s.env.OnGetVersion(
		singleBlockRetentionParallelismChangeID,
		temporalworkflow.DefaultVersion,
		singleBlockRetentionParallelismVersion,
	).Return(temporalworkflow.DefaultVersion)

	_, err := s.workflow.Execute(context.Background(), &SingleBlockRetentionRequest{
		Tag:             2,
		StartHeight:     100,
		EndHeight:       120,
		Parallelism:     2,
		MaxObjectRanges: 2,
	})
	require.ErrorContains(s.T(), err, "legacy single_block_retention execution requires parallelism=1")
}

func (s *singleBlockRetentionTestSuite) TestLegacyReplayRejectsContainingApprovalBeforeActivities() {
	s.env.OnGetVersion(
		singleBlockRetentionRangeSweepChangeID,
		temporalworkflow.DefaultVersion,
		singleBlockRetentionRangeSweepVersion,
	).Return(temporalworkflow.DefaultVersion)

	_, err := s.workflow.Execute(context.Background(), &SingleBlockRetentionRequest{
		Tag:                         2,
		StartHeight:                 423300000,
		EndHeight:                   423550000,
		EligibilityCutoff:           testSingleBlockRetentionEligibilityCutoff,
		MaxObjectRanges:             250,
		Parallelism:                 10,
		Execute:                     true,
		ProductionDeleteEnabled:     true,
		DirectStorageClientsGuarded: true,
		SingleBlockWritersGuarded:   true,
		FallbackReadsValidated:      true,
		ApprovedChain:               "solana-mainnet",
		ApprovedStartHeight:         423300000,
		ApprovedEndHeight:           437068000,
	})
	require.ErrorContains(s.T(), err, "must exactly match the selection range")
}

func (s *singleBlockRetentionTestSuite) TestReplayPreParallelismHistory() {
	replayer := worker.NewWorkflowReplayer()
	replayer.RegisterWorkflowWithOptions(s.workflow.execute, temporalworkflow.RegisterOptions{
		Name: s.workflow.name,
	})
	require.NoError(
		s.T(),
		replayer.ReplayWorkflowHistoryFromJSONFile(
			nil,
			"testdata/single_block_retention_pre_parallelism_history.json",
		),
	)
}

func (s *singleBlockRetentionTestSuite) TestExecutionRejectsParallelismAboveMaximum() {
	_, err := s.workflow.Execute(context.Background(), &SingleBlockRetentionRequest{
		Tag:             2,
		StartHeight:     100,
		EndHeight:       120,
		Parallelism:     singleBlockRetentionMaxParallelism + 1,
		MaxObjectRanges: 2,
	})
	require.ErrorContains(s.T(), err, "parallelism(21) exceeds max(20)")
}

func (s *singleBlockRetentionTestSuite) TestLegacyExecuteRequiresExactCohortApproval() {
	cohort := testRetentionCohort("consolidated/100-110.cscb.zstd", 100, 110)
	var selectRequest *activity.SingleBlockRetentionSelectRequest
	s.env.OnGetVersion(
		singleBlockRetentionRangeSweepChangeID,
		temporalworkflow.DefaultVersion,
		singleBlockRetentionRangeSweepVersion,
	).Return(temporalworkflow.DefaultVersion)
	s.env.OnActivity(activity.ActivitySingleBlockRetentionSelect, mock.Anything, mock.Anything).
		Return(func(_ context.Context, request *activity.SingleBlockRetentionSelectRequest) (*activity.SingleBlockRetentionSelectResponse, error) {
			selectRequest = request
			return &activity.SingleBlockRetentionSelectResponse{
				Cohorts: []retirement.RetentionCohort{cohort},
			}, nil
		})

	_, err := s.workflow.Execute(context.Background(), &SingleBlockRetentionRequest{
		Tag:                         0,
		StartHeight:                 100,
		EndHeight:                   210,
		Execute:                     true,
		DirectStorageClientsGuarded: true,
		SingleBlockWritersGuarded:   true,
		FallbackReadsValidated:      true,
		ApprovedChain:               "solana-mainnet",
		ApprovedStartHeight:         100,
		ApprovedEndHeight:           210,
	})
	require.ErrorContains(s.T(), err, "does not exactly match the approved range")
	require.Equal(
		s.T(),
		s.cfg.Workflows.SingleBlockRetention.GetEffectiveBlockTag(0),
		selectRequest.Tag,
	)
	require.True(s.T(), selectRequest.EligibilityCutoff.IsZero())
}

func (s *singleBlockRetentionTestSuite) TestLegacyExecutePinsResolvedTagAcrossActivities() {
	cohort := testRetentionCohort("consolidated/100-110.cscb.zstd", 100, 110)
	effectiveTag := s.cfg.Workflows.SingleBlockRetention.GetEffectiveBlockTag(0)
	s.env.OnGetVersion(
		singleBlockRetentionRangeSweepChangeID,
		temporalworkflow.DefaultVersion,
		singleBlockRetentionRangeSweepVersion,
	).Return(temporalworkflow.DefaultVersion)
	s.env.OnActivity(activity.ActivitySingleBlockRetentionSelect, mock.Anything, mock.Anything).
		Return(func(_ context.Context, request *activity.SingleBlockRetentionSelectRequest) (*activity.SingleBlockRetentionSelectResponse, error) {
			require.Equal(s.T(), effectiveTag, request.Tag)
			require.True(s.T(), request.EligibilityCutoff.IsZero())
			s.cfg.Workflows.SingleBlockRetention.BlockTag.Stable = effectiveTag + 1
			return &activity.SingleBlockRetentionSelectResponse{
				Cohorts: []retirement.RetentionCohort{cohort},
			}, nil
		})
	s.env.OnActivity(activity.ActivitySingleBlockRetentionProcess, mock.Anything, mock.Anything).
		Return(func(_ context.Context, request *activity.SingleBlockRetentionProcessRequest) (*activity.SingleBlockRetentionRangeResult, error) {
			require.Equal(s.T(), effectiveTag, request.Tag)
			require.True(s.T(), request.EligibilityCutoff.IsZero())
			return &activity.SingleBlockRetentionRangeResult{
				Cohort:                   request.Cohort,
				ScannedRows:              request.Cohort.RowCount,
				DeletedVerifiedRows:      request.Cohort.RowCount,
				VerifiedThroughExclusive: request.Cohort.EndHeight,
				Terminal:                 true,
			}, nil
		})

	_, err := s.workflow.Execute(context.Background(), &SingleBlockRetentionRequest{
		StartHeight:                 cohort.StartHeight,
		EndHeight:                   cohort.EndHeight,
		Execute:                     true,
		DirectStorageClientsGuarded: true,
		SingleBlockWritersGuarded:   true,
		FallbackReadsValidated:      true,
		ApprovedChain:               "solana-mainnet",
		ApprovedStartHeight:         cohort.StartHeight,
		ApprovedEndHeight:           cohort.EndHeight,
	})
	require.NoError(s.T(), err)
}

func (s *singleBlockRetentionTestSuite) TestExecuteContinuesAsNewWithCumulativeCheckpoint() {
	cohort := testRetentionCohort("consolidated/100-110.cscb.zstd", 100, 110)
	effectiveTag := s.cfg.Workflows.SingleBlockRetention.GetEffectiveBlockTag(0)
	var selectRequest *activity.SingleBlockRetentionSelectRequest
	var processRequest *activity.SingleBlockRetentionProcessRequest
	s.env.OnGetVersion(
		singleBlockRetentionParallelismChangeID,
		temporalworkflow.DefaultVersion,
		singleBlockRetentionParallelismVersion,
	).Return(temporalworkflow.Version(singleBlockRetentionParallelismVersion))
	s.env.OnActivity(activity.ActivitySingleBlockRetentionSelect, mock.Anything, mock.Anything).
		Return(func(_ context.Context, request *activity.SingleBlockRetentionSelectRequest) (*activity.SingleBlockRetentionSelectResponse, error) {
			selectRequest = request
			return &activity.SingleBlockRetentionSelectResponse{
				Cohorts: []retirement.RetentionCohort{cohort},
				HasMore: true,
			}, nil
		})
	s.env.OnActivity(activity.ActivitySingleBlockRetentionProcess, mock.Anything, mock.Anything).
		Return(func(_ context.Context, request *activity.SingleBlockRetentionProcessRequest) (*activity.SingleBlockRetentionRangeResult, error) {
			processRequest = request
			return &activity.SingleBlockRetentionRangeResult{
				Cohort:                   cohort,
				ScannedRows:              10,
				DeletedVerifiedRows:      10,
				DeletedVersions:          10,
				RetiredBytes:             1000,
				VerifiedThroughExclusive: 110,
				Terminal:                 true,
			}, nil
		})

	_, err := s.workflow.Execute(context.Background(), &SingleBlockRetentionRequest{
		Tag:                         0,
		StartHeight:                 100,
		EndHeight:                   120,
		EligibilityCutoff:           testSingleBlockRetentionEligibilityCutoff,
		MaxObjectRanges:             1,
		Parallelism:                 2,
		Execute:                     true,
		DirectStorageClientsGuarded: true,
		SingleBlockWritersGuarded:   true,
		FallbackReadsValidated:      true,
		ApprovedChain:               "solana-mainnet",
		ApprovedStartHeight:         90,
		ApprovedEndHeight:           130,
	})
	require.Error(s.T(), err)
	require.True(s.T(), IsContinueAsNewError(err))

	var continueAsNewErr *temporalworkflow.ContinueAsNewError
	require.True(s.T(), errors.As(err, &continueAsNewErr))
	var nextRequest SingleBlockRetentionRequest
	require.NoError(
		s.T(),
		converter.GetDefaultDataConverter().FromPayloads(continueAsNewErr.Input, &nextRequest),
	)
	require.Equal(s.T(), uint64(100), nextRequest.StartHeight)
	require.Equal(s.T(), uint64(120), nextRequest.EndHeight)
	require.Equal(s.T(), uint64(90), nextRequest.ApprovedStartHeight)
	require.Equal(s.T(), uint64(130), nextRequest.ApprovedEndHeight)
	require.Equal(s.T(), 2, nextRequest.Parallelism)
	require.Equal(s.T(), encodeSingleBlockRetentionEffectiveTag(effectiveTag), nextRequest.Tag)
	require.Equal(s.T(), encodeSingleBlockRetentionEffectiveTag(effectiveTag), selectRequest.Tag)
	require.Equal(s.T(), encodeSingleBlockRetentionEffectiveTag(effectiveTag), processRequest.Tag)
	require.Equal(s.T(), testSingleBlockRetentionEligibilityCutoff, processRequest.EligibilityCutoff)
	require.Equal(s.T(), uint64(90), processRequest.ApprovedStartHeight)
	require.Equal(s.T(), uint64(130), processRequest.ApprovedEndHeight)
	require.NotNil(s.T(), nextRequest.Checkpoint)
	require.False(s.T(), selectRequest.EligibilityCutoff.IsZero())
	require.Equal(s.T(), selectRequest.EligibilityCutoff, nextRequest.Checkpoint.EligibilityCutoff)
	require.Equal(s.T(), testSingleBlockRetentionEligibilityCutoff, nextRequest.Checkpoint.EligibilityCutoff)
	require.Equal(s.T(), effectiveTag, nextRequest.Checkpoint.EffectiveTag)
	require.Equal(s.T(), uint64(1), nextRequest.Checkpoint.ContinueAsNewCount)
	require.Equal(s.T(), uint64(1), nextRequest.Checkpoint.SelectedObjectRanges)
	require.Equal(s.T(), uint64(1), nextRequest.Checkpoint.ProcessedObjectRanges)
	require.Equal(s.T(), uint64(1), nextRequest.Checkpoint.CompletedObjectRangeCount)
	require.Equal(s.T(), uint64(10), nextRequest.Checkpoint.DeletedVerifiedRows)
	require.Equal(s.T(), uint64(10), nextRequest.Checkpoint.DeletedVersions)
	require.Equal(s.T(), uint64(1000), nextRequest.Checkpoint.RetiredBytes)
	require.Equal(s.T(), uint64(110), nextRequest.Checkpoint.LastCompletedObjectRange.EndHeight)
}

func TestEncodeSingleBlockRetentionEffectiveTag(t *testing.T) {
	require.Equal(t, uint32(math.MaxUint32), encodeSingleBlockRetentionEffectiveTag(0))
	require.Equal(t, uint32(2), encodeSingleBlockRetentionEffectiveTag(2))
}

func (s *singleBlockRetentionTestSuite) TestExecuteContinuesAsNewWhenCompletionProbeFindsBacklog() {
	first := testRetentionCohort("consolidated/100-110.cscb.zstd", 100, 110)
	second := testRetentionCohort("consolidated/110-120.cscb.zstd", 110, 120)
	s.env.OnActivity(activity.ActivitySingleBlockRetentionSelect, mock.Anything, mock.Anything).
		Return(&activity.SingleBlockRetentionSelectResponse{
			Cohorts: []retirement.RetentionCohort{first},
		}, nil).
		Once()
	s.env.OnActivity(activity.ActivitySingleBlockRetentionProcess, mock.Anything, mock.Anything).
		Return(&activity.SingleBlockRetentionRangeResult{
			Cohort:                   first,
			ScannedRows:              first.RowCount,
			DeletedVerifiedRows:      first.RowCount,
			VerifiedThroughExclusive: first.EndHeight,
			Terminal:                 true,
		}, nil).
		Once()
	s.env.OnActivity(activity.ActivitySingleBlockRetentionSelect, mock.Anything, mock.Anything).
		Return(&activity.SingleBlockRetentionSelectResponse{
			Cohorts: []retirement.RetentionCohort{second},
		}, nil).
		Once()

	_, err := s.workflow.Execute(context.Background(), &SingleBlockRetentionRequest{
		Tag:                         2,
		StartHeight:                 100,
		EndHeight:                   120,
		EligibilityCutoff:           testSingleBlockRetentionEligibilityCutoff,
		MaxObjectRanges:             1,
		Execute:                     true,
		DirectStorageClientsGuarded: true,
		SingleBlockWritersGuarded:   true,
		FallbackReadsValidated:      true,
		ApprovedChain:               "solana-mainnet",
		ApprovedStartHeight:         100,
		ApprovedEndHeight:           120,
	})
	require.Error(s.T(), err)
	require.True(s.T(), IsContinueAsNewError(err))

	var continueAsNewErr *temporalworkflow.ContinueAsNewError
	require.True(s.T(), errors.As(err, &continueAsNewErr))
	var nextRequest SingleBlockRetentionRequest
	require.NoError(
		s.T(),
		converter.GetDefaultDataConverter().FromPayloads(continueAsNewErr.Input, &nextRequest),
	)
	require.NotNil(s.T(), nextRequest.Checkpoint)
	require.Equal(s.T(), uint64(1), nextRequest.Checkpoint.ContinueAsNewCount)
	require.Equal(s.T(), uint64(1), nextRequest.Checkpoint.CompletedObjectRangeCount)
	require.Equal(s.T(), first.EndHeight, nextRequest.Checkpoint.LastCompletedObjectRange.EndHeight)
}

func (s *singleBlockRetentionTestSuite) TestExecutePreservesExplicitTagZeroEncoding() {
	cohort := testRetentionCohort("consolidated/100-110.cscb.zstd", 100, 110)
	selectCalls := 0
	s.env.OnActivity(activity.ActivitySingleBlockRetentionSelect, mock.Anything, mock.Anything).
		Return(func(_ context.Context, request *activity.SingleBlockRetentionSelectRequest) (*activity.SingleBlockRetentionSelectResponse, error) {
			require.Equal(s.T(), uint32(math.MaxUint32), request.Tag)
			selectCalls++
			if selectCalls == 1 {
				return &activity.SingleBlockRetentionSelectResponse{
					Cohorts: []retirement.RetentionCohort{cohort},
				}, nil
			}
			return &activity.SingleBlockRetentionSelectResponse{}, nil
		}).
		Twice()
	s.env.OnActivity(activity.ActivitySingleBlockRetentionProcess, mock.Anything, mock.Anything).
		Return(func(_ context.Context, request *activity.SingleBlockRetentionProcessRequest) (*activity.SingleBlockRetentionRangeResult, error) {
			require.Equal(s.T(), uint32(math.MaxUint32), request.Tag)
			return &activity.SingleBlockRetentionRangeResult{
				Cohort:                   request.Cohort,
				ScannedRows:              request.Cohort.RowCount,
				DeletedVerifiedRows:      request.Cohort.RowCount,
				VerifiedThroughExclusive: request.Cohort.EndHeight,
				Terminal:                 true,
			}, nil
		}).
		Once()

	_, err := s.workflow.Execute(context.Background(), &SingleBlockRetentionRequest{
		Tag:                         math.MaxUint32,
		StartHeight:                 100,
		EndHeight:                   110,
		EligibilityCutoff:           testSingleBlockRetentionEligibilityCutoff,
		Execute:                     true,
		DirectStorageClientsGuarded: true,
		SingleBlockWritersGuarded:   true,
		FallbackReadsValidated:      true,
		ApprovedChain:               "solana-mainnet",
		ApprovedStartHeight:         100,
		ApprovedEndHeight:           110,
	})
	require.NoError(s.T(), err)

	var result SingleBlockRetentionResult
	require.NoError(s.T(), s.env.GetWorkflowResult(&result))
	require.Equal(s.T(), uint32(0), result.Tag)
	require.True(s.T(), result.SweepCompleted)
}

func (s *singleBlockRetentionTestSuite) TestContinuationReturnsCumulativeFinalResult() {
	startedAt := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	firstCompleted := &SingleBlockRetentionCompletedRange{
		ConsolidatedObjectKey: "consolidated/100-110.cscb.zstd",
		StartHeight:           100,
		EndHeight:             110,
		EligibleRows:          10,
	}
	second := testRetentionCohort("consolidated/110-120.cscb.zstd", 110, 120)
	s.env.SetContinuedExecutionRunID("previous-run")
	s.env.OnActivity(activity.ActivitySingleBlockRetentionSelect, mock.Anything, mock.Anything).
		Return(&activity.SingleBlockRetentionSelectResponse{
			Cohorts: []retirement.RetentionCohort{second},
		}, nil).
		Once()
	s.env.OnActivity(activity.ActivitySingleBlockRetentionSelect, mock.Anything, mock.Anything).
		Return(&activity.SingleBlockRetentionSelectResponse{}, nil).
		Once()
	s.env.OnActivity(activity.ActivitySingleBlockRetentionProcess, mock.Anything, mock.Anything).
		Return(&activity.SingleBlockRetentionRangeResult{
			Cohort:                   second,
			ScannedRows:              10,
			DeletedVerifiedRows:      10,
			DeletedVersions:          10,
			RetiredBytes:             1000,
			VerifiedThroughExclusive: 120,
			Terminal:                 true,
		}, nil)

	_, err := s.workflow.Execute(context.Background(), &SingleBlockRetentionRequest{
		Tag:                         2,
		StartHeight:                 100,
		EndHeight:                   120,
		EligibilityCutoff:           startedAt,
		MaxObjectRanges:             1,
		Execute:                     true,
		DirectStorageClientsGuarded: true,
		SingleBlockWritersGuarded:   true,
		FallbackReadsValidated:      true,
		ApprovedChain:               "solana-mainnet",
		ApprovedStartHeight:         100,
		ApprovedEndHeight:           120,
		Checkpoint: &SingleBlockRetentionCheckpoint{
			StartedAt:                 startedAt,
			EligibilityCutoff:         startedAt,
			EffectiveTag:              2,
			ContinueAsNewCount:        1,
			SelectedObjectRanges:      1,
			ProcessedObjectRanges:     1,
			CompletedObjectRangeCount: 1,
			ScannedRows:               10,
			DeletedVerifiedRows:       10,
			DeletedVersions:           10,
			RetiredBytes:              1000,
			LastCompletedObjectRange:  firstCompleted,
		},
	})
	require.NoError(s.T(), err)

	var result SingleBlockRetentionResult
	require.NoError(s.T(), s.env.GetWorkflowResult(&result))
	require.Equal(s.T(), startedAt, result.StartedAt)
	require.Equal(s.T(), startedAt, result.EligibilityCutoff)
	require.True(s.T(), result.SweepCompleted)
	require.False(s.T(), result.MoreEligibleRanges)
	require.Equal(s.T(), uint64(1), result.ContinueAsNewCount)
	require.Equal(s.T(), uint64(2), result.SelectedObjectRanges)
	require.Equal(s.T(), uint64(2), result.ProcessedObjectRanges)
	require.Equal(s.T(), uint64(2), result.CompletedObjectRangeCount)
	require.Equal(s.T(), uint64(20), result.DeletedVerifiedRows)
	require.Equal(s.T(), uint64(20), result.DeletedVersions)
	require.Equal(s.T(), uint64(2000), result.RetiredBytes)
	require.Equal(s.T(), second.EndHeight, result.LastCompletedObjectRange.EndHeight)
	require.Equal(s.T(), []SingleBlockRetentionCompletedRange{
		{
			ConsolidatedObjectKey: second.ConsolidatedObjectKey,
			StartHeight:           second.StartHeight,
			EndHeight:             second.EndHeight,
			EligibleRows:          second.RowCount,
		},
	}, result.CompletedObjectRanges)
	require.Len(s.T(), result.RangeResults, 1)
}

func (s *singleBlockRetentionTestSuite) TestInitialExecutionRejectsCallerSuppliedCheckpoint() {
	_, err := s.workflow.Execute(context.Background(), &SingleBlockRetentionRequest{
		Tag:                         2,
		StartHeight:                 100,
		EndHeight:                   120,
		EligibilityCutoff:           testSingleBlockRetentionEligibilityCutoff,
		Execute:                     true,
		DirectStorageClientsGuarded: true,
		SingleBlockWritersGuarded:   true,
		FallbackReadsValidated:      true,
		ApprovedChain:               "solana-mainnet",
		ApprovedStartHeight:         100,
		ApprovedEndHeight:           120,
		Checkpoint: &SingleBlockRetentionCheckpoint{
			StartedAt:          time.Now().UTC(),
			ContinueAsNewCount: 1,
		},
	})
	require.ErrorContains(s.T(), err, "checkpoint is internal")
}

func (s *singleBlockRetentionTestSuite) TestRangeExecutionRequiresApprovedDryRunCutoff() {
	_, err := s.workflow.Execute(context.Background(), &SingleBlockRetentionRequest{
		Tag:                         2,
		StartHeight:                 100,
		EndHeight:                   120,
		Execute:                     true,
		DirectStorageClientsGuarded: true,
		SingleBlockWritersGuarded:   true,
		FallbackReadsValidated:      true,
		ApprovedChain:               "solana-mainnet",
		ApprovedStartHeight:         100,
		ApprovedEndHeight:           120,
	})
	require.ErrorContains(s.T(), err, "eligibility cutoff from its approved dry run")
}

func (s *singleBlockRetentionTestSuite) TestContinuationRejectsChangedEligibilityCutoff() {
	startedAt := testSingleBlockRetentionEligibilityCutoff.Add(time.Hour)
	s.env.SetContinuedExecutionRunID("previous-run")

	_, err := s.workflow.Execute(context.Background(), &SingleBlockRetentionRequest{
		Tag:                         2,
		StartHeight:                 100,
		EndHeight:                   120,
		EligibilityCutoff:           testSingleBlockRetentionEligibilityCutoff.Add(-time.Minute),
		Execute:                     true,
		DirectStorageClientsGuarded: true,
		SingleBlockWritersGuarded:   true,
		FallbackReadsValidated:      true,
		ApprovedChain:               "solana-mainnet",
		ApprovedStartHeight:         100,
		ApprovedEndHeight:           120,
		Checkpoint: &SingleBlockRetentionCheckpoint{
			StartedAt:          startedAt,
			EligibilityCutoff:  testSingleBlockRetentionEligibilityCutoff,
			EffectiveTag:       2,
			ContinueAsNewCount: 1,
		},
	})
	require.ErrorContains(s.T(), err, "eligibility cutoff changed across continuation")
}

func (s *singleBlockRetentionTestSuite) TestExecuteRetriesSafetyQuiescenceOnce() {
	cohort := testRetentionCohort("consolidated/100-110.cscb.zstd", 100, 110)
	s.env.OnActivity(activity.ActivitySingleBlockRetentionSelect, mock.Anything, mock.Anything).
		Return(&activity.SingleBlockRetentionSelectResponse{
			Cohorts: []retirement.RetentionCohort{cohort},
		}, nil).
		Once()
	s.env.OnActivity(activity.ActivitySingleBlockRetentionSelect, mock.Anything, mock.Anything).
		Return(&activity.SingleBlockRetentionSelectResponse{}, nil).
		Once()
	attempt := 0
	s.env.OnActivity(activity.ActivitySingleBlockRetentionProcess, mock.Anything, mock.Anything).
		Return(func(_ context.Context, request *activity.SingleBlockRetentionProcessRequest) (*activity.SingleBlockRetentionRangeResult, error) {
			attempt++
			if attempt == 1 {
				firstIncompleteHeight := request.Cohort.StartHeight
				return &activity.SingleBlockRetentionRangeResult{
					Cohort:                   request.Cohort,
					ScannedRows:              request.Cohort.RowCount,
					DeferredRows:             request.Cohort.RowCount,
					VerifiedThroughExclusive: request.Cohort.StartHeight,
					FirstIncompleteHeight:    &firstIncompleteHeight,
					RetryAfter:               time.Minute,
					RetryReason:              retirement.SkipCSCBSafetyQuiescenceActive,
				}, nil
			}
			return &activity.SingleBlockRetentionRangeResult{
				Cohort:                   request.Cohort,
				ScannedRows:              request.Cohort.RowCount,
				DeletedVerifiedRows:      request.Cohort.RowCount,
				VerifiedThroughExclusive: request.Cohort.EndHeight,
				Terminal:                 true,
			}, nil
		}).Twice()

	_, err := s.workflow.Execute(context.Background(), &SingleBlockRetentionRequest{
		Tag:                         2,
		StartHeight:                 100,
		EndHeight:                   110,
		EligibilityCutoff:           testSingleBlockRetentionEligibilityCutoff,
		Execute:                     true,
		DirectStorageClientsGuarded: true,
		SingleBlockWritersGuarded:   true,
		FallbackReadsValidated:      true,
		ApprovedChain:               "solana-mainnet",
		ApprovedStartHeight:         100,
		ApprovedEndHeight:           110,
	})
	require.NoError(s.T(), err)
	require.Equal(s.T(), 2, attempt)
}

func (s *singleBlockRetentionTestSuite) TestExecuteFailsClosedWhenRangeIsIncomplete() {
	cohort := testRetentionCohort("consolidated/100-110.cscb.zstd", 100, 110)
	firstIncompleteHeight := uint64(105)
	s.env.OnActivity(activity.ActivitySingleBlockRetentionSelect, mock.Anything, mock.Anything).
		Return(&activity.SingleBlockRetentionSelectResponse{
			Cohorts: []retirement.RetentionCohort{cohort},
		}, nil)
	s.env.OnActivity(activity.ActivitySingleBlockRetentionProcess, mock.Anything, mock.Anything).
		Return(&activity.SingleBlockRetentionRangeResult{
			Cohort:                   cohort,
			ScannedRows:              10,
			AlreadyRetiredRows:       9,
			FailedRows:               1,
			VerifiedThroughExclusive: 105,
			FirstIncompleteHeight:    &firstIncompleteHeight,
		}, nil)

	_, err := s.workflow.Execute(context.Background(), &SingleBlockRetentionRequest{
		Tag:                         2,
		StartHeight:                 100,
		EndHeight:                   110,
		EligibilityCutoff:           testSingleBlockRetentionEligibilityCutoff,
		Execute:                     true,
		DirectStorageClientsGuarded: true,
		SingleBlockWritersGuarded:   true,
		FallbackReadsValidated:      true,
		ApprovedChain:               "solana-mainnet",
		ApprovedStartHeight:         100,
		ApprovedEndHeight:           110,
	})
	require.ErrorContains(s.T(), err, "did not finish")
}

func TestValidateSingleBlockRetentionExecutionRequestGates(t *testing.T) {
	validRequest := func() *SingleBlockRetentionRequest {
		return &SingleBlockRetentionRequest{
			StartHeight:                 100,
			EndHeight:                   110,
			EligibilityCutoff:           testSingleBlockRetentionEligibilityCutoff,
			Execute:                     true,
			DirectStorageClientsGuarded: true,
			SingleBlockWritersGuarded:   true,
			FallbackReadsValidated:      true,
			ApprovedChain:               "solana-mainnet",
			ApprovedStartHeight:         100,
			ApprovedEndHeight:           110,
		}
	}
	require.NoError(t, validateSingleBlockRetentionExecutionRequest(validRequest(), true))

	campaignRequest := validRequest()
	campaignRequest.StartHeight = 423300000
	campaignRequest.EndHeight = 423550000
	campaignRequest.ApprovedStartHeight = 423300000
	campaignRequest.ApprovedEndHeight = 437068000
	require.NoError(t, validateSingleBlockRetentionExecutionRequest(campaignRequest, true))
	require.ErrorContains(
		t,
		validateSingleBlockRetentionExecutionRequest(campaignRequest, false),
		"must exactly match the selection range",
	)

	request := validRequest()
	request.StartHeight = 0
	request.EndHeight = 0
	require.ErrorContains(
		t,
		validateSingleBlockRetentionExecutionRequest(request, true),
		"explicit exact selection range",
	)

	request = validRequest()
	request.ApprovedChain = ""
	require.ErrorContains(
		t,
		validateSingleBlockRetentionExecutionRequest(request, true),
		"operator approval chain",
	)

	request = validRequest()
	request.ApprovedStartHeight = 110
	request.ApprovedEndHeight = 110
	require.ErrorContains(
		t,
		validateSingleBlockRetentionExecutionRequest(request, true),
		"valid approved range",
	)

	request = validRequest()
	request.ApprovedStartHeight = 101
	require.ErrorContains(
		t,
		validateSingleBlockRetentionExecutionRequest(request, true),
		"outside the approved envelope",
	)

	request = validRequest()
	request.ApprovedEndHeight = 109
	require.ErrorContains(
		t,
		validateSingleBlockRetentionExecutionRequest(request, true),
		"outside the approved envelope",
	)

	request = validRequest()
	request.FallbackReadsValidated = false
	require.ErrorContains(
		t,
		validateSingleBlockRetentionExecutionRequest(request, true),
		"explicit fallback-disabled read validation",
	)

	request = validRequest()
	request.FallbackErrorCount = 3
	require.ErrorContains(
		t,
		validateSingleBlockRetentionExecutionRequest(request, true),
		"zero fallback read errors",
	)
}

func TestValidateApprovedSingleBlockRetentionCohort(t *testing.T) {
	request := &SingleBlockRetentionRequest{
		ApprovedStartHeight: 100,
		ApprovedEndHeight:   120,
	}
	require.NoError(t, validateApprovedSingleBlockRetentionCohort(
		testRetentionCohort("consolidated/105-110.cscb.zstd", 105, 110),
		request,
		true,
	))
	require.ErrorContains(
		t,
		validateApprovedSingleBlockRetentionCohort(
			testRetentionCohort("consolidated/99-110.cscb.zstd", 99, 110),
			request,
			true,
		),
		"outside the approved envelope",
	)
	require.NoError(t, validateApprovedSingleBlockRetentionCohort(
		testRetentionCohort("consolidated/100-120.cscb.zstd", 100, 120),
		request,
		false,
	))
	require.ErrorContains(
		t,
		validateApprovedSingleBlockRetentionCohort(
			testRetentionCohort("consolidated/105-110.cscb.zstd", 105, 110),
			request,
			false,
		),
		"does not exactly match the approved range",
	)
}

func TestValidateSingleBlockRetentionCohorts(t *testing.T) {
	request := &SingleBlockRetentionRequest{
		StartHeight: 100,
		EndHeight:   130,
	}
	first := testRetentionCohort("consolidated/100-110.cscb.zstd", 100, 110)
	second := testRetentionCohort("consolidated/110-120.cscb.zstd", 110, 120)
	require.NoError(t, validateSingleBlockRetentionCohorts(
		[]retirement.RetentionCohort{second, first},
		request,
		true,
	))

	duplicate := second
	duplicate.ConsolidatedObjectKey = first.ConsolidatedObjectKey
	require.ErrorContains(t, validateSingleBlockRetentionCohorts(
		[]retirement.RetentionCohort{first, duplicate},
		request,
		true,
	), "duplicate CSCB object")
}

func TestValidateSingleBlockRetentionRangeResult(t *testing.T) {
	cohort := testRetentionCohort("consolidated/100-110.cscb.zstd", 100, 110)
	require.NoError(t, validateSingleBlockRetentionRangeResult(
		cohort,
		terminalSingleBlockRetentionRangeResult(cohort),
	))
	mismatch := terminalSingleBlockRetentionRangeResult(cohort)
	mismatch.Cohort.EndHeight++
	require.ErrorContains(t, validateSingleBlockRetentionRangeResult(cohort, mismatch), "does not match request")

	incompleteTerminal := terminalSingleBlockRetentionRangeResult(cohort)
	incompleteTerminal.VerifiedThroughExclusive--
	require.ErrorContains(
		t,
		validateSingleBlockRetentionRangeResult(cohort, incompleteTerminal),
		"inconsistent terminal progress",
	)

	firstIncompleteHeight := cohort.EndHeight - 1
	contradictoryTerminal := terminalSingleBlockRetentionRangeResult(cohort)
	contradictoryTerminal.VerifiedThroughExclusive = firstIncompleteHeight
	contradictoryTerminal.FirstIncompleteHeight = &firstIncompleteHeight
	require.ErrorContains(
		t,
		validateSingleBlockRetentionRangeResult(cohort, contradictoryTerminal),
		"inconsistent incomplete progress",
	)

	impossibleAccounting := terminalSingleBlockRetentionRangeResult(cohort)
	impossibleAccounting.DeletedVerifiedRows++
	require.ErrorContains(
		t,
		validateSingleBlockRetentionRangeResult(cohort, impossibleAccounting),
		"row accounting exceeds scanned rows",
	)
}

func testRetentionCohort(key string, start uint64, end uint64) retirement.RetentionCohort {
	return retirement.RetentionCohort{
		ConsolidatedObjectKey: key,
		StartHeight:           start,
		EndHeight:             end,
		RowCount:              end - start,
		EligibleAt:            testSingleBlockRetentionEligibilityCutoff,
	}
}

func terminalSingleBlockRetentionRangeResult(
	cohort retirement.RetentionCohort,
) *activity.SingleBlockRetentionRangeResult {
	return &activity.SingleBlockRetentionRangeResult{
		Cohort:                   cohort,
		ScannedRows:              cohort.RowCount,
		DeletedVerifiedRows:      cohort.RowCount,
		DeletedVersions:          cohort.RowCount,
		VerifiedThroughExclusive: cohort.EndHeight,
		Terminal:                 true,
	}
}

// deferredSingleBlockRetentionRangeResult is the activity result for a cohort
// that was scanned and wholly deferred for the given reason.
func deferredSingleBlockRetentionRangeResult(cohort retirement.RetentionCohort, reason string) *activity.SingleBlockRetentionRangeResult {
	firstIncompleteHeight := cohort.StartHeight
	return &activity.SingleBlockRetentionRangeResult{
		Cohort:                   cohort,
		ScannedRows:              cohort.RowCount,
		DeferredRows:             cohort.RowCount,
		VerifiedThroughExclusive: cohort.StartHeight,
		FirstIncompleteHeight:    &firstIncompleteHeight,
		RetryAfter:               time.Minute,
		RetryReason:              reason,
	}
}

func singleCohortRetentionRequest(cohort retirement.RetentionCohort) *SingleBlockRetentionRequest {
	return &SingleBlockRetentionRequest{
		Tag:                         2,
		StartHeight:                 cohort.StartHeight,
		EndHeight:                   cohort.EndHeight,
		EligibilityCutoff:           testSingleBlockRetentionEligibilityCutoff,
		MaxObjectRanges:             1,
		Parallelism:                 1,
		Execute:                     true,
		DirectStorageClientsGuarded: true,
		SingleBlockWritersGuarded:   true,
		FallbackReadsValidated:      true,
		ApprovedChain:               "solana-mainnet",
		ApprovedStartHeight:         cohort.StartHeight,
		ApprovedEndHeight:           cohort.EndHeight,
	}
}

// TestExecuteRetriesDeferredCohortAcrossReasons is the INF-1603 shape: a
// cohort deferred for the safety quiescence and then, on its retry, for a
// claim a restarted worker left behind — two transient causes — must still
// complete on the next retry instead of failing the sweep.
func (s *singleBlockRetentionTestSuite) TestExecuteRetriesDeferredCohortAcrossReasons() {
	cohort := testRetentionCohort("consolidated/100-110.cscb.zstd", 100, 110)
	s.env.OnActivity(activity.ActivitySingleBlockRetentionSelect, mock.Anything, mock.Anything).
		Return(&activity.SingleBlockRetentionSelectResponse{Cohorts: []retirement.RetentionCohort{cohort}}, nil).
		Once()
	s.env.OnActivity(activity.ActivitySingleBlockRetentionSelect, mock.Anything, mock.Anything).
		Return(&activity.SingleBlockRetentionSelectResponse{}, nil).
		Once()
	reasons := []string{retirement.SkipCSCBSafetyQuiescenceActive, retirement.SkipRetirementClaimActive}
	attempt := 0
	s.env.OnActivity(activity.ActivitySingleBlockRetentionProcess, mock.Anything, mock.Anything).
		Return(func(_ context.Context, request *activity.SingleBlockRetentionProcessRequest) (*activity.SingleBlockRetentionRangeResult, error) {
			attempt++
			if attempt <= len(reasons) {
				return deferredSingleBlockRetentionRangeResult(request.Cohort, reasons[attempt-1]), nil
			}
			return terminalSingleBlockRetentionRangeResult(request.Cohort), nil
		}).
		Times(3)
	_, err := s.workflow.Execute(context.Background(), singleCohortRetentionRequest(cohort))
	require.NoError(s.T(), err)
	require.Equal(s.T(), 3, attempt)
	var result SingleBlockRetentionResult
	require.NoError(s.T(), s.env.GetWorkflowResult(&result))
	require.Equal(s.T(), uint64(1), result.CompletedObjectRangeCount)
}

// TestExecuteFailsCohortAfterDeferralRetryBudget pins the bound: the budget
// is a total, not per reason, so a cohort that stays deferred fails the sweep
// after maxSingleBlockRetentionDeferralRetries retries.
func (s *singleBlockRetentionTestSuite) TestExecuteFailsCohortAfterDeferralRetryBudget() {
	cohort := testRetentionCohort("consolidated/100-110.cscb.zstd", 100, 110)
	s.env.OnActivity(activity.ActivitySingleBlockRetentionSelect, mock.Anything, mock.Anything).
		Return(&activity.SingleBlockRetentionSelectResponse{Cohorts: []retirement.RetentionCohort{cohort}}, nil).
		Once()
	attempt := 0
	s.env.OnActivity(activity.ActivitySingleBlockRetentionProcess, mock.Anything, mock.Anything).
		Return(func(_ context.Context, request *activity.SingleBlockRetentionProcessRequest) (*activity.SingleBlockRetentionRangeResult, error) {
			attempt++
			return deferredSingleBlockRetentionRangeResult(request.Cohort, retirement.SkipRetirementClaimActive), nil
		}).
		Times(1 + maxSingleBlockRetentionDeferralRetries)
	_, err := s.workflow.Execute(context.Background(), singleCohortRetentionRequest(cohort))
	require.Error(s.T(), err)
	require.Contains(s.T(), err.Error(), "remained deferred after bounded retry (3): retirement_claim_active")
	require.Equal(s.T(), 1+maxSingleBlockRetentionDeferralRetries, attempt)
}

// TestExecuteKeepsSingleDeferralRetryOnLegacyVersion: a run whose history
// predates the retry budget must replay with the one retry it recorded.
func (s *singleBlockRetentionTestSuite) TestExecuteKeepsSingleDeferralRetryOnLegacyVersion() {
	s.env.OnGetVersion(singleBlockRetentionDeferralRetriesChangeID, temporalworkflow.DefaultVersion, singleBlockRetentionDeferralRetriesVersion).
		Return(temporalworkflow.DefaultVersion)
	cohort := testRetentionCohort("consolidated/100-110.cscb.zstd", 100, 110)
	s.env.OnActivity(activity.ActivitySingleBlockRetentionSelect, mock.Anything, mock.Anything).
		Return(&activity.SingleBlockRetentionSelectResponse{Cohorts: []retirement.RetentionCohort{cohort}}, nil).
		Once()
	attempt := 0
	s.env.OnActivity(activity.ActivitySingleBlockRetentionProcess, mock.Anything, mock.Anything).
		Return(func(_ context.Context, request *activity.SingleBlockRetentionProcessRequest) (*activity.SingleBlockRetentionRangeResult, error) {
			attempt++
			return deferredSingleBlockRetentionRangeResult(request.Cohort, retirement.SkipCSCBSafetyQuiescenceActive), nil
		}).
		Times(2)
	_, err := s.workflow.Execute(context.Background(), singleCohortRetentionRequest(cohort))
	require.Error(s.T(), err)
	require.Contains(s.T(), err.Error(), "remained deferred after bounded retry (1)")
	require.Equal(s.T(), 2, attempt)
}
