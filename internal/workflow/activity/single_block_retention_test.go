package activity

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/storage/retirement"
	"github.com/coinbase/chainstorage/protos/coinbase/c3/common"
)

func TestSummarizeSingleBlockRetentionReportReturnsExactCompletedRange(t *testing.T) {
	cohort := retirement.RetentionCohort{
		ConsolidatedObjectKey: "consolidated/100-104.cscb.zstd",
		StartHeight:           100,
		EndHeight:             104,
		RowCount:              3,
		EligibleAt:            time.Now().Add(-time.Hour),
	}
	report := &retirement.Report{
		StartHeight: 100,
		EndHeight:   104,
		Items: []retirement.Candidate{
			{
				Height:           100,
				ConsolidatedKey:  cohort.ConsolidatedObjectKey,
				Action:           retirement.ActionDeletedVerified,
				VersionIDs:       []string{"v1", "v2"},
				SingleBlockBytes: 1000,
			},
			{
				Height:          101,
				ConsolidatedKey: cohort.ConsolidatedObjectKey,
				Action:          retirement.ActionAlreadyDeleted,
			},
			{
				Height:     102,
				Action:     retirement.ActionSkip,
				SkipReason: retirement.SkipSkippedBlock,
			},
			{
				Height:                 103,
				ConsolidatedKey:        cohort.ConsolidatedObjectKey,
				Action:                 retirement.ActionDeletedVerified,
				VersionIDs:             []string{"v3"},
				DeleteMarkerVersionIDs: []string{"dm1"},
				SingleBlockBytes:       2000,
			},
		},
	}

	result, err := summarizeSingleBlockRetentionReport(cohort, report)
	require.NoError(t, err)
	require.True(t, result.Terminal)
	require.Nil(t, result.FirstIncompleteHeight)
	require.Equal(t, uint64(104), result.VerifiedThroughExclusive)
	require.Equal(t, uint64(2), result.DeletedVerifiedRows)
	require.Equal(t, uint64(1), result.AlreadyRetiredRows)
	require.Equal(t, uint64(1), result.SkippedSlots)
	require.Equal(t, uint64(3), result.DeletedVersions)
	require.Equal(t, uint64(1), result.DeletedMarkers)
	require.Equal(t, uint64(3000), result.RetiredBytes)
	require.Empty(t, result.Blockers)
}

func TestSummarizeSingleBlockRetentionReportIdentifiesFirstIncompleteHeight(t *testing.T) {
	cohort := retirement.RetentionCohort{
		ConsolidatedObjectKey: "consolidated/100-103.cscb.zstd",
		StartHeight:           100,
		EndHeight:             103,
		RowCount:              3,
		EligibleAt:            time.Now().Add(-time.Hour),
	}
	report := &retirement.Report{
		StartHeight: 100,
		EndHeight:   103,
		Items: []retirement.Candidate{
			{
				Height:          100,
				ConsolidatedKey: cohort.ConsolidatedObjectKey,
				Action:          retirement.ActionDeletedVerified,
			},
			{
				Height:          101,
				ConsolidatedKey: cohort.ConsolidatedObjectKey,
				Action:          retirement.ActionSkip,
				SkipReason:      retirement.SkipCSCBSafetyQuiescenceActive,
			},
			{
				Height:          102,
				ConsolidatedKey: cohort.ConsolidatedObjectKey,
				Action:          retirement.ActionSkip,
				SkipReason:      retirement.SkipCSCBSafetyQuiescenceActive,
			},
		},
	}

	result, err := summarizeSingleBlockRetentionReport(cohort, report)
	require.NoError(t, err)
	require.False(t, result.Terminal)
	require.NotNil(t, result.FirstIncompleteHeight)
	require.Equal(t, uint64(101), *result.FirstIncompleteHeight)
	require.Equal(t, uint64(101), result.VerifiedThroughExclusive)
	require.Equal(t, uint64(2), result.DeferredRows)
	require.Equal(t, []SingleBlockRetentionReasonCount{{
		Reason: retirement.SkipCSCBSafetyQuiescenceActive,
		Rows:   2,
	}}, result.Blockers)
	require.True(t, setSingleBlockRetentionRetry(result))
	require.Equal(t, retirement.RetentionSafetyQuiescencePeriod, result.RetryAfter)
	require.Equal(t, retirement.SkipCSCBSafetyQuiescenceActive, result.RetryReason)
}

func TestSetSingleBlockRetentionRetryUsesRemainingQuiescence(t *testing.T) {
	result := &SingleBlockRetentionRangeResult{
		QuiescenceRetryAfter: 10 * time.Minute,
		DeferredRows:         1,
		Blockers: []SingleBlockRetentionReasonCount{{
			Reason: retirement.SkipCSCBSafetyQuiescenceActive,
			Rows:   1,
		}},
	}

	require.True(t, setSingleBlockRetentionRetry(result))
	require.Equal(t, 10*time.Minute+retentionQuiescenceRetryGrace, result.RetryAfter)
	require.Equal(t, retirement.SkipCSCBSafetyQuiescenceActive, result.RetryReason)

	// Without a remaining hint (or when the padded hint would exceed the full
	// period), the retry falls back to the full quiescence period.
	fallback := &SingleBlockRetentionRangeResult{
		DeferredRows: 1,
		Blockers: []SingleBlockRetentionReasonCount{{
			Reason: retirement.SkipCSCBSafetyQuiescenceActive,
			Rows:   1,
		}},
	}
	require.True(t, setSingleBlockRetentionRetry(fallback))
	require.Equal(t, retirement.RetentionSafetyQuiescencePeriod, fallback.RetryAfter)
}

func TestSetSingleBlockRetentionRetryDefersActiveClaim(t *testing.T) {
	result := &SingleBlockRetentionRangeResult{
		DeferredRows: 1,
		Blockers: []SingleBlockRetentionReasonCount{{
			Reason: retirement.SkipRetirementClaimActive,
			Rows:   1,
		}},
	}

	require.True(t, setSingleBlockRetentionRetry(result))
	require.Equal(t, retirement.RetirementClaimLease, result.RetryAfter)
	require.Equal(t, retirement.SkipRetirementClaimActive, result.RetryReason)
}

func TestSummarizeSingleBlockRetentionReportRejectsMixedConsolidatedObjects(t *testing.T) {
	cohort := retirement.RetentionCohort{
		ConsolidatedObjectKey: "consolidated/a.cscb.zstd",
		StartHeight:           100,
		EndHeight:             101,
		RowCount:              1,
		EligibleAt:            time.Now().Add(-time.Hour),
	}
	report := &retirement.Report{
		StartHeight: 100,
		EndHeight:   101,
		Items: []retirement.Candidate{{
			Height:          100,
			ConsolidatedKey: "consolidated/b.cscb.zstd",
			Action:          retirement.ActionDeletedVerified,
		}},
	}

	_, err := summarizeSingleBlockRetentionReport(cohort, report)
	require.ErrorContains(t, err, "belongs to consolidated object")
}

func TestValidateSingleBlockRetentionExecutionPlanFailsClosed(t *testing.T) {
	cohort := retirement.RetentionCohort{
		ConsolidatedObjectKey: "consolidated/a.cscb.zstd",
		StartHeight:           100,
		EndHeight:             102,
		RowCount:              1,
		EligibleAt:            time.Now().Add(-time.Hour),
	}
	valid := &retirement.Report{
		StartHeight: 100,
		EndHeight:   102,
		Items: []retirement.Candidate{
			{
				Height:          100,
				ConsolidatedKey: cohort.ConsolidatedObjectKey,
				Action:          retirement.ActionDeleteObjectVersion,
			},
			{
				Height:          101,
				ConsolidatedKey: cohort.ConsolidatedObjectKey,
				Action:          retirement.ActionAlreadyDeleted,
			},
		},
	}
	require.NoError(t, validateSingleBlockRetentionExecutionPlan(cohort, valid))

	mixedObject := *valid
	mixedObject.Items = append([]retirement.Candidate(nil), valid.Items...)
	mixedObject.Items[1].ConsolidatedKey = "consolidated/b.cscb.zstd"
	require.ErrorContains(
		t,
		validateSingleBlockRetentionExecutionPlan(cohort, &mixedObject),
		"belongs to consolidated object",
	)

	blocked := *valid
	blocked.Items = append([]retirement.Candidate(nil), valid.Items...)
	blocked.Items[1].Action = retirement.ActionSkip
	blocked.Items[1].SkipReason = retirement.SkipRetentionPeriodActive
	require.ErrorContains(
		t,
		validateSingleBlockRetentionExecutionPlan(cohort, &blocked),
		"is not executable",
	)

	missing := *valid
	missing.Items = valid.Items[:1]
	require.ErrorContains(
		t,
		validateSingleBlockRetentionExecutionPlan(cohort, &missing),
		"does not cover cohort",
	)

}

func TestValidateSingleBlockRetentionProcessRequestRequiresExecutionGates(t *testing.T) {
	cfg, err := config.New(config.WithEnvironment(config.EnvProduction))
	require.NoError(t, err)
	base := &SingleBlockRetentionProcessRequest{
		Cohort: retirement.RetentionCohort{
			ConsolidatedObjectKey: "consolidated/a.cscb.zstd",
			StartHeight:           100,
			EndHeight:             101,
			RowCount:              1,
			EligibleAt:            time.Now().Add(-time.Hour),
		},
		Execute: true,
	}

	err = validateSingleBlockRetentionProcessRequest(cfg, base)
	require.ErrorContains(t, err, "direct storage clients")

	base.DirectStorageClientsGuarded = true
	err = validateSingleBlockRetentionProcessRequest(cfg, base)
	require.ErrorContains(t, err, "single-block writer")

	base.SingleBlockWritersGuarded = true
	err = validateSingleBlockRetentionProcessRequest(cfg, base)
	require.ErrorContains(t, err, "explicit fallback-disabled")

	base.FallbackReadsValidated = true
	base.FallbackErrorCount = 1
	err = validateSingleBlockRetentionProcessRequest(cfg, base)
	require.ErrorContains(t, err, "zero fallback")

	base.FallbackErrorCount = 0
	err = validateSingleBlockRetentionProcessRequest(cfg, base)
	require.ErrorContains(t, err, "operator approval chain")

	base.ApprovedChain = "wrong-chain"
	base.ApprovedStartHeight = 100
	base.ApprovedEndHeight = 101
	err = validateSingleBlockRetentionProcessRequest(cfg, base)
	require.ErrorContains(t, err, "approval names chain")

	base.ApprovedChain = singleBlockRetentionExpectedChain(cfg)
	base.ApprovedStartHeight = 101
	err = validateSingleBlockRetentionProcessRequest(cfg, base)
	require.ErrorContains(t, err, "outside approval envelope")

	base.ApprovedStartHeight = 99
	base.ApprovedEndHeight = 102
	err = validateSingleBlockRetentionProcessRequest(cfg, base)
	require.ErrorContains(t, err, "production-delete")

	base.ProductionDeleteEnabled = true
	require.NoError(t, validateSingleBlockRetentionProcessRequest(cfg, base))
}

func TestSingleBlockRetentionPlanRequestUsesOperatorApprovalVerbatim(t *testing.T) {
	cfg, err := config.New(
		config.WithBlockchain(common.Blockchain_BLOCKCHAIN_SOLANA),
		config.WithNetwork(common.Network_NETWORK_SOLANA_MAINNET),
	)
	require.NoError(t, err)
	a := &SingleBlockRetention{config: cfg}
	cohort := retirement.RetentionCohort{
		ConsolidatedObjectKey: "consolidated/a.cscb.zstd",
		StartHeight:           100,
		EndHeight:             110,
		RowCount:              10,
		EligibleAt:            time.Now().Add(-time.Hour),
	}

	executeReq, err := a.planRequest(&SingleBlockRetentionProcessRequest{
		Cohort:              cohort,
		EligibilityCutoff:   cohort.EligibleAt,
		Execute:             true,
		ApprovedChain:       "solana-mainnet",
		ApprovedStartHeight: 90,
		ApprovedEndHeight:   120,
	})
	require.NoError(t, err)
	require.Equal(t, retirement.Approval{
		Chain:                "solana-mainnet",
		StartHeight:          90,
		EndHeight:            120,
		AllowContainingRange: true,
	}, executeReq.Approval)
	require.Equal(t, cohort.EligibleAt.UTC(), executeReq.EligibilityCutoff)
	require.Equal(t, cfg.AWS.Bucket, executeReq.Bucket)
	require.Equal(t, "", executeReq.StorageGeneration)

	tagZeroReq, err := a.planRequest(&SingleBlockRetentionProcessRequest{
		Tag:    math.MaxUint32,
		Cohort: cohort,
	})
	require.NoError(t, err)
	require.Equal(t, uint32(0), tagZeroReq.Tag)

	// An omitted operator approval stays omitted; it is never synthesized
	// from the selected cohort.
	dryRunReq, err := a.planRequest(&SingleBlockRetentionProcessRequest{Cohort: cohort})
	require.NoError(t, err)
	require.Equal(t, retirement.Approval{}, dryRunReq.Approval)
	require.False(t, dryRunReq.EligibilityCutoff.IsZero())
	require.Equal(t, dryRunReq.Now, dryRunReq.EligibilityCutoff)

	cfg.AWS.BlockStorage = config.BlockStorageConfig{
		WriteGeneration: "v2",
		Generations: map[string]config.BlockStorageGenerationConfig{
			"v2": {Bucket: "v2-blocks"},
		},
	}
	v2Req, err := a.planRequest(&SingleBlockRetentionProcessRequest{Cohort: cohort})
	require.NoError(t, err)
	require.Equal(t, "v2-blocks", v2Req.Bucket)
	require.Equal(t, "v2", v2Req.StorageGeneration)
}

func TestResolveSingleBlockRetentionEligibilityCutoffSupportsLegacyPayload(t *testing.T) {
	now := time.Date(2026, 7, 31, 4, 0, 0, 0, time.FixedZone("test", 8*60*60))
	require.Equal(t, now.UTC(), resolveSingleBlockRetentionEligibilityCutoff(time.Time{}, now))

	cutoff := now.Add(-time.Hour)
	require.Equal(t, cutoff.UTC(), resolveSingleBlockRetentionEligibilityCutoff(cutoff, now.Add(time.Hour)))
}

func TestSingleBlockRetentionChainNamesUseOperatorCompatibleComponents(t *testing.T) {
	cfg, err := config.New(
		config.WithBlockchain(common.Blockchain_BLOCKCHAIN_SOLANA),
		config.WithNetwork(common.Network_NETWORK_SOLANA_MAINNET),
	)
	require.NoError(t, err)

	blockchain, network, sidechain := singleBlockRetentionChainNames(cfg)
	require.Equal(t, "solana", blockchain)
	require.Equal(t, "mainnet", network)
	require.Empty(t, sidechain)
}

func TestWatchStopChannelCancelsOnWorkerStop(t *testing.T) {
	stop := make(chan struct{})
	ctx, stopped, cancel := watchStopChannel(context.Background(), stop)
	defer cancel()
	require.NoError(t, ctx.Err())
	require.False(t, stopped())
	close(stop)
	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("context was not canceled after the worker stop channel closed")
	}
	require.True(t, stopped())
	require.ErrorIs(t, ctx.Err(), context.Canceled)
}

func TestWatchStopChannelDoesNotReportAPlainCancelAsWorkerStop(t *testing.T) {
	stop := make(chan struct{})
	ctx, stopped, cancel := watchStopChannel(context.Background(), stop)
	require.NoError(t, ctx.Err())
	cancel()
	<-ctx.Done()
	require.False(t, stopped())
}

func TestWatchWorkerStopOutsideAnActivityIsANoop(t *testing.T) {
	ctx, stopped, cancel := watchWorkerStop(context.Background())
	defer cancel()
	require.NoError(t, ctx.Err())
	require.False(t, stopped())
}
