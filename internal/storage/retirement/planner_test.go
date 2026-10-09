package retirement

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/coinbase/chainstorage/internal/storage/blobstorage/cscb"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

type fakeRepo struct {
	rows               []MetadataRow
	manifests          map[int64]RetirementManifest
	safetyObservations map[string]fakeSafetyObservation
	safetyNow          time.Time
	recordDeletedErr   error
	recordDeletedCalls int
	finalizeErr        error
	finalizeCalls      int
	renewCalls         int
	releaseCalls       int
	// outcomeBlocksUntilDeadline makes RecordRetirementOutcome consume its
	// context's whole deadline and fail with it, standing in for a stalled
	// database during shutdown.
	outcomeBlocksUntilDeadline bool
	// cleanupContexts observes the context of every outcome and release
	// write, in call order, so tests can pin that the cleanup is detached
	// from the canceled operation and bounded by a deadline.
	cleanupContexts []cleanupContextObservation
}

type cleanupContextObservation struct {
	live        bool
	hasDeadline bool
	deadline    time.Time
}

func observeCleanupContext(ctx context.Context) cleanupContextObservation {
	deadline, hasDeadline := ctx.Deadline()
	return cleanupContextObservation{live: ctx.Err() == nil, hasDeadline: hasDeadline, deadline: deadline}
}

type fakeSafetyObservation struct {
	configurationSHA256 string
	firstObservedAt     time.Time
}

func (r *fakeRepo) ListMetadataRows(ctx context.Context, tag uint32, startHeight uint64, endHeight uint64, limit uint64) ([]MetadataRow, error) {
	if limit > 0 && uint64(len(r.rows)) > limit {
		return r.rows[:limit], nil
	}
	return r.rows, nil
}

func (r *fakeRepo) GetMetadataRow(ctx context.Context, blockMetadataID int64) (MetadataRow, error) {
	for _, row := range r.rows {
		if row.BlockMetadataID == blockMetadataID {
			return row, nil
		}
	}
	return MetadataRow{}, fmt.Errorf("metadata row not found: %d", blockMetadataID)
}

func (r *fakeRepo) PrepareRetirement(
	ctx context.Context,
	manifest RetirementManifest,
	expectedStorageGeneration string,
) error {
	row, err := r.GetMetadataRow(ctx, manifest.BlockMetadataID)
	if err != nil {
		return err
	}
	if row.PrimaryStorageGeneration != expectedStorageGeneration || row.Shadow == nil ||
		row.Shadow.SingleBlockStorageGeneration != expectedStorageGeneration ||
		row.Shadow.ConsolidatedStorageGeneration != expectedStorageGeneration {
		return errors.New("storage generation changed")
	}
	if r.manifests == nil {
		r.manifests = make(map[int64]RetirementManifest)
	}
	if existing, ok := r.manifests[manifest.BlockMetadataID]; ok && !samePreparedManifest(existing, manifest) {
		return errors.New("manifest conflict")
	}
	r.manifests[manifest.BlockMetadataID] = manifest
	for i := range r.rows {
		if r.rows[i].BlockMetadataID == manifest.BlockMetadataID {
			preparedAt := manifest.PreparedAt
			r.rows[i].RetirementFencedAt = &preparedAt
			r.rows[i].Retirement = &manifest
		}
	}
	return nil
}

func (r *fakeRepo) ObserveRetentionSafety(
	ctx context.Context,
	bucket string,
	consolidatedObjectKey string,
	configurationSHA256 string,
) (time.Time, time.Time, error) {
	if r.safetyObservations == nil {
		r.safetyObservations = make(map[string]fakeSafetyObservation)
	}
	observedAt := r.safetyNow
	if observedAt.IsZero() {
		observedAt = time.Now().UTC()
	}
	key := bucket + "\x00" + consolidatedObjectKey
	observation, ok := r.safetyObservations[key]
	if !ok {
		observation = fakeSafetyObservation{
			configurationSHA256: configurationSHA256,
			firstObservedAt:     observedAt.Add(-RetentionSafetyQuiescencePeriod),
		}
	} else if observation.configurationSHA256 != configurationSHA256 {
		observation.configurationSHA256 = configurationSHA256
		observation.firstObservedAt = observedAt
	}
	r.safetyObservations[key] = observation
	return observation.firstObservedAt, observedAt, nil
}

func (r *fakeRepo) ClaimRetirement(
	ctx context.Context,
	blockMetadataID int64,
	claimToken string,
	claimedAt time.Time,
	claimExpiresAt time.Time,
) error {
	manifest, ok := r.manifests[blockMetadataID]
	if !ok {
		return errors.New("manifest not found")
	}
	if (manifest.State == RetirementStateDeleting || manifest.State == RetirementStateDeletedPendingVerification) &&
		manifest.ClaimExpiresAt != nil && manifest.ClaimExpiresAt.After(claimedAt) {
		return ErrRetirementClaimUnavailable
	}
	if manifest.State != RetirementStateEligible &&
		manifest.State != RetirementStateDeleting &&
		manifest.State != RetirementStateDeletedPendingVerification {
		return ErrRetirementClaimUnavailable
	}
	if manifest.State == RetirementStateEligible {
		manifest.State = RetirementStateDeleting
	}
	manifest.ClaimToken = claimToken
	manifest.ClaimExpiresAt = &claimExpiresAt
	if manifest.DeleteStartedAt == nil {
		manifest.DeleteStartedAt = &claimedAt
	}
	manifest.LastAttemptAt = &claimedAt
	manifest.AttemptCount++
	if manifest.State == RetirementStateDeletedPendingVerification {
		manifest.Outcome = "verification_started"
	} else {
		manifest.Outcome = "delete_started"
	}
	r.manifests[blockMetadataID] = manifest
	r.updateManifestRow(manifest)
	return nil
}

func (r *fakeRepo) RenewRetirementClaim(
	ctx context.Context,
	blockMetadataID int64,
	claimToken string,
	renewedAt time.Time,
	claimExpiresAt time.Time,
) error {
	r.renewCalls++
	manifest, ok := r.manifests[blockMetadataID]
	if !ok || manifest.State != RetirementStateDeleting && manifest.State != RetirementStateDeletedPendingVerification ||
		manifest.ClaimToken != claimToken ||
		manifest.ClaimExpiresAt == nil || !manifest.ClaimExpiresAt.After(renewedAt) {
		return ErrRetirementClaimUnavailable
	}
	manifest.ClaimExpiresAt = &claimExpiresAt
	manifest.LastAttemptAt = &renewedAt
	r.manifests[blockMetadataID] = manifest
	r.updateManifestRow(manifest)
	return nil
}

func (r *fakeRepo) ReleaseRetirementClaim(ctx context.Context, blockMetadataID int64, claimToken string) error {
	r.releaseCalls++
	r.cleanupContexts = append(r.cleanupContexts, observeCleanupContext(ctx))
	manifest, ok := r.manifests[blockMetadataID]
	if !ok || manifest.ClaimToken != claimToken || manifest.ClaimExpiresAt == nil {
		return nil
	}
	released := time.Now().UTC()
	manifest.ClaimExpiresAt = &released
	r.manifests[blockMetadataID] = manifest
	r.updateManifestRow(manifest)
	return nil
}

func (r *fakeRepo) RecordRetirementOutcome(
	ctx context.Context,
	blockMetadataID int64,
	claimToken string,
	outcome string,
	attemptedAt time.Time,
) error {
	r.cleanupContexts = append(r.cleanupContexts, observeCleanupContext(ctx))
	if r.outcomeBlocksUntilDeadline {
		<-ctx.Done()
		return ctx.Err()
	}
	manifest, ok := r.manifests[blockMetadataID]
	if !ok || manifest.State != RetirementStateDeleting && manifest.State != RetirementStateDeletedPendingVerification ||
		manifest.ClaimToken != claimToken {
		return ErrRetirementClaimUnavailable
	}
	manifest.Outcome = outcome
	manifest.LastAttemptAt = &attemptedAt
	r.manifests[blockMetadataID] = manifest
	r.updateManifestRow(manifest)
	return nil
}

func (r *fakeRepo) RecordRetirementObjectDeleted(
	ctx context.Context,
	blockMetadataID int64,
	claimToken string,
	outcome string,
) (time.Time, error) {
	r.recordDeletedCalls++
	if r.recordDeletedErr != nil {
		return time.Time{}, r.recordDeletedErr
	}
	manifest, ok := r.manifests[blockMetadataID]
	if !ok || manifest.State != RetirementStateDeleting || manifest.ClaimToken != claimToken || manifest.ClaimExpiresAt == nil {
		return time.Time{}, errors.New("manifest is not deleting")
	}
	deletedAt := time.Now().UTC()
	manifest.State = RetirementStateDeletedPendingVerification
	manifest.SingleBlockObjectKey = ""
	manifest.SingleBlockObjectETag = ""
	manifest.DeletedAt = &deletedAt
	manifest.LastAttemptAt = &deletedAt
	manifest.Outcome = outcome
	r.manifests[blockMetadataID] = manifest
	for i := range r.rows {
		if r.rows[i].BlockMetadataID != blockMetadataID {
			continue
		}
		r.rows[i].SingleBlockObjectKey = ""
		if r.rows[i].Shadow != nil {
			r.rows[i].Shadow.SingleBlockObjectKey = ""
			r.rows[i].Shadow.SingleBlockObjectDeletedAt = &deletedAt
		}
		r.rows[i].Retirement = &manifest
	}
	return deletedAt, nil
}

func (r *fakeRepo) FinalizeRetirement(
	ctx context.Context,
	blockMetadataID int64,
	claimToken string,
	outcome string,
) (time.Time, error) {
	r.finalizeCalls++
	if r.finalizeErr != nil {
		return time.Time{}, r.finalizeErr
	}
	manifest, ok := r.manifests[blockMetadataID]
	if !ok || manifest.State != RetirementStateDeletedPendingVerification || manifest.ClaimToken != claimToken ||
		manifest.ClaimExpiresAt == nil || manifest.DeletedAt == nil {
		return time.Time{}, errors.New("manifest is not pending verification")
	}
	verifiedAt := time.Now().UTC()
	manifest.State = RetirementStateDeletedVerified
	manifest.ClaimToken = ""
	manifest.ClaimExpiresAt = nil
	manifest.VerifiedAt = &verifiedAt
	manifest.LastAttemptAt = &verifiedAt
	manifest.Outcome = outcome
	r.manifests[blockMetadataID] = manifest
	for i := range r.rows {
		if r.rows[i].BlockMetadataID != blockMetadataID {
			continue
		}
		r.rows[i].Retirement = &manifest
	}
	return verifiedAt, nil
}

func (r *fakeRepo) updateManifestRow(manifest RetirementManifest) {
	for i := range r.rows {
		if r.rows[i].BlockMetadataID == manifest.BlockMetadataID {
			copy := manifest
			r.rows[i].Retirement = &copy
		}
	}
}

func (r *fakeRepo) ListPendingRetirements(
	ctx context.Context,
	tag uint32,
	startHeight uint64,
	endHeight uint64,
	eligibilityCutoff time.Time,
	limit uint64,
) ([]RetirementManifest, error) {
	var manifests []RetirementManifest
	for _, manifest := range r.manifests {
		if manifest.Tag == tag && manifest.Height >= startHeight && manifest.Height < endHeight &&
			(manifest.State == RetirementStateEligible || manifest.State == RetirementStateDeleting ||
				manifest.State == RetirementStateDeletedPendingVerification) &&
			r.retirementEligibleAt(manifest.BlockMetadataID, eligibilityCutoff) {
			manifests = append(manifests, manifest)
		}
	}
	sort.Slice(manifests, func(i, j int) bool { return manifests[i].Height < manifests[j].Height })
	if limit > 0 && uint64(len(manifests)) > limit {
		manifests = manifests[:limit]
	}
	return manifests, nil
}

func (r *fakeRepo) retirementEligibleAt(blockMetadataID int64, eligibilityCutoff time.Time) bool {
	for _, row := range r.rows {
		if row.BlockMetadataID != blockMetadataID {
			continue
		}
		return row.Shadow != nil &&
			row.Shadow.SingleBlockDeleteAfter != nil &&
			!row.Shadow.SingleBlockDeleteAfter.After(eligibilityCutoff)
	}
	return false
}

type fakeStore struct {
	heads         map[string]ObjectHead
	versionHeads  map[string]ObjectHead
	topologies    map[string]ObjectVersionTopology
	objects       map[string][]byte
	headCalls     map[string]int
	versionCalls  map[string]int
	policyCalls   map[string]int
	policyErrors  map[string]error
	policyHashes  map[string]string
	policyHook    func(key string, call int) (RetentionSafetySnapshot, error)
	deleted       []Candidate
	deleteMutates bool
	deleteErrors  map[string]error
	listHook      func(key string, call int) ObjectVersionTopology
	headHook      func(key string, call int) ObjectHead
}

func newFakeStore() *fakeStore {
	return &fakeStore{
		heads:        make(map[string]ObjectHead),
		versionHeads: make(map[string]ObjectHead),
		topologies:   make(map[string]ObjectVersionTopology),
		objects:      make(map[string][]byte),
		headCalls:    make(map[string]int),
		versionCalls: make(map[string]int),
		policyCalls:  make(map[string]int),
		policyErrors: make(map[string]error),
		policyHashes: make(map[string]string),
		deleteErrors: make(map[string]error),
	}
}

func (s *fakeStore) InspectObjectRetentionSafety(ctx context.Context, bucket string, key string) (RetentionSafetySnapshot, error) {
	s.policyCalls[key]++
	if s.policyHook != nil {
		return s.policyHook(key, s.policyCalls[key])
	}
	configurationSHA256 := s.policyHashes[key]
	if configurationSHA256 == "" {
		configurationSHA256 = keySHA256("safe:" + key)
	}
	return RetentionSafetySnapshot{ConfigurationSHA256: configurationSHA256}, s.policyErrors[key]
}

func (s *fakeStore) HeadObject(ctx context.Context, bucket string, key string) (ObjectHead, error) {
	s.headCalls[key]++
	if s.headHook != nil {
		return s.headHook(key, s.headCalls[key]), nil
	}
	return s.heads[key], nil
}

func (s *fakeStore) HeadObjectVersion(ctx context.Context, bucket string, key string, versionID string) (ObjectHead, error) {
	return s.versionHeads[versionObjectKey(key, versionID)], nil
}

func (s *fakeStore) ListObjectVersions(ctx context.Context, bucket string, key string) (ObjectVersionTopology, error) {
	s.versionCalls[key]++
	if s.listHook != nil {
		return s.listHook(key, s.versionCalls[key]), nil
	}
	return s.topologies[key], nil
}

func (s *fakeStore) ReadObjectVersion(ctx context.Context, bucket string, key string, versionID string) ([]byte, error) {
	return append([]byte(nil), s.objects[versionObjectKey(key, versionID)]...), nil
}

func (s *fakeStore) ReadObjectVersionRange(ctx context.Context, bucket string, key string, versionID string, offset uint64, length uint64) ([]byte, error) {
	data := s.objects[versionObjectKey(key, versionID)]
	if offset >= uint64(len(data)) {
		return nil, errors.New("range starts after object")
	}
	end := offset + length
	if end > uint64(len(data)) {
		end = uint64(len(data))
	}
	return append([]byte(nil), data[offset:end]...), nil
}

func (s *fakeStore) DeleteObjectVersion(ctx context.Context, bucket string, key string, versionID string) error {
	if err := s.deleteErrors[versionID]; err != nil {
		return err
	}
	s.deleted = append(s.deleted, Candidate{
		Bucket:    bucket,
		Key:       key,
		VersionID: versionID,
	})
	if s.deleteMutates {
		topology := s.topologies[key]
		versions := topology.Versions[:0]
		for _, version := range topology.Versions {
			if version.VersionID != versionID {
				versions = append(versions, version)
			}
		}
		topology.Versions = versions
		markers := topology.DeleteMarkers[:0]
		for _, marker := range topology.DeleteMarkers {
			if marker.VersionID != versionID {
				markers = append(markers, marker)
			}
		}
		topology.DeleteMarkers = markers
		if len(topology.Versions) == 0 && len(topology.DeleteMarkers) == 0 {
			delete(s.topologies, key)
			delete(s.heads, key)
		} else {
			s.topologies[key] = topology
		}
	}
	return nil
}

type fakePayloadVerifier struct {
	digest             string
	consolidatedDigest string
	verifyErr          error
	consolidatedErr    error
	verifyCalls        int
	consolidatedCalls  int
	verifyHook         func(call int) (string, error)
}

func (v *fakePayloadVerifier) Verify(ctx context.Context, candidate Candidate) (string, error) {
	v.verifyCalls++
	if v.verifyHook != nil {
		return v.verifyHook(v.verifyCalls)
	}
	return v.digest, v.verifyErr
}

func (v *fakePayloadVerifier) VerifyConsolidated(ctx context.Context, candidate Candidate) (string, error) {
	v.consolidatedCalls++
	return v.consolidatedDigest, v.consolidatedErr
}

func newTestPlanner(repo *fakeRepo, store *fakeStore) (*Planner, *fakePayloadVerifier) {
	digest := strings.Repeat("a", sha256HexLength)
	verifier := &fakePayloadVerifier{digest: digest, consolidatedDigest: digest}
	planner := &Planner{repo: repo, store: store}
	planner.verifierFactory = func() blockPayloadVerifier { return verifier }
	return planner, verifier
}

func testPlanner(repo *fakeRepo, store *fakeStore) *Planner {
	planner, _ := newTestPlanner(repo, store)
	return planner
}

func versionObjectKey(key string, versionID string) string {
	return key + "\x00" + versionID
}

func safeTopology(versionID string, etag string, bytes uint64) ObjectVersionTopology {
	return ObjectVersionTopology{Versions: []ObjectVersion{{
		VersionID: versionID,
		ETag:      etag,
		Bytes:     bytes,
		IsLatest:  true,
	}}}
}

func TestPlannerPlan_NotEligibleRetentionPeriod(t *testing.T) {
	require := require.New(t)
	now := time.Date(2026, 6, 21, 12, 0, 0, 0, time.UTC)
	validatedAt := now.Add(-time.Hour)
	store := newFakeStore()
	row := testRow(100, "hash-100", "single-block/100.zstd", "consolidated/canary.cscb.zstd", validatedAt)
	planner := testPlanner(&fakeRepo{rows: []MetadataRow{row}}, store)

	report, err := planner.Plan(context.Background(), testRequest(now, false))
	require.NoError(err)
	require.Len(report.Items, 1)
	require.Equal(ActionSkip, report.Items[0].Action)
	require.Equal(SkipRetentionPeriodActive, report.Items[0].SkipReason)
	require.Zero(store.headCalls[row.Shadow.ConsolidatedObjectKey])
	require.Zero(store.versionCalls[row.SingleBlockObjectKey])
}

func TestPlannerPlan_UsesFrozenEligibilityCutoff(t *testing.T) {
	require := require.New(t)
	cutoff := time.Date(2026, 6, 21, 12, 0, 0, 0, time.UTC)
	now := cutoff.Add(2 * time.Hour)
	row := testRow(
		100,
		"hash-100",
		"single-block/100.zstd",
		"consolidated/canary.cscb.zstd",
		cutoff.Add(-8*24*time.Hour),
	)
	deleteAfter := cutoff.Add(time.Hour)
	row.Shadow.SingleBlockDeleteAfter = &deleteAfter
	req := testRequest(now, false)
	req.EligibilityCutoff = cutoff

	report, err := testPlanner(&fakeRepo{rows: []MetadataRow{row}}, newFakeStore()).Plan(context.Background(), req)
	require.NoError(err)
	require.Len(report.Items, 1)
	require.Equal(SkipRetentionPeriodActive, report.Items[0].SkipReason)
}

func TestPlannerPlan_MissingRetirementMarkerIsNeverEligible(t *testing.T) {
	require := require.New(t)
	now := time.Date(2026, 6, 21, 12, 0, 0, 0, time.UTC)
	validatedAt := now.Add(-30 * 24 * time.Hour)
	row := testRow(100, "hash-100", "single-block/100.zstd", "consolidated/canary.cscb.zstd", validatedAt)
	row.Shadow.SingleBlockRetentionStartedAt = nil
	row.Shadow.SingleBlockDeleteAfter = nil
	store := newFakeStore()

	report, err := testPlanner(&fakeRepo{rows: []MetadataRow{row}}, store).Plan(context.Background(), testRequest(now, false))
	require.NoError(err)
	require.Len(report.Items, 1)
	require.Equal(ActionSkip, report.Items[0].Action)
	require.Equal(SkipMissingRetirementMarker, report.Items[0].SkipReason)
	require.Zero(store.headCalls[row.Shadow.ConsolidatedObjectKey])
	require.Zero(store.versionCalls[row.SingleBlockObjectKey])
}

func TestPlannerPlan_MissingCSCBObject(t *testing.T) {
	require := require.New(t)
	now := time.Date(2026, 6, 21, 12, 0, 0, 0, time.UTC)
	validatedAt := now.Add(-8 * 24 * time.Hour)
	row := testRow(100, "hash-100", "single-block/100.zstd", "consolidated/missing.cscb.zstd", validatedAt)
	store := newFakeStore()
	store.topologies[row.SingleBlockObjectKey] = safeTopology("single-block-v1", "single-block-etag", 42)
	planner := testPlanner(&fakeRepo{rows: []MetadataRow{row}}, store)

	report, err := planner.Plan(context.Background(), testRequest(now, false))
	require.NoError(err)
	require.Len(report.Items, 1)
	require.Equal(ActionSkip, report.Items[0].Action)
	require.Equal(SkipMissingCSCBObject, report.Items[0].SkipReason)
	require.Equal("single-block-v1", report.Items[0].VersionID)
	require.Equal(uint64(42), report.Items[0].SingleBlockBytes)
}

func TestPlannerPlan_RequiresLiveCSCBWriteOncePolicy(t *testing.T) {
	require := require.New(t)
	now := time.Date(2026, 6, 21, 12, 0, 0, 0, time.UTC)
	row := testRow(100, "hash-100", "single-block/100.zstd", "consolidated/canary.cscb.zstd", now.Add(-8*24*time.Hour))
	store := newFakeStore()
	store.topologies[row.SingleBlockObjectKey] = safeTopology("single-block-v1", "single-block-etag", 42)
	store.heads[row.PrimaryObjectKey] = cscbObjectHead(1024, 1024)
	store.policyErrors[row.PrimaryObjectKey] = errors.New("bucket policy is missing")

	report, err := testPlanner(&fakeRepo{rows: []MetadataRow{row}}, store).Plan(context.Background(), testRequest(now, false))
	require.NoError(err)
	require.Len(report.Items, 1)
	require.Equal(ActionSkip, report.Items[0].Action)
	require.Equal(SkipCSCBWriteOncePolicyUnverified, report.Items[0].SkipReason)
	require.False(report.Items[0].CSCBWriteOncePolicy)
	require.False(report.SafetyGates.CSCBWriteOncePolicyVerified)
	require.Equal(1, store.policyCalls[row.PrimaryObjectKey])
}

func TestPlannerPlan_InvalidMetadataReference(t *testing.T) {
	require := require.New(t)
	now := time.Date(2026, 6, 21, 12, 0, 0, 0, time.UTC)
	validatedAt := now.Add(-8 * 24 * time.Hour)
	row := testRow(100, "hash-100", "single-block/100.zstd", "consolidated/canary.cscb.zstd", validatedAt)
	row.Shadow.SingleBlockObjectKey = "single-block/stale.zstd"
	store := newFakeStore()
	planner := testPlanner(&fakeRepo{rows: []MetadataRow{row}}, store)

	report, err := planner.Plan(context.Background(), testRequest(now, false))
	require.NoError(err)
	require.Len(report.Items, 1)
	require.Equal(ActionSkip, report.Items[0].Action)
	require.Equal(SkipInvalidMetadataReference, report.Items[0].SkipReason)
	require.Zero(store.headCalls[row.Shadow.ConsolidatedObjectKey])
}

func TestPlannerPlan_StorageGenerationMismatchIsNeverRetired(t *testing.T) {
	now := time.Date(2026, 6, 21, 12, 0, 0, 0, time.UTC)
	validatedAt := now.Add(-8 * 24 * time.Hour)

	tests := []struct {
		name   string
		mutate func(*MetadataRow)
	}{
		{
			name: "primary",
			mutate: func(row *MetadataRow) {
				row.PrimaryStorageGeneration = "v2"
			},
		},
		{
			name: "source shadow",
			mutate: func(row *MetadataRow) {
				row.Shadow.SingleBlockStorageGeneration = "v2"
			},
		},
		{
			name: "consolidated shadow",
			mutate: func(row *MetadataRow) {
				row.Shadow.ConsolidatedStorageGeneration = "v2"
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require := require.New(t)
			row := testRow(100, "hash-100", "single-block/100.zstd", "consolidated/canary.cscb.zstd", validatedAt)
			test.mutate(&row)
			store := newFakeStore()

			report, err := testPlanner(&fakeRepo{rows: []MetadataRow{row}}, store).Plan(context.Background(), testRequest(now, false))
			require.NoError(err)
			require.Len(report.Items, 1)
			require.Equal(ActionSkip, report.Items[0].Action)
			require.Equal(SkipStorageGenerationMismatch, report.Items[0].SkipReason)
			require.Empty(store.headCalls)
			require.Empty(store.versionCalls)
		})
	}
}

func TestPlannerPlan_ActiveSingleBlockMetadataIsNeverRetired(t *testing.T) {
	require := require.New(t)
	now := time.Date(2026, 6, 21, 12, 0, 0, 0, time.UTC)
	validatedAt := now.Add(-8 * 24 * time.Hour)
	row := activeSingleBlockTestRow(100, "hash-100", "single-block/100.zstd", "consolidated/canary.cscb.zstd", validatedAt)
	store := newFakeStore()
	store.topologies[row.SingleBlockObjectKey] = safeTopology("single-block-v1", "single-block-etag", 42)
	store.heads[row.Shadow.ConsolidatedObjectKey] = cscbObjectHead(1024, 1024)

	report, err := testPlanner(&fakeRepo{rows: []MetadataRow{row}}, store).Plan(context.Background(), testRequest(now, false))
	require.NoError(err)
	require.Len(report.Items, 1)
	require.Equal(ActionSkip, report.Items[0].Action)
	require.Equal(SkipActiveMetadataStillSingleBlock, report.Items[0].SkipReason)
	require.Zero(store.headCalls[row.Shadow.ConsolidatedObjectKey])
	require.Zero(store.versionCalls[row.SingleBlockObjectKey])
}

func TestPlannerPlan_ValidationAndApprovalGates(t *testing.T) {
	require := require.New(t)
	now := time.Date(2026, 6, 21, 12, 0, 0, 0, time.UTC)
	validatedAt := now.Add(-8 * 24 * time.Hour)

	t.Run("validation not passed", func(t *testing.T) {
		row := testRow(100, "hash-100", "single-block/100.zstd", "consolidated/canary.cscb.zstd", validatedAt)
		row.Shadow.ValidatedAt = nil
		store := newFakeStore()
		report, err := testPlanner(&fakeRepo{rows: []MetadataRow{row}}, store).Plan(context.Background(), testRequest(now, false))
		require.NoError(err)
		require.Len(report.Items, 1)
		require.Equal(ActionSkip, report.Items[0].Action)
		require.Equal(SkipValidationNotPassed, report.Items[0].SkipReason)
		require.Zero(store.headCalls[row.Shadow.ConsolidatedObjectKey])
		require.Zero(store.versionCalls[row.SingleBlockObjectKey])
	})

	t.Run("range not approved", func(t *testing.T) {
		row := testRow(100, "hash-100", "single-block/100.zstd", "consolidated/canary.cscb.zstd", validatedAt)
		store := newFakeStore()
		req := testRequest(now, false)
		req.Approval.EndHeight = req.EndHeight - 1
		report, err := testPlanner(&fakeRepo{rows: []MetadataRow{row}}, store).Plan(context.Background(), req)
		require.NoError(err)
		require.Len(report.Items, 1)
		require.Equal(ActionSkip, report.Items[0].Action)
		require.Equal(SkipChainRangeNotApproved, report.Items[0].SkipReason)
		require.Zero(store.headCalls[row.Shadow.ConsolidatedObjectKey])
		require.Zero(store.versionCalls[row.SingleBlockObjectKey])
	})

	t.Run("dry run may omit approval and still reaches safety inspection", func(t *testing.T) {
		row := testRow(100, "hash-100", "single-block/100.zstd", "consolidated/canary.cscb.zstd", validatedAt)
		store := newFakeStore()
		store.topologies[row.SingleBlockObjectKey] = safeTopology("single-block-v1", "single-block-etag", 42)
		store.heads[row.PrimaryObjectKey] = cscbObjectHead(1024, 1024)
		req := testRequest(now, false)
		req.Approval = Approval{}
		report, err := testPlanner(&fakeRepo{rows: []MetadataRow{row}}, store).Plan(context.Background(), req)
		require.NoError(err)
		require.Len(report.Items, 1)
		require.Equal(ActionReportOnly, report.Items[0].Action)
		require.Empty(report.Items[0].SkipReason)
	})

	t.Run("execution never derives an approval from omitted operator input", func(t *testing.T) {
		row := testRow(100, "hash-100", "single-block/100.zstd", "consolidated/canary.cscb.zstd", validatedAt)
		store := newFakeStore()
		req := testRequest(now, true)
		req.ProductionDeleteEnabled = true
		req.Approval = Approval{}
		report, err := testPlanner(&fakeRepo{rows: []MetadataRow{row}}, store).Plan(context.Background(), req)
		require.NoError(err)
		require.Len(report.Items, 1)
		require.Equal(ActionSkip, report.Items[0].Action)
		require.Equal(SkipChainRangeNotApproved, report.Items[0].SkipReason)
		require.Zero(store.versionCalls[row.SingleBlockObjectKey])
	})
}

func TestApprovalMatchesExactRangeUnlessContainmentIsExplicit(t *testing.T) {
	now := time.Date(2026, 6, 21, 12, 0, 0, 0, time.UTC)
	req := testRequest(now, true)
	req.Approval.StartHeight = req.StartHeight - 1000
	req.Approval.EndHeight = req.EndHeight + 1000
	require.False(t, approvalMatches(req))

	req.Approval.AllowContainingRange = true
	require.True(t, approvalMatches(req))

	req.Approval.StartHeight = req.StartHeight + 1
	require.False(t, approvalMatches(req))

	req = testRequest(now, true)
	req.Approval.EndHeight = req.EndHeight - 1
	require.False(t, approvalMatches(req))
}

func TestPlannerPlan_FallbackAndDirectStorageClientGates(t *testing.T) {
	require := require.New(t)
	now := time.Date(2026, 6, 21, 12, 0, 0, 0, time.UTC)
	validatedAt := now.Add(-8 * 24 * time.Hour)
	row := testRow(100, "hash-100", "single-block/100.zstd", "consolidated/canary.cscb.zstd", validatedAt)

	t.Run("fallback errors", func(t *testing.T) {
		req := testRequest(now, false)
		req.FallbackErrorCount = 1
		report, err := testPlanner(&fakeRepo{rows: []MetadataRow{row}}, newFakeStore()).Plan(context.Background(), req)
		require.NoError(err)
		require.Equal(SkipActiveFallbackOrReadErrors, report.Items[0].SkipReason)
	})

	t.Run("dry run does not require direct storage client assertion", func(t *testing.T) {
		req := testRequest(now, false)
		req.DirectStorageClientsGuarded = false
		store := newFakeStore()
		store.topologies[row.SingleBlockObjectKey] = safeTopology("single-block-v1", "single-block-etag", 42)
		store.heads[row.PrimaryObjectKey] = cscbObjectHead(1024, 1024)
		report, err := testPlanner(&fakeRepo{rows: []MetadataRow{row}}, store).Plan(context.Background(), req)
		require.NoError(err)
		require.Equal(ActionReportOnly, report.Items[0].Action)
		require.Empty(report.Items[0].SkipReason)
		require.False(report.SafetyGates.DirectStorageClientsGuarded)
	})

	t.Run("execution requires direct storage client assertion", func(t *testing.T) {
		req := testRequest(now, true)
		req.DirectStorageClientsGuarded = false
		report, err := testPlanner(&fakeRepo{rows: []MetadataRow{row}}, newFakeStore()).Plan(context.Background(), req)
		require.NoError(err)
		require.Equal(SkipDirectStorageClientsNotGuarded, report.Items[0].SkipReason)
	})
}

func TestPlannerPlan_RequiresGuardedSingleBlockWritersOnlyForExecution(t *testing.T) {
	require := require.New(t)
	now := time.Date(2026, 6, 21, 12, 0, 0, 0, time.UTC)
	row := testRow(100, "hash-100", "single-block/100.zstd", "consolidated/canary.cscb.zstd", now.Add(-8*24*time.Hour))
	store := newFakeStore()
	store.topologies[row.SingleBlockObjectKey] = safeTopology("single-block-v1", "single-block-etag", 42)
	store.heads[row.PrimaryObjectKey] = cscbObjectHead(1024, 1024)
	planner := testPlanner(&fakeRepo{rows: []MetadataRow{row}}, store)

	dryRunReq := testRequest(now, false)
	dryRunReq.SingleBlockWritersGuarded = false
	dryRunReport, err := planner.Plan(context.Background(), dryRunReq)
	require.NoError(err)
	require.Equal(ActionReportOnly, dryRunReport.Items[0].Action)
	require.False(dryRunReport.SafetyGates.SingleBlockWritersGuarded)

	executeReq := testRequest(now, true)
	executeReq.ProductionDeleteEnabled = true
	executeReq.SingleBlockWritersGuarded = false
	executeReport, err := planner.Plan(context.Background(), executeReq)
	require.NoError(err)
	require.Equal(ActionSkip, executeReport.Items[0].Action)
	require.Equal(SkipSingleBlockWritersNotGuarded, executeReport.Items[0].SkipReason)
	require.Error(planner.Apply(context.Background(), executeReq, executeReport))
}

func TestPlannerPlan_SkippedReorgAndPromotedRows(t *testing.T) {
	require := require.New(t)
	now := time.Date(2026, 6, 21, 12, 0, 0, 0, time.UTC)
	validatedAt := now.Add(-8 * 24 * time.Hour)
	skipped := testRow(100, "hash-100", "", "consolidated/canary.cscb.zstd", validatedAt)
	skipped.Skipped = true

	stale := testRow(101, "hash-101", "single-block/101.zstd", "consolidated/canary.cscb.zstd", validatedAt)
	stale.Shadow.Hash = "hash-before-reorg"

	promoted := testRow(102, "hash-102", "consolidated/primary.cscb.zstd", "consolidated/canary.cscb.zstd", validatedAt)
	promoted.PrimaryObjectFormat = api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_CSCB_BATCH
	promoted.PrimaryByteLength = 123
	promoted.PrimaryObjectKey = "consolidated/primary.cscb.zstd"

	report, err := testPlanner(&fakeRepo{rows: []MetadataRow{skipped, stale, promoted}}, newFakeStore()).Plan(context.Background(), testRequest(now, false))
	require.NoError(err)
	require.Len(report.Items, 3)
	require.Equal(SkipSkippedBlock, report.Items[0].SkipReason)
	require.Equal(SkipInvalidMetadataReference, report.Items[1].SkipReason)
	require.Equal(SkipInvalidMetadataReference, report.Items[2].SkipReason)
}

func TestPlannerPlan_PromotedRowUsesShadowSingleBlockKeyAndRetireAfter(t *testing.T) {
	require := require.New(t)
	now := time.Date(2026, 6, 21, 12, 0, 0, 0, time.UTC)
	validatedAt := now.Add(-time.Hour)
	retiredAt := now.Add(-2 * time.Minute)
	retireAfter := now.Add(-time.Minute)
	singleBlockKey := "single-block/102.zstd"
	cscbKey := "consolidated/primary.cscb.zstd"
	row := testRow(102, "hash-102", singleBlockKey, cscbKey, validatedAt)
	row.Shadow.SingleBlockRetentionStartedAt = &retiredAt
	row.Shadow.SingleBlockDeleteAfter = &retireAfter
	store := newFakeStore()
	store.topologies[singleBlockKey] = safeTopology("single-block-v1", "single-block-etag", 42)
	store.heads[cscbKey] = cscbObjectHead(1024, 1024)

	report, err := testPlanner(&fakeRepo{rows: []MetadataRow{row}}, store).Plan(context.Background(), testRequest(now, false))
	require.NoError(err)
	require.Len(report.Items, 1)
	require.Equal(ActionReportOnly, report.Items[0].Action)
	require.Equal(singleBlockKey, report.Items[0].Key)
	require.Equal(cscbKey, report.Items[0].ConsolidatedKey)
	require.Equal(retiredAt, *report.Items[0].RetiredAt)
	require.Equal(retireAfter, *report.Items[0].EligibleAt)
	require.Empty(report.Items[0].SkipReason)
}

func TestPlannerPlan_RejectsMarkerOnlyTopology(t *testing.T) {
	require := require.New(t)
	now := time.Date(2026, 6, 21, 12, 0, 0, 0, time.UTC)
	validatedAt := now.Add(-8 * 24 * time.Hour)
	row := testRow(100, "hash-100", "single-block/100.zstd", "consolidated/canary.cscb.zstd", validatedAt)
	store := newFakeStore()
	store.topologies[row.SingleBlockObjectKey] = ObjectVersionTopology{DeleteMarkers: []ObjectDeleteMarker{{
		VersionID: "delete-marker-v1",
		IsLatest:  true,
	}}}
	report, err := testPlanner(&fakeRepo{rows: []MetadataRow{row}}, store).Plan(context.Background(), testRequest(now, false))
	require.NoError(err)
	require.Len(report.Items, 1)
	require.Equal(ActionSkip, report.Items[0].Action)
	require.Equal(SkipUnsafeSingleBlockVersionTopology, report.Items[0].SkipReason)
	require.Equal(1, report.Summary.DeleteMarkerRows)
	require.Zero(store.headCalls[row.Shadow.ConsolidatedObjectKey])
}

func TestPlannerPlan_AcceptsImmutableMultiVersionTopology(t *testing.T) {
	require := require.New(t)
	now := time.Date(2026, 6, 21, 12, 0, 0, 0, time.UTC)
	validatedAt := now.Add(-8 * 24 * time.Hour)
	row := testRow(100, "hash-100", "single-block/100.zstd", "consolidated/canary.cscb.zstd", validatedAt)
	store := newFakeStore()
	store.topologies[row.SingleBlockObjectKey] = ObjectVersionTopology{
		Versions: []ObjectVersion{
			{VersionID: "single-block-v2", ETag: "etag-v2", Bytes: 42, IsLatest: true},
			{VersionID: "single-block-v1", ETag: "etag-v1", Bytes: 41},
		},
		DeleteMarkers: []ObjectDeleteMarker{{VersionID: "delete-marker-v1"}},
	}
	store.heads[row.PrimaryObjectKey] = cscbObjectHead(1024, 1024)

	report, err := testPlanner(&fakeRepo{rows: []MetadataRow{row}}, store).Plan(context.Background(), testRequest(now, false))
	require.NoError(err)
	require.Equal(ActionReportOnly, report.Items[0].Action)
	require.Empty(report.Items[0].SkipReason)
	require.Equal("single-block-v2", report.Items[0].VersionID)
	require.Equal(2, report.Items[0].SingleBlockVersions)
	require.Equal(1, report.Items[0].DeleteMarkers)
}

func TestPlannerPlan_AcceptsCurrentDeleteMarkerWithHistoricalDataVersions(t *testing.T) {
	require := require.New(t)
	now := time.Date(2026, 6, 21, 12, 0, 0, 0, time.UTC)
	row := testRow(100, "hash-100", "single-block/100.zstd", "consolidated/canary.cscb.zstd", now.Add(-8*24*time.Hour))
	store := newFakeStore()
	store.topologies[row.SingleBlockObjectKey] = ObjectVersionTopology{
		Versions: []ObjectVersion{
			{VersionID: "single-block-v2", ETag: "etag-v2", Bytes: 42},
			{VersionID: "single-block-v1", ETag: "etag-v1", Bytes: 41},
		},
		DeleteMarkers: []ObjectDeleteMarker{{VersionID: "delete-marker-v3", IsLatest: true}},
	}
	store.heads[row.PrimaryObjectKey] = cscbObjectHead(1024, 1024)

	report, err := testPlanner(&fakeRepo{rows: []MetadataRow{row}}, store).Plan(context.Background(), testRequest(now, false))
	require.NoError(err)
	require.Equal(ActionReportOnly, report.Items[0].Action)
	require.Empty(report.Items[0].SkipReason)
	require.Equal("single-block-v2", report.Items[0].VersionID)
	require.Equal([]string{"single-block-v2", "single-block-v1"}, report.Items[0].VersionIDs)
	require.Equal([]string{"delete-marker-v3"}, report.Items[0].DeleteMarkerVersionIDs)
}

func TestPlannerPlan_RejectsTopologyWithoutLatestEntry(t *testing.T) {
	require := require.New(t)
	now := time.Date(2026, 6, 21, 12, 0, 0, 0, time.UTC)
	row := testRow(100, "hash-100", "single-block/100.zstd", "consolidated/canary.cscb.zstd", now.Add(-8*24*time.Hour))
	store := newFakeStore()
	store.topologies[row.SingleBlockObjectKey] = ObjectVersionTopology{Versions: []ObjectVersion{{
		VersionID: "single-block-v1",
		ETag:      "etag-v1",
		Bytes:     42,
	}}}

	report, err := testPlanner(&fakeRepo{rows: []MetadataRow{row}}, store).Plan(context.Background(), testRequest(now, false))
	require.NoError(err)
	require.Equal(ActionSkip, report.Items[0].Action)
	require.Equal(SkipUnsafeSingleBlockVersionTopology, report.Items[0].SkipReason)
	require.Zero(store.headCalls[row.Shadow.ConsolidatedObjectKey])
}

func TestPlannerPlan_RejectsMutableNullVersionIDs(t *testing.T) {
	require := require.New(t)
	now := time.Date(2026, 6, 21, 12, 0, 0, 0, time.UTC)
	row := testRow(100, "hash-100", "single-block/100.zstd", "consolidated/canary.cscb.zstd", now.Add(-8*24*time.Hour))
	store := newFakeStore()
	store.topologies[row.SingleBlockObjectKey] = safeTopology("null", "single-block-etag", 42)
	store.heads[row.PrimaryObjectKey] = cscbObjectHead(1024, 1024)

	report, err := testPlanner(&fakeRepo{rows: []MetadataRow{row}}, store).Plan(context.Background(), testRequest(now, false))
	require.NoError(err)
	require.Equal(SkipSingleBlockVersionIDUnavailable, report.Items[0].SkipReason)

	store.topologies[row.SingleBlockObjectKey] = safeTopology("single-block-v1", "single-block-etag", 42)
	head := cscbObjectHead(1024, 1024)
	head.VersionID = "null"
	store.heads[row.PrimaryObjectKey] = head
	report, err = testPlanner(&fakeRepo{rows: []MetadataRow{row}}, store).Plan(context.Background(), testRequest(now, false))
	require.NoError(err)
	require.Equal(SkipMissingCSCBObject, report.Items[0].SkipReason)

	store.topologies[row.SingleBlockObjectKey] = ObjectVersionTopology{
		Versions:      []ObjectVersion{{VersionID: "single-block-v1", ETag: "single-block-etag", Bytes: 42, IsLatest: true}},
		DeleteMarkers: []ObjectDeleteMarker{{VersionID: "null"}},
	}
	store.heads[row.PrimaryObjectKey] = cscbObjectHead(1024, 1024)
	report, err = testPlanner(&fakeRepo{rows: []MetadataRow{row}}, store).Plan(context.Background(), testRequest(now, false))
	require.NoError(err)
	require.Equal(SkipSingleBlockVersionIDUnavailable, report.Items[0].SkipReason)
}

func TestPlannerPlan_RequiresIndependentPayloadComparison(t *testing.T) {
	require := require.New(t)
	now := time.Date(2026, 6, 21, 12, 0, 0, 0, time.UTC)
	row := testRow(100, "hash-100", "single-block/100.zstd", "consolidated/canary.cscb.zstd", now.Add(-8*24*time.Hour))
	store := newFakeStore()
	store.topologies[row.SingleBlockObjectKey] = safeTopology("single-block-v1", "single-block-etag", 42)
	store.heads[row.PrimaryObjectKey] = cscbObjectHead(1024, 1024)
	planner, verifier := newTestPlanner(&fakeRepo{rows: []MetadataRow{row}}, store)
	verifier.verifyErr = errors.New("payloads differ")

	report, err := planner.Plan(context.Background(), testRequest(now, false))
	require.NoError(err)
	require.Equal(ActionSkip, report.Items[0].Action)
	require.Equal(SkipSingleBlockPayloadMismatch, report.Items[0].SkipReason)
	require.Empty(report.Items[0].PayloadSHA256)
}

func TestPlannerPlan_DryRunOutput(t *testing.T) {
	require := require.New(t)
	now := time.Date(2026, 6, 21, 12, 0, 0, 0, time.UTC)
	validatedAt := now.Add(-8 * 24 * time.Hour)
	row := testRow(100, "hash-100", "single-block/100.zstd", "consolidated/canary.cscb.zstd", validatedAt)
	store := newFakeStore()
	store.topologies[row.SingleBlockObjectKey] = safeTopology("single-block-v1", "single-block-etag", 42)
	store.heads[row.Shadow.ConsolidatedObjectKey] = cscbObjectHead(1024, 1024)
	report, err := testPlanner(&fakeRepo{rows: []MetadataRow{row}}, store).Plan(context.Background(), testRequest(now, false))
	require.NoError(err)

	var buf bytes.Buffer
	require.NoError(WriteReportJSON(&buf, report))
	var decoded Report
	require.NoError(json.Unmarshal(buf.Bytes(), &decoded))
	require.True(decoded.DryRun)
	require.Equal("production", decoded.Environment)
	require.Equal("solana", decoded.Blockchain)
	require.Equal("mainnet", decoded.Network)
	require.Equal(uint64(428058000), decoded.StartHeight)
	require.Equal(uint64(428059000), decoded.EndHeight)
	require.Equal(1, decoded.Summary.EligibleRows)
	require.Equal(ActionReportOnly, decoded.Items[0].Action)
	require.Equal("single-block-v1", decoded.Items[0].VersionID)
	require.Empty(decoded.Items[0].SkipReason)
}

func TestPlannerPlan_ExactSolanaCanaryReportShape(t *testing.T) {
	require := require.New(t)
	now := time.Date(2026, 6, 21, 12, 0, 0, 0, time.UTC)
	validatedAt := now.Add(-8 * 24 * time.Hour)
	const (
		startHeight          = uint64(428058000)
		endHeight            = uint64(428059000)
		cscbKey              = "BLOCKCHAIN_SOLANA/NETWORK_SOLANA_MAINNET/consolidated/v=7/shard=00000000042805000000-00000000042806000000/00000000000428058000-00000000000428059000-deadbeef.cscb.zstd"
		bucket               = "co-chainstorage-solana-mainnet-prod"
		singleBlockBytesEach = uint64(123)
	)

	rows := make([]MetadataRow, 0, endHeight-startHeight)
	store := newFakeStore()
	store.heads[cscbKey] = cscbObjectHead(512<<10, 2<<20)
	for height := startHeight; height < endHeight; height++ {
		hash := fmt.Sprintf("solana-hash-%d", height)
		key := fmt.Sprintf("BLOCKCHAIN_SOLANA/NETWORK_SOLANA_MAINNET/0/%d/%s.zstd", height, hash)
		rows = append(rows, testRow(height, hash, key, cscbKey, validatedAt))
		store.topologies[key] = safeTopology(
			fmt.Sprintf("single-block-version-%d", height),
			fmt.Sprintf("single-block-etag-%d", height),
			singleBlockBytesEach,
		)
	}
	req := testRequest(now, false)
	req.Bucket = bucket
	req.StartHeight = startHeight
	req.EndHeight = endHeight
	report, err := testPlanner(&fakeRepo{rows: rows}, store).Plan(context.Background(), req)
	require.NoError(err)

	require.Equal(bucket, report.Bucket)
	require.Equal(startHeight, report.StartHeight)
	require.Equal(endHeight, report.EndHeight)
	require.Len(report.Items, 1000)
	require.Equal(1000, report.Summary.TotalRows)
	require.Equal(1000, report.Summary.EligibleRows)
	require.Zero(report.Summary.SkippedRows)
	require.Equal(uint64(1000)*singleBlockBytesEach, report.Summary.SingleBlockBytes)
	require.Equal(uint64(1000)*singleBlockBytesEach, report.Summary.EligibleBytes)
	require.Equal(1, store.headCalls[cscbKey])
	require.Equal(1, store.policyCalls[cscbKey])
	require.True(report.SafetyGates.CSCBWriteOncePolicyVerified)

	first := report.Items[0]
	require.Equal(startHeight, first.Height)
	require.Equal("solana-hash-428058000", first.Hash)
	require.Equal("single-block-version-428058000", first.VersionID)
	require.Equal(singleBlockBytesEach, first.SingleBlockBytes)
	require.Equal(cscbKey, first.ConsolidatedKey)
	require.Equal(ActionReportOnly, first.Action)
	require.Empty(first.SkipReason)

	last := report.Items[len(report.Items)-1]
	require.Equal(endHeight-1, last.Height)
	require.Equal("solana-hash-428058999", last.Hash)
	require.Equal("single-block-version-428058999", last.VersionID)
	require.Equal(cscbKey, last.ConsolidatedKey)
	require.Equal(ActionReportOnly, last.Action)
}

func TestPlannerApply_RefetchesLiveCSCBWriteOncePolicyImmediatelyBeforeDelete(t *testing.T) {
	require := require.New(t)
	_, repo, store, planner, _, req, report := newExecutableTestFixture(t)
	key := report.Items[0].ConsolidatedKey
	require.Equal(1, store.policyCalls[key])
	// Step the safety-cache clock past the revalidation interval on every
	// lookup so the row after preflight must re-verify against live S3.
	cacheClock := req.Now
	planner.retentionSafetyClock = func() time.Time {
		cacheClock = cacheClock.Add(retentionSafetyRevalidationInterval + time.Second)
		return cacheClock
	}
	store.policyHook = func(hookKey string, call int) (RetentionSafetySnapshot, error) {
		snapshot := RetentionSafetySnapshot{ConfigurationSHA256: keySHA256("safe:" + hookKey)}
		if call >= 3 {
			return snapshot, errors.New("bucket policy was removed")
		}
		return snapshot, nil
	}

	err := planner.Apply(context.Background(), req, report)
	require.Error(err)
	require.Empty(store.deleted)
	require.Equal(SkipCSCBWriteOncePolicyUnverified, report.Items[0].SkipReason)
	require.False(report.SafetyGates.CSCBWriteOncePolicyVerified)
	require.Equal(3, store.policyCalls[key])
	manifest := repo.manifests[report.Items[0].BlockMetadataID]
	require.Equal(RetirementStateDeleting, manifest.State)
	require.Equal(SkipCSCBWriteOncePolicyUnverified, manifest.Outcome)
}

func TestPlannerApply_RequiresStableRetentionSafetyConfigurationBeforeDelete(t *testing.T) {
	require := require.New(t)
	_, repo, store, planner, _, req, report := newExecutableTestFixture(t)
	key := report.Items[0].ConsolidatedKey
	observedAt := req.Now
	repo.safetyNow = observedAt
	repo.safetyObservations = map[string]fakeSafetyObservation{
		req.Bucket + "\x00" + key: {
			configurationSHA256: keySHA256("previous-safe-configuration"),
			firstObservedAt:     observedAt.Add(-RetentionSafetyQuiescencePeriod),
		},
	}
	newConfigurationSHA256 := keySHA256("new-safe-configuration")
	store.policyHashes[key] = newConfigurationSHA256

	err := planner.Apply(context.Background(), req, report)
	require.Error(err)
	require.Empty(store.deleted)
	require.Equal(SkipCSCBSafetyQuiescenceActive, report.Items[0].SkipReason)
	require.Equal(ActionSkip, report.Items[0].Action)
	_, prepared := repo.manifests[report.Items[0].BlockMetadataID]
	require.False(prepared)
	observation := repo.safetyObservations[req.Bucket+"\x00"+key]
	require.Equal(newConfigurationSHA256, observation.configurationSHA256)
	require.Equal(observedAt, observation.firstObservedAt)
}

func TestVerifyRetentionSafetyDoesNotCreditFailedInspectionTime(t *testing.T) {
	require := require.New(t)
	observedAt := time.Date(2026, 6, 21, 12, 0, 0, 0, time.UTC)
	key := "consolidated/canary.cscb.zstd"
	item := Candidate{Bucket: "integration-bucket", ConsolidatedKey: key}
	observationKey := item.Bucket + "\x00" + key
	repo := &fakeRepo{
		safetyNow: observedAt,
		safetyObservations: map[string]fakeSafetyObservation{
			observationKey: {
				configurationSHA256: keySHA256("previous-safe-configuration"),
				firstObservedAt:     observedAt.Add(-time.Hour),
			},
		},
	}
	store := newFakeStore()
	store.policyErrors[key] = errors.New("GetBucketLifecycleConfiguration temporarily unavailable")
	planner := testPlanner(repo, store)

	reason, _, err := planner.verifyCSCBRetentionSafety(context.Background(), &item)
	require.Error(err)
	require.Equal(SkipCSCBWriteOncePolicyUnverified, reason)
	observation := repo.safetyObservations[observationKey]
	require.Equal(unverifiedRetentionSafetySHA256, observation.configurationSHA256)
	require.Equal(observedAt, observation.firstObservedAt)

	repo.safetyNow = observedAt.Add(20 * time.Minute)
	delete(store.policyErrors, key)
	reason, retryAfter, err := planner.verifyCSCBRetentionSafety(context.Background(), &item)
	require.Error(err)
	require.Equal(SkipCSCBSafetyQuiescenceActive, reason)
	require.Equal(RetentionSafetyQuiescencePeriod, retryAfter)
	observation = repo.safetyObservations[observationKey]
	require.Equal(keySHA256("safe:"+key), observation.configurationSHA256)
	require.Equal(repo.safetyNow, observation.firstObservedAt)
}

func TestPlannerApply_VerifiesCSCBWriteOncePolicyOncePerPassWithinInterval(t *testing.T) {
	require := require.New(t)
	now := time.Date(2026, 6, 21, 12, 0, 0, 0, time.UTC)
	cscbKey := "consolidated/shared.cscb.zstd"
	first := testRow(428058000, "hash-428058000", "single-block/428058000.zstd", cscbKey, now.Add(-8*24*time.Hour))
	second := testRow(428058001, "hash-428058001", "single-block/428058001.zstd", cscbKey, now.Add(-8*24*time.Hour))
	repo := &fakeRepo{rows: []MetadataRow{first, second}}
	store := newFakeStore()
	store.deleteMutates = true
	for _, row := range repo.rows {
		store.topologies[row.SingleBlockObjectKey] = safeTopology("single-block-v1", "single-block-etag", 42)
	}
	cscbHead := cscbObjectHead(1024, 1024)
	store.heads[cscbKey] = cscbHead
	store.versionHeads[versionObjectKey(cscbKey, cscbHead.VersionID)] = cscbHead
	planner := testPlanner(repo, store)
	req := testRequest(now, true)
	req.ProductionDeleteEnabled = true
	report, err := planner.Plan(context.Background(), req)
	require.NoError(err)
	require.Equal(1, store.policyCalls[cscbKey])

	// Rows within the revalidation interval reuse the preflight verification
	// instead of re-issuing bucket control-plane calls per delete.
	require.NoError(planner.Apply(context.Background(), req, report))
	require.Len(store.deleted, 2)
	require.Equal(2, store.policyCalls[cscbKey])
}

func TestPlannerHeartbeatsFromRowAndVersionLoops(t *testing.T) {
	require := require.New(t)
	now := time.Date(2026, 6, 21, 12, 0, 0, 0, time.UTC)
	row := testRow(428058000, "hash-428058000", "single-block/428058000.zstd", "consolidated/canary.cscb.zstd", now.Add(-8*24*time.Hour))
	repo := &fakeRepo{rows: []MetadataRow{row}}
	store := newFakeStore()
	store.deleteMutates = true
	store.topologies[row.SingleBlockObjectKey] = safeTopology("single-block-v1", "single-block-etag", 42)
	cscbHead := cscbObjectHead(1024, 1024)
	store.heads[row.PrimaryObjectKey] = cscbHead
	store.versionHeads[versionObjectKey(row.PrimaryObjectKey, cscbHead.VersionID)] = cscbHead
	planner := testPlanner(repo, store)
	var beats []string
	WithHeartbeat(func(_ context.Context, details ...any) {
		require.NotEmpty(details)
		label, ok := details[0].(string)
		require.True(ok)
		beats = append(beats, label)
	})(planner)

	req := testRequest(now, true)
	req.ProductionDeleteEnabled = true
	report, err := planner.Plan(context.Background(), req)
	require.NoError(err)
	require.Contains(beats, "retirement.plan.row")

	require.NoError(planner.Apply(context.Background(), req, report))
	require.Contains(beats, "retirement.apply.row")
	require.Contains(beats, "retirement.delete.version")
}

func TestPlannerApply_RefusesToStartWhenAlreadyCanceled(t *testing.T) {
	require := require.New(t)
	now := time.Date(2026, 6, 21, 12, 0, 0, 0, time.UTC)
	row := testRow(428058000, "hash-428058000", "single-block/428058000.zstd", "consolidated/canary.cscb.zstd", now.Add(-8*24*time.Hour))
	repo := &fakeRepo{rows: []MetadataRow{row}}
	store := newFakeStore()
	store.deleteMutates = true
	store.topologies[row.SingleBlockObjectKey] = safeTopology("single-block-v1", "single-block-etag", 42)
	cscbHead := cscbObjectHead(1024, 1024)
	store.heads[row.PrimaryObjectKey] = cscbHead
	store.versionHeads[versionObjectKey(row.PrimaryObjectKey, cscbHead.VersionID)] = cscbHead
	planner := testPlanner(repo, store)
	req := testRequest(now, true)
	req.ProductionDeleteEnabled = true
	report, err := planner.Plan(context.Background(), req)
	require.NoError(err)
	require.Equal(ActionDeleteObjectVersion, report.Items[0].Action)

	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	err = planner.Apply(canceledCtx, req, report)
	require.ErrorIs(err, context.Canceled)
	require.Empty(store.deleted)
	require.Equal(ActionSkip, report.Items[0].Action)
	require.Equal(SkipNotAttemptedAfterFailure, report.Items[0].SkipReason)
}

func TestPlannerApply_HaltsBetweenRowsOnCancellation(t *testing.T) {
	require := require.New(t)
	now := time.Date(2026, 6, 21, 12, 0, 0, 0, time.UTC)
	cscbKey := "consolidated/shared.cscb.zstd"
	first := testRow(428058000, "hash-428058000", "single-block/428058000.zstd", cscbKey, now.Add(-8*24*time.Hour))
	second := testRow(428058001, "hash-428058001", "single-block/428058001.zstd", cscbKey, now.Add(-8*24*time.Hour))
	repo := &fakeRepo{rows: []MetadataRow{first, second}}
	store := newFakeStore()
	store.deleteMutates = true
	for _, row := range repo.rows {
		store.topologies[row.SingleBlockObjectKey] = safeTopology("single-block-v1", "single-block-etag", 42)
	}
	// The first row carries a second, older version so the cancellation
	// that fires on its first delete is observed by the next delete of the
	// SAME row — inside its claimed operation — rather than only by the
	// second row's pre-claim check.
	firstTopology := store.topologies[first.SingleBlockObjectKey]
	firstTopology.Versions = append(firstTopology.Versions, ObjectVersion{VersionID: "single-block-v0", ETag: "older-etag", Bytes: 42})
	store.topologies[first.SingleBlockObjectKey] = firstTopology
	cscbHead := cscbObjectHead(1024, 1024)
	store.heads[cscbKey] = cscbHead
	store.versionHeads[versionObjectKey(cscbKey, cscbHead.VersionID)] = cscbHead
	planner := testPlanner(repo, store)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// Cancellation arrives while the first row is deleting; the second row
	// must never be attempted.
	WithHeartbeat(func(_ context.Context, details ...any) {
		if len(details) > 0 && details[0] == "retirement.delete.version" {
			cancel()
		}
	})(planner)

	req := testRequest(now, true)
	req.ProductionDeleteEnabled = true
	report, err := planner.Plan(context.Background(), req)
	require.NoError(err)

	err = planner.Apply(ctx, req, report)
	require.ErrorIs(err, context.Canceled)
	require.Len(store.deleted, 1)
	require.Equal(ActionSkip, report.Items[1].Action)
	require.Equal(SkipNotAttemptedAfterFailure, report.Items[1].SkipReason)
	// The interrupted row's claim is released with its outcome recorded, so
	// the attempt Temporal reschedules can take it over immediately instead
	// of waiting out the lease (INF-1603). The never-attempted row was never
	// claimed, so nothing is released for it.
	require.Equal(1, repo.releaseCalls)
	interrupted := repo.manifests[first.BlockMetadataID]
	require.NotNil(interrupted.ClaimExpiresAt)
	require.False(interrupted.ClaimExpiresAt.After(time.Now().UTC()), "claim must be expired, got %s", interrupted.ClaimExpiresAt)
	require.Equal(SkipRetentionRunCanceled, interrupted.Outcome)
	// Both cleanup writes ran on a context that outlived the cancellation
	// (otherwise a stopping worker could never record or release) but that
	// carried a deadline inside the cleanup budget, so a stalled database
	// cannot hold the shutdown past the worker's stop timeout.
	require.Len(repo.cleanupContexts, 2, "outcome write then release")
	for i, observed := range repo.cleanupContexts {
		require.True(observed.live, "cleanup write %d ran on an already-canceled context", i)
		require.True(observed.hasDeadline, "cleanup write %d has no deadline", i)
		require.False(observed.deadline.After(time.Now().Add(RetirementClaimCleanupTimeout)), "cleanup write %d deadline %s exceeds the cleanup budget", i, observed.deadline)
	}
}

// TestPlannerApply_ReleasesClaimWhenOutcomeWriteExhaustsItsDeadline pins that
// the release does not share the outcome write's deadline: a stalled outcome
// write that runs to its timeout must leave the release a live, freshly
// bounded context, or the row stays leased for the full RetirementClaimLease
// on the worker that is going away (watch-owl on PR #212).
func TestPlannerApply_ReleasesClaimWhenOutcomeWriteExhaustsItsDeadline(t *testing.T) {
	require := require.New(t)
	now := time.Date(2026, 6, 21, 12, 0, 0, 0, time.UTC)
	cscbKey := "consolidated/shared.cscb.zstd"
	row := testRow(428058000, "hash-428058000", "single-block/428058000.zstd", cscbKey, now.Add(-8*24*time.Hour))
	repo := &fakeRepo{rows: []MetadataRow{row}, outcomeBlocksUntilDeadline: true}
	store := newFakeStore()
	store.deleteMutates = true
	topology := safeTopology("single-block-v1", "single-block-etag", 42)
	topology.Versions = append(topology.Versions, ObjectVersion{VersionID: "single-block-v0", ETag: "older-etag", Bytes: 42})
	store.topologies[row.SingleBlockObjectKey] = topology
	cscbHead := cscbObjectHead(1024, 1024)
	store.heads[cscbKey] = cscbHead
	store.versionHeads[versionObjectKey(cscbKey, cscbHead.VersionID)] = cscbHead
	planner := testPlanner(repo, store)
	planner.claimCleanupTimeout = 50 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	WithHeartbeat(func(_ context.Context, details ...any) {
		if len(details) > 0 && details[0] == "retirement.delete.version" {
			cancel()
		}
	})(planner)

	req := testRequest(now, true)
	req.ProductionDeleteEnabled = true
	report, err := planner.Plan(context.Background(), req)
	require.NoError(err)

	started := time.Now()
	err = planner.Apply(ctx, req, report)
	require.ErrorIs(err, context.Canceled)
	// The outcome write's own deadline failure is reported alongside the
	// cancellation, not swallowed.
	require.ErrorIs(err, context.DeadlineExceeded)
	require.Len(repo.cleanupContexts, 2, "outcome write then release")
	outcomeCtx, releaseCtx := repo.cleanupContexts[0], repo.cleanupContexts[1]
	require.True(outcomeCtx.live && outcomeCtx.hasDeadline)
	// The outcome write consumed its whole deadline...
	require.False(outcomeCtx.deadline.After(time.Now()), "outcome deadline %s should have expired by now", outcomeCtx.deadline)
	// ...and the release still ran live, on a deadline that started after
	// the outcome write gave up rather than one shared with it.
	require.True(releaseCtx.live, "release ran on an expired context")
	require.True(releaseCtx.hasDeadline)
	require.True(releaseCtx.deadline.After(outcomeCtx.deadline), "release deadline %s must postdate the outcome deadline %s", releaseCtx.deadline, outcomeCtx.deadline)
	require.False(releaseCtx.deadline.After(started.Add(2*planner.claimCleanupTimeout).Add(time.Second)), "release deadline %s exceeds the cleanup budget", releaseCtx.deadline)
	require.Equal(1, repo.releaseCalls)
	released := repo.manifests[row.BlockMetadataID]
	require.NotNil(released.ClaimExpiresAt)
	require.False(released.ClaimExpiresAt.After(time.Now().UTC()), "claim must be released, got %s", released.ClaimExpiresAt)
}

// TestPlannerApply_StalledOutcomeWriteKeepsLeaseOnPlainFailure pins that the
// release decision reads the operation's own error, not the joined cleanup
// errors. An outcome write that times out contributes a DeadlineExceeded to
// the returned error; if the release condition read that, an ordinary S3
// failure whose cleanup happened to stall would give up the lease that is
// meant to be the row's cool-down before it is retried (INF-1603).
func TestPlannerApply_StalledOutcomeWriteKeepsLeaseOnPlainFailure(t *testing.T) {
	require := require.New(t)
	now := time.Date(2026, 6, 21, 12, 0, 0, 0, time.UTC)
	cscbKey := "consolidated/shared.cscb.zstd"
	row := testRow(428058000, "hash-428058000", "single-block/428058000.zstd", cscbKey, now.Add(-8*24*time.Hour))
	repo := &fakeRepo{rows: []MetadataRow{row}, outcomeBlocksUntilDeadline: true}
	store := newFakeStore()
	store.deleteMutates = true
	store.topologies[row.SingleBlockObjectKey] = safeTopology("single-block-v1", "single-block-etag", 42)
	cscbHead := cscbObjectHead(1024, 1024)
	store.heads[cscbKey] = cscbHead
	store.versionHeads[versionObjectKey(cscbKey, cscbHead.VersionID)] = cscbHead
	store.deleteErrors["single-block-v1"] = errors.New("s3 delete failed")
	planner := testPlanner(repo, store)
	planner.claimCleanupTimeout = 50 * time.Millisecond

	req := testRequest(now, true)
	req.ProductionDeleteEnabled = true
	report, err := planner.Plan(context.Background(), req)
	require.NoError(err)

	err = planner.Apply(context.Background(), req, report)
	require.Error(err)
	require.NotErrorIs(err, context.Canceled)
	// The stalled outcome write's deadline error is joined into the result...
	require.ErrorIs(err, context.DeadlineExceeded)
	// ...but it is cleanup noise, not the operation's verdict: the row failed
	// on S3, so the claim keeps its lease and the release never runs.
	require.Equal(0, repo.releaseCalls)
	require.Len(repo.cleanupContexts, 1, "only the outcome write should have run")
	failed := repo.manifests[row.BlockMetadataID]
	require.NotNil(failed.ClaimExpiresAt)
	require.True(failed.ClaimExpiresAt.After(time.Now().UTC()), "claim lease must be kept, got %s", failed.ClaimExpiresAt)
}

func TestPlannerApply_KeepsClaimLeaseOnNonCancellationFailure(t *testing.T) {
	require := require.New(t)
	now := time.Date(2026, 6, 21, 12, 0, 0, 0, time.UTC)
	cscbKey := "consolidated/shared.cscb.zstd"
	row := testRow(428058000, "hash-428058000", "single-block/428058000.zstd", cscbKey, now.Add(-8*24*time.Hour))
	repo := &fakeRepo{rows: []MetadataRow{row}}
	store := newFakeStore()
	store.deleteMutates = true
	store.topologies[row.SingleBlockObjectKey] = safeTopology("single-block-v1", "single-block-etag", 42)
	cscbHead := cscbObjectHead(1024, 1024)
	store.heads[cscbKey] = cscbHead
	store.versionHeads[versionObjectKey(cscbKey, cscbHead.VersionID)] = cscbHead
	store.deleteErrors["single-block-v1"] = errors.New("s3 delete failed")
	planner := testPlanner(repo, store)
	req := testRequest(now, true)
	req.ProductionDeleteEnabled = true
	report, err := planner.Plan(context.Background(), req)
	require.NoError(err)
	err = planner.Apply(context.Background(), req, report)
	require.Error(err)
	require.NotErrorIs(err, context.Canceled)
	// A failure that is not a cancellation keeps its lease: the claim is the
	// cool-down before the row is retried, and only a run that is going away
	// gives it up early.
	require.Equal(0, repo.releaseCalls)
	failed := repo.manifests[row.BlockMetadataID]
	require.NotNil(failed.ClaimExpiresAt)
	require.True(failed.ClaimExpiresAt.After(time.Now().UTC()))
}

func TestPlannerApply_RequiresProductionGateAndFinalizesMetadata(t *testing.T) {
	require := require.New(t)
	now := time.Date(2026, 6, 21, 12, 0, 0, 0, time.UTC)
	row := testRow(428058000, "hash-428058000", "single-block/428058000.zstd", "consolidated/canary.cscb.zstd", now.Add(-8*24*time.Hour))
	repo := &fakeRepo{rows: []MetadataRow{row}}
	store := newFakeStore()
	store.deleteMutates = true
	store.topologies[row.SingleBlockObjectKey] = safeTopology("single-block-v1", "single-block-etag", 42)
	cscbHead := cscbObjectHead(1024, 1024)
	store.heads[row.PrimaryObjectKey] = cscbHead
	store.versionHeads[versionObjectKey(row.PrimaryObjectKey, cscbHead.VersionID)] = cscbHead
	planner := testPlanner(repo, store)
	req := testRequest(now, true)
	req.ProductionDeleteEnabled = true
	report, err := planner.Plan(context.Background(), req)
	require.NoError(err)
	require.Equal(ActionDeleteObjectVersion, report.Items[0].Action)

	blockedReq := req
	blockedReq.ProductionDeleteEnabled = false
	err = planner.Apply(context.Background(), blockedReq, report)
	require.Error(err)
	require.Empty(store.deleted)

	err = planner.Apply(context.Background(), req, report)
	require.NoError(err)
	require.Len(store.deleted, 1)
	require.Equal("single-block-v1", store.deleted[0].VersionID)
	require.Equal(ActionDeletedVerified, report.Items[0].Action)
	require.Empty(report.Items[0].Key)
	require.NotNil(report.Items[0].SingleBlockDeletedAt)
	require.NotNil(report.Items[0].RetirementVerifiedAt)
	manifest := repo.manifests[row.BlockMetadataID]
	require.Equal(RetirementStateDeletedVerified, manifest.State)
	require.Empty(manifest.SingleBlockObjectKey)
	require.NotNil(manifest.DeletedAt)
	require.Empty(repo.rows[0].SingleBlockObjectKey)
	require.Empty(repo.rows[0].Shadow.SingleBlockObjectKey)
	require.NotNil(repo.rows[0].Shadow.SingleBlockObjectDeletedAt)

	dryRunReq := req
	dryRunReq.Execute = false
	dryRunReq.ProductionDeleteEnabled = false
	finalReport, err := planner.Plan(context.Background(), dryRunReq)
	require.NoError(err)
	require.Equal(ActionAlreadyDeleted, finalReport.Items[0].Action)
	require.NotNil(finalReport.Items[0].SingleBlockDeletedAt)
	require.NotNil(finalReport.Items[0].RetirementVerifiedAt)
	require.Empty(finalReport.Items[0].Key)
	require.Equal(keySHA256(row.SingleBlockObjectKey), manifest.SingleBlockObjectKeySHA256)

	v2Req := dryRunReq
	v2Req.Bucket = "generation-v2"
	v2Req.StorageGeneration = "v2"
	v2Report, err := planner.Plan(context.Background(), v2Req)
	require.NoError(err)
	require.Len(v2Report.Items, 1)
	require.Equal(ActionSkip, v2Report.Items[0].Action)
	require.Equal(SkipStorageGenerationMismatch, v2Report.Items[0].SkipReason)
}

func TestPlannerApply_DeletesEveryPinnedVersionAndDeleteMarker(t *testing.T) {
	require := require.New(t)
	row, repo, store, planner, _, req, report := newMultiVersionExecutableTestFixture(t)

	err := planner.Apply(context.Background(), req, report)
	require.NoError(err)
	require.Equal(ActionDeletedVerified, report.Items[0].Action)
	require.Len(store.deleted, 3)
	require.Equal([]string{"single-block-v1", "single-block-v2", "delete-marker-v1"}, []string{
		store.deleted[0].VersionID,
		store.deleted[1].VersionID,
		store.deleted[2].VersionID,
	})
	require.Empty(store.topologies[row.SingleBlockObjectKey].Versions)
	require.Empty(store.topologies[row.SingleBlockObjectKey].DeleteMarkers)
	require.Empty(repo.rows[0].SingleBlockObjectKey)
	require.Empty(repo.rows[0].Shadow.SingleBlockObjectKey)
	manifest := repo.manifests[row.BlockMetadataID]
	require.Equal(RetirementStateDeletedVerified, manifest.State)
	require.Equal([]string{"single-block-v2", "single-block-v1"}, manifest.SingleBlockObjectVersionIDs)
	require.Equal([]string{"delete-marker-v1"}, manifest.SingleBlockDeleteMarkerVersionIDs)
}

func TestPlannerReconcile_ResumesPartiallyDeletedPinnedTopology(t *testing.T) {
	require := require.New(t)
	row, repo, store, planner, _, req, report := newMultiVersionExecutableTestFixture(t)
	store.deleteErrors["single-block-v2"] = errors.New("temporary S3 failure")

	err := planner.Apply(context.Background(), req, report)
	require.Error(err)
	require.Equal([]string{"single-block-v1"}, []string{store.deleted[0].VersionID})
	require.Equal(RetirementStateDeleting, repo.manifests[row.BlockMetadataID].State)
	require.Len(store.topologies[row.SingleBlockObjectKey].Versions, 1)
	require.Len(store.topologies[row.SingleBlockObjectKey].DeleteMarkers, 1)
	require.Equal(row.SingleBlockObjectKey, repo.rows[0].SingleBlockObjectKey)

	delete(store.deleteErrors, "single-block-v2")
	manifest := repo.manifests[row.BlockMetadataID]
	expiredAt := time.Now().UTC().Add(-time.Second)
	manifest.ClaimExpiresAt = &expiredAt
	repo.manifests[row.BlockMetadataID] = manifest
	repo.updateManifestRow(manifest)

	reconcileReport, err := planner.Reconcile(context.Background(), req)
	require.NoError(err)
	require.Equal(ActionDeletedVerified, reconcileReport.Items[0].Action)
	require.Equal([]string{"single-block-v1", "single-block-v2", "delete-marker-v1"}, []string{
		store.deleted[0].VersionID,
		store.deleted[1].VersionID,
		store.deleted[2].VersionID,
	})
	require.Empty(repo.rows[0].SingleBlockObjectKey)
	require.Empty(repo.rows[0].Shadow.SingleBlockObjectKey)
	require.Equal(RetirementStateDeletedVerified, repo.manifests[row.BlockMetadataID].State)
}

func TestPlannerReconcile_ExcludesManifestBeyondFrozenEligibilityCutoff(t *testing.T) {
	require := require.New(t)
	_, repo, store, planner, _, req, _ := newMultiVersionExecutableTestFixture(t)
	cutoff := req.Now
	deleteAfter := cutoff.Add(time.Hour)
	repo.rows[0].Shadow.SingleBlockDeleteAfter = &deleteAfter
	req.Now = cutoff.Add(2 * time.Hour)
	req.EligibilityCutoff = cutoff

	report, err := planner.Reconcile(context.Background(), req)
	require.NoError(err)
	require.Empty(report.Items)
	require.Empty(store.deleted)
}

func TestPlannerReconcile_RejectsUnpinnedVersionAfterPartialDeletion(t *testing.T) {
	require := require.New(t)
	row, repo, store, planner, _, req, report := newMultiVersionExecutableTestFixture(t)
	store.deleteErrors["single-block-v2"] = errors.New("temporary S3 failure")
	require.Error(planner.Apply(context.Background(), req, report))

	delete(store.deleteErrors, "single-block-v2")
	topology := store.topologies[row.SingleBlockObjectKey]
	topology.Versions = append(topology.Versions, ObjectVersion{
		VersionID: "unpinned-v3",
		ETag:      "unpinned-etag",
		Bytes:     40,
	})
	store.topologies[row.SingleBlockObjectKey] = topology
	manifest := repo.manifests[row.BlockMetadataID]
	expiredAt := time.Now().UTC().Add(-time.Second)
	manifest.ClaimExpiresAt = &expiredAt
	repo.manifests[row.BlockMetadataID] = manifest
	repo.updateManifestRow(manifest)

	reconcileReport, err := planner.Reconcile(context.Background(), req)
	require.Error(err)
	require.Equal(ActionSkip, reconcileReport.Items[0].Action)
	require.Equal(SkipUnsafeSingleBlockVersionTopology, reconcileReport.Items[0].SkipReason)
	require.Len(store.deleted, 1)
	require.Equal(row.SingleBlockObjectKey, repo.rows[0].SingleBlockObjectKey)
}

func TestPlannerApply_RejectsTamperedReportBeforeManifestWrite(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(req PlanRequest, report *Report)
	}{
		{
			name: "single-block key",
			mutate: func(req PlanRequest, report *Report) {
				report.Items[0].Key = "single-block/other.zstd"
			},
		},
		{
			name: "height outside approval",
			mutate: func(req PlanRequest, report *Report) {
				report.Items[0].Height = req.EndHeight
			},
		},
		{
			name: "dry-run report",
			mutate: func(req PlanRequest, report *Report) {
				report.DryRun = true
			},
		},
		{
			name: "production gate mismatch",
			mutate: func(req PlanRequest, report *Report) {
				report.SafetyGates.ProductionDeleteEnabled = false
			},
		},
		{
			name: "single-block writer gate mismatch",
			mutate: func(req PlanRequest, report *Report) {
				report.SafetyGates.SingleBlockWritersGuarded = false
			},
		},
		{
			name: "write-once policy gate mismatch",
			mutate: func(req PlanRequest, report *Report) {
				report.SafetyGates.CSCBWriteOncePolicyVerified = false
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			require := require.New(t)
			_, repo, store, planner, _, req, report := newExecutableTestFixture(t)
			test.mutate(req, report)
			err := planner.Apply(context.Background(), req, report)
			require.Error(err)
			require.Empty(repo.manifests)
			require.Empty(store.deleted)
		})
	}
}

func TestPlannerApply_RevalidatesTopologyAfterManifestPersistence(t *testing.T) {
	require := require.New(t)
	_, repo, store, planner, _, req, report := newExecutableTestFixture(t)
	safe := store.topologies[report.Items[0].Key]
	store.listHook = func(key string, call int) ObjectVersionTopology {
		if call < 3 {
			return safe
		}
		return ObjectVersionTopology{Versions: []ObjectVersion{
			safe.Versions[0],
			{VersionID: "unexpected-v2", ETag: "unexpected-etag", Bytes: 1, IsLatest: false},
		}}
	}

	err := planner.Apply(context.Background(), req, report)
	require.Error(err)
	require.Empty(store.deleted)
	require.Equal(SkipUnsafeSingleBlockVersionTopology, report.Items[0].SkipReason)
	manifest := repo.manifests[report.Items[0].BlockMetadataID]
	require.Equal(RetirementStateDeleting, manifest.State)
	require.NotEmpty(manifest.SingleBlockObjectKey)
	require.Equal(1, manifest.AttemptCount)
	require.Equal(SkipUnsafeSingleBlockVersionTopology, manifest.Outcome)
}

func TestPlannerApply_StopsAfterFirstFailure(t *testing.T) {
	require := require.New(t)
	now := time.Date(2026, 6, 21, 12, 0, 0, 0, time.UTC)
	first := testRow(428058000, "hash-428058000", "single-block/428058000.zstd", "consolidated/first.cscb.zstd", now.Add(-8*24*time.Hour))
	second := testRow(428058001, "hash-428058001", "single-block/428058001.zstd", "consolidated/second.cscb.zstd", now.Add(-8*24*time.Hour))
	repo := &fakeRepo{rows: []MetadataRow{first, second}}
	store := newFakeStore()
	store.deleteMutates = true
	for _, row := range repo.rows {
		store.topologies[row.SingleBlockObjectKey] = safeTopology("single-block-v1", "single-block-etag", 42)
		head := cscbObjectHead(1024, 1024)
		store.heads[row.PrimaryObjectKey] = head
		store.versionHeads[versionObjectKey(row.PrimaryObjectKey, head.VersionID)] = head
	}
	planner := testPlanner(repo, store)
	req := testRequest(now, true)
	req.ProductionDeleteEnabled = true
	report, err := planner.Plan(context.Background(), req)
	require.NoError(err)
	require.Len(report.Items, 2)

	firstSafe := store.topologies[first.SingleBlockObjectKey]
	store.listHook = func(key string, call int) ObjectVersionTopology {
		if key != first.SingleBlockObjectKey || call < 3 {
			return store.topologies[key]
		}
		return ObjectVersionTopology{Versions: []ObjectVersion{
			firstSafe.Versions[0],
			{VersionID: "unexpected-v2", ETag: "unexpected-etag", Bytes: 1},
		}}
	}

	err = planner.Apply(context.Background(), req, report)
	require.Error(err)
	require.Empty(store.deleted)
	require.Equal(SkipUnsafeSingleBlockVersionTopology, report.Items[0].SkipReason)
	require.Equal(SkipNotAttemptedAfterFailure, report.Items[1].SkipReason)
	_, secondManifestExists := repo.manifests[second.BlockMetadataID]
	require.False(secondManifestExists)
}

func TestPlannerApply_DoesNotFinalizeWhenCSCBChangesAfterSingleBlockDelete(t *testing.T) {
	require := require.New(t)
	row, repo, store, planner, _, req, report := newExecutableTestFixture(t)
	store.headHook = func(key string, call int) ObjectHead {
		head := store.heads[key]
		// Call 1 is Plan, call 2 the pre-fence revalidation, call 3 the
		// post-claim revalidation; call 4 is the post-delete verification that
		// must observe the replaced CSCB.
		if key == row.PrimaryObjectKey && call >= 4 {
			head.VersionID = "replacement-version"
			head.ETag = "replacement-etag"
		}
		return head
	}

	err := planner.Apply(context.Background(), req, report)
	require.Error(err)
	require.Len(store.deleted, 1)
	require.Equal(ActionDeletedObjectVersion, report.Items[0].Action)
	require.Equal(1, report.Summary.DeletedRows)
	require.Equal(SkipCSCBObjectChanged, report.Items[0].SkipReason)
	require.Equal(RetirementStateDeletedPendingVerification, repo.manifests[row.BlockMetadataID].State)
	require.Empty(repo.manifests[row.BlockMetadataID].SingleBlockObjectKey)
	require.Empty(repo.rows[0].SingleBlockObjectKey)
	require.Empty(repo.rows[0].Shadow.SingleBlockObjectKey)
	require.NotNil(repo.rows[0].Shadow.SingleBlockObjectDeletedAt)
	require.Equal(1, repo.recordDeletedCalls)
	require.Zero(repo.finalizeCalls)

	v2Req := req
	v2Req.Execute = false
	v2Req.ProductionDeleteEnabled = false
	v2Req.Bucket = "generation-v2"
	v2Req.StorageGeneration = "v2"
	v2Report, err := planner.Plan(context.Background(), v2Req)
	require.NoError(err)
	require.Len(v2Report.Items, 1)
	require.Equal(ActionSkip, v2Report.Items[0].Action)
	require.Equal(SkipStorageGenerationMismatch, v2Report.Items[0].SkipReason)
}

func TestPlannerApply_UsesFreshVerifierAfterRecordingDeletion(t *testing.T) {
	require := require.New(t)
	row, repo, store, planner, _, req, report := newExecutableTestFixture(t)
	digest := strings.Repeat("a", sha256HexLength)
	created := 0
	planner.verifierFactory = func() blockPayloadVerifier {
		created++
		verifier := &fakePayloadVerifier{digest: digest, consolidatedDigest: digest}
		if created == 2 {
			verifier.consolidatedErr = errors.New("fresh ranged read failed")
		}
		return verifier
	}

	err := planner.Apply(context.Background(), req, report)
	require.Error(err)
	require.Contains(err.Error(), "fresh ranged read failed")
	require.Equal(2, created)
	require.Len(store.deleted, 1)
	require.Equal(ActionDeletedObjectVersion, report.Items[0].Action)
	require.Equal(1, report.Summary.DeletedRows)
	require.Equal(RetirementStateDeletedPendingVerification, repo.manifests[row.BlockMetadataID].State)
	require.Empty(repo.rows[0].SingleBlockObjectKey)
	require.Zero(repo.finalizeCalls)
}

func TestPlannerApply_RejectsBlockThatIsNoLongerCanonical(t *testing.T) {
	require := require.New(t)
	_, repo, store, planner, _, req, report := newExecutableTestFixture(t)
	repo.rows[0].Canonical = false

	err := planner.Apply(context.Background(), req, report)
	require.Error(err)
	require.Equal(SkipMetadataChanged, report.Items[0].SkipReason)
	require.Empty(repo.manifests)
	require.Empty(store.deleted)
}

func TestPlannerApply_RejectsStorageGenerationChangeAfterPlan(t *testing.T) {
	require := require.New(t)
	_, repo, store, planner, _, req, report := newExecutableTestFixture(t)
	repo.rows[0].PrimaryStorageGeneration = "v2"
	repo.rows[0].Shadow.SingleBlockStorageGeneration = "v2"
	repo.rows[0].Shadow.ConsolidatedStorageGeneration = "v2"

	err := planner.Apply(context.Background(), req, report)
	require.Error(err)
	require.Equal(ActionSkip, report.Items[0].Action)
	require.Equal(SkipMetadataChanged, report.Items[0].SkipReason)
	require.Empty(repo.manifests)
	require.Empty(store.deleted)
}

func TestPlannerReconcile_RequiresGuardedSingleBlockWritersForExecution(t *testing.T) {
	require := require.New(t)
	_, _, _, planner, _, req, _ := newExecutableTestFixture(t)
	req.SingleBlockWritersGuarded = false

	report, err := planner.Reconcile(context.Background(), req)
	require.ErrorContains(err, "single-block writers")
	require.Nil(report)
}

func TestPlannerReconcile_CompletesCrashBeforeDeletionRecord(t *testing.T) {
	require := require.New(t)
	row, repo, store, planner, _, req, report := newExecutableTestFixture(t)
	repo.recordDeletedErr = errors.New("database unavailable")

	err := planner.Apply(context.Background(), req, report)
	require.Error(err)
	require.Equal(ActionSkip, report.Items[0].Action)
	require.Equal(SkipPostDeleteVerificationFailed, report.Items[0].SkipReason)
	require.Len(store.deleted, 1)
	require.Equal(RetirementStateDeleting, repo.manifests[row.BlockMetadataID].State)
	require.Equal(row.SingleBlockObjectKey, repo.rows[0].SingleBlockObjectKey)

	repo.recordDeletedErr = nil
	manifest := repo.manifests[row.BlockMetadataID]
	expiredAt := time.Now().UTC().Add(-time.Second)
	manifest.ClaimExpiresAt = &expiredAt
	repo.manifests[row.BlockMetadataID] = manifest
	repo.updateManifestRow(manifest)
	repo.rows[0].Canonical = false
	reconcileReport, err := planner.Reconcile(context.Background(), req)
	require.NoError(err)
	require.Len(reconcileReport.Items, 1)
	require.Equal(ActionDeletedVerified, reconcileReport.Items[0].Action)
	require.Len(store.deleted, 1)
	require.Equal(RetirementStateDeletedVerified, repo.manifests[row.BlockMetadataID].State)
	require.Empty(repo.rows[0].SingleBlockObjectKey)
	require.Empty(repo.rows[0].Shadow.SingleBlockObjectKey)
}

func TestPlannerReconcile_CompletesCrashAfterDeletionRecord(t *testing.T) {
	require := require.New(t)
	row, repo, store, planner, verifier, req, report := newExecutableTestFixture(t)
	repo.finalizeErr = errors.New("database unavailable")

	err := planner.Apply(context.Background(), req, report)
	require.Error(err)
	require.Equal(ActionDeletedObjectVersion, report.Items[0].Action)
	require.Equal(SkipPostDeleteVerificationFailed, report.Items[0].SkipReason)
	require.Len(store.deleted, 1)
	require.Equal(RetirementStateDeletedPendingVerification, repo.manifests[row.BlockMetadataID].State)
	require.Equal(SkipPostDeleteVerificationFailed, repo.manifests[row.BlockMetadataID].Outcome)
	require.Empty(repo.rows[0].SingleBlockObjectKey)
	require.Empty(repo.rows[0].Shadow.SingleBlockObjectKey)
	require.NotNil(repo.rows[0].Shadow.SingleBlockObjectDeletedAt)
	dryRunReq := req
	dryRunReq.Execute = false
	dryRunReq.ProductionDeleteEnabled = false
	dryRunReport, err := planner.Reconcile(context.Background(), dryRunReq)
	require.NoError(err)
	require.Equal(ActionDeletedObjectVersion, dryRunReport.Items[0].Action)
	require.Equal(SkipRetirementVerificationPending, dryRunReport.Items[0].SkipReason)
	require.Equal(1, dryRunReport.Summary.DeletedRows)
	require.NotNil(dryRunReport.Items[0].SingleBlockDeletedAt)
	require.Nil(dryRunReport.Items[0].RetirementVerifiedAt)

	repo.finalizeErr = nil
	manifest := repo.manifests[row.BlockMetadataID]
	expiredAt := time.Now().UTC().Add(-time.Second)
	manifest.ClaimExpiresAt = &expiredAt
	repo.manifests[row.BlockMetadataID] = manifest
	repo.updateManifestRow(manifest)
	repo.rows[0].Canonical = false
	reconcileReport, err := planner.Reconcile(context.Background(), req)
	require.NoError(err)
	require.Len(reconcileReport.Items, 1)
	require.Equal(ActionDeletedVerified, reconcileReport.Items[0].Action)
	require.Equal(1, reconcileReport.Summary.DeletedRows)
	require.Len(store.deleted, 1)
	require.Equal(2, verifier.consolidatedCalls)
	require.Equal(RetirementStateDeletedVerified, repo.manifests[row.BlockMetadataID].State)
	require.Empty(repo.rows[0].SingleBlockObjectKey)
	require.Empty(repo.rows[0].Shadow.SingleBlockObjectKey)
	require.NotNil(repo.rows[0].Shadow.SingleBlockObjectDeletedAt)
}

func TestPlannerReconcile_RejectsStorageGenerationChangeBeforePendingVerification(t *testing.T) {
	require := require.New(t)
	_, repo, store, planner, _, req, report := newExecutableTestFixture(t)
	repo.finalizeErr = errors.New("database unavailable")

	err := planner.Apply(context.Background(), req, report)
	require.Error(err)
	require.Equal(RetirementStateDeletedPendingVerification, repo.manifests[repo.rows[0].BlockMetadataID].State)
	finalizeCalls := repo.finalizeCalls

	repo.finalizeErr = nil
	manifest := repo.manifests[repo.rows[0].BlockMetadataID]
	expiredAt := time.Now().UTC().Add(-time.Second)
	manifest.ClaimExpiresAt = &expiredAt
	repo.manifests[repo.rows[0].BlockMetadataID] = manifest
	repo.updateManifestRow(manifest)
	repo.rows[0].PrimaryStorageGeneration = "v2"
	repo.rows[0].Shadow.SingleBlockStorageGeneration = "v2"
	repo.rows[0].Shadow.ConsolidatedStorageGeneration = "v2"
	policyCalls := store.policyCalls[report.Items[0].ConsolidatedKey]
	headCalls := store.headCalls[report.Items[0].ConsolidatedKey]
	versionCalls := store.versionCalls[versionObjectKey(report.Items[0].ConsolidatedKey, report.Items[0].CSCBVersionID)]

	reconcileReport, err := planner.Reconcile(context.Background(), req)
	require.NoError(err)
	require.Len(reconcileReport.Items, 1)
	require.Equal(ActionDeletedObjectVersion, reconcileReport.Items[0].Action)
	require.Equal(SkipMetadataChanged, reconcileReport.Items[0].SkipReason)
	require.Equal(finalizeCalls, repo.finalizeCalls)
	require.Len(store.deleted, 1)
	require.Equal(policyCalls, store.policyCalls[report.Items[0].ConsolidatedKey])
	require.Equal(headCalls, store.headCalls[report.Items[0].ConsolidatedKey])
	require.Equal(versionCalls, store.versionCalls[versionObjectKey(report.Items[0].ConsolidatedKey, report.Items[0].CSCBVersionID)])
	require.Equal(RetirementStateDeletedPendingVerification, repo.manifests[repo.rows[0].BlockMetadataID].State)
}

func TestPlannerReconcile_DoesNotFinalizeExternallyMissingEligibleObject(t *testing.T) {
	require := require.New(t)
	row, repo, store, planner, _, req, report := newExecutableTestFixture(t)
	manifest := manifestFromCandidate(report.Items[0], req.Now)
	require.NoError(repo.PrepareRetirement(context.Background(), manifest, req.StorageGeneration))
	delete(store.topologies, row.SingleBlockObjectKey)

	reconcileReport, err := planner.Reconcile(context.Background(), req)
	require.Error(err)
	require.Len(reconcileReport.Items, 1)
	require.Equal(ActionSkip, reconcileReport.Items[0].Action)
	require.Equal(SkipMetadataChanged, reconcileReport.Items[0].SkipReason)
	require.Equal(RetirementStateEligible, repo.manifests[row.BlockMetadataID].State)
	require.Equal(row.SingleBlockObjectKey, repo.rows[0].SingleBlockObjectKey)
	require.Zero(repo.finalizeCalls)
}

func TestPlannerReconcile_SkipsActiveRetirementClaim(t *testing.T) {
	require := require.New(t)
	row, repo, store, planner, _, req, report := newExecutableTestFixture(t)
	manifest := manifestFromCandidate(report.Items[0], req.Now)
	require.NoError(repo.PrepareRetirement(context.Background(), manifest, req.StorageGeneration))
	claimedAt := time.Now().UTC()
	require.NoError(repo.ClaimRetirement(
		context.Background(),
		row.BlockMetadataID,
		"active-claim",
		claimedAt,
		claimedAt.Add(time.Hour),
	))

	reconcileReport, err := planner.Reconcile(context.Background(), req)
	require.NoError(err)
	require.Len(reconcileReport.Items, 1)
	require.Equal(ActionSkip, reconcileReport.Items[0].Action)
	require.Equal(SkipRetirementClaimActive, reconcileReport.Items[0].SkipReason)
	require.Empty(store.deleted)
	require.Equal(1, repo.manifests[row.BlockMetadataID].AttemptCount)
}

func TestPlannerReconcile_TakesOverExpiredRetirementClaim(t *testing.T) {
	require := require.New(t)
	row, repo, store, planner, _, req, report := newExecutableTestFixture(t)
	manifest := manifestFromCandidate(report.Items[0], req.Now)
	require.NoError(repo.PrepareRetirement(context.Background(), manifest, req.StorageGeneration))
	claimedAt := time.Now().UTC().Add(-2 * time.Hour)
	require.NoError(repo.ClaimRetirement(
		context.Background(),
		row.BlockMetadataID,
		"expired-claim",
		claimedAt,
		claimedAt.Add(time.Hour),
	))

	reconcileReport, err := planner.Reconcile(context.Background(), req)
	require.NoError(err)
	require.Len(reconcileReport.Items, 1)
	require.Equal(ActionDeletedVerified, reconcileReport.Items[0].Action)
	require.Len(store.deleted, 1)
	require.Equal(RetirementStateDeletedVerified, repo.manifests[row.BlockMetadataID].State)
	require.Equal(2, repo.manifests[row.BlockMetadataID].AttemptCount)
}

func TestPlannerReconcile_ReturnsLiveCSCBWriteOncePolicyFailure(t *testing.T) {
	require := require.New(t)
	row, repo, store, planner, _, req, report := newExecutableTestFixture(t)
	manifest := manifestFromCandidate(report.Items[0], req.Now)
	require.NoError(repo.PrepareRetirement(context.Background(), manifest, req.StorageGeneration))
	store.policyErrors[manifest.ConsolidatedObjectKey] = errors.New("GetBucketPolicy temporarily unavailable")

	reconcileReport, err := planner.Reconcile(context.Background(), req)
	require.Error(err)
	require.Len(reconcileReport.Items, 1)
	require.Equal(SkipCSCBWriteOncePolicyUnverified, reconcileReport.Items[0].SkipReason)
	require.Empty(store.deleted)
	require.Equal(RetirementStateEligible, repo.manifests[row.BlockMetadataID].State)
	require.Empty(repo.manifests[row.BlockMetadataID].Outcome)
}

func TestPlannerPlan_RejectsInconsistentFinalizedRetirement(t *testing.T) {
	require := require.New(t)
	_, repo, _, planner, _, req, report := newExecutableTestFixture(t)
	require.NoError(planner.Apply(context.Background(), req, report))

	finalReport, err := planner.Plan(context.Background(), req)
	require.NoError(err)
	require.Equal(ActionAlreadyDeleted, finalReport.Items[0].Action)

	repo.rows[0].PrimaryObjectKey = "single-block/regressed.gzip"
	inconsistentReport, err := planner.Plan(context.Background(), req)
	require.NoError(err)
	require.Equal(ActionSkip, inconsistentReport.Items[0].Action)
	require.Equal(SkipMetadataChanged, inconsistentReport.Items[0].SkipReason)
}

func TestPlannerPlan_UsesCSCBUncompressedLengthForPlacementBounds(t *testing.T) {
	require := require.New(t)
	now := time.Date(2026, 6, 21, 12, 0, 0, 0, time.UTC)
	validatedAt := now.Add(-8 * 24 * time.Hour)
	row := testRow(100, "hash-100", "single-block/100.zstd", "consolidated/canary.cscb.zstd", validatedAt)
	row.PrimaryByteOffset = 900
	row.PrimaryByteLength = 100
	row.Shadow.ByteOffset = 900
	row.Shadow.ByteLength = 100
	store := newFakeStore()
	store.topologies[row.SingleBlockObjectKey] = safeTopology("single-block-v1", "single-block-etag", 42)
	store.heads[row.Shadow.ConsolidatedObjectKey] = cscbObjectHead(100, 1000)

	report, err := testPlanner(&fakeRepo{rows: []MetadataRow{row}}, store).Plan(context.Background(), testRequest(now, false))
	require.NoError(err)
	require.Equal(ActionReportOnly, report.Items[0].Action)
	require.Empty(report.Items[0].SkipReason)
}

func TestPlannerPlan_InvalidCSCBUncompressedLengthFailsClosed(t *testing.T) {
	now := time.Date(2026, 6, 21, 12, 0, 0, 0, time.UTC)
	validatedAt := now.Add(-8 * 24 * time.Hour)

	for _, test := range []struct {
		name            string
		compressedBytes uint64
		metadata        map[string]string
	}{
		{name: "missing", compressedBytes: 1024, metadata: nil},
		{name: "zero compressed bytes", compressedBytes: 0, metadata: cscbObjectMetadata("100", cscbFormatMetadataValue, cscbCompressionScopeMetadataValue)},
		{name: "wrong format", compressedBytes: 1024, metadata: cscbObjectMetadata("100", "not-cscb", cscbCompressionScopeMetadataValue)},
		{name: "wrong compression scope", compressedBytes: 1024, metadata: cscbObjectMetadata("100", cscbFormatMetadataValue, "whole-object")},
		{name: "malformed length", compressedBytes: 1024, metadata: cscbObjectMetadata("invalid", cscbFormatMetadataValue, cscbCompressionScopeMetadataValue)},
		{name: "zero length", compressedBytes: 1024, metadata: cscbObjectMetadata("0", cscbFormatMetadataValue, cscbCompressionScopeMetadataValue)},
	} {
		t.Run(test.name, func(t *testing.T) {
			require := require.New(t)
			row := testRow(100, "hash-100", "single-block/100.zstd", "consolidated/canary.cscb.zstd", validatedAt)
			store := newFakeStore()
			store.topologies[row.SingleBlockObjectKey] = safeTopology("single-block-v1", "single-block-etag", 42)
			store.heads[row.Shadow.ConsolidatedObjectKey] = ObjectHead{
				Exists:    true,
				Bytes:     test.compressedBytes,
				VersionID: "cscb-v1",
				ETag:      "cscb-etag",
				Metadata:  test.metadata,
			}

			report, err := testPlanner(&fakeRepo{rows: []MetadataRow{row}}, store).Plan(context.Background(), testRequest(now, false))
			require.NoError(err)
			require.Equal(ActionSkip, report.Items[0].Action)
			require.Equal(SkipInvalidMetadataReference, report.Items[0].SkipReason)
		})
	}
}

func TestPlannerPlan_CSCBLifecycleExpirationFailsClosed(t *testing.T) {
	require := require.New(t)
	now := time.Date(2026, 6, 21, 12, 0, 0, 0, time.UTC)
	row := testRow(428058000, "hash-428058000", "single-block/428058000.zstd", "consolidated/canary.cscb.zstd", now.Add(-8*24*time.Hour))
	store := newFakeStore()
	store.topologies[row.SingleBlockObjectKey] = safeTopology("single-block-v1", "single-block-etag", 42)
	head := cscbObjectHead(1024, 1024)
	head.Expiration = `expiry-date="Mon, 20 Jul 2026 00:00:00 GMT", rule-id="expire-current"`
	store.heads[row.PrimaryObjectKey] = head

	report, err := testPlanner(&fakeRepo{rows: []MetadataRow{row}}, store).Plan(context.Background(), testRequest(now, false))
	require.NoError(err)
	require.Equal(ActionSkip, report.Items[0].Action)
	require.Equal(SkipCSCBLifecycleExpirationActive, report.Items[0].SkipReason)
	require.Zero(store.policyCalls[row.PrimaryObjectKey])
}

func TestPlannerApply_DoesNotDeleteWhenCSCBLifecycleExpirationAppears(t *testing.T) {
	require := require.New(t)
	row, repo, store, planner, _, req, report := newExecutableTestFixture(t)
	store.headHook = func(key string, call int) ObjectHead {
		head := store.heads[key]
		// Call 1 is Plan, call 2 the pre-fence revalidation; call 3 is the
		// post-claim revalidation that must observe the expiration before any
		// version is deleted.
		if key == row.PrimaryObjectKey && call >= 3 {
			head.Expiration = `expiry-date="Mon, 20 Jul 2026 00:00:00 GMT", rule-id="expire-current"`
		}
		return head
	}

	err := planner.Apply(context.Background(), req, report)
	require.Error(err)
	require.Empty(store.deleted)
	require.Equal(SkipCSCBLifecycleExpirationActive, report.Items[0].SkipReason)
	require.Equal(RetirementStateDeleting, repo.manifests[row.BlockMetadataID].State)
}

func newExecutableTestFixture(t *testing.T) (
	MetadataRow,
	*fakeRepo,
	*fakeStore,
	*Planner,
	*fakePayloadVerifier,
	PlanRequest,
	*Report,
) {
	t.Helper()
	require := require.New(t)
	now := time.Date(2026, 6, 21, 12, 0, 0, 0, time.UTC)
	row := testRow(
		428058000,
		"hash-428058000",
		"single-block/428058000.zstd",
		"consolidated/canary.cscb.zstd",
		now.Add(-8*24*time.Hour),
	)
	repo := &fakeRepo{rows: []MetadataRow{row}}
	store := newFakeStore()
	store.deleteMutates = true
	store.topologies[row.SingleBlockObjectKey] = safeTopology("single-block-v1", "single-block-etag", 42)
	cscbHead := cscbObjectHead(1024, 1024)
	store.heads[row.PrimaryObjectKey] = cscbHead
	store.versionHeads[versionObjectKey(row.PrimaryObjectKey, cscbHead.VersionID)] = cscbHead
	planner, verifier := newTestPlanner(repo, store)
	req := testRequest(now, true)
	req.ProductionDeleteEnabled = true
	report, err := planner.Plan(context.Background(), req)
	require.NoError(err)
	require.Len(report.Items, 1)
	require.Equal(ActionDeleteObjectVersion, report.Items[0].Action)
	return row, repo, store, planner, verifier, req, report
}

func newMultiVersionExecutableTestFixture(t *testing.T) (
	MetadataRow,
	*fakeRepo,
	*fakeStore,
	*Planner,
	*fakePayloadVerifier,
	PlanRequest,
	*Report,
) {
	t.Helper()
	require := require.New(t)
	now := time.Date(2026, 6, 21, 12, 0, 0, 0, time.UTC)
	row := testRow(
		428058000,
		"hash-428058000",
		"single-block/428058000.zstd",
		"consolidated/canary.cscb.zstd",
		now.Add(-8*24*time.Hour),
	)
	repo := &fakeRepo{rows: []MetadataRow{row}}
	store := newFakeStore()
	store.deleteMutates = true
	store.topologies[row.SingleBlockObjectKey] = ObjectVersionTopology{
		Versions: []ObjectVersion{
			{VersionID: "single-block-v2", ETag: "etag-v2", Bytes: 42, IsLatest: true},
			{VersionID: "single-block-v1", ETag: "etag-v1", Bytes: 41},
		},
		DeleteMarkers: []ObjectDeleteMarker{{VersionID: "delete-marker-v1"}},
	}
	cscbHead := cscbObjectHead(1024, 1024)
	store.heads[row.PrimaryObjectKey] = cscbHead
	store.versionHeads[versionObjectKey(row.PrimaryObjectKey, cscbHead.VersionID)] = cscbHead
	planner, verifier := newTestPlanner(repo, store)
	req := testRequest(now, true)
	req.ProductionDeleteEnabled = true
	report, err := planner.Plan(context.Background(), req)
	require.NoError(err)
	require.Len(report.Items, 1)
	require.Equal(ActionDeleteObjectVersion, report.Items[0].Action)
	return row, repo, store, planner, verifier, req, report
}

func testRequest(now time.Time, execute bool) PlanRequest {
	return PlanRequest{
		Environment:                 "production",
		Blockchain:                  "solana",
		Network:                     "mainnet",
		Bucket:                      "co-chainstorage-solana-mainnet-prod",
		Tag:                         0,
		StartHeight:                 428058000,
		EndHeight:                   428059000,
		Now:                         now,
		Execute:                     execute,
		DirectStorageClientsGuarded: true,
		SingleBlockWritersGuarded:   true,
		Approval: Approval{
			Chain:       "solana-mainnet",
			StartHeight: 428058000,
			EndHeight:   428059000,
		},
	}
}

func testRow(height uint64, hash string, singleBlockKey string, cscbKey string, validatedAt time.Time) MetadataRow {
	retiredAt := validatedAt
	retireAfter := validatedAt.Add(72 * time.Hour)
	return MetadataRow{
		BlockMetadataID:           int64(height),
		Canonical:                 true,
		Tag:                       0,
		Height:                    height,
		Hash:                      hash,
		PrimaryObjectKey:          cscbKey,
		SingleBlockObjectKey:      singleBlockKey,
		PrimaryObjectFormat:       api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_CSCB_BATCH,
		PrimaryByteOffset:         100,
		PrimaryByteLength:         200,
		PrimaryUncompressedLength: 300,
		Shadow: &ConsolidationShadow{
			Tag:                           0,
			Height:                        height,
			Hash:                          hash,
			SingleBlockObjectKey:          singleBlockKey,
			ConsolidatedObjectKey:         cscbKey,
			ObjectFormat:                  api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_CSCB_BATCH,
			ByteOffset:                    100,
			ByteLength:                    200,
			UncompressedLength:            300,
			ValidatedAt:                   &validatedAt,
			SingleBlockRetentionStartedAt: &retiredAt,
			SingleBlockDeleteAfter:        &retireAfter,
			FormatVersion:                 1,
		},
	}
}

func cscbObjectHead(compressedBytes uint64, uncompressedBytes uint64) ObjectHead {
	return ObjectHead{
		Exists:    true,
		Bytes:     compressedBytes,
		VersionID: "cscb-v1",
		ETag:      "cscb-etag",
		Metadata: cscbObjectMetadata(
			fmt.Sprintf("%d", uncompressedBytes),
			cscbFormatMetadataValue,
			cscbCompressionScopeMetadataValue,
		),
	}
}

func cscbObjectMetadata(uncompressedLength string, format string, compressionScope string) map[string]string {
	return map[string]string{
		cscbFormatMetadataKey:             format,
		cscbCompressionScopeMetadataKey:   compressionScope,
		cscbUncompressedLengthMetadataKey: uncompressedLength,
	}
}

// lockedRepository and lockedObjectStore make the single-threaded fakes safe
// for row-parallel planner tests without touching the serial fixtures.
type lockedRepository struct {
	mu    sync.Mutex
	inner Repository
}

func (l *lockedRepository) ListMetadataRows(ctx context.Context, tag uint32, startHeight uint64, endHeight uint64, limit uint64) ([]MetadataRow, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.inner.ListMetadataRows(ctx, tag, startHeight, endHeight, limit)
}

func (l *lockedRepository) GetMetadataRow(ctx context.Context, blockMetadataID int64) (MetadataRow, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.inner.GetMetadataRow(ctx, blockMetadataID)
}

func (l *lockedRepository) PrepareRetirement(ctx context.Context, manifest RetirementManifest, expectedStorageGeneration string) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.inner.PrepareRetirement(ctx, manifest, expectedStorageGeneration)
}

func (l *lockedRepository) ObserveRetentionSafety(ctx context.Context, bucket string, consolidatedObjectKey string, configurationSHA256 string) (time.Time, time.Time, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.inner.ObserveRetentionSafety(ctx, bucket, consolidatedObjectKey, configurationSHA256)
}

func (l *lockedRepository) ClaimRetirement(ctx context.Context, blockMetadataID int64, claimToken string, claimedAt time.Time, claimExpiresAt time.Time) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.inner.ClaimRetirement(ctx, blockMetadataID, claimToken, claimedAt, claimExpiresAt)
}

func (l *lockedRepository) RenewRetirementClaim(ctx context.Context, blockMetadataID int64, claimToken string, renewedAt time.Time, claimExpiresAt time.Time) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.inner.RenewRetirementClaim(ctx, blockMetadataID, claimToken, renewedAt, claimExpiresAt)
}

func (l *lockedRepository) ReleaseRetirementClaim(ctx context.Context, blockMetadataID int64, claimToken string) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.inner.ReleaseRetirementClaim(ctx, blockMetadataID, claimToken)
}

func (l *lockedRepository) RecordRetirementOutcome(ctx context.Context, blockMetadataID int64, claimToken string, outcome string, attemptedAt time.Time) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.inner.RecordRetirementOutcome(ctx, blockMetadataID, claimToken, outcome, attemptedAt)
}

func (l *lockedRepository) RecordRetirementObjectDeleted(ctx context.Context, blockMetadataID int64, claimToken string, outcome string) (time.Time, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.inner.RecordRetirementObjectDeleted(ctx, blockMetadataID, claimToken, outcome)
}

func (l *lockedRepository) FinalizeRetirement(ctx context.Context, blockMetadataID int64, claimToken string, outcome string) (time.Time, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.inner.FinalizeRetirement(ctx, blockMetadataID, claimToken, outcome)
}

func (l *lockedRepository) ListPendingRetirements(ctx context.Context, tag uint32, startHeight uint64, endHeight uint64, eligibilityCutoff time.Time, limit uint64) ([]RetirementManifest, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.inner.ListPendingRetirements(ctx, tag, startHeight, endHeight, eligibilityCutoff, limit)
}

type lockedObjectStore struct {
	mu    sync.Mutex
	inner ObjectStore
}

func (l *lockedObjectStore) InspectObjectRetentionSafety(ctx context.Context, bucket string, key string) (RetentionSafetySnapshot, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.inner.InspectObjectRetentionSafety(ctx, bucket, key)
}

func (l *lockedObjectStore) HeadObject(ctx context.Context, bucket string, key string) (ObjectHead, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.inner.HeadObject(ctx, bucket, key)
}

func (l *lockedObjectStore) HeadObjectVersion(ctx context.Context, bucket string, key string, versionID string) (ObjectHead, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.inner.HeadObjectVersion(ctx, bucket, key, versionID)
}

func (l *lockedObjectStore) ListObjectVersions(ctx context.Context, bucket string, key string) (ObjectVersionTopology, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.inner.ListObjectVersions(ctx, bucket, key)
}

func (l *lockedObjectStore) ReadObjectVersion(ctx context.Context, bucket string, key string, versionID string) ([]byte, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.inner.ReadObjectVersion(ctx, bucket, key, versionID)
}

func (l *lockedObjectStore) ReadObjectVersionRange(ctx context.Context, bucket string, key string, versionID string, offset uint64, length uint64) ([]byte, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.inner.ReadObjectVersionRange(ctx, bucket, key, versionID, offset, length)
}

func (l *lockedObjectStore) DeleteObjectVersion(ctx context.Context, bucket string, key string, versionID string) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.inner.DeleteObjectVersion(ctx, bucket, key, versionID)
}

type lockedPayloadVerifier struct {
	mu    sync.Mutex
	inner blockPayloadVerifier
}

func (l *lockedPayloadVerifier) Verify(ctx context.Context, candidate Candidate) (string, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.inner.Verify(ctx, candidate)
}

func (l *lockedPayloadVerifier) VerifyConsolidated(ctx context.Context, candidate Candidate) (string, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.inner.VerifyConsolidated(ctx, candidate)
}

func newRowParallelismFixture(t *testing.T, parallelism int) (*fakeRepo, *fakeStore, *Planner, PlanRequest) {
	t.Helper()
	now := time.Date(2026, 6, 21, 12, 0, 0, 0, time.UTC)
	rows := make([]MetadataRow, 0, 6)
	repo := &fakeRepo{}
	store := newFakeStore()
	store.deleteMutates = true
	for i := range 6 {
		height := uint64(428058000 + i)
		cscbKey := "consolidated/first.cscb.zstd"
		if i >= 3 {
			cscbKey = "consolidated/second.cscb.zstd"
		}
		row := testRow(height, fmt.Sprintf("hash-%d", height), fmt.Sprintf("single-block/%d.zstd", height), cscbKey, now.Add(-8*24*time.Hour))
		rows = append(rows, row)
		store.topologies[row.SingleBlockObjectKey] = safeTopology(fmt.Sprintf("v-%d", height), "single-block-etag", 42)
	}
	repo.rows = rows
	cscbHead := cscbObjectHead(1024, 1024)
	for _, cscbKey := range []string{"consolidated/first.cscb.zstd", "consolidated/second.cscb.zstd"} {
		store.heads[cscbKey] = cscbHead
		store.versionHeads[versionObjectKey(cscbKey, cscbHead.VersionID)] = cscbHead
	}
	digest := strings.Repeat("a", sha256HexLength)
	planner := NewPlanner(
		&lockedRepository{inner: repo},
		&lockedObjectStore{inner: store},
		WithRowParallelism(parallelism),
	)
	sharedVerifier := &lockedPayloadVerifier{inner: &fakePayloadVerifier{digest: digest, consolidatedDigest: digest}}
	planner.verifierFactory = func() blockPayloadVerifier { return sharedVerifier }
	req := testRequest(now, true)
	req.EndHeight = 428059000
	req.ProductionDeleteEnabled = true
	return repo, store, planner, req
}

func TestPlannerPlanAndApply_RowParallelismMatchesSerial(t *testing.T) {
	require := require.New(t)
	serialRepo, serialStore, serialPlanner, serialReq := newRowParallelismFixture(t, 1)
	parallelRepo, parallelStore, parallelPlanner, parallelReq := newRowParallelismFixture(t, 4)

	serialReport, err := serialPlanner.Plan(context.Background(), serialReq)
	require.NoError(err)
	parallelReport, err := parallelPlanner.Plan(context.Background(), parallelReq)
	require.NoError(err)
	require.Len(parallelReport.Items, len(serialReport.Items))
	for i := range serialReport.Items {
		require.Equal(serialReport.Items[i].Height, parallelReport.Items[i].Height)
		require.Equal(serialReport.Items[i].Action, parallelReport.Items[i].Action)
		require.Equal(serialReport.Items[i].SkipReason, parallelReport.Items[i].SkipReason)
	}
	require.Equal(serialReport.Summary, parallelReport.Summary)

	require.NoError(serialPlanner.Apply(context.Background(), serialReq, serialReport))
	require.NoError(parallelPlanner.Apply(context.Background(), parallelReq, parallelReport))
	require.Len(parallelStore.deleted, len(serialStore.deleted))
	require.Equal(serialReport.Summary, parallelReport.Summary)
	require.Len(parallelRepo.manifests, len(serialRepo.manifests))
	for blockMetadataID, serialManifest := range serialRepo.manifests {
		parallelManifest, ok := parallelRepo.manifests[blockMetadataID]
		require.True(ok)
		require.Equal(serialManifest.State, parallelManifest.State)
		require.Equal(RetirementStateDeletedVerified, parallelManifest.State)
	}
	for i := range serialReport.Items {
		require.Equal(serialReport.Items[i].Action, parallelReport.Items[i].Action)
		require.Equal(ActionDeletedVerified, parallelReport.Items[i].Action)
	}
}

// TestBudgetedRowParallelismUsesRealSolanaChunkSize pins the sizing against
// the production numbers behind INF-1330: a Solana 1000-block CSCB is ~11.1 GB
// uncompressed over 100 chunks, so the largest chunk is ~111 MB and a
// configured parallelism of 8 pinned ~1.8 GB in Apply, OOM-killing the worker.
func TestBudgetedRowParallelismUsesRealSolanaChunkSize(t *testing.T) {
	require := require.New(t)
	const solanaChunkBytes = uint64(111_077_517)
	req := PlanRequest{RowMemoryBudgetBytes: uint64(1) << 30}
	planner := &Planner{rowParallelism: 8}

	// Apply pins two chunks per worker, so a 1 GiB budget affords 4 workers.
	require.Equal(4, planner.budgetedRowParallelism(req, solanaChunkBytes, 2))
	// Plan pins one chunk per worker, so the same budget affords more, still
	// clamped by the configured value.
	require.Equal(8, planner.budgetedRowParallelism(req, solanaChunkBytes, 1))
	// A budget smaller than a single worker still yields a usable worker.
	tiny := req
	tiny.RowMemoryBudgetBytes = 1024
	require.Equal(1, planner.budgetedRowParallelism(tiny, solanaChunkBytes, 2))
	// An unset budget must not disable the guard.
	unset := req
	unset.RowMemoryBudgetBytes = 0
	require.Equal(4, planner.budgetedRowParallelism(unset, solanaChunkBytes, 2))
	// A serial planner is never widened by sizing.
	require.Equal(1, (&Planner{rowParallelism: 1}).budgetedRowParallelism(req, solanaChunkBytes, 2))
}

// TestBudgetedRowParallelismFailsClosed covers the inputs that must never
// widen a pass: unmeasurable chunks, and a declared length large enough to
// overflow the per-worker product (which at the 2^63 boundary with two
// verifiers would otherwise divide by zero).
func TestBudgetedRowParallelismFailsClosed(t *testing.T) {
	require := require.New(t)
	req := PlanRequest{RowMemoryBudgetBytes: uint64(1) << 30}
	planner := &Planner{rowParallelism: 8}

	require.Equal(1, planner.budgetedRowParallelism(req, 0, 2), "unmeasurable chunk must reduce to serial")
	require.Equal(1, planner.budgetedRowParallelism(req, 1<<20, 0), "invalid verifier count must reduce to serial")
	require.Equal(1, planner.budgetedRowParallelism(req, uint64(1)<<63, 2), "overflow must reduce to serial")
	require.Equal(1, planner.budgetedRowParallelism(req, ^uint64(0), 2), "overflow must reduce to serial")
}

// TestMaxChunkUncompressedBytesUsesLargestChunk proves sizing bounds the worst
// chunk rather than an average: chunk payloads are uneven, and an immutable
// object may have been encoded under different chunk settings than the current
// configuration.
func TestMaxChunkUncompressedBytesUsesLargestChunk(t *testing.T) {
	require := require.New(t)
	require.Zero(maxChunkUncompressedBytes(nil))
	require.Zero(maxChunkUncompressedBytes(&cscb.Index{}))
	require.Equal(uint64(900), maxChunkUncompressedBytes(&cscb.Index{
		Chunks: []cscb.ChunkDescriptor{
			{UncompressedLength: 100},
			{UncompressedLength: 900},
			{UncompressedLength: 300},
		},
	}), "an average would report 433 and under-bound the peak")
}

func TestPlannerApply_DoesNotFenceWhenCSCBVanishesAfterPlan(t *testing.T) {
	require := require.New(t)
	row, repo, store, planner, _, req, report := newExecutableTestFixture(t)
	// The pinned CSCB disappears between Plan and Apply. The pre-fence
	// revalidation must fail the row before PrepareRetirement stamps the
	// irreversible single-block upload fence.
	delete(store.heads, row.PrimaryObjectKey)

	err := planner.Apply(context.Background(), req, report)
	require.Error(err)
	require.Empty(store.deleted)
	require.Empty(repo.manifests)
	require.Equal(SkipCSCBObjectChanged, report.Items[0].SkipReason)
}

func TestPlannerApply_ThrottlesClaimRenewalsWithinLease(t *testing.T) {
	require := require.New(t)
	_, repo, store, planner, _, req, report := newExecutableTestFixture(t)

	require.NoError(planner.Apply(context.Background(), req, report))
	require.Len(store.deleted, 1)
	require.Equal(ActionDeletedVerified, report.Items[0].Action)
	// A freshly claimed row skips every half-life renewal but still pays
	// exactly one unconditional ownership fence before its destructive
	// sequence.
	require.Equal(1, repo.renewCalls)
}

func TestMaybeRenewRetirementClaimRenewsAfterHalfLife(t *testing.T) {
	require := require.New(t)
	now := time.Now().UTC()
	expiresAt := now.Add(RetirementClaimLease)
	repo := &fakeRepo{
		manifests: map[int64]RetirementManifest{
			428058000: {
				BlockMetadataID: 428058000,
				State:           RetirementStateDeleting,
				ClaimToken:      "claim-token",
				ClaimExpiresAt:  &expiresAt,
			},
		},
	}
	planner := testPlanner(repo, newFakeStore())

	fresh := &retirementClaim{token: "claim-token", lastRenewedAt: now}
	require.NoError(planner.maybeRenewRetirementClaim(context.Background(), 428058000, fresh))
	require.Zero(repo.renewCalls)
	require.Equal(now, fresh.lastRenewedAt)

	aged := &retirementClaim{token: "claim-token", lastRenewedAt: now.Add(-RetirementClaimLease/retirementClaimRenewalFraction - time.Second)}
	require.NoError(planner.maybeRenewRetirementClaim(context.Background(), 428058000, aged))
	require.Equal(1, repo.renewCalls)
	require.True(aged.lastRenewedAt.After(now.Add(-time.Minute)))
}

func TestPlannerPrimeRetentionSafetyStartsQuiescenceClocks(t *testing.T) {
	require := require.New(t)
	repo := &fakeRepo{}
	store := newFakeStore()
	planner := testPlanner(repo, store)
	keys := []string{
		"BLOCKCHAIN_SOLANA/consolidated/a.cscb.zstd",
		"BLOCKCHAIN_SOLANA/consolidated/b.cscb.zstd",
		"BLOCKCHAIN_SOLANA/consolidated/a.cscb.zstd",
	}

	primed, err := planner.PrimeRetentionSafety(context.Background(), "priming-bucket", keys)
	require.NoError(err)
	require.Equal(2, primed)
	// One bucket-level inspection covers every primed key.
	require.Equal(1, store.policyCalls[keys[0]])
	require.Zero(store.policyCalls[keys[1]])
	expectedSHA256 := keySHA256("safe:" + keys[0])
	for _, key := range keys[:2] {
		observation, ok := repo.safetyObservations["priming-bucket\x00"+key]
		require.True(ok)
		require.Equal(expectedSHA256, observation.configurationSHA256)
	}

	store.policyErrors[keys[0]] = errors.New("bucket policy unreadable")
	primed, err = planner.PrimeRetentionSafety(context.Background(), "priming-bucket", keys[:1])
	require.Error(err)
	require.Zero(primed)
}

func TestPlannerApply_SurfacesRemainingQuiescenceRetryAfter(t *testing.T) {
	require := require.New(t)
	_, repo, store, planner, _, req, report := newExecutableTestFixture(t)
	key := report.Items[0].ConsolidatedKey
	elapsed := 5 * time.Minute
	repo.safetyNow = req.Now
	repo.safetyObservations = map[string]fakeSafetyObservation{
		req.Bucket + "\x00" + key: {
			configurationSHA256: keySHA256("safe:" + key),
			firstObservedAt:     req.Now.Add(-elapsed),
		},
	}

	err := planner.Apply(context.Background(), req, report)
	require.Error(err)
	require.Empty(store.deleted)
	require.Equal(SkipCSCBSafetyQuiescenceActive, report.Items[0].SkipReason)
	require.Equal(RetentionSafetyQuiescencePeriod-elapsed, report.QuiescenceRetryAfter)
}

func activeSingleBlockTestRow(height uint64, hash string, singleBlockKey string, cscbKey string, validatedAt time.Time) MetadataRow {
	row := testRow(height, hash, singleBlockKey, cscbKey, validatedAt)
	row.PrimaryObjectKey = singleBlockKey
	row.PrimaryObjectFormat = api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_SINGLE_BLOCK
	row.PrimaryByteOffset = 0
	row.PrimaryByteLength = 0
	row.PrimaryUncompressedLength = 0
	return row
}
