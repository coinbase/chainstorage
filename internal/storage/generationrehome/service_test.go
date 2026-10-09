package generationrehome

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

type fakeRepository struct {
	inspection    Inspection
	inspectErr    error
	rehomeErr     error
	alreadyTarget bool
	inspectCalls  int
	rehomeCalls   []RehomeRequest
}

func (r *fakeRepository) Inspect(ctx context.Context, tag uint32, objectKey string, targetGeneration string, evidenceSHA256 string) (Inspection, error) {
	r.inspectCalls++
	return r.inspection, r.inspectErr
}

func (r *fakeRepository) Rehome(ctx context.Context, req RehomeRequest) (bool, error) {
	r.rehomeCalls = append(r.rehomeCalls, req)
	return r.alreadyTarget, r.rehomeErr
}

type fakeObjectStore struct {
	heads              map[string]ObjectHead
	retentionSafetyErr error
}

func (s *fakeObjectStore) InspectObjectRetentionSafety(ctx context.Context, bucket string, key string) error {
	return s.retentionSafetyErr
}

func (s *fakeObjectStore) HeadObject(ctx context.Context, bucket string, key string) (ObjectHead, error) {
	return s.heads[fmt.Sprintf("current/%s/%s", bucket, key)], nil
}

func (s *fakeObjectStore) HeadObjectVersion(ctx context.Context, bucket string, key string, versionID string) (ObjectHead, error) {
	return s.heads[fmt.Sprintf("version/%s/%s/%s", bucket, key, versionID)], nil
}

func TestServiceRunDryRunAndExecute(t *testing.T) {
	object := testObject()
	for _, execute := range []bool{false, true} {
		t.Run(fmt.Sprintf("execute=%t", execute), func(t *testing.T) {
			repository := &fakeRepository{inspection: legacyInspection(object)}
			service := NewService(repository, testObjectStore(object))
			req := testRequest(object, execute)

			report, err := service.Run(context.Background(), req, []Object{object})
			require.NoError(t, err)
			require.Len(t, report.Items, 1)
			if execute {
				require.Equal(t, ActionRehomed, report.Items[0].Action)
				require.Equal(t, uint64(1), report.Summary.CompletedObjects)
				require.Len(t, repository.rehomeCalls, 1)
			} else {
				require.Equal(t, ActionReady, report.Items[0].Action)
				require.Equal(t, uint64(1), report.Summary.ReadyObjects)
				require.Empty(t, repository.rehomeCalls)
			}
		})
	}
}

func TestServiceRunRejectsApprovalMismatchBeforeExternalInspection(t *testing.T) {
	object := testObject()
	repository := &fakeRepository{inspection: legacyInspection(object)}
	service := NewService(repository, testObjectStore(object))
	req := testRequest(object, true)
	req.Approval.Rows++

	_, err := service.Run(context.Background(), req, []Object{object})
	require.ErrorContains(t, err, "approval row count mismatch")
	require.Empty(t, repository.rehomeCalls)
}

func TestServiceRunRejectsUnsafeDestinationBeforeMetadataInspection(t *testing.T) {
	object := testObject()
	repository := &fakeRepository{inspection: legacyInspection(object)}
	store := testObjectStore(object)
	store.retentionSafetyErr = errors.New("destination bucket is mutable")
	service := NewService(repository, store)

	report, err := service.Run(context.Background(), testRequest(object, true), []Object{object})
	require.ErrorContains(t, err, "destination retention safety verification failed")
	require.ErrorContains(t, err, "destination bucket is mutable")
	require.Zero(t, repository.inspectCalls)
	require.Empty(t, repository.rehomeCalls)
	require.Equal(t, ActionFailed, report.Items[0].Action)
}

func TestServiceRunRejectsMixedGenerationCohort(t *testing.T) {
	object := testObject()
	inspection := legacyInspection(object)
	inspection.PrimaryLegacyRows--
	inspection.PrimaryTargetRows++
	repository := &fakeRepository{inspection: inspection}
	service := NewService(repository, testObjectStore(object))

	report, err := service.Run(context.Background(), testRequest(object, false), []Object{object})
	require.ErrorContains(t, err, "storage generation cohort is mixed")
	require.Equal(t, ActionFailed, report.Items[0].Action)
}

func TestServiceRunRecognizesAlreadyTarget(t *testing.T) {
	object := testObject()
	inspection := legacyInspection(object)
	inspection.PrimaryLegacyRows = 0
	inspection.ConsolidatedLegacyRows = 0
	inspection.PrimaryTargetRows = object.ExpectedRows
	inspection.ConsolidatedTargetRows = object.ExpectedRows
	inspection.CompletedAudit = true
	repository := &fakeRepository{inspection: inspection}
	service := NewService(repository, testObjectStore(object))

	report, err := service.Run(context.Background(), testRequest(object, true), []Object{object})
	require.NoError(t, err)
	require.Equal(t, ActionAlreadyTarget, report.Items[0].Action)
	require.Empty(t, repository.rehomeCalls)
}

func testObject() Object {
	object := Object{
		ObjectKey:         "consolidated/object.cscb.zstd",
		StartHeight:       100,
		EndHeight:         110,
		ExpectedRows:      8,
		ExpectedCanonical: 7,
		ExpectedFenced:    3,
		ExpectedDeleted:   3,
		Source: ObjectVersion{
			VersionID: "source-version",
			ETag:      "source-etag",
			Bytes:     100,
		},
		Destination: ObjectVersion{
			VersionID: "destination-version",
			ETag:      "destination-etag",
			Bytes:     100,
		},
	}
	object.EvidenceSHA256 = evidenceSHA256(object)
	return object
}

func legacyInspection(object Object) Inspection {
	return Inspection{
		TotalRows:              object.ExpectedRows,
		CanonicalRows:          object.ExpectedCanonical,
		StartHeight:            object.StartHeight,
		EndHeight:              object.EndHeight,
		FencedRows:             object.ExpectedFenced,
		DeletedVerifiedRows:    object.ExpectedDeleted,
		PrimaryLegacyRows:      object.ExpectedRows,
		ConsolidatedLegacyRows: object.ExpectedRows,
	}
}

func testRequest(object Object, execute bool) Request {
	return Request{
		Environment:       "production",
		Chain:             "solana-mainnet",
		Tag:               2,
		SourceBucket:      "legacy",
		DestinationBucket: "v2",
		TargetGeneration:  "v2",
		Execute:           execute,
		Approval: Approval{
			Chain:      "solana-mainnet",
			Objects:    1,
			Rows:       object.ExpectedRows,
			Transition: TransitionLegacyNullToV2,
		},
	}
}

func testObjectStore(object Object) *fakeObjectStore {
	store := &fakeObjectStore{heads: make(map[string]ObjectHead)}
	for _, entry := range []struct {
		bucket  string
		version ObjectVersion
	}{
		{bucket: "legacy", version: object.Source},
		{bucket: "v2", version: object.Destination},
	} {
		head := ObjectHead{
			Exists:    true,
			VersionID: entry.version.VersionID,
			ETag:      entry.version.ETag,
			Bytes:     entry.version.Bytes,
		}
		store.heads[fmt.Sprintf("current/%s/%s", entry.bucket, object.ObjectKey)] = head
		store.heads[fmt.Sprintf("version/%s/%s/%s", entry.bucket, object.ObjectKey, entry.version.VersionID)] = head
	}
	return store
}
