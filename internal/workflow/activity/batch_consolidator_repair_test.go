package activity

import (
	"context"
	"testing"
	"time"

	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/proto"

	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/storage/cscbrepair"
	storageutils "github.com/coinbase/chainstorage/internal/storage/utils"
	"github.com/coinbase/chainstorage/internal/utils/testutil"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

func (s *BatchConsolidatorTestSuite) TestRepairExistingCSCBRebuildsPinnedPayloadsAndPromotesAtomically() {
	require := testutil.Require(s.T())
	records, blocks := makeConsolidatorFixture(1, 1000)
	request := &BatchConsolidatorRequest{
		Mode:               config.ConsolidationModeRepairExistingCSCB,
		Tag:                2,
		StartHeight:        900,
		EndHeight:          1200,
		MaxBlocks:          100,
		RepairExecutionKey: testRepairExecutionKey,
	}
	oldKey := "consolidated/dirty.cscb.zstd"
	newKey := "consolidated/clean.cscb.zstd"
	prepared := repairActivityManifest(cscbrepair.StatePrepared, oldKey, "")
	completed := repairActivityManifest(cscbrepair.StateCompleted, oldKey, newKey)
	fake := &repairActivityFake{
		prepared:  prepared,
		verified:  completed,
		completed: completed,
		pinnedPayloads: []cscbrepair.PinnedPayload{{
			BlockMetadataID: records[0].ID,
			Metadata:        records[0].Metadata,
			RawBlockPayload: marshalRepairBlock(s.T(), blocks[0]),
		}},
	}
	s.batchConsolidator.repairer = fake
	s.batchConsolidator.config.AWS.Storage.Consolidation.LocalSpillDir = s.T().TempDir()

	s.blobStorage.EXPECT().
		UploadConsolidated(gomock.Any(), gomock.Any()).
		Return(newKey, makeConsolidatorPlacements(records), nil)

	response, err := s.batchConsolidator.Execute(s.env.BackgroundContext(), request)
	require.NoError(err)
	require.Equal(&BatchConsolidatorResponse{
		StartHeight:        1000,
		EndHeight:          1001,
		ScannedBlocks:      1,
		ConsolidatedBlocks: 1,
		PromotedBlocks:     1,
		ObjectKey:          newKey,
		OldObjectKey:       oldKey,
		RepairedObjects:    1,
	}, response)
	require.Equal([]string{"prepare", "visit_pinned", "verify_promote"}, fake.calls)
	require.Equal(testRepairExecutionKey, fake.executionKey)
}

func (s *BatchConsolidatorTestSuite) TestRepairExistingCSCBRejectsMissingPinnedPayloadBeforeUpload() {
	require := testutil.Require(s.T())
	oldKey := "consolidated/dirty.cscb.zstd"
	prepared := repairActivityManifest(cscbrepair.StatePrepared, oldKey, "")
	fake := &repairActivityFake{prepared: prepared}
	s.batchConsolidator.repairer = fake

	_, err := s.batchConsolidator.Execute(s.env.BackgroundContext(), &BatchConsolidatorRequest{
		Mode:               config.ConsolidationModeRepairExistingCSCB,
		Tag:                2,
		StartHeight:        900,
		EndHeight:          1200,
		MaxBlocks:          100,
		RepairExecutionKey: testRepairExecutionKey,
	})
	require.ErrorContains(err, "pinned payload count mismatch")
	require.Equal([]string{"prepare", "visit_pinned"}, fake.calls)
}

func (s *BatchConsolidatorTestSuite) TestRepairExistingCSCBResumesRestoredManifestWithPinnedPayload() {
	require := testutil.Require(s.T())
	records, blocks := makeConsolidatorFixture(1, 1000)
	oldKey := "consolidated/dirty.cscb.zstd"
	newKey := "consolidated/clean.cscb.zstd"
	restored := repairActivityManifest(cscbrepair.StateRestored, oldKey, "")
	completed := repairActivityManifest(cscbrepair.StateCompleted, oldKey, newKey)
	fake := &repairActivityFake{
		prepared:  restored,
		verified:  completed,
		completed: completed,
		pinnedPayloads: []cscbrepair.PinnedPayload{{
			BlockMetadataID: records[0].ID,
			Metadata:        records[0].Metadata,
			RawBlockPayload: marshalRepairBlock(s.T(), blocks[0]),
		}},
	}
	s.batchConsolidator.repairer = fake
	s.batchConsolidator.config.AWS.Storage.Consolidation.LocalSpillDir = s.T().TempDir()

	s.blobStorage.EXPECT().
		UploadConsolidated(gomock.Any(), gomock.Any()).
		Return(newKey, makeConsolidatorPlacements(records), nil)

	response, err := s.batchConsolidator.Execute(s.env.BackgroundContext(), &BatchConsolidatorRequest{
		Mode:               config.ConsolidationModeRepairExistingCSCB,
		Tag:                2,
		StartHeight:        900,
		EndHeight:          1200,
		MaxBlocks:          100,
		RepairExecutionKey: testRepairExecutionKey,
	})
	require.NoError(err)
	require.Equal(uint64(1), response.RepairedObjects)
	require.Equal([]string{"prepare", "visit_pinned", "verify_promote"}, fake.calls)
}

func (s *BatchConsolidatorTestSuite) TestRepairExistingCSCBCompletesAllNonCanonicalObjectWithoutRebuild() {
	require := testutil.Require(s.T())
	oldKey := "consolidated/dirty.cscb.zstd"
	prepared := repairActivityManifest(cscbrepair.StatePrepared, oldKey, "")
	prepared.CanonicalBlockCount = 0
	restored := repairActivityManifest(cscbrepair.StateRestored, oldKey, "")
	restored.CanonicalBlockCount = 0
	completed := repairActivityManifest(cscbrepair.StateCompleted, oldKey, "")
	completed.CanonicalBlockCount = 0
	fake := &repairActivityFake{prepared: prepared, restored: restored, completed: completed}
	s.batchConsolidator.repairer = fake

	response, err := s.batchConsolidator.Execute(s.env.BackgroundContext(), &BatchConsolidatorRequest{
		Mode:               config.ConsolidationModeRepairExistingCSCB,
		Tag:                2,
		StartHeight:        900,
		EndHeight:          1200,
		MaxBlocks:          100,
		RepairExecutionKey: testRepairExecutionKey,
	})
	require.NoError(err)
	require.Equal(uint64(1), response.RepairedObjects)
	require.Zero(response.ConsolidatedBlocks)
	require.Empty(response.ObjectKey)
	require.Equal([]string{"prepare", "restore", "complete"}, fake.calls)
	require.Equal(testRepairExecutionKey, fake.executionKey)
}

func (s *BatchConsolidatorTestSuite) TestRepairExistingCSCBRecordsAlreadyCleanObjectWithoutRebuild() {
	require := testutil.Require(s.T())
	oldKey := "consolidated/already-clean.cscb.zstd"
	completed := repairActivityManifest(cscbrepair.StateCompleted, oldKey, "")
	completed.Outcome = cscbrepair.OutcomeAlreadyCleanStorageNeutral
	fake := &repairActivityFake{prepared: completed}
	s.batchConsolidator.repairer = fake

	response, err := s.batchConsolidator.Execute(s.env.BackgroundContext(), &BatchConsolidatorRequest{
		Mode:               config.ConsolidationModeRepairExistingCSCB,
		Tag:                2,
		StartHeight:        900,
		EndHeight:          1200,
		MaxBlocks:          100,
		RepairExecutionKey: testRepairExecutionKey,
	})
	require.NoError(err)
	require.Equal(uint64(1), response.RepairedObjects)
	require.Equal(uint64(1), response.ScannedBlocks)
	require.Zero(response.ConsolidatedBlocks)
	require.Zero(response.PromotedBlocks)
	require.Equal(oldKey, response.ObjectKey)
	require.Equal([]string{"prepare"}, fake.calls)
}

func (s *BatchConsolidatorTestSuite) TestRepairExistingCSCBPreparesAssignedObject() {
	require := testutil.Require(s.T())
	oldKey := "consolidated/assigned-dirty.cscb.zstd"
	completed := repairActivityManifest(cscbrepair.StateCompleted, oldKey, "")
	completed.Outcome = cscbrepair.OutcomeAlreadyCleanStorageNeutral
	fake := &repairActivityFake{prepared: completed}
	s.batchConsolidator.repairer = fake

	response, err := s.batchConsolidator.Execute(s.env.BackgroundContext(), &BatchConsolidatorRequest{
		Mode:               config.ConsolidationModeRepairExistingCSCB,
		Tag:                2,
		StartHeight:        900,
		EndHeight:          1200,
		MaxBlocks:          100,
		RepairExecutionKey: testRepairExecutionKey,
		RepairObjectKey:    oldKey,
	})
	require.NoError(err)
	require.Equal(oldKey, response.OldObjectKey)
	require.Equal([]string{"prepare_object"}, fake.calls)
	require.Equal(oldKey, fake.objectKey)
}

func (s *BatchConsolidatorTestSuite) TestRepairExistingCSCBCandidateListing() {
	require := testutil.Require(s.T())
	objectKeys := []string{"consolidated/dirty-b.cscb.zstd", "consolidated/dirty-a.cscb.zstd"}
	fake := &repairActivityFake{candidateObjectKeys: objectKeys}
	s.batchConsolidator.repairer = fake

	response, err := s.batchConsolidator.executeRepairCandidates(context.Background(), &BatchConsolidatorRepairCandidatesRequest{
		Tag:         2,
		StartHeight: 900,
		EndHeight:   1200,
		Limit:       2,
	})
	require.NoError(err)
	require.Equal(objectKeys, response.ObjectKeys)
	require.Equal(2, fake.candidateLimit)
}

func repairActivityManifest(state cscbrepair.State, oldKey string, newKey string) *cscbrepair.Manifest {
	return &cscbrepair.Manifest{
		ID:                       42,
		Tag:                      2,
		State:                    state,
		StartHeight:              1000,
		EndHeight:                1001,
		CanonicalBlockCount:      1,
		TotalBlockCount:          1,
		OldConsolidatedObjectKey: oldKey,
		NewConsolidatedObjectKey: newKey,
		Blocks: []cscbrepair.Block{{
			BlockMetadataID:      10,
			Canonical:            true,
			Tag:                  2,
			Height:               1000,
			Hash:                 "hash-1000",
			SingleBlockObjectKey: "single-block/1000.gzip",
		}},
	}
}

type repairActivityFake struct {
	cscbrepair.Repairer
	prepared            *cscbrepair.Manifest
	restored            *cscbrepair.Manifest
	verified            *cscbrepair.Manifest
	completed           *cscbrepair.Manifest
	calls               []string
	executionKey        string
	objectKey           string
	candidateObjectKeys []string
	candidateLimit      int
	pinnedPayloads      []cscbrepair.PinnedPayload
}

func (f *repairActivityFake) PrepareNext(_ context.Context, executionKey string, _ uint32, _ uint64, _ uint64, _ uint64, _ cscbrepair.Progress) (*cscbrepair.Manifest, error) {
	f.calls = append(f.calls, "prepare")
	f.executionKey = executionKey
	return f.prepared, nil
}

func (f *repairActivityFake) PrepareObject(_ context.Context, executionKey string, _ uint32, _ uint64, _ uint64, _ uint64, objectKey string, _ cscbrepair.Progress) (*cscbrepair.Manifest, error) {
	f.calls = append(f.calls, "prepare_object")
	f.executionKey = executionKey
	f.objectKey = objectKey
	return f.prepared, nil
}

func (f *repairActivityFake) ListCandidates(_ context.Context, _ uint32, _ uint64, _ uint64, limit int) ([]string, error) {
	f.candidateLimit = limit
	return f.candidateObjectKeys, nil
}

func (f *repairActivityFake) Restore(context.Context, int64, cscbrepair.Progress) (*cscbrepair.Manifest, error) {
	f.calls = append(f.calls, "restore")
	return f.restored, nil
}

func (f *repairActivityFake) VerifyRebuilt(context.Context, int64, cscbrepair.Progress) (*cscbrepair.Manifest, error) {
	f.calls = append(f.calls, "verify")
	return f.verified, nil
}

func (f *repairActivityFake) VisitPinnedPayloads(
	_ context.Context,
	_ int64,
	_ cscbrepair.Progress,
	visit func(cscbrepair.PinnedPayload) error,
) error {
	f.calls = append(f.calls, "visit_pinned")
	for _, payload := range f.pinnedPayloads {
		if err := visit(payload); err != nil {
			return err
		}
	}
	return nil
}

func (f *repairActivityFake) VerifyAndPromote(
	context.Context,
	int64,
	string,
	[]cscbrepair.RebuiltPlacement,
	time.Duration,
	cscbrepair.Progress,
) (*cscbrepair.Manifest, error) {
	f.calls = append(f.calls, "verify_promote")
	return f.verified, nil
}

func (f *repairActivityFake) Complete(context.Context, int64, cscbrepair.Progress) (*cscbrepair.Manifest, error) {
	f.calls = append(f.calls, "complete")
	return f.completed, nil
}

const testRepairExecutionKey = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

func marshalRepairBlock(t *testing.T, block *api.Block) []byte {
	t.Helper()
	raw, err := proto.Marshal(storageutils.CloneBlockWithoutStoragePlacement(block))
	testutil.Require(t).NoError(err)
	return raw
}
