package cscbrepair

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"hash/crc32"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/coinbase/chainstorage/internal/storage/blobstorage/cscb"
	"github.com/coinbase/chainstorage/internal/storage/retirement"
	storageutils "github.com/coinbase/chainstorage/internal/storage/utils"
	"github.com/coinbase/chainstorage/protos/coinbase/c3/common"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

func TestValidateCandidateAllowsAllNonCanonicalRows(t *testing.T) {
	candidate := validRepairCandidate()
	candidate.Blocks[0].Canonical = false
	candidate.CanonicalBlockCount = 0
	require.NoError(t, validateCandidate(candidate, 1000, 1001, 1))
}

func TestValidateCandidateRejectsUnsafeSingleBlockState(t *testing.T) {
	valid := validRepairCandidate()
	require.NoError(t, validateCandidate(valid, 1000, 1001, 1))

	tests := map[string]func(*Manifest){
		"retirement fence":         func(candidate *Manifest) { candidate.Blocks[0].RetirementFenced = true },
		"retirement manifest":      func(candidate *Manifest) { candidate.Blocks[0].RetirementManifestExists = true },
		"deleted single block":     func(candidate *Manifest) { candidate.Blocks[0].SingleBlockObjectDeleted = true },
		"missing single block key": func(candidate *Manifest) { candidate.Blocks[0].SingleBlockObjectKey = "" },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			candidate := validRepairCandidate()
			mutate(candidate)
			require.ErrorContains(t, validateCandidate(candidate, 1000, 1001, 1), "blocked or incomplete")
		})
	}
}

func TestPrepareObjectRejectsNonLegacyCandidateBeforeS3AfterWriteRollback(t *testing.T) {
	tests := map[string]func(*Block){
		"primary":             func(block *Block) { block.StorageGeneration = "v2" },
		"single block shadow": func(block *Block) { block.SingleBlockStorageGeneration = "v2" },
		"consolidated shadow": func(block *Block) { block.ConsolidatedStorageGeneration = "v2" },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			candidate := validRepairCandidate()
			mutate(&candidate.Blocks[0])
			repository := &preparationRepository{candidate: candidate}
			store := &preparationObjectStore{repository: repository}
			repairer := NewRepairer(repository, store, "legacy-repair-bucket")

			_, err := repairer.PrepareObject(
				context.Background(),
				executionKey("write-generation-rollback"),
				candidate.Tag,
				candidate.StartHeight,
				candidate.EndHeight,
				1,
				candidate.OldConsolidatedObjectKey,
				nil,
			)
			require.ErrorContains(t, err, "legacy storage generation")
			require.False(t, repository.fenced)
			require.Zero(t, store.listCalls, "non-legacy repair must fail before S3")
		})
	}
}

func TestValidateRebuiltPlacementsRejectsNonLegacyUpload(t *testing.T) {
	candidate := validRepairCandidate()
	_, err := validateRebuiltPlacements(candidate, "consolidated/rebuilt.cscb.zstd", []RebuiltPlacement{{
		BlockMetadataID:    candidate.Blocks[0].BlockMetadataID,
		Height:             candidate.Blocks[0].Height,
		Hash:               candidate.Blocks[0].Hash,
		ObjectFormat:       api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_CSCB_BATCH,
		StorageGeneration:  "v2",
		ByteLength:         100,
		UncompressedLength: 200,
	}})
	require.ErrorContains(t, err, "legacy storage generation")
}

func TestCompareCandidateRowsRejectsStorageGenerationChange(t *testing.T) {
	tests := map[string]func(*Block){
		"primary":             func(block *Block) { block.StorageGeneration = "v2" },
		"single block shadow": func(block *Block) { block.SingleBlockStorageGeneration = "v2" },
		"consolidated shadow": func(block *Block) { block.ConsolidatedStorageGeneration = "v2" },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			pinned := validRepairCandidate()
			current := *pinned
			current.Blocks = append([]Block(nil), pinned.Blocks...)
			mutate(&current.Blocks[0])

			require.ErrorContains(t, compareCandidateRows(&current, pinned), "row changed")
		})
	}
}

func TestPrepareNextRejectsPendingRepairOutsideApprovedRange(t *testing.T) {
	pending := validRepairCandidate()
	pending.State = StatePrepared
	repository := &pendingRepairRepository{pending: pending}
	repairer := NewRepairer(repository, &pendingObjectStore{}, "repair-bucket")

	_, err := repairer.PrepareNext(context.Background(), executionKey("outside-range"), 2, 2000, 2001, 1, nil)
	require.ErrorContains(t, err, "pending CSCB repair does not fit the approved request")
	require.ErrorContains(t, err, "exceeds approved range")
}

func TestPrepareNextPersistsFenceBeforeS3InspectionAndRetriesSameManifest(t *testing.T) {
	inspectionErr := errors.New("S3 inspection stopped")
	repository := &preparationRepository{candidate: validRepairCandidate()}
	store := &preparationObjectStore{repository: repository, listErr: inspectionErr}
	repairer := NewRepairer(repository, store, "repair-bucket")
	key := executionKey("durable-fence")

	_, err := repairer.PrepareNext(context.Background(), key, 2, 1000, 1001, 1, nil)
	require.ErrorIs(t, err, inspectionErr)
	require.True(t, repository.fenced)
	require.True(t, repository.bound)
	require.False(t, repository.inspectionRecorded)
	require.Equal(t, StatePreparing, repository.manifest.State)
	require.Empty(t, repository.manifest.OldConsolidatedObjectVersion.VersionID)
	require.Len(t, repository.manifest.RowSetSHA256, 64)
	require.Equal(t, objectKeySHA256("single/1000.gzip"), repository.manifest.Blocks[0].SingleBlockObjectKeySHA256)
	require.Equal(t, 1, repository.fenceCalls)

	_, err = repairer.PrepareNext(context.Background(), key, 2, 1000, 1001, 1, nil)
	require.ErrorIs(t, err, inspectionErr)
	require.Equal(t, 1, repository.fenceCalls, "retry must reuse the durable preparing manifest")
	require.Equal(t, 2, store.listCalls)
}

func TestPrepareObjectSelectsOnlyAssignedObject(t *testing.T) {
	inspectionErr := errors.New("S3 inspection stopped")
	candidate := validRepairCandidate()
	repository := &preparationRepository{candidate: candidate}
	store := &preparationObjectStore{repository: repository, listErr: inspectionErr}
	repairer := NewRepairer(repository, store, "repair-bucket")

	_, err := repairer.PrepareObject(
		context.Background(),
		executionKey("assigned-object"),
		candidate.Tag,
		candidate.StartHeight,
		candidate.EndHeight,
		1,
		candidate.OldConsolidatedObjectKey,
		nil,
	)
	require.ErrorIs(t, err, inspectionErr)
	require.Equal(t, candidate.OldConsolidatedObjectKey, repository.requestedObjectKey)
	require.True(t, repository.fenced)
	require.True(t, repository.bound)
}

func TestPrepareObjectRejectsExecutionBoundToDifferentObject(t *testing.T) {
	bound := validRepairCandidate()
	bound.ID = 41
	bound.State = StatePrepared
	repository := &boundRepairRepository{manifest: bound}
	repairer := NewRepairer(repository, &pendingObjectStore{}, "repair-bucket")

	_, err := repairer.PrepareObject(
		context.Background(),
		executionKey("bound-object"),
		bound.Tag,
		bound.StartHeight,
		bound.EndHeight,
		1,
		"consolidated/different.cscb.zstd",
		nil,
	)
	require.ErrorContains(t, err, "bound CSCB repair object changed")
}

func TestPrepareObjectReturnsCompletedManifestFromConcurrentWorkflow(t *testing.T) {
	completed := validRepairCandidate()
	completed.ID = 42
	completed.State = StateCompleted
	repository := &completedObjectRepository{manifest: completed}
	repairer := NewRepairer(repository, &pendingObjectStore{}, "repair-bucket")
	key := executionKey("completed-object")

	manifest, err := repairer.PrepareObject(
		context.Background(),
		key,
		completed.Tag,
		completed.StartHeight,
		completed.EndHeight,
		1,
		completed.OldConsolidatedObjectKey,
		nil,
	)
	require.NoError(t, err)
	require.Same(t, completed, manifest)
	require.Equal(t, key, repository.executionKey)
}

func TestListCandidatesDelegatesExactLimit(t *testing.T) {
	repository := &candidateListRepository{
		objectKeys: []string{"consolidated/dirty-b.cscb.zstd", "consolidated/dirty-a.cscb.zstd"},
	}
	repairer := NewRepairer(repository, &pendingObjectStore{}, "repair-bucket")

	objectKeys, err := repairer.ListCandidates(context.Background(), 2, 1000, 1200, 2)
	require.NoError(t, err)
	require.Equal(t, repository.objectKeys, objectKeys)
	require.Equal(t, 2, repository.limit)
}

func TestPrepareNextCompletesAlreadyCleanCSCBWithoutRestore(t *testing.T) {
	candidate := validRepairCandidate()
	candidate.Blocks[0].Hash = "clean-hash"
	candidate.Blocks[0].OldByteOffset = 0
	block := &api.Block{Metadata: &api.BlockMetadata{
		Tag:    candidate.Tag,
		Height: candidate.Blocks[0].Height,
		Hash:   candidate.Blocks[0].Hash,
	}}
	payload, err := proto.Marshal(block)
	require.NoError(t, err)
	singleObject, err := storageutils.Compress(payload, api.Compression_GZIP)
	require.NoError(t, err)
	candidate.Blocks[0].OldByteLength = uint64(len(payload))
	candidate.Blocks[0].OldUncompressedLength = uint64(len(payload))
	candidate.OldConsolidatedObjectKey = "consolidated/clean.cscb.gzip"
	candidate.Blocks[0].OldConsolidatedObjectKey = candidate.OldConsolidatedObjectKey
	cscbObject := buildRepairSingleBlockCSCB(t, retirement.Candidate{
		BlockMetadataID:    candidate.Blocks[0].BlockMetadataID,
		Tag:                candidate.Tag,
		Height:             candidate.Blocks[0].Height,
		Hash:               candidate.Blocks[0].Hash,
		ByteOffset:         0,
		ByteLength:         uint64(len(payload)),
		UncompressedLength: uint64(len(payload)),
	}, payload)

	repository := &preparationRepository{candidate: candidate}
	store := &preparationObjectStore{
		repository: repository,
		topologies: map[string]retirement.ObjectVersionTopology{
			candidate.OldConsolidatedObjectKey: {
				Versions: []retirement.ObjectVersion{{VersionID: "clean-cscb-version", ETag: "clean-cscb-etag", Bytes: uint64(len(cscbObject)), IsLatest: true}},
			},
			candidate.Blocks[0].SingleBlockObjectKey: {
				Versions: []retirement.ObjectVersion{
					{VersionID: "single-current-version", ETag: "single-etag", Bytes: uint64(len(singleObject)), IsLatest: true},
					{VersionID: "single-historical-version", ETag: "single-etag", Bytes: uint64(len(singleObject))},
				},
			},
		},
		objects: map[string][]byte{
			candidate.OldConsolidatedObjectKey:       cscbObject,
			candidate.Blocks[0].SingleBlockObjectKey: singleObject,
		},
	}

	manifest, err := NewRepairer(repository, store, "repair-bucket").PrepareNext(
		context.Background(),
		executionKey("already-clean"),
		candidate.Tag,
		candidate.StartHeight,
		candidate.EndHeight,
		1,
		nil,
	)
	require.NoError(t, err)
	require.Equal(t, StateCompleted, manifest.State)
	require.Equal(t, alreadyCleanOutcome, manifest.Outcome)
	require.NotNil(t, manifest.VerifiedAt)
	require.NotNil(t, manifest.CompletedAt)
	require.Nil(t, manifest.RestoredAt)
	require.True(t, repository.inspectionRecorded)
	require.True(t, repository.alreadyClean)
}

func TestPrepareNextUsesPinnedSingleBlockAsAuthoritativeRepairPayload(t *testing.T) {
	candidate := validRepairCandidate()
	candidate.Blocks[0].Hash = "repair-hash"
	candidate.Blocks[0].OldByteOffset = 0

	currentBlock := &api.Block{
		Blockchain: common.Blockchain_BLOCKCHAIN_SOLANA,
		Network:    common.Network_NETWORK_SOLANA_MAINNET,
		Metadata: &api.BlockMetadata{
			Tag:           candidate.Tag,
			Height:        candidate.Blocks[0].Height,
			Hash:          candidate.Blocks[0].Hash,
			ObjectKeyMain: candidate.Blocks[0].SingleBlockObjectKey,
			ObjectFormat:  api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_SINGLE_BLOCK,
		},
		Blobdata: &api.Block_Solana{
			Solana: &api.SolanaBlobdata{Header: []byte(`{"transactions":["current"]}`)},
		},
	}
	oldBlock := proto.Clone(currentBlock).(*api.Block)
	oldBlock.GetSolana().Header = []byte(`{"transactions":["stale"]}`)

	currentPayload, err := proto.Marshal(currentBlock)
	require.NoError(t, err)
	currentObject, err := storageutils.Compress(currentPayload, api.Compression_GZIP)
	require.NoError(t, err)
	oldPayload, err := proto.Marshal(oldBlock)
	require.NoError(t, err)
	candidate.Blocks[0].OldByteLength = uint64(len(oldPayload))
	candidate.Blocks[0].OldUncompressedLength = uint64(len(oldPayload))
	oldObject := buildRepairSingleBlockCSCB(t, retirement.Candidate{
		BlockMetadataID:    candidate.Blocks[0].BlockMetadataID,
		Tag:                candidate.Tag,
		Height:             candidate.Blocks[0].Height,
		Hash:               candidate.Blocks[0].Hash,
		ByteOffset:         0,
		ByteLength:         uint64(len(oldPayload)),
		UncompressedLength: uint64(len(oldPayload)),
	}, oldPayload)

	repository := &preparationRepository{candidate: candidate}
	store := &preparationObjectStore{
		repository: repository,
		topologies: map[string]retirement.ObjectVersionTopology{
			candidate.OldConsolidatedObjectKey: {
				Versions: []retirement.ObjectVersion{{
					VersionID: "old-cscb-version",
					ETag:      "old-cscb-etag",
					Bytes:     uint64(len(oldObject)),
					IsLatest:  true,
				}},
			},
			candidate.Blocks[0].SingleBlockObjectKey: {
				Versions: []retirement.ObjectVersion{{
					VersionID: "current-single-version",
					ETag:      "current-single-etag",
					Bytes:     uint64(len(currentObject)),
					IsLatest:  true,
				}},
			},
		},
		objects: map[string][]byte{
			candidate.OldConsolidatedObjectKey:       oldObject,
			candidate.Blocks[0].SingleBlockObjectKey: currentObject,
		},
	}
	repairer := NewRepairer(repository, store, "repair-bucket")

	manifest, err := repairer.PrepareNext(
		context.Background(),
		executionKey("authoritative-single-block"),
		candidate.Tag,
		candidate.StartHeight,
		candidate.EndHeight,
		1,
		nil,
	)
	require.NoError(t, err)
	require.Equal(t, StatePrepared, manifest.State)
	require.False(t, repository.alreadyClean)

	currentNormalized := storageutils.CloneBlockWithoutStoragePlacement(currentBlock)
	expectedPayload, err := (proto.MarshalOptions{Deterministic: true}).Marshal(currentNormalized)
	require.NoError(t, err)
	expectedDigest := sha256.Sum256(expectedPayload)
	require.Equal(t, hex.EncodeToString(expectedDigest[:]), manifest.Blocks[0].PayloadSHA256)

	var pinned PinnedPayload
	require.NoError(t, repairer.VisitPinnedPayloads(
		context.Background(),
		manifest.ID,
		nil,
		func(payload PinnedPayload) error {
			pinned = payload
			return nil
		},
	))
	var rebuilt api.Block
	require.NoError(t, proto.Unmarshal(pinned.RawBlockPayload, &rebuilt))
	require.False(t, storageutils.HasBlockStoragePlacement(&rebuilt))
	require.True(t, proto.Equal(currentNormalized, &rebuilt))
	require.False(t, proto.Equal(storageutils.CloneBlockWithoutStoragePlacement(oldBlock), &rebuilt))
}

func TestInspectSingleBlockObjectVersionPinsCurrentWithoutReadingHistoricalPayloads(t *testing.T) {
	candidate := singleBlockVersionCandidate()
	currentPayload := singleBlockVersionPayload(t, candidate, `{"blockhash":"block-hash","transactions":[]}`)
	historicalPayload := singleBlockVersionPayload(t, candidate, `{"blockhash":"block-hash","transactions":["stale"]}`)
	require.NotEqual(t, currentPayload, historicalPayload)
	store := &versionedObjectStore{
		topology: retirement.ObjectVersionTopology{Versions: []retirement.ObjectVersion{
			{VersionID: "current-version", ETag: "current-etag", Bytes: uint64(len(currentPayload)), IsLatest: true},
			{VersionID: "historical-version", ETag: "historical-etag", Bytes: uint64(len(historicalPayload))},
		}},
		objects: map[string][]byte{
			"current-version":    currentPayload,
			"historical-version": historicalPayload,
		},
	}
	repairer := &repairerImpl{store: store, bucket: "repair-bucket"}

	version, err := repairer.inspectSingleBlockObjectVersion(context.Background(), candidate)
	require.NoError(t, err)
	require.Equal(t, ObjectVersion{
		VersionID: "current-version",
		ETag:      "current-etag",
		Bytes:     uint64(len(currentPayload)),
	}, version)
	require.Empty(t, store.readVersionIDs)
}

func TestInspectSingleBlockObjectVersionDoesNotReadSingleVersionTwice(t *testing.T) {
	candidate := singleBlockVersionCandidate()
	store := &versionedObjectStore{
		topology: retirement.ObjectVersionTopology{Versions: []retirement.ObjectVersion{
			{VersionID: "current-version", ETag: "current-etag", Bytes: 4, IsLatest: true},
		}},
	}
	repairer := &repairerImpl{store: store, bucket: "repair-bucket"}

	version, err := repairer.inspectSingleBlockObjectVersion(context.Background(), candidate)
	require.NoError(t, err)
	require.Equal(t, "current-version", version.VersionID)
	require.Empty(t, store.readVersionIDs)
}

func TestInspectSingleBlockObjectVersionAllowsManyImmutableVersionsWithoutReading(t *testing.T) {
	candidate := singleBlockVersionCandidate()
	versions := make([]retirement.ObjectVersion, 20)
	for i := range versions {
		versions[i] = retirement.ObjectVersion{
			VersionID: fmt.Sprintf("version-%d", i),
			ETag:      "same-etag",
			Bytes:     4,
			IsLatest:  i == 0,
		}
	}
	store := &versionedObjectStore{
		topology: retirement.ObjectVersionTopology{Versions: versions},
	}
	repairer := &repairerImpl{store: store, bucket: "repair-bucket"}

	version, err := repairer.inspectSingleBlockObjectVersion(context.Background(), candidate)
	require.NoError(t, err)
	require.Equal(t, "version-0", version.VersionID)
	require.Empty(t, store.readVersionIDs)
}

func TestInspectSingleBlockObjectVersionRejectsDeleteMarker(t *testing.T) {
	candidate := singleBlockVersionCandidate()
	store := &versionedObjectStore{
		topology: retirement.ObjectVersionTopology{
			Versions: []retirement.ObjectVersion{
				{VersionID: "current-version", ETag: "same-etag", Bytes: 4, IsLatest: true},
			},
			DeleteMarkers: []retirement.ObjectDeleteMarker{{VersionID: "delete-marker"}},
		},
	}
	repairer := &repairerImpl{store: store, bucket: "repair-bucket"}

	_, err := repairer.inspectSingleBlockObjectVersion(context.Background(), candidate)
	require.ErrorContains(t, err, "zero delete markers")
}

func TestRequirePinnedSingleBlockObjectVersionRejectsNewLatestVersion(t *testing.T) {
	pinned := ObjectVersion{VersionID: "pinned-version", ETag: "same-etag", Bytes: 4}
	store := &versionedObjectStore{
		topology: retirement.ObjectVersionTopology{Versions: []retirement.ObjectVersion{
			{VersionID: "new-version", ETag: "same-etag", Bytes: 4, IsLatest: true},
			{VersionID: pinned.VersionID, ETag: pinned.ETag, Bytes: pinned.Bytes},
		}},
	}
	repairer := &repairerImpl{store: store, bucket: "repair-bucket"}

	err := repairer.requirePinnedSingleBlockObjectVersion(context.Background(), "single/1000.zstd", pinned)
	require.ErrorContains(t, err, "current version changed")
}

func TestRequirePinnedSingleBlockObjectVersionAllowsHistoricalDuplicateRemoval(t *testing.T) {
	pinned := ObjectVersion{VersionID: "pinned-version", ETag: "same-etag", Bytes: 4}
	store := &versionedObjectStore{
		topology: retirement.ObjectVersionTopology{Versions: []retirement.ObjectVersion{
			{VersionID: pinned.VersionID, ETag: pinned.ETag, Bytes: pinned.Bytes, IsLatest: true},
		}},
	}
	repairer := &repairerImpl{store: store, bucket: "repair-bucket"}

	require.NoError(t, repairer.requirePinnedSingleBlockObjectVersion(context.Background(), "single/1000.zstd", pinned))
}

func TestRestoreZeroCanonicalRepairResumesWithHistoricalSingleBlockVersion(t *testing.T) {
	manifest := completionManifest(t)
	manifest.State = StatePrepared
	repository := &phaseBoundaryRepository{manifest: manifest}
	store := &completionObjectStore{topologies: map[string]retirement.ObjectVersionTopology{
		manifest.OldConsolidatedObjectKey: {
			Versions: []retirement.ObjectVersion{objectTopologyVersion(manifest.OldConsolidatedObjectVersion)},
		},
		manifest.Blocks[0].SingleBlockObjectKey: {
			Versions: []retirement.ObjectVersion{
				objectTopologyVersion(manifest.Blocks[0].SingleBlockObjectVersion),
				{VersionID: "historical-version", ETag: "historical-etag", Bytes: 99},
			},
		},
	}}

	_, err := NewRepairer(repository, store, manifest.Bucket).Restore(context.Background(), manifest.ID, nil)
	require.NoError(t, err)
	require.True(t, repository.restoreCalled)
}

func TestVisitPinnedPayloadsReadsPinnedCurrentVersionAndStripsStoragePlacement(t *testing.T) {
	manifest := phaseBoundaryManifest(t, StatePrepared)
	block := &manifest.Blocks[0]
	rawBlock := &api.Block{Metadata: &api.BlockMetadata{
		Tag:                block.Tag,
		Height:             block.Height,
		Hash:               block.Hash,
		ObjectKeyMain:      block.SingleBlockObjectKey,
		ObjectFormat:       api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_SINGLE_BLOCK,
		ByteOffset:         10,
		ByteLength:         20,
		UncompressedLength: 30,
	}}
	normalized := storageutils.CloneBlockWithoutStoragePlacement(rawBlock)
	normalizedPayload, err := (proto.MarshalOptions{Deterministic: true}).Marshal(normalized)
	require.NoError(t, err)
	digest := sha256.Sum256(normalizedPayload)
	block.PayloadSHA256 = hex.EncodeToString(digest[:])
	payload, err := proto.Marshal(rawBlock)
	require.NoError(t, err)
	compressed, err := storageutils.Compress(payload, api.Compression_GZIP)
	require.NoError(t, err)
	block.SingleBlockObjectVersion.Bytes = uint64(len(compressed))

	repository := &phaseBoundaryRepository{manifest: manifest}
	store := &completionObjectStore{
		topologies: map[string]retirement.ObjectVersionTopology{
			manifest.OldConsolidatedObjectKey: {
				Versions: []retirement.ObjectVersion{objectTopologyVersion(manifest.OldConsolidatedObjectVersion)},
			},
			block.SingleBlockObjectKey: {
				Versions: []retirement.ObjectVersion{
					objectTopologyVersion(block.SingleBlockObjectVersion),
					{VersionID: "historical-version", ETag: "historical-etag", Bytes: 99},
				},
			},
		},
		objects: map[string][]byte{block.SingleBlockObjectKey: compressed},
	}

	var pinned PinnedPayload
	err = NewRepairer(repository, store, manifest.Bucket).VisitPinnedPayloads(
		context.Background(),
		manifest.ID,
		nil,
		func(payload PinnedPayload) error {
			pinned = payload
			return nil
		},
	)
	require.NoError(t, err)
	require.Equal(t, []string{block.SingleBlockObjectVersion.VersionID}, store.readVersionIDs)
	require.Equal(t, block.BlockMetadataID, pinned.BlockMetadataID)
	require.Equal(t, block.Tag, pinned.Metadata.GetTag())
	require.Equal(t, block.Height, pinned.Metadata.GetHeight())
	require.Equal(t, block.Hash, pinned.Metadata.GetHash())
	var rebuilt api.Block
	require.NoError(t, proto.Unmarshal(pinned.RawBlockPayload, &rebuilt))
	require.False(t, storageutils.HasBlockStoragePlacement(&rebuilt))
}

func TestVisitPinnedPayloadsRejectsPersistedDigestMismatch(t *testing.T) {
	manifest := phaseBoundaryManifest(t, StatePrepared)
	block := &manifest.Blocks[0]
	rawBlock := &api.Block{Metadata: &api.BlockMetadata{
		Tag:    block.Tag,
		Height: block.Height,
		Hash:   block.Hash,
	}}
	payload, err := proto.Marshal(rawBlock)
	require.NoError(t, err)
	compressed, err := storageutils.Compress(payload, api.Compression_GZIP)
	require.NoError(t, err)
	block.SingleBlockObjectVersion.Bytes = uint64(len(compressed))
	block.PayloadSHA256 = strings.Repeat("0", 64)

	repository := &phaseBoundaryRepository{manifest: manifest}
	store := &completionObjectStore{
		topologies: map[string]retirement.ObjectVersionTopology{
			manifest.OldConsolidatedObjectKey: {
				Versions: []retirement.ObjectVersion{objectTopologyVersion(manifest.OldConsolidatedObjectVersion)},
			},
			block.SingleBlockObjectKey: {
				Versions: []retirement.ObjectVersion{objectTopologyVersion(block.SingleBlockObjectVersion)},
			},
		},
		objects: map[string][]byte{block.SingleBlockObjectKey: compressed},
	}

	err = NewRepairer(repository, store, manifest.Bucket).VisitPinnedPayloads(
		context.Background(), manifest.ID, nil, func(PinnedPayload) error { return nil },
	)
	require.ErrorContains(t, err, "single-block payload digest changed before rebuild")
}

func TestRepairPhasesRejectSingleBlockTopologyDriftBeforeMutation(t *testing.T) {
	phases := []struct {
		name   string
		state  State
		invoke func(Repairer, int64) (*Manifest, error)
	}{
		{
			name:  "restore",
			state: StatePrepared,
			invoke: func(repairer Repairer, repairID int64) (*Manifest, error) {
				return repairer.Restore(context.Background(), repairID, nil)
			},
		},
		{
			name:  "verify rebuilt",
			state: StateRestored,
			invoke: func(repairer Repairer, repairID int64) (*Manifest, error) {
				return repairer.VerifyRebuilt(context.Background(), repairID, nil)
			},
		},
		{
			name:  "complete",
			state: StateVerified,
			invoke: func(repairer Repairer, repairID int64) (*Manifest, error) {
				return repairer.Complete(context.Background(), repairID, nil)
			},
		},
	}
	drifts := []struct {
		name     string
		topology func(ObjectVersion) retirement.ObjectVersionTopology
	}{
		{
			name: "new latest version",
			topology: func(pinned ObjectVersion) retirement.ObjectVersionTopology {
				return retirement.ObjectVersionTopology{Versions: []retirement.ObjectVersion{
					{VersionID: "new-version", ETag: pinned.ETag, Bytes: pinned.Bytes, IsLatest: true},
					objectTopologyVersion(pinned),
				}}
			},
		},
		{
			name: "delete marker",
			topology: func(pinned ObjectVersion) retirement.ObjectVersionTopology {
				return retirement.ObjectVersionTopology{
					Versions:      []retirement.ObjectVersion{objectTopologyVersion(pinned)},
					DeleteMarkers: []retirement.ObjectDeleteMarker{{VersionID: "delete-marker", IsLatest: true}},
				}
			},
		},
	}

	for _, phase := range phases {
		for _, drift := range drifts {
			t.Run(phase.name+"/"+drift.name, func(t *testing.T) {
				manifest := phaseBoundaryManifest(t, phase.state)
				repository := &phaseBoundaryRepository{manifest: manifest}
				store := &completionObjectStore{topologies: map[string]retirement.ObjectVersionTopology{
					manifest.OldConsolidatedObjectKey: {
						Versions: []retirement.ObjectVersion{objectTopologyVersion(manifest.OldConsolidatedObjectVersion)},
					},
					manifest.NewConsolidatedObjectKey: {
						Versions: []retirement.ObjectVersion{objectTopologyVersion(manifest.NewConsolidatedObjectVersion)},
					},
					manifest.Blocks[0].SingleBlockObjectKey: drift.topology(manifest.Blocks[0].SingleBlockObjectVersion),
				}}

				_, err := phase.invoke(NewRepairer(repository, store, "repair-bucket"), manifest.ID)
				require.ErrorContains(t, err, "single-block object changed")
				require.False(t, repository.restoreCalled)
				require.False(t, repository.recordVerifiedCalled)
				require.False(t, repository.completeCalled)
			})
		}
	}
}

func TestVerifyRebuiltResumesManifestWithPreSemanticSolanaDigest(t *testing.T) {
	manifest := phaseBoundaryManifest(t, StateRestored)
	block := &manifest.Blocks[0]
	rawBlock := &api.Block{
		Blockchain: common.Blockchain_BLOCKCHAIN_SOLANA,
		Network:    common.Network_NETWORK_SOLANA_MAINNET,
		Metadata: &api.BlockMetadata{
			Tag:    block.Tag,
			Height: block.Height,
			Hash:   block.Hash,
		},
		Blobdata: &api.Block_Solana{Solana: &api.SolanaBlobdata{
			Header: []byte("{\n  \"transactions\": [],\n  \"blockhash\": \"block-hash\"\n}"),
		}},
	}
	payload, err := (proto.MarshalOptions{Deterministic: true}).Marshal(rawBlock)
	require.NoError(t, err)
	preSemanticDigest := sha256.Sum256(payload)
	block.PayloadSHA256 = hex.EncodeToString(preSemanticDigest[:])
	block.NewByteOffset = 0
	block.NewByteLength = uint64(len(payload))
	block.NewUncompressedLength = uint64(len(payload))

	singleObject, err := storageutils.Compress(payload, api.Compression_GZIP)
	require.NoError(t, err)
	block.SingleBlockObjectVersion.Bytes = uint64(len(singleObject))
	newCSCB := buildRepairSingleBlockCSCB(t, retirement.Candidate{
		BlockMetadataID: block.BlockMetadataID,
		Tag:             block.Tag,
		Height:          block.Height,
		Hash:            block.Hash,
		ByteOffset:      block.NewByteOffset,
	}, payload)
	manifest.NewConsolidatedObjectVersion.Bytes = uint64(len(newCSCB))

	repository := &phaseBoundaryRepository{manifest: manifest}
	store := &completionObjectStore{
		topologies: map[string]retirement.ObjectVersionTopology{
			manifest.NewConsolidatedObjectKey: {
				Versions: []retirement.ObjectVersion{objectTopologyVersion(manifest.NewConsolidatedObjectVersion)},
			},
			block.SingleBlockObjectKey: {
				Versions: []retirement.ObjectVersion{objectTopologyVersion(block.SingleBlockObjectVersion)},
			},
		},
		objects: map[string][]byte{
			manifest.NewConsolidatedObjectKey: newCSCB,
			block.SingleBlockObjectKey:        singleObject,
		},
	}

	_, err = NewRepairer(repository, store, manifest.Bucket).VerifyRebuilt(context.Background(), manifest.ID, nil)
	require.NoError(t, err)
	require.True(t, repository.recordVerifiedCalled)
	require.Equal(t, hex.EncodeToString(preSemanticDigest[:]), block.PayloadSHA256)
}

func TestCompleteRejectsRetainedObjectTopologyDrift(t *testing.T) {
	manifest := completionManifest(t)
	repository := &completionRepository{manifest: manifest}
	store := &completionObjectStore{
		topologies: map[string]retirement.ObjectVersionTopology{
			manifest.OldConsolidatedObjectKey: {
				Versions: []retirement.ObjectVersion{
					objectTopologyVersion(manifest.OldConsolidatedObjectVersion),
					{VersionID: "unexpected-version", ETag: "unexpected-etag", Bytes: 1},
				},
			},
		},
	}

	_, err := NewRepairer(repository, store, "repair-bucket").Complete(context.Background(), manifest.ID, nil)
	require.ErrorContains(t, err, "retained CSCB repair source changed before completion")
	require.ErrorContains(t, err, "expected one data version and zero delete markers")
	require.False(t, repository.completeCalled)
	require.False(t, repository.committed)
}

func TestCompletePropagatesDatabaseFailureAfterValidation(t *testing.T) {
	manifest := completionManifest(t)
	completeErr := errors.New("database completion failed")
	repository := &completionRepository{manifest: manifest, completeErr: completeErr}
	store := &completionObjectStore{
		topologies: map[string]retirement.ObjectVersionTopology{
			manifest.OldConsolidatedObjectKey: {
				Versions: []retirement.ObjectVersion{objectTopologyVersion(manifest.OldConsolidatedObjectVersion)},
			},
			manifest.Blocks[0].SingleBlockObjectKey: {
				Versions: []retirement.ObjectVersion{objectTopologyVersion(manifest.Blocks[0].SingleBlockObjectVersion)},
			},
		},
		objects: map[string][]byte{
			manifest.Blocks[0].SingleBlockObjectKey: completionSingleBlockPayload(t, manifest.Blocks[0]),
		},
	}

	_, err := NewRepairer(repository, store, "repair-bucket").Complete(context.Background(), manifest.ID, nil)
	require.ErrorIs(t, err, completeErr)
	require.True(t, repository.completeCalled)
	require.False(t, repository.committed)
}

func TestValidateRebuiltMetadataRequiresFreshRetentionAndCleanNonCanonicalRows(t *testing.T) {
	restoredAt := time.Now().UTC()
	validatedAt := restoredAt.Add(time.Second)
	retentionStartedAt := restoredAt.Add(2 * time.Second)
	deleteAfter := retentionStartedAt.Add(72 * time.Hour)
	manifest := &Manifest{
		ID:                  8,
		RestoredAt:          &restoredAt,
		CanonicalBlockCount: 1,
		Blocks: []Block{
			{
				BlockMetadataID:           1,
				Canonical:                 true,
				ActiveObjectKey:           "consolidated/clean.cscb.zstd",
				ActiveObjectFormat:        int32(api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_CSCB_BATCH),
				NewConsolidatedObjectKey:  "consolidated/clean.cscb.zstd",
				NewByteLength:             100,
				NewUncompressedLength:     200,
				NewValidatedAt:            &validatedAt,
				NewRetentionStartedAt:     &retentionStartedAt,
				NewSingleBlockDeleteAfter: &deleteAfter,
			},
			{
				BlockMetadataID:      2,
				SingleBlockObjectKey: "single/2.gzip",
				ActiveObjectKey:      "single/2.gzip",
				ActiveObjectFormat:   int32(api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_SINGLE_BLOCK),
			},
		},
	}
	require.NoError(t, validateRebuiltMetadata(manifest, "consolidated/clean.cscb.zstd"))
	manifest.Blocks[0].StorageGeneration = "v2"
	require.ErrorContains(t, validateRebuiltMetadata(manifest, "consolidated/clean.cscb.zstd"), "legacy storage generation")
	manifest.Blocks[0].StorageGeneration = ""

	staleStartedAt := restoredAt.Add(-time.Second)
	manifest.Blocks[0].NewRetentionStartedAt = &staleStartedAt
	require.ErrorContains(t, validateRebuiltMetadata(manifest, "consolidated/clean.cscb.zstd"), "freshly promoted")

	manifest.Blocks[0].NewRetentionStartedAt = &retentionStartedAt
	manifest.Blocks[1].NewRetentionStartedAt = &retentionStartedAt
	require.ErrorContains(t, validateRebuiltMetadata(manifest, "consolidated/clean.cscb.zstd"), "did not remain on single-block storage")

	manifest.Blocks[1].NewRetentionStartedAt = nil
	tooSoon := retentionStartedAt.Add(71*time.Hour + 59*time.Minute)
	manifest.Blocks[0].NewSingleBlockDeleteAfter = &tooSoon
	require.ErrorContains(t, validateRebuiltMetadata(manifest, "consolidated/clean.cscb.zstd"), "required 72-hour")
}

func validRepairCandidate() *Manifest {
	return &Manifest{
		Tag:                      2,
		OldConsolidatedObjectKey: "consolidated/dirty.cscb.zstd",
		StartHeight:              1000,
		EndHeight:                1001,
		CanonicalBlockCount:      1,
		TotalBlockCount:          1,
		Blocks: []Block{{
			BlockMetadataID:          1,
			Canonical:                true,
			Tag:                      2,
			Height:                   1000,
			SingleBlockObjectKey:     "single/1000.gzip",
			OldConsolidatedObjectKey: "consolidated/dirty.cscb.zstd",
			OldByteLength:            100,
			OldUncompressedLength:    200,
		}},
	}
}

func singleBlockVersionCandidate() retirement.Candidate {
	return retirement.Candidate{
		Bucket: "repair-bucket",
		Key:    "single/1000.gzip",
		Tag:    2,
		Height: 1000,
		Hash:   "block-hash",
	}
}

func singleBlockVersionPayload(t *testing.T, candidate retirement.Candidate, header string) []byte {
	t.Helper()
	payload, err := proto.Marshal(&api.Block{
		Metadata: &api.BlockMetadata{
			Tag:    candidate.Tag,
			Height: candidate.Height,
			Hash:   candidate.Hash,
		},
		Blobdata: &api.Block_Solana{
			Solana: &api.SolanaBlobdata{Header: []byte(header)},
		},
	})
	require.NoError(t, err)
	compressed, err := storageutils.Compress(payload, api.Compression_GZIP)
	require.NoError(t, err)
	return compressed
}

type pendingRepairRepository struct {
	Repository
	pending *Manifest
}

type boundRepairRepository struct {
	Repository
	manifest *Manifest
}

func (r *boundRepairRepository) FindByExecutionKey(context.Context, string) (*Manifest, bool, error) {
	return r.manifest, true, nil
}

type candidateListRepository struct {
	Repository
	objectKeys []string
	limit      int
}

type completedObjectRepository struct {
	Repository
	manifest     *Manifest
	executionKey string
}

func (r *completedObjectRepository) FindByExecutionKey(context.Context, string) (*Manifest, bool, error) {
	return nil, false, nil
}

func (r *completedObjectRepository) FindByObjectKey(context.Context, uint32, string) (*Manifest, error) {
	return r.manifest, nil
}

func (r *completedObjectRepository) BindExecutionKey(_ context.Context, executionKey string, repairID int64) (*Manifest, error) {
	if repairID != r.manifest.ID {
		return nil, errors.New("unexpected repair id")
	}
	r.executionKey = executionKey
	return r.manifest, nil
}

func (r *candidateListRepository) ListCandidateObjectKeys(_ context.Context, _ uint32, _ uint64, _ uint64, limit int) ([]string, error) {
	r.limit = limit
	return r.objectKeys, nil
}

func (r *pendingRepairRepository) FindPending(context.Context, uint32, uint64, uint64) (*Manifest, error) {
	return r.pending, nil
}

func (r *pendingRepairRepository) FindByExecutionKey(context.Context, string) (*Manifest, bool, error) {
	return nil, false, nil
}

func executionKey(seed string) string {
	value := make([]byte, 64)
	for i := range value {
		value[i] = "0123456789abcdef"[(i+len(seed))%16]
	}
	return string(value)
}

type pendingObjectStore struct{ retirement.ObjectStore }

type versionedObjectStore struct {
	retirement.ObjectStore
	topology       retirement.ObjectVersionTopology
	objects        map[string][]byte
	readVersionIDs []string
}

func (s *versionedObjectStore) ListObjectVersions(
	context.Context,
	string,
	string,
) (retirement.ObjectVersionTopology, error) {
	return s.topology, nil
}

func (s *versionedObjectStore) ReadObjectVersion(
	_ context.Context,
	_ string,
	_ string,
	versionID string,
) ([]byte, error) {
	s.readVersionIDs = append(s.readVersionIDs, versionID)
	payload, ok := s.objects[versionID]
	if !ok {
		return nil, errors.New("object version not found")
	}
	return append([]byte(nil), payload...), nil
}

type preparationRepository struct {
	Repository
	candidate          *Manifest
	manifest           *Manifest
	executionKey       string
	fenced             bool
	bound              bool
	inspectionRecorded bool
	alreadyClean       bool
	fenceCalls         int
	requestedObjectKey string
}

func (r *preparationRepository) FindByExecutionKey(_ context.Context, executionKey string) (*Manifest, bool, error) {
	if r.bound && executionKey == r.executionKey {
		return r.manifest, true, nil
	}
	return nil, false, nil
}

func (r *preparationRepository) FindPending(context.Context, uint32, uint64, uint64) (*Manifest, error) {
	return nil, nil
}

func (r *preparationRepository) FindByObjectKey(_ context.Context, _ uint32, objectKey string) (*Manifest, error) {
	r.requestedObjectKey = objectKey
	return nil, nil
}

func (r *preparationRepository) FindNextCandidate(context.Context, uint32, uint64, uint64) (*Manifest, error) {
	return r.candidate, nil
}

func (r *preparationRepository) FindCandidateByObjectKey(_ context.Context, _ uint32, objectKey string) (*Manifest, error) {
	r.requestedObjectKey = objectKey
	if r.candidate != nil && r.candidate.OldConsolidatedObjectKey == objectKey {
		return r.candidate, nil
	}
	return nil, nil
}

func (r *preparationRepository) FenceCandidate(_ context.Context, manifest *Manifest) (*Manifest, error) {
	r.fenceCalls++
	r.fenced = true
	copy := *manifest
	copy.ID = 41
	copy.Blocks = append([]Block(nil), manifest.Blocks...)
	r.manifest = &copy
	return r.manifest, nil
}

func (r *preparationRepository) BindExecutionKey(_ context.Context, executionKey string, repairID int64) (*Manifest, error) {
	if !r.fenced || r.manifest == nil || repairID != r.manifest.ID {
		return nil, errors.New("execution bound before durable fence")
	}
	r.bound = true
	r.executionKey = executionKey
	return r.manifest, nil
}

func (r *preparationRepository) RecordInspection(
	_ context.Context,
	repairID int64,
	oldObject ObjectVersion,
	blocks []Block,
	alreadyClean bool,
) (*Manifest, error) {
	if !r.bound || r.manifest == nil || repairID != r.manifest.ID {
		return nil, errors.New("inspection recorded before durable execution binding")
	}
	r.inspectionRecorded = true
	r.alreadyClean = alreadyClean
	r.manifest.OldConsolidatedObjectVersion = oldObject
	r.manifest.Blocks = append([]Block(nil), blocks...)
	if alreadyClean {
		now := time.Now().UTC()
		r.manifest.State = StateCompleted
		r.manifest.Outcome = alreadyCleanOutcome
		r.manifest.VerifiedAt = &now
		r.manifest.CompletedAt = &now
	} else {
		r.manifest.State = StatePrepared
	}
	return r.manifest, nil
}

func (r *preparationRepository) Get(context.Context, int64) (*Manifest, error) {
	return r.manifest, nil
}

type preparationObjectStore struct {
	retirement.ObjectStore
	repository *preparationRepository
	listErr    error
	listCalls  int
	topologies map[string]retirement.ObjectVersionTopology
	objects    map[string][]byte
}

func (s *preparationObjectStore) ListObjectVersions(
	_ context.Context,
	_ string,
	key string,
) (retirement.ObjectVersionTopology, error) {
	s.listCalls++
	if s.repository == nil || !s.repository.fenced || !s.repository.bound {
		return retirement.ObjectVersionTopology{}, errors.New("S3 inspected before durable repair fence")
	}
	if s.listErr != nil {
		return retirement.ObjectVersionTopology{}, s.listErr
	}
	return s.topologies[key], nil
}

func (s *preparationObjectStore) ReadObjectVersion(
	_ context.Context,
	_ string,
	key string,
	_ string,
) ([]byte, error) {
	object, ok := s.objects[key]
	if !ok {
		return nil, errors.New("object not found")
	}
	return object, nil
}

func (s *preparationObjectStore) ReadObjectVersionRange(
	_ context.Context,
	_ string,
	key string,
	_ string,
	offset uint64,
	length uint64,
) ([]byte, error) {
	object, ok := s.objects[key]
	if !ok {
		return nil, errors.New("object not found")
	}
	end := offset + length
	if end < offset || offset > uint64(len(object)) {
		return nil, errors.New("object range out of bounds")
	}
	if end > uint64(len(object)) {
		end = uint64(len(object))
	}
	return append([]byte(nil), object[offset:end]...), nil
}

type completionRepository struct {
	Repository
	manifest       *Manifest
	completeErr    error
	completeCalled bool
	committed      bool
}

type phaseBoundaryRepository struct {
	Repository
	manifest             *Manifest
	restoreCalled        bool
	recordVerifiedCalled bool
	completeCalled       bool
}

func (r *phaseBoundaryRepository) Get(context.Context, int64) (*Manifest, error) {
	return r.manifest, nil
}

func (r *phaseBoundaryRepository) GetRebuilt(context.Context, int64) (*Manifest, error) {
	return r.manifest, nil
}

func (r *phaseBoundaryRepository) RestoreToSingleBlock(context.Context, int64) (*Manifest, error) {
	r.restoreCalled = true
	return r.manifest, nil
}

func (r *phaseBoundaryRepository) RecordVerified(context.Context, int64, string, ObjectVersion) (*Manifest, error) {
	r.recordVerifiedCalled = true
	return r.manifest, nil
}

func (r *phaseBoundaryRepository) CompleteRetainingOldObject(context.Context, int64, string) (*Manifest, error) {
	r.completeCalled = true
	return r.manifest, nil
}

func (r *completionRepository) Get(context.Context, int64) (*Manifest, error) {
	return r.manifest, nil
}

func (r *completionRepository) GetRebuilt(context.Context, int64) (*Manifest, error) {
	return r.manifest, nil
}

func (r *completionRepository) CompleteRetainingOldObject(
	_ context.Context,
	_ int64,
	_ string,
) (*Manifest, error) {
	r.completeCalled = true
	if r.completeErr != nil {
		return nil, r.completeErr
	}
	r.committed = true
	return r.manifest, nil
}

type completionObjectStore struct {
	retirement.ObjectStore
	topologies     map[string]retirement.ObjectVersionTopology
	objects        map[string][]byte
	readVersionIDs []string
}

func (s *completionObjectStore) ListObjectVersions(
	_ context.Context,
	_ string,
	key string,
) (retirement.ObjectVersionTopology, error) {
	return s.topologies[key], nil
}

func (s *completionObjectStore) ReadObjectVersion(
	_ context.Context,
	_ string,
	key string,
	versionID string,
) ([]byte, error) {
	s.readVersionIDs = append(s.readVersionIDs, versionID)
	return s.objects[key], nil
}

func (s *completionObjectStore) ReadObjectVersionRange(
	_ context.Context,
	_ string,
	key string,
	_ string,
	offset uint64,
	length uint64,
) ([]byte, error) {
	object, ok := s.objects[key]
	if !ok {
		return nil, errors.New("object not found")
	}
	end := offset + length
	if end < offset || offset > uint64(len(object)) {
		return nil, errors.New("object range out of bounds")
	}
	if end > uint64(len(object)) {
		end = uint64(len(object))
	}
	return append([]byte(nil), object[offset:end]...), nil
}

func completionManifest(t *testing.T) *Manifest {
	t.Helper()
	block := Block{
		BlockMetadataID:          1,
		Canonical:                false,
		Tag:                      2,
		Height:                   1000,
		Hash:                     "block-hash",
		SingleBlockObjectKey:     "single/1000.gzip",
		SingleBlockObjectVersion: ObjectVersion{VersionID: "single-version", ETag: "single-etag", Bytes: 100},
	}
	raw := &api.Block{Metadata: &api.BlockMetadata{Tag: block.Tag, Height: block.Height, Hash: block.Hash}}
	normalized, err := (proto.MarshalOptions{Deterministic: true}).Marshal(storageutils.CloneBlockWithoutStoragePlacement(raw))
	require.NoError(t, err)
	digest := sha256.Sum256(normalized)
	block.PayloadSHA256 = hex.EncodeToString(digest[:])
	return &Manifest{
		ID:                           7,
		State:                        StateRestored,
		Bucket:                       "repair-bucket",
		OldConsolidatedObjectKey:     "consolidated/dirty.cscb.zstd",
		OldConsolidatedObjectVersion: ObjectVersion{VersionID: "old-version", ETag: "old-etag", Bytes: 200},
		CanonicalBlockCount:          0,
		TotalBlockCount:              1,
		Blocks:                       []Block{block},
	}
}

func phaseBoundaryManifest(t *testing.T, state State) *Manifest {
	t.Helper()
	manifest := completionManifest(t)
	manifest.State = state
	manifest.CanonicalBlockCount = 1
	manifest.NewConsolidatedObjectKey = "consolidated/clean.cscb.zstd"
	manifest.NewConsolidatedObjectVersion = ObjectVersion{VersionID: "new-version", ETag: "new-etag", Bytes: 300}
	manifest.Blocks[0].Canonical = true
	manifest.Blocks[0].ActiveObjectKey = manifest.NewConsolidatedObjectKey
	manifest.Blocks[0].NewConsolidatedObjectKey = manifest.NewConsolidatedObjectKey
	return manifest
}

func completionSingleBlockPayload(t *testing.T, block Block) []byte {
	t.Helper()
	payload, err := proto.Marshal(&api.Block{Metadata: &api.BlockMetadata{
		Tag:    block.Tag,
		Height: block.Height,
		Hash:   block.Hash,
	}})
	require.NoError(t, err)
	compressed, err := storageutils.Compress(payload, api.Compression_GZIP)
	require.NoError(t, err)
	return compressed
}

func buildRepairSingleBlockCSCB(t *testing.T, candidate retirement.Candidate, payload []byte) []byte {
	t.Helper()
	compressed := gzipRepairPayload(t, payload)
	envelope := make([]byte, cscb.EnvelopeHeaderSize+cscb.BlockIndexRecordSize+cscb.ChunkIndexRecordSize)
	copy(envelope[0:4], []byte("ENV1"))
	binary.LittleEndian.PutUint64(envelope[8:16], 1)
	binary.LittleEndian.PutUint64(envelope[16:24], 1)
	binary.LittleEndian.PutUint64(envelope[24:32], candidate.Height)
	binary.LittleEndian.PutUint64(envelope[32:40], candidate.Height+1)
	binary.LittleEndian.PutUint64(envelope[40:48], cscb.EnvelopeHeaderSize)
	binary.LittleEndian.PutUint64(envelope[48:56], cscb.BlockIndexRecordSize)
	chunkOffset := cscb.EnvelopeHeaderSize + cscb.BlockIndexRecordSize
	binary.LittleEndian.PutUint64(envelope[56:64], uint64(chunkOffset))
	binary.LittleEndian.PutUint64(envelope[64:72], cscb.ChunkIndexRecordSize)

	blockRecord := envelope[cscb.EnvelopeHeaderSize:chunkOffset]
	binary.LittleEndian.PutUint64(blockRecord[0:8], candidate.Height)
	binary.LittleEndian.PutUint64(blockRecord[8:16], candidate.ByteOffset)
	binary.LittleEndian.PutUint64(blockRecord[16:24], uint64(len(payload)))
	binary.LittleEndian.PutUint32(blockRecord[24:28], crc32.ChecksumIEEE(payload))
	hash := sha256.Sum256([]byte(candidate.Hash))
	copy(blockRecord[48:80], hash[:])
	binary.LittleEndian.PutUint64(blockRecord[80:88], uint64(candidate.BlockMetadataID))

	payloadOffset := uint64(cscb.HeaderSize + len(envelope))
	chunkRecord := envelope[chunkOffset:]
	binary.LittleEndian.PutUint64(chunkRecord[8:16], candidate.Height)
	binary.LittleEndian.PutUint64(chunkRecord[16:24], candidate.Height+1)
	binary.LittleEndian.PutUint64(chunkRecord[24:32], payloadOffset)
	binary.LittleEndian.PutUint64(chunkRecord[32:40], uint64(len(compressed)))
	binary.LittleEndian.PutUint64(chunkRecord[48:56], uint64(len(payload)))
	binary.LittleEndian.PutUint32(chunkRecord[56:60], crc32.ChecksumIEEE(payload))
	binary.LittleEndian.PutUint32(chunkRecord[60:64], 1)

	header := make([]byte, cscb.HeaderSize)
	copy(header[0:4], []byte("CSCB"))
	header[4] = 1
	header[5] = 1
	header[6] = 2
	binary.LittleEndian.PutUint32(header[8:12], 1)
	binary.LittleEndian.PutUint32(header[12:16], 1)
	binary.LittleEndian.PutUint64(header[16:24], candidate.Height)
	binary.LittleEndian.PutUint64(header[24:32], candidate.Height+1)
	binary.LittleEndian.PutUint64(header[32:40], cscb.HeaderSize)
	binary.LittleEndian.PutUint64(header[40:48], uint64(len(envelope)))
	binary.LittleEndian.PutUint32(header[48:52], crc32.ChecksumIEEE(envelope))
	binary.LittleEndian.PutUint32(header[52:56], cscb.BlockIndexRecordSize)
	binary.LittleEndian.PutUint32(header[56:60], cscb.ChunkIndexRecordSize)
	binary.LittleEndian.PutUint64(header[60:68], payloadOffset)
	binary.LittleEndian.PutUint64(header[68:76], uint64(len(compressed)))
	binary.LittleEndian.PutUint64(header[76:84], uint64(len(payload)))
	binary.LittleEndian.PutUint32(header[84:88], crc32.ChecksumIEEE(payload))

	object := make([]byte, 0, len(header)+len(envelope)+len(compressed))
	object = append(object, header...)
	object = append(object, envelope...)
	object = append(object, compressed...)
	return object
}

func gzipRepairPayload(t *testing.T, payload []byte) []byte {
	t.Helper()
	var buffer bytes.Buffer
	writer := gzip.NewWriter(&buffer)
	_, err := writer.Write(payload)
	require.NoError(t, err)
	require.NoError(t, writer.Close())
	return buffer.Bytes()
}

func objectTopologyVersion(version ObjectVersion) retirement.ObjectVersion {
	return retirement.ObjectVersion{
		VersionID: version.VersionID,
		ETag:      version.ETag,
		Bytes:     version.Bytes,
		IsLatest:  true,
	}
}
