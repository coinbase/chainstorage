package cscbrepair

import (
	"context"
	"time"

	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

type (
	State string

	Progress func(stage string, completed int, total int, height uint64)

	ObjectVersion struct {
		VersionID string
		ETag      string
		Bytes     uint64
	}

	PinnedPayload struct {
		BlockMetadataID int64
		Metadata        *api.BlockMetadata
		RawBlockPayload []byte
	}

	RebuiltPlacement struct {
		BlockMetadataID    int64
		Height             uint64
		Hash               string
		ObjectFormat       api.BlockObjectFormat
		StorageGeneration  string
		ByteOffset         uint64
		ByteLength         uint64
		UncompressedLength uint64
	}

	Block struct {
		BlockMetadataID               int64
		Canonical                     bool
		Tag                           uint32
		Height                        uint64
		Hash                          string
		Skipped                       bool
		RetirementFenced              bool
		RetirementManifestExists      bool
		StorageGeneration             string
		SingleBlockStorageGeneration  string
		ConsolidatedStorageGeneration string
		SingleBlockObjectKey          string
		SingleBlockObjectKeySHA256    string
		SingleBlockObjectDeleted      bool
		SingleBlockObjectVersion      ObjectVersion
		PayloadSHA256                 string
		OldConsolidatedObjectKey      string
		OldByteOffset                 uint64
		OldByteLength                 uint64
		OldUncompressedLength         uint64
		ActiveObjectKey               string
		ActiveObjectFormat            int32
		NewConsolidatedObjectKey      string
		NewByteOffset                 uint64
		NewByteLength                 uint64
		NewUncompressedLength         uint64
		NewValidatedAt                *time.Time
		NewRetentionStartedAt         *time.Time
		NewSingleBlockDeleteAfter     *time.Time
	}

	Manifest struct {
		ID                           int64
		Tag                          uint32
		State                        State
		Bucket                       string
		OldConsolidatedObjectKey     string
		OldConsolidatedObjectVersion ObjectVersion
		StartHeight                  uint64
		EndHeight                    uint64
		CanonicalBlockCount          uint64
		TotalBlockCount              uint64
		RowSetSHA256                 string
		NewConsolidatedObjectKey     string
		NewConsolidatedObjectVersion ObjectVersion
		Outcome                      string
		PreparedAt                   time.Time
		RestoredAt                   *time.Time
		VerifiedAt                   *time.Time
		CompletedAt                  *time.Time
		Blocks                       []Block
	}

	Repository interface {
		FindByExecutionKey(ctx context.Context, executionKey string) (*Manifest, bool, error)
		FindPending(ctx context.Context, tag uint32, startHeight uint64, endHeight uint64) (*Manifest, error)
		FindByObjectKey(ctx context.Context, tag uint32, objectKey string) (*Manifest, error)
		FindNextCandidate(ctx context.Context, tag uint32, startHeight uint64, endHeight uint64) (*Manifest, error)
		FindCandidateByObjectKey(ctx context.Context, tag uint32, objectKey string) (*Manifest, error)
		ListCandidateObjectKeys(ctx context.Context, tag uint32, startHeight uint64, endHeight uint64, limit int) ([]string, error)
		FenceCandidate(ctx context.Context, manifest *Manifest) (*Manifest, error)
		RecordInspection(ctx context.Context, repairID int64, oldObject ObjectVersion, blocks []Block, alreadyClean bool) (*Manifest, error)
		BindExecutionKey(ctx context.Context, executionKey string, repairID int64) (*Manifest, error)
		BindNoCandidateExecution(ctx context.Context, executionKey string, tag uint32, startHeight uint64, endHeight uint64) (*Manifest, error)
		Get(ctx context.Context, repairID int64) (*Manifest, error)
		RestoreToSingleBlock(ctx context.Context, repairID int64) (*Manifest, error)
		GetRebuilt(ctx context.Context, repairID int64) (*Manifest, error)
		RecordVerified(ctx context.Context, repairID int64, objectKey string, object ObjectVersion) (*Manifest, error)
		PromoteCompleted(ctx context.Context, repairID int64, objectKey string, object ObjectVersion, placements []RebuiltPlacement, singleBlockObjectRetention time.Duration) (*Manifest, error)
		CompleteRetainingOldObject(ctx context.Context, repairID int64, outcome string) (*Manifest, error)
	}

	Repairer interface {
		ListCandidates(ctx context.Context, tag uint32, startHeight uint64, endHeight uint64, limit int) ([]string, error)
		PrepareNext(ctx context.Context, executionKey string, tag uint32, startHeight uint64, endHeight uint64, maxBlocks uint64, progress Progress) (*Manifest, error)
		PrepareObject(ctx context.Context, executionKey string, tag uint32, startHeight uint64, endHeight uint64, maxBlocks uint64, objectKey string, progress Progress) (*Manifest, error)
		Restore(ctx context.Context, repairID int64, progress Progress) (*Manifest, error)
		Get(ctx context.Context, repairID int64) (*Manifest, error)
		VerifyRebuilt(ctx context.Context, repairID int64, progress Progress) (*Manifest, error)
		VisitPinnedPayloads(ctx context.Context, repairID int64, progress Progress, visit func(PinnedPayload) error) error
		VerifyAndPromote(ctx context.Context, repairID int64, objectKey string, placements []RebuiltPlacement, singleBlockObjectRetention time.Duration, progress Progress) (*Manifest, error)
		Complete(ctx context.Context, repairID int64, progress Progress) (*Manifest, error)
	}
)

const (
	StatePreparing State = "preparing"
	StatePrepared  State = "prepared"
	StateRestored  State = "restored"
	StateVerified  State = "verified"
	StateCompleted State = "completed"

	OutcomeAlreadyCleanStorageNeutral = "already_clean_storage_neutral"
)

func reportProgress(progress Progress, stage string, completed int, total int, height uint64) {
	if progress != nil {
		progress(stage, completed, total, height)
	}
}
