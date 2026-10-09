package generationrehome

import (
	"context"
	"time"
)

type (
	ObjectVersion struct {
		VersionID string `json:"version_id"`
		ETag      string `json:"etag"`
		Bytes     uint64 `json:"bytes"`
	}

	Object struct {
		ObjectKey         string        `json:"object_key"`
		StartHeight       uint64        `json:"start_height"`
		EndHeight         uint64        `json:"end_height"`
		ExpectedRows      uint64        `json:"expected_rows"`
		ExpectedCanonical uint64        `json:"expected_canonical_rows"`
		ExpectedFenced    uint64        `json:"expected_fenced_rows"`
		ExpectedDeleted   uint64        `json:"expected_deleted_verified_rows"`
		Source            ObjectVersion `json:"source"`
		Destination       ObjectVersion `json:"destination"`
		EvidenceSHA256    string        `json:"evidence_sha256"`
	}

	Inspection struct {
		TotalRows              uint64
		CanonicalRows          uint64
		StartHeight            uint64
		EndHeight              uint64
		MissingShadowRows      uint64
		InvalidPlacementRows   uint64
		FencedRows             uint64
		DeletedVerifiedRows    uint64
		ActiveRetentionRows    uint64
		PinnedRepairRows       uint64
		PrimaryLegacyRows      uint64
		PrimaryTargetRows      uint64
		PrimaryOtherRows       uint64
		ConsolidatedLegacyRows uint64
		ConsolidatedTargetRows uint64
		ConsolidatedOtherRows  uint64
		CompletedAudit         bool
	}

	ObjectHead struct {
		Exists    bool
		VersionID string
		ETag      string
		Bytes     uint64
	}

	Request struct {
		Environment       string
		Chain             string
		Tag               uint32
		SourceBucket      string
		DestinationBucket string
		TargetGeneration  string
		Execute           bool
		Approval          Approval
		Progress          func(completed int, total int, item ReportItem)
	}

	Approval struct {
		Chain      string
		Objects    uint64
		Rows       uint64
		Transition string
	}

	Report struct {
		GeneratedAt       time.Time    `json:"generated_at"`
		DryRun            bool         `json:"dry_run"`
		Environment       string       `json:"environment"`
		Chain             string       `json:"chain"`
		Tag               uint32       `json:"tag"`
		SourceBucket      string       `json:"source_bucket"`
		DestinationBucket string       `json:"destination_bucket"`
		Transition        string       `json:"transition"`
		Summary           Summary      `json:"summary"`
		Items             []ReportItem `json:"items"`
	}

	Summary struct {
		Objects          uint64 `json:"objects"`
		Rows             uint64 `json:"rows"`
		FencedRows       uint64 `json:"fenced_rows"`
		ReadyObjects     uint64 `json:"ready_objects"`
		CompletedObjects uint64 `json:"completed_objects"`
		AlreadyTarget    uint64 `json:"already_target_objects"`
		FailedObjects    uint64 `json:"failed_objects"`
	}

	ReportItem struct {
		ObjectKey      string `json:"object_key"`
		EvidenceSHA256 string `json:"evidence_sha256"`
		StartHeight    uint64 `json:"start_height"`
		EndHeight      uint64 `json:"end_height"`
		Rows           uint64 `json:"rows"`
		FencedRows     uint64 `json:"fenced_rows"`
		Action         string `json:"action"`
		Error          string `json:"error,omitempty"`
	}

	Repository interface {
		Inspect(ctx context.Context, tag uint32, objectKey string, targetGeneration string, evidenceSHA256 string) (Inspection, error)
		Rehome(ctx context.Context, req RehomeRequest) (alreadyTarget bool, err error)
	}

	ObjectStore interface {
		InspectObjectRetentionSafety(ctx context.Context, bucket string, key string) error
		HeadObject(ctx context.Context, bucket string, key string) (ObjectHead, error)
		HeadObjectVersion(ctx context.Context, bucket string, key string, versionID string) (ObjectHead, error)
	}

	RehomeRequest struct {
		Tag               uint32
		SourceBucket      string
		DestinationBucket string
		TargetGeneration  string
		Object            Object
	}
)

const (
	TransitionLegacyNullToV2 = "legacy-null-to-v2"
	ActionReady              = "ready"
	ActionRehome             = "rehome"
	ActionRehomed            = "rehomed"
	ActionAlreadyTarget      = "already_target"
	ActionFailed             = "failed"
)
