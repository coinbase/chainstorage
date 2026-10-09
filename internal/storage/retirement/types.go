package retirement

import (
	"context"
	"errors"
	"regexp"
	"time"

	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

var storageGenerationPattern = regexp.MustCompile(`^v[1-9][0-9]*$`)

func isValidStorageGeneration(generation string) bool {
	return generation == "" || storageGenerationPattern.MatchString(generation)
}

type (
	Repository interface {
		ListMetadataRows(ctx context.Context, tag uint32, startHeight uint64, endHeight uint64, limit uint64) ([]MetadataRow, error)
		GetMetadataRow(ctx context.Context, blockMetadataID int64) (MetadataRow, error)
		PrepareRetirement(ctx context.Context, manifest RetirementManifest, expectedStorageGeneration string) error
		ObserveRetentionSafety(ctx context.Context, bucket string, consolidatedObjectKey string, configurationSHA256 string) (time.Time, time.Time, error)
		ClaimRetirement(ctx context.Context, blockMetadataID int64, claimToken string, claimedAt time.Time, claimExpiresAt time.Time) error
		RenewRetirementClaim(ctx context.Context, blockMetadataID int64, claimToken string, renewedAt time.Time, claimExpiresAt time.Time) error
		RecordRetirementOutcome(ctx context.Context, blockMetadataID int64, claimToken string, outcome string, attemptedAt time.Time) error
		// ReleaseRetirementClaim expires the claim identified by claimToken
		// immediately, if it is still held, so the next attempt can take the
		// row over without waiting out the lease. A claim that has already
		// expired or belongs to another token is left untouched (no error).
		ReleaseRetirementClaim(ctx context.Context, blockMetadataID int64, claimToken string) error
		RecordRetirementObjectDeleted(ctx context.Context, blockMetadataID int64, claimToken string, outcome string) (time.Time, error)
		FinalizeRetirement(ctx context.Context, blockMetadataID int64, claimToken string, outcome string) (time.Time, error)
		ListPendingRetirements(
			ctx context.Context,
			tag uint32,
			startHeight uint64,
			endHeight uint64,
			eligibilityCutoff time.Time,
			limit uint64,
		) ([]RetirementManifest, error)
	}

	ObjectStore interface {
		InspectObjectRetentionSafety(ctx context.Context, bucket string, key string) (RetentionSafetySnapshot, error)
		HeadObject(ctx context.Context, bucket string, key string) (ObjectHead, error)
		HeadObjectVersion(ctx context.Context, bucket string, key string, versionID string) (ObjectHead, error)
		ListObjectVersions(ctx context.Context, bucket string, key string) (ObjectVersionTopology, error)
		ReadObjectVersion(ctx context.Context, bucket string, key string, versionID string) ([]byte, error)
		ReadObjectVersionRange(ctx context.Context, bucket string, key string, versionID string, offset uint64, length uint64) ([]byte, error)
		DeleteObjectVersion(ctx context.Context, bucket string, key string, versionID string) error
	}

	MetadataRow struct {
		BlockMetadataID           int64
		Canonical                 bool
		Tag                       uint32
		Height                    uint64
		Hash                      string
		Skipped                   bool
		PrimaryObjectKey          string
		PrimaryStorageGeneration  string
		SingleBlockObjectKey      string
		PrimaryObjectFormat       api.BlockObjectFormat
		PrimaryByteOffset         uint64
		PrimaryByteLength         uint64
		PrimaryUncompressedLength uint64
		RetirementFencedAt        *time.Time
		Shadow                    *ConsolidationShadow
		Retirement                *RetirementManifest
	}

	ConsolidationShadow struct {
		Tag                           uint32
		Height                        uint64
		Hash                          string
		SingleBlockObjectKey          string
		SingleBlockStorageGeneration  string
		ConsolidatedObjectKey         string
		ConsolidatedStorageGeneration string
		ObjectFormat                  api.BlockObjectFormat
		ByteOffset                    uint64
		ByteLength                    uint64
		UncompressedLength            uint64
		ValidatedAt                   *time.Time
		SingleBlockRetentionStartedAt *time.Time
		SingleBlockDeleteAfter        *time.Time
		SingleBlockObjectDeletedAt    *time.Time
		FormatVersion                 int
	}

	ObjectHead struct {
		Exists     bool
		Bytes      uint64
		VersionID  string
		ETag       string
		Metadata   map[string]string
		DeleteMark bool
		Expiration string
	}

	ObjectVersion struct {
		VersionID    string
		ETag         string
		Bytes        uint64
		IsLatest     bool
		LastModified *time.Time
	}

	ObjectDeleteMarker struct {
		VersionID    string
		IsLatest     bool
		LastModified *time.Time
	}

	ObjectVersionTopology struct {
		Versions      []ObjectVersion
		DeleteMarkers []ObjectDeleteMarker
	}

	RetentionSafetySnapshot struct {
		ConfigurationSHA256 string
	}

	RetirementManifest struct {
		BlockMetadataID                   int64
		Tag                               uint32
		Height                            uint64
		Hash                              string
		State                             string
		Bucket                            string
		SingleBlockObjectKey              string
		SingleBlockObjectKeySHA256        string
		SingleBlockObjectVersionIDs       []string
		SingleBlockDeleteMarkerVersionIDs []string
		SingleBlockObjectETag             string
		SingleBlockObjectBytes            uint64
		ConsolidatedObjectKey             string
		ConsolidatedObjectVersionID       string
		ConsolidatedObjectETag            string
		ConsolidatedByteOffset            uint64
		ConsolidatedByteLength            uint64
		ConsolidatedUncompressedLength    uint64
		PayloadSHA256                     string
		Outcome                           string
		AttemptCount                      int
		ClaimToken                        string
		ClaimExpiresAt                    *time.Time
		PreparedAt                        time.Time
		DeleteStartedAt                   *time.Time
		LastAttemptAt                     *time.Time
		DeletedAt                         *time.Time
		VerifiedAt                        *time.Time
	}

	PlanRequest struct {
		Environment                 string
		Blockchain                  string
		Network                     string
		Sidechain                   string
		Bucket                      string
		StorageGeneration           string
		Tag                         uint32
		StartHeight                 uint64
		EndHeight                   uint64
		Limit                       uint64
		Now                         time.Time
		EligibilityCutoff           time.Time
		Execute                     bool
		ProductionDeleteEnabled     bool
		DirectStorageClientsGuarded bool
		SingleBlockWritersGuarded   bool
		FallbackErrorCount          uint64
		Approval                    Approval
		// RowMemoryBudgetBytes caps the decompressed CSCB chunk working set a
		// cohort may pin. The planner lowers row parallelism until the
		// cohort's largest chunk fits. Zero applies the package default
		// rather than disabling the bound.
		RowMemoryBudgetBytes uint64
	}

	Approval struct {
		Chain                string `json:"chain"`
		StartHeight          uint64 `json:"start_height"`
		EndHeight            uint64 `json:"end_height"`
		AllowContainingRange bool   `json:"allow_containing_range,omitempty"`
	}

	Report struct {
		GeneratedAt time.Time   `json:"generated_at"`
		DryRun      bool        `json:"dry_run"`
		Environment string      `json:"environment"`
		Blockchain  string      `json:"blockchain"`
		Network     string      `json:"network"`
		Sidechain   string      `json:"sidechain,omitempty"`
		Bucket      string      `json:"bucket"`
		Tag         uint32      `json:"tag"`
		StartHeight uint64      `json:"start_height"`
		EndHeight   uint64      `json:"end_height"`
		Approval    Approval    `json:"approval"`
		SafetyGates SafetyGates `json:"safety_gates"`
		Summary     Summary     `json:"summary"`
		// QuiescenceRetryAfter is the longest remaining CSCB safety-quiescence
		// wait observed by this pass, so a deferred run can retry when the
		// quiescence window actually elapses instead of a full period later.
		QuiescenceRetryAfter time.Duration `json:"quiescence_retry_after,omitempty"`
		Items                []Candidate   `json:"items"`
	}

	SafetyGates struct {
		DirectStorageClientsGuarded bool   `json:"direct_storage_clients_guarded"`
		SingleBlockWritersGuarded   bool   `json:"single_block_writers_guarded"`
		FallbackReadErrors          uint64 `json:"fallback_read_errors"`
		VersionedDeleteMode         string `json:"versioned_delete_mode"`
		CSCBWriteOncePolicyMode     string `json:"cscb_write_once_policy_mode"`
		CSCBWriteOncePolicyVerified bool   `json:"cscb_write_once_policy_verified"`
		ProductionDeleteEnabled     bool   `json:"production_delete_enabled"`
	}

	Summary struct {
		TotalRows        int    `json:"total_rows"`
		EligibleRows     int    `json:"eligible_rows"`
		SkippedRows      int    `json:"skipped_rows"`
		DeletedRows      int    `json:"deleted_rows"`
		SingleBlockBytes uint64 `json:"single_block_bytes"`
		EligibleBytes    uint64 `json:"eligible_bytes"`
		DeleteMarkerRows int    `json:"delete_marker_rows"`
	}

	Candidate struct {
		Bucket                 string     `json:"bucket"`
		Key                    string     `json:"key"`
		VersionID              string     `json:"version_id,omitempty"`
		VersionIDs             []string   `json:"single_block_version_ids,omitempty"`
		DeleteMarkerVersionIDs []string   `json:"single_block_delete_marker_version_ids,omitempty"`
		Height                 uint64     `json:"height"`
		Hash                   string     `json:"hash"`
		SingleBlockBytes       uint64     `json:"single_block_bytes"`
		ConsolidatedKey        string     `json:"consolidated_key"`
		BlockMetadataID        int64      `json:"block_metadata_id"`
		Tag                    uint32     `json:"tag"`
		SingleBlockETag        string     `json:"single_block_etag,omitempty"`
		SingleBlockKeySHA256   string     `json:"single_block_key_sha256,omitempty"`
		SingleBlockVersions    int        `json:"single_block_version_count"`
		DeleteMarkers          int        `json:"delete_marker_count"`
		CSCBVersionID          string     `json:"cscb_version_id,omitempty"`
		CSCBETag               string     `json:"cscb_etag,omitempty"`
		CSCBWriteOncePolicy    bool       `json:"cscb_write_once_policy_verified"`
		PayloadSHA256          string     `json:"payload_sha256,omitempty"`
		ByteOffset             uint64     `json:"byte_offset,omitempty"`
		ByteLength             uint64     `json:"byte_length,omitempty"`
		UncompressedLength     uint64     `json:"uncompressed_length,omitempty"`
		RetirementState        string     `json:"retirement_state,omitempty"`
		RetirementAttempts     int        `json:"retirement_attempts,omitempty"`
		RetirementOutcome      string     `json:"retirement_outcome,omitempty"`
		ValidatedAt            *time.Time `json:"validated_at"`
		RetiredAt              *time.Time `json:"retired_at"`
		EligibleAt             *time.Time `json:"eligible_at"`
		SingleBlockDeletedAt   *time.Time `json:"single_block_deleted_at,omitempty"`
		RetirementVerifiedAt   *time.Time `json:"retirement_verified_at,omitempty"`
		Action                 string     `json:"action"`
		SkipReason             string     `json:"skip_reason"`
	}
)

const (
	ActionSkip                 = "skip"
	ActionReportOnly           = "report_only"
	ActionDeleteObjectVersion  = "delete_object_version"
	ActionDeletedObjectVersion = "deleted_object_version"
	ActionDeletedVerified      = "deleted_verified"
	ActionAlreadyDeleted       = "already_deleted"

	SkipSkippedBlock                     = "skipped_block"
	SkipMissingSingleBlockKey            = "missing_single_block_key"
	SkipMissingConsolidationShadow       = "missing_consolidation_shadow"
	SkipValidationNotPassed              = "validation_not_passed"
	SkipActiveMetadataStillSingleBlock   = "active_metadata_still_single_block"
	SkipMissingRetirementMarker          = "missing_retirement_marker"
	SkipInvalidMetadataReference         = "invalid_metadata_reference"
	SkipStorageGenerationMismatch        = "storage_generation_mismatch"
	SkipRetentionPeriodActive            = "retention_period_active"
	SkipChainRangeNotApproved            = "chain_range_not_approved"
	SkipActiveFallbackOrReadErrors       = "active_fallback_or_read_errors"
	SkipDirectStorageClientsNotGuarded   = "direct_storage_clients_not_guarded"
	SkipSingleBlockWritersNotGuarded     = "single_block_writers_not_guarded"
	SkipMissingCSCBObject                = "missing_cscb_object"
	SkipSingleBlockObjectMissing         = "single_block_object_missing"
	SkipSingleBlockCurrentDeleteMarker   = "single_block_current_delete_marker"
	SkipSingleBlockVersionIDUnavailable  = "single_block_version_id_unavailable"
	SkipUnsafeSingleBlockVersionTopology = "unsafe_single_block_version_topology"
	SkipSingleBlockPayloadMismatch       = "single_block_payload_mismatch"
	SkipMetadataChanged                  = "metadata_changed"
	SkipCSCBObjectChanged                = "cscb_object_changed"
	SkipCSCBLifecycleExpirationActive    = "cscb_lifecycle_expiration_active"
	SkipCSCBWriteOncePolicyUnverified    = "cscb_write_once_policy_not_verified"
	SkipCSCBSafetyQuiescenceActive       = "cscb_safety_quiescence_active"
	SkipPostDeleteVerificationFailed     = "post_delete_verification_failed"
	SkipRetirementVerificationPending    = "retirement_verification_pending"
	SkipRetirementAlreadyFinalized       = "retirement_already_finalized"
	SkipRetirementClaimActive            = "retirement_claim_active"
	SkipProductionDeletionDisabled       = "production_deletion_disabled"
	SkipNotAttemptedAfterFailure         = "not_attempted_after_failure"
	SkipRetentionRunCanceled             = "retention_run_canceled"
	SkipObjectInspectionFailed           = "object_inspection_failed"
	SkipVersionedDeleteFailed            = "versioned_delete_failed"

	RetirementStateEligible                   = "eligible"
	RetirementStateDeleting                   = "deleting"
	RetirementStateDeletedPendingVerification = "deleted_pending_verification"
	RetirementStateDeletedVerified            = "deleted_verified"
)

var ErrRetirementClaimUnavailable = errors.New("retirement claim unavailable")
