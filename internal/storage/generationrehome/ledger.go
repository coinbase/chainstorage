package generationrehome

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"

	"golang.org/x/xerrors"
)

type ledgerRecord struct {
	Type                 string `json:"type"`
	ObjectKey            string `json:"object_key"`
	MinHeight            uint64 `json:"min_height"`
	EndHeightExclusive   uint64 `json:"end_height_exclusive"`
	ReferencedRows       uint64 `json:"referenced_rows"`
	CanonicalRows        uint64 `json:"canonical_rows"`
	ValidPlacementRows   uint64 `json:"valid_placement_rows"`
	ConsistentShadowRows uint64 `json:"consistent_shadow_rows"`
	FencedRows           uint64 `json:"fenced_rows"`
	DeletedRetentionRows uint64 `json:"deleted_retention_rows"`
	CopyEligible         bool   `json:"copy_eligible"`
	SourceSize           uint64 `json:"source_size"`
	SourceETag           string `json:"source_etag"`
	SourceVersionID      string `json:"source_version_id"`
	DestinationSize      uint64 `json:"destination_size"`
	DestinationETag      string `json:"destination_etag"`
	DestinationVersionID string `json:"destination_version_id"`
	Verification         string `json:"verification"`
}

// LoadFencedCopyLedger reads the append-only audit/copy ledger and returns only
// objects whose consolidated placement includes retired, fenced rows.
func LoadFencedCopyLedger(reader io.Reader) ([]Object, error) {
	audits := make(map[string]ledgerRecord)
	copies := make(map[string]ledgerRecord)
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 64*1024), 4*1024*1024)
	for line := 1; scanner.Scan(); line++ {
		var record ledgerRecord
		if err := json.Unmarshal(scanner.Bytes(), &record); err != nil {
			return nil, xerrors.Errorf("invalid copy ledger line %d: %w", line, err)
		}
		switch record.Type {
		case "audit_object":
			if record.FencedRows == 0 {
				continue
			}
			if _, exists := audits[record.ObjectKey]; exists {
				return nil, xerrors.Errorf("duplicate fenced audit object %q", record.ObjectKey)
			}
			audits[record.ObjectKey] = record
		case "copy_verified":
			if existing, exists := copies[record.ObjectKey]; exists {
				if existing.SourceVersionID != record.SourceVersionID || existing.DestinationVersionID != record.DestinationVersionID {
					return nil, xerrors.Errorf("conflicting verified copies for object %q", record.ObjectKey)
				}
				continue
			}
			copies[record.ObjectKey] = record
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, xerrors.Errorf("failed to read copy ledger: %w", err)
	}
	if len(audits) == 0 {
		return nil, xerrors.New("copy ledger contains no fenced audit objects")
	}

	objects := make([]Object, 0, len(audits))
	for key, audit := range audits {
		if err := validateAuditRecord(audit); err != nil {
			return nil, xerrors.Errorf("invalid fenced audit object %q: %w", key, err)
		}
		copyRecord, ok := copies[key]
		if !ok {
			return nil, xerrors.Errorf("fenced audit object %q has no verified copy", key)
		}
		if err := validateCopyRecord(copyRecord); err != nil {
			return nil, xerrors.Errorf("invalid verified copy for object %q: %w", key, err)
		}
		object := Object{
			ObjectKey:         key,
			StartHeight:       audit.MinHeight,
			EndHeight:         audit.EndHeightExclusive,
			ExpectedRows:      audit.ReferencedRows,
			ExpectedCanonical: audit.CanonicalRows,
			ExpectedFenced:    audit.FencedRows,
			ExpectedDeleted:   audit.DeletedRetentionRows,
			Source: ObjectVersion{
				VersionID: copyRecord.SourceVersionID,
				ETag:      copyRecord.SourceETag,
				Bytes:     copyRecord.SourceSize,
			},
			Destination: ObjectVersion{
				VersionID: copyRecord.DestinationVersionID,
				ETag:      copyRecord.DestinationETag,
				Bytes:     copyRecord.DestinationSize,
			},
		}
		object.EvidenceSHA256 = evidenceSHA256(object)
		objects = append(objects, object)
	}
	sort.Slice(objects, func(i, j int) bool {
		return objects[i].ObjectKey < objects[j].ObjectKey
	})
	return objects, nil
}

func validateAuditRecord(record ledgerRecord) error {
	if strings.TrimSpace(record.ObjectKey) == "" {
		return xerrors.New("object key is required")
	}
	if record.EndHeightExclusive <= record.MinHeight {
		return xerrors.New("height range is invalid")
	}
	if record.ReferencedRows == 0 || record.FencedRows == 0 || record.FencedRows > record.ReferencedRows {
		return xerrors.New("fenced row counts are invalid")
	}
	if record.CanonicalRows > record.ReferencedRows {
		return xerrors.New("canonical row count exceeds referenced rows")
	}
	if record.ValidPlacementRows != record.ReferencedRows || record.ConsistentShadowRows != record.ReferencedRows {
		return xerrors.New("not every referenced row has a valid, consistent placement")
	}
	if record.DeletedRetentionRows != record.FencedRows {
		return xerrors.New("not every fenced row has deleted retention")
	}
	if !record.CopyEligible {
		return xerrors.New("object is not copy eligible")
	}
	return nil
}

func validateCopyRecord(record ledgerRecord) error {
	if !immutableVersionID(record.SourceVersionID) || !immutableVersionID(record.DestinationVersionID) {
		return xerrors.New("source and destination immutable version ids are required")
	}
	if record.SourceETag == "" || record.DestinationETag == "" {
		return xerrors.New("source and destination etags are required")
	}
	if record.SourceSize == 0 || record.SourceSize != record.DestinationSize {
		return xerrors.New("source and destination sizes must match and be positive")
	}
	if record.Verification == "" {
		return xerrors.New("copy verification evidence is required")
	}
	return nil
}

func evidenceSHA256(object Object) string {
	digest := sha256.New()
	_, _ = fmt.Fprintf(
		digest,
		"%d:%s\x1f%d\x1f%d\x1f%d\x1f%d\x1f%d\x1f%d\x1f%d:%s\x1f%d:%s\x1f%d\x1f%d:%s\x1f%d:%s\x1f%d\n",
		len(object.ObjectKey), object.ObjectKey,
		object.StartHeight,
		object.EndHeight,
		object.ExpectedRows,
		object.ExpectedCanonical,
		object.ExpectedFenced,
		object.ExpectedDeleted,
		len(object.Source.VersionID), object.Source.VersionID,
		len(object.Source.ETag), object.Source.ETag,
		object.Source.Bytes,
		len(object.Destination.VersionID), object.Destination.VersionID,
		len(object.Destination.ETag), object.Destination.ETag,
		object.Destination.Bytes,
	)
	return hex.EncodeToString(digest.Sum(nil))
}

func immutableVersionID(value string) bool {
	value = strings.TrimSpace(value)
	return value != "" && !strings.EqualFold(value, "null")
}
