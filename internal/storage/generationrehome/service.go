package generationrehome

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"regexp"
	"time"

	"golang.org/x/xerrors"
)

var generationPattern = regexp.MustCompile(`^v[1-9][0-9]*$`)

type Service struct {
	repository Repository
	store      ObjectStore
	now        func() time.Time
}

func NewService(repository Repository, store ObjectStore) *Service {
	return &Service{
		repository: repository,
		store:      store,
		now:        time.Now,
	}
}

func (s *Service) Run(ctx context.Context, req Request, objects []Object) (*Report, error) {
	report := newReport(req, objects, s.now().UTC())
	if err := validateRequest(req, objects); err != nil {
		return report, err
	}

	for _, object := range objects {
		item := ReportItem{
			ObjectKey:      object.ObjectKey,
			EvidenceSHA256: object.EvidenceSHA256,
			StartHeight:    object.StartHeight,
			EndHeight:      object.EndHeight,
			Rows:           object.ExpectedRows,
			FencedRows:     object.ExpectedFenced,
		}
		if err := s.verifyObjectCopy(ctx, req, object); err != nil {
			item.Action = ActionFailed
			item.Error = err.Error()
			report.Items = append(report.Items, item)
			report.Summary.FailedObjects++
			reportProgress(req, report, item, len(objects))
			return report, xerrors.Errorf("object copy verification failed for %q: %w", object.ObjectKey, err)
		}

		inspection, err := s.repository.Inspect(ctx, req.Tag, object.ObjectKey, req.TargetGeneration, object.EvidenceSHA256)
		if err != nil {
			item.Action = ActionFailed
			item.Error = err.Error()
			report.Items = append(report.Items, item)
			report.Summary.FailedObjects++
			reportProgress(req, report, item, len(objects))
			return report, xerrors.Errorf("metadata inspection failed for %q: %w", object.ObjectKey, err)
		}
		alreadyTarget, err := validateInspection(object, inspection)
		if err != nil {
			item.Action = ActionFailed
			item.Error = err.Error()
			report.Items = append(report.Items, item)
			report.Summary.FailedObjects++
			reportProgress(req, report, item, len(objects))
			return report, xerrors.Errorf("metadata inspection rejected object %q: %w", object.ObjectKey, err)
		}
		if alreadyTarget {
			item.Action = ActionAlreadyTarget
			report.Summary.AlreadyTarget++
			report.Items = append(report.Items, item)
			reportProgress(req, report, item, len(objects))
			continue
		}

		item.Action = ActionReady
		report.Summary.ReadyObjects++
		if !req.Execute {
			report.Items = append(report.Items, item)
			reportProgress(req, report, item, len(objects))
			continue
		}

		item.Action = ActionRehome
		alreadyTarget, err = s.repository.Rehome(ctx, RehomeRequest{
			Tag:               req.Tag,
			SourceBucket:      req.SourceBucket,
			DestinationBucket: req.DestinationBucket,
			TargetGeneration:  req.TargetGeneration,
			Object:            object,
		})
		if err != nil {
			item.Action = ActionFailed
			item.Error = err.Error()
			report.Items = append(report.Items, item)
			report.Summary.FailedObjects++
			reportProgress(req, report, item, len(objects))
			return report, xerrors.Errorf("metadata rehome failed for %q: %w", object.ObjectKey, err)
		}
		if alreadyTarget {
			item.Action = ActionAlreadyTarget
			report.Summary.AlreadyTarget++
		} else {
			item.Action = ActionRehomed
			report.Summary.CompletedObjects++
		}
		report.Items = append(report.Items, item)
		reportProgress(req, report, item, len(objects))
	}
	return report, nil
}

func reportProgress(req Request, report *Report, item ReportItem, total int) {
	if req.Progress != nil {
		req.Progress(len(report.Items), total, item)
	}
}

func validateRequest(req Request, objects []Object) error {
	if req.Chain == "" || req.SourceBucket == "" || req.DestinationBucket == "" {
		return xerrors.New("chain, source bucket, and destination bucket are required")
	}
	if req.SourceBucket == req.DestinationBucket {
		return xerrors.New("source and destination buckets must be different")
	}
	if !generationPattern.MatchString(req.TargetGeneration) {
		return xerrors.Errorf("unsupported target generation %q", req.TargetGeneration)
	}
	if req.TargetGeneration != "v2" {
		return xerrors.Errorf("fenced legacy rehome is restricted to target generation v2, got %q", req.TargetGeneration)
	}
	if len(objects) == 0 {
		return xerrors.New("at least one fenced object is required")
	}

	var rows uint64
	seen := make(map[string]struct{}, len(objects))
	for _, object := range objects {
		if object.ObjectKey == "" || object.EvidenceSHA256 == "" {
			return xerrors.New("every object requires a key and evidence digest")
		}
		if _, ok := seen[object.ObjectKey]; ok {
			return xerrors.Errorf("duplicate object %q", object.ObjectKey)
		}
		seen[object.ObjectKey] = struct{}{}
		if object.EvidenceSHA256 != evidenceSHA256(object) {
			return xerrors.Errorf("evidence digest mismatch for object %q", object.ObjectKey)
		}
		if ^uint64(0)-rows < object.ExpectedRows {
			return xerrors.New("approved row count overflow")
		}
		rows += object.ExpectedRows
	}
	if req.Execute {
		if req.Approval.Chain != req.Chain {
			return xerrors.Errorf("approval chain mismatch: approved=%q actual=%q", req.Approval.Chain, req.Chain)
		}
		if req.Approval.Objects != uint64(len(objects)) {
			return xerrors.Errorf("approval object count mismatch: approved=%d actual=%d", req.Approval.Objects, len(objects))
		}
		if req.Approval.Rows != rows {
			return xerrors.Errorf("approval row count mismatch: approved=%d actual=%d", req.Approval.Rows, rows)
		}
		if req.Approval.Transition != TransitionLegacyNullToV2 {
			return xerrors.Errorf("approval transition mismatch: approved=%q required=%q", req.Approval.Transition, TransitionLegacyNullToV2)
		}
	}
	return nil
}

func validateInspection(object Object, inspection Inspection) (bool, error) {
	if inspection.TotalRows != object.ExpectedRows ||
		inspection.CanonicalRows != object.ExpectedCanonical ||
		inspection.StartHeight != object.StartHeight ||
		inspection.EndHeight != object.EndHeight {
		return false, xerrors.Errorf(
			"row cohort changed: rows=%d/%d canonical=%d/%d range=[%d,%d)/[%d,%d)",
			inspection.TotalRows, object.ExpectedRows,
			inspection.CanonicalRows, object.ExpectedCanonical,
			inspection.StartHeight, inspection.EndHeight,
			object.StartHeight, object.EndHeight,
		)
	}
	if inspection.MissingShadowRows != 0 || inspection.InvalidPlacementRows != 0 {
		return false, xerrors.Errorf("invalid placement cohort: missing_shadow=%d invalid_placement=%d", inspection.MissingShadowRows, inspection.InvalidPlacementRows)
	}
	if inspection.FencedRows != object.ExpectedFenced || inspection.DeletedVerifiedRows != object.ExpectedDeleted {
		return false, xerrors.Errorf(
			"retirement cohort changed: fenced=%d/%d deleted_verified=%d/%d",
			inspection.FencedRows, object.ExpectedFenced,
			inspection.DeletedVerifiedRows, object.ExpectedDeleted,
		)
	}
	if inspection.ActiveRetentionRows != 0 || inspection.PinnedRepairRows != 0 {
		return false, xerrors.Errorf("active safety work exists: retention_rows=%d pinned_repair_rows=%d", inspection.ActiveRetentionRows, inspection.PinnedRepairRows)
	}

	allLegacy := inspection.PrimaryLegacyRows == inspection.TotalRows &&
		inspection.ConsolidatedLegacyRows == inspection.TotalRows &&
		inspection.PrimaryTargetRows == 0 && inspection.ConsolidatedTargetRows == 0 &&
		inspection.PrimaryOtherRows == 0 && inspection.ConsolidatedOtherRows == 0
	allTarget := inspection.PrimaryTargetRows == inspection.TotalRows &&
		inspection.ConsolidatedTargetRows == inspection.TotalRows &&
		inspection.PrimaryLegacyRows == 0 && inspection.ConsolidatedLegacyRows == 0 &&
		inspection.PrimaryOtherRows == 0 && inspection.ConsolidatedOtherRows == 0
	if allLegacy {
		return false, nil
	}
	if allTarget {
		if !inspection.CompletedAudit {
			return false, xerrors.New("target generation metadata has no matching completed rehome audit")
		}
		return true, nil
	}
	return false, xerrors.Errorf(
		"storage generation cohort is mixed: primary_legacy=%d primary_target=%d primary_other=%d shadow_legacy=%d shadow_target=%d shadow_other=%d",
		inspection.PrimaryLegacyRows,
		inspection.PrimaryTargetRows,
		inspection.PrimaryOtherRows,
		inspection.ConsolidatedLegacyRows,
		inspection.ConsolidatedTargetRows,
		inspection.ConsolidatedOtherRows,
	)
}

func (s *Service) verifyObjectCopy(ctx context.Context, req Request, object Object) error {
	checks := []struct {
		name    string
		bucket  string
		version ObjectVersion
		current bool
	}{
		{name: "source current", bucket: req.SourceBucket, version: object.Source, current: true},
		{name: "source pinned", bucket: req.SourceBucket, version: object.Source},
		{name: "destination current", bucket: req.DestinationBucket, version: object.Destination, current: true},
		{name: "destination pinned", bucket: req.DestinationBucket, version: object.Destination},
	}
	for _, check := range checks {
		var head ObjectHead
		var err error
		if check.current {
			head, err = s.store.HeadObject(ctx, check.bucket, object.ObjectKey)
		} else {
			head, err = s.store.HeadObjectVersion(ctx, check.bucket, object.ObjectKey, check.version.VersionID)
		}
		if err != nil {
			return xerrors.Errorf("%s head failed: %w", check.name, err)
		}
		if !head.Exists {
			return xerrors.Errorf("%s object is missing", check.name)
		}
		if head.VersionID != check.version.VersionID || head.ETag != check.version.ETag || head.Bytes != check.version.Bytes {
			return xerrors.Errorf(
				"%s evidence mismatch: version=%q/%q etag=%q/%q bytes=%d/%d",
				check.name,
				head.VersionID, check.version.VersionID,
				head.ETag, check.version.ETag,
				head.Bytes, check.version.Bytes,
			)
		}
	}
	if err := s.store.InspectObjectRetentionSafety(ctx, req.DestinationBucket, object.ObjectKey); err != nil {
		return xerrors.Errorf("destination retention safety verification failed: %w", err)
	}
	return nil
}

func newReport(req Request, objects []Object, generatedAt time.Time) *Report {
	report := &Report{
		GeneratedAt:       generatedAt,
		DryRun:            !req.Execute,
		Environment:       req.Environment,
		Chain:             req.Chain,
		Tag:               req.Tag,
		SourceBucket:      req.SourceBucket,
		DestinationBucket: req.DestinationBucket,
		Transition:        TransitionLegacyNullToV2,
		Items:             make([]ReportItem, 0, len(objects)),
	}
	for _, object := range objects {
		report.Summary.Objects++
		report.Summary.Rows += object.ExpectedRows
		report.Summary.FencedRows += object.ExpectedFenced
	}
	return report
}

func WriteReportJSON(writer io.Writer, report *Report) error {
	encoder := json.NewEncoder(writer)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(report); err != nil {
		return fmt.Errorf("failed to encode generation rehome report: %w", err)
	}
	return nil
}
