package generationrehome

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLoadFencedCopyLedger(t *testing.T) {
	ledger := strings.Join([]string{
		`{"type":"audit_object","object_key":"simple","min_height":1,"end_height_exclusive":2,"referenced_rows":1,"canonical_rows":1,"valid_placement_rows":1,"consistent_shadow_rows":1,"copy_eligible":true,"simple_cutover_eligible":true}`,
		`{"type":"audit_object","object_key":"fenced","min_height":10,"end_height_exclusive":20,"referenced_rows":8,"canonical_rows":7,"valid_placement_rows":8,"consistent_shadow_rows":8,"fenced_rows":3,"deleted_retention_rows":3,"copy_eligible":true}`,
		`{"type":"copy_verified","object_key":"fenced","source_size":100,"source_etag":"source-etag","source_version_id":"source-version","destination_size":100,"destination_etag":"destination-etag","destination_version_id":"destination-version","verification":"complete"}`,
	}, "\n")

	objects, err := LoadFencedCopyLedger(strings.NewReader(ledger))
	require.NoError(t, err)
	require.Len(t, objects, 1)
	object := objects[0]
	require.Equal(t, "fenced", object.ObjectKey)
	require.Equal(t, uint64(8), object.ExpectedRows)
	require.Equal(t, uint64(3), object.ExpectedFenced)
	require.Equal(t, uint64(100), object.Destination.Bytes)
	require.Len(t, object.EvidenceSHA256, 64)
	require.Equal(t, evidenceSHA256(object), object.EvidenceSHA256)
}

func TestLoadFencedCopyLedgerRejectsMissingCopy(t *testing.T) {
	ledger := `{"type":"audit_object","object_key":"fenced","min_height":10,"end_height_exclusive":20,"referenced_rows":8,"canonical_rows":8,"valid_placement_rows":8,"consistent_shadow_rows":8,"fenced_rows":8,"deleted_retention_rows":8,"copy_eligible":true}`

	_, err := LoadFencedCopyLedger(strings.NewReader(ledger))
	require.ErrorContains(t, err, "has no verified copy")
}

func TestLoadFencedCopyLedgerRejectsIncompleteRetirement(t *testing.T) {
	ledger := strings.Join([]string{
		`{"type":"audit_object","object_key":"fenced","min_height":10,"end_height_exclusive":20,"referenced_rows":8,"canonical_rows":8,"valid_placement_rows":8,"consistent_shadow_rows":8,"fenced_rows":8,"deleted_retention_rows":7,"copy_eligible":true}`,
		`{"type":"copy_verified","object_key":"fenced","source_size":100,"source_etag":"source-etag","source_version_id":"source-version","destination_size":100,"destination_etag":"destination-etag","destination_version_id":"destination-version","verification":"complete"}`,
	}, "\n")

	_, err := LoadFencedCopyLedger(strings.NewReader(ledger))
	require.ErrorContains(t, err, "not every fenced row has deleted retention")
}
