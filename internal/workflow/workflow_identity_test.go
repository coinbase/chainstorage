package workflow

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSingleBlockRetentionWorkflowIdentity(t *testing.T) {
	identity := GetWorkflowIdentify("single_block_retention")
	require.Equal(t, SingleBlockRetentionIdentity, identity)

	name, err := identity.String()
	require.NoError(t, err)
	require.Equal(t, "workflow.single_block_retention", name)

	request, err := identity.UnmarshalJsonStringToRequest(
		`{"Tag":2,"StartHeight":100,"EndHeight":200,"MaxObjectRanges":25,"Execute":false}`,
	)
	require.NoError(t, err)
	require.Equal(t, SingleBlockRetentionRequest{
		Tag:             2,
		StartHeight:     100,
		EndHeight:       200,
		MaxObjectRanges: 25,
	}, request)
}
