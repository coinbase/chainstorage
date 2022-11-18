package fixtures

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGetJson(t *testing.T) {
	_, err := ReadFile("parser/bitcoin/get_block.json")
	require.NoError(t, err)
}
