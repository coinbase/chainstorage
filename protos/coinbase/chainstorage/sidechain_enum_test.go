package chainstorage

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSidechainGetName(t *testing.T) {
	require := require.New(t)

	require.Equal("ethereum-mainnet-beacon", SideChain_SIDECHAIN_ETHEREUM_MAINNET_BEACON.GetName())
	require.Equal("ethereum-holesky-beacon", SideChain_SIDECHAIN_ETHEREUM_HOLESKY_BEACON.GetName())
	require.Equal("none", SideChain_SIDECHAIN_NONE.GetName())
}
