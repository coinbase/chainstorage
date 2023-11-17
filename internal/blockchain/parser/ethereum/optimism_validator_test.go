package ethereum

import (
	"context"
	"fmt"
	"testing"

	"go.uber.org/fx"

	"github.com/coinbase/chainstorage/internal/blockchain/parser/internal"
	"github.com/coinbase/chainstorage/internal/utils/fixtures"
	"github.com/coinbase/chainstorage/internal/utils/testapp"
	"github.com/coinbase/chainstorage/internal/utils/testutil"
	"github.com/coinbase/chainstorage/protos/coinbase/c3/common"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

func TestOptimismValidator_Success(t *testing.T) {
	require := testutil.Require(t)

	var parser internal.Parser
	app := testapp.New(
		t,
		Module,
		internal.Module,
		testapp.WithBlockchainNetwork(common.Blockchain_BLOCKCHAIN_OPTIMISM, common.Network_NETWORK_OPTIMISM_MAINNET),
		fx.Populate(&parser),
	)
	defer app.Close()
	require.NotNil(parser)

	// Generate the fixture with:
	// go run ./cmd/admin block --blockchain optimism --network mainnet --env development --height 100000000 --out internal/utils/fixtures/parser/optimism/mainnet/native_block_100000000.json

	// For this test, we cover multiple types of blocks:
	// - block 10000: early block.
	// - block 100000000: pre-bedrock upgrade.
	// - block 105237730: post-bedrock upgrade with 2 transactions.
	// - block 109641760: largest block thus far with 953 transactions.

	blocks := []int{10000, 100000000, 105237730, 109860869}
	for _, b := range blocks {
		t.Log("testing optimism block", b)
		var block api.NativeBlock
		path := fmt.Sprintf("parser/optimism/mainnet/native_block_%d.json", b)

		err := fixtures.UnmarshalPB(path, &block)
		require.NoError(err)

		err = parser.ValidateBlock(context.Background(), &block)
		require.NoError(err)
	}
}
