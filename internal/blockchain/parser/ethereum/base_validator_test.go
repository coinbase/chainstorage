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

func TestBaseValidator_Success(t *testing.T) {
	require := testutil.Require(t)

	var parser internal.Parser
	app := testapp.New(
		t,
		Module,
		internal.Module,
		testapp.WithBlockchainNetwork(common.Blockchain_BLOCKCHAIN_BASE, common.Network_NETWORK_BASE_MAINNET),
		fx.Populate(&parser),
	)
	defer app.Close()
	require.NotNil(parser)

	// Generate the fixture with:
	// go run ./cmd/admin block --blockchain base --network mainnet --env development --height 100 --out internal/utils/fixtures/parser/base/mainnet/native_block_100.json

	// For this test, we cover multiple types of blocks:
	// 1. block 100: block with a single transaction.
	// 1. block 77976: pre-bedrock block with 2 transactions.
	// 2. block 2000000: post-bedrock block with 15 transactions.
	// 3. block 4051331: largest block thus far with 984 transactions.

	blocks := []int{100, 77976, 2000000, 4051331}
	for _, b := range blocks {
		t.Log("testing base block", b)
		var block api.NativeBlock
		path := fmt.Sprintf("parser/base/mainnet/native_block_%d.json", b)

		err := fixtures.UnmarshalPB(path, &block)
		require.NoError(err)

		err = parser.ValidateBlock(context.Background(), &block)
		require.NoError(err)
	}
}

func TestBaseGoerliValidator_Success(t *testing.T) {
	require := testutil.Require(t)

	var parser internal.Parser
	app := testapp.New(
		t,
		Module,
		internal.Module,
		testapp.WithBlockchainNetwork(common.Blockchain_BLOCKCHAIN_BASE, common.Network_NETWORK_BASE_GOERLI),
		fx.Populate(&parser),
	)
	defer app.Close()
	require.NotNil(parser)

	// Generate the fixture with:
	// go run ./cmd/admin block --blockchain base --network goerli --env development --height 100 --out internal/utils/fixtures/parser/base/goerli/native_block_100.json

	// For this test, we cover multiple types of blocks:
	// 1. block 100: pre-regolith upgrade: a single *system* transaction.
	// 2. block 10000000: post regolith upgrade: isSystemTx is deprecated.

	blocks := []int{100, 10000000}
	for _, b := range blocks {
		t.Log("testing base block", b)
		var block api.NativeBlock
		path := fmt.Sprintf("parser/base/goerli/native_block_%d.json", b)

		err := fixtures.UnmarshalPB(path, &block)
		require.NoError(err)

		err = parser.ValidateBlock(context.Background(), &block)
		require.NoError(err)
	}
}
