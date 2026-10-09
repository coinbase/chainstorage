package bitcoin

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/fx"
	"google.golang.org/protobuf/proto"

	"github.com/coinbase/chainstorage/internal/blockchain/parser/internal"
	"github.com/coinbase/chainstorage/internal/utils/testapp"
	"github.com/coinbase/chainstorage/internal/utils/testutil"
	"github.com/coinbase/chainstorage/protos/coinbase/c3/common"
)

func newBitcoinParser(t testing.TB) (internal.NativeParser, testapp.TestApp) {
	t.Helper()
	var parser internal.NativeParser
	app := testapp.New(t,
		testapp.WithBlockchainNetwork(common.Blockchain_BLOCKCHAIN_BITCOIN, common.Network_NETWORK_BITCOIN_MAINNET),
		fx.Provide(NewBitcoinNativeParser),
		fx.Populate(&parser),
	)
	return parser, app
}

// TestBitcoinParseBlockOptions verifies that each ParseOption produces the
// expected field-level changes, and that the protobuf-serialized output
// shrinks meaningfully when options are used.
func TestBitcoinParseBlockOptions(t *testing.T) {
	require := require.New(t)

	parser, app := newBitcoinParser(t)
	defer app.Close()

	rawBlock, err := testutil.LoadRawBlock("parser/bitcoin/raw_block_731379.json")
	require.NoError(err)

	ctx := context.Background()

	base, err := parser.ParseBlock(ctx, rawBlock)
	require.NoError(err)
	require.NotNil(base)

	noScripts, err := parser.ParseBlock(ctx, rawBlock, internal.WithSkipScripts())
	require.NoError(err)

	noWitnesses, err := parser.ParseBlock(ctx, rawBlock, internal.WithSkipWitnesses())
	require.NoError(err)

	noBoth, err := parser.ParseBlock(ctx, rawBlock, internal.WithSkipScripts(), internal.WithSkipWitnesses())
	require.NoError(err)

	// Field-level assertions.
	for i, tx := range noScripts.GetBitcoin().GetTransactions() {
		require.Empty(tx.Hex, "tx[%d] Hex should be empty with WithSkipScripts", i)
		for j, in := range tx.Inputs {
			if in.ScriptSignature != nil {
				require.Empty(in.ScriptSignature.Assembly, "tx[%d].Inputs[%d].ScriptSignature.Assembly", i, j)
				require.Empty(in.ScriptSignature.Hex, "tx[%d].Inputs[%d].ScriptSignature.Hex", i, j)
			}
		}
		for j, out := range tx.Outputs {
			if out.ScriptPublicKey != nil {
				require.Empty(out.ScriptPublicKey.Assembly, "tx[%d].Outputs[%d].ScriptPublicKey.Assembly", i, j)
				require.Empty(out.ScriptPublicKey.Hex, "tx[%d].Outputs[%d].ScriptPublicKey.Hex", i, j)
			}
		}
	}

	for i, tx := range noWitnesses.GetBitcoin().GetTransactions() {
		for j, in := range tx.Inputs {
			require.Empty(in.TransactionInputWitnesses, "tx[%d].Inputs[%d].TransactionInputWitnesses", i, j)
		}
	}

	// Serialized-size comparison: the output with options dropped should be
	// meaningfully smaller than baseline.
	baseSize := proto.Size(base)
	noScriptsSize := proto.Size(noScripts)
	noWitnessesSize := proto.Size(noWitnesses)
	noBothSize := proto.Size(noBoth)

	t.Logf("serialized NativeBlock sizes (bytes): baseline=%d skip_scripts=%d (-%d%%) skip_witnesses=%d (-%d%%) skip_both=%d (-%d%%)",
		baseSize,
		noScriptsSize, pct(baseSize, noScriptsSize),
		noWitnessesSize, pct(baseSize, noWitnessesSize),
		noBothSize, pct(baseSize, noBothSize),
	)

	require.Less(noScriptsSize, baseSize, "WithSkipScripts should shrink serialized NativeBlock")
	require.Less(noWitnessesSize, baseSize, "WithSkipWitnesses should shrink serialized NativeBlock")
	require.LessOrEqual(noBothSize, noScriptsSize)
	require.LessOrEqual(noBothSize, noWitnessesSize)

	// Other fields stay intact.
	require.Equal(base.Hash, noScripts.Hash)
	require.Equal(base.Height, noScripts.Height)
	require.Equal(len(base.GetBitcoin().GetTransactions()), len(noScripts.GetBitcoin().GetTransactions()))

	// Address derivation still works when scripts are skipped: any tx output
	// whose baseline carries a non-empty address should still have it with
	// SkipScripts, because Assembly is preserved just long enough for the
	// pubkey-script derivation path.
	for i, tx := range base.GetBitcoin().GetTransactions() {
		for j, out := range tx.Outputs {
			baseAddr := out.GetScriptPublicKey().GetAddress()
			skipAddr := noScripts.GetBitcoin().GetTransactions()[i].Outputs[j].GetScriptPublicKey().GetAddress()
			require.Equal(baseAddr, skipAddr, "tx[%d].Outputs[%d] address should survive SkipScripts", i, j)
		}
	}
}

func pct(base, cur int) int {
	if base <= 0 {
		return 0
	}
	return (base - cur) * 100 / base
}
