package chainstorage_test

import (
	"bytes"
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/coinbase/chainstorage/internal/utils/fixtures"
	"github.com/coinbase/chainstorage/protos/coinbase/c3/common"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

// TestWalkBitcoinEnvelope_Parity walks a hand-built block and asserts
// that Header and InputTransactions are exposed via chunk offsets
// (not materialized) while the rest of the block matches proto.Marshal
// round-trip behavior.
func TestWalkBitcoinEnvelope_Parity(t *testing.T) {
	require := require.New(t)

	original := &api.Block{
		Blockchain: common.Blockchain_BLOCKCHAIN_BITCOIN,
		Network:    common.Network_NETWORK_BITCOIN_MAINNET,
		Metadata: &api.BlockMetadata{
			Tag:    1,
			Height: 696402,
			Hash:   "000000000000000000088a771bf9592a8bd3e9a5dc4c5a18876b65b283f0fb1e",
		},
		Blobdata: &api.Block_Bitcoin{
			Bitcoin: &api.BitcoinBlobdata{
				Header: []byte(`{"hash":"000000","height":696402,"tx":[{"txid":"abc"}]}`),
				InputTransactions: []*api.RepeatedBytes{
					{Data: [][]byte{[]byte(`{"txid":"prev1"}`), []byte(`{"txid":"prev2"}`)}},
					{Data: [][]byte{[]byte(`{"txid":"prev3"}`)}},
				},
				RawInputTransactions: map[string][]byte{
					"tx1": []byte("raw1"),
				},
			},
		},
	}

	raw, err := proto.Marshal(original)
	require.NoError(err)

	block, chunks, err := api.WalkBitcoinEnvelope(bytes.NewReader(raw))
	require.NoError(err)

	// Top-level fields decode normally.
	require.Equal(original.Blockchain, block.Blockchain)
	require.Equal(original.Network, block.Network)
	require.Equal(original.Metadata.Tag, block.Metadata.Tag)
	require.Equal(original.Metadata.Hash, block.Metadata.Hash)

	// Blob: Header, InputTransactions, AND RawInputTransactions are
	// all skipped by the walker — each is exposed lazily via a chunk
	// ref or can be recomputed from InputTransactions. Skipping
	// RawInputTransactions avoids doubling peak RAM on chains that
	// duplicate data between the two fields (e.g. zcash).
	blob := block.GetBitcoin()
	require.NotNil(blob)
	require.Nil(blob.Header, "header must be left nil — walker exposes it via chunks.Header")
	require.Nil(blob.InputTransactions, "input_transactions must be left nil — walker exposes it via chunks.InputTransactionsGroups")
	require.Nil(blob.RawInputTransactions, "raw_input_transactions must be left nil — walker skips it (currently unused by the streaming parser)")

	// Header chunk: offset+length point at the header bytes.
	require.Equal(int64(len(original.GetBitcoin().Header)), chunks.Header.Length)
	require.Equal(original.GetBitcoin().Header, raw[chunks.Header.Offset:chunks.Header.Offset+chunks.Header.Length])

	// Each InputTransactions group is a separate chunk reference. The
	// bytes at its offset+length proto.Unmarshal to the original
	// RepeatedBytes.
	require.Equal(len(original.GetBitcoin().InputTransactions), len(chunks.InputTransactionsGroups))
	for i, ref := range chunks.InputTransactionsGroups {
		got := &api.RepeatedBytes{}
		require.NoError(proto.Unmarshal(raw[ref.Offset:ref.Offset+ref.Length], got))
		require.Equal(original.GetBitcoin().InputTransactions[i].Data, got.Data, "group %d mismatch", i)
	}
}

// TestWalkBitcoinEnvelope_Fixture walks a real bitcoin block fixture.
func TestWalkBitcoinEnvelope_Fixture(t *testing.T) {
	require := require.New(t)

	header, err := fixtures.ReadFile("parser/bitcoin/get_block.json")
	require.NoError(err)
	tx1, err := fixtures.ReadFile("parser/bitcoin/get_raw_transaction.json")
	require.NoError(err)
	tx2, err := fixtures.ReadFile("parser/bitcoin/get_raw_transaction_tx2.json")
	require.NoError(err)

	original := &api.Block{
		Blockchain: common.Blockchain_BLOCKCHAIN_BITCOIN,
		Network:    common.Network_NETWORK_BITCOIN_MAINNET,
		Metadata:   &api.BlockMetadata{Tag: 1, Height: 696402},
		Blobdata: &api.Block_Bitcoin{
			Bitcoin: &api.BitcoinBlobdata{
				Header: header,
				InputTransactions: []*api.RepeatedBytes{
					{Data: [][]byte{}},
					{Data: [][]byte{tx1, tx2}},
				},
			},
		},
	}
	raw, err := proto.Marshal(original)
	require.NoError(err)

	block, chunks, err := api.WalkBitcoinEnvelope(bytes.NewReader(raw))
	require.NoError(err)
	require.Equal(int64(len(header)), chunks.Header.Length)
	require.Equal(header, raw[chunks.Header.Offset:chunks.Header.Offset+chunks.Header.Length])
	require.Nil(block.GetBitcoin().Header)
	require.Nil(block.GetBitcoin().InputTransactions)
	require.Equal(2, len(chunks.InputTransactionsGroups))

	// Recover the 2nd group via its chunk ref.
	got := &api.RepeatedBytes{}
	ref := chunks.InputTransactionsGroups[1]
	require.NoError(proto.Unmarshal(raw[ref.Offset:ref.Offset+ref.Length], got))
	require.Equal([][]byte{tx1, tx2}, got.Data)
}

// TestWalkBitcoinEnvelope_UnknownBlockField ensures the walker errors
// on a field number not registered in its known set.
func TestWalkBitcoinEnvelope_UnknownBlockField(t *testing.T) {
	require := require.New(t)

	// tag = (77 << 3) | 0 = 616 → varint 0xE8, 0x04; value = 0x2A (42)
	buf := []byte{0xE8, 0x04, 0x2A}
	_, _, err := api.WalkBitcoinEnvelope(bytes.NewReader(buf))
	require.Error(err)
	require.Contains(err.Error(), "unknown Block field 77")
}

// TestWalkBitcoinEnvelope_UnknownBitcoinBlobField checks the same at
// the BitcoinBlobdata level.
func TestWalkBitcoinEnvelope_UnknownBitcoinBlobField(t *testing.T) {
	require := require.New(t)

	// Outer tag (field 101, wiretype 2) = 0xAA 0x06; length=3;
	// inner: field 99 varint 1 = 0x98 0x06 0x01
	buf := []byte{0xAA, 0x06, 0x03, 0x98, 0x06, 0x01}
	_, _, err := api.WalkBitcoinEnvelope(bytes.NewReader(buf))
	require.Error(err)
	require.Contains(err.Error(), "unknown BitcoinBlobdata field 99")
}

// TestWalkBitcoinEnvelope_TeedToFile verifies the tee pattern: walker
// reads from a tee, bytes land in a spool file, and we can reopen
// the spool + seek to a chunk offset to recover the chunk.
func TestWalkBitcoinEnvelope_TeedToFile(t *testing.T) {
	require := require.New(t)

	original := &api.Block{
		Blockchain: common.Blockchain_BLOCKCHAIN_BITCOIN,
		Network:    common.Network_NETWORK_BITCOIN_MAINNET,
		Metadata:   &api.BlockMetadata{Tag: 1, Height: 42},
		Blobdata: &api.Block_Bitcoin{
			Bitcoin: &api.BitcoinBlobdata{
				Header: []byte(`{"hash":"ab","tx":[{"txid":"xyz"}]}`),
				InputTransactions: []*api.RepeatedBytes{
					{Data: [][]byte{[]byte("prev1")}},
				},
			},
		},
	}
	raw, err := proto.Marshal(original)
	require.NoError(err)

	spool, err := os.CreateTemp(t.TempDir(), "chainstorage-spool-*.bin")
	require.NoError(err)
	defer spool.Close()

	tee := io.TeeReader(bytes.NewReader(raw), spool)
	block, chunks, err := api.WalkBitcoinEnvelope(tee)
	require.NoError(err)
	require.NoError(spool.Sync())

	// Recover Header by seeking into the spool file.
	f, err := os.Open(spool.Name())
	require.NoError(err)
	defer f.Close()
	_, err = f.Seek(chunks.Header.Offset, io.SeekStart)
	require.NoError(err)
	got, err := io.ReadAll(io.LimitReader(f, chunks.Header.Length))
	require.NoError(err)
	require.Equal(original.GetBitcoin().Header, got)

	full, err := os.ReadFile(spool.Name())
	require.NoError(err)
	require.Equal(raw, full)

	// Recover input_transactions[0] by seeking.
	require.Equal(1, len(chunks.InputTransactionsGroups))
	ref := chunks.InputTransactionsGroups[0]
	groupBytes, err := os.ReadFile(spool.Name())
	require.NoError(err)
	gotGroup := &api.RepeatedBytes{}
	require.NoError(proto.Unmarshal(groupBytes[ref.Offset:ref.Offset+ref.Length], gotGroup))
	require.Equal([][]byte{[]byte("prev1")}, gotGroup.Data)

	// Block's Bitcoin has neither Header nor InputTransactions.
	require.Nil(block.GetBitcoin().Header)
	require.Nil(block.GetBitcoin().InputTransactions)
}
