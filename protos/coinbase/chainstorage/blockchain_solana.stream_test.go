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

// TestWalkSolanaEnvelope_Parity walks a hand-built block and asserts
// that the header is exposed via a chunk offset (not materialized)
// while the rest of the block matches proto.Marshal round-trip
// behavior.
func TestWalkSolanaEnvelope_Parity(t *testing.T) {
	require := require.New(t)

	header := []byte(`{"blockhash":"E7ks","blockHeight":1,"parentSlot":0,"blockTime":1,"transactions":[{"transaction":{"signatures":["sig"]}}]}`)
	original := &api.Block{
		Blockchain: common.Blockchain_BLOCKCHAIN_SOLANA,
		Network:    common.Network_NETWORK_SOLANA_MAINNET,
		Metadata: &api.BlockMetadata{
			Tag:          2,
			Height:       195545750,
			ParentHeight: 195545749,
			Hash:         "E7ksVVZ9kFjjxrKVtuMXXhM4fVczSDiiFtMkcgFh9jcd",
			ParentHash:   "DXFTuumL1TAV85kE8WxvzqsSmbWrPK3ZDMSdqysEGeiS",
		},
		Blobdata: &api.Block_Solana{
			Solana: &api.SolanaBlobdata{Header: header},
		},
	}
	raw, err := proto.Marshal(original)
	require.NoError(err)

	block, chunks, err := api.WalkSolanaEnvelope(bytes.NewReader(raw))
	require.NoError(err)

	// Top-level fields decode normally.
	require.Equal(original.Blockchain, block.Blockchain)
	require.Equal(original.Network, block.Network)
	require.Equal(original.Metadata.Tag, block.Metadata.Tag)
	require.Equal(original.Metadata.Height, block.Metadata.Height)
	require.Equal(original.Metadata.ParentHeight, block.Metadata.ParentHeight)
	require.Equal(original.Metadata.Hash, block.Metadata.Hash)
	require.Equal(original.Metadata.ParentHash, block.Metadata.ParentHash)

	// Blob: the header is skipped by the walker and exposed via the
	// chunk ref.
	blob := block.GetSolana()
	require.NotNil(blob)
	require.Nil(blob.Header, "header must be left nil — walker exposes it via chunks.Header")
	require.Equal(int64(len(header)), chunks.Header.Length)
	require.Equal(header, raw[chunks.Header.Offset:chunks.Header.Offset+chunks.Header.Length])
}

// TestWalkSolanaEnvelope_Fixture walks a real Solana v2 block fixture.
func TestWalkSolanaEnvelope_Fixture(t *testing.T) {
	require := require.New(t)

	header, err := fixtures.ReadFile("parser/solana/block_241043141_v2.json")
	require.NoError(err)
	original := &api.Block{
		Blockchain: common.Blockchain_BLOCKCHAIN_SOLANA,
		Network:    common.Network_NETWORK_SOLANA_MAINNET,
		Metadata:   &api.BlockMetadata{Tag: 2, Height: 241043141},
		Blobdata:   &api.Block_Solana{Solana: &api.SolanaBlobdata{Header: header}},
	}
	raw, err := proto.Marshal(original)
	require.NoError(err)

	block, chunks, err := api.WalkSolanaEnvelope(bytes.NewReader(raw))
	require.NoError(err)
	require.Equal(int64(len(header)), chunks.Header.Length)
	require.Equal(header, raw[chunks.Header.Offset:chunks.Header.Offset+chunks.Header.Length])
	require.Nil(block.GetSolana().Header)
	require.Equal(uint64(241043141), block.GetMetadata().GetHeight())
}

// TestWalkSolanaEnvelope_UnknownBlockField ensures the walker errors
// on a field number not registered in its known set.
func TestWalkSolanaEnvelope_UnknownBlockField(t *testing.T) {
	require := require.New(t)
	// tag = (77 << 3) | 0 = 616 → varint 0xE8, 0x04; value = 0x2A (42)
	buf := []byte{0xE8, 0x04, 0x2A}
	_, _, err := api.WalkSolanaEnvelope(bytes.NewReader(buf))
	require.Error(err)
	require.Contains(err.Error(), "unknown Block field 77")
}

// TestWalkSolanaEnvelope_UnknownSolanaBlobField checks the same at
// the SolanaBlobdata level.
func TestWalkSolanaEnvelope_UnknownSolanaBlobField(t *testing.T) {
	require := require.New(t)
	// Outer tag (field 103, wiretype 2) = 0xBA 0x06; length=3;
	// inner: field 99 varint 1 = 0x98 0x06 0x01
	buf := []byte{0xBA, 0x06, 0x03, 0x98, 0x06, 0x01}
	_, _, err := api.WalkSolanaEnvelope(bytes.NewReader(buf))
	require.Error(err)
	require.Contains(err.Error(), "unknown SolanaBlobdata field 99")
}

// TestWalkSolanaEnvelope_TeedToFile verifies the spool pattern: the
// walker reads from a tee, bytes land in a spool file, and the header
// is recovered by seeking to the recorded offset.
func TestWalkSolanaEnvelope_TeedToFile(t *testing.T) {
	require := require.New(t)

	header := []byte(`{"blockhash":"ab","transactions":[]}`)
	original := &api.Block{
		Blockchain: common.Blockchain_BLOCKCHAIN_SOLANA,
		Network:    common.Network_NETWORK_SOLANA_MAINNET,
		Metadata:   &api.BlockMetadata{Tag: 2, Height: 42},
		Blobdata:   &api.Block_Solana{Solana: &api.SolanaBlobdata{Header: header}},
	}
	raw, err := proto.Marshal(original)
	require.NoError(err)

	spool, err := os.CreateTemp(t.TempDir(), "chainstorage-spool-*.bin")
	require.NoError(err)
	defer spool.Close()

	tee := io.TeeReader(bytes.NewReader(raw), spool)
	block, chunks, err := api.WalkSolanaEnvelope(tee)
	require.NoError(err)
	require.NoError(spool.Sync())

	f, err := os.Open(spool.Name())
	require.NoError(err)
	defer f.Close()
	_, err = f.Seek(chunks.Header.Offset, io.SeekStart)
	require.NoError(err)
	got, err := io.ReadAll(io.LimitReader(f, chunks.Header.Length))
	require.NoError(err)
	require.Equal(header, got)

	full, err := os.ReadFile(spool.Name())
	require.NoError(err)
	require.Equal(raw, full)
	require.Nil(block.GetSolana().Header)
}
