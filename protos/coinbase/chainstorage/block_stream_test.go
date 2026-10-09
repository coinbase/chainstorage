package chainstorage_test

// Parity + drift tests for the generic WalkBlockEnvelope.
//
// For each chain, construct a Block with a representative blobdata
// populated, marshal it, walk it, and assert the walked Block is
// byte-for-byte equivalent (via proto.Equal) to the original.
//
// These tests serve two purposes:
//  1. Correctness of the walker across every blobdata variant.
//  2. Drift defense. Adding a new field to any Blobdata message
//     doesn't require walker code changes (the walker is reflection-
//     based), but the parity test exercises the full round-trip.
//     Kind-level drift (e.g. adding a new proto Kind the walker
//     doesn't support) fails here with a clear error.

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/coinbase/chainstorage/protos/coinbase/c3/common"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

func TestWalkBlockEnvelope_Bitcoin(t *testing.T) {
	original := &api.Block{
		Blockchain: common.Blockchain_BLOCKCHAIN_BITCOIN,
		Network:    common.Network_NETWORK_BITCOIN_MAINNET,
		Metadata:   &api.BlockMetadata{Tag: 1, Height: 696402, Hash: "abc"},
		Blobdata: &api.Block_Bitcoin{
			Bitcoin: &api.BitcoinBlobdata{
				Header: []byte(`{"hash":"abc","tx":[]}`),
				InputTransactions: []*api.RepeatedBytes{
					{Data: [][]byte{[]byte("prev1"), []byte("prev2")}},
				},
				RawInputTransactions: map[string][]byte{
					"tx1": []byte("rawtx1"),
					"tx2": []byte("rawtx2"),
				},
			},
		},
	}
	assertParity(t, original)
}

func TestWalkBlockEnvelope_Ethereum(t *testing.T) {
	original := &api.Block{
		Blockchain: common.Blockchain_BLOCKCHAIN_ETHEREUM,
		Network:    common.Network_NETWORK_ETHEREUM_MAINNET,
		Metadata:   &api.BlockMetadata{Tag: 1, Height: 19_000_000},
		Blobdata: &api.Block_Ethereum{
			Ethereum: &api.EthereumBlobdata{
				Header:              []byte(`{"hash":"0x123","number":"0x1"}`),
				TransactionReceipts: [][]byte{[]byte(`{"r":1}`), []byte(`{"r":2}`)},
				TransactionTraces:   [][]byte{[]byte(`{"t":1}`)},
				Uncles:              [][]byte{[]byte(`{"u":1}`)},
			},
		},
	}
	assertParity(t, original)
}

func TestWalkBlockEnvelope_EthereumBeacon(t *testing.T) {
	original := &api.Block{
		Blockchain: common.Blockchain_BLOCKCHAIN_ETHEREUM,
		SideChain:  api.SideChain_SIDECHAIN_ETHEREUM_MAINNET_BEACON,
		Metadata:   &api.BlockMetadata{Tag: 1, Height: 1},
		Blobdata: &api.Block_EthereumBeacon{
			EthereumBeacon: &api.EthereumBeaconBlobdata{
				Header: []byte(`{"slot":"1"}`),
				Block:  []byte(`{"body":{}}`),
				Blobs:  []byte(`[]`),
			},
		},
	}
	assertParity(t, original)
}

func TestWalkBlockEnvelope_Solana(t *testing.T) {
	original := &api.Block{
		Blockchain: common.Blockchain_BLOCKCHAIN_SOLANA,
		Network:    common.Network_NETWORK_SOLANA_MAINNET,
		Metadata:   &api.BlockMetadata{Tag: 1, Height: 123456},
		Blobdata: &api.Block_Solana{
			Solana: &api.SolanaBlobdata{
				Header: []byte(`{"blockHeight":123456}`),
			},
		},
	}
	assertParity(t, original)
}

func TestWalkBlockEnvelope_Aptos(t *testing.T) {
	original := &api.Block{
		Blockchain: common.Blockchain_BLOCKCHAIN_APTOS,
		Network:    common.Network_NETWORK_APTOS_MAINNET,
		Metadata:   &api.BlockMetadata{Tag: 1, Height: 100},
		Blobdata: &api.Block_Aptos{
			Aptos: &api.AptosBlobdata{
				Block: []byte(`{"block_height":"100"}`),
			},
		},
	}
	assertParity(t, original)
}

func TestWalkBlockEnvelope_Rosetta(t *testing.T) {
	original := &api.Block{
		Blockchain: common.Blockchain_BLOCKCHAIN_TRON,
		Metadata:   &api.BlockMetadata{Tag: 1, Height: 42},
		Blobdata: &api.Block_Rosetta{
			Rosetta: &api.RosettaBlobdata{
				Header:            []byte(`{"block_identifier":{}}`),
				OtherTransactions: [][]byte{[]byte(`{"ot":1}`)},
				RawBlock:          []byte(`{"raw":"data"}`),
			},
		},
	}
	assertParity(t, original)
}

// TestWalkBlockEnvelope_Empty covers a minimal Block (no blobdata).
func TestWalkBlockEnvelope_Empty(t *testing.T) {
	original := &api.Block{
		Blockchain: common.Blockchain_BLOCKCHAIN_BITCOIN,
		Metadata:   &api.BlockMetadata{Tag: 1, Height: 0},
	}
	assertParity(t, original)
}

// TestWalkBlockEnvelope_UnknownField confirms strict behavior: a field
// number not present in the api.Block descriptor produces an error.
func TestWalkBlockEnvelope_UnknownField(t *testing.T) {
	require := require.New(t)
	// field 77, wire type 0 varint, value 1
	raw := []byte{0xE8, 0x04, 0x01}
	_, err := api.WalkBlockEnvelope(bytes.NewReader(raw))
	require.Error(err)
	require.Contains(err.Error(), "unknown field 77")
}

// assertParity round-trips `original` through proto.Marshal +
// WalkBlockEnvelope and asserts the walked result equals the input.
func assertParity(t *testing.T, original *api.Block) {
	t.Helper()
	raw, err := proto.Marshal(original)
	require.NoError(t, err)

	walked, err := api.WalkBlockEnvelope(bytes.NewReader(raw))
	require.NoError(t, err)

	// proto.Equal handles oneof + map/repeated equivalence correctly.
	require.Truef(t, proto.Equal(original, walked),
		"walker output does not equal original\noriginal: %+v\nwalked:   %+v",
		original, walked)
}
