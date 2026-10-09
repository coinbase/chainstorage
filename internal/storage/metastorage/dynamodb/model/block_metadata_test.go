package model

import (
	"testing"

	"github.com/stretchr/testify/require"

	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

func TestBlockMetadataToProto_ConsolidatedFields(t *testing.T) {
	entry := &BlockMetaDataDDBEntry{
		Tag:                1,
		Hash:               "0xabc",
		ParentHash:         "0xdef",
		Height:             100,
		ParentHeight:       99,
		ObjectKeyMain:      "consolidated/v=1/shard=00000000000000000000-00000000000000009999/00000000000000000100-00000000000000000199-deadbeef.cscb.zstd",
		Timestamp:          123456,
		ObjectFormat:       int32(api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_CSCB_BATCH),
		ByteOffset:         4096,
		ByteLength:         8192,
		UncompressedLength: 8192,
	}

	actual := BlockMetadataToProto(entry)

	require.Equal(t, api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_CSCB_BATCH, actual.GetObjectFormat())
	require.Equal(t, uint64(4096), actual.GetByteOffset())
	require.Equal(t, uint64(8192), actual.GetByteLength())
	require.Equal(t, uint64(8192), actual.GetUncompressedLength())
	require.Equal(t, entry.ObjectKeyMain, actual.GetObjectKeyMain())
}

func TestBlockMetadataToProto_SingleBlockMissingFields(t *testing.T) {
	entry := &BlockMetaDataDDBEntry{
		Tag:           1,
		Hash:          "0xabc",
		ParentHash:    "0xdef",
		Height:        100,
		ParentHeight:  99,
		ObjectKeyMain: "1/100/0xabc.gzip",
		Timestamp:     123456,
	}

	actual := BlockMetadataToProto(entry)

	require.Equal(t, api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_SINGLE_BLOCK, actual.GetObjectFormat())
	require.Zero(t, actual.GetByteOffset())
	require.Zero(t, actual.GetByteLength())
	require.Zero(t, actual.GetUncompressedLength())
}
