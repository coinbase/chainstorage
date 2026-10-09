package utils

import (
	"testing"

	"github.com/stretchr/testify/require"

	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

func TestCloneBlockWithoutStoragePlacement(t *testing.T) {
	block := &api.Block{Metadata: &api.BlockMetadata{
		ObjectKeyMain:      "single-block/block.gzip",
		ObjectFormat:       api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_CSCB_BATCH,
		ByteOffset:         10,
		ByteLength:         20,
		UncompressedLength: 30,
		StorageGeneration:  "v2",
		Hash:               "hash",
	}}

	require.True(t, HasBlockStoragePlacement(block))
	clone := CloneBlockWithoutStoragePlacement(block)
	require.False(t, HasBlockStoragePlacement(clone))
	require.Equal(t, "hash", clone.Metadata.Hash)
	require.Equal(t, "", clone.Metadata.StorageGeneration)
	require.Equal(t, "single-block/block.gzip", block.Metadata.ObjectKeyMain)
	require.Equal(t, "v2", block.Metadata.StorageGeneration)
}
