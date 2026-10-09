package dynamodb

import (
	"testing"

	"github.com/stretchr/testify/require"

	ddbmodel "github.com/coinbase/chainstorage/internal/storage/metastorage/dynamodb/model"
	"github.com/coinbase/chainstorage/internal/utils/testutil"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

func TestMakeBlockMetaDataDDBEntries_ConsolidatedFields(t *testing.T) {
	block := testutil.MakeBlockMetadata(100, tag)
	block.ObjectKeyMain = "consolidated/v=1/shard=00000000000000000000-00000000000000009999/00000000000000000100-00000000000000000199-deadbeef.cscb.zstd"
	block.ObjectFormat = api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_CSCB_BATCH
	block.ByteOffset = 4096
	block.ByteLength = 8192
	block.UncompressedLength = 8192

	entries := (&blockStorageImpl{}).makeBlockMetaDataDDBEntries(true, []*api.BlockMetadata{block})

	require.Len(t, entries, 3)
	for _, entry := range entries {
		blockEntry, ok := entry.(*ddbmodel.BlockMetaDataDDBEntry)
		require.True(t, ok)
		require.Equal(t, int32(api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_CSCB_BATCH), blockEntry.ObjectFormat)
		require.Equal(t, uint64(4096), blockEntry.ByteOffset)
		require.Equal(t, uint64(8192), blockEntry.ByteLength)
		require.Equal(t, uint64(8192), blockEntry.UncompressedLength)
	}
	require.Equal(t, getBlockRidWithBlockHash(block.Hash), entries[0].(*ddbmodel.BlockMetaDataDDBEntry).BlockRid)
	require.Equal(t, getCanonicalBlockRid(), entries[1].(*ddbmodel.BlockMetaDataDDBEntry).BlockRid)
	require.Equal(t, getBlockPidForLatest(block.Tag), entries[2].(*ddbmodel.BlockMetaDataDDBEntry).BlockPid)
	require.Equal(t, getCanonicalBlockRid(), entries[2].(*ddbmodel.BlockMetaDataDDBEntry).BlockRid)
}
