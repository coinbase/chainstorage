package postgres

import (
	"database/sql/driver"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/coinbase/chainstorage/internal/utils/testutil"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

func TestBlockObjectByteFields_SingleBlockUsesNullSentinel(t *testing.T) {
	block := testutil.MakeBlockMetadata(100, tag)

	byteOffset, byteLength, uncompressedLength := blockObjectByteFields(block)

	require.False(t, byteOffset.Valid)
	require.False(t, byteLength.Valid)
	require.False(t, uncompressedLength.Valid)
}

func TestBlockObjectByteFields_ConsolidatedFields(t *testing.T) {
	block := testutil.MakeBlockMetadata(100, tag)
	block.ObjectFormat = api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_CSCB_BATCH
	block.ByteOffset = 4096
	block.ByteLength = 8192
	block.UncompressedLength = 8192

	byteOffset, byteLength, uncompressedLength := blockObjectByteFields(block)

	require.True(t, byteOffset.Valid)
	require.Equal(t, int64(4096), byteOffset.Int64)
	require.True(t, byteLength.Valid)
	require.Equal(t, int64(8192), byteLength.Int64)
	require.True(t, uncompressedLength.Valid)
	require.Equal(t, int64(8192), uncompressedLength.Int64)
}

func TestChunkSlice(t *testing.T) {
	tests := []struct {
		name      string
		total     int
		size      int
		wantSizes []int
	}{
		{name: "empty", total: 0, size: 1000, wantSizes: nil},
		{name: "below chunk", total: 999, size: 1000, wantSizes: []int{999}},
		{name: "exact chunk", total: 1000, size: 1000, wantSizes: []int{1000}},
		{name: "one over", total: 1001, size: 1000, wantSizes: []int{1000, 1}},
		{name: "production batch", total: 2500, size: 1000, wantSizes: []int{1000, 1000, 500}},
		{name: "non-positive size means one chunk", total: 7, size: 0, wantSizes: []int{7}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			items := make([]int, test.total)
			for i := range items {
				items[i] = i
			}
			chunks := chunkSlice(items, test.size)
			require.Len(t, chunks, len(test.wantSizes))
			next := 0
			for i, chunk := range chunks {
				require.Len(t, chunk, test.wantSizes[i])
				for _, item := range chunk {
					require.Equal(t, next, item, "chunks must cover the input in order")
					next++
				}
			}
			require.Equal(t, test.total, next)
		})
	}
}

func TestPartitionBlocksForPersist_PreservesOrder(t *testing.T) {
	blocks := testutil.MakeBlockMetadatas(6, tag)
	blocks[1] = &api.BlockMetadata{Tag: tag, Height: 1, Skipped: true}
	blocks[4] = &api.BlockMetadata{Tag: tag, Height: 4, Skipped: true}

	regular, skipped := partitionBlocksForPersist(blocks)

	require.Equal(t, []*api.BlockMetadata{blocks[0], blocks[2], blocks[3], blocks[5]}, regular)
	require.Equal(t, []*api.BlockMetadata{blocks[1], blocks[4]}, skipped)
}

func TestDedupeBlocksKeepLast(t *testing.T) {
	a := testutil.MakeBlockMetadata(10, tag)
	b := testutil.MakeBlockMetadata(11, tag)
	aReplay := testutil.MakeBlockMetadata(10, tag)
	aReplay.ObjectKeyMain = "replayed"
	c := testutil.MakeBlockMetadata(12, tag)

	t.Run("keeps last occurrence in last-occurrence order", func(t *testing.T) {
		deduped := dedupeBlocksKeepLast([]*api.BlockMetadata{a, b, aReplay, c}, regularBlockKey)
		require.Equal(t, []*api.BlockMetadata{b, aReplay, c}, deduped)
	})

	t.Run("returns input when there are no duplicates", func(t *testing.T) {
		input := []*api.BlockMetadata{a, b, c}
		deduped := dedupeBlocksKeepLast(input, regularBlockKey)
		require.Equal(t, input, deduped)
	})

	t.Run("skipped key is tag and height", func(t *testing.T) {
		first := &api.BlockMetadata{Tag: tag, Height: 5, Skipped: true}
		second := &api.BlockMetadata{Tag: tag, Height: 5, Skipped: true}
		otherTag := &api.BlockMetadata{Tag: tag + 1, Height: 5, Skipped: true}
		deduped := dedupeBlocksKeepLast([]*api.BlockMetadata{first, otherTag, second}, skippedBlockKey)
		require.Equal(t, []*api.BlockMetadata{otherTag, second}, deduped)
	})

	t.Run("regular key is tag and hash", func(t *testing.T) {
		otherTag := testutil.MakeBlockMetadata(10, tag+1)
		deduped := dedupeBlocksKeepLast([]*api.BlockMetadata{a, otherTag}, regularBlockKey)
		require.Equal(t, []*api.BlockMetadata{a, otherTag}, deduped)
	})
}

func TestBlockMetadataUpsertQuery_RegularKeepsConflictClauseAndGuards(t *testing.T) {
	query := blockMetadataUpsertQuery(false)

	require.Contains(t, query, "ON CONFLICT (tag, hash) WHERE hash IS NOT NULL AND NOT skipped DO UPDATE SET")
	require.Contains(t, query, "RETURNING id, tag, hash")
	require.Contains(t, query, "NULLIF(input.storage_generation, '')")
	require.Contains(t, query, "$13::TEXT[]")
	require.Contains(t, query, "ORDER BY input.ordinal")
	// Every placement column is guarded by the retirement fence and the repair pin.
	for _, column := range []string{"object_key_main", "object_format", "byte_offset", "byte_length", "uncompressed_length", "storage_generation"} {
		require.Contains(t, query, "THEN block_metadata."+column+"\n", "column %s must keep the existing value when guarded", column)
		require.Contains(t, query, "ELSE EXCLUDED."+column+"\n", "column %s must take the new value otherwise", column)
	}
	require.Equal(t, 6, strings.Count(query, "WHEN block_metadata.single_block_retention_fenced_at IS NOT NULL"))
	require.Equal(t, 6, strings.Count(query, "SELECT 1 FROM cscb_repair_block repair_block"))
	// hash, tag and height are never rewritten on conflict, which is what makes the RETURNING key stable.
	require.NotContains(t, query, "hash = EXCLUDED.hash")
	require.NotContains(t, query, "height = EXCLUDED.height")
}

func TestBlockMetadataUpsertQuery_SkippedKeepsConflictClause(t *testing.T) {
	query := blockMetadataUpsertQuery(true)

	require.Contains(t, query, "ON CONFLICT (tag, height) WHERE skipped = true DO UPDATE SET")
	require.Contains(t, query, "hash = EXCLUDED.hash")
	require.Contains(t, query, "storage_generation = EXCLUDED.storage_generation")
	require.Contains(t, query, "RETURNING id, tag, height")
	require.NotContains(t, query, "cscb_repair_block")
}

func arrayLiteral(t *testing.T, arg interface{}) string {
	t.Helper()
	valuer, ok := arg.(driver.Valuer)
	require.True(t, ok, "argument %T must be a driver.Valuer", arg)
	value, err := valuer.Value()
	require.NoError(t, err)
	literal, ok := value.(string)
	require.True(t, ok, "array literal must be a string, got %T", value)
	return literal
}

func TestBlockMetadataColumnArrays(t *testing.T) {
	genesis := testutil.MakeBlockMetadata(0, tag)
	genesis.ParentHeight = 42 // must be ignored for height 0
	genesis.Timestamp = nil
	genesis.Hash = "" // sent as an empty string, never NULL
	cscb := testutil.MakeBlockMetadata(1, tag)
	cscb.ObjectFormat = api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_CSCB_BATCH
	cscb.ByteOffset = 64
	cscb.ByteLength = 128
	cscb.UncompressedLength = 256
	cscb.StorageGeneration = "v2"
	skipped := &api.BlockMetadata{Tag: tag, Height: 2, Skipped: true}

	args := blockMetadataColumnArrays([]*api.BlockMetadata{genesis, cscb, skipped})

	require.Len(t, args, 13)
	require.Equal(t, "{0,1,2}", arrayLiteral(t, args[0]), "heights")
	require.Equal(t, "{1,1,1}", arrayLiteral(t, args[1]), "tags")
	require.Equal(t, `{"",`+`"`+cscb.Hash+`",""}`, arrayLiteral(t, args[2]), "hashes")
	require.Equal(t, "{0,0,0}", arrayLiteral(t, args[4]), "parent heights: genesis forced to 0, skipped has none")
	require.Equal(t, "{0,"+timestampLiteral(cscb)+",0}", arrayLiteral(t, args[6]), "timestamps")
	require.Equal(t, "{f,f,t}", arrayLiteral(t, args[7]), "skipped")
	require.Equal(t, "{0,1,0}", arrayLiteral(t, args[8]), "object formats")
	require.Equal(t, "{NULL,64,NULL}", arrayLiteral(t, args[9]), "byte offsets")
	require.Equal(t, "{NULL,128,NULL}", arrayLiteral(t, args[10]), "byte lengths")
	require.Equal(t, "{NULL,256,NULL}", arrayLiteral(t, args[11]), "uncompressed lengths")
	require.Equal(t, `{"","v2",""}`, arrayLiteral(t, args[12]), "storage generations; empty becomes NULL via NULLIF in SQL")
}

func timestampLiteral(block *api.BlockMetadata) string {
	return strconv.FormatInt(block.GetTimestamp().GetSeconds(), 10)
}

func TestResolveCanonicalRows(t *testing.T) {
	regular := testutil.MakeBlockMetadata(10, tag)
	skippedSameHeight := &api.BlockMetadata{Tag: tag, Height: 10, Skipped: true}
	next := testutil.MakeBlockMetadata(11, tag)
	ids := newPersistedBlockIDs(3)
	ids.regular[regularBlockKey(regular)] = 100
	ids.skipped[skippedBlockKey(skippedSameHeight)] = 200
	ids.regular[regularBlockKey(next)] = 300

	t.Run("last block at a height wins regardless of group", func(t *testing.T) {
		rows, err := resolveCanonicalRows([]*api.BlockMetadata{regular, skippedSameHeight, next}, ids)
		require.NoError(t, err)
		require.Equal(t, []canonicalRow{
			{height: 10, blockMetadataID: 200, tag: tag},
			{height: 11, blockMetadataID: 300, tag: tag},
		}, rows)

		rows, err = resolveCanonicalRows([]*api.BlockMetadata{skippedSameHeight, regular, next}, ids)
		require.NoError(t, err)
		require.Equal(t, []canonicalRow{
			{height: 10, blockMetadataID: 100, tag: tag},
			{height: 11, blockMetadataID: 300, tag: tag},
		}, rows)
	})

	t.Run("same height under different tags are distinct", func(t *testing.T) {
		otherTag := testutil.MakeBlockMetadata(10, tag+1)
		ids.regular[regularBlockKey(otherTag)] = 400
		rows, err := resolveCanonicalRows([]*api.BlockMetadata{regular, otherTag}, ids)
		require.NoError(t, err)
		require.Equal(t, []canonicalRow{
			{height: 10, blockMetadataID: 100, tag: tag},
			{height: 10, blockMetadataID: 400, tag: tag + 1},
		}, rows)
	})

	t.Run("missing id is an error", func(t *testing.T) {
		unknown := testutil.MakeBlockMetadata(12, tag)
		_, err := resolveCanonicalRows([]*api.BlockMetadata{unknown}, ids)
		require.Error(t, err)
		require.Contains(t, err.Error(), "missing block metadata id")
	})
}
