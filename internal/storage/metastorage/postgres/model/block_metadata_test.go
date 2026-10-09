package model

import (
	"database/sql"
	"testing"

	"github.com/stretchr/testify/require"

	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

type blockMetadataScanner struct {
	objectFormat       int32
	storageGeneration  sql.NullString
	byteOffset         sql.NullInt64
	byteLength         sql.NullInt64
	uncompressedLength sql.NullInt64
}

func (s blockMetadataScanner) Scan(dest ...interface{}) error {
	*dest[0].(*int64) = 123
	*dest[1].(*uint64) = 100
	*dest[2].(*uint32) = 1
	*dest[3].(*string) = "0xabc"
	*dest[4].(*string) = "0xdef"
	*dest[5].(*uint64) = 99
	*dest[6].(*string) = "1/100/0xabc.gzip"
	*dest[7].(*int64) = 123456
	*dest[8].(*bool) = false
	*dest[9].(*int32) = s.objectFormat
	*dest[10].(*sql.NullInt64) = s.byteOffset
	*dest[11].(*sql.NullInt64) = s.byteLength
	*dest[12].(*sql.NullInt64) = s.uncompressedLength
	*dest[13].(*sql.NullString) = s.storageGeneration
	return nil
}

func TestScanBlockMetadata_SingleBlockNullByteFields(t *testing.T) {
	actual, err := scanBlockMetadata(blockMetadataScanner{})

	require.NoError(t, err)
	require.Equal(t, api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_SINGLE_BLOCK, actual.GetObjectFormat())
	require.Zero(t, actual.GetByteOffset())
	require.Zero(t, actual.GetByteLength())
	require.Zero(t, actual.GetUncompressedLength())
	require.Empty(t, actual.GetStorageGeneration())
}

func TestScanBlockMetadata_ConsolidatedFields(t *testing.T) {
	actual, err := scanBlockMetadata(blockMetadataScanner{
		objectFormat:       int32(api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_CSCB_BATCH),
		storageGeneration:  sql.NullString{String: "v2", Valid: true},
		byteOffset:         sql.NullInt64{Int64: 4096, Valid: true},
		byteLength:         sql.NullInt64{Int64: 8192, Valid: true},
		uncompressedLength: sql.NullInt64{Int64: 8192, Valid: true},
	})

	require.NoError(t, err)
	require.Equal(t, api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_CSCB_BATCH, actual.GetObjectFormat())
	require.Equal(t, uint64(4096), actual.GetByteOffset())
	require.Equal(t, uint64(8192), actual.GetByteLength())
	require.Equal(t, uint64(8192), actual.GetUncompressedLength())
	require.Equal(t, "v2", actual.GetStorageGeneration())
}
