package model

import (
	"database/sql"

	"github.com/coinbase/chainstorage/internal/utils/utils"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

// Scanner interface that both *sql.Row and *sql.Rows implement
type Scanner interface {
	Scan(dest ...interface{}) error
}

// scanBlockMetadata scans a single row into a BlockMetadata struct.
// Schema: id, height, tag, hash, parent_hash, parent_height, object_key_main,
// timestamp, skipped, object_format, byte_offset, byte_length, uncompressed_length,
// storage_generation
func scanBlockMetadata(scanner Scanner) (*api.BlockMetadata, error) {
	var block api.BlockMetadata
	var timestamp int64
	var objectFormat int32
	var storageGeneration sql.NullString
	var byteOffset, byteLength, uncompressedLength sql.NullInt64
	var id int64 // We get this but don't need it in the result

	err := scanner.Scan(
		&id,
		&block.Height,
		&block.Tag,
		&block.Hash,
		&block.ParentHash,
		&block.ParentHeight,
		&block.ObjectKeyMain,
		&timestamp,
		&block.Skipped,
		&objectFormat,
		&byteOffset,
		&byteLength,
		&uncompressedLength,
		&storageGeneration,
	)
	if err != nil {
		return nil, err
	}

	block.Timestamp = utils.ToTimestamp(timestamp)
	block.ObjectFormat = api.BlockObjectFormat(objectFormat)
	block.StorageGeneration = storageGeneration.String
	if byteOffset.Valid {
		block.ByteOffset = uint64(byteOffset.Int64)
	}
	if byteLength.Valid {
		block.ByteLength = uint64(byteLength.Int64)
	}
	if uncompressedLength.Valid {
		block.UncompressedLength = uint64(uncompressedLength.Int64)
	}
	return &block, nil
}

// BlockMetadataFromRow converts a postgres row into a BlockMetadata proto
// Used for direct block_metadata table queries
// Schema: see scanBlockMetadata
func BlockMetadataFromRow(db *sql.DB, row *sql.Row) (*api.BlockMetadata, error) {
	return scanBlockMetadata(row)
}

// BlockMetadataFromCanonicalRow converts a postgres row from canonical join into a BlockMetadata proto
// Used for queries that join canonical_blocks with block_metadata
// Schema: see scanBlockMetadata
func BlockMetadataFromCanonicalRow(db *sql.DB, row *sql.Row) (*api.BlockMetadata, error) {
	return scanBlockMetadata(row)
}

// scanBlockMetadataRows scans multiple rows into BlockMetadata structs
func scanBlockMetadataRows(rows *sql.Rows) ([]*api.BlockMetadata, error) {
	var blocks []*api.BlockMetadata
	for rows.Next() {
		block, err := scanBlockMetadata(rows)
		if err != nil {
			return nil, err
		}
		blocks = append(blocks, block)
	}
	return blocks, nil
}

// BlockMetadataFromRows converts multiple postgres rows into BlockMetadata protos
// Used for direct block_metadata table queries
func BlockMetadataFromRows(db *sql.DB, rows *sql.Rows) ([]*api.BlockMetadata, error) {
	return scanBlockMetadataRows(rows)
}

// BlockMetadataFromCanonicalRows converts multiple postgres rows from canonical joins into BlockMetadata protos
// Used for queries that join canonical_blocks with block_metadata
func BlockMetadataFromCanonicalRows(db *sql.DB, rows *sql.Rows) ([]*api.BlockMetadata, error) {
	return scanBlockMetadataRows(rows)
}
