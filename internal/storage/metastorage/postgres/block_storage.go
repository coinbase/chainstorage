package postgres

import (
	"context"
	"database/sql"
	stderrors "errors"
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/lib/pq"
	"golang.org/x/xerrors"
	"google.golang.org/protobuf/proto"

	"github.com/coinbase/chainstorage/internal/blockchain/parser"
	"github.com/coinbase/chainstorage/internal/storage/cscbrepairlock"
	"github.com/coinbase/chainstorage/internal/storage/internal/errors"
	"github.com/coinbase/chainstorage/internal/storage/metastorage/internal"
	"github.com/coinbase/chainstorage/internal/storage/metastorage/postgres/model"
	"github.com/coinbase/chainstorage/internal/storage/retirementlock"
	"github.com/coinbase/chainstorage/internal/utils/instrument"
	"github.com/coinbase/chainstorage/internal/utils/utils"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

type (
	blockStorageImpl struct {
		db                               *sql.DB
		blockStartHeight                 uint64
		instrumentPersistBlockMetas      instrument.Instrument
		instrumentGetLatestBlock         instrument.InstrumentWithResult[*api.BlockMetadata]
		instrumentGetBlockByHash         instrument.InstrumentWithResult[*api.BlockMetadata]
		instrumentGetBlockByHeight       instrument.InstrumentWithResult[*api.BlockMetadata]
		instrumentGetBlocksByHeightRange instrument.InstrumentWithResult[[]*api.BlockMetadata]
		instrumentGetBlocksByHeights     instrument.InstrumentWithResult[[]*api.BlockMetadata]
		instrumentGetBlockByTimestamp    instrument.InstrumentWithResult[*api.BlockMetadata]
	}

	singleBlockUploadGuard struct {
		conn       *sql.Conn
		tx         *sql.Tx
		fenced     bool
		once       sync.Once
		releaseErr error
	}
)

const (
	blockMetadataColumns = `
		id, height, tag, hash, parent_hash, parent_height, object_key_main,
		timestamp, skipped, object_format, byte_offset, byte_length, uncompressed_length, storage_generation`
	canonicalBlockMetadataColumns = `
		bm.id, bm.height, bm.tag, bm.hash, bm.parent_hash, bm.parent_height, bm.object_key_main,
		bm.timestamp, bm.skipped, bm.object_format, bm.byte_offset, bm.byte_length, bm.uncompressed_length,
		bm.storage_generation`
)

func blockMetadataByHashQuery() string {
	return fmt.Sprintf(`
				SELECT %s
				FROM block_metadata
				WHERE tag = $1 AND height = $2 AND hash = $3 AND skipped = false
				LIMIT 1`, blockMetadataColumns)
}

func singleBlockUploadGuardByHashQuery() string {
	return `
		SELECT
			bm.single_block_retention_fenced_at IS NOT NULL
			OR EXISTS (
				SELECT 1
				FROM cscb_repair_block repair_block
				JOIN cscb_repair_manifest repair ON repair.id = repair_block.repair_id
				WHERE repair_block.block_metadata_id = bm.id
					AND repair.state <> 'completed'
			)
		FROM block_metadata bm
		WHERE bm.tag = $1
			AND bm.height = $2
			AND bm.hash = $3
			AND bm.skipped = FALSE
		FOR UPDATE`
}

// A block without a hash can only be identified by its canonical tag and height.
// Resolve that row through canonical_blocks so the lookup remains indexed.
func singleBlockUploadGuardWithoutHashQuery() string {
	return `
		SELECT
			bm.single_block_retention_fenced_at IS NOT NULL
			OR EXISTS (
				SELECT 1
				FROM cscb_repair_block repair_block
				JOIN cscb_repair_manifest repair ON repair.id = repair_block.repair_id
				WHERE repair_block.block_metadata_id = bm.id
					AND repair.state <> 'completed'
			)
		FROM block_metadata bm
		WHERE bm.id = (
			SELECT cb.block_metadata_id
			FROM canonical_blocks cb
			WHERE cb.tag = $1
				AND cb.height = $2
			FOR UPDATE
		)
			AND bm.tag = $1
			AND bm.height = $2
			AND bm.hash IS NULL
			AND bm.skipped = FALSE
		FOR UPDATE OF bm`
}

func newBlockStorage(db *sql.DB, params Params) (internal.BlockStorage, error) {
	metrics := params.Metrics.SubScope("block_storage").Tagged(map[string]string{
		"storage_type": "postgres",
	})
	accessor := blockStorageImpl{
		db:                               db,
		blockStartHeight:                 params.Config.Chain.BlockStartHeight,
		instrumentPersistBlockMetas:      instrument.New(metrics, "persist_block_metas"),
		instrumentGetLatestBlock:         instrument.NewWithResult[*api.BlockMetadata](metrics, "get_latest_block"),
		instrumentGetBlockByHash:         instrument.NewWithResult[*api.BlockMetadata](metrics, "get_block_by_hash"),
		instrumentGetBlockByHeight:       instrument.NewWithResult[*api.BlockMetadata](metrics, "get_block_by_height"),
		instrumentGetBlocksByHeightRange: instrument.NewWithResult[[]*api.BlockMetadata](metrics, "get_blocks_by_height_range"),
		instrumentGetBlocksByHeights:     instrument.NewWithResult[[]*api.BlockMetadata](metrics, "get_blocks_by_heights"),
		instrumentGetBlockByTimestamp:    instrument.NewWithResult[*api.BlockMetadata](metrics, "get_block_by_timestamp"),
	}
	return &accessor, nil
}

func (b *blockStorageImpl) AcquireSingleBlockUploadGuard(
	ctx context.Context,
	tag uint32,
	height uint64,
	hash string,
) (internal.SingleBlockUploadGuard, error) {
	conn, err := b.db.Conn(ctx)
	if err != nil {
		return nil, xerrors.Errorf("failed to acquire single-block upload connection: %w", err)
	}
	tx, err := conn.BeginTx(context.Background(), nil)
	if err != nil {
		_ = conn.Close()
		return nil, xerrors.Errorf("failed to begin single-block upload guard: %w", err)
	}
	if err := retirementlock.Acquire(ctx, tx, tag, height, hash); err != nil {
		_ = tx.Rollback()
		_ = conn.Close()
		return nil, err
	}
	var singleBlockWriteFenced bool
	if hash == "" {
		err = tx.QueryRowContext(ctx, singleBlockUploadGuardWithoutHashQuery(), tag, height).Scan(&singleBlockWriteFenced)
	} else {
		err = tx.QueryRowContext(ctx, singleBlockUploadGuardByHashQuery(), tag, height, hash).Scan(&singleBlockWriteFenced)
	}
	if err != nil && err != sql.ErrNoRows {
		_ = tx.Rollback()
		_ = conn.Close()
		return nil, xerrors.Errorf("failed to acquire single-block upload guard: %w", err)
	}
	return &singleBlockUploadGuard{
		conn:   conn,
		tx:     tx,
		fenced: err == nil && singleBlockWriteFenced,
	}, nil
}

func (g *singleBlockUploadGuard) RetirementFenced() bool {
	return g != nil && g.fenced
}

func (g *singleBlockUploadGuard) Release() error {
	if g == nil || g.tx == nil {
		return nil
	}
	g.once.Do(func() {
		g.releaseErr = g.tx.Rollback()
		if g.releaseErr == sql.ErrTxDone {
			g.releaseErr = nil
		}
		if g.conn != nil {
			g.releaseErr = stderrors.Join(g.releaseErr, g.conn.Close())
		}
	})
	return g.releaseErr
}

func (b *blockStorageImpl) PersistBlockMetas(
	ctx context.Context, updateWatermark bool, blocks []*api.BlockMetadata, lastBlock *api.BlockMetadata) error {
	return b.instrumentPersistBlockMetas.Instrument(ctx, func(ctx context.Context) error {
		if len(blocks) == 0 {
			return nil
		}

		// Sort blocks by height for chain validation.
		// IMPORTANT: When multiple blocks have the same height (e.g., during a reorg), this
		// implementation follows the "last block wins" principle - the last block for a given height,
		// in caller order, becomes the canonical block for that height. The sort is stable so that
		// caller order among equal heights is preserved. This behavior is consistent with the DynamoDB
		// implementation where the last block overwrites the canonical entry.
		//
		// The canonical_blocks table uses "ON CONFLICT (height, tag) DO UPDATE" which means:
		// - If multiple blocks in the input have the same height, the last one wins in canonical_blocks
		// - All blocks are still stored in block_metadata (allowing retrieval by specific hash)
		// - Only the last block for each height becomes the canonical one
		//
		// Callers should ensure that when multiple blocks exist for the same height, the desired
		// canonical block is placed last in the blocks array for that height.
		sort.SliceStable(blocks, func(i, j int) bool {
			return blocks[i].Height < blocks[j].Height
		})
		if err := parser.ValidateChain(blocks, lastBlock); err != nil {
			return xerrors.Errorf("failed to validate chain: %w", err)
		}

		// Create transaction with timeout context
		// Use a reasonable timeout for block persistence operations
		txCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()

		tx, err := b.db.BeginTx(txCtx, nil)
		if err != nil {
			return xerrors.Errorf("failed to begin transaction: %w", err)
		}
		committed := false
		defer func() {
			if !committed {
				if rollbackErr := tx.Rollback(); rollbackErr != nil {
					// Log the rollback error but don't override the original error
					// In a production environment, you might want to use a proper logger here
					// For now, we'll just ignore the rollback error as it's already a failure case
					_ = rollbackErr
				}
			}
		}()

		tags := make([]uint32, 0, len(blocks))
		for _, block := range blocks {
			if block != nil {
				tags = append(tags, block.Tag)
			}
		}
		if err := cscbrepairlock.AcquireTagsShared(txCtx, tx, tags); err != nil {
			return err
		}

		// Persist in height-ordered chunks with set-based statements instead of two round trips per
		// block. Within a chunk, block_metadata rows are upserted first (regular, then skipped, both
		// ascending by height) and canonical_blocks rows last, which keeps the row-lock order of the
		// per-block implementation at chunk granularity for concurrent writers.
		ids := newPersistedBlockIDs(len(blocks))
		for _, chunk := range chunkSlice(blocks, persistBlockMetasChunkSize) {
			regular, skipped := partitionBlocksForPersist(chunk)
			// Postgres rejects a multi-row ON CONFLICT DO UPDATE that touches the same row twice
			// ("cannot affect row a second time"), so collapse duplicates on each conflict key,
			// keeping the last occurrence. This matches the per-row implementation, where a later
			// duplicate simply updated the row inserted by an earlier one.
			regular = dedupeBlocksKeepLast(regular, regularBlockKey)
			skipped = dedupeBlocksKeepLast(skipped, skippedBlockKey)
			if err := upsertBlockMetadataChunk(txCtx, tx, regular, false, ids); err != nil {
				return err
			}
			if err := upsertBlockMetadataChunk(txCtx, tx, skipped, true, ids); err != nil {
				return err
			}

			// Simply insert or update canonical blocks like DynamoDB does
			// The "last write wins" behavior matches DynamoDB's TransactWriteItems
			// Chain validation happens in update_watermark activity, not here
			canonical, err := resolveCanonicalRows(chunk, ids)
			if err != nil {
				return err
			}
			if err := upsertCanonicalRows(txCtx, tx, canonical); err != nil {
				return err
			}
		}

		// Update watermark if requested
		// Set is_watermark=TRUE for the highest block to mark it as validated
		// This prevents canonical chain leakage to streamer before validation
		if updateWatermark && len(blocks) > 0 {
			highestBlock := blocks[len(blocks)-1]

			// Probabilistically clear old watermarks (1 in 5000 chance)
			// This prevents unbounded accumulation while keeping the operation rare enough
			// to have negligible performance impact
			if rand.Intn(5000) == 0 {
				// Clear all old watermarks for this tag, keeping only the current one
				// This is safe because only GetLatestBlock uses is_watermark filter,
				// and it only needs the highest watermarked block
				clearQuery := `
					UPDATE canonical_blocks
					SET is_watermark = FALSE
					WHERE tag = $1 AND is_watermark = TRUE`
				_, err = tx.ExecContext(txCtx, clearQuery, highestBlock.Tag)
				if err != nil {
					// Log but don't fail - cleanup is best-effort
					// In production, you might want to use a proper logger here
					_ = err
				}
			}

			// Set the new watermark
			watermarkQuery := `
				UPDATE canonical_blocks
				SET is_watermark = TRUE
				WHERE tag = $1 AND height = $2`
			_, err = tx.ExecContext(txCtx, watermarkQuery, highestBlock.Tag, highestBlock.Height)
			if err != nil {
				return xerrors.Errorf("failed to update watermark for height %d: %w", highestBlock.Height, err)
			}
		}

		// Commit transaction
		err = tx.Commit()
		if err != nil {
			return xerrors.Errorf("failed to commit transaction: %w", err)
		}
		committed = true
		return nil
	})
}

func (b *blockStorageImpl) GetLatestBlock(ctx context.Context, tag uint32) (*api.BlockMetadata, error) {
	return b.instrumentGetLatestBlock.Instrument(ctx, func(ctx context.Context) (*api.BlockMetadata, error) {
		// Get the latest canonical block by highest height
		// Only return blocks that have been validated (is_watermark=TRUE)
		// This prevents canonical chain leakage to streamer before validation
		query := fmt.Sprintf(`
			SELECT %s
			FROM canonical_blocks cb
			JOIN block_metadata bm ON cb.block_metadata_id = bm.id
			WHERE cb.tag = $1 AND cb.is_watermark = TRUE
			ORDER BY cb.height DESC
			LIMIT 1`, canonicalBlockMetadataColumns)
		row := b.db.QueryRowContext(ctx, query, tag)
		block, err := model.BlockMetadataFromCanonicalRow(b.db, row)
		if err != nil {
			if err == sql.ErrNoRows {
				return nil, xerrors.Errorf("no latest block found: %w", errors.ErrItemNotFound)
			}
			return nil, xerrors.Errorf("failed to get latest block: %w", err)
		}
		return block, nil
	})
}

func (b *blockStorageImpl) GetBlockByHash(ctx context.Context, tag uint32, height uint64, blockHash string) (*api.BlockMetadata, error) {
	return b.instrumentGetBlockByHash.Instrument(ctx, func(ctx context.Context) (*api.BlockMetadata, error) {
		if err := b.validateHeight(height); err != nil {
			return nil, err
		}
		var row *sql.Row
		if blockHash == "" {
			// Get the canonical block at this height (could be regular or skipped)
			query := fmt.Sprintf(`
				SELECT %s
				FROM canonical_blocks cb
				JOIN block_metadata bm ON cb.block_metadata_id = bm.id
				WHERE cb.tag = $1 AND cb.height = $2
				LIMIT 1`, canonicalBlockMetadataColumns)
			row = b.db.QueryRowContext(ctx, query, tag, height)
		} else {
			// Query block_metadata directly for the specific hash
			row = b.db.QueryRowContext(ctx, blockMetadataByHashQuery(), tag, height, blockHash)
		}

		block, err := model.BlockMetadataFromRow(b.db, row)
		if err != nil {
			if err == sql.ErrNoRows {
				return nil, xerrors.Errorf("block not found: %w", errors.ErrItemNotFound)
			}
			return nil, xerrors.Errorf("failed to get block by hash: %w", err)
		}
		return block, nil
	})
}

func (b *blockStorageImpl) GetBlockByHeight(ctx context.Context, tag uint32, height uint64) (*api.BlockMetadata, error) {
	return b.instrumentGetBlockByHeight.Instrument(ctx, func(ctx context.Context) (*api.BlockMetadata, error) {
		if err := b.validateHeight(height); err != nil {
			return nil, err
		}
		// Get block from canonical_blocks table (includes both regular and skipped blocks)
		query := fmt.Sprintf(`
			SELECT %s
			FROM canonical_blocks cb
			JOIN block_metadata bm ON cb.block_metadata_id = bm.id
			WHERE cb.tag = $1 AND cb.height = $2
			LIMIT 1`, canonicalBlockMetadataColumns)
		row := b.db.QueryRowContext(ctx, query, tag, height)
		block, err := model.BlockMetadataFromCanonicalRow(b.db, row)
		if err != nil {
			if err == sql.ErrNoRows {
				return nil, xerrors.Errorf("block at height %d not found: %w", height, errors.ErrItemNotFound)
			}
			return nil, xerrors.Errorf("failed to get block by height: %w", err)
		}
		return block, nil
	})
}

func (b *blockStorageImpl) GetBlocksByHeightRange(ctx context.Context, tag uint32, startHeight uint64, endHeight uint64) ([]*api.BlockMetadata, error) {
	return b.instrumentGetBlocksByHeightRange.Instrument(ctx, func(ctx context.Context) ([]*api.BlockMetadata, error) {
		if startHeight >= endHeight {
			return nil, errors.ErrOutOfRange
		}
		if err := b.validateHeight(startHeight); err != nil {
			return nil, err
		}

		// Get all blocks (canonical and skipped) from canonical_blocks table
		query := fmt.Sprintf(`
			SELECT %s
			FROM canonical_blocks cb
			JOIN block_metadata bm ON cb.block_metadata_id = bm.id
			WHERE cb.tag = $1 AND cb.height >= $2 AND cb.height < $3
			ORDER BY cb.height ASC`, canonicalBlockMetadataColumns)
		rows, err := b.db.QueryContext(ctx, query, tag, startHeight, endHeight)
		if err != nil {
			return nil, xerrors.Errorf("failed to query blocks by height range: %w", err)
		}
		defer func() {
			if closeErr := rows.Close(); closeErr != nil && err == nil {
				err = xerrors.Errorf("failed to close rows: %w", closeErr)
			}
		}()

		blocks, err := model.BlockMetadataFromCanonicalRows(b.db, rows)
		if err != nil {
			return nil, xerrors.Errorf("failed to scan block rows: %w", err)
		}

		// Check if we have all blocks in the range (no gaps)
		expectedCount := int(endHeight - startHeight)
		if len(blocks) != expectedCount {
			return nil, xerrors.Errorf("missing blocks in range [%d, %d): expected %d, got %d: %w",
				startHeight, endHeight, expectedCount, len(blocks), errors.ErrItemNotFound)
		}

		// Verify no gaps in heights
		for i, block := range blocks {
			expectedHeight := startHeight + uint64(i)
			if block.Height != expectedHeight {
				return nil, xerrors.Errorf("gap in block heights: expected %d, got %d: %w",
					expectedHeight, block.Height, errors.ErrItemNotFound)
			}
		}

		// Validate chain continuity (parent hash matching) like DynamoDB does
		// This is critical for detecting reorgs and triggering recovery logic
		if err = parser.ValidateChain(blocks, nil); err != nil {
			return nil, xerrors.Errorf("failed to validate chain: %w", err)
		}

		return blocks, nil
	})
}

func (b *blockStorageImpl) GetBlocksByHeights(ctx context.Context, tag uint32, heights []uint64) ([]*api.BlockMetadata, error) {
	return b.instrumentGetBlocksByHeights.Instrument(ctx, func(ctx context.Context) ([]*api.BlockMetadata, error) {
		for _, height := range heights {
			if err := b.validateHeight(height); err != nil {
				return nil, err
			}
		}
		if len(heights) == 0 {
			return []*api.BlockMetadata{}, nil
		}
		// Build dynamic query with placeholders for IN clause
		placeholders := make([]string, len(heights))
		args := make([]interface{}, len(heights)+1)
		args[0] = tag // First argument is tag
		for i, height := range heights {
			placeholders[i] = fmt.Sprintf("$%d", i+2) // Start from $2 since $1 is tag
			args[i+1] = height
		}
		query := fmt.Sprintf(`
			SELECT %s
			FROM canonical_blocks cb
			JOIN block_metadata bm ON cb.block_metadata_id = bm.id
			WHERE cb.tag = $1 AND cb.height IN (%s)
			ORDER BY cb.height ASC`,
			canonicalBlockMetadataColumns,
			strings.Join(placeholders, ", "))

		rows, err := b.db.QueryContext(ctx, query, args...)
		if err != nil {
			return nil, xerrors.Errorf("failed to query blocks by heights: %w", err)
		}
		defer func() {
			if closeErr := rows.Close(); closeErr != nil && err == nil {
				err = xerrors.Errorf("failed to close rows: %w", closeErr)
			}
		}()

		blocks, err := model.BlockMetadataFromCanonicalRows(b.db, rows)
		if err != nil {
			return nil, xerrors.Errorf("failed to scan block rows: %w", err)
		}

		// Verify we got all requested blocks and return them in the same order as requested
		blockMap := make(map[uint64]*api.BlockMetadata)
		for _, block := range blocks {
			blockMap[block.Height] = block
		}

		orderedBlocks := make([]*api.BlockMetadata, len(heights))
		for i, height := range heights {
			block, exists := blockMap[height]
			if !exists {
				return nil, xerrors.Errorf("block at height %d not found: %w", height, errors.ErrItemNotFound)
			}
			orderedBlocks[i] = block
		}

		return orderedBlocks, nil
	})
}

func (b *blockStorageImpl) validateHeight(height uint64) error {
	if height < b.blockStartHeight {
		return xerrors.Errorf("height(%d) should be no less than blockStartHeight(%d): %w",
			height, b.blockStartHeight, errors.ErrInvalidHeight)
	}
	return nil
}

// GetWatermarkCount returns the number of watermarked blocks for monitoring purposes
// This metric helps track watermark accumulation and determine if cleanup is needed
func (b *blockStorageImpl) GetWatermarkCount(ctx context.Context, tag uint32) (int64, error) {
	var count int64
	query := `SELECT COUNT(*) FROM canonical_blocks WHERE tag = $1 AND is_watermark = TRUE`
	err := b.db.QueryRowContext(ctx, query, tag).Scan(&count)
	if err != nil {
		return 0, xerrors.Errorf("failed to get watermark count: %w", err)
	}
	return count, nil
}

func (b *blockStorageImpl) GetBlockByTimestamp(ctx context.Context, tag uint32, timestamp uint64) (*api.BlockMetadata, error) {
	return b.instrumentGetBlockByTimestamp.Instrument(ctx, func(ctx context.Context) (*api.BlockMetadata, error) {
		// Query to get the latest block before or at the given timestamp
		query := fmt.Sprintf(`
			SELECT %s
			FROM canonical_blocks cb
			JOIN block_metadata bm ON cb.block_metadata_id = bm.id
			WHERE cb.tag = $1 AND bm.timestamp <= $2
			ORDER BY bm.timestamp DESC, bm.height DESC
			LIMIT 1
		`, canonicalBlockMetadataColumns)

		var blockId int64
		var height uint64
		var blockTag uint32
		var hash, parentHash, objectKeyMain sql.NullString
		var parentHeight uint64
		var blockTimestamp int64
		var skipped bool
		var objectFormat int32
		var storageGeneration sql.NullString
		var byteOffset, byteLength, uncompressedLength sql.NullInt64

		err := b.db.QueryRowContext(ctx, query, tag, timestamp).Scan(
			&blockId, &height, &blockTag, &hash, &parentHash, &parentHeight, &objectKeyMain, &blockTimestamp, &skipped,
			&objectFormat, &byteOffset, &byteLength, &uncompressedLength, &storageGeneration)
		if err != nil {
			if err == sql.ErrNoRows {
				fmt.Printf("[DEBUG] No block found for tag=%d, timestamp=%d\n", tag, timestamp)
				return nil, xerrors.Errorf("no block found before timestamp %d: %w", timestamp, errors.ErrItemNotFound)
			}
			fmt.Printf("[DEBUG] Failed to get block by timestamp: %v\n", err)
			return nil, xerrors.Errorf("failed to get block by timestamp: %w", err)
		}

		if !hash.Valid || blockTimestamp == 0 {
			fmt.Printf("[DEBUG] Invalid block data: height=%d, blockTimestamp=%d, hash.Valid=%v\n", height, blockTimestamp, hash.Valid)
			return nil, xerrors.Errorf("no block found before timestamp %d: %w", timestamp, errors.ErrItemNotFound)
		}

		return &api.BlockMetadata{
			Tag:                blockTag,
			Hash:               hash.String,
			ParentHash:         parentHash.String,
			Height:             height,
			ParentHeight:       parentHeight,
			ObjectKeyMain:      objectKeyMain.String,
			Timestamp:          utils.ToTimestamp(blockTimestamp),
			Skipped:            skipped,
			ObjectFormat:       api.BlockObjectFormat(objectFormat),
			ByteOffset:         uint64Value(byteOffset),
			ByteLength:         uint64Value(byteLength),
			UncompressedLength: uint64Value(uncompressedLength),
			StorageGeneration:  storageGeneration.String,
		}, nil
	})
}

func (b *blockStorageImpl) GetBlockConsolidationShadow(ctx context.Context, block *api.BlockMetadata) (*api.BlockMetadata, error) {
	shadows, err := b.GetBlocksConsolidationShadow(ctx, []*api.BlockMetadata{block})
	if err != nil {
		return nil, err
	}
	if len(shadows) == 0 || shadows[0] == nil {
		return nil, xerrors.Errorf("consolidation shadow not found: %w", errors.ErrItemNotFound)
	}
	return shadows[0], nil
}

func (b *blockStorageImpl) GetBlocksConsolidationShadow(ctx context.Context, blocks []*api.BlockMetadata) ([]*api.BlockMetadata, error) {
	shadows := make([]*api.BlockMetadata, len(blocks))
	placeholders := make([]string, 0, len(blocks))
	args := make([]interface{}, 0, len(blocks)*6)
	nextArg := 1
	for i, block := range blocks {
		if block.GetSkipped() {
			continue
		}
		placeholders = append(placeholders, fmt.Sprintf(
			"($%d::int, $%d::int, $%d::bigint, $%d::varchar, $%d::text, $%d::text)",
			nextArg,
			nextArg+1,
			nextArg+2,
			nextArg+3,
			nextArg+4,
			nextArg+5,
		))
		args = append(args, i, block.GetTag(), block.GetHeight(), block.GetHash(), block.GetObjectKeyMain(), nullableStorageGeneration(block.GetStorageGeneration()))
		nextArg += 6
	}
	if len(placeholders) == 0 {
		return shadows, nil
	}

	query := fmt.Sprintf(`
		WITH input(ord, tag, height, hash, single_block_object_key_main, single_block_storage_generation) AS (
			VALUES %s
		)
		SELECT
			input.ord,
			shadow.consolidated_object_key_main,
			shadow.consolidated_storage_generation,
			shadow.object_format,
			shadow.byte_offset,
			shadow.byte_length,
			shadow.uncompressed_length
		FROM input
		LEFT JOIN LATERAL (
			SELECT consolidated_object_key_main, consolidated_storage_generation, object_format, byte_offset, byte_length, uncompressed_length
			FROM block_consolidation_shadow
			WHERE tag = input.tag
				AND height = input.height
				AND hash = input.hash
				AND single_block_object_key_main = input.single_block_object_key_main
				AND single_block_storage_generation IS NOT DISTINCT FROM input.single_block_storage_generation
				AND validated_at IS NOT NULL
			ORDER BY created_at DESC
			LIMIT 1
		) shadow ON true
		ORDER BY input.ord ASC`, strings.Join(placeholders, ", "))
	rows, err := b.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, xerrors.Errorf("failed to get consolidation shadows: %w", err)
	}
	defer func() {
		_ = rows.Close()
	}()

	for rows.Next() {
		var ord int
		var objectKey sql.NullString
		var storageGeneration sql.NullString
		var objectFormat sql.NullInt64
		var byteOffset, byteLength sql.NullInt64
		var uncompressedLength sql.NullInt64
		if err := rows.Scan(
			&ord,
			&objectKey,
			&storageGeneration,
			&objectFormat,
			&byteOffset,
			&byteLength,
			&uncompressedLength,
		); err != nil {
			return nil, xerrors.Errorf("failed to scan consolidation shadow: %w", err)
		}
		if !objectKey.Valid {
			continue
		}
		if ord < 0 || ord >= len(blocks) {
			return nil, xerrors.Errorf("consolidation shadow input order out of range: %d", ord)
		}
		shadows[ord] = makeConsolidationShadowBlockMetadata(
			blocks[ord],
			objectKey.String,
			storageGeneration.String,
			api.BlockObjectFormat(objectFormat.Int64),
			uint64Value(byteOffset),
			uint64Value(byteLength),
			uint64Value(uncompressedLength),
		)
	}
	if err := rows.Err(); err != nil {
		return nil, xerrors.Errorf("failed to iterate consolidation shadows: %w", err)
	}
	return shadows, nil
}

func (b *blockStorageImpl) GetBlocksMissingConsolidationShadow(ctx context.Context, tag uint32, startHeight, endHeight uint64, limit uint64) ([]*internal.BlockMetadataRecord, error) {
	if endHeight <= startHeight {
		return nil, nil
	}
	if limit == 0 {
		return nil, xerrors.New("consolidation shadow scan limit must be positive")
	}
	query := fmt.Sprintf(`
		SELECT %s
		FROM block_metadata bm
		JOIN canonical_blocks cb ON cb.block_metadata_id = bm.id
			AND cb.tag = bm.tag
			AND cb.height = bm.height
		WHERE bm.tag = $1
			AND bm.height >= $2
			AND bm.height < $3
			AND bm.skipped = false
			AND bm.byte_length IS NULL
			AND bm.object_key_main IS NOT NULL
			AND bm.object_key_main <> ''
			AND NOT EXISTS (
				SELECT 1
				FROM block_consolidation_shadow shadow
				WHERE shadow.block_metadata_id = bm.id
					AND shadow.single_block_object_key_main = bm.object_key_main
					AND shadow.single_block_storage_generation IS NOT DISTINCT FROM bm.storage_generation
					AND shadow.validated_at IS NOT NULL
			)
		ORDER BY bm.height ASC
		LIMIT $4`, canonicalBlockMetadataColumns)

	rows, err := b.db.QueryContext(ctx, query, tag, startHeight, endHeight, limit)
	if err != nil {
		return nil, xerrors.Errorf("failed to scan unconsolidated blocks: %w", err)
	}
	defer func() {
		_ = rows.Close()
	}()

	records := make([]*internal.BlockMetadataRecord, 0)
	for rows.Next() {
		var id int64
		var height uint64
		var blockTag uint32
		var hash, parentHash, objectKeyMain sql.NullString
		var parentHeight uint64
		var blockTimestamp int64
		var skipped bool
		var objectFormat int32
		var storageGeneration sql.NullString
		var byteOffset, byteLength, uncompressedLength sql.NullInt64
		if err := rows.Scan(
			&id,
			&height,
			&blockTag,
			&hash,
			&parentHash,
			&parentHeight,
			&objectKeyMain,
			&blockTimestamp,
			&skipped,
			&objectFormat,
			&byteOffset,
			&byteLength,
			&uncompressedLength,
			&storageGeneration,
		); err != nil {
			return nil, xerrors.Errorf("failed to scan unconsolidated block: %w", err)
		}
		records = append(records, &internal.BlockMetadataRecord{
			ID: id,
			Metadata: &api.BlockMetadata{
				Tag:                blockTag,
				Hash:               hash.String,
				ParentHash:         parentHash.String,
				Height:             height,
				ParentHeight:       parentHeight,
				ObjectKeyMain:      objectKeyMain.String,
				Timestamp:          utils.ToTimestamp(blockTimestamp),
				Skipped:            skipped,
				ObjectFormat:       api.BlockObjectFormat(objectFormat),
				ByteOffset:         uint64Value(byteOffset),
				ByteLength:         uint64Value(byteLength),
				UncompressedLength: uint64Value(uncompressedLength),
				StorageGeneration:  storageGeneration.String,
			},
		})
	}
	if err := rows.Err(); err != nil {
		return nil, xerrors.Errorf("failed to iterate unconsolidated blocks: %w", err)
	}
	return records, nil
}

func (b *blockStorageImpl) GetFirstBlockMissingConsolidationShadow(ctx context.Context, tag uint32, startHeight, endHeight uint64) (uint64, bool, error) {
	if endHeight <= startHeight {
		return 0, false, nil
	}
	const query = `
		SELECT bm.height
		FROM block_metadata bm
		JOIN canonical_blocks cb ON cb.block_metadata_id = bm.id
			AND cb.tag = bm.tag
			AND cb.height = bm.height
		WHERE bm.tag = $1
			AND bm.height >= $2
			AND bm.height < $3
			AND bm.skipped = false
			AND bm.byte_length IS NULL
			AND bm.object_key_main IS NOT NULL
			AND bm.object_key_main <> ''
			AND NOT EXISTS (
				SELECT 1
				FROM block_consolidation_shadow shadow
				WHERE shadow.block_metadata_id = bm.id
					AND shadow.single_block_object_key_main = bm.object_key_main
					AND shadow.single_block_storage_generation IS NOT DISTINCT FROM bm.storage_generation
					AND shadow.validated_at IS NOT NULL
			)
		ORDER BY bm.height ASC
		LIMIT 1`
	var height uint64
	if err := b.db.QueryRowContext(ctx, query, tag, startHeight, endHeight).Scan(&height); err != nil {
		if err == sql.ErrNoRows {
			return 0, false, nil
		}
		return 0, false, xerrors.Errorf("failed to get first block missing consolidation shadow: %w", err)
	}
	return height, true, nil
}

func (b *blockStorageImpl) GetBlockConsolidationShadowStats(ctx context.Context, tag uint32, startHeight, endHeight uint64) (*internal.ConsolidationShadowStats, error) {
	if endHeight <= startHeight {
		return &internal.ConsolidationShadowStats{}, nil
	}
	const query = `
		WITH eligible AS (
			SELECT
				bm.id,
				bm.tag,
				bm.height,
				bm.hash,
				bm.object_key_main,
				bm.storage_generation,
				bm.object_format,
				bm.byte_offset,
				bm.byte_length,
				bm.uncompressed_length
			FROM canonical_blocks cb
			JOIN block_metadata bm ON bm.id = cb.block_metadata_id
			WHERE cb.tag = $1
				AND cb.height >= $2
				AND cb.height < $3
				AND bm.skipped = false
				AND bm.object_key_main IS NOT NULL
				AND bm.object_key_main <> ''
		)
		SELECT
			COUNT(DISTINCT (shadow.consolidated_storage_generation, shadow.consolidated_object_key_main))
				FILTER (WHERE shadow.block_metadata_id IS NOT NULL),
			COUNT(shadow.block_metadata_id),
			COUNT(*)
		FROM eligible
		LEFT JOIN block_consolidation_shadow shadow ON shadow.block_metadata_id = eligible.id
			AND shadow.tag = eligible.tag
			AND shadow.height = eligible.height
			AND shadow.hash = eligible.hash
			AND shadow.tag = $1
			AND shadow.height >= $2
			AND shadow.height < $3
			AND (
				(
					shadow.single_block_object_key_main = eligible.object_key_main
					AND shadow.single_block_storage_generation IS NOT DISTINCT FROM eligible.storage_generation
					AND eligible.byte_length IS NULL
					AND shadow.object_format = $4
					AND shadow.byte_offset >= 0
					AND shadow.byte_length > 0
					AND shadow.uncompressed_length IS NOT NULL
					AND shadow.uncompressed_length > 0
				)
				OR (
					eligible.object_key_main = shadow.consolidated_object_key_main
					AND eligible.storage_generation IS NOT DISTINCT FROM shadow.consolidated_storage_generation
					AND eligible.object_format = $4
					AND eligible.object_format = shadow.object_format
					AND eligible.byte_offset = shadow.byte_offset
					AND eligible.byte_length = shadow.byte_length
					AND eligible.byte_length > 0
					AND eligible.uncompressed_length = shadow.uncompressed_length
					AND eligible.uncompressed_length IS NOT NULL
					AND eligible.uncompressed_length > 0
				)
			)
			AND shadow.validated_at IS NOT NULL
			AND shadow.consolidated_object_key_main IS NOT NULL
			AND shadow.consolidated_object_key_main <> ''`

	var objects, blocks, eligibleBlocks uint64
	if err := b.db.QueryRowContext(ctx, query, tag, startHeight, endHeight, int32(api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_CSCB_BATCH)).Scan(&objects, &blocks, &eligibleBlocks); err != nil {
		return nil, xerrors.Errorf("failed to get consolidation shadow stats: %w", err)
	}
	return &internal.ConsolidationShadowStats{
		Objects:        objects,
		Blocks:         blocks,
		EligibleBlocks: eligibleBlocks,
	}, nil
}

func (b *blockStorageImpl) GetFirstPromotableBlockConsolidationShadow(ctx context.Context, tag uint32, startHeight, endHeight uint64) (uint64, bool, error) {
	if endHeight <= startHeight {
		return 0, false, nil
	}
	const query = `
		SELECT shadow.height
		FROM block_consolidation_shadow shadow
		JOIN block_metadata bm ON bm.id = shadow.block_metadata_id
			AND shadow.tag = bm.tag
			AND shadow.height = bm.height
			AND shadow.hash = bm.hash
			AND shadow.single_block_object_key_main = bm.object_key_main
			AND shadow.single_block_storage_generation IS NOT DISTINCT FROM bm.storage_generation
		JOIN canonical_blocks cb ON cb.block_metadata_id = bm.id
			AND cb.tag = bm.tag
			AND cb.height = bm.height
		WHERE shadow.tag = $1
			AND shadow.height >= $2
			AND shadow.height < $3
			AND bm.skipped = false
			AND bm.byte_length IS NULL
			AND bm.object_key_main IS NOT NULL
			AND bm.object_key_main <> ''
			AND shadow.validated_at IS NOT NULL
			AND shadow.consolidated_object_key_main IS NOT NULL
			AND shadow.consolidated_object_key_main <> ''
			AND shadow.object_format = $4
			AND shadow.byte_offset >= 0
			AND shadow.byte_length > 0
			AND shadow.uncompressed_length IS NOT NULL
			AND shadow.uncompressed_length > 0
		ORDER BY shadow.height ASC
		LIMIT 1`
	var height uint64
	if err := b.db.QueryRowContext(
		ctx,
		query,
		tag,
		startHeight,
		endHeight,
		int32(api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_CSCB_BATCH),
	).Scan(&height); err != nil {
		if err == sql.ErrNoRows {
			return 0, false, nil
		}
		return 0, false, xerrors.Errorf("failed to get first promotable consolidation shadow: %w", err)
	}
	return height, true, nil
}

func (b *blockStorageImpl) GetBlockConsolidationCursor(ctx context.Context, name string, tag uint32) (uint64, bool, error) {
	if name == "" {
		return 0, false, xerrors.New("block consolidation cursor name must be non-empty")
	}
	const query = `
		SELECT height
		FROM block_consolidation_cursor
		WHERE name = $1 AND tag = $2`
	var height uint64
	if err := b.db.QueryRowContext(ctx, query, name, tag).Scan(&height); err != nil {
		if err == sql.ErrNoRows {
			return 0, false, nil
		}
		return 0, false, xerrors.Errorf("failed to get block consolidation cursor %q for tag %d: %w", name, tag, err)
	}
	return height, true, nil
}

func (b *blockStorageImpl) SetBlockConsolidationCursor(ctx context.Context, name string, tag uint32, height uint64) error {
	if name == "" {
		return xerrors.New("block consolidation cursor name must be non-empty")
	}
	const query = `
		INSERT INTO block_consolidation_cursor (name, tag, height, updated_at)
		VALUES ($1, $2, $3, NOW())
		ON CONFLICT (name, tag) DO UPDATE SET
			height = GREATEST(block_consolidation_cursor.height, EXCLUDED.height),
			updated_at = NOW()`
	if _, err := b.db.ExecContext(ctx, query, name, tag, height); err != nil {
		return xerrors.Errorf("failed to set block consolidation cursor %q for tag %d to height %d: %w", name, tag, height, err)
	}
	return nil
}

func (b *blockStorageImpl) ResetBlockConsolidationCursor(ctx context.Context, name string, tag uint32, height uint64) error {
	if name == "" {
		return xerrors.New("block consolidation cursor name must be non-empty")
	}
	// Exact write, no GREATEST: this is the lowering path reconciliation uses
	// for the retention floor watermark (see ResetBlockConsolidationCursor on
	// the interface). Nothing else should need a cursor to move backwards.
	const query = `
		INSERT INTO block_consolidation_cursor (name, tag, height, updated_at)
		VALUES ($1, $2, $3, NOW())
		ON CONFLICT (name, tag) DO UPDATE SET
			height = EXCLUDED.height,
			updated_at = NOW()`
	if _, err := b.db.ExecContext(ctx, query, name, tag, height); err != nil {
		return xerrors.Errorf("failed to reset block consolidation cursor %q for tag %d to height %d: %w", name, tag, height, err)
	}
	return nil
}

func (b *blockStorageImpl) PersistBlockConsolidationShadows(ctx context.Context, placements []*internal.ConsolidationShadowPlacement) error {
	if len(placements) == 0 {
		return nil
	}
	nonNilPlacements := make([]*internal.ConsolidationShadowPlacement, 0, len(placements))
	for _, placement := range placements {
		if placement != nil {
			nonNilPlacements = append(nonNilPlacements, placement)
		}
	}
	if len(nonNilPlacements) == 0 {
		return nil
	}

	tags := make([]uint32, 0, len(nonNilPlacements))
	blockMetadataIDs := make([]int64, 0, len(nonNilPlacements))
	tagValues := make([]int64, 0, len(nonNilPlacements))
	heights := make([]int64, 0, len(nonNilPlacements))
	hashes := make([]string, 0, len(nonNilPlacements))
	singleBlockObjectKeys := make([]string, 0, len(nonNilPlacements))
	consolidatedObjectKeys := make([]string, 0, len(nonNilPlacements))
	singleBlockStorageGenerations := make([]string, 0, len(nonNilPlacements))
	consolidatedStorageGenerations := make([]string, 0, len(nonNilPlacements))
	objectFormats := make([]int64, 0, len(nonNilPlacements))
	byteOffsets := make([]int64, 0, len(nonNilPlacements))
	byteLengths := make([]int64, 0, len(nonNilPlacements))
	uncompressedLengths := make([]int64, 0, len(nonNilPlacements))
	for _, placement := range nonNilPlacements {
		tags = append(tags, placement.Tag)
		blockMetadataIDs = append(blockMetadataIDs, placement.BlockMetadataID)
		tagValues = append(tagValues, int64(placement.Tag))
		heights = append(heights, int64(placement.Height))
		hashes = append(hashes, placement.Hash)
		singleBlockObjectKeys = append(singleBlockObjectKeys, placement.SingleBlockObjectKeyMain)
		consolidatedObjectKeys = append(consolidatedObjectKeys, placement.ConsolidatedObjectKeyMain)
		singleBlockStorageGenerations = append(singleBlockStorageGenerations, placement.SingleBlockStorageGeneration)
		consolidatedStorageGenerations = append(consolidatedStorageGenerations, placement.ConsolidatedStorageGeneration)
		objectFormats = append(objectFormats, int64(placement.ObjectFormat))
		byteOffsets = append(byteOffsets, int64(placement.ByteOffset))
		byteLengths = append(byteLengths, int64(placement.ByteLength))
		uncompressedLengths = append(uncompressedLengths, int64(placement.UncompressedLength))
	}

	tx, err := b.db.BeginTx(ctx, nil)
	if err != nil {
		return xerrors.Errorf("failed to begin consolidation shadow transaction: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()
	if err := cscbrepairlock.AcquireTagsShared(ctx, tx, tags); err != nil {
		return err
	}

	const query = `
		WITH input AS (
			SELECT
				placement.block_metadata_id,
				placement.tag,
				placement.height,
				placement.hash,
				placement.single_block_object_key_main,
				placement.consolidated_object_key_main,
				NULLIF(placement.single_block_storage_generation, '') AS single_block_storage_generation,
				NULLIF(placement.consolidated_storage_generation, '') AS consolidated_storage_generation,
				placement.object_format,
				placement.byte_offset,
				placement.byte_length,
				placement.uncompressed_length,
				placement.ordinal
			FROM UNNEST(
				$1::BIGINT[],
				$2::INTEGER[],
				$3::BIGINT[],
				$4::TEXT[],
				$5::TEXT[],
				$6::TEXT[],
				$7::TEXT[],
				$8::TEXT[],
				$9::SMALLINT[],
				$10::BIGINT[],
				$11::BIGINT[],
				$12::BIGINT[]
			) WITH ORDINALITY AS placement(
				block_metadata_id,
				tag,
				height,
				hash,
				single_block_object_key_main,
				consolidated_object_key_main,
				single_block_storage_generation,
				consolidated_storage_generation,
				object_format,
				byte_offset,
				byte_length,
				uncompressed_length,
				ordinal
			)
		), matched AS MATERIALIZED (
			SELECT input.*
			FROM input
			JOIN block_metadata bm
				ON bm.id = input.block_metadata_id
				AND bm.tag = input.tag
				AND bm.height = input.height
				AND bm.hash = input.hash
				AND bm.object_key_main = input.single_block_object_key_main
				AND bm.storage_generation IS NOT DISTINCT FROM input.single_block_storage_generation
				AND bm.skipped = false
			FOR UPDATE OF bm
		), upserted AS (
			INSERT INTO block_consolidation_shadow (
			block_metadata_id, tag, height, hash, single_block_object_key_main, consolidated_object_key_main,
			single_block_storage_generation, consolidated_storage_generation,
			object_format, byte_offset, byte_length, uncompressed_length, validated_at
		)
		SELECT
			block_metadata_id, tag, height, hash, single_block_object_key_main, consolidated_object_key_main,
			single_block_storage_generation, consolidated_storage_generation,
			object_format, byte_offset, byte_length, uncompressed_length, NOW()
		FROM matched
		ON CONFLICT (block_metadata_id) DO UPDATE SET
			tag = EXCLUDED.tag,
			height = EXCLUDED.height,
			hash = EXCLUDED.hash,
			single_block_object_key_main = EXCLUDED.single_block_object_key_main,
			consolidated_object_key_main = EXCLUDED.consolidated_object_key_main,
			single_block_storage_generation = EXCLUDED.single_block_storage_generation,
			consolidated_storage_generation = EXCLUDED.consolidated_storage_generation,
			object_format = EXCLUDED.object_format,
			byte_offset = EXCLUDED.byte_offset,
			byte_length = EXCLUDED.byte_length,
			uncompressed_length = EXCLUDED.uncompressed_length,
			single_block_retention_started_at = CASE
				WHEN block_consolidation_shadow.single_block_object_key_main = EXCLUDED.single_block_object_key_main
					AND block_consolidation_shadow.single_block_storage_generation IS NOT DISTINCT FROM EXCLUDED.single_block_storage_generation
					THEN block_consolidation_shadow.single_block_retention_started_at
				ELSE NULL
			END,
			single_block_delete_after = CASE
				WHEN block_consolidation_shadow.single_block_object_key_main = EXCLUDED.single_block_object_key_main
					AND block_consolidation_shadow.single_block_storage_generation IS NOT DISTINCT FROM EXCLUDED.single_block_storage_generation
					THEN block_consolidation_shadow.single_block_delete_after
				ELSE NULL
			END,
			validated_at = EXCLUDED.validated_at
		WHERE block_consolidation_shadow.single_block_object_deleted_at IS NULL
			RETURNING block_metadata_id
		)
		SELECT
			(SELECT COUNT(*) FROM input),
			(SELECT COUNT(*) FROM matched),
			(SELECT COUNT(*) FROM upserted),
			COALESCE((
				SELECT input.block_metadata_id
				FROM input
				LEFT JOIN matched USING (ordinal)
				WHERE matched.ordinal IS NULL
				ORDER BY input.ordinal
				LIMIT 1
			), 0),
			COALESCE((
				SELECT input.height
				FROM input
				LEFT JOIN matched USING (ordinal)
				WHERE matched.ordinal IS NULL
				ORDER BY input.ordinal
				LIMIT 1
			), 0),
			COALESCE((
				SELECT matched.block_metadata_id
				FROM matched
				LEFT JOIN upserted USING (block_metadata_id)
				WHERE upserted.block_metadata_id IS NULL
				ORDER BY matched.ordinal
				LIMIT 1
			), 0),
			COALESCE((
				SELECT matched.height
				FROM matched
				LEFT JOIN upserted USING (block_metadata_id)
				WHERE upserted.block_metadata_id IS NULL
				ORDER BY matched.ordinal
				LIMIT 1
			), 0)`

	var inputCount int64
	var matchedCount int64
	var upsertedCount int64
	var unmatchedMetadataID int64
	var unmatchedHeight int64
	var rejectedMetadataID int64
	var rejectedHeight int64
	if err := tx.QueryRowContext(
		ctx,
		query,
		pq.Array(blockMetadataIDs),
		pq.Array(tagValues),
		pq.Array(heights),
		pq.Array(hashes),
		pq.Array(singleBlockObjectKeys),
		pq.Array(consolidatedObjectKeys),
		pq.Array(singleBlockStorageGenerations),
		pq.Array(consolidatedStorageGenerations),
		pq.Array(objectFormats),
		pq.Array(byteOffsets),
		pq.Array(byteLengths),
		pq.Array(uncompressedLengths),
	).Scan(
		&inputCount,
		&matchedCount,
		&upsertedCount,
		&unmatchedMetadataID,
		&unmatchedHeight,
		&rejectedMetadataID,
		&rejectedHeight,
	); err != nil {
		return xerrors.Errorf("failed to persist consolidation shadows: %w", err)
	}
	if inputCount != int64(len(nonNilPlacements)) {
		return xerrors.Errorf("failed to persist consolidation shadows: expected %d inputs, got %d", len(nonNilPlacements), inputCount)
	}
	if matchedCount != inputCount {
		return xerrors.Errorf("failed to persist consolidation shadow for metadata_id=%d height=%d: primary metadata identity mismatch", unmatchedMetadataID, unmatchedHeight)
	}
	if upsertedCount != matchedCount {
		return xerrors.Errorf("failed to persist consolidation shadow for metadata_id=%d height=%d: existing shadow is not writable", rejectedMetadataID, rejectedHeight)
	}
	if err := tx.Commit(); err != nil {
		return xerrors.Errorf("failed to commit consolidation shadows: %w", err)
	}
	committed = true
	return nil
}

// promotionInvalidShadowQuery and promotionQuery repeat the canonical range on
// block_metadata (bm.tag, bm.height). Their byte_length IS NULL and
// skipped = false predicates match idx_block_metadata_unconsolidated
// (tag, height), and unpromoted rows are a thin sliver clustered at the end of
// the table, so ANALYZE can sample none of them and record
// null_frac(byte_length) = 0, as robinhood-mainnet's 02:03Z autoanalyze did on
// 2026-10-08. The planner then estimates that index at one row and, without
// bm.height, drives the join from it with no index condition, so the scan
// covers every unpromoted row: a backlog makes each attempt slower than the
// last, and every promotion hit the 60s statement_timeout with 821,921 rows
// behind it a day later. Bounded, the same selection reads only the window
// (10,000 rows in 50ms on robinhood). Bound block_metadata only: the same range
// on block_consolidation_shadow becomes its index condition and turns the
// shadow lookup into a nested range scan (15.7s for the same window).
//
// The updated CTE in promotionQuery deliberately has no byte_length IS NULL
// re-check: that predicate let the same unbounded index drive a nested loop
// against the candidates CTE. Candidates are already selected with it and
// locked FOR UPDATE, which re-evaluates it against the latest row version, so
// the update reaches each row by primary key instead, and
// PromoteBlockConsolidationShadows still requires every candidate to be
// promoted and marked for retirement exactly once.
const promotionInvalidShadowQuery = `
		SELECT shadow.block_metadata_id, shadow.height
		FROM canonical_blocks cb
		JOIN block_metadata bm ON bm.id = cb.block_metadata_id
		JOIN block_consolidation_shadow shadow ON shadow.block_metadata_id = bm.id
			AND shadow.tag = bm.tag
			AND shadow.height = bm.height
			AND shadow.hash = bm.hash
			AND shadow.single_block_object_key_main = bm.object_key_main
			AND shadow.single_block_storage_generation IS NOT DISTINCT FROM bm.storage_generation
		WHERE cb.tag = $1
			AND cb.height >= $2
			AND cb.height < $3
			AND bm.tag = $1
			AND bm.height >= $2
			AND bm.height < $3
			AND bm.skipped = false
			AND bm.byte_length IS NULL
			AND bm.object_key_main IS NOT NULL
			AND bm.object_key_main <> ''
			AND shadow.validated_at IS NOT NULL
			AND (
				shadow.consolidated_object_key_main IS NULL
				OR shadow.consolidated_object_key_main = ''
				OR shadow.object_format <> $4
				OR shadow.byte_offset < 0
				OR shadow.byte_length <= 0
				OR shadow.uncompressed_length IS NULL
				OR shadow.uncompressed_length <= 0
			)
		ORDER BY cb.height ASC
		LIMIT 1`

// promotionQuery: see promotionInvalidShadowQuery.
const promotionQuery = `
		WITH database_clock AS (
			SELECT clock_timestamp() AS retention_started_at
		), candidates AS (
			SELECT
				bm.id AS block_metadata_id,
				bm.tag,
				bm.height,
				bm.hash,
				bm.object_key_main AS single_block_object_key_main,
				bm.storage_generation AS single_block_storage_generation,
				shadow.consolidated_object_key_main,
				shadow.consolidated_storage_generation,
				shadow.object_format,
				shadow.byte_offset,
				shadow.byte_length,
				shadow.uncompressed_length
			FROM canonical_blocks cb
			JOIN block_metadata bm ON bm.id = cb.block_metadata_id
			JOIN block_consolidation_shadow shadow ON shadow.block_metadata_id = bm.id
				AND shadow.tag = bm.tag
				AND shadow.height = bm.height
				AND shadow.hash = bm.hash
				AND shadow.single_block_object_key_main = bm.object_key_main
				AND shadow.single_block_storage_generation IS NOT DISTINCT FROM bm.storage_generation
			WHERE cb.tag = $1
				AND cb.height >= $2
				AND cb.height < $3
				AND bm.tag = $1
				AND bm.height >= $2
				AND bm.height < $3
				AND bm.skipped = false
				AND bm.byte_length IS NULL
				AND bm.object_key_main IS NOT NULL
				AND bm.object_key_main <> ''
				AND shadow.validated_at IS NOT NULL
				AND shadow.consolidated_object_key_main IS NOT NULL
				AND shadow.consolidated_object_key_main <> ''
				AND shadow.object_format = $5
				AND shadow.byte_offset >= 0
				AND shadow.byte_length > 0
				AND shadow.uncompressed_length IS NOT NULL
				AND shadow.uncompressed_length > 0
			ORDER BY cb.height ASC
			LIMIT $4
			FOR UPDATE OF cb, bm, shadow
		),
		updated AS (
			UPDATE block_metadata bm
			SET
				object_key_main = candidates.consolidated_object_key_main,
				storage_generation = candidates.consolidated_storage_generation,
				object_format = candidates.object_format,
				byte_offset = candidates.byte_offset,
				byte_length = candidates.byte_length,
				uncompressed_length = candidates.uncompressed_length
			FROM candidates
			WHERE bm.id = candidates.block_metadata_id
				AND bm.tag = candidates.tag
				AND bm.height = candidates.height
				AND bm.hash = candidates.hash
				AND bm.object_key_main = candidates.single_block_object_key_main
				AND bm.storage_generation IS NOT DISTINCT FROM candidates.single_block_storage_generation
				AND bm.skipped = false
			RETURNING bm.id
		),
		retired AS (
			UPDATE block_consolidation_shadow shadow
			SET
				single_block_retention_started_at = database_clock.retention_started_at,
				single_block_delete_after = database_clock.retention_started_at + ($6 * INTERVAL '1 microsecond')
			FROM candidates, database_clock
			WHERE shadow.block_metadata_id = candidates.block_metadata_id
				AND shadow.tag = candidates.tag
				AND shadow.height = candidates.height
				AND shadow.hash = candidates.hash
				AND shadow.single_block_object_key_main = candidates.single_block_object_key_main
				AND shadow.single_block_storage_generation IS NOT DISTINCT FROM candidates.single_block_storage_generation
			RETURNING shadow.block_metadata_id
		)
		SELECT
			(SELECT COUNT(*) FROM candidates),
			(SELECT COUNT(*) FROM updated),
			(SELECT COUNT(*) FROM retired)`

func (b *blockStorageImpl) PromoteBlockConsolidationShadows(
	ctx context.Context,
	tag uint32,
	startHeight uint64,
	endHeight uint64,
	limit uint64,
	singleBlockObjectRetention time.Duration,
) (*internal.ConsolidationPromotionResult, error) {
	if endHeight <= startHeight {
		return &internal.ConsolidationPromotionResult{}, nil
	}
	if limit == 0 {
		return nil, xerrors.New("consolidation promotion limit must be positive")
	}
	if singleBlockObjectRetention <= 0 {
		return nil, xerrors.New("single-block object retention must be positive")
	}
	if err := b.validateHeight(startHeight); err != nil {
		return nil, err
	}
	tx, err := b.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, xerrors.Errorf("failed to begin consolidation promotion transaction: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()
	if err := cscbrepairlock.AcquireTagShared(ctx, tx, tag); err != nil {
		return nil, err
	}

	var invalidMetadataID int64
	var invalidHeight uint64
	err = tx.QueryRowContext(
		ctx,
		promotionInvalidShadowQuery,
		tag,
		startHeight,
		endHeight,
		int32(api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_CSCB_BATCH),
	).Scan(&invalidMetadataID, &invalidHeight)
	if err != nil && err != sql.ErrNoRows {
		return nil, xerrors.Errorf("failed to validate consolidation promotion shadows: %w", err)
	}
	if err == nil {
		return nil, xerrors.Errorf("invalid consolidation shadow metadata for metadata_id=%d height=%d", invalidMetadataID, invalidHeight)
	}

	var candidates, promoted, retired uint64
	if err := tx.QueryRowContext(
		ctx,
		promotionQuery,
		tag,
		startHeight,
		endHeight,
		limit,
		int32(api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_CSCB_BATCH),
		singleBlockObjectRetention.Microseconds(),
	).Scan(&candidates, &promoted, &retired); err != nil {
		return nil, xerrors.Errorf("failed to promote consolidation shadows: %w", err)
	}
	if candidates != promoted {
		return nil, xerrors.Errorf("consolidation promotion guard failed: selected %d rows but promoted %d", candidates, promoted)
	}
	if candidates != retired {
		return nil, xerrors.Errorf("consolidation retirement guard failed: selected %d rows but marked %d for retirement", candidates, retired)
	}
	if err := tx.Commit(); err != nil {
		return nil, xerrors.Errorf("failed to commit consolidation promotion: %w", err)
	}
	committed = true
	return &internal.ConsolidationPromotionResult{
		Blocks: promoted,
	}, nil
}

func makeConsolidationShadowBlockMetadata(
	block *api.BlockMetadata,
	objectKey string,
	storageGeneration string,
	objectFormat api.BlockObjectFormat,
	byteOffset uint64,
	byteLength uint64,
	uncompressedLength uint64,
) *api.BlockMetadata {
	shadow := proto.Clone(block).(*api.BlockMetadata)
	shadow.ObjectKeyMain = objectKey
	shadow.StorageGeneration = storageGeneration
	shadow.ObjectFormat = objectFormat
	shadow.ByteOffset = byteOffset
	shadow.ByteLength = byteLength
	shadow.UncompressedLength = uncompressedLength
	return shadow
}

func uint64Value(value sql.NullInt64) uint64 {
	if !value.Valid {
		return 0
	}
	return uint64(value.Int64)
}

func nullableStorageGeneration(generation string) interface{} {
	if generation == "" {
		return nil
	}
	return generation
}

// persistBlockMetasChunkSize bounds the number of rows in one set-based statement issued by
// PersistBlockMetas. It is a variable so benchmarks can sweep it.
var persistBlockMetasChunkSize = 1000

type (
	blockHashKey struct {
		tag  uint32
		hash string
	}

	blockHeightKey struct {
		tag    uint32
		height uint64
	}

	canonicalRow struct {
		height          uint64
		blockMetadataID int64
		tag             uint32
	}

	// persistedBlockIDs maps each upserted block_metadata row back to its id using the
	// conflict key of the partial unique index the row was upserted under.
	persistedBlockIDs struct {
		regular map[blockHashKey]int64
		skipped map[blockHeightKey]int64
	}
)

func newPersistedBlockIDs(capacity int) *persistedBlockIDs {
	return &persistedBlockIDs{
		regular: make(map[blockHashKey]int64, capacity),
		skipped: make(map[blockHeightKey]int64, capacity),
	}
}

func (p *persistedBlockIDs) lookup(block *api.BlockMetadata) (int64, bool) {
	if block.Skipped {
		id, ok := p.skipped[skippedBlockKey(block)]
		return id, ok
	}
	id, ok := p.regular[regularBlockKey(block)]
	return id, ok
}

// regularBlockKey is the conflict key of unique_tag_hash_regular.
func regularBlockKey(block *api.BlockMetadata) blockHashKey {
	return blockHashKey{tag: block.Tag, hash: block.Hash}
}

// skippedBlockKey is the conflict key of unique_tag_height_skipped.
func skippedBlockKey(block *api.BlockMetadata) blockHeightKey {
	return blockHeightKey{tag: block.Tag, height: block.Height}
}

func chunkSlice[T any](items []T, size int) [][]T {
	if size <= 0 {
		size = len(items)
	}
	if len(items) == 0 {
		return nil
	}
	chunks := make([][]T, 0, (len(items)+size-1)/size)
	for start := 0; start < len(items); start += size {
		end := start + size
		if end > len(items) {
			end = len(items)
		}
		chunks = append(chunks, items[start:end])
	}
	return chunks
}

// partitionBlocksForPersist splits blocks into regular and skipped groups, preserving order.
func partitionBlocksForPersist(blocks []*api.BlockMetadata) (regular []*api.BlockMetadata, skipped []*api.BlockMetadata) {
	regular = make([]*api.BlockMetadata, 0, len(blocks))
	for _, block := range blocks {
		if block.Skipped {
			skipped = append(skipped, block)
		} else {
			regular = append(regular, block)
		}
	}
	return regular, skipped
}

// dedupeBlocksKeepLast drops every block whose key reappears later in the slice. The order of the
// surviving blocks follows the position of their last occurrence.
func dedupeBlocksKeepLast[K comparable](blocks []*api.BlockMetadata, key func(*api.BlockMetadata) K) []*api.BlockMetadata {
	last := make(map[K]int, len(blocks))
	for i, block := range blocks {
		last[key(block)] = i
	}
	if len(last) == len(blocks) {
		return blocks
	}
	deduped := make([]*api.BlockMetadata, 0, len(last))
	for i, block := range blocks {
		if last[key(block)] == i {
			deduped = append(deduped, block)
		}
	}
	return deduped
}

const blockMetadataUpsertSource = `
		INSERT INTO block_metadata (
			height, tag, hash, parent_hash, parent_height, object_key_main, timestamp, skipped,
			object_format, byte_offset, byte_length, uncompressed_length, storage_generation
		)
		SELECT
			input.height, input.tag, input.hash, input.parent_hash, input.parent_height, input.object_key_main,
			input.timestamp, input.skipped, input.object_format, input.byte_offset, input.byte_length,
			input.uncompressed_length, NULLIF(input.storage_generation, '')
		FROM UNNEST(
			$1::BIGINT[], $2::INTEGER[], $3::TEXT[], $4::TEXT[], $5::BIGINT[], $6::TEXT[], $7::BIGINT[],
			$8::BOOLEAN[], $9::SMALLINT[], $10::BIGINT[], $11::BIGINT[], $12::BIGINT[], $13::TEXT[]
		) WITH ORDINALITY AS input(
			height, tag, hash, parent_hash, parent_height, object_key_main, timestamp, skipped,
			object_format, byte_offset, byte_length, uncompressed_length, storage_generation, ordinal
		)
		ORDER BY input.ordinal`

// blockMetadataUpsertQuery returns the set-based upsert for one group of blocks. The two groups use
// different partial unique indexes and therefore different conflict resolution. Every input array
// must have the same length; rows are processed in array order.
func blockMetadataUpsertQuery(skipped bool) string {
	if skipped {
		return blockMetadataUpsertSource + `
		ON CONFLICT (tag, height) WHERE skipped = true DO UPDATE SET
			hash = EXCLUDED.hash,
			parent_hash = EXCLUDED.parent_hash,
			parent_height = EXCLUDED.parent_height,
			object_key_main = EXCLUDED.object_key_main,
			timestamp = EXCLUDED.timestamp,
			skipped = EXCLUDED.skipped,
			object_format = EXCLUDED.object_format,
			byte_offset = EXCLUDED.byte_offset,
			byte_length = EXCLUDED.byte_length,
			uncompressed_length = EXCLUDED.uncompressed_length,
			storage_generation = EXCLUDED.storage_generation
		RETURNING id, tag, height`
	}
	// A block whose single-block object has been fenced for retirement, or that is pinned by a CSCB
	// repair, keeps its current placement. The fence and repair triggers enforce the same invariant.
	return blockMetadataUpsertSource + `
		ON CONFLICT (tag, hash) WHERE hash IS NOT NULL AND NOT skipped DO UPDATE SET
			parent_hash = EXCLUDED.parent_hash,
			parent_height = EXCLUDED.parent_height,
			object_key_main = CASE
				WHEN block_metadata.single_block_retention_fenced_at IS NOT NULL
					OR EXISTS (
						SELECT 1 FROM cscb_repair_block repair_block
						WHERE repair_block.block_metadata_id = block_metadata.id
					) THEN block_metadata.object_key_main
				ELSE EXCLUDED.object_key_main
			END,
			timestamp = EXCLUDED.timestamp,
			skipped = EXCLUDED.skipped,
			object_format = CASE
				WHEN block_metadata.single_block_retention_fenced_at IS NOT NULL
					OR EXISTS (
						SELECT 1 FROM cscb_repair_block repair_block
						WHERE repair_block.block_metadata_id = block_metadata.id
					) THEN block_metadata.object_format
				ELSE EXCLUDED.object_format
			END,
			byte_offset = CASE
				WHEN block_metadata.single_block_retention_fenced_at IS NOT NULL
					OR EXISTS (
						SELECT 1 FROM cscb_repair_block repair_block
						WHERE repair_block.block_metadata_id = block_metadata.id
					) THEN block_metadata.byte_offset
				ELSE EXCLUDED.byte_offset
			END,
			byte_length = CASE
				WHEN block_metadata.single_block_retention_fenced_at IS NOT NULL
					OR EXISTS (
						SELECT 1 FROM cscb_repair_block repair_block
						WHERE repair_block.block_metadata_id = block_metadata.id
					) THEN block_metadata.byte_length
				ELSE EXCLUDED.byte_length
			END,
			uncompressed_length = CASE
				WHEN block_metadata.single_block_retention_fenced_at IS NOT NULL
					OR EXISTS (
						SELECT 1 FROM cscb_repair_block repair_block
						WHERE repair_block.block_metadata_id = block_metadata.id
					) THEN block_metadata.uncompressed_length
				ELSE EXCLUDED.uncompressed_length
			END,
			storage_generation = CASE
				WHEN block_metadata.single_block_retention_fenced_at IS NOT NULL
					OR EXISTS (
						SELECT 1 FROM cscb_repair_block repair_block
						WHERE repair_block.block_metadata_id = block_metadata.id
					) THEN block_metadata.storage_generation
				ELSE EXCLUDED.storage_generation
			END
		RETURNING id, tag, hash`
}

const canonicalBlocksUpsertQuery = `
		INSERT INTO canonical_blocks (height, block_metadata_id, tag)
		SELECT input.height, input.block_metadata_id, input.tag
		FROM UNNEST($1::BIGINT[], $2::BIGINT[], $3::INTEGER[]) WITH ORDINALITY
			AS input(height, block_metadata_id, tag, ordinal)
		ORDER BY input.ordinal
		ON CONFLICT (height, tag) DO UPDATE
		SET block_metadata_id = EXCLUDED.block_metadata_id`

// blockMetadataColumnArrays builds the 13 array parameters of blockMetadataUpsertQuery.
func blockMetadataColumnArrays(blocks []*api.BlockMetadata) []interface{} {
	n := len(blocks)
	heights := make([]int64, 0, n)
	tags := make([]int64, 0, n)
	hashes := make([]string, 0, n)
	parentHashes := make([]string, 0, n)
	parentHeights := make([]int64, 0, n)
	objectKeys := make([]string, 0, n)
	timestamps := make([]int64, 0, n)
	skipped := make([]bool, 0, n)
	objectFormats := make([]int64, 0, n)
	byteOffsets := make([]sql.NullInt64, 0, n)
	byteLengths := make([]sql.NullInt64, 0, n)
	uncompressedLengths := make([]sql.NullInt64, 0, n)
	storageGenerations := make([]string, 0, n)
	for _, block := range blocks {
		// The genesis block has neither a timestamp nor a parent.
		var unixTimestamp int64
		if ts := block.GetTimestamp(); ts != nil {
			unixTimestamp = ts.GetSeconds()
		}
		var parentHeight uint64
		if block.Height != 0 {
			parentHeight = block.ParentHeight
		}
		byteOffset, byteLength, uncompressedLength := blockObjectByteFields(block)

		heights = append(heights, int64(block.Height))
		tags = append(tags, int64(block.Tag))
		hashes = append(hashes, block.Hash)
		parentHashes = append(parentHashes, block.ParentHash)
		parentHeights = append(parentHeights, int64(parentHeight))
		objectKeys = append(objectKeys, block.ObjectKeyMain)
		timestamps = append(timestamps, unixTimestamp)
		skipped = append(skipped, block.Skipped)
		objectFormats = append(objectFormats, int64(block.GetObjectFormat()))
		byteOffsets = append(byteOffsets, byteOffset)
		byteLengths = append(byteLengths, byteLength)
		uncompressedLengths = append(uncompressedLengths, uncompressedLength)
		storageGenerations = append(storageGenerations, block.GetStorageGeneration())
	}
	return []interface{}{
		pq.Array(heights),
		pq.Array(tags),
		pq.Array(hashes),
		pq.Array(parentHashes),
		pq.Array(parentHeights),
		pq.Array(objectKeys),
		pq.Array(timestamps),
		pq.Array(skipped),
		pq.Array(objectFormats),
		pq.Array(byteOffsets),
		pq.Array(byteLengths),
		pq.Array(uncompressedLengths),
		pq.Array(storageGenerations),
	}
}

// upsertBlockMetadataChunk upserts one deduplicated group of blocks and records their ids.
// Ids are matched by conflict key rather than by RETURNING row order, which Postgres does not
// guarantee.
func upsertBlockMetadataChunk(ctx context.Context, tx *sql.Tx, blocks []*api.BlockMetadata, skipped bool, ids *persistedBlockIDs) (err error) {
	if len(blocks) == 0 {
		return nil
	}
	group := "regular"
	if skipped {
		group = "skipped"
	}
	firstHeight, lastHeight := blocks[0].Height, blocks[len(blocks)-1].Height
	rows, err := tx.QueryContext(ctx, blockMetadataUpsertQuery(skipped), blockMetadataColumnArrays(blocks)...)
	if err != nil {
		return xerrors.Errorf("failed to insert %s block metadata for heights [%d, %d]: %w", group, firstHeight, lastHeight, err)
	}
	// The rows must be fully drained and closed before the next statement on this transaction.
	defer func() {
		if closeErr := rows.Close(); closeErr != nil && err == nil {
			err = xerrors.Errorf("failed to close %s block metadata rows: %w", group, closeErr)
		}
	}()
	returned := 0
	for rows.Next() {
		var id, tag int64
		if skipped {
			var height int64
			if err := rows.Scan(&id, &tag, &height); err != nil {
				return xerrors.Errorf("failed to scan %s block metadata id: %w", group, err)
			}
			ids.skipped[blockHeightKey{tag: uint32(tag), height: uint64(height)}] = id
		} else {
			var hash sql.NullString
			if err := rows.Scan(&id, &tag, &hash); err != nil {
				return xerrors.Errorf("failed to scan %s block metadata id: %w", group, err)
			}
			ids.regular[blockHashKey{tag: uint32(tag), hash: hash.String}] = id
		}
		returned++
	}
	if err := rows.Err(); err != nil {
		return xerrors.Errorf("failed to read %s block metadata ids for heights [%d, %d]: %w", group, firstHeight, lastHeight, err)
	}
	if returned != len(blocks) {
		return xerrors.Errorf("%s block metadata upsert for heights [%d, %d] returned %d ids for %d blocks", group, firstHeight, lastHeight, returned, len(blocks))
	}
	return nil
}

// resolveCanonicalRows picks the canonical block_metadata id for every height in the chunk. When
// several blocks share a height, the last one in the (stable-sorted) chunk wins.
func resolveCanonicalRows(chunk []*api.BlockMetadata, ids *persistedBlockIDs) ([]canonicalRow, error) {
	last := make(map[blockHeightKey]int, len(chunk))
	for i, block := range chunk {
		last[blockHeightKey{tag: block.Tag, height: block.Height}] = i
	}
	rows := make([]canonicalRow, 0, len(last))
	for i, block := range chunk {
		if last[blockHeightKey{tag: block.Tag, height: block.Height}] != i {
			continue
		}
		id, ok := ids.lookup(block)
		if !ok {
			return nil, xerrors.Errorf("missing block metadata id for tag %d height %d hash %q skipped %v", block.Tag, block.Height, block.Hash, block.Skipped)
		}
		rows = append(rows, canonicalRow{height: block.Height, blockMetadataID: id, tag: block.Tag})
	}
	return rows, nil
}

func upsertCanonicalRows(ctx context.Context, tx *sql.Tx, rows []canonicalRow) error {
	if len(rows) == 0 {
		return nil
	}
	heights := make([]int64, 0, len(rows))
	blockMetadataIDs := make([]int64, 0, len(rows))
	tags := make([]int64, 0, len(rows))
	for _, row := range rows {
		heights = append(heights, int64(row.height))
		blockMetadataIDs = append(blockMetadataIDs, row.blockMetadataID)
		tags = append(tags, int64(row.tag))
	}
	if _, err := tx.ExecContext(ctx, canonicalBlocksUpsertQuery, pq.Array(heights), pq.Array(blockMetadataIDs), pq.Array(tags)); err != nil {
		return xerrors.Errorf("failed to insert canonical blocks for heights [%d, %d]: %w", rows[0].height, rows[len(rows)-1].height, err)
	}
	return nil
}

func blockObjectByteFields(block *api.BlockMetadata) (sql.NullInt64, sql.NullInt64, sql.NullInt64) {
	if block.GetObjectFormat() != api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_CSCB_BATCH {
		return sql.NullInt64{}, sql.NullInt64{}, sql.NullInt64{}
	}
	return sql.NullInt64{Int64: int64(block.GetByteOffset()), Valid: true},
		sql.NullInt64{Int64: int64(block.GetByteLength()), Valid: true},
		sql.NullInt64{Int64: int64(block.GetUncompressedLength()), Valid: true}
}
