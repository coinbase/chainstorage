package firestore

import (
	"context"
	"fmt"
	"sort"

	"cloud.google.com/go/firestore"
	"golang.org/x/xerrors"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/gogo/status"

	"github.com/coinbase/chainstorage/internal/blockchain/parser"
	"github.com/coinbase/chainstorage/internal/storage/internal/errors"
	"github.com/coinbase/chainstorage/internal/storage/metastorage/internal"
	"github.com/coinbase/chainstorage/internal/utils/instrument"
	"github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

type (
	blockStorageImpl struct {
		client                           *firestore.Client
		projectId                        string
		env                              string
		blockStartHeight                 uint64
		instrumentPersistBlockMetas      instrument.Instrument
		instrumentGetLatestBlock         instrument.InstrumentWithResult[*chainstorage.BlockMetadata]
		instrumentGetBlockByHash         instrument.InstrumentWithResult[*chainstorage.BlockMetadata]
		instrumentGetBlockByHeight       instrument.InstrumentWithResult[*chainstorage.BlockMetadata]
		instrumentGetBlocksByHeightRange instrument.InstrumentWithResult[[]*chainstorage.BlockMetadata]
		instrumentGetBlocksByHeights     instrument.InstrumentWithResult[[]*chainstorage.BlockMetadata]
	}
)

func newBlockStorage(params Params, client *firestore.Client) (internal.BlockStorage, error) {
	metrics := params.Metrics.SubScope("block_storage").Tagged(map[string]string{
		"storage_type": "firestore",
	})
	accessor := blockStorageImpl{
		client:                           client,
		projectId:                        params.Config.GCP.Project,
		env:                              params.Config.ConfigName,
		blockStartHeight:                 params.Config.Chain.BlockStartHeight,
		instrumentPersistBlockMetas:      instrument.New(metrics, "persist_block_metas"),
		instrumentGetLatestBlock:         instrument.NewWithResult[*chainstorage.BlockMetadata](metrics, "get_latest_block"),
		instrumentGetBlockByHash:         instrument.NewWithResult[*chainstorage.BlockMetadata](metrics, "get_block_by_hash"),
		instrumentGetBlockByHeight:       instrument.NewWithResult[*chainstorage.BlockMetadata](metrics, "get_block_by_height"),
		instrumentGetBlocksByHeightRange: instrument.NewWithResult[[]*chainstorage.BlockMetadata](metrics, "get_blocks_by_height_range"),
		instrumentGetBlocksByHeights:     instrument.NewWithResult[[]*chainstorage.BlockMetadata](metrics, "get_blocks_by_heights"),
	}
	return &accessor, nil
}

// GetBlockByHash implements internal.BlockStorage.
func (b *blockStorageImpl) GetBlockByHash(ctx context.Context, tag uint32, height uint64, blockHash string) (*chainstorage.BlockMetadata, error) {
	if err := b.validateHeight(height); err != nil {
		return nil, err
	}
	return b.instrumentGetBlockByHash.Instrument(ctx, func(ctx context.Context) (*chainstorage.BlockMetadata, error) {
		if blockHash == "" {
			return b.GetBlockByHeight(ctx, tag, height)
		}
		return b.getBlock(ctx, b.getBlockDocRef(tag, height, blockHash))
	})
}

// GetBlockByHeight implements internal.BlockStorage.
func (b *blockStorageImpl) GetBlockByHeight(ctx context.Context, tag uint32, height uint64) (*chainstorage.BlockMetadata, error) {
	if err := b.validateHeight(height); err != nil {
		return nil, err
	}
	return b.instrumentGetBlockByHeight.Instrument(ctx, func(ctx context.Context) (*chainstorage.BlockMetadata, error) {
		return b.getBlock(ctx, b.getCanonicalBlockDocRef(tag, height))
	})
}

// GetBlocksByHeightRange implements internal.BlockStorage.
func (b *blockStorageImpl) GetBlocksByHeightRange(ctx context.Context, tag uint32, startHeight uint64, endHeight uint64) ([]*chainstorage.BlockMetadata, error) {
	if startHeight >= endHeight {
		return nil, xerrors.Errorf(
			"startHeight(%d) should be less than endHeight(%d): %w",
			startHeight, endHeight, errors.ErrOutOfRange)
	}
	if err := b.validateHeight(startHeight); err != nil {
		return nil, err
	}
	return b.instrumentGetBlocksByHeightRange.Instrument(ctx, func(ctx context.Context) ([]*chainstorage.BlockMetadata, error) {
		startDocRef := b.getCanonicalBlockDocRef(tag, startHeight)
		endDocRef := b.getCanonicalBlockDocRef(tag, endHeight)
		docs := startDocRef.Parent.Query.
			StartAt(startDocRef).EndBefore(endDocRef).
			OrderBy(firestore.DocumentID, firestore.Asc).
			Documents(ctx)
		blocks := make([]*chainstorage.BlockMetadata, endHeight-startHeight)
		expecting := startHeight
		for {
			doc, err := docs.Next()
			if err == iterator.Done {
				break
			}
			if err != nil {
				return nil, xerrors.Errorf("failed to get blocks: %w", err)
			}
			block, err := b.intoBlockMetadata(doc)
			if err != nil {
				return nil, xerrors.Errorf("failed to parse block data: %w", err)
			}
			if block.Height != expecting {
				return nil, xerrors.Errorf(
					"block metadata with height %d not found: %w",
					expecting, errors.ErrItemNotFound)
			}
			blocks[expecting-startHeight] = block
			expecting = expecting + 1
		}
		if expecting < endHeight {
			return nil, xerrors.Errorf(
				"block metadata with height %d not found: %w",
				expecting, errors.ErrItemNotFound)
		}
		return blocks, nil
	})
}

// GetBlocksByHeights implements internal.BlockStorage.
func (b *blockStorageImpl) GetBlocksByHeights(ctx context.Context, tag uint32, heights []uint64) ([]*chainstorage.BlockMetadata, error) {
	for _, height := range heights {
		if err := b.validateHeight(height); err != nil {
			return nil, err
		}
	}
	return b.instrumentGetBlocksByHeights.Instrument(ctx, func(ctx context.Context) ([]*chainstorage.BlockMetadata, error) {
		docRefs := make([]*firestore.DocumentRef, len(heights))
		for i, height := range heights {
			docRefs[i] = b.getCanonicalBlockDocRef(tag, height)
		}
		docs, err := b.client.GetAll(ctx, docRefs)
		if err != nil {
			return nil, xerrors.Errorf("failed to get blocks by heights: %w", err)
		}
		blocks := make([]*chainstorage.BlockMetadata, len(heights))
		expecting := 0
		for i, doc := range docs {
			if !doc.Exists() {
				return nil, xerrors.Errorf(
					"block metadata with height %d not found: %w",
					heights[i], errors.ErrItemNotFound)
			}
			block, err := b.intoBlockMetadata(doc)
			if err != nil {
				return nil, xerrors.Errorf("failed to parse document snapshot into block metadata: %w", err)
			}
			if block.Height != heights[expecting] {
				return nil, xerrors.Errorf(
					"block metadata with height %d not found: %w",
					heights[expecting], errors.ErrItemNotFound)
			}
			expecting = expecting + 1
			blocks[i] = block
		}
		if expecting < len(heights) {
			return nil, xerrors.Errorf(
				"block metadata with height %d not found: %w",
				heights[expecting], errors.ErrItemNotFound)
		}
		return blocks, nil
	})
}

// GetLatestBlock implements internal.BlockStorage.
func (b *blockStorageImpl) GetLatestBlock(ctx context.Context, tag uint32) (*chainstorage.BlockMetadata, error) {
	return b.instrumentGetLatestBlock.Instrument(ctx, func(ctx context.Context) (*chainstorage.BlockMetadata, error) {
		return b.getBlock(ctx, b.getLatestBlockDocRef(tag))
	})
}

// PersistBlockMetas implements internal.BlockStorage.
func (b *blockStorageImpl) PersistBlockMetas(ctx context.Context, updateWatermark bool, blocks []*chainstorage.BlockMetadata, lastBlock *chainstorage.BlockMetadata) error {
	if len(blocks) == 0 {
		return nil
	}
	return b.instrumentPersistBlockMetas.Instrument(ctx, func(sg_ctx context.Context) error {
		sort.Slice(blocks, func(i, j int) bool {
			return blocks[i].Height < blocks[j].Height
		})
		if err := parser.ValidateChain(blocks, lastBlock); err != nil {
			return xerrors.Errorf("failed to validate chain: %w", err)
		}

		// Blocks with hash as key can be written in random order
		bulkWriter := b.client.BulkWriter(sg_ctx)
		for _, block := range blocks {
			docRef := b.getBlockDocRef(block.Tag, block.Height, block.Hash)
			_, err := bulkWriter.Set(docRef, b.fromBlockMetadata(block))
			if err != nil {
				return xerrors.Errorf("failed to add block %+v to BulkWriter: %w", block, err)
			}
		}
		bulkWriter.End()

		// Canonical blocks must be written in order to avoid canonical chain inconsistency with best effort
		writeCount := len(blocks)
		if updateWatermark {
			// Append watermark block to be the last one to update
			writeCount++
		}
		// First chunk as the residual
		firstChunkSize := writeCount % maxBulkWriteSize
		for start, end := 0, firstChunkSize; end <= writeCount; start, end = end, end+maxBulkWriteSize {
			err := b.client.RunTransaction(sg_ctx, func(ctx context.Context, t *firestore.Transaction) error {
				for i := start; i < end; i++ {
					var docRef *firestore.DocumentRef
					var block *chainstorage.BlockMetadata
					if i == len(blocks) {
						block = blocks[i-1]
						docRef = b.getLatestBlockDocRef(block.Tag)
					} else {
						block = blocks[i]
						docRef = b.getCanonicalBlockDocRef(block.Tag, block.Height)
					}
					err := t.Set(docRef, b.fromBlockMetadata(block))
					if err != nil {
						return xerrors.Errorf("failed to add block %+v in firestore transaction: %w", block, err)
					}
				}
				return nil
			})
			if err != nil {
				return xerrors.Errorf("failed to add canonical blocks in firestore transaction: %w", err)
			}
		}
		return nil
	})
}

func (b *blockStorageImpl) validateHeight(height uint64) error {
	if height < b.blockStartHeight {
		return xerrors.Errorf(
			"height(%d) should be no less than blockStartHeight(%d): %w",
			height, b.blockStartHeight, errors.ErrInvalidHeight)
	}
	return nil
}

func (b *blockStorageImpl) getLatestBlockDocRef(tag uint32) *firestore.DocumentRef {
	return b.client.Doc(fmt.Sprintf("env/%s/blocks/%d-latest", b.env, tag))
}

func (b *blockStorageImpl) getCanonicalBlockDocRef(tag uint32, height uint64) *firestore.DocumentRef {
	return b.client.Doc(fmt.Sprintf("env/%s/blocks/canonical-%d-%020d", b.env, tag, height))
}

func (b *blockStorageImpl) getBlockDocRef(tag uint32, height uint64, hash string) *firestore.DocumentRef {
	return b.client.Doc(fmt.Sprintf("env/%s/blocks/%d-%020d-%s", b.env, tag, height, hash))
}

func (b *blockStorageImpl) getBlock(ctx context.Context, docRef *firestore.DocumentRef) (*chainstorage.BlockMetadata, error) {
	doc, err := docRef.Get(ctx)
	if err != nil && status.Code(err) != codes.NotFound {
		return nil, xerrors.Errorf("failed to get block: %w", err)
	}
	if !doc.Exists() {
		return nil, errors.ErrItemNotFound
	}
	block, err := b.intoBlockMetadata(doc)
	if err != nil {
		return nil, xerrors.Errorf("failed to parse block data: %w", err)
	}
	return block, nil
}

// firestore does not support storing uin64, hence we use int64 to store block height
type firestoreBlockMetadata struct {
	Hash          string
	ParentHash    string
	Height        int64
	ParentHeight  int64
	ObjectKeyMain string
	Skipped       bool
	Timestamp     *timestamppb.Timestamp
	Tag           uint32
}

func (*blockStorageImpl) fromBlockMetadata(block *chainstorage.BlockMetadata) *firestoreBlockMetadata {
	return &firestoreBlockMetadata{
		Hash:          block.Hash,
		ParentHash:    block.ParentHash,
		Height:        int64(block.Height),
		ParentHeight:  int64(block.ParentHeight),
		ObjectKeyMain: block.ObjectKeyMain,
		Skipped:       block.Skipped,
		Tag:           block.Tag,
		Timestamp:     block.Timestamp,
	}
}

func (*blockStorageImpl) intoBlockMetadata(doc *firestore.DocumentSnapshot) (*chainstorage.BlockMetadata, error) {
	var s firestoreBlockMetadata
	err := doc.DataTo(&s)
	if err != nil {
		return nil, xerrors.Errorf("failed to parse document into BlockMetadata: %w", err)
	}
	if s.Height < 0 {
		return nil, xerrors.Errorf("expecting block Height to be uint64, but got %d", s.Height)
	}
	if s.ParentHeight < 0 {
		return nil, xerrors.Errorf("expecting block ParentHeight to be uint64, but got %d", s.ParentHeight)
	}
	return &chainstorage.BlockMetadata{
		Hash:          s.Hash,
		ParentHash:    s.ParentHash,
		Height:        uint64(s.Height),
		ParentHeight:  uint64(s.ParentHeight),
		ObjectKeyMain: s.ObjectKeyMain,
		Skipped:       s.Skipped,
		Timestamp:     s.Timestamp,
		Tag:           s.Tag,
	}, nil
}
