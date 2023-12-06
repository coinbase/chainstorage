package firestore

import (
	"context"
	"fmt"
	"io"
	"reflect"
	"sort"
	"unsafe"

	"cloud.google.com/go/firestore"
	vkit "cloud.google.com/go/firestore/apiv1"
	"cloud.google.com/go/firestore/apiv1/firestorepb"
	"golang.org/x/xerrors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/coinbase/chainstorage/internal/blockchain/parser"
	"github.com/coinbase/chainstorage/internal/storage/internal/errors"
	"github.com/coinbase/chainstorage/internal/storage/metastorage/internal"
	"github.com/coinbase/chainstorage/internal/utils/instrument"
	"github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
	"github.com/gogo/status"
)

type (
	blockStorageImpl struct {
		client                           *firestore.Client
		grpc_client                      *vkit.Client
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
	metrics := params.Metrics.SubScope("block_storage_firestore")

	field := reflect.ValueOf(client).Elem().FieldByName("c")
	grpc_client, ok := reflect.NewAt(field.Type(), unsafe.Pointer(field.UnsafeAddr())).Elem().Interface().(*vkit.Client)
	if !ok {
		return nil, xerrors.New("failed to get firestore grpc client")
	}
	accessor := blockStorageImpl{
		client:                           client,
		grpc_client:                      grpc_client,
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
		databasePath := fmt.Sprintf("projects/%s/databases/(default)", b.projectId)
		parent := databasePath + "/documents"
		md, ok := metadata.FromOutgoingContext(ctx)
		if !ok {
			md = metadata.New(nil)
		}
		md = md.Copy()
		md["google-cloud-resource-prefix"] = []string{databasePath}
		c, err := b.grpc_client.RunQuery(metadata.NewOutgoingContext(ctx, md), &firestorepb.RunQueryRequest{
			Parent: parent,
			QueryType: &firestorepb.RunQueryRequest_StructuredQuery{
				StructuredQuery: &firestorepb.StructuredQuery{
					From: []*firestorepb.StructuredQuery_CollectionSelector{
						{
							AllDescendants: true,
						},
					},
					StartAt: &firestorepb.Cursor{
						Values: []*firestorepb.Value{
							{
								ValueType: &firestorepb.Value_ReferenceValue{
									ReferenceValue: b.getCanonicalBlockDocRef(tag, startHeight).Path,
								},
							},
						},
						Before: true,
					},
					EndAt: &firestorepb.Cursor{
						Values: []*firestorepb.Value{
							{
								ValueType: &firestorepb.Value_ReferenceValue{
									ReferenceValue: b.getCanonicalBlockDocRef(tag, endHeight).Path,
								},
							},
						},
						Before: true,
					},
					OrderBy: []*firestorepb.StructuredQuery_Order{
						{
							Field: &firestorepb.StructuredQuery_FieldReference{
								FieldPath: "__name__",
							},
							Direction: firestorepb.StructuredQuery_ASCENDING,
						},
					},
				},
			},
		})
		if err != nil {
			return nil, xerrors.Errorf("failed to get blocks: %w", err)
		}
		blocks := make([]*chainstorage.BlockMetadata, endHeight-startHeight)
		r, err := c.Recv()
		expecting := startHeight
		for {
			if err == io.EOF {
				break
			}
			if err != nil {
				return nil, xerrors.Errorf("failed to get blocks: %w", err)
			}
			if r.Document == nil {
				r, err = c.Recv()
				continue
			}
			doc := &firestore.DocumentSnapshot{}
			field := reflect.ValueOf(doc).Elem().FieldByName("proto")
			reflect.NewAt(field.Type(), unsafe.Pointer(field.UnsafeAddr())).Elem().Set(reflect.ValueOf(r.Document))
			var block *chainstorage.BlockMetadata
			block, err = b.intoBlockMetadata(doc)
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
			r, err = c.Recv()
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
	return b.instrumentPersistBlockMetas.Instrument(ctx, func(ctx context.Context) error {
		if len(blocks) == 0 {
			return nil
		}
		sort.Slice(blocks, func(i, j int) bool {
			return blocks[i].Height < blocks[j].Height
		})
		if err := parser.ValidateChain(blocks, lastBlock); err != nil {
			return xerrors.Errorf("failed to validate chain: %w", err)
		}
		bulkSize := 0
		bulkWriter := b.client.BulkWriter(ctx)

		for _, block := range blocks {
			docRef := b.getBlockDocRef(block.Tag, block.Height, block.Hash)
			_, err := bulkWriter.Set(docRef, b.fromBlockMetadata(block))
			if err != nil {
				return xerrors.Errorf("failed to add block %+v to BulkWriter: %w", block, err)
			}
			bulkSize++
			if bulkSize >= maxBulkWriteSize {
				bulkWriter.Flush()
				bulkSize = 0
			}
			docRef = b.getCanonicalBlockDocRef(block.Tag, block.Height)
			_, err = bulkWriter.Set(docRef, b.fromBlockMetadata(block))
			if err != nil {
				return xerrors.Errorf("failed to add block to BulkWriter: %w", err)
			}
			bulkSize++
			if bulkSize >= maxBulkWriteSize {
				bulkWriter.Flush()
				bulkSize = 0
			}
		}
		bulkWriter.End()

		if updateWatermark {
			watermarkBlock := blocks[len(blocks)-1]
			docRef := b.getLatestBlockDocRef(watermarkBlock.Tag)
			_, err := docRef.Set(ctx, b.fromBlockMetadata(watermarkBlock))
			if err != nil {
				return xerrors.Errorf("failed to update watermark block: %w", err)
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

func (*blockStorageImpl) fromBlockMetadata(block *chainstorage.BlockMetadata) map[string]interface{} {
	v := make(map[string]interface{})
	v["Hash"] = block.Hash
	v["ParentHash"] = block.ParentHash
	v["Height"] = int64(block.Height)
	v["ParentHeight"] = int64(block.ParentHeight)
	v["ObjectKeyMain"] = block.ObjectKeyMain
	v["Skipped"] = block.Skipped
	v["Tag"] = block.Tag
	v["Timestamp"] = block.Timestamp
	return v
}

func (*blockStorageImpl) intoBlockMetadata(doc *firestore.DocumentSnapshot) (*chainstorage.BlockMetadata, error) {
	b := &chainstorage.BlockMetadata{}
	var s struct {
		Hash          string
		ParentHash    string
		Height        int64
		ParentHeight  int64
		ObjectKeyMain string
		Skipped       bool
		Timestamp     *timestamppb.Timestamp
		Tag           uint32
	}
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
	b.Hash = s.Hash
	b.ParentHash = s.ParentHash
	b.Height = uint64(s.Height)
	b.ParentHeight = uint64(s.ParentHeight)
	b.ObjectKeyMain = s.ObjectKeyMain
	b.Skipped = s.Skipped
	b.Timestamp = s.Timestamp
	b.Tag = s.Tag
	return b, nil
}
