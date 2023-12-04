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
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/coinbase/chainstorage/internal/blockchain/parser"
	storage_errors "github.com/coinbase/chainstorage/internal/storage/internal/errors"
	"github.com/coinbase/chainstorage/internal/storage/metastorage/internal"
	"github.com/coinbase/chainstorage/internal/utils/instrument"
	"github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
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
	block, err := b.instrumentGetBlockByHash.Instrument(ctx, func(ctx context.Context) (*chainstorage.BlockMetadata, error) {
		return b.getBlock(ctx, fmt.Sprintf("env/%s/blocks/%d-%020d-%s", b.env, tag, height, blockHash))
	})
	if err != nil {
		return nil, err
	}
	return block, nil
}

// GetBlockByHeight implements internal.BlockStorage.
func (b *blockStorageImpl) GetBlockByHeight(ctx context.Context, tag uint32, height uint64) (*chainstorage.BlockMetadata, error) {
	var block *chainstorage.BlockMetadata
	block, err := b.instrumentGetBlockByHeight.Instrument(ctx, func(ctx context.Context) (*chainstorage.BlockMetadata, error) {
		return b.getBlock(ctx, fmt.Sprintf("env/%s/blocks/canonical-%d-%020d", b.env, tag, height))
	})
	if err != nil {
		return nil, err
	}
	return block, nil
}

// GetBlocksByHeightRange implements internal.BlockStorage.
func (b *blockStorageImpl) GetBlocksByHeightRange(ctx context.Context, tag uint32, startHeight uint64, endHeight uint64) ([]*chainstorage.BlockMetadata, error) {
	blocks, err := b.instrumentGetBlocksByHeightRange.Instrument(ctx, func(ctx context.Context) ([]*chainstorage.BlockMetadata, error) {
		md, ok := metadata.FromOutgoingContext(ctx)
		if !ok {
			md = metadata.New(nil)
		}
		md = md.Copy()
		md["google-cloud-resource-prefix"] = []string{fmt.Sprintf("projects/%s/databases/(default)", b.projectId)}
		c, err := b.grpc_client.RunQuery(metadata.NewOutgoingContext(ctx, md), &firestorepb.RunQueryRequest{
			Parent: fmt.Sprintf("projects/%s/databases/(default)/documents", b.projectId),
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
									ReferenceValue: fmt.Sprintf("projects/chainstorage-local/databases/(default)/documents/env/%s/blocks/canonical-%d-%020d", b.env, tag, startHeight),
								},
							},
						},
						Before: true,
					},
					EndAt: &firestorepb.Cursor{
						Values: []*firestorepb.Value{
							{
								ValueType: &firestorepb.Value_ReferenceValue{
									ReferenceValue: fmt.Sprintf("projects/chainstorage-local/databases/(default)/documents/env/%s/blocks/canonical-%d-%020d", b.env, tag, endHeight),
								},
							},
						},
						Before: false,
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
		blocks := make([]*chainstorage.BlockMetadata, 0)
		r, err := c.Recv()
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
			blocks = append(blocks, block)
			r, err = c.Recv()
		}
		return blocks, nil
	})
	if err != nil {
		return nil, err
	}
	return blocks, nil
}

// GetBlocksByHeights implements internal.BlockStorage.
func (b *blockStorageImpl) GetBlocksByHeights(ctx context.Context, tag uint32, heights []uint64) ([]*chainstorage.BlockMetadata, error) {
	blocks, err := b.instrumentGetBlocksByHeights.Instrument(ctx, func(ctx context.Context) ([]*chainstorage.BlockMetadata, error) {
		docRefs := make([]*firestore.DocumentRef, 0)
		for _, height := range heights {
			docRef := b.client.Doc(fmt.Sprintf("env/%s/blocks/canonical-%d-%020d", b.env, tag, height))
			docRefs = append(docRefs, docRef)
		}
		docs, err := b.client.GetAll(ctx, docRefs)
		if err != nil {
			return nil, xerrors.Errorf("failed to get blocks by heights: %w", err)
		}
		blocks := make([]*chainstorage.BlockMetadata, 0)
		for _, doc := range docs {
			block, err := b.intoBlockMetadata(doc)
			if err != nil {
				return nil, xerrors.Errorf("failed to parse document snapshot into block metadata: %w", err)
			}
			blocks = append(blocks, block)
		}
		return blocks, nil
	})
	if err != nil {
		return nil, err
	}
	return blocks, nil
}

// GetLatestBlock implements internal.BlockStorage.
func (b *blockStorageImpl) GetLatestBlock(ctx context.Context, tag uint32) (*chainstorage.BlockMetadata, error) {
	block, err := b.instrumentGetLatestBlock.Instrument(ctx, func(ctx context.Context) (*chainstorage.BlockMetadata, error) {
		return b.getBlock(ctx, fmt.Sprintf("env/%s/blocks/%d-latest", b.env, tag))
	})
	if err != nil {
		return nil, err
	}
	return block, nil
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
			docRef := b.client.Doc(fmt.Sprintf("env/%s/blocks/%d-%020d-%s", b.env, block.Tag, block.Height, block.Hash))
			_, err := bulkWriter.Set(docRef, b.fromBlockMetadata(block))
			if err != nil {
				return xerrors.Errorf("failed to add block %+v to BulkWriter: %w", block, err)
			}
			bulkSize++
			if bulkSize >= maxBulkWriteSize {
				bulkWriter.Flush()
				bulkSize = 0
			}
			docRef = b.client.Doc(fmt.Sprintf("env/%s/blocks/canonical-%d-%020d", b.env, block.Tag, block.Height))
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
			docRef := b.client.Doc(fmt.Sprintf("env/%s/blocks/%d-latest", b.env, watermarkBlock.Tag))
			_, err := docRef.Set(ctx, b.fromBlockMetadata(watermarkBlock))
			if err != nil {
				return xerrors.Errorf("failed to update watermark block: %w", err)
			}
		}

		return nil
	})
}

func (b *blockStorageImpl) getBlock(ctx context.Context, docName string) (*chainstorage.BlockMetadata, error) {
	docRef := b.client.Doc(docName)
	doc, err := docRef.Get(ctx)
	if err != nil {
		return nil, xerrors.Errorf("failed to get block: %w", err)
	}
	if !doc.Exists() {
		return nil, storage_errors.ErrItemNotFound
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
