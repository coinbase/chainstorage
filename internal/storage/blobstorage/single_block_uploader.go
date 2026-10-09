package blobstorage

import (
	"context"

	"go.uber.org/fx"

	storageinternal "github.com/coinbase/chainstorage/internal/storage/blobstorage/internal"
	"github.com/coinbase/chainstorage/internal/storage/metastorage"
	"github.com/coinbase/chainstorage/internal/utils/fxparams"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

type (
	// SingleBlockUploader is the only production interface that can create
	// single-block objects. Every call is serialized with the metadata retirement
	// fence before it reaches the raw blob store.
	SingleBlockUploader interface {
		Upload(ctx context.Context, block *api.Block, compression api.Compression) (string, error)
		UploadRaw(ctx context.Context, rawBlockData *RawBlockData) (string, error)
	}

	blobStorageProviderParams struct {
		fx.In
		fxparams.Params
		S3          storageinternal.BlobStorageFactory `name:"blobstorage/s3"`
		GCS         storageinternal.BlobStorageFactory `name:"blobstorage/gcs"`
		MetaStorage metastorage.MetaStorage
	}

	blobStorageResult struct {
		fx.Out
		BlobStorage         BlobStorage
		SingleBlockUploader SingleBlockUploader
	}

	safeBlobStorage struct {
		storage storageinternal.BlobStorage
	}

	guardedSingleBlockUploader struct {
		raw         storageinternal.SingleBlockUploader
		metaStorage metastorage.MetaStorage
	}
)

func withBlobStorageFactory(params blobStorageProviderParams) (blobStorageResult, error) {
	core, err := storageinternal.WithBlobStorageFactory(storageinternal.BlobStorageFactoryParams{
		Params: params.Params,
		S3:     params.S3,
		GCS:    params.GCS,
	})
	if err != nil {
		return blobStorageResult{}, err
	}
	return blobStorageResult{
		BlobStorage: &safeBlobStorage{storage: core},
		SingleBlockUploader: &guardedSingleBlockUploader{
			raw:         core,
			metaStorage: params.MetaStorage,
		},
	}, nil
}

func (s *safeBlobStorage) UploadConsolidated(ctx context.Context, blocks []ConsolidatedBlockPayload) (string, []BlockPlacement, error) {
	return s.storage.UploadConsolidated(ctx, blocks)
}

func (s *safeBlobStorage) Download(ctx context.Context, metadata *api.BlockMetadata) (*api.Block, error) {
	return s.storage.Download(ctx, metadata)
}

func (s *safeBlobStorage) DownloadMany(ctx context.Context, metadata []*api.BlockMetadata) ([]*api.Block, error) {
	return s.storage.DownloadMany(ctx, metadata)
}

func (s *safeBlobStorage) PreSign(ctx context.Context, metadata *api.BlockMetadata) (string, error) {
	return s.storage.PreSign(ctx, metadata)
}

func (u *guardedSingleBlockUploader) Upload(
	ctx context.Context,
	block *api.Block,
	compression api.Compression,
) (string, error) {
	var metadata *api.BlockMetadata
	if block != nil {
		metadata = block.Metadata
	}
	return metastorage.UploadSingleBlockObject(ctx, u.metaStorage, metadata, func() (string, error) {
		return u.raw.Upload(ctx, block, compression)
	})
}

func (u *guardedSingleBlockUploader) UploadRaw(
	ctx context.Context,
	rawBlockData *RawBlockData,
) (string, error) {
	var metadata *api.BlockMetadata
	if rawBlockData != nil {
		metadata = rawBlockData.BlockMetadata
	}
	return metastorage.UploadSingleBlockObject(ctx, u.metaStorage, metadata, func() (string, error) {
		return u.raw.UploadRaw(ctx, rawBlockData)
	})
}
