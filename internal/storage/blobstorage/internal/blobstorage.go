package internal

import (
	"context"

	"go.uber.org/fx"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/utils/fxparams"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

type (
	BlobStorage interface {
		Upload(ctx context.Context, block *api.Block, compression api.Compression) (string, error)
		Download(ctx context.Context, metadata *api.BlockMetadata) (*api.Block, error)
		PreSign(ctx context.Context, objectKey string) (string, error)
	}

	BlobStorageFactory interface {
		Create() (BlobStorage, error)
	}

	BlobStorageFactoryParams struct {
		fx.In
		fxparams.Params
		S3  BlobStorageFactory `name:"blobstorage/s3"`
		GCS BlobStorageFactory `name:"blobstorage/gcs"`
	}
)

func WithBlobStorageFactory(params BlobStorageFactoryParams) (BlobStorage, error) {
	var factory BlobStorageFactory
	storageType := params.Config.StorageType.BlobStorageType
	switch storageType {
	case config.BlobStorageType_UNSPECIFIED, config.BlobStorageType_S3:
		factory = params.S3
	case config.BlobStorageType_GCS:
		factory = params.GCS
	}
	if factory == nil {
		return nil, xerrors.Errorf("blob storage type is not implemented: %v", storageType)
	}
	result, err := factory.Create()
	if err != nil {
		return nil, xerrors.Errorf("failed to create blob storage of type %v: %w", storageType, err)
	}
	return result, nil
}
