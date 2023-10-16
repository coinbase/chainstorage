package blobstorage

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
	}

	BlobStorageFactory interface {
		Create() BlobStorage
	}

	BlobStorageFactories struct {
		fx.In
		fxparams.Params
		S3 BlobStorageFactory `name:"s3"`
	}
)

func WithBlobStorageFactory() fx.Option {
	return fx.Options(
		fx.Provide(fx.Annotated{
			Name:   "s3",
			Target: NewS3BlobStorageFactory,
		}),
	)
}

func NewBlobStorage(factories BlobStorageFactories) (BlobStorage, error) {
	switch factories.Config.StorageType.BlobStorageType {
	// If it's unspecified, defaults to S3
	case config.BlobStorageType_UNSPECIFIED, config.BlobStorageType_S3:
		return factories.S3.Create(), nil
	}
	return nil, xerrors.Errorf(
		"blob storage type is not implemented: %v",
		factories.Config.StorageType.BlobStorageType)
}
