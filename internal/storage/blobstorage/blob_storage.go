package blobstorage

import (
	"context"

	"go.uber.org/fx"
	"golang.org/x/xerrors"

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
	// TODO make it enum and read from config, e.g. factories.Config.BlobStorageType
	block_storage_type := "s3"
	switch block_storage_type {
	case "s3":
		return factories.S3.Create(), nil
	}
	return nil, xerrors.Errorf("blob storage type is not implemented: %v ", block_storage_type)
}
