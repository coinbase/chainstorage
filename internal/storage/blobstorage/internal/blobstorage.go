package internal

import (
	"context"

	"go.uber.org/fx"

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
		S3 BlobStorageFactory `name:"blobstorage/s3"`
	}
)
