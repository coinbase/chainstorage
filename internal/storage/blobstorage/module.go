package blobstorage

import (
	"go.uber.org/fx"

	"github.com/coinbase/chainstorage/internal/storage/blobstorage/internal"
	"github.com/coinbase/chainstorage/internal/storage/blobstorage/s3"
)

type (
	BlobStorage              = internal.BlobStorage
	BlobStorageFactory       = internal.BlobStorageFactory
	BlobStorageFactoryParams = internal.BlobStorageFactoryParams
)

var Module = fx.Options(
	fx.Provide(internal.WithBlobStorageFactory),
	s3.Module,
)
