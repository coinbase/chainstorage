package blobstorage

import (
	"go.uber.org/fx"

	"github.com/coinbase/chainstorage/internal/storage/blobstorage/gcs"
	"github.com/coinbase/chainstorage/internal/storage/blobstorage/internal"
	"github.com/coinbase/chainstorage/internal/storage/blobstorage/s3"
)

type (
	BlobStorage              = internal.BlobStorage
	BlobStorageFactory       = internal.BlobStorageFactory
	BlobStorageFactoryParams = internal.BlobStorageFactoryParams
	RawBlockData             = internal.RawBlockData
)

var Module = fx.Options(
	fx.Provide(internal.WithBlobStorageFactory),
	s3.Module,
	gcs.Module,
)
