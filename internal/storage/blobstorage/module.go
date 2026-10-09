package blobstorage

import (
	"go.uber.org/fx"

	"github.com/coinbase/chainstorage/internal/storage/blobstorage/gcs"
	"github.com/coinbase/chainstorage/internal/storage/blobstorage/internal"
	"github.com/coinbase/chainstorage/internal/storage/blobstorage/s3"
)

type (
	BlobStorage                = internal.BlobStorage
	BlobStorageFactory         = internal.BlobStorageFactory
	BlobStorageFactoryParams   = internal.BlobStorageFactoryParams
	RawBlockData               = internal.RawBlockData
	PayloadSource              = internal.PayloadSource
	BytesPayloadSource         = internal.BytesPayloadSource
	FilePayloadSource          = internal.FilePayloadSource
	ConsolidatedBlockPayload   = internal.ConsolidatedBlockPayload
	BlockPlacement             = internal.BlockPlacement
	ConsolidatedUploadProgress = internal.ConsolidatedUploadProgress
)

var (
	NewFilePayloadSource             = internal.NewFilePayloadSource
	WithConsolidatedUploadProgress   = internal.WithConsolidatedUploadProgress
	RecordConsolidatedUploadProgress = internal.RecordConsolidatedUploadProgress

	Module = fx.Module("blobstorage",
		fx.Provide(withBlobStorageFactory),
		s3.Module,
		gcs.Module,
	)
)
