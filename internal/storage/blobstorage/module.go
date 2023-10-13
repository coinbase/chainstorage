package blobstorage

import (
	"github.com/coinbase/chainstorage/internal/storage/blobstorage/downloader"
	"go.uber.org/fx"
)

var Module = fx.Options(
	fx.Provide(NewBlobStorage),
	WithBlobStorageFactory(),
	downloader.Module,
)
