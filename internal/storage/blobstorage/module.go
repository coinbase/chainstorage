package blobstorage

import (
	"go.uber.org/fx"

	"github.com/coinbase/chainstorage/internal/storage/blobstorage/downloader"
	"github.com/coinbase/chainstorage/internal/storage/blobstorage/s3"
)

var Module = fx.Options(
	fx.Provide(NewBlobStorage),
	s3.Module,
	downloader.Module,
)
