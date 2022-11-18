package blobstorage

import (
	"go.uber.org/fx"

	"github.com/coinbase/chainstorage/internal/storage/blobstorage/downloader"
)

var Module = fx.Options(
	fx.Provide(New),
	downloader.Module,
)
