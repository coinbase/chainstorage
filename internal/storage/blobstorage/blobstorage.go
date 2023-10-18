package blobstorage

import (
	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/storage/blobstorage/internal"
)

type (
	BlobStorage          = internal.BlobStorage
	BlobStorageFactory   = internal.BlobStorageFactory
	BlobStorageFactories = internal.BlobStorageFactories
)

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
