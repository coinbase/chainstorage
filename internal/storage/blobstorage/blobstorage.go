package blobstorage

import (
	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/storage/blobstorage/internal"
)

type (
	BlobStorage              = internal.BlobStorage
	BlobStorageFactory       = internal.BlobStorageFactory
	BlobStorageFactoryParams = internal.BlobStorageFactoryParams
)

func createBlobStorageAndWrapError(factory BlobStorageFactory, message string) (BlobStorage, error) {
	bs, err := factory.Create()
	if err != nil {
		return nil, xerrors.Errorf("%v, error: %w", message, err)
	}
	return bs, nil
}

func New(factories BlobStorageFactoryParams) (BlobStorage, error) {
	switch factories.Config.StorageType.BlobStorageType {
	// If it's unspecified, defaults to S3
	case config.BlobStorageType_UNSPECIFIED, config.BlobStorageType_S3:
		return createBlobStorageAndWrapError(factories.S3, "failed to create s3 blob storage")
	default:
		return nil, xerrors.Errorf(
			"blob storage type is not implemented: %v",
			factories.Config.StorageType.BlobStorageType)
	}
}
