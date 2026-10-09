package metastorage

import (
	"context"
	"errors"

	"golang.org/x/xerrors"

	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

var ErrSingleBlockRetirementFenced = errors.New("single-block upload blocked by retirement fence")

type singleBlockUploadStorage interface {
	GetBlockByHash(ctx context.Context, tag uint32, height uint64, blockHash string) (*api.BlockMetadata, error)
	SingleBlockUploadGuardStorage
}

// UploadSingleBlockObject serializes a single-block PUT with retirement
// preparation. Storage implementations must return a non-nil guard even when
// canonical metadata does not exist yet.
func UploadSingleBlockObject(
	ctx context.Context,
	metadataStorage singleBlockUploadStorage,
	metadata *api.BlockMetadata,
	upload func() (string, error),
) (objectKey string, err error) {
	if metadata == nil {
		return "", xerrors.New("block metadata is required for guarded upload")
	}
	if metadataStorage == nil {
		return "", xerrors.New("metadata storage is required for guarded upload")
	}
	guard, err := metadataStorage.AcquireSingleBlockUploadGuard(ctx, metadata.Tag, metadata.Height, metadata.Hash)
	if err != nil {
		return "", err
	}
	if guard == nil {
		return "", xerrors.New("single-block upload guard is required")
	}
	defer func() {
		if releaseErr := guard.Release(); releaseErr != nil {
			err = errors.Join(err, releaseErr)
		}
	}()

	if !guard.RetirementFenced() {
		objectKey, err = upload()
		return objectKey, err
	}
	return "", xerrors.Errorf(
		"%w: tag=%d height=%d hash=%s",
		ErrSingleBlockRetirementFenced,
		metadata.Tag,
		metadata.Height,
		metadata.Hash,
	)
}
