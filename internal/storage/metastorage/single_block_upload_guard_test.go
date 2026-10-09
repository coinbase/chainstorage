package metastorage

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

type uploadGuardTestStorage struct {
	guard       SingleBlockUploadGuard
	existing    *api.BlockMetadata
	acquireErr  error
	metadataErr error
}

func (s *uploadGuardTestStorage) AcquireSingleBlockUploadGuard(
	ctx context.Context,
	tag uint32,
	height uint64,
	hash string,
) (SingleBlockUploadGuard, error) {
	return s.guard, s.acquireErr
}

func (s *uploadGuardTestStorage) GetBlockByHash(
	ctx context.Context,
	tag uint32,
	height uint64,
	blockHash string,
) (*api.BlockMetadata, error) {
	return s.existing, s.metadataErr
}

type uploadGuardTestLease struct {
	fenced      bool
	releaseErr  error
	releaseCall int
}

func (g *uploadGuardTestLease) RetirementFenced() bool {
	return g.fenced
}

func (g *uploadGuardTestLease) Release() error {
	g.releaseCall++
	return g.releaseErr
}

func TestUploadSingleBlockObjectFailsClosedForRetirementFence(t *testing.T) {
	require := require.New(t)
	metadata := testGuardMetadata()
	lease := &uploadGuardTestLease{fenced: true}
	storage := &uploadGuardTestStorage{guard: lease}
	uploadCalls := 0

	objectKey, err := UploadSingleBlockObject(context.Background(), storage, metadata, func() (string, error) {
		uploadCalls++
		return "single-block/new.gzip", nil
	})

	require.ErrorIs(err, ErrSingleBlockRetirementFenced)
	require.Empty(objectKey)
	require.Zero(uploadCalls)
	require.Equal(1, lease.releaseCall)
}

func TestUploadSingleBlockObjectHoldsGuardThroughPut(t *testing.T) {
	require := require.New(t)
	metadata := testGuardMetadata()
	lease := &uploadGuardTestLease{}
	storage := &uploadGuardTestStorage{guard: lease}
	uploadCalls := 0

	objectKey, err := UploadSingleBlockObject(context.Background(), storage, metadata, func() (string, error) {
		uploadCalls++
		require.Zero(lease.releaseCall)
		return "single-block/new.gzip", nil
	})

	require.NoError(err)
	require.Equal("single-block/new.gzip", objectKey)
	require.Equal(1, uploadCalls)
	require.Equal(1, lease.releaseCall)
}

func TestUploadSingleBlockObjectFailsClosedForMissingGuard(t *testing.T) {
	require := require.New(t)
	storage := &uploadGuardTestStorage{}
	uploadCalls := 0

	objectKey, err := UploadSingleBlockObject(context.Background(), storage, testGuardMetadata(), func() (string, error) {
		uploadCalls++
		return "single-block/new.gzip", nil
	})

	require.ErrorContains(err, "single-block upload guard is required")
	require.Empty(objectKey)
	require.Zero(uploadCalls)
}

func TestUploadSingleBlockObjectJoinsReleaseFailure(t *testing.T) {
	require := require.New(t)
	metadata := testGuardMetadata()
	uploadErr := errors.New("put failed")
	releaseErr := errors.New("release failed")
	lease := &uploadGuardTestLease{releaseErr: releaseErr}
	storage := &uploadGuardTestStorage{guard: lease}

	_, err := UploadSingleBlockObject(context.Background(), storage, metadata, func() (string, error) {
		return "", uploadErr
	})

	require.ErrorIs(err, uploadErr)
	require.ErrorIs(err, releaseErr)
	require.Equal(1, lease.releaseCall)
}

func testGuardMetadata() *api.BlockMetadata {
	return &api.BlockMetadata{
		Tag:        2,
		Height:     429600000,
		Hash:       "hash-429600000",
		ParentHash: "hash-429599999",
	}
}
