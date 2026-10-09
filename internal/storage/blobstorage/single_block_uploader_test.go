package blobstorage

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/fx"
	"go.uber.org/mock/gomock"

	storageinternal "github.com/coinbase/chainstorage/internal/storage/blobstorage/internal"
	"github.com/coinbase/chainstorage/internal/storage/metastorage"
	metastoragemocks "github.com/coinbase/chainstorage/internal/storage/metastorage/mocks"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

type singleBlockUploadTestRaw struct {
	uploadCalls    int
	uploadRawCalls int
}

func (u *singleBlockUploadTestRaw) Upload(context.Context, *api.Block, api.Compression) (string, error) {
	u.uploadCalls++
	return "single-block-key", nil
}

func (u *singleBlockUploadTestRaw) UploadRaw(context.Context, *storageinternal.RawBlockData) (string, error) {
	u.uploadRawCalls++
	return "single-block-raw-key", nil
}

type fencedSingleBlockUploadTestGuard struct{}

func (fencedSingleBlockUploadTestGuard) RetirementFenced() bool {
	return true
}

func (fencedSingleBlockUploadTestGuard) Release() error {
	return nil
}

func TestSingleBlockUploaderRejectsFencedUploadBeforeRawStorage(t *testing.T) {
	controller := gomock.NewController(t)
	metaStorage := metastoragemocks.NewMockMetaStorage(controller)
	raw := &singleBlockUploadTestRaw{}
	uploader := &guardedSingleBlockUploader{raw: raw, metaStorage: metaStorage}
	block := &api.Block{Metadata: &api.BlockMetadata{Tag: 2, Height: 123, Hash: "hash"}}
	metaStorage.EXPECT().AcquireSingleBlockUploadGuard(gomock.Any(), uint32(2), uint64(123), "hash").
		Return(fencedSingleBlockUploadTestGuard{}, nil)

	_, err := uploader.Upload(context.Background(), block, api.Compression_GZIP)
	require.ErrorIs(t, err, metastorage.ErrSingleBlockRetirementFenced)
	require.Zero(t, raw.uploadCalls)
}

func TestSingleBlockUploaderGuardsRawUpload(t *testing.T) {
	controller := gomock.NewController(t)
	metaStorage := metastoragemocks.NewMockMetaStorage(controller)
	raw := &singleBlockUploadTestRaw{}
	uploader := &guardedSingleBlockUploader{raw: raw, metaStorage: metaStorage}
	rawBlock := &RawBlockData{BlockMetadata: &api.BlockMetadata{Tag: 2, Height: 124, Hash: "hash-raw"}}
	metaStorage.EXPECT().AcquireSingleBlockUploadGuard(gomock.Any(), uint32(2), uint64(124), "hash-raw").
		Return(metastorage.NewUnfencedSingleBlockUploadGuard(), nil)

	key, err := uploader.UploadRaw(context.Background(), rawBlock)
	require.NoError(t, err)
	require.Equal(t, "single-block-raw-key", key)
	require.Equal(t, 1, raw.uploadRawCalls)
}

func TestSafeBlobStorageDoesNotExposeSingleBlockUploads(t *testing.T) {
	core := &singleBlockUploadTestCore{}
	var storage BlobStorage = &safeBlobStorage{storage: core}

	_, exposesRawUpload := storage.(SingleBlockUploader)
	require.False(t, exposesRawUpload)
}

func TestModuleDoesNotExposeRawBlobStorageFactory(t *testing.T) {
	type rawFactoryParams struct {
		fx.In
		S3 storageinternal.BlobStorageFactory `name:"blobstorage/s3"`
	}

	app := fx.New(
		Module,
		fx.NopLogger,
		fx.Invoke(func(rawFactoryParams) {}),
	)
	require.Error(t, app.Err())
	require.Contains(t, app.Err().Error(), "blobstorage/s3")
}

type singleBlockUploadTestCore struct {
	singleBlockUploadTestRaw
}

func (*singleBlockUploadTestCore) UploadConsolidated(context.Context, []storageinternal.ConsolidatedBlockPayload) (string, []storageinternal.BlockPlacement, error) {
	return "", nil, nil
}

func (*singleBlockUploadTestCore) Download(context.Context, *api.BlockMetadata) (*api.Block, error) {
	return nil, nil
}

func (*singleBlockUploadTestCore) DownloadMany(context.Context, []*api.BlockMetadata) ([]*api.Block, error) {
	return nil, nil
}

func (*singleBlockUploadTestCore) PreSign(context.Context, *api.BlockMetadata) (string, error) {
	return "", nil
}
