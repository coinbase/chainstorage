package s3

import (
	"context"
	"errors"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"golang.org/x/xerrors"

	chains3 "github.com/coinbase/chainstorage/internal/s3"
	storageerrors "github.com/coinbase/chainstorage/internal/storage/internal/errors"
	storage_utils "github.com/coinbase/chainstorage/internal/storage/utils"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

func downloadSingleBlockFromBucket(
	ctx context.Context,
	bucket string,
	downloader chains3.Downloader,
	metrics *blobStorageMetrics,
	metadata *api.BlockMetadata,
) (*api.Block, error) {
	key := metadata.GetObjectKeyMain()
	buf := manager.NewWriteAtBuffer([]byte{})

	size, err := downloader.Download(ctx, buf, &awss3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return nil, storageerrors.ErrRequestCanceled
		}
		return nil, xerrors.Errorf("failed to download from s3 (bucket=%s, key=%s): %w", bucket, key, err)
	}

	metrics.blobDownloadedSize.Record(time.Duration(size) * time.Millisecond)

	compression := storage_utils.GetCompressionType(key)
	blockData, err := storage_utils.Decompress(buf.Bytes(), compression)
	if err != nil {
		return nil, xerrors.Errorf("failed to decompress block data with type %v: %w", compression.String(), err)
	}

	return unmarshalBlockData(bucket, key, metadata, blockData)
}
