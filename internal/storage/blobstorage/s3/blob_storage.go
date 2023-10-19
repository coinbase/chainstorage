package s3

import (
	"bytes"
	"context"
	"crypto/md5" // #nosec G501
	"encoding/base64"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/request"
	awss3 "github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/uber-go/tally"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"golang.org/x/xerrors"
	"google.golang.org/protobuf/proto"

	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/s3"
	"github.com/coinbase/chainstorage/internal/storage/blobstorage/internal"
	"github.com/coinbase/chainstorage/internal/storage/internal/errors"
	storage_utils "github.com/coinbase/chainstorage/internal/storage/utils"
	"github.com/coinbase/chainstorage/internal/utils/fxparams"
	"github.com/coinbase/chainstorage/internal/utils/instrument"
	"github.com/coinbase/chainstorage/internal/utils/log"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

type (
	BlobStorageParams struct {
		fx.In
		fxparams.Params
		Downloader s3.Downloader
		Uploader   s3.Uploader
	}

	blobStorageFactory struct {
		params BlobStorageParams
	}

	blobStorageImpl struct {
		logger             *zap.Logger
		config             *config.Config
		bucket             string
		downloader         s3.Downloader
		uploader           s3.Uploader
		blobStorageMetrics *blobStorageMetrics
		instrumentUpload   instrument.Call
		instrumentDownload instrument.Call
	}

	blobStorageMetrics struct {
		blobDownloadedSize tally.Timer
		blobUploadedSize   tally.Timer
	}
)

const (
	blobUploaderScopeName   = "uploader"
	blobDownloaderScopeName = "downloader"
	blobSizeMetricName      = "blob_size"
)

var _ internal.BlobStorage = (*blobStorageImpl)(nil)

func NewFactory(params BlobStorageParams) internal.BlobStorageFactory {
	return &blobStorageFactory{params}
}

// Create implements BlobStorageFactory.
func (f *blobStorageFactory) Create() internal.BlobStorage {
	instance, _ := New(f.params)
	return instance
}

func New(params BlobStorageParams) (internal.BlobStorage, error) {
	metrics := params.Metrics.SubScope("blob_storage")
	return &blobStorageImpl{
		logger:             log.WithPackage(params.Logger),
		config:             params.Config,
		bucket:             params.Config.AWS.Bucket,
		downloader:         params.Downloader,
		uploader:           params.Uploader,
		blobStorageMetrics: newBlobStorageMetrics(metrics),
		instrumentUpload:   instrument.NewCall(metrics, "upload"),
		instrumentDownload: instrument.NewCall(metrics, "download"),
	}, nil
}

func newBlobStorageMetrics(scope tally.Scope) *blobStorageMetrics {
	return &blobStorageMetrics{
		blobDownloadedSize: scope.SubScope(blobDownloaderScopeName).Timer(blobSizeMetricName),
		blobUploadedSize:   scope.SubScope(blobUploaderScopeName).Timer(blobSizeMetricName),
	}
}

func (s *blobStorageImpl) Upload(ctx context.Context, block *api.Block, compression api.Compression) (string, error) {
	var key string
	if err := s.instrumentUpload.Instrument(ctx, func(ctx context.Context) error {
		defer s.logDuration("upload", time.Now())

		// Skip the upload if the block itself is skipped.
		if block.Metadata.Skipped {
			return nil
		}

		data, err := proto.Marshal(block)
		if err != nil {
			return xerrors.Errorf("failed to marshal block: %w", err)
		}

		key = fmt.Sprintf(
			"%s/%s/%d/%d/%s",
			block.Blockchain,
			block.Network,
			block.Metadata.Tag,
			block.Metadata.Height,
			block.Metadata.Hash,
		)

		data, err = storage_utils.Compress(data, compression)
		if err != nil {
			return xerrors.Errorf("failed to compress data with type %v: %w", compression.String(), err)
		}
		key, err = storage_utils.GetObjectKey(key, compression)
		if err != nil {
			return xerrors.Errorf("failed to get object key: %w", err)
		}

		// #nosec G401
		h := md5.New()
		size, err := h.Write(data)
		if err != nil {
			return xerrors.Errorf("failed to compute checksum: %w", err)
		}

		checksum := base64.StdEncoding.EncodeToString(h.Sum(nil))

		if _, err := s.uploader.UploadWithContext(ctx, &s3manager.UploadInput{
			Bucket:     aws.String(s.bucket),
			Key:        aws.String(key),
			Body:       bytes.NewReader(data),
			ContentMD5: aws.String(checksum),
		}); err != nil {
			return xerrors.Errorf("failed to upload to s3: %w", err)
		}

		// a workaround to use timer
		s.blobStorageMetrics.blobUploadedSize.Record(time.Duration(size) * time.Millisecond)

		return nil
	}); err != nil {
		return "", err
	}

	return key, nil
}

func (s *blobStorageImpl) Download(ctx context.Context, metadata *api.BlockMetadata) (*api.Block, error) {
	var block api.Block
	if err := s.instrumentDownload.Instrument(ctx, func(ctx context.Context) error {
		defer s.logDuration("download", time.Now())

		if metadata.Skipped {
			// No blob data is available when the block is skipped.
			block = api.Block{
				Blockchain: s.config.Chain.Blockchain,
				Network:    s.config.Chain.Network,
				Metadata:   metadata,
				Blobdata:   nil,
			}
			return nil
		}

		key := metadata.ObjectKeyMain
		buf := aws.NewWriteAtBuffer([]byte{})

		size, err := s.downloader.DownloadWithContext(ctx, buf, &awss3.GetObjectInput{
			Bucket: aws.String(s.bucket),
			Key:    aws.String(key),
		})
		if err != nil {
			if aerr, ok := err.(awserr.Error); ok && aerr.Code() == request.CanceledErrorCode {
				return errors.ErrRequestCanceled
			}
			return xerrors.Errorf("failed to download from s3 (bucket=%s, key=%s): %w", s.bucket, key, err)
		}

		// a workaround to use timer
		s.blobStorageMetrics.blobDownloadedSize.Record(time.Duration(size) * time.Millisecond)

		compression := storage_utils.GetCompressionType(key)
		blockData, err := storage_utils.Decompress(buf.Bytes(), compression)
		if err != nil {
			return xerrors.Errorf("failed to decompress block data with type %v: %w", compression.String(), err)
		}

		err = proto.Unmarshal(blockData, &block)
		if err != nil {
			return xerrors.Errorf("failed to unmarshal data downloaded from s3 bucket %s key %s: %w", s.bucket, key, err)
		}

		// When metadata is loaded from meta storage,
		// the new fields, e.g. ParentHeight, may be populated with default values.
		// Overwrite metadata using the one loaded from meta storage.
		block.Metadata = metadata
		return nil
	}); err != nil {
		return nil, err
	}

	return &block, nil
}

func (s *blobStorageImpl) logDuration(method string, start time.Time) {
	s.logger.Debug(
		"blob_storage",
		zap.String("method", method),
		zap.Duration("duration", time.Since(start)),
	)
}
