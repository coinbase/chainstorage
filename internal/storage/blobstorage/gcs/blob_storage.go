package gcs

import (
	"bytes"
	"context"
	"crypto/md5" // #nosec G501
	"fmt"
	"io"
	"time"

	"cloud.google.com/go/storage"
	"github.com/uber-go/tally/v4"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"golang.org/x/xerrors"
	"google.golang.org/protobuf/proto"

	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/storage/blobstorage/internal"
	storage_utils "github.com/coinbase/chainstorage/internal/storage/utils"
	"github.com/coinbase/chainstorage/internal/utils/finalizer"
	"github.com/coinbase/chainstorage/internal/utils/fxparams"
	"github.com/coinbase/chainstorage/internal/utils/instrument"
	"github.com/coinbase/chainstorage/internal/utils/log"
	"github.com/coinbase/chainstorage/protos/coinbase/c3/common"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

type (
	BlobStorageParams struct {
		fx.In
		fxparams.Params
	}

	blobStorageFactory struct {
		params BlobStorageParams
	}

	blobStorageImpl struct {
		logger                 *zap.Logger
		config                 *config.Config
		project                string
		bucket                 string
		client                 *storage.Client
		presignedUrlExpiration time.Duration
		blobStorageMetrics     *blobStorageMetrics
		instrumentUpload       instrument.InstrumentWithResult[string]
		instrumentUploadRaw    instrument.InstrumentWithResult[string]
		instrumentDownload     instrument.InstrumentWithResult[*api.Block]
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
func (f *blobStorageFactory) Create() (internal.BlobStorage, error) {
	return New(f.params)
}

func New(params BlobStorageParams) (internal.BlobStorage, error) {
	metrics := params.Metrics.SubScope("blob_storage").Tagged(map[string]string{
		"storage_type": "gcs",
	})
	if params.Config.GCP == nil {
		return nil, xerrors.Errorf("GCP project id not configured")
	}
	if len(params.Config.GCP.Bucket) == 0 {
		return nil, xerrors.Errorf("GCP bucket not configure for blob storage")
	}
	if params.Config.GCP.PresignedUrlExpiration == 0 {
		return nil, xerrors.Errorf("GCP presign url expiration not configure for blob storage")
	}
	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, xerrors.Errorf("failed to create GCS client: %w", err)
	}
	blobStorageMetrics := &blobStorageMetrics{
		blobDownloadedSize: metrics.SubScope(blobDownloaderScopeName).Timer(blobSizeMetricName),
		blobUploadedSize:   metrics.SubScope(blobUploaderScopeName).Timer(blobSizeMetricName),
	}
	return &blobStorageImpl{
		logger:                 log.WithPackage(params.Logger),
		config:                 params.Config,
		project:                params.Config.GCP.Project,
		bucket:                 params.Config.GCP.Bucket,
		client:                 client,
		presignedUrlExpiration: params.Config.GCP.PresignedUrlExpiration,
		blobStorageMetrics:     blobStorageMetrics,
		instrumentUpload:       instrument.NewWithResult[string](metrics, "upload"),
		instrumentUploadRaw:    instrument.NewWithResult[string](metrics, "upload_raw"),
		instrumentDownload:     instrument.NewWithResult[*api.Block](metrics, "download"),
	}, nil
}

func (s *blobStorageImpl) getObjectKey(blockchain common.Blockchain, sidechain api.SideChain, network common.Network, metadata *api.BlockMetadata, compression api.Compression) (string, error) {
	var key string
	var err error
	blockchainNetwork := fmt.Sprintf("%s/%s", blockchain, network)
	tagHeightHash := fmt.Sprintf("%d/%d/%s", metadata.Tag, metadata.Height, metadata.Hash)
	if s.config.Chain.Sidechain != api.SideChain_SIDECHAIN_NONE {
		key = fmt.Sprintf(
			"%s/%s/%s", blockchainNetwork, sidechain, tagHeightHash,
		)
	} else {
		key = fmt.Sprintf(
			"%s/%s", blockchainNetwork, tagHeightHash,
		)
	}
	key, err = storage_utils.GetObjectKey(key, compression)
	if err != nil {
		return "", xerrors.Errorf("failed to get object key: %w", err)
	}
	return key, nil
}

func (s *blobStorageImpl) uploadRaw(ctx context.Context, rawBlockData *internal.RawBlockData) (string, error) {
	key, err := s.getObjectKey(rawBlockData.Blockchain, rawBlockData.SideChain, rawBlockData.Network, rawBlockData.BlockMetadata, rawBlockData.BlockDataCompression)
	if err != nil {
		return "", err
	}

	// #nosec G401
	h := md5.New()
	size, err := h.Write(rawBlockData.BlockData)
	if err != nil {
		return "", xerrors.Errorf("failed to compute checksum: %w", err)
	}

	checksum := h.Sum(nil)

	object := s.client.Bucket(s.bucket).Object(key)
	w := object.NewWriter(ctx)
	finalizer := finalizer.WithCloser(w)
	defer finalizer.Finalize()

	_, err = w.Write(rawBlockData.BlockData)
	if err != nil {
		return "", xerrors.Errorf("failed to upload block data: %w", err)
	}
	err = finalizer.Close()
	if err != nil {
		return "", xerrors.Errorf("failed to upload block data: %w", err)
	}

	attrs := w.Attrs()
	if !bytes.Equal(checksum, attrs.MD5) {
		return "", xerrors.Errorf("uploaded block md5 checksum %x is different from expected %x", attrs.MD5, checksum)
	}

	// a workaround to use timer
	s.blobStorageMetrics.blobUploadedSize.Record(time.Duration(size) * time.Millisecond)

	return key, nil
}

func (s *blobStorageImpl) UploadRaw(ctx context.Context, rawBlockData *internal.RawBlockData) (string, error) {
	return s.instrumentUploadRaw.Instrument(ctx, func(ctx context.Context) (string, error) {
		defer s.logDuration("upload", time.Now())

		// Skip the upload if the block itself is skipped.
		if rawBlockData.BlockMetadata.Skipped {
			return "", nil
		}

		return s.uploadRaw(ctx, rawBlockData)
	})
}

func (s *blobStorageImpl) Upload(ctx context.Context, block *api.Block, compression api.Compression) (string, error) {
	return s.instrumentUpload.Instrument(ctx, func(ctx context.Context) (string, error) {
		defer s.logDuration("upload", time.Now())

		// Skip the upload if the block itself is skipped.
		if block.Metadata.Skipped {
			return "", nil
		}
		data, err := proto.Marshal(block)
		if err != nil {
			return "", xerrors.Errorf("failed to marshal block: %w", err)
		}

		data, err = storage_utils.Compress(data, compression)
		if err != nil {
			return "", xerrors.Errorf("failed to compress data with type %v: %w", compression.String(), err)
		}

		return s.uploadRaw(ctx, &internal.RawBlockData{
			Blockchain:           block.Blockchain,
			SideChain:            block.SideChain,
			Network:              block.Network,
			BlockMetadata:        block.Metadata,
			BlockData:            data,
			BlockDataCompression: compression,
		})
	})
}

func (s *blobStorageImpl) Download(ctx context.Context, metadata *api.BlockMetadata) (*api.Block, error) {
	return s.instrumentDownload.Instrument(ctx, func(ctx context.Context) (*api.Block, error) {
		defer s.logDuration("download", time.Now())

		if metadata.Skipped {
			// No blob data is available when the block is skipped.
			return &api.Block{
				Blockchain: s.config.Chain.Blockchain,
				Network:    s.config.Chain.Network,
				SideChain:  s.config.Chain.Sidechain,
				Metadata:   metadata,
				Blobdata:   nil,
			}, nil
		}

		key := metadata.ObjectKeyMain

		object := s.client.Bucket(s.bucket).Object(key)
		reader, err := object.NewReader(ctx)
		finalizer := finalizer.WithCloser(reader)
		defer finalizer.Finalize()
		if err != nil {
			return nil, xerrors.Errorf("failed to download from gcs (bucket=%s, key=%s): %w", s.bucket, key, err)
		}
		buf, err := io.ReadAll(reader)
		if err != nil {
			return nil, xerrors.Errorf("failed to download from gcs (bucket=%s, key=%s): %w", s.bucket, key, err)
		}

		// a workaround to use timer
		s.blobStorageMetrics.blobDownloadedSize.Record(time.Duration(len(buf)) * time.Millisecond)

		compression := storage_utils.GetCompressionType(key)
		blockData, err := storage_utils.Decompress(buf, compression)
		if err != nil {
			return nil, xerrors.Errorf("failed to decompress block data with type %v: %w", compression.String(), err)
		}

		var block api.Block
		err = proto.Unmarshal(blockData, &block)
		if err != nil {
			return nil, xerrors.Errorf("failed to unmarshal data downloaded from s3 bucket %s key %s: %w", s.bucket, key, err)
		}

		// When metadata is loaded from meta storage,
		// the new fields, e.g. ParentHeight, may be populated with default values.
		// Overwrite metadata using the one loaded from meta storage.
		block.Metadata = metadata
		return &block, finalizer.Close()
	})
}

// PreSign implements internal.BlobStorage.
func (s *blobStorageImpl) PreSign(ctx context.Context, objectKey string) (string, error) {
	fileUrl, err := s.client.Bucket(s.bucket).SignedURL(objectKey, &storage.SignedURLOptions{
		Expires: time.Now().Add(s.presignedUrlExpiration),
		Method:  "GET",
	})
	if err != nil {
		return "", xerrors.Errorf("failed to generate presigned url: %w", err)
	}
	return fileUrl, nil
}

func (s *blobStorageImpl) logDuration(method string, start time.Time) {
	s.logger.Debug(
		"blob_storage",
		zap.String("storage_type", "gcs"),
		zap.String("method", method),
		zap.Duration("duration", time.Since(start)),
	)
}
