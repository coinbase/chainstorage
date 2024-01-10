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
		logger             *zap.Logger
		config             *config.Config
		project            string
		bucket             string
		client             *storage.Client
		blobStorageMetrics *blobStorageMetrics
		instrumentUpload   instrument.InstrumentWithResult[string]
		instrumentDownload instrument.InstrumentWithResult[*api.Block]
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
		logger:             log.WithPackage(params.Logger),
		config:             params.Config,
		project:            params.Config.GCP.Project,
		bucket:             params.Config.GCP.Bucket,
		client:             client,
		blobStorageMetrics: blobStorageMetrics,
		instrumentUpload:   instrument.NewWithResult[string](metrics, "upload"),
		instrumentDownload: instrument.NewWithResult[*api.Block](metrics, "download"),
	}, nil
}

func (s *blobStorageImpl) Upload(ctx context.Context, block *api.Block, compression api.Compression) (string, error) {
	return s.instrumentUpload.Instrument(ctx, func(ctx context.Context) (string, error) {
		var key string
		defer s.logDuration("upload", time.Now())

		// Skip the upload if the block itself is skipped.
		if block.Metadata.Skipped {
			return "", nil
		}

		data, err := proto.Marshal(block)
		if err != nil {
			return "", xerrors.Errorf("failed to marshal block: %w", err)
		}

		blockchainNetwork := fmt.Sprintf("%s/%s", block.Blockchain, block.Network)
		tagHeightHash := fmt.Sprintf("%d/%d/%s", block.Metadata.Tag, block.Metadata.Height, block.Metadata.Hash)
		if s.config.Chain.Sidechain != api.SideChain_SIDECHAIN_NONE {
			key = fmt.Sprintf(
				"%s/%s/%s", blockchainNetwork, block.SideChain, tagHeightHash,
			)
		} else {
			key = fmt.Sprintf(
				"%s/%s", blockchainNetwork, tagHeightHash,
			)
		}

		data, err = storage_utils.Compress(data, compression)
		if err != nil {
			return "", xerrors.Errorf("failed to compress data with type %v: %w", compression.String(), err)
		}
		key, err = storage_utils.GetObjectKey(key, compression)
		if err != nil {
			return "", xerrors.Errorf("failed to get object key: %w", err)
		}

		// #nosec G401
		h := md5.New()
		size, err := h.Write(data)
		if err != nil {
			return "", xerrors.Errorf("failed to compute checksum: %w", err)
		}

		checksum := h.Sum(nil)

		object := s.client.Bucket(s.bucket).Object(key)
		w := object.NewWriter(ctx)
		finalizer := finalizer.WithCloser(w)
		defer finalizer.Finalize()

		_, err = w.Write(data)
		if err != nil {
			return "", xerrors.Errorf("failed to upload block data: %w", err)
		}
		err = finalizer.Close()
		if err != nil {
			return "", xerrors.Errorf("failed to upload block data: %w", err)
		}

		attrs := w.Attrs()
		if err != nil {
			return "", xerrors.Errorf("failed to load attributes for uploaded block data: %w", err)
		}
		if !bytes.Equal(checksum, attrs.MD5) {
			return "", xerrors.Errorf("uploaded block md5 checksum %x is different from expected %x", attrs.MD5, checksum)
		}

		// a workaround to use timer
		s.blobStorageMetrics.blobUploadedSize.Record(time.Duration(size) * time.Millisecond)

		return key, nil
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

func (s *blobStorageImpl) logDuration(method string, start time.Time) {
	s.logger.Debug(
		"blob_storage",
		zap.String("storage_type", "gcs"),
		zap.String("method", method),
		zap.Duration("duration", time.Since(start)),
	)
}
