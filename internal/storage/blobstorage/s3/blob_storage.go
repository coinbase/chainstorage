package s3

import (
	"bytes"
	"context"
	"crypto/md5" // #nosec G501
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"sort"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	awss3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/uber-go/tally/v4"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"golang.org/x/xerrors"
	"google.golang.org/protobuf/proto"

	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/s3"
	"github.com/coinbase/chainstorage/internal/storage/blobstorage/cscb"
	"github.com/coinbase/chainstorage/internal/storage/blobstorage/internal"
	storageerrors "github.com/coinbase/chainstorage/internal/storage/internal/errors"
	storage_utils "github.com/coinbase/chainstorage/internal/storage/utils"
	"github.com/coinbase/chainstorage/internal/utils/fxparams"
	"github.com/coinbase/chainstorage/internal/utils/instrument"
	"github.com/coinbase/chainstorage/internal/utils/log"
	"github.com/coinbase/chainstorage/internal/utils/syncgroup"
	"github.com/coinbase/chainstorage/protos/coinbase/c3/common"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

type (
	BlobStorageParams struct {
		fx.In
		fxparams.Params
		Client     s3.Client
		Downloader s3.Downloader
		Uploader   s3.Uploader
	}

	blobStorageFactory struct {
		params BlobStorageParams
	}

	blobStorageImpl struct {
		logger                 *zap.Logger
		config                 *config.Config
		bucket                 string
		storageGeneration      string
		client                 s3.Client
		downloader             s3.Downloader
		uploader               s3.Uploader
		blobStorageMetrics     *blobStorageMetrics
		instrumentUpload       instrument.InstrumentWithResult[string]
		instrumentUploadRaw    instrument.InstrumentWithResult[string]
		instrumentDownload     instrument.InstrumentWithResult[*api.Block]
		instrumentDownloadMany instrument.InstrumentWithResult[[]*api.Block]
	}

	blobStorageMetrics struct {
		blobDownloadedSize tally.Timer
		blobUploadedSize   tally.Timer
	}

	downloadRef struct {
		index    int
		bucket   string
		metadata *api.BlockMetadata
	}

	cscbObjectLocation struct {
		bucket string
		key    string
	}

	cscbBlockDownload struct {
		ref   downloadRef
		block *cscb.BlockDescriptor
		chunk *cscb.ChunkDescriptor
	}
)

const (
	blobUploaderScopeName   = "uploader"
	blobDownloaderScopeName = "downloader"
	blobSizeMetricName      = "blob_size"

	cscbMetadataFormat             = "chainstorage-format"
	cscbMetadataCompressionScope   = "chainstorage-compression-scope"
	cscbMetadataSHA256             = "chainstorage-sha256"
	cscbMetadataCodec              = "chainstorage-codec"
	cscbMetadataUncompressedLength = "chainstorage-uncompressed-length"

	maxSinglePutObjectSize = 5 * 1024 * 1024 * 1024

	cscbInitialIndexReadSize = 64 * 1024
)

var _ internal.BlobStorageCore = (*blobStorageImpl)(nil)

func newFactory(params BlobStorageParams) internal.BlobStorageFactory {
	return &blobStorageFactory{params}
}

// Create implements BlobStorageFactory.
func (f *blobStorageFactory) Create() (internal.BlobStorageCore, error) {
	return newBlobStorage(f.params)
}

func newBlobStorage(params BlobStorageParams) (internal.BlobStorageCore, error) {
	metrics := params.Metrics.SubScope("blob_storage").Tagged(map[string]string{
		"storage_type": "s3",
	})
	bucket, err := params.Config.WriteBlockStorageBucket()
	if err != nil {
		return nil, xerrors.Errorf("failed to resolve write block storage bucket: %w", err)
	}
	storageGeneration, err := params.Config.WriteBlockStorageGeneration()
	if err != nil {
		return nil, xerrors.Errorf("failed to resolve write block storage generation: %w", err)
	}
	return &blobStorageImpl{
		logger:                 log.WithPackage(params.Logger),
		config:                 params.Config,
		bucket:                 bucket,
		storageGeneration:      storageGeneration,
		client:                 params.Client,
		downloader:             params.Downloader,
		uploader:               params.Uploader,
		blobStorageMetrics:     newBlobStorageMetrics(metrics),
		instrumentUpload:       instrument.NewWithResult[string](metrics, "upload"),
		instrumentUploadRaw:    instrument.NewWithResult[string](metrics, "upload_raw"),
		instrumentDownload:     instrument.NewWithResult[*api.Block](metrics, "download"),
		instrumentDownloadMany: instrument.NewWithResult[[]*api.Block](metrics, "download_many"),
	}, nil
}

func newBlobStorageMetrics(scope tally.Scope) *blobStorageMetrics {
	return &blobStorageMetrics{
		blobDownloadedSize: scope.SubScope(blobDownloaderScopeName).Timer(blobSizeMetricName),
		blobUploadedSize:   scope.SubScope(blobUploaderScopeName).Timer(blobSizeMetricName),
	}
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

	checksum := base64.StdEncoding.EncodeToString(h.Sum(nil))

	if _, err := s.uploader.Upload(ctx, &awss3.PutObjectInput{
		Bucket:     aws.String(s.bucket),
		Key:        aws.String(key),
		Body:       bytes.NewReader(rawBlockData.BlockData),
		ContentMD5: aws.String(checksum),
		ACL:        awss3types.ObjectCannedACLBucketOwnerFullControl,
	}); err != nil {
		return "", xerrors.Errorf("failed to upload to s3: %w", err)
	}
	rawBlockData.BlockMetadata.StorageGeneration = s.storageGeneration

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

func (s *blobStorageImpl) UploadConsolidated(ctx context.Context, blocks []internal.ConsolidatedBlockPayload) (string, []internal.BlockPlacement, error) {
	defer s.logDuration("upload_consolidated", time.Now())

	totalStart := time.Now()
	consolidation := s.config.AWS.Storage.Consolidation
	internal.RecordConsolidatedUploadProgress(ctx, "s3_encode_config_loaded", len(blocks))
	encodeStart := time.Now()
	object, err := cscb.Encode(ctx, cscb.EncodeConfig{
		Blockchain:                s.config.Chain.Blockchain,
		Network:                   s.config.Chain.Network,
		SideChain:                 s.config.Chain.Sidechain,
		Codec:                     consolidation.Codec,
		CodecLevel:                consolidation.CodecLevel,
		ZstdLongDistanceWindowLog: consolidation.ZstdLongDistanceWindowLog,
		MaxBlocks:                 consolidation.MaxBlocks,
		CompressionChunkBlocks:    consolidation.CompressionChunkBlocks,
		MaxChunkCompressedBytes:   consolidation.MaxChunkCompressedBytes,
		MaxChunkUncompressedBytes: consolidation.MaxChunkUncompressedBytes,
		MaxCompressedBytes:        consolidation.MaxCompressedBytes,
		MaxUncompressedBytes:      consolidation.MaxUncompressedBytes,
		ShardSize:                 consolidation.ShardSize,
		MemoryBudgetBytes:         consolidation.MemoryBudgetBytes,
		LocalSpillDir:             consolidation.LocalSpillDir,
		LocalSpillMaxBytes:        consolidation.LocalSpillMaxBytes,
	}, blocks)
	if err != nil {
		return "", nil, err
	}
	defer func() { _ = object.Close() }()
	encodeDuration := time.Since(encodeStart)
	internal.RecordConsolidatedUploadProgress(ctx, "s3_object_encoded", object.Key, object.Length)
	s.logger.Info(
		"encoded CSCB object",
		zap.String("object_key", object.Key),
		zap.Int("blocks", len(blocks)),
		zap.Uint64("compressed_bytes", object.Length),
		zap.Uint64("payload_compressed_bytes", object.PayloadCompressedLength),
		zap.Uint64("payload_uncompressed_bytes", object.PayloadUncompressedLength),
		zap.Duration("duration", encodeDuration),
	)

	headExistingStart := time.Now()
	found, err := s.validateExistingConsolidatedObject(ctx, object)
	if err != nil {
		return "", nil, err
	}
	headExistingDuration := time.Since(headExistingStart)
	s.logger.Info(
		"checked existing CSCB object",
		zap.String("object_key", object.Key),
		zap.Bool("found", found),
		zap.Duration("duration", headExistingDuration),
	)
	var uploadDuration time.Duration
	var uploadMD5Duration time.Duration
	var uploadPutDuration time.Duration
	var headUploadedDuration time.Duration
	if !found {
		uploadStart := time.Now()
		uploaded, md5Duration, putDuration, err := s.uploadConsolidatedObject(ctx, object, consolidation.Codec)
		if err != nil {
			return "", nil, err
		}
		uploadDuration = time.Since(uploadStart)
		uploadMD5Duration = md5Duration
		uploadPutDuration = putDuration
		s.logger.Info(
			"put CSCB object",
			zap.String("object_key", object.Key),
			zap.Bool("uploaded", uploaded),
			zap.Uint64("bytes", object.Length),
			zap.Duration("md5_duration", uploadMD5Duration),
			zap.Duration("s3_put_duration", uploadPutDuration),
			zap.Duration("duration", uploadDuration),
		)
		if uploaded {
			headUploadedStart := time.Now()
			if err := s.validateUploadedConsolidatedObject(ctx, object); err != nil {
				return "", nil, err
			}
			headUploadedDuration = time.Since(headUploadedStart)
			s.logger.Info(
				"validated uploaded CSCB object",
				zap.String("object_key", object.Key),
				zap.Uint64("bytes", object.Length),
				zap.Duration("duration", headUploadedDuration),
			)
			s.blobStorageMetrics.blobUploadedSize.Record(time.Duration(object.Length) * time.Millisecond)
		}
	}
	s.logger.Info(
		"finished consolidated upload",
		zap.String("object_key", object.Key),
		zap.Bool("existing_object", found),
		zap.Int("blocks", len(blocks)),
		zap.Uint64("compressed_bytes", object.Length),
		zap.Uint64("payload_uncompressed_bytes", object.PayloadUncompressedLength),
		zap.Duration("encode_duration", encodeDuration),
		zap.Duration("head_existing_duration", headExistingDuration),
		zap.Duration("md5_duration", uploadMD5Duration),
		zap.Duration("s3_put_duration", uploadPutDuration),
		zap.Duration("upload_duration", uploadDuration),
		zap.Duration("head_uploaded_duration", headUploadedDuration),
		zap.Duration("total_duration", time.Since(totalStart)),
	)
	for i := range object.Placements {
		object.Placements[i].StorageGeneration = s.storageGeneration
	}
	return object.Key, object.Placements, nil
}

func (s *blobStorageImpl) uploadConsolidatedObject(ctx context.Context, object *cscb.Object, compression api.Compression) (bool, time.Duration, time.Duration, error) {
	if object.Length > maxSinglePutObjectSize {
		return false, 0, 0, xerrors.Errorf("CSCB object length %d exceeds v1 single-put limit %d", object.Length, maxSinglePutObjectSize)
	}
	reader, err := object.Open()
	if err != nil {
		return false, 0, 0, err
	}
	defer func() { _ = reader.Close() }()

	internal.RecordConsolidatedUploadProgress(ctx, "s3_md5_started", object.Key, object.Length)
	md5Start := time.Now()
	contentMD5, err := computeContentMD5(ctx, object)
	md5Duration := time.Since(md5Start)
	if err != nil {
		return false, md5Duration, 0, err
	}
	internal.RecordConsolidatedUploadProgress(ctx, "s3_md5_finished", object.Key, object.Length)
	metadata := map[string]string{
		cscbMetadataFormat:             "cscb",
		cscbMetadataCompressionScope:   "batch-chunked",
		cscbMetadataSHA256:             object.SHA256,
		cscbMetadataCodec:              codecName(compression),
		cscbMetadataUncompressedLength: fmt.Sprintf("%d", object.PayloadUncompressedLength),
	}
	internal.RecordConsolidatedUploadProgress(ctx, "s3_put_started", object.Key, object.Length)
	putStart := time.Now()
	if _, err := s.client.PutObject(ctx, &awss3.PutObjectInput{
		Bucket:        aws.String(s.bucket),
		Key:           aws.String(object.Key),
		Body:          internal.NewConsolidatedUploadProgressReadCloser(ctx, "s3_put_read", reader),
		ContentLength: aws.Int64(int64(object.Length)),
		ContentMD5:    aws.String(contentMD5),
		ACL:           awss3types.ObjectCannedACLBucketOwnerFullControl,
		IfNoneMatch:   aws.String("*"),
		Metadata:      metadata,
	}, func(options *awss3.Options) {
		// Content-MD5 is the integrity check here. Avoid SDK-added CRC32 trailers
		// that switch large uploads to aws-chunked request bodies.
		options.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired
	}); err != nil {
		putDuration := time.Since(putStart)
		if isObjectPreconditionFailed(err) {
			found, validateErr := s.validateExistingConsolidatedObject(ctx, object)
			if validateErr != nil {
				return false, md5Duration, putDuration, validateErr
			}
			if found {
				return false, md5Duration, putDuration, nil
			}
		}
		return false, md5Duration, putDuration, xerrors.Errorf("failed to upload CSCB object to s3: %w", err)
	}
	putDuration := time.Since(putStart)
	internal.RecordConsolidatedUploadProgress(ctx, "s3_put_finished", object.Key, object.Length)
	return true, md5Duration, putDuration, nil
}

func computeContentMD5(ctx context.Context, object *cscb.Object) (string, error) {
	reader, err := object.Open()
	if err != nil {
		return "", err
	}
	defer func() { _ = reader.Close() }()

	// #nosec G401 -- S3 Content-MD5 is a transport integrity header, not a cryptographic trust boundary.
	h := md5.New()
	if _, err := io.Copy(h, internal.NewConsolidatedUploadProgressReadCloser(ctx, "s3_md5_read", reader)); err != nil {
		return "", xerrors.Errorf("failed to compute CSCB content md5: %w", err)
	}
	return base64.StdEncoding.EncodeToString(h.Sum(nil)), nil
}

func (s *blobStorageImpl) validateExistingConsolidatedObject(ctx context.Context, object *cscb.Object) (bool, error) {
	internal.RecordConsolidatedUploadProgress(ctx, "s3_head_existing_started", object.Key)
	out, err := s.client.HeadObject(ctx, &awss3.HeadObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(object.Key),
	})
	if err != nil {
		if isObjectNotFound(err) {
			internal.RecordConsolidatedUploadProgress(ctx, "s3_head_existing_not_found", object.Key)
			return false, nil
		}
		return false, xerrors.Errorf("failed to head CSCB object (bucket=%s, key=%s): %w", s.bucket, object.Key, err)
	}
	if err := validateConsolidatedHead(out, object); err != nil {
		return false, err
	}
	internal.RecordConsolidatedUploadProgress(ctx, "s3_head_existing_finished", object.Key, object.Length)
	return true, nil
}

func (s *blobStorageImpl) validateUploadedConsolidatedObject(ctx context.Context, object *cscb.Object) error {
	internal.RecordConsolidatedUploadProgress(ctx, "s3_head_uploaded_started", object.Key)
	out, err := s.client.HeadObject(ctx, &awss3.HeadObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(object.Key),
	})
	if err != nil {
		return xerrors.Errorf("failed to validate uploaded CSCB object (bucket=%s, key=%s): %w", s.bucket, object.Key, err)
	}
	if err := validateConsolidatedHead(out, object); err != nil {
		return err
	}
	internal.RecordConsolidatedUploadProgress(ctx, "s3_head_uploaded_finished", object.Key, object.Length)
	return nil
}

func validateConsolidatedHead(out *awss3.HeadObjectOutput, object *cscb.Object) error {
	if out == nil {
		return xerrors.New("empty CSCB HeadObject response")
	}
	if out.ContentLength == nil {
		return xerrors.Errorf("CSCB object %s has no content length", object.Key)
	}
	if uint64(*out.ContentLength) != object.Length {
		return xerrors.Errorf("CSCB object %s length mismatch: got %d want %d", object.Key, *out.ContentLength, object.Length)
	}
	if metadataSHA := out.Metadata[cscbMetadataSHA256]; metadataSHA != object.SHA256 {
		return xerrors.Errorf("CSCB object %s sha256 metadata mismatch: got %q want %q", object.Key, metadataSHA, object.SHA256)
	}
	return nil
}

func isObjectNotFound(err error) bool {
	var notFound *awss3types.NotFound
	if errors.As(err, &notFound) {
		return true
	}
	var noSuchKey *awss3types.NoSuchKey
	if errors.As(err, &noSuchKey) {
		return true
	}
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		switch apiErr.ErrorCode() {
		case "NotFound", "NoSuchKey", "404":
			return true
		}
	}
	return false
}

func isObjectPreconditionFailed(err error) bool {
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		switch apiErr.ErrorCode() {
		case "PreconditionFailed", "412":
			return true
		}
	}
	return false
}

func codecName(compression api.Compression) string {
	switch compression {
	case api.Compression_GZIP:
		return "gzip"
	case api.Compression_ZSTD:
		return "zstd"
	default:
		return "unknown"
	}
}

func (s *blobStorageImpl) Download(ctx context.Context, metadata *api.BlockMetadata) (*api.Block, error) {
	return s.instrumentDownload.Instrument(ctx, func(ctx context.Context) (*api.Block, error) {
		defer s.logDuration("download", time.Now())

		if metadata == nil {
			return nil, xerrors.New("block metadata is required")
		}
		if metadata.GetSkipped() {
			return s.skippedBlock(metadata), nil
		}
		bucket, err := s.bucketForMetadata(metadata)
		if err != nil {
			return nil, err
		}
		if isSingleBlockObject(metadata) {
			return s.downloadSingleBlock(ctx, bucket, metadata)
		}
		return s.downloadCSCBBlock(ctx, bucket, metadata)
	})
}

func (s *blobStorageImpl) DownloadMany(ctx context.Context, metadatas []*api.BlockMetadata) ([]*api.Block, error) {
	return s.instrumentDownloadMany.Instrument(ctx, func(ctx context.Context) ([]*api.Block, error) {
		defer s.logDuration("download_many", time.Now())

		result := make([]*api.Block, len(metadatas))
		singleBlockRefs := make([]downloadRef, 0, len(metadatas))
		cscbRefsByObject := make(map[cscbObjectLocation][]downloadRef)
		for i, metadata := range metadatas {
			if metadata == nil {
				return nil, xerrors.Errorf("block metadata is required at index %d", i)
			}
			if metadata.GetSkipped() {
				result[i] = s.skippedBlock(metadata)
				continue
			}
			bucket, err := s.bucketForMetadata(metadata)
			if err != nil {
				return nil, xerrors.Errorf("failed to resolve block storage at index %d: %w", i, err)
			}
			ref := downloadRef{
				index:    i,
				bucket:   bucket,
				metadata: metadata,
			}
			if isSingleBlockObject(metadata) {
				singleBlockRefs = append(singleBlockRefs, ref)
				continue
			}
			key := metadata.GetObjectKeyMain()
			if key == "" {
				return nil, xerrors.Errorf("missing CSCB object key for height %d", metadata.GetHeight())
			}
			location := cscbObjectLocation{bucket: bucket, key: key}
			cscbRefsByObject[location] = append(cscbRefsByObject[location], ref)
		}

		group, ctx := syncgroup.New(ctx, syncgroup.WithThrottling(s.downloadWorkerLimit()))
		for _, ref := range singleBlockRefs {
			ref := ref
			group.Go(func() error {
				block, err := s.downloadSingleBlock(ctx, ref.bucket, ref.metadata)
				if err != nil {
					return xerrors.Errorf("failed to download single-block object (input={%+v}): %w", ref.metadata, err)
				}
				result[ref.index] = block
				return nil
			})
		}
		for location, refs := range cscbRefsByObject {
			location, refs := location, refs
			group.Go(func() error {
				if err := s.downloadCSCBObject(ctx, location.bucket, location.key, refs, result); err != nil {
					return xerrors.Errorf("failed to download CSCB object (bucket=%s key=%s): %w", location.bucket, location.key, err)
				}
				return nil
			})
		}
		if err := group.Wait(); err != nil {
			return nil, err
		}
		return result, nil
	})
}

func (s *blobStorageImpl) bucketForMetadata(metadata *api.BlockMetadata) (string, error) {
	bucket, err := s.config.ResolveBlockStorageBucket(metadata.GetStorageGeneration())
	if err != nil {
		return "", xerrors.Errorf(
			"failed to resolve block storage generation for height %d: %w",
			metadata.GetHeight(),
			err,
		)
	}
	return bucket, nil
}

func (s *blobStorageImpl) skippedBlock(metadata *api.BlockMetadata) *api.Block {
	return &api.Block{
		Blockchain: s.config.Chain.Blockchain,
		Network:    s.config.Chain.Network,
		SideChain:  s.config.Chain.Sidechain,
		Metadata:   metadata,
		Blobdata:   nil,
	}
}

func isSingleBlockObject(metadata *api.BlockMetadata) bool {
	return metadata.GetObjectFormat() != api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_CSCB_BATCH || metadata.GetByteLength() == 0
}

func (s *blobStorageImpl) downloadWorkerLimit() int {
	limit := int(s.config.Api.NumWorkers)
	if limit <= 0 {
		return 1
	}
	return limit
}

func (s *blobStorageImpl) downloadSingleBlock(ctx context.Context, bucket string, metadata *api.BlockMetadata) (*api.Block, error) {
	return downloadSingleBlockFromBucket(ctx, bucket, s.downloader, s.blobStorageMetrics, metadata)
}

func (s *blobStorageImpl) downloadCSCBBlock(ctx context.Context, bucket string, metadata *api.BlockMetadata) (*api.Block, error) {
	result := make([]*api.Block, 1)
	ref := downloadRef{
		index:    0,
		bucket:   bucket,
		metadata: metadata,
	}
	if err := s.downloadCSCBObject(ctx, bucket, metadata.GetObjectKeyMain(), []downloadRef{ref}, result); err != nil {
		return nil, err
	}
	return result[0], nil
}

func (s *blobStorageImpl) downloadCSCBObject(ctx context.Context, bucket string, key string, refs []downloadRef, result []*api.Block) error {
	index, err := s.readCSCBIndex(ctx, bucket, key)
	if err != nil {
		return err
	}

	downloadsByChunk := make(map[uint32][]cscbBlockDownload)
	for _, ref := range refs {
		block, chunk, err := index.LookupBlock(ref.metadata)
		if err != nil {
			return err
		}
		downloadsByChunk[chunk.Index] = append(downloadsByChunk[chunk.Index], cscbBlockDownload{
			ref:   ref,
			block: block,
			chunk: chunk,
		})
	}

	chunkIndexes := make([]int, 0, len(downloadsByChunk))
	for chunkIndex := range downloadsByChunk {
		chunkIndexes = append(chunkIndexes, int(chunkIndex))
	}
	sort.Ints(chunkIndexes)

	for _, chunkIndex := range chunkIndexes {
		downloads := downloadsByChunk[uint32(chunkIndex)]
		chunk := downloads[0].chunk
		blocks := make([]*cscb.BlockDescriptor, len(downloads))
		for i, download := range downloads {
			blocks[i] = download.block
		}
		blockPayloads, err := s.downloadCSCBChunkPayloads(ctx, bucket, key, index.Header.Codec, chunk, blocks)
		if err != nil {
			return err
		}
		for i, download := range downloads {
			blockPayload := blockPayloads[i]
			block, err := unmarshalBlockData(bucket, key, download.ref.metadata, blockPayload)
			if err != nil {
				return err
			}
			result[download.ref.index] = block
		}
	}
	return nil
}

func (s *blobStorageImpl) readCSCBIndex(ctx context.Context, bucket string, key string) (*cscb.Index, error) {
	first, err := s.readObjectRange(ctx, bucket, key, 0, cscbInitialIndexReadSize-1)
	if err != nil {
		return nil, err
	}
	required, err := cscb.HeaderEnvelopeLength(first)
	if err != nil {
		return nil, err
	}
	if required <= uint64(len(first)) {
		return cscb.ParseIndex(first)
	}
	remaining, err := s.readObjectRange(ctx, bucket, key, uint64(len(first)), required-1)
	if err != nil {
		return nil, err
	}
	indexData := make([]byte, 0, required)
	indexData = append(indexData, first...)
	indexData = append(indexData, remaining...)
	return cscb.ParseIndex(indexData)
}

func (s *blobStorageImpl) downloadCSCBChunkPayloads(ctx context.Context, bucket string, key string, codec api.Compression, chunk *cscb.ChunkDescriptor, blocks []*cscb.BlockDescriptor) ([][]byte, error) {
	if len(blocks) == 0 {
		return nil, xerrors.New("CSCB block descriptors are required")
	}
	body, err := s.openObjectRangeByLength(ctx, bucket, key, chunk.CompressedPayloadOffset, chunk.CompressedLength)
	if err != nil {
		return nil, err
	}
	defer func() { _ = body.Close() }()

	countingBody := &countingReadCloser{ReadCloser: body}
	limitedBody, err := limitReaderByLength(countingBody, chunk.CompressedLength)
	if err != nil {
		return nil, err
	}
	payloads, err := cscb.ExtractBlockPayloadsFromChunkFrame(limitedBody, codec, chunk, blocks)
	s.blobStorageMetrics.blobDownloadedSize.Record(time.Duration(countingBody.bytesRead) * time.Millisecond)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return nil, storageerrors.ErrRequestCanceled
		}
		return nil, err
	}
	return payloads, nil
}

func (s *blobStorageImpl) openObjectRangeByLength(ctx context.Context, bucket string, key string, offset uint64, length uint64) (io.ReadCloser, error) {
	if length == 0 {
		return nil, xerrors.Errorf("empty range read for key %s at offset %d", key, offset)
	}
	end, err := inclusiveRangeEnd(offset, length)
	if err != nil {
		return nil, err
	}
	return s.openObjectRange(ctx, bucket, key, offset, end)
}

func (s *blobStorageImpl) readObjectRange(ctx context.Context, bucket string, key string, start uint64, end uint64) ([]byte, error) {
	if end < start {
		return nil, xerrors.Errorf("invalid range for key %s: start=%d end=%d", key, start, end)
	}
	body, err := s.openObjectRange(ctx, bucket, key, start, end)
	if err != nil {
		return nil, err
	}
	defer func() { _ = body.Close() }()
	expectedLength := end - start + 1
	data, err := readExpectedRangeBody(body, expectedLength, start == 0)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return nil, storageerrors.ErrRequestCanceled
		}
		return nil, xerrors.Errorf("failed to read s3 range body (bucket=%s, key=%s, range=bytes=%d-%d): %w", bucket, key, start, end, err)
	}
	s.blobStorageMetrics.blobDownloadedSize.Record(time.Duration(len(data)) * time.Millisecond)
	return data, nil
}

func (s *blobStorageImpl) openObjectRange(ctx context.Context, bucket string, key string, start uint64, end uint64) (io.ReadCloser, error) {
	if end < start {
		return nil, xerrors.Errorf("invalid range for key %s: start=%d end=%d", key, start, end)
	}
	output, err := s.client.GetObject(ctx, &awss3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Range:  aws.String(fmt.Sprintf("bytes=%d-%d", start, end)),
	})
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return nil, storageerrors.ErrRequestCanceled
		}
		return nil, xerrors.Errorf("failed to range download from s3 (bucket=%s, key=%s, range=bytes=%d-%d): %w", bucket, key, start, end, err)
	}
	if output.Body == nil {
		return nil, xerrors.Errorf("empty s3 body (bucket=%s, key=%s, range=bytes=%d-%d)", bucket, key, start, end)
	}
	return output.Body, nil
}

func limitReaderByLength(reader io.Reader, length uint64) (io.Reader, error) {
	const maxInt64 = uint64(1<<63 - 1)
	if length >= maxInt64 {
		return nil, xerrors.Errorf("range body length %d is too large", length)
	}
	return io.LimitReader(reader, int64(length)), nil
}

type countingReadCloser struct {
	io.ReadCloser
	bytesRead uint64
}

func (r *countingReadCloser) Read(p []byte) (int, error) {
	n, err := r.ReadCloser.Read(p)
	r.bytesRead += uint64(n)
	return n, err
}

func readExpectedRangeBody(body io.Reader, expectedLength uint64, allowShort bool) ([]byte, error) {
	const maxInt64 = uint64(1<<63 - 1)
	if expectedLength >= maxInt64 {
		return nil, xerrors.Errorf("range body length %d is too large", expectedLength)
	}
	data, err := io.ReadAll(io.LimitReader(body, int64(expectedLength)+1))
	if err != nil {
		return nil, err
	}
	if uint64(len(data)) > expectedLength || (!allowShort && uint64(len(data)) != expectedLength) {
		return nil, xerrors.Errorf("range body length mismatch: got %d want %d", len(data), expectedLength)
	}
	return data, nil
}

func inclusiveRangeEnd(offset uint64, length uint64) (uint64, error) {
	if length == 0 {
		return 0, xerrors.New("range length must be positive")
	}
	if offset > ^uint64(0)-(length-1) {
		return 0, xerrors.Errorf("range overflow: offset=%d length=%d", offset, length)
	}
	return offset + length - 1, nil
}

func unmarshalBlockData(bucket string, key string, metadata *api.BlockMetadata, blockData []byte) (*api.Block, error) {
	var block api.Block
	err := proto.Unmarshal(blockData, &block)
	if err != nil {
		return nil, xerrors.Errorf("failed to unmarshal data downloaded from s3 bucket %s key %s: %w", bucket, key, err)
	}

	// When metadata is loaded from meta storage,
	// the new fields, e.g. ParentHeight, may be populated with default values.
	// Overwrite metadata using the one loaded from meta storage.
	block.Metadata = metadata
	return &block, nil
}

func (s *blobStorageImpl) PreSign(ctx context.Context, metadata *api.BlockMetadata) (string, error) {
	if metadata == nil {
		return "", xerrors.New("block metadata is required")
	}
	bucket, err := s.bucketForMetadata(metadata)
	if err != nil {
		return "", err
	}
	presignClient := awss3.NewPresignClient(s.client.(*awss3.Client))
	presignResult, err := presignClient.PresignGetObject(ctx, &awss3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(metadata.GetObjectKeyMain()),
	}, awss3.WithPresignExpires(s.config.AWS.PresignedUrlExpiration))
	if err != nil {
		return "", xerrors.Errorf("failed to generate presigned url: %w", err)
	}
	return presignResult.URL, nil
}

func (s *blobStorageImpl) logDuration(method string, start time.Time) {
	s.logger.Debug(
		"blob_storage",
		zap.String("storage_type", "s3"),
		zap.String("method", method),
		zap.Duration("duration", time.Since(start)),
	)
}
