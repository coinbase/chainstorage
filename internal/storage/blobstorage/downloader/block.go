package downloader

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"runtime"
	"sort"
	"sync"
	"time"

	"go.uber.org/fx"
	"go.uber.org/zap"
	"golang.org/x/xerrors"
	"google.golang.org/protobuf/proto"
	tracehttp "gopkg.in/DataDog/dd-trace-go.v1/contrib/net/http"

	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/storage/blobstorage/cscb"
	"github.com/coinbase/chainstorage/internal/storage/internal/errors"
	storage_utils "github.com/coinbase/chainstorage/internal/storage/utils"
	"github.com/coinbase/chainstorage/internal/utils/finalizer"
	"github.com/coinbase/chainstorage/internal/utils/fxparams"
	"github.com/coinbase/chainstorage/internal/utils/log"
	"github.com/coinbase/chainstorage/internal/utils/retry"
	"github.com/coinbase/chainstorage/internal/utils/syncgroup"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

type (
	BlockDownloader interface {
		Download(ctx context.Context, blockFile *api.BlockFile) (*api.Block, error)
		DownloadMany(ctx context.Context, blockFiles []*api.BlockFile) ([]*api.Block, error)

		// OpenRawBlockPayload opens the validated, decompressed protobuf
		// payload bytes for one raw block without unmarshaling api.Block.
		// Callers MUST close the returned payload.
		OpenRawBlockPayload(ctx context.Context, blockFile *api.BlockFile) (*RawBlockPayload, error)

		// OpenRawBlockPayloads opens an iterator over validated,
		// decompressed protobuf payload bytes in blockFiles order without
		// unmarshaling api.Block. The iterator returns io.EOF when
		// exhausted. Callers MUST close each returned payload and the
		// iterator.
		OpenRawBlockPayloads(ctx context.Context, blockFiles []*api.BlockFile) (RawBlockPayloadIterator, error)

		// DownloadStream spools the compressed block to a local temp
		// file, decompresses to a second temp file, and returns a
		// chain-agnostic SpooledBlock handle. The decompressed spool
		// is held open until SpooledBlock.Close() is called — callers
		// MUST close it to release disk.
		//
		// This is plumbing only: no proto parsing happens at the
		// downloader layer. Chain-specific walkers live in the
		// parser package, which consumes the SpooledBlock to produce
		// a StreamedBlock with chain-aware lazy accessors.
		//
		// A runtime cleanup is wired as a safety net for leaks but
		// runs non-deterministically; do not rely on it.
		DownloadStream(ctx context.Context, blockFile *api.BlockFile) (*SpooledBlock, error)

		// OpenSpooledBlocks opens an iterator of SpooledBlocks over
		// blockFiles in input order. Unlike calling DownloadStream per
		// block, consecutive CSCB block files in the same chunk share one
		// range request and one decompressor pass (chunk-once). Callers
		// MUST close each returned SpooledBlock and the iterator.
		OpenSpooledBlocks(ctx context.Context, blockFiles []*api.BlockFile) (SpooledBlockIterator, error)
	}

	// SpooledBlock is a chain-agnostic handle over the decompressed
	// bytes of a block, backed by a local temp file (DownloadStream)
	// or by an in-memory payload (OpenSpooledBlocks). The parser
	// package consumes it and produces a chain-specific
	// StreamedBlock with lazy accessors.
	//
	// Open may be called multiple times; each call returns an
	// independent io.ReadCloser positioned at byte 0 of the
	// decompressed stream that also implements io.ReaderAt. The
	// caller is responsible for closing every returned reader.
	//
	// Close releases the backing store (removes the temp file or
	// drops the payload) and is safe to call multiple times.
	SpooledBlock struct {
		// BlockFile is the source descriptor (chain, tag, height,
		// hash, compression). Parser implementations branch on
		// BlockFile.Skipped, among other fields.
		BlockFile *api.BlockFile

		// Open returns a fresh read-only handle on the decompressed
		// spool file. Never nil for non-skipped blocks; for skipped
		// blocks Open returns an empty reader so callers can treat
		// the API uniformly.
		Open func() (io.ReadCloser, error)

		closeOnce sync.Once
		closeFn   func() error
	}

	BlockDownloaderParams struct {
		fx.In
		fxparams.Params
		HttpClient HTTPClient
	}

	HTTPClient interface {
		Do(req *http.Request) (*http.Response, error)
	}

	blockDownloaderImpl struct {
		config        *config.Config
		logger        *zap.Logger
		httpClient    HTTPClient
		retry         retry.RetryWithResult[*api.Block]
		retryBytes    retry.RetryWithResult[[]byte]
		retryPayloads retry.RetryWithResult[[][]byte]
		retryRaw      retry.RetryWithResult[*RawBlockPayload]
	}

	downloadRef struct {
		index     int
		blockFile *api.BlockFile
	}

	cscbBlockDownload struct {
		ref   downloadRef
		block *cscb.BlockDescriptor
		chunk *cscb.ChunkDescriptor
	}

	downloadLimiter chan struct{}
)

// Close releases the locally-spooled decompressed file. Safe to call
// multiple times; after Close, Open returns a file-not-found error.
func (s *SpooledBlock) Close() error {
	if s == nil {
		return nil
	}
	var err error
	s.closeOnce.Do(func() {
		if s.closeFn != nil {
			err = s.closeFn()
		}
	})
	return err
}

const (
	timeout                  = time.Second * 30
	cscbInitialIndexReadSize = 64 * 1024
)

func NewBlockDownloader(params BlockDownloaderParams) BlockDownloader {
	logger := log.WithPackage(params.Logger)
	return &blockDownloaderImpl{
		config:        params.Config,
		logger:        logger,
		httpClient:    params.HttpClient,
		retry:         retry.NewWithResult[*api.Block](retry.WithLogger(logger)),
		retryBytes:    retry.NewWithResult[[]byte](retry.WithLogger(logger)),
		retryPayloads: retry.NewWithResult[[][]byte](retry.WithLogger(logger)),
		retryRaw:      retry.NewWithResult[*RawBlockPayload](retry.WithLogger(logger)),
	}
}

func NewHTTPClient() HTTPClient {
	httpClient := &http.Client{
		Timeout: timeout,
	}
	httpClient = tracehttp.WrapClient(httpClient, tracehttp.RTWithResourceNamer(func(req *http.Request) string {
		return "/chainstorage/blobstorage/downloader"
	}))
	return httpClient
}

func (d *blockDownloaderImpl) Download(ctx context.Context, blockFile *api.BlockFile) (*api.Block, error) {
	if blockFile.Skipped {
		// No blob data is available when the block is skipped.
		return d.skippedBlock(blockFile), nil
	}

	if isCSCBBlockFile(blockFile) {
		defer d.logDuration(time.Now())
		payload, err := d.downloadCSCBBlockPayload(ctx, blockFile)
		if err != nil {
			return nil, err
		}
		return unmarshalCSCBBlock(blockFile, payload)
	}

	defer d.logDuration(time.Now())
	return d.retry.Retry(ctx, func(ctx context.Context) (*api.Block, error) {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, blockFile.FileUrl, nil)
		if err != nil {
			return nil, xerrors.Errorf("failed to create download request: %w", err)
		}

		httpResp, err := d.httpClient.Do(req)
		if err != nil {
			return nil, retry.Retryable(xerrors.Errorf("failed to download block file: %w", err))
		}

		finalizer := finalizer.WithCloser(httpResp.Body)
		defer finalizer.Finalize()

		if statusCode := httpResp.StatusCode; statusCode != http.StatusOK {
			if statusCode == http.StatusRequestTimeout ||
				statusCode == http.StatusTooManyRequests ||
				statusCode >= http.StatusInternalServerError {
				return nil, retry.Retryable(xerrors.Errorf("received %d status code: %w", statusCode, errors.ErrDownloadFailure))
			} else {
				return nil, xerrors.Errorf("received non-retryable %d status code: %w", statusCode, errors.ErrDownloadFailure)
			}
		}

		bodyBytes, err := ioutil.ReadAll(httpResp.Body)
		if err != nil {
			return nil, retry.Retryable(xerrors.Errorf("failed to read body: %w", err))
		}

		block := new(api.Block)
		blockData, err := storage_utils.Decompress(bodyBytes, blockFile.Compression)
		if err != nil {
			return nil, xerrors.Errorf("failed to decompress block data with type %v: %w", blockFile.Compression.String(), err)
		}

		if err := proto.Unmarshal(blockData, block); err != nil {
			return nil, xerrors.Errorf("failed to unmarshal file contents: %w", err)
		}

		return block, finalizer.Close()
	})
}

func (d *blockDownloaderImpl) DownloadMany(ctx context.Context, blockFiles []*api.BlockFile) ([]*api.Block, error) {
	defer d.logDuration(time.Now())

	result := make([]*api.Block, len(blockFiles))
	singleBlockRefs := make([]downloadRef, 0, len(blockFiles))
	cscbRefsByURL := make(map[string][]downloadRef)
	for i, blockFile := range blockFiles {
		if blockFile.GetSkipped() {
			result[i] = d.skippedBlock(blockFile)
			continue
		}
		ref := downloadRef{
			index:     i,
			blockFile: blockFile,
		}
		if !isCSCBBlockFile(blockFile) {
			singleBlockRefs = append(singleBlockRefs, ref)
			continue
		}
		fileURL := blockFile.GetFileUrl()
		if fileURL == "" {
			return nil, xerrors.Errorf("missing CSCB file url for height %d", blockFile.GetHeight())
		}
		cscbRefsByURL[fileURL] = append(cscbRefsByURL[fileURL], ref)
	}

	limiter := newDownloadLimiter(d.downloadWorkerLimit())
	group, ctx := syncgroup.New(ctx)
	for _, ref := range singleBlockRefs {
		ref := ref
		group.Go(func() error {
			var block *api.Block
			err := limiter.Do(ctx, func() error {
				var err error
				block, err = d.Download(ctx, ref.blockFile)
				return err
			})
			if err != nil {
				return xerrors.Errorf("failed to download single-block file (height=%d): %w", ref.blockFile.GetHeight(), err)
			}
			result[ref.index] = block
			return nil
		})
	}
	for fileURL, refs := range cscbRefsByURL {
		fileURL, refs := fileURL, refs
		group.Go(func() error {
			if err := d.downloadCSCBFile(ctx, limiter, fileURL, refs, result); err != nil {
				return xerrors.Errorf("failed to download CSCB block file %s: %w", fileURL, err)
			}
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		return nil, err
	}
	return result, nil
}

func (c *blockDownloaderImpl) logDuration(start time.Time) {
	c.logger.Debug(
		"downloader.request",
		zap.Duration("duration", time.Since(start)),
	)
}

// DownloadStream spools decompressed block bytes to a local file and
// returns a chain-agnostic SpooledBlock handle over that file. Single-block
// objects stream the compressed HTTP body through a decompressor
// directly into the spool. CSCB objects range-read the target
// compressed chunk, decode and validate that one chunk in memory, and
// spool only the requested block payload. The spool persists until
// SpooledBlock.Close() is called — callers MUST close it.
//
// There is only ONE tempfile per call (the decompressed spool). HTTP
// retries truncate + rewind the spool and reissue the GET. This keeps
// per-block I/O cost and tempfile churn minimal under concurrent
// workloads.
//
// For skipped blocks, the returned SpooledBlock has an Open() that
// returns an empty reader and a no-op Close; the parser detects
// skipped blocks via BlockFile.Skipped.
func (d *blockDownloaderImpl) DownloadStream(ctx context.Context, blockFile *api.BlockFile) (*SpooledBlock, error) {
	if blockFile.Skipped {
		return &SpooledBlock{
			BlockFile: blockFile,
			Open: func() (io.ReadCloser, error) {
				return io.NopCloser(io.LimitReader(nil, 0)), nil
			},
			closeFn: func() error { return nil },
		}, nil
	}

	defer d.logDuration(time.Now())

	// Single tempfile: the decompressed spool. Ownership transfers
	// to the returned SpooledBlock on success; removed here on any
	// error path.
	tmpDecompressed, err := os.CreateTemp("", "chainstorage-block-decompressed-*.bin")
	if err != nil {
		return nil, xerrors.Errorf("create decompressed spool: %w", err)
	}
	decompressedPath := tmpDecompressed.Name()

	transferred := false
	defer func() {
		if !transferred {
			_ = tmpDecompressed.Close()
			_ = os.Remove(decompressedPath)
		}
	}()

	if err := d.spoolDecompressedToFile(ctx, blockFile, tmpDecompressed); err != nil {
		return nil, err
	}
	// Intentionally skip Sync: the spool is read back through the
	// OS page cache by the same process, so flushing to disk only
	// adds fdatasync latency under concurrent load.
	if err := tmpDecompressed.Close(); err != nil {
		return nil, xerrors.Errorf("close decompressed spool: %w", err)
	}

	stream := &SpooledBlock{
		BlockFile: blockFile,
		Open: func() (io.ReadCloser, error) {
			f, err := os.Open(decompressedPath)
			if err != nil {
				return nil, xerrors.Errorf("open decompressed spool: %w", err)
			}
			return f, nil
		},
		closeFn: func() error {
			if err := os.Remove(decompressedPath); err != nil && !os.IsNotExist(err) {
				return xerrors.Errorf("remove decompressed spool: %w", err)
			}
			return nil
		},
	}

	// Safety net: if the caller leaks the stream, GC will eventually
	// clean up the spool file. Non-deterministic — callers must still
	// explicitly Close().
	runtime.AddCleanup(stream, func(path string) {
		_ = os.Remove(path)
	}, decompressedPath)

	transferred = true
	return stream, nil
}

// spoolDecompressedToFile performs a (retryable) HTTP GET, wraps the
// response body in the appropriate decompressor, and writes the
// decompressed block payload to dst. Single-block objects are streamed into
// dst. CSCB objects buffer one decompressed chunk in memory for chunk
// CRC validation, then write only the requested block payload to dst.
// The file is truncated + rewound on each retry attempt. This combines
// download + decompression into a single pass with only one tempfile,
// reducing per-block I/O cost under concurrent workloads compared to
// separate compressed + decompressed spools.
func (d *blockDownloaderImpl) spoolDecompressedToFile(ctx context.Context, blockFile *api.BlockFile, dst *os.File) error {
	if isCSCBBlockFile(blockFile) {
		payload, err := d.downloadCSCBBlockPayload(ctx, blockFile)
		if err != nil {
			return err
		}
		if _, err := dst.Write(payload); err != nil {
			return xerrors.Errorf("write CSCB block payload to spool: %w", err)
		}
		return nil
	}

	_, err := d.retry.Retry(ctx, func(ctx context.Context) (*api.Block, error) {
		if _, err := dst.Seek(0, io.SeekStart); err != nil {
			return nil, err
		}
		if err := dst.Truncate(0); err != nil {
			return nil, err
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, blockFile.FileUrl, nil)
		if err != nil {
			return nil, xerrors.Errorf("create request: %w", err)
		}

		resp, err := d.httpClient.Do(req)
		if err != nil {
			return nil, retry.Retryable(xerrors.Errorf("http do: %w", err))
		}
		closer := finalizer.WithCloser(resp.Body)
		defer closer.Finalize()

		if sc := resp.StatusCode; sc != http.StatusOK {
			if sc == http.StatusRequestTimeout || sc == http.StatusTooManyRequests || sc >= http.StatusInternalServerError {
				return nil, retry.Retryable(xerrors.Errorf("status %d: %w", sc, errors.ErrDownloadFailure))
			}
			return nil, xerrors.Errorf("status %d: %w", sc, errors.ErrDownloadFailure)
		}

		dec, err := storage_utils.DecompressReader(resp.Body, blockFile.Compression)
		if err != nil {
			return nil, xerrors.Errorf("wrap decompressor: %w", err)
		}
		defer func() { _ = dec.Close() }()

		if _, err := io.Copy(dst, dec); err != nil {
			return nil, retry.Retryable(xerrors.Errorf("decompress to spool: %w", err))
		}
		return nil, closer.Close()
	})
	return err
}

func (d *blockDownloaderImpl) downloadCSCBBlockPayload(ctx context.Context, blockFile *api.BlockFile) ([]byte, error) {
	index, err := d.readCSCBIndex(ctx, nil, blockFile.GetFileUrl())
	if err != nil {
		return nil, err
	}
	block, chunk, err := index.LookupBlock(blockFileToMetadata(blockFile))
	if err != nil {
		return nil, err
	}
	blockPayloads, err := d.downloadCSCBChunkPayloads(ctx, blockFile.GetFileUrl(), index.Header.Codec, chunk, []*cscb.BlockDescriptor{block})
	if err != nil {
		return nil, err
	}
	return blockPayloads[0], nil
}

func (d *blockDownloaderImpl) downloadCSCBFile(ctx context.Context, limiter downloadLimiter, fileURL string, refs []downloadRef, result []*api.Block) error {
	index, err := d.readCSCBIndex(ctx, limiter, fileURL)
	if err != nil {
		return err
	}

	downloadsByChunk := make(map[uint32][]cscbBlockDownload)
	for _, ref := range refs {
		metadata := blockFileToMetadata(ref.blockFile)
		block, chunk, err := index.LookupBlock(metadata)
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

	group, ctx := syncgroup.New(ctx)
	for _, chunkIndex := range chunkIndexes {
		downloads := downloadsByChunk[uint32(chunkIndex)]
		group.Go(func() error {
			return limiter.Do(ctx, func() error {
				chunk := downloads[0].chunk
				blocks := make([]*cscb.BlockDescriptor, len(downloads))
				for i, download := range downloads {
					blocks[i] = download.block
				}
				blockPayloads, err := d.downloadCSCBChunkPayloads(ctx, fileURL, index.Header.Codec, chunk, blocks)
				if err != nil {
					return err
				}
				for i, download := range downloads {
					blockPayload := blockPayloads[i]
					block, err := unmarshalCSCBBlock(download.ref.blockFile, blockPayload)
					if err != nil {
						return err
					}
					result[download.ref.index] = block
				}
				return nil
			})
		})
	}
	if err := group.Wait(); err != nil {
		return err
	}
	return nil
}

func (d *blockDownloaderImpl) readCSCBIndex(ctx context.Context, limiter downloadLimiter, fileURL string) (*cscb.Index, error) {
	first, err := d.readHTTPRange(ctx, limiter, fileURL, 0, cscbInitialIndexReadSize-1)
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
	remaining, err := d.readHTTPRange(ctx, limiter, fileURL, uint64(len(first)), required-1)
	if err != nil {
		return nil, err
	}
	indexData := make([]byte, 0, required)
	indexData = append(indexData, first...)
	indexData = append(indexData, remaining...)
	return cscb.ParseIndex(indexData)
}

func (d *blockDownloaderImpl) downloadCSCBChunkPayloads(ctx context.Context, fileURL string, codec api.Compression, chunk *cscb.ChunkDescriptor, blocks []*cscb.BlockDescriptor) ([][]byte, error) {
	if len(blocks) == 0 {
		return nil, xerrors.New("CSCB block descriptors are required")
	}
	return d.retryPayloads.Retry(ctx, func(ctx context.Context) ([][]byte, error) {
		payloads, err := d.downloadCSCBChunkPayloadsUnthrottled(ctx, fileURL, codec, chunk, blocks)
		if err != nil && (xerrors.Is(err, io.EOF) || xerrors.Is(err, io.ErrUnexpectedEOF)) {
			return nil, retry.Retryable(err)
		}
		return payloads, err
	})
}

func (d *blockDownloaderImpl) downloadCSCBChunkPayloadsUnthrottled(ctx context.Context, fileURL string, codec api.Compression, chunk *cscb.ChunkDescriptor, blocks []*cscb.BlockDescriptor) ([][]byte, error) {
	body, err := d.openHTTPRangeByLengthUnthrottled(ctx, fileURL, chunk.CompressedPayloadOffset, chunk.CompressedLength)
	if err != nil {
		return nil, err
	}
	defer func() { _ = body.Close() }()

	limitedBody, err := limitReaderByLength(body, chunk.CompressedLength)
	if err != nil {
		return nil, err
	}
	return cscb.ExtractBlockPayloadsFromChunkFrame(limitedBody, codec, chunk, blocks)
}

func (d *blockDownloaderImpl) readHTTPRange(ctx context.Context, limiter downloadLimiter, fileURL string, start uint64, end uint64) ([]byte, error) {
	if end < start {
		return nil, xerrors.Errorf("invalid range for %s: start=%d end=%d", fileURL, start, end)
	}
	var body []byte
	err := limiter.Do(ctx, func() error {
		var err error
		body, err = d.readHTTPRangeUnthrottled(ctx, fileURL, start, end)
		return err
	})
	return body, err
}

func (d *blockDownloaderImpl) readHTTPRangeUnthrottled(ctx context.Context, fileURL string, start uint64, end uint64) ([]byte, error) {
	return d.retryBytes.Retry(ctx, func(ctx context.Context) ([]byte, error) {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, fileURL, nil)
		if err != nil {
			return nil, xerrors.Errorf("failed to create range download request: %w", err)
		}
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", start, end))

		httpResp, err := d.httpClient.Do(req)
		if err != nil {
			return nil, retry.Retryable(xerrors.Errorf("failed to range download block file: %w", err))
		}

		finalizer := finalizer.WithCloser(httpResp.Body)
		defer finalizer.Finalize()

		ignoredInitialRange := false
		if statusCode := httpResp.StatusCode; statusCode != http.StatusPartialContent {
			if statusCode == http.StatusOK && start == 0 {
				// Some test transports or non-S3-compatible stores may
				// ignore Range for the initial index read. Treat that as
				// usable because the object starts at byte 0 and contains
				// at least the header/envelope we need.
				ignoredInitialRange = true
			} else if statusCode == http.StatusRequestTimeout ||
				statusCode == http.StatusTooManyRequests ||
				statusCode >= http.StatusInternalServerError {
				return nil, retry.Retryable(xerrors.Errorf("received %d status code: %w", statusCode, errors.ErrDownloadFailure))
			} else {
				return nil, xerrors.Errorf("received non-retryable %d status code: %w", statusCode, errors.ErrDownloadFailure)
			}
		}

		expectedLength := end - start + 1
		var bodyBytes []byte
		var readErr error
		if ignoredInitialRange {
			bodyBytes, readErr = readRangePrefixBody(httpResp.Body, expectedLength)
		} else {
			bodyBytes, readErr = readExpectedRangeBody(httpResp.Body, expectedLength, start == 0)
		}
		if readErr != nil {
			return nil, retry.Retryable(xerrors.Errorf("failed to read range body: %w", readErr))
		}
		return bodyBytes, finalizer.Close()
	})
}

func (d *blockDownloaderImpl) openHTTPRangeByLengthUnthrottled(ctx context.Context, fileURL string, offset uint64, length uint64) (io.ReadCloser, error) {
	if length == 0 {
		return nil, xerrors.Errorf("empty range read for %s at offset %d", fileURL, offset)
	}
	end, err := inclusiveRangeEnd(offset, length)
	if err != nil {
		return nil, err
	}
	return d.openHTTPRangeUnthrottled(ctx, fileURL, offset, end)
}

func (d *blockDownloaderImpl) openHTTPRangeUnthrottled(ctx context.Context, fileURL string, start uint64, end uint64) (io.ReadCloser, error) {
	if end < start {
		return nil, xerrors.Errorf("invalid range for %s: start=%d end=%d", fileURL, start, end)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fileURL, nil)
	if err != nil {
		return nil, xerrors.Errorf("failed to create range download request: %w", err)
	}
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", start, end))

	httpResp, err := d.httpClient.Do(req)
	if err != nil {
		return nil, retry.Retryable(xerrors.Errorf("failed to range download block file: %w", err))
	}
	if statusCode := httpResp.StatusCode; statusCode != http.StatusPartialContent {
		if httpResp.Body != nil {
			_ = httpResp.Body.Close()
		}
		if statusCode == http.StatusRequestTimeout ||
			statusCode == http.StatusTooManyRequests ||
			statusCode >= http.StatusInternalServerError {
			return nil, retry.Retryable(xerrors.Errorf("received %d status code: %w", statusCode, errors.ErrDownloadFailure))
		}
		return nil, xerrors.Errorf("received non-retryable %d status code: %w", statusCode, errors.ErrDownloadFailure)
	}
	if httpResp.Body == nil {
		return nil, xerrors.Errorf("empty range body for %s", fileURL)
	}
	return httpResp.Body, nil
}

func limitReaderByLength(reader io.Reader, length uint64) (io.Reader, error) {
	const maxInt64 = uint64(1<<63 - 1)
	if length >= maxInt64 {
		return nil, xerrors.Errorf("range body length %d is too large", length)
	}
	return io.LimitReader(reader, int64(length)), nil
}

func readRangePrefixBody(body io.Reader, maxLength uint64) ([]byte, error) {
	const maxInt64 = uint64(1<<63 - 1)
	if maxLength >= maxInt64 {
		return nil, xerrors.Errorf("range body length %d is too large", maxLength)
	}
	return io.ReadAll(io.LimitReader(body, int64(maxLength)))
}

func readExpectedRangeBody(body io.Reader, expectedLength uint64, allowShort bool) ([]byte, error) {
	const maxInt64 = uint64(1<<63 - 1)
	if expectedLength >= maxInt64 {
		return nil, xerrors.Errorf("range body length %d is too large", expectedLength)
	}
	bodyBytes, err := io.ReadAll(io.LimitReader(body, int64(expectedLength)+1))
	if err != nil {
		return nil, err
	}
	if uint64(len(bodyBytes)) > expectedLength || (!allowShort && uint64(len(bodyBytes)) != expectedLength) {
		return nil, xerrors.Errorf("range body length mismatch: got %d want %d", len(bodyBytes), expectedLength)
	}
	return bodyBytes, nil
}

func newDownloadLimiter(limit int) downloadLimiter {
	if limit <= 0 {
		return nil
	}
	return make(downloadLimiter, limit)
}

func (l downloadLimiter) Do(ctx context.Context, fn func() error) error {
	if l == nil {
		return fn()
	}
	select {
	case l <- struct{}{}:
		defer func() { <-l }()
		return fn()
	case <-ctx.Done():
		return ctx.Err()
	}
}

func isCSCBBlockFile(blockFile *api.BlockFile) bool {
	return blockFile.GetObjectFormat() == api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_CSCB_BATCH && blockFile.GetByteLength() != 0
}

func (d *blockDownloaderImpl) skippedBlock(blockFile *api.BlockFile) *api.Block {
	return &api.Block{
		Blockchain: d.config.Chain.Blockchain,
		Network:    d.config.Chain.Network,
		SideChain:  d.config.Chain.Sidechain,
		Metadata: &api.BlockMetadata{
			Tag:     blockFile.GetTag(),
			Height:  blockFile.GetHeight(),
			Skipped: true,
		},
		Blobdata: nil,
	}
}

func blockFileToMetadata(blockFile *api.BlockFile) *api.BlockMetadata {
	return &api.BlockMetadata{
		Tag:                blockFile.GetTag(),
		Hash:               blockFile.GetHash(),
		ParentHash:         blockFile.GetParentHash(),
		Height:             blockFile.GetHeight(),
		ParentHeight:       blockFile.GetParentHeight(),
		Skipped:            blockFile.GetSkipped(),
		Timestamp:          blockFile.GetBlockTimestamp(),
		ObjectFormat:       blockFile.GetObjectFormat(),
		ByteOffset:         blockFile.GetByteOffset(),
		ByteLength:         blockFile.GetByteLength(),
		UncompressedLength: blockFile.GetUncompressedLength(),
	}
}

func unmarshalCSCBBlock(blockFile *api.BlockFile, blockData []byte) (*api.Block, error) {
	var block api.Block
	if err := proto.Unmarshal(blockData, &block); err != nil {
		return nil, xerrors.Errorf("failed to unmarshal CSCB block payload: %w", err)
	}
	block.Metadata = blockFileToMetadata(blockFile)
	return &block, nil
}

func (d *blockDownloaderImpl) downloadWorkerLimit() int {
	if limit := int(d.config.SDK.NumWorkers); limit > 0 {
		return limit
	}
	if limit := int(d.config.Api.NumWorkers); limit > 0 {
		return limit
	}
	return 1
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
