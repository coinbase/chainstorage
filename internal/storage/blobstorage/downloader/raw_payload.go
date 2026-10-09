package downloader

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"sync"

	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/storage/blobstorage/cscb"
	"github.com/coinbase/chainstorage/internal/storage/internal/errors"
	storage_utils "github.com/coinbase/chainstorage/internal/storage/utils"
	"github.com/coinbase/chainstorage/internal/utils/retry"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

type (
	// RawBlockPayload is an open reader for one validated, decompressed
	// protobuf-serialized api.Block payload. It deliberately does not unmarshal
	// the payload.
	RawBlockPayload struct {
		BlockFile *api.BlockFile
		Length    uint64

		reader    io.ReadCloser
		closeOnce sync.Once
	}

	// RawBlockPayloadIterator yields raw block payloads in input order and
	// returns io.EOF when exhausted. The caller owns each returned payload and
	// must close it before advancing if it wants bounded resources.
	RawBlockPayloadIterator interface {
		Next(ctx context.Context) (*RawBlockPayload, error)
		Close() error
	}

	rawBlockPayloadIteratorImpl struct {
		downloader *blockDownloaderImpl
		blockFiles []*api.BlockFile
		current    int
		pending    []*RawBlockPayload
		indexByURL map[string]*cscb.Index
		closed     bool
	}

	multiCloserReadCloser struct {
		io.Reader
		closers   []io.Closer
		closeOnce sync.Once
	}

	exactLengthReadCloser struct {
		reader    io.ReadCloser
		height    uint64
		remaining uint64
		validated bool
		closed    bool
	}

	limitedReadCloser struct {
		io.Reader
		closer io.Closer
	}
)

func (d *blockDownloaderImpl) OpenRawBlockPayload(ctx context.Context, blockFile *api.BlockFile) (*RawBlockPayload, error) {
	if blockFile == nil {
		return nil, xerrors.New("block file is required")
	}
	if blockFile.GetSkipped() {
		return newBytesRawBlockPayload(blockFile, nil), nil
	}
	if isCSCBBlockFile(blockFile) {
		return d.openRawCSCBBlockPayload(ctx, blockFile)
	}
	return d.retryRaw.Retry(ctx, func(ctx context.Context) (*RawBlockPayload, error) {
		payload, err := d.openRawSingleBlockPayload(ctx, blockFile)
		if err != nil && (xerrors.Is(err, io.EOF) || xerrors.Is(err, io.ErrUnexpectedEOF)) {
			return nil, retry.Retryable(err)
		}
		return payload, err
	})
}

func (d *blockDownloaderImpl) OpenRawBlockPayloads(ctx context.Context, blockFiles []*api.BlockFile) (RawBlockPayloadIterator, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return &rawBlockPayloadIteratorImpl{
		downloader: d,
		blockFiles: blockFiles,
		indexByURL: make(map[string]*cscb.Index),
	}, nil
}

func (p *RawBlockPayload) Read(buf []byte) (int, error) {
	if p == nil || p.reader == nil {
		return 0, xerrors.New("raw block payload is closed")
	}
	return p.reader.Read(buf)
}

func (p *RawBlockPayload) Close() error {
	if p == nil {
		return nil
	}
	var err error
	p.closeOnce.Do(func() {
		if p.reader != nil {
			err = p.reader.Close()
		}
		p.reader = nil
	})
	return err
}

func (i *rawBlockPayloadIteratorImpl) Next(ctx context.Context) (*RawBlockPayload, error) {
	if i.closed {
		return nil, xerrors.New("raw block payload iterator is closed")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(i.pending) > 0 {
		next := i.pending[0]
		i.pending = i.pending[1:]
		return next, nil
	}
	if i.current >= len(i.blockFiles) {
		return nil, io.EOF
	}

	blockFile := i.blockFiles[i.current]
	if blockFile.GetSkipped() || !isCSCBBlockFile(blockFile) {
		i.current++
		return i.downloader.OpenRawBlockPayload(ctx, blockFile)
	}
	if err := i.loadNextCSCBGroup(ctx); err != nil {
		return nil, err
	}
	return i.Next(ctx)
}

func (i *rawBlockPayloadIteratorImpl) Close() error {
	if i.closed {
		return nil
	}
	i.closed = true
	var closeErr error
	for _, payload := range i.pending {
		if err := payload.Close(); closeErr == nil && err != nil {
			closeErr = err
		}
	}
	i.pending = nil
	return closeErr
}

func (i *rawBlockPayloadIteratorImpl) loadNextCSCBGroup(ctx context.Context) error {
	firstFile := i.blockFiles[i.current]
	fileURL := firstFile.GetFileUrl()
	if fileURL == "" {
		return xerrors.Errorf("missing CSCB file url for height %d", firstFile.GetHeight())
	}
	index, err := i.getCSCBIndex(ctx, fileURL)
	if err != nil {
		return err
	}
	firstBlock, firstChunk, err := index.LookupBlock(blockFileToMetadata(firstFile))
	if err != nil {
		return err
	}

	downloads := []cscbBlockDownload{{
		ref: downloadRef{
			index:     i.current,
			blockFile: firstFile,
		},
		block: firstBlock,
		chunk: firstChunk,
	}}
	nextIndex := i.current + 1
	for nextIndex < len(i.blockFiles) {
		nextFile := i.blockFiles[nextIndex]
		if nextFile.GetSkipped() || !isCSCBBlockFile(nextFile) || nextFile.GetFileUrl() != fileURL {
			break
		}
		nextBlock, nextChunk, err := index.LookupBlock(blockFileToMetadata(nextFile))
		if err != nil {
			return err
		}
		if nextChunk.Index != firstChunk.Index {
			break
		}
		downloads = append(downloads, cscbBlockDownload{
			ref: downloadRef{
				index:     nextIndex,
				blockFile: nextFile,
			},
			block: nextBlock,
			chunk: nextChunk,
		})
		nextIndex++
	}

	blocks := make([]*cscb.BlockDescriptor, len(downloads))
	for j, download := range downloads {
		blocks[j] = download.block
	}
	payloads, err := i.downloader.downloadCSCBChunkPayloads(ctx, fileURL, index.Header.Codec, firstChunk, blocks)
	if err != nil {
		return err
	}
	if len(payloads) != len(downloads) {
		return xerrors.Errorf("CSCB raw payload count mismatch: got %d want %d", len(payloads), len(downloads))
	}

	i.pending = make([]*RawBlockPayload, len(payloads))
	for j, payload := range payloads {
		i.pending[j] = newBytesRawBlockPayload(downloads[j].ref.blockFile, payload)
	}
	i.current = nextIndex
	return nil
}

func (i *rawBlockPayloadIteratorImpl) getCSCBIndex(ctx context.Context, fileURL string) (*cscb.Index, error) {
	if index, ok := i.indexByURL[fileURL]; ok {
		return index, nil
	}
	index, err := i.downloader.readCSCBIndex(ctx, nil, fileURL)
	if err != nil {
		return nil, err
	}
	i.indexByURL[fileURL] = index
	return index, nil
}

func (d *blockDownloaderImpl) openRawSingleBlockPayload(ctx context.Context, blockFile *api.BlockFile) (*RawBlockPayload, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, blockFile.GetFileUrl(), nil)
	if err != nil {
		return nil, xerrors.Errorf("failed to create download request: %w", err)
	}

	httpResp, err := d.httpClient.Do(req)
	if err != nil {
		return nil, retry.Retryable(xerrors.Errorf("failed to download block file: %w", err))
	}
	if statusCode := httpResp.StatusCode; statusCode != http.StatusOK {
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
		return nil, xerrors.Errorf("empty body for %s", blockFile.GetFileUrl())
	}
	decoder, err := storage_utils.DecompressReader(httpResp.Body, blockFile.GetCompression())
	if err != nil {
		_ = httpResp.Body.Close()
		return nil, xerrors.Errorf("wrap decompressor: %w", err)
	}
	reader := io.ReadCloser(&multiCloserReadCloser{
		Reader:  decoder,
		closers: []io.Closer{decoder, httpResp.Body},
	})
	length := blockFile.GetUncompressedLength()
	if length != 0 {
		reader = &exactLengthReadCloser{
			reader:    reader,
			height:    blockFile.GetHeight(),
			remaining: length,
		}
	}
	return newRawBlockPayload(blockFile, length, reader), nil
}

func (d *blockDownloaderImpl) openRawCSCBBlockPayload(ctx context.Context, blockFile *api.BlockFile) (*RawBlockPayload, error) {
	fileURL := blockFile.GetFileUrl()
	if fileURL == "" {
		return nil, xerrors.Errorf("missing CSCB file url for height %d", blockFile.GetHeight())
	}
	index, err := d.readCSCBIndex(ctx, nil, fileURL)
	if err != nil {
		return nil, err
	}
	block, chunk, err := index.LookupBlock(blockFileToMetadata(blockFile))
	if err != nil {
		return nil, err
	}
	return d.retryRaw.Retry(ctx, func(ctx context.Context) (*RawBlockPayload, error) {
		body, err := d.openHTTPRangeByLengthUnthrottled(ctx, fileURL, chunk.CompressedPayloadOffset, chunk.CompressedLength)
		if err != nil {
			return nil, err
		}
		limitedBody, err := limitReaderByLength(body, chunk.CompressedLength)
		if err != nil {
			_ = body.Close()
			return nil, err
		}
		reader, err := cscb.OpenBlockPayloadFromChunkFrame(&limitedReadCloser{
			Reader: limitedBody,
			closer: body,
		}, index.Header.Codec, chunk, block)
		if err != nil {
			return nil, err
		}
		return newRawBlockPayload(blockFile, block.PayloadLength, reader), nil
	})
}

func newBytesRawBlockPayload(blockFile *api.BlockFile, payload []byte) *RawBlockPayload {
	return newRawBlockPayload(blockFile, uint64(len(payload)), io.NopCloser(bytes.NewReader(payload)))
}

func newRawBlockPayload(blockFile *api.BlockFile, length uint64, reader io.ReadCloser) *RawBlockPayload {
	return &RawBlockPayload{
		BlockFile: blockFile,
		Length:    length,
		reader:    reader,
	}
}

func (r *multiCloserReadCloser) Close() error {
	var closeErr error
	r.closeOnce.Do(func() {
		for _, closer := range r.closers {
			if closer == nil {
				continue
			}
			if err := closer.Close(); closeErr == nil && err != nil {
				closeErr = err
			}
		}
	})
	return closeErr
}

func (r *exactLengthReadCloser) Read(buf []byte) (int, error) {
	if r.closed {
		return 0, xerrors.New("single-block raw payload reader is closed")
	}
	if r.remaining == 0 {
		return 0, r.validateEOF()
	}
	if len(buf) == 0 {
		return 0, nil
	}
	if uint64(len(buf)) > r.remaining {
		buf = buf[:int(r.remaining)]
	}

	n, err := r.reader.Read(buf)
	if n > 0 {
		r.remaining -= uint64(n)
	}
	if err != nil {
		if r.remaining > 0 {
			if err == io.EOF {
				err = io.ErrUnexpectedEOF
			}
			return n, xerrors.Errorf("single-block raw payload length mismatch at height %d: missing %d bytes: %w", r.height, r.remaining, err)
		}
		if err == io.EOF {
			r.validated = true
		}
		return n, err
	}
	return n, nil
}

func (r *exactLengthReadCloser) Close() error {
	if r.closed {
		return nil
	}
	var closeErr error
	if !r.validated {
		if _, err := io.Copy(io.Discard, r); err != nil {
			closeErr = err
		}
	}
	r.closed = true
	if err := r.reader.Close(); closeErr == nil && err != nil {
		closeErr = err
	}
	return closeErr
}

func (r *exactLengthReadCloser) validateEOF() error {
	if r.validated {
		return io.EOF
	}
	var extra [1]byte
	n, err := r.reader.Read(extra[:])
	if n > 0 {
		r.validated = true
		return xerrors.Errorf("single-block raw payload length mismatch at height %d: payload exceeds expected length", r.height)
	}
	if err == nil {
		return nil
	}
	if err == io.EOF {
		r.validated = true
		return io.EOF
	}
	return err
}

func (r *limitedReadCloser) Close() error {
	return r.closer.Close()
}
