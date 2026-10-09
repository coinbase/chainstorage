package utils

import (
	"bytes"
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"path/filepath"

	"github.com/klauspost/compress/zstd"
	"golang.org/x/xerrors"

	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

const (
	GzipFileSuffix = ".gzip"
	ZstdFileSuffix = ".zstd"
)

type Compressor interface {
	Compress(data []byte) ([]byte, error)
	Decompress(data []byte) ([]byte, error)
	GetObjectKey(key string) string
}

func GetCompressionType(fileURL string) api.Compression {
	ext := filepath.Ext(fileURL)
	switch ext {
	case GzipFileSuffix:
		return api.Compression_GZIP
	case ZstdFileSuffix:
		return api.Compression_ZSTD
	}
	return api.Compression_NONE
}

func CompressorFactory(compressionType api.Compression) (Compressor, error) {
	switch compressionType {
	case api.Compression_GZIP:
		return &GzipCompressor{}, nil
	case api.Compression_ZSTD:
		return &ZstdCompressor{}, nil
	default:
		return nil, errors.New("unsupported compression type")
	}
}

// ------ GZIP ------
type GzipCompressor struct{}

func (g *GzipCompressor) Compress(data []byte) ([]byte, error) {
	var buf bytes.Buffer
	writer := gzip.NewWriter(&buf)

	if _, err := writer.Write(data); err != nil {
		return nil, xerrors.Errorf("failed to write compressed data with gzip: %w", err)
	}
	if err := writer.Close(); err != nil {
		return nil, xerrors.Errorf("failed to close gzip writer: %w", err)
	}

	return buf.Bytes(), nil
}

func (g *GzipCompressor) Decompress(data []byte) ([]byte, error) {
	reader, err := gzip.NewReader(bytes.NewBuffer(data))
	if err != nil {
		return nil, xerrors.Errorf("failed to initiate gzip reader: %w", err)
	}
	decoded, err := ioutil.ReadAll(reader)
	if err != nil {
		return nil, xerrors.Errorf("failed to read data: %w", err)
	}
	if err := reader.Close(); err != nil {
		return nil, xerrors.Errorf("failed to close gzip reader: %w", err)
	}
	return decoded, nil
}

func (g *GzipCompressor) GetObjectKey(key string) string {
	return fmt.Sprintf("%s%s", key, GzipFileSuffix)
}

// ------ ZSTD ------
type ZstdCompressor struct{}

func (z *ZstdCompressor) Compress(data []byte) ([]byte, error) {
	writer, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault))
	if err != nil {
		return nil, xerrors.Errorf("failed to write compressed data with zstd: %w", err)
	}
	if err := writer.Close(); err != nil {
		return nil, xerrors.Errorf("failed to close zstd writer: %w", err)
	}
	return writer.EncodeAll(data, nil), nil
}

func (z *ZstdCompressor) Decompress(data []byte) ([]byte, error) {
	decoder, err := zstd.NewReader(nil)
	if err != nil {
		return nil, xerrors.Errorf("failed to initiate zstd reader: %w", err)
	}
	defer decoder.Close()
	decoded, err := decoder.DecodeAll(data, nil)
	if err != nil {
		return nil, xerrors.Errorf("failed to read data with zstd: %w", err)
	}
	return decoded, nil
}

func (z *ZstdCompressor) GetObjectKey(key string) string {
	return fmt.Sprintf("%s%s", key, ZstdFileSuffix)
}

// DecompressReader wraps r in a streaming decompressor for the given
// compression type. The returned reader must be closed.
//
// For api.Compression_NONE, returns an io.NopCloser around r so callers
// can uniformly defer Close().
func DecompressReader(r io.Reader, compression api.Compression) (io.ReadCloser, error) {
	switch compression {
	case api.Compression_NONE:
		return io.NopCloser(r), nil
	case api.Compression_GZIP:
		gr, err := gzip.NewReader(r)
		if err != nil {
			return nil, xerrors.Errorf("gzip reader: %w", err)
		}
		return gr, nil
	case api.Compression_ZSTD:
		// WithDecoderConcurrency(1): sequential decoding only.
		// WithDecoderLowmem(true): enable the low-memory decoder path;
		// the concurrent decoder otherwise pre-decodes blocks ahead of
		// the reader and retains them in an internal channel, which on
		// a multi-GB zcash block balloons to several GB of transient
		// heap even though the caller never asks for that much at
		// once.
		zr, err := zstd.NewReader(r,
			zstd.WithDecoderConcurrency(1),
			zstd.WithDecoderLowmem(true),
			zstd.WithDecoderMaxMemory(256*1024*1024),
		)
		if err != nil {
			return nil, xerrors.Errorf("zstd reader: %w", err)
		}
		return &zstdReadCloser{zr}, nil
	default:
		return nil, xerrors.Errorf("unsupported compression type: %v", compression)
	}
}

// zstdReadCloser adapts *zstd.Decoder (whose Close returns nothing) to
// io.ReadCloser.
type zstdReadCloser struct {
	*zstd.Decoder
}

func (z *zstdReadCloser) Close() error {
	z.Decoder.Close()
	return nil
}
