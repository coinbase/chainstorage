package utils

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io/ioutil"
	"strings"

	"golang.org/x/xerrors"

	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

const (
	GzipFileSuffix = ".gzip"
)

func GetCompressionType(fileURL string) api.Compression {
	if strings.HasSuffix(fileURL, GzipFileSuffix) {
		return api.Compression_GZIP
	}
	return api.Compression_NONE
}

func Compress(data []byte, compression api.Compression) ([]byte, error) {
	if compression == api.Compression_NONE {
		return data, nil
	}

	if compression == api.Compression_GZIP {
		var buf bytes.Buffer
		zw := gzip.NewWriter(&buf)
		if _, err := zw.Write(data); err != nil {
			return nil, xerrors.Errorf("failed to write compressed data: %w", err)
		}
		if err := zw.Close(); err != nil {
			return nil, xerrors.Errorf("failed to close writer: %w", err)
		}

		return buf.Bytes(), nil
	}

	return nil, xerrors.Errorf("failed to compress with unsupported type %v", compression.String())
}

func Decompress(data []byte, compression api.Compression) ([]byte, error) {
	if compression == api.Compression_NONE {
		return data, nil
	}

	if compression == api.Compression_GZIP {
		zr, err := gzip.NewReader(bytes.NewBuffer(data))
		if err != nil {
			return nil, xerrors.Errorf("failed to initiate reader: %w", err)
		}
		decoded, err := ioutil.ReadAll(zr)
		if err != nil {
			return nil, xerrors.Errorf("failed to read data: %w", err)
		}
		if err := zr.Close(); err != nil {
			return nil, xerrors.Errorf("failed to close reader: %w", err)
		}
		return decoded, nil
	}

	return nil, xerrors.Errorf("failed to decompress with unsupported type %v", compression.String())
}

func GetObjectKey(key string, compression api.Compression) (string, error) {
	if compression == api.Compression_NONE {
		return key, nil
	}

	if compression == api.Compression_GZIP {
		key = fmt.Sprintf("%s%s", key, GzipFileSuffix)
		return key, nil
	}

	return "", xerrors.Errorf("failed to get object key with unsupported type %v", compression.String())
}
