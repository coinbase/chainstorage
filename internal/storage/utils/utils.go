package utils

import (
	"golang.org/x/xerrors"

	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

func Compress(data []byte, compression api.Compression) ([]byte, error) {
	if compression == api.Compression_NONE {
		return data, nil
	}

	compressor, err := CompressorFactory(compression)
	if err != nil {
		return nil, err
	}
	coded, err := compressor.Compress(data)
	if err != nil {
		return nil, err
	}
	return coded, nil
}

func Decompress(data []byte, compression api.Compression) ([]byte, error) {
	if compression == api.Compression_NONE {
		return data, nil
	}
	compressor, err := CompressorFactory(compression)
	if err != nil {
		return nil, err
	}
	decoded, err := compressor.Decompress(data)
	if err != nil {
		return nil, err
	}
	return decoded, nil
}

func GetObjectKey(key string, compression api.Compression) (string, error) {
	if compression == api.Compression_NONE {
		return key, nil
	}
	compressor, err := CompressorFactory(compression)
	if err != nil {
		return "", xerrors.Errorf("failed to Get Object Key with: %w", err)
	}
	return compressor.GetObjectKey(key), nil
}
