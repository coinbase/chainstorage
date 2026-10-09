package utils

import (
	"bytes"
	"testing"

	"github.com/coinbase/chainstorage/internal/utils/testutil"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

func TestGetCompressionType(t *testing.T) {
	tests := []struct {
		fileURL     string
		compression api.Compression
	}{
		{
			fileURL:     "a",
			compression: api.Compression_NONE,
		},
		{
			fileURL:     "agzip",
			compression: api.Compression_NONE,
		},
		{
			fileURL:     "a.gzip",
			compression: api.Compression_GZIP,
		},
		{
			fileURL:     "bzstd",
			compression: api.Compression_NONE,
		},
		{
			fileURL:     "b.zstd",
			compression: api.Compression_ZSTD,
		},
	}
	for _, test := range tests {
		t.Run(test.fileURL, func(t *testing.T) {
			require := testutil.Require(t)

			require.Equal(test.compression, GetCompressionType(test.fileURL))
		})
	}
}

func TestCompress(t *testing.T) {
	tests := []struct {
		testName    string
		data        []byte
		compression api.Compression
	}{
		{
			"emptyData",
			[]byte{},
			api.Compression_GZIP,
		},
		{
			"blockDataCompression",
			[]byte(`
			{
				"hash": "0xbaa42c",
				"number": "0xacc290",
			}`),
			api.Compression_GZIP,
		},
		{
			"emptyData",
			[]byte{},
			api.Compression_ZSTD,
		},
		{
			"blockDataCompression",
			[]byte(`
			{
				"hash": "0xbaa42c",
				"number": "0xacc290",
			}`),
			api.Compression_ZSTD,
		},
		{
			"blockData",
			[]byte(`
			{
				"hash": "0xbaa42c",
				"number": "0xacc290",
			}`),
			api.Compression_NONE,
		},
	}
	for _, test := range tests {
		t.Run(test.testName, func(t *testing.T) {
			require := testutil.Require(t)

			compressed, err := Compress(test.data, test.compression)
			require.NoError(err)

			decompressed, err := Decompress(compressed, test.compression)
			require.NoError(err)
			require.True(bytes.Equal(decompressed, test.data))
		})
	}
}

func TestGetObjectKey(t *testing.T) {
	tests := []struct {
		key         string
		compression api.Compression
		expected    string
	}{
		{
			"key1",
			api.Compression_GZIP,
			"key1.gzip",
		},
		{
			"key2",
			api.Compression_NONE,
			"key2",
		},
		{
			"key3",
			api.Compression_ZSTD,
			"key3.zstd",
		},
	}
	for _, test := range tests {
		t.Run(test.key, func(t *testing.T) {
			require := testutil.Require(t)

			objectKey, err := GetObjectKey(test.key, test.compression)
			require.NoError(err)
			require.Equal(test.expected, objectKey)
		})
	}
}
