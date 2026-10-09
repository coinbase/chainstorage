package cscb

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/coinbase/chainstorage/internal/utils/testutil"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

func chunkFrame(t *testing.T, data []byte, chunk *ChunkDescriptor) io.ReadCloser {
	t.Helper()
	start := chunk.CompressedPayloadOffset
	end := start + chunk.CompressedLength
	return io.NopCloser(bytes.NewReader(data[start:end]))
}

func TestChunkBlockReader_SequentialParityWithExtract(t *testing.T) {
	require := testutil.Require(t)
	object, err := Encode(context.Background(), testEncodeConfig(api.Compression_ZSTD), testPayloads())
	require.NoError(err)
	defer object.Close()
	data, ok := object.Bytes()
	require.True(ok)
	index, err := ParseIndex(data)
	require.NoError(err)

	// Chunk 0 holds heights 100 and 101 (CompressionChunkBlocks: 2).
	chunk := &index.Chunks[0]
	blocks := []*BlockDescriptor{&index.Blocks[0], &index.Blocks[1]}
	expected, err := ExtractBlockPayloadsFromChunkFrame(chunkFrame(t, data, chunk), api.Compression_ZSTD, chunk, blocks)
	require.NoError(err)

	reader, err := NewChunkBlockReader(chunkFrame(t, data, chunk), api.Compression_ZSTD, chunk, blocks)
	require.NoError(err)
	defer reader.Close()
	for i, block := range blocks {
		desc, payload, err := reader.Next()
		require.NoError(err)
		require.Equal(block.Height, desc.Height)
		require.Equal(expected[i], payload)
	}
	_, _, err = reader.Next()
	require.ErrorIs(err, io.EOF)
	require.NoError(reader.Close())
	_, _, err = reader.Next()
	require.Error(err, "closed reader rejects Next")
}

func TestChunkBlockReader_SkipsUnrequestedBlocks(t *testing.T) {
	require := testutil.Require(t)
	object, err := Encode(context.Background(), testEncodeConfig(api.Compression_ZSTD), testPayloads())
	require.NoError(err)
	defer object.Close()
	data, ok := object.Bytes()
	require.True(ok)
	index, err := ParseIndex(data)
	require.NoError(err)

	chunk := &index.Chunks[0]
	reader, err := NewChunkBlockReader(chunkFrame(t, data, chunk), api.Compression_ZSTD, chunk, []*BlockDescriptor{&index.Blocks[1]})
	require.NoError(err)
	defer reader.Close()
	desc, payload, err := reader.Next()
	require.NoError(err)
	require.Equal(uint64(101), desc.Height)
	require.Equal([]byte("bravo-bravo"), payload)
}

func TestChunkBlockReader_RejectsOutOfOrderAndForeignBlocks(t *testing.T) {
	require := testutil.Require(t)
	object, err := Encode(context.Background(), testEncodeConfig(api.Compression_ZSTD), testPayloads())
	require.NoError(err)
	defer object.Close()
	data, ok := object.Bytes()
	require.True(ok)
	index, err := ParseIndex(data)
	require.NoError(err)

	chunk := &index.Chunks[0]
	_, err = NewChunkBlockReader(chunkFrame(t, data, chunk), api.Compression_ZSTD, chunk, []*BlockDescriptor{&index.Blocks[1], &index.Blocks[0]})
	require.ErrorContains(err, "out of order")

	_, err = NewChunkBlockReader(chunkFrame(t, data, chunk), api.Compression_ZSTD, chunk, []*BlockDescriptor{&index.Blocks[2]})
	require.ErrorContains(err, "chunk mismatch", "height 102 lives in chunk 1")

	_, err = NewChunkBlockReader(chunkFrame(t, data, chunk), api.Compression_ZSTD, chunk, nil)
	require.Error(err)
}

func TestChunkBlockReader_ReportsCRCMismatchAndTruncation(t *testing.T) {
	require := testutil.Require(t)
	object, err := Encode(context.Background(), testEncodeConfig(api.Compression_ZSTD), testPayloads())
	require.NoError(err)
	defer object.Close()
	data, ok := object.Bytes()
	require.True(ok)
	index, err := ParseIndex(data)
	require.NoError(err)

	chunk := &index.Chunks[0]
	corrupted := index.Blocks[0]
	corrupted.PayloadCRC32 ^= 0xffffffff
	reader, err := NewChunkBlockReader(chunkFrame(t, data, chunk), api.Compression_ZSTD, chunk, []*BlockDescriptor{&corrupted})
	require.NoError(err)
	_, _, err = reader.Next()
	require.ErrorContains(err, "CRC mismatch")
	require.NoError(reader.Close())

	// A frame cut short surfaces as an unexpected EOF on the block that
	// needed the missing bytes, which is what the downloader retries on.
	start := chunk.CompressedPayloadOffset
	truncated := io.NopCloser(bytes.NewReader(data[start : start+chunk.CompressedLength/2]))
	reader, err = NewChunkBlockReader(truncated, api.Compression_ZSTD, chunk, []*BlockDescriptor{&index.Blocks[0], &index.Blocks[1]})
	require.NoError(err)
	defer reader.Close()
	var sawErr error
	for {
		_, _, err := reader.Next()
		if err != nil {
			sawErr = err
			break
		}
	}
	require.ErrorIs(sawErr, io.ErrUnexpectedEOF)
}
