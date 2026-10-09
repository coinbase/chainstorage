package cscb

import (
	"bytes"
	"context"
	"io"
	"strings"
	"testing"

	"github.com/coinbase/chainstorage/internal/utils/testutil"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

func TestParseIndexAndExtractBlockPayload(t *testing.T) {
	require := testutil.Require(t)

	object, err := Encode(context.Background(), testEncodeConfig(api.Compression_ZSTD), testPayloads())
	require.NoError(err)
	defer object.Close()

	data, ok := object.Bytes()
	require.True(ok)
	index, err := ParseIndex(data)
	require.NoError(err)
	require.Equal(api.Compression_ZSTD, index.Header.Codec)
	require.Len(index.Blocks, 3)
	require.Len(index.Chunks, 2)

	metadata := &api.BlockMetadata{
		Tag:                7,
		Height:             101,
		Hash:               "hash-101",
		ObjectKeyMain:      object.Key,
		ObjectFormat:       api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_CSCB_BATCH,
		ByteOffset:         object.Placements[1].ByteOffset,
		ByteLength:         object.Placements[1].ByteLength,
		UncompressedLength: object.Placements[1].UncompressedLength,
	}
	block, chunk, err := index.LookupBlock(metadata)
	require.NoError(err)
	require.Equal(uint32(0), chunk.Index)

	frame := data[chunk.CompressedPayloadOffset : chunk.CompressedPayloadOffset+chunk.CompressedLength]
	chunkPayload, err := DecodeChunkFrame(bytes.NewReader(frame), index.Header.Codec)
	require.NoError(err)

	require.NoError(ValidateChunkPayload(chunkPayload, chunk))
	payload, err := ExtractBlockPayload(chunkPayload, block)
	require.NoError(err)
	require.Equal([]byte("bravo-bravo"), payload)

	streamedPayloads, err := ExtractBlockPayloadsFromChunkFrame(bytes.NewReader(frame), index.Header.Codec, chunk, []*BlockDescriptor{block})
	require.NoError(err)
	require.Len(streamedPayloads, 1)
	require.Equal([]byte("bravo-bravo"), streamedPayloads[0])

	stream, err := OpenBlockPayloadFromChunkFrame(io.NopCloser(bytes.NewReader(frame)), index.Header.Codec, chunk, block)
	require.NoError(err)
	buf := make([]byte, 5)
	n, err := stream.Read(buf)
	require.NoError(err)
	require.Equal(5, n)
	require.Equal([]byte("bravo"), buf)
	require.NoError(stream.Close())
}

func TestOpenBlockPayloadFromChunkFrameReportsCRCMismatch(t *testing.T) {
	require := testutil.Require(t)

	object, err := Encode(context.Background(), testEncodeConfig(api.Compression_ZSTD), testPayloads())
	require.NoError(err)
	defer object.Close()

	data, ok := object.Bytes()
	require.True(ok)
	index, err := ParseIndex(data)
	require.NoError(err)

	metadata := &api.BlockMetadata{
		Tag:                7,
		Height:             101,
		Hash:               "hash-101",
		ObjectKeyMain:      object.Key,
		ObjectFormat:       api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_CSCB_BATCH,
		ByteOffset:         object.Placements[1].ByteOffset,
		ByteLength:         object.Placements[1].ByteLength,
		UncompressedLength: object.Placements[1].UncompressedLength,
	}
	block, chunk, err := index.LookupBlock(metadata)
	require.NoError(err)
	corruptBlock := *block
	corruptBlock.PayloadCRC32 ^= 0x1

	frame := data[chunk.CompressedPayloadOffset : chunk.CompressedPayloadOffset+chunk.CompressedLength]
	stream, err := OpenBlockPayloadFromChunkFrame(io.NopCloser(bytes.NewReader(frame)), index.Header.Codec, chunk, &corruptBlock)
	require.NoError(err)
	_, err = io.ReadAll(stream)
	require.Error(err)
	require.Contains(err.Error(), "CRC mismatch")
}

func TestExtractBlockPayloadsFromChunkFramePreservesInputOrder(t *testing.T) {
	require := testutil.Require(t)

	object, err := Encode(context.Background(), testEncodeConfig(api.Compression_ZSTD), testPayloads())
	require.NoError(err)
	defer object.Close()

	data, ok := object.Bytes()
	require.True(ok)
	index, err := ParseIndex(data)
	require.NoError(err)
	require.Len(index.Chunks, 2)

	metadata0 := &api.BlockMetadata{
		Tag:                7,
		Height:             100,
		Hash:               "hash-100",
		ObjectKeyMain:      object.Key,
		ObjectFormat:       api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_CSCB_BATCH,
		ByteOffset:         object.Placements[0].ByteOffset,
		ByteLength:         object.Placements[0].ByteLength,
		UncompressedLength: object.Placements[0].UncompressedLength,
	}
	metadata1 := &api.BlockMetadata{
		Tag:                7,
		Height:             101,
		Hash:               "hash-101",
		ObjectKeyMain:      object.Key,
		ObjectFormat:       api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_CSCB_BATCH,
		ByteOffset:         object.Placements[1].ByteOffset,
		ByteLength:         object.Placements[1].ByteLength,
		UncompressedLength: object.Placements[1].UncompressedLength,
	}
	block0, chunk0, err := index.LookupBlock(metadata0)
	require.NoError(err)
	block1, chunk1, err := index.LookupBlock(metadata1)
	require.NoError(err)
	require.Equal(chunk0.Index, chunk1.Index)

	frame := data[chunk0.CompressedPayloadOffset : chunk0.CompressedPayloadOffset+chunk0.CompressedLength]
	payloads, err := ExtractBlockPayloadsFromChunkFrame(bytes.NewReader(frame), index.Header.Codec, chunk0, []*BlockDescriptor{block1, block0})
	require.NoError(err)
	require.Len(payloads, 2)
	require.Equal([]byte("bravo-bravo"), payloads[0])
	require.Equal([]byte("alpha"), payloads[1])

	duplicatePayloads, err := ExtractBlockPayloadsFromChunkFrame(bytes.NewReader(frame), index.Header.Codec, chunk0, []*BlockDescriptor{block1, block0, block1})
	require.NoError(err)
	require.Len(duplicatePayloads, 3)
	require.Equal([]byte("bravo-bravo"), duplicatePayloads[0])
	require.Equal([]byte("alpha"), duplicatePayloads[1])
	require.Equal([]byte("bravo-bravo"), duplicatePayloads[2])
	duplicatePayloads[0][0] = 'B'
	require.Equal([]byte("bravo-bravo"), duplicatePayloads[2])
}

func TestLookupBlockRejectsMismatchedMetadata(t *testing.T) {
	require := testutil.Require(t)

	object, err := Encode(context.Background(), testEncodeConfig(api.Compression_ZSTD), testPayloads())
	require.NoError(err)
	defer object.Close()

	data, ok := object.Bytes()
	require.True(ok)
	index, err := ParseIndex(data)
	require.NoError(err)

	_, _, err = index.LookupBlock(&api.BlockMetadata{
		Tag:                7,
		Height:             101,
		Hash:               "wrong-hash",
		ObjectKeyMain:      object.Key,
		ObjectFormat:       api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_CSCB_BATCH,
		ByteOffset:         object.Placements[1].ByteOffset,
		ByteLength:         object.Placements[1].ByteLength,
		UncompressedLength: object.Placements[1].UncompressedLength,
	})
	require.Error(err)
	require.Contains(err.Error(), "hash mismatch")
}

func TestDecodeChunkFrameSupportsMaxAllowedZstdWindow(t *testing.T) {
	require := testutil.Require(t)

	windowLog := MaxZstdLongDistanceWindowLog
	cfg := testEncodeConfig(api.Compression_ZSTD)
	cfg.ZstdLongDistanceWindowLog = &windowLog
	payload := []byte(strings.Repeat("solana-block-payload-", 1024))
	blocks := testPayloads()[:1]
	blocks[0] = makePayload(7, 100, "hash-100", 9001, payload)
	object, err := Encode(context.Background(), cfg, blocks)
	require.NoError(err)
	defer object.Close()

	data, ok := object.Bytes()
	require.True(ok)
	index, err := ParseIndex(data)
	require.NoError(err)
	require.Len(index.Chunks, 1)
	chunk := &index.Chunks[0]
	frame := data[chunk.CompressedPayloadOffset : chunk.CompressedPayloadOffset+chunk.CompressedLength]

	chunkPayload, err := DecodeChunkFrame(bytes.NewReader(frame), index.Header.Codec)
	require.NoError(err)
	require.NoError(ValidateChunkPayload(chunkPayload, chunk))
	require.Equal(payload, chunkPayload)
}

func TestReaderSupportsExistingChunkAboveNewEncoderCap(t *testing.T) {
	require := testutil.Require(t)

	// A nil cap reproduces an object produced before max_chunk_uncompressed_bytes
	// was configured. Its first chunk is deliberately larger than the simulated
	// new encoder cap; readers must continue to use the persisted CSCB index.
	legacyObject, err := Encode(context.Background(), testEncodeConfig(api.Compression_ZSTD), testPayloads())
	require.NoError(err)
	defer legacyObject.Close()
	data, ok := legacyObject.Bytes()
	require.True(ok)

	index, err := ParseIndex(data)
	require.NoError(err)
	require.Len(index.Chunks, 2)
	newEncoderCap := uint64(15)
	chunk := &index.Chunks[0]
	require.Greater(chunk.UncompressedLength, newEncoderCap)

	block := &index.Blocks[1]
	frame := data[chunk.CompressedPayloadOffset : chunk.CompressedPayloadOffset+chunk.CompressedLength]
	stream, err := OpenBlockPayloadFromChunkFrame(
		io.NopCloser(bytes.NewReader(frame)),
		index.Header.Codec,
		chunk,
		block,
	)
	require.NoError(err)
	payload, err := io.ReadAll(stream)
	require.NoError(err)
	require.NoError(stream.Close())
	require.Equal([]byte("bravo-bravo"), payload)
}
