package cscb

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/klauspost/compress/zstd"

	"github.com/coinbase/chainstorage/internal/storage/blobstorage/internal"
	"github.com/coinbase/chainstorage/internal/utils/testutil"
	"github.com/coinbase/chainstorage/protos/coinbase/c3/common"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

func TestEncodeCSCB_Zstd(t *testing.T) {
	require := testutil.Require(t)

	object, err := Encode(context.Background(), testEncodeConfig(api.Compression_ZSTD), testPayloads())
	require.NoError(err)
	defer object.Close()

	data, ok := object.Bytes()
	require.True(ok)
	require.Equal(uint64(len(data)), object.Length)
	require.Equal(api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_CSCB_BATCH, object.Placements[0].ObjectFormat)
	require.Equal("BLOCKCHAIN_SOLANA/NETWORK_SOLANA_MAINNET/consolidated/v=7/shard=00000000000000000000-00000000000000010000/00000000000000000100-00000000000000000103-"+object.SHA256+".cscb.zstd", object.Key)

	header := data[:HeaderSize]
	envelope := data[HeaderSize : HeaderSize+int(binary.LittleEndian.Uint64(header[40:48]))]
	require.Equal([]byte("CSCB"), header[0:4])
	require.Equal(byte(formatVersion), header[4])
	require.Equal(byte(codecZstd), header[5])
	require.Equal(byte(compressionScopeChunked), header[6])
	require.Equal(uint32(3), binary.LittleEndian.Uint32(header[8:12]))
	require.Equal(uint32(2), binary.LittleEndian.Uint32(header[12:16]))
	require.Equal(uint64(100), binary.LittleEndian.Uint64(header[16:24]))
	require.Equal(uint64(103), binary.LittleEndian.Uint64(header[24:32]))
	require.Equal(uint64(HeaderSize), binary.LittleEndian.Uint64(header[32:40]))
	require.Equal(uint64(80+3*BlockIndexRecordSize+2*ChunkIndexRecordSize), binary.LittleEndian.Uint64(header[40:48]))
	require.Equal(crc32.ChecksumIEEE(envelope), binary.LittleEndian.Uint32(header[48:52]))
	require.Equal(uint32(BlockIndexRecordSize), binary.LittleEndian.Uint32(header[52:56]))
	require.Equal(uint32(ChunkIndexRecordSize), binary.LittleEndian.Uint32(header[56:60]))
	require.Equal(uint64(HeaderSize+len(envelope)), binary.LittleEndian.Uint64(header[60:68]))

	require.Equal([]byte("ENV1"), envelope[0:4])
	require.Equal(uint64(3), binary.LittleEndian.Uint64(envelope[8:16]))
	require.Equal(uint64(2), binary.LittleEndian.Uint64(envelope[16:24]))
	require.Equal(uint64(100), binary.LittleEndian.Uint64(envelope[24:32]))
	require.Equal(uint64(103), binary.LittleEndian.Uint64(envelope[32:40]))
	require.Equal(uint64(EnvelopeHeaderSize), binary.LittleEndian.Uint64(envelope[40:48]))

	payloads := [][]byte{[]byte("alpha"), []byte("bravo-bravo"), []byte("charlie")}
	firstBlock := envelope[EnvelopeHeaderSize : EnvelopeHeaderSize+BlockIndexRecordSize]
	require.Equal(uint64(100), binary.LittleEndian.Uint64(firstBlock[0:8]))
	require.Equal(uint64(0), binary.LittleEndian.Uint64(firstBlock[8:16]))
	require.Equal(uint64(len(payloads[0])), binary.LittleEndian.Uint64(firstBlock[16:24]))
	require.Equal(crc32.ChecksumIEEE(payloads[0]), binary.LittleEndian.Uint32(firstBlock[24:28]))
	require.Equal(uint32(0), binary.LittleEndian.Uint32(firstBlock[28:32]))
	require.Equal(uint64(0), binary.LittleEndian.Uint64(firstBlock[32:40]))
	hash := sha256.Sum256([]byte("hash-100"))
	require.Equal(hash[:], firstBlock[48:80])
	require.Equal(uint64(9001), binary.LittleEndian.Uint64(firstBlock[80:88]))

	secondBlock := envelope[EnvelopeHeaderSize+BlockIndexRecordSize : EnvelopeHeaderSize+2*BlockIndexRecordSize]
	require.Equal(uint64(len(payloads[0])), binary.LittleEndian.Uint64(secondBlock[8:16]))
	require.Equal(uint32(0), binary.LittleEndian.Uint32(secondBlock[28:32]))
	require.Equal(uint64(len(payloads[0])), binary.LittleEndian.Uint64(secondBlock[32:40]))

	chunkIndexOffset := EnvelopeHeaderSize + 3*BlockIndexRecordSize
	chunk0 := envelope[chunkIndexOffset : chunkIndexOffset+ChunkIndexRecordSize]
	chunk1 := envelope[chunkIndexOffset+ChunkIndexRecordSize : chunkIndexOffset+2*ChunkIndexRecordSize]
	require.Equal(uint32(0), binary.LittleEndian.Uint32(chunk0[0:4]))
	require.Equal(uint64(100), binary.LittleEndian.Uint64(chunk0[8:16]))
	require.Equal(uint64(102), binary.LittleEndian.Uint64(chunk0[16:24]))
	require.Equal(uint64(HeaderSize+len(envelope)), binary.LittleEndian.Uint64(chunk0[24:32]))
	require.Equal(uint64(0), binary.LittleEndian.Uint64(chunk0[40:48]))
	require.Equal(uint64(len(payloads[0])+len(payloads[1])), binary.LittleEndian.Uint64(chunk0[48:56]))
	require.Equal(uint32(2), binary.LittleEndian.Uint32(chunk0[60:64]))

	require.Equal(uint32(1), binary.LittleEndian.Uint32(chunk1[0:4]))
	require.Equal(uint64(102), binary.LittleEndian.Uint64(chunk1[8:16]))
	require.Equal(uint64(103), binary.LittleEndian.Uint64(chunk1[16:24]))
	require.Equal(uint64(len(payloads[0])+len(payloads[1])), binary.LittleEndian.Uint64(chunk1[40:48]))
	require.Equal(uint64(len(payloads[2])), binary.LittleEndian.Uint64(chunk1[48:56]))
	require.Equal(uint32(1), binary.LittleEndian.Uint32(chunk1[60:64]))

	require.Equal(uint64(0), object.Placements[0].ByteOffset)
	require.Equal(uint64(len(payloads[0])), object.Placements[0].ByteLength)
	require.Equal(uint64(len(payloads[0])+len(payloads[1])), object.Placements[2].ByteOffset)
	require.Equal(object.Key, object.Placements[2].ObjectKey)

	require.Equal(append(append(payloads[0], payloads[1]...), payloads[2]...), readAllChunks(t, data, header, envelope, api.Compression_ZSTD))
}

func TestEncodeCSCB_Gzip(t *testing.T) {
	require := testutil.Require(t)

	object, err := Encode(context.Background(), testEncodeConfig(api.Compression_GZIP), testPayloads())
	require.NoError(err)
	defer object.Close()

	data, ok := object.Bytes()
	require.True(ok)
	require.Equal(byte(codecGzip), data[5])
	require.Equal(".cscb.gzip", object.Key[len(object.Key)-len(".cscb.gzip"):])
	header := data[:HeaderSize]
	envelope := data[HeaderSize : HeaderSize+int(binary.LittleEndian.Uint64(header[40:48]))]
	require.Equal(append(append([]byte("alpha"), []byte("bravo-bravo")...), []byte("charlie")...), readAllChunks(t, data, header, envelope, api.Compression_GZIP))
}

func TestObjectOpenInMemoryReaderIsSeekable(t *testing.T) {
	require := testutil.Require(t)

	object, err := Encode(context.Background(), testEncodeConfig(api.Compression_ZSTD), testPayloads())
	require.NoError(err)
	defer object.Close()

	reader, err := object.Open()
	require.NoError(err)
	defer reader.Close()

	seeker, ok := reader.(io.Seeker)
	require.True(ok)
	offset, err := seeker.Seek(4, io.SeekStart)
	require.NoError(err)
	require.Equal(int64(4), offset)

	buf := make([]byte, 4)
	n, err := reader.Read(buf)
	require.NoError(err)
	require.Equal(4, n)
	require.Equal([]byte{byte(formatVersion), byte(codecZstd), byte(compressionScopeChunked), 0}, buf)
}

func TestEncodeCSCB_SpillsWhenMemoryBudgetExceeded(t *testing.T) {
	require := testutil.Require(t)

	memoryBudget := uint64(1)
	cfg := testEncodeConfig(api.Compression_ZSTD)
	cfg.MemoryBudgetBytes = &memoryBudget
	cfg.LocalSpillDir = filepath.Join(t.TempDir(), "missing-spill-dir")
	object, err := Encode(context.Background(), cfg, testPayloads())
	require.NoError(err)

	_, ok := object.Bytes()
	require.False(ok)
	reader, err := object.Open()
	require.NoError(err)
	data, err := io.ReadAll(reader)
	require.NoError(err)
	require.NoError(reader.Close())
	require.Len(data, int(object.Length))
	tempFile := object.tempFile
	require.NotEmpty(tempFile)
	require.True(strings.HasPrefix(tempFile, cfg.LocalSpillDir))
	require.NoError(object.Close())
	_, err = os.Stat(tempFile)
	require.True(os.IsNotExist(err))
}

func TestEncodeCSCB_AllowsOversizedSingleBlockAsDedicatedChunk(t *testing.T) {
	require := testutil.Require(t)

	maxChunkUncompressedBytes := uint64(10)
	cfg := testEncodeConfig(api.Compression_ZSTD)
	cfg.MaxChunkUncompressedBytes = &maxChunkUncompressedBytes
	object, err := Encode(context.Background(), cfg, testPayloads())
	require.NoError(err)
	defer object.Close()

	data, ok := object.Bytes()
	require.True(ok)
	index, err := ParseIndex(data)
	require.NoError(err)
	require.Len(index.Chunks, 3)
	require.Equal([]uint64{5, 11, 7}, []uint64{
		index.Chunks[0].UncompressedLength,
		index.Chunks[1].UncompressedLength,
		index.Chunks[2].UncompressedLength,
	})
	require.Equal([]uint32{1, 1, 1}, []uint32{
		index.Chunks[0].BlockCount,
		index.Chunks[1].BlockCount,
		index.Chunks[2].BlockCount,
	})
	require.Equal([]uint32{0, 1, 2}, []uint32{
		index.Blocks[0].ChunkIndex,
		index.Blocks[1].ChunkIndex,
		index.Blocks[2].ChunkIndex,
	})
	require.Greater(index.Chunks[1].UncompressedLength, maxChunkUncompressedBytes)
}

func TestEncodeCSCB_SplitsAtMaxChunkUncompressedBytes(t *testing.T) {
	require := testutil.Require(t)

	maxChunkUncompressedBytes := uint64(11)
	cfg := testEncodeConfig(api.Compression_ZSTD)
	cfg.MaxChunkUncompressedBytes = &maxChunkUncompressedBytes
	object, err := Encode(context.Background(), cfg, testPayloads())
	require.NoError(err)
	defer object.Close()

	data, ok := object.Bytes()
	require.True(ok)
	index, err := ParseIndex(data)
	require.NoError(err)
	require.Len(index.Chunks, 3)
	require.Equal([]uint64{5, 11, 7}, []uint64{
		index.Chunks[0].UncompressedLength,
		index.Chunks[1].UncompressedLength,
		index.Chunks[2].UncompressedLength,
	})
	for _, chunk := range index.Chunks {
		require.LessOrEqual(chunk.UncompressedLength, maxChunkUncompressedBytes)
	}
}

func TestEncodeCSCB_MaxChunkUncompressedBytesDoesNotChangeObjectsBelowCap(t *testing.T) {
	require := testutil.Require(t)

	legacyObject, err := Encode(context.Background(), testEncodeConfig(api.Compression_ZSTD), testPayloads())
	require.NoError(err)
	defer legacyObject.Close()
	legacyData, ok := legacyObject.Bytes()
	require.True(ok)

	maxChunkUncompressedBytes := uint64(16)
	cappedCfg := testEncodeConfig(api.Compression_ZSTD)
	cappedCfg.MaxChunkUncompressedBytes = &maxChunkUncompressedBytes
	cappedObject, err := Encode(context.Background(), cappedCfg, testPayloads())
	require.NoError(err)
	defer cappedObject.Close()
	cappedData, ok := cappedObject.Bytes()
	require.True(ok)

	require.Equal(legacyData, cappedData)
	require.Equal(legacyObject.Key, cappedObject.Key)
	require.Equal(legacyObject.Placements, cappedObject.Placements)
}

func TestEncodeCSCB_EnforcesLocalSpillMaxBytes(t *testing.T) {
	require := testutil.Require(t)

	memoryBudget := uint64(1)
	localSpillMaxBytes := uint64(1)
	cfg := testEncodeConfig(api.Compression_GZIP)
	cfg.MemoryBudgetBytes = &memoryBudget
	cfg.LocalSpillMaxBytes = &localSpillMaxBytes
	cfg.LocalSpillDir = t.TempDir()

	_, err := Encode(context.Background(), cfg, testPayloads())
	require.Error(err)
	require.Contains(err.Error(), "local_spill_max_bytes")
}

func testEncodeConfig(codec api.Compression) EncodeConfig {
	return EncodeConfig{
		Blockchain:             common.Blockchain_BLOCKCHAIN_SOLANA,
		Network:                common.Network_NETWORK_SOLANA_MAINNET,
		Codec:                  codec,
		CodecLevel:             6,
		MaxBlocks:              1000,
		CompressionChunkBlocks: 2,
		MaxCompressedBytes:     1024 * 1024,
		MaxUncompressedBytes:   1024 * 1024,
		ShardSize:              10000,
	}
}

func testPayloads() []internal.ConsolidatedBlockPayload {
	return []internal.ConsolidatedBlockPayload{
		makePayload(7, 100, "hash-100", 9001, []byte("alpha")),
		makePayload(7, 101, "hash-101", 9002, []byte("bravo-bravo")),
		makePayload(7, 102, "hash-102", 9003, []byte("charlie")),
	}
}

func makePayload(tag uint32, height uint64, hash string, metadataID int64, raw []byte) internal.ConsolidatedBlockPayload {
	return internal.ConsolidatedBlockPayload{
		Metadata: &api.BlockMetadata{
			Tag:    tag,
			Height: height,
			Hash:   hash,
		},
		MetadataID:         metadataID,
		RawBlockPayload:    internal.BytesPayloadSource(raw),
		UncompressedLength: uint64(len(raw)),
	}
}

func readAllChunks(t *testing.T, data []byte, header []byte, envelope []byte, codec api.Compression) []byte {
	t.Helper()
	require := testutil.Require(t)
	chunkCount := int(binary.LittleEndian.Uint32(header[12:16]))
	chunkIndexOffset := EnvelopeHeaderSize + int(binary.LittleEndian.Uint64(envelope[8:16]))*BlockIndexRecordSize
	var out []byte
	for i := 0; i < chunkCount; i++ {
		record := envelope[chunkIndexOffset+i*ChunkIndexRecordSize : chunkIndexOffset+(i+1)*ChunkIndexRecordSize]
		offset := binary.LittleEndian.Uint64(record[24:32])
		length := binary.LittleEndian.Uint64(record[32:40])
		frame := data[offset : offset+length]
		var reader io.ReadCloser
		var err error
		switch codec {
		case api.Compression_GZIP:
			reader, err = gzip.NewReader(bytes.NewReader(frame))
		case api.Compression_ZSTD:
			zr, zerr := zstd.NewReader(bytes.NewReader(frame))
			require.NoError(zerr)
			defer zr.Close()
			decoded, zerr := io.ReadAll(zr)
			require.NoError(zerr)
			out = append(out, decoded...)
			continue
		}
		require.NoError(err)
		decoded, err := io.ReadAll(reader)
		require.NoError(err)
		require.NoError(reader.Close())
		out = append(out, decoded...)
	}
	return out
}
