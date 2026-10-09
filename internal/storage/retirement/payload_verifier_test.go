package retirement

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"hash/crc32"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/coinbase/chainstorage/internal/storage/blobstorage/cscb"
	storageutils "github.com/coinbase/chainstorage/internal/storage/utils"
	"github.com/coinbase/chainstorage/protos/coinbase/c3/common"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

func TestPayloadVerifier_ParsesAndMatchesExactPinnedVersions(t *testing.T) {
	require := require.New(t)
	candidate, store, payload := retirementPayloadFixture(t)
	verifier := newPayloadVerifier(store)

	digest, err := verifier.Verify(context.Background(), candidate)
	require.NoError(err)
	var block api.Block
	require.NoError(proto.Unmarshal(payload, &block))
	expected, err := canonicalBlockDigest(storageutils.CloneBlockWithoutStoragePlacement(&block))
	require.NoError(err)
	require.Equal(expected, digest)

	delete(store.objects, versionObjectKey(candidate.Key, candidate.VersionID))
	digest, err = verifier.VerifyConsolidated(context.Background(), candidate)
	require.NoError(err)
	require.Equal(expected, digest)
}

func TestPayloadVerifier_RejectsCSCBPayloadWithStoragePlacementMetadata(t *testing.T) {
	require := require.New(t)
	candidate, store, payload := retirementPayloadFixture(t)

	var consolidated api.Block
	require.NoError(proto.Unmarshal(payload, &consolidated))
	consolidated.Metadata.ObjectKeyMain = candidate.Key
	consolidated.Metadata.ObjectFormat = api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_CSCB_BATCH
	consolidated.Metadata.ByteOffset = 123
	consolidated.Metadata.ByteLength = 456
	consolidated.Metadata.UncompressedLength = 789
	consolidatedPayload, err := proto.Marshal(&consolidated)
	require.NoError(err)
	candidate.ByteLength = uint64(len(consolidatedPayload))
	candidate.UncompressedLength = uint64(len(consolidatedPayload))
	store.objects[versionObjectKey(candidate.ConsolidatedKey, candidate.CSCBVersionID)] = buildSingleBlockCSCB(t, candidate, consolidatedPayload)

	_, err = newPayloadVerifier(store).Verify(context.Background(), candidate)
	require.Error(err)
	require.Contains(err.Error(), "retains storage placement metadata")

	_, err = newPayloadVerifier(store).VerifyConsolidated(context.Background(), candidate)
	require.Error(err)
	require.Contains(err.Error(), "retains storage placement metadata")
}

func TestPayloadVerifier_FreshInstanceDoesNotReuseCachedCSCBBytes(t *testing.T) {
	require := require.New(t)
	candidate, store, _ := retirementPayloadFixture(t)
	verifier := newPayloadVerifier(store)

	_, err := verifier.VerifyConsolidated(context.Background(), candidate)
	require.NoError(err)
	delete(store.objects, versionObjectKey(candidate.ConsolidatedKey, candidate.CSCBVersionID))

	_, err = verifier.VerifyConsolidated(context.Background(), candidate)
	require.NoError(err, "the same verifier intentionally caches its parsed index and chunk")
	_, err = newPayloadVerifier(store).VerifyConsolidated(context.Background(), candidate)
	require.Error(err)
}

func TestPayloadVerifier_ParsesSupportedSingleBlockCompression(t *testing.T) {
	tests := []struct {
		name        string
		suffix      string
		compression api.Compression
	}{
		{name: "none", compression: api.Compression_NONE},
		{name: "gzip", suffix: storageutils.GzipFileSuffix, compression: api.Compression_GZIP},
		{name: "zstd", suffix: storageutils.ZstdFileSuffix, compression: api.Compression_ZSTD},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require := require.New(t)
			candidate, store, payload := retirementPayloadFixture(t)
			delete(store.objects, versionObjectKey(candidate.Key, candidate.VersionID))
			candidate.Key = "single-block/429600000" + test.suffix
			compressed, err := storageutils.Compress(payload, test.compression)
			require.NoError(err)
			store.objects[versionObjectKey(candidate.Key, candidate.VersionID)] = compressed

			_, err = newPayloadVerifier(store).Verify(context.Background(), candidate)
			require.NoError(err)
		})
	}
}

func TestPayloadVerifier_RejectsSerializedMismatch(t *testing.T) {
	require := require.New(t)
	candidate, store, _ := retirementPayloadFixture(t)
	different := &api.Block{
		Blockchain: common.Blockchain_BLOCKCHAIN_ETHEREUM,
		Network:    common.Network_NETWORK_SOLANA_MAINNET,
		Metadata: &api.BlockMetadata{
			Tag:    candidate.Tag,
			Height: candidate.Height,
			Hash:   candidate.Hash,
		},
	}
	differentPayload, err := proto.Marshal(different)
	require.NoError(err)
	store.objects[versionObjectKey(candidate.Key, candidate.VersionID)] = gzipPayload(t, differentPayload)

	_, err = newPayloadVerifier(store).Verify(context.Background(), candidate)
	require.Error(err)
	require.Contains(err.Error(), "payloads differ")
}

func TestPayloadVerifier_RejectsMatchingPayloadWithWrongBlockIdentity(t *testing.T) {
	require := require.New(t)
	candidate, store, _ := retirementPayloadFixture(t)
	wrong := &api.Block{
		Blockchain: common.Blockchain_BLOCKCHAIN_SOLANA,
		Network:    common.Network_NETWORK_SOLANA_MAINNET,
		Metadata: &api.BlockMetadata{
			Tag:    candidate.Tag,
			Height: candidate.Height + 1,
			Hash:   candidate.Hash,
		},
	}
	wrongPayload, err := proto.Marshal(wrong)
	require.NoError(err)
	store.objects[versionObjectKey(candidate.Key, candidate.VersionID)] = gzipPayload(t, wrongPayload)
	store.objects[versionObjectKey(candidate.ConsolidatedKey, candidate.CSCBVersionID)] = buildSingleBlockCSCB(t, candidate, wrongPayload)

	_, err = newPayloadVerifier(store).Verify(context.Background(), candidate)
	require.Error(err)
	require.Contains(err.Error(), "identity mismatch")
}

func TestPayloadVerifier_RejectsMalformedSingleBlockAndCSCBPayloads(t *testing.T) {
	t.Run("single-block protobuf", func(t *testing.T) {
		require := require.New(t)
		candidate, store, _ := retirementPayloadFixture(t)
		store.objects[versionObjectKey(candidate.Key, candidate.VersionID)] = gzipPayload(t, []byte("not-a-protobuf"))
		_, err := newPayloadVerifier(store).Verify(context.Background(), candidate)
		require.Error(err)
		require.Contains(err.Error(), "parse pinned single-block")
	})

	t.Run("CSCB chunk checksum", func(t *testing.T) {
		require := require.New(t)
		candidate, store, _ := retirementPayloadFixture(t)
		key := versionObjectKey(candidate.ConsolidatedKey, candidate.CSCBVersionID)
		object := append([]byte(nil), store.objects[key]...)
		object[len(object)-1] ^= 0xff
		store.objects[key] = object
		_, err := newPayloadVerifier(store).Verify(context.Background(), candidate)
		require.Error(err)
	})
}

func retirementPayloadFixture(t *testing.T) (Candidate, *fakeStore, []byte) {
	t.Helper()
	require := require.New(t)
	block := &api.Block{
		Blockchain: common.Blockchain_BLOCKCHAIN_SOLANA,
		Network:    common.Network_NETWORK_SOLANA_MAINNET,
		Metadata: &api.BlockMetadata{
			Tag:    2,
			Height: 429600000,
			Hash:   "solana-hash-429600000",
		},
	}
	payload, err := proto.Marshal(block)
	require.NoError(err)
	candidate := Candidate{
		Bucket:             "bucket",
		Key:                "single-block/429600000.gzip",
		VersionID:          "single-block-v1",
		Height:             block.Metadata.Height,
		Hash:               block.Metadata.Hash,
		BlockMetadataID:    9001,
		Tag:                block.Metadata.Tag,
		ConsolidatedKey:    "consolidated/canary.cscb.gzip",
		CSCBVersionID:      "cscb-v1",
		ByteOffset:         0,
		ByteLength:         uint64(len(payload)),
		UncompressedLength: uint64(len(payload)),
	}
	store := newFakeStore()
	store.objects[versionObjectKey(candidate.Key, candidate.VersionID)] = gzipPayload(t, payload)
	store.objects[versionObjectKey(candidate.ConsolidatedKey, candidate.CSCBVersionID)] = buildSingleBlockCSCB(t, candidate, payload)
	return candidate, store, payload
}

func buildSingleBlockCSCB(t *testing.T, candidate Candidate, payload []byte) []byte {
	t.Helper()
	compressed := gzipPayload(t, payload)
	envelope := make([]byte, cscb.EnvelopeHeaderSize+cscb.BlockIndexRecordSize+cscb.ChunkIndexRecordSize)
	copy(envelope[0:4], []byte("ENV1"))
	binary.LittleEndian.PutUint64(envelope[8:16], 1)
	binary.LittleEndian.PutUint64(envelope[16:24], 1)
	binary.LittleEndian.PutUint64(envelope[24:32], candidate.Height)
	binary.LittleEndian.PutUint64(envelope[32:40], candidate.Height+1)
	binary.LittleEndian.PutUint64(envelope[40:48], cscb.EnvelopeHeaderSize)
	binary.LittleEndian.PutUint64(envelope[48:56], cscb.BlockIndexRecordSize)
	chunkOffset := cscb.EnvelopeHeaderSize + cscb.BlockIndexRecordSize
	binary.LittleEndian.PutUint64(envelope[56:64], uint64(chunkOffset))
	binary.LittleEndian.PutUint64(envelope[64:72], cscb.ChunkIndexRecordSize)

	blockRecord := envelope[cscb.EnvelopeHeaderSize:chunkOffset]
	binary.LittleEndian.PutUint64(blockRecord[0:8], candidate.Height)
	binary.LittleEndian.PutUint64(blockRecord[8:16], candidate.ByteOffset)
	binary.LittleEndian.PutUint64(blockRecord[16:24], uint64(len(payload)))
	binary.LittleEndian.PutUint32(blockRecord[24:28], crc32.ChecksumIEEE(payload))
	binary.LittleEndian.PutUint32(blockRecord[28:32], 0)
	binary.LittleEndian.PutUint64(blockRecord[32:40], 0)
	hash := sha256.Sum256([]byte(candidate.Hash))
	copy(blockRecord[48:80], hash[:])
	binary.LittleEndian.PutUint64(blockRecord[80:88], uint64(candidate.BlockMetadataID))

	payloadOffset := uint64(cscb.HeaderSize + len(envelope))
	chunkRecord := envelope[chunkOffset:]
	binary.LittleEndian.PutUint32(chunkRecord[0:4], 0)
	binary.LittleEndian.PutUint64(chunkRecord[8:16], candidate.Height)
	binary.LittleEndian.PutUint64(chunkRecord[16:24], candidate.Height+1)
	binary.LittleEndian.PutUint64(chunkRecord[24:32], payloadOffset)
	binary.LittleEndian.PutUint64(chunkRecord[32:40], uint64(len(compressed)))
	binary.LittleEndian.PutUint64(chunkRecord[40:48], 0)
	binary.LittleEndian.PutUint64(chunkRecord[48:56], uint64(len(payload)))
	binary.LittleEndian.PutUint32(chunkRecord[56:60], crc32.ChecksumIEEE(payload))
	binary.LittleEndian.PutUint32(chunkRecord[60:64], 1)

	header := make([]byte, cscb.HeaderSize)
	copy(header[0:4], []byte("CSCB"))
	header[4] = 1
	header[5] = 1
	header[6] = 2
	binary.LittleEndian.PutUint32(header[8:12], 1)
	binary.LittleEndian.PutUint32(header[12:16], 1)
	binary.LittleEndian.PutUint64(header[16:24], candidate.Height)
	binary.LittleEndian.PutUint64(header[24:32], candidate.Height+1)
	binary.LittleEndian.PutUint64(header[32:40], cscb.HeaderSize)
	binary.LittleEndian.PutUint64(header[40:48], uint64(len(envelope)))
	binary.LittleEndian.PutUint32(header[48:52], crc32.ChecksumIEEE(envelope))
	binary.LittleEndian.PutUint32(header[52:56], cscb.BlockIndexRecordSize)
	binary.LittleEndian.PutUint32(header[56:60], cscb.ChunkIndexRecordSize)
	binary.LittleEndian.PutUint64(header[60:68], payloadOffset)
	binary.LittleEndian.PutUint64(header[68:76], uint64(len(compressed)))
	binary.LittleEndian.PutUint64(header[76:84], uint64(len(payload)))
	binary.LittleEndian.PutUint32(header[84:88], crc32.ChecksumIEEE(payload))

	object := make([]byte, 0, len(header)+len(envelope)+len(compressed))
	object = append(object, header...)
	object = append(object, envelope...)
	object = append(object, compressed...)
	return object
}

func gzipPayload(t *testing.T, payload []byte) []byte {
	t.Helper()
	var buffer bytes.Buffer
	writer := gzip.NewWriter(&buffer)
	_, err := writer.Write(payload)
	require.NoError(t, err)
	require.NoError(t, writer.Close())
	return buffer.Bytes()
}
