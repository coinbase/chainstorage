// Package downloadertest builds consolidated (CSCB) block objects and
// serves them over HTTP range requests for tests outside the blobstorage
// tree, which cannot import blobstorage/internal to call cscb.Encode
// themselves (the SDK range-stream tests use it).
package downloadertest

import (
	"context"
	"fmt"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/coinbase/chainstorage/internal/storage/blobstorage/cscb"
	blobstorageinternal "github.com/coinbase/chainstorage/internal/storage/blobstorage/internal"
	"github.com/coinbase/chainstorage/internal/utils/testutil"
	"github.com/coinbase/chainstorage/protos/coinbase/c3/common"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

// CSCBObject is one encoded consolidated object plus the block files
// that address each block inside it. FileUrl is filled in by Serve.
type CSCBObject struct {
	Data       []byte
	Index      *cscb.Index
	BlockFiles []*api.BlockFile
	object     *cscb.Object
}

// EncodeCSCB consolidates blocks (in the given order, heights ascending)
// into one zstd CSCB object with chunkBlocks blocks per chunk.
func EncodeCSCB(tb testing.TB, blocks []*api.Block, chunkBlocks uint64) *CSCBObject {
	tb.Helper()
	require := testutil.Require(tb)

	payloads := make([]blobstorageinternal.ConsolidatedBlockPayload, len(blocks))
	for i, block := range blocks {
		blockBytes, err := proto.Marshal(block)
		require.NoError(err)
		payloads[i] = blobstorageinternal.ConsolidatedBlockPayload{
			Metadata:           block.GetMetadata(),
			MetadataID:         int64(i + 1),
			RawBlockPayload:    blobstorageinternal.BytesPayloadSource(blockBytes),
			UncompressedLength: uint64(len(blockBytes)),
		}
	}
	first := blocks[0]
	object, err := cscb.Encode(context.Background(), cscb.EncodeConfig{
		Blockchain:             first.GetBlockchain(),
		Network:                first.GetNetwork(),
		Codec:                  api.Compression_ZSTD,
		CodecLevel:             1,
		MaxBlocks:              uint64(len(blocks)),
		CompressionChunkBlocks: chunkBlocks,
		ShardSize:              10_000,
	}, payloads)
	require.NoError(err)
	tb.Cleanup(func() { _ = object.Close() })
	data, ok := object.Bytes()
	require.True(ok, "test objects are expected to stay in memory")
	index, err := cscb.ParseIndex(data)
	require.NoError(err)

	blockFiles := make([]*api.BlockFile, len(blocks))
	for i, placement := range object.Placements {
		source := blocks[i].GetMetadata()
		blockFiles[i] = &api.BlockFile{
			Tag:                source.GetTag(),
			Hash:               source.GetHash(),
			ParentHash:         source.GetParentHash(),
			Height:             source.GetHeight(),
			ParentHeight:       source.GetParentHeight(),
			Compression:        api.Compression_ZSTD,
			ObjectFormat:       placement.ObjectFormat,
			ByteOffset:         placement.ByteOffset,
			ByteLength:         placement.ByteLength,
			UncompressedLength: placement.UncompressedLength,
		}
	}
	return &CSCBObject{Data: data, Index: index, BlockFiles: blockFiles, object: object}
}

// ChunkRange returns the Range header value the downloader sends for
// chunk i, for asserting how many times a chunk was fetched.
func (o *CSCBObject) ChunkRange(i int) string {
	chunk := o.Index.Chunks[i]
	return fmt.Sprintf("bytes=%d-%d", chunk.CompressedPayloadOffset, chunk.CompressedPayloadOffset+chunk.CompressedLength-1)
}

// RangeServer serves one object over HTTP range requests and records
// every Range header it sees.
type RangeServer struct {
	*httptest.Server
	mu     sync.Mutex
	ranges []string
	// truncate, when set, cuts the body of requests for truncateRange to
	// truncateKeep bytes, either once or on every request.
	truncateRange string
	truncateKeep  int
	truncateLeft  int // -1 = always
}

// TruncateOnce makes the next request for exactly rangeValue return only
// the first keep bytes of its body, exercising the mid-chunk resume path.
func (s *RangeServer) TruncateOnce(rangeValue string, keep int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.truncateRange, s.truncateKeep, s.truncateLeft = rangeValue, keep, 1
}

// TruncateAlways truncates every request for rangeValue, so retries
// cannot recover.
func (s *RangeServer) TruncateAlways(rangeValue string, keep int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.truncateRange, s.truncateKeep, s.truncateLeft = rangeValue, keep, -1
}

// Serve starts a range server for the object and points every block
// file at it.
func (o *CSCBObject) Serve(tb testing.TB) *RangeServer {
	tb.Helper()
	server := &RangeServer{}
	server.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rangeValue := r.Header.Get("Range")
		server.mu.Lock()
		server.ranges = append(server.ranges, rangeValue)
		truncate := server.truncateRange != "" && server.truncateRange == rangeValue && server.truncateLeft != 0
		keep := server.truncateKeep
		if truncate && server.truncateLeft > 0 {
			server.truncateLeft--
		}
		server.mu.Unlock()

		var start, end uint64
		if _, err := fmt.Sscanf(rangeValue, "bytes=%d-%d", &start, &end); err != nil || start >= uint64(len(o.Data)) || end < start {
			w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
			return
		}
		if end >= uint64(len(o.Data)) {
			end = uint64(len(o.Data)) - 1
		}
		body := o.Data[start : end+1]
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(o.Data)))
		w.WriteHeader(http.StatusPartialContent)
		if truncate && keep < len(body) {
			body = body[:keep]
		}
		_, _ = w.Write(body)
	}))
	tb.Cleanup(server.Close)
	for _, file := range o.BlockFiles {
		file.FileUrl = server.URL
	}
	return server
}

// Ranges returns the Range headers seen so far, in request order.
func (s *RangeServer) Ranges() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.ranges...)
}

// CountRange returns how many requests carried exactly rangeValue.
func (s *RangeServer) CountRange(rangeValue string) int {
	n := 0
	for _, r := range s.Ranges() {
		if r == rangeValue {
			n++
		}
	}
	return n
}

// SyntheticBlocks builds count Solana-shaped blocks at heights
// startHeight.. with payloadSize bytes of highly compressible blob each,
// for size-driven tests and benchmarks.
func SyntheticBlocks(startHeight uint64, count int, payloadSize int) []*api.Block {
	return syntheticBlocks(startHeight, count, payloadSize, false)
}

// IncompressibleBlocks is SyntheticBlocks with pseudo-random blob bytes,
// so the compressed chunk is about as long as the raw payloads and a
// truncation at N compressed bytes lands roughly N raw bytes in.
func IncompressibleBlocks(startHeight uint64, count int, payloadSize int) []*api.Block {
	return syntheticBlocks(startHeight, count, payloadSize, true)
}

func syntheticBlocks(startHeight uint64, count int, payloadSize int, random bool) []*api.Block {
	rng := rand.New(rand.NewSource(int64(startHeight)))
	blocks := make([]*api.Block, count)
	for i := range blocks {
		height := startHeight + uint64(i)
		payload := make([]byte, payloadSize)
		if random {
			_, _ = rng.Read(payload)
		} else {
			for j := range payload {
				payload[j] = byte('a' + (i+j)%26)
			}
		}
		blocks[i] = &api.Block{
			Blockchain: common.Blockchain_BLOCKCHAIN_SOLANA,
			Network:    common.Network_NETWORK_SOLANA_MAINNET,
			Metadata: &api.BlockMetadata{
				Tag:          2,
				Hash:         fmt.Sprintf("hash-%d", height),
				ParentHash:   fmt.Sprintf("hash-%d", height-1),
				Height:       height,
				ParentHeight: height - 1,
			},
			Blobdata: &api.Block_Solana{Solana: &api.SolanaBlobdata{Header: payload}},
		}
	}
	return blocks
}
