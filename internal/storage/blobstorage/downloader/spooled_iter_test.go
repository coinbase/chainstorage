package downloader

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/go-cmp/cmp"
	"go.uber.org/fx"
	"golang.org/x/xerrors"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"

	"github.com/coinbase/chainstorage/internal/storage/blobstorage/downloader/downloadertest"
	"github.com/coinbase/chainstorage/internal/utils/testapp"
	"github.com/coinbase/chainstorage/internal/utils/testutil"
	"github.com/coinbase/chainstorage/protos/coinbase/c3/common"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

func (s *blockDownloaderTestSuite) newDownloaderFor(server *httptest.Server) BlockDownloader {
	var dl BlockDownloader
	s.app = testapp.New(
		s.T(),
		fx.Provide(func() HTTPClient { return server.Client() }),
		fx.Provide(NewBlockDownloader),
		fx.Populate(&dl),
	)
	return dl
}

// readSpooled reads a SpooledBlock to the end, checking the reader also
// satisfies the parser's io.ReaderAt requirement, and closes it.
func readSpooled(t *testing.T, spooled *SpooledBlock) *api.Block {
	t.Helper()
	require := testutil.Require(t)
	rc, err := spooled.Open()
	require.NoError(err)
	_, isReaderAt := rc.(io.ReaderAt)
	require.True(isReaderAt, "spool reader must implement io.ReaderAt")
	gotBytes, err := io.ReadAll(rc)
	require.NoError(err)
	require.NoError(rc.Close())
	require.NoError(spooled.Close())
	var block api.Block
	require.NoError(proto.Unmarshal(gotBytes, &block))
	return &block
}

func (s *blockDownloaderTestSuite) TestOpenSpooledBlocks_CSCBChunkOnce() {
	require := testutil.Require(s.T())
	blocks := downloadertest.SyntheticBlocks(100, 6, 64)
	object := downloadertest.EncodeCSCB(s.T(), blocks, 3)
	server := object.Serve(s.T())
	dl := s.newDownloaderFor(server.Server)

	iter, err := dl.OpenSpooledBlocks(context.Background(), object.BlockFiles)
	require.NoError(err)
	defer iter.Close()

	for i, blockFile := range object.BlockFiles {
		spooled, err := iter.Next(context.Background())
		require.NoError(err)
		require.Equal(blockFile, spooled.BlockFile)
		got := readSpooled(s.T(), spooled)
		if diff := cmp.Diff(blocks[i], got, protocmp.Transform()); diff != "" {
			require.FailNow(diff)
		}
	}
	_, err = iter.Next(context.Background())
	require.True(err == io.EOF, "end of range is exactly io.EOF: %v", err)

	require.Equal(1, server.CountRange(object.ChunkRange(0)), "chunk 0 fetched once for its 3 blocks")
	require.Equal(1, server.CountRange(object.ChunkRange(1)), "chunk 1 fetched once for its 3 blocks")
	require.Len(server.Ranges(), 3, "index read + one range per chunk")
}

func (s *blockDownloaderTestSuite) TestOpenSpooledBlocks_SparseRequestStaysChunkOnce() {
	require := testutil.Require(s.T())
	blocks := downloadertest.SyntheticBlocks(100, 6, 64)
	object := downloadertest.EncodeCSCB(s.T(), blocks, 6)
	server := object.Serve(s.T())
	dl := s.newDownloaderFor(server.Server)

	// Heights 100, 102, 105 out of one chunk: ascending, with gaps.
	requested := []*api.BlockFile{object.BlockFiles[0], object.BlockFiles[2], object.BlockFiles[5]}
	iter, err := dl.OpenSpooledBlocks(context.Background(), requested)
	require.NoError(err)
	defer iter.Close()
	for _, want := range []int{0, 2, 5} {
		spooled, err := iter.Next(context.Background())
		require.NoError(err)
		got := readSpooled(s.T(), spooled)
		require.Equal(blocks[want].GetMetadata().GetHeight(), got.GetMetadata().GetHeight())
	}
	_, err = iter.Next(context.Background())
	require.ErrorIs(err, io.EOF)
	require.Equal(1, server.CountRange(object.ChunkRange(0)))
}

func (s *blockDownloaderTestSuite) TestOpenSpooledBlocks_DescendingInputSplitsGroups() {
	require := testutil.Require(s.T())
	blocks := downloadertest.SyntheticBlocks(100, 4, 64)
	object := downloadertest.EncodeCSCB(s.T(), blocks, 4)
	server := object.Serve(s.T())
	dl := s.newDownloaderFor(server.Server)

	// Input order is preserved even when it runs against chunk order;
	// the cost is a second range request, never a reordered yield.
	requested := []*api.BlockFile{object.BlockFiles[1], object.BlockFiles[0]}
	iter, err := dl.OpenSpooledBlocks(context.Background(), requested)
	require.NoError(err)
	defer iter.Close()
	for _, want := range []int{1, 0} {
		spooled, err := iter.Next(context.Background())
		require.NoError(err)
		got := readSpooled(s.T(), spooled)
		require.Equal(blocks[want].GetMetadata().GetHeight(), got.GetMetadata().GetHeight())
	}
	require.Equal(2, server.CountRange(object.ChunkRange(0)))
}

func (s *blockDownloaderTestSuite) TestOpenSpooledBlocks_MixedSkippedAndSingleBlock() {
	require := testutil.Require(s.T())
	blocks := downloadertest.SyntheticBlocks(100, 2, 64)
	object := downloadertest.EncodeCSCB(s.T(), blocks, 2)
	server := object.Serve(s.T())

	single := &api.Block{
		Blockchain: common.Blockchain_BLOCKCHAIN_SOLANA,
		Network:    common.Network_NETWORK_SOLANA_MAINNET,
		Metadata:   &api.BlockMetadata{Tag: 2, Height: 900, Hash: "single"},
	}
	singleBytes, err := proto.Marshal(single)
	require.NoError(err)
	singleServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(singleBytes)
	}))
	defer singleServer.Close()
	singleFile := &api.BlockFile{Tag: 2, Height: 900, Hash: "single", FileUrl: singleServer.URL}
	skippedFile := &api.BlockFile{Tag: 2, Height: 901, Skipped: true}

	dl := s.newDownloaderFor(server.Server)
	requested := []*api.BlockFile{object.BlockFiles[0], skippedFile, singleFile, object.BlockFiles[1]}
	iter, err := dl.OpenSpooledBlocks(context.Background(), requested)
	require.NoError(err)
	defer iter.Close()

	got := readSpooled(s.T(), mustNext(s.T(), iter))
	require.Equal(uint64(100), got.GetMetadata().GetHeight())

	skipped := mustNext(s.T(), iter)
	require.True(skipped.BlockFile.GetSkipped())
	rc, err := skipped.Open()
	require.NoError(err)
	empty, err := io.ReadAll(rc)
	require.NoError(err)
	require.Empty(empty)
	require.NoError(skipped.Close())

	got = readSpooled(s.T(), mustNext(s.T(), iter))
	require.Equal(uint64(900), got.GetMetadata().GetHeight())

	got = readSpooled(s.T(), mustNext(s.T(), iter))
	require.Equal(uint64(101), got.GetMetadata().GetHeight())

	_, err = iter.Next(context.Background())
	require.ErrorIs(err, io.EOF)
	// The two CSCB blocks were separated by other files, so the chunk
	// was legitimately opened twice; each open is still one request.
	require.Equal(2, server.CountRange(object.ChunkRange(0)))
}

func (s *blockDownloaderTestSuite) TestOpenSpooledBlocks_ResumesMidGroupAfterTruncatedChunk() {
	require := testutil.Require(s.T())
	const blockSize = 256 * 1024
	blocks := downloadertest.IncompressibleBlocks(100, 4, blockSize)
	object := downloadertest.EncodeCSCB(s.T(), blocks, 4)
	server := object.Serve(s.T())
	// Incompressible payloads make compressed bytes ≈ raw bytes, so
	// cutting the first response at 2.5 blocks lets blocks 100 and 101
	// out and kills the read of 102: the retry must resume at 102 with a
	// fresh chunk request, not restart the group (which would hand out
	// block 100's payload again and fail the parity check below).
	server.TruncateOnce(object.ChunkRange(0), blockSize*5/2)
	dl := s.newDownloaderFor(server.Server)

	iter, err := dl.OpenSpooledBlocks(context.Background(), object.BlockFiles)
	require.NoError(err)
	defer iter.Close()
	for i := range object.BlockFiles {
		if i == 2 {
			require.Equal(1, server.CountRange(object.ChunkRange(0)), "blocks 100 and 101 came from the truncated response")
		}
		spooled, err := iter.Next(context.Background())
		require.NoError(err, "block %d", i)
		got := readSpooled(s.T(), spooled)
		if diff := cmp.Diff(blocks[i], got, protocmp.Transform()); diff != "" {
			require.FailNow(diff)
		}
	}
	require.Equal(2, server.CountRange(object.ChunkRange(0)), "one truncated attempt plus one reopen")
}

func (s *blockDownloaderTestSuite) TestOpenSpooledBlocks_ExhaustedRetriesAreNeverEOF() {
	require := testutil.Require(s.T())
	// Blocks must exceed zstd's 128 KiB block size, or a truncated
	// response may not contain one complete compressed block and even
	// height 100 fails to decode.
	const blockSize = 256 * 1024
	blocks := downloadertest.IncompressibleBlocks(100, 3, blockSize)
	object := downloadertest.EncodeCSCB(s.T(), blocks, 3)
	server := object.Serve(s.T())
	server.TruncateAlways(object.ChunkRange(0), blockSize*3/2)
	dl := s.newDownloaderFor(server.Server)

	iter, err := dl.OpenSpooledBlocks(context.Background(), object.BlockFiles)
	require.NoError(err)
	defer iter.Close()
	got := readSpooled(s.T(), mustNext(s.T(), iter))
	require.Equal(uint64(100), got.GetMetadata().GetHeight())

	_, err = iter.Next(context.Background())
	require.Error(err)
	require.False(err == io.EOF, "a failed download must not look like the end of the range")
	require.False(xerrors.Is(err, io.EOF), "nor unwrap to it: %v", err)
	require.ErrorContains(err, "height 101")

	_, again := iter.Next(context.Background())
	require.Equal(err, again, "the error is sticky; Next cannot skip the failed block")
	require.GreaterOrEqual(server.CountRange(object.ChunkRange(0)), 2, "retries reopened the chunk")
}

func (s *blockDownloaderTestSuite) TestOpenSpooledBlocks_CloseAndCancel() {
	require := testutil.Require(s.T())
	blocks := downloadertest.SyntheticBlocks(100, 3, 64)
	object := downloadertest.EncodeCSCB(s.T(), blocks, 3)
	server := object.Serve(s.T())
	dl := s.newDownloaderFor(server.Server)

	iter, err := dl.OpenSpooledBlocks(context.Background(), object.BlockFiles)
	require.NoError(err)
	first := mustNext(s.T(), iter)
	require.NoError(first.Close())
	_, err = first.Open()
	require.Error(err, "closed spooled block must not reopen")
	require.NoError(iter.Close())
	require.NoError(iter.Close(), "idempotent")
	_, err = iter.Next(context.Background())
	require.ErrorContains(err, "closed")

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	iter, err = dl.OpenSpooledBlocks(ctx, object.BlockFiles)
	require.ErrorIs(err, context.Canceled)
	require.Nil(iter)
}

func mustNext(t *testing.T, iter SpooledBlockIterator) *SpooledBlock {
	t.Helper()
	spooled, err := iter.Next(context.Background())
	testutil.Require(t).NoError(err)
	return spooled
}

// BenchmarkSpooledBlocks_ChunkOnceVsPerBlock measures the point of the
// iterator: walking a 25-block chunk through OpenSpooledBlocks
// decompresses it once, while DownloadStream per block re-reads and
// re-decompresses the chunk prefix for every block.
func BenchmarkSpooledBlocks_ChunkOnceVsPerBlock(b *testing.B) {
	const blocksPerChunk = 25
	blocks := downloadertest.SyntheticBlocks(100, blocksPerChunk, 1<<20)
	object := downloadertest.EncodeCSCB(b, blocks, blocksPerChunk)
	server := object.Serve(b)

	var dl BlockDownloader
	app := testapp.New(
		b,
		fx.Provide(func() HTTPClient { return server.Client() }),
		fx.Provide(NewBlockDownloader),
		fx.Populate(&dl),
	)
	defer app.Close()

	b.Run("download_stream_per_block", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			for _, file := range object.BlockFiles {
				spooled, err := dl.DownloadStream(context.Background(), file)
				if err != nil {
					b.Fatal(err)
				}
				rc, _ := spooled.Open()
				_, _ = io.Copy(io.Discard, rc)
				_ = rc.Close()
				_ = spooled.Close()
			}
		}
	})
	b.Run("open_spooled_blocks", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			iter, err := dl.OpenSpooledBlocks(context.Background(), object.BlockFiles)
			if err != nil {
				b.Fatal(err)
			}
			for {
				spooled, err := iter.Next(context.Background())
				if err == io.EOF {
					break
				}
				if err != nil {
					b.Fatal(err)
				}
				rc, _ := spooled.Open()
				_, _ = io.Copy(io.Discard, rc)
				_ = rc.Close()
				_ = spooled.Close()
			}
			_ = iter.Close()
		}
	})
}
