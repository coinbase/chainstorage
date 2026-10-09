package bitcoin

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"runtime"
	"testing"
	"time"

	"go.uber.org/fx"
	"go.uber.org/zap"

	"google.golang.org/protobuf/proto"

	"github.com/coinbase/chainstorage/internal/blockchain/parser/internal"
	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/storage/blobstorage/downloader"
	"github.com/coinbase/chainstorage/internal/utils/fxparams"
	"github.com/coinbase/chainstorage/internal/utils/testapp"
	"github.com/coinbase/chainstorage/protos/coinbase/c3/common"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

// benchSeekedReader wraps a ReadCloser with a LimitReader view.
type benchSeekedReader struct {
	io.ReadCloser
	limit io.Reader
}

func (b *benchSeekedReader) Read(p []byte) (int, error) { return b.limit.Read(p) }

func protoSize(m proto.Message) int { return proto.Size(m) }

// TestZcashLargeBlockBench_Streaming runs the new end-to-end streaming
// path on a large zcash block: HTTP GET (served from a local httptest
// Server backed by the compressed .zstd file) → DownloadStream
// (disk-spool + decompress + proto.Unmarshal) → StreamBlockIter
// (iter.Seq2 consumption).
//
// Gated by env var CHAINSTORAGE_BENCH_ZCASH_ZSTD. Optional:
//
//	CHAINSTORAGE_BENCH_OPTS="skipScripts,skipWitnesses,skipShielded"
//
// Example:
//
//	CHAINSTORAGE_BENCH_ZCASH_ZSTD=/tmp/zcash_bench/block_322102.zstd \
//	go test -v -run=TestZcashLargeBlockBench_Streaming -count=1 -timeout=30m \
//	    ./internal/blockchain/parser/bitcoin/
func TestZcashLargeBlockBench_Streaming(t *testing.T) {
	zstdPath := os.Getenv("CHAINSTORAGE_BENCH_ZCASH_ZSTD")
	if zstdPath == "" {
		t.Skip("set CHAINSTORAGE_BENCH_ZCASH_ZSTD to the compressed .zstd file to run")
	}

	st, err := os.Stat(zstdPath)
	if err != nil {
		t.Fatalf("stat %s: %v", zstdPath, err)
	}
	t.Logf("serving: %s (%.2f MB compressed)", zstdPath, float64(st.Size())/1e6)

	// Serve via an in-process httptest server. Use ServeFile so the
	// compressed bytes stream from disk without buffering 700+ MB in RAM
	// on the server side.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, zstdPath)
	}))
	defer srv.Close()

	// Build the parser via fx.
	var parser internal.NativeParser
	var cfg *config.Config
	app := testapp.New(t,
		testapp.WithBlockchainNetwork(common.Blockchain_BLOCKCHAIN_ZCASH, common.Network_NETWORK_ZCASH_MAINNET),
		fx.Provide(NewZcashNativeParser),
		fx.Populate(&parser),
		fx.Populate(&cfg),
	)
	defer app.Close()

	// Build the downloader directly, wiring in the httptest client so
	// the test stays in-process and no TLS validation kicks in.
	dl := downloader.NewBlockDownloader(downloader.BlockDownloaderParams{
		Params:     fxparams.Params{Logger: zap.NewNop(), Config: cfg},
		HttpClient: srv.Client(),
	})

	bitcoinImpl, ok := parser.(internal.BitcoinStreamer)
	if !ok {
		t.Fatalf("parser %T does not implement BitcoinStreamer", parser)
	}

	opts := buildBenchOpts(t, os.Getenv("CHAINSTORAGE_BENCH_OPTS"))

	bf := &api.BlockFile{
		Tag:         1,
		FileUrl:     srv.URL,
		Compression: api.Compression_ZSTD,
	}

	ctx := context.Background()

	runtime.GC()
	runtime.GC()
	before := heapAlloc()
	peak := newHeapPeak()
	peak.start()

	var (
		txCount         int
		headerHash      string
		headerHeight    uint64
		headerCostStart time.Time
		headerCost      time.Duration
	)
	start := time.Now()
	dlStart := time.Now()
	spooled, err := dl.DownloadStream(ctx, bf)
	if err != nil {
		t.Fatalf("DownloadStream: %v", err)
	}
	defer spooled.Close()
	dlElapsed := time.Since(dlStart)

	// Walk the proto envelope once to collect chunk offsets.
	walkStart := time.Now()
	r, err := spooled.Open()
	if err != nil {
		t.Fatalf("open spool: %v", err)
	}
	_, chunks, walkErr := api.WalkBitcoinEnvelope(r)
	r.Close()
	if walkErr != nil {
		t.Fatalf("walk bitcoin envelope: %v", walkErr)
	}
	walkElapsed := time.Since(walkStart)

	openHeader := func() (io.ReadCloser, error) {
		f, err := spooled.Open()
		if err != nil {
			return nil, err
		}
		if _, err := f.(*os.File).Seek(chunks.Header.Offset, io.SeekStart); err != nil {
			f.Close()
			return nil, err
		}
		return &benchSeekedReader{ReadCloser: f, limit: io.LimitReader(f, chunks.Header.Length)}, nil
	}
	groups := chunks.InputTransactionsGroups
	loadGroup := func(i int) (*api.RepeatedBytes, error) {
		if i < 0 || i >= len(groups) {
			return nil, nil
		}
		ref := groups[i]
		f, err := spooled.Open()
		if err != nil {
			return nil, err
		}
		defer f.Close()
		if _, err := f.(*os.File).Seek(ref.Offset, io.SeekStart); err != nil {
			return nil, err
		}
		b := make([]byte, ref.Length)
		if _, err := io.ReadFull(f, b); err != nil {
			return nil, err
		}
		rb := &api.RepeatedBytes{}
		if err := proto.Unmarshal(b, rb); err != nil {
			return nil, err
		}
		return rb, nil
	}

	iterStart := time.Now()
	bstream := bitcoinImpl.StreamBlockIter(ctx, openHeader, loadGroup, opts...)
	for tx, err := range bstream.Transactions() {
		if err != nil {
			t.Fatalf("iterate tx: %v", err)
		}
		txCount++
		_ = tx
	}
	iterElapsed := time.Since(iterStart)

	// Header after iteration should be "free" per the BlockStream contract.
	headerCostStart = time.Now()
	h, err := bstream.Header()
	headerCost = time.Since(headerCostStart)
	if err != nil {
		t.Fatalf("Header: %v", err)
	}
	headerHash = h.GetHash()
	headerHeight = h.GetHeight()
	elapsed := time.Since(start)
	peak.stop()
	after := heapAlloc()

	t.Logf("\n=== Phase 2 streaming (DownloadStreamBitcoin -> StreamBlockIter) ===")
	t.Logf("total elapsed:          %s", elapsed)
	t.Logf("  download+spool:       %s", dlElapsed)
	t.Logf("  walk envelope:        %s", walkElapsed)
	t.Logf("  iter transactions:    %s", iterElapsed)
	t.Logf("tx count (visited):     %d", txCount)
	t.Logf("Header() cost:          %s (free after iteration)", headerCost)
	t.Logf("heap before:            %s", humanBytes(before))
	t.Logf("heap after:             %s", humanBytes(after))
	t.Logf("heap peak:              %s (+%s)", humanBytes(peak.peak), humanBytes(subU(peak.peak, before)))
	t.Logf("block hash:             %s", headerHash)
	t.Logf("block height:           %d", headerHeight)

	// ----- Non-streaming baseline: Download + ParseBlock -----
	// Drop all iter-derived references so GC can reclaim before the
	// next measurement.
	runtime.GC()
	runtime.GC()
	before2 := heapAlloc()
	peak2 := newHeapPeak()
	peak2.start()

	start2 := time.Now()
	dl2Start := time.Now()
	rawBlock, err := dl.Download(ctx, bf)
	if err != nil {
		t.Fatalf("Download: %v", err)
	}
	dl2Elapsed := time.Since(dl2Start)
	parse2Start := time.Now()
	nb, err := parser.ParseBlock(ctx, rawBlock, opts...)
	if err != nil {
		t.Fatalf("ParseBlock: %v", err)
	}
	parse2Elapsed := time.Since(parse2Start)
	elapsed2 := time.Since(start2)
	peak2.stop()
	after2 := heapAlloc()

	t.Logf("\n=== Legacy baseline (Download -> ParseBlock) ===")
	t.Logf("total elapsed:          %s", elapsed2)
	t.Logf("  download+unmarshal:   %s", dl2Elapsed)
	t.Logf("  parse block:          %s", parse2Elapsed)
	t.Logf("tx count:               %d", nb.NumTransactions)
	t.Logf("heap before:            %s", humanBytes(before2))
	t.Logf("heap after:             %s", humanBytes(after2))
	t.Logf("heap peak:              %s (+%s)", humanBytes(peak2.peak), humanBytes(subU(peak2.peak, before2)))
	t.Logf("persistent NB size:     %s", humanBytes(uint64(protoSize(nb))))

	// ----- Phase 2 chain-agnostic: DownloadStream (generic walker) + ParseBlock -----
	nb = nil
	runtime.GC()
	runtime.GC()
	before3 := heapAlloc()
	peak3 := newHeapPeak()
	peak3.start()

	start3 := time.Now()
	spooled3, err := dl.DownloadStream(ctx, bf)
	if err != nil {
		t.Fatalf("DownloadStream: %v", err)
	}
	spoolReader, err := spooled3.Open()
	if err != nil {
		t.Fatalf("open spool: %v", err)
	}
	block3, err := api.WalkBlockEnvelope(spoolReader)
	spoolReader.Close()
	if err != nil {
		t.Fatalf("WalkBlockEnvelope: %v", err)
	}
	nb3, err := parser.ParseBlock(ctx, block3, opts...)
	spooled3.Close()
	if err != nil {
		t.Fatalf("ParseBlock: %v", err)
	}
	elapsed3 := time.Since(start3)
	peak3.stop()
	after3 := heapAlloc()

	t.Logf("\n=== Phase 2 generic (DownloadStream -> WalkBlockEnvelope -> ParseBlock) ===")
	t.Logf("total elapsed:          %s", elapsed3)
	t.Logf("tx count:               %d", nb3.NumTransactions)
	t.Logf("heap before:            %s", humanBytes(before3))
	t.Logf("heap after:             %s", humanBytes(after3))
	t.Logf("heap peak:              %s (+%s)", humanBytes(peak3.peak), humanBytes(subU(peak3.peak, before3)))
	t.Logf("persistent NB size:     %s", humanBytes(uint64(protoSize(nb3))))

	t.Logf("\n=== Peak heap summary ===")
	t.Logf("legacy (Download):                     %s", humanBytes(subU(peak2.peak, before2)))
	t.Logf("Phase 2 generic (DownloadStream):      %s", humanBytes(subU(peak3.peak, before3)))
	t.Logf("Phase 2 bitcoin (DownloadStream+iter): %s", humanBytes(subU(peak.peak, before)))
}
