package bitcoin

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/coinbase/chainstorage/internal/blockchain/parser/internal"
	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/storage/blobstorage/downloader"
	storage_utils "github.com/coinbase/chainstorage/internal/storage/utils"
	"github.com/coinbase/chainstorage/internal/utils/fxparams"
	"github.com/coinbase/chainstorage/internal/utils/testapp"
	"github.com/coinbase/chainstorage/protos/coinbase/c3/common"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

// TestProdParityBitcoinFamily downloads random blocks from the prod
// S3 buckets for each bitcoin-family chain and asserts that the
// full legacy pipeline (Download → proto.Unmarshal → ParseBlock)
// and the full streaming pipeline (DownloadStream → walker → iter)
// produce identical transaction lists.
//
// Timing and memory measurements cover the ENTIRE pipeline for each
// path — read + decompress + parse/stream. A local httptest server
// serves cached compressed blocks to the real downloader so the
// measurements include the production code path rather than just
// the final parse step.
//
// Gated by env var CHAINSTORAGE_PROD_PARITY=1 so it does not run in
// default `go test` sweeps. Requires AWS_PROFILE=cipherowl-prod (or
// equivalent credentials) and network access to S3 for the initial
// cache population.
//
// Blocks are cached on disk under CHAINSTORAGE_PROD_PARITY_CACHE_DIR
// (default ~/data/chainstorage/{chain}/) as their compressed bytes,
// so re-runs skip the S3 fetch. Deterministic height selection via
// seed makes the same cache reusable across runs.
//
// Tuning knobs:
//
//	CHAINSTORAGE_PROD_PARITY_COUNT=1000       (blocks per chain, default 1000)
//	CHAINSTORAGE_PROD_PARITY_WORKERS=20       (concurrent downloads; forced to 1 when PROFILE_MEM=1)
//	CHAINSTORAGE_PROD_PARITY_SEED=42          (deterministic height selection)
//	CHAINSTORAGE_PROD_PARITY_CHAINS=bitcoin,dash,zcash
//	CHAINSTORAGE_PROD_PARITY_CACHE_DIR=~/data/chainstorage
//	CHAINSTORAGE_PROD_PARITY_PROFILE_MEM=1    (measure per-block peak heap for both paths; runs serially)
func TestProdParityBitcoinFamily(t *testing.T) {
	if os.Getenv("CHAINSTORAGE_PROD_PARITY") != "1" {
		t.Skip("set CHAINSTORAGE_PROD_PARITY=1 (and AWS_PROFILE=cipherowl-prod) to run")
	}

	count := envInt("CHAINSTORAGE_PROD_PARITY_COUNT", 1000)
	workers := envInt("CHAINSTORAGE_PROD_PARITY_WORKERS", 20)
	seed := envInt("CHAINSTORAGE_PROD_PARITY_SEED", 42)
	profileMem := os.Getenv("CHAINSTORAGE_PROD_PARITY_PROFILE_MEM") == "1"
	if profileMem {
		// Per-block heap measurement requires serial execution:
		// HeapAlloc is process-global and concurrent workers pollute
		// each other's peak readings.
		workers = 1
	}
	cacheDir := os.Getenv("CHAINSTORAGE_PROD_PARITY_CACHE_DIR")
	if cacheDir == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			t.Fatalf("resolve home dir: %v", err)
		}
		cacheDir = filepath.Join(home, "data", "chainstorage")
	}

	allChains := []prodChain{
		{
			name:        "bitcoin",
			blockchain:  common.Blockchain_BLOCKCHAIN_BITCOIN,
			network:     common.Network_NETWORK_BITCOIN_MAINNET,
			bucket:      "co-chainstorage-bitcoin-mainnet-prod",
			keyPrefix:   "BLOCKCHAIN_BITCOIN/NETWORK_BITCOIN_MAINNET/2/",
			fileExt:     ".gzip",
			maxHeight:   945_818,
			compression: api.Compression_GZIP,
			newParser:   NewBitcoinNativeParser,
		},
		{
			name:        "dash",
			blockchain:  common.Blockchain_BLOCKCHAIN_DASH,
			network:     common.Network_NETWORK_DASH_MAINNET,
			bucket:      "co-chainstorage-dash-mainnet-prod",
			keyPrefix:   "BLOCKCHAIN_DASH/NETWORK_DASH_MAINNET/1/",
			fileExt:     ".zstd",
			maxHeight:   2_457_707,
			compression: api.Compression_ZSTD,
			newParser:   NewDashNativeParser,
		},
		{
			name:        "zcash",
			blockchain:  common.Blockchain_BLOCKCHAIN_ZCASH,
			network:     common.Network_NETWORK_ZCASH_MAINNET,
			bucket:      "co-chainstorage-zcash-mainnet-prod",
			keyPrefix:   "BLOCKCHAIN_ZCASH/NETWORK_ZCASH_MAINNET/1/",
			fileExt:     ".zstd",
			maxHeight:   3_313_706,
			compression: api.Compression_ZSTD,
			newParser:   NewZcashNativeParser,
		},
	}

	selected := envChains(os.Getenv("CHAINSTORAGE_PROD_PARITY_CHAINS"), allChains)

	ctx := context.Background()
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx)
	if err != nil {
		t.Fatalf("load aws config: %v", err)
	}
	s3client := s3.NewFromConfig(awsCfg)

	for _, c := range selected {
		c := c
		chainCacheDir := filepath.Join(cacheDir, c.name)
		if err := os.MkdirAll(chainCacheDir, 0o755); err != nil {
			t.Fatalf("mkdir cache: %v", err)
		}
		t.Run(c.name, func(t *testing.T) {
			runProdParity(t, ctx, s3client, c, chainCacheDir, count, workers, seed, profileMem)
		})
	}
}

type prodChain struct {
	name        string
	blockchain  common.Blockchain
	network     common.Network
	bucket      string
	keyPrefix   string // ends with "/"
	fileExt     string // ".gzip" or ".zstd"
	maxHeight   int
	compression api.Compression
	newParser   any // fx.Provide constructor
}

func runProdParity(t *testing.T, ctx context.Context, s3client *s3.Client, c prodChain, cacheDir string, count, workers, seed int, profileMem bool) {
	var parser internal.NativeParser
	var cfg *config.Config
	app := testapp.New(t,
		testapp.WithBlockchainNetwork(c.blockchain, c.network),
		fx.Provide(c.newParser),
		fx.Populate(&parser),
		fx.Populate(&cfg),
	)
	defer app.Close()

	streamer, ok := parser.(internal.BitcoinStreamer)
	if !ok {
		t.Fatalf("%s parser does not implement BitcoinStreamer", c.name)
	}

	// Local httptest server serving compressed blocks by filename.
	// Each block's BlockFile.FileUrl points here so the real
	// downloader exercises the full HTTP+decompress path.
	srv := httptest.NewServer(http.FileServer(http.Dir(cacheDir)))
	defer srv.Close()

	dl := downloader.NewBlockDownloader(downloader.BlockDownloaderParams{
		Params:     fxparams.Params{Logger: zap.NewNop(), Config: cfg},
		HttpClient: srv.Client(),
	})

	rng := rand.New(rand.NewSource(int64(seed)))
	heights := make([]int, count)
	for i := range heights {
		heights[i] = 1 + rng.Intn(c.maxHeight)
	}

	// Ensure every sampled height is present on disk up front, so
	// the pipeline-timing worker loop never has to talk to S3. The
	// pre-fetch is serial + I/O-bound; the actual measurement runs
	// afterwards against only the local cache.
	prefetchHeights(ctx, t, s3client, c, cacheDir, heights)

	type job struct {
		height   int
		fileName string
	}
	jobs := make(chan job, len(heights))
	for _, h := range heights {
		entry, err := findCachedFile(cacheDir, h, c.fileExt)
		if err != nil {
			continue
		}
		jobs <- job{height: h, fileName: entry}
	}
	close(jobs)

	var (
		attempted  atomic.Int64
		mismatches atomic.Int64
		mu         sync.Mutex
		failures   []parityFailure
		samples    []blockSample
	)

	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range jobs {
				attempted.Add(1)
				bf := &api.BlockFile{
					Tag:         1,
					Height:      uint64(j.height),
					Compression: c.compression,
					FileUrl:     srv.URL + "/" + j.fileName,
				}
				var (
					sample blockSample
					err    error
				)
				if profileMem {
					sample = measureFullPipelineProfile(dl, parser, streamer, bf, j.height)
				} else {
					sample, err = measureFullPipelineParity(dl, parser, streamer, bf, j.height)
					if err != nil {
						mismatches.Add(1)
						mu.Lock()
						failures = append(failures, parityFailure{
							height: j.height,
							key:    j.fileName,
							err:    err,
						})
						mu.Unlock()
						continue
					}
				}
				mu.Lock()
				samples = append(samples, sample)
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	t.Logf("[%s] attempted=%d mismatches=%d (full pipeline timing: read + decompress + parse/stream)",
		c.name, attempted.Load(), mismatches.Load())

	reportSamples(t, c.name, samples, profileMem)

	if len(failures) > 0 {
		for i, f := range failures {
			if i >= 10 {
				t.Logf("  ... and %d more failures", len(failures)-10)
				break
			}
			t.Logf("  FAIL height=%d file=%s err=%v", f.height, f.key, f.err)
		}
		t.Fatalf("[%s] %d parity mismatches", c.name, mismatches.Load())
	}
}

type parityFailure struct {
	height int
	key    string // local file name (within chain cache dir)
	err    error
}

// blockSample records per-block full-pipeline timing + (optionally)
// memory metrics for one block.
type blockSample struct {
	height     int
	txCount    int
	rawSize    int // proto.Size of the *api.Block after unmarshal
	parseTime  time.Duration
	streamTime time.Duration
	parsePeak  uint64 // bytes; populated only when profileMem=true
	streamPeak uint64 // bytes; populated only when profileMem=true
}

// prefetchHeights downloads any missing blocks to disk serially via
// the S3 client. Returns when all heights are either cached or known
// missing (404-equivalent).
func prefetchHeights(ctx context.Context, t *testing.T, s3client *s3.Client, c prodChain, cacheDir string, heights []int) {
	t.Helper()
	var fetched, cached, missing int
	for _, h := range heights {
		name, err := findCachedFile(cacheDir, h, c.fileExt)
		if err == nil && name != "" {
			cached++
			continue
		}
		if downloaded := s3DownloadAndCache(ctx, s3client, c, cacheDir, h); downloaded {
			fetched++
		} else {
			missing++
		}
	}
	t.Logf("[%s] prefetch: cached=%d fetched=%d missing=%d", c.name, cached, fetched, missing)
}

// findCachedFile returns the bare filename (no dir) for height's
// cached object, if present. Returns "" if the expected file doesn't
// exist.
func findCachedFile(cacheDir string, height int, fileExt string) (string, error) {
	name := strconv.Itoa(height) + fileExt
	path := filepath.Join(cacheDir, name)
	st, err := os.Stat(path)
	if err != nil {
		return "", err
	}
	if st.IsDir() {
		return "", fmt.Errorf("unexpected directory at %s", path)
	}
	return name, nil
}

// s3DownloadAndCache fetches the block at height from S3 and persists
// it under cacheDir using the {height}{ext} filename convention.
// Returns true when a block was successfully downloaded.
func s3DownloadAndCache(ctx context.Context, s3client *s3.Client, c prodChain, cacheDir string, height int) bool {
	prefix := c.keyPrefix + strconv.Itoa(height) + "/"
	list, err := s3client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket:  aws.String(c.bucket),
		Prefix:  aws.String(prefix),
		MaxKeys: aws.Int32(2),
	})
	if err != nil || len(list.Contents) == 0 {
		return false
	}
	key := *list.Contents[0].Key
	obj, err := s3client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(c.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return false
	}
	defer obj.Body.Close()
	compressedBytes, err := io.ReadAll(obj.Body)
	if err != nil {
		return false
	}
	localPath := filepath.Join(cacheDir, strconv.Itoa(height)+c.fileExt)
	tmpPath := localPath + ".tmp"
	if err := os.WriteFile(tmpPath, compressedBytes, 0o644); err != nil {
		return false
	}
	return os.Rename(tmpPath, localPath) == nil
}

// measureFullPipelineParity times both full pipelines on a block and
// asserts their transaction lists match. Used in the concurrent mode
// (workers > 1).
//
// Legacy pipeline:  dl.Download → parser.ParseBlock
// Streaming path:   dl.DownloadStream → WalkBitcoinEnvelope → StreamBlockIter
//
// Both timers cover HTTP read + decompression + parse/stream. Parity
// comparison happens outside the streaming timer.
func measureFullPipelineParity(dl downloader.BlockDownloader, parser internal.NativeParser, streamer internal.BitcoinStreamer, bf *api.BlockFile, height int) (blockSample, error) {
	ctx := context.Background()
	sample := blockSample{height: height}

	// --- Legacy pipeline ---
	legacyStart := time.Now()
	rawBlock, err := dl.Download(ctx, bf)
	if err != nil {
		return sample, fmt.Errorf("Download: %w", err)
	}
	baseline, err := parser.ParseBlock(ctx, rawBlock)
	sample.parseTime = time.Since(legacyStart)
	if err != nil {
		return sample, fmt.Errorf("ParseBlock: %w", err)
	}
	sample.rawSize = proto.Size(rawBlock)
	baseTxs := baseline.GetBitcoin().GetTransactions()

	// --- Streaming pipeline ---
	streamStart := time.Now()
	spooled, err := dl.DownloadStream(ctx, bf)
	if err != nil {
		return sample, fmt.Errorf("DownloadStream: %w", err)
	}
	streamed, iterErr := runStreamOverSpool(ctx, streamer, spooled)
	sample.streamTime = time.Since(streamStart)
	spooled.Close()
	if iterErr != nil {
		return sample, iterErr
	}
	sample.txCount = len(streamed)

	if len(baseTxs) != len(streamed) {
		return sample, fmt.Errorf("tx count: ParseBlock=%d StreamBlockIter=%d", len(baseTxs), len(streamed))
	}
	for i, b := range baseTxs {
		s := streamed[i]
		if b.TransactionId != s.TransactionId {
			return sample, fmt.Errorf("tx[%d] id: %q vs %q", i, b.TransactionId, s.TransactionId)
		}
		if b.Hash != s.Hash {
			return sample, fmt.Errorf("tx[%d] hash: %q vs %q", i, b.Hash, s.Hash)
		}
		if b.InputCount != s.InputCount {
			return sample, fmt.Errorf("tx[%d] InputCount: %d vs %d", i, b.InputCount, s.InputCount)
		}
		if b.OutputCount != s.OutputCount {
			return sample, fmt.Errorf("tx[%d] OutputCount: %d vs %d", i, b.OutputCount, s.OutputCount)
		}
	}
	return sample, nil
}

// measureFullPipelineProfile times and measures peak heap delta for
// both pipelines in isolation. The streaming path DOES NOT accumulate
// transactions — this reflects the intrinsic streaming peak-RAM
// profile a production consumer would see, not the collection-for-
// parity overhead.
//
// Must be called serially: runtime.MemStats is process-global.
func measureFullPipelineProfile(dl downloader.BlockDownloader, parser internal.NativeParser, streamer internal.BitcoinStreamer, bf *api.BlockFile, height int) blockSample {
	ctx := context.Background()
	sample := blockSample{height: height}

	// --- Legacy pipeline ---
	//
	// Scoped inside a closure so rawBlock + baseline drop out of
	// scope when it returns. Without the closure, local-var refs
	// are still live on the goroutine stack when we call
	// runtime.GC() below, which would prevent reclamation before
	// the streaming measurement starts.
	parseOK := func() bool {
		runtime.GC()
		runtime.GC()
		parseBaseline := heapAlloc()
		parsePeak := newHeapPeak()
		parsePeak.start()
		legacyStart := time.Now()
		rawBlock, err := dl.Download(ctx, bf)
		if err != nil {
			parsePeak.stop()
			return false
		}
		baseline, err := parser.ParseBlock(ctx, rawBlock)
		sample.parseTime = time.Since(legacyStart)
		parsePeak.stop()
		sample.parsePeak = peakDelta(parsePeak.peak, parseBaseline)
		if err != nil {
			return false
		}
		sample.rawSize = proto.Size(rawBlock)
		if bc := baseline.GetBitcoin(); bc != nil {
			sample.txCount = len(bc.GetTransactions())
		}
		return true
	}()
	if !parseOK {
		return sample
	}

	// Force GC now that the closure's locals are out of scope, so
	// streaming's peak measurement starts from a clean baseline.
	runtime.GC()
	runtime.GC()

	// --- Streaming pipeline (no tx accumulation) ---
	streamBaseline := heapAlloc()
	streamPeak := newHeapPeak()
	streamPeak.start()
	streamStart := time.Now()
	spooled, err := dl.DownloadStream(ctx, bf)
	if err != nil {
		streamPeak.stop()
		return sample
	}
	_ = runStreamOverSpoolDiscard(ctx, streamer, spooled)
	sample.streamTime = time.Since(streamStart)
	streamPeak.stop()
	sample.streamPeak = peakDelta(streamPeak.peak, streamBaseline)
	_ = spooled.Close()

	return sample
}

// runStreamOverSpool walks the spooled bitcoin envelope, wires
// seek-based loaders, and collects emitted transactions for parity
// comparison.
func runStreamOverSpool(ctx context.Context, streamer internal.BitcoinStreamer, spooled *downloader.SpooledBlock) ([]*api.BitcoinTransaction, error) {
	opener, loadGroup, closeFn, err := spoolLoaders(spooled)
	if err != nil {
		return nil, err
	}
	defer closeFn()
	stream := streamer.StreamBlockIter(ctx, opener, loadGroup)
	var out []*api.BitcoinTransaction
	for tx, iterErr := range stream.Transactions() {
		if iterErr != nil {
			return nil, fmt.Errorf("iter: %w", iterErr)
		}
		out = append(out, tx)
	}
	return out, nil
}

// runStreamOverSpoolDiscard walks the spool + iterates without
// retaining transactions. Used in memory-profile mode so peak heap
// reflects the intrinsic streaming profile.
func runStreamOverSpoolDiscard(ctx context.Context, streamer internal.BitcoinStreamer, spooled *downloader.SpooledBlock) error {
	opener, loadGroup, closeFn, err := spoolLoaders(spooled)
	if err != nil {
		return err
	}
	defer closeFn()
	stream := streamer.StreamBlockIter(ctx, opener, loadGroup)
	for _, iterErr := range stream.Transactions() {
		if iterErr != nil {
			return fmt.Errorf("iter: %w", iterErr)
		}
	}
	return nil
}

// spoolLoaders walks the spool once, caches a single file handle,
// and returns pread-based loaders. Mirrors the internal parser's
// optimized loader implementation so this test measures the
// production code path. Callers MUST invoke the returned close fn.
func spoolLoaders(spooled *downloader.SpooledBlock) (func() (io.ReadCloser, error), func(int) (*api.RepeatedBytes, error), func() error, error) {
	r, err := spooled.Open()
	if err != nil {
		return nil, nil, nil, fmt.Errorf("open spool: %w", err)
	}
	_, chunks, walkErr := api.WalkBitcoinEnvelope(r)
	r.Close()
	if walkErr != nil {
		return nil, nil, nil, fmt.Errorf("walk: %w", walkErr)
	}

	// One handle for the whole iterator lifetime; ReadAt/pread keeps
	// the loaders syscall-light under many-tx blocks.
	handle, err := spooled.Open()
	if err != nil {
		return nil, nil, nil, fmt.Errorf("open spool for loaders: %w", err)
	}
	readerAt, ok := handle.(io.ReaderAt)
	if !ok {
		handle.Close()
		return nil, nil, nil, fmt.Errorf("spool reader not io.ReaderAt")
	}

	openHeader := func() (io.ReadCloser, error) {
		return io.NopCloser(io.NewSectionReader(readerAt, chunks.Header.Offset, chunks.Header.Length)), nil
	}
	groups := chunks.InputTransactionsGroups
	loadGroup := func(i int) (*api.RepeatedBytes, error) {
		if i < 0 || i >= len(groups) {
			return nil, nil
		}
		ref := groups[i]
		b := make([]byte, ref.Length)
		if _, err := readerAt.ReadAt(b, ref.Offset); err != nil && err != io.EOF {
			return nil, fmt.Errorf("readat group [%d]: %w", i, err)
		}
		rb := &api.RepeatedBytes{}
		if err := proto.Unmarshal(b, rb); err != nil {
			return nil, err
		}
		return rb, nil
	}
	return openHeader, loadGroup, handle.Close, nil
}

// reportSamples prints aggregate timing (and memory) stats for a
// chain's parity run.
func reportSamples(t *testing.T, chain string, samples []blockSample, profileMem bool) {
	if len(samples) == 0 {
		return
	}

	parseTimes := make([]time.Duration, len(samples))
	streamTimes := make([]time.Duration, len(samples))
	for i, s := range samples {
		parseTimes[i] = s.parseTime
		streamTimes[i] = s.streamTime
	}
	sort.Slice(parseTimes, func(i, j int) bool { return parseTimes[i] < parseTimes[j] })
	sort.Slice(streamTimes, func(i, j int) bool { return streamTimes[i] < streamTimes[j] })

	t.Logf("[%s] full-pipeline timing (n=%d)", chain, len(samples))
	t.Logf("  Download+ParseBlock:        mean=%s p50=%s p95=%s p99=%s max=%s",
		meanDur(parseTimes), pctDur(parseTimes, 0.50),
		pctDur(parseTimes, 0.95), pctDur(parseTimes, 0.99), parseTimes[len(parseTimes)-1])
	t.Logf("  DownloadStream+StreamIter:  mean=%s p50=%s p95=%s p99=%s max=%s",
		meanDur(streamTimes), pctDur(streamTimes, 0.50),
		pctDur(streamTimes, 0.95), pctDur(streamTimes, 0.99), streamTimes[len(streamTimes)-1])

	// Top-5 largest blocks by raw size.
	bySize := make([]blockSample, len(samples))
	copy(bySize, samples)
	sort.Slice(bySize, func(i, j int) bool { return bySize[i].rawSize > bySize[j].rawSize })
	top := 5
	if len(bySize) < top {
		top = len(bySize)
	}
	t.Logf("[%s] top-%d largest blocks (by proto.Size):", chain, top)
	for i := 0; i < top; i++ {
		s := bySize[i]
		base := fmt.Sprintf("  h=%d size=%s txs=%d parse=%s stream=%s",
			s.height, humanBytes(uint64(s.rawSize)), s.txCount, s.parseTime, s.streamTime)
		if profileMem {
			base += fmt.Sprintf(" parsePeak=%s streamPeak=%s",
				humanBytes(s.parsePeak), humanBytes(s.streamPeak))
		}
		t.Logf("%s", base)
	}

	if profileMem {
		parsePeaks := make([]uint64, len(samples))
		streamPeaks := make([]uint64, len(samples))
		for i, s := range samples {
			parsePeaks[i] = s.parsePeak
			streamPeaks[i] = s.streamPeak
		}
		sort.Slice(parsePeaks, func(i, j int) bool { return parsePeaks[i] < parsePeaks[j] })
		sort.Slice(streamPeaks, func(i, j int) bool { return streamPeaks[i] < streamPeaks[j] })
		t.Logf("[%s] full-pipeline peak heap (n=%d)", chain, len(samples))
		t.Logf("  Download+ParseBlock:        p50=%s p95=%s p99=%s max=%s",
			humanBytes(pctU64(parsePeaks, 0.50)), humanBytes(pctU64(parsePeaks, 0.95)),
			humanBytes(pctU64(parsePeaks, 0.99)), humanBytes(parsePeaks[len(parsePeaks)-1]))
		t.Logf("  DownloadStream+StreamIter:  p50=%s p95=%s p99=%s max=%s",
			humanBytes(pctU64(streamPeaks, 0.50)), humanBytes(pctU64(streamPeaks, 0.95)),
			humanBytes(pctU64(streamPeaks, 0.99)), humanBytes(streamPeaks[len(streamPeaks)-1]))
	}
}

func meanDur(ds []time.Duration) time.Duration {
	var total time.Duration
	for _, d := range ds {
		total += d
	}
	return total / time.Duration(len(ds))
}

func pctDur(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(float64(len(sorted)-1) * p)
	return sorted[idx]
}

func pctU64(sorted []uint64, p float64) uint64 {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(float64(len(sorted)-1) * p)
	return sorted[idx]
}

func peakDelta(peak, baseline uint64) uint64 {
	if peak > baseline {
		return peak - baseline
	}
	return 0
}

func envInt(name string, def int) int {
	if v := os.Getenv(name); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func envChains(spec string, all []prodChain) []prodChain {
	if spec == "" {
		return all
	}
	want := make(map[string]bool)
	for _, n := range strings.Split(spec, ",") {
		want[strings.TrimSpace(n)] = true
	}
	var out []prodChain
	for _, c := range all {
		if want[c.name] {
			out = append(out, c)
		}
	}
	return out
}

// Unused in the new full-pipeline flow; kept so `decodeCompressed`
// has one caller to satisfy the linter if future tests want it.
var _ = decodeCompressed

func decodeCompressed(data []byte, compression api.Compression) (*api.Block, error) {
	decoded, err := storage_utils.Decompress(data, compression)
	if err != nil {
		return nil, fmt.Errorf("decompress: %w", err)
	}
	var block api.Block
	if err := proto.Unmarshal(decoded, &block); err != nil {
		return nil, fmt.Errorf("unmarshal: %w", err)
	}
	return &block, nil
}
