package bitcoin

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"strings"
	"testing"
	"time"

	"go.uber.org/fx"
	"google.golang.org/protobuf/proto"

	"github.com/coinbase/chainstorage/internal/blockchain/parser/internal"
	"github.com/coinbase/chainstorage/internal/utils/testapp"
	"github.com/coinbase/chainstorage/protos/coinbase/c3/common"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

// TestZcashLargeBlockBench runs a one-shot ParseBlock benchmark on a real
// pre-downloaded, pre-decompressed zcash block.
//
// Gated by env var CHAINSTORAGE_BENCH_ZCASH_BLOCK (path to the .pb file)
// so it does not run in the default `go test` sweep. Optional:
//
//	CHAINSTORAGE_BENCH_OPTS="skipScripts,skipWitnesses,hexAsBytes"
//
// The .pb file is the protobuf-serialized api.Block after zstd decompress
// — i.e. the shape the blob downloader's proto.Unmarshal expects.
//
// Example:
//
//	CHAINSTORAGE_BENCH_ZCASH_BLOCK=/tmp/zcash_bench/block_322102.pb \
//	go test -v -run=TestZcashLargeBlockBench -timeout=30m \
//	    ./internal/blockchain/parser/bitcoin/
func TestZcashLargeBlockBench(t *testing.T) {
	path := os.Getenv("CHAINSTORAGE_BENCH_ZCASH_BLOCK")
	if path == "" {
		t.Skip("set CHAINSTORAGE_BENCH_ZCASH_BLOCK to run")
	}

	var parser internal.NativeParser
	app := testapp.New(t,
		testapp.WithBlockchainNetwork(common.Blockchain_BLOCKCHAIN_ZCASH, common.Network_NETWORK_ZCASH_MAINNET),
		fx.Provide(NewZcashNativeParser),
		fx.Populate(&parser),
	)
	defer app.Close()

	t.Logf("loading %s", path)

	// Read + Unmarshal inside a closure so the compressed `data`
	// slice (multi-GB) goes out of scope before the GC below —
	// otherwise its reference stays live on the goroutine stack
	// and inflates the subsequent measurements.
	var rawBlock api.Block
	func() {
		t0 := time.Now()
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("ReadFile: %v", err)
		}
		t.Logf("read: %.2f MB in %s", mbf(len(data)), time.Since(t0))

		t0 = time.Now()
		if err := proto.Unmarshal(data, &rawBlock); err != nil {
			t.Fatalf("proto.Unmarshal: %v", err)
		}
		t.Logf("proto.Unmarshal api.Block envelope: %s", time.Since(t0))
	}()
	runtime.GC()
	runtime.GC()

	blob := rawBlock.GetBitcoin()
	if blob == nil {
		t.Fatalf("no bitcoin blobdata")
	}
	t.Logf("header JSON size: %.2f MB", mbf(len(blob.GetHeader())))
	t.Logf("input tx groups:  %d", len(blob.GetInputTransactions()))

	opts := buildBenchOpts(t, os.Getenv("CHAINSTORAGE_BENCH_OPTS"))

	ctx := context.Background()

	// ----- ParseBlock -----
	t.Logf("\n=== ParseBlock ===")
	runtime.GC()
	runtime.GC()
	before := heapAlloc()
	peak := newHeapPeak()
	peak.start()
	parseStart := time.Now()
	nb, err := parser.ParseBlock(ctx, &rawBlock, opts...)
	parseElapsed := time.Since(parseStart)
	peak.stop()
	if err != nil {
		t.Fatalf("ParseBlock: %v", err)
	}
	after := heapAlloc()
	t.Logf("  elapsed:              %s", parseElapsed)
	t.Logf("  tx count:             %d", nb.NumTransactions)
	t.Logf("  heap before:          %s", humanBytes(before))
	t.Logf("  heap after:           %s", humanBytes(after))
	t.Logf("  heap peak:            %s (+%s)", humanBytes(peak.peak), humanBytes(subU(peak.peak, before)))
	t.Logf("  persistent NB size:   %s", humanBytes(uint64(proto.Size(nb))))
}

func buildBenchOpts(t *testing.T, s string) []internal.ParseOption {
	if s == "" {
		return nil
	}
	var out []internal.ParseOption
	for _, name := range strings.Split(s, ",") {
		switch strings.TrimSpace(name) {
		case "":
		case "skipScripts":
			out = append(out, internal.WithSkipScripts())
		case "skipWitnesses":
			out = append(out, internal.WithSkipWitnesses())
		case "skipShielded":
			out = append(out, internal.WithSkipShielded())
		default:
			t.Fatalf("unknown bench opt %q", name)
		}
	}
	return out
}

func mbf(n int) float64 { return float64(n) / 1e6 }

func heapAlloc() uint64 {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	return ms.HeapAlloc
}

func subU(a, b uint64) uint64 {
	if a < b {
		return 0
	}
	return a - b
}

func humanBytes(b uint64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := uint64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.2f %cB", float64(b)/float64(div), "KMGTPE"[exp])
}

type heapPeakTracker struct {
	peak   uint64
	stopCh chan struct{}
	doneCh chan struct{}
}

func newHeapPeak() *heapPeakTracker { return &heapPeakTracker{} }

func (p *heapPeakTracker) start() {
	p.stopCh = make(chan struct{})
	p.doneCh = make(chan struct{})
	// Capture an initial snapshot so short ops that finish before the
	// first tick still record a baseline peak.
	if h := heapAlloc(); h > p.peak {
		p.peak = h
	}
	go func() {
		defer close(p.doneCh)
		// 5ms tick catches ~10ms+ operations cleanly; sub-ms blocks
		// may still read as baseline (fine — no peak to observe).
		tick := time.NewTicker(5 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-p.stopCh:
				return
			case <-tick.C:
				if h := heapAlloc(); h > p.peak {
					p.peak = h
				}
			}
		}
	}()
}

func (p *heapPeakTracker) stop() {
	close(p.stopCh)
	<-p.doneCh
	// Final snapshot so we don't miss a peak that hit between the
	// last tick and the stop signal.
	if h := heapAlloc(); h > p.peak {
		p.peak = h
	}
}
