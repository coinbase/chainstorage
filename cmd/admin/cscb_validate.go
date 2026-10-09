package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"github.com/spf13/cobra"
	"go.uber.org/fx"
	"golang.org/x/xerrors"
	"google.golang.org/protobuf/proto"

	"github.com/coinbase/chainstorage/internal/gateway"
	"github.com/coinbase/chainstorage/internal/storage/blobstorage/downloader"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

const (
	defaultCSCBValidationTimeout = 2 * time.Minute
	defaultCSCBRandomSeed        = int64(914)
)

var (
	cscbURLRegexp = regexp.MustCompile(`https?://[^\s,})]+`)

	cscbFlags struct {
		startHeight          uint64
		endHeight            uint64
		tag                  uint32
		chunkBlocks          uint64
		randomSamples        int
		randomSeed           int64
		iterations           int
		fullObjectIterations int
		timeout              time.Duration
		fallbackHeight       uint64
		disableFallbackCase  bool
		serverAddress        string
		grpc                 bool
		clientID             string
		reportJSON           string
		reportMarkdown       string
	}

	cscbCmd = &cobra.Command{
		Use:   "cscb",
		Short: "CSCB validation utilities",
	}

	cscbValidateCmd = &cobra.Command{
		Use:   "validate",
		Short: "Compare single-block and consolidated CSCB reads and write a validation report.",
		RunE:  runCSCBValidate,
	}
)

type (
	cscbValidationCase struct {
		Name           string   `json:"name"`
		Kind           string   `json:"kind"`
		StartHeight    uint64   `json:"start_height"`
		EndHeight      uint64   `json:"end_height"`
		Heights        []uint64 `json:"heights,omitempty"`
		ExpectFallback bool     `json:"expect_fallback"`
		Iterations     int      `json:"iterations"`
	}

	cscbValidationReport struct {
		GeneratedAt            string                     `json:"generated_at"`
		Command                string                     `json:"command"`
		Environment            string                     `json:"environment"`
		Blockchain             string                     `json:"blockchain"`
		Network                string                     `json:"network"`
		Tag                    uint32                     `json:"tag"`
		StartHeight            uint64                     `json:"start_height"`
		EndHeight              uint64                     `json:"end_height"`
		ChunkBlocks            uint64                     `json:"chunk_blocks"`
		RandomSeed             int64                      `json:"random_seed"`
		RandomSamples          int                        `json:"random_samples"`
		Iterations             int                        `json:"iterations"`
		FullObjectIterations   int                        `json:"full_object_iterations"`
		FallbackHeight         uint64                     `json:"fallback_height,omitempty"`
		ServerAddressOverride  string                     `json:"server_address_override,omitempty"`
		GRPCTransportOverride  bool                       `json:"grpc_transport_override"`
		ClientID               string                     `json:"client_id,omitempty"`
		Passed                 bool                       `json:"passed"`
		TotalCases             int                        `json:"total_cases"`
		PassedCases            int                        `json:"passed_cases"`
		FailedCases            int                        `json:"failed_cases"`
		CorrectnessMismatches  int                        `json:"correctness_mismatches"`
		ObservedFallbackBlocks int                        `json:"observed_fallback_blocks"`
		Cases                  []cscbValidationCaseResult `json:"cases"`
	}

	cscbValidationCaseResult struct {
		Name                          string            `json:"name"`
		Kind                          string            `json:"kind"`
		StartHeight                   uint64            `json:"start_height"`
		EndHeight                     uint64            `json:"end_height"`
		Heights                       []uint64          `json:"heights,omitempty"`
		Iterations                    int               `json:"iterations"`
		ExpectFallback                bool              `json:"expect_fallback"`
		ObservedFallback              bool              `json:"observed_fallback"`
		ObservedFallbackBlocks        int               `json:"observed_fallback_blocks"`
		Correct                       bool              `json:"correct"`
		Passed                        bool              `json:"passed"`
		Error                         string            `json:"error,omitempty"`
		MismatchHeights               []uint64          `json:"mismatch_heights,omitempty"`
		SingleBlock                   cscbSourceSummary `json:"single_block"`
		Consolidated                  cscbSourceSummary `json:"consolidated"`
		LatencyRatioConsolidated      float64           `json:"latency_ratio_consolidated_vs_single_block,omitempty"`
		BytesRatioConsolidated        float64           `json:"bytes_ratio_consolidated_vs_single_block,omitempty"`
		CanonicalHashesCompared       int               `json:"canonical_hashes_compared"`
		FirstCanonicalHashSingleBlock string            `json:"first_canonical_hash_single_block,omitempty"`
		FirstCanonicalHashCSCB        string            `json:"first_canonical_hash_consolidated,omitempty"`
	}

	cscbSourceSummary struct {
		Latency    cscbLatencySummary `json:"latency"`
		HTTP       cscbHTTPSummary    `json:"http"`
		Files      cscbFileSummary    `json:"files"`
		ErrorCount int                `json:"error_count"`
	}

	cscbLatencySummary struct {
		Samples int     `json:"samples"`
		P50Ms   float64 `json:"p50_ms"`
		P95Ms   float64 `json:"p95_ms"`
		MinMs   float64 `json:"min_ms"`
		MaxMs   float64 `json:"max_ms"`
	}

	cscbHTTPSummary struct {
		Requests int64 `json:"requests"`
		Bytes    int64 `json:"bytes"`
	}

	cscbFileSummary struct {
		Count                     int      `json:"count"`
		SingleBlockObjectCount    int      `json:"single_block_object_count"`
		ConsolidatedObjectCount   int      `json:"consolidated_object_count"`
		SkippedCount              int      `json:"skipped_count"`
		MetadataByteLength        uint64   `json:"metadata_byte_length"`
		MetadataUncompressedBytes uint64   `json:"metadata_uncompressed_bytes"`
		Formats                   []string `json:"formats"`
		FirstHeight               uint64   `json:"first_height,omitempty"`
		LastHeight                uint64   `json:"last_height,omitempty"`
	}

	cscbFetchResult struct {
		files    []*api.BlockFile
		digests  []cscbBlockDigest
		duration time.Duration
		http     cscbHTTPSnapshot
		err      error
	}

	cscbBlockDigest struct {
		height uint64
		hash   string
	}

	cscbHTTPSnapshot struct {
		requests int64
		bytes    int64
	}

	cscbCountingHTTPClient struct {
		base     downloader.HTTPClient
		requests atomic.Int64
		bytes    atomic.Int64
	}

	cscbCountingReadCloser struct {
		body   io.ReadCloser
		client *cscbCountingHTTPClient
	}

	cscbValidator struct {
		client     gateway.Client
		downloader downloader.BlockDownloader
		counter    *cscbCountingHTTPClient
		tag        uint32
		timeout    time.Duration
	}
)

func init() {
	cscbValidateCmd.Flags().Uint64Var(&cscbFlags.startHeight, "start-height", 0, "inclusive start height for the CSCB canary object")
	cscbValidateCmd.Flags().Uint64Var(&cscbFlags.endHeight, "end-height", 0, "exclusive end height for the CSCB canary object")
	cscbValidateCmd.Flags().Uint32Var(&cscbFlags.tag, "tag", 0, "block tag; 0 uses chainstorage effective stable tag")
	cscbValidateCmd.Flags().Uint64Var(&cscbFlags.chunkBlocks, "chunk-blocks", 25, "expected CSCB compression chunk size in blocks")
	cscbValidateCmd.Flags().IntVar(&cscbFlags.randomSamples, "random-samples", 5, "number of deterministic random single-block samples")
	cscbValidateCmd.Flags().Int64Var(&cscbFlags.randomSeed, "random-seed", defaultCSCBRandomSeed, "random seed for single-block sample selection")
	cscbValidateCmd.Flags().IntVar(&cscbFlags.iterations, "iterations", 3, "iterations for non-full-object cases")
	cscbValidateCmd.Flags().IntVar(&cscbFlags.fullObjectIterations, "full-object-iterations", 1, "iterations for the full-object range case")
	cscbValidateCmd.Flags().DurationVar(&cscbFlags.timeout, "timeout", defaultCSCBValidationTimeout, "timeout for each source read")
	cscbValidateCmd.Flags().Uint64Var(&cscbFlags.fallbackHeight, "fallback-height", 0, "height expected to miss consolidated metadata; defaults to start-height - 1 when possible")
	cscbValidateCmd.Flags().BoolVar(&cscbFlags.disableFallbackCase, "disable-fallback-case", false, "skip the consolidated miss/fallback case")
	cscbValidateCmd.Flags().StringVar(&cscbFlags.serverAddress, "server-address", "", "override Chainstorage SDK server address for this run")
	cscbValidateCmd.Flags().BoolVar(&cscbFlags.grpc, "grpc", false, "force gRPC SDK transport; useful with a verified localhost port-forward")
	cscbValidateCmd.Flags().StringVar(&cscbFlags.clientID, "client-id", "INF-914-cscb-dual-read-validation", "client ID header for server-side metrics/logs")
	cscbValidateCmd.Flags().StringVar(&cscbFlags.reportJSON, "report-json", "", "write JSON report to this path")
	cscbValidateCmd.Flags().StringVar(&cscbFlags.reportMarkdown, "report-md", "", "write Markdown report to this path")

	if err := cscbValidateCmd.MarkFlagRequired("start-height"); err != nil {
		panic(err)
	}
	if err := cscbValidateCmd.MarkFlagRequired("end-height"); err != nil {
		panic(err)
	}

	cscbCmd.AddCommand(cscbValidateCmd)
	rootCmd.AddCommand(cscbCmd)
}

func runCSCBValidate(cmd *cobra.Command, args []string) error {
	if cscbFlags.startHeight >= cscbFlags.endHeight {
		return xerrors.New("start-height must be less than end-height")
	}
	if cscbFlags.chunkBlocks == 0 {
		return xerrors.New("chunk-blocks must be positive")
	}
	if cscbFlags.randomSamples < 0 {
		return xerrors.New("random-samples cannot be negative")
	}
	if cscbFlags.iterations <= 0 {
		return xerrors.New("iterations must be positive")
	}
	if cscbFlags.fullObjectIterations <= 0 {
		return xerrors.New("full-object-iterations must be positive")
	}
	if cscbFlags.timeout <= 0 {
		return xerrors.New("timeout must be positive")
	}
	if cscbFlags.serverAddress != "" {
		if err := os.Setenv("CHAINSTORAGE_SDK_CHAINSTORAGE_ADDRESS", cscbFlags.serverAddress); err != nil {
			return xerrors.Errorf("failed to set server address override: %w", err)
		}
	}
	if cscbFlags.grpc {
		if err := os.Setenv("CHAINSTORAGE_SDK_RESTFUL", "false"); err != nil {
			return xerrors.Errorf("failed to set gRPC transport override: %w", err)
		}
	}

	counter := newCSCBCountingHTTPClient(downloader.NewHTTPClient())
	var deps struct {
		fx.In
		Client     gateway.Client
		Downloader downloader.BlockDownloader
	}
	app := startApp(
		fx.Provide(func() downloader.HTTPClient { return counter }),
		fx.Provide(downloader.NewBlockDownloader),
		gateway.WithClientID(cscbFlags.clientID),
		fx.Populate(&deps),
	)
	defer app.Close()

	tag := app.Config().GetEffectiveBlockTag(cscbFlags.tag)
	validator := &cscbValidator{
		client:     deps.Client,
		downloader: deps.Downloader,
		counter:    counter,
		tag:        tag,
		timeout:    cscbFlags.timeout,
	}
	report, validationErr := validator.run(cmd.Context(), cscbValidationRunConfig{
		startHeight:          cscbFlags.startHeight,
		endHeight:            cscbFlags.endHeight,
		chunkBlocks:          cscbFlags.chunkBlocks,
		randomSamples:        cscbFlags.randomSamples,
		randomSeed:           cscbFlags.randomSeed,
		iterations:           cscbFlags.iterations,
		fullObjectIterations: cscbFlags.fullObjectIterations,
		fallbackHeight:       cscbFlags.fallbackHeight,
		disableFallbackCase:  cscbFlags.disableFallbackCase,
		serverAddress:        cscbFlags.serverAddress,
		grpc:                 cscbFlags.grpc,
		clientID:             cscbFlags.clientID,
	})
	report.Command = strings.Join(os.Args, " ")
	report.Environment = string(env)
	report.Blockchain = blockchain.String()
	report.Network = network.String()

	if cscbFlags.reportJSON != "" {
		if err := writeCSCBJSONReport(cscbFlags.reportJSON, report); err != nil {
			return err
		}
	}
	if cscbFlags.reportMarkdown != "" {
		if err := writeCSCBMarkdownReport(cscbFlags.reportMarkdown, report); err != nil {
			return err
		}
	}
	if validationErr != nil {
		return validationErr
	}
	if !report.Passed {
		return xerrors.Errorf("CSCB validation failed: %d failed cases, %d mismatch heights", report.FailedCases, report.CorrectnessMismatches)
	}
	return nil
}

type cscbValidationRunConfig struct {
	startHeight          uint64
	endHeight            uint64
	chunkBlocks          uint64
	randomSamples        int
	randomSeed           int64
	iterations           int
	fullObjectIterations int
	fallbackHeight       uint64
	disableFallbackCase  bool
	serverAddress        string
	grpc                 bool
	clientID             string
}

func (v *cscbValidator) run(ctx context.Context, cfg cscbValidationRunConfig) (*cscbValidationReport, error) {
	fallbackHeight := cfg.fallbackHeight
	if fallbackHeight == 0 && cfg.startHeight > 0 {
		fallbackHeight = cfg.startHeight - 1
	}
	cases := buildCSCBValidationCases(cfg, fallbackHeight)

	report := &cscbValidationReport{
		GeneratedAt:           time.Now().UTC().Format(time.RFC3339),
		Tag:                   v.tag,
		StartHeight:           cfg.startHeight,
		EndHeight:             cfg.endHeight,
		ChunkBlocks:           cfg.chunkBlocks,
		RandomSeed:            cfg.randomSeed,
		RandomSamples:         cfg.randomSamples,
		Iterations:            cfg.iterations,
		FullObjectIterations:  cfg.fullObjectIterations,
		FallbackHeight:        fallbackHeight,
		ServerAddressOverride: cfg.serverAddress,
		GRPCTransportOverride: cfg.grpc,
		ClientID:              cfg.clientID,
		TotalCases:            len(cases),
	}

	for _, validationCase := range cases {
		result := v.runCase(ctx, validationCase)
		report.Cases = append(report.Cases, result)
		if result.Passed {
			report.PassedCases++
		} else {
			report.FailedCases++
		}
		report.CorrectnessMismatches += len(result.MismatchHeights)
		report.ObservedFallbackBlocks += result.ObservedFallbackBlocks
	}
	report.Passed = report.FailedCases == 0
	return report, nil
}

func buildCSCBValidationCases(cfg cscbValidationRunConfig, fallbackHeight uint64) []cscbValidationCase {
	cases := []cscbValidationCase{
		{
			Name:        "single_first_block",
			Kind:        "single",
			StartHeight: cfg.startHeight,
			EndHeight:   cfg.startHeight + 1,
			Heights:     []uint64{cfg.startHeight},
			Iterations:  cfg.iterations,
		},
		{
			Name:        "single_last_block",
			Kind:        "single",
			StartHeight: cfg.endHeight - 1,
			EndHeight:   cfg.endHeight,
			Heights:     []uint64{cfg.endHeight - 1},
			Iterations:  cfg.iterations,
		},
	}

	for _, height := range chunkBoundaryHeights(cfg.startHeight, cfg.endHeight, cfg.chunkBlocks) {
		cases = append(cases, cscbValidationCase{
			Name:        fmt.Sprintf("single_chunk_boundary_%d", height),
			Kind:        "single",
			StartHeight: height,
			EndHeight:   height + 1,
			Heights:     []uint64{height},
			Iterations:  cfg.iterations,
		})
	}

	for _, height := range randomSampleHeights(cfg.startHeight, cfg.endHeight, cfg.randomSamples, cfg.randomSeed) {
		cases = append(cases, cscbValidationCase{
			Name:        fmt.Sprintf("single_random_%d", height),
			Kind:        "single",
			StartHeight: height,
			EndHeight:   height + 1,
			Heights:     []uint64{height},
			Iterations:  cfg.iterations,
		})
	}

	withinStart := boundedHeight(cfg.startHeight+5, cfg.startHeight, cfg.endHeight-1)
	withinEnd := boundedHeight(withinStart+10, withinStart+1, cfg.endHeight)
	cases = append(cases, cscbValidationCase{
		Name:        "range_within_one_chunk",
		Kind:        "range",
		StartHeight: withinStart,
		EndHeight:   withinEnd,
		Iterations:  cfg.iterations,
	})

	boundary := cfg.startHeight + cfg.chunkBlocks
	if boundary <= cfg.startHeight || boundary >= cfg.endHeight {
		boundary = cfg.startHeight + (cfg.endHeight-cfg.startHeight)/2
	}
	acrossStart := boundedHeight(boundary-5, cfg.startHeight, cfg.endHeight-1)
	acrossEnd := boundedHeight(boundary+5, acrossStart+1, cfg.endHeight)
	cases = append(cases, cscbValidationCase{
		Name:        "range_across_chunk_boundary",
		Kind:        "range",
		StartHeight: acrossStart,
		EndHeight:   acrossEnd,
		Iterations:  cfg.iterations,
	})

	twoChunkEnd := boundedHeight(cfg.startHeight+2*cfg.chunkBlocks, cfg.startHeight+1, cfg.endHeight)
	cases = append(cases, cscbValidationCase{
		Name:        "range_two_chunks",
		Kind:        "range",
		StartHeight: cfg.startHeight,
		EndHeight:   twoChunkEnd,
		Iterations:  cfg.iterations,
	})

	cases = append(cases, cscbValidationCase{
		Name:        "range_full_object",
		Kind:        "range",
		StartHeight: cfg.startHeight,
		EndHeight:   cfg.endHeight,
		Iterations:  cfg.fullObjectIterations,
	})

	if !cfg.disableFallbackCase && fallbackHeight > 0 {
		cases = append(cases, cscbValidationCase{
			Name:           "single_consolidated_miss_fallback",
			Kind:           "single",
			StartHeight:    fallbackHeight,
			EndHeight:      fallbackHeight + 1,
			Heights:        []uint64{fallbackHeight},
			ExpectFallback: true,
			Iterations:     cfg.iterations,
		})
	}

	return cases
}

func chunkBoundaryHeights(startHeight uint64, endHeight uint64, chunkBlocks uint64) []uint64 {
	if chunkBlocks == 0 || endHeight <= startHeight+1 {
		return nil
	}
	candidates := make([]uint64, 0, 6)
	for boundary := startHeight + chunkBlocks; boundary < endHeight && len(candidates) < 6; boundary += chunkBlocks {
		if boundary > startHeight {
			candidates = append(candidates, boundary-1)
		}
		candidates = append(candidates, boundary)
	}
	return uniqueSortedHeights(candidates, startHeight, endHeight)
}

func randomSampleHeights(startHeight uint64, endHeight uint64, samples int, seed int64) []uint64 {
	if samples <= 0 || endHeight <= startHeight {
		return nil
	}
	span := endHeight - startHeight
	if uint64(samples) >= span {
		heights := make([]uint64, 0, span)
		for height := startHeight; height < endHeight; height++ {
			heights = append(heights, height)
		}
		return heights
	}

	rng := rand.New(rand.NewSource(seed))
	seen := make(map[uint64]struct{}, samples)
	for len(seen) < samples {
		height := startHeight + uint64(rng.Int63n(int64(span)))
		seen[height] = struct{}{}
	}

	heights := make([]uint64, 0, len(seen))
	for height := range seen {
		heights = append(heights, height)
	}
	sort.Slice(heights, func(i int, j int) bool { return heights[i] < heights[j] })
	return heights
}

func uniqueSortedHeights(heights []uint64, startHeight uint64, endHeight uint64) []uint64 {
	seen := make(map[uint64]struct{}, len(heights))
	for _, height := range heights {
		if height < startHeight || height >= endHeight {
			continue
		}
		seen[height] = struct{}{}
	}
	out := make([]uint64, 0, len(seen))
	for height := range seen {
		out = append(out, height)
	}
	sort.Slice(out, func(i int, j int) bool { return out[i] < out[j] })
	return out
}

func boundedHeight(value uint64, min uint64, max uint64) uint64 {
	if value < min {
		return min
	}
	if value > max {
		return max
	}
	return value
}

func (v *cscbValidator) runCase(ctx context.Context, validationCase cscbValidationCase) cscbValidationCaseResult {
	result := cscbValidationCaseResult{
		Name:           validationCase.Name,
		Kind:           validationCase.Kind,
		StartHeight:    validationCase.StartHeight,
		EndHeight:      validationCase.EndHeight,
		Heights:        validationCase.Heights,
		Iterations:     validationCase.Iterations,
		ExpectFallback: validationCase.ExpectFallback,
		Correct:        true,
		Passed:         true,
	}

	var singleBlockDurations []time.Duration
	var consolidatedDurations []time.Duration
	var singleBlockRequests int64
	var singleBlockBytes int64
	var consolidatedRequests int64
	var consolidatedBytes int64

	for i := 0; i < validationCase.Iterations; i++ {
		singleBlock := v.fetch(ctx, validationCase, api.BlockReadSource_BLOCK_READ_SOURCE_SINGLE_BLOCK)
		consolidated := v.fetch(ctx, validationCase, api.BlockReadSource_BLOCK_READ_SOURCE_CONSOLIDATED)

		if singleBlock.err != nil {
			result.SingleBlock.ErrorCount++
			result.Error = appendResultError(result.Error, "single-block", singleBlock.err)
		} else {
			singleBlockDurations = append(singleBlockDurations, singleBlock.duration)
			singleBlockRequests += singleBlock.http.requests
			singleBlockBytes += singleBlock.http.bytes
			if result.SingleBlock.Files.Count == 0 {
				result.SingleBlock.Files = summarizeCSCBFiles(singleBlock.files)
			}
		}

		if consolidated.err != nil {
			result.Consolidated.ErrorCount++
			result.Error = appendResultError(result.Error, "consolidated", consolidated.err)
		} else {
			consolidatedDurations = append(consolidatedDurations, consolidated.duration)
			consolidatedRequests += consolidated.http.requests
			consolidatedBytes += consolidated.http.bytes
			if result.Consolidated.Files.Count == 0 {
				result.Consolidated.Files = summarizeCSCBFiles(consolidated.files)
			}
		}

		if singleBlock.err != nil || consolidated.err != nil {
			result.Correct = false
			result.Passed = false
			continue
		}

		comparison := compareCSCBDigests(singleBlock.digests, consolidated.digests)
		if !comparison.correct {
			result.Correct = false
			result.Passed = false
			result.MismatchHeights = appendUnique(result.MismatchHeights, comparison.mismatchHeights...)
		}
		if result.CanonicalHashesCompared == 0 && comparison.hashesCompared > 0 {
			result.CanonicalHashesCompared = comparison.hashesCompared
			result.FirstCanonicalHashSingleBlock = comparison.firstSingleBlockHash
			result.FirstCanonicalHashCSCB = comparison.firstConsolidatedHash
		}
	}

	result.SingleBlock.Latency = summarizeDurations(singleBlockDurations)
	result.Consolidated.Latency = summarizeDurations(consolidatedDurations)
	result.SingleBlock.HTTP = cscbHTTPSummary{Requests: singleBlockRequests, Bytes: singleBlockBytes}
	result.Consolidated.HTTP = cscbHTTPSummary{Requests: consolidatedRequests, Bytes: consolidatedBytes}
	result.ObservedFallbackBlocks = result.Consolidated.Files.SingleBlockObjectCount
	result.ObservedFallback = result.ObservedFallbackBlocks > 0
	if validationCase.ExpectFallback && !result.ObservedFallback {
		result.Passed = false
		result.Error = appendResultErrorString(result.Error, "expected consolidated read to fall back to single-block metadata")
	}
	if !validationCase.ExpectFallback && result.ObservedFallback {
		result.Passed = false
		result.Error = appendResultErrorString(result.Error, "unexpected consolidated fallback to single-block metadata")
	}
	result.LatencyRatioConsolidated = ratio(result.Consolidated.Latency.P50Ms, result.SingleBlock.Latency.P50Ms)
	result.BytesRatioConsolidated = ratio(float64(result.Consolidated.HTTP.Bytes), float64(result.SingleBlock.HTTP.Bytes))
	return result
}

func (v *cscbValidator) fetch(ctx context.Context, validationCase cscbValidationCase, readSource api.BlockReadSource) cscbFetchResult {
	ctx, cancel := context.WithTimeout(ctx, v.timeout)
	defer cancel()

	v.counter.Reset()
	start := time.Now()
	files, err := v.getFiles(ctx, validationCase, readSource)
	if err != nil {
		return cscbFetchResult{duration: time.Since(start), http: v.counter.Snapshot(), err: err}
	}
	digests, err := v.hashFiles(ctx, files)
	return cscbFetchResult{
		files:    files,
		digests:  digests,
		duration: time.Since(start),
		http:     v.counter.Snapshot(),
		err:      err,
	}
}

func (v *cscbValidator) getFiles(ctx context.Context, validationCase cscbValidationCase, readSource api.BlockReadSource) ([]*api.BlockFile, error) {
	if validationCase.Kind == "single" {
		resp, err := v.client.GetBlockFile(ctx, &api.GetBlockFileRequest{
			Tag:        v.tag,
			Height:     validationCase.StartHeight,
			ReadSource: readSource,
		})
		if err != nil {
			return nil, xerrors.Errorf("GetBlockFile failed: %w", err)
		}
		if resp.GetFile() == nil {
			return nil, xerrors.New("GetBlockFile returned nil file")
		}
		return []*api.BlockFile{resp.GetFile()}, nil
	}

	resp, err := v.client.GetBlockFilesByRange(ctx, &api.GetBlockFilesByRangeRequest{
		Tag:         v.tag,
		StartHeight: validationCase.StartHeight,
		EndHeight:   validationCase.EndHeight,
		ReadSource:  readSource,
	})
	if err != nil {
		return nil, xerrors.Errorf("GetBlockFilesByRange failed: %w", err)
	}
	if len(resp.GetFiles()) == 0 {
		return nil, xerrors.New("GetBlockFilesByRange returned no files")
	}
	return resp.GetFiles(), nil
}

func (v *cscbValidator) hashFiles(ctx context.Context, files []*api.BlockFile) (_ []cscbBlockDigest, retErr error) {
	iter, err := v.downloader.OpenRawBlockPayloads(ctx, files)
	if err != nil {
		return nil, xerrors.Errorf("OpenRawBlockPayloads failed: %w", err)
	}
	defer func() {
		if closeErr := iter.Close(); retErr == nil && closeErr != nil {
			retErr = xerrors.Errorf("close raw block payload iterator failed: %w", closeErr)
		}
	}()

	digests := make([]cscbBlockDigest, 0, len(files))
	for {
		payload, err := iter.Next(ctx)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, xerrors.Errorf("read raw block payload failed: %w", err)
		}

		digest, digestErr := canonicalRawPayloadHash(payload)
		var closeErr error
		if payload != nil {
			closeErr = payload.Close()
		}
		if digestErr != nil {
			return nil, digestErr
		}
		if closeErr != nil {
			return nil, xerrors.Errorf("close raw block payload failed: %w", closeErr)
		}
		digests = append(digests, digest)
	}
	return digests, nil
}

type cscbBlockComparison struct {
	correct               bool
	mismatchHeights       []uint64
	hashesCompared        int
	firstSingleBlockHash  string
	firstConsolidatedHash string
}

func compareCSCBBlocks(singleBlock []*api.Block, consolidated []*api.Block) cscbBlockComparison {
	comparison := cscbBlockComparison{correct: true}
	if len(singleBlock) != len(consolidated) {
		comparison.correct = false
		comparison.mismatchHeights = append(comparison.mismatchHeights, collectBlockHeights(singleBlock)...)
		comparison.mismatchHeights = append(comparison.mismatchHeights, collectBlockHeights(consolidated)...)
		comparison.mismatchHeights = uniqueSortedHeights(comparison.mismatchHeights, 0, ^uint64(0))
		return comparison
	}
	for i := range singleBlock {
		singleBlockHash, singleBlockErr := canonicalBlockHash(singleBlock[i])
		consolidatedHash, consolidatedErr := canonicalBlockHash(consolidated[i])
		height := firstNonZeroHeight(singleBlock[i], consolidated[i])
		if comparison.hashesCompared == 0 {
			comparison.firstSingleBlockHash = singleBlockHash
			comparison.firstConsolidatedHash = consolidatedHash
		}
		comparison.hashesCompared++
		if singleBlockErr != nil || consolidatedErr != nil || singleBlockHash != consolidatedHash {
			comparison.correct = false
			comparison.mismatchHeights = append(comparison.mismatchHeights, height)
		}
	}
	comparison.mismatchHeights = uniqueSortedHeights(comparison.mismatchHeights, 0, ^uint64(0))
	return comparison
}

func compareCSCBDigests(singleBlock []cscbBlockDigest, consolidated []cscbBlockDigest) cscbBlockComparison {
	comparison := cscbBlockComparison{correct: true}
	if len(singleBlock) != len(consolidated) {
		comparison.correct = false
		comparison.mismatchHeights = append(comparison.mismatchHeights, collectDigestHeights(singleBlock)...)
		comparison.mismatchHeights = append(comparison.mismatchHeights, collectDigestHeights(consolidated)...)
		comparison.mismatchHeights = uniqueSortedHeights(comparison.mismatchHeights, 0, ^uint64(0))
		return comparison
	}

	for i := range singleBlock {
		if comparison.hashesCompared == 0 {
			comparison.firstSingleBlockHash = singleBlock[i].hash
			comparison.firstConsolidatedHash = consolidated[i].hash
		}
		comparison.hashesCompared++

		if singleBlock[i].height != consolidated[i].height || singleBlock[i].hash != consolidated[i].hash {
			comparison.correct = false
			comparison.mismatchHeights = append(comparison.mismatchHeights, singleBlock[i].height, consolidated[i].height)
		}
	}
	comparison.mismatchHeights = uniqueSortedHeights(comparison.mismatchHeights, 0, ^uint64(0))
	return comparison
}

func collectDigestHeights(digests []cscbBlockDigest) []uint64 {
	heights := make([]uint64, 0, len(digests))
	for _, digest := range digests {
		heights = append(heights, digest.height)
	}
	return heights
}

func collectBlockHeights(blocks []*api.Block) []uint64 {
	heights := make([]uint64, 0, len(blocks))
	for _, block := range blocks {
		if height := firstNonZeroHeight(block); height > 0 {
			heights = append(heights, height)
		}
	}
	return heights
}

func firstNonZeroHeight(blocks ...*api.Block) uint64 {
	for _, block := range blocks {
		if block == nil || block.GetMetadata() == nil {
			continue
		}
		if height := block.GetMetadata().GetHeight(); height > 0 {
			return height
		}
	}
	return 0
}

func canonicalBlockHash(block *api.Block) (string, error) {
	if block == nil {
		return "", xerrors.New("nil block")
	}
	if block.Metadata != nil {
		originalObjectKeyMain := block.Metadata.ObjectKeyMain
		originalObjectFormat := block.Metadata.ObjectFormat
		originalByteOffset := block.Metadata.ByteOffset
		originalByteLength := block.Metadata.ByteLength
		originalUncompressedLength := block.Metadata.UncompressedLength

		block.Metadata.ObjectKeyMain = ""
		block.Metadata.ObjectFormat = api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_SINGLE_BLOCK
		block.Metadata.ByteOffset = 0
		block.Metadata.ByteLength = 0
		block.Metadata.UncompressedLength = 0

		defer func() {
			block.Metadata.ObjectKeyMain = originalObjectKeyMain
			block.Metadata.ObjectFormat = originalObjectFormat
			block.Metadata.ByteOffset = originalByteOffset
			block.Metadata.ByteLength = originalByteLength
			block.Metadata.UncompressedLength = originalUncompressedLength
		}()
	}
	payload, err := proto.MarshalOptions{Deterministic: true}.Marshal(block)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:]), nil
}

func canonicalRawPayloadHash(payload *downloader.RawBlockPayload) (cscbBlockDigest, error) {
	if payload == nil || payload.BlockFile == nil {
		return cscbBlockDigest{}, xerrors.New("nil raw block payload")
	}
	data, err := io.ReadAll(payload)
	if err != nil {
		return cscbBlockDigest{}, xerrors.Errorf("read raw block payload for height %d failed: %w", payload.BlockFile.GetHeight(), err)
	}
	hash, err := canonicalRawBlockDataHash(payload.BlockFile, data)
	if err != nil {
		return cscbBlockDigest{}, err
	}
	return cscbBlockDigest{height: payload.BlockFile.GetHeight(), hash: hash}, nil
}

func canonicalRawBlockDataHash(blockFile *api.BlockFile, data []byte) (string, error) {
	if blockFile == nil {
		return "", xerrors.New("nil block file")
	}
	if blockFile.GetSkipped() {
		return canonicalBlockHash(&api.Block{
			Metadata: &api.BlockMetadata{
				Tag:     blockFile.GetTag(),
				Height:  blockFile.GetHeight(),
				Skipped: true,
			},
		})
	}

	var block api.Block
	if err := proto.Unmarshal(data, &block); err != nil {
		return "", xerrors.Errorf("unmarshal raw block payload for height %d failed: %w", blockFile.GetHeight(), err)
	}
	if isCSCBFile(blockFile) {
		block.Metadata = cscbMetadataFromBlockFile(blockFile)
	}
	return canonicalBlockHash(&block)
}

func cscbMetadataFromBlockFile(blockFile *api.BlockFile) *api.BlockMetadata {
	return &api.BlockMetadata{
		Tag:                blockFile.GetTag(),
		Hash:               blockFile.GetHash(),
		ParentHash:         blockFile.GetParentHash(),
		Height:             blockFile.GetHeight(),
		ParentHeight:       blockFile.GetParentHeight(),
		Skipped:            blockFile.GetSkipped(),
		Timestamp:          blockFile.GetBlockTimestamp(),
		ObjectFormat:       blockFile.GetObjectFormat(),
		ByteOffset:         blockFile.GetByteOffset(),
		ByteLength:         blockFile.GetByteLength(),
		UncompressedLength: blockFile.GetUncompressedLength(),
	}
}

func summarizeDurations(durations []time.Duration) cscbLatencySummary {
	if len(durations) == 0 {
		return cscbLatencySummary{}
	}
	sorted := append([]time.Duration(nil), durations...)
	sort.Slice(sorted, func(i int, j int) bool { return sorted[i] < sorted[j] })
	return cscbLatencySummary{
		Samples: len(sorted),
		P50Ms:   durationMillis(percentileDuration(sorted, 0.50)),
		P95Ms:   durationMillis(percentileDuration(sorted, 0.95)),
		MinMs:   durationMillis(sorted[0]),
		MaxMs:   durationMillis(sorted[len(sorted)-1]),
	}
}

func percentileDuration(sorted []time.Duration, percentile float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	if percentile <= 0 {
		return sorted[0]
	}
	if percentile >= 1 {
		return sorted[len(sorted)-1]
	}
	index := int(math.Ceil(percentile*float64(len(sorted)))) - 1
	if index < 0 {
		index = 0
	}
	if index >= len(sorted) {
		index = len(sorted) - 1
	}
	return sorted[index]
}

func durationMillis(duration time.Duration) float64 {
	return float64(duration.Microseconds()) / 1000
}

func summarizeCSCBFiles(files []*api.BlockFile) cscbFileSummary {
	summary := cscbFileSummary{Count: len(files)}
	formats := make(map[string]struct{})
	for i, file := range files {
		if i == 0 {
			summary.FirstHeight = file.GetHeight()
		}
		summary.LastHeight = file.GetHeight()
		if file.GetSkipped() {
			summary.SkippedCount++
		} else if isCSCBFile(file) {
			summary.ConsolidatedObjectCount++
			formats[cscbBlockObjectFormatName(file.GetObjectFormat())] = struct{}{}
		} else {
			summary.SingleBlockObjectCount++
			formats[cscbBlockObjectFormatName(file.GetObjectFormat())] = struct{}{}
		}
		summary.MetadataByteLength += file.GetByteLength()
		summary.MetadataUncompressedBytes += file.GetUncompressedLength()
	}
	for format := range formats {
		summary.Formats = append(summary.Formats, format)
	}
	sort.Strings(summary.Formats)
	return summary
}

func cscbBlockObjectFormatName(format api.BlockObjectFormat) string {
	if format == api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_SINGLE_BLOCK {
		return "BLOCK_OBJECT_FORMAT_SINGLE_BLOCK"
	}
	return format.String()
}

func isCSCBFile(file *api.BlockFile) bool {
	return file.GetObjectFormat() == api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_CSCB_BATCH && file.GetByteLength() > 0
}

func appendUnique(existing []uint64, additions ...uint64) []uint64 {
	seen := make(map[uint64]struct{}, len(existing)+len(additions))
	for _, value := range existing {
		seen[value] = struct{}{}
	}
	for _, value := range additions {
		seen[value] = struct{}{}
	}
	out := make([]uint64, 0, len(seen))
	for value := range seen {
		out = append(out, value)
	}
	sort.Slice(out, func(i int, j int) bool { return out[i] < out[j] })
	return out
}

func appendResultError(existing string, source string, err error) string {
	return appendResultErrorString(existing, fmt.Sprintf("%s: %s", source, sanitizeCSCBError(err)))
}

func appendResultErrorString(existing string, message string) string {
	if existing == "" {
		return message
	}
	return existing + "; " + message
}

func sanitizeCSCBError(err error) string {
	if err == nil {
		return ""
	}
	return sanitizeCSCBString(err.Error())
}

func sanitizeCSCBString(value string) string {
	return cscbURLRegexp.ReplaceAllString(value, "<redacted-url>")
}

func ratio(numerator float64, denominator float64) float64 {
	if denominator == 0 {
		return 0
	}
	return numerator / denominator
}

func newCSCBCountingHTTPClient(base downloader.HTTPClient) *cscbCountingHTTPClient {
	return &cscbCountingHTTPClient{base: base}
}

func (c *cscbCountingHTTPClient) Do(req *http.Request) (*http.Response, error) {
	c.requests.Add(1)
	resp, err := c.base.Do(req)
	if err != nil || resp == nil || resp.Body == nil {
		return resp, err
	}
	resp.Body = &cscbCountingReadCloser{body: resp.Body, client: c}
	return resp, nil
}

func (c *cscbCountingHTTPClient) Reset() {
	c.requests.Store(0)
	c.bytes.Store(0)
}

func (c *cscbCountingHTTPClient) Snapshot() cscbHTTPSnapshot {
	return cscbHTTPSnapshot{
		requests: c.requests.Load(),
		bytes:    c.bytes.Load(),
	}
}

func (r *cscbCountingReadCloser) Read(p []byte) (int, error) {
	n, err := r.body.Read(p)
	if n > 0 {
		r.client.bytes.Add(int64(n))
	}
	return n, err
}

func (r *cscbCountingReadCloser) Close() error {
	return r.body.Close()
}

func writeCSCBJSONReport(path string, report *cscbValidationReport) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return xerrors.Errorf("failed to create report directory: %w", err)
	}
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return xerrors.Errorf("failed to marshal JSON report: %w", err)
	}
	data = append(data, '\n')
	if err := os.WriteFile(path, data, 0o644); err != nil {
		return xerrors.Errorf("failed to write JSON report: %w", err)
	}
	return nil
}

func writeCSCBMarkdownReport(path string, report *cscbValidationReport) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return xerrors.Errorf("failed to create report directory: %w", err)
	}
	var b strings.Builder
	fmt.Fprintf(&b, "# CSCB Dual-Read Validation Report\n\n")
	fmt.Fprintf(&b, "- generated_at: `%s`\n", report.GeneratedAt)
	fmt.Fprintf(&b, "- target: `%s/%s/%s`\n", report.Environment, report.Blockchain, report.Network)
	fmt.Fprintf(&b, "- tag: `%d`\n", report.Tag)
	fmt.Fprintf(&b, "- canary_range: `[%d, %d)`\n", report.StartHeight, report.EndHeight)
	fmt.Fprintf(&b, "- chunk_blocks: `%d`\n", report.ChunkBlocks)
	if report.ServerAddressOverride != "" {
		fmt.Fprintf(&b, "- server_address_override: `%s`\n", report.ServerAddressOverride)
	}
	fmt.Fprintf(&b, "- grpc_transport_override: `%t`\n", report.GRPCTransportOverride)
	if report.ClientID != "" {
		fmt.Fprintf(&b, "- client_id: `%s`\n", report.ClientID)
	}
	fmt.Fprintf(&b, "- passed: `%t`\n", report.Passed)
	fmt.Fprintf(&b, "- cases: `%d passed / %d total`\n", report.PassedCases, report.TotalCases)
	fmt.Fprintf(&b, "- mismatch_heights: `%d`\n", report.CorrectnessMismatches)
	fmt.Fprintf(&b, "- observed_fallback_blocks: `%d`\n\n", report.ObservedFallbackBlocks)

	fmt.Fprintf(&b, "## Case Results\n\n")
	fmt.Fprintf(&b, "| case | range | pass | fallback | single-block p50/p95 ms | consolidated p50/p95 ms | single-block bytes | consolidated bytes | mismatches |\n")
	fmt.Fprintf(&b, "|---|---:|---:|---:|---:|---:|---:|---:|---:|\n")
	for _, c := range report.Cases {
		fmt.Fprintf(
			&b,
			"| %s | `[%d,%d)` | %t | %t | %.2f / %.2f | %.2f / %.2f | %d | %d | %s |\n",
			c.Name,
			c.StartHeight,
			c.EndHeight,
			c.Passed,
			c.ObservedFallback,
			c.SingleBlock.Latency.P50Ms,
			c.SingleBlock.Latency.P95Ms,
			c.Consolidated.Latency.P50Ms,
			c.Consolidated.Latency.P95Ms,
			c.SingleBlock.HTTP.Bytes,
			c.Consolidated.HTTP.Bytes,
			formatHeights(c.MismatchHeights),
		)
	}

	fmt.Fprintf(&b, "\n## Source Summary\n\n")
	for _, c := range report.Cases {
		if c.Error != "" {
			fmt.Fprintf(&b, "### %s\n\nerror: `%s`\n\n", c.Name, c.Error)
			continue
		}
		fmt.Fprintf(&b, "### %s\n\n", c.Name)
		fmt.Fprintf(&b, "- single-block files: `%d`, skipped: `%d`, single_block_objects: `%d`, consolidated_objects: `%d`, formats: `%s`, HTTP requests: `%d`, bytes: `%d`\n",
			c.SingleBlock.Files.Count,
			c.SingleBlock.Files.SkippedCount,
			c.SingleBlock.Files.SingleBlockObjectCount,
			c.SingleBlock.Files.ConsolidatedObjectCount,
			strings.Join(c.SingleBlock.Files.Formats, ","),
			c.SingleBlock.HTTP.Requests,
			c.SingleBlock.HTTP.Bytes,
		)
		fmt.Fprintf(&b, "- consolidated files: `%d`, skipped: `%d`, single_block_objects: `%d`, consolidated_objects: `%d`, formats: `%s`, HTTP requests: `%d`, bytes: `%d`\n",
			c.Consolidated.Files.Count,
			c.Consolidated.Files.SkippedCount,
			c.Consolidated.Files.SingleBlockObjectCount,
			c.Consolidated.Files.ConsolidatedObjectCount,
			strings.Join(c.Consolidated.Files.Formats, ","),
			c.Consolidated.HTTP.Requests,
			c.Consolidated.HTTP.Bytes,
		)
		fmt.Fprintf(&b, "- canonical hashes compared: `%d`\n\n", c.CanonicalHashesCompared)
	}
	return os.WriteFile(path, []byte(b.String()), 0o644)
}

func formatHeights(heights []uint64) string {
	if len(heights) == 0 {
		return ""
	}
	values := make([]string, len(heights))
	for i, height := range heights {
		values[i] = fmt.Sprintf("%d", height)
	}
	return strings.Join(values, ",")
}
