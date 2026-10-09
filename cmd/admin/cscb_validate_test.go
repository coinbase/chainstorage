package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

func TestBuildCSCBValidationCasesIncludesRequiredCoverage(t *testing.T) {
	require := require.New(t)
	cfg := cscbValidationRunConfig{
		startHeight:          1000,
		endHeight:            1100,
		chunkBlocks:          25,
		randomSamples:        2,
		randomSeed:           914,
		iterations:           3,
		fullObjectIterations: 1,
	}

	cases := buildCSCBValidationCases(cfg, 999)
	names := make(map[string]cscbValidationCase, len(cases))
	for _, validationCase := range cases {
		names[validationCase.Name] = validationCase
	}

	require.Contains(names, "single_first_block")
	require.Contains(names, "single_last_block")
	require.Contains(names, "range_within_one_chunk")
	require.Contains(names, "range_across_chunk_boundary")
	require.Contains(names, "range_full_object")
	require.Contains(names, "single_consolidated_miss_fallback")
	require.True(names["single_consolidated_miss_fallback"].ExpectFallback)
	require.Equal(1, names["range_full_object"].Iterations)
	require.Equal(3, names["range_across_chunk_boundary"].Iterations)
	require.NotEmpty(chunkBoundaryHeights(1000, 1100, 25))
	require.Len(randomSampleHeights(1000, 1100, 2, 914), 2)
}

func TestCompareCSCBBlocksIgnoresSourceSpecificMetadata(t *testing.T) {
	require := require.New(t)
	singleBlock := &api.Block{
		Metadata: &api.BlockMetadata{
			Tag:           1,
			Height:        42,
			Hash:          "hash",
			ParentHash:    "parent",
			ObjectKeyMain: "single-block/object",
		},
	}
	consolidated := &api.Block{
		Metadata: &api.BlockMetadata{
			Tag:                1,
			Height:             42,
			Hash:               "hash",
			ParentHash:         "parent",
			ObjectFormat:       api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_CSCB_BATCH,
			ByteOffset:         1024,
			ByteLength:         2048,
			UncompressedLength: 4096,
		},
	}

	comparison := compareCSCBBlocks([]*api.Block{singleBlock}, []*api.Block{consolidated})
	require.True(comparison.correct)
	require.Empty(comparison.mismatchHeights)
	require.Equal(1, comparison.hashesCompared)
	require.Equal(comparison.firstSingleBlockHash, comparison.firstConsolidatedHash)
}

func TestCompareCSCBBlocksDetectsMismatch(t *testing.T) {
	require := require.New(t)
	singleBlock := &api.Block{
		Metadata: &api.BlockMetadata{Height: 42, Hash: "hash"},
	}
	consolidated := &api.Block{
		Metadata: &api.BlockMetadata{Height: 42, Hash: "different"},
	}

	comparison := compareCSCBBlocks([]*api.Block{singleBlock}, []*api.Block{consolidated})
	require.False(comparison.correct)
	require.Equal([]uint64{42}, comparison.mismatchHeights)
}

func TestCompareCSCBBlocksHandlesNilBlocks(t *testing.T) {
	require := require.New(t)
	consolidated := &api.Block{
		Metadata: &api.BlockMetadata{Height: 42, Hash: "hash"},
	}

	comparison := compareCSCBBlocks([]*api.Block{nil}, []*api.Block{consolidated})
	require.False(comparison.correct)
	require.Equal([]uint64{42}, comparison.mismatchHeights)
}

func TestCanonicalBlockHashRestoresMetadata(t *testing.T) {
	require := require.New(t)
	block := &api.Block{
		Metadata: &api.BlockMetadata{
			Height:             42,
			Hash:               "hash",
			ObjectKeyMain:      "consolidated/object",
			ObjectFormat:       api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_CSCB_BATCH,
			ByteOffset:         10,
			ByteLength:         20,
			UncompressedLength: 30,
		},
	}

	_, err := canonicalBlockHash(block)
	require.NoError(err)
	require.Equal("consolidated/object", block.Metadata.ObjectKeyMain)
	require.Equal(api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_CSCB_BATCH, block.Metadata.ObjectFormat)
	require.Equal(uint64(10), block.Metadata.ByteOffset)
	require.Equal(uint64(20), block.Metadata.ByteLength)
	require.Equal(uint64(30), block.Metadata.UncompressedLength)
}

func TestCanonicalRawBlockDataHashIgnoresSourceSpecificMetadata(t *testing.T) {
	require := require.New(t)
	block := &api.Block{
		Metadata: &api.BlockMetadata{
			Tag:                1,
			Height:             42,
			Hash:               "hash",
			ParentHash:         "parent",
			ObjectFormat:       api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_CSCB_BATCH,
			ByteOffset:         1024,
			ByteLength:         2048,
			UncompressedLength: 4096,
		},
	}
	payload, err := proto.Marshal(block)
	require.NoError(err)

	rawHash, err := canonicalRawBlockDataHash(&api.BlockFile{Height: 42}, payload)
	require.NoError(err)
	expectedHash, err := canonicalBlockHash(block)
	require.NoError(err)
	require.Equal(expectedHash, rawHash)
}

func TestCanonicalRawBlockDataHashUsesCSCBBlockFileMetadata(t *testing.T) {
	require := require.New(t)
	block := &api.Block{
		Metadata: &api.BlockMetadata{
			Tag:        1,
			Height:     42,
			Hash:       "stale-embedded-hash",
			ParentHash: "stale-embedded-parent",
		},
	}
	payload, err := proto.Marshal(block)
	require.NoError(err)

	blockFile := &api.BlockFile{
		Tag:          1,
		Height:       42,
		Hash:         "metadata-hash",
		ParentHash:   "metadata-parent",
		ObjectFormat: api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_CSCB_BATCH,
		ByteLength:   uint64(len(payload)),
	}
	rawHash, err := canonicalRawBlockDataHash(blockFile, payload)
	require.NoError(err)

	expectedHash, err := canonicalBlockHash(&api.Block{
		Metadata: &api.BlockMetadata{
			Tag:          1,
			Height:       42,
			Hash:         "metadata-hash",
			ParentHash:   "metadata-parent",
			ObjectFormat: api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_CSCB_BATCH,
			ByteLength:   uint64(len(payload)),
		},
	})
	require.NoError(err)
	require.Equal(expectedHash, rawHash)
}

func TestCanonicalRawBlockDataHashHandlesSkippedBlock(t *testing.T) {
	require := require.New(t)
	rawHash, err := canonicalRawBlockDataHash(&api.BlockFile{Tag: 1, Height: 42, Skipped: true}, nil)
	require.NoError(err)
	expectedHash, err := canonicalBlockHash(&api.Block{
		Metadata: &api.BlockMetadata{
			Tag:     1,
			Height:  42,
			Skipped: true,
		},
	})
	require.NoError(err)
	require.Equal(expectedHash, rawHash)
}

func TestCompareCSCBDigestsDetectsMismatch(t *testing.T) {
	require := require.New(t)
	comparison := compareCSCBDigests(
		[]cscbBlockDigest{{height: 42, hash: "single-block"}},
		[]cscbBlockDigest{{height: 42, hash: "consolidated"}},
	)

	require.False(comparison.correct)
	require.Equal([]uint64{42}, comparison.mismatchHeights)
	require.Equal(1, comparison.hashesCompared)
	require.Equal("single-block", comparison.firstSingleBlockHash)
	require.Equal("consolidated", comparison.firstConsolidatedHash)
}

func TestCompareCSCBDigestsHandlesLengthMismatch(t *testing.T) {
	require := require.New(t)
	comparison := compareCSCBDigests(
		[]cscbBlockDigest{{height: 0, hash: "hash"}},
		[]cscbBlockDigest{{height: 0, hash: "hash"}, {height: 1, hash: "hash"}},
	)

	require.False(comparison.correct)
	require.Equal([]uint64{0, 1}, comparison.mismatchHeights)
	require.Zero(comparison.hashesCompared)
}

func TestSummarizeDurationsUsesNearestRankPercentile(t *testing.T) {
	require := require.New(t)
	summary := summarizeDurations([]time.Duration{
		10 * time.Millisecond,
		20 * time.Millisecond,
		30 * time.Millisecond,
		40 * time.Millisecond,
		50 * time.Millisecond,
	})

	require.Equal(5, summary.Samples)
	require.Equal(float64(30), summary.P50Ms)
	require.Equal(float64(50), summary.P95Ms)
	require.Equal(float64(10), summary.MinMs)
	require.Equal(float64(50), summary.MaxMs)
}

func TestSummarizeCSCBFilesDoesNotCountSkippedBlocksAsSingleBlockFallback(t *testing.T) {
	require := require.New(t)

	summary := summarizeCSCBFiles([]*api.BlockFile{
		{
			Height:       100,
			ObjectFormat: api.BlockObjectFormat_BLOCK_OBJECT_FORMAT_CSCB_BATCH,
			ByteLength:   1024,
		},
		{
			Height:  101,
			Skipped: true,
		},
		{
			Height:     102,
			ByteLength: 512,
		},
	})

	require.Equal(3, summary.Count)
	require.Equal(1, summary.ConsolidatedObjectCount)
	require.Equal(1, summary.SkippedCount)
	require.Equal(1, summary.SingleBlockObjectCount)
	require.Equal(uint64(100), summary.FirstHeight)
	require.Equal(uint64(102), summary.LastHeight)
	require.Equal([]string{"BLOCK_OBJECT_FORMAT_CSCB_BATCH", "BLOCK_OBJECT_FORMAT_SINGLE_BLOCK"}, summary.Formats)
}

func TestSanitizeCSCBStringRedactsEmbeddedURLs(t *testing.T) {
	require := require.New(t)
	errText := `Download failed: blockFile={FileUrl:"https://bucket.s3.amazonaws.com/key?X-Amz-Signature=secret", Height:42}`

	sanitized := sanitizeCSCBString(errText)
	require.NotContains(sanitized, "X-Amz-Signature")
	require.NotContains(sanitized, "secret")
	require.Contains(sanitized, "<redacted-url>")
}
