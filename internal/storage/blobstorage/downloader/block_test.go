package downloader

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/suite"
	"go.uber.org/fx"
	"golang.org/x/xerrors"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"

	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/storage/blobstorage/cscb"
	blobstorageinternal "github.com/coinbase/chainstorage/internal/storage/blobstorage/internal"
	"github.com/coinbase/chainstorage/internal/storage/internal/errors"
	storage_utils "github.com/coinbase/chainstorage/internal/storage/utils"
	"github.com/coinbase/chainstorage/internal/utils/testapp"
	"github.com/coinbase/chainstorage/internal/utils/testutil"
	"github.com/coinbase/chainstorage/protos/coinbase/c3/common"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

var (
	expectedBlock = &api.Block{
		Blockchain: common.Blockchain_BLOCKCHAIN_ETHEREUM,
		Network:    common.Network_NETWORK_ETHEREUM_MAINNET,
	}

	expectedSkippedBlock = &api.Block{
		Blockchain: common.Blockchain_BLOCKCHAIN_ETHEREUM,
		Network:    common.Network_NETWORK_ETHEREUM_MAINNET,
		Metadata: &api.BlockMetadata{
			Tag:     1,
			Height:  123,
			Skipped: true,
		},
	}

	expectedBlockBytes, _           = proto.Marshal(expectedBlock)
	expectedBlockCompressedBytes, _ = storage_utils.Compress(expectedBlockBytes, api.Compression_GZIP)
)

type (
	blockDownloaderTestSuite struct {
		suite.Suite
		app              testapp.TestApp
		httpServer       *httptest.Server
		downloader       BlockDownloader
		blockFile        *api.BlockFile
		skippedBlockFile *api.BlockFile
	}

	httpServerFunc func() *httptest.Server
	httpClientFunc func() HTTPClient
)

func TestBlockDownloaderSuite(t *testing.T) {
	suite.Run(t, new(blockDownloaderTestSuite))
}

func TestReadExpectedRangeBodyAllowsShortInitialRead(t *testing.T) {
	require := testutil.Require(t)

	actual, err := readExpectedRangeBody(bytes.NewReader([]byte("short")), 64*1024, true)
	require.NoError(err)
	require.Equal([]byte("short"), actual)
}

func TestReadExpectedRangeBodyRejectsShortNonInitialRead(t *testing.T) {
	require := testutil.Require(t)

	_, err := readExpectedRangeBody(bytes.NewReader([]byte("short")), 10, false)
	require.Error(err)
	require.Contains(err.Error(), "range body length mismatch")
}

func TestReadExpectedRangeBodyRejectsOversizedRead(t *testing.T) {
	require := testutil.Require(t)

	_, err := readExpectedRangeBody(bytes.NewReader([]byte("too-long")), 3, true)
	require.Error(err)
	require.Contains(err.Error(), "range body length mismatch")
}

func TestReadRangePrefixBodyTruncatesOversizedRead(t *testing.T) {
	require := testutil.Require(t)

	actual, err := readRangePrefixBody(bytes.NewReader([]byte("too-long")), 3)
	require.NoError(err)
	require.Equal([]byte("too"), actual)
}

func (s *blockDownloaderTestSuite) SetupTest() {
	s.app = testapp.New(
		s.T(),
	)
	s.blockFile = &api.BlockFile{
		Tag:          1,
		Hash:         "0xabc",
		ParentHash:   "0xdef",
		Height:       123,
		ParentHeight: 122,
		Skipped:      false,
	}
	s.skippedBlockFile = &api.BlockFile{
		Tag:     1,
		Height:  123,
		Skipped: true,
	}
}

func (s *blockDownloaderTestSuite) TearDownTest() {
	if s.httpServer != nil {
		s.httpServer.Close()
	}
	s.app.Close()
}

func (s *blockDownloaderTestSuite) TestDownloadFailure() {
	require := testutil.Require(s.T())
	s.app = testapp.New(
		s.T(),
		fx.Provide(s.newHttpServerFunc(http.MethodGet, http.StatusInternalServerError, []byte(nil))),
		fx.Populate(&s.httpServer),
		fx.Provide(s.newHttpClientFunc()),
		fx.Provide(NewBlockDownloader),
		fx.Populate(&s.downloader),
	)

	_, err := s.downloader.Download(context.Background(), s.blockFile)
	require.True(xerrors.Is(err, errors.ErrDownloadFailure))
}

func (s *blockDownloaderTestSuite) TestUnmarshalFailure() {
	require := testutil.Require(s.T())
	s.app = testapp.New(
		s.T(),
		fx.Provide(s.newHttpServerFunc(http.MethodGet, http.StatusOK, []byte("foo"))),
		fx.Populate(&s.httpServer),
		fx.Provide(s.newHttpClientFunc()),
		fx.Provide(NewBlockDownloader),
		fx.Populate(&s.downloader),
	)

	resp, err := s.downloader.Download(context.Background(), s.blockFile)
	require.Nil(resp)
	require.Error(err)
}

func (s *blockDownloaderTestSuite) TestSuccess() {
	require := testutil.Require(s.T())
	s.app = testapp.New(
		s.T(),
		fx.Provide(s.newHttpServerFunc(http.MethodGet, http.StatusOK, expectedBlockBytes)),
		fx.Populate(&s.httpServer),
		fx.Provide(s.newHttpClientFunc()),
		fx.Provide(NewBlockDownloader),
		fx.Populate(&s.downloader),
	)

	rawBlock, err := s.downloader.Download(context.Background(), s.blockFile)
	require.NoError(err)
	if diff := cmp.Diff(expectedBlock, rawBlock, protocmp.Transform()); diff != "" {
		require.FailNow(diff)
	}
}

func (s *blockDownloaderTestSuite) TestSuccess_Gzip() {
	require := testutil.Require(s.T())
	s.app = testapp.New(
		s.T(),
		fx.Provide(s.newHttpServerFunc(http.MethodGet, http.StatusOK, expectedBlockCompressedBytes)),
		fx.Populate(&s.httpServer),
		fx.Provide(s.newHttpClientFunc()),
		fx.Provide(NewBlockDownloader),
		fx.Populate(&s.downloader),
	)

	s.blockFile.Compression = api.Compression_GZIP

	rawBlock, err := s.downloader.Download(context.Background(), s.blockFile)
	require.NoError(err)
	if diff := cmp.Diff(expectedBlock, rawBlock, protocmp.Transform()); diff != "" {
		require.FailNow(diff)
	}
}

func (s *blockDownloaderTestSuite) TestSkipped() {
	require := testutil.Require(s.T())
	s.app = testapp.New(
		s.T(),
		fx.Provide(NewBlockDownloader),
		fx.Populate(&s.downloader),
		fx.Provide(s.newHttpClientFunc()),
	)

	rawBlock, err := s.downloader.Download(context.Background(), s.skippedBlockFile)
	require.NoError(err)
	if diff := cmp.Diff(expectedSkippedBlock, rawBlock, protocmp.Transform()); diff != "" {
		require.FailNow(diff)
	}
}

func (s *blockDownloaderTestSuite) TestDownloadStream_Success() {
	require := testutil.Require(s.T())

	bitcoinBlock := &api.Block{
		Blockchain: common.Blockchain_BLOCKCHAIN_BITCOIN,
		Network:    common.Network_NETWORK_BITCOIN_MAINNET,
		Metadata:   &api.BlockMetadata{Tag: 1, Height: 123, Hash: "0xabc"},
		Blobdata: &api.Block_Bitcoin{
			Bitcoin: &api.BitcoinBlobdata{
				Header: []byte(`{"hash":"0xabc","height":123,"tx":[]}`),
			},
		},
	}
	blockBytes, err := proto.Marshal(bitcoinBlock)
	require.NoError(err)

	s.app = testapp.New(
		s.T(),
		fx.Provide(s.newHttpServerFunc(http.MethodGet, http.StatusOK, blockBytes)),
		fx.Populate(&s.httpServer),
		fx.Provide(s.newHttpClientFunc()),
		fx.Provide(NewBlockDownloader),
		fx.Populate(&s.downloader),
	)

	spooled, err := s.downloader.DownloadStream(context.Background(), s.blockFile)
	require.NoError(err)
	defer spooled.Close()

	require.Equal(s.blockFile, spooled.BlockFile)

	// Open should yield the full decompressed proto bytes (no
	// compression in this test since the HTTP body is already raw).
	rc, err := spooled.Open()
	require.NoError(err)
	defer rc.Close()
	gotBytes, err := io.ReadAll(rc)
	require.NoError(err)
	require.Equal(blockBytes, gotBytes)
}

func (s *blockDownloaderTestSuite) TestDownloadStream_Skipped() {
	require := testutil.Require(s.T())
	s.app = testapp.New(
		s.T(),
		fx.Provide(NewBlockDownloader),
		fx.Populate(&s.downloader),
		fx.Provide(s.newHttpClientFunc()),
	)

	spooled, err := s.downloader.DownloadStream(context.Background(), s.skippedBlockFile)
	require.NoError(err)
	defer spooled.Close()

	require.True(spooled.BlockFile.GetSkipped())
	rc, err := spooled.Open()
	require.NoError(err)
	defer rc.Close()
	got, err := io.ReadAll(rc)
	require.NoError(err)
	require.Empty(got)
}

func (s *blockDownloaderTestSuite) TestDownloadStream_CloseRemovesSpool() {
	require := testutil.Require(s.T())

	bitcoinBlock := &api.Block{
		Blockchain: common.Blockchain_BLOCKCHAIN_BITCOIN,
		Network:    common.Network_NETWORK_BITCOIN_MAINNET,
		Metadata:   &api.BlockMetadata{Tag: 1, Height: 42, Hash: "0xabc"},
		Blobdata: &api.Block_Bitcoin{
			Bitcoin: &api.BitcoinBlobdata{Header: []byte(`{"hash":"0xabc"}`)},
		},
	}
	blockBytes, err := proto.Marshal(bitcoinBlock)
	require.NoError(err)

	s.app = testapp.New(
		s.T(),
		fx.Provide(s.newHttpServerFunc(http.MethodGet, http.StatusOK, blockBytes)),
		fx.Populate(&s.httpServer),
		fx.Provide(s.newHttpClientFunc()),
		fx.Provide(NewBlockDownloader),
		fx.Populate(&s.downloader),
	)

	spooled, err := s.downloader.DownloadStream(context.Background(), s.blockFile)
	require.NoError(err)

	// Open works before Close.
	rc, err := spooled.Open()
	require.NoError(err)
	require.NoError(rc.Close())

	require.NoError(spooled.Close())
	// Second Close must be a no-op.
	require.NoError(spooled.Close())

	// After Close, Open fails (spool removed).
	_, err = spooled.Open()
	require.Error(err)
}

func (s *blockDownloaderTestSuite) TestDownload_CSCB() {
	require := testutil.Require(s.T())
	fixture := newCSCBDownloaderFixture(s.T(), 3, 2)
	defer fixture.close()

	server, recorder := newRangeHTTPServer(fixture.data)
	defer server.Close()
	for _, file := range fixture.blockFiles {
		file.FileUrl = server.URL
	}

	s.app = testapp.New(
		s.T(),
		fx.Provide(func() HTTPClient { return server.Client() }),
		fx.Provide(NewBlockDownloader),
		fx.Populate(&s.downloader),
	)

	rawBlock, err := s.downloader.Download(context.Background(), fixture.blockFiles[1])
	require.NoError(err)
	expected := proto.Clone(fixture.blocks[1]).(*api.Block)
	expected.Metadata = blockFileToMetadata(fixture.blockFiles[1])
	if diff := cmp.Diff(expected, rawBlock, protocmp.Transform()); diff != "" {
		require.FailNow(diff)
	}

	ranges := recorder.rangesSnapshot()
	require.Equal([]string{
		"bytes=0-65535",
		rangeHeader(fixture.index.Chunks[0].CompressedPayloadOffset, fixture.index.Chunks[0].CompressedLength),
	}, ranges)
}

func (s *blockDownloaderTestSuite) TestDownload_CSCBInitialIndexIgnoresRangeWithBoundedPrefix() {
	require := testutil.Require(s.T())
	fixture := newCSCBDownloaderFixture(s.T(), 3, 2)
	defer fixture.close()

	data := append([]byte{}, fixture.data...)
	if len(data) <= 64*1024 {
		data = append(data, bytes.Repeat([]byte{0}, 64*1024-len(data)+1)...)
	}
	server, recorder := newInitialRangeIgnoringHTTPServer(data)
	defer server.Close()
	for _, file := range fixture.blockFiles {
		file.FileUrl = server.URL
	}

	s.app = testapp.New(
		s.T(),
		fx.Provide(func() HTTPClient { return server.Client() }),
		fx.Provide(NewBlockDownloader),
		fx.Populate(&s.downloader),
	)

	rawBlock, err := s.downloader.Download(context.Background(), fixture.blockFiles[1])
	require.NoError(err)
	expected := proto.Clone(fixture.blocks[1]).(*api.Block)
	expected.Metadata = blockFileToMetadata(fixture.blockFiles[1])
	if diff := cmp.Diff(expected, rawBlock, protocmp.Transform()); diff != "" {
		require.FailNow(diff)
	}

	ranges := recorder.rangesSnapshot()
	require.Equal([]string{
		"bytes=0-65535",
		rangeHeader(fixture.index.Chunks[0].CompressedPayloadOffset, fixture.index.Chunks[0].CompressedLength),
	}, ranges)
}

func (s *blockDownloaderTestSuite) TestDownloadMany_CSCBGroupsByChunk() {
	require := testutil.Require(s.T())
	fixture := newCSCBDownloaderFixture(s.T(), 4, 2)
	defer fixture.close()

	server, recorder := newRangeHTTPServer(fixture.data)
	defer server.Close()
	for _, file := range fixture.blockFiles {
		file.FileUrl = server.URL
	}

	s.app = testapp.New(
		s.T(),
		fx.Provide(func() HTTPClient { return server.Client() }),
		fx.Provide(NewBlockDownloader),
		fx.Populate(&s.downloader),
	)

	blockFiles := []*api.BlockFile{
		fixture.blockFiles[1],
		fixture.blockFiles[0],
		fixture.blockFiles[3],
	}
	rawBlocks, err := s.downloader.DownloadMany(context.Background(), blockFiles)
	require.NoError(err)
	require.Len(rawBlocks, len(blockFiles))

	for i, blockFile := range blockFiles {
		sourceIndex := int(blockFile.GetHeight() - 100)
		expected := proto.Clone(fixture.blocks[sourceIndex]).(*api.Block)
		expected.Metadata = blockFileToMetadata(blockFile)
		if diff := cmp.Diff(expected, rawBlocks[i], protocmp.Transform()); diff != "" {
			require.FailNow(diff)
		}
	}

	ranges := recorder.rangesSnapshot()
	require.ElementsMatch([]string{
		"bytes=0-65535",
		rangeHeader(fixture.index.Chunks[0].CompressedPayloadOffset, fixture.index.Chunks[0].CompressedLength),
		rangeHeader(fixture.index.Chunks[1].CompressedPayloadOffset, fixture.index.Chunks[1].CompressedLength),
	}, ranges)
}

func (s *blockDownloaderTestSuite) TestDownloadMany_CSCBAllowsDuplicateInputs() {
	require := testutil.Require(s.T())
	fixture := newCSCBDownloaderFixture(s.T(), 4, 2)
	defer fixture.close()

	server, recorder := newRangeHTTPServer(fixture.data)
	defer server.Close()
	for _, file := range fixture.blockFiles {
		file.FileUrl = server.URL
	}

	s.app = testapp.New(
		s.T(),
		fx.Provide(func() HTTPClient { return server.Client() }),
		fx.Provide(NewBlockDownloader),
		fx.Populate(&s.downloader),
	)

	blockFiles := []*api.BlockFile{
		fixture.blockFiles[1],
		fixture.blockFiles[1],
	}
	rawBlocks, err := s.downloader.DownloadMany(context.Background(), blockFiles)
	require.NoError(err)
	require.Len(rawBlocks, len(blockFiles))

	for i, blockFile := range blockFiles {
		expected := proto.Clone(fixture.blocks[1]).(*api.Block)
		expected.Metadata = blockFileToMetadata(blockFile)
		if diff := cmp.Diff(expected, rawBlocks[i], protocmp.Transform()); diff != "" {
			require.FailNow(diff)
		}
	}

	ranges := recorder.rangesSnapshot()
	require.Equal([]string{
		"bytes=0-65535",
		rangeHeader(fixture.index.Chunks[0].CompressedPayloadOffset, fixture.index.Chunks[0].CompressedLength),
	}, ranges)
}

func (s *blockDownloaderTestSuite) TestDownloadMany_CSCBLimitsConcurrencyAcrossFiles() {
	require := testutil.Require(s.T())
	const workerLimit = 2

	fixtureA := newCSCBDownloaderFixture(s.T(), 4, 1)
	defer fixtureA.close()
	fixtureB := newCSCBDownloaderFixture(s.T(), 4, 1)
	defer fixtureB.close()
	tracker := &rangeConcurrencyTracker{}

	serverA, _ := newRangeHTTPServerWithTracker(fixtureA.data, tracker, 20*time.Millisecond)
	defer serverA.Close()
	serverB, _ := newRangeHTTPServerWithTracker(fixtureB.data, tracker, 20*time.Millisecond)
	defer serverB.Close()
	for _, file := range fixtureA.blockFiles {
		file.FileUrl = serverA.URL
	}
	for _, file := range fixtureB.blockFiles {
		file.FileUrl = serverB.URL
	}

	s.app = testapp.New(
		s.T(),
		fx.Invoke(func(cfg *config.Config) {
			cfg.SDK.NumWorkers = workerLimit
			cfg.Api.NumWorkers = workerLimit
		}),
		fx.Provide(func() HTTPClient { return serverA.Client() }),
		fx.Provide(NewBlockDownloader),
		fx.Populate(&s.downloader),
	)

	blockFiles := append([]*api.BlockFile{}, fixtureA.blockFiles...)
	blockFiles = append(blockFiles, fixtureB.blockFiles...)
	rawBlocks, err := s.downloader.DownloadMany(context.Background(), blockFiles)
	require.NoError(err)
	require.Len(rawBlocks, len(blockFiles))
	require.LessOrEqual(tracker.peakActive(), workerLimit)
}

func (s *blockDownloaderTestSuite) TestDownloadMany_MixedSingleBlockAndCSCBPreservesOrder() {
	require := testutil.Require(s.T())
	fixture := newCSCBDownloaderFixture(s.T(), 2, 2)
	defer fixture.close()

	cscbServer, _ := newRangeHTTPServer(fixture.data)
	defer cscbServer.Close()
	for _, file := range fixture.blockFiles {
		file.FileUrl = cscbServer.URL
	}

	singleBlock := &api.Block{
		Blockchain: common.Blockchain_BLOCKCHAIN_ETHEREUM,
		Network:    common.Network_NETWORK_ETHEREUM_MAINNET,
		Metadata:   &api.BlockMetadata{Tag: 1, Height: 900, Hash: "single-block-hash"},
	}
	singleBlockPayload, err := proto.Marshal(singleBlock)
	require.NoError(err)
	singleBlockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(http.MethodGet, r.Method)
		_, _ = w.Write(singleBlockPayload)
	}))
	defer singleBlockServer.Close()

	singleBlockFile := &api.BlockFile{
		Tag:     1,
		Height:  900,
		Hash:    "single-block-hash",
		FileUrl: singleBlockServer.URL,
	}

	s.app = testapp.New(
		s.T(),
		fx.Provide(func() HTTPClient { return cscbServer.Client() }),
		fx.Provide(NewBlockDownloader),
		fx.Populate(&s.downloader),
	)

	rawBlocks, err := s.downloader.DownloadMany(context.Background(), []*api.BlockFile{
		fixture.blockFiles[1],
		singleBlockFile,
		fixture.blockFiles[0],
	})
	require.NoError(err)
	require.Len(rawBlocks, 3)

	expectedFirst := proto.Clone(fixture.blocks[1]).(*api.Block)
	expectedFirst.Metadata = blockFileToMetadata(fixture.blockFiles[1])
	expectedThird := proto.Clone(fixture.blocks[0]).(*api.Block)
	expectedThird.Metadata = blockFileToMetadata(fixture.blockFiles[0])
	if diff := cmp.Diff(expectedFirst, rawBlocks[0], protocmp.Transform()); diff != "" {
		require.FailNow(diff)
	}
	if diff := cmp.Diff(singleBlock, rawBlocks[1], protocmp.Transform()); diff != "" {
		require.FailNow(diff)
	}
	if diff := cmp.Diff(expectedThird, rawBlocks[2], protocmp.Transform()); diff != "" {
		require.FailNow(diff)
	}
}

func (s *blockDownloaderTestSuite) TestDownloadStream_CSCB() {
	require := testutil.Require(s.T())
	fixture := newCSCBDownloaderFixture(s.T(), 2, 2)
	defer fixture.close()

	server, _ := newRangeHTTPServer(fixture.data)
	defer server.Close()
	for _, file := range fixture.blockFiles {
		file.FileUrl = server.URL
	}

	s.app = testapp.New(
		s.T(),
		fx.Provide(func() HTTPClient { return server.Client() }),
		fx.Provide(NewBlockDownloader),
		fx.Populate(&s.downloader),
	)

	spooled, err := s.downloader.DownloadStream(context.Background(), fixture.blockFiles[1])
	require.NoError(err)
	defer spooled.Close()

	rc, err := spooled.Open()
	require.NoError(err)
	defer rc.Close()
	gotBytes, err := io.ReadAll(rc)
	require.NoError(err)

	var rawBlock api.Block
	require.NoError(proto.Unmarshal(gotBytes, &rawBlock))
	expected := proto.Clone(fixture.blocks[1]).(*api.Block)
	if diff := cmp.Diff(expected, &rawBlock, protocmp.Transform()); diff != "" {
		require.FailNow(diff)
	}
}

func (s *blockDownloaderTestSuite) TestOpenRawBlockPayload_SingleBlockGzip() {
	require := testutil.Require(s.T())
	s.app = testapp.New(
		s.T(),
		fx.Provide(s.newHttpServerFunc(http.MethodGet, http.StatusOK, expectedBlockCompressedBytes)),
		fx.Populate(&s.httpServer),
		fx.Provide(s.newHttpClientFunc()),
		fx.Provide(NewBlockDownloader),
		fx.Populate(&s.downloader),
	)
	s.blockFile.Compression = api.Compression_GZIP
	s.blockFile.UncompressedLength = uint64(len(expectedBlockBytes))

	payload, err := s.downloader.OpenRawBlockPayload(context.Background(), s.blockFile)
	require.NoError(err)
	defer payload.Close()
	require.Equal(s.blockFile, payload.BlockFile)
	require.Equal(uint64(len(expectedBlockBytes)), payload.Length)

	got, err := io.ReadAll(payload)
	require.NoError(err)
	require.Equal(expectedBlockBytes, got)
}

func (s *blockDownloaderTestSuite) TestOpenRawBlockPayload_SingleBlockDetectsShortPayload() {
	require := testutil.Require(s.T())
	s.app = testapp.New(
		s.T(),
		fx.Provide(s.newHttpServerFunc(http.MethodGet, http.StatusOK, expectedBlockBytes[:len(expectedBlockBytes)-1])),
		fx.Populate(&s.httpServer),
		fx.Provide(s.newHttpClientFunc()),
		fx.Provide(NewBlockDownloader),
		fx.Populate(&s.downloader),
	)
	s.blockFile.UncompressedLength = uint64(len(expectedBlockBytes))

	payload, err := s.downloader.OpenRawBlockPayload(context.Background(), s.blockFile)
	require.NoError(err)
	defer payload.Close()

	_, err = io.ReadAll(payload)
	require.Error(err)
	require.Contains(err.Error(), "length mismatch")
}

func (s *blockDownloaderTestSuite) TestOpenRawBlockPayload_SingleBlockDetectsOversizedPayload() {
	require := testutil.Require(s.T())
	s.app = testapp.New(
		s.T(),
		fx.Provide(s.newHttpServerFunc(http.MethodGet, http.StatusOK, append(append([]byte(nil), expectedBlockBytes...), []byte("extra")...))),
		fx.Populate(&s.httpServer),
		fx.Provide(s.newHttpClientFunc()),
		fx.Provide(NewBlockDownloader),
		fx.Populate(&s.downloader),
	)
	s.blockFile.UncompressedLength = uint64(len(expectedBlockBytes))

	payload, err := s.downloader.OpenRawBlockPayload(context.Background(), s.blockFile)
	require.NoError(err)
	defer payload.Close()

	_, err = io.ReadAll(payload)
	require.Error(err)
	require.Contains(err.Error(), "exceeds expected length")
}

func (s *blockDownloaderTestSuite) TestOpenRawBlockPayload_CSCBStreamsSinglePayload() {
	require := testutil.Require(s.T())
	fixture := newCSCBDownloaderFixture(s.T(), 2, 2)
	defer fixture.close()

	server, recorder := newRangeHTTPServer(fixture.data)
	defer server.Close()
	for _, file := range fixture.blockFiles {
		file.FileUrl = server.URL
	}

	s.app = testapp.New(
		s.T(),
		fx.Provide(func() HTTPClient { return server.Client() }),
		fx.Provide(NewBlockDownloader),
		fx.Populate(&s.downloader),
	)

	payload, err := s.downloader.OpenRawBlockPayload(context.Background(), fixture.blockFiles[1])
	require.NoError(err)
	defer payload.Close()
	require.Equal(fixture.blockFiles[1], payload.BlockFile)
	require.Equal(fixture.blockFiles[1].GetByteLength(), payload.Length)

	gotBytes, err := io.ReadAll(payload)
	require.NoError(err)
	var rawBlock api.Block
	require.NoError(proto.Unmarshal(gotBytes, &rawBlock))
	expected := proto.Clone(fixture.blocks[1]).(*api.Block)
	if diff := cmp.Diff(expected, &rawBlock, protocmp.Transform()); diff != "" {
		require.FailNow(diff)
	}

	require.Equal([]string{
		"bytes=0-65535",
		rangeHeader(fixture.index.Chunks[0].CompressedPayloadOffset, fixture.index.Chunks[0].CompressedLength),
	}, recorder.rangesSnapshot())
}

func (s *blockDownloaderTestSuite) TestOpenRawBlockPayloads_CSCBGroupsByChunk() {
	require := testutil.Require(s.T())
	fixture := newCSCBDownloaderFixture(s.T(), 4, 2)
	defer fixture.close()

	server, recorder := newRangeHTTPServer(fixture.data)
	defer server.Close()
	for _, file := range fixture.blockFiles {
		file.FileUrl = server.URL
	}

	s.app = testapp.New(
		s.T(),
		fx.Provide(func() HTTPClient { return server.Client() }),
		fx.Provide(NewBlockDownloader),
		fx.Populate(&s.downloader),
	)

	blockFiles := []*api.BlockFile{
		fixture.blockFiles[1],
		fixture.blockFiles[0],
		fixture.blockFiles[3],
	}
	iter, err := s.downloader.OpenRawBlockPayloads(context.Background(), blockFiles)
	require.NoError(err)
	defer iter.Close()

	for i, blockFile := range blockFiles {
		payload, err := iter.Next(context.Background())
		require.NoError(err)
		require.Equal(blockFile, payload.BlockFile)
		gotBytes, err := io.ReadAll(payload)
		require.NoError(err)
		require.NoError(payload.Close())

		var rawBlock api.Block
		require.NoError(proto.Unmarshal(gotBytes, &rawBlock))
		sourceIndex := int(blockFile.GetHeight() - 100)
		expected := proto.Clone(fixture.blocks[sourceIndex]).(*api.Block)
		if diff := cmp.Diff(expected, &rawBlock, protocmp.Transform()); diff != "" {
			require.FailNow(fmt.Sprintf("payload %d mismatch: %s", i, diff))
		}
	}
	_, err = iter.Next(context.Background())
	require.ErrorIs(err, io.EOF)

	require.ElementsMatch([]string{
		"bytes=0-65535",
		rangeHeader(fixture.index.Chunks[0].CompressedPayloadOffset, fixture.index.Chunks[0].CompressedLength),
		rangeHeader(fixture.index.Chunks[1].CompressedPayloadOffset, fixture.index.Chunks[1].CompressedLength),
	}, recorder.rangesSnapshot())
}

func BenchmarkOpenRawBlockPayload_CSCB_vs_Download(b *testing.B) {
	fixture := newCSCBDownloaderFixtureWithPayloadSize(b, 2, 2, 4*1024*1024)
	defer fixture.close()

	server, _ := newRangeHTTPServer(fixture.data)
	defer server.Close()
	for _, file := range fixture.blockFiles {
		file.FileUrl = server.URL
	}

	var dl BlockDownloader
	app := testapp.New(
		b,
		fx.Provide(func() HTTPClient { return server.Client() }),
		fx.Provide(NewBlockDownloader),
		fx.Populate(&dl),
	)
	defer app.Close()

	blockFile := fixture.blockFiles[1]
	b.Run("download_unmarshal", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			block, err := dl.Download(context.Background(), blockFile)
			if err != nil {
				b.Fatal(err)
			}
			if got := len(block.GetSolana().GetHeader()); got == 0 {
				b.Fatalf("expected solana payload, got %d bytes", got)
			}
		}
	})
	b.Run("open_raw_payload", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			payload, err := dl.OpenRawBlockPayload(context.Background(), blockFile)
			if err != nil {
				b.Fatal(err)
			}
			if _, err := io.Copy(io.Discard, payload); err != nil {
				_ = payload.Close()
				b.Fatal(err)
			}
			if err := payload.Close(); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func (s *blockDownloaderTestSuite) newHttpClientFunc() httpClientFunc {
	return func() HTTPClient {
		return s.httpServer.Client()
	}
}

func (s *blockDownloaderTestSuite) newHttpServerFunc(httpMethod string, respStatusCode int, bodyBytes []byte) httpServerFunc {
	return func() *httptest.Server {
		server := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
			s.T().Logf("HELLO WORLD: %s", bodyBytes)
			require := testutil.Require(s.T())
			require.Equal(httpMethod, request.Method)
			writer.WriteHeader(respStatusCode)
			if _, err := writer.Write(bodyBytes); err != nil {
				require.NoError(err)
			}
		}))
		s.blockFile.FileUrl = server.URL
		return server
	}
}

type cscbDownloaderFixture struct {
	object     *cscb.Object
	data       []byte
	index      *cscb.Index
	blocks     []*api.Block
	blockFiles []*api.BlockFile
}

func newCSCBDownloaderFixture(t *testing.T, blockCount int, chunkBlocks uint64) *cscbDownloaderFixture {
	return newCSCBDownloaderFixtureWithPayloadSize(t, blockCount, chunkBlocks, 0)
}

func newCSCBDownloaderFixtureWithPayloadSize(t testing.TB, blockCount int, chunkBlocks uint64, payloadSize int) *cscbDownloaderFixture {
	t.Helper()
	require := testutil.Require(t)

	blocks := make([]*api.Block, blockCount)
	payloads := make([]blobstorageinternal.ConsolidatedBlockPayload, blockCount)
	for i := 0; i < blockCount; i++ {
		height := uint64(100 + i)
		block := &api.Block{
			Blockchain: common.Blockchain_BLOCKCHAIN_ETHEREUM,
			Network:    common.Network_NETWORK_ETHEREUM_MAINNET,
			Metadata: &api.BlockMetadata{
				Tag:          1,
				Hash:         fmt.Sprintf("hash-%d", height),
				ParentHash:   fmt.Sprintf("hash-%d", height-1),
				Height:       height,
				ParentHeight: height - 1,
			},
		}
		if payloadSize > 0 {
			block.Blockchain = common.Blockchain_BLOCKCHAIN_SOLANA
			block.Network = common.Network_NETWORK_SOLANA_MAINNET
			block.Blobdata = &api.Block_Solana{
				Solana: &api.SolanaBlobdata{
					Header: bytes.Repeat([]byte{byte('a' + i)}, payloadSize),
				},
			}
		}
		blockBytes, err := proto.Marshal(block)
		require.NoError(err)
		blocks[i] = block
		payloads[i] = blobstorageinternal.ConsolidatedBlockPayload{
			Metadata:           block.Metadata,
			MetadataID:         int64(i + 1),
			RawBlockPayload:    blobstorageinternal.BytesPayloadSource(blockBytes),
			UncompressedLength: uint64(len(blockBytes)),
		}
	}

	object, err := cscb.Encode(context.Background(), cscb.EncodeConfig{
		Blockchain:             common.Blockchain_BLOCKCHAIN_ETHEREUM,
		Network:                common.Network_NETWORK_ETHEREUM_MAINNET,
		Codec:                  api.Compression_ZSTD,
		CodecLevel:             1,
		MaxBlocks:              uint64(blockCount),
		CompressionChunkBlocks: chunkBlocks,
		ShardSize:              10_000,
	}, payloads)
	require.NoError(err)
	data, ok := object.Bytes()
	require.True(ok)
	index, err := cscb.ParseIndex(data)
	require.NoError(err)

	blockFiles := make([]*api.BlockFile, blockCount)
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

	return &cscbDownloaderFixture{
		object:     object,
		data:       data,
		index:      index,
		blocks:     blocks,
		blockFiles: blockFiles,
	}
}

func (f *cscbDownloaderFixture) close() {
	if f != nil && f.object != nil {
		_ = f.object.Close()
	}
}

type rangeRequestRecorder struct {
	mu      sync.Mutex
	ranges  []string
	tracker *rangeConcurrencyTracker
	delay   time.Duration
}

type rangeConcurrencyTracker struct {
	mu     sync.Mutex
	active int
	peak   int
}

func newRangeHTTPServer(data []byte) (*httptest.Server, *rangeRequestRecorder) {
	return newRangeHTTPServerWithTracker(data, nil, 0)
}

func newInitialRangeIgnoringHTTPServer(data []byte) (*httptest.Server, *rangeRequestRecorder) {
	recorder := &rangeRequestRecorder{}
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodGet {
			writer.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		rangeValue := request.Header.Get("Range")
		done := recorder.record(rangeValue)
		defer done()
		if rangeValue == "bytes=0-65535" {
			_, _ = writer.Write(data)
			return
		}

		writeRangeResponse(writer, rangeValue, data)
	}))
	return server, recorder
}

func newRangeHTTPServerWithTracker(data []byte, tracker *rangeConcurrencyTracker, delay time.Duration) (*httptest.Server, *rangeRequestRecorder) {
	recorder := &rangeRequestRecorder{
		tracker: tracker,
		delay:   delay,
	}
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodGet {
			writer.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		rangeValue := request.Header.Get("Range")
		done := recorder.record(rangeValue)
		defer done()
		if recorder.delay > 0 {
			time.Sleep(recorder.delay)
		}
		if rangeValue == "" {
			_, _ = writer.Write(data)
			return
		}

		writeRangeResponse(writer, rangeValue, data)
	}))
	return server, recorder
}

func writeRangeResponse(writer http.ResponseWriter, rangeValue string, data []byte) {
	var start, end uint64
	if _, err := fmt.Sscanf(rangeValue, "bytes=%d-%d", &start, &end); err != nil {
		writer.WriteHeader(http.StatusBadRequest)
		return
	}
	if start >= uint64(len(data)) || end < start {
		writer.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
		return
	}
	if end >= uint64(len(data)) {
		end = uint64(len(data)) - 1
	}
	writer.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(data)))
	writer.WriteHeader(http.StatusPartialContent)
	_, _ = writer.Write(data[start : end+1])
}

func (r *rangeRequestRecorder) record(rangeValue string) func() {
	r.mu.Lock()
	r.ranges = append(r.ranges, rangeValue)
	r.mu.Unlock()
	if r.tracker == nil {
		return func() {}
	}
	return r.tracker.begin()
}

func (r *rangeRequestRecorder) rangesSnapshot() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.ranges...)
}

func (t *rangeConcurrencyTracker) begin() func() {
	t.mu.Lock()
	t.active++
	if t.active > t.peak {
		t.peak = t.active
	}
	t.mu.Unlock()
	return func() {
		t.mu.Lock()
		t.active--
		t.mu.Unlock()
	}
}

func (t *rangeConcurrencyTracker) peakActive() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.peak
}

func rangeHeader(offset uint64, length uint64) string {
	return fmt.Sprintf("bytes=%d-%d", offset, offset+length-1)
}
