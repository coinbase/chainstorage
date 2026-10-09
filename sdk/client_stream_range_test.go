package sdk

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"testing"

	"github.com/stretchr/testify/suite"
	"go.uber.org/fx"
	"go.uber.org/mock/gomock"
	"golang.org/x/xerrors"
	"google.golang.org/protobuf/proto"

	"github.com/coinbase/chainstorage/internal/blockchain/parser"
	"github.com/coinbase/chainstorage/internal/gateway"
	"github.com/coinbase/chainstorage/internal/storage/blobstorage/downloader"
	"github.com/coinbase/chainstorage/internal/storage/blobstorage/downloader/downloadertest"
	downloadermocks "github.com/coinbase/chainstorage/internal/storage/blobstorage/downloader/mocks"
	"github.com/coinbase/chainstorage/internal/utils/fixtures"
	"github.com/coinbase/chainstorage/internal/utils/testapp"
	"github.com/coinbase/chainstorage/internal/utils/testutil"
	"github.com/coinbase/chainstorage/protos/coinbase/c3/common"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
	apimocks "github.com/coinbase/chainstorage/protos/coinbase/chainstorage/mocks"
)

// streamRangeClientSuite wires a solana-mainnet SDK client to the REAL
// downloader over an httptest range server, so StreamNativeBlocksByRange
// is exercised end to end: gateway metadata (mocked) → CSCB range
// request → chunk-once extraction → Solana streaming parser.
type streamRangeClientSuite struct {
	suite.Suite

	ctrl          *gomock.Controller
	app           testapp.TestApp
	gatewayClient *apimocks.MockChainStorageClient
	object        *downloadertest.CSCBObject
	server        *downloadertest.RangeServer
	blocks        []*api.Block
	client        Client
	parser        parser.Parser
	require       *testutil.Assertions
}

func TestStreamRangeClientSuite(t *testing.T) {
	suite.Run(t, new(streamRangeClientSuite))
}

// Two real slots in one chunk. The heights are consecutive in the
// object even though the fixtures are not, because the walker only
// cares about the height the metadata carries.
var rangeFixtures = []struct {
	path string
	slot uint64
	hash string
}{
	{path: "parser/solana/block_241043141_v2.json", slot: 1000, hash: "7UVhKXDoFXfQWHRRMNaXiEXiQsabDvU7oz4TRLHFuzd8"},
	{path: "parser/solana/block_195545749_v2.json", slot: 1001, hash: "hash-1001"},
}

func (s *streamRangeClientSuite) SetupTest() {
	s.ctrl = gomock.NewController(s.T())
	s.require = testutil.Require(s.T())
	s.gatewayClient = apimocks.NewMockChainStorageClient(s.ctrl)

	s.blocks = make([]*api.Block, len(rangeFixtures))
	for i, f := range rangeFixtures {
		header, err := fixtures.ReadFile(f.path)
		s.require.NoError(err)
		s.blocks[i] = &api.Block{
			Blockchain: common.Blockchain_BLOCKCHAIN_SOLANA,
			Network:    common.Network_NETWORK_SOLANA_MAINNET,
			Metadata:   &api.BlockMetadata{Tag: 2, Height: f.slot, Hash: f.hash, ParentHeight: f.slot - 1},
			Blobdata:   &api.Block_Solana{Solana: &api.SolanaBlobdata{Header: header}},
		}
	}
	s.object = downloadertest.EncodeCSCB(s.T(), s.blocks, uint64(len(s.blocks)))
	s.server = s.object.Serve(s.T())

	s.app = testapp.New(
		s.T(),
		Module,
		parser.Module,
		testapp.WithBlockchainNetwork(common.Blockchain_BLOCKCHAIN_SOLANA, common.Network_NETWORK_SOLANA_MAINNET),
		fx.Provide(func() downloader.HTTPClient { return s.server.Client() }),
		fx.Provide(downloader.NewBlockDownloader),
		fx.Provide(func() gateway.Client { return s.gatewayClient }),
		fx.Populate(&s.client, &s.parser),
	)
}

func (s *streamRangeClientSuite) TearDownTest() {
	s.app.Close()
	s.ctrl.Finish()
}

func (s *streamRangeClientSuite) expectRange(readSource api.BlockReadSource, files []*api.BlockFile) {
	s.gatewayClient.EXPECT().
		GetBlockFilesByRange(gomock.Any(), &api.GetBlockFilesByRangeRequest{Tag: 2, StartHeight: 1000, EndHeight: 1002, ReadSource: readSource}).
		Return(&api.GetBlockFilesByRangeResponse{Files: files}, nil)
}

func (s *streamRangeClientSuite) TestStreamNativeBlocksByRange_ChunkOnceParity() {
	s.expectRange(api.BlockReadSource_BLOCK_READ_SOURCE_DEFAULT, s.object.BlockFiles)

	iter, err := s.client.StreamNativeBlocksByRange(context.Background(), 2, 1000, 1002)
	s.require.NoError(err)
	defer iter.Close()

	for i, rawBlock := range s.blocks {
		baseline, err := s.parser.ParseNativeBlock(context.Background(), rawBlock)
		s.require.NoError(err)

		native, err := iter.Next(context.Background())
		s.require.NoError(err, "block %d", i)
		s.require.Equal(rawBlock.GetMetadata().GetHeight(), native.GetMetadata().GetHeight())
		ss := native.GetSolana()
		s.require.NotNil(ss)

		var txs []*api.SolanaTransactionV2
		for tx, iterErr := range ss.Transactions() {
			s.require.NoError(iterErr)
			txs = append(txs, tx)
		}
		base := baseline.GetSolanaV2().GetTransactions()
		s.require.Equal(len(base), len(txs), "block %d", i)
		for j := range txs {
			s.require.True(proto.Equal(base[j], txs[j]), "block %d transaction %d", i, j)
		}
		header, err := ss.Header()
		s.require.NoError(err)
		s.require.Equal(rawBlock.GetMetadata().GetHeight(), header.GetSlot())
		s.require.NoError(native.Close())
	}
	_, err = iter.Next(context.Background())
	s.require.True(err == io.EOF, "end of range is exactly io.EOF: %v", err)

	s.require.Equal(1, s.server.CountRange(s.object.ChunkRange(0)), "both slots came out of one chunk request")
}

func (s *streamRangeClientSuite) TestStreamNativeBlocksByRange_FilterAppliesToEveryBlock() {
	s.expectRange(api.BlockReadSource_BLOCK_READ_SOURCE_DEFAULT, s.object.BlockFiles)

	seen := map[uint64]int{}
	var current uint64
	iter, err := s.client.StreamNativeBlocksByRange(context.Background(), 2, 1000, 1002,
		WithTransactionFilter(func(raw json.RawMessage) (bool, error) {
			seen[current]++
			return bytes.Contains(raw, []byte(`"Vote111111111111111111111111111111111111111"`)), nil
		}))
	s.require.NoError(err)
	defer iter.Close()

	for _, rawBlock := range s.blocks {
		current = rawBlock.GetMetadata().GetHeight()
		native, err := iter.Next(context.Background())
		s.require.NoError(err)
		kept := 0
		for raw, iterErr := range native.GetSolana().RawTransactions() {
			s.require.NoError(iterErr)
			s.require.True(bytes.Contains(raw, []byte("Vote111111111111111111111111111111111111111")))
			kept++
		}
		var decoded struct {
			Transactions []json.RawMessage `json:"transactions"`
		}
		s.require.NoError(json.Unmarshal(rawBlock.GetSolana().GetHeader(), &decoded))
		wantKept := 0
		for _, tx := range decoded.Transactions {
			if bytes.Contains(tx, []byte(`"Vote111111111111111111111111111111111111111"`)) {
				wantKept++
			}
		}
		s.require.Equal(len(decoded.Transactions), seen[current], "filter saw every transaction of slot %d", current)
		s.require.Equal(wantKept, kept, "slot %d yields exactly the transactions the filter kept", current)
		s.require.NoError(native.Close())
	}
	// The second fixture (1,446 transactions) is mostly votes; the first
	// has a single non-vote transaction, so both branches ran.
	s.require.Greater(seen[1001], 1000)
}

func (s *streamRangeClientSuite) TestStreamNativeBlocksByRange_SingleBlockFallback() {
	s.gatewayClient.EXPECT().
		GetBlockFilesByRange(gomock.Any(), &api.GetBlockFilesByRangeRequest{Tag: 2, StartHeight: 1000, EndHeight: 1001, ReadSource: api.BlockReadSource_BLOCK_READ_SOURCE_DEFAULT}).
		Return(nil, xerrors.New("consolidated metadata unavailable"))
	s.gatewayClient.EXPECT().
		GetBlockFilesByRange(gomock.Any(), &api.GetBlockFilesByRangeRequest{Tag: 2, StartHeight: 1000, EndHeight: 1001, ReadSource: api.BlockReadSource_BLOCK_READ_SOURCE_SINGLE_BLOCK}).
		Return(&api.GetBlockFilesByRangeResponse{Files: s.object.BlockFiles[:1]}, nil)

	// endHeight 0 means one block, matching OpenRawBlockPayloadsByRange.
	iter, err := s.client.StreamNativeBlocksByRange(context.Background(), 2, 1000, 0)
	s.require.NoError(err)
	defer iter.Close()
	native, err := iter.Next(context.Background())
	s.require.NoError(err)
	s.require.Equal(uint64(1000), native.GetMetadata().GetHeight())
	s.require.NoError(native.Close())
	_, err = iter.Next(context.Background())
	s.require.ErrorIs(err, io.EOF)
}

// TestStreamNativeBlocksByRange_DownloadFailureIsNotEOF is the walker's
// safety property: a chunk that cannot be read must never look like the
// end of the range, or a checkpointing consumer would skip it.
func (s *streamRangeClientSuite) TestStreamNativeBlocksByRange_DownloadFailureIsNotEOF() {
	s.expectRange(api.BlockReadSource_BLOCK_READ_SOURCE_DEFAULT, s.object.BlockFiles)
	s.server.TruncateAlways(s.object.ChunkRange(0), 64)

	iter, err := s.client.StreamNativeBlocksByRange(context.Background(), 2, 1000, 1002)
	s.require.NoError(err, "metadata succeeded; the failure is lazy")
	defer iter.Close()
	_, err = iter.Next(context.Background())
	s.require.Error(err)
	s.require.False(err == io.EOF)
	s.require.False(xerrors.Is(err, io.EOF), "%v", err)
	_, again := iter.Next(context.Background())
	s.require.Equal(err, again, "terminal error is sticky")
}

func (s *streamRangeClientSuite) TestStreamNativeBlocksByRange_BothSourcesFail() {
	s.gatewayClient.EXPECT().GetBlockFilesByRange(gomock.Any(), gomock.Any()).Return(nil, xerrors.New("down")).Times(2)
	iter, err := s.client.StreamNativeBlocksByRange(context.Background(), 2, 1000, 1002)
	s.require.Error(err)
	s.require.Nil(iter)
}

// TestStreamNativeBlocksByRange_SkippedSlot uses the downloader mock to
// hand back a skipped spool in the middle of the range and checks the
// iterator yields an empty Solana stream for it rather than stopping.
func TestStreamNativeBlocksByRange_SkippedSlot(t *testing.T) {
	require := testutil.Require(t)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	gatewayClient := apimocks.NewMockChainStorageClient(ctrl)
	downloaderClient := downloadermocks.NewMockBlockDownloader(ctrl)

	var client Client
	app := testapp.New(
		t,
		Module,
		parser.Module,
		testapp.WithBlockchainNetwork(common.Blockchain_BLOCKCHAIN_SOLANA, common.Network_NETWORK_SOLANA_MAINNET),
		fx.Provide(func() downloader.BlockDownloader { return downloaderClient }),
		fx.Provide(func() gateway.Client { return gatewayClient }),
		fx.Populate(&client),
	)
	defer app.Close()

	skipped := &api.BlockFile{Tag: 2, Height: 5, Skipped: true}
	gatewayClient.EXPECT().GetBlockFilesByRange(gomock.Any(), gomock.Any()).
		Return(&api.GetBlockFilesByRangeResponse{Files: []*api.BlockFile{skipped}}, nil)
	downloaderClient.EXPECT().OpenSpooledBlocks(gomock.Any(), []*api.BlockFile{skipped}).
		Return(&stubSpooledIterator{blocks: []*downloader.SpooledBlock{{
			BlockFile: skipped,
			Open:      func() (io.ReadCloser, error) { return io.NopCloser(bytes.NewReader(nil)), nil },
		}}}, nil)

	iter, err := client.StreamNativeBlocksByRange(context.Background(), 2, 5, 6)
	require.NoError(err)
	defer iter.Close()
	native, err := iter.Next(context.Background())
	require.NoError(err)
	require.True(native.GetMetadata().GetSkipped())
	ss := native.GetSolana()
	require.NotNil(ss, "skipped slots still expose an (empty) solana stream")
	for range ss.Transactions() {
		require.FailNow("skipped slot yielded a transaction")
	}
	require.NoError(native.Close())
	_, err = iter.Next(context.Background())
	require.ErrorIs(err, io.EOF)
}

type stubSpooledIterator struct {
	blocks []*downloader.SpooledBlock
	pos    int
}

func (s *stubSpooledIterator) Next(context.Context) (*downloader.SpooledBlock, error) {
	if s.pos >= len(s.blocks) {
		return nil, io.EOF
	}
	b := s.blocks[s.pos]
	s.pos++
	return b, nil
}

func (s *stubSpooledIterator) Close() error { return nil }
