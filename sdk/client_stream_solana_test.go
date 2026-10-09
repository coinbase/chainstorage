package sdk

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/suite"
	"go.uber.org/fx"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/proto"

	"github.com/coinbase/chainstorage/internal/blockchain/parser"
	"github.com/coinbase/chainstorage/internal/gateway"
	"github.com/coinbase/chainstorage/internal/storage/blobstorage/downloader"
	downloadermocks "github.com/coinbase/chainstorage/internal/storage/blobstorage/downloader/mocks"
	"github.com/coinbase/chainstorage/internal/utils/fixtures"
	"github.com/coinbase/chainstorage/internal/utils/testapp"
	"github.com/coinbase/chainstorage/internal/utils/testutil"
	"github.com/coinbase/chainstorage/protos/coinbase/c3/common"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
	apimocks "github.com/coinbase/chainstorage/protos/coinbase/chainstorage/mocks"
)

// streamSolanaClientSuite brings up an SDK Client wired for
// solana-mainnet so StreamNativeBlock streams through the Solana parser.
type streamSolanaClientSuite struct {
	suite.Suite

	ctrl             *gomock.Controller
	app              testapp.TestApp
	gatewayClient    *apimocks.MockChainStorageClient
	downloaderClient *downloadermocks.MockBlockDownloader
	client           Client
	parser           parser.Parser
	require          *testutil.Assertions
}

func TestStreamSolanaClientSuite(t *testing.T) {
	suite.Run(t, new(streamSolanaClientSuite))
}

func (s *streamSolanaClientSuite) SetupTest() {
	s.ctrl = gomock.NewController(s.T())
	s.require = testutil.Require(s.T())
	s.gatewayClient = apimocks.NewMockChainStorageClient(s.ctrl)
	s.downloaderClient = downloadermocks.NewMockBlockDownloader(s.ctrl)
	s.app = testapp.New(
		s.T(),
		Module,
		parser.Module,
		testapp.WithBlockchainNetwork(common.Blockchain_BLOCKCHAIN_SOLANA, common.Network_NETWORK_SOLANA_MAINNET),
		fx.Provide(func() downloader.BlockDownloader { return s.downloaderClient }),
		fx.Provide(func() gateway.Client { return s.gatewayClient }),
		fx.Populate(&s.client, &s.parser),
	)
	s.require.NotNil(s.client)
	s.require.NotNil(s.parser)
}

func (s *streamSolanaClientSuite) TearDownTest() {
	s.app.Close()
	s.ctrl.Finish()
}

const (
	solanaStreamTag    = uint32(2)
	solanaStreamHeight = uint64(241043141)
	solanaStreamHash   = "7UVhKXDoFXfQWHRRMNaXiEXiQsabDvU7oz4TRLHFuzd8"
)

// spoolFixtureBlock proto-marshals a Solana block built from the v2
// fixture into a temp file and returns the SpooledBlock the downloader
// mock hands back, plus the raw block for baseline comparison.
func (s *streamSolanaClientSuite) spoolFixtureBlock(bf *api.BlockFile) (*downloader.SpooledBlock, *api.Block) {
	header, err := fixtures.ReadFile("parser/solana/block_241043141_v2.json")
	s.require.NoError(err)
	rawBlock := &api.Block{
		Blockchain: common.Blockchain_BLOCKCHAIN_SOLANA,
		Network:    common.Network_NETWORK_SOLANA_MAINNET,
		Metadata: &api.BlockMetadata{
			Tag: solanaStreamTag, Height: solanaStreamHeight, Hash: solanaStreamHash,
			ParentHash: "8KrXYfWrGMBJg6owJ5U5X1c6rxh3iVxbybvSK5hbTZtA", ParentHeight: solanaStreamHeight - 1,
		},
		Blobdata: &api.Block_Solana{Solana: &api.SolanaBlobdata{Header: header}},
	}
	blockBytes, err := proto.Marshal(rawBlock)
	s.require.NoError(err)

	spoolFile, err := os.CreateTemp(s.T().TempDir(), "chainstorage-spool-*.bin")
	s.require.NoError(err)
	_, err = spoolFile.Write(blockBytes)
	s.require.NoError(err)
	s.require.NoError(spoolFile.Close())

	return &downloader.SpooledBlock{
		BlockFile: bf,
		Open: func() (io.ReadCloser, error) {
			return os.Open(spoolFile.Name())
		},
	}, rawBlock
}

// TestStreamNativeBlock_SolanaEndToEnd verifies that a solana-configured
// SDK client plumbs a streamed block through the downloader into the
// Solana parser's iterator and that the result matches the
// whole-block parse.
func (s *streamSolanaClientSuite) TestStreamNativeBlock_SolanaEndToEnd() {
	bf := &api.BlockFile{Tag: solanaStreamTag, Height: solanaStreamHeight, Hash: solanaStreamHash}
	spooled, rawBlock := s.spoolFixtureBlock(bf)
	s.gatewayClient.EXPECT().GetBlockFile(gomock.Any(), gomock.Any()).Return(&api.GetBlockFileResponse{File: bf}, nil)
	s.downloaderClient.EXPECT().DownloadStream(gomock.Any(), bf).Return(spooled, nil)

	baseline, err := s.parser.ParseNativeBlock(context.Background(), rawBlock)
	s.require.NoError(err)

	native, err := s.client.StreamNativeBlock(context.Background(), solanaStreamTag, solanaStreamHeight, solanaStreamHash)
	s.require.NoError(err)
	defer native.Close()

	s.require.Equal(solanaStreamHeight, native.GetMetadata().GetHeight())
	ss := native.GetSolana()
	s.require.NotNil(ss, "solana-configured client must populate GetSolana()")
	s.require.Nil(native.GetBitcoin(), "solana config must leave GetBitcoin() nil")
	s.require.Nil(native.GetEthereum(), "solana config must leave GetEthereum() nil")

	var txs []*api.SolanaTransactionV2
	for tx, iterErr := range ss.Transactions() {
		s.require.NoError(iterErr)
		txs = append(txs, tx)
	}
	base := baseline.GetSolanaV2()
	s.require.Equal(len(base.GetTransactions()), len(txs))
	for i := range txs {
		s.require.True(proto.Equal(base.GetTransactions()[i], txs[i]), "transaction %d", i)
	}
	header, err := ss.Header()
	s.require.NoError(err)
	s.require.Equal(solanaStreamHash, header.GetBlockHash())
	s.require.Equal(solanaStreamHeight, header.GetSlot())
}

// TestStreamNativeBlock_SolanaTransactionFilter verifies the SDK-level
// option reaches the walker.
func (s *streamSolanaClientSuite) TestStreamNativeBlock_SolanaTransactionFilter() {
	bf := &api.BlockFile{Tag: solanaStreamTag, Height: solanaStreamHeight, Hash: solanaStreamHash}
	spooled, _ := s.spoolFixtureBlock(bf)
	s.gatewayClient.EXPECT().GetBlockFile(gomock.Any(), gomock.Any()).Return(&api.GetBlockFileResponse{File: bf}, nil)
	s.downloaderClient.EXPECT().DownloadStream(gomock.Any(), bf).Return(spooled, nil)

	needle := []byte("Vote111111111111111111111111111111111111111")
	native, err := s.client.StreamNativeBlock(context.Background(), solanaStreamTag, solanaStreamHeight, solanaStreamHash,
		WithTransactionFilter(func(raw json.RawMessage) (bool, error) {
			return !bytes.Contains(raw, needle), nil
		}))
	s.require.NoError(err)
	defer native.Close()

	ss := native.GetSolana()
	s.require.NotNil(ss)
	count := 0
	for tx, iterErr := range ss.Transactions() {
		s.require.NoError(iterErr)
		for _, key := range tx.GetPayload().GetMessage().GetAccountKeys() {
			s.require.NotEqual(string(needle), key.GetPubkey(), "vote transactions must have been filtered out")
		}
		count++
	}
	s.require.Greater(count, 0)
}

// TestStreamNativeBlock_SolanaSkippedSlot: a skipped slot yields an
// empty stream rather than a nil accessor, so range walkers need no
// special case.
func (s *streamSolanaClientSuite) TestStreamNativeBlock_SolanaSkippedSlot() {
	bf := &api.BlockFile{Tag: solanaStreamTag, Height: solanaStreamHeight, Hash: solanaStreamHash, Skipped: true}
	spooled := &downloader.SpooledBlock{
		BlockFile: bf,
		Open: func() (io.ReadCloser, error) {
			return io.NopCloser(bytes.NewReader(nil)), nil
		},
	}
	s.gatewayClient.EXPECT().GetBlockFile(gomock.Any(), gomock.Any()).Return(&api.GetBlockFileResponse{File: bf}, nil)
	s.downloaderClient.EXPECT().DownloadStream(gomock.Any(), bf).Return(spooled, nil)

	native, err := s.client.StreamNativeBlock(context.Background(), solanaStreamTag, solanaStreamHeight, solanaStreamHash)
	s.require.NoError(err)
	defer native.Close()

	s.require.True(native.GetMetadata().GetSkipped())
	ss := native.GetSolana()
	s.require.NotNil(ss)
	for range ss.Transactions() {
		s.require.Fail("skipped slot must yield no transactions")
	}
	rewards, err := ss.Rewards()
	s.require.NoError(err)
	s.require.Empty(rewards)
	_, err = ss.Header()
	s.require.Error(err)
}
