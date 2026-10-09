package bitcoin

import (
	"context"
	"testing"

	"github.com/stretchr/testify/suite"
	"go.uber.org/fx"

	"github.com/coinbase/chainstorage/internal/blockchain/parser/internal"
	"github.com/coinbase/chainstorage/internal/utils/fixtures"
	"github.com/coinbase/chainstorage/internal/utils/testapp"
	"github.com/coinbase/chainstorage/internal/utils/testutil"
	"github.com/coinbase/chainstorage/protos/coinbase/c3/common"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

type dashNativeParserTestSuite struct {
	suite.Suite
	app    testapp.TestApp
	parser internal.NativeParser
}

func TestDashNativeParserTestSuite(t *testing.T) {
	suite.Run(t, new(dashNativeParserTestSuite))
}

func (s *dashNativeParserTestSuite) SetupTest() {
	s.app = testapp.New(s.T(),
		testapp.WithBlockchainNetwork(common.Blockchain_BLOCKCHAIN_DASH, common.Network_NETWORK_DASH_MAINNET),
		fx.Provide(NewDashNativeParser),
		fx.Populate(&s.parser),
	)
	s.NotNil(s.parser)
}

func (s *dashNativeParserTestSuite) TearDownTest() {
	s.app.Close()
}

func (s *dashNativeParserTestSuite) TestParseDashBlock_PubKeyAddress() {
	require := testutil.Require(s.T())

	// Dash P2PK outputs omit the "address" field; the parser must derive it
	// using Dash's P2PKH version byte (0x4c) to produce an X-prefixed address.
	header, err := fixtures.ReadFile("parser/bitcoin/dash_get_block_pubkey_case.json")
	require.NoError(err)

	block := &api.Block{
		Blockchain: common.Blockchain_BLOCKCHAIN_DASH,
		Network:    common.Network_NETWORK_DASH_MAINNET,
		Metadata:   bitcoinMetadata,
		Blobdata: &api.Block_Bitcoin{
			Bitcoin: &api.BitcoinBlobdata{
				Header: header,
			},
		},
	}

	nativeBlock, err := s.parser.ParseBlock(context.Background(), block)
	require.NoError(err)

	output := nativeBlock.GetBitcoin().Transactions[0].Outputs[0]
	require.Equal(bitcoinScriptTypePubKey, output.ScriptPublicKey.Type)
	// The derived address must use Dash's version byte (0x4c), producing an X-prefix.
	require.Equal("XbaQuXMDwGR6ns9LroMPCcEEuR4xr8L6ew", output.ScriptPublicKey.Address)
}

func (s *dashNativeParserTestSuite) TestParseDashBlock_AllowsMissingHash() {
	require := testutil.Require(s.T())

	// This fixture has transactions without the "hash" field.
	// Bitcoin parser rejects it, but Dash parser should accept it
	// because the preprocessor fills Hash from TxId before validation.
	header, err := fixtures.ReadFile("parser/bitcoin/errcases/get_block_err_txnohash.json")
	require.NoError(err)

	block := &api.Block{
		Blockchain: common.Blockchain_BLOCKCHAIN_DASH,
		Network:    common.Network_NETWORK_DASH_MAINNET,
		Metadata:   bitcoinMetadata,
		Blobdata: &api.Block_Bitcoin{
			Bitcoin: &api.BitcoinBlobdata{
				Header: header,
			},
		},
	}

	_, err = s.parser.ParseBlock(context.Background(), block)
	// The block may still fail for other reasons (e.g., missing input transactions),
	// but it should NOT fail with validation error for Hash.
	if err != nil {
		require.NotContains(err.Error(), "Key: 'BitcoinBlock.Tx[0].Hash'")
	}
}

func (s *dashNativeParserTestSuite) TestParseDashBlock_RejectsEmptyTxIdAndHash() {
	require := testutil.Require(s.T())

	// When both TxId and Hash are missing, the preprocessor cannot fill Hash,
	// so validation must reject it.
	header, err := fixtures.ReadFile("parser/bitcoin/errcases/get_block_err_txnoid.json")
	require.NoError(err)

	block := &api.Block{
		Blockchain: common.Blockchain_BLOCKCHAIN_DASH,
		Network:    common.Network_NETWORK_DASH_MAINNET,
		Metadata:   bitcoinMetadata,
		Blobdata: &api.Block_Bitcoin{
			Bitcoin: &api.BitcoinBlobdata{
				Header: header,
			},
		},
	}

	_, err = s.parser.ParseBlock(context.Background(), block)
	require.Error(err)
	require.Contains(err.Error(), "failed to validate bitcoin block")
}

func (s *dashNativeParserTestSuite) TestParseDashBlock_StillValidatesRequiredFields() {
	require := testutil.Require(s.T())

	tests := []struct {
		name     string
		file     string
		messages []string
	}{
		{
			name: "DashBlock_requires_hash",
			file: "parser/bitcoin/errcases/get_block_err_nohash.json",
			messages: []string{
				"failed to validate bitcoin block",
				"Key: 'BitcoinBlock.Hash'",
			},
		},
		{
			name: "DashBlock_requires_time",
			file: "parser/bitcoin/errcases/get_block_err_notime.json",
			messages: []string{
				"failed to validate bitcoin block",
				"Key: 'BitcoinBlock.Time'",
			},
		},
	}

	for _, test := range tests {
		require.NotContains(test.name, " ")
		s.T().Run(test.name, func(t *testing.T) {
			header, err := fixtures.ReadFile(test.file)
			require.NoError(err)

			block := &api.Block{
				Blockchain: common.Blockchain_BLOCKCHAIN_DASH,
				Network:    common.Network_NETWORK_DASH_MAINNET,
				Metadata:   bitcoinMetadata,
				Blobdata: &api.Block_Bitcoin{
					Bitcoin: &api.BitcoinBlobdata{
						Header: header,
					},
				},
			}

			_, err = s.parser.ParseBlock(context.Background(), block)
			require.Error(err)
			for _, msg := range test.messages {
				require.Contains(err.Error(), msg)
			}
		})
	}
}
