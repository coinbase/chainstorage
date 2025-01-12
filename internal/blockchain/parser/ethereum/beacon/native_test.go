package beacon

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"
	"go.uber.org/fx"

	"github.com/coinbase/chainstorage/internal/blockchain/parser/internal"
	"github.com/coinbase/chainstorage/internal/utils/fixtures"
	"github.com/coinbase/chainstorage/internal/utils/testapp"
	"github.com/coinbase/chainstorage/internal/utils/testutil"
	"github.com/coinbase/chainstorage/protos/coinbase/c3/common"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

type parserTestSuite struct {
	suite.Suite

	ctrl    *gomock.Controller
	testapp testapp.TestApp
	parser  internal.Parser
}

func TestParserTestSuite(t *testing.T) {
	suite.Run(t, new(parserTestSuite))
}

func (s *parserTestSuite) SetupTest() {
	s.ctrl = gomock.NewController(s.T())

	var parser internal.Parser
	s.testapp = testapp.New(
		s.T(),
		Module,
		internal.Module,
		testapp.WithBlockchainNetworkSidechain(common.Blockchain_BLOCKCHAIN_ETHEREUM, common.Network_NETWORK_ETHEREUM_HOLESKY, api.SideChain_SIDECHAIN_ETHEREUM_HOLESKY_BEACON),
		fx.Populate(&parser),
	)

	s.parser = parser
	s.NotNil(s.parser)
}

func (s *parserTestSuite) TearDownTest() {
	s.testapp.Close()
	s.ctrl.Finish()
}

func (s *parserTestSuite) TestParseBeaconBlock() {
	require := testutil.Require(s.T())

	block := &api.Block{
		Blockchain: common.Blockchain_BLOCKCHAIN_ETHEREUM,
		Network:    common.Network_NETWORK_ETHEREUM_HOLESKY,
		SideChain:  api.SideChain_SIDECHAIN_ETHEREUM_HOLESKY_BEACON,
		Metadata: &api.BlockMetadata{
			Tag:        1,
			Hash:       "0xbf0bf1a2d342ac5a0d84ea0e2a2fc7d3d7b0fff2c221dc643bb1f9933401adc0",
			ParentHash: "0xcbe950dda3533e3c257fd162b33d791f9073eb42e4da21def569451e9323c33e",
			Height:     100,
			Timestamp:  testutil.MustTimestamp("2023-09-28T12:20:00Z"),
		},
		Blobdata: &api.Block_EthereumBeacon{
			EthereumBeacon: &api.EthereumBeaconBlobdata{
				Header: fixtures.MustReadFile("client/ethereum/holesky/beacon/header_100.json"),
				Block:  fixtures.MustReadFile("client/ethereum/holesky/beacon/block_100.json"),
				Blobs:  fixtures.MustReadFile("client/ethereum/holesky/beacon/blobs_100.json"),
			},
		},
	}

	var expectedBlock api.NativeBlock
	err := fixtures.UnmarshalPB("parser/ethereum/holesky/beacon/native_block_100.json", &expectedBlock)
	require.NoError(err)

	nativeBlock, err := s.parser.ParseNativeBlock(context.Background(), block)
	require.NoError(err)
	require.Equal(expectedBlock.Blockchain, nativeBlock.Blockchain)
	require.Equal(expectedBlock.Network, nativeBlock.Network)
	require.Equal(expectedBlock.SideChain, nativeBlock.SideChain)
	require.Equal(expectedBlock.Timestamp, nativeBlock.Timestamp)
	require.Equal(expectedBlock.Skipped, nativeBlock.Skipped)
	require.Equal(expectedBlock.Height, nativeBlock.Height)
	require.Equal(expectedBlock.Hash, nativeBlock.Hash)
	require.Equal(expectedBlock.ParentHash, nativeBlock.ParentHash)

	actual := nativeBlock.GetEthereumBeacon()
	expected := expectedBlock.GetEthereumBeacon()
	require.NotNil(actual)
	require.Equal(expected.Header, actual.Header)
	require.Equal(expected.Block, actual.Block)
	require.Equal(expected.Blobs, actual.Blobs)
}

func (s *parserTestSuite) TestParseBeaconBlock_Genesis() {
	require := testutil.Require(s.T())

	block := &api.Block{
		Blockchain: common.Blockchain_BLOCKCHAIN_ETHEREUM,
		Network:    common.Network_NETWORK_ETHEREUM_HOLESKY,
		SideChain:  api.SideChain_SIDECHAIN_ETHEREUM_HOLESKY_BEACON,
		Metadata: &api.BlockMetadata{
			Tag:        1,
			Hash:       "0xab09edd9380f8451c3ff5c809821174a36dce606fea8b5ea35ea936915dbf889",
			ParentHash: "0x0000000000000000000000000000000000000000000000000000000000000000",
			Height:     0,
			Timestamp:  testutil.MustTimestamp("2023-09-28T12:00:00Z"),
		},
		Blobdata: &api.Block_EthereumBeacon{
			EthereumBeacon: &api.EthereumBeaconBlobdata{
				Header: fixtures.MustReadFile("client/ethereum/holesky/beacon/header_0.json"),
				Block:  fixtures.MustReadFile("client/ethereum/holesky/beacon/block_0.json"),
				Blobs:  fixtures.MustReadFile("client/ethereum/holesky/beacon/blobs_empty_list.json"),
			},
		},
	}

	var expectedBlock api.NativeBlock
	err := fixtures.UnmarshalPB("parser/ethereum/holesky/beacon/native_block_0.json", &expectedBlock)
	require.NoError(err)

	nativeBlock, err := s.parser.ParseNativeBlock(context.Background(), block)
	require.NoError(err)
	require.Equal(expectedBlock.Blockchain, nativeBlock.Blockchain)
	require.Equal(expectedBlock.Network, nativeBlock.Network)
	require.Equal(expectedBlock.SideChain, nativeBlock.SideChain)
	require.Equal(expectedBlock.Timestamp, nativeBlock.Timestamp)
	require.Equal(expectedBlock.Skipped, nativeBlock.Skipped)
	require.Equal(expectedBlock.Height, nativeBlock.Height)
	require.Equal(expectedBlock.Hash, nativeBlock.Hash)
	require.Equal(expectedBlock.ParentHash, nativeBlock.ParentHash)

	actual := nativeBlock.GetEthereumBeacon()
	expected := expectedBlock.GetEthereumBeacon()
	require.NotNil(actual)
	require.Equal(expected.Header, actual.Header)
	require.Equal(expected.Block, actual.Block)
}

func (s *parserTestSuite) TestParseBeaconBlock_UnknownVersion() {
	require := testutil.Require(s.T())

	block := &api.Block{
		Blockchain: common.Blockchain_BLOCKCHAIN_ETHEREUM,
		Network:    common.Network_NETWORK_ETHEREUM_HOLESKY,
		SideChain:  api.SideChain_SIDECHAIN_ETHEREUM_HOLESKY_BEACON,
		Metadata: &api.BlockMetadata{
			Tag:        1,
			Hash:       "0xbf0bf1a2d342ac5a0d84ea0e2a2fc7d3d7b0fff2c221dc643bb1f9933401adc0",
			ParentHash: "0xcbe950dda3533e3c257fd162b33d791f9073eb42e4da21def569451e9323c33e",
			Height:     100,
			Timestamp:  testutil.MustTimestamp("2023-09-28T12:20:00Z"),
		},
		Blobdata: &api.Block_EthereumBeacon{
			EthereumBeacon: &api.EthereumBeaconBlobdata{
				Header: fixtures.MustReadFile("client/ethereum/holesky/beacon/header_100.json"),
				Block:  fixtures.MustReadFile("client/ethereum/holesky/beacon/block_unknown_version.json"),
			},
		},
	}

	_, err := s.parser.ParseNativeBlock(context.Background(), block)
	require.Error(err)
	require.Contains(err.Error(), "failed to parse block version")
}

func (s *parserTestSuite) TestParseBeaconBlock_Skipped() {
	require := testutil.Require(s.T())

	block := &api.Block{
		Blockchain: common.Blockchain_BLOCKCHAIN_ETHEREUM,
		Network:    common.Network_NETWORK_ETHEREUM_HOLESKY,
		SideChain:  api.SideChain_SIDECHAIN_ETHEREUM_HOLESKY_BEACON,
		Metadata: &api.BlockMetadata{
			Tag:     1,
			Height:  100,
			Skipped: true,
		},
		Blobdata: nil,
	}

	nativeBlock, err := s.parser.ParseNativeBlock(context.Background(), block)
	require.NoError(err)
	require.Equal(common.Blockchain_BLOCKCHAIN_ETHEREUM, nativeBlock.Blockchain)
	require.Equal(common.Network_NETWORK_ETHEREUM_HOLESKY, nativeBlock.Network)
	require.Equal(api.SideChain_SIDECHAIN_ETHEREUM_HOLESKY_BEACON, nativeBlock.SideChain)
	require.Equal(true, nativeBlock.Skipped)
	require.Equal(uint64(100), nativeBlock.Height)

	actual := nativeBlock.GetEthereumBeacon()
	require.Nil(actual)
}

func (s *parserTestSuite) TestParseBeaconBlock_MissBlockData() {
	require := testutil.Require(s.T())

	block := &api.Block{
		Blockchain: common.Blockchain_BLOCKCHAIN_ETHEREUM,
		Network:    common.Network_NETWORK_ETHEREUM_HOLESKY,
		SideChain:  api.SideChain_SIDECHAIN_ETHEREUM_HOLESKY_BEACON,
		Metadata: &api.BlockMetadata{
			Tag:        1,
			Hash:       "0xbf0bf1a2d342ac5a0d84ea0e2a2fc7d3d7b0fff2c221dc643bb1f9933401adc0",
			ParentHash: "0xcbe950dda3533e3c257fd162b33d791f9073eb42e4da21def569451e9323c33e",
			Height:     100,
			Timestamp:  testutil.MustTimestamp("2023-09-28T12:20:00Z"),
		},
		Blobdata: &api.Block_EthereumBeacon{
			EthereumBeacon: &api.EthereumBeaconBlobdata{
				Header: fixtures.MustReadFile("client/ethereum/holesky/beacon/header_100.json"),
			},
		},
	}

	_, err := s.parser.ParseNativeBlock(context.Background(), block)
	require.Error(err)
	require.Contains(err.Error(), "block data is empty")
}

func (s *parserTestSuite) TestParseBeaconBlockHeader_MismatchBlockHash() {
	require := testutil.Require(s.T())

	block := &api.Block{
		Blockchain: common.Blockchain_BLOCKCHAIN_ETHEREUM,
		Network:    common.Network_NETWORK_ETHEREUM_HOLESKY,
		SideChain:  api.SideChain_SIDECHAIN_ETHEREUM_HOLESKY_BEACON,
		Metadata: &api.BlockMetadata{
			Tag:        1,
			Hash:       "0x000",
			ParentHash: "0xcbe950dda3533e3c257fd162b33d791f9073eb42e4da21def569451e9323c33e",
			Height:     100,
			Timestamp:  testutil.MustTimestamp("2023-09-28T12:20:00Z"),
		},
		Blobdata: &api.Block_EthereumBeacon{
			EthereumBeacon: &api.EthereumBeaconBlobdata{
				Header: fixtures.MustReadFile("client/ethereum/holesky/beacon/header_100.json"),
			},
		},
	}

	_, err := s.parser.ParseNativeBlock(context.Background(), block)
	require.Error(err)
	require.Contains(err.Error(), "block root=0xbf0bf1a2d342ac5a0d84ea0e2a2fc7d3d7b0fff2c221dc643bb1f9933401adc0 does not match metadata in header")
}

func (s *parserTestSuite) TestParseBeaconBlockHeader_MismatchSlot() {
	require := testutil.Require(s.T())

	block := &api.Block{
		Blockchain: common.Blockchain_BLOCKCHAIN_ETHEREUM,
		Network:    common.Network_NETWORK_ETHEREUM_HOLESKY,
		SideChain:  api.SideChain_SIDECHAIN_ETHEREUM_HOLESKY_BEACON,
		Metadata: &api.BlockMetadata{
			Tag:        1,
			Hash:       "0xbf0bf1a2d342ac5a0d84ea0e2a2fc7d3d7b0fff2c221dc643bb1f9933401adc0",
			ParentHash: "0xcbe950dda3533e3c257fd162b33d791f9073eb42e4da21def569451e9323c33e",
			Height:     10,
			Timestamp:  testutil.MustTimestamp("2023-09-28T12:20:00Z"),
		},
		Blobdata: &api.Block_EthereumBeacon{
			EthereumBeacon: &api.EthereumBeaconBlobdata{
				Header: fixtures.MustReadFile("client/ethereum/holesky/beacon/header_100.json"),
			},
		},
	}

	_, err := s.parser.ParseNativeBlock(context.Background(), block)
	require.Error(err)
	require.Contains(err.Error(), "block slot=100 does not match metadata in header")
}

func (s *parserTestSuite) TestParseBeaconBlock_MismatchBlobsHeader() {
	require := testutil.Require(s.T())

	block := &api.Block{
		Blockchain: common.Blockchain_BLOCKCHAIN_ETHEREUM,
		Network:    common.Network_NETWORK_ETHEREUM_HOLESKY,
		SideChain:  api.SideChain_SIDECHAIN_ETHEREUM_HOLESKY_BEACON,
		Metadata: &api.BlockMetadata{
			Tag:        1,
			Hash:       "0xbf0bf1a2d342ac5a0d84ea0e2a2fc7d3d7b0fff2c221dc643bb1f9933401adc0",
			ParentHash: "0xcbe950dda3533e3c257fd162b33d791f9073eb42e4da21def569451e9323c33e",
			Height:     100,
			Timestamp:  testutil.MustTimestamp("2023-09-28T12:20:00Z"),
		},
		Blobdata: &api.Block_EthereumBeacon{
			EthereumBeacon: &api.EthereumBeaconBlobdata{
				Header: fixtures.MustReadFile("client/ethereum/holesky/beacon/header_100.json"),
				Block:  fixtures.MustReadFile("client/ethereum/holesky/beacon/block_100.json"),
				Blobs:  fixtures.MustReadFile("client/ethereum/holesky/beacon/blobs_10.json"),
			},
		},
	}

	_, err := s.parser.ParseNativeBlock(context.Background(), block)
	require.Error(err)
	require.Contains(err.Error(), "blob slot=10 does not match metadata")
}

func (s *parserTestSuite) TestParseBeaconBlock_MismatchBlobsSize() {
	require := testutil.Require(s.T())

	block := &api.Block{
		Blockchain: common.Blockchain_BLOCKCHAIN_ETHEREUM,
		Network:    common.Network_NETWORK_ETHEREUM_HOLESKY,
		SideChain:  api.SideChain_SIDECHAIN_ETHEREUM_HOLESKY_BEACON,
		Metadata: &api.BlockMetadata{
			Tag:        1,
			Hash:       "0xbf0bf1a2d342ac5a0d84ea0e2a2fc7d3d7b0fff2c221dc643bb1f9933401adc0",
			ParentHash: "0xcbe950dda3533e3c257fd162b33d791f9073eb42e4da21def569451e9323c33e",
			Height:     100,
			Timestamp:  testutil.MustTimestamp("2023-09-28T12:20:00Z"),
		},
		Blobdata: &api.Block_EthereumBeacon{
			EthereumBeacon: &api.EthereumBeaconBlobdata{
				Header: fixtures.MustReadFile("client/ethereum/holesky/beacon/header_100.json"),
				Block:  fixtures.MustReadFile("client/ethereum/holesky/beacon/block_100.json"),
				Blobs:  fixtures.MustReadFile("client/ethereum/holesky/beacon/blobs_empty_list.json"),
			},
		},
	}

	_, err := s.parser.ParseNativeBlock(context.Background(), block)
	require.Error(err)
	require.Contains(err.Error(), "blob count=0 does not match blobKzgCommitments count=1")
}

func (s *parserTestSuite) TestParseBeaconBlock_MissBlobs() {
	require := testutil.Require(s.T())

	block := &api.Block{
		Blockchain: common.Blockchain_BLOCKCHAIN_ETHEREUM,
		Network:    common.Network_NETWORK_ETHEREUM_HOLESKY,
		SideChain:  api.SideChain_SIDECHAIN_ETHEREUM_HOLESKY_BEACON,
		Metadata: &api.BlockMetadata{
			Tag:        1,
			Hash:       "0xbf0bf1a2d342ac5a0d84ea0e2a2fc7d3d7b0fff2c221dc643bb1f9933401adc0",
			ParentHash: "0xcbe950dda3533e3c257fd162b33d791f9073eb42e4da21def569451e9323c33e",
			Height:     100,
			Timestamp:  testutil.MustTimestamp("2023-09-28T12:20:00Z"),
		},
		Blobdata: &api.Block_EthereumBeacon{
			EthereumBeacon: &api.EthereumBeaconBlobdata{
				Header: fixtures.MustReadFile("client/ethereum/holesky/beacon/header_100.json"),
				Block:  fixtures.MustReadFile("client/ethereum/holesky/beacon/block_100.json"),
			},
		},
	}

	_, err := s.parser.ParseNativeBlock(context.Background(), block)
	require.Error(err)
	require.Contains(err.Error(), "blobs data is empty but blobKzgCommitments is not, expected=1")
}

func (s *parserTestSuite) TestParseBeaconBlock_MismatchBlobKzgCommitment() {
	require := testutil.Require(s.T())

	block := &api.Block{
		Blockchain: common.Blockchain_BLOCKCHAIN_ETHEREUM,
		Network:    common.Network_NETWORK_ETHEREUM_HOLESKY,
		SideChain:  api.SideChain_SIDECHAIN_ETHEREUM_HOLESKY_BEACON,
		Metadata: &api.BlockMetadata{
			Tag:        1,
			Hash:       "0xbf0bf1a2d342ac5a0d84ea0e2a2fc7d3d7b0fff2c221dc643bb1f9933401adc0",
			ParentHash: "0xcbe950dda3533e3c257fd162b33d791f9073eb42e4da21def569451e9323c33e",
			Height:     100,
			Timestamp:  testutil.MustTimestamp("2023-09-28T12:20:00Z"),
		},
		Blobdata: &api.Block_EthereumBeacon{
			EthereumBeacon: &api.EthereumBeaconBlobdata{
				Header: fixtures.MustReadFile("client/ethereum/holesky/beacon/header_100.json"),
				Block:  fixtures.MustReadFile("client/ethereum/holesky/beacon/block_100_incorrect_kzg.json"),
				Blobs:  fixtures.MustReadFile("client/ethereum/holesky/beacon/blobs_100.json"),
			},
		},
	}

	_, err := s.parser.ParseNativeBlock(context.Background(), block)
	require.Error(err)
	require.Contains(err.Error(), "KzgCommitment does not match on blob ")
}

func (s *parserTestSuite) TestParseBeaconBlock_Blobs_Null() {
	// For historical blocks persisted before blobs API was introduced, their blobs field is null.
	require := testutil.Require(s.T())

	block := &api.Block{
		Blockchain: common.Blockchain_BLOCKCHAIN_ETHEREUM,
		Network:    common.Network_NETWORK_ETHEREUM_HOLESKY,
		SideChain:  api.SideChain_SIDECHAIN_ETHEREUM_HOLESKY_BEACON,
		Metadata: &api.BlockMetadata{
			Tag:        1,
			Hash:       "0xab09edd9380f8451c3ff5c809821174a36dce606fea8b5ea35ea936915dbf889",
			ParentHash: "0x0000000000000000000000000000000000000000000000000000000000000000",
			Height:     0,
			Timestamp:  testutil.MustTimestamp("2023-09-28T12:00:00Z"),
		},
		Blobdata: &api.Block_EthereumBeacon{
			EthereumBeacon: &api.EthereumBeaconBlobdata{
				Header: fixtures.MustReadFile("client/ethereum/holesky/beacon/header_0.json"),
				Block:  fixtures.MustReadFile("client/ethereum/holesky/beacon/block_0.json"),
			},
		},
	}

	nativeBlock, err := s.parser.ParseNativeBlock(context.Background(), block)
	require.NoError(err)

	actual := nativeBlock.GetEthereumBeacon()
	require.NotNil(actual)
	require.NotNil(actual.Header)
	require.NotNil(actual.Block)
	require.Nil(actual.Blobs)
}

func (s *parserTestSuite) TestParseBeaconBlock_Blobs_EmptyList() {
	// For blocks persisted after blobs API was introduced, blobs of skipped blocks and without blobs is empty list.

	require := testutil.Require(s.T())

	block := &api.Block{
		Blockchain: common.Blockchain_BLOCKCHAIN_ETHEREUM,
		Network:    common.Network_NETWORK_ETHEREUM_HOLESKY,
		SideChain:  api.SideChain_SIDECHAIN_ETHEREUM_HOLESKY_BEACON,
		Metadata: &api.BlockMetadata{
			Tag:        1,
			Hash:       "0xbf0bf1a2d342ac5a0d84ea0e2a2fc7d3d7b0fff2c221dc643bb1f9933401adc0",
			ParentHash: "0xcbe950dda3533e3c257fd162b33d791f9073eb42e4da21def569451e9323c33e",
			Height:     100,
			Timestamp:  testutil.MustTimestamp("2023-09-28T12:20:00Z"),
		},
		Blobdata: &api.Block_EthereumBeacon{
			EthereumBeacon: &api.EthereumBeaconBlobdata{
				Header: fixtures.MustReadFile("client/ethereum/holesky/beacon/header_100.json"),
				Block:  fixtures.MustReadFile("client/ethereum/holesky/beacon/block_100_missing_kzg_commitments.json"),
				Blobs:  fixtures.MustReadFile("client/ethereum/holesky/beacon/blobs_empty_list.json"),
			},
		},
	}

	nativeBlock, err := s.parser.ParseNativeBlock(context.Background(), block)
	require.NoError(err)

	actual := nativeBlock.GetEthereumBeacon()
	require.NotNil(actual)
	require.NotNil(actual.Header)
	require.NotNil(actual.Block)
	require.Equal(0, len(actual.Blobs))
}
