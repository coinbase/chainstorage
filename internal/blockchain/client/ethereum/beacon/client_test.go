package beacon

import (
	"context"
	"fmt"
	"math"
	"net/http"
	"testing"

	"github.com/golang/protobuf/ptypes/timestamp"
	"github.com/stretchr/testify/suite"
	"go.uber.org/fx"
	"go.uber.org/mock/gomock"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/blockchain/client/internal"
	"github.com/coinbase/chainstorage/internal/blockchain/parser"
	"github.com/coinbase/chainstorage/internal/blockchain/restapi"
	restapimocks "github.com/coinbase/chainstorage/internal/blockchain/restapi/mocks"
	"github.com/coinbase/chainstorage/internal/dlq"
	"github.com/coinbase/chainstorage/internal/utils/fixtures"
	"github.com/coinbase/chainstorage/internal/utils/testapp"
	"github.com/coinbase/chainstorage/internal/utils/testutil"
	"github.com/coinbase/chainstorage/protos/coinbase/c3/common"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

type (
	clientTestSuite struct {
		suite.Suite

		ctrl       *gomock.Controller
		app        testapp.TestApp
		restClient *restapimocks.MockClient
		client     internal.Client
	}
)

const (
	beaconTag    uint32 = 1
	beaconHeight uint64 = 100
	beaconHash          = "0xbf0bf1a2d342ac5a0d84ea0e2a2fc7d3d7b0fff2c221dc643bb1f9933401adc0"

	timestamp100 = "2023-09-28T12:20:00Z"
	timestamp101 = "2023-09-28T12:20:12Z"
)

func TestEthereumBeaconClientTestSuite(t *testing.T) {
	suite.Run(t, new(clientTestSuite))
}

func (s *clientTestSuite) SetupTest() {
	s.ctrl = gomock.NewController(s.T())
	s.restClient = restapimocks.NewMockClient(s.ctrl)

	var result internal.ClientParams
	s.app = testapp.New(
		s.T(),
		testapp.WithBlockchainNetworkSidechain(common.Blockchain_BLOCKCHAIN_ETHEREUM, common.Network_NETWORK_ETHEREUM_HOLESKY, api.SideChain_SIDECHAIN_ETHEREUM_HOLESKY_BEACON),
		Module,
		testRestModule(s.restClient),
		fx.Populate(&result),
	)

	s.client = result.Master
	s.NotNil(s.client)
}

func (s *clientTestSuite) TearDownTest() {
	s.app.Close()
	s.ctrl.Finish()
}

func (s *clientTestSuite) TestEthereumBeaconClient_New() {
	var result restapi.ClientParams
	app := testapp.New(
		s.T(),
		Module,
		internal.Module,
		restapi.Module,
		testapp.WithBlockchainNetworkSidechain(common.Blockchain_BLOCKCHAIN_ETHEREUM, common.Network_NETWORK_ETHEREUM_HOLESKY, api.SideChain_SIDECHAIN_ETHEREUM_HOLESKY_BEACON),
		fx.Provide(dlq.NewNop),
		fx.Provide(parser.NewNop),
		fx.Populate(&result),
	)
	defer app.Close()

	s.NotNil(result.Master)
	s.NotNil(result.Slave)
	s.NotNil(result.Validator)
	s.NotNil(result.Consensus)
}

func (s *clientTestSuite) TestEthereumBeacon_GetLatestBlock() {
	require := testutil.Require(s.T())

	headerResponse := fixtures.MustReadFile("client/ethereum/holesky/beacon/header_100.json")

	s.restClient.EXPECT().Call(gomock.Any(), gomock.Any(), nil).
		Times(1).
		DoAndReturn(func(ctx context.Context, method *restapi.RequestMethod, requestBody []byte) ([]byte, error) {
			require.Equal(getBlockHeaderMethodName, method.Name)
			require.Equal(getLatestBlockHeaderMethodPath, method.ParamsPath)
			return headerResponse, nil
		})

	latest, err := s.client.GetLatestHeight(context.Background())
	require.NoError(err)
	require.Equal(uint64(100), latest)
}

func (s *clientTestSuite) TestEthereumBeacon_GetLatestBlock_Failure() {
	require := testutil.Require(s.T())

	fakeErr := xerrors.Errorf("received http error: %w", &restapi.HTTPError{
		Code:     http.StatusInternalServerError,
		Response: "fake http error",
	})

	s.restClient.EXPECT().Call(gomock.Any(), gomock.Any(), nil).
		Times(1).
		DoAndReturn(func(ctx context.Context, method *restapi.RequestMethod, requestBody []byte) ([]byte, error) {
			require.Equal(getBlockHeaderMethodName, method.Name)
			require.Equal(getLatestBlockHeaderMethodPath, method.ParamsPath)
			return nil, fakeErr
		})

	_, err := s.client.GetLatestHeight(context.Background())
	require.Error(err)

	var errHTTP *restapi.HTTPError
	require.True(xerrors.As(err, &errHTTP))
	require.Equal(http.StatusInternalServerError, errHTTP.Code)
}

func (s *clientTestSuite) TestEthereumBeacon_BatchGetBlockMetadata() {
	require := testutil.Require(s.T())

	headerResponse1 := fixtures.MustReadFile("client/ethereum/holesky/beacon/header_100.json")
	headerResponse2 := fixtures.MustReadFile("client/ethereum/holesky/beacon/header_101.json")

	attempts := 0
	s.restClient.EXPECT().Call(gomock.Any(), gomock.Any(), nil).
		Times(2).
		DoAndReturn(func(ctx context.Context, method *restapi.RequestMethod, requestBody []byte) ([]byte, error) {
			require.Equal(getBlockHeaderMethodName, method.Name)

			attempts += 1
			blockHeight := beaconHeight + uint64(attempts) - 1
			require.Equal(fmt.Sprintf(getBlockHeaderMethodPath, blockHeight), method.ParamsPath)

			if attempts == 1 {
				return headerResponse1, nil
			} else {
				return headerResponse2, nil
			}
		})

	results, err := s.client.BatchGetBlockMetadata(context.Background(), beaconTag, beaconHeight, beaconHeight+2)
	require.NoError(err)
	require.Equal(2, len(results))

	result1 := results[0]
	require.NotEmpty(result1)
	require.Equal(beaconHeight, result1.Height)
	require.False(result1.Skipped)
	require.Equal(beaconTag, result1.Tag)
	require.Equal(uint64(0), result1.ParentHeight)
	require.Equal(beaconHash, result1.Hash)
	require.Equal("0xcbe950dda3533e3c257fd162b33d791f9073eb42e4da21def569451e9323c33e", result1.ParentHash)
	require.Equal(testutil.MustTimestamp(timestamp100), result1.Timestamp)

	result2 := results[1]
	require.NotEmpty(result2)
	require.Equal(uint64(beaconHeight+1), result2.Height)
	require.False(result2.Skipped)
	require.Equal(beaconTag, result2.Tag)
	require.Equal(uint64(0), result2.ParentHeight)
	require.Equal("0x00532b86ef78f73da656b65033a9dfaf8daf9fe121eee4d1f77cb556b3cd4f7b", result2.Hash)
	require.Equal(beaconHash, result2.ParentHash)
	require.Equal(testutil.MustTimestamp(timestamp101), result2.Timestamp)
}

func (s *clientTestSuite) TestEthereumBeacon_BatchGetBlockMetadata_SkippedBlock() {
	require := testutil.Require(s.T())

	fakeErr := xerrors.Errorf("fake http error: %w", &restapi.HTTPError{
		Code:     http.StatusNotFound,
		Response: "fake http error",
	})

	headerResponse1 := fixtures.MustReadFile("client/ethereum/holesky/beacon/header_100.json")

	attempts := 0
	s.restClient.EXPECT().Call(gomock.Any(), gomock.Any(), nil).
		Times(2).
		DoAndReturn(func(ctx context.Context, method *restapi.RequestMethod, requestBody []byte) ([]byte, error) {
			require.Equal(getBlockHeaderMethodName, method.Name)

			attempts += 1
			blockHeight := beaconHeight + uint64(attempts) - 1
			require.Equal(fmt.Sprintf(getBlockHeaderMethodPath, blockHeight), method.ParamsPath)

			if attempts == 1 {
				return headerResponse1, nil
			} else {
				return nil, fakeErr
			}
		})

	results, err := s.client.BatchGetBlockMetadata(context.Background(), beaconTag, beaconHeight, beaconHeight+2)
	require.NoError(err)
	require.Equal(2, len(results))

	result1 := results[0]
	require.NotEmpty(result1)
	require.Equal(beaconHeight, result1.Height)
	require.False(result1.Skipped)
	require.Equal(beaconTag, result1.Tag)
	require.Equal(uint64(0), result1.ParentHeight)
	require.Equal(beaconHash, result1.Hash)
	require.Equal("0xcbe950dda3533e3c257fd162b33d791f9073eb42e4da21def569451e9323c33e", result1.ParentHash)
	require.Equal(testutil.MustTimestamp(timestamp100), result1.Timestamp)

	result2 := results[1]
	require.NotEmpty(result2)
	require.Equal(uint64(beaconHeight+1), result2.Height)
	require.True(result2.Skipped)
	require.Equal(beaconTag, result2.Tag)
	require.Equal(uint64(0), result2.ParentHeight)
	require.Equal("", result2.Hash)
	require.Equal("", result2.ParentHash)
	require.Equal(testutil.MustTimestamp(timestamp101), result2.Timestamp)
}

func (s *clientTestSuite) TestEthereumBeacon_GetBlockByHeight() {
	require := testutil.Require(s.T())

	headerResponse := fixtures.MustReadFile("client/ethereum/holesky/beacon/header_100.json")
	blockResponse := fixtures.MustReadFile("client/ethereum/holesky/beacon/block_100.json")
	blobsResponse := fixtures.MustReadFile("client/ethereum/holesky/beacon/blobs_100.json")

	s.restClient.EXPECT().Call(gomock.Any(), gomock.Any(), nil).
		Times(1).
		DoAndReturn(func(ctx context.Context, method *restapi.RequestMethod, requestBody []byte) ([]byte, error) {
			require.Equal(getBlockHeaderMethodName, method.Name)
			require.Equal(fmt.Sprintf(getBlockHeaderMethodPath, beaconHeight), method.ParamsPath)
			return headerResponse, nil
		})

	s.restClient.EXPECT().Call(gomock.Any(), gomock.Any(), nil).
		Times(1).
		DoAndReturn(func(ctx context.Context, method *restapi.RequestMethod, requestBody []byte) ([]byte, error) {
			require.Equal(getBlockMethodName, method.Name)
			require.Equal(fmt.Sprintf(getBlockMethodPath, beaconHash), method.ParamsPath)
			return blockResponse, nil
		})

	s.restClient.EXPECT().Call(gomock.Any(), gomock.Any(), nil).
		Times(1).
		DoAndReturn(func(ctx context.Context, method *restapi.RequestMethod, requestBody []byte) ([]byte, error) {
			require.Equal(getBlockBlobsMethodName, method.Name)
			require.Equal(fmt.Sprintf(getBlockBlobsMethodPath, beaconHash), method.ParamsPath)
			return blobsResponse, nil
		})

	block, err := s.client.GetBlockByHeight(context.Background(), beaconTag, beaconHeight)
	require.NoError(err)

	require.Equal(common.Blockchain_BLOCKCHAIN_ETHEREUM, block.Blockchain)
	require.Equal(common.Network_NETWORK_ETHEREUM_HOLESKY, block.Network)
	require.Equal(api.SideChain_SIDECHAIN_ETHEREUM_HOLESKY_BEACON, block.SideChain)

	// Block metadata
	metadata := block.Metadata
	require.NotNil(metadata)
	require.Equal(beaconTag, metadata.Tag)
	require.Equal(beaconHash, metadata.Hash)
	require.Equal("0xcbe950dda3533e3c257fd162b33d791f9073eb42e4da21def569451e9323c33e", metadata.ParentHash)
	require.Equal(beaconHeight, metadata.Height)
	require.Equal(uint64(0), metadata.ParentHeight)
	require.False(metadata.Skipped)
	require.Equal(testutil.MustTimestamp(timestamp100), metadata.Timestamp)

	// Block blob data
	blobdata := block.GetEthereumBeacon()
	require.NotNil(blobdata)
	require.NotEmpty(blobdata.Header)
	require.NotEmpty(blobdata.Block)
	require.NotEmpty(blobdata.Blobs)
}

func (s *clientTestSuite) TestEthereumBeacon_GetBlockByHeight_SkippedBlock() {
	require := testutil.Require(s.T())

	fakeErr := xerrors.Errorf("fake http error: %w", &restapi.HTTPError{
		Code:     http.StatusNotFound,
		Response: "fake http error",
	})

	s.restClient.EXPECT().Call(gomock.Any(), gomock.Any(), nil).
		Times(1).
		DoAndReturn(func(ctx context.Context, method *restapi.RequestMethod, requestBody []byte) ([]byte, error) {
			require.Equal(getBlockHeaderMethodName, method.Name)
			require.Equal(fmt.Sprintf(getBlockHeaderMethodPath, beaconHeight), method.ParamsPath)
			return nil, fakeErr
		})

	block, err := s.client.GetBlockByHeight(context.Background(), beaconTag, beaconHeight)
	require.NoError(err)

	require.Equal(common.Blockchain_BLOCKCHAIN_ETHEREUM, block.Blockchain)
	require.Equal(common.Network_NETWORK_ETHEREUM_HOLESKY, block.Network)
	require.Equal(api.SideChain_SIDECHAIN_ETHEREUM_HOLESKY_BEACON, block.SideChain)

	metadata := block.Metadata
	require.NotNil(metadata)
	require.Equal(beaconTag, metadata.Tag)
	require.Equal("", metadata.Hash)
	require.Equal("", metadata.ParentHash)
	require.Equal(beaconHeight, metadata.Height)
	require.Equal(uint64(0), metadata.ParentHeight)
	require.True(metadata.Skipped)
	require.Equal(testutil.MustTimestamp(timestamp100), metadata.Timestamp)

	blobdata := block.GetEthereumBeacon()
	require.Nil(blobdata)
}

func (s *clientTestSuite) TestEthereumBeacon_GetBlockByHeight_BlockNotFound() {
	require := testutil.Require(s.T())

	fakeErr := xerrors.Errorf("fake http error: %w", &restapi.HTTPError{
		Code:     http.StatusNotFound,
		Response: "fake http error",
	})

	headerResponse := fixtures.MustReadFile("client/ethereum/holesky/beacon/header_100.json")

	s.restClient.EXPECT().Call(gomock.Any(), gomock.Any(), nil).
		Times(1).
		DoAndReturn(func(ctx context.Context, method *restapi.RequestMethod, requestBody []byte) ([]byte, error) {
			require.Equal(getBlockHeaderMethodName, method.Name)
			require.Equal(fmt.Sprintf(getBlockHeaderMethodPath, beaconHeight), method.ParamsPath)
			return headerResponse, nil
		})

	s.restClient.EXPECT().Call(gomock.Any(), gomock.Any(), nil).
		AnyTimes().
		DoAndReturn(func(ctx context.Context, method *restapi.RequestMethod, requestBody []byte) ([]byte, error) {
			require.Equal(getBlockMethodName, method.Name)
			require.Equal(fmt.Sprintf(getBlockMethodPath, beaconHash), method.ParamsPath)
			return nil, fakeErr
		})

	_, err := s.client.GetBlockByHeight(context.Background(), beaconTag, beaconHeight)
	require.Error(err)
	require.ErrorIs(err, internal.ErrBlockNotFound)
	require.Contains(err.Error(), "failed to get block (height=100, hash=0xbf0bf1a2d342ac5a0d84ea0e2a2fc7d3d7b0fff2c221dc643bb1f9933401adc0) in GetBlockByHeight")
}

func (s *clientTestSuite) TestEthereumBeacon_GetBlockByHeight_BlobsNotFound() {
	require := testutil.Require(s.T())

	fakeErr := xerrors.Errorf("fake http error: %w", &restapi.HTTPError{
		Code:     http.StatusNotFound,
		Response: "fake http error",
	})
	headerResponse := fixtures.MustReadFile("client/ethereum/holesky/beacon/header_100.json")
	blockResponse := fixtures.MustReadFile("client/ethereum/holesky/beacon/block_100.json")

	s.restClient.EXPECT().Call(gomock.Any(), gomock.Any(), nil).
		Times(1).
		DoAndReturn(func(ctx context.Context, method *restapi.RequestMethod, requestBody []byte) ([]byte, error) {
			require.Equal(getBlockHeaderMethodName, method.Name)
			require.Equal(fmt.Sprintf(getBlockHeaderMethodPath, beaconHeight), method.ParamsPath)
			return headerResponse, nil
		})

	s.restClient.EXPECT().Call(gomock.Any(), gomock.Any(), nil).
		Times(1).
		DoAndReturn(func(ctx context.Context, method *restapi.RequestMethod, requestBody []byte) ([]byte, error) {
			require.Equal(getBlockMethodName, method.Name)
			require.Equal(fmt.Sprintf(getBlockMethodPath, beaconHash), method.ParamsPath)
			return blockResponse, nil
		})

	s.restClient.EXPECT().Call(gomock.Any(), gomock.Any(), nil).
		AnyTimes().
		DoAndReturn(func(ctx context.Context, method *restapi.RequestMethod, requestBody []byte) ([]byte, error) {
			require.Equal(getBlockBlobsMethodName, method.Name)
			require.Equal(fmt.Sprintf(getBlockBlobsMethodPath, beaconHash), method.ParamsPath)
			return nil, fakeErr
		})
	_, err := s.client.GetBlockByHeight(context.Background(), beaconTag, beaconHeight)
	require.Error(err)
	require.ErrorIs(err, internal.ErrBlockNotFound)
	require.Contains(err.Error(), "failed to get block blobs (height=100, hash=0xbf0bf1a2d342ac5a0d84ea0e2a2fc7d3d7b0fff2c221dc643bb1f9933401adc0) in GetBlockByHeight")
}

func (s *clientTestSuite) TestEthereumBeacon_GetBlockByHeight_MissingBlockHash() {
	require := testutil.Require(s.T())

	headerResponse := fixtures.MustReadFile("client/ethereum/holesky/beacon/header_missing_hash.json")

	s.restClient.EXPECT().Call(gomock.Any(), gomock.Any(), nil).
		Times(1).
		DoAndReturn(func(ctx context.Context, method *restapi.RequestMethod, requestBody []byte) ([]byte, error) {
			require.Equal(getBlockHeaderMethodName, method.Name)
			require.Equal(fmt.Sprintf(getBlockHeaderMethodPath, beaconHeight), method.ParamsPath)
			return headerResponse, nil
		})

	_, err := s.client.GetBlockByHeight(context.Background(), beaconTag, beaconHeight)
	require.Error(err)
	require.Contains(err.Error(), "Field validation for 'Root' failed on the 'required' tag")
}

func (s *clientTestSuite) TestEthereumBeacon_GetBlockByHeight_MismatchBlockHeight() {
	require := testutil.Require(s.T())

	headerResponse := fixtures.MustReadFile("client/ethereum/holesky/beacon/header_101.json")

	s.restClient.EXPECT().Call(gomock.Any(), gomock.Any(), nil).
		Times(1).
		DoAndReturn(func(ctx context.Context, method *restapi.RequestMethod, requestBody []byte) ([]byte, error) {
			require.Equal(getBlockHeaderMethodName, method.Name)
			require.Equal(fmt.Sprintf(getBlockHeaderMethodPath, beaconHeight), method.ParamsPath)
			return headerResponse, nil
		})

	_, err := s.client.GetBlockByHeight(context.Background(), beaconTag, beaconHeight)
	require.Error(err)
	require.Contains(err.Error(), "get inconsistent block heights, expected: 100, actual: 101")
}

func (s *clientTestSuite) TestEthereumBeacon_GetBlockByHash() {
	require := testutil.Require(s.T())

	headerResponse := fixtures.MustReadFile("client/ethereum/holesky/beacon/header_100.json")
	blockResponse := fixtures.MustReadFile("client/ethereum/holesky/beacon/block_100.json")
	blobsResponse := fixtures.MustReadFile("client/ethereum/holesky/beacon/blobs_100.json")

	s.restClient.EXPECT().Call(gomock.Any(), gomock.Any(), nil).
		Times(1).
		DoAndReturn(func(ctx context.Context, method *restapi.RequestMethod, requestBody []byte) ([]byte, error) {
			require.Equal(getBlockHeaderMethodName, method.Name)
			require.Equal(fmt.Sprintf(getBlockHeaderMethodPath, beaconHash), method.ParamsPath)
			return headerResponse, nil
		})

	s.restClient.EXPECT().Call(gomock.Any(), gomock.Any(), nil).
		Times(1).
		DoAndReturn(func(ctx context.Context, method *restapi.RequestMethod, requestBody []byte) ([]byte, error) {
			require.Equal(getBlockMethodName, method.Name)
			require.Equal(fmt.Sprintf(getBlockMethodPath, beaconHash), method.ParamsPath)
			return blockResponse, nil
		})

	s.restClient.EXPECT().Call(gomock.Any(), gomock.Any(), nil).
		Times(1).
		DoAndReturn(func(ctx context.Context, method *restapi.RequestMethod, requestBody []byte) ([]byte, error) {
			require.Equal(getBlockBlobsMethodName, method.Name)
			require.Equal(fmt.Sprintf(getBlockBlobsMethodPath, beaconHash), method.ParamsPath)
			return blobsResponse, nil
		})

	block, err := s.client.GetBlockByHash(context.Background(), beaconTag, beaconHeight, beaconHash)
	require.NoError(err)

	require.Equal(common.Blockchain_BLOCKCHAIN_ETHEREUM, block.Blockchain)
	require.Equal(common.Network_NETWORK_ETHEREUM_HOLESKY, block.Network)
	require.Equal(api.SideChain_SIDECHAIN_ETHEREUM_HOLESKY_BEACON, block.SideChain)

	// Block metadata
	metadata := block.Metadata
	require.NotNil(metadata)
	require.Equal(beaconTag, metadata.Tag)
	require.Equal(beaconHash, metadata.Hash)
	require.Equal("0xcbe950dda3533e3c257fd162b33d791f9073eb42e4da21def569451e9323c33e", metadata.ParentHash)
	require.Equal(beaconHeight, metadata.Height)
	require.Equal(uint64(0), metadata.ParentHeight)
	require.False(metadata.Skipped)
	require.Equal(testutil.MustTimestamp(timestamp100), metadata.Timestamp)

	// Block blob data
	blobdata := block.GetEthereumBeacon()
	require.NotNil(blobdata)
	require.NotEmpty(blobdata.Header)
	require.NotEmpty(blobdata.Block)
	require.NotEmpty(blobdata.Blobs)
}

func (s *clientTestSuite) TestEthereumBeacon_GetBlockByHash_SkippedBlock() {
	require := testutil.Require(s.T())

	block, err := s.client.GetBlockByHash(context.Background(), beaconTag, beaconHeight, "")
	require.NoError(err)

	require.Equal(common.Blockchain_BLOCKCHAIN_ETHEREUM, block.Blockchain)
	require.Equal(common.Network_NETWORK_ETHEREUM_HOLESKY, block.Network)
	require.Equal(api.SideChain_SIDECHAIN_ETHEREUM_HOLESKY_BEACON, block.SideChain)

	metadata := block.Metadata
	require.NotNil(metadata)
	require.Equal(beaconTag, metadata.Tag)
	require.Equal("", metadata.Hash)
	require.Equal("", metadata.ParentHash)
	require.Equal(beaconHeight, metadata.Height)
	require.Equal(uint64(0), metadata.ParentHeight)
	require.True(metadata.Skipped)
	require.Equal(testutil.MustTimestamp(timestamp100), metadata.Timestamp)

	blobdata := block.GetEthereumBeacon()
	require.Nil(blobdata)
}

func (s *clientTestSuite) TestEthereumBeacon_GetBlockByHash_HeaderNotFound() {
	require := testutil.Require(s.T())

	fakeErr := xerrors.Errorf("fake http error: %w", &restapi.HTTPError{
		Code:     http.StatusNotFound,
		Response: "fake http error",
	})

	s.restClient.EXPECT().Call(gomock.Any(), gomock.Any(), nil).
		Times(1).
		DoAndReturn(func(ctx context.Context, method *restapi.RequestMethod, requestBody []byte) ([]byte, error) {
			require.Equal(getBlockHeaderMethodName, method.Name)
			require.Equal(fmt.Sprintf(getBlockHeaderMethodPath, beaconHash), method.ParamsPath)
			return nil, fakeErr
		})

	_, err := s.client.GetBlockByHash(context.Background(), beaconTag, beaconHeight, beaconHash)
	require.Error(err)
	require.Contains(err.Error(), "block header (height=100, hash=0xbf0bf1a2d342ac5a0d84ea0e2a2fc7d3d7b0fff2c221dc643bb1f9933401adc0) not found")
}

func (s *clientTestSuite) TestEthereumBeacon_GetBlockByHash_MissingBlockHash() {
	require := testutil.Require(s.T())

	headerResponse := fixtures.MustReadFile("client/ethereum/holesky/beacon/header_missing_hash.json")

	s.restClient.EXPECT().Call(gomock.Any(), gomock.Any(), nil).
		Times(1).
		DoAndReturn(func(ctx context.Context, method *restapi.RequestMethod, requestBody []byte) ([]byte, error) {
			require.Equal(getBlockHeaderMethodName, method.Name)
			require.Equal(fmt.Sprintf(getBlockHeaderMethodPath, beaconHash), method.ParamsPath)
			return headerResponse, nil
		})

	_, err := s.client.GetBlockByHash(context.Background(), beaconTag, beaconHeight, beaconHash)
	require.Error(err)
	require.Contains(err.Error(), "Field validation for 'Root' failed on the 'required' tag")
}

func (s *clientTestSuite) TestEthereumBeacon_GetBlockByHash_MismatchBlockHash() {
	require := testutil.Require(s.T())

	headerResponse := fixtures.MustReadFile("client/ethereum/holesky/beacon/header_100_incorrect_hash.json")

	s.restClient.EXPECT().Call(gomock.Any(), gomock.Any(), nil).
		Times(1).
		DoAndReturn(func(ctx context.Context, method *restapi.RequestMethod, requestBody []byte) ([]byte, error) {
			require.Equal(getBlockHeaderMethodName, method.Name)
			require.Equal(fmt.Sprintf(getBlockHeaderMethodPath, beaconHash), method.ParamsPath)
			return headerResponse, nil
		})

	_, err := s.client.GetBlockByHash(context.Background(), beaconTag, beaconHeight, beaconHash)
	require.Error(err)
	require.Contains(err.Error(), "get inconsistent block hashes")
}

func (s *clientTestSuite) TestEthereumBeacon_GetBlockByHash_BlockNotFound() {
	require := testutil.Require(s.T())

	fakeErr := xerrors.Errorf("fake http error: %w", &restapi.HTTPError{
		Code:     http.StatusNotFound,
		Response: "fake http error",
	})
	headerResponse := fixtures.MustReadFile("client/ethereum/holesky/beacon/header_100.json")

	s.restClient.EXPECT().Call(gomock.Any(), gomock.Any(), nil).
		Times(1).
		DoAndReturn(func(ctx context.Context, method *restapi.RequestMethod, requestBody []byte) ([]byte, error) {
			require.Equal(getBlockHeaderMethodName, method.Name)
			require.Equal(fmt.Sprintf(getBlockHeaderMethodPath, beaconHash), method.ParamsPath)
			return headerResponse, nil
		})

	s.restClient.EXPECT().Call(gomock.Any(), gomock.Any(), nil).
		AnyTimes().
		DoAndReturn(func(ctx context.Context, method *restapi.RequestMethod, requestBody []byte) ([]byte, error) {
			require.Equal(getBlockMethodName, method.Name)
			require.Equal(fmt.Sprintf(getBlockMethodPath, beaconHash), method.ParamsPath)
			return nil, fakeErr
		})

	_, err := s.client.GetBlockByHash(context.Background(), beaconTag, beaconHeight, beaconHash)
	require.Error(err)
	require.ErrorIs(err, internal.ErrBlockNotFound)
	require.Contains(err.Error(), "failed to get block (height=100, hash=0xbf0bf1a2d342ac5a0d84ea0e2a2fc7d3d7b0fff2c221dc643bb1f9933401adc0) in GetBlockByHash")
}

func (s *clientTestSuite) TestEthereumBeacon_GetBlockByHash_BlobsNotFound() {
	require := testutil.Require(s.T())

	fakeErr := xerrors.Errorf("fake http error: %w", &restapi.HTTPError{
		Code:     http.StatusNotFound,
		Response: "fake http error",
	})
	headerResponse := fixtures.MustReadFile("client/ethereum/holesky/beacon/header_100.json")
	blockResponse := fixtures.MustReadFile("client/ethereum/holesky/beacon/block_100.json")

	s.restClient.EXPECT().Call(gomock.Any(), gomock.Any(), nil).
		Times(1).
		DoAndReturn(func(ctx context.Context, method *restapi.RequestMethod, requestBody []byte) ([]byte, error) {
			require.Equal(getBlockHeaderMethodName, method.Name)
			require.Equal(fmt.Sprintf(getBlockHeaderMethodPath, beaconHash), method.ParamsPath)
			return headerResponse, nil
		})

	s.restClient.EXPECT().Call(gomock.Any(), gomock.Any(), nil).
		Times(1).
		DoAndReturn(func(ctx context.Context, method *restapi.RequestMethod, requestBody []byte) ([]byte, error) {
			require.Equal(getBlockMethodName, method.Name)
			require.Equal(fmt.Sprintf(getBlockMethodPath, beaconHash), method.ParamsPath)
			return blockResponse, nil
		})

	s.restClient.EXPECT().Call(gomock.Any(), gomock.Any(), nil).
		AnyTimes().
		DoAndReturn(func(ctx context.Context, method *restapi.RequestMethod, requestBody []byte) ([]byte, error) {
			require.Equal(getBlockBlobsMethodName, method.Name)
			require.Equal(fmt.Sprintf(getBlockBlobsMethodPath, beaconHash), method.ParamsPath)
			return nil, fakeErr
		})

	_, err := s.client.GetBlockByHash(context.Background(), beaconTag, beaconHeight, beaconHash)
	require.Error(err)
	require.ErrorIs(err, internal.ErrBlockNotFound)
	require.Contains(err.Error(), "failed to get block blobs (height=100, hash=0xbf0bf1a2d342ac5a0d84ea0e2a2fc7d3d7b0fff2c221dc643bb1f9933401adc0) in GetBlockByHash")
}

func (s *clientTestSuite) TestEthereumBeacon_GetBlockByHash_GetBlockRetry() {
	require := testutil.Require(s.T())

	fakeErr := xerrors.Errorf("fake http error: %w", &restapi.HTTPError{
		Code:     http.StatusNotFound,
		Response: "fake http error",
	})
	headerResponse := fixtures.MustReadFile("client/ethereum/holesky/beacon/header_100.json")
	blockResponse := fixtures.MustReadFile("client/ethereum/holesky/beacon/block_100.json")
	blobsResponse := fixtures.MustReadFile("client/ethereum/holesky/beacon/blobs_100.json")

	s.restClient.EXPECT().Call(gomock.Any(), gomock.Any(), nil).
		Times(1).
		DoAndReturn(func(ctx context.Context, method *restapi.RequestMethod, requestBody []byte) ([]byte, error) {
			require.Equal(getBlockHeaderMethodName, method.Name)
			require.Equal(fmt.Sprintf(getBlockHeaderMethodPath, beaconHash), method.ParamsPath)
			return headerResponse, nil
		})

	attempts := 0
	s.restClient.EXPECT().Call(gomock.Any(), gomock.Any(), nil).
		Times(2).
		DoAndReturn(func(ctx context.Context, method *restapi.RequestMethod, requestBody []byte) ([]byte, error) {
			require.Equal(getBlockMethodName, method.Name)
			require.Equal(fmt.Sprintf(getBlockMethodPath, beaconHash), method.ParamsPath)
			if attempts == 0 {
				attempts += 1
				return nil, fakeErr
			}
			return blockResponse, nil
		})

	s.restClient.EXPECT().Call(gomock.Any(), gomock.Any(), nil).
		Times(1).
		DoAndReturn(func(ctx context.Context, method *restapi.RequestMethod, requestBody []byte) ([]byte, error) {
			require.Equal(getBlockBlobsMethodName, method.Name)
			require.Equal(fmt.Sprintf(getBlockBlobsMethodPath, beaconHash), method.ParamsPath)
			return blobsResponse, nil
		})

	block, err := s.client.GetBlockByHash(context.Background(), beaconTag, beaconHeight, beaconHash)
	require.NoError(err)

	require.Equal(common.Blockchain_BLOCKCHAIN_ETHEREUM, block.Blockchain)
	require.Equal(common.Network_NETWORK_ETHEREUM_HOLESKY, block.Network)
	require.Equal(api.SideChain_SIDECHAIN_ETHEREUM_HOLESKY_BEACON, block.SideChain)

	// Block metadata
	metadata := block.Metadata
	require.NotNil(metadata)
	require.Equal(beaconTag, metadata.Tag)
	require.Equal(beaconHash, metadata.Hash)
	require.Equal("0xcbe950dda3533e3c257fd162b33d791f9073eb42e4da21def569451e9323c33e", metadata.ParentHash)
	require.Equal(beaconHeight, metadata.Height)
	require.Equal(uint64(0), metadata.ParentHeight)
	require.False(metadata.Skipped)
	require.Equal(testutil.MustTimestamp(timestamp100), metadata.Timestamp)

	// Block blob data
	blobdata := block.GetEthereumBeacon()
	require.NotNil(blobdata)
	require.NotEmpty(blobdata.Header)
	require.NotEmpty(blobdata.Block)
	require.NotEmpty(blobdata.Blobs)
}

func (s *clientTestSuite) TestEthereumBeacon_GetBlockByHash_GetBlobsRetry() {
	require := testutil.Require(s.T())

	fakeErr := xerrors.Errorf("fake http error: %w", &restapi.HTTPError{
		Code:     http.StatusNotFound,
		Response: "fake http error",
	})
	headerResponse := fixtures.MustReadFile("client/ethereum/holesky/beacon/header_100.json")
	blockResponse := fixtures.MustReadFile("client/ethereum/holesky/beacon/block_100.json")
	blobsResponse := fixtures.MustReadFile("client/ethereum/holesky/beacon/blobs_100.json")

	s.restClient.EXPECT().Call(gomock.Any(), gomock.Any(), nil).
		Times(1).
		DoAndReturn(func(ctx context.Context, method *restapi.RequestMethod, requestBody []byte) ([]byte, error) {
			require.Equal(getBlockHeaderMethodName, method.Name)
			require.Equal(fmt.Sprintf(getBlockHeaderMethodPath, beaconHash), method.ParamsPath)
			return headerResponse, nil
		})

	s.restClient.EXPECT().Call(gomock.Any(), gomock.Any(), nil).
		Times(1).
		DoAndReturn(func(ctx context.Context, method *restapi.RequestMethod, requestBody []byte) ([]byte, error) {
			require.Equal(getBlockMethodName, method.Name)
			require.Equal(fmt.Sprintf(getBlockMethodPath, beaconHash), method.ParamsPath)
			return blockResponse, nil
		})

	attempts := 0
	s.restClient.EXPECT().Call(gomock.Any(), gomock.Any(), nil).
		Times(2).
		DoAndReturn(func(ctx context.Context, method *restapi.RequestMethod, requestBody []byte) ([]byte, error) {
			require.Equal(getBlockBlobsMethodName, method.Name)
			require.Equal(fmt.Sprintf(getBlockBlobsMethodPath, beaconHash), method.ParamsPath)
			if attempts == 0 {
				attempts += 1
				return nil, fakeErr
			}
			return blobsResponse, nil
		})

	block, err := s.client.GetBlockByHash(context.Background(), beaconTag, beaconHeight, beaconHash)
	require.NoError(err)

	require.Equal(common.Blockchain_BLOCKCHAIN_ETHEREUM, block.Blockchain)
	require.Equal(common.Network_NETWORK_ETHEREUM_HOLESKY, block.Network)
	require.Equal(api.SideChain_SIDECHAIN_ETHEREUM_HOLESKY_BEACON, block.SideChain)

	// Block metadata
	metadata := block.Metadata
	require.NotNil(metadata)
	require.Equal(beaconTag, metadata.Tag)
	require.Equal(beaconHash, metadata.Hash)
	require.Equal("0xcbe950dda3533e3c257fd162b33d791f9073eb42e4da21def569451e9323c33e", metadata.ParentHash)
	require.Equal(beaconHeight, metadata.Height)
	require.Equal(uint64(0), metadata.ParentHeight)
	require.False(metadata.Skipped)
	require.Equal(testutil.MustTimestamp(timestamp100), metadata.Timestamp)

	// Block blob data
	blobdata := block.GetEthereumBeacon()
	require.NotNil(blobdata)
	require.NotEmpty(blobdata.Header)
	require.NotEmpty(blobdata.Block)
	require.NotEmpty(blobdata.Blobs)
}

func (s *clientTestSuite) TestEthereumBeacon_GetBlockByHash_EmptyBlobs() {
	require := testutil.Require(s.T())

	headerResponse := fixtures.MustReadFile("client/ethereum/holesky/beacon/header_100.json")
	blockResponse := fixtures.MustReadFile("client/ethereum/holesky/beacon/block_100.json")
	blobsResponse := fixtures.MustReadFile("client/ethereum/holesky/beacon/blobs_empty_list.json")

	s.restClient.EXPECT().Call(gomock.Any(), gomock.Any(), nil).
		Times(1).
		DoAndReturn(func(ctx context.Context, method *restapi.RequestMethod, requestBody []byte) ([]byte, error) {
			require.Equal(getBlockHeaderMethodName, method.Name)
			require.Equal(fmt.Sprintf(getBlockHeaderMethodPath, beaconHash), method.ParamsPath)
			return headerResponse, nil
		})

	s.restClient.EXPECT().Call(gomock.Any(), gomock.Any(), nil).
		Times(1).
		DoAndReturn(func(ctx context.Context, method *restapi.RequestMethod, requestBody []byte) ([]byte, error) {
			require.Equal(getBlockMethodName, method.Name)
			require.Equal(fmt.Sprintf(getBlockMethodPath, beaconHash), method.ParamsPath)
			return blockResponse, nil
		})

	s.restClient.EXPECT().Call(gomock.Any(), gomock.Any(), nil).
		Times(1).
		DoAndReturn(func(ctx context.Context, method *restapi.RequestMethod, requestBody []byte) ([]byte, error) {
			require.Equal(getBlockBlobsMethodName, method.Name)
			require.Equal(fmt.Sprintf(getBlockBlobsMethodPath, beaconHash), method.ParamsPath)
			return blobsResponse, nil
		})

	block, err := s.client.GetBlockByHash(context.Background(), beaconTag, beaconHeight, beaconHash)
	require.NoError(err)

	require.Equal(common.Blockchain_BLOCKCHAIN_ETHEREUM, block.Blockchain)
	require.Equal(common.Network_NETWORK_ETHEREUM_HOLESKY, block.Network)
	require.Equal(api.SideChain_SIDECHAIN_ETHEREUM_HOLESKY_BEACON, block.SideChain)

	// Block metadata
	metadata := block.Metadata
	require.NotNil(metadata)
	require.Equal(beaconTag, metadata.Tag)
	require.Equal(beaconHash, metadata.Hash)
	require.Equal("0xcbe950dda3533e3c257fd162b33d791f9073eb42e4da21def569451e9323c33e", metadata.ParentHash)
	require.Equal(beaconHeight, metadata.Height)
	require.Equal(uint64(0), metadata.ParentHeight)
	require.False(metadata.Skipped)
	require.Equal(testutil.MustTimestamp(timestamp100), metadata.Timestamp)

	// Block blob data
	blobdata := block.GetEthereumBeacon()
	require.NotNil(blobdata)
	require.NotEmpty(blobdata.Header)
	require.NotEmpty(blobdata.Block)
	require.NotEmpty(blobdata.Blobs)
}

func (s *clientTestSuite) TestEthereumBeacon_GetBlockTimestamp() {
	headerResponse := fixtures.MustReadFile("client/ethereum/holesky/beacon/header_100.json")

	tests := []struct {
		name     string
		height   uint64
		expected *timestamp.Timestamp
	}{
		{
			name:     "height_100",
			height:   100,
			expected: &timestamp.Timestamp{Seconds: 1695903600},
		},
	}
	for _, test := range tests {
		s.Run(test.name, func() {
			require := testutil.Require(s.T())

			s.restClient.EXPECT().Call(gomock.Any(), gomock.Any(), nil).
				Times(1).
				DoAndReturn(func(ctx context.Context, method *restapi.RequestMethod, requestBody []byte) ([]byte, error) {
					return headerResponse, nil
				})

			t, err := s.client.BatchGetBlockMetadata(context.Background(), beaconTag, test.height, test.height+1)
			require.NoError(err)
			require.Equal(test.expected, t[0].Timestamp)
		})
	}
}

func (s *clientTestSuite) TestEthereumBeacon_GetBlockTimestamp_Failure() {
	fakeErr := xerrors.Errorf("fake http error: %w", &restapi.HTTPError{
		Code:     http.StatusNotFound,
		Response: "fake http error",
	})

	tests := []struct {
		name   string
		height uint64
	}{
		{
			name:   "overflow_1",
			height: uint64(768614336404564650),
		},
		{
			name:   "overflow_2",
			height: uint64(768614336404564660),
		},
		{
			name:   "overflow_3",
			height: math.MaxUint64 - 1,
		},
	}
	for _, test := range tests {
		s.Run(test.name, func() {
			require := testutil.Require(s.T())

			s.restClient.EXPECT().Call(gomock.Any(), gomock.Any(), nil).
				Times(1).
				DoAndReturn(func(ctx context.Context, method *restapi.RequestMethod, requestBody []byte) ([]byte, error) {
					return nil, fakeErr
				})

			_, err := s.client.BatchGetBlockMetadata(context.Background(), beaconTag, test.height, test.height+1)
			require.Error(err)
			require.Contains(err.Error(), "block timestamp overflow")
		})
	}
}

func testRestModule(client *restapimocks.MockClient) fx.Option {
	return fx.Options(
		internal.Module,
		fx.Provide(fx.Annotated{
			Name:   "master",
			Target: func() restapi.Client { return client },
		}),
		fx.Provide(fx.Annotated{
			Name:   "slave",
			Target: func() restapi.Client { return client },
		}),
		fx.Provide(fx.Annotated{
			Name:   "validator",
			Target: func() restapi.Client { return client },
		}),
		fx.Provide(fx.Annotated{
			Name:   "consensus",
			Target: func() restapi.Client { return client },
		}),
		fx.Provide(dlq.NewNop),
		fx.Provide(parser.NewNop),
	)
}
