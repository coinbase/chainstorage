package aptos

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"go.uber.org/fx"
	"go.uber.org/mock/gomock"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/blockchain/client/internal"
	"github.com/coinbase/chainstorage/internal/blockchain/jsonrpc"
	"github.com/coinbase/chainstorage/internal/blockchain/parser"
	"github.com/coinbase/chainstorage/internal/blockchain/parser/aptos"
	"github.com/coinbase/chainstorage/internal/blockchain/restapi"
	restapimocks "github.com/coinbase/chainstorage/internal/blockchain/restapi/mocks"
	"github.com/coinbase/chainstorage/internal/dlq"
	"github.com/coinbase/chainstorage/internal/utils/fixtures"
	"github.com/coinbase/chainstorage/internal/utils/testapp"
	"github.com/coinbase/chainstorage/internal/utils/testutil"
	"github.com/coinbase/chainstorage/protos/coinbase/c3/common"
)

type aptosClientTestSuite struct {
	suite.Suite

	ctrl       *gomock.Controller
	app        testapp.TestApp
	restClient *restapimocks.MockClient
	client     internal.Client
}

const (
	aptosTag           = uint32(2)
	aptosHeight        = uint64(10000)
	aptosParentHeight  = uint64(9999)
	aptosHash          = "0x7eee0512cef0754f58890802a6c3ba1e1032bb1820f45b825adbbee4f10d9d71"
	aptosParentHash    = ""
	aptosTimestamp     = "2022-10-12T22:48:48Z"
	aptosHeight1       = uint64(10001)
	aptosParentHeight1 = uint64(10000)
	aptosHash1         = "0xb9ad6c058ef4cdb42cb7a5bc4d54f18ec1fa222a21426ad5e66b313397db6ae6"
	aptosParentHash1   = ""
	aptosTimestamp1    = "2022-10-12T22:48:49Z"
)

func TestAptosClientTestSuite(t *testing.T) {
	suite.Run(t, new(aptosClientTestSuite))
}

func (s *aptosClientTestSuite) SetupTest() {
	s.ctrl = gomock.NewController(s.T())
	s.restClient = restapimocks.NewMockClient(s.ctrl)

	var result internal.ClientParams
	s.app = testapp.New(
		s.T(),
		testapp.WithBlockchainNetwork(common.Blockchain_BLOCKCHAIN_APTOS, common.Network_NETWORK_APTOS_MAINNET),
		Module,
		jsonrpc.Module,
		testRestModule(s.restClient),
		fx.Populate(&result),
	)

	s.client = result.Master
	s.NotNil(s.client)
}

func (s *aptosClientTestSuite) TearDownTest() {
	s.app.Close()
	s.ctrl.Finish()
}

func (s *aptosClientTestSuite) TestBatchGetBlockMetadata_Success() {
	require := testutil.Require(s.T())

	metaResponse1 := fixtures.MustReadFile("client/aptos/block_header.json")
	metaResponse2 := fixtures.MustReadFile("client/aptos/block_header1.json")

	expectedMethod1 := &restapi.RequestMethod{
		Name:       "GetBlockByHeight",
		ParamsPath: "/blocks/by_height/10000?with_transactions=false",
		Timeout:    5 * time.Second,
	}
	expectedMethod2 := &restapi.RequestMethod{
		Name:       "GetBlockByHeight",
		ParamsPath: "/blocks/by_height/10001?with_transactions=false",
		Timeout:    5 * time.Second,
	}
	s.restClient.EXPECT().Call(gomock.Any(), expectedMethod1, gomock.Any()).Return(metaResponse1, nil)
	s.restClient.EXPECT().Call(gomock.Any(), expectedMethod2, gomock.Any()).Return(metaResponse2, nil)

	results, err := s.client.BatchGetBlockMetadata(context.Background(), aptosTag, aptosHeight, aptosHeight+2)
	require.NoError(err)
	require.Equal(2, len(results))

	result1 := results[0]
	require.NotNil(result1)
	require.Equal(aptosHeight, result1.Height)
	require.Equal(aptosParentHeight, result1.ParentHeight)
	require.Equal(aptosHash, result1.Hash)
	require.Equal(aptosParentHash, result1.ParentHash)
	require.Equal(testutil.MustTimestamp(aptosTimestamp), result1.Timestamp)

	result2 := results[1]
	require.NotNil(result2)
	require.Equal(aptosHeight1, result2.Height)
	require.Equal(aptosParentHeight1, result2.ParentHeight)
	require.Equal(aptosHash1, result2.Hash)
	require.Equal(aptosParentHash1, result2.ParentHash)
	require.Equal(testutil.MustTimestamp(aptosTimestamp1), result2.Timestamp)
}

func (s *aptosClientTestSuite) TestBatchGetBlockMetadata_Failure() {
	require := testutil.Require(s.T())

	metaResponse1 := fixtures.MustReadFile("client/aptos/block_header.json")
	expectedMethod1 := &restapi.RequestMethod{
		Name:       "GetBlockByHeight",
		ParamsPath: "/blocks/by_height/10000?with_transactions=false",
		Timeout:    5 * time.Second,
	}

	expectedMethod2 := &restapi.RequestMethod{
		Name:       "GetBlockByHeight",
		ParamsPath: "/blocks/by_height/10001?with_transactions=false",
		Timeout:    5 * time.Second,
	}
	expectedErrAptos := AptosError{
		Message:     "test error",
		ErrorCode:   "web_framework_error",
		VmErrorCode: 0,
	}
	response, _ := json.Marshal(&expectedErrAptos)
	expectedHTTPErr := restapi.HTTPError{
		Code:     http.StatusInternalServerError,
		Response: string(response),
	}
	callErr := xerrors.Errorf("received http error: %w", &expectedHTTPErr)

	s.restClient.EXPECT().Call(gomock.Any(), expectedMethod1, gomock.Any()).Return(metaResponse1, nil)
	// Make the second request failed
	s.restClient.EXPECT().Call(gomock.Any(), expectedMethod2, gomock.Any()).Return(nil, callErr)

	results, err := s.client.BatchGetBlockMetadata(context.Background(), aptosTag, aptosHeight, aptosHeight+2)
	require.Nil(results)
	require.Error(err)

	var errHTTP *restapi.HTTPError
	require.True(errors.As(err, &errHTTP))
	require.Equal(expectedHTTPErr, *errHTTP)
}

func (s *aptosClientTestSuite) TestGetBlockByHeight() {
	require := testutil.Require(s.T())

	blockResponse := fixtures.MustReadFile("client/aptos/block.json")

	expectedMethod := &restapi.RequestMethod{
		Name:       "GetBlockByHeight",
		ParamsPath: "/blocks/by_height/10000?with_transactions=true",
		Timeout:    5 * time.Second,
	}
	s.restClient.EXPECT().Call(
		gomock.Any(),
		expectedMethod,
		gomock.Any(),
	).AnyTimes().Return(blockResponse, nil)

	block, err := s.client.GetBlockByHeight(context.Background(), aptosTag, aptosHeight)
	require.NoError(err)
	require.Equal(common.Blockchain_BLOCKCHAIN_APTOS, block.Blockchain)
	require.Equal(common.Network_NETWORK_APTOS_MAINNET, block.Network)
	require.Equal(aptosTag, block.Metadata.Tag)
	require.Equal(aptosHeight, block.Metadata.Height)
	require.Equal(aptosParentHeight, block.Metadata.ParentHeight)
	require.Equal(aptosHash, block.Metadata.Hash)
	require.Equal(aptosParentHash, block.Metadata.ParentHash)
	require.Equal(testutil.MustTimestamp(aptosTimestamp), block.Metadata.Timestamp)
	require.Less(0, len(block.GetAptos().GetBlock()))
}

func (s *aptosClientTestSuite) TestGetBlockByHeight_AptosError() {
	require := testutil.Require(s.T())

	expectedErrAptos := AptosError{
		Message:     "test error",
		ErrorCode:   "web_framework_error",
		VmErrorCode: 0,
	}
	response, _ := json.Marshal(&expectedErrAptos)
	expectedHTTPErr := restapi.HTTPError{
		Code:     http.StatusInternalServerError,
		Response: string(response),
	}
	callErr := xerrors.Errorf("received http error: %w", &expectedHTTPErr)

	expectedMethod := &restapi.RequestMethod{
		Name:       "GetBlockByHeight",
		ParamsPath: "/blocks/by_height/10000?with_transactions=true",
		Timeout:    5 * time.Second,
	}

	s.restClient.EXPECT().Call(
		gomock.Any(),
		expectedMethod,
		gomock.Any(),
	).AnyTimes().Return(nil, callErr)

	block, err := s.client.GetBlockByHeight(context.Background(), aptosTag, aptosHeight)
	require.Nil(block)
	require.Error(err)

	var errHTTP *restapi.HTTPError
	require.True(errors.As(err, &errHTTP))
	require.Equal(expectedHTTPErr, *errHTTP)
}

func (s *aptosClientTestSuite) TestGetBlockByHeight_NotFound() {
	require := testutil.Require(s.T())

	expectedErrAptos := AptosError{
		Message:     "test error",
		ErrorCode:   "block_not_found",
		VmErrorCode: 0,
	}
	response, _ := json.Marshal(&expectedErrAptos)
	callErr := xerrors.Errorf("received http error: %w", &restapi.HTTPError{
		Code:     http.StatusInternalServerError,
		Response: string(response),
	})

	expectedMethod := &restapi.RequestMethod{
		Name:       "GetBlockByHeight",
		ParamsPath: "/blocks/by_height/10000?with_transactions=true",
		Timeout:    5 * time.Second,
	}

	s.restClient.EXPECT().Call(
		gomock.Any(),
		expectedMethod,
		gomock.Any(),
	).AnyTimes().Return(nil, callErr)

	block, err := s.client.GetBlockByHeight(context.Background(), aptosTag, aptosHeight)
	require.Nil(block)
	require.Error(err)
	require.True(errors.Is(err, internal.ErrBlockNotFound))
}

func (s *aptosClientTestSuite) TestGetBlockByHash() {
	require := testutil.Require(s.T())

	blockResponse := fixtures.MustReadFile("client/aptos/block.json")

	expectedMethod := &restapi.RequestMethod{
		Name:       "GetBlockByHeight",
		ParamsPath: "/blocks/by_height/10000?with_transactions=true",
		Timeout:    5 * time.Second,
	}
	s.restClient.EXPECT().Call(
		gomock.Any(),
		expectedMethod,
		gomock.Any(),
	).AnyTimes().Return(blockResponse, nil)

	block, err := s.client.GetBlockByHash(context.Background(), aptosTag, aptosHeight, aptosHash)
	require.NoError(err)
	require.Equal(common.Blockchain_BLOCKCHAIN_APTOS, block.Blockchain)
	require.Equal(common.Network_NETWORK_APTOS_MAINNET, block.Network)
	require.Equal(aptosTag, block.Metadata.Tag)
	require.Equal(aptosHeight, block.Metadata.Height)
	require.Equal(aptosParentHeight, block.Metadata.ParentHeight)
	require.Equal(aptosHash, block.Metadata.Hash)
	require.Equal(aptosParentHash, block.Metadata.ParentHash)
	require.Equal(testutil.MustTimestamp(aptosTimestamp), block.Metadata.Timestamp)
	require.Less(0, len(block.GetAptos().GetBlock()))
}

func (s *aptosClientTestSuite) TestGetBlockByHash_AptosError() {
	require := testutil.Require(s.T())

	expectedErrAptos := AptosError{
		Message:     "test error",
		ErrorCode:   "web_framework_error",
		VmErrorCode: 0,
	}
	response, _ := json.Marshal(&expectedErrAptos)
	expectedHTTPErr := restapi.HTTPError{
		Code:     http.StatusInternalServerError,
		Response: string(response),
	}
	callErr := xerrors.Errorf("received http error: %w", &expectedHTTPErr)

	expectedMethod := &restapi.RequestMethod{
		Name:       "GetBlockByHeight",
		ParamsPath: "/blocks/by_height/10000?with_transactions=true",
		Timeout:    5 * time.Second,
	}

	s.restClient.EXPECT().Call(
		gomock.Any(),
		expectedMethod,
		gomock.Any(),
	).AnyTimes().Return(nil, callErr)

	block, err := s.client.GetBlockByHash(context.Background(), aptosTag, aptosHeight, aptosHash)
	require.Nil(block)
	require.Error(err)

	var errHTTP *restapi.HTTPError
	require.True(errors.As(err, &errHTTP))
	require.Equal(expectedHTTPErr, *errHTTP)
}

func (s *aptosClientTestSuite) TestGetBlockByHash_WrongHash() {
	require := testutil.Require(s.T())

	blockResponse := fixtures.MustReadFile("client/aptos/block.json")

	expectedMethod := &restapi.RequestMethod{
		Name:       "GetBlockByHeight",
		ParamsPath: "/blocks/by_height/10000?with_transactions=true",
		Timeout:    5 * time.Second,
	}
	s.restClient.EXPECT().Call(
		gomock.Any(),
		expectedMethod,
		gomock.Any(),
	).AnyTimes().Return(blockResponse, nil)

	block, err := s.client.GetBlockByHash(context.Background(), aptosTag, aptosHeight, "dummy_hash")
	require.Nil(block)
	require.Error(err)
	require.Contains(err.Error(), "expected=dummy_hash")
	require.Contains(err.Error(), fmt.Sprintf("actual=%s", aptosHash))
}

func (s *aptosClientTestSuite) TestGetLatestBlock() {
	require := testutil.Require(s.T())

	expectedLegerInfo := aptos.AptosLedgerInfoLit{
		ChainID:             1,
		Epoch:               "1925",
		OldestLedgerVersion: "0",
		LedgerTimestamp:     "1679445180598692",
		NodeRole:            "full_node",
		OldestBlockHeight:   "0",
		LatestBlockHeight:   "40846989",
		GitHash:             "cc30c46ad41cd1577935466036eb1903b7cbc973",
	}
	expectedResponse, err := json.Marshal(&expectedLegerInfo)
	require.NoError(err)

	expectedMethod := &restapi.RequestMethod{
		Name:       "GetLedgerInfo",
		ParamsPath: "", // No parameter URls
		Timeout:    5 * time.Second,
	}
	s.restClient.EXPECT().Call(
		gomock.Any(),
		expectedMethod,
		gomock.Any(),
	).AnyTimes().Return(expectedResponse, nil)

	height, err := s.client.GetLatestHeight(context.Background())
	require.NoError(err)
	require.Equal(uint64(40846989), height)
}

func (s *aptosClientTestSuite) TestGetLatestBlock_AptosError() {
	require := testutil.Require(s.T())

	expectedErrAptos := AptosError{
		Message:     "test error",
		ErrorCode:   "web_framework_error",
		VmErrorCode: 0,
	}
	response, _ := json.Marshal(&expectedErrAptos)
	expectedHTTPErr := restapi.HTTPError{
		Code:     http.StatusInternalServerError,
		Response: string(response),
	}
	callErr := xerrors.Errorf("received http error: %w", &expectedHTTPErr)

	expectedMethod := &restapi.RequestMethod{
		Name:       "GetLedgerInfo",
		ParamsPath: "", // No parameter URls
		Timeout:    5 * time.Second,
	}

	s.restClient.EXPECT().Call(
		gomock.Any(),
		expectedMethod,
		gomock.Any(),
	).AnyTimes().Return(nil, callErr)

	height, err := s.client.GetLatestHeight(context.Background())
	require.Equal(uint64(0), height)
	require.Error(err)

	var errHTTP *restapi.HTTPError
	require.True(errors.As(err, &errHTTP))
	require.Equal(expectedHTTPErr, *errHTTP)
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
