package aptos_test

import (
	"context"
	"fmt"
	"math"
	"testing"

	"github.com/golang/protobuf/ptypes/timestamp"
	"github.com/stretchr/testify/suite"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/blockchain/client"
	"github.com/coinbase/chainstorage/internal/blockchain/jsonrpc"
	"github.com/coinbase/chainstorage/internal/blockchain/parser"
	"github.com/coinbase/chainstorage/internal/blockchain/restapi"
	"github.com/coinbase/chainstorage/internal/dlq"
	"github.com/coinbase/chainstorage/internal/utils/testapp"
	"github.com/coinbase/chainstorage/internal/utils/testutil"
	"github.com/coinbase/chainstorage/protos/coinbase/c3/common"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

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
	// This block contains a user transaction
	aptosHeight2       = uint64(44991655)
	aptosParentHeight2 = uint64(44991654)
	aptosHash2         = "0xa6ee6266348bc84793388aa376453a4c076821b479b97f5faba68ec03cf7b253"
	aptosParentHash2   = ""
	aptosTimestamp2    = "2023-04-07T04:57:52Z"
)

var (
	writeResource1 = `{"epoch_interval":"7200000000","height":"10000","new_block_events":{"counter":"10001","guid":{"id":{"addr":"0x1","creation_num":"3"}}},"update_epoch_interval_events":{"counter":"0","guid":{"id":{"addr":"0x1","creation_num":"4"}}}}`

	writeResource2 = `{"validators":[{"failed_proposals":"0","successful_proposals":"62"},{"failed_proposals":"0","successful_proposals":"47"},{"failed_proposals":"0","successful_proposals":"32"},{"failed_proposals":"0","successful_proposals":"27"},{"failed_proposals":"0","successful_proposals":"28"},{"failed_proposals":"3","successful_proposals":"256"},{"failed_proposals":"0","successful_proposals":"120"},{"failed_proposals":"0","successful_proposals":"60"},{"failed_proposals":"0","successful_proposals":"29"},{"failed_proposals":"0","successful_proposals":"87"},{"failed_proposals":"0","successful_proposals":"171"},{"failed_proposals":"0","successful_proposals":"173"},{"failed_proposals":"0","successful_proposals":"163"},{"failed_proposals":"0","successful_proposals":"179"},{"failed_proposals":"0","successful_proposals":"337"},{"failed_proposals":"0","successful_proposals":"44"},{"failed_proposals":"0","successful_proposals":"39"},{"failed_proposals":"0","successful_proposals":"35"},{"failed_proposals":"0","successful_proposals":"31"},{"failed_proposals":"0","successful_proposals":"39"},{"failed_proposals":"0","successful_proposals":"899"},{"failed_proposals":"1","successful_proposals":"818"},{"failed_proposals":"0","successful_proposals":"844"},{"failed_proposals":"6","successful_proposals":"0"},{"failed_proposals":"4","successful_proposals":"769"},{"failed_proposals":"0","successful_proposals":"890"},{"failed_proposals":"1","successful_proposals":"836"},{"failed_proposals":"0","successful_proposals":"862"},{"failed_proposals":"0","successful_proposals":"771"},{"failed_proposals":"0","successful_proposals":"722"},{"failed_proposals":"0","successful_proposals":"629"}]}`

	writeResource3 = `{"microseconds":"1665614928907827"}`

	writeResource4 = `{"epoch_interval":"7200000000","height":"44991655","new_block_events":{"counter":"44991656","guid":{"id":{"addr":"0x1","creation_num":"3"}}},"update_epoch_interval_events":{"counter":"0","guid":{"id":{"addr":"0x1","creation_num":"4"}}}}`

	writeResource5 = `{"validators":[{"failed_proposals":"0","successful_proposals":"49"},{"failed_proposals":"0","successful_proposals":"37"},{"failed_proposals":"0","successful_proposals":"33"},{"failed_proposals":"0","successful_proposals":"29"},{"failed_proposals":"0","successful_proposals":"18"},{"failed_proposals":"0","successful_proposals":"142"},{"failed_proposals":"0","successful_proposals":"78"},{"failed_proposals":"0","successful_proposals":"33"},{"failed_proposals":"0","successful_proposals":"20"},{"failed_proposals":"0","successful_proposals":"67"},{"failed_proposals":"0","successful_proposals":"126"},{"failed_proposals":"0","successful_proposals":"117"},{"failed_proposals":"0","successful_proposals":"134"},{"failed_proposals":"0","successful_proposals":"125"},{"failed_proposals":"0","successful_proposals":"250"},{"failed_proposals":"0","successful_proposals":"19"},{"failed_proposals":"0","successful_proposals":"23"},{"failed_proposals":"0","successful_proposals":"24"},{"failed_proposals":"0","successful_proposals":"24"},{"failed_proposals":"0","successful_proposals":"16"},{"failed_proposals":"0","successful_proposals":"509"},{"failed_proposals":"0","successful_proposals":"543"},{"failed_proposals":"0","successful_proposals":"536"},{"failed_proposals":"0","successful_proposals":"497"},{"failed_proposals":"0","successful_proposals":"547"},{"failed_proposals":"0","successful_proposals":"525"},{"failed_proposals":"0","successful_proposals":"501"},{"failed_proposals":"0","successful_proposals":"501"},{"failed_proposals":"0","successful_proposals":"478"},{"failed_proposals":"0","successful_proposals":"495"},{"failed_proposals":"0","successful_proposals":"385"},{"failed_proposals":"0","successful_proposals":"19"},{"failed_proposals":"0","successful_proposals":"32"},{"failed_proposals":"0","successful_proposals":"21"},{"failed_proposals":"0","successful_proposals":"23"},{"failed_proposals":"0","successful_proposals":"28"},{"failed_proposals":"0","successful_proposals":"26"},{"failed_proposals":"0","successful_proposals":"24"},{"failed_proposals":"0","successful_proposals":"23"},{"failed_proposals":"0","successful_proposals":"34"},{"failed_proposals":"0","successful_proposals":"28"},{"failed_proposals":"0","successful_proposals":"25"},{"failed_proposals":"0","successful_proposals":"22"},{"failed_proposals":"0","successful_proposals":"516"},{"failed_proposals":"0","successful_proposals":"24"},{"failed_proposals":"0","successful_proposals":"539"},{"failed_proposals":"0","successful_proposals":"32"},{"failed_proposals":"0","successful_proposals":"23"},{"failed_proposals":"0","successful_proposals":"23"},{"failed_proposals":"0","successful_proposals":"22"},{"failed_proposals":"0","successful_proposals":"422"},{"failed_proposals":"0","successful_proposals":"475"},{"failed_proposals":"0","successful_proposals":"24"},{"failed_proposals":"0","successful_proposals":"24"},{"failed_proposals":"0","successful_proposals":"95"},{"failed_proposals":"0","successful_proposals":"23"},{"failed_proposals":"0","successful_proposals":"353"},{"failed_proposals":"0","successful_proposals":"46"},{"failed_proposals":"0","successful_proposals":"24"},{"failed_proposals":"0","successful_proposals":"21"},{"failed_proposals":"0","successful_proposals":"22"},{"failed_proposals":"0","successful_proposals":"32"},{"failed_proposals":"0","successful_proposals":"527"},{"failed_proposals":"0","successful_proposals":"513"},{"failed_proposals":"0","successful_proposals":"297"},{"failed_proposals":"0","successful_proposals":"393"},{"failed_proposals":"0","successful_proposals":"29"},{"failed_proposals":"0","successful_proposals":"27"},{"failed_proposals":"0","successful_proposals":"206"},{"failed_proposals":"0","successful_proposals":"30"},{"failed_proposals":"0","successful_proposals":"28"},{"failed_proposals":"0","successful_proposals":"22"},{"failed_proposals":"0","successful_proposals":"15"},{"failed_proposals":"0","successful_proposals":"170"},{"failed_proposals":"0","successful_proposals":"102"},{"failed_proposals":"0","successful_proposals":"92"},{"failed_proposals":"0","successful_proposals":"96"},{"failed_proposals":"0","successful_proposals":"39"},{"failed_proposals":"0","successful_proposals":"508"},{"failed_proposals":"0","successful_proposals":"459"},{"failed_proposals":"0","successful_proposals":"21"},{"failed_proposals":"0","successful_proposals":"535"},{"failed_proposals":"0","successful_proposals":"30"},{"failed_proposals":"0","successful_proposals":"28"},{"failed_proposals":"0","successful_proposals":"523"},{"failed_proposals":"0","successful_proposals":"25"},{"failed_proposals":"0","successful_proposals":"482"},{"failed_proposals":"0","successful_proposals":"151"},{"failed_proposals":"0","successful_proposals":"128"},{"failed_proposals":"0","successful_proposals":"532"},{"failed_proposals":"0","successful_proposals":"412"},{"failed_proposals":"0","successful_proposals":"400"},{"failed_proposals":"0","successful_proposals":"25"},{"failed_proposals":"0","successful_proposals":"16"},{"failed_proposals":"0","successful_proposals":"441"},{"failed_proposals":"0","successful_proposals":"526"},{"failed_proposals":"0","successful_proposals":"470"},{"failed_proposals":"0","successful_proposals":"338"},{"failed_proposals":"0","successful_proposals":"372"},{"failed_proposals":"0","successful_proposals":"53"},{"failed_proposals":"0","successful_proposals":"425"},{"failed_proposals":"0","successful_proposals":"41"},{"failed_proposals":"0","successful_proposals":"9"},{"failed_proposals":"0","successful_proposals":"17"}]}`

	writeResource6 = `{"microseconds":"1680843472566566"}`

	writeResource7 = `{"coin":{"value":"103186898"},"deposit_events":{"counter":"21","guid":{"id":{"addr":"0x49de4df35975d3e3563d276f9c54524d2588197c6dd6906e9f5f083570351c44","creation_num":"2"}}},"frozen":false,"withdraw_events":{"counter":"70","guid":{"id":{"addr":"0x49de4df35975d3e3563d276f9c54524d2588197c6dd6906e9f5f083570351c44","creation_num":"3"}}}}`

	writeResource8 = `{"coin":{"value":"141100965309177"},"deposit_events":{"counter":"41170","guid":{"id":{"addr":"0x84b1675891d370d5de8f169031f9c3116d7add256ecf50a4bc71e3135ddba6e0","creation_num":"2"}}},"frozen":false,"withdraw_events":{"counter":"52015","guid":{"id":{"addr":"0x84b1675891d370d5de8f169031f9c3116d7add256ecf50a4bc71e3135ddba6e0","creation_num":"3"}}}}`

	writeResource9 = `{"authentication_key":"0x84b1675891d370d5de8f169031f9c3116d7add256ecf50a4bc71e3135ddba6e0","coin_register_events":{"counter":"1","guid":{"id":{"addr":"0x84b1675891d370d5de8f169031f9c3116d7add256ecf50a4bc71e3135ddba6e0","creation_num":"0"}}},"guid_creation_num":"4","key_rotation_events":{"counter":"0","guid":{"id":{"addr":"0x84b1675891d370d5de8f169031f9c3116d7add256ecf50a4bc71e3135ddba6e0","creation_num":"1"}}},"rotation_capability_offer":{"for":{"vec":[]}},"sequence_number":"52015","signer_capability_offer":{"for":{"vec":[]}}}`

	eventData1 = `{"epoch":"2","failed_proposer_indices":[],"hash":"0x7eee0512cef0754f58890802a6c3ba1e1032bb1820f45b825adbbee4f10d9d71","height":"10000","previous_block_votes_bitvec":"0xffff0e36","proposer":"0x324df1e27c4129a58d73851ae0e9366064dc666a73e747051e203694a4cb257","round":"10014","time_microseconds":"1665614928907827"}`

	eventData2 = `{"epoch":"2120","failed_proposer_indices":[],"hash":"0xa6ee6266348bc84793388aa376453a4c076821b479b97f5faba68ec03cf7b253","height":"44991655","previous_block_votes_bitvec":"0x81ffc7eeffe8ef5e8fe5bdffbf","proposer":"0x9da88926fd4d773fd499fc41830a82fe9c9ff3508435e7a16b2d8f529e77cdda","round":"19479","time_microseconds":"1680843472566566"}`

	eventData3 = `{"amount":"100000000"}`
)

type aptosTestSuite struct {
	suite.Suite

	app           testapp.TestApp
	logger        *zap.Logger
	client        client.Client
	restapiClient restapi.Client
	parser        parser.Parser
}

func TestIntegrationAptosTestSuite(t *testing.T) {
	suite.Run(t, new(aptosTestSuite))
}

func (s *aptosTestSuite) SetupTest() {
	var deps struct {
		fx.In
		Client        client.Client  `name:"slave"`
		RestapiClient restapi.Client `name:"slave"`
		Parser        parser.Parser
	}
	s.app = testapp.New(
		s.T(),
		testapp.WithFunctional(),
		testapp.WithBlockchainNetwork(common.Blockchain_BLOCKCHAIN_APTOS, common.Network_NETWORK_APTOS_MAINNET),
		jsonrpc.Module,
		restapi.Module,
		client.Module,
		parser.Module,
		fx.Provide(dlq.NewNop),
		fx.Populate(&deps),
	)

	s.logger = s.app.Logger()
	s.client = deps.Client
	s.restapiClient = deps.RestapiClient
	s.parser = deps.Parser
	s.Require().NotNil(s.client)
	s.Require().NotNil(s.restapiClient)
	s.Require().NotNil(s.parser)
}

func (s *aptosTestSuite) TearDownTest() {
	if s.app != nil {
		s.app.Close()
	}
}

func (s *aptosTestSuite) TestBatchGetBlockMetadata_General() {
	require := testutil.Require(s.T())

	ctx := context.Background()
	const numBlocks = 25

	metadatas, err := s.client.BatchGetBlockMetadata(ctx, aptosTag, aptosHeight, aptosHeight+numBlocks)
	require.NoError(err)
	s.logger.Info("aptos metadatas", zap.Reflect("metadatas", metadatas))
	require.Equal(numBlocks, len(metadatas))

	metadata := metadatas[0]
	require.Equal(aptosTag, metadata.Tag)
	require.Equal(aptosHeight, metadata.Height)
	require.Equal(aptosParentHeight, metadata.ParentHeight)
	require.Equal(aptosHash, metadata.Hash)
	require.Equal(aptosParentHash, metadata.ParentHash)
	require.Equal(testutil.MustTimestamp(aptosTimestamp), metadata.Timestamp)

	for i := uint64(1); i < numBlocks; i++ {
		metadata = metadatas[i]
		require.Equal(aptosTag, metadata.Tag)
		require.Equal(aptosHeight+i, metadata.Height)
		require.Equal(aptosHeight+i-1, metadata.ParentHeight)
	}
}

func (s *aptosTestSuite) TestBatchGetBlockMetadata_Specific() {
	require := testutil.Require(s.T())

	ctx := context.Background()
	const numBlocks = 2

	metadatas, err := s.client.BatchGetBlockMetadata(ctx, aptosTag, aptosHeight, aptosHeight+numBlocks)
	require.NoError(err)
	s.logger.Info("aptos metadatas", zap.Reflect("metadatas", metadatas))
	require.Equal(numBlocks, len(metadatas))

	metadata := metadatas[0]
	require.Equal(aptosTag, metadata.Tag)
	require.Equal(aptosHeight, metadata.Height)
	require.Equal(aptosParentHeight, metadata.ParentHeight)
	require.Equal(aptosHash, metadata.Hash)
	require.Equal(aptosParentHash, metadata.ParentHash)
	require.Equal(testutil.MustTimestamp(aptosTimestamp), metadata.Timestamp)

	metadata = metadatas[1]
	require.Equal(aptosTag, metadata.Tag)
	require.Equal(aptosHeight1, metadata.Height)
	require.Equal(aptosParentHeight1, metadata.ParentHeight)
	require.Equal(aptosHash1, metadata.Hash)
	require.Equal(aptosParentHash1, metadata.ParentHash)
	require.Equal(testutil.MustTimestamp(aptosTimestamp1), metadata.Timestamp)
}

func (s *aptosTestSuite) TestGetBlockByHeight_Success() {
	require := testutil.Require(s.T())

	ctx := context.Background()

	block, err := s.client.GetBlockByHeight(ctx, aptosTag, aptosHeight)
	require.NoError(err)
	s.logger.Info("aptos block", zap.Reflect("block", block.Metadata))
	require.Equal(common.Blockchain_BLOCKCHAIN_APTOS, block.Blockchain)
	require.Equal(common.Network_NETWORK_APTOS_MAINNET, block.Network)
	require.Equal(aptosTag, block.Metadata.Tag)
	require.Equal(aptosHeight, block.Metadata.Height)
	require.Equal(aptosParentHeight, block.Metadata.ParentHeight)
	require.Equal(aptosHash, block.Metadata.Hash)
	require.Equal(aptosParentHash, block.Metadata.ParentHash)
	require.Less(0, len(block.GetAptos().GetBlock()))
	require.False(block.Metadata.Skipped)
	require.Equal(testutil.MustTimestamp(aptosTimestamp), block.Metadata.Timestamp)

	block, err = s.client.GetBlockByHeight(ctx, aptosTag, aptosHeight1)
	require.NoError(err)
	s.logger.Info("aptos block", zap.Reflect("block", block.Metadata))
	require.Equal(common.Blockchain_BLOCKCHAIN_APTOS, block.Blockchain)
	require.Equal(common.Network_NETWORK_APTOS_MAINNET, block.Network)
	require.Equal(aptosTag, block.Metadata.Tag)
	require.Equal(aptosHeight1, block.Metadata.Height)
	require.Equal(aptosParentHeight1, block.Metadata.ParentHeight)
	require.Equal(aptosHash1, block.Metadata.Hash)
	require.Equal(aptosParentHash1, block.Metadata.ParentHash)
	require.Less(0, len(block.GetAptos().GetBlock()))
	require.False(block.Metadata.Skipped)
	require.Equal(testutil.MustTimestamp(aptosTimestamp1), block.Metadata.Timestamp)
}

func (s *aptosTestSuite) TestGetBlockByHeight_NotFound() {
	require := testutil.Require(s.T())

	ctx := context.Background()
	// Query the block height which is uint64_max.
	_, err := s.client.GetBlockByHeight(ctx, aptosTag, math.MaxUint64)
	require.Error(err)
	require.True(xerrors.Is(err, client.ErrBlockNotFound), err.Error())
}

func (s *aptosTestSuite) TestGetBlockByHash_Success() {
	require := testutil.Require(s.T())

	ctx := context.Background()

	block, err := s.client.GetBlockByHash(ctx, aptosTag, aptosHeight, aptosHash)
	require.NoError(err)
	s.logger.Info("aptos block", zap.Reflect("block", block.Metadata))
	require.Equal(common.Blockchain_BLOCKCHAIN_APTOS, block.Blockchain)
	require.Equal(common.Network_NETWORK_APTOS_MAINNET, block.Network)
	require.Equal(aptosTag, block.Metadata.Tag)
	require.Equal(aptosHeight, block.Metadata.Height)
	require.Equal(aptosParentHeight, block.Metadata.ParentHeight)
	require.Equal(aptosHash, block.Metadata.Hash)
	require.Equal(aptosParentHash, block.Metadata.ParentHash)
	require.Less(0, len(block.GetAptos().GetBlock()))
	require.False(block.Metadata.Skipped)
	require.Equal(testutil.MustTimestamp(aptosTimestamp), block.Metadata.Timestamp)
}

func (s *aptosTestSuite) TestGetBlockByHash_HashNotMatched() {
	require := testutil.Require(s.T())

	ctx := context.Background()

	// Use a wrong dummy hash to make the hash validation failed.
	block, err := s.client.GetBlockByHash(ctx, aptosTag, aptosHeight, "dummy_hash")
	require.Nil(block)
	require.Error(err)
	require.Contains(err.Error(), "expected=dummy_hash")
	require.Contains(err.Error(), fmt.Sprintf("actual=%s", aptosHash))
}

func (s *aptosTestSuite) TestGetLatestHeight() {
	require := testutil.Require(s.T())

	ctx := context.Background()
	height, err := s.client.GetLatestHeight(ctx)
	require.NoError(err)
	s.logger.Info("aptos height", zap.Uint64("height", height))
	// For the time when this test is added, the latest block height is more than 40M.
	require.Greater(height, uint64(40000000))
}

// Test a block with block metadata transaction and state checkpoint transaction.
func (s *aptosTestSuite) TestGetAndParseBlock() {
	require := testutil.Require(s.T())

	ctx := context.Background()

	block, err := s.client.GetBlockByHeight(ctx, aptosTag, aptosHeight)
	require.NoError(err)
	s.logger.Info("aptos block", zap.Reflect("block", block.Metadata))
	require.Equal(common.Blockchain_BLOCKCHAIN_APTOS, block.Blockchain)
	require.Equal(common.Network_NETWORK_APTOS_MAINNET, block.Network)
	require.Equal(aptosTag, block.Metadata.Tag)
	require.Equal(aptosHeight, block.Metadata.Height)
	require.Equal(aptosParentHeight, block.Metadata.ParentHeight)
	require.Equal(aptosHash, block.Metadata.Hash)
	require.Equal(aptosParentHash, block.Metadata.ParentHash)
	require.Less(0, len(block.GetAptos().GetBlock()))
	require.False(block.Metadata.Skipped)
	require.Equal(testutil.MustTimestamp(aptosTimestamp), block.Metadata.Timestamp)

	numTransactions := 2
	nativeBlock, err := s.parser.ParseNativeBlock(context.Background(), block)
	require.NoError(err)
	expectedTimestamp := testutil.MustTimestamp(aptosTimestamp)
	require.Equal(common.Blockchain_BLOCKCHAIN_APTOS, nativeBlock.Blockchain)
	require.Equal(common.Network_NETWORK_APTOS_MAINNET, nativeBlock.Network)
	require.Equal(aptosTag, nativeBlock.Tag)
	require.Equal(aptosHeight, nativeBlock.Height)
	require.Equal(aptosParentHeight, nativeBlock.ParentHeight)
	require.Equal(aptosHash, nativeBlock.Hash)
	require.Equal(aptosParentHash, nativeBlock.ParentHash)
	require.Equal(expectedTimestamp, nativeBlock.Timestamp)
	require.Equal(numTransactions, int(nativeBlock.NumTransactions))

	aptosBlock := nativeBlock.GetAptos()
	require.NotNil(aptosBlock)
	require.Equal(&api.AptosHeader{
		BlockHeight: aptosHeight,
		BlockHash:   aptosHash,
		BlockTime:   expectedTimestamp,
	}, aptosBlock.GetHeader())
	require.Equal(numTransactions, len(aptosBlock.GetTransactions()))

	// The first transaction: block metadata transaction
	transaction := aptosBlock.GetTransactions()[0]
	require.Equal(&api.AptosTransaction{
		Timestamp:   expectedTimestamp,
		Version:     20083,
		BlockHeight: 10000,
		Info: &api.AptosTransactionInfo{
			Hash:                "0x0a0ea9f2c10639794ad183308f33ae01722675a6b13115aaf9af284d1f64fad2",
			StateChangeHash:     "0x69dfd32387cdf3cecfd957812bb25f83586b0b5c1a3e1eabdf116f128bc9503c",
			EventRootHash:       "0xcdb57d4606ea74b47b6f7b304376d18e667dfbcce271ee7cbd4f0cd69b5c2736",
			GasUsed:             0,
			Success:             true,
			VmStatus:            "Executed successfully",
			AccumulatorRootHash: "0x55488600270a86152286cbc9b38bcde1fe1ac42cbc8135335d05b6e33a4090cd",
			Changes: []*api.AptosWriteSetChange{
				{
					Type: api.AptosWriteSetChange_WRITE_RESOURCE,
					Change: &api.AptosWriteSetChange_WriteResource{
						WriteResource: &api.AptosWriteResource{
							Address:      "0x1",
							StateKeyHash: "0x5ddf404c60e96e9485beafcabb95609fed8e38e941a725cae4dcec8296fb32d7",
							TypeStr:      "0x1::block::BlockResource",
							Data:         writeResource1,
						},
					},
				},
				{
					Type: api.AptosWriteSetChange_WRITE_RESOURCE,
					Change: &api.AptosWriteSetChange_WriteResource{
						WriteResource: &api.AptosWriteResource{
							Address:      "0x1",
							StateKeyHash: "0x8048c954221814b04533a9f0a9946c3a8d472ac62df5accb9f47c097e256e8b6",
							TypeStr:      "0x1::stake::ValidatorPerformance",
							Data:         writeResource2,
						},
					},
				},
				{
					Type: api.AptosWriteSetChange_WRITE_RESOURCE,
					Change: &api.AptosWriteSetChange_WriteResource{
						WriteResource: &api.AptosWriteResource{
							Address:      "0x1",
							StateKeyHash: "0x7b1615bf012d3c94223f3f76287ee2f7bdf31d364071128b256aeff0841b626d",
							TypeStr:      "0x1::timestamp::CurrentTimeMicroseconds",
							Data:         writeResource3,
						},
					},
				},
			},
		},
		Type: api.AptosTransaction_BLOCK_METADATA,
		TxnData: &api.AptosTransaction_BlockMetadata{
			BlockMetadata: &api.AptosBlockMetadataTransaction{
				Id:    "0x7eee0512cef0754f58890802a6c3ba1e1032bb1820f45b825adbbee4f10d9d71",
				Epoch: 2,
				Round: 10014,
				Events: []*api.AptosEvent{
					{
						Key: &api.AptosEventKey{
							CreationNumber: 3,
							AccountAddress: "0x1",
						},
						SequenceNumber: 10000,
						Type:           "0x1::block::NewBlockEvent",
						Data:           string(eventData1),
					},
				},
				PreviousBlockVotesBitvec: []byte{
					255,
					255,
					14,
					54,
				},
				Proposer:              "0x324df1e27c4129a58d73851ae0e9366064dc666a73e747051e203694a4cb257",
				FailedProposerIndices: []uint32{},
			},
		},
	}, transaction)

	// The second transaction: state checkpoint transaction
	transaction = aptosBlock.GetTransactions()[1]
	require.Equal(&api.AptosTransaction{
		Timestamp:   expectedTimestamp,
		Version:     20084,
		BlockHeight: 10000,
		Info: &api.AptosTransactionInfo{
			Hash:            "0x67131d11785ca4d027c136f4f4a80e14b30e4acc830f2dc4890e82244f9aad31",
			StateChangeHash: "0xafb6e14fe47d850fd0a7395bcfb997ffacf4715e0f895cc162c218e4a7564bc6",
			EventRootHash:   "0x414343554d554c41544f525f504c414345484f4c4445525f4841534800000000",
			OptionalStateCheckpointHash: &api.AptosTransactionInfo_StateCheckpointHash{
				StateCheckpointHash: "0x32b73b0478aae08821a350d9e28ec4bec9d55fa93a8a04f55e0fc7912fc0ce78",
			},
			GasUsed:             0,
			Success:             true,
			VmStatus:            "Executed successfully",
			AccumulatorRootHash: "0x8a662ed7ff3c2aa743494c86e763440362b3f409a46124342015a910ff51acb8",
			Changes:             []*api.AptosWriteSetChange{},
		},
		Type: api.AptosTransaction_STATE_CHECKPOINT,
		TxnData: &api.AptosTransaction_StateCheckpoint{
			StateCheckpoint: &api.AptosStateCheckpointTransaction{},
		},
	}, transaction)
}

// Test a block with block metadata transaction, user transaction, and state checkpoint transaction.
func (s *aptosTestSuite) TestGetAndParseBlock_UserTransaction() {
	require := testutil.Require(s.T())

	ctx := context.Background()

	block, err := s.client.GetBlockByHeight(ctx, aptosTag, aptosHeight2)
	require.NoError(err)
	s.logger.Info("aptos block", zap.Reflect("block", block.Metadata))
	require.Equal(common.Blockchain_BLOCKCHAIN_APTOS, block.Blockchain)
	require.Equal(common.Network_NETWORK_APTOS_MAINNET, block.Network)
	require.Equal(aptosTag, block.Metadata.Tag)
	require.Equal(aptosHeight2, block.Metadata.Height)
	require.Equal(aptosParentHeight2, block.Metadata.ParentHeight)
	require.Equal(aptosHash2, block.Metadata.Hash)
	require.Equal(aptosParentHash2, block.Metadata.ParentHash)
	require.Less(0, len(block.GetAptos().GetBlock()))
	require.False(block.Metadata.Skipped)
	require.Equal(testutil.MustTimestamp(aptosTimestamp2), block.Metadata.Timestamp)

	numTransactions := 3
	nativeBlock, err := s.parser.ParseNativeBlock(context.Background(), block)
	require.NoError(err)
	expectedTimestamp := testutil.MustTimestamp(aptosTimestamp2)
	require.Equal(common.Blockchain_BLOCKCHAIN_APTOS, nativeBlock.Blockchain)
	require.Equal(common.Network_NETWORK_APTOS_MAINNET, nativeBlock.Network)
	require.Equal(aptosTag, nativeBlock.Tag)
	require.Equal(aptosHeight2, nativeBlock.Height)
	require.Equal(aptosParentHeight2, nativeBlock.ParentHeight)
	require.Equal(aptosHash2, nativeBlock.Hash)
	require.Equal(aptosParentHash2, nativeBlock.ParentHash)
	require.Equal(expectedTimestamp, nativeBlock.Timestamp)
	require.Equal(numTransactions, int(nativeBlock.NumTransactions))

	aptosBlock := nativeBlock.GetAptos()
	require.NotNil(aptosBlock)
	require.Equal(&api.AptosHeader{
		BlockHeight: aptosHeight2,
		BlockHash:   aptosHash2,
		BlockTime:   expectedTimestamp,
	}, aptosBlock.GetHeader())
	require.Equal(numTransactions, len(aptosBlock.GetTransactions()))

	// The first transaction: block metadata transaction
	transaction := aptosBlock.GetTransactions()[0]
	require.Equal(&api.AptosTransaction{
		Timestamp:   expectedTimestamp,
		Version:     115805295,
		BlockHeight: aptosHeight2,
		Info: &api.AptosTransactionInfo{
			Hash:                "0x9950e9fee0c1d47330a465221fc4a5081423eea429a83742610d2c548bf11b83",
			StateChangeHash:     "0x168f34d1b30a26a971a919885525229bdbbf2718e400c50635e5a6ac5dc9bef8",
			EventRootHash:       "0x68ba9d386ce0a19f47b0f9ffe4cd27b970cb3874daaa08c7e961b099457b1ef4",
			GasUsed:             0,
			Success:             true,
			VmStatus:            "Executed successfully",
			AccumulatorRootHash: "0x6cd1c61bcb1822dfb3c4e10b4b0d66e3da5215a2ce504b9ee0114c3a8a70c0f0",
			Changes: []*api.AptosWriteSetChange{
				{
					Type: api.AptosWriteSetChange_WRITE_RESOURCE,
					Change: &api.AptosWriteSetChange_WriteResource{
						WriteResource: &api.AptosWriteResource{
							Address:      "0x1",
							StateKeyHash: "0x5ddf404c60e96e9485beafcabb95609fed8e38e941a725cae4dcec8296fb32d7",
							TypeStr:      "0x1::block::BlockResource",
							Data:         writeResource4,
						},
					},
				},
				{
					Type: api.AptosWriteSetChange_WRITE_RESOURCE,
					Change: &api.AptosWriteSetChange_WriteResource{
						WriteResource: &api.AptosWriteResource{
							Address:      "0x1",
							StateKeyHash: "0x8048c954221814b04533a9f0a9946c3a8d472ac62df5accb9f47c097e256e8b6",
							TypeStr:      "0x1::stake::ValidatorPerformance",
							Data:         writeResource5,
						},
					},
				},
				{
					Type: api.AptosWriteSetChange_WRITE_RESOURCE,
					Change: &api.AptosWriteSetChange_WriteResource{
						WriteResource: &api.AptosWriteResource{
							Address:      "0x1",
							StateKeyHash: "0x7b1615bf012d3c94223f3f76287ee2f7bdf31d364071128b256aeff0841b626d",
							TypeStr:      "0x1::timestamp::CurrentTimeMicroseconds",
							Data:         writeResource6,
						},
					},
				},
			},
		},
		Type: api.AptosTransaction_BLOCK_METADATA,
		TxnData: &api.AptosTransaction_BlockMetadata{
			BlockMetadata: &api.AptosBlockMetadataTransaction{
				Id:    "0xa6ee6266348bc84793388aa376453a4c076821b479b97f5faba68ec03cf7b253",
				Epoch: 2120,
				Round: 19479,
				Events: []*api.AptosEvent{
					{
						Key: &api.AptosEventKey{
							CreationNumber: 3,
							AccountAddress: "0x1",
						},
						SequenceNumber: 44991655,
						Type:           "0x1::block::NewBlockEvent",
						Data:           string(eventData2),
					},
				},
				PreviousBlockVotesBitvec: []byte{
					129,
					255,
					199,
					238,
					255,
					232,
					239,
					94,
					143,
					229,
					189,
					255,
					191,
				},
				Proposer:              "0x9da88926fd4d773fd499fc41830a82fe9c9ff3508435e7a16b2d8f529e77cdda",
				FailedProposerIndices: []uint32{},
			},
		},
	}, transaction)

	// The second transaction: user transaction
	transaction = aptosBlock.GetTransactions()[1]
	require.Equal(&api.AptosTransaction{
		Timestamp:   expectedTimestamp,
		Version:     115805296,
		BlockHeight: aptosHeight2,
		Info: &api.AptosTransactionInfo{
			Hash:                "0x9e3bc0297f3eb7a96267ac277fea8a3731ede0ed71387eed0ade0910a272c2bd",
			StateChangeHash:     "0x81fa1390a90afc3966d2fd76347dbffdb18e701d7b24231fa2dee0f3d427d799",
			EventRootHash:       "0x464cf21a3864b0a1f7681a22a9fcef9589db80b67c239ecbecb03149070a4566",
			GasUsed:             590,
			Success:             true,
			VmStatus:            "Executed successfully",
			AccumulatorRootHash: "0xcf6a0291afb04e70ea91532354ed4688633c3892c407c55657d7b9e1319e4450",
			Changes: []*api.AptosWriteSetChange{
				{
					Type: api.AptosWriteSetChange_WRITE_RESOURCE,
					Change: &api.AptosWriteSetChange_WriteResource{
						WriteResource: &api.AptosWriteResource{
							Address:      "0x49de4df35975d3e3563d276f9c54524d2588197c6dd6906e9f5f083570351c44",
							StateKeyHash: "0x2d740310e9734f4eb2148c649a15543e5311cfee63c61d80b117f89ab79e11b0",
							TypeStr:      "0x1::coin::CoinStore<0x1::aptos_coin::AptosCoin>",
							Data:         writeResource7,
						},
					},
				},
				{
					Type: api.AptosWriteSetChange_WRITE_RESOURCE,
					Change: &api.AptosWriteSetChange_WriteResource{
						WriteResource: &api.AptosWriteResource{
							Address:      "0x84b1675891d370d5de8f169031f9c3116d7add256ecf50a4bc71e3135ddba6e0",
							StateKeyHash: "0x3af446f0f4f0065f2f8427159f93a96fbebcea04bdb409d0ac9bf6a599c9e7cf",
							TypeStr:      "0x1::coin::CoinStore<0x1::aptos_coin::AptosCoin>",
							Data:         writeResource8,
						},
					},
				},
				{
					Type: api.AptosWriteSetChange_WRITE_RESOURCE,
					Change: &api.AptosWriteSetChange_WriteResource{
						WriteResource: &api.AptosWriteResource{
							Address:      "0x84b1675891d370d5de8f169031f9c3116d7add256ecf50a4bc71e3135ddba6e0",
							StateKeyHash: "0x58eabee9ff80b8c553c0f5677d10d8b66e99e4d9284ab8789eda7efc1ee51daa",
							TypeStr:      "0x1::account::Account",
							Data:         writeResource9,
						},
					},
				},
				{
					Type: api.AptosWriteSetChange_WRITE_TABLE_ITEM,
					Change: &api.AptosWriteSetChange_WriteTableItem{
						WriteTableItem: &api.AptosWriteTableItem{
							StateKeyHash: "0x6e4b28d40f98a106a65163530924c0dcb40c1349d3aa915d108b4d6cfc1ddb19",
							Handle:       "0x1b854694ae746cdbd8d44186ca4929b2b337df21d1c74633be19b2710552fdca",
							Key:          "0x0619dc29a0aac8fa146714058e8dd6d2d0f3bdf5f6331907bf91f3acd81e6935",
							Value:        "0x057f217efa476d010000000000000000",
							Data:         &api.AptosWriteTableItemData{},
						},
					},
				},
			},
		},
		Type: api.AptosTransaction_USER,
		TxnData: &api.AptosTransaction_User{
			User: &api.AptosUserTransaction{
				Request: &api.AptosUserTransactionRequest{
					Sender:                  "0x84b1675891d370d5de8f169031f9c3116d7add256ecf50a4bc71e3135ddba6e0",
					SequenceNumber:          52014,
					MaxGasAmount:            4000,
					GasUnitPrice:            144,
					ExpirationTimestampSecs: &timestamp.Timestamp{Seconds: 1680886669},
					Payload: &api.AptosTransactionPayload{
						Type: api.AptosTransactionPayload_ENTRY_FUNCTION_PAYLOAD,
						Payload: &api.AptosTransactionPayload_EntryFunctionPayload{
							EntryFunctionPayload: &api.AptosEntryFunctionPayload{
								Function: &api.AptosEntryFunctionId{
									Module: &api.AptosMoveModuleId{
										Address: "0x1",
										Name:    "aptos_account",
									},
									FunctionName: "transfer",
								},
								TypeArguments: []string{},
								Arguments: [][]byte{
									[]byte(`"0x49de4df35975d3e3563d276f9c54524d2588197c6dd6906e9f5f083570351c44"`),
									[]byte(`"100000000"`),
								},
							},
						},
					},
					Signature: &api.AptosSignature{
						Type: api.AptosSignature_ED25519,
						Signature: &api.AptosSignature_Ed25519{
							Ed25519: &api.AptosEd25519Signature{
								PublicKey: "0x5e9a2968688a26663d59693541591947b9b00f37c0ea2bed1cb7bcc90040a58e",
								Signature: "0xd1e592cbbc0fb4434522c392b0925ec71a4bc24bdfff6877fc8d5aa67e3cb93e6e28c8678615b7fbbcd1cad1e3053ae8cd5be287448e0d76e9669aa87c2bf700",
							},
						},
					},
				},
				Events: []*api.AptosEvent{
					{
						Key: &api.AptosEventKey{
							CreationNumber: 3,
							AccountAddress: "0x84b1675891d370d5de8f169031f9c3116d7add256ecf50a4bc71e3135ddba6e0",
						},
						SequenceNumber: 52014,
						Type:           "0x1::coin::WithdrawEvent",
						Data:           string(eventData3),
					},
					{
						Key: &api.AptosEventKey{
							CreationNumber: 2,
							AccountAddress: "0x49de4df35975d3e3563d276f9c54524d2588197c6dd6906e9f5f083570351c44",
						},
						SequenceNumber: 20,
						Type:           "0x1::coin::DepositEvent",
						Data:           string(eventData3),
					},
				},
			},
		},
	}, transaction)

	// The third transaction: state checkpoint transaction
	transaction = aptosBlock.GetTransactions()[2]
	require.Equal(&api.AptosTransaction{
		Timestamp:   expectedTimestamp,
		Version:     115805297,
		BlockHeight: aptosHeight2,
		Info: &api.AptosTransactionInfo{
			Hash:            "0xb08a4aa7f4815db5e37f187615e2718eb68f2c9ce327f833e21298ad28a4fdcd",
			StateChangeHash: "0xafb6e14fe47d850fd0a7395bcfb997ffacf4715e0f895cc162c218e4a7564bc6",
			EventRootHash:   "0x414343554d554c41544f525f504c414345484f4c4445525f4841534800000000",
			OptionalStateCheckpointHash: &api.AptosTransactionInfo_StateCheckpointHash{
				StateCheckpointHash: "0x26e5843a572c8666ea2f4f564d4d957bb777a9ae3036ca64f0f508bfd99671d2",
			},
			GasUsed:             0,
			Success:             true,
			VmStatus:            "Executed successfully",
			AccumulatorRootHash: "0x3bf19636dbfb501033634081ef49c74c8e734b4349751c5c4173c5eb796d77dc",
			Changes:             []*api.AptosWriteSetChange{},
		},
		Type: api.AptosTransaction_STATE_CHECKPOINT,
		TxnData: &api.AptosTransaction_StateCheckpoint{
			StateCheckpoint: &api.AptosStateCheckpointTransaction{},
		},
	}, transaction)
}
