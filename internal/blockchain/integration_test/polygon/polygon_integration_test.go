package polygon_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/suite"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/blockchain/client"
	"github.com/coinbase/chainstorage/internal/blockchain/jsonrpc"
	"github.com/coinbase/chainstorage/internal/blockchain/parser"
	"github.com/coinbase/chainstorage/internal/blockchain/restapi"
	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/dlq"
	"github.com/coinbase/chainstorage/internal/utils/testapp"
	"github.com/coinbase/chainstorage/internal/utils/testutil"
	"github.com/coinbase/chainstorage/protos/coinbase/c3/common"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

const (
	maticTag                          = uint32(1)
	maticBlockHash                    = "0x036c404134ef4d9e715db44adbd897d2895fcb43818bdb53b17bba8b15be5f23"
	maticHeight                       = uint64(20710000)
	maticParentHash                   = "0x3ec1b852e52c40db7d5d6a520fe90821a28fea0090325b362af83563b1f1ed6a"
	maticBlockTimestamp               = "2021-10-28T16:48:59Z"
	maticHeightOfTraceTransactions    = uint64(2304)
	maticBlockHashOfTraceTransactions = "0x2e3f40ddbdb9d5316356af2ac7ac94ba3e5ccbb6fc3b9a73671cdd5bb27570e5"

	maticHeightNoTransactions    = uint64(2303)
	maticBlockHashNoTransactions = "0x9b863b8348e030fc6f2a566b7ad2914d4d9f39e93d0454e978e8509d3d14b91a"
)

type polygonIntegrationTestSuite struct {
	suite.Suite

	app    testapp.TestApp
	logger *zap.Logger
	client client.Client
	parser parser.Parser
}

func TestIntegrationPolygonTestSuite(t *testing.T) {
	suite.Run(t, new(polygonIntegrationTestSuite))
}

func (s *polygonIntegrationTestSuite) SetupTest() {
	require := testutil.Require(s.T())

	cfg, err := config.New(
		config.WithBlockchain(common.Blockchain_BLOCKCHAIN_POLYGON),
		config.WithNetwork(common.Network_NETWORK_POLYGON_MAINNET),
		config.WithEnvironment(config.EnvLocal),
	)
	require.NoError(err)

	var deps struct {
		fx.In
		Client client.Client `name:"slave"`
		Parser parser.Parser
	}
	s.app = testapp.New(
		s.T(),
		testapp.WithFunctional(),
		jsonrpc.Module,
		restapi.Module,
		client.Module,
		parser.Module,
		testapp.WithConfig(cfg),
		fx.Provide(dlq.NewNop),
		fx.Populate(&deps),
	)

	s.logger = s.app.Logger()
	s.client = deps.Client
	s.parser = deps.Parser
	require.NotNil(s.client)
	require.NotNil(s.parser)
}

func (s *polygonIntegrationTestSuite) TearDownTest() {
	if s.app != nil {
		s.app.Close()
	}
}

func (s *polygonIntegrationTestSuite) TestPolygonGetBlock() {
	tests := []struct {
		name     string
		getBlock func() (*api.Block, error)
	}{
		{
			name: "GetBlockByHash",
			getBlock: func() (*api.Block, error) {
				return s.client.GetBlockByHash(context.Background(), maticTag, maticHeight, maticBlockHash)
			},
		},
		{
			name: "GetBlockByHeight",
			getBlock: func() (*api.Block, error) {
				return s.client.GetBlockByHeight(context.Background(), maticTag, maticHeight)

			},
		},
	}

	for _, test := range tests {
		s.Run(test.name, func() {
			s.app.Logger().Info("fetching block")
			require := testutil.Require(s.T())
			rawBlock, err := test.getBlock()
			require.NoError(err)

			s.Equal(common.Blockchain_BLOCKCHAIN_POLYGON, rawBlock.Blockchain)
			s.Equal(common.Network_NETWORK_POLYGON_MAINNET, rawBlock.Network)
			s.Equal(maticTag, rawBlock.Metadata.Tag)
			s.Equal(maticBlockHash, rawBlock.Metadata.Hash)
			s.Equal(maticParentHash, rawBlock.Metadata.ParentHash)
			s.Equal(maticHeight, rawBlock.Metadata.Height)
			s.Equal(testutil.MustTimestamp(maticBlockTimestamp), rawBlock.Metadata.Timestamp)

			// See https://polygonscan.com/block/20710000
			nativeBlock, err := s.parser.ParseNativeBlock(context.Background(), rawBlock)
			require.NoError(err)
			require.Equal(common.Blockchain_BLOCKCHAIN_POLYGON, nativeBlock.Blockchain)
			require.Equal(common.Network_NETWORK_POLYGON_MAINNET, nativeBlock.Network)
			require.Equal(maticTag, nativeBlock.Tag)
			require.Equal(maticBlockHash, nativeBlock.Hash)
			require.Equal(maticParentHash, nativeBlock.ParentHash)
			require.Equal(maticHeight, nativeBlock.Height)
			require.Equal(testutil.MustTimestamp(maticBlockTimestamp), nativeBlock.Timestamp)
			require.Equal(uint64(59), nativeBlock.NumTransactions)

			block := nativeBlock.GetEthereum()
			require.NotNil(block)

			header := block.Header
			require.NotNil(header)

			require.Equal(maticBlockHash, header.Hash)
			require.Equal(maticParentHash, header.ParentHash)
			require.Equal(maticHeight, header.Number)
			require.Equal(testutil.MustTimestamp(maticBlockTimestamp), header.Timestamp)
			require.Equal("0x374e81a6bd6df0c947274dc5d7fa425759ef01647c0c4d434bc923a1c47e7865", header.Transactions[58])
			require.Equal("0x261a83017b98d869366ba6d65ef57d3a41606e6c5132f7020782e60775570cd2", header.Transactions[5])
			require.Equal("0x0000000000000000", header.Nonce)
			require.Equal("0x1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347", header.Sha3Uncles)
			require.Equal("0x2675f824c13a088301e0c433d503800680f469888b27b509aa2a80481996638840cd194f441205585857049a0a7c6101223296e0f8de20031398f5888630b21394183441e9804121c0234b3b9c2c48a0f83b8f2a834834125253881ca20cff9044de75498691a18802061072040bae604c0c043598ad0743891080b481cb018071016f20c90e8ac980b113e23421563c010165c1194d603a20504cc010381822aa819003b1d367c040024d9028882501e8c1a0fc000488f00123e92a0842714693401942d48a24580f630223e0124900c364a1188d99d3367836810e04182005001908898386018424d981904180000ea604266da8e885c16181080263121c14", header.LogsBloom)
			require.Equal("0x71b16b4dd7b26b6c2599f733dd9b4fdc17ba5759406b6737be573ad22eaf6bdf", header.TransactionsRoot)
			require.Equal("0x67a72dd5ce13a49aab6a3bebe63874c3a4fa15f32a797f022783f6bd06df84af", header.StateRoot)
			require.Equal("0x4edca83bdb6ac9ffe8a4f91de6797b8a08b66722af8072d5956078c88dbcfb7b", header.ReceiptsRoot)
			require.Equal("0x0000000000000000000000000000000000000000", header.Miner)
			require.Equal("0x7b5000af8ab69fd59eb0d4f5762bff57c9c04385", header.GetAuthor())
			require.Equal(uint64(14), header.Difficulty)
			require.Equal("243925491", header.TotalDifficulty)
			require.Equal("0xd682020983626f7288676f312e31372e32856c696e7578000000000000000000afd341ce48242077156ca38017f9b9232fac05f0766e4483a305445378b9d64737237f4d2d00e418d32bc8b02f99c47392fa7f7d795fc02c907ff1fdfc6c868801", header.ExtraData)
			require.Equal(uint64(43792), header.Size)
			require.Equal(uint64(20979735), header.GasLimit)
			require.Equal(uint64(9716028), header.GasUsed)
			require.Empty(header.Uncles)
			require.Nil(header.GetOptionalBaseFeePerGas())
			require.Equal("0x0000000000000000000000000000000000000000000000000000000000000000", header.MixHash)
			require.Equal(59, len(header.Transactions))

			// see https://polygonscan.com/tx/0x374e81a6bd6df0c947274dc5d7fa425759ef01647c0c4d434bc923a1c47e7865
			transactionIndex := uint64(58)
			transactionHash := "0x374e81a6bd6df0c947274dc5d7fa425759ef01647c0c4d434bc923a1c47e7865"
			transaction := block.Transactions[58]
			transactionFrom := "0xaf66758a4ddfc2c6a9cb3c07280b65324a325625"
			transactionTo := "0x1b02da8cb0d097eb8d57a175b88c7d8b47997506"
			s.app.Logger().Info("transaction:", zap.Reflect("transaction", transaction))
			require.Equal(transactionHash, transaction.Hash)
			require.Equal(maticBlockHash, transaction.BlockHash)
			require.Equal(transactionIndex, transaction.Index)
			require.Equal(transactionFrom, transaction.From)
			require.Equal(transactionTo, transaction.To)
			require.Equal(uint64(178100), transaction.Gas)
			require.Equal(uint64(30000000000), transaction.GasPrice)
			require.Equal("220000000000000000000", transaction.Value)
			require.Equal(uint64(0), transaction.Type)
			require.Nil(transaction.GetOptionalMaxFeePerGas())
			require.Nil(transaction.GetOptionalMaxPriorityFeePerGas())
			require.Nil(transaction.GetOptionalTransactionAccessList())
			require.Equal(testutil.MustTimestamp(maticBlockTimestamp), transaction.BlockTimestamp)

			transactionReceipt := transaction.Receipt
			require.NotNil(transactionReceipt)
			s.app.Logger().Info("transaction receipt:", zap.Reflect("transaction_receipt", transactionReceipt))
			require.Equal(transactionHash, transactionReceipt.TransactionHash)
			require.Equal(transactionIndex, transactionReceipt.TransactionIndex)
			require.Equal(maticBlockHash, transactionReceipt.BlockHash)
			require.Equal(maticHeight, transactionReceipt.BlockNumber)
			require.Equal(transactionFrom, transactionReceipt.From)
			require.Equal(transactionTo, transactionReceipt.To)
			require.Equal(uint64(9716028), transactionReceipt.CumulativeGasUsed)
			require.Equal(uint64(122881), transactionReceipt.GasUsed)
			require.Equal("0x00200000000000000000000080000000000000000020000000000000000000000000000000000000000000100000000000008000000000000000000000000000000004000000400000000008000000a00800040000000000000100008008020000000000000100000000000000000000000000000000000080000010000000000000020080000000800000000020000000000041000000080000004000000000200000020000000040000000000000004040000000000810000000000000004000001002000000000003000000000000000000008000801000108000000000000000000000000000000000000000000000000000000000402000000000100800", transactionReceipt.LogsBloom)
			require.Equal(uint64(1), transactionReceipt.GetStatus())
			require.Equal(uint64(0), transactionReceipt.Type)
			require.Equal(uint64(30000000000), transactionReceipt.EffectiveGasPrice)

			// see https://polygonscan.com/tx/0x374e81a6bd6df0c947274dc5d7fa425759ef01647c0c4d434bc923a1c47e7865#eventlog
			require.Equal(8, len(transactionReceipt.Logs))
			eventLog := transactionReceipt.Logs[0]
			s.app.Logger().Info("event log:", zap.Reflect("event_log", eventLog))
			require.False(eventLog.Removed)
			require.Equal(uint64(376), eventLog.LogIndex)
			require.Equal(transactionHash, eventLog.TransactionHash)
			require.Equal(transactionIndex, eventLog.TransactionIndex)
			require.Equal(maticBlockHash, eventLog.BlockHash)
			require.Equal(maticHeight, eventLog.BlockNumber)
			require.Equal("0x0000000000000000000000000000000000001010", eventLog.Address)
			require.Equal("0x00000000000000000000000000000000000000000000000bed1d0263d9f0000000000000000000000000000000000000000000000000000bfeb7901eeddf5bb60000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000119a8dbb13ef5bb600000000000000000000000000000000000000000000000bed1d0263d9f00000", eventLog.Data)
			require.Equal([]string{
				"0xe6497e3ee548a3372136af2fcb0696db31fc6cf20260707645068bd3fe97f3c4",
				"0x0000000000000000000000000000000000000000000000000000000000001010",
				"0x000000000000000000000000af66758a4ddfc2c6a9cb3c07280b65324a325625",
				"0x0000000000000000000000001b02da8cb0d097eb8d57a175b88c7d8b47997506",
			}, eventLog.Topics)

			// see https://polygonscan.com/vmtrace?txhash=0x374e81a6bd6df0c947274dc5d7fa425759ef01647c0c4d434bc923a1c47e7865&type=gethtrace2
			transactionFlattenedTraces := transaction.FlattenedTraces
			require.Equal(8, len(transactionFlattenedTraces))
			s.app.Logger().Info("polygon transaction flattened traces:", zap.Reflect("transaction_flattened_traces", transactionFlattenedTraces))

			require.Equal("CALL", transactionFlattenedTraces[0].Type)
			require.Equal(transactionFrom, transactionFlattenedTraces[0].From)
			require.Equal(transactionTo, transactionFlattenedTraces[0].To)
			require.Equal("220000000000000000000", transactionFlattenedTraces[0].Value)
			require.Equal(uint64(4), transactionFlattenedTraces[0].Subtraces)
			require.Equal([]uint64{}, transactionFlattenedTraces[0].TraceAddress)
			require.Equal(uint64(20710000), transactionFlattenedTraces[0].BlockNumber)
			require.Equal(maticBlockHash, transactionFlattenedTraces[0].BlockHash)
			require.Equal(transactionHash, transactionFlattenedTraces[0].TransactionHash)
			require.Equal(uint64(58), transactionFlattenedTraces[0].TransactionIndex)
			require.Equal("CALL", transactionFlattenedTraces[0].CallType)
			require.Equal("CALL", transactionFlattenedTraces[0].TraceType)
			require.Equal("CALL_0x374e81a6bd6df0c947274dc5d7fa425759ef01647c0c4d434bc923a1c47e7865", transactionFlattenedTraces[0].TraceId)
			require.Equal(uint64(1), transactionFlattenedTraces[0].Status)

			err = s.parser.ValidateBlock(context.Background(), nativeBlock)
			require.NoError(err)
		})
	}
}

func (s *polygonIntegrationTestSuite) TestPolygonGetBlock_TraceBlockWithIgnoredTransaction() {
	s.app.Logger().Info("fetching block")
	require := testutil.Require(s.T())
	rawBlock, err := s.client.GetBlockByHeight(context.Background(), maticTag, maticHeightOfTraceTransactions)
	require.NoError(err)

	s.Equal(common.Blockchain_BLOCKCHAIN_POLYGON, rawBlock.Blockchain)
	s.Equal(common.Network_NETWORK_POLYGON_MAINNET, rawBlock.Network)
	s.Equal(maticTag, rawBlock.Metadata.Tag)
	s.Equal(maticBlockHashOfTraceTransactions, rawBlock.Metadata.Hash)
	s.Equal(maticHeightOfTraceTransactions, rawBlock.Metadata.Height)

	// See https://polygonscan.com/block/2304
	nativeBlock, err := s.parser.ParseNativeBlock(context.Background(), rawBlock)
	require.NoError(err)
	require.Equal(common.Blockchain_BLOCKCHAIN_POLYGON, nativeBlock.Blockchain)
	require.Equal(common.Network_NETWORK_POLYGON_MAINNET, nativeBlock.Network)
	require.Equal(maticTag, nativeBlock.Tag)
	require.Equal(maticBlockHashOfTraceTransactions, nativeBlock.Hash)
	require.Equal(maticHeightOfTraceTransactions, nativeBlock.Height)
	require.Equal(uint64(1), nativeBlock.NumTransactions)

	block := nativeBlock.GetEthereum()
	require.NotNil(block)

	header := block.Header
	require.NotNil(header)

	// see https://polygonscan.com/tx/0xef029df186fe80862876987b237d848734d534c1c857f49bb9606a1b38f2b6b8
	transaction := block.Transactions[0]
	transactionFlattenedTraces := transaction.FlattenedTraces
	require.Equal(1, len(transactionFlattenedTraces))
	s.app.Logger().Info("transaction trace:", zap.Reflect("transaction_flattened_trace", transactionFlattenedTraces))
	require.Equal("0xef029df186fe80862876987b237d848734d534c1c857f49bb9606a1b38f2b6b8", transactionFlattenedTraces[0].TransactionHash)
	require.Empty(transactionFlattenedTraces[0].Type)
	require.Empty(transactionFlattenedTraces[0].From)
	require.Empty(transactionFlattenedTraces[0].To)
	require.Equal("0", transactionFlattenedTraces[0].Value)
}

func (s *polygonIntegrationTestSuite) TestPolygonGetBlock_TraceBlockWithoutTransaction() {
	s.app.Logger().Info("fetching block")
	require := testutil.Require(s.T())
	rawBlock, err := s.client.GetBlockByHeight(context.Background(), maticTag, maticHeightNoTransactions)
	require.NoError(err)

	s.Equal(common.Blockchain_BLOCKCHAIN_POLYGON, rawBlock.Blockchain)
	s.Equal(common.Network_NETWORK_POLYGON_MAINNET, rawBlock.Network)
	s.Equal(maticTag, rawBlock.Metadata.Tag)
	s.Equal(maticBlockHashNoTransactions, rawBlock.Metadata.Hash)
	s.Equal(maticHeightNoTransactions, rawBlock.Metadata.Height)

	// See https://polygonscan.com/block/2303
	nativeBlock, err := s.parser.ParseNativeBlock(context.Background(), rawBlock)
	require.NoError(err)
	require.Equal(common.Blockchain_BLOCKCHAIN_POLYGON, nativeBlock.Blockchain)
	require.Equal(common.Network_NETWORK_POLYGON_MAINNET, nativeBlock.Network)
	require.Equal(maticTag, nativeBlock.Tag)
	require.Equal(maticBlockHashNoTransactions, nativeBlock.Hash)
	require.Equal(maticHeightNoTransactions, nativeBlock.Height)
	require.Equal(uint64(0), nativeBlock.NumTransactions)

	block := nativeBlock.GetEthereum()
	require.NotNil(block)

	header := block.Header
	require.NotNil(header)
}

func (s *polygonIntegrationTestSuite) TestPolygonGetBlock_NotFound() {
	const (
		heightNotFound = 99_999_999
		hashNotFound   = "0x0000000000000000000000000000000000000000000000000000000000000000"
	)

	require := testutil.Require(s.T())

	_, err := s.client.GetBlockByHeight(context.Background(), maticTag, heightNotFound)
	require.Error(err)
	require.True(xerrors.Is(err, client.ErrBlockNotFound), err.Error())

	_, err = s.client.GetBlockByHash(context.Background(), maticTag, heightNotFound, hashNotFound)
	require.Error(err)
	require.True(xerrors.Is(err, client.ErrBlockNotFound), err.Error())
}

func (s *polygonIntegrationTestSuite) TestPolygon_ValidateBlock() {
	tests := []struct {
		name        string
		blockHeight uint64
		blockHash   string
		parentHash  string
		getBlock    func(height uint64, hash string) (*api.Block, error)
	}{
		{
			name:        "GetBlockByHeight",
			blockHeight: 1000000,
			blockHash:   "0x29fa73e3da83ddac98f527254fe37002e052725a88904bac14f03e919e1e2876",
			parentHash:  "0xfc473711db106d409984a47c30e66e121c096c56b08b5c03f87491d36d5e5aac",
			getBlock: func(height uint64, hash string) (*api.Block, error) {
				return s.client.GetBlockByHeight(context.Background(), maticTag, height)
			},
		},
		{
			name:        "GetBlockByHeight",
			blockHeight: 10000000,
			blockHash:   "0x57d179b4ed2379580c46d9809c8918e28c4f1debea8e15013749694f37c14105",
			parentHash:  "0xee82cb38f164bfdb75092bcc5c1cb302385a48b2d9abd647af934ecb89db61f4",
			getBlock: func(height uint64, hash string) (*api.Block, error) {
				return s.client.GetBlockByHeight(context.Background(), maticTag, height)
			},
		},
		{
			name:        "GetBlockByHeight",
			blockHeight: 20000000,
			blockHash:   "0x8b047896ef57b3ebe10a6b0e4cc28a1e0491b09706c6e0f2b5eff3992cf04730",
			parentHash:  "0x3a61eaf4cc52b8c3a217fbcfd0e862b59e01d504368b659db9314921879dcf9e",
			getBlock: func(height uint64, hash string) (*api.Block, error) {
				return s.client.GetBlockByHeight(context.Background(), maticTag, height)
			},
		},
		{
			name:        "GetBlockByHeight",
			blockHeight: 40000000,
			blockHash:   "0x5c4ba8c6c5346ed2d1bc691ac71b67019cf093b1f8903e8ee7f1ba3e3ac8db29",
			parentHash:  "0xab8a751de0aa74f1fbace583002dc9c9df3f95b781a6a29f9fb19a2f2a36522e",
			getBlock: func(height uint64, hash string) (*api.Block, error) {
				return s.client.GetBlockByHeight(context.Background(), maticTag, height)
			},
		},
		{
			name:        "GetBlockByHeight",
			blockHeight: 43200000,
			blockHash:   "0xae5b1532c6e95f1c02d857d1ed76639a33dfaacc7b4e368129be438d4dee6631",
			parentHash:  "0x8c5deea07d3734a71b6a5845c6367300bbbd0c966b5333788e54eaccb9d0548c",
			getBlock: func(height uint64, hash string) (*api.Block, error) {
				return s.client.GetBlockByHeight(context.Background(), maticTag, height)
			},
		},
		{
			name:        "GetBlockByHash",
			blockHeight: 43200000,
			blockHash:   "0xae5b1532c6e95f1c02d857d1ed76639a33dfaacc7b4e368129be438d4dee6631",
			parentHash:  "0x8c5deea07d3734a71b6a5845c6367300bbbd0c966b5333788e54eaccb9d0548c",
			getBlock: func(height uint64, hash string) (*api.Block, error) {
				return s.client.GetBlockByHash(context.Background(), maticTag, height, hash)
			},
		},
	}

	for _, test := range tests {
		s.Run(test.name, func() {
			s.app.Logger().Info("fetching block")
			require := testutil.Require(s.T())
			rawBlock, err := test.getBlock(test.blockHeight, test.blockHash)
			require.NoError(err)

			require.Equal(common.Blockchain_BLOCKCHAIN_POLYGON, rawBlock.Blockchain)
			require.Equal(common.Network_NETWORK_POLYGON_MAINNET, rawBlock.Network)
			require.Equal(maticTag, rawBlock.Metadata.Tag)
			require.Equal(test.blockHash, rawBlock.Metadata.Hash)
			require.Equal(test.parentHash, rawBlock.Metadata.ParentHash)
			require.Equal(test.blockHeight, rawBlock.Metadata.Height)
			require.Equal(test.blockHeight-1, rawBlock.Metadata.ParentHeight)

			nativeBlock, err := s.parser.ParseNativeBlock(context.Background(), rawBlock)
			require.NoError(err)

			err = s.parser.ValidateBlock(context.Background(), nativeBlock)
			require.NoError(err)
		})
	}
}

func (s *polygonIntegrationTestSuite) TestPolygon_ValidateBlock_Debug() {
	tests := []struct {
		name        string
		blockHeight uint64
		blockHash   string
		parentHash  string
		getBlock    func(height uint64, hash string) (*api.Block, error)
	}{
		{
			name:        "GetBlockByHeight",
			blockHeight: 45546650,
			blockHash:   "0x6811096340742fe16b8aee0f8bbcd4600724a176775e524751b791d18f523eed",
			parentHash:  "0xf90c8dae22afdfddf824bc63ad4d80a1944e5371d8df163473e8fe617f1a09ef",
			getBlock: func(height uint64, hash string) (*api.Block, error) {
				return s.client.GetBlockByHeight(context.Background(), maticTag, height)
			},
		},
		{
			name:        "GetBlockByHash",
			blockHeight: 45546650,
			blockHash:   "0x6811096340742fe16b8aee0f8bbcd4600724a176775e524751b791d18f523eed",
			parentHash:  "0xf90c8dae22afdfddf824bc63ad4d80a1944e5371d8df163473e8fe617f1a09ef",
			getBlock: func(height uint64, hash string) (*api.Block, error) {
				return s.client.GetBlockByHash(context.Background(), maticTag, height, hash)
			},
		},
	}

	for _, test := range tests {
		s.Run(test.name, func() {
			s.app.Logger().Info("fetching block")
			require := testutil.Require(s.T())
			rawBlock, err := test.getBlock(test.blockHeight, test.blockHash)
			require.NoError(err)

			require.Equal(common.Blockchain_BLOCKCHAIN_POLYGON, rawBlock.Blockchain)
			require.Equal(common.Network_NETWORK_POLYGON_MAINNET, rawBlock.Network)
			require.Equal(maticTag, rawBlock.Metadata.Tag)
			require.Equal(test.blockHash, rawBlock.Metadata.Hash)
			require.Equal(test.parentHash, rawBlock.Metadata.ParentHash)
			require.Equal(test.blockHeight, rawBlock.Metadata.Height)
			require.Equal(test.blockHeight-1, rawBlock.Metadata.ParentHeight)

			nativeBlock, err := s.parser.ParseNativeBlock(context.Background(), rawBlock)
			require.NoError(err)

			err = s.parser.ValidateBlock(context.Background(), nativeBlock)
			require.NoError(err)
		})
	}
}
