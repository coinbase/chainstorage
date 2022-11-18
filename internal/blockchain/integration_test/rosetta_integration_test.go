package integration_test

import (
	"context"
	"fmt"
	"testing"

	"go.uber.org/fx"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/blockchain/client"
	"github.com/coinbase/chainstorage/internal/blockchain/jsonrpc"
	"github.com/coinbase/chainstorage/internal/blockchain/parser"
	"github.com/coinbase/chainstorage/internal/dlq"
	"github.com/coinbase/chainstorage/internal/utils/testapp"
	"github.com/coinbase/chainstorage/internal/utils/testutil"
	"github.com/coinbase/chainstorage/protos/coinbase/c3/common"
)

type rosettaTestAsset struct {
	// FriendlyAssetName is a friendly name to use in logs,
	// for instance in reports of benchmark tests
	FriendlyAssetName string
	Blockchain        common.Blockchain
	Network           common.Network
	// BlockHash is a valid block hash in the  network
	BlockHash string
	// Blockheight is height of the block indicated
	// by BlockHash
	BlockHeight uint64
	// BlockTimestamp is timestamp for the block
	// corresponding to the BlockHash
	BlockTimestamp string
	// ParentBlockHash is hash of the parent block
	// for the block indicated by BlockHash
	ParentBlockHash string
	// NumTransactions is the number of transactions in
	// the block indicated by BlockHash
	NumTransactions uint64
	// Tag is the version Tag to use when querying Chainstorage APIs
	Tag uint32
	// InvalidBlockHash is an invalid block hash
	// used to test block-not-found errors
	InvalidBlockHash string
	// InvalidBlockHeight is an invalid block height
	// used to test block-not-found errors
	InvalidBlockHeight uint64
}

// rosettaTestAssets is list of Rosetta assets to run integration
// and benchmark tests  for. When onboarding new Rosetta assets,
// add a new rosettaTestAsset instance to this list to enable tests.
var rosettaTestAssets = []rosettaTestAsset{
	{
		FriendlyAssetName:  "dogecoin-mainnet",
		Blockchain:         common.Blockchain_BLOCKCHAIN_DOGECOIN,
		Network:            common.Network_NETWORK_DOGECOIN_MAINNET,
		BlockHash:          "1dd0c843e9c487acc21af4504024c7ef9bb56220aac81a035b36517f78a02b0d",
		BlockHeight:        3840970,
		BlockTimestamp:     "2021-08-04T18:51:21Z",
		ParentBlockHash:    "515f4f5c9e54541ec2e39e5d270347d5c6c5f2575e38663791daa44b5e8c2507",
		NumTransactions:    1,
		Tag:                1,
		InvalidBlockHash:   "0000000000000000000000000000000000000000000000000000000000000000",
		InvalidBlockHeight: 99_999_999,
	},
}

func TestIntegrationGetRosettaBlock(t *testing.T) {
	for _, asset := range rosettaTestAssets {
		testGetRosettaBlock(t, asset)
	}
}

func TestIntegrationGetRosettaBlock_NotFound(t *testing.T) {
	for _, asset := range rosettaTestAssets {
		testGetRosettaBlock_NotFound(t, asset)
	}
}

func testGetRosettaBlock(t *testing.T, asset rosettaTestAsset) {
	require := testutil.Require(t)

	var deps struct {
		fx.In
		Client client.Client `name:"slave"`
		Parser parser.Parser
	}

	app := testapp.New(
		t,
		testapp.WithFunctional(),
		testapp.WithBlockchainNetwork(
			asset.Blockchain,
			asset.Network),
		jsonrpc.Module,
		client.Module,
		parser.Module,
		fx.Provide(dlq.NewNop),
		fx.Populate(&deps),
	)
	defer app.Close()

	t.Run(fmt.Sprintf("GetBlockByHeight-%s-%s", asset.Blockchain, asset.Network), func(t *testing.T) {
		rawBlock, err := deps.Client.GetBlockByHeight(context.Background(), asset.Tag, asset.BlockHeight)
		require.NoError(err)
		require.NotNil(rawBlock)
		require.NotNil(rawBlock.Metadata)
		require.Equal(asset.Tag, rawBlock.Metadata.Tag)
		require.Equal(asset.BlockHash, rawBlock.Metadata.Hash)
		require.Equal(asset.ParentBlockHash, rawBlock.Metadata.ParentHash)
		require.Equal(asset.BlockHeight, rawBlock.Metadata.Height)
		if len(asset.BlockTimestamp) > 0 {
			require.Equal(testutil.MustTimestamp(asset.BlockTimestamp), rawBlock.Metadata.Timestamp)
		}

		nativeBlock, err := deps.Parser.ParseNativeBlock(context.Background(), rawBlock)
		require.NoError(err)
		require.NotNil(nativeBlock)
		require.Equal(asset.Blockchain, nativeBlock.Blockchain)
		require.Equal(asset.Network, nativeBlock.Network)
		require.Equal(asset.Tag, nativeBlock.Tag)
		require.Equal(asset.BlockHash, nativeBlock.Hash)
		require.Equal(asset.ParentBlockHash, nativeBlock.ParentHash)
		require.Equal(asset.BlockHeight, nativeBlock.Height)
		if len(asset.BlockTimestamp) > 0 {
			require.Equal(testutil.MustTimestamp(asset.BlockTimestamp), nativeBlock.Timestamp)
		}
		require.Equal(asset.NumTransactions, nativeBlock.NumTransactions)
		require.False(nativeBlock.Skipped)
	})

	t.Run(fmt.Sprintf("GetBlockByHash-%s-%s", asset.Blockchain, asset.Network), func(t *testing.T) {
		rawBlock, err := deps.Client.GetBlockByHash(context.Background(), asset.Tag, asset.BlockHeight, asset.BlockHash)
		require.NoError(err)
		require.NotNil(rawBlock)
		require.NotNil(rawBlock.Metadata)
		require.Equal(asset.Tag, rawBlock.Metadata.Tag)
		if len(asset.BlockTimestamp) > 0 {
			require.Equal(testutil.MustTimestamp(asset.BlockTimestamp), rawBlock.Metadata.Timestamp)
		}

		nativeBlock, err := deps.Parser.ParseNativeBlock(context.Background(), rawBlock)
		require.NoError(err)
		require.NotNil(nativeBlock)
		require.Equal(asset.Blockchain, nativeBlock.Blockchain)
		require.Equal(asset.Network, nativeBlock.Network)
		require.Equal(asset.Tag, nativeBlock.Tag)
		require.Equal(asset.BlockHash, nativeBlock.Hash)
		require.Equal(asset.ParentBlockHash, nativeBlock.ParentHash)
		require.Equal(asset.BlockHeight, nativeBlock.Height)
		if len(asset.BlockTimestamp) > 0 {
			require.Equal(testutil.MustTimestamp(asset.BlockTimestamp), nativeBlock.Timestamp)
		}
		require.Equal(asset.NumTransactions, nativeBlock.NumTransactions)
		require.False(nativeBlock.Skipped)
	})

	t.Run(fmt.Sprintf("GetLatestHeight-%s-%s", asset.Blockchain, asset.Network), func(t *testing.T) {
		height, err := deps.Client.GetLatestHeight(context.Background())
		require.NoError(err)
		app.Logger().Info(fmt.Sprintf("latest height of rosetta asset = %d", height))
	})
}

func testGetRosettaBlock_NotFound(t *testing.T, asset rosettaTestAsset) {
	require := testutil.Require(t)

	var deps struct {
		fx.In
		Client client.Client `name:"slave"`
	}

	app := testapp.New(
		t,
		testapp.WithFunctional(),
		testapp.WithBlockchainNetwork(
			asset.Blockchain,
			asset.Network),
		jsonrpc.Module,
		client.Module,
		parser.Module,
		fx.Provide(dlq.NewNop),
		fx.Populate(&deps),
	)
	defer app.Close()

	_, err := deps.Client.GetBlockByHeight(context.Background(), asset.Tag, asset.InvalidBlockHeight)
	require.Error(err)
	require.True(xerrors.Is(err, client.ErrBlockNotFound), err.Error())

	_, err = deps.Client.GetBlockByHash(context.Background(), asset.Tag, asset.InvalidBlockHeight, asset.InvalidBlockHash)
	require.Error(err)
	require.True(xerrors.Is(err, client.ErrBlockNotFound), err.Error())
}
