package rosetta_test

import (
	"context"
	"fmt"
	"math"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/fx"
	"go.uber.org/zap"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/utils/testutil"
	"github.com/coinbase/chainstorage/protos/coinbase/c3/common"

	"github.com/coinbase/chainstorage/internal/blockchain/client"
	"github.com/coinbase/chainstorage/internal/blockchain/jsonrpc"
	"github.com/coinbase/chainstorage/internal/blockchain/parser"
	"github.com/coinbase/chainstorage/internal/blockchain/restapi"
	"github.com/coinbase/chainstorage/internal/dlq"
	"github.com/coinbase/chainstorage/internal/utils/syncgroup"
	"github.com/coinbase/chainstorage/internal/utils/testapp"
)

const (
	invalidBlockHeight = uint64(math.MaxInt64)
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
		InvalidBlockHeight: invalidBlockHeight,
	},
}

// rosettaPerfTestAssets is a list of test cases to test the performance of the client.
// Blocks with large number of transactions or operations are good candidates here.
var rosettaPerfTestAssets = []rosettaTestAsset{
	{
		// This block has 3042 "OtherTransactions".
		FriendlyAssetName:  "dogecoin-mainnet",
		Blockchain:         common.Blockchain_BLOCKCHAIN_DOGECOIN,
		Network:            common.Network_NETWORK_DOGECOIN_MAINNET,
		BlockHash:          "931c1c0ea2bf321fa0f9e6c7cc7a09f01702f2e1f8bba0de12f41b4d530d3884",
		BlockHeight:        4740311,
		BlockTimestamp:     "2023-05-31T17:27:57Z",
		ParentBlockHash:    "1f103ff5bf5f3883849ff81955b03083a93e8b1b3f09901905ad984834218298",
		NumTransactions:    3042,
		Tag:                1,
		InvalidBlockHash:   "0000000000000000000000000000000000000000000000000000000000000000",
		InvalidBlockHeight: invalidBlockHeight,
	},
}

func TestIntegrationGetRosettaBlock(t *testing.T) {
	for _, asset := range rosettaTestAssets {
		testGetRosettaBlock(t, asset, false)
	}
}

func TestIntegrationGetRosettaBlock_NotFound(t *testing.T) {
	for _, asset := range rosettaTestAssets {
		testGetRosettaBlock_NotFound(t, asset)
	}
}

func TestIntegrationGetRosettaBlock_Perf(t *testing.T) {
	for _, asset := range rosettaPerfTestAssets {
		testGetRosettaBlock(t, asset, true)
	}
}

func testGetRosettaBlock(t *testing.T, asset rosettaTestAsset, perfTest bool) {
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
		restapi.Module,
		client.Module,
		parser.Module,
		fx.Provide(dlq.NewNop),
		fx.Populate(&deps),
	)
	defer app.Close()

	if !perfTest {
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
	}

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

	if !perfTest {
		t.Run(fmt.Sprintf("GetLatestHeight-%s-%s", asset.Blockchain, asset.Network), func(t *testing.T) {
			height, err := deps.Client.GetLatestHeight(context.Background())
			require.NoError(err)
			app.Logger().Info(fmt.Sprintf("latest height of rosetta asset = %d", height))
		})
	}
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
		restapi.Module,
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

func BenchmarkDogecoinRosettaGetBlockByHeight(b *testing.B) {
	// cmd: TEST_FILTER=BenchmarkDogecoinRosettaGetBlockByHeight make benchmark | awk 'match($4, "throughput") {print $0}'

	// Benchmark results
	// -------------------------------------
	// | parallelism | blocks | blocks/sec |
	// -------------------------------------
	// | 2           | 200    | 0.706      |
	// | 8           | 200    | 3.478      |
	// | 16          | 200    | 4.119      |
	// | 24          | 200    | 4.070      |
	// | 32          | 200    | 3.798      |
	// -------------------------------------

	require := testutil.Require(b)
	const parallelism = 16
	const lowerBound = uint64(1000000)
	upperBound := lowerBound + uint64(b.N)

	var deps struct {
		fx.In
		Client client.Client `name:"slave"`
		Parser parser.Parser
	}

	app := testapp.New(
		b,
		testapp.WithFunctional(),
		testapp.WithBlockchainNetwork(
			common.Blockchain_BLOCKCHAIN_DOGECOIN,
			common.Network_NETWORK_DOGECOIN_MAINNET),
		jsonrpc.Module,
		restapi.Module,
		client.Module,
		parser.Module,
		fx.Provide(dlq.NewNop),
		fx.Populate(&deps),
	)
	defer app.Close()

	startTime := time.Now()
	var processed int32
	g, _ := syncgroup.New(context.Background())
	inputChannel := make(chan uint64, b.N)

	for height := lowerBound; height < upperBound; height++ {
		inputChannel <- height
	}
	close(inputChannel)

	for i := 0; i < parallelism; i++ {
		g.Go(func() error {
			for height := range inputChannel {
				app.Logger().Info("processing block", zap.Uint64("height", height))
				rawBlock, err := deps.Client.GetBlockByHeight(context.Background(), 0, height)
				if err != nil {
					return xerrors.Errorf("failed to get block for height %d", height)
				}
				if height != rawBlock.Metadata.Height {
					return xerrors.Errorf(
						"rosetta block has the wrong height, expected: %d, actual: %d",
						height,
						rawBlock.Metadata.Height)
				}
				nativeBlock, err := deps.Parser.ParseNativeBlock(context.Background(), rawBlock)
				if err != nil {
					return xerrors.Errorf("failed to parse rosetta block to a native block %w", err)
				}
				block := nativeBlock.GetRosetta()
				if block == nil {
					return xerrors.Errorf("native block misses rosetta blob")
				}
				atomic.AddInt32(&processed, 1)
			}
			return nil
		})
	}

	require.NoError(g.Wait())
	elapsed := time.Since(startTime)
	throughput := fmt.Sprintf("%.3f", float64(processed)/elapsed.Seconds())
	app.Logger().Info("throughput",
		zap.String("blocks_per_second", throughput),
		zap.Int32("processed", processed),
		zap.Duration("elapsed", elapsed),
		zap.Int("parallelism", parallelism),
	)
}

func TestIntegrationBatchGetBlockMetadata(t *testing.T) {
	const (
		fromHeight = uint64(3907800)
		blockRange = uint64(8)
		tag        = uint32(1)
	)

	require := testutil.Require(t)

	var deps struct {
		fx.In
		NativeClient client.Client `name:"slave"`
	}

	app := testapp.New(
		t,
		testapp.WithFunctional(),
		testapp.WithBlockchainNetwork(common.Blockchain_BLOCKCHAIN_DOGECOIN, common.Network_NETWORK_DOGECOIN_MAINNET),
		jsonrpc.Module,
		restapi.Module,
		client.Module,
		parser.Module,
		fx.Provide(dlq.NewNop),
		fx.Populate(&deps),
	)
	defer app.Close()

	toHeight := fromHeight + blockRange
	blockMetadata, err := deps.NativeClient.BatchGetBlockMetadata(context.Background(), tag, fromHeight, toHeight)
	require.NoError(err)
	for i := 0; i < int(blockRange); i++ {
		require.Equal(blockMetadata[i].Tag, tag)
		require.Equal(blockMetadata[i].Height, fromHeight+uint64(i))
	}
}

func TestIntegrationBatchGetBlockMetadata_Fail(t *testing.T) {
	const (
		blockRange = uint64(2)
		tag        = uint32(1)
	)

	require := testutil.Require(t)

	var deps struct {
		fx.In
		NativeClient client.Client `name:"slave"`
	}

	app := testapp.New(
		t,
		testapp.WithFunctional(),
		testapp.WithBlockchainNetwork(common.Blockchain_BLOCKCHAIN_DOGECOIN, common.Network_NETWORK_DOGECOIN_MAINNET),
		jsonrpc.Module,
		restapi.Module,
		client.Module,
		parser.Module,
		fx.Provide(dlq.NewNop),
		fx.Populate(&deps),
	)
	defer app.Close()

	fromHeight := invalidBlockHeight - blockRange
	_, err := deps.NativeClient.BatchGetBlockMetadata(context.Background(), tag, fromHeight, invalidBlockHeight)
	require.Error(err)
	require.Contains(err.Error(), "failed to get block metadata in parallel")
}
