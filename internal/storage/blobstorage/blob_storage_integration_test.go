package blobstorage

import (
	"context"
	"testing"

	"github.com/golang/protobuf/proto"
	"go.uber.org/fx"

	"github.com/coinbase/chainstorage/internal/s3"
	"github.com/coinbase/chainstorage/internal/utils/testapp"
	"github.com/coinbase/chainstorage/internal/utils/testutil"
	"github.com/coinbase/chainstorage/protos/coinbase/c3/common"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

func TestIntegrationBlobStorage(t *testing.T) {
	const expectedObjectKey = "BLOCKCHAIN_ETHEREUM/NETWORK_ETHEREUM_MAINNET/1/12345/0xabcde"

	require := testutil.Require(t)

	var storage BlobStorage
	app := testapp.New(
		t,
		testapp.WithIntegration(),
		Module,
		s3.Module,
		fx.Populate(&storage),
	)
	defer app.Close()

	require.NotNil(storage)

	block := &api.Block{
		Blockchain: common.Blockchain_BLOCKCHAIN_ETHEREUM,
		Network:    common.Network_NETWORK_ETHEREUM_MAINNET,
		Metadata: &api.BlockMetadata{
			Tag:    1,
			Height: 12345,
			Hash:   "0xabcde",
		},
	}

	objectKey, err := storage.Upload(context.Background(), block, api.Compression_NONE)
	require.NoError(err)
	require.Equal(expectedObjectKey, objectKey)
	block.Metadata.ObjectKeyMain = objectKey

	metadata := &api.BlockMetadata{
		Tag:           1,
		Height:        12345,
		Hash:          "0xabcde",
		ObjectKeyMain: objectKey,
	}
	downloadedBlock, err := storage.Download(context.Background(), metadata)
	require.NoError(err)
	require.True(proto.Equal(block, downloadedBlock))
}

func TestIntegrationBlobStorageIntegration_GzipFormat(t *testing.T) {
	const expectedObjectKey = "BLOCKCHAIN_SOLANA/NETWORK_SOLANA_MAINNET/1/12345/0xabcde.gzip"

	require := testutil.Require(t)

	var storage BlobStorage
	app := testapp.New(
		t,
		testapp.WithIntegration(),
		testapp.WithBlockchainNetwork(common.Blockchain_BLOCKCHAIN_SOLANA, common.Network_NETWORK_SOLANA_MAINNET),
		Module,
		s3.Module,
		fx.Populate(&storage),
	)
	defer app.Close()

	require.NotNil(storage)

	block := &api.Block{
		Blockchain: common.Blockchain_BLOCKCHAIN_SOLANA,
		Network:    common.Network_NETWORK_SOLANA_MAINNET,
		Metadata: &api.BlockMetadata{
			Tag:    1,
			Height: 12345,
			Hash:   "0xabcde",
		},
	}

	objectKey, err := storage.Upload(context.Background(), block, api.Compression_GZIP)
	require.NoError(err)
	require.Equal(expectedObjectKey, objectKey)
	block.Metadata.ObjectKeyMain = objectKey

	metadata := &api.BlockMetadata{
		Tag:           1,
		Height:        12345,
		Hash:          "0xabcde",
		ObjectKeyMain: objectKey,
	}
	downloadedBlock, err := storage.Download(context.Background(), metadata)
	require.NoError(err)
	require.True(proto.Equal(block, downloadedBlock))
}
