package gcs

import (
	"context"
	"testing"

	"github.com/stretchr/testify/suite"
	"go.uber.org/fx"
	"google.golang.org/protobuf/proto"

	"github.com/stretchr/testify/suite"

	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/storage/blobstorage/internal"
	"github.com/coinbase/chainstorage/internal/utils/testapp"
	"github.com/coinbase/chainstorage/internal/utils/testutil"
	"github.com/coinbase/chainstorage/protos/coinbase/c3/common"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

type gcpBlobStorageTestSuite struct {
	suite.Suite
	config  *config.Config
	storage internal.BlobStorage
}

func (s *gcpBlobStorageTestSuite) SetupTest() {
	require := testutil.Require(s.T())

	var storage internal.BlobStorage
	cfg, err := config.New()
	require.NoError(err)
	cfg.StorageType.BlobStorageType = config.BlobStorageType_GCS
	cfg.GCP.Bucket = "test"
	s.config = cfg
	app := testapp.New(
		s.T(),
		fx.Provide(New),
		testapp.WithIntegration(),
		testapp.WithConfig(s.config),
		fx.Populate(&storage),
		fx.Populate(&cfg),
	)
	defer app.Close()
	s.storage = storage
	require.NotNil(s.storage)
}

func (s *gcpBlobStorageTestSuite) TestIntegrationGcsBlobStorage() {
	const expectedObjectKey = "BLOCKCHAIN_ETHEREUM/NETWORK_ETHEREUM_MAINNET/1/12345/0xabcde"
	require := testutil.Require(s.T())

	block := &api.Block{
		Blockchain: common.Blockchain_BLOCKCHAIN_ETHEREUM,
		Network:    common.Network_NETWORK_ETHEREUM_MAINNET,
		Metadata: &api.BlockMetadata{
			Tag:    1,
			Height: 12345,
			Hash:   "0xabcde",
		},
	}

	objectKey, err := s.storage.Upload(context.Background(), block, api.Compression_NONE)
	require.NoError(err)
	require.Equal(expectedObjectKey, objectKey)
	block.Metadata.ObjectKeyMain = objectKey

	metadata := &api.BlockMetadata{
		Tag:           1,
		Height:        12345,
		Hash:          "0xabcde",
		ObjectKeyMain: objectKey,
	}
	downloadedBlock, err := s.storage.Download(context.Background(), metadata)
	require.NoError(err)
	require.True(proto.Equal(block, downloadedBlock))
}

func (s *gcpBlobStorageTestSuite) TestIntegrationGcsBlobStorageIntegration_GzipFormat() {
	const expectedObjectKey = "BLOCKCHAIN_SOLANA/NETWORK_SOLANA_MAINNET/1/12345/0xabcde.gzip"

	require := testutil.Require(s.T())

	block := &api.Block{
		Blockchain: common.Blockchain_BLOCKCHAIN_SOLANA,
		Network:    common.Network_NETWORK_SOLANA_MAINNET,
		Metadata: &api.BlockMetadata{
			Tag:    1,
			Height: 12345,
			Hash:   "0xabcde",
		},
	}

	objectKey, err := s.storage.Upload(context.Background(), block, api.Compression_GZIP)
	require.NoError(err)
	require.Equal(expectedObjectKey, objectKey)
	block.Metadata.ObjectKeyMain = objectKey

	metadata := &api.BlockMetadata{
		Tag:           1,
		Height:        12345,
		Hash:          "0xabcde",
		ObjectKeyMain: objectKey,
	}
	downloadedBlock, err := s.storage.Download(context.Background(), metadata)
	require.NoError(err)
	require.True(proto.Equal(block, downloadedBlock))
}

func TestIntegrationGcsBlobStorageTestSuite(t *testing.T) {
	require := testutil.Require(t)
	cfg, err := config.New()
	require.NoError(err)
	suite.Run(t, &gcpBlobStorageTestSuite{config: cfg})
}
