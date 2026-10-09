package config

import (
	"strings"
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
)

func TestBlockStorageConfigDecodesGenerationRegistry(t *testing.T) {
	v := viper.New()
	v.SetConfigType("yaml")
	require.NoError(t, v.ReadConfig(strings.NewReader(`
aws:
  block_storage:
    write_generation: v3
    generations:
      v2:
        bucket: blocks-v2
      v3:
        bucket: blocks-v3
`)))

	var cfg Config
	require.NoError(t, v.Unmarshal(&cfg))
	require.Equal(t, StorageGeneration("v3"), cfg.AWS.BlockStorage.WriteGeneration)
	require.Equal(t, "blocks-v2", cfg.AWS.BlockStorage.Generations["v2"].Bucket)
	require.Equal(t, "blocks-v3", cfg.AWS.BlockStorage.Generations["v3"].Bucket)
}

func TestValidateStorageGenerationConfig(t *testing.T) {
	tests := []struct {
		name        string
		configure   func(*Config)
		expectedErr string
	}{
		{
			name: "accepts extensible registry",
			configure: func(cfg *Config) {
				cfg.AWS.BlockStorage = BlockStorageConfig{
					WriteGeneration: "v3",
					Generations: map[string]BlockStorageGenerationConfig{
						"v2": {Bucket: "blocks-v2"},
						"v3": {Bucket: "blocks-v3"},
					},
				}
			},
		},
		{
			name: "write generation requires mapping",
			configure: func(cfg *Config) {
				cfg.AWS.BlockStorage.WriteGeneration = "v2"
			},
			expectedErr: "write_generation=v2 is not configured",
		},
		{
			name: "rejects malformed generation",
			configure: func(cfg *Config) {
				cfg.AWS.BlockStorage.Generations = map[string]BlockStorageGenerationConfig{
					"blue": {Bucket: "blocks-blue"},
				}
			},
			expectedErr: "invalid aws.block_storage.generations key \"blue\"",
		},
		{
			name: "rejects duplicate physical bucket",
			configure: func(cfg *Config) {
				cfg.AWS.BlockStorage.Generations = map[string]BlockStorageGenerationConfig{
					"v2": {Bucket: cfg.AWS.Bucket},
				}
			},
			expectedErr: "must use different buckets",
		},
		{
			name: "rejects whitespace in bucket",
			configure: func(cfg *Config) {
				cfg.AWS.BlockStorage.Generations = map[string]BlockStorageGenerationConfig{
					"v2": {Bucket: " blocks-v2 "},
				}
			},
			expectedErr: "must be non-empty without surrounding whitespace",
		},
		{
			name: "nonlegacy requires S3",
			configure: func(cfg *Config) {
				cfg.StorageType.BlobStorageType = BlobStorageType_GCS
				cfg.AWS.BlockStorage.Generations = map[string]BlockStorageGenerationConfig{
					"v2": {Bucket: "blocks-v2"},
				}
			},
			expectedErr: "require S3 blob storage",
		},
		{
			name: "nonlegacy requires Postgres",
			configure: func(cfg *Config) {
				cfg.StorageType.MetaStorageType = MetaStorageType_DYNAMODB
				cfg.AWS.BlockStorage.Generations = map[string]BlockStorageGenerationConfig{
					"v2": {Bucket: "blocks-v2"},
				}
			},
			expectedErr: "require Postgres meta storage",
		},
		{
			name: "unspecified metadata backend remains DynamoDB",
			configure: func(cfg *Config) {
				cfg.StorageType.MetaStorageType = MetaStorageType_UNSPECIFIED
				cfg.AWS.Postgres = &PostgresConfig{}
				cfg.AWS.DynamoDB = nil
				cfg.AWS.BlockStorage.Generations = map[string]BlockStorageGenerationConfig{
					"v2": {Bucket: "blocks-v2"},
				}
			},
			expectedErr: "require Postgres meta storage",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg := &Config{
				StorageType: StorageType{
					BlobStorageType: BlobStorageType_S3,
					MetaStorageType: MetaStorageType_POSTGRES,
				},
			}
			cfg.AWS.Bucket = "blocks-legacy"
			cfg.AWS.BlockStorage.WriteGeneration = "legacy"
			test.configure(cfg)

			err := cfg.validateStorageGenerationConfig()
			if test.expectedErr == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, test.expectedErr)
		})
	}
}
