package config_test

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-playground/validator/v10"

	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/utils/fixtures"
	"github.com/coinbase/chainstorage/internal/utils/testapp"
	"github.com/coinbase/chainstorage/internal/utils/testutil"
	"github.com/coinbase/chainstorage/protos/coinbase/c3/common"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

var (
	networkUnCompressed = map[common.Network]bool{}

	latestEventTag = map[string]map[common.Network]uint32{
		config.DefaultNamespace: {
			common.Network_NETWORK_ETHEREUM_MAINNET:  1,
			common.Network_NETWORK_ARBITRUM_MAINNET:  1,
			common.Network_NETWORK_AVACCHAIN_MAINNET: 1,
			common.Network_NETWORK_BSC_MAINNET:       1,
			common.Network_NETWORK_OPTIMISM_MAINNET:  1,
		},
	}

	latestPolygonDevEventTag  = uint32(2)
	latestEthereumDevEventTag = uint32(2)

	workflowSessionEnabled = map[string]map[common.Network]bool{
		config.DefaultNamespace: {
			common.Network_NETWORK_POLYGON_MAINNET: true,
			common.Network_NETWORK_SOLANA_MAINNET:  true,
		},
	}
)

func TestValidateConfigs(t *testing.T) {
	testapp.TestAllConfigs(t, func(t *testing.T, cfg *config.Config) {
		require := testutil.Require(t)

		configName := cfg.ConfigName
		normalizedConfigName := strings.ReplaceAll(configName, "_", "-")

		require.True(cfg.AWS.IsLocalStack)
		require.True(cfg.AWS.IsResetLocal)

		require.Equal("us-east-1", cfg.AWS.Region)
		require.NotEmpty(cfg.AWS.Bucket)
		require.NotEmpty(cfg.Cadence.Address)
		require.NotEmpty(cfg.Cadence.Domain)

		require.Equal(fmt.Sprintf("chainstorage-%v", normalizedConfigName), cfg.Cadence.Domain)
		require.NotEmpty(cfg.Cadence.RetentionPeriod)
		// Cadence workflow config validation
		require.NotEmpty(cfg.Workflows.Backfiller)
		require.False(cfg.Workflows.Backfiller.Enabled)
		require.NotEmpty(cfg.Workflows.Backfiller.TaskList)
		require.NotEmpty(cfg.Workflows.Backfiller.WorkflowRunTimeout)
		require.NotEmpty(cfg.Workflows.Backfiller.ActivityStartToCloseTimeout)
		require.Equal("workflow.backfiller", cfg.Workflows.Backfiller.WorkflowIdentity)
		require.NotEmpty(cfg.Workflows.Backfiller.BatchSize)
		require.NotEmpty(cfg.Workflows.Backfiller.CheckpointSize)
		require.LessOrEqual(cfg.Workflows.Backfiller.CheckpointSize/cfg.Workflows.Backfiller.MiniBatchSize, uint64(10000), "CheckpointSize is too large. There is a maximum limit of 50,000 events that is enforced by Temporal.")
		require.NotEmpty(cfg.Workflows.Backfiller.NumConcurrentExtractors)
		require.NotEmpty(cfg.Workflows.Poller.Parallelism)
		require.NotEmpty(cfg.Workflows.Monitor.Parallelism)
		require.NotEmpty(cfg.Workflows.Poller.SessionCreationTimeout)
		require.Equal(cfg.Workflows.Poller.BackoffInterval, cfg.Workflows.Streamer.BackoffInterval)
		require.LessOrEqual(cfg.Workflows.Poller.BackoffInterval, cfg.Chain.BlockTime)
		require.NotNil(cfg.Workflows.Poller.ActivityRetry)
		require.Greater(cfg.Workflows.Poller.ActivityRetry.MaximumAttempts, int32(3))
		require.NotNil(cfg.Workflows.Poller.WorkflowRetry)
		require.LessOrEqual(int32(3), cfg.Workflows.Poller.WorkflowRetry.MaximumAttempts)
		require.NotNil(cfg.Workflows.Monitor.WorkflowRetry)
		require.LessOrEqual(int32(3), cfg.Workflows.Monitor.WorkflowRetry.MaximumAttempts)
		require.NotNil(cfg.Workflows.CrossValidator.WorkflowRetry)
		require.LessOrEqual(int32(3), cfg.Workflows.CrossValidator.WorkflowRetry.MaximumAttempts)
		require.NotNil(cfg.Workflows.Streamer.WorkflowRetry)
		require.LessOrEqual(int32(3), cfg.Workflows.Streamer.WorkflowRetry.MaximumAttempts)
		require.NotEmpty(cfg.Workflows.Backfiller.ActivityScheduleToCloseTimeout)
		require.Greater(cfg.Workflows.Backfiller.ActivityScheduleToCloseTimeout.Seconds(), cfg.Workflows.Backfiller.ActivityStartToCloseTimeout.Seconds())
		require.NotEmpty(cfg.Workflows.Poller.ActivityScheduleToCloseTimeout)
		require.Greater(cfg.Workflows.Poller.ActivityScheduleToCloseTimeout.Seconds(), cfg.Workflows.Poller.ActivityStartToCloseTimeout.Seconds())
		require.NotEmpty(cfg.Workflows.Monitor.ActivityScheduleToCloseTimeout)
		require.Greater(cfg.Workflows.Monitor.ActivityScheduleToCloseTimeout.Seconds(), cfg.Workflows.Monitor.ActivityStartToCloseTimeout.Seconds())
		require.NotEmpty(cfg.Workflows.Streamer.ActivityScheduleToCloseTimeout)
		require.Greater(cfg.Workflows.Streamer.ActivityScheduleToCloseTimeout.Seconds(), cfg.Workflows.Streamer.ActivityStartToCloseTimeout.Seconds())

		require.NotEmpty(cfg.Workflows.Workers)
		// Cadence workflow identity
		require.NotEmpty(cfg.Workflows.Backfiller.WorkflowIdentity)
		require.NotEmpty(cfg.Workflows.Poller.WorkflowIdentity)
		require.NotEmpty(cfg.Workflows.Benchmarker.WorkflowIdentity)
		require.NotEmpty(cfg.Workflows.Monitor.WorkflowIdentity)

		require.Equal(cfg.Chain.BlockTag, cfg.Workflows.Backfiller.BlockTag)
		require.Equal(cfg.Chain.BlockTag, cfg.Workflows.Poller.BlockTag)
		require.Equal(cfg.Chain.BlockTag, cfg.Workflows.Benchmarker.BlockTag)
		require.Equal(cfg.Chain.BlockTag, cfg.Workflows.Monitor.BlockTag)

		require.Equal(cfg.Chain.EventTag, cfg.Workflows.Backfiller.EventTag)
		require.Equal(cfg.Chain.EventTag, cfg.Workflows.Poller.EventTag)
		require.Equal(cfg.Chain.EventTag, cfg.Workflows.Benchmarker.EventTag)
		require.Equal(cfg.Chain.EventTag, cfg.Workflows.Monitor.EventTag)

		require.NotEmpty(cfg.Chain.IrreversibleDistance)
		require.Equal(cfg.Chain.IrreversibleDistance, cfg.Workflows.Streamer.IrreversibleDistance)
		require.NotEmpty(cfg.Workflows.Monitor.IrreversibleDistance)

		if len(cfg.Chain.Rosetta.Blockchain) > 0 {
			require.NotEmpty(cfg.Chain.Rosetta.Network)
		}

		if cfg.Namespace() != config.DefaultNamespace {
			return
		}

		require.GreaterOrEqual(cfg.Chain.EventTag.Latest, cfg.Chain.EventTag.Stable)
		require.Equal(api.Compression_GZIP, cfg.AWS.Storage.DataCompression)
		require.Equal(expectedConsolidationConfig(cfg), cfg.AWS.Storage.Consolidation)
		require.Equal(5*time.Minute, cfg.Workflows.BatchConsolidator.ActivityHeartbeatTimeout)

		require.Equal(fmt.Sprintf("chainstorage-%v", normalizedConfigName), cfg.Cadence.Domain)

		require.Equal(cfg.AWS.Storage, cfg.Workflows.Backfiller.Storage)
		require.Equal(cfg.AWS.Storage, cfg.Workflows.Poller.Storage)

		require.Equal(3000, cfg.Api.RateLimit.GlobalRPS)
		require.Equal(2000, cfg.Api.RateLimit.PerClientRPS)

		if cfg.Api.RateLimit.GlobalRPS > 0 && cfg.Api.RateLimit.PerClientRPS > 0 {
			require.Greater(cfg.Api.RateLimit.GlobalRPS, cfg.Api.RateLimit.PerClientRPS)
		}
	})
}

func TestSolanaProductionCadenceKeepAliveConfig(t *testing.T) {
	require := testutil.Require(t)

	cfg, err := config.New(
		config.WithEnvironment(config.EnvProduction),
		config.WithBlockchain(common.Blockchain_BLOCKCHAIN_SOLANA),
		config.WithNetwork(common.Network_NETWORK_SOLANA_MAINNET),
	)

	require.NoError(err)
	require.Equal(10*time.Second, cfg.Cadence.KeepAliveTime)
	require.Equal(30*time.Second, cfg.Cadence.KeepAliveTimeout)
	require.True(cfg.AWS.Storage.Consolidation.ReadShadowFirst)
	require.Equal(config.BlobStorageType_S3, cfg.StorageType.BlobStorageType)
	require.Equal(config.MetaStorageType_POSTGRES, cfg.StorageType.MetaStorageType)
}

func TestDerivedConfigValues(t *testing.T) {
	testapp.TestAllConfigs(t, func(t *testing.T, cfg *config.Config) {
		require := testutil.Require(t)
		configName := cfg.ConfigName
		normalizedConfigName := strings.ReplaceAll(configName, "_", "-")

		// Verify template derived configs.
		var dynamoDBPtr *config.DynamoDBConfig
		if cfg.AWS.DynamoDB != nil {
			dynamoDB := config.DynamoDBConfig{
				BlockTable:                    fmt.Sprintf("example_chainstorage_blocks_%v", configName),
				EventTable:                    cfg.AWS.DynamoDB.EventTable,
				EventTableHeightIndex:         cfg.AWS.DynamoDB.EventTableHeightIndex,
				VersionedEventTable:           fmt.Sprintf("example_chainstorage_versioned_block_events_%v", configName),
				VersionedEventTableBlockIndex: fmt.Sprintf("example_chainstorage_versioned_block_events_by_block_id_%v", configName),
				TransactionTable:              cfg.AWS.DynamoDB.TransactionTable,
				// Skip DynamoDB.Arn verification
				Arn: "",
			}
			dynamoDBPtr = &dynamoDB
		}

		var postgresPtr *config.PostgresConfig
		if cfg.AWS.Postgres != nil {
			postgres := config.PostgresConfig{
				Host:             cfg.AWS.Postgres.Host,
				Port:             cfg.AWS.Postgres.Port,
				Database:         cfg.AWS.Postgres.Database,
				User:             cfg.AWS.Postgres.User,
				Password:         cfg.AWS.Postgres.Password,
				SSLMode:          cfg.AWS.Postgres.SSLMode,
				MaxConnections:   cfg.AWS.Postgres.MaxConnections,
				MinConnections:   cfg.AWS.Postgres.MinConnections,
				MaxIdleTime:      cfg.AWS.Postgres.MaxIdleTime,
				MaxLifetime:      cfg.AWS.Postgres.MaxLifetime,
				ConnectTimeout:   cfg.AWS.Postgres.ConnectTimeout,
				StatementTimeout: cfg.AWS.Postgres.StatementTimeout,
				Schema:           cfg.AWS.Postgres.Schema,
				TablePrefix:      cfg.AWS.Postgres.TablePrefix,
			}
			postgresPtr = &postgres
		}

		expectedAWS := config.AwsConfig{
			Region: "us-east-1",
			Bucket: fmt.Sprintf("example-chainstorage-%v-%v", normalizedConfigName, cfg.AwsEnv()),
			BlockStorage: config.BlockStorageConfig{
				WriteGeneration: "legacy",
			},
			DynamoDB:               dynamoDBPtr,
			Postgres:               postgresPtr,
			IsLocalStack:           cfg.AWS.IsLocalStack,
			IsResetLocal:           cfg.AWS.IsResetLocal,
			PresignedUrlExpiration: 30 * time.Minute,
			DLQ: config.SQSConfig{
				Name:                  fmt.Sprintf("example_chainstorage_blocks_%v_dlq", configName),
				VisibilityTimeoutSecs: 600,
				DelaySecs:             900,
				OwnerAccountId:        cfg.AWS.DLQ.OwnerAccountId,
			},
			Storage: config.StorageConfig{
				DataCompression: getDataCompressionType(cfg),
				Consolidation:   defaultConsolidationConfig(),
			},
			// Skip AWS account verification
			AWSAccount: cfg.AWS.AWSAccount,
		}
		expectedAWS.Storage.Consolidation = expectedConsolidationConfig(cfg)
		require.Equal(expectedAWS, cfg.AWS)
	})
}

func expectedConsolidationConfig(cfg *config.Config) config.ConsolidationConfig {
	consolidation := defaultConsolidationConfig()
	if cfg.Env() == config.EnvProduction &&
		cfg.Chain.Blockchain == common.Blockchain_BLOCKCHAIN_SOLANA &&
		cfg.Chain.Network == common.Network_NETWORK_SOLANA_MAINNET {
		consolidation.ReadShadowFirst = true
	}
	return consolidation
}

func TestAWSAccountEnvParsing(t *testing.T) {
	for _, testConfig := range testapp.TestConfigs {
		namespace := testConfig.Namespace
		for _, configName := range testConfig.ConfigNames {
			for _, account := range testapp.AWSAccountsToTest {
				name := fmt.Sprintf("%v/%v/%v", namespace, configName, account)
				t.Run(name, func(t *testing.T) {
					require := testutil.Require(t)

					err := os.Setenv(config.EnvVarEnvironment, string(account))
					require.NoError(err)
					defer os.Unsetenv(config.EnvVarEnvironment)

					blockchain, network, sidechain, err := config.ParseConfigName(configName)
					require.NoError(err)
					cfg, err := config.New(
						config.WithNamespace(namespace),
						config.WithBlockchain(blockchain),
						config.WithNetwork(network),
						config.WithSidechain(sidechain),
					)
					require.NoError(err)
					require.Equal(namespace, cfg.Namespace())
					require.Equal(config.GetEnv(), cfg.Env())
					require.Equal(blockchain, cfg.Blockchain())
					require.Equal(network, cfg.Network())
					require.Equal(sidechain, cfg.Sidechain())
				})
			}
		}
	}
}

func TestConfig_OverrideBlockchainNetwork(t *testing.T) {
	require := testutil.Require(t)

	cfg, err := config.New(
		config.WithBlockchain(common.Blockchain_BLOCKCHAIN_ETHEREUM),
		config.WithNetwork(common.Network_NETWORK_ETHEREUM_GOERLI),
	)
	require.NoError(err)
	require.Equal(common.Blockchain_BLOCKCHAIN_ETHEREUM, cfg.Blockchain())
	require.Equal(common.Network_NETWORK_ETHEREUM_GOERLI, cfg.Network())
}

func TestConfig_OverrideNamespace(t *testing.T) {
	require := testutil.Require(t)

	const namespace = "chainstorage"
	cfg, err := config.New(
		config.WithNamespace(namespace),
		config.WithBlockchain(common.Blockchain_BLOCKCHAIN_ETHEREUM),
		config.WithNetwork(common.Network_NETWORK_ETHEREUM_MAINNET),
	)
	require.NoError(err)
	require.Equal(namespace, cfg.Namespace())
	require.Equal(common.Blockchain_BLOCKCHAIN_ETHEREUM, cfg.Blockchain())
	require.Equal(common.Network_NETWORK_ETHEREUM_MAINNET, cfg.Network())
}

func TestConfig_UnknownEnvVar(t *testing.T) {
	require := testutil.Require(t)

	err := os.Setenv(config.EnvVarConfigName, "foobar")
	require.NoError(err)
	defer os.Unsetenv(config.EnvVarConfigName)

	err = os.Setenv(config.EnvVarEnvironment, "prod")
	require.NoError(err)
	defer os.Unsetenv(config.EnvVarEnvironment)

	_, err = config.New()
	require.Error(err)
}

func TestConfig_OverrideWithUnknownEnvVar(t *testing.T) {
	require := testutil.Require(t)

	err := os.Setenv(config.EnvVarConfigName, "foobar")
	require.NoError(err)
	defer os.Unsetenv(config.EnvVarConfigName)

	err = os.Setenv(config.EnvVarEnvironment, "prod")
	require.NoError(err)
	defer os.Unsetenv(config.EnvVarEnvironment)

	cfg, err := config.New(
		config.WithEnvironment(config.EnvProduction),
		config.WithBlockchain(common.Blockchain_BLOCKCHAIN_DOGECOIN),
		config.WithNetwork(common.Network_NETWORK_DOGECOIN_MAINNET),
	)
	require.NoError(err)
	require.Equal(config.EnvProduction, cfg.Env())
	require.Equal(common.Blockchain_BLOCKCHAIN_DOGECOIN, cfg.Blockchain())
	require.Equal(common.Network_NETWORK_DOGECOIN_MAINNET, cfg.Network())
}

func TestConfigOverridingByEnvSettings(t *testing.T) {
	testapp.TestAllConfigs(t, func(t *testing.T, cfg *config.Config) {
		require := testutil.Require(t)

		expectedEndpoints := []config.Endpoint{
			{
				Name:     "testCluster1",
				Url:      "testUrl1",
				User:     "testUser1",
				Password: "testPassword1",
				Weight:   1,
			},
			{
				Name:     "testCluster2",
				Url:      "testUrl2",
				User:     "testUser2",
				Password: "testPassword2",
				Weight:   2,
			},
		}

		jsonRpcEndpoint := `
		{
			"endpoints": [
				{
					"name": "testCluster1",
					"url": "testUrl1",
					"user": "testUser1",
					"password": "testPassword1",
					"weight": 1
				},
				{
					"name": "testCluster2",
					"url": "testUrl2",
					"user": "testUser2",
					"password": "testPassword2",
					"weight": 2
				}
			],
			"endpoint_config": {
				"sticky_session": {
				  "cookie_hash": "baz"
				},
				"headers": {
				  "key1": "val1",
				  "key2": "val2"
				}
			},
			"endpoint_config_failover": {
				"sticky_session": {
				  "header_hash": "qux"
				},
				"headers": {
				  "key3": "val3"
				}
			}
		}
		`

		jsonFunctionalTest := `
		{
		  "skip_functional_test": [
			{
			  "config_name": "bitcoin-mainnet"
			},
			{
			  "config_name": "dogecoin-mainnet"
			}
		  ]
		}
		`

		err := os.Setenv("CHAINSTORAGE_CHAIN_CLIENT_MASTER_ENDPOINT_GROUP", jsonRpcEndpoint)
		require.NoError(err)
		defer os.Unsetenv("CHAINSTORAGE_CHAIN_CLIENT_MASTER_ENDPOINT_GROUP")

		err = os.Setenv("CHAINSTORAGE_CHAIN_CLIENT_SLAVE_ENDPOINT_GROUP", jsonRpcEndpoint)
		require.NoError(err)
		defer os.Unsetenv("CHAINSTORAGE_CHAIN_CLIENT_SLAVE_ENDPOINT_GROUP")

		err = os.Setenv("CHAINSTORAGE_CHAIN_CLIENT_VALIDATOR_ENDPOINT_GROUP", jsonRpcEndpoint)
		require.NoError(err)
		defer os.Unsetenv("CHAINSTORAGE_CHAIN_CLIENT_VALIDATOR_ENDPOINT_GROUP")

		err = os.Setenv("CHAINSTORAGE_CHAIN_CLIENT_CONSENSUS_ENDPOINT_GROUP", jsonRpcEndpoint)
		require.NoError(err)
		defer os.Unsetenv("CHAINSTORAGE_CHAIN_CLIENT_CONSENSUS_ENDPOINT_GROUP")

		err = os.Setenv("CHAINSTORAGE_FUNCTIONAL_TEST", jsonFunctionalTest)
		require.NoError(err)
		defer os.Unsetenv("CHAINSTORAGE_FUNCTIONAL_TEST")

		// Reload config using the env var.
		cfg, err = config.New(
			config.WithNamespace(cfg.Namespace()),
			config.WithEnvironment(cfg.Env()),
			config.WithBlockchain(cfg.Blockchain()),
			config.WithNetwork(cfg.Network()),
			config.WithSidechain(cfg.Sidechain()),
		)
		require.NoError(err)
		require.Equal(expectedEndpoints, cfg.Chain.Client.Master.EndpointGroup.Endpoints,
			"config yml is likely broken since environment variable "+
				"'CHAINSTORAGE_CHAIN_CLIENT_MASTER_ENDPOINTS' no longer overrides the config values")
		require.Equal(expectedEndpoints, cfg.Chain.Client.Slave.EndpointGroup.Endpoints,
			"config yml is likely broken since environment variable "+
				"'CHAINSTORAGE_CHAIN_CLIENT_SLAVE_ENDPOINTS' no longer overrides the config values")
		require.Equal(expectedEndpoints, cfg.Chain.Client.Validator.EndpointGroup.Endpoints,
			"config yml is likely broken since environment variable "+
				"'CHAINSTORAGE_CHAIN_CLIENT_VALIDATOR_ENDPOINTS' no longer overrides the config values")
		require.Equal(expectedEndpoints, cfg.Chain.Client.Consensus.EndpointGroup.Endpoints,
			"config yml is likely broken since environment variable "+
				"'CHAINSTORAGE_CHAIN_CLIENT_CONSENSUS_ENDPOINTS' no longer overrides the config values")

		require.Equal(2, len(cfg.FunctionalTest.SkipFunctionalTest))

		require.Equal(2, len(cfg.Chain.Client.Master.EndpointGroup.EndpointConfig.Headers))
		require.Equal(2, len(cfg.Chain.Client.Slave.EndpointGroup.EndpointConfig.Headers))
		require.Equal(2, len(cfg.Chain.Client.Validator.EndpointGroup.EndpointConfig.Headers))
		require.Equal(2, len(cfg.Chain.Client.Consensus.EndpointGroup.EndpointConfig.Headers))

		require.Equal(1, len(cfg.Chain.Client.Master.EndpointGroup.EndpointConfigFailover.Headers))
		require.Equal(1, len(cfg.Chain.Client.Slave.EndpointGroup.EndpointConfigFailover.Headers))
		require.Equal(1, len(cfg.Chain.Client.Validator.EndpointGroup.EndpointConfigFailover.Headers))
		require.Equal(1, len(cfg.Chain.Client.Consensus.EndpointGroup.EndpointConfigFailover.Headers))

		require.Equal("baz", cfg.Chain.Client.Master.EndpointGroup.EndpointConfig.StickySession.CookieHash)
		require.Equal("baz", cfg.Chain.Client.Slave.EndpointGroup.EndpointConfig.StickySession.CookieHash)
		require.Equal("baz", cfg.Chain.Client.Validator.EndpointGroup.EndpointConfig.StickySession.CookieHash)
		require.Equal("baz", cfg.Chain.Client.Consensus.EndpointGroup.EndpointConfig.StickySession.CookieHash)

		require.Equal("qux", cfg.Chain.Client.Master.EndpointGroup.EndpointConfigFailover.StickySession.HeaderHash)
		require.Equal("qux", cfg.Chain.Client.Slave.EndpointGroup.EndpointConfigFailover.StickySession.HeaderHash)
		require.Equal("qux", cfg.Chain.Client.Validator.EndpointGroup.EndpointConfigFailover.StickySession.HeaderHash)
		require.Equal("qux", cfg.Chain.Client.Consensus.EndpointGroup.EndpointConfigFailover.StickySession.HeaderHash)
	})
}

func TestConfigOverrideConfigPath(t *testing.T) {
	require := testutil.Require(t)
	err := os.Setenv(config.EnvVarConfigPath, "../../config/chainstorage/ethereum/mainnet/base.yml")
	require.NoError(err)
	defer os.Unsetenv(config.EnvVarConfigPath)

	cfg, err := config.New()
	require.NoError(err)

	require.Equal(common.Blockchain_BLOCKCHAIN_ETHEREUM, cfg.Blockchain())
	require.Equal(common.Network_NETWORK_ETHEREUM_MAINNET, cfg.Network())
}

func TestEndpointParsing(t *testing.T) {
	require := testutil.Require(t)

	expectedEndpointGroup := config.EndpointGroup{
		Endpoints: []config.Endpoint{
			{
				Name:     "testCluster",
				Url:      "testUrl",
				User:     "testUser",
				Password: "testPassword",
				Weight:   1,
				RPS:      0,
			},
		},
		EndpointConfig: config.EndpointConfig{
			StickySession: config.StickySessionConfig{
				CookieHash: "sync_session",
			},
		},
	}

	var endpointGroup config.EndpointGroup
	require.NoError(endpointGroup.UnmarshalText([]byte("")))
	require.Error(endpointGroup.UnmarshalText([]byte("{}")))
	require.Error(endpointGroup.UnmarshalText([]byte(`{endpoints:[]}`)))
	inputBytes, err := makeTestEndpointGroupBytes("testCluster", "testUrl", "testUser", "testPass", 1, "a", true, "")
	require.NoError(err)
	require.Error(endpointGroup.UnmarshalText(inputBytes))
	inputBytes, err = makeTestEndpointGroupBytes("testCluster", "testUrl", "testUser", "testPass", 1, "", true, "")
	require.NoError(err)
	require.NoError(endpointGroup.UnmarshalText(inputBytes))
	inputBytes, err = makeTestEndpointGroupBytes("testCluster", "testUrl", "testUser", "testPass", 1, "a", false, "")
	require.NoError(err)
	require.NoError(endpointGroup.UnmarshalText(inputBytes))
	inputBytes, err = makeTestEndpointGroupBytes("testCluster", "testUrl", "testUser", "testPass", 1, "", false, "a")
	require.NoError(err)
	require.NoError(endpointGroup.UnmarshalText(inputBytes))
	inputBytes, err = makeTestEndpointGroupBytes("testCluster", "", "testUser", "testPass", 1, "", false, "a")
	require.NoError(err)
	require.Error(endpointGroup.UnmarshalText(inputBytes))
	inputBytes, err = makeTestEndpointGroupBytes("", "testUrl", "testUser", "testPass", 1, "", false, "a")
	require.NoError(err)
	require.Error(endpointGroup.UnmarshalText(inputBytes))
	inputBytes, err = makeTestEndpointGroupBytes("testCluster", "testUrl", "testUser", "testPass", 1, "", true, "a")
	require.NoError(err)
	require.Error(endpointGroup.UnmarshalText(inputBytes))
	inputBytes, err = makeTestEndpointGroupBytes("testCluster", "testUrl", "testUser", "testPass", 1, "a", false, "a")
	require.NoError(err)
	require.Error(endpointGroup.UnmarshalText(inputBytes))
	// non positive weight should fail
	text := `
	{
		"endpoints": [
			{
				"name": "testCluster",
				"url": "testUrl1",
				"user": "testUser1",
				"password": "testPassword1",
				"weight": -1,
			}
		]
	}
	`
	require.Error(endpointGroup.UnmarshalText([]byte(text)))
	// overflow weight should fail
	text = `
	{
		"sticky_session_key": "sync_session",
		"endpoints": [
			{
				"url": "testUrl1",
				"user": "testUser1",
				"password": "testPassword1",
				"weight": 256,
			}
		]
	}
	`
	require.Error(endpointGroup.UnmarshalText([]byte(text)))
	inputBytes, err = makeTestEndpointGroupBytes("testCluster", "testUrl", "testUser", "testPassword", 1, "sync_session", false, "")
	require.NoError(err)
	require.NoError(endpointGroup.UnmarshalText(inputBytes))
	require.Equal(expectedEndpointGroup, endpointGroup)
	expectedEndpointGroup = config.EndpointGroup{
		Endpoints: []config.Endpoint{
			{
				Name:     "testCluster",
				Url:      "testUrl",
				User:     "testUser",
				Password: "testPassword",
				Weight:   1,
			},
		},
		EndpointConfig: config.EndpointConfig{
			StickySession: config.StickySessionConfig{
				HeaderHash: "testHeader",
			},
		},
	}
	inputBytes, err = makeTestEndpointGroupBytes("testCluster", "testUrl", "testUser", "testPassword", 1, "", false, "testHeader")
	require.NoError(err)
	require.NoError(endpointGroup.UnmarshalText(inputBytes))
	require.Equal(expectedEndpointGroup, endpointGroup)
}

func TestEndpointGroupWithExtraUrls(t *testing.T) {
	require := testutil.Require(t)

	expectedEndpointGroup := config.EndpointGroup{
		Endpoints: []config.Endpoint{
			{
				Name:     "testCluster",
				Url:      "testUrl",
				User:     "testUser",
				Password: "testPassword",
				Weight:   1,
				ExtraUrls: map[string]string{
					"extraUrl": "testExtraUrl",
				},
			},
		},
	}

	// test endpoints with ports
	text := `
	{
		"sticky_session_key": "sync_session",
		"endpoints": [
			{
				"name": "testCluster",
				"url": "testUrl",
				"user": "testUser",
				"password": "testPassword",
				"weight": 1,
			    "extra_urls": {
					"extraUrl": "testExtraUrl"
				}
			}
		]
	}`

	var endpointGroup config.EndpointGroup
	require.NoError(endpointGroup.UnmarshalText([]byte(text)))
	require.Equal(expectedEndpointGroup, endpointGroup)
}

func TestEndpointGroupWithRPSCountBatch(t *testing.T) {
	require := testutil.Require(t)

	text := `
	{
		"endpoints": [
			{
				"name": "quicknode",
				"url": "testUrl",
				"weight": 1,
				"rps": 300,
				"rps_count_batch": true
			}
		]
	}`

	var endpointGroup config.EndpointGroup
	require.NoError(endpointGroup.UnmarshalText([]byte(text)))
	require.Equal(300, endpointGroup.Endpoints[0].RPS)
	require.True(endpointGroup.Endpoints[0].RPSCountBatch)

	// Defaults to false when unset.
	text = `
	{
		"endpoints": [
			{
				"name": "quicknode",
				"url": "testUrl",
				"weight": 1,
				"rps": 300
			}
		]
	}`

	var endpointGroupDefault config.EndpointGroup
	require.NoError(endpointGroupDefault.UnmarshalText([]byte(text)))
	require.False(endpointGroupDefault.Endpoints[0].RPSCountBatch)
}

// The endpoint group reaches the config through two decoders: encoding/json
// when it is supplied as a JSON string (env var), and mapstructure when it is
// supplied as a YAML mapping. mapstructure only falls back to a
// case-insensitive field-name match, so a struct field without a mapstructure
// tag silently loses every snake_case key. Load a YAML config end to end to
// make sure both decoders agree.
func TestEndpointGroupParsedFromYAML(t *testing.T) {
	require := testutil.Require(t)

	const yamlEndpointGroup = `    master:
      endpoint_group:
        endpoints:
          - name: primary
            provider_id: quicknode
            url: https://primary.example.com
            weight: 1
            rps: 100
            rps_count_batch: true
            extra_urls:
              trace: https://trace.example.com
        endpoints_failover:
          - name: backup
            url: https://backup.example.com
            weight: 1
            rps: 50
        endpoint_config:
          headers:
            x-api-key: secret
`

	base, err := os.ReadFile("../../config/chainstorage/ethereum/mainnet/base.yml")
	require.NoError(err)

	const placeholder = "    master:\n      endpoint_group: \"\"\n"
	require.Contains(string(base), placeholder)
	patched := strings.Replace(string(base), placeholder, yamlEndpointGroup, 1)

	configPath := filepath.Join(t.TempDir(), "base.yml")
	require.NoError(os.WriteFile(configPath, []byte(patched), 0600))
	require.NoError(os.Setenv(config.EnvVarConfigPath, configPath))
	defer os.Unsetenv(config.EnvVarConfigPath)

	cfg, err := config.New()
	require.NoError(err)

	group := cfg.Chain.Client.Master.EndpointGroup
	require.Equal(1, len(group.Endpoints))

	endpoint := group.Endpoints[0]
	require.Equal("primary", endpoint.Name)
	require.Equal("quicknode", endpoint.ProviderID)
	require.Equal("https://primary.example.com", endpoint.Url)
	require.Equal(100, endpoint.RPS)
	require.True(endpoint.RPSCountBatch)
	require.Equal(map[string]string{"trace": "https://trace.example.com"}, endpoint.ExtraUrls)

	require.Equal(1, len(group.EndpointsFailover))
	require.Equal("backup", group.EndpointsFailover[0].Name)
	require.Equal(map[string]string{"x-api-key": "secret"}, group.EndpointConfig.Headers)
}

func TestEndpointGroup(t *testing.T) {
	tests := []struct {
		fixture  string
		expected config.EndpointGroup
	}{
		{
			fixture: "no_failover",
			expected: config.EndpointGroup{
				Endpoints: []config.Endpoint{
					{
						Name:     "chain-storage-eth-04",
						Url:      "https://254c3b9c-be59-41c3-8f6f-3cc3342c7b3c.ethereum.bison.run",
						User:     "foo",
						Password: "bar",
						Weight:   4,
					},
				},
				UseFailover: false,
			},
		},
		{
			fixture: "non_zero_rps",
			expected: config.EndpointGroup{
				Endpoints: []config.Endpoint{
					{
						Name:     "chain-storage-eth-04",
						Url:      "https://254c3b9c-be59-41c3-8f6f-3cc3342c7b3c.ethereum.bison.run",
						User:     "foo",
						Password: "bar",
						Weight:   4,
						RPS:      100,
					},
				},
				UseFailover: false,
			},
		},
		{
			fixture: "no_failover_sticky",
			expected: config.EndpointGroup{
				Endpoints: []config.Endpoint{
					{
						Name:     "chain-storage-eth-04",
						Url:      "https://254c3b9c-be59-41c3-8f6f-3cc3342c7b3c.ethereum.bison.run",
						User:     "foo",
						Password: "bar",
						Weight:   4,
					},
				},
				EndpointConfig: config.EndpointConfig{
					StickySession: config.StickySessionConfig{
						CookieHash: "baz",
					},
				},
				UseFailover: false,
			},
		},
		{
			fixture: "no_failover_header",
			expected: config.EndpointGroup{
				Endpoints: []config.Endpoint{
					{
						Name:     "chain-storage-eth-04",
						Url:      "https://254c3b9c-be59-41c3-8f6f-3cc3342c7b3c.ethereum.bison.run",
						User:     "foo",
						Password: "bar",
						Weight:   4,
					},
				},
				EndpointConfig: config.EndpointConfig{
					StickySession: config.StickySessionConfig{
						CookieHash: "baz",
					},
					Headers: map[string]string{
						"key1": "val1",
						"key2": "val2",
					},
				},
				UseFailover: false,
			},
		},
		{
			fixture: "with_failover",
			expected: config.EndpointGroup{
				Endpoints: []config.Endpoint{
					{
						Name:     "chain-storage-eth-04",
						Url:      "https://254c3b9c-be59-41c3-8f6f-3cc3342c7b3c.ethereum.bison.run",
						User:     "foo",
						Password: "bar",
						Weight:   4,
					},
				},
				EndpointsFailover: []config.Endpoint{
					{
						Name:     "chain-storage-eth-03",
						Url:      "https://e608c8ee-9d88-4861-b2b1-adea0d41664b.ethereum.bison.run",
						User:     "foo",
						Password: "bar",
						Weight:   4,
					},
					{
						Name:     "chain-storage-eth-07",
						Url:      "https://79612859-a432-46d7-8512-f76a0605ac73.ethereum.bison.run",
						User:     "foo",
						Password: "bar",
						Weight:   4,
					},
				},
				UseFailover: true,
			},
		},
		{
			fixture: "with_failover_sticky",
			expected: config.EndpointGroup{
				Endpoints: []config.Endpoint{
					{
						Name:     "chain-storage-eth-04",
						Url:      "https://254c3b9c-be59-41c3-8f6f-3cc3342c7b3c.ethereum.bison.run",
						User:     "foo",
						Password: "bar",
						Weight:   4,
					},
				},
				EndpointsFailover: []config.Endpoint{
					{
						Name:     "chain-storage-eth-03",
						Url:      "https://e608c8ee-9d88-4861-b2b1-adea0d41664b.ethereum.bison.run",
						User:     "foo",
						Password: "bar",
						Weight:   4,
					},
					{
						Name:     "chain-storage-eth-07",
						Url:      "https://79612859-a432-46d7-8512-f76a0605ac73.ethereum.bison.run",
						User:     "foo",
						Password: "bar",
						Weight:   4,
					},
				},
				UseFailover: false,
				EndpointConfig: config.EndpointConfig{
					StickySession: config.StickySessionConfig{
						CookieHash: "baz",
					},
				},
				EndpointConfigFailover: config.EndpointConfig{
					StickySession: config.StickySessionConfig{
						HeaderHash: "qux",
					},
				},
			},
		},
		{
			fixture: "with_failover_header",
			expected: config.EndpointGroup{
				Endpoints: []config.Endpoint{
					{
						Name:     "chain-storage-eth-04",
						Url:      "https://254c3b9c-be59-41c3-8f6f-3cc3342c7b3c.ethereum.bison.run",
						User:     "foo",
						Password: "bar",
						Weight:   4,
					},
				},
				EndpointsFailover: []config.Endpoint{
					{
						Name:     "chain-storage-eth-03",
						Url:      "https://e608c8ee-9d88-4861-b2b1-adea0d41664b.ethereum.bison.run",
						User:     "foo",
						Password: "bar",
						Weight:   4,
					},
					{
						Name:     "chain-storage-eth-07",
						Url:      "https://79612859-a432-46d7-8512-f76a0605ac73.ethereum.bison.run",
						User:     "foo",
						Password: "bar",
						Weight:   4,
					},
				},
				UseFailover: false,
				EndpointConfig: config.EndpointConfig{
					StickySession: config.StickySessionConfig{
						CookieHash: "baz",
					},
					Headers: map[string]string{
						"key1": "val1",
						"key2": "val2",
					},
				},
				EndpointConfigFailover: config.EndpointConfig{
					StickySession: config.StickySessionConfig{
						HeaderHash: "qux",
					},
					Headers: map[string]string{
						"key3": "val3",
						"key4": "val4",
					},
				},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.fixture, func(t *testing.T) {
			require := testutil.Require(t)

			fixture := fixtures.MustReadFile(fmt.Sprintf("config/endpoint_group/%v.json", test.fixture))
			var actual config.EndpointGroup
			err := actual.UnmarshalText(fixture)
			require.NoError(err)
			require.Equal(test.expected, actual)
		})
	}
}

func TestEndpointGroup_Error(t *testing.T) {
	tests := []struct {
		fixture string
	}{
		{
			fixture: "invalid_name",
		},
		{
			fixture: "invalid_url",
		},
		{
			fixture: "invalid_sticky",
		},
		{
			fixture: "invalid_failover_sticky",
		},
		{
			fixture: "invalid_json",
		},
	}
	for _, test := range tests {
		t.Run(test.fixture, func(t *testing.T) {
			require := testutil.Require(t)

			fixture := fixtures.MustReadFile(fmt.Sprintf("config/endpoint_group/%v.json", test.fixture))
			var actual config.EndpointGroup
			err := actual.UnmarshalText(fixture)
			require.Error(err)
		})
	}
}

func TestConsolidationEnabledRejectsDynamoDB(t *testing.T) {
	require := testutil.Require(t)

	require.NoError(os.Setenv("CHAINSTORAGE_AWS_STORAGE_CONSOLIDATION_ENABLED", "true"))
	require.NoError(os.Setenv("CHAINSTORAGE_AWS_STORAGE_CONSOLIDATION_MODE", string(config.ConsolidationModeShadowDualWrite)))
	defer os.Unsetenv("CHAINSTORAGE_AWS_STORAGE_CONSOLIDATION_ENABLED")
	defer os.Unsetenv("CHAINSTORAGE_AWS_STORAGE_CONSOLIDATION_MODE")

	_, err := config.New()
	require.Error(err)
	require.Contains(err.Error(), "requires Postgres meta storage")
}

func TestConsolidationReadShadowFirstRejectsDynamoDB(t *testing.T) {
	require := testutil.Require(t)

	require.NoError(os.Setenv("CHAINSTORAGE_AWS_STORAGE_CONSOLIDATION_READ_SHADOW_FIRST", "true"))
	defer os.Unsetenv("CHAINSTORAGE_AWS_STORAGE_CONSOLIDATION_READ_SHADOW_FIRST")

	_, err := config.New()
	require.Error(err)
	require.Contains(err.Error(), "requires Postgres meta storage")
}

func TestConsolidationAutoConsolidateModeAccepted(t *testing.T) {
	require := testutil.Require(t)

	t.Setenv("CHAINSTORAGE_AWS_STORAGE_CONSOLIDATION_MODE", string(config.ConsolidationModeAutoConsolidate))

	cfg, err := config.New()
	require.NoError(err)
	require.Equal(config.ConsolidationModeAutoConsolidate, cfg.AWS.Storage.Consolidation.Mode)
}

func TestConsolidationHistoricalBackfillModeAcceptedAsDeprecatedAlias(t *testing.T) {
	require := testutil.Require(t)

	t.Setenv("CHAINSTORAGE_AWS_STORAGE_CONSOLIDATION_MODE", string(config.ConsolidationModeHistoricalBackfill))

	cfg, err := config.New()
	require.NoError(err)
	require.Equal(config.ConsolidationModeHistoricalBackfill, cfg.AWS.Storage.Consolidation.Mode)
}

func TestStorageGenerationDefaultsToLegacy(t *testing.T) {
	require := testutil.Require(t)

	cfg, err := config.New()
	require.NoError(err)
	require.Equal(config.StorageGeneration("legacy"), cfg.AWS.BlockStorage.WriteGeneration)
	generation, err := cfg.WriteBlockStorageGeneration()
	require.NoError(err)
	require.Equal(
		"",
		generation,
	)
	bucket, err := cfg.WriteBlockStorageBucket()
	require.NoError(err)
	require.Equal(cfg.AWS.Bucket, bucket)
}

func TestStorageGenerationV2Config(t *testing.T) {
	require := testutil.Require(t)
	cfg, err := config.New()
	require.NoError(err)
	cfg.AWS.BlockStorage = config.BlockStorageConfig{
		WriteGeneration: "v2",
		Generations: map[string]config.BlockStorageGenerationConfig{
			"v2": {Bucket: "chainstorage-blocks-v2"},
			"v3": {Bucket: "chainstorage-blocks-v3"},
		},
	}
	generation, err := cfg.WriteBlockStorageGeneration()
	require.NoError(err)
	require.Equal(
		"v2",
		generation,
	)
	bucket, err := cfg.WriteBlockStorageBucket()
	require.NoError(err)
	require.Equal("chainstorage-blocks-v2", bucket)
	legacyBucket, err := cfg.ResolveBlockStorageBucket("")
	require.NoError(err)
	require.Equal(cfg.AWS.Bucket, legacyBucket)
	v3Bucket, err := cfg.ResolveBlockStorageBucket("v3")
	require.NoError(err)
	require.Equal("chainstorage-blocks-v3", v3Bucket)
}

func TestStorageGenerationV2CanRemainDormant(t *testing.T) {
	require := testutil.Require(t)
	cfg, err := config.New()
	require.NoError(err)
	cfg.AWS.BlockStorage.Generations = map[string]config.BlockStorageGenerationConfig{
		"v2": {Bucket: "chainstorage-blocks-v2"},
	}
	bucket, err := cfg.WriteBlockStorageBucket()
	require.NoError(err)
	require.Equal(cfg.AWS.Bucket, bucket)
}

func TestResolveBlockStorageBucketRejectsUnavailableOrUnknownGeneration(t *testing.T) {
	require := testutil.Require(t)
	cfg, err := config.New()
	require.NoError(err)

	_, err = cfg.ResolveBlockStorageBucket("v2")
	require.ErrorContains(err, "unconfigured storage generation \"v2\"")
	_, err = cfg.ResolveBlockStorageBucket("future")
	require.ErrorContains(err, "unsupported block storage generation \"future\"")
}

func TestWriteBlockStorageGenerationRejectsInvalidManualConfig(t *testing.T) {
	require := testutil.Require(t)
	cfg, err := config.New()
	require.NoError(err)
	cfg.AWS.BlockStorage.WriteGeneration = config.StorageGeneration("future")

	_, err = cfg.WriteBlockStorageGeneration()
	require.ErrorContains(err, "invalid aws.block_storage.write_generation")
	_, err = cfg.WriteBlockStorageBucket()
	require.ErrorContains(err, "invalid aws.block_storage.write_generation")
}

func TestConsolidationSingleBlockObjectRetentionConfig(t *testing.T) {
	require := testutil.Require(t)

	cfg, err := config.New()
	require.NoError(err)
	require.Equal(config.DefaultSingleBlockObjectRetention, cfg.AWS.Storage.Consolidation.SingleBlockObjectRetention)

	t.Setenv("CHAINSTORAGE_AWS_STORAGE_CONSOLIDATION_SINGLE_BLOCK_OBJECT_RETENTION", "24h")
	cfg, err = config.New()
	require.NoError(err)
	require.Equal(24*time.Hour, cfg.AWS.Storage.Consolidation.SingleBlockObjectRetention)
}

func TestDeprecatedConsolidationConfigAliases(t *testing.T) {
	require := testutil.Require(t)

	t.Setenv("CHAINSTORAGE_AWS_STORAGE_CONSOLIDATION_MODE", "legacy_only")
	t.Setenv("CHAINSTORAGE_AWS_STORAGE_CONSOLIDATION_LEGACY_OBJECT_RETENTION", "24h")
	t.Setenv("CHAINSTORAGE_AWS_STORAGE_CONSOLIDATION_LEGACY_SHADOW_WRITE_AFTER_SYNCER_CUTOVER", "true")

	cfg, err := config.New()
	require.NoError(err)
	require.Equal(config.ConsolidationModeSingleBlockOnly, cfg.AWS.Storage.Consolidation.Mode)
	require.Equal(24*time.Hour, cfg.AWS.Storage.Consolidation.SingleBlockObjectRetention)
	require.True(cfg.AWS.Storage.Consolidation.SingleBlockShadowWriteAfterSyncerCutover)
}

func TestCanonicalConsolidationConfigTakesPrecedenceOverDeprecatedAliases(t *testing.T) {
	require := testutil.Require(t)

	t.Setenv("CHAINSTORAGE_AWS_STORAGE_CONSOLIDATION_SINGLE_BLOCK_OBJECT_RETENTION", "48h")
	t.Setenv("CHAINSTORAGE_AWS_STORAGE_CONSOLIDATION_LEGACY_OBJECT_RETENTION", "24h")
	t.Setenv("CHAINSTORAGE_AWS_STORAGE_CONSOLIDATION_SINGLE_BLOCK_SHADOW_WRITE_AFTER_SYNCER_CUTOVER", "false")
	t.Setenv("CHAINSTORAGE_AWS_STORAGE_CONSOLIDATION_LEGACY_SHADOW_WRITE_AFTER_SYNCER_CUTOVER", "true")

	cfg, err := config.New()
	require.NoError(err)
	require.Equal(48*time.Hour, cfg.AWS.Storage.Consolidation.SingleBlockObjectRetention)
	require.False(cfg.AWS.Storage.Consolidation.SingleBlockShadowWriteAfterSyncerCutover)
}

func TestConsolidationSingleBlockObjectRetentionMustBePositiveWhenEnabled(t *testing.T) {
	require := testutil.Require(t)

	t.Setenv("CHAINSTORAGE_STORAGE_TYPE_META", "POSTGRES")
	t.Setenv("CHAINSTORAGE_AWS_STORAGE_CONSOLIDATION_ENABLED", "true")
	t.Setenv("CHAINSTORAGE_AWS_STORAGE_CONSOLIDATION_MODE", string(config.ConsolidationModeAutoConsolidate))
	t.Setenv("CHAINSTORAGE_AWS_STORAGE_CONSOLIDATION_SINGLE_BLOCK_OBJECT_RETENTION", "0s")

	_, err := config.New()
	require.Error(err)
	require.Contains(err.Error(), "single_block_object_retention must be positive")
}

func TestConsolidationMaxChunkUncompressedBytesMustBePositiveWhenEnabled(t *testing.T) {
	require := testutil.Require(t)

	t.Setenv("CHAINSTORAGE_STORAGE_TYPE_META", "POSTGRES")
	t.Setenv("CHAINSTORAGE_AWS_STORAGE_CONSOLIDATION_ENABLED", "true")
	t.Setenv("CHAINSTORAGE_AWS_STORAGE_CONSOLIDATION_MODE", string(config.ConsolidationModeAutoConsolidate))
	t.Setenv("CHAINSTORAGE_AWS_STORAGE_CONSOLIDATION_MAX_CHUNK_UNCOMPRESSED_BYTES", "0")

	_, err := config.New()
	require.Error(err)
	require.Contains(err.Error(), "max_chunk_uncompressed_bytes must be positive when set")
}

func TestConsolidationPromoteFinalizedRequiresSafePromotionLag(t *testing.T) {
	tests := []struct {
		name        string
		env         map[string]string
		expectedErr string
	}{
		{
			name: "missing safe promotion lag",
			env: map[string]string{
				"CHAINSTORAGE_AWS_STORAGE_CONSOLIDATION_PROMOTION_GATE_HEIGHT": "100000",
			},
			expectedErr: "requires safe_promotion_lag",
		},
		{
			name: "safe promotion lag below irreversible distance",
			env: map[string]string{
				"CHAINSTORAGE_AWS_STORAGE_CONSOLIDATION_PROMOTION_GATE_HEIGHT": "100000",
				"CHAINSTORAGE_AWS_STORAGE_CONSOLIDATION_SAFE_PROMOTION_LAG":    "1",
			},
			expectedErr: "must be at least irreversible_distance",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require := testutil.Require(t)
			t.Setenv("CHAINSTORAGE_STORAGE_TYPE_META", "POSTGRES")
			t.Setenv("CHAINSTORAGE_AWS_STORAGE_CONSOLIDATION_ENABLED", "true")
			t.Setenv("CHAINSTORAGE_AWS_STORAGE_CONSOLIDATION_MODE", string(config.ConsolidationModePromoteFinalized))
			for k, v := range test.env {
				t.Setenv(k, v)
			}

			_, err := config.New()
			require.Error(err)
			require.Contains(err.Error(), test.expectedErr)
		})
	}
}

func TestConsolidationPromoteFinalizedAllowsMissingPromotionGate(t *testing.T) {
	require := testutil.Require(t)

	t.Setenv("CHAINSTORAGE_STORAGE_TYPE_META", "POSTGRES")
	t.Setenv("CHAINSTORAGE_AWS_STORAGE_CONSOLIDATION_ENABLED", "true")
	t.Setenv("CHAINSTORAGE_AWS_STORAGE_CONSOLIDATION_MODE", string(config.ConsolidationModePromoteFinalized))
	t.Setenv("CHAINSTORAGE_AWS_STORAGE_CONSOLIDATION_SAFE_PROMOTION_LAG", "100000")

	cfg, err := config.New()
	require.NoError(err)
	require.Nil(cfg.AWS.Storage.Consolidation.PromotionGateHeight)
	require.NotNil(cfg.AWS.Storage.Consolidation.SafePromotionLag)
}

func TestConsolidationPromoteFinalizedRejectsDynamoDB(t *testing.T) {
	require := testutil.Require(t)

	t.Setenv("CHAINSTORAGE_AWS_STORAGE_CONSOLIDATION_ENABLED", "true")
	t.Setenv("CHAINSTORAGE_AWS_STORAGE_CONSOLIDATION_MODE", string(config.ConsolidationModePromoteFinalized))
	t.Setenv("CHAINSTORAGE_AWS_STORAGE_CONSOLIDATION_PROMOTION_GATE_HEIGHT", "100000")
	t.Setenv("CHAINSTORAGE_AWS_STORAGE_CONSOLIDATION_SAFE_PROMOTION_LAG", "100000")

	_, err := config.New()
	require.Error(err)
	require.Contains(err.Error(), "requires Postgres meta storage")
}

func TestParseConfigName(t *testing.T) {
	tests := []struct {
		configName string
		blockchain common.Blockchain
		network    common.Network
		sidechain  api.SideChain
	}{
		{
			configName: "ethereum_mainnet",
			blockchain: common.Blockchain_BLOCKCHAIN_ETHEREUM,
			network:    common.Network_NETWORK_ETHEREUM_MAINNET,
			sidechain:  api.SideChain_SIDECHAIN_NONE,
		},
		{
			configName: "ethereum_goerli",
			blockchain: common.Blockchain_BLOCKCHAIN_ETHEREUM,
			network:    common.Network_NETWORK_ETHEREUM_GOERLI,
			sidechain:  api.SideChain_SIDECHAIN_NONE,
		},
		{
			configName: "bsc_mainnet",
			blockchain: common.Blockchain_BLOCKCHAIN_BSC,
			network:    common.Network_NETWORK_BSC_MAINNET,
			sidechain:  api.SideChain_SIDECHAIN_NONE,
		},
		{
			configName: "bsc_testnet",
			blockchain: common.Blockchain_BLOCKCHAIN_BSC,
			network:    common.Network_NETWORK_BSC_TESTNET,
			sidechain:  api.SideChain_SIDECHAIN_NONE,
		},
		{
			configName: "ethereum-mainnet",
			blockchain: common.Blockchain_BLOCKCHAIN_ETHEREUM,
			network:    common.Network_NETWORK_ETHEREUM_MAINNET,
			sidechain:  api.SideChain_SIDECHAIN_NONE,
		},
		{
			configName: "ethereum-mainnet-beacon",
			blockchain: common.Blockchain_BLOCKCHAIN_ETHEREUM,
			network:    common.Network_NETWORK_ETHEREUM_MAINNET,
			sidechain:  api.SideChain_SIDECHAIN_ETHEREUM_MAINNET_BEACON,
		},
	}
	for _, test := range tests {
		t.Run(test.configName, func(t *testing.T) {
			require := testutil.Require(t)

			actualBlockchain, actualNetwork, actualSidechain, err := config.ParseConfigName(test.configName)
			require.NoError(err)
			require.Equal(test.blockchain, actualBlockchain)
			require.Equal(test.network, actualNetwork)
			require.Equal(test.sidechain, actualSidechain)
		})
	}
}

func makeTestEndpointGroupBytes(name string, url string, user string, password string, weight uint8, cookieHash string, cookiePassive bool, headerHash string) ([]byte, error) {
	eg := config.EndpointGroup{
		Endpoints: []config.Endpoint{
			{
				Name:     name,
				Url:      url,
				User:     user,
				Password: password,
				Weight:   weight,
			},
		},
		EndpointConfig: config.EndpointConfig{
			StickySession: config.StickySessionConfig{
				CookieHash:    cookieHash,
				CookiePassive: cookiePassive,
				HeaderHash:    headerHash,
			},
		},
	}
	return json.Marshal(&eg)
}

func getDataCompressionType(cfg *config.Config) api.Compression {
	if networkUnCompressed[cfg.Network()] {
		return api.Compression_NONE
	}

	return api.Compression_GZIP
}

func defaultConsolidationConfig() config.ConsolidationConfig {
	memoryBudgetBytes := uint64(268435456)
	return config.ConsolidationConfig{
		Enabled:                    false,
		Mode:                       config.ConsolidationModeSingleBlockOnly,
		Codec:                      api.Compression_ZSTD,
		CodecLevel:                 6,
		MaxCompressedBytes:         2147483648,
		MaxUncompressedBytes:       137438953472,
		MaxBlocks:                  1000,
		CompressionChunkBlocks:     10,
		FlushInterval:              time.Minute,
		ShadowTimeout:              30 * time.Second,
		MaxInflightRawBlocks:       4,
		MemoryBudgetBytes:          &memoryBudgetBytes,
		LocalSpillDir:              "/tmp/chainstorage-cscb",
		ShardSize:                  10000,
		MultipartThreshold:         134217728,
		ReadShadowFirst:            false,
		SingleBlockObjectRetention: config.DefaultSingleBlockObjectRetention,
	}
}

func TestUseFailoverEndpoints(t *testing.T) {
	testapp.TestAllConfigs(t, func(t *testing.T, cfg *config.Config) {
		require := testutil.Require(t)

		masterEndpoints := `{"endpoints": [{"name": "master", "url": "url"}], "endpoints_failover": [{"name": "master_failover", "url": "url"}], "use_failover": false, "endpoint_config": {"headers": {"k1":"v1"}}}`
		slaveEndpoints := `{"endpoints": [{"name": "slave", "url": "url"}], "endpoints_failover": [{"name": "slave_failover", "url": "url"}], "use_failover": false, "endpoint_config": {"headers": {"k1":"v1"}}}`
		validatorEndpoints := `{"endpoints": [{"name": "validator", "url": "url"}], "endpoints_failover": [{"name": "validator_failover", "url": "url"}], "use_failover": false, "endpoint_config": {"headers": {"k1":"v1"}}}`
		consensusEndpoints := `{"endpoints": [{"name": "consensus", "url": "url"}], "endpoints_failover": [{"name": "consensus_failover", "url": "url"}], "use_failover": false, "endpoint_config": {"headers": {"k1":"v1"}}}`

		err := os.Setenv("CHAINSTORAGE_CHAIN_CLIENT_MASTER_ENDPOINT_GROUP", masterEndpoints)
		require.NoError(err)
		defer os.Unsetenv("CHAINSTORAGE_CHAIN_CLIENT_MASTER_ENDPOINT_GROUP")
		err = os.Setenv("CHAINSTORAGE_CHAIN_CLIENT_SLAVE_ENDPOINT_GROUP", slaveEndpoints)
		require.NoError(err)
		defer os.Unsetenv("CHAINSTORAGE_CHAIN_CLIENT_SLAVE_ENDPOINT_GROUP")
		err = os.Setenv("CHAINSTORAGE_CHAIN_CLIENT_VALIDATOR_ENDPOINT_GROUP", validatorEndpoints)
		require.NoError(err)
		defer os.Unsetenv("CHAINSTORAGE_CHAIN_CLIENT_VALIDATOR_ENDPOINT_GROUP")
		err = os.Setenv("CHAINSTORAGE_CHAIN_CLIENT_CONSENSUS_ENDPOINT_GROUP", consensusEndpoints)
		require.NoError(err)
		defer os.Unsetenv("CHAINSTORAGE_CHAIN_CLIENT_CONSENSUS_ENDPOINT_GROUP")

		// Reload config using the env var.
		cfg, err = config.New(
			config.WithNamespace(cfg.Namespace()),
			config.WithEnvironment(cfg.Env()),
			config.WithBlockchain(cfg.Blockchain()),
			config.WithNetwork(cfg.Network()),
			config.WithSidechain(cfg.Sidechain()),
		)
		require.NoError(err)
		require.False(cfg.Chain.Client.Master.EndpointGroup.UseFailover)
		require.False(cfg.Chain.Client.Slave.EndpointGroup.UseFailover)
		require.False(cfg.Chain.Client.Validator.EndpointGroup.UseFailover)
		require.False(cfg.Chain.Client.Consensus.EndpointGroup.UseFailover)
		require.Equal("master", cfg.Chain.Client.Master.EndpointGroup.Endpoints[0].Name)
		require.Equal("slave", cfg.Chain.Client.Slave.EndpointGroup.Endpoints[0].Name)
		require.Equal("validator", cfg.Chain.Client.Validator.EndpointGroup.Endpoints[0].Name)
		require.Equal("consensus", cfg.Chain.Client.Consensus.EndpointGroup.Endpoints[0].Name)
		require.Equal("master_failover", cfg.Chain.Client.Master.EndpointGroup.EndpointsFailover[0].Name)
		require.Equal("slave_failover", cfg.Chain.Client.Slave.EndpointGroup.EndpointsFailover[0].Name)
		require.Equal("validator_failover", cfg.Chain.Client.Validator.EndpointGroup.EndpointsFailover[0].Name)
		require.Equal("consensus_failover", cfg.Chain.Client.Consensus.EndpointGroup.EndpointsFailover[0].Name)
		require.Equal("v1", cfg.Chain.Client.Master.EndpointGroup.EndpointConfig.Headers["k1"])
		require.Equal("v1", cfg.Chain.Client.Slave.EndpointGroup.EndpointConfig.Headers["k1"])
		require.Equal("v1", cfg.Chain.Client.Validator.EndpointGroup.EndpointConfig.Headers["k1"])
		require.Equal("v1", cfg.Chain.Client.Consensus.EndpointGroup.EndpointConfig.Headers["k1"])

		masterEndpoints = `{"endpoints": [{"name": "master", "url": "url"}], "endpoints_failover": [{"name": "master_failover", "url": "url"}], "use_failover": true}`
		err = os.Setenv("CHAINSTORAGE_CHAIN_CLIENT_MASTER_ENDPOINT_GROUP", masterEndpoints)
		require.NoError(err)
		defer os.Unsetenv("CHAINSTORAGE_CHAIN_CLIENT_MASTER_ENDPOINT_GROUP")

		cfg, err = config.New(
			config.WithNamespace(cfg.Namespace()),
			config.WithEnvironment(cfg.Env()),
			config.WithBlockchain(cfg.Blockchain()),
			config.WithNetwork(cfg.Network()),
			config.WithSidechain(cfg.Sidechain()),
		)

		require.NoError(err)
		require.True(cfg.Chain.Client.Master.EndpointGroup.UseFailover)
		require.False(cfg.Chain.Client.Slave.EndpointGroup.UseFailover)
		require.False(cfg.Chain.Client.Validator.EndpointGroup.UseFailover)
		require.False(cfg.Chain.Client.Consensus.EndpointGroup.UseFailover)
		require.Equal("master", cfg.Chain.Client.Master.EndpointGroup.Endpoints[0].Name)
		require.Equal("slave", cfg.Chain.Client.Slave.EndpointGroup.Endpoints[0].Name)
		require.Equal("validator", cfg.Chain.Client.Validator.EndpointGroup.Endpoints[0].Name)
		require.Equal("consensus", cfg.Chain.Client.Consensus.EndpointGroup.Endpoints[0].Name)
		require.Equal("master_failover", cfg.Chain.Client.Master.EndpointGroup.EndpointsFailover[0].Name)
		require.Equal("slave_failover", cfg.Chain.Client.Slave.EndpointGroup.EndpointsFailover[0].Name)
		require.Equal("validator_failover", cfg.Chain.Client.Validator.EndpointGroup.EndpointsFailover[0].Name)
		require.Equal("consensus_failover", cfg.Chain.Client.Consensus.EndpointGroup.EndpointsFailover[0].Name)

		slaveEndpoints = `{"endpoints": [{"name": "slave", "url": "url"}], "endpoints_failover": [{"name": "slave_failover", "url": "url"}], "use_failover": true}`
		err = os.Setenv("CHAINSTORAGE_CHAIN_CLIENT_SLAVE_ENDPOINT_GROUP", slaveEndpoints)
		require.NoError(err)
		defer os.Unsetenv("CHAINSTORAGE_CHAIN_CLIENT_SLAVE_ENDPOINT_GROUP")

		cfg, err = config.New(
			config.WithNamespace(cfg.Namespace()),
			config.WithEnvironment(cfg.Env()),
			config.WithBlockchain(cfg.Blockchain()),
			config.WithNetwork(cfg.Network()),
			config.WithSidechain(cfg.Sidechain()),
		)

		require.NoError(err)
		require.True(cfg.Chain.Client.Master.EndpointGroup.UseFailover)
		require.True(cfg.Chain.Client.Slave.EndpointGroup.UseFailover)
		require.False(cfg.Chain.Client.Validator.EndpointGroup.UseFailover)
		require.False(cfg.Chain.Client.Consensus.EndpointGroup.UseFailover)
		require.Equal("master", cfg.Chain.Client.Master.EndpointGroup.Endpoints[0].Name)
		require.Equal("slave", cfg.Chain.Client.Slave.EndpointGroup.Endpoints[0].Name)
		require.Equal("validator", cfg.Chain.Client.Validator.EndpointGroup.Endpoints[0].Name)
		require.Equal("master_failover", cfg.Chain.Client.Master.EndpointGroup.EndpointsFailover[0].Name)
		require.Equal("slave_failover", cfg.Chain.Client.Slave.EndpointGroup.EndpointsFailover[0].Name)
		require.Equal("validator_failover", cfg.Chain.Client.Validator.EndpointGroup.EndpointsFailover[0].Name)

		validatorEndpoints = `{"endpoints": [{"name": "validator", "url": "url"}], "endpoints_failover": [{"name": "validator_failover", "url": "url"}], "use_failover": true}`
		err = os.Setenv("CHAINSTORAGE_CHAIN_CLIENT_VALIDATOR_ENDPOINT_GROUP", validatorEndpoints)
		require.NoError(err)
		defer os.Unsetenv("CHAINSTORAGE_CHAIN_CLIENT_VALIDATOR_ENDPOINT_GROUP")

		cfg, err = config.New(
			config.WithNamespace(cfg.Namespace()),
			config.WithEnvironment(cfg.Env()),
			config.WithBlockchain(cfg.Blockchain()),
			config.WithNetwork(cfg.Network()),
			config.WithSidechain(cfg.Sidechain()),
		)

		require.NoError(err)
		require.True(cfg.Chain.Client.Master.EndpointGroup.UseFailover)
		require.True(cfg.Chain.Client.Slave.EndpointGroup.UseFailover)
		require.True(cfg.Chain.Client.Validator.EndpointGroup.UseFailover)
		require.False(cfg.Chain.Client.Consensus.EndpointGroup.UseFailover)
		require.Equal("master", cfg.Chain.Client.Master.EndpointGroup.Endpoints[0].Name)
		require.Equal("slave", cfg.Chain.Client.Slave.EndpointGroup.Endpoints[0].Name)
		require.Equal("validator", cfg.Chain.Client.Validator.EndpointGroup.Endpoints[0].Name)
		require.Equal("master_failover", cfg.Chain.Client.Master.EndpointGroup.EndpointsFailover[0].Name)
		require.Equal("slave_failover", cfg.Chain.Client.Slave.EndpointGroup.EndpointsFailover[0].Name)
		require.Equal("validator_failover", cfg.Chain.Client.Validator.EndpointGroup.EndpointsFailover[0].Name)

		consensusEndpoints = `{"endpoints": [{"name": "consensus", "url": "url"}], "endpoints_failover": [{"name": "consensus_failover", "url": "url"}], "use_failover": true}`
		err = os.Setenv("CHAINSTORAGE_CHAIN_CLIENT_CONSENSUS_ENDPOINT_GROUP", consensusEndpoints)
		require.NoError(err)
		defer os.Unsetenv("CHAINSTORAGE_CHAIN_CLIENT_CONSENSUS_ENDPOINT_GROUP")

		cfg, err = config.New(
			config.WithNamespace(cfg.Namespace()),
			config.WithEnvironment(cfg.Env()),
			config.WithBlockchain(cfg.Blockchain()),
			config.WithNetwork(cfg.Network()),
			config.WithSidechain(cfg.Sidechain()),
		)
		require.NoError(err)
		require.True(cfg.Chain.Client.Consensus.EndpointGroup.UseFailover)
	})
}

func TestGetCommonTags(t *testing.T) {
	require := testutil.Require(t)

	cfg, err := config.New()
	require.NoError(err)
	require.Equal(map[string]string{
		"blockchain": "ethereum",
		"network":    "ethereum-mainnet",
		"tier":       "1",
		"sidechain":  "none",
	}, cfg.GetCommonTags())
}

func TestDefaultHttpTimeout(t *testing.T) {
	require := testutil.Require(t)

	cfg, err := config.New()
	require.NoError(err)
	require.Equal(0*time.Second, cfg.Chain.Client.HttpTimeout)
}

func TestDefaultTxBatchSize(t *testing.T) {
	require := testutil.Require(t)

	cfg, err := config.New()
	require.NoError(err)
	require.Equal(100, cfg.Chain.Client.TxBatchSize)
}

func TestValidateAWSstorageConfig(t *testing.T) {
	// Test that Postgres validation is skipped when Postgres is nil
	require := testutil.Require(t)

	cfg, err := config.New()
	require.NoError(err)

	// Test 1: DynamoDB config when Postgres is nil
	cfg.AWS.Postgres = nil
	cfg.AWS.DynamoDB = &config.DynamoDBConfig{
		BlockTable:                    "block_table",
		VersionedEventTable:           "versioned_event_table",
		VersionedEventTableBlockIndex: "versioned_event_table_block_index",
	}

	// Validate the config - this should NOT fail even though Postgres is nil
	// and its fields (Host, Port, Database, SSLMode) are marked as required
	validate := validator.New()
	err = validate.Struct(&cfg.AWS)
	require.NoError(err, "Validation should not fail when Postgres is nil")

	require.Equal("block_table", cfg.AWS.DynamoDB.BlockTable)
	require.Equal("versioned_event_table", cfg.AWS.DynamoDB.VersionedEventTable)
	require.Equal("versioned_event_table_block_index", cfg.AWS.DynamoDB.VersionedEventTableBlockIndex)
	cfg.AWS.DynamoDB = &config.DynamoDBConfig{
		BlockTable:                    "block_table",
		VersionedEventTable:           "versioned_event_table",
		VersionedEventTableBlockIndex: "versioned_event_table_block_index",
	}
	require.Equal("block_table", cfg.AWS.DynamoDB.BlockTable)
	require.Equal("versioned_event_table", cfg.AWS.DynamoDB.VersionedEventTable)
	require.Equal("versioned_event_table_block_index", cfg.AWS.DynamoDB.VersionedEventTableBlockIndex)
	// Test 2: Postgres config when DynamoDB is nil - with valid config
	cfg.AWS.DynamoDB = nil
	cfg.AWS.Postgres = &config.PostgresConfig{
		Host:     "localhost",
		Port:     5432,
		Database: "chainstorage",
		SSLMode:  "disable",
	}
	err = validate.Struct(&cfg.AWS)
	require.NoError(err, "Validation should pass with valid Postgres config")
	require.Equal("localhost", cfg.AWS.Postgres.Host)
	require.Equal(5432, cfg.AWS.Postgres.Port)
	require.Equal("chainstorage", cfg.AWS.Postgres.Database)
	require.Equal("disable", cfg.AWS.Postgres.SSLMode)

	// Test 3: Postgres config with missing required fields should fail validation
	cfg.AWS.DynamoDB = nil
	cfg.AWS.Postgres = &config.PostgresConfig{
		Host: "localhost",
		Port: 5432,
		// Missing Database and SSLMode which are required
	}
	err = validate.Struct(&cfg.AWS)
	require.Error(err, "Validation should fail when Postgres is not nil but has missing required fields")
	// Test 4: Both configs valid when both are not nil
	cfg.AWS.DynamoDB = &config.DynamoDBConfig{
		BlockTable:                    "block_table",
		VersionedEventTable:           "versioned_event_table",
		VersionedEventTableBlockIndex: "versioned_event_table_block_index",
	}
	cfg.AWS.Postgres = &config.PostgresConfig{
		Host:     "localhost",
		Port:     5432,
		Database: "chainstorage",
		SSLMode:  "disable",
	}
	err = validate.Struct(&cfg.AWS)
	require.NoError(err, "Validation should pass when both configs are valid")
	require.Equal("block_table", cfg.AWS.DynamoDB.BlockTable)
	require.Equal("versioned_event_table", cfg.AWS.DynamoDB.VersionedEventTable)
	require.Equal("versioned_event_table_block_index", cfg.AWS.DynamoDB.VersionedEventTableBlockIndex)
	require.Equal("localhost", cfg.AWS.Postgres.Host)
	require.Equal(5432, cfg.AWS.Postgres.Port)
	require.Equal("chainstorage", cfg.AWS.Postgres.Database)
	require.Equal("disable", cfg.AWS.Postgres.SSLMode)

	// Test 5: Both configs nil - validation should FAIL
	// because at least one storage backend is required (required_without validation)
	cfg.AWS.DynamoDB = nil
	cfg.AWS.Postgres = nil
	err = validate.Struct(&cfg.AWS)
	require.Error(err, "Validation should fail when both configs are nil - at least one storage backend is required")
}
