package config_test

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

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

func TestDerivedConfigValues(t *testing.T) {
	testapp.TestAllConfigs(t, func(t *testing.T, cfg *config.Config) {
		require := testutil.Require(t)
		configName := cfg.ConfigName
		normalizedConfigName := strings.ReplaceAll(configName, "_", "-")

		// Verify template derived configs.
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

		expectedAWS := config.AwsConfig{
			Region:                 "us-east-1",
			Bucket:                 fmt.Sprintf("example-chainstorage-%v-%v", normalizedConfigName, cfg.AwsEnv()),
			DynamoDB:               dynamoDB,
			IsLocalStack:           true,
			IsResetLocal:           true,
			PresignedUrlExpiration: 30 * time.Minute,
			DLQ: config.SQSConfig{
				Name:                  fmt.Sprintf("example_chainstorage_blocks_%v_dlq", configName),
				VisibilityTimeoutSecs: 600,
				DelaySecs:             900,
				OwnerAccountId:        cfg.AWS.DLQ.OwnerAccountId,
			},
			Storage: config.StorageConfig{
				DataCompression: getDataCompressionType(cfg),
			},
			// Skip AWS account verification
			AWSAccount: cfg.AWS.AWSAccount,
		}
		require.Equal(expectedAWS, cfg.AWS)
	})
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
