package testapp

import (
	"fmt"
	"strings"
	"testing"

	"github.com/uber-go/tally"
	"go.uber.org/fx"
	"go.uber.org/fx/fxtest"
	"go.uber.org/zap"

	services2 "github.com/coinbase/chainstorage/sdk/services"

	"github.com/coinbase/chainstorage/internal/aws"
	"github.com/coinbase/chainstorage/internal/blockchain/endpoints"
	"github.com/coinbase/chainstorage/internal/cadence"
	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/tracer"
	"github.com/coinbase/chainstorage/internal/utils/fxparams"
	"github.com/coinbase/chainstorage/internal/utils/testutil"
	"github.com/coinbase/chainstorage/protos/coinbase/c3/common"
)

type (
	TestApp interface {
		Close()
		Logger() *zap.Logger
		Config() *config.Config
	}

	TestFn func(t *testing.T, cfg *config.Config)

	TestConfig struct {
		Namespace   string
		ConfigNames []string
	}

	testAppImpl struct {
		app    *fxtest.App
		logger *zap.Logger
		config *config.Config
	}

	localOnlyOption struct {
		fx.Option
	}
)

var (
	TestConfigs = []TestConfig{
		{
			Namespace: "chainstorage",
			ConfigNames: []string{
				"arbitrum-mainnet",
				"avacchain-mainnet",
				"bitcoin-mainnet",
				"bsc-mainnet",
				"dogecoin-mainnet",
				"ethereum-goerli",
				"ethereum-mainnet",
				"optimism-mainnet",
				"polygon-mainnet",
				"solana-mainnet",
			},
		},
	}
)

func New(t testing.TB, opts ...fx.Option) TestApp {
	manager := services2.NewMockSystemManager()

	var cfg *config.Config
	opts = append(
		opts,
		aws.Module,
		cadence.Module,
		config.Module,
		endpoints.Module,
		fxparams.Module,
		tracer.Module,
		fx.NopLogger,
		fx.Provide(func() testing.TB { return t }),
		fx.Provide(func() *zap.Logger { return manager.Logger() }),
		fx.Provide(func() tally.Scope { return tally.NoopScope }),
		fx.Provide(func() services2.SystemManager { return manager }),
		fx.Populate(&cfg),
	)

	app := fxtest.New(t, opts...)
	app.RequireStart()
	return &testAppImpl{
		app:    app,
		logger: manager.Logger(),
		config: cfg,
	}
}

// WithConfig overrides the default config.
func WithConfig(cfg *config.Config) fx.Option {
	return config.WithCustomConfig(cfg)
}

// WithIntegration runs the test only if $TEST_TYPE is integration.
func WithIntegration() fx.Option {
	return &localOnlyOption{
		Option: fx.Invoke(func(tb testing.TB, cfg *config.Config, logger *zap.Logger) {
			if !cfg.IsIntegrationTest() {
				logger.Warn("skipping integration test", zap.String("test", tb.Name()))
				tb.Skip()
			}
		}),
	}
}

// WithFunctional runs the test only if $TEST_TYPE is functional.
func WithFunctional() fx.Option {
	return &localOnlyOption{
		Option: fx.Invoke(func(tb testing.TB, cfg *config.Config, logger *zap.Logger) {
			if !cfg.IsFunctionalTest() {
				logger.Warn("skipping functional test", zap.String("test", tb.Name()))
				tb.Skip()
			}

			normalizedConfigName := strings.ReplaceAll(cfg.ConfigName, "_", "-")
			if !cfg.FunctionalTest.Empty() {
				for _, ft := range cfg.FunctionalTest.SkipFunctionalTest {
					if normalizedConfigName == ft.ConfigName {
						logger.Warn("skipping functional test for config",
							zap.String("test", tb.Name()),
							zap.String("config_name", cfg.ConfigName),
						)
						tb.Skip()
					}
				}
			}
		}),
	}
}

// WithBlockchainNetwork loads the config according to the specified blockchain and network.
func WithBlockchainNetwork(blockchain common.Blockchain, network common.Network) fx.Option {
	cfg, err := config.New(
		config.WithBlockchain(blockchain),
		config.WithNetwork(network),
	)
	if err != nil {
		panic(err)
	}

	return WithConfig(cfg)
}

func (a *testAppImpl) Close() {
	a.app.RequireStop()
}

func (a *testAppImpl) Logger() *zap.Logger {
	return a.logger
}

func (a *testAppImpl) Config() *config.Config {
	return a.config
}

var envsToTest = []config.Env{
	config.EnvLocal,
	config.EnvDevelopment,
	config.EnvProduction,
}

var AWSAccountsToTest = []config.AWSAccount{
	"",
	config.AWSAccountDevelopment,
	config.AWSAccountProduction,
}

func TestAllConfigs(t *testing.T, fn TestFn) {
	for _, testConfig := range TestConfigs {
		namespace := testConfig.Namespace
		for _, configName := range testConfig.ConfigNames {
			name := fmt.Sprintf("%v/%v", namespace, configName)
			t.Run(name, func(t *testing.T) {
				for _, env := range envsToTest {
					t.Run(string(env), func(t *testing.T) {
						require := testutil.Require(t)
						blockchain, network, err := config.ParseConfigName(configName)
						require.NoError(err)

						cfg, err := config.New(
							config.WithNamespace(namespace),
							config.WithEnvironment(env),
							config.WithBlockchain(blockchain),
							config.WithNetwork(network),
						)
						require.NoError(err)
						require.Equal(namespace, cfg.Namespace())
						require.Equal(env, cfg.Env())
						require.Equal(blockchain, cfg.Blockchain())
						require.Equal(network, cfg.Network())

						fn(t, cfg)
					})
				}
			})
		}
	}
}
