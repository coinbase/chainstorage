package main

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/testsuite"
	"go.uber.org/fx"

	"github.com/coinbase/chainstorage/internal/cadence"
	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/utils/testapp"
	"github.com/coinbase/chainstorage/sdk/services"
)

type (
	CronTestSuite struct {
		testsuite.WorkflowTestSuite
		t *testing.T
	}
)

func (s CronTestSuite) T() *testing.T {
	return s.t
}

func TestCronAppDependencyGraph(t *testing.T) {
	manager := services.NewManager()
	defer manager.Shutdown()

	require.NoError(t, fx.ValidateApp(cronAppOptions(manager)...))
}

func TestIntegrationCron(t *testing.T) {
	testapp.TestAllConfigs(t, func(t *testing.T, cfg *config.Config) {
		if !cfg.IsFunctionalTest() {
			t.Skip()
		}

		if cfg.Env() != config.EnvDevelopment {
			// Only connect to the dependencies in the development environment, because the cadence cluster is
			// unavailable in the local environment and inaccessible in the production environment.
			return
		}

		ts := &CronTestSuite{t: t}
		env := cadence.NewTestEnv(ts)
		manager := startManager(
			config.WithCustomConfig(cfg),
			cadence.WithTestEnv(env),
		)

		manager.Shutdown()
		manager.WaitForInterrupt()
	})
}
