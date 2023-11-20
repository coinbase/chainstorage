package main

import (
	"testing"
	"time"

	"go.temporal.io/sdk/testsuite"

	"github.com/coinbase/chainstorage/internal/cadence"
	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/utils/testapp"
)

type APITestSuite struct {
	testsuite.WorkflowTestSuite
	t *testing.T
}

func (s APITestSuite) T() *testing.T {
	return s.t
}

func TestIntegrationAPI(t *testing.T) {
	testapp.TestAllConfigs(t, func(t *testing.T, cfg *config.Config) {
		if !cfg.IsFunctionalTest() {
			t.Skip()
		}

		if cfg.Env() != config.EnvDevelopment {
			// Only connect to the dependencies in the development environment, because the cadence cluster is
			// unavailable in the local environment and inaccessible in the production environment.
			return
		}

		ts := &APITestSuite{t: t}
		env := cadence.NewTestEnv(ts)
		manager := startManager(
			config.WithCustomConfig(cfg),
			cadence.WithTestEnv(env),
		)

		time.Sleep(100 * time.Millisecond)
		manager.Shutdown()
		manager.WaitForInterrupt()
	})
}
