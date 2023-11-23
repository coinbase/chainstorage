package main

import (
	"testing"

	"go.temporal.io/sdk/testsuite"

	"github.com/coinbase/chainstorage/internal/cadence"
	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/utils/testapp"
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
