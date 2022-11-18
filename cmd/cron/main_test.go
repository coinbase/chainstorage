package main

import (
	"testing"

	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/utils/testapp"
)

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

		manager := startManager(
			config.WithCustomConfig(cfg),
		)

		manager.Shutdown()
		manager.WaitForInterrupt()
	})
}
