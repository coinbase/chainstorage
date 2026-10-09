package tally

import (
	"testing"

	"github.com/uber-go/tally/v4"
	"go.uber.org/fx"

	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/utils/testapp"
	"github.com/coinbase/chainstorage/internal/utils/testutil"
)

func TestNewReporterDefaultNoStatsD(t *testing.T) {
	testapp.TestAllConfigs(t, func(t *testing.T, cfg *config.Config) {
		require := testutil.Require(t)

		var reporter tally.StatsReporter
		testapp.New(
			t,
			testapp.WithConfig(cfg),
			fx.Provide(NewStatsReporter),
			fx.Populate(&reporter),
		)

		require.Equal(tally.NullStatsReporter, reporter)
		require.Equal(false, reporter.Capabilities().Reporting())
		require.Equal(false, reporter.Capabilities().Tagging())
	})
}

func TestNewReporterDefaultWithStatsD(t *testing.T) {
	testapp.TestAllConfigs(t, func(t *testing.T, cfg *config.Config) {
		require := testutil.Require(t)
		cfg.StatsD = &config.StatsDConfig{
			Address: "localhost:8125",
		}
		var reporter tally.StatsReporter
		testapp.New(
			t,
			testapp.WithConfig(cfg),
			fx.Provide(NewStatsReporter),
			fx.Populate(&reporter),
		)
		require.NotEqual(tally.NullStatsReporter, reporter)
		require.Equal(true, reporter.Capabilities().Reporting())
		require.Equal(true, reporter.Capabilities().Tagging())
	})
}

func TestNewReporterDefaultWithPrometheus(t *testing.T) {
	testapp.TestAllConfigs(t, func(t *testing.T, cfg *config.Config) {
		require := testutil.Require(t)
		cfg.Prometheus = &config.PrometheusConfig{
			// use any available port
			Port: 0,
		}

		var reporter tally.StatsReporter
		app := testapp.New(
			t,
			testapp.WithConfig(cfg),
			fx.Provide(NewStatsReporter),
			fx.Populate(&reporter),
		)

		// close app after the test so that the port is released
		t.Cleanup(app.Close)

		require.NotEqual(tally.NullStatsReporter, reporter)
		require.Equal(true, reporter.Capabilities().Reporting())
		require.Equal(true, reporter.Capabilities().Tagging())
	})
}
