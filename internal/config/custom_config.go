package config

import (
	"go.uber.org/fx"
)

type (
	Params struct {
		fx.In
		CustomConfig *customConfig `optional:"true"`
	}

	customConfig struct {
		config *Config
	}
)

// WithCustomConfig injects a custom config to replace the default one.
func WithCustomConfig(config *Config) fx.Option {
	return fx.Provide(func() *customConfig {
		return &customConfig{
			config: config,
		}
	})
}
