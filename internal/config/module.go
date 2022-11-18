package config

import (
	"go.uber.org/fx"
)

var Module = fx.Options(
	fx.Provide(NewFacade),
)

func NewFacade(params Params) (*Config, error) {
	if params.CustomConfig != nil {
		return params.CustomConfig.config, nil
	}

	return New()
}
