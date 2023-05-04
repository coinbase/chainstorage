package tracer

import (
	"github.com/opentracing/opentracing-go"
	"go.uber.org/fx"

	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/sdk/services"
)

type (
	Params struct {
		fx.In
		Config  *config.Config
		Manager services.SystemManager
	}
)

func NewTracer(params Params) opentracing.Tracer {
	if params.Config.IsTest() {
		return opentracing.NoopTracer{}
	}

	return params.Manager.Tracer()
}
