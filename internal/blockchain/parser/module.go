package parser

import (
	"go.uber.org/fx"
)

var Module = fx.Options(
	fx.Provide(NewParser),
	WithParserFactory(),
)
