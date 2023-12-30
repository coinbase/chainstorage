package gcs

import (
	"go.uber.org/fx"
)

var Module = fx.Options(
	fx.Provide(fx.Annotated{
		Name:   "blobstorage/gcs",
		Target: NewFactory,
	}),
)
