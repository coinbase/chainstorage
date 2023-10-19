package s3

import (
	"go.uber.org/fx"
)

var Module = fx.Options(
	fx.Provide(fx.Annotated{
		Name:   "blobstorage/s3",
		Target: NewFactory,
	}),
)
