package s3

import (
	"go.uber.org/fx"
)

var Module = fx.Options(
	fx.Provide(fx.Annotated{
		Name:   "s3",
		Target: NewS3BlobStorageFactory,
	}),
)
