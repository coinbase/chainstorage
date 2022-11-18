package s3

import (
	"go.uber.org/fx"
)

var Module = fx.Options(
	fx.Provide(NewS3),
	fx.Provide(NewDownloader),
	fx.Provide(NewUploader),
	fx.Provide(NewClient),
)
