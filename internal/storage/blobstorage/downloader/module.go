package downloader

import "go.uber.org/fx"

var Module = fx.Options(
	fx.Provide(NewBlockDownloader),
	fx.Provide(NewHTTPClient),
)
