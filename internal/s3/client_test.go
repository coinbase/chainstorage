package s3

import (
	"testing"

	"go.uber.org/fx"

	"github.com/coinbase/chainstorage/internal/utils/testapp"
	"github.com/coinbase/chainstorage/internal/utils/testutil"
)

func TestIntegrationS3(t *testing.T) {
	require := testutil.Require(t)

	var downloader Downloader
	var uploader Uploader
	app := testapp.New(
		t,
		testapp.WithIntegration(),
		Module,
		fx.Populate(&downloader),
		fx.Populate(&uploader),
	)
	defer app.Close()

	require.NotNil(downloader)
	require.NotNil(uploader)
}
