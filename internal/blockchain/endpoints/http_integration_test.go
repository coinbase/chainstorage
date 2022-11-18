package endpoints

import (
	"bytes"
	"context"
	"net/http"
	"testing"

	"go.uber.org/zap/zaptest"

	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/utils/log"
	"github.com/coinbase/chainstorage/internal/utils/testutil"
	"github.com/coinbase/chainstorage/protos/coinbase/c3/common"
)

func TestIntegrationALBStickySession(t *testing.T) {
	require := testutil.Require(t)

	logger := log.NewDevelopment()

	cfg, err := config.New(
		config.WithBlockchain(common.Blockchain_BLOCKCHAIN_POLYGON),
		config.WithNetwork(common.Network_NETWORK_POLYGON_MAINNET),
	)
	require.NoError(err)

	// skip this test if the $TEST_TYPE is not functional or ALBStickySession is not enabled
	if !cfg.IsFunctionalTest() || !cfg.Chain.Client.Master.EndpointGroup.ActiveStickySession().CookiePassive {
		logger.Warn("skipping ALB sticky session test")
		t.Skip()
	}

	params := EndpointProviderParams{
		Config: cfg,
		Logger: zaptest.NewLogger(t),
	}
	master, err := newEndpointProvider(params.Logger, params.Config, &params.Config.Chain.Client.Master.EndpointGroup, "master")
	require.NoError(err)

	endpoint, err := master.GetEndpoint(context.Background())
	require.NoError(err)

	client := endpoint.Client
	jar := client.Jar
	require.NotNil(jar)
	_, ok := jar.(http.CookieJar)
	require.True(ok)

	url := endpoint.Config.Url

	requestBody := []byte(`{"jsonrpc":"2.0", "method": "eth_blockNumber", "id": 0}`)
	request, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(requestBody))
	require.NoError(err)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", "application/json")

	cookies := jar.Cookies(request.URL)
	require.Equal(0, len(cookies))

	_, err = client.Do(request)
	require.NoError(err)

	cookies = jar.Cookies(request.URL)
	require.Equal(2, len(cookies))

	var cookieKeys [2]string
	for i, cookie := range cookies {
		cookieKeys[i] = cookie.Name
	}
	require.Contains(cookieKeys, "AWSALB")
}
