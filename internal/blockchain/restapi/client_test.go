package restapi_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"go.uber.org/fx"
	"go.uber.org/mock/gomock"

	"github.com/coinbase/chainstorage/internal/blockchain/restapi"
	restapimocks "github.com/coinbase/chainstorage/internal/blockchain/restapi/mocks"
	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/utils/retry"
	"github.com/coinbase/chainstorage/internal/utils/testapp"
	"github.com/coinbase/chainstorage/internal/utils/testutil"
)

// A dummy raw block
type DummyBlock struct {
	Hash string `json:"hash"`
}

// A dummy error response
type DummyErr struct {
	ErrorCode string `json:"error_code"`
}

type clientParams struct {
	fx.In
	Master restapi.Client `name:"master"`
	Slave  restapi.Client `name:"slave"`
}

func TestCall(t *testing.T) {
	require := testutil.Require(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	httpClient := restapimocks.NewMockHTTPClient(ctrl)
	body := ioutil.NopCloser(strings.NewReader(`{"hash": "0xabcd"}`))
	httpResponse := &http.Response{
		StatusCode: http.StatusOK,
		Body:       body,
	}
	httpClient.EXPECT().Do(gomock.Any()).Return(httpResponse, nil)

	var params clientParams
	app := testapp.New(
		t,
		withDummyEndpoints(),
		fx.Provide(restapi.New),
		fx.Provide(func() restapi.HTTPClient {
			return httpClient
		}),
		fx.Populate(&params),
	)
	defer app.Close()

	client := params.Master
	require.NotNil(client)
	response, err := client.Call(context.Background(),
		&restapi.RequestMethod{Name: "hello", ParamsPath: "path", Timeout: time.Duration(5)},
		nil)
	require.NoError(err)

	var block DummyBlock
	err = json.Unmarshal(response, &block)
	require.NoError(err)
	require.Equal("0xabcd", block.Hash)
}

func TestCall_RequestError(t *testing.T) {
	require := testutil.Require(t)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	httpClient := restapimocks.NewMockHTTPClient(ctrl)
	// Construct a REST API error response.
	body := ioutil.NopCloser(strings.NewReader(`{"error_code": "web_framework_error"}`))
	httpResponse := &http.Response{
		StatusCode: http.StatusBadRequest,
		Body:       body,
	}
	httpClient.EXPECT().Do(gomock.Any()).Return(httpResponse, nil)

	var params clientParams
	app := testapp.New(
		t,
		withDummyEndpoints(),
		fx.Provide(restapi.New),
		fx.Provide(func() restapi.HTTPClient {
			return httpClient
		}),
		fx.Populate(&params),
	)
	defer app.Close()

	client := params.Master
	require.NotNil(client)
	response, err := client.Call(context.Background(),
		&restapi.RequestMethod{Name: "hello", ParamsPath: "path", Timeout: time.Duration(5)},
		nil)

	require.Nil(response)
	require.Error(err)
	require.Contains(err.Error(), "method=&{hello path 5ns}")
	require.Contains(err.Error(), "requestBody=[]")
	require.Contains(err.Error(), "endpoint=node_name")

	// Check the http reponse error
	var errHTTP *restapi.HTTPError
	require.True(errors.As(err, &errHTTP))
	require.Equal(http.StatusBadRequest, errHTTP.Code)
	require.Equal(`{"error_code": "web_framework_error"}`, errHTTP.Response)

	// Check the rest api response error
	errOut := &DummyErr{}
	require.NoError(json.Unmarshal([]byte(errHTTP.Response), &errOut))
	require.Equal("web_framework_error", errOut.ErrorCode)
}

func TestCall_RequestError_FailedWithRetry(t *testing.T) {
	require := testutil.Require(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	httpClient := restapimocks.NewMockHTTPClient(ctrl)
	// Construct a REST API error response.
	httpClient.EXPECT().Do(gomock.Any()).DoAndReturn(func(_ *http.Request) (*http.Response, error) {
		body := ioutil.NopCloser(strings.NewReader(`{"error_code": "block_not_found"}`))
		return &http.Response{
			StatusCode: http.StatusInternalServerError,
			Body:       body,
		}, nil
	}).Times(retry.DefaultMaxAttempts)

	var params clientParams
	app := testapp.New(
		t,
		withDummyEndpoints(),
		fx.Provide(restapi.New),
		fx.Provide(func() restapi.HTTPClient {
			return httpClient
		}),
		fx.Populate(&params),
	)
	defer app.Close()

	client := params.Master
	require.NotNil(client)
	response, err := client.Call(context.Background(),
		&restapi.RequestMethod{Name: "hello", ParamsPath: "path", Timeout: time.Duration(5)},
		nil)

	require.Nil(response)
	require.Error(err)

	require.Contains(err.Error(), "method=&{hello path 5ns}")
	require.Contains(err.Error(), "requestBody=[]")
	require.Contains(err.Error(), "endpoint=node_name")

	// Check the http reponse error
	var errHTTP *restapi.HTTPError
	require.True(errors.As(err, &errHTTP))
	require.Equal(http.StatusInternalServerError, errHTTP.Code)
	require.Equal(`{"error_code": "block_not_found"}`, errHTTP.Response)

	// Check the rest api response error
	errOut := &DummyErr{}
	require.NoError(json.Unmarshal([]byte(errHTTP.Response), &errOut))
	require.Equal("block_not_found", errOut.ErrorCode)
}

func TestCall_RequestError_SucceededAfterRetries(t *testing.T) {
	require := testutil.Require(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	httpClient := restapimocks.NewMockHTTPClient(ctrl)
	failedResp := &http.Response{
		StatusCode: http.StatusTooManyRequests,
		Body:       ioutil.NopCloser(strings.NewReader(`{"error_code": "block_not_found"}`)),
	}
	successfulResp := &http.Response{
		StatusCode: http.StatusOK,
		Body:       ioutil.NopCloser(strings.NewReader(`{"hash": "0xabcd"}`)),
	}
	gomock.InOrder(
		httpClient.EXPECT().Do(gomock.Any()).Return(failedResp, nil),
		httpClient.EXPECT().Do(gomock.Any()).Return(successfulResp, nil),
	)

	var params clientParams
	app := testapp.New(
		t,
		withDummyEndpoints(),
		fx.Provide(restapi.New),
		fx.Provide(func() restapi.HTTPClient {
			return httpClient
		}),
		fx.Populate(&params),
	)
	defer app.Close()

	client := params.Master
	require.NotNil(client)
	response, err := client.Call(context.Background(),
		&restapi.RequestMethod{Name: "hello", ParamsPath: "path", Timeout: time.Duration(5)},
		nil)

	require.NoError(err)
	require.NotNil(response)
	var block DummyBlock
	err = json.Unmarshal(response, &block)
	require.NoError(err)
	require.Equal("0xabcd", block.Hash)
}

func TestCall_RequestError_WithCustomizedAttempts(t *testing.T) {
	require := testutil.Require(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	httpClient := restapimocks.NewMockHTTPClient(ctrl)
	maxAttempts := 4
	httpClient.EXPECT().Do(gomock.Any()).DoAndReturn(func(_ *http.Request) (*http.Response, error) {
		body := ioutil.NopCloser(strings.NewReader(`{"error_code": "block_not_found"}`))
		return &http.Response{
			StatusCode: http.StatusInternalServerError,
			Body:       body,
		}, nil
	}).Times(maxAttempts)

	var params clientParams
	app := testapp.New(
		t,
		withRetryMaxAttempts(maxAttempts),
		fx.Provide(restapi.New),
		fx.Provide(func() restapi.HTTPClient {
			return httpClient
		}),
		fx.Populate(&params),
	)
	defer app.Close()

	client := params.Master
	require.NotNil(client)
	response, err := client.Call(context.Background(),
		&restapi.RequestMethod{Name: "hello", ParamsPath: "path", Timeout: time.Duration(5)},
		nil)
	require.Error(err)
	require.Nil(response)

	require.Contains(err.Error(), "method=&{hello path 5ns}")
	require.Contains(err.Error(), "requestBody=[]")
	require.Contains(err.Error(), "endpoint=node_name")

	// Check the http reponse error
	var errHTTP *restapi.HTTPError
	require.True(errors.As(err, &errHTTP))
	require.Equal(http.StatusInternalServerError, errHTTP.Code)
	require.Equal(`{"error_code": "block_not_found"}`, errHTTP.Response)

	// Check the rest api response error
	errOut := &DummyErr{}
	require.NoError(json.Unmarshal([]byte(errHTTP.Response), &errOut))
	require.Equal("block_not_found", errOut.ErrorCode)
}

func TestCall_URLError(t *testing.T) {
	require := testutil.Require(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	httpClient := restapimocks.NewMockHTTPClient(ctrl)
	urlError := &url.Error{
		Op:  "Post",
		URL: "foo.com",
		Err: fmt.Errorf("a test error"),
	}
	httpClient.EXPECT().Do(gomock.Any()).Return(nil, urlError).Times(retry.DefaultMaxAttempts)

	var params clientParams
	app := testapp.New(
		t,
		withDummyEndpoints(),
		fx.Provide(restapi.New),
		fx.Provide(func() restapi.HTTPClient {
			return httpClient
		}),
		fx.Populate(&params),
	)
	defer app.Close()

	client := params.Master
	require.NotNil(client)
	response, err := client.Call(context.Background(),
		&restapi.RequestMethod{Name: "hello", ParamsPath: "foo.com", Timeout: time.Duration(5)},
		nil)
	require.Error(err)
	require.Nil(response)

	var uerr *url.Error
	require.False(errors.As(err, &uerr))
	errMsg := errors.Unwrap(err).Error()
	require.Contains(errMsg, "a test error")
	require.NotContains(errMsg, "foo.com")
}

func withDummyEndpoints() fx.Option {
	cfg, err := config.New()
	if err != nil {
		panic(err)
	}

	dummyEndpoints := []config.Endpoint{
		{
			Name:   "node_name",
			Url:    "node_url",
			Weight: 1,
		},
	}
	cfg.Chain.Client = config.ClientConfig{
		Master: config.JSONRPCConfig{
			EndpointGroup: config.EndpointGroup{
				Endpoints: dummyEndpoints,
			},
		},
		Slave: config.JSONRPCConfig{
			EndpointGroup: config.EndpointGroup{
				Endpoints: dummyEndpoints,
			},
		},
	}

	return testapp.WithConfig(cfg)
}

func withRetryMaxAttempts(maxAttempts int) fx.Option {
	cfg, err := config.New()
	if err != nil {
		panic(err)
	}

	dummyEndpoints := []config.Endpoint{
		{
			Name:   "node_name",
			Url:    "node_url",
			Weight: 1,
		},
	}
	cfg.Chain.Client = config.ClientConfig{
		Master: config.JSONRPCConfig{
			EndpointGroup: config.EndpointGroup{
				Endpoints: dummyEndpoints,
			},
		},
		Slave: config.JSONRPCConfig{
			EndpointGroup: config.EndpointGroup{
				Endpoints: dummyEndpoints,
			},
		},
		Retry: config.ClientRetryConfig{MaxAttempts: maxAttempts},
	}

	return testapp.WithConfig(cfg)
}
