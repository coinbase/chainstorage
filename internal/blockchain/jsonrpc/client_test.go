package jsonrpc_test

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

	"github.com/coinbase/chainstorage/internal/blockchain/jsonrpc"
	jsonrpcmocks "github.com/coinbase/chainstorage/internal/blockchain/jsonrpc/mocks"
	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/utils/retry"
	"github.com/coinbase/chainstorage/internal/utils/testapp"
	"github.com/coinbase/chainstorage/internal/utils/testutil"
)

type Block struct {
	Hash string `json:"hash"`
}

type clientParams struct {
	fx.In
	Master jsonrpc.Client `name:"master"`
	Slave  jsonrpc.Client `name:"slave"`
}

func TestCall(t *testing.T) {
	require := testutil.Require(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	httpClient := jsonrpcmocks.NewMockHTTPClient(ctrl)
	body := ioutil.NopCloser(strings.NewReader(`{"jsonrpc":"2.0","id":0,"result":{"hash": "0xabcd"}}`))
	httpResponse := &http.Response{
		StatusCode: http.StatusOK,
		Body:       body,
	}
	httpClient.EXPECT().Do(gomock.Any()).Return(httpResponse, nil)

	var params clientParams
	app := testapp.New(
		t,
		withDummyEndpoints(),
		fx.Provide(jsonrpc.New),
		fx.Provide(func() jsonrpc.HTTPClient {
			return httpClient
		}),
		fx.Populate(&params),
	)
	defer app.Close()

	client := params.Master
	require.NotNil(client)
	response, err := client.Call(context.Background(),
		&jsonrpc.RequestMethod{Name: "hello", Timeout: time.Duration(5)},
		jsonrpc.Params{
			"0x1234",
			true,
		})
	require.NoError(err)

	var block Block
	err = response.Unmarshal(&block)
	require.NoError(err)
	require.Equal("0xabcd", block.Hash)
}

func TestCall_HTTPError(t *testing.T) {
	require := testutil.Require(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	httpClient := jsonrpcmocks.NewMockHTTPClient(ctrl)
	body := ioutil.NopCloser(strings.NewReader(`an unexpected error occurred`))
	httpResponse := &http.Response{
		StatusCode: http.StatusBadRequest,
		Body:       body,
	}
	httpClient.EXPECT().Do(gomock.Any()).Return(httpResponse, nil)

	var params clientParams
	app := testapp.New(
		t,
		withDummyEndpoints(),
		fx.Provide(jsonrpc.New),
		fx.Provide(func() jsonrpc.HTTPClient {
			return httpClient
		}),
		fx.Populate(&params),
	)
	defer app.Close()

	client := params.Master
	require.NotNil(client)
	_, err := client.Call(context.Background(),
		&jsonrpc.RequestMethod{Name: "hello", Timeout: time.Duration(5)},
		jsonrpc.Params{
			"0x1234",
			true,
		})
	require.Error(err)
	require.Contains(err.Error(), "method=&{hello 5ns}")
	require.Contains(err.Error(), "params=[0x1234 true]")
	require.Contains(err.Error(), "endpoint=node_name")

	var errHTTP *jsonrpc.HTTPError
	require.True(errors.As(err, &errHTTP))
	require.Equal(400, errHTTP.Code)
	require.Equal("an unexpected error occurred", errHTTP.Response)
}

func TestCall_RPCError(t *testing.T) {
	require := testutil.Require(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	httpClient := jsonrpcmocks.NewMockHTTPClient(ctrl)
	body := ioutil.NopCloser(strings.NewReader(`{"jsonrpc":"2.0","id":0,"error":{"code": 8, "message": "an unexpected error occurred"}}`))
	httpResponse := &http.Response{
		StatusCode: http.StatusOK,
		Body:       body,
	}
	httpClient.EXPECT().Do(gomock.Any()).Return(httpResponse, nil)

	var params clientParams
	app := testapp.New(
		t,
		withDummyEndpoints(),
		fx.Provide(jsonrpc.New),
		fx.Provide(func() jsonrpc.HTTPClient {
			return httpClient
		}),
		fx.Populate(&params),
	)
	defer app.Close()

	client := params.Master
	require.NotNil(client)
	_, err := client.Call(context.Background(),
		&jsonrpc.RequestMethod{Name: "hello", Timeout: time.Duration(5)},
		jsonrpc.Params{
			"0x1234",
			true,
		})
	require.Error(err)
	require.Contains(err.Error(), "method=&{hello 5ns}")
	require.Contains(err.Error(), "params=[0x1234 true]")
	require.Contains(err.Error(), "endpoint=node_name")

	var errRPC *jsonrpc.RPCError
	require.True(errors.As(err, &errRPC))
	require.Equal(8, errRPC.Code)
	require.Equal("an unexpected error occurred", errRPC.Message)
}

func TestCall_AllowsRPCError(t *testing.T) {
	require := testutil.Require(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	httpClient := jsonrpcmocks.NewMockHTTPClient(ctrl)
	body := ioutil.NopCloser(strings.NewReader(`{"jsonrpc":"2.0","id":0,"error":{"code": 8, "message": "an unexpected error occurred"}}`))
	httpResponse := &http.Response{
		StatusCode: http.StatusOK,
		Body:       body,
	}
	httpClient.EXPECT().Do(gomock.Any()).Return(httpResponse, nil)

	var params clientParams
	app := testapp.New(
		t,
		withDummyEndpoints(),
		fx.Provide(jsonrpc.New),
		fx.Provide(func() jsonrpc.HTTPClient {
			return httpClient
		}),
		fx.Populate(&params),
	)
	defer app.Close()

	client := params.Master
	require.NotNil(client)
	response, err := client.Call(context.Background(),
		&jsonrpc.RequestMethod{Name: "hello", Timeout: time.Duration(5)},
		jsonrpc.Params{
			"0x1234",
			true,
		},
		jsonrpc.WithAllowsRPCError(),
	)
	require.NoError(err)
	var errRPC *jsonrpc.RPCError
	require.True(errors.As(response.Error, &errRPC))
	require.Equal(8, errRPC.Code)
	require.Equal("an unexpected error occurred", errRPC.Message)
}

func TestCall_RPCError_StatusNotOK(t *testing.T) {
	require := testutil.Require(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	httpClient := jsonrpcmocks.NewMockHTTPClient(ctrl)
	httpClient.EXPECT().Do(gomock.Any()).DoAndReturn(func(_ *http.Request) (*http.Response, error) {
		body := ioutil.NopCloser(strings.NewReader(`{"jsonrpc":"2.0","id":0,"error":{"code": 8, "message": "an unexpected error occurred"}}`))
		return &http.Response{
			StatusCode: http.StatusInternalServerError,
			Body:       body,
		}, nil
	}).Times(retry.DefaultMaxAttempts)

	var params clientParams
	app := testapp.New(
		t,
		withDummyEndpoints(),
		fx.Provide(jsonrpc.New),
		fx.Provide(func() jsonrpc.HTTPClient {
			return httpClient
		}),
		fx.Populate(&params),
	)
	defer app.Close()

	client := params.Master
	require.NotNil(client)
	result, err := client.Call(context.Background(),
		&jsonrpc.RequestMethod{Name: "hello", Timeout: time.Duration(5)},
		jsonrpc.Params{
			"0x1234",
			true,
		})
	require.Error(err)
	require.Nil(result)

	var errRPC *jsonrpc.RPCError
	require.True(errors.As(err, &errRPC))
	require.Equal(8, errRPC.Code)
	require.Equal("an unexpected error occurred", errRPC.Message)
}

func TestCall_RPCError_TooManyRequests(t *testing.T) {
	require := testutil.Require(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	httpClient := jsonrpcmocks.NewMockHTTPClient(ctrl)
	httpClient.EXPECT().Do(gomock.Any()).DoAndReturn(func(_ *http.Request) (*http.Response, error) {
		body := ioutil.NopCloser(strings.NewReader(`{"jsonrpc":"2.0","id":0,"error":{"code": 429, "message": "too many requests"}}`))
		return &http.Response{
			StatusCode: http.StatusTooManyRequests,
			Body:       body,
		}, nil
	}).Times(retry.DefaultMaxAttempts)

	var params clientParams
	app := testapp.New(
		t,
		withDummyEndpoints(),
		fx.Provide(jsonrpc.New),
		fx.Provide(func() jsonrpc.HTTPClient {
			return httpClient
		}),
		fx.Populate(&params),
	)
	defer app.Close()

	client := params.Master
	require.NotNil(client)
	result, err := client.Call(context.Background(),
		&jsonrpc.RequestMethod{Name: "hello", Timeout: time.Duration(5)},
		jsonrpc.Params{
			"0x1234",
			true,
		})
	require.Error(err)
	require.Nil(result)

	var errRPC *jsonrpc.RPCError
	require.True(errors.As(err, &errRPC))
	require.Equal(429, errRPC.Code)
	require.Equal("too many requests", errRPC.Message)
}

func TestCall_RPCError_429_SucceededAfterRetries(t *testing.T) {
	require := testutil.Require(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	httpClient := jsonrpcmocks.NewMockHTTPClient(ctrl)
	failedResp := &http.Response{
		StatusCode: http.StatusTooManyRequests,
		Body:       ioutil.NopCloser(strings.NewReader(`{"jsonrpc":"2.0","id":0,"error":{"code": 429, "message": "too many requests"}}`)),
	}
	successfulResp := &http.Response{
		StatusCode: http.StatusOK,
		Body:       ioutil.NopCloser(strings.NewReader(`{"jsonrpc":"2.0","id":0,"result":{"hash": "0xabcd"}}`)),
	}
	gomock.InOrder(
		httpClient.EXPECT().Do(gomock.Any()).Return(failedResp, nil),
		httpClient.EXPECT().Do(gomock.Any()).Return(successfulResp, nil),
	)

	var params clientParams
	app := testapp.New(
		t,
		withDummyEndpoints(),
		fx.Provide(jsonrpc.New),
		fx.Provide(func() jsonrpc.HTTPClient {
			return httpClient
		}),
		fx.Populate(&params),
	)
	defer app.Close()

	client := params.Master
	require.NotNil(client)
	result, err := client.Call(context.Background(),
		&jsonrpc.RequestMethod{Name: "hello", Timeout: time.Duration(5)},
		jsonrpc.Params{
			"0x1234",
			true,
		})
	require.NoError(err)
	require.NotNil(result)
}

func TestCall_RPCError_StatusNotOK_WithCustomizedAttempts(t *testing.T) {
	require := testutil.Require(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	httpClient := jsonrpcmocks.NewMockHTTPClient(ctrl)
	maxAttempts := 4
	httpClient.EXPECT().Do(gomock.Any()).DoAndReturn(func(_ *http.Request) (*http.Response, error) {
		body := ioutil.NopCloser(strings.NewReader(`{"jsonrpc":"2.0","id":0,"error":{"code": 8, "message": "an unexpected error occurred"}}`))
		return &http.Response{
			StatusCode: http.StatusInternalServerError,
			Body:       body,
		}, nil
	}).Times(maxAttempts)

	var params clientParams
	app := testapp.New(
		t,
		withRetryMaxAttempts(maxAttempts),
		fx.Provide(jsonrpc.New),
		fx.Provide(func() jsonrpc.HTTPClient {
			return httpClient
		}),
		fx.Populate(&params),
	)
	defer app.Close()

	client := params.Master
	require.NotNil(client)
	result, err := client.Call(context.Background(),
		&jsonrpc.RequestMethod{Name: "hello", Timeout: time.Duration(5)},
		jsonrpc.Params{
			"0x1234",
			true,
		})
	require.Error(err)
	require.Nil(result)

	var errRPC *jsonrpc.RPCError
	require.True(errors.As(err, &errRPC))
	require.Equal(8, errRPC.Code)
	require.Equal("an unexpected error occurred", errRPC.Message)
}

func TestCall_URLError(t *testing.T) {
	require := testutil.Require(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	httpClient := jsonrpcmocks.NewMockHTTPClient(ctrl)
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
		fx.Provide(jsonrpc.New),
		fx.Provide(func() jsonrpc.HTTPClient {
			return httpClient
		}),
		fx.Populate(&params),
	)
	defer app.Close()

	client := params.Master
	require.NotNil(client)
	result, err := client.Call(context.Background(),
		&jsonrpc.RequestMethod{Name: "hello", Timeout: time.Duration(5)},
		jsonrpc.Params{
			"0x1234",
			true,
		})
	require.Error(err)
	require.Nil(result)

	var uerr *url.Error
	require.False(errors.As(err, &uerr))
	errMsg := errors.Unwrap(err).Error()
	require.Contains(errMsg, "a test error")
	require.NotContains(errMsg, "foo.com")
}

func TestBatchCall(t *testing.T) {
	require := testutil.Require(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	httpClient := jsonrpcmocks.NewMockHTTPClient(ctrl)
	body := ioutil.NopCloser(strings.NewReader(`
		[
			{"jsonrpc":"2.0","id":0,"result":{"hash": "0xabcd"}},
			{"jsonrpc":"2.0","id":2,"result":{"hash": "0xabcf"}},
			{"jsonrpc":"2.0","id":1,"result":{"hash": "0xabce"}}
		]`))
	httpResponse := &http.Response{
		StatusCode: http.StatusOK,
		Body:       body,
	}
	httpClient.EXPECT().Do(gomock.Any()).Return(httpResponse, nil)

	var params clientParams
	app := testapp.New(
		t,
		withDummyEndpoints(),
		fx.Provide(jsonrpc.New),
		fx.Provide(func() jsonrpc.HTTPClient {
			return httpClient
		}),
		fx.Populate(&params),
	)
	defer app.Close()

	client := params.Master
	require.NotNil(client)
	batchParams := []jsonrpc.Params{
		{"0x1234"},
		{"0x1235"},
		{"0x1236"},
	}
	batchResponse, err := client.BatchCall(context.Background(),
		&jsonrpc.RequestMethod{Name: "hello", Timeout: time.Duration(5)},
		batchParams)
	require.NoError(err)

	// Though the response is out of order, BatchCall is expected to return them in ID order.
	expectedHashes := []string{"0xabcd", "0xabce", "0xabcf"}
	for i, response := range batchResponse {
		var block Block
		err = response.Unmarshal(&block)
		require.NoError(err)
		require.Equal(expectedHashes[i], block.Hash)
	}
}

func TestBatchCall_RPCError(t *testing.T) {
	require := testutil.Require(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	httpClient := jsonrpcmocks.NewMockHTTPClient(ctrl)
	body := ioutil.NopCloser(strings.NewReader(`
		[
			{"jsonrpc":"2.0","id":0,"result":{"hash": "0xabcd"}},
			{"jsonrpc":"2.0","id":2,"result":{"hash": "0xabcf"}},
			{"jsonrpc":"2.0","id":1,"error":{"code": -3, "message": "an unexpected error occurred"}}
		]`))
	httpResponse := &http.Response{
		StatusCode: http.StatusOK,
		Body:       body,
	}
	httpClient.EXPECT().Do(gomock.Any()).Return(httpResponse, nil)

	var params clientParams
	app := testapp.New(
		t,
		withDummyEndpoints(),
		fx.Provide(jsonrpc.New),
		fx.Provide(func() jsonrpc.HTTPClient {
			return httpClient
		}),
		fx.Populate(&params),
	)
	defer app.Close()

	client := params.Master
	require.NotNil(client)
	batchParams := []jsonrpc.Params{
		{"0x1234"},
		{"0x1235"},
		{"0x1236"},
	}
	_, err := client.BatchCall(context.Background(),
		&jsonrpc.RequestMethod{Name: "hello", Timeout: time.Duration(5)},
		batchParams)
	require.Error(err)

	var errRPC *jsonrpc.RPCError
	require.True(errors.As(err, &errRPC))
	require.Equal(-3, errRPC.Code)
	require.Equal("an unexpected error occurred", errRPC.Message)
}

func TestBatchCall_AllowsRPCError(t *testing.T) {
	require := testutil.Require(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	httpClient := jsonrpcmocks.NewMockHTTPClient(ctrl)
	body := ioutil.NopCloser(strings.NewReader(`
		[
			{"jsonrpc":"2.0","id":0,"result":{"hash": "0xabcd"}},
			{"jsonrpc":"2.0","id":2,"result":{"hash": "0xabcf"}},
			{"jsonrpc":"2.0","id":1,"error":{"code": -3, "message": "an unexpected error occurred"}}
		]`))
	httpResponse := &http.Response{
		StatusCode: http.StatusOK,
		Body:       body,
	}
	httpClient.EXPECT().Do(gomock.Any()).Return(httpResponse, nil)

	var params clientParams
	app := testapp.New(
		t,
		withDummyEndpoints(),
		fx.Provide(jsonrpc.New),
		fx.Provide(func() jsonrpc.HTTPClient {
			return httpClient
		}),
		fx.Populate(&params),
	)
	defer app.Close()

	client := params.Master
	require.NotNil(client)
	batchParams := []jsonrpc.Params{
		{"0x1234"},
		{"0x1235"},
		{"0x1236"},
	}
	responses, err := client.BatchCall(context.Background(),
		&jsonrpc.RequestMethod{Name: "hello", Timeout: time.Duration(5)},
		batchParams,
		jsonrpc.WithAllowsRPCError(),
	)
	require.NoError(err)

	require.Nil(responses[0].Error)
	require.NotNil(responses[1].Error)
	require.Nil(responses[2].Error)

	var errRPC *jsonrpc.RPCError
	require.True(errors.As(responses[1].Error, &errRPC))
	require.Equal(-3, errRPC.Code)
	require.Equal("an unexpected error occurred", errRPC.Message)
}

func TestBatchCall_WrongNumberOfResponses(t *testing.T) {
	require := testutil.Require(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	httpClient := jsonrpcmocks.NewMockHTTPClient(ctrl)
	body := ioutil.NopCloser(strings.NewReader(`
		[
			{"jsonrpc":"2.0","id":0,"result":{"hash": "0xabcd"}},
			{"jsonrpc":"2.0","id":2,"result":{"hash": "0xabcf"}}
		]`))
	httpResponse := &http.Response{
		StatusCode: http.StatusOK,
		Body:       body,
	}
	httpClient.EXPECT().Do(gomock.Any()).Return(httpResponse, nil)

	var params clientParams
	app := testapp.New(
		t,
		withDummyEndpoints(),
		fx.Provide(jsonrpc.New),
		fx.Provide(func() jsonrpc.HTTPClient {
			return httpClient
		}),
		fx.Populate(&params),
	)
	defer app.Close()

	client := params.Master
	require.NotNil(client)
	batchParams := []jsonrpc.Params{
		{"0x1234"},
		{"0x1235"},
		{"0x1236"},
	}
	_, err := client.BatchCall(context.Background(),
		&jsonrpc.RequestMethod{Name: "hello", Timeout: time.Duration(5)},
		batchParams)
	require.Error(err)
	require.Contains(err.Error(), "received wrong number of responses")
	require.Contains(err.Error(), "want=3, got=2")
}

func TestBatchCall_DuplicateID(t *testing.T) {
	require := testutil.Require(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	httpClient := jsonrpcmocks.NewMockHTTPClient(ctrl)
	body := ioutil.NopCloser(strings.NewReader(`
		[
			{"jsonrpc":"2.0","id":0,"result":{"hash": "0xabcd"}},
			{"jsonrpc":"2.0","id":1,"result":{"hash": "0xabcf"}},
			{"jsonrpc":"2.0","id":1,"result":{"hash": "0xabce"}}
		]`))
	httpResponse := &http.Response{
		StatusCode: http.StatusOK,
		Body:       body,
	}
	httpClient.EXPECT().Do(gomock.Any()).Return(httpResponse, nil)

	var params clientParams
	app := testapp.New(
		t,
		withDummyEndpoints(),
		fx.Provide(jsonrpc.New),
		fx.Provide(func() jsonrpc.HTTPClient {
			return httpClient
		}),
		fx.Populate(&params),
	)
	defer app.Close()

	client := params.Master
	require.NotNil(client)
	batchParams := []jsonrpc.Params{
		{"0x1234"},
		{"0x1235"},
		{"0x1236"},
	}
	_, err := client.BatchCall(context.Background(),
		&jsonrpc.RequestMethod{Name: "hello", Timeout: time.Duration(5)},
		batchParams)
	require.Error(err)
	require.Contains(err.Error(), "missing response")
}

func TestBatchCall_RetryNullResponse(t *testing.T) {
	require := testutil.Require(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	httpClient := jsonrpcmocks.NewMockHTTPClient(ctrl)

	attempts := 0
	httpClient.EXPECT().Do(gomock.Any()).Times(retry.DefaultMaxAttempts).
		DoAndReturn(func(req *http.Request) (*http.Response, error) {
			attempts += 1
			body := `
			[
				{"jsonrpc":"2.0","id":0,"result":{"hash": "0xabcd"}},
				{"jsonrpc":"2.0","id":1,"result":{"hash": "0xabce"}},
				{"jsonrpc":"2.0","id":2,"result":{"hash": "0xabcf"}}
			]`
			if attempts < retry.DefaultMaxAttempts {
				// Return a null response except for the last retry attempt.
				body = `
				[
					{"jsonrpc":"2.0","id":0,"result":{"hash": "0xabcd"}},
					{"jsonrpc":"2.0","id":1,"result":{"hash": "0xabce"}},
					{"jsonrpc":"2.0","id":2,"result":null}
				]`
			}

			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       ioutil.NopCloser(strings.NewReader(body)),
			}, nil
		})

	var params clientParams
	app := testapp.New(
		t,
		withDummyEndpoints(),
		fx.Provide(jsonrpc.New),
		fx.Provide(func() jsonrpc.HTTPClient {
			return httpClient
		}),
		fx.Populate(&params),
	)
	defer app.Close()

	client := params.Master
	require.NotNil(client)
	batchParams := []jsonrpc.Params{
		{"0x1234"},
		{"0x1235"},
		{"0x1236"},
	}
	batchResponse, err := client.BatchCall(context.Background(),
		&jsonrpc.RequestMethod{Name: "hello", Timeout: time.Duration(5)},
		batchParams)
	require.NoError(err)
	require.Equal(retry.DefaultMaxAttempts, attempts)

	expectedHashes := []string{"0xabcd", "0xabce", "0xabcf"}
	for i, response := range batchResponse {
		var block Block
		err = response.Unmarshal(&block)
		require.NoError(err)
		require.Equal(expectedHashes[i], block.Hash)
	}
}

func TestBatchCall_URLError(t *testing.T) {
	require := testutil.Require(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	httpClient := jsonrpcmocks.NewMockHTTPClient(ctrl)
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
		fx.Provide(jsonrpc.New),
		fx.Provide(func() jsonrpc.HTTPClient {
			return httpClient
		}),
		fx.Populate(&params),
	)
	defer app.Close()

	client := params.Master
	require.NotNil(client)
	batchParams := []jsonrpc.Params{
		{"0x1234"},
		{"0x1235"},
		{"0x1236"},
	}
	_, err := client.BatchCall(context.Background(),
		&jsonrpc.RequestMethod{Name: "hello", Timeout: time.Duration(5)},
		batchParams)
	require.Error(err)

	var uerr *url.Error
	require.False(errors.As(err, &uerr))
	errMsg := errors.Unwrap(err).Error()
	require.Contains(errMsg, "a test error")
	require.NotContains(errMsg, "foo.com")
}

func TestNullResponse(t *testing.T) {
	tests := []struct {
		name     string
		expected bool
		result   json.RawMessage
	}{
		{
			name:     "normal",
			expected: false,
			result:   json.RawMessage(`{"foo": "bar"}`),
		},
		{
			name:     "empty",
			expected: true,
			result:   json.RawMessage("{}"),
		},
		{
			name:     "zero",
			expected: true,
			result:   json.RawMessage{},
		},
		{
			name:     "null",
			expected: true,
			result:   json.RawMessage("null"),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require := testutil.Require(t)
			response := jsonrpc.Response{Result: test.result}
			require.Equal(test.expected, jsonrpc.IsNullOrEmpty(response.Result))
		})
	}
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
