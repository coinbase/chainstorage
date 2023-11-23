package gateway

import (
	"context"
	"strings"
	"time"

	"google.golang.org/grpc/credentials"

	"github.com/coinbase/chainstorage/internal/utils/consts"

	grpc_retry "github.com/grpc-ecosystem/go-grpc-middleware/retry"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"golang.org/x/xerrors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/coinbase/chainstorage/internal/utils/fxparams"
	"github.com/coinbase/chainstorage/internal/utils/log"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
	"github.com/coinbase/chainstorage/sdk/services"
)

type (
	Params struct {
		fx.In
		fxparams.Params
		Lifecycle fx.Lifecycle
		Manager   services.SystemManager
		Options   []ClientOption `group:"options"`
	}

	ClientOption func(cfg *clientConfig)

	clientConfig struct {
		serverAddress string
		clientID      string
	}

	Client = api.ChainStorageClient

	// GrpcError defines the interface of an error returned by "google.golang.org/grpc/status".
	// See https://dev.to/khepin/go-1-13-errors-and-grpc-errors-1gik for more details.
	GrpcError interface {
		Error() string
		GRPCStatus() *status.Status
	}
)

const (
	sendMsgSize                    = 1024 * 1024       // 1 MB
	recvMsgSize                    = 1024 * 1024 * 100 // 100 MB
	maxRetries                     = 5
	backoffScalar                  = 500 * time.Millisecond
	backoffJitter                  = 0.2
	resourceExhaustedBackoffScalar = 1000 * time.Millisecond
	resourceExhaustedBackoffJitter = 0.5
)

var (
	// Note: codes.ResourceExhausted is also a retryable code and we handled it in a different retrying intercepter.
	defaultRetryableCodesMap = map[codes.Code]bool{
		codes.Unavailable:      true,
		codes.Internal:         true,
		codes.DeadlineExceeded: true,
		codes.Aborted:          true,
	}
)

func NewChainstorageClient(params Params) (Client, error) {
	address := params.Config.SDK.ChainstorageAddress
	authHeader := params.Config.SDK.AuthHeader
	authToken := params.Config.SDK.AuthToken
	restful := params.Config.SDK.Restful
	manager := params.Manager
	logger := log.WithPackage(manager.Logger())
	ctx := manager.ServiceContext()

	if restful {
		// Coinbase exposes the gRPC endpoints through restful interfaces.
		return newRestClient(params)
	}

	cfg := clientConfig{
		serverAddress: params.Config.SDK.ChainstorageAddress,
		clientID:      "",
	}
	for _, opt := range params.Options {
		opt(&cfg)
	}

	retryableCodes := getDefaultRetryableCodes()
	retryOpts := []grpc_retry.CallOption{
		grpc_retry.WithMax(maxRetries),
		grpc_retry.WithBackoff(grpc_retry.BackoffExponentialWithJitter(backoffScalar, backoffJitter)),
		grpc_retry.WithCodes(retryableCodes...),
	}

	// For codes.ResourceExhausted error type, add separate retry interceptors with different backoff policy to avoid server overload and potential retry failures
	resourceExhaustedRetryOpts := []grpc_retry.CallOption{
		grpc_retry.WithMax(maxRetries),
		grpc_retry.WithBackoff(grpc_retry.BackoffExponentialWithJitter(resourceExhaustedBackoffScalar, resourceExhaustedBackoffJitter)),
		grpc_retry.WithCodes(codes.ResourceExhausted),
	}

	unaryChains := []grpc.UnaryClientInterceptor{
		// XXX: Add your own interceptors here.
		unaryAuthInterceptor(authHeader, authToken),
		grpc_retry.UnaryClientInterceptor(retryOpts...),
		grpc_retry.UnaryClientInterceptor(resourceExhaustedRetryOpts...),
	}

	streamChains := []grpc.StreamClientInterceptor{
		// XXX: Add your own interceptors here.
		streamAuthInterceptor(authHeader, authToken),
	}
	if cfg.clientID != "" {
		unaryChains = append(unaryChains, unaryClientTaggingInterceptor(cfg.clientID))
		streamChains = append(streamChains, streamClientTaggingInterceptor(cfg.clientID))
	}

	opts := []grpc.DialOption{
		grpc.WithChainUnaryInterceptor(unaryChains...),
		grpc.WithChainStreamInterceptor(streamChains...),
		grpc.WithDefaultCallOptions(grpc.MaxCallSendMsgSize(sendMsgSize), grpc.MaxCallRecvMsgSize(recvMsgSize)),
	}

	if strings.HasPrefix(address, "http://") || strings.Contains(address, "localhost") {
		opts = append(opts, grpc.WithInsecure())
	} else {
		opts = append(opts, grpc.WithTransportCredentials(credentials.NewClientTLSFromCert(nil, "")))
	}

	conn, err := grpc.DialContext(ctx, address, opts...)
	if err != nil {
		return nil, xerrors.Errorf("failed to dial grpc: %w", err)
	}

	params.Lifecycle.Append(fx.Hook{
		OnStop: func(ctx context.Context) error {
			if err := conn.Close(); err != nil {
				return xerrors.Errorf("failed to close chainstorage connection: %w", err)
			}

			return nil
		},
	})

	client := api.NewChainStorageClient(conn)
	logger.Info(
		"created chainstorage client",
		zap.String("env", string(params.Config.Env())),
		zap.String("blockchain", params.Config.Chain.Blockchain.String()),
		zap.String("network", params.Config.Chain.Network.String()),
		zap.String("address", address),
		zap.String("sidechain", params.Config.Chain.Sidechain.String()),
		zap.String("address", cfg.serverAddress),
		zap.String("client_id", cfg.clientID),
	)

	return client, nil
}

func getDefaultRetryableCodes() []codes.Code {
	retryableCodes := make([]codes.Code, 0, len(defaultRetryableCodesMap))
	for code := range defaultRetryableCodesMap {
		retryableCodes = append(retryableCodes, code)
	}
	return retryableCodes
}

func IsRetryableCode(code codes.Code) bool {
	return defaultRetryableCodesMap[code] || code == codes.ResourceExhausted
}

func WithClientID(clientID string) fx.Option {
	return fx.Provide(fx.Annotated{
		Group: "options",
		Target: func() ClientOption {
			return func(cfg *clientConfig) {
				if clientID != "" {
					cfg.clientID = clientID
				}
			}
		},
	})
}

func WithServerAddress(serverAddress string) fx.Option {
	return fx.Provide(fx.Annotated{
		Group: "options",
		Target: func() ClientOption {
			return func(cfg *clientConfig) {
				if serverAddress != "" {
					cfg.serverAddress = serverAddress
				}
			}
		},
	})
}

func unaryClientTaggingInterceptor(clientID string) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		return invoker(metadata.AppendToOutgoingContext(ctx, consts.ClientIDHeader, clientID), method, req, reply, cc, opts...)
	}
}

func unaryAuthInterceptor(authHeader string, authToken string) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		ctx = getAuthContext(ctx, authHeader, authToken)
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}

func streamClientTaggingInterceptor(clientID string) grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		return streamer(metadata.AppendToOutgoingContext(ctx, consts.ClientIDHeader, clientID), desc, cc, method, opts...)
	}
}

func streamAuthInterceptor(authHeader string, authToken string) grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		ctx = getAuthContext(ctx, authHeader, authToken)
		return streamer(ctx, desc, cc, method, opts...)
	}
}

func getAuthContext(ctx context.Context, authHeader string, authToken string) context.Context {
	if authHeader == "" || authToken == "" {
		return ctx
	}

	return metadata.AppendToOutgoingContext(ctx, authHeader, authToken)
}
