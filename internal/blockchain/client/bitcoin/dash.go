package bitcoin

import (
	"github.com/go-playground/validator/v10"

	"github.com/coinbase/chainstorage/internal/blockchain/client/internal"
	"github.com/coinbase/chainstorage/internal/blockchain/jsonrpc"
	"github.com/coinbase/chainstorage/internal/utils/log"
)

func NewDashClientFactory(params internal.JsonrpcClientParams) internal.ClientFactory {
	return internal.NewJsonrpcClientFactory(params, func(client jsonrpc.Client) internal.Client {
		logger := log.WithPackage(params.Logger)
		return &bitcoinClient{
			config:                       params.Config,
			logger:                       logger,
			client:                       client,
			validate:                     validator.New(),
			methods:                      newRPCMethods(rpcMethodsOverrideFromConfig(params.Config)),
			preserveRawInputTransactions: true,
		}
	})
}
