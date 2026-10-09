package bitcoin

import (
	"time"

	"github.com/go-playground/validator/v10"

	"github.com/coinbase/chainstorage/internal/blockchain/client/internal"
	"github.com/coinbase/chainstorage/internal/blockchain/jsonrpc"
	"github.com/coinbase/chainstorage/internal/utils/log"
)

func NewZcashClientFactory(params internal.JsonrpcClientParams) internal.ClientFactory {
	return internal.NewJsonrpcClientFactory(params, func(client jsonrpc.Client) internal.Client {
		logger := log.WithPackage(params.Logger)
		return &bitcoinClient{
			config:   params.Config,
			logger:   logger,
			client:   client,
			validate: validator.New(),
			methods: newRPCMethods(
				rpcMethodsOverride{rpcMethodGetRawTransaction: &jsonrpc.RequestMethod{Name: defaultGetRawTransaction.Name, Timeout: 10 * time.Second}},
				rpcMethodsOverrideFromConfig(params.Config),
			),
			preserveRawInputTransactions: true,
			getRawTxParams: func(txid string, _ string) jsonrpc.Params {
				return jsonrpc.Params{txid, 1}
			},
		}
	})
}
