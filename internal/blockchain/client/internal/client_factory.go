package internal

import (
	"go.uber.org/fx"

	"github.com/coinbase/chainstorage/internal/blockchain/jsonrpc"
	"github.com/coinbase/chainstorage/internal/blockchain/restapi"
	"github.com/coinbase/chainstorage/internal/dlq"
	"github.com/coinbase/chainstorage/internal/utils/fxparams"
)

type (
	ClientFactory interface {
		Master() Client
		Slave() Client
		Validator() Client
		Consensus() Client
	}

	JsonrpcClientParams struct {
		fx.In
		fxparams.Params
		MasterClient    jsonrpc.Client `name:"master"`
		SlaveClient     jsonrpc.Client `name:"slave"`
		ValidatorClient jsonrpc.Client `name:"validator"`
		ConsensusClient jsonrpc.Client `name:"consensus"`
		DLQ             dlq.DLQ
	}

	JsonrpcClientFactoryFn func(client jsonrpc.Client) Client

	jsonrpcClientFactory struct {
		masterClient    jsonrpc.Client
		slaveClient     jsonrpc.Client
		validatorClient jsonrpc.Client
		consensusClient jsonrpc.Client
		clientFactory   JsonrpcClientFactoryFn
	}

	RestapiClientParams struct {
		fx.In
		fxparams.Params
		MasterClient    restapi.Client `name:"master"`
		SlaveClient     restapi.Client `name:"slave"`
		ValidatorClient restapi.Client `name:"validator"`
		ConsensusClient restapi.Client `name:"consensus"`
		DLQ             dlq.DLQ
	}

	RestapiClientFactoryFn func(client restapi.Client) Client

	restapiClientFactory struct {
		masterClient    restapi.Client
		slaveClient     restapi.Client
		validatorClient restapi.Client
		consensusClient restapi.Client
		clientFactory   RestapiClientFactoryFn
	}
)

func NewJsonrpcClientFactory(params JsonrpcClientParams, clientFactory JsonrpcClientFactoryFn) ClientFactory {
	return &jsonrpcClientFactory{
		masterClient:    params.MasterClient,
		slaveClient:     params.SlaveClient,
		validatorClient: params.ValidatorClient,
		consensusClient: params.ConsensusClient,
		clientFactory:   clientFactory,
	}
}

func (f *jsonrpcClientFactory) Master() Client {
	return f.clientFactory(f.masterClient)
}

func (f *jsonrpcClientFactory) Slave() Client {
	return f.clientFactory(f.slaveClient)
}

func (f *jsonrpcClientFactory) Validator() Client {
	return f.clientFactory(f.validatorClient)
}

func (f *jsonrpcClientFactory) Consensus() Client {
	return f.clientFactory(f.consensusClient)
}

func NewRestapiClientFactory(params RestapiClientParams, clientFactory RestapiClientFactoryFn) ClientFactory {
	return &restapiClientFactory{
		masterClient:    params.MasterClient,
		slaveClient:     params.SlaveClient,
		validatorClient: params.ValidatorClient,
		consensusClient: params.ConsensusClient,
		clientFactory:   clientFactory,
	}
}

func (f *restapiClientFactory) Master() Client {
	return f.clientFactory(f.masterClient)
}

func (f *restapiClientFactory) Slave() Client {
	return f.clientFactory(f.slaveClient)
}

func (f *restapiClientFactory) Validator() Client {
	return f.clientFactory(f.validatorClient)
}

func (f *restapiClientFactory) Consensus() Client {
	return f.clientFactory(f.consensusClient)
}
