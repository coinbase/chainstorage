package ethereum

import (
	"github.com/coinbase/chainstorage/internal/blockchain/client/internal"
	"github.com/coinbase/chainstorage/internal/blockchain/parser/ethereum/types"
)

const (
	// Before changing this constant, make sure to update the SLA config.
	baseCommitmentLevel = types.CommitmentLevelLatest
)

func NewBaseClientFactory(params internal.JsonrpcClientParams) internal.ClientFactory {
	// Base shares the same data schema as Ethereum since it is an internal EVM chain.
	return NewEthereumClientFactory(params, WithEthereumCommitmentLevel(baseCommitmentLevel))
}
