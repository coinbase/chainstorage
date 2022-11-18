package client

func NewOptimismClientFactory(params EthereumClientParams) ClientFactory {
	// Optimism shares the same data schema as Ethereum since it is an EVM chain.
	return NewEthereumClientFactory(params)
}
