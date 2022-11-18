package client

func NewAvacchainClientFactory(params EthereumClientParams) ClientFactory {
	// Avalanche shares the same data schema as Ethereum since it is an EVM chain.
	return NewEthereumClientFactory(params)
}
