package client

func NewPolygonClientFactory(params EthereumClientParams) ClientFactory {
	// Polygon shares the same data schema as Ethereum.
	return NewEthereumClientFactory(params)
}
