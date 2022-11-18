package parser

func NewAvacchainNativeParser(params ParserParams, opts ...ParserFactoryOption) (NativeParser, error) {
	// Avalanche shares the same data schema as Ethereum since its an EVM chain.
	return NewEthereumNativeParser(params, opts...)
}
