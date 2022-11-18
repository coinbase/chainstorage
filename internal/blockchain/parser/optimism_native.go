package parser

func NewOptimismNativeParser(params ParserParams, opts ...ParserFactoryOption) (NativeParser, error) {
	// Optimism shares the same data schema as Ethereum since its an EVM chain.
	return NewEthereumNativeParser(params, opts...)
}
