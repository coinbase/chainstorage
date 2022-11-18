package parser

func NewPolygonNativeParser(params ParserParams, opts ...ParserFactoryOption) (NativeParser, error) {
	// Polygon shares the same data schema as Ethereum
	return NewEthereumNativeParser(params, opts...)
}
