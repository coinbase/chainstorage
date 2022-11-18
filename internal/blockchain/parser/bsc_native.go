package parser

func NewBscNativeParser(params ParserParams, opts ...ParserFactoryOption) (NativeParser, error) {
	// BSC shares the same data schema as Ethereums.
	return NewEthereumNativeParser(params, opts...)
}
