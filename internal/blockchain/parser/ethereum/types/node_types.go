package types

type BlockRange struct {
	StartHeight uint64 // Inclusive
	EndHeight   uint64 // Exclusive
}

type EthereumNodeType int

const (
	EthereumNodeType_UNKNOWN EthereumNodeType = iota
	EthereumNodeType_ARCHIVAL
	EthereumNodeType_FULL
	EthereumNodeType_ALCHEMY_POLYGON
)

func (t EthereumNodeType) TracesEnabled() bool {
	return t == EthereumNodeType_ARCHIVAL || t == EthereumNodeType_ALCHEMY_POLYGON
}

func (t EthereumNodeType) ReceiptsEnabled() bool {
	return t == EthereumNodeType_ARCHIVAL || t == EthereumNodeType_ALCHEMY_POLYGON
}
