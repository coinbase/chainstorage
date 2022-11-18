package common

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNetworkEnumGetName(t *testing.T) {
	var (
		assert = assert.New(t)
	)

	expectedNames := map[Network]string{
		Network_NETWORK_UNKNOWN: "unknown",

		Network_NETWORK_BITCOIN_MAINNET: "bitcoin-mainnet",
		Network_NETWORK_BITCOIN_TESTNET: "bitcoin-testnet",

		Network_NETWORK_ETHEREUM_MAINNET: "ethereum-mainnet",
		Network_NETWORK_ETHEREUM_TESTNET: "ethereum-testnet",
		Network_NETWORK_ETHEREUM_GOERLI:  "ethereum-goerli",

		Network_NETWORK_SOLANA_MAINNET: "solana-mainnet",
		Network_NETWORK_SOLANA_TESTNET: "solana-testnet",
	}

	for network, name := range expectedNames {
		assert.Equal(name, network.GetName())
	}
}
