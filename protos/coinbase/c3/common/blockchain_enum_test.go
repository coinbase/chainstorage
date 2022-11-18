package common

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBlockchainEnumGetName(t *testing.T) {
	assert := assert.New(t)

	expectedNames := map[Blockchain]string{
		Blockchain_BLOCKCHAIN_UNKNOWN:  "unknown",
		Blockchain_BLOCKCHAIN_BITCOIN:  "bitcoin",
		Blockchain_BLOCKCHAIN_ETHEREUM: "ethereum",
		Blockchain_BLOCKCHAIN_SOLANA:   "solana",
	}

	for blockchain, name := range expectedNames {
		assert.Equal(name, blockchain.GetName())
	}
}
