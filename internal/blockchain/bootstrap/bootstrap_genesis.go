package bootstrap

import (
	"fmt"
	"strings"

	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/utils/fixtures"
	"github.com/coinbase/chainstorage/protos/coinbase/c3/common"
)

type GenesisAllocation struct {
	AccountIdentifier AccountIdentifier `json:"account_identifier"`
	Currency          Currency          `json:"currency"`
	Value             string            `json:"value"`
}

type AccountIdentifier struct {
	Address string `json:"address"`
}

type Currency struct {
	Symbol   string `json:"symbol"`
	Decimals uint32 `json:"decimals"`
}

// GenerateGenesisAllocations creates the bootstrap allocations
// for a particular genesis file.
func GenerateGenesisAllocations(network common.Network) ([]*GenesisAllocation, error) {
	if network.GetName() == "" {
		return nil, xerrors.New("network name is empty")
	}
	chainInfo := strings.Split(network.GetName(), "-")
	filePath := fmt.Sprintf("genesis_files/%s/%s.json", chainInfo[0], chainInfo[1])

	var genesisAllocations []*GenesisAllocation
	fixtures.MustUnmarshalJSON(filePath, &genesisAllocations)

	return genesisAllocations, nil
}
