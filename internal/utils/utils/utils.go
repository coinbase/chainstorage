package utils

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	"golang.org/x/xerrors"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/coinbase/chainstorage/protos/coinbase/c3/common"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

func ParseCompression(compression string) (api.Compression, error) {
	if compression == "" {
		return api.Compression_NONE, nil
	}

	compression = strings.ToUpper(compression)
	parsedCompression, ok := api.Compression_value[compression]
	if !ok {
		return api.Compression_NONE, xerrors.Errorf("failed to parse compression type %v", compression)
	}
	return api.Compression(parsedCompression), nil
}

func ToTimestamp(seconds int64) *timestamppb.Timestamp {
	if seconds == 0 {
		return nil
	}

	return &timestamppb.Timestamp{
		Seconds: seconds,
	}
}

func SinceTimestamp(timestamp *timestamppb.Timestamp) time.Duration {
	var res time.Duration
	if timestamp.GetSeconds() > 0 || timestamp.GetNanos() > 0 {
		if t := timestamp.AsTime(); !t.IsZero() {
			res = time.Since(t)
		}
	}

	return res
}

// GenerateSha256HashString A hash function to obfuscate the input string.
// This is to prevent the input from being leaked to the public.
func GenerateSha256HashString(input string) string {
	hash := sha256.Sum256([]byte(input))
	return hex.EncodeToString(hash[:])
}

// ParseBlockchain converts a blockchain name, e.g. `ethereum`, to the proto definition,
// e.g. `common.Blockchain_BLOCKCHAIN_ETHEREUM`.
func ParseBlockchain(blockchainName string) (common.Blockchain, error) {
	formattedBlockchainName := fmt.Sprintf(
		"BLOCKCHAIN_%s",
		strings.ToUpper(blockchainName),
	)
	parsedBlockchain, ok := common.Blockchain_value[formattedBlockchainName]
	if !ok {
		return common.Blockchain_BLOCKCHAIN_UNKNOWN,
			xerrors.Errorf("error blockchain name: `%s` did not parse correctly to an enum", blockchainName)
	}

	return common.Blockchain(parsedBlockchain), nil
}

// ParseNetwork converts a network name, e.g. `stellar-testnet`, to the proto definition,
// e.g. `common.Network_NETWORK_STELLAR_TESTNET`
func ParseNetwork(networkName string) (common.Network, error) {
	formattedNetworkName := fmt.Sprintf(
		"NETWORK_%s",
		strings.Replace(strings.ToUpper(networkName), "-", "_", 1),
	)
	parsedNetwork, ok := common.Network_value[formattedNetworkName]
	if !ok {
		return common.Network_NETWORK_UNKNOWN,
			xerrors.Errorf("error network name: `%s` did not parse correctly to an enum", networkName)
	}

	return common.Network(parsedNetwork), nil
}

// ParseSidechain converts a sidechain name, e.g. `ethereum-mainnet-beacon`, to the proto definition,
// e.g. `api.SideChain_SIDECHAIN_ETHEREUM_MAINNET_BEACON`
func ParseSidechain(sidechainName string) (api.SideChain, error) {
	formattedSidechainName := fmt.Sprintf(
		"SIDECHAIN_%s",
		strings.Replace(strings.ToUpper(sidechainName), "-", "_", 2),
	)
	parsedSidechain, ok := api.SideChain_value[formattedSidechainName]
	if !ok {
		return api.SideChain_SIDECHAIN_NONE,
			fmt.Errorf("error sidechain name: `%s` did not parse correctly to an enum", sidechainName)
	}

	return api.SideChain(parsedSidechain), nil
}
