package common

import (
	"strings"
)

const networkPrefix = "NETWORK_"

// GetName returns the canonical network name without the added prefix.
func (n Network) GetName() string {
	fullName := EnumName(Network_name, int32(n))

	networkName := strings.Replace(fullName, networkPrefix, "", 1)

	return strings.ToLower(strings.ReplaceAll(networkName, "_", "-"))
}
