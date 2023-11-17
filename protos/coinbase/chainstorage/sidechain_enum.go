package chainstorage

import (
	"strconv"
	"strings"
)

const sidechainPrefix = "SIDECHAIN_"

// GetName returns the canonical sidechain name without the added prefix.
func (n SideChain) GetName() string {
	fullName := EnumName(SideChain_name, int32(n))

	sidechainName := strings.Replace(fullName, sidechainPrefix, "", 1)

	return strings.ToLower(strings.ReplaceAll(sidechainName, "_", "-"))
}

// EnumName returns the string value of the enum based on the given int
func EnumName(m map[int32]string, v int32) string {
	s, ok := m[v]
	if ok {
		return s
	}
	return strconv.Itoa(int(v))
}
