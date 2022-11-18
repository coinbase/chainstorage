package common

import (
	"strconv"
	"strings"
)

const blockchainPrefix = "BLOCKCHAIN_"

// GetName returns the canonical blockchain name without the added prefix.
func (x Blockchain) GetName() string {
	fullName := EnumName(Blockchain_name, int32(x))

	return strings.ToLower(strings.Replace(fullName, blockchainPrefix, "", 1))
}

// EnumName returns the string value of the enum based on the given int
func EnumName(m map[int32]string, v int32) string {
	s, ok := m[v]
	if ok {
		return s
	}
	return strconv.Itoa(int(v))
}
