package parser

import (
	"context"
	"fmt"
	"math/big"
	"strings"

	"github.com/btcsuite/btcutil/base58"
	geth "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"golang.org/x/xerrors"

	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

type nopParser struct{}

const (
	ethHexAddressLength = 40
	genesisBlockIndex   = int64(0)
)

var _ Parser = (*nopParser)(nil)

func NewNop() Parser {
	return nopParser{}
}

func (p nopParser) ParseNativeBlock(ctx context.Context, rawBlock *api.Block) (*api.NativeBlock, error) {
	return &api.NativeBlock{}, nil
}

func (p nopParser) ParseRosettaBlock(ctx context.Context, rawBlock *api.Block) (*api.RosettaBlock, error) {
	return &api.RosettaBlock{}, nil
}

// ValidateChain checks if the chain is continuous.
func ValidateChain(blocks []*api.BlockMetadata, lastBlock *api.BlockMetadata) error {
	for _, currBlock := range blocks {
		if currBlock.Skipped {
			continue
		}

		// Skip the check if last block is unavailable OR parent hash is unavailable.
		if lastBlock != nil && !lastBlock.Skipped && currBlock.ParentHash != "" {
			if lastBlock.Hash != currBlock.ParentHash || lastBlock.Height != currBlock.ParentHeight {
				return xerrors.Errorf("chain is not continuous (last={%+v}, curr={%+v}): %w", lastBlock, currBlock, ErrInvalidChain)
			}
		}

		lastBlock = currBlock
	}

	return nil
}

// hexToBig converts a hex string into big integer
func hexToBig(hex string) (*big.Int, error) {
	cleanedHex, err := cleanHexString(hex)
	if err != nil {
		return nil, xerrors.Errorf("failed to clean hex string %v: %w", hex, err)
	}

	val, err := hexutil.DecodeBig(cleanedHex)
	if err != nil {
		return nil, xerrors.Errorf("failed to decode big number %v: %w", hex, err)
	}

	return val, nil
}

// cleanHexString cleans up a hex string by removing all leading 0s
func cleanHexString(hex string) (string, error) {
	var cleaned string

	if !has0xPrefix(hex) {
		return "", xerrors.Errorf("string missing 0x prefix: %v", hex)
	}

	cleaned = hex[2:]
	if len(cleaned) > 1 && cleaned[0] == '0' {
		cleaned = strings.TrimLeft(hex[2:], "0")
	}

	// if trimming all left 0s returned an empty string
	// set clean to be "0" so that `hexutil` can properly parse
	if cleaned == "" {
		cleaned = "0"
	}

	return "0x" + cleaned, nil
}

// cleanAddress cleans up an address string by removing all characters leading up to the address
func cleanAddress(address string) (string, error) {
	var cleaned string

	if len(address) < ethHexAddressLength {
		return "", xerrors.Errorf("address too short: %v", address)
	}

	cleaned = address[len(address)-ethHexAddressLength:]

	return "0x" + cleaned, nil
}

// has0xPrefix returns true if the string beings with `0x` or `0X`
func has0xPrefix(str string) bool {
	return len(str) >= 2 && str[0] == '0' && (str[1] == 'x' || str[1] == 'X')
}

// truncatedError truncates the error message if it is too long.
// Without the truncation, the error may be dropped by datadog.
type truncatedError struct {
	err error
}

var (
	_ error           = (*truncatedError)(nil)
	_ xerrors.Wrapper = (*truncatedError)(nil)
)

func newTruncatedError(err error) error {
	if err == nil {
		return nil
	}

	return &truncatedError{err: err}
}

func (e *truncatedError) Error() string {
	const maxLength = 256

	msg := e.err.Error()
	if len(msg) <= maxLength {
		return msg
	}

	return fmt.Sprintf("%v...", msg[0:maxLength])
}

func (e *truncatedError) Unwrap() error {
	return e.err
}

func encodeBase58(b []byte) string {
	return base58.Encode(b)
}

func decodeBase58(s string) []byte {
	return base58.Decode(s)
}

// bigInt returns a *big.Int representation of a value.
func bigInt(value string) (*big.Int, error) {
	parsedVal, ok := new(big.Int).SetString(value, 10)
	if !ok {
		return nil, xerrors.Errorf("%s is not an integer", value)
	}
	return parsedVal, nil
}

func checksumAddress(address string) (string, error) {
	mixedCaseAddress, err := geth.NewMixedcaseAddressFromString(address)
	if err != nil {
		return "", xerrors.Errorf("fail to normalize address=%s: %w", address, err)
	}
	return mixedCaseAddress.Address().Hex(), nil
}
