package internal

import (
	"context"
	"fmt"
	"math/big"
	"strings"

	"github.com/btcsuite/btcd/btcutil/base58"
	"github.com/cockroachdb/errors"
	geth "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"

	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

type nopParser struct{}

const (
	ethHexAddressLength = 40
	GenesisBlockIndex   = int64(0)
)

var _ Parser = (*nopParser)(nil)

func NewNop() Parser {
	return nopParser{}
}

func (p nopParser) ParseNativeBlock(ctx context.Context, rawBlock *api.Block) (*api.NativeBlock, error) {
	return &api.NativeBlock{}, nil
}

func (p nopParser) GetNativeTransaction(ctx context.Context, nativeBlock *api.NativeBlock, transactionHash string) (*api.NativeTransaction, error) {
	return &api.NativeTransaction{}, nil
}

func (p nopParser) ParseRosettaBlock(ctx context.Context, rawBlock *api.Block) (*api.RosettaBlock, error) {
	return &api.RosettaBlock{}, nil
}

func (p nopParser) CompareNativeBlocks(ctx context.Context, height uint64, expectedBlock, actualBlock *api.NativeBlock) error {
	return nil
}

func (p nopParser) ValidateBlock(ctx context.Context, nativeBlock *api.NativeBlock) error {
	return nil
}

func (p nopParser) ValidateAccountState(ctx context.Context, req *api.ValidateAccountStateRequest) (*api.ValidateAccountStateResponse, error) {
	return nil, nil
}

func (p nopParser) ValidateRosettaBlock(ctx context.Context, req *api.ValidateRosettaBlockRequest, actualRosettaBlock *api.RosettaBlock) error {
	return nil
}

// ValidateChain checks if the chain is continuous.
func ValidateChain(blocks []*api.BlockMetadata, lastBlock *api.BlockMetadata) error {
	for _, currBlock := range blocks {
		if currBlock.Skipped {
			continue
		}

		// Skip the check if last block is unavailable OR parent hash is unavailable.
		// Check block height continuous only when its parent height is available.
		if lastBlock != nil && !lastBlock.Skipped && currBlock.ParentHash != "" {
			if lastBlock.Hash != currBlock.ParentHash || (currBlock.ParentHeight != 0 && lastBlock.Height != currBlock.ParentHeight) {
				return fmt.Errorf("chain is not continuous (last={%+v}, curr={%+v}): %w", lastBlock, currBlock, ErrInvalidChain)
			}
		}

		lastBlock = currBlock
	}

	return nil
}

// HexToBig converts a hex string into big integer
func HexToBig(hex string) (*big.Int, error) {
	cleanedHex, err := CleanHexString(hex)
	if err != nil {
		return nil, fmt.Errorf("failed to clean hex string %v: %w", hex, err)
	}

	val, err := hexutil.DecodeBig(cleanedHex)
	if err != nil {
		return nil, fmt.Errorf("failed to decode big number %v: %w", hex, err)
	}

	return val, nil
}

// CleanHexString cleans up a hex string by removing all leading 0s
func CleanHexString(hex string) (string, error) {
	var cleaned string

	if !Has0xPrefix(hex) {
		return "", fmt.Errorf("string missing 0x prefix: %v", hex)
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

// CleanAddress cleans up an address string by removing all characters leading up to the address
func CleanAddress(address string) (string, error) {
	var cleaned string

	if len(address) < ethHexAddressLength {
		return "", fmt.Errorf("address too short: %v", address)
	}

	cleaned = address[len(address)-ethHexAddressLength:]

	return "0x" + cleaned, nil
}

// Has0xPrefix returns true if the string beings with `0x` or `0X`
func Has0xPrefix(str string) bool {
	return len(str) >= 2 && str[0] == '0' && (str[1] == 'x' || str[1] == 'X')
}

// truncatedError truncates the error message if it is too long.
// Without the truncation, the error may be dropped by datadog.
type truncatedError struct {
	err error
}

var (
	_ error          = (*truncatedError)(nil)
	_ errors.Wrapper = (*truncatedError)(nil)
)

func NewTruncatedError(err error) error {
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

func EncodeBase58(b []byte) string {
	return base58.Encode(b)
}

func DecodeBase58(s string) []byte {
	return base58.Decode(s)
}

// bigInt returns a *big.Int representation of a value.
func BigInt(value string) (*big.Int, error) {
	parsedVal, ok := new(big.Int).SetString(value, 10)
	if !ok {
		return nil, fmt.Errorf("%s is not an integer", value)
	}
	return parsedVal, nil
}

func ChecksumAddress(address string) (string, error) {
	mixedCaseAddress, err := geth.NewMixedcaseAddressFromString(address)
	if err != nil {
		return "", fmt.Errorf("fail to normalize address=%s: %w", address, err)
	}
	return mixedCaseAddress.Address().Hex(), nil
}
