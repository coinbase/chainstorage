package internal

import (
	"encoding/hex"
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/coinbase/chainstorage/internal/utils/testutil"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

type validateChainTestSuite struct {
	suite.Suite
	require   *testutil.Assertions
	blocks    []*api.BlockMetadata
	lastBlock *api.BlockMetadata
}

func TestValidateChainTestSuite(t *testing.T) {
	suite.Run(t, new(validateChainTestSuite))
}

func (s *validateChainTestSuite) SetupTest() {
	s.require = testutil.Require(s.T())
	s.blocks = testutil.MakeBlockMetadatasFromStartHeight(1_000_000, 20, 1)
	s.lastBlock = testutil.MakeBlockMetadata(999_999, 1)
}

func (s *validateChainTestSuite) TestValidateChain_Continuous() {
	err := ValidateChain(s.blocks, nil)
	s.require.NoError(err)

	err = ValidateChain(s.blocks, s.lastBlock)
	s.require.NoError(err)
}

func (s *validateChainTestSuite) TestValidateChain_EmptyParentHash() {
	block := s.blocks[len(s.blocks)-1]
	block.ParentHash = ""

	err := ValidateChain(s.blocks, nil)
	s.require.NoError(err)
}

func (s *validateChainTestSuite) TestValidateChain_EmptyParentHashAndInconsistentParentHeight() {
	// This test case verifies the following edge case:
	// last block: https://explorer.solana.com/block/15984013
	//   {hash:"8dfmrv2YbTTUJqyvd7pfKJzjnMRnWVQCbNqF8rbjy6yj" parent_hash:"EcBGjcXR1Xs8gMbcKJs1Ffrd8kAZGTszz8ifXyj3EPpc" height:15984013 parent_height:15984012}
	// curr block: https://explorer.solana.com/block/15984075
	//   {hash:"4kM9y9ucKjfoTmjt41Qn83xpZbBUgLDAbVfySKbyUcRD" height:15984075 parent_height:15984074}
	// Notice that the parent height of the curr block is technically incorrect,
	// but we're skipping the validation given that parent hash is "11111111111111111111111111111111".
	block := s.blocks[len(s.blocks)-1]
	block.ParentHash = ""
	block.ParentHeight = 1_000_000

	err := ValidateChain(s.blocks, nil)
	s.require.NoError(err)
}

func (s *validateChainTestSuite) TestValidateChain_Skipped() {
	block := s.blocks[len(s.blocks)-1]
	block.ParentHeight = 1_000_000
	block.Skipped = true

	err := ValidateChain(s.blocks, nil)
	s.require.NoError(err)
}

func (s *validateChainTestSuite) TestValidateChain_LastBlockSkipped() {
	lastBlock := &api.BlockMetadata{
		Tag:     1,
		Height:  999_999,
		Skipped: true,
	}

	err := ValidateChain(s.blocks, lastBlock)
	s.require.NoError(err)
}

func (s *validateChainTestSuite) TestValidateChain_WrongLastBlock() {
	s.lastBlock.Hash = "abc"

	err := ValidateChain(s.blocks, s.lastBlock)
	s.require.Error(err)
}

func (s *validateChainTestSuite) TestValidateChain_WrongParentHash() {
	block := s.blocks[len(s.blocks)-1]
	block.ParentHash = "abc"

	err := ValidateChain(s.blocks, nil)
	s.require.Error(err)
}

func (s *validateChainTestSuite) TestValidateChain_WrongParentHeight() {
	block := s.blocks[len(s.blocks)-1]
	block.ParentHeight = 999_999

	err := ValidateChain(s.blocks, nil)
	s.require.Error(err)
}

func TestHexToBig(t *testing.T) {
	tests := []struct {
		name     string
		expected string
		input    string
	}{
		{
			name:     "smallNumberWithLeading0s",
			expected: "291",
			input:    "0x000000123",
		},
		{
			name:     "smallNumberWithoutLeading0s",
			expected: "291",
			input:    "0x123",
		},
		{
			name:     "bigNumberWithLeading0s",
			expected: "20988295442770257793",
			input:    "0x00000012345678123456781",
		},
		{
			name:     "bigNumberWithoutLeading0s",
			expected: "20988295442770257793",
			input:    "0x00000012345678123456781",
		},
		{
			name:     "zero",
			expected: "0",
			input:    "0x0",
		},
		{
			name:     "zeros",
			expected: "0",
			input:    "0x000000",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require := testutil.Require(t)

			actual, err := HexToBig(test.input)
			require.NoError(err)
			require.Equal(test.expected, actual.String())
		})
	}
}

func TestHexToBig_InvalidInput(t *testing.T) {
	tests := []struct {
		name     string
		expected string
		input    string
	}{
		{
			name:  "missing0x",
			input: "123",
		},
		{
			name:  "nan",
			input: "0xefg",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require := testutil.Require(t)

			_, err := HexToBig(test.input)
			require.Error(err)
		})
	}
}

func TestCleanHexString(t *testing.T) {
	tests := []struct {
		name     string
		expected string
		input    string
	}{
		{
			name:     "withLeading0s",
			expected: "0x1234",
			input:    "0x00001234",
		},
		{
			name:     "withoutLeading0s",
			expected: "0x1234",
			input:    "0x1234",
		},
		{
			name:     "zero",
			expected: "0x0",
			input:    "0x0",
		},
		{
			name:     "zeros",
			expected: "0x0",
			input:    "0x0000",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require := testutil.Require(t)

			actual, err := CleanHexString(test.input)
			require.NoError(err)
			require.Equal(test.expected, actual)
		})
	}
}

func TestCleanAddress(t *testing.T) {
	tests := []struct {
		name     string
		expected string
		input    string
	}{
		{
			name:     "withLeading0s",
			expected: "0x23b2eac2a34fec26d92b4fe45a1c2e5c3562ba9d",
			input:    "0x0000000023b2eac2a34fec26d92b4fe45a1c2e5c3562ba9d",
		},
		{
			name:     "withoutLeading0s",
			expected: "0x23b2eac2a34fec26d92b4fe45a1c2e5c3562ba9d",
			input:    "0x23b2eac2a34fec26d92b4fe45a1c2e5c3562ba9d",
		},
		{
			name:     "addressWithLeading0s",
			expected: "0x00b2eac2a34fec26d92b4fe45a1c2e5c3562ba9d",
			input:    "0x0000000000b2eac2a34fec26d92b4fe45a1c2e5c3562ba9d",
		},
		{
			name:     "allZeros",
			expected: "0x0000000000000000000000000000000000000000",
			input:    "0x000000000000000000000000000000000000000000000000",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require := testutil.Require(t)

			actual, err := CleanAddress(test.input)
			require.NoError(err)
			require.Equal(test.expected, actual)
		})
	}
}

func TestTruncatedError(t *testing.T) {
	tests := []struct {
		name     string
		expected string
		input    error
	}{
		{
			name:     "short",
			expected: "short error",
			input:    errors.New("short error"),
		},
		{
			name:     "maxLength",
			expected: strings.Repeat("a", 256),
			input:    errors.New(strings.Repeat("a", 256)),
		},
		{
			name:     "truncated",
			expected: strings.Repeat("a", 256) + "...",
			input:    errors.New(strings.Repeat("a", 257)),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require := testutil.Require(t)

			err := NewTruncatedError(test.input)
			require.Error(err)
			require.Equal(test.expected, err.Error())
		})
	}
}

func TestDecodeBase58(t *testing.T) {
	// Use https://www.better-converter.com/Encoders-Decoders/Base58Check-to-Hexadecimal-Decoder to decode the string.
	tests := []struct {
		name     string
		expected string // hex encoded
		input    string // base58 encoded
	}{
		{
			name:     "valid",
			expected: "b592495137b5abc2cd60ebb446451fb43ee7aa83b9be450c5e5762ec4d6ee4a7",
			input:    "DDnAqxJVFo2GVTujibHt5cjevHMSE9bo8HJaydHoshdp",
		},
		{
			name:     "invalid",
			expected: "",
			input:    "AqxIoO",
		},
		{
			name:     "empty",
			expected: "",
			input:    "",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require := testutil.Require(t)
			expected, err := hex.DecodeString(test.expected)
			require.NoError(err)
			actual := DecodeBase58(test.input)
			require.Equal(expected, actual)
		})
	}
}
