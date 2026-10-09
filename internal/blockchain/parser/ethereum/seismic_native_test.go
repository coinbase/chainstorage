package ethereum

import (
	"crypto/rand"
	"encoding/hex"
	"math/big"
	"testing"

	seismicAes "github.com/SeismicSystems/aes"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/stretchr/testify/require"

	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

// testAESKeyHex is a 256-bit test AES key (hex-encoded, no 0x prefix).
const testAESKeyHex = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

// encryptTestAmount encrypts a big.Int value using AES-GCM, returning the
// ABI-encoded log data (hex string with 0x prefix) matching the Transfer event layout.
func encryptTestAmount(t *testing.T, aesKeyHex string, amount *big.Int) string {
	t.Helper()

	aesKey, err := hex.DecodeString(aesKeyHex)
	require.NoError(t, err)

	aesGCM, err := seismicAes.CreateAESGCM(aesKey)
	require.NoError(t, err)

	// Pad amount to 32 bytes (uint256)
	plaintext := amount.Bytes()
	padded := make([]byte, 32)
	copy(padded[32-len(plaintext):], plaintext)

	// Encrypt: ciphertext || nonce (12 bytes)
	nonce := make([]byte, seismicAes.NONCE_LENGTH)
	_, err = rand.Read(nonce)
	require.NoError(t, err)

	ciphertext := aesGCM.Seal(nil, nonce, padded, nil)
	// Format expected by DecryptAESGCM: ciphertext || nonce
	encrypted := append(ciphertext, nonce...)

	// ABI-encode as bytes (dynamic type)
	bytesType, err := abi.NewType("bytes", "", nil)
	require.NoError(t, err)

	args := abi.Arguments{{Type: bytesType}}
	encoded, err := args.Pack(encrypted)
	require.NoError(t, err)

	return "0x" + hex.EncodeToString(encoded)
}

// testKeyHash returns the keccak256 hash of the test AES key, matching what the parser computes.
func testKeyHash(t *testing.T) string {
	t.Helper()
	aesKey, err := hex.DecodeString(testAESKeyHex)
	require.NoError(t, err)
	return seismicAes.Keccak256Hash(aesKey).Hex()
}

func TestParseSRC20TokenTransfer(t *testing.T) {
	parser, err := NewSeismicSRC20Parser(testAESKeyHex)
	require.NoError(t, err)

	keyHash := testKeyHash(t)

	t.Run("ValidTransfer", func(t *testing.T) {
		amount := big.NewInt(1000000)
		data := encryptTestAmount(t, testAESKeyHex, amount)

		eventLog := &api.EthereumEventLog{
			Topics: []string{
				SRCTransferEventTopic,
				"0x000000000000000000000000aabbccddee11223344556677889900aabbccddee",
				"0x00000000000000000000000011223344556677889900aabbccddeeff00112233",
				keyHash,
			},
			Data:             data,
			Address:          "0x0000000000000000000000001234567890abcdef1234567890abcdef12345678",
			TransactionHash:  "0xdeadbeef",
			TransactionIndex: 5,
			LogIndex:         3,
			BlockHash:        "0xblockhash",
			BlockNumber:      100,
		}

		transfer, err := parser.ParseSRC20TokenTransfer(eventLog)
		require.NoError(t, err)
		require.NotNil(t, transfer)

		require.Equal(t, "0x1234567890abcdef1234567890abcdef12345678", transfer.TokenAddress)
		require.Equal(t, "0xaabbccddee11223344556677889900aabbccddee", transfer.FromAddress)
		require.Equal(t, "0x11223344556677889900aabbccddeeff00112233", transfer.ToAddress)
		require.Equal(t, amount.String(), transfer.Value)
		require.Equal(t, "0xdeadbeef", transfer.TransactionHash)
		require.Equal(t, uint64(5), transfer.TransactionIndex)
		require.Equal(t, uint64(3), transfer.LogIndex)
		require.Equal(t, "0xblockhash", transfer.BlockHash)
		require.Equal(t, uint64(100), transfer.BlockNumber)

		src20 := transfer.GetSrc20()
		require.NotNil(t, src20)
		require.Equal(t, "0xaabbccddee11223344556677889900aabbccddee", src20.FromAddress)
		require.Equal(t, "0x11223344556677889900aabbccddeeff00112233", src20.ToAddress)
		require.Equal(t, amount.String(), src20.Value)
	})

	t.Run("ZeroAmount", func(t *testing.T) {
		amount := big.NewInt(0)
		data := encryptTestAmount(t, testAESKeyHex, amount)

		eventLog := &api.EthereumEventLog{
			Topics: []string{
				SRCTransferEventTopic,
				"0x000000000000000000000000aabbccddee11223344556677889900aabbccddee",
				"0x00000000000000000000000011223344556677889900aabbccddeeff00112233",
				keyHash,
			},
			Data:    data,
			Address: "0x0000000000000000000000001234567890abcdef1234567890abcdef12345678",
		}

		transfer, err := parser.ParseSRC20TokenTransfer(eventLog)
		require.NoError(t, err)
		require.NotNil(t, transfer)
		require.Equal(t, "0", transfer.Value)
	})

	t.Run("LargeAmount", func(t *testing.T) {
		// Use a large uint256-range value
		amount := new(big.Int)
		amount.SetString("115792089237316195423570985008687907853269984665640564039457584007913129639935", 10) // max uint256
		data := encryptTestAmount(t, testAESKeyHex, amount)

		eventLog := &api.EthereumEventLog{
			Topics: []string{
				SRCTransferEventTopic,
				"0x000000000000000000000000aabbccddee11223344556677889900aabbccddee",
				"0x00000000000000000000000011223344556677889900aabbccddeeff00112233",
				keyHash,
			},
			Data:    data,
			Address: "0x0000000000000000000000001234567890abcdef1234567890abcdef12345678",
		}

		transfer, err := parser.ParseSRC20TokenTransfer(eventLog)
		require.NoError(t, err)
		require.NotNil(t, transfer)
		require.Equal(t, amount.String(), transfer.Value)
	})

	t.Run("WrongTopicCount_TooFew", func(t *testing.T) {
		eventLog := &api.EthereumEventLog{
			Topics: []string{
				SRCTransferEventTopic,
				"0x000000000000000000000000aabbccddee11223344556677889900aabbccddee",
			},
		}

		transfer, err := parser.ParseSRC20TokenTransfer(eventLog)
		require.NoError(t, err)
		require.Nil(t, transfer)
	})

	t.Run("WrongEventTopic", func(t *testing.T) {
		eventLog := &api.EthereumEventLog{
			Topics: []string{
				"0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef", // ERC20 Transfer topic
				"0x000000000000000000000000aabbccddee11223344556677889900aabbccddee",
				"0x00000000000000000000000011223344556677889900aabbccddeeff00112233",
				keyHash,
			},
		}

		transfer, err := parser.ParseSRC20TokenTransfer(eventLog)
		require.NoError(t, err)
		require.Nil(t, transfer)
	})

	t.Run("WrongKeyHash", func(t *testing.T) {
		eventLog := &api.EthereumEventLog{
			Topics: []string{
				SRCTransferEventTopic,
				"0x000000000000000000000000aabbccddee11223344556677889900aabbccddee",
				"0x00000000000000000000000011223344556677889900aabbccddeeff00112233",
				"0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef",
			},
		}

		transfer, err := parser.ParseSRC20TokenTransfer(eventLog)
		require.NoError(t, err)
		require.Nil(t, transfer)
	})

	t.Run("EmptyTopics", func(t *testing.T) {
		eventLog := &api.EthereumEventLog{
			Topics: []string{},
		}

		transfer, err := parser.ParseSRC20TokenTransfer(eventLog)
		require.NoError(t, err)
		require.Nil(t, transfer)
	})

	t.Run("InvalidLogData", func(t *testing.T) {
		eventLog := &api.EthereumEventLog{
			Topics: []string{
				SRCTransferEventTopic,
				"0x000000000000000000000000aabbccddee11223344556677889900aabbccddee",
				"0x00000000000000000000000011223344556677889900aabbccddeeff00112233",
				keyHash,
			},
			Data:    "0xnotvalidhex",
			Address: "0x0000000000000000000000001234567890abcdef1234567890abcdef12345678",
		}

		transfer, err := parser.ParseSRC20TokenTransfer(eventLog)
		require.Error(t, err)
		require.Nil(t, transfer)
	})

	t.Run("AddressTooShort", func(t *testing.T) {
		amount := big.NewInt(500)
		data := encryptTestAmount(t, testAESKeyHex, amount)

		eventLog := &api.EthereumEventLog{
			Topics: []string{
				SRCTransferEventTopic,
				"0xshort",
				"0x00000000000000000000000011223344556677889900aabbccddeeff00112233",
				keyHash,
			},
			Data:    data,
			Address: "0x0000000000000000000000001234567890abcdef1234567890abcdef12345678",
		}

		transfer, err := parser.ParseSRC20TokenTransfer(eventLog)
		require.Error(t, err)
		require.Nil(t, transfer)
	})
}

func TestNewSeismicSRC20Parser(t *testing.T) {
	t.Run("ValidKey", func(t *testing.T) {
		parser, err := NewSeismicSRC20Parser(testAESKeyHex)
		require.NoError(t, err)
		require.NotNil(t, parser)
	})

	t.Run("ValidKeyWith0xPrefix", func(t *testing.T) {
		parser, err := NewSeismicSRC20Parser("0x" + testAESKeyHex)
		require.NoError(t, err)
		require.NotNil(t, parser)
	})

	t.Run("EmptyKey", func(t *testing.T) {
		parser, err := NewSeismicSRC20Parser("")
		require.Error(t, err)
		require.Nil(t, parser)
		require.Contains(t, err.Error(), "SRC20 AES key is required")
	})

	t.Run("InvalidHexKey", func(t *testing.T) {
		parser, err := NewSeismicSRC20Parser("not-a-hex-key")
		require.Error(t, err)
		require.Nil(t, parser)
	})
}
