package ethereum

import (
	"crypto/cipher"
	"encoding/hex"
	"os"
	"strings"

	"github.com/SeismicSystems/aes"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/blockchain/parser/internal"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

const (
	// SRCTransferEventTopic is the event signature for SRC20 Transfer events.
	// Exported for use in ethereum_native.go for outer topic check.
	SRCTransferEventTopic = "0x80ffa007a69623ef13594f5e8178eee6c4ef2d0cba74c08329e879f695b7d3f6"

	src20Abi = `[
		{
			"type": "event",
			"name": "Approval",
			"inputs": [
				{"name": "owner", "type": "address", "indexed": true, "internalType": "address"},
				{"name": "spender", "type": "address", "indexed": true, "internalType": "address"},
				{"name": "encryptKeyHash", "type": "bytes32", "indexed": true, "internalType": "bytes32"},
				{"name": "encryptedAmount", "type": "bytes", "indexed": false, "internalType": "bytes"}
			],
			"anonymous": false
		},
		{
			"type": "event",
			"name": "Transfer",
			"inputs": [
				{"name": "from", "type": "address", "indexed": true, "internalType": "address"},
				{"name": "to", "type": "address", "indexed": true, "internalType": "address"},
				{"name": "encryptKeyHash", "type": "bytes32", "indexed": true, "internalType": "bytes32"},
				{"name": "encryptedAmount", "type": "bytes", "indexed": false, "internalType": "bytes"}
			],
			"anonymous": false
		}
	]`
)

// SRC20TokenTransferParser handles parsing of Seismic's encrypted SRC20 token transfers.
// Interface defined for testability and mock support.
type SRC20TokenTransferParser interface {
	ParseSRC20TokenTransfer(eventLog *api.EthereumEventLog) (*api.EthereumTokenTransfer, error)
}

// seismicSRC20Parser implements SRC20TokenTransferParser for Seismic chain.
type seismicSRC20Parser struct {
	src20ABI *abi.ABI
	aesGCM   cipher.AEAD
	keyHash  string
}

// NewSeismicSRC20Parser creates a new SRC20 token transfer parser.
// aesKeyHex should be a hex-encoded AES key (with or without "0x" prefix).
func NewSeismicSRC20Parser(aesKeyHex string) (SRC20TokenTransferParser, error) {
	if aesKeyHex == "" {
		return nil, xerrors.New("SRC20 AES key is required")
	}

	// Parse ABI
	contractAbi, err := abi.JSON(strings.NewReader(src20Abi))
	if err != nil {
		return nil, xerrors.Errorf("failed to parse SRC20 ABI: %w", err)
	}

	// Decode AES key
	aesKey, err := hex.DecodeString(strings.TrimPrefix(aesKeyHex, "0x"))
	if err != nil {
		return nil, xerrors.Errorf("failed to decode AES key: %w", err)
	}

	// Create AES-GCM cipher
	aesGCM, err := aes.CreateAESGCM(aesKey)
	if err != nil {
		return nil, xerrors.Errorf("failed to create AES-GCM cipher: %w", err)
	}

	return &seismicSRC20Parser{
		src20ABI: &contractAbi,
		aesGCM:   aesGCM,
		keyHash:  aes.Keccak256Hash(aesKey).Hex(),
	}, nil
}

func (p *seismicSRC20Parser) ParseSRC20TokenTransfer(eventLog *api.EthereumEventLog) (*api.EthereumTokenTransfer, error) {
	// Defensive check: although the outer layer already checks the topic, we verify here for safety
	// and the key hash is the last topic, only keep those have same key hash with hash of the AES key
	if len(eventLog.Topics) != 4 || eventLog.Topics[0] != SRCTransferEventTopic || eventLog.Topics[3] != p.keyHash {
		return nil, nil
	}

	// Parse event data
	var transferEvent struct {
		EncryptedAmount []byte
	}

	logData, err := hex.DecodeString(strings.TrimPrefix(eventLog.Data, "0x"))
	if err != nil {
		return nil, xerrors.Errorf("failed to decode log data: %w", err)
	}

	if err := p.src20ABI.UnpackIntoInterface(&transferEvent, "Transfer", logData); err != nil {
		return nil, xerrors.Errorf("failed to unpack Transfer event: %w", err)
	}

	value, err := aes.DecryptAESGCM(transferEvent.EncryptedAmount, p.aesGCM)
	if err != nil {
		return nil, xerrors.Errorf("failed to decrypt amount: %w", err)
	}
	// Clean addresses from indexed topics
	tokenAddress, err := internal.CleanAddress(eventLog.Address)
	if err != nil {
		return nil, xerrors.Errorf("failed to clean token address for src20: %w", err)
	}
	fromAddress, err := internal.CleanAddress(eventLog.Topics[1])
	if err != nil {
		return nil, xerrors.Errorf("failed to clean from address for src20: %w", err)
	}
	toAddress, err := internal.CleanAddress(eventLog.Topics[2])
	if err != nil {
		return nil, xerrors.Errorf("failed to clean to address for src20: %w", err)
	}

	valueStr := value.String()

	return &api.EthereumTokenTransfer{
		TokenAddress:     tokenAddress,
		FromAddress:      fromAddress,
		ToAddress:        toAddress,
		Value:            valueStr,
		TransactionHash:  eventLog.TransactionHash,
		TransactionIndex: eventLog.TransactionIndex,
		LogIndex:         eventLog.LogIndex,
		BlockHash:        eventLog.BlockHash,
		BlockNumber:      eventLog.BlockNumber,
		TokenTransfer: &api.EthereumTokenTransfer_Src20{
			Src20: &api.SRC20TokenTransfer{
				FromAddress: fromAddress,
				ToAddress:   toAddress,
				Value:       valueStr,
			},
		},
	}, nil
}

// NewSeismicNativeParser creates a new Seismic native parser.
// It extends the Ethereum parser with SRC20 token transfer parsing capability.
func NewSeismicNativeParser(params internal.ParserParams, opts ...internal.ParserFactoryOption) (internal.NativeParser, error) {
	// Get AES key from config, with fallback to environment variable.
	// This keeps config.New() generic while handling Seismic-specific logic here.
	aesKey := ""
	if params.Config.Chain.CustomParams != nil {
		aesKey = params.Config.Chain.CustomParams["src20_aes_key"]
	}
	if aesKey == "" {
		aesKey = os.Getenv("SRC20_AES_KEY")
	}

	// If AES key is available, create SRC20 parser
	if aesKey != "" {
		src20Parser, err := NewSeismicSRC20Parser(aesKey)
		if err != nil {
			return nil, xerrors.Errorf("failed to create SRC20 parser: %w", err)
		}
		opts = append(opts, WithSRC20Parser(src20Parser))
	} else {
		params.Logger.Warn("SRC20_AES_KEY is not configured for Seismic parser; SRC20 token transfers will be skipped.")
	}

	// Seismic timestamps are in milliseconds, convert to seconds.
	opts = append(opts, WithTimestampInMs())

	return NewEthereumNativeParser(params, opts...)
}
