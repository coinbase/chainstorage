package bitcoin

import (
	"bytes"
	"encoding/json"

	"github.com/go-playground/validator/v10"

	"github.com/coinbase/chainstorage/internal/blockchain/parser/internal"
	"github.com/coinbase/chainstorage/internal/utils/log"
)

// zcashTxForFilter is a lightweight view over a raw transaction used by
// the shielded-tx filter. Raw JSON fields are kept opaque so we can
// classify without allocating into the shared BitcoinTransaction struct.
type zcashTxForFilter map[string]json.RawMessage

func NewZcashNativeParser(params internal.ParserParams, opts ...internal.ParserFactoryOption) (internal.NativeParser, error) {
	v := validator.New()
	v.RegisterStructValidation(validateBitcoinScriptPubKey, BitcoinScriptPubKey{})
	// Note: validateBitcoinTransactionVinVout is intentionally NOT registered.
	// Zcash shielded transactions can have empty vin (shielded-to-transparent)
	// or empty vout (transparent-to-shielded) and must pass struct validation
	// even though the subsequent txFilter will drop them from the output.
	return &bitcoinNativeParserImpl{
		logger:       log.WithPackage(params.Logger),
		validate:     v,
		preprocessTx: backfillTxHashSingle,
		txFilter:     isTransparentZcashTx,
	}, nil
}

// isTransparentZcashTx returns true only when the transaction has both
// vin and vout. Coinbase transactions are always kept. Matches the
// bitcoinNativeParserImpl.txFilter signature so it can be wired
// directly.
func isTransparentZcashTx(txRaw json.RawMessage) (bool, error) {
	var tx zcashTxForFilter
	if err := json.Unmarshal(txRaw, &tx); err != nil {
		return false, err
	}

	coinbase, err := isZcashCoinbaseTx(tx)
	if err != nil {
		return false, err
	}
	if coinbase {
		return true, nil
	}

	return hasMeaningfulJSONField(tx["vin"]) && hasMeaningfulJSONField(tx["vout"]), nil
}

// isZcashCoinbaseTx checks whether a transaction is a coinbase by inspecting
// the first vin entry for a "coinbase" field.
func isZcashCoinbaseTx(tx zcashTxForFilter) (bool, error) {
	vinRaw, ok := tx["vin"]
	if !ok || !hasMeaningfulJSONField(vinRaw) {
		return false, nil
	}

	var vin []json.RawMessage
	if err := json.Unmarshal(vinRaw, &vin); err != nil {
		return false, err
	}
	if len(vin) == 0 {
		return false, nil
	}

	var firstVin map[string]json.RawMessage
	if err := json.Unmarshal(vin[0], &firstVin); err != nil {
		return false, err
	}

	return hasMeaningfulJSONField(firstVin["coinbase"]), nil
}

func hasMeaningfulJSONField(raw json.RawMessage) bool {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 {
		return false
	}

	switch string(trimmed) {
	case "null", "[]", "{}":
		return false
	default:
		return true
	}
}
