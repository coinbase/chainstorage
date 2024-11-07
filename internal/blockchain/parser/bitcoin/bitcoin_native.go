package bitcoin

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"

	"github.com/btcsuite/btcd/btcutil"
	"github.com/btcsuite/btcd/btcutil/base58"
	"github.com/go-playground/validator/v10"
	"github.com/golang/protobuf/ptypes/timestamp"
	"go.uber.org/zap"
	"golang.org/x/crypto/ripemd160"

	"github.com/coinbase/chainstorage/internal/blockchain/parser/internal"
	"github.com/coinbase/chainstorage/internal/utils/log"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

// All supported types can be seen in bitcoin-core docs for getaddressinfo
// https://bitcoincore.org/en/doc/0.21.0/rpc/wallet/getaddressinfo/ (witness_v1_taproot is not specified here)
// https://github.com/bitcoin/bitcoin/blob/7e83e74e7fca7e1adee2174fee447a86af9bc68d/src/script/standard.cpp#L48
const (
	bitcoinScriptTypeNonstandard         string = "nonstandard"
	bitcoinScriptTypePubKey              string = "pubkey"
	bitcoinScriptTypePubKeyHash          string = "pubkeyhash"
	bitcoinScriptTypeWitnessV0PubKeyHash string = "witness_v0_keyhash"
	bitcoinScriptTypeScriptHash          string = "scripthash"
	bitcoinScriptTypeWitnessV0ScriptHash string = "witness_v0_scripthash"
	bitcoinScriptTypeMultisig            string = "multisig"
	bitcoinScriptTypeNullData            string = "nulldata"
	bitcoinScriptTypeWitnessUnknown      string = "witness_unknown"
	bitcoinScriptTypeWitnessV1Taproot    string = "witness_v1_taproot"
)

type (
	BitcoinBoolean         bool
	BitcoinString          string
	BitcoinHexString       string
	BitcoinQuantity        uint64
	BitcoinDecimalQuantity float64

	// BitcoinBlock https://developer.bitcoin.org/reference/rpc/getblock.html
	BitcoinBlock struct {
		Hash              BitcoinHexString       `json:"hash" validate:"required"`
		StrippedSize      BitcoinQuantity        `json:"strippedsize"`
		Size              BitcoinQuantity        `json:"size"`
		Weight            BitcoinQuantity        `json:"weight"`
		Height            BitcoinQuantity        `json:"height" validate:"required_with=PreviousBlockHash"`
		Version           BitcoinQuantity        `json:"version"`
		VersionHex        BitcoinHexString       `json:"versionHex"`
		MerkleRoot        BitcoinHexString       `json:"merkleroot"`
		Tx                []*BitcoinTransaction  `json:"tx" validate:"required,min=1,dive,required"`
		Time              BitcoinQuantity        `json:"time" validate:"required"`
		MedianTime        BitcoinQuantity        `json:"mediantime"`
		Nonce             BitcoinQuantity        `json:"nonce"`
		Bits              BitcoinHexString       `json:"bits"`
		Difficulty        BitcoinDecimalQuantity `json:"difficulty"`
		ChainWork         BitcoinHexString       `json:"chainwork"`
		NTx               BitcoinQuantity        `json:"nTx"`
		PreviousBlockHash BitcoinHexString       `json:"previousblockhash" validate:"required_with=Height"`
		NextBlockHash     BitcoinHexString       `json:"nextblockhash"`
	}

	// BitcoinTransaction https://developer.bitcoin.org/reference/rpc/getrawtransaction.html
	BitcoinTransaction struct {
		Hex       BitcoinHexString            `json:"hex"`
		TxId      BitcoinHexString            `json:"txid" validate:"required"`
		Hash      BitcoinHexString            `json:"hash" validate:"required"`
		Size      BitcoinQuantity             `json:"size"`
		Vsize     BitcoinQuantity             `json:"vsize"`
		Weight    BitcoinQuantity             `json:"weight"`
		Version   BitcoinQuantity             `json:"version"`
		LockTime  BitcoinQuantity             `json:"locktime"`
		Vin       []*BitcoinTransactionInput  `json:"vin" validate:"required,min=1,dive,required"`
		Vout      []*BitcoinTransactionOutput `json:"vout" validate:"required,min=1,dive,required"`
		BlockHash BitcoinHexString            `json:"blockhash"`
		BlockTime BitcoinQuantity             `json:"blocktime"`
		Time      BitcoinQuantity             `json:"time"`
	}

	BitcoinTransactionInput struct {
		Coinbase    BitcoinHexString   `json:"coinbase"`
		TxId        BitcoinHexString   `json:"txid" validate:"required_without=Coinbase"`
		Vout        BitcoinQuantity    `json:"vout"`
		ScriptSig   *BitcoinScriptSig  `json:"scriptSig"`
		Sequence    BitcoinQuantity    `json:"sequence"`
		TxInWitness []BitcoinHexString `json:"txinwitness"`
	}

	BitcoinScriptSig struct {
		Asm BitcoinString    `json:"asm"`
		Hex BitcoinHexString `json:"hex"`
	}

	BitcoinTransactionOutput struct {
		Value        BitcoinDecimalQuantity `json:"value"`
		N            BitcoinQuantity        `json:"n"`
		ScriptPubKey *BitcoinScriptPubKey   `json:"scriptPubKey" validate:"required"`
	}

	// BitcoinScriptPubKey ...
	// note that reqSigs and addresses are kept for backward compatibility from what's in storage
	BitcoinScriptPubKey struct {
		Asm       BitcoinString    `json:"asm"`
		Hex       BitcoinHexString `json:"hex"`
		ReqSigs   BitcoinQuantity  `json:"reqSigs"` // deprecated: https://github.com/bitcoin/bitcoin/pull/20286
		Type      BitcoinString    `json:"type"`
		Addresses []BitcoinString  `json:"addresses"` // deprecated: https://github.com/bitcoin/bitcoin/pull/20286
		Address   BitcoinString    `json:"address"`
	}

	// BitcoinBlockLit is a light version of BitcoinBlock
	// Fields not used during data ingestion are removed.
	BitcoinBlockLit struct {
		Hash              BitcoinHexString         `json:"hash" validate:"required"`
		PreviousBlockHash BitcoinHexString         `json:"previousblockhash" validate:"required_with=Height"`
		Height            BitcoinQuantity          `json:"height" validate:"required_with=PreviousBlockHash"`
		Transactions      []*BitcoinTransactionLit `json:"tx" validate:"required"`
		Time              BitcoinQuantity          `json:"time" validate:"required"`
	}

	BitcoinTransactionLit struct {
		Identifier BitcoinHexString              `json:"txid" validate:"required"`
		Inputs     []*BitcoinTransactionInputLit `json:"vin" validate:"required"`
	}

	// Used by the custom unmarshaler of BitcoinTransactionLit.
	bitcoinTransactionLit struct {
		Identifier BitcoinHexString              `json:"txid" validate:"required"`
		Inputs     []*BitcoinTransactionInputLit `json:"vin" validate:"required"`
	}

	BitcoinTransactionInputLit struct {
		Coinbase   BitcoinHexString `json:"coinbase"`
		Identifier BitcoinHexString `json:"txid" validate:"required_without=Coinbase"`
		Vout       BitcoinQuantity  `json:"vout" validate:"required_with=Identifier"`
	}

	BitcoinInputTransactionLit struct {
		TxId BitcoinHexString            `json:"txid" validate:"required"`
		Vout []*BitcoinTransactionOutput `json:"vout" validate:"required,min=1,dive,required"`
	}

	bitcoinNativeParserImpl struct {
		logger   *zap.Logger
		validate *validator.Validate
	}
)

var _ internal.NativeParser = (*bitcoinNativeParserImpl)(nil)

var pubKeyScriptRegexp = regexp.MustCompile("^([[:xdigit:]]*) OP_CHECKSIG$")

func validateBitcoinScriptPubKey(sl validator.StructLevel) {
	pubKey := sl.Current().Interface().(BitcoinScriptPubKey)

	// Addresses is deprecated: https://github.com/bitcoin/bitcoin/pull/20286
	// we need it here for backcompat with already ingested data
	address := pubKey.Address
	if len(pubKey.Addresses) > 0 && len(address) == 0 {
		address = pubKey.Addresses[0]
	}

	switch pubKey.Type.Value() {
	// The `nonstandard` and `nulldata` script types do not contain address
	case bitcoinScriptTypeNullData, bitcoinScriptTypeNonstandard:
		if len(address) > 0 {
			sl.ReportError(address, "Address[null]", "Address[null]", "bspk_an", "")
		}
	// The `multisig` script types can have anywhere from 0 to many addresses.
	// We do not record a canonical "address" for these script pub keys
	case bitcoinScriptTypeMultisig:
	// Bitcoin core has stopped returning address for pubkey scripts since https://github.com/bitcoin/bitcoin/pull/16725.
	// addresses and regSigs are deprecated: https://github.com/bitcoin/bitcoin/pull/20286
	case bitcoinScriptTypePubKey:
		if len(address) == 0 {
			match := pubKeyScriptRegexp.FindStringSubmatch(pubKey.Asm.Value())
			if len(match) == 0 {
				sl.ReportError(address, "Address[pubkey:m]", "Address[pubkey:m]", "bspk_apm", "")
			}

			_, err := hex.DecodeString(match[1])
			if err != nil {
				sl.ReportError(address, "Address[pubkey:d]", "Address[pubkey:d]", "bspk_apd", "")
			}
		}
	// Types that we expect to be able to parse address for
	case bitcoinScriptTypePubKeyHash, bitcoinScriptTypeScriptHash, bitcoinScriptTypeWitnessV0PubKeyHash, bitcoinScriptTypeWitnessV0ScriptHash, bitcoinScriptTypeWitnessUnknown, bitcoinScriptTypeWitnessV1Taproot:
		if len(address) == 0 {
			sl.ReportError(address, "Address[main]", "Address[main]", "bspk_a", "")
		}
	default:
		sl.ReportError(address, "Address[unsupported]", "Address[unsupported]", "bspk_as", "")
	}
}

func NewBitcoinNativeParser(params internal.ParserParams, opts ...internal.ParserFactoryOption) (internal.NativeParser, error) {
	v := validator.New()
	v.RegisterStructValidation(validateBitcoinScriptPubKey, BitcoinScriptPubKey{})
	return &bitcoinNativeParserImpl{
		logger:   log.WithPackage(params.Logger),
		validate: v,
	}, nil
}

func (b *bitcoinNativeParserImpl) ParseBlock(ctx context.Context, rawBlock *api.Block) (*api.NativeBlock, error) {
	metadata := rawBlock.GetMetadata()
	if metadata == nil {
		return nil, errors.New("metadata not found")
	}

	blobdata := rawBlock.GetBitcoin()
	if blobdata == nil {
		return nil, errors.New("bitcoin blobdata not found")
	}

	var block BitcoinBlock
	if err := json.Unmarshal(blobdata.GetHeader(), &block); err != nil {
		return nil, fmt.Errorf("failed to parse bitcoin block with %+v: %w", metadata, err)
	}

	if err := b.validateStruct(block); err != nil {
		return nil, fmt.Errorf("failed to validate bitcoin block %+v: %w", metadata, err)
	}

	header := block.GetApiBitcoinHeader()
	transactions, err := b.parseTransactions(blobdata, block.Tx)
	if err != nil {
		return nil, fmt.Errorf("parseTransactions failed for %+v: %w", metadata, err)
	}

	return &api.NativeBlock{
		Blockchain:      rawBlock.Blockchain,
		Network:         rawBlock.Network,
		Tag:             metadata.Tag,
		Hash:            metadata.Hash,
		ParentHash:      metadata.ParentHash,
		Height:          metadata.Height,
		ParentHeight:    metadata.ParentHeight,
		Timestamp:       header.Timestamp,
		NumTransactions: uint64(len(transactions)),
		Block: &api.NativeBlock_Bitcoin{
			Bitcoin: &api.BitcoinBlock{
				Header:       header,
				Transactions: transactions,
			},
		},
	}, nil
}

func (b *bitcoinNativeParserImpl) GetTransaction(ctx context.Context, nativeBlock *api.NativeBlock, transactionHash string) (*api.NativeTransaction, error) {
	return nil, internal.ErrNotImplemented
}

func (b *bitcoinNativeParserImpl) parseTransactions(
	data *api.BitcoinBlobdata, rawTransactions []*BitcoinTransaction,
) ([]*api.BitcoinTransaction, error) {
	metadataMap := make(map[string][]*api.BitcoinTransactionOutput)
	for i, rawTransaction := range data.GetInputTransactions() {
		for j, input := range rawTransaction.GetData() {
			var inputTx BitcoinInputTransactionLit
			if err := json.Unmarshal(input, &inputTx); err != nil {
				return nil, fmt.Errorf("failed to parse input transaction on [%d][%d]: %w", i, j, err)
			}

			if err := b.validateStruct(inputTx); err != nil {
				return nil, fmt.Errorf("failed to validate bitcoin input transaction %v: %w", inputTx.TxId, err)
			}

			if len(inputTx.Vout) != 1 {
				return nil, fmt.Errorf("unexpected length of input transaction's output (expected=1, len=%d)", len(inputTx.Vout))
			}

			inputTxId := inputTx.TxId.Value()
			if _, ok := metadataMap[inputTxId]; !ok {
				metadataMap[inputTxId] = make([]*api.BitcoinTransactionOutput, 0)
			}

			outputTx, err := inputTx.Vout[0].ToApiBitcoinTransactionOutput()
			if err != nil {
				return nil, fmt.Errorf("failed to convert to transaction output: %w", err)
			}

			metadataMap[inputTxId] = append(metadataMap[inputTxId], outputTx)
		}
	}

	transactions, err := b.parseApiBitcoinTransactions(rawTransactions, metadataMap)
	if err != nil {
		return nil, err
	}

	return transactions, nil
}

func (b *bitcoinNativeParserImpl) parseApiBitcoinTransactions(
	rawTransactions []*BitcoinTransaction, metadataMap map[string][]*api.BitcoinTransactionOutput,
) ([]*api.BitcoinTransaction, error) {
	transactions := make([]*api.BitcoinTransaction, len(rawTransactions))
	for i, rawTx := range rawTransactions {
		transaction, err := rawTx.ToApiBitcoinTransaction(i, metadataMap)
		if err != nil {
			return nil, err
		}

		transactions[i] = transaction
	}

	return transactions, nil
}

func (b *bitcoinNativeParserImpl) validateStruct(s any) error {
	if err := b.validate.Struct(s); err != nil {
		// Errors returned by validator may be very long and get dropped by datadog.
		return internal.NewTruncatedError(err)
	}

	return nil
}

func (b *BitcoinBlock) GetApiBitcoinHeader() *api.BitcoinHeader {
	if b == nil {
		return nil
	}

	return &api.BitcoinHeader{
		Hash:                 b.Hash.Value(),
		StrippedSize:         b.StrippedSize.Value(),
		Size:                 b.Size.Value(),
		Weight:               b.Weight.Value(),
		Height:               b.Height.Value(),
		Version:              b.Version.Value(),
		VersionHex:           b.VersionHex.Value(),
		MerkleRoot:           b.MerkleRoot.Value(),
		Time:                 b.Time.Value(),
		MedianTime:           b.MedianTime.Value(),
		Nonce:                b.Nonce.Value(),
		Bits:                 b.Bits.Value(),
		Difficulty:           b.Difficulty.String(),
		ChainWork:            b.ChainWork.Value(),
		NumberOfTransactions: b.NTx.Value(),
		PreviousBlockHash:    b.PreviousBlockHash.Value(),
		NextBlockHash:        b.NextBlockHash.Value(),
		Timestamp: &timestamp.Timestamp{
			Seconds: int64(b.Time.Value()),
		},
	}
}

func (t *BitcoinTransaction) ToApiBitcoinTransaction(index int, metadataMap map[string][]*api.BitcoinTransactionOutput) (*api.BitcoinTransaction, error) {
	vin, err := parseVin(t.Vin, metadataMap)
	if err != nil {
		return nil, err
	}

	vout, err := parseVout(t.Vout)
	if err != nil {
		return nil, err
	}

	var isCoinbase bool
	var inputValue uint64
	for _, input := range vin {
		if input.Coinbase != "" || input.TransactionId == "" {
			isCoinbase = true
		} else {
			inputValue += input.GetFromOutput().GetValue()
		}
	}

	var outputValue uint64
	for _, output := range vout {
		outputValue += output.GetValue()
	}

	var fee uint64
	if !isCoinbase {
		fee = inputValue - outputValue
	}

	return &api.BitcoinTransaction{
		Hex:           t.Hex.Value(),
		TransactionId: t.TxId.Value(),
		Hash:          t.Hash.Value(),
		Size:          t.Size.Value(),
		VirtualSize:   t.Vsize.Value(),
		Weight:        t.Weight.Value(),
		Version:       t.Version.Value(),
		LockTime:      t.LockTime.Value(),
		Inputs:        vin,
		Outputs:       vout,
		BlockHash:     t.BlockHash.Value(),
		BlockTime:     t.BlockTime.Value(),
		Time:          t.Time.Value(),
		IsCoinbase:    isCoinbase,
		Index:         uint64(index),
		InputCount:    uint64(len(vin)),
		OutputCount:   uint64(len(vout)),
		InputValue:    inputValue,
		OutputValue:   outputValue,
		Fee:           fee,
	}, nil
}

func parseVin(vin []*BitcoinTransactionInput, metadataMap map[string][]*api.BitcoinTransactionOutput) ([]*api.BitcoinTransactionInput, error) {
	inputs := make([]*api.BitcoinTransactionInput, len(vin))
	for i, inputTx := range vin {
		input := inputTx.ToApiBitcoinTransactionInput(uint64(i))
		// Note that there is no transaction ID for coinbase transaction
		if input.TransactionId != "" && metadataMap != nil {
			outputs, ok := metadataMap[input.TransactionId]
			if !ok || len(outputs) == 0 {
				return nil, fmt.Errorf("parseVin at not found on metadataMap for [%d] with tx: %s", i, input.TransactionId)
			}

			for _, output := range outputs {
				if output.Index == input.FromOutputIndex {
					input.FromOutput = output
					break
				}
			}

			if input.FromOutput == nil {
				return nil, fmt.Errorf("output index not found in input transaction's vout, outputs=%+v, input=%+v", outputs, input)
			}
		}

		inputs[i] = input
	}

	return inputs, nil
}

func (i *BitcoinTransactionInput) ToApiBitcoinTransactionInput(index uint64) *api.BitcoinTransactionInput {
	scriptSig := i.ScriptSig.ToApiBitcoinScriptSignature()
	witnesses := hexStringsToStrings(i.TxInWitness)
	return &api.BitcoinTransactionInput{
		Coinbase:                  i.Coinbase.Value(),
		TransactionId:             i.TxId.Value(),
		FromOutputIndex:           i.Vout.Value(),
		ScriptSignature:           scriptSig,
		Sequence:                  i.Sequence.Value(),
		TransactionInputWitnesses: witnesses,
		Index:                     index,
	}
}

func hexStringsToStrings(hexStrings []BitcoinHexString) []string {
	if hexStrings == nil {
		return nil
	}

	result := make([]string, len(hexStrings))
	for i, hexString := range hexStrings {
		result[i] = hexString.Value()
	}

	return result
}

func parseVout(vout []*BitcoinTransactionOutput) ([]*api.BitcoinTransactionOutput, error) {
	outputs := make([]*api.BitcoinTransactionOutput, len(vout))
	for i, outputTx := range vout {
		output, err := outputTx.ToApiBitcoinTransactionOutput()
		if err != nil {
			return nil, fmt.Errorf("failed to convert transaction output: %w", err)
		}

		outputs[i] = output
	}

	return outputs, nil
}

func (o *BitcoinTransactionOutput) ToApiBitcoinTransactionOutput() (*api.BitcoinTransactionOutput, error) {
	value, err := btcutil.NewAmount(o.Value.Value())
	if err != nil {
		return nil, fmt.Errorf("failed to convert value %v to btc amount: %w", o.Value.Value(), err)
	}

	scriptPubKey, err := o.ScriptPubKey.ToApiBitcoinScriptPublicKey()
	if err != nil {
		return nil, fmt.Errorf("failed to convert script public key: %w", err)
	}

	return &api.BitcoinTransactionOutput{
		Value:           uint64(value),
		Index:           o.N.Value(),
		ScriptPublicKey: scriptPubKey,
	}, nil
}

func (k *BitcoinScriptPubKey) ToApiBitcoinScriptPublicKey() (*api.BitcoinScriptPublicKey, error) {
	if k == nil {
		return nil, nil
	}

	if k.Type.Value() != bitcoinScriptTypeMultisig {
		// Addresses is deprecated: https://github.com/bitcoin/bitcoin/pull/20286
		// we need it here for backcompat with already ingested data
		if len(k.Addresses) > 0 && len(k.Address) == 0 {
			// taking only the first one
			k.Address = k.Addresses[0]
		}
	}

	scriptPublicKey := &api.BitcoinScriptPublicKey{
		Assembly: k.Asm.Value(),
		Hex:      k.Hex.Value(),
		Type:     k.Type.Value(),
		Address:  k.Address.Value(),
	}

	transformedScriptPublicKey, err := transformScriptPublicKey(scriptPublicKey)
	if err != nil {
		return nil, err
	}

	return transformedScriptPublicKey, nil
}

func (s *BitcoinScriptSig) ToApiBitcoinScriptSignature() *api.BitcoinScriptSignature {
	if s == nil {
		return nil
	}

	return &api.BitcoinScriptSignature{
		Assembly: s.Asm.Value(),
		Hex:      s.Hex.Value(),
	}
}

func (v BitcoinBoolean) Value() bool {
	return bool(v)
}

func (v BitcoinString) Value() string {
	return string(v)
}

func (v BitcoinHexString) Value() string {
	return string(v)
}

func (v BitcoinQuantity) Value() uint64 {
	return uint64(v)
}

func (v BitcoinDecimalQuantity) String() string {
	return fmt.Sprintf("%.2f", v)
}

func (v BitcoinDecimalQuantity) Value() float64 {
	return float64(v)
}

func (v *BitcoinTransactionLit) UnmarshalJSON(input []byte) error {
	// The transaction may be a string or a struct, depending on whether full transaction objects are requested.
	if len(input) > 0 && input[0] == '"' {
		return json.Unmarshal(input, &v.Identifier)
	}

	// Use a different struct to avoid calling this custom unmarshaler recursively.
	var out bitcoinTransactionLit
	if err := json.Unmarshal(input, &out); err != nil {
		return fmt.Errorf("failed to unmarshal struct: %w", err)
	}

	v.Identifier = out.Identifier
	v.Inputs = out.Inputs
	return nil
}

func transformScriptPublicKey(
	scriptPubKey *api.BitcoinScriptPublicKey,
) (*api.BitcoinScriptPublicKey, error) {
	switch scriptPubKey.Type {
	case bitcoinScriptTypePubKey:
		if len(scriptPubKey.Address) > 0 {
			return scriptPubKey, nil
		}

		// Attempt to fall back to parsing the address from the script.
		account, err := parseAccountFromPubKeyScript(scriptPubKey)
		if err != nil {
			return nil, err
		}

		scriptPubKey.Address = account
		return scriptPubKey, nil
	default:
		return scriptPubKey, nil
	}
}

func parseAccountFromPubKeyScript(
	scriptPubKey *api.BitcoinScriptPublicKey,
) (string, error) {
	if scriptPubKey.Type != bitcoinScriptTypePubKey {
		return "", errors.New("not of type pubkey")
	}

	match := pubKeyScriptRegexp.FindStringSubmatch(scriptPubKey.GetAssembly())
	if len(match) == 0 {
		return "", fmt.Errorf("could not parse pubkey script: %s", scriptPubKey.GetAssembly())
	}

	pubKey, err := hex.DecodeString(match[1])
	if err != nil {
		return "", fmt.Errorf("could not decode pubkey hex: %w", err)
	}

	// Hash writes never return errors.
	sha256Hash := sha256.New()
	_, _ = sha256Hash.Write(pubKey)
	ripemd160Hash := ripemd160.New()
	_, _ = ripemd160Hash.Write(sha256Hash.Sum(nil))
	address := base58.CheckEncode(ripemd160Hash.Sum(nil), 0)

	return address, nil
}
