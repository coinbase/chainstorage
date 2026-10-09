package bitcoin

import (
	"github.com/go-playground/validator/v10"

	"github.com/coinbase/chainstorage/internal/blockchain/parser/internal"
	"github.com/coinbase/chainstorage/internal/utils/log"
)

func NewDashNativeParser(params internal.ParserParams, opts ...internal.ParserFactoryOption) (internal.NativeParser, error) {
	v := validator.New()
	v.RegisterStructValidation(validateBitcoinScriptPubKey, BitcoinScriptPubKey{})
	return &bitcoinNativeParserImpl{
		logger:           log.WithPackage(params.Logger),
		validate:         v,
		preprocessTx:     backfillTxHashSingle,
		p2pkhVersionByte: 0x4c,
	}, nil
}
