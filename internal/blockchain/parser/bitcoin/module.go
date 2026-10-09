package bitcoin

import (
	"go.uber.org/fx"

	"github.com/coinbase/chainstorage/internal/blockchain/parser/internal"
)

var Module = fx.Options(
	internal.NewParserBuilder("bitcoin", NewBitcoinNativeParser).
		SetCheckerFactory(NewBitcoinChecker).
		SetRosettaParserFactory(NewBitcoinRosettaParser).
		Build(),
	internal.NewParserBuilder("dash", NewDashNativeParser).
		SetCheckerFactory(NewBitcoinChecker).
		SetRosettaParserFactory(NewBitcoinRosettaParser).
		Build(),
	internal.NewParserBuilder("zcash", NewZcashNativeParser).
		SetCheckerFactory(NewBitcoinChecker).
		SetRosettaParserFactory(NewBitcoinRosettaParser).
		Build(),
)
