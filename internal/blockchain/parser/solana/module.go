package solana

import (
	"go.uber.org/fx"

	"github.com/coinbase/chainstorage/internal/blockchain/parser/internal"
)

var Module = fx.Options(
	internal.NewParserBuilder("solana", NewSolanaNativeParser).
		SetRosettaParserFactory(NewSolanaRosettaParser).
		SetCheckerFactory(NewSolanaChecker).
		Build(),
)
