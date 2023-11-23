package rosetta

import (
	"go.uber.org/fx"

	"github.com/coinbase/chainstorage/internal/blockchain/parser/internal"
)

var Module = fx.Options(
	internal.NewParserBuilder("rosetta", NewRosettaNativeParser).
		SetRosettaParserFactory(NewRosettaRosettaParser).
		SetCheckerFactory(NewRosettaChecker).
		Build(),
)
