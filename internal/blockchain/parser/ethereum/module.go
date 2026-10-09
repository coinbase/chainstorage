package ethereum

import (
	"go.uber.org/fx"

	"github.com/coinbase/chainstorage/internal/blockchain/parser/ethereum/beacon"
	"github.com/coinbase/chainstorage/internal/blockchain/parser/internal"
)

var Module = fx.Options(
	internal.NewParserBuilder("bsc", NewBscNativeParser).
		Build(),
	internal.NewParserBuilder("ethereum", NewEthereumNativeParser).
		SetRosettaParserFactory(NewEthereumRosettaParser).
		SetCheckerFactory(NewEthereumChecker).
		SetValidatorFactory(NewEthereumValidator).
		Build(),
	internal.NewParserBuilder("polygon", NewPolygonNativeParser).
		SetRosettaParserFactory(NewPolygonRosettaParser).
		SetCheckerFactory(NewPolygonChecker).
		SetValidatorFactory(NewPolygonValidator).
		Build(),
	internal.NewParserBuilder("avacchain", NewAvacchainNativeParser).
		SetCheckerFactory(NewAvacchainChecker).
		Build(),
	internal.NewParserBuilder("arbitrum", NewArbitrumNativeParser).
		SetCheckerFactory(NewArbitrumChecker).
		Build(),
	internal.NewParserBuilder("optimism", NewOptimismNativeParser).
		SetValidatorFactory(NewOptimismValidator).
		Build(),
	internal.NewParserBuilder("base", NewBaseNativeParser).
		SetRosettaParserFactory(NewBaseRosettaParser).
		SetCheckerFactory(NewBaseChecker).
		SetValidatorFactory(NewBaseValidator).
		Build(),
	internal.NewParserBuilder("fantom", NewFantomNativeParser).
		Build(),
	internal.NewParserBuilder("tron", NewTronNativeParser).
		SetValidatorFactory(NewBaseValidator).
		Build(),
	internal.NewParserBuilder("story", NewStoryNativeParser).
		SetValidatorFactory(NewStoryValidator).
		Build(),
	internal.NewParserBuilder("ethereumclassic", NewEthereumClassicNativeParser).
		SetValidatorFactory(NewEthereumClassicValidator).
		Build(),
	internal.NewParserBuilder("plasma", NewPlasmaNativeParser).
		SetValidatorFactory(NewPlasmaValidator).
		Build(),
	internal.NewParserBuilder("monad", NewMonadNativeParser).
		SetValidatorFactory(NewMonadValidator).
		Build(),
	internal.NewParserBuilder("abstract", NewAbstractNativeParser).
		SetValidatorFactory(NewAbstractValidator).
		Build(),
	internal.NewParserBuilder("megaeth", NewMegaethNativeParser).
		SetValidatorFactory(NewMegaethValidator).
		Build(),
	internal.NewParserBuilder("seismic", NewSeismicNativeParser).
		SetValidatorFactory(NewSeismicValidator).
		Build(),
	internal.NewParserBuilder("tempo", NewTempoNativeParser).
		SetValidatorFactory(NewTempoValidator).
		Build(),
	internal.NewParserBuilder("robinhood", NewRobinhoodNativeParser).
		SetValidatorFactory(NewRobinhoodValidator).
		Build(),
	internal.NewParserBuilder("arc", NewArcNativeParser).
		SetValidatorFactory(NewArcValidator).
		Build(),
	beacon.Module,
)
