package parser

import (
	"context"
	"testing"

	"go.uber.org/fx"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/blockchain/parser/internal"
	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/utils/testapp"
	"github.com/coinbase/chainstorage/internal/utils/testutil"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

func TestNewParser(t *testing.T) {
	testapp.TestAllConfigs(t, func(t *testing.T, cfg *config.Config) {
		require := testutil.Require(t)

		var deps struct {
			fx.In
			Parser internal.Parser
		}
		app := testapp.New(t,
			testapp.WithConfig(cfg),
			Module,
			fx.Populate(&deps),
		)
		defer app.Close()

		require.NotNil(deps.Parser)
	})
}

func TestParserNotImplemented(t *testing.T) {
	testapp.TestAllConfigs(t, func(t *testing.T, cfg *config.Config) {
		require := testutil.Require(t)

		block := &api.Block{}
		var parser internal.Parser
		app := testapp.New(
			t,
			testapp.WithConfig(cfg),
			Module,
			fx.Populate(&parser),
		)
		defer app.Close()
		require.NotNil(parser)

		// Rosetta parsers are not applicable to sidechains
		if cfg.Chain.Sidechain != api.SideChain_SIDECHAIN_NONE {
			return
		}

		_, err := parser.ParseRosettaBlock(context.Background(), block)
		require.Error(err)
		if cfg.Chain.Feature.RosettaParser {
			require.False(xerrors.Is(err, internal.ErrNotImplemented), "'%v' should NOT be ErrNotImplemented", err.Error())
		} else {
			require.True(xerrors.Is(err, internal.ErrNotImplemented), "'%v' should be ErrNotImplemented", err.Error())
		}
	})
}
