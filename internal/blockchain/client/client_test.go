package client

import (
	"testing"

	"go.uber.org/fx"

	"github.com/coinbase/chainstorage/internal/blockchain/jsonrpc"
	"github.com/coinbase/chainstorage/internal/blockchain/parser"
	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/dlq"
	"github.com/coinbase/chainstorage/internal/utils/testapp"
	"github.com/coinbase/chainstorage/internal/utils/testutil"
)

func TestNewClient(t *testing.T) {
	testapp.TestAllConfigs(t, func(t *testing.T, cfg *config.Config) {
		require := testutil.Require(t)

		var deps struct {
			fx.In
			MasterClient    Client `name:"master"`
			SlaveClient     Client `name:"slave"`
			ValidatorClient Client `name:"validator"`
		}
		app := testapp.New(t,
			testapp.WithConfig(cfg),
			Module,
			jsonrpc.Module,
			parser.Module,
			fx.Provide(dlq.NewNop),
			fx.Populate(&deps),
		)
		defer app.Close()

		require.NotNil(deps.MasterClient)
		require.NotNil(deps.SlaveClient)
		require.NotNil(deps.ValidatorClient)
	})
}
