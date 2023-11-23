package ethereum

import (
	"context"
	"testing"

	"github.com/stretchr/testify/suite"
	"go.uber.org/fx"

	"github.com/coinbase/chainstorage/internal/blockchain/parser/internal"
	"github.com/coinbase/chainstorage/internal/utils/fixtures"
	"github.com/coinbase/chainstorage/internal/utils/testapp"
	"github.com/coinbase/chainstorage/internal/utils/testutil"
	"github.com/coinbase/chainstorage/protos/coinbase/c3/common"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

type ethereumPreProcessorTestSuite struct {
	suite.Suite
	app          testapp.TestApp
	checker      internal.Checker
	preProcessor internal.PreProcessor
}

func TestEthereumPreProcessorTestSuite(t *testing.T) {
	suite.Run(t, new(ethereumPreProcessorTestSuite))
}

func (s *ethereumPreProcessorTestSuite) SetupTest() {
	s.app = testapp.New(
		s.T(),
		testapp.WithBlockchainNetwork(common.Blockchain_BLOCKCHAIN_ETHEREUM, common.Network_NETWORK_ETHEREUM_MAINNET),
		fx.Provide(NewEthereumChecker),
		fx.Populate(&s.checker),
	)
	s.preProcessor = s.checker.GetPreProcessor()
}

func (s *ethereumPreProcessorTestSuite) TearDownTest() {
	s.app.Close()
}

func (s *ethereumPreProcessorTestSuite) TestPreProcessNativeBlock() {
	require := testutil.Require(s.T())

	var expectedBlock api.NativeBlock
	fixtures.MustUnmarshalPB("parser/avacchain/native_block_31878202_altered.json", &expectedBlock)

	var actualBlock api.NativeBlock
	fixtures.MustUnmarshalPB("parser/avacchain/native_block_31878202.json", &actualBlock)

	ctx := context.Background()
	err := s.checker.CompareNativeBlocks(ctx, 31878202, &expectedBlock, &actualBlock)
	require.NoError(err)
}
