package ethereum

import (
	"context"
	"testing"

	"github.com/stretchr/testify/suite"
	"go.uber.org/fx"
	"go.uber.org/mock/gomock"

	"github.com/coinbase/chainstorage/internal/blockchain/parser/internal"

	"github.com/coinbase/chainstorage/internal/utils/testapp"
	"github.com/coinbase/chainstorage/internal/utils/testutil"
	"github.com/coinbase/chainstorage/protos/coinbase/c3/common"
)

const (
	baseTag          uint32 = 1
	baseHeight       uint64 = 0x5ea32
	baseParentHeight uint64 = 0x5ea31
	baseHash                = "0xcd2bbaef960686844a016bb301c60e5726d8e71db04ee19014ee3dfada7351b4"
	baseParentHash          = "0xc942f74871d6ee46c3fcf3c715508f27f7580f237b5c2ee85e7ec8db9bb80538"
)

type baseParserTestSuite struct {
	suite.Suite

	controller *gomock.Controller
	testapp    testapp.TestApp
	parser     internal.Parser
}

func TestBaseParserTestSuite(t *testing.T) {
	suite.Run(t, new(baseParserTestSuite))
}

func (s *baseParserTestSuite) SetupTest() {
	s.controller = gomock.NewController(s.T())

	var parser internal.Parser
	s.testapp = testapp.New(
		s.T(),
		Module,
		internal.Module,
		testapp.WithBlockchainNetwork(common.Blockchain_BLOCKCHAIN_BASE, common.Network_NETWORK_BASE_GOERLI),
		fx.Populate(&parser),
	)

	s.parser = parser
	s.NotNil(s.parser)
}

func (s *baseParserTestSuite) TearDownTest() {
	s.testapp.Close()
	s.controller.Finish()
}

func (s *baseParserTestSuite) TestParseBlock1041847() {
	require := testutil.Require(s.T())

	rawBlock, err := testutil.LoadRawBlock("parser/base/goerli/block_1041847_raw_base_goerli.json")

	require.NoError(err)

	nativeBlock, err := s.parser.ParseNativeBlock(context.Background(), rawBlock)
	require.NoError(err)
	require.NotNil(nativeBlock)
}
