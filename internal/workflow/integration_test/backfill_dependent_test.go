package integration

import (
	"context"
	"time"

	"github.com/stretchr/testify/suite"
	"go.temporal.io/sdk/testsuite"
	"go.uber.org/fx"

	"github.com/coinbase/chainstorage/internal/blockchain/client"
	"github.com/coinbase/chainstorage/internal/blockchain/jsonrpc"
	"github.com/coinbase/chainstorage/internal/blockchain/parser"
	"github.com/coinbase/chainstorage/internal/blockchain/restapi"
	"github.com/coinbase/chainstorage/internal/cadence"
	"github.com/coinbase/chainstorage/internal/dlq"
	"github.com/coinbase/chainstorage/internal/s3"
	"github.com/coinbase/chainstorage/internal/storage"
	"github.com/coinbase/chainstorage/internal/storage/blobstorage"
	"github.com/coinbase/chainstorage/internal/storage/metastorage"
	"github.com/coinbase/chainstorage/internal/utils/testapp"
	"github.com/coinbase/chainstorage/internal/utils/testutil"
	"github.com/coinbase/chainstorage/internal/workflow"
	"github.com/coinbase/chainstorage/protos/coinbase/c3/common"
)

type (
	backfillerDependencies struct {
		fx.In
		Backfiller  *workflow.Backfiller
		BlobStorage blobstorage.BlobStorage
		MetaStorage metastorage.MetaStorage
	}
	backfillerDependentTestSuite struct {
		suite.Suite
		testsuite.WorkflowTestSuite
		backfillDependencies *backfillerDependencies
	}
)

func (s *backfillerDependentTestSuite) backfillData(startHeight, endHeight uint64, tag uint32, blockchain common.Blockchain, network common.Network) {
	require := testutil.Require(s.T())
	backfillerEnv := cadence.NewTestEnv(s)
	backfillerEnv.SetTestTimeout(10 * time.Minute)

	var deps backfillerDependencies
	app := testapp.New(
		s.T(),
		testapp.WithFunctional(),
		testapp.WithBlockchainNetwork(blockchain, network),
		cadence.WithTestEnv(backfillerEnv),
		workflow.Module,
		client.Module,
		jsonrpc.Module,
		restapi.Module,
		s3.Module,
		storage.Module,
		dlq.Module,
		parser.Module,
		fx.Populate(&deps),
	)
	defer app.Close()

	_, err := deps.Backfiller.Execute(context.Background(), &workflow.BackfillerRequest{
		Tag:                     tag,
		StartHeight:             startHeight,
		EndHeight:               endHeight,
		NumConcurrentExtractors: 2,
		UpdateWatermark:         true,
	})
	require.NoError(err)
	s.backfillDependencies = &deps
}
