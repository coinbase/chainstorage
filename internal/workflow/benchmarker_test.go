package workflow

import (
	"context"
	"fmt"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/mock"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
	"go.uber.org/fx"

	"github.com/stretchr/testify/suite"

	"github.com/coinbase/chainstorage/internal/blockchain/client"
	clientmocks "github.com/coinbase/chainstorage/internal/blockchain/client/mocks"
	"github.com/coinbase/chainstorage/internal/cadence"
	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/dlq"
	"github.com/coinbase/chainstorage/internal/storage/blobstorage"
	blobstoragemocks "github.com/coinbase/chainstorage/internal/storage/blobstorage/mocks"
	"github.com/coinbase/chainstorage/internal/storage/metastorage"
	metastoragemocks "github.com/coinbase/chainstorage/internal/storage/metastorage/mocks"
	"github.com/coinbase/chainstorage/internal/utils/testapp"
	"github.com/coinbase/chainstorage/internal/utils/testutil"
)

type benchmarkerTestSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite
	env         *cadence.TestEnv
	ctrl        *gomock.Controller
	app         testapp.TestApp
	benchmarker *Benchmarker
	config      *config.Config
}

func TestBenchmarkerTestSuite(t *testing.T) {
	suite.Run(t, new(benchmarkerTestSuite))
}

func (s *benchmarkerTestSuite) SetupTest() {
	s.env = cadence.NewTestEnv(s)
	s.ctrl = gomock.NewController(s.T())
	blockchainClient := clientmocks.NewMockClient(s.ctrl)
	metadataAccessor := metastoragemocks.NewMockMetaStorage(s.ctrl)
	blobStorage := blobstoragemocks.NewMockBlobStorage(s.ctrl)
	s.app = testapp.New(
		s.T(),
		Module,
		cadence.WithTestEnv(s.env),
		fx.Provide(func() blobstorage.BlobStorage {
			return blobStorage
		}),
		fx.Provide(fx.Annotated{
			Name: "slave",
			Target: func() client.Client {
				return blockchainClient
			},
		}),
		fx.Provide(func() metastorage.MetaStorage {
			return metadataAccessor
		}),
		fx.Provide(dlq.NewNop),
		fx.Invoke(NewBackfiller),
		fx.Populate(&s.config),
		fx.Populate(&s.benchmarker),
	)
}

func (s *benchmarkerTestSuite) TearDownTest() {
	s.app.Close()
	s.ctrl.Finish()
}

func (s *benchmarkerTestSuite) TestBenchmarker() {
	const (
		tag                     = uint32(1)
		startHeight             = 1
		endHeight               = 12000000
		stepSize                = 1000000
		samplesToTest           = 10
		numConcurrentExtractors = 4
	)

	require := testutil.Require(s.T())

	seen := make(map[string]bool)
	expect := make(map[string]bool)
	numSubWorkflows := 0
	for childWfStart := startHeight; childWfStart < endHeight; childWfStart += stepSize {
		childWfEnd := childWfStart + samplesToTest
		if childWfEnd > endHeight {
			childWfEnd = endHeight
		}
		key := fmt.Sprintf("%d-%d", childWfStart, childWfEnd)
		expect[key] = true
		numSubWorkflows += 1
	}

	s.env.OnWorkflow(s.config.Workflows.Backfiller.WorkflowIdentity, mock.Anything, mock.Anything).
		Times(numSubWorkflows).
		Return(func(ctx workflow.Context, request *BackfillerRequest) error {
			require.Equal(tag, request.Tag)
			key := fmt.Sprintf("%d-%d", request.StartHeight, request.EndHeight)
			_, ok := seen[key]
			require.False(ok)
			seen[key] = true
			return nil
		})

	_, err := s.benchmarker.Execute(context.Background(), &BenchmarkerRequest{
		Tag:                     tag,
		StartHeight:             startHeight,
		EndHeight:               endHeight,
		StepSize:                stepSize,
		SamplesToTest:           samplesToTest,
		NumConcurrentExtractors: numConcurrentExtractors,
	})
	require.NoError(err)

	require.Equal(expect, seen)
}

func (s *benchmarkerTestSuite) TestBenchmarker_ValidateRequest() {
	require := testutil.Require(s.T())

	// requires start + step + sample <= end
	_, err := s.benchmarker.Execute(context.Background(), &BenchmarkerRequest{
		Tag:                     tag,
		StartHeight:             100,
		EndHeight:               101,
		StepSize:                1,
		SamplesToTest:           1,
		NumConcurrentExtractors: numConcurrentExtractors,
	})
	require.Error(err)
	require.Contains(err.Error(), "invalid workflow request")
	require.Contains(err.Error(), "Field validation for 'EndHeight' failed on the 'e_sss' tag")

	// requires step > 0
	_, err = s.benchmarker.Execute(context.Background(), &BenchmarkerRequest{
		Tag:                     tag,
		StartHeight:             startHeight,
		EndHeight:               endHeight,
		StepSize:                0,
		SamplesToTest:           1,
		NumConcurrentExtractors: numConcurrentExtractors,
	})
	require.Error(err)
	require.Contains(err.Error(), "invalid workflow request")
	require.Contains(err.Error(), "Field validation for 'StepSize' failed on the 'gt' tag")

	// requires sample > 0
	_, err = s.benchmarker.Execute(context.Background(), &BenchmarkerRequest{
		Tag:                     tag,
		StartHeight:             startHeight,
		EndHeight:               endHeight,
		StepSize:                1,
		SamplesToTest:           0,
		NumConcurrentExtractors: numConcurrentExtractors,
	})
	require.Error(err)
	require.Contains(err.Error(), "invalid workflow request")
	require.Contains(err.Error(), "Field validation for 'SamplesToTest' failed on the 'gt' tag")

	// requires extractors > 0
	_, err = s.benchmarker.Execute(context.Background(), &BenchmarkerRequest{
		Tag:                     tag,
		StartHeight:             startHeight,
		EndHeight:               endHeight,
		StepSize:                1,
		SamplesToTest:           1,
		NumConcurrentExtractors: 0,
	})
	require.Error(err)
	require.Contains(err.Error(), "invalid workflow request")
	require.Contains(err.Error(), "Field validation for 'NumConcurrentExtractors' failed on the 'gt' tag")
}
