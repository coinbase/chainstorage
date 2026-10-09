package workflow

import (
	"context"
	"encoding/hex"
	"sync"
	"sync/atomic"
	"time"

	"github.com/stretchr/testify/mock"
	temporalworkflow "go.temporal.io/sdk/workflow"

	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/utils/testutil"
	"github.com/coinbase/chainstorage/internal/workflow/activity"
)

func (s *batchConsolidatorTestSuite) TestRepairExistingCSCBCompletesThenChecksForNextObject() {
	require := testutil.Require(s.T())
	s.env.OnGetVersion(
		batchConsolidatorRepairParallelismChangeID,
		temporalworkflow.DefaultVersion,
		batchConsolidatorRepairParallelismVersion,
	).Return(temporalworkflow.DefaultVersion)
	s.cfg.Workflows.BatchConsolidator.IrreversibleDistance = 10
	s.mockAutoConsolidateLatestHeight(2000)
	var requests []*activity.BatchConsolidatorRequest
	s.env.OnActivity(activity.ActivityBatchConsolidator, mock.Anything, mock.Anything).
		Return(func(_ context.Context, request *activity.BatchConsolidatorRequest) (*activity.BatchConsolidatorResponse, error) {
			requests = append(requests, request)
			if len(requests) > 1 {
				return &activity.BatchConsolidatorResponse{
					StartHeight: request.StartHeight,
					EndHeight:   request.EndHeight,
				}, nil
			}
			return &activity.BatchConsolidatorResponse{
				StartHeight:        1000,
				EndHeight:          1100,
				ScannedBlocks:      100,
				ConsolidatedBlocks: 100,
				ObjectKey:          "consolidated/clean.cscb.zstd",
				OldObjectKey:       "consolidated/dirty.cscb.zstd",
				RepairedObjects:    1,
			}, nil
		}).Twice()

	_, err := s.batchConsolidator.Execute(context.Background(), &BatchConsolidatorRequest{
		Mode:        config.ConsolidationModeRepairExistingCSCB,
		Tag:         2,
		StartHeight: 1000,
		EndHeight:   1100,
		MaxBlocks:   100,
	})
	require.NoError(err)
	require.Len(requests, 2)
	for _, request := range requests {
		require.Equal(config.ConsolidationModeRepairExistingCSCB, request.Mode)
		require.Equal(uint32(2), request.Tag)
		require.Equal(uint64(1000), request.StartHeight)
		require.Equal(uint64(1100), request.EndHeight)
		require.Equal(uint64(100), request.MaxBlocks)
		require.True(isRepairExecutionKey(request.RepairExecutionKey))
	}
	require.NotEqual(requests[0].RepairExecutionKey, requests[1].RepairExecutionKey)
}

func isRepairExecutionKey(value string) bool {
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == 32
}

func (s *batchConsolidatorTestSuite) TestRepairExistingCSCBRequiresSerialExecution() {
	require := testutil.Require(s.T())
	s.env.OnGetVersion(
		batchConsolidatorRepairParallelismChangeID,
		temporalworkflow.DefaultVersion,
		batchConsolidatorRepairParallelismVersion,
	).Return(temporalworkflow.DefaultVersion)

	_, err := s.batchConsolidator.Execute(context.Background(), &BatchConsolidatorRequest{
		Mode:        config.ConsolidationModeRepairExistingCSCB,
		Tag:         2,
		StartHeight: 1000,
		EndHeight:   1100,
		MaxBlocks:   100,
		Parallelism: 2,
	})
	require.Error(err)
	require.Contains(err.Error(), "legacy repair_existing_cscb execution requires parallelism=1")
}

func (s *batchConsolidatorTestSuite) TestRepairExistingCSCBProcessesDistinctObjectsInParallel() {
	require := testutil.Require(s.T())
	s.env.OnGetVersion(
		batchConsolidatorRepairParallelismChangeID,
		temporalworkflow.DefaultVersion,
		batchConsolidatorRepairParallelismVersion,
	).Return(temporalworkflow.Version(batchConsolidatorRepairParallelismVersion))
	s.cfg.Workflows.BatchConsolidator.IrreversibleDistance = 10
	s.mockAutoConsolidateLatestHeight(2000)

	objectKeys := []string{
		"consolidated/dirty-1100-1200.cscb.zstd",
		"consolidated/dirty-1000-1100.cscb.zstd",
	}
	var candidateCalls atomic.Int32
	s.env.OnActivity(activity.ActivityBatchConsolidatorRepairCandidates, mock.Anything, mock.Anything).
		Return(func(_ context.Context, request *activity.BatchConsolidatorRepairCandidatesRequest) (*activity.BatchConsolidatorRepairCandidatesResponse, error) {
			require.Equal(uint32(2), request.Tag)
			require.Equal(uint64(1000), request.StartHeight)
			require.Equal(uint64(1200), request.EndHeight)
			require.Equal(2, request.Limit)
			if candidateCalls.Add(1) == 1 {
				return &activity.BatchConsolidatorRepairCandidatesResponse{ObjectKeys: objectKeys}, nil
			}
			return &activity.BatchConsolidatorRepairCandidatesResponse{}, nil
		}).Twice()

	var inFlight atomic.Int32
	var maxInFlight atomic.Int32
	var requestsMu sync.Mutex
	requests := make([]*activity.BatchConsolidatorRequest, 0, len(objectKeys))
	s.env.OnActivity(activity.ActivityBatchConsolidator, mock.Anything, mock.Anything).
		Return(func(_ context.Context, request *activity.BatchConsolidatorRequest) (*activity.BatchConsolidatorResponse, error) {
			requestsMu.Lock()
			requests = append(requests, request)
			requestsMu.Unlock()
			current := inFlight.Add(1)
			for {
				previous := maxInFlight.Load()
				if current <= previous || maxInFlight.CompareAndSwap(previous, current) {
					break
				}
			}
			time.Sleep(50 * time.Millisecond)
			inFlight.Add(-1)
			return &activity.BatchConsolidatorResponse{
				StartHeight:        1000,
				EndHeight:          1100,
				ScannedBlocks:      100,
				ConsolidatedBlocks: 100,
				ObjectKey:          request.RepairObjectKey + ".clean",
				OldObjectKey:       request.RepairObjectKey,
				RepairedObjects:    1,
			}, nil
		}).Twice()

	_, err := s.batchConsolidator.Execute(context.Background(), &BatchConsolidatorRequest{
		Mode:        config.ConsolidationModeRepairExistingCSCB,
		Tag:         2,
		StartHeight: 1000,
		EndHeight:   1200,
		MaxBlocks:   100,
		Parallelism: 2,
	})
	require.NoError(err)
	require.Equal(int32(2), maxInFlight.Load())
	require.Len(requests, 2)
	require.ElementsMatch(objectKeys, []string{requests[0].RepairObjectKey, requests[1].RepairObjectKey})
	require.True(isRepairExecutionKey(requests[0].RepairExecutionKey))
	require.True(isRepairExecutionKey(requests[1].RepairExecutionKey))
	require.NotEqual(requests[0].RepairExecutionKey, requests[1].RepairExecutionKey)
}
