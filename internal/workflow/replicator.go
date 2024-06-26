package workflow

import (
	"context"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/workflow"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"golang.org/x/xerrors"
	"sort"
	"strconv"

	"github.com/coinbase/chainstorage/internal/cadence"
	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/utils/fxparams"
	"github.com/coinbase/chainstorage/internal/utils/utils"
	"github.com/coinbase/chainstorage/internal/workflow/activity"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

type (
	Replicator struct {
		baseWorkflow
		replicator      *activity.Replicator
		updateWatermark *activity.UpdateWatermark
	}

	ReplicatorParams struct {
		fx.In
		fxparams.Params
		Runtime         cadence.Runtime
		Replicator      *activity.Replicator
		UpdateWatermark *activity.UpdateWatermark
	}

	ReplicatorRequest struct {
		Tag             uint32
		StartHeight     uint64
		EndHeight       uint64 `validate:"gt=0,gtfield=StartHeight"`
		UpdateWatermark bool
		DataCompression string // Optional. If not specified, it is read from the workflow config.
		BatchSize       uint64 // Optional. If not specified, it is read from the workflow config.
		MiniBatchSize   uint64 // Optional. If not specified, it is read from the workflow config.
		CheckpointSize  uint64 // Optional. If not specified, it is read from the workflow config.
		Parallelism     int    // Optional. If not specified, it is read from the workflow config.
	}
)

const (
	// Replicator metrics. need to have `workflow.replicator` as prefix
	replicatorHeightGauge             = "workflow.replicator.height"
	replicatorGapGauge                = "workflow.replicator.gap"
	replicatorTimeSinceLastBlockGauge = "workflow.replicator.time_since_last_block"
)

// GetTags implements InstrumentedRequest.
func (r *ReplicatorRequest) GetTags() map[string]string {
	return map[string]string{
		tagBlockTag: strconv.Itoa(int(r.Tag)),
	}
}

var (
	_ InstrumentedRequest = (*ReplicatorRequest)(nil)
)

func NewReplicator(params ReplicatorParams) *Replicator {
	w := &Replicator{
		baseWorkflow:    newBaseWorkflow(&params.Config.Workflows.Replicator, params.Runtime),
		replicator:      params.Replicator,
		updateWatermark: params.UpdateWatermark,
	}
	w.registerWorkflow(w.execute)
	return w
}

func (w *Replicator) Execute(ctx context.Context, request *ReplicatorRequest) (client.WorkflowRun, error) {
	return w.startWorkflow(ctx, w.name, request)
}

func (w *Replicator) execute(ctx workflow.Context, request *ReplicatorRequest) error {
	return w.executeWorkflow(ctx, request, func() error {
		if err := w.validateRequest(request); err != nil {
			return err
		}

		var cfg config.ReplicatorWorkflowConfig
		if err := w.readConfig(ctx, &cfg); err != nil {
			return xerrors.Errorf("failed to read config: %w", err)
		}

		if !request.UpdateWatermark {
			// Set the default
			request.UpdateWatermark = true
		}

		batchSize := cfg.BatchSize
		if request.BatchSize > 0 {
			batchSize = request.BatchSize
		}

		miniBatchSize := cfg.MiniBatchSize
		if request.MiniBatchSize > 0 {
			miniBatchSize = request.MiniBatchSize
		}

		checkpointSize := cfg.CheckpointSize
		if request.CheckpointSize > 0 {
			checkpointSize = request.CheckpointSize
		}

		parallelism := cfg.Parallelism
		if request.Parallelism > 0 {
			parallelism = request.Parallelism
		}

		var dataCompression api.Compression
		var err error
		dataCompression = cfg.Storage.DataCompression
		if request.DataCompression != "" {
			dataCompression, err = utils.ParseCompression(request.DataCompression)
			if err != nil {
				return xerrors.Errorf("failed to parse data compression: %w", err)
			}
		}

		tag := cfg.GetEffectiveBlockTag(request.Tag)
		logger := w.getLogger(ctx).With(
			zap.Reflect("request", request),
			zap.Reflect("config", cfg),
		)

		logger.Info("workflow started", zap.Uint64("batchSize", batchSize))
		ctx = w.withActivityOptions(ctx)

		metrics := w.runtime.GetMetricsHandler(ctx).WithTags(map[string]string{
			tagBlockTag: strconv.Itoa(int(request.Tag)),
		})
		for startHeight := request.StartHeight; startHeight < request.EndHeight; startHeight = startHeight + batchSize {
			if startHeight >= request.StartHeight+checkpointSize {
				newRequest := *request
				newRequest.StartHeight = startHeight
				logger.Info(
					"checkpoint reached",
					zap.Reflect("newRequest", newRequest),
				)
				return workflow.NewContinueAsNewError(ctx, w.name, &newRequest)
			}
			endHeight := startHeight + batchSize
			if endHeight > request.EndHeight {
				endHeight = request.EndHeight
			}

			wg := workflow.NewWaitGroup(ctx)
			wg.Add(parallelism)
			miniBatchCount := int((endHeight-startHeight-1)/miniBatchSize + 1)
			inputChannel := workflow.NewNamedBufferedChannel(ctx, "replicator.input", miniBatchCount)
			for batchStart := startHeight; batchStart < endHeight; batchStart = batchStart + miniBatchSize {
				inputChannel.Send(ctx, batchStart)
			}
			inputChannel.Close()

			reprocessChannel := workflow.NewNamedBufferedChannel(ctx, "replicator.reprocess", miniBatchCount)
			defer reprocessChannel.Close()

			var responses []activity.ReplicatorResponse

			// Phase 1: running mini batches in parallel.
			for i := 0; i < parallelism; i++ {
				workflow.Go(ctx, func(ctx workflow.Context) {
					defer wg.Done()
					for {
						var batchStart uint64
						if ok := inputChannel.Receive(ctx, &batchStart); !ok {
							break
						}
						batchEnd := batchStart + miniBatchSize
						if batchEnd > endHeight {
							batchEnd = endHeight
						}
						replicatorResponse, err := w.replicator.Execute(ctx, &activity.ReplicatorRequest{
							Tag:         tag,
							StartHeight: batchStart,
							EndHeight:   batchEnd,
							Parallelism: parallelism,
							Compression: dataCompression,
						})
						if err != nil {
							reprocessChannel.Send(ctx, batchStart)
							logger.Warn(
								"queued for reprocessing",
								zap.Uint64("batchStart", batchStart),
								zap.Error(err),
							)
						}
						responses = append(responses, *replicatorResponse)
					}
				})
			}
			wg.Wait(ctx)

			// Phase 2: reprocess any failed batches sequentially.
			// This should happen rarely (only if we over stress the cluster or the cluster itself was crashing).
			for {
				var batchStart uint64
				if ok := reprocessChannel.ReceiveAsync(&batchStart); !ok {
					break
				}
				batchEnd := batchStart + miniBatchSize
				if batchEnd > endHeight {
					batchEnd = endHeight
				}
				retryResponse, err := w.replicator.Execute(ctx, &activity.ReplicatorRequest{
					Tag:         tag,
					StartHeight: batchStart,
					EndHeight:   batchEnd,
					Parallelism: parallelism,
					Compression: dataCompression,
				})
				if err != nil {
					return xerrors.Errorf("failed to replicate block from %d to %d: %w", batchStart, batchEnd, err)
				}
				responses = append(responses, *retryResponse)
			}

			sort.Slice(responses, func(i, j int) bool {
				return responses[i].LatestBlockHeight < responses[j].LatestBlockHeight
			})

			// Phase 3: update watermark
			if request.UpdateWatermark {
				_, err := w.updateWatermark.Execute(ctx, &activity.UpdateWatermarkRequest{
					Tag:           request.Tag,
					ValidateStart: startHeight - 1,
					BlockHeight:   endHeight - 1,
				})
				if err != nil {
					return xerrors.Errorf("failed to update watermark: %w", err)
				}
			}

			if len(responses) > 0 {
				metrics.Gauge(replicatorHeightGauge).Update(float64(responses[len(responses)-1].LatestBlockHeight))
				metrics.Gauge(replicatorGapGauge).Update(float64(request.EndHeight - responses[len(responses)-1].LatestBlockHeight + 1))
				metrics.Gauge(replicatorTimeSinceLastBlockGauge).Update(utils.SinceTimestamp(responses[len(responses)-1].LatestBlockTimestamp).Seconds())
			}
		}

		logger.Info("workflow finished")
		return nil
	})
}
