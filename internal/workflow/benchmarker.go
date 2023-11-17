package workflow

import (
	"context"
	"fmt"

	"github.com/go-playground/validator/v10"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/workflow"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/cadence"
	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/utils/fxparams"
)

type (
	Benchmarker struct {
		baseWorkflow
		backfillerConfig *config.BackfillerWorkflowConfig
		backfillerName   string
	}

	BenchmarkerParams struct {
		fx.In
		fxparams.Params
		Runtime cadence.Runtime
		Config  *config.Config
	}

	BenchmarkerRequest struct {
		Tag                     uint32
		StartHeight             uint64
		EndHeight               uint64 `validate:"gt=0,gtfield=StartHeight"`
		StepSize                uint64 `validate:"gt=0"`
		SamplesToTest           uint64 `validate:"gt=0"`
		NumConcurrentExtractors int    `validate:"gt=0"`
		MiniBatchSize           uint64
	}
)

func validateStruct(sl validator.StructLevel) {
	req := sl.Current().Interface().(BenchmarkerRequest)
	if req.EndHeight < req.StartHeight+req.StepSize+req.SamplesToTest {
		sl.ReportError(req.EndHeight, "EndHeight", "EndHeight", "e_sss", "")
	}
}

func NewBenchmarker(params BenchmarkerParams) *Benchmarker {
	w := &Benchmarker{
		baseWorkflow:     newBaseWorkflow(&params.Config.Workflows.Benchmarker, params.Runtime),
		backfillerConfig: &params.Config.Workflows.Backfiller,
		backfillerName:   params.Config.Workflows.Backfiller.WorkflowIdentity,
	}
	w.validate.RegisterStructValidation(validateStruct, BenchmarkerRequest{})
	w.registerWorkflow(w.execute)
	return w
}

func (w *Benchmarker) Execute(ctx context.Context, request *BenchmarkerRequest) (client.WorkflowRun, error) {
	return w.startWorkflow(ctx, w.name, request)
}

func (b *Benchmarker) execute(ctx workflow.Context, request *BenchmarkerRequest) error {
	if err := b.validateRequest(request); err != nil {
		return err
	}

	var cfg config.BenchmarkerWorkflowConfig
	if err := b.readConfig(ctx, &cfg); err != nil {
		return xerrors.Errorf("failed to read config: %w", err)
	}

	logger := b.getLogger(ctx).With(
		zap.Reflect("request", request),
	)

	logger.Info("workflow started")
	for childWfStart := request.StartHeight; childWfStart < request.EndHeight; childWfStart += request.StepSize {

		childWfEnd := childWfStart + request.SamplesToTest
		if childWfEnd > request.EndHeight {
			childWfEnd = request.EndHeight
		}
		childWfId := fmt.Sprintf("%s-%s-%d-%d",
			cfg.WorkflowIdentity,
			b.backfillerConfig.WorkflowIdentity,
			childWfStart, childWfEnd,
		)
		cwo := workflow.ChildWorkflowOptions{
			WorkflowID:            childWfId,
			WorkflowRunTimeout:    cfg.ChildWorkflowExecutionStartToCloseTimeout,
			ParentClosePolicy:     enums.PARENT_CLOSE_POLICY_TERMINATE,
			WorkflowIDReusePolicy: enums.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
			WaitForCancellation:   true,
		}
		ctx = workflow.WithChildOptions(ctx, cwo)

		childWfStartTime := workflow.Now(ctx)
		tag := cfg.BlockTag.GetEffectiveBlockTag(request.Tag)
		future := workflow.ExecuteChildWorkflow(ctx, b.backfillerName, &BackfillerRequest{
			Tag:                     tag,
			StartHeight:             childWfStart,
			EndHeight:               childWfEnd,
			NumConcurrentExtractors: request.NumConcurrentExtractors,
			MiniBatchSize:           request.MiniBatchSize,
		})
		if err := future.Get(ctx, nil); err != nil {
			logger.Error("child backfiller workflow failed.", zap.Error(err),
				zap.Reflect("request", request))
			return err
		}

		childWfLatency := workflow.Now(ctx).Sub(childWfStartTime)
		logger.Info(
			"processed child workflow",
			zap.Uint64("childWfStart", childWfStart),
			zap.Uint64("childWfEnd", childWfEnd),
			zap.Float64("blocksPerSecond", float64(childWfEnd-childWfStart)/childWfLatency.Seconds()),
		)
	}

	logger.Info("workflow finished")

	return nil
}
