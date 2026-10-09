package workflow

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/workflow"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/cadence"
	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/utils/fxparams"
	"github.com/coinbase/chainstorage/internal/workflow/activity"
)

type (
	Migrator struct {
		baseWorkflow
		migrator                   *activity.Migrator
		getLatestBlockHeight       *activity.GetLatestBlockHeightActivity
		getLatestBlockFromPostgres *activity.GetLatestBlockFromPostgresActivity
		getLatestEventFromPostgres *activity.GetLatestEventFromPostgresActivity
		getMaxEventId              *activity.GetMaxEventIdActivity
	}

	MigratorParams struct {
		fx.In
		fxparams.Params
		Runtime                    cadence.Runtime
		Migrator                   *activity.Migrator
		GetLatestBlockHeight       *activity.GetLatestBlockHeightActivity
		GetLatestBlockFromPostgres *activity.GetLatestBlockFromPostgresActivity
		GetLatestEventFromPostgres *activity.GetLatestEventFromPostgresActivity
		GetMaxEventId              *activity.GetMaxEventIdActivity
	}

	MigratorRequest struct {
		StartEventSequence int64 // Start event sequence
		EndEventSequence   int64 // End event sequence (0 = auto-detect)
		EventTag           uint32
		Tag                uint32
		BatchSize          uint64 // Optional. If not specified, it is read from the workflow config.
		CheckpointSize     uint64 // Optional. If not specified, it is read from the workflow config.
		Parallelism        int    // Optional. If not specified, it is read from the workflow config.
		BackoffInterval    string // Optional. If not specified, it is read from the workflow config.
		ContinuousSync     bool   // Optional. Whether to continuously sync data in infinite loop mode
		SyncInterval       string // Optional. Interval for continuous sync (e.g., "1m", "30s"). Defaults to 1 minute if not specified or invalid.
		AutoResume         bool   // Optional. Automatically determine StartEventSequence from latest event in PostgreSQL destination
	}
)

var (
	_ InstrumentedRequest = (*MigratorRequest)(nil)
)

const (
	// migrator metrics. need to have `workflow.migrator` as prefix
	migratorHeightGauge   = "workflow.migrator.height"
	migratorBlocksCounter = "workflow.migrator.blocks_migrated"
	migratorEventsCounter = "workflow.migrator.events_migrated"
	migratorProgressGauge = "workflow.migrator.progress"
)

func NewMigrator(params MigratorParams) *Migrator {
	w := &Migrator{
		baseWorkflow:               newBaseWorkflow(&params.Config.Workflows.Migrator, params.Runtime),
		migrator:                   params.Migrator,
		getLatestBlockHeight:       params.GetLatestBlockHeight,
		getLatestBlockFromPostgres: params.GetLatestBlockFromPostgres,
		getLatestEventFromPostgres: params.GetLatestEventFromPostgres,
		getMaxEventId:              params.GetMaxEventId,
	}
	w.registerWorkflow(w.execute)
	return w
}

func (w *Migrator) Execute(ctx context.Context, request *MigratorRequest) (client.WorkflowRun, error) {
	workflowID := w.name
	if request.Tag != 0 {
		workflowID = fmt.Sprintf("%s/block_tag=%d", w.name, request.Tag)
	}
	return w.startMigratorWorkflow(ctx, workflowID, request)
}

// startMigratorWorkflow starts a migrator workflow with a custom reuse policy
// that allows restarting failed workflows but prevents concurrent execution
func (w *Migrator) startMigratorWorkflow(ctx context.Context, workflowID string, request *MigratorRequest) (client.WorkflowRun, error) {
	if err := w.validateRequestCtx(ctx, request); err != nil {
		return nil, err
	}
	cfg := w.config.Base()
	workflowOptions := client.StartWorkflowOptions{
		ID:                                       workflowID,
		TaskQueue:                                cfg.TaskList,
		WorkflowRunTimeout:                       cfg.WorkflowRunTimeout,
		WorkflowExecutionErrorWhenAlreadyStarted: true,
		RetryPolicy:                              w.getRetryPolicy(cfg.WorkflowRetry),
	}
	execution, err := w.runtime.ExecuteWorkflow(ctx, workflowOptions, w.name, request)
	if err != nil {
		return nil, xerrors.Errorf("failed to execute workflow: %w", err)
	}
	return execution, nil
}

func (w *Migrator) execute(ctx workflow.Context, request *MigratorRequest) error {
	return w.executeWorkflow(ctx, request, func() error {
		if err := w.validateRequest(request); err != nil {
			return err
		}

		var cfg config.MigratorWorkflowConfig
		if err := w.readConfig(ctx, &cfg); err != nil {
			return xerrors.Errorf("failed to read config: %w", err)
		}

		// Event-driven migration always processes both blocks and events

		batchSize := cfg.BatchSize
		if request.BatchSize > 0 {
			batchSize = request.BatchSize
		}

		checkpointSize := cfg.CheckpointSize
		if request.CheckpointSize > 0 {
			checkpointSize = request.CheckpointSize
		}

		parallelism := cfg.Parallelism
		if parallelism <= 0 {
			parallelism = 1 // Fallback default if config not set
		}
		if request.Parallelism > 0 {
			parallelism = request.Parallelism
		}

		backoffInterval := cfg.BackoffInterval
		if request.BackoffInterval != "" {
			parsedInterval, err := time.ParseDuration(request.BackoffInterval)
			if err != nil {
				return xerrors.Errorf("failed to parse backoff interval: %w", err)
			}
			backoffInterval = parsedInterval
		}

		// Use config's continuous sync setting as default if not explicitly set in request
		continuousSync := cfg.ContinuousSync || request.ContinuousSync

		syncInterval := defaultSyncInterval
		if cfg.SyncInterval > 0 {
			syncInterval = cfg.SyncInterval
		}
		if request.SyncInterval != "" {
			interval, err := time.ParseDuration(request.SyncInterval)
			if err == nil {
				syncInterval = interval
			}
		}

		tag := cfg.GetEffectiveBlockTag(request.Tag)
		eventTag := cfg.GetEffectiveEventTag(request.EventTag)
		metrics := w.getMetricsHandler(ctx).WithTags(map[string]string{
			tagBlockTag: strconv.Itoa(int(tag)),
		})
		logger := w.getLogger(ctx).With(
			zap.Reflect("request", request),
			zap.Reflect("config", cfg),
			zap.Uint32("effectiveEventTag", eventTag),
		)

		// Set up activity options early so we can use activities
		ctx = w.withActivityOptions(ctx)

		// Handle auto-resume functionality - use latest event sequence
		if request.AutoResume && request.StartEventSequence == 0 {
			logger.Info("AutoResume enabled, querying PostgreSQL destination for latest migrated event")
			postgresEventResp, err := w.getLatestEventFromPostgres.Execute(ctx, &activity.GetLatestEventFromPostgresRequest{EventTag: eventTag})
			if err != nil {
				return xerrors.Errorf("failed to get latest event from PostgreSQL: %w", err)
			}

			if postgresEventResp.Found {
				// Resume from the next event sequence
				request.StartEventSequence = postgresEventResp.Sequence + 1
				logger.Info("Auto-resume: found latest event in PostgreSQL destination",
					zap.Int64("latestEventSequence", postgresEventResp.Sequence),
					zap.Int64("resumeFromSequence", request.StartEventSequence))
			} else {
				// No events found in destination, start from the beginning
				request.StartEventSequence = 1 // Events start at 1
				logger.Info("Auto-resume: no events found in PostgreSQL destination, starting from beginning")
			}
		}

		// Handle end event sequence auto-detection if not provided
		if request.EndEventSequence == 0 {
			logger.Info("No end event sequence provided, fetching max event ID from DynamoDB...")

			// Query DynamoDB for the actual max event ID
			maxEventResp, err := w.getMaxEventId.Execute(ctx, &activity.GetMaxEventIdRequest{
				EventTag: eventTag,
			})
			if err != nil {
				return xerrors.Errorf("failed to get max event ID from DynamoDB: %w", err)
			}

			if !maxEventResp.Found {
				logger.Warn("No events found in DynamoDB")
				if continuousSync {
					// In continuous sync, if no events exist yet, wait and retry
					logger.Info("No events in DynamoDB, waiting for sync interval before retry",
						zap.Duration("syncInterval", syncInterval))
					err := workflow.Sleep(ctx, syncInterval)
					if err != nil {
						return xerrors.Errorf("workflow sleep failed while waiting for events: %w", err)
					}
					// Continue as new to retry
					newRequest := *request
					return workflow.NewContinueAsNewError(ctx, w.name, &newRequest)
				}
				return xerrors.New("No events found in DynamoDB to migrate")
			}

			request.EndEventSequence = maxEventResp.MaxEventId
			logger.Info("Found max event ID in DynamoDB",
				zap.Int64("maxEventId", maxEventResp.MaxEventId),
				zap.Int64("startEventSequence", request.StartEventSequence))
		}

		// Validate end sequence after auto-detection and auto-resume
		if !continuousSync && request.StartEventSequence >= request.EndEventSequence {
			return xerrors.Errorf("startEventSequence (%d) must be less than endEventSequence (%d)",
				request.StartEventSequence, request.EndEventSequence)
		}

		// Additional handling for continuous sync:
		// If EndEventSequence <= StartEventSequence, we are caught up.
		if continuousSync && request.EndEventSequence != 0 && request.EndEventSequence <= request.StartEventSequence {
			logger.Info("Continuous sync: caught up (no new events).",
				zap.Int64("startEventSequence", request.StartEventSequence),
				zap.Int64("endEventSequence", request.EndEventSequence))

			// No special event catch-up needed - normal migration handles this

			// Prepare for next cycle
			newRequest := *request
			newRequest.StartEventSequence = request.EndEventSequence
			newRequest.EndEventSequence = 0 // re-detect on next cycle

			// Wait for syncInterval before starting a new continuous sync workflow
			logger.Info("waiting for sync interval before next catch-up cycle",
				zap.Duration("syncInterval", syncInterval))
			err := workflow.Sleep(ctx, syncInterval)
			if err != nil {
				return xerrors.Errorf("workflow sleep failed during caught-up continuous sync: %w", err)
			}

			logger.Info("starting next continuous sync cycle after catch-up",
				zap.Int64("nextStartEventSequence", newRequest.StartEventSequence))
			return workflow.NewContinueAsNewError(ctx, w.name, &newRequest)
		}

		// Special case: if auto-resume found we're already caught up
		if request.AutoResume && request.StartEventSequence >= request.EndEventSequence {
			logger.Info("Auto-resume detected: already caught up, no migration needed",
				zap.Int64("startEventSequence", request.StartEventSequence),
				zap.Int64("endEventSequence", request.EndEventSequence))
			return nil // Successfully completed with no work to do
		}

		// Log migration mode
		logger.Info("Starting event-driven migration workflow")

		logger.Info("migrator workflow started")

		totalEventRange := request.EndEventSequence - request.StartEventSequence
		processedEvents := int64(0)

		for batchStart := request.StartEventSequence; batchStart < request.EndEventSequence; batchStart += int64(batchSize) {
			// Check for checkpoint - only check after processing at least one batch
			processedSoFar := batchStart - request.StartEventSequence
			if processedSoFar > 0 && processedSoFar >= int64(checkpointSize) {
				newRequest := *request
				newRequest.StartEventSequence = batchStart
				logger.Info("checkpoint reached", zap.Reflect("newRequest", newRequest))
				return workflow.NewContinueAsNewError(ctx, w.name, &newRequest)
			}

			batchEnd := batchStart + int64(batchSize)
			if batchEnd > request.EndEventSequence {
				batchEnd = request.EndEventSequence
			}

			logger.Info("migrating event batch",
				zap.Int64("batchStart", batchStart),
				zap.Int64("batchEnd", batchEnd))

			// Execute a single migrator activity for the entire batch.
			migratorRequest := &activity.MigratorRequest{
				StartEventSequence: batchStart,
				EndEventSequence:   batchEnd,
				EventTag:           eventTag,
				Tag:                tag,
				Parallelism:        parallelism,
			}

			response, err := w.migrator.Execute(ctx, migratorRequest)
			if err != nil {
				logger.Error(
					"failed to migrate batch",
					zap.Int64("batchStart", batchStart),
					zap.Int64("batchEnd", batchEnd),
					zap.Error(err),
				)
				return xerrors.Errorf("failed to migrate batch [%v, %v): %w", batchStart, batchEnd, err)
			}
			if !response.Success {
				logger.Error(
					"migration batch failed",
					zap.Int64("batchStart", batchStart),
					zap.Int64("batchEnd", batchEnd),
					zap.String("message", response.Message),
				)
				return xerrors.Errorf("migration batch failed [%v, %v): %s", batchStart, batchEnd, response.Message)
			}

			// Update metrics for the whole batch after all shards complete
			processedEvents += batchEnd - batchStart
			progress := float64(processedEvents) / float64(totalEventRange) * 100

			metrics.Gauge(migratorHeightGauge).Update(float64(batchEnd - 1))
			metrics.Counter(migratorBlocksCounter).Inc(int64(response.BlocksMigrated))
			metrics.Counter(migratorEventsCounter).Inc(int64(response.EventsMigrated))
			metrics.Gauge(migratorProgressGauge).Update(progress)

			logger.Info(
				"migrated batch successfully",
				zap.Int64("batchStart", batchStart),
				zap.Int64("batchEnd", batchEnd),
				zap.Int("blocksMigrated", response.BlocksMigrated),
				zap.Int("eventsMigrated", response.EventsMigrated),
				zap.Float64("progress", progress),
			)

			// Add backoff if configured
			if backoffInterval > 0 {
				_ = workflow.Sleep(ctx, backoffInterval)
			}
		}

		if continuousSync {
			logger.Info("continuous sync enabled, preparing for next sync cycle")
			newRequest := *request
			newRequest.StartEventSequence = request.EndEventSequence
			newRequest.EndEventSequence = 0 // Will be auto-detected on next cycle
			newRequest.AutoResume = false   // AutoResume should only happen on first workflow run

			// Wait for syncInterval before starting a new continuous sync workflow
			logger.Info("waiting for sync interval before next cycle",
				zap.Duration("syncInterval", syncInterval))
			err := workflow.Sleep(ctx, syncInterval)
			if err != nil {
				return xerrors.Errorf("workflow sleep failed during continuous sync: %w", err)
			}

			logger.Info("starting new continuous sync workflow",
				zap.Int64("nextStartEventSequence", newRequest.StartEventSequence),
				zap.Reflect("newRequest", newRequest))
			return workflow.NewContinueAsNewError(ctx, w.name, &newRequest)
		}

		logger.Info("migrator workflow finished",
			zap.Int64("totalEvents", totalEventRange),
			zap.Int64("processedEvents", processedEvents))

		return nil
	})
}

func (r *MigratorRequest) GetTags() map[string]string {
	return map[string]string{
		tagBlockTag: strconv.Itoa(int(r.Tag)),
	}
}
