package cron

import (
	"context"
	"math"
	"strings"
	"time"

	"github.com/VividCortex/ewma"
	"github.com/uber-go/tally"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/blockchain/client"
	"github.com/coinbase/chainstorage/internal/blockchain/endpoints"
	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/storage"
	"github.com/coinbase/chainstorage/internal/utils/fxparams"
	"github.com/coinbase/chainstorage/internal/utils/instrument"
	"github.com/coinbase/chainstorage/internal/utils/log"
	"github.com/coinbase/chainstorage/internal/utils/syncgroup"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

type (
	NodeCanaryTaskParams struct {
		fx.In
		fxparams.Params
		Config          *config.Config
		Clients         client.ClientParams
		BlockStorage    storage.BlockStorage
		FailoverManager endpoints.FailoverManager
	}

	nodeCanaryTask struct {
		config                 *config.Config
		logger                 *zap.Logger
		master                 client.Client
		slave                  client.Client
		validator              client.Client
		blockStorage           storage.BlockStorage
		failoverManager        endpoints.FailoverManager
		masterHeightAverage    ewma.MovingAverage
		slaveHeightAverage     ewma.MovingAverage
		validatorHeightAverage ewma.MovingAverage
		metrics                *nodeCanaryMetrics
	}

	nodeCanaryMetrics struct {
		masterHeight                 tally.Gauge
		slaveHeight                  tally.Gauge
		validatorHeight              tally.Gauge
		masterSlaveDistance          tally.Gauge
		masterSlaveDistanceWithinSLA tally.Counter
		masterSlaveDistanceOutOfSLA  tally.Counter
		masterDistance               tally.Gauge
		masterDistanceWithinSLA      tally.Counter
		masterDistanceOutOfSLA       tally.Counter
		slaveDistance                tally.Gauge
		slaveDistanceWithinSLA       tally.Counter
		slaveDistanceOutOfSLA        tally.Counter
		validatorDistance            tally.Gauge
		timeSinceLastBlock           tally.Gauge
		timeSinceLastBlockWithinSLA  tally.Counter
		timeSinceLastBlockOutOfSLA   tally.Counter
		blockTimeDelta               tally.Gauge
		blockTimeDeltaWithinSLA      tally.Counter
		blockTimeDeltaOutOfSLA       tally.Counter
		blockHeightDelta             tally.Gauge
		blockHeightDeltaWithinSLA    tally.Counter
		blockHeightDeltaOutOfSLA     tally.Counter
		masterPrimaryHealthCheck     instrument.Call
		masterSecondaryHealthCheck   instrument.Call
		slavePrimaryHealthCheck      instrument.Call
		slaveSecondaryHealthCheck    instrument.Call
		validatorPrimaryHealthCheck  instrument.Call
	}
)

const (
	movingAverageAge = 10
	nodeCanaryScope  = "node_canary"
	slaMetric        = "sla"

	outOfSLAMsg = "out_of_sla"
	expectedTag = "expected"
	actualTag   = "actual"

	severityTag = "severity"
	sev1        = "sev1"
	sev2        = "sev2"
	sev3        = "sev3"

	resultTypeTag     = "result_type"
	resultTypeSuccess = "success"
	resultTypeError   = "error"

	healthCheckMsg  = "node.health_check"
	healthCheckType = "health_check_type"

	slaTypeTag                        = "sla_type"
	masterHeightMetric                = "master_height"
	slaveHeightMetric                 = "slave_height"
	validatorHeightMetric             = "validator_height"
	masterSlaveDistanceMetric         = "master_slave_distance"
	masterDistanceMetric              = "master_distance"
	slaveDistanceMetric               = "slave_distance"
	validatorDistanceMetric           = "validator_distance"
	timeSinceLastBlockMetric          = "time_since_last_block"
	blockTimeDeltaMetric              = "block_time_delta"
	blockHeightDeltaMetric            = "block_height_delta"
	masterPrimaryHealthCheckMetric    = "master_primary_health_check"
	masterSecondaryHealthCheckMetric  = "master_secondary_health_check"
	slavePrimaryHealthCheckMetric     = "slave_primary_health_check"
	slaveSecondaryHealthCheckMetric   = "slave_secondary_health_check"
	validatorPrimaryHealthCheckMetric = "validator_primary_health_check"
)

func NewNodeCanary(params NodeCanaryTaskParams) (Task, error) {
	logger := log.WithPackage(params.Logger)
	return &nodeCanaryTask{
		config:                 params.Config,
		logger:                 logger,
		master:                 params.Clients.Master,
		slave:                  params.Clients.Slave,
		validator:              params.Clients.Validator,
		blockStorage:           params.BlockStorage,
		failoverManager:        params.FailoverManager,
		masterHeightAverage:    ewma.NewMovingAverage(movingAverageAge),
		slaveHeightAverage:     ewma.NewMovingAverage(movingAverageAge),
		validatorHeightAverage: ewma.NewMovingAverage(movingAverageAge),
		metrics:                newNodeCanaryMetrics(params.Metrics, logger),
	}, nil
}

func newNodeCanaryMetrics(rootScope tally.Scope, logger *zap.Logger) *nodeCanaryMetrics {
	scope := rootScope.SubScope(subScope).SubScope(nodeCanaryScope)

	// Use the same metric name so that we can set up alerts and dashboards more easily.
	// Each sla type has success vs error so that we can calculate SLA in percentage.
	newSLACounter := func(sla string, severity string, resultType string) tally.Counter {
		return rootScope.Tagged(map[string]string{
			slaTypeTag:    sla,
			severityTag:   severity,
			resultTypeTag: resultType,
		}).Counter(slaMetric)
	}

	newHealthCheckCall := func(metricName string) instrument.Call {
		return instrument.NewCall(
			scope,
			metricName,
			instrument.WithLogger(logger.With(zap.String(healthCheckType, metricName)), healthCheckMsg),
			instrument.WithFilter(func(err error) bool {
				// ErrSkipped is returned from one of the goroutines when there is potentially a chain reorg.
				// As a result, the health check would be cancelled by syncgroup.
				return xerrors.Is(err, context.Canceled)
			}),
		)
	}

	return &nodeCanaryMetrics{
		masterHeight:                 scope.Gauge(masterHeightMetric),
		slaveHeight:                  scope.Gauge(slaveHeightMetric),
		validatorHeight:              scope.Gauge(validatorHeightMetric),
		masterSlaveDistance:          scope.Gauge(masterSlaveDistanceMetric),
		masterSlaveDistanceWithinSLA: newSLACounter(masterSlaveDistanceMetric, sev3, resultTypeSuccess),
		masterSlaveDistanceOutOfSLA:  newSLACounter(masterSlaveDistanceMetric, sev3, resultTypeError),
		masterDistance:               scope.Gauge(masterDistanceMetric),
		masterDistanceWithinSLA:      newSLACounter(masterDistanceMetric, sev3, resultTypeSuccess),
		masterDistanceOutOfSLA:       newSLACounter(masterDistanceMetric, sev3, resultTypeError),
		slaveDistance:                scope.Gauge(slaveDistanceMetric),
		slaveDistanceWithinSLA:       newSLACounter(slaveDistanceMetric, sev3, resultTypeSuccess),
		slaveDistanceOutOfSLA:        newSLACounter(slaveDistanceMetric, sev3, resultTypeError),
		validatorDistance:            scope.Gauge(validatorDistanceMetric),
		timeSinceLastBlock:           scope.Gauge(timeSinceLastBlockMetric),
		timeSinceLastBlockWithinSLA:  newSLACounter(timeSinceLastBlockMetric, sev2, resultTypeSuccess),
		timeSinceLastBlockOutOfSLA:   newSLACounter(timeSinceLastBlockMetric, sev2, resultTypeError),
		blockTimeDelta:               scope.Gauge(blockTimeDeltaMetric),
		blockTimeDeltaWithinSLA:      newSLACounter(blockTimeDeltaMetric, sev1, resultTypeSuccess),
		blockTimeDeltaOutOfSLA:       newSLACounter(blockTimeDeltaMetric, sev1, resultTypeError),
		blockHeightDelta:             scope.Gauge(blockHeightDeltaMetric),
		blockHeightDeltaWithinSLA:    newSLACounter(blockHeightDeltaMetric, sev1, resultTypeSuccess),
		blockHeightDeltaOutOfSLA:     newSLACounter(blockHeightDeltaMetric, sev1, resultTypeError),
		masterPrimaryHealthCheck:     newHealthCheckCall(masterPrimaryHealthCheckMetric),
		masterSecondaryHealthCheck:   newHealthCheckCall(masterSecondaryHealthCheckMetric),
		slavePrimaryHealthCheck:      newHealthCheckCall(slavePrimaryHealthCheckMetric),
		slaveSecondaryHealthCheck:    newHealthCheckCall(slaveSecondaryHealthCheckMetric),
		validatorPrimaryHealthCheck:  newHealthCheckCall(validatorPrimaryHealthCheckMetric),
	}
}

func (t *nodeCanaryTask) Name() string {
	return "node_canary"
}

func (t *nodeCanaryTask) Spec() string {
	return "@every 5s"
}

func (t *nodeCanaryTask) Parallelism() int64 {
	return 1
}

func (t *nodeCanaryTask) Enabled() bool {
	return !t.config.Cron.DisableNodeCanary
}

func (t *nodeCanaryTask) DelayStartDuration() time.Duration {
	return 0
}

func (t *nodeCanaryTask) Run(ctx context.Context) error {
	tag := t.config.GetStableBlockTag()
	now := time.Now()
	sla := t.config.SLA
	group, ctx := syncgroup.New(ctx)

	// Note that failoverCtx is set to nil if failover is unavailable.
	failoverCtx, err := t.failoverManager.WithFailoverContext(ctx)
	if err != nil {
		if !xerrors.Is(err, endpoints.ErrFailoverUnavailable) {
			return xerrors.Errorf("failed to create failover context: %w", err)
		}
	}

	var masterHeight uint64
	var masterDistance uint64
	var masterBlock *api.BlockMetadata
	group.Go(func() error {
		if err := t.metrics.masterPrimaryHealthCheck.Instrument(ctx, func(ctx context.Context) error {
			height, err := t.master.GetLatestHeight(ctx)
			if err != nil {
				return xerrors.Errorf("failed to ping master: %w", err)
			}

			masterHeight = height
			return nil
		}); err != nil {
			return nil
		}

		blocks, err := t.master.BatchGetBlockMetadata(ctx, tag, masterHeight, masterHeight+1)
		if err != nil {
			t.logger.Info("skipping node canary task due to chain reorg", zap.Error(err))
			return ErrSkipped
		}

		if len(blocks) != 1 {
			return xerrors.Errorf("unexpected number of blocks: %v", len(blocks))
		}

		masterBlock = blocks[0]

		t.metrics.masterHeight.Update(float64(masterHeight))
		// Distance between current height of master and its moving average.
		if t.masterHeightAverage.Value() != 0 {
			masterDistance = uint64(math.Abs(float64(masterHeight) - t.masterHeightAverage.Value()))
			t.metrics.masterDistance.Update(float64(masterDistance))

			if masterDistance < sla.OutOfSyncNodeDistance {
				t.metrics.masterDistanceWithinSLA.Inc(1)
			} else {
				t.metrics.masterDistanceOutOfSLA.Inc(1)
				t.logger.Error(
					outOfSLAMsg,
					zap.Uint64("height", masterHeight),
					zap.String(slaTypeTag, masterDistanceMetric),
					zap.Uint64(expectedTag, sla.OutOfSyncNodeDistance),
					zap.Uint64(actualTag, masterDistance),
				)
			}

		}

		// Keep track of the moving average.
		// Note that master and slave are usually backed by a pool of load-balanced nodes.
		// The moving average is an approximation of the average height of all the nodes.
		t.masterHeightAverage.Add(float64(masterHeight))
		return nil
	})

	var slaveHeight uint64
	var slaveDistance uint64
	group.Go(func() error {
		if err := t.metrics.slavePrimaryHealthCheck.Instrument(ctx, func(ctx context.Context) error {
			height, err := t.slave.GetLatestHeight(ctx)
			if err != nil {
				return xerrors.Errorf("failed to ping slave: %w", err)
			}

			slaveHeight = height
			return nil
		}); err != nil {
			return nil
		}

		t.metrics.slaveHeight.Update(float64(slaveHeight))
		// Distance between current height of slave and its moving average.
		if t.slaveHeightAverage.Value() != 0 {
			slaveDistance = uint64(math.Abs(float64(slaveHeight) - t.slaveHeightAverage.Value()))
			t.metrics.slaveDistance.Update(float64(slaveDistance))

			if slaveDistance < sla.OutOfSyncNodeDistance {
				t.metrics.slaveDistanceWithinSLA.Inc(1)
			} else {
				t.metrics.slaveDistanceOutOfSLA.Inc(1)
				t.logger.Error(
					outOfSLAMsg,
					zap.Uint64("height", slaveHeight),
					zap.String(slaTypeTag, slaveDistanceMetric),
					zap.Uint64(expectedTag, sla.OutOfSyncNodeDistance),
					zap.Uint64(actualTag, slaveDistance),
				)
			}

		}
		t.slaveHeightAverage.Add(float64(slaveHeight))

		return nil
	})

	var validatorHeight uint64
	var validatorDistance uint64
	if !t.config.Chain.Client.Validator.EndpointGroup.Empty() {
		// Do not fail the task if the health check on validator nodes returns an error.
		group.Go(func() error {
			if err := t.metrics.validatorPrimaryHealthCheck.Instrument(ctx, func(ctx context.Context) error {
				height, err := t.validator.GetLatestHeight(ctx)
				if err != nil {
					return xerrors.Errorf("failed to ping validator: %w", err)
				}

				validatorHeight = height
				return nil
			}); err != nil {
				return nil
			}

			t.metrics.validatorHeight.Update(float64(validatorHeight))
			// Distance between current height of validator and its moving average.
			if t.validatorHeightAverage.Value() != 0 {
				validatorDistance = uint64(math.Abs(float64(validatorHeight) - t.validatorHeightAverage.Value()))
				t.metrics.validatorDistance.Update(float64(validatorDistance))
			}
			t.validatorHeightAverage.Add(float64(validatorHeight))

			return nil
		})
	}

	if failoverCtx != nil {
		// Do not fail the task if the health check on secondary nodes returns an error.
		group.Go(func() error {
			_ = t.metrics.masterSecondaryHealthCheck.Instrument(failoverCtx, func(ctx context.Context) error {
				if _, err = t.master.GetLatestHeight(ctx); err != nil {
					return xerrors.Errorf("failed to ping master with failover context: %w", err)
				}

				return nil
			})

			return nil
		})

		group.Go(func() error {
			_ = t.metrics.slaveSecondaryHealthCheck.Instrument(failoverCtx, func(ctx context.Context) error {
				if _, err = t.slave.GetLatestHeight(ctx); err != nil {
					return xerrors.Errorf("failed to ping slave with failover context: %w", err)
				}

				return nil
			})

			return nil
		})
	}

	var persistedBlock *api.BlockMetadata
	group.Go(func() error {
		block, err := t.blockStorage.GetLatestBlock(ctx, tag)
		if err != nil {
			if xerrors.Is(err, storage.ErrItemNotFound) {
				// Note that persistedBlock is nil when no block is available.
				return nil
			}

			return xerrors.Errorf("failed to get latest block from storage: %w", err)
		}

		persistedBlock = block
		return nil
	})

	if err := group.Wait(); err != nil {
		if xerrors.Is(err, ErrSkipped) {
			return nil
		}

		return xerrors.Errorf("failed to finish node canary task: %w", err)
	}

	// Distance between master and slave.
	var masterSlaveDistance uint64
	if masterHeight > 0 && slaveHeight > 0 {
		masterSlaveDistance = uint64(math.Abs(float64(masterHeight) - float64(slaveHeight)))
		t.metrics.masterSlaveDistance.Update(float64(masterSlaveDistance))

		// Complain if the nodes are out of sync.
		if masterSlaveDistance < sla.OutOfSyncNodeDistance {
			t.metrics.masterSlaveDistanceWithinSLA.Inc(1)
		} else {
			t.metrics.masterSlaveDistanceOutOfSLA.Inc(1)
			t.logger.Error(
				outOfSLAMsg,
				zap.Uint64("height", masterHeight),
				zap.String(slaTypeTag, masterSlaveDistanceMetric),
				zap.Uint64(expectedTag, sla.OutOfSyncNodeDistance),
				zap.Uint64(actualTag, masterSlaveDistance),
			)
		}
	}

	// Measure SLA by comparing the latest block in the storage and blockchain.
	// Because each chain has different SLA expectations,
	// the SLA is loaded from the config and a corresponding counter is emitted if it is out of SLA.
	var timeSinceLastBlock time.Duration
	var blockHeightDelta uint64
	var blockTimeDelta time.Duration
	if persistedBlock != nil && persistedBlock.GetTimestamp() != nil && masterBlock != nil && t.isPollerPresent() {
		timeSinceLastBlock = now.Sub(persistedBlock.Timestamp.AsTime())
		t.metrics.timeSinceLastBlock.Update(timeSinceLastBlock.Seconds())

		if timeSinceLastBlock < sla.TimeSinceLastBlock {
			t.metrics.timeSinceLastBlockWithinSLA.Inc(1)
		} else {
			t.metrics.timeSinceLastBlockOutOfSLA.Inc(1)
			t.logger.Error(
				outOfSLAMsg,
				zap.Uint64("height", masterHeight),
				zap.String(slaTypeTag, timeSinceLastBlockMetric),
				zap.String(expectedTag, sla.TimeSinceLastBlock.String()),
				zap.String(actualTag, timeSinceLastBlock.String()),
			)
		}

		blockHeightDelta = uint64(math.Max(0, float64(masterBlock.Height)-float64(persistedBlock.Height)))
		t.metrics.blockHeightDelta.Update(float64(blockHeightDelta))
		if blockHeightDelta < sla.BlockHeightDelta {
			t.metrics.blockHeightDeltaWithinSLA.Inc(1)
		} else {
			t.metrics.blockHeightDeltaOutOfSLA.Inc(1)
			t.logger.Error(
				outOfSLAMsg,
				zap.Uint64("height", masterHeight),
				zap.String(slaTypeTag, blockHeightDeltaMetric),
				zap.Uint64(expectedTag, sla.BlockHeightDelta),
				zap.Uint64(actualTag, blockHeightDelta),
			)
		}

		if masterBlock.Timestamp != nil {
			blockTimeDelta = masterBlock.Timestamp.AsTime().Sub(persistedBlock.Timestamp.AsTime())
			if blockTimeDelta < 0 {
				blockTimeDelta = 0
			}
			t.metrics.blockTimeDelta.Update(blockTimeDelta.Seconds())

			if blockTimeDelta < sla.BlockTimeDelta {
				t.metrics.blockTimeDeltaWithinSLA.Inc(1)
			} else {
				t.metrics.blockTimeDeltaOutOfSLA.Inc(1)
				t.logger.Error(
					outOfSLAMsg,
					zap.Uint64("height", masterHeight),
					zap.String(slaTypeTag, blockTimeDeltaMetric),
					zap.String(expectedTag, sla.BlockTimeDelta.String()),
					zap.String(actualTag, blockTimeDelta.String()),
				)
			}
		}
	}

	t.logger.Info(
		"finished node canary task",
		zap.Uint64("masterSlaveDistance", masterSlaveDistance),
		zap.Uint64("masterDistance", masterDistance),
		zap.Uint64("masterHeight", masterHeight),
		zap.Uint64("masterHeightAverage", uint64(t.masterHeightAverage.Value())),
		zap.Uint64("slaveDistance", slaveDistance),
		zap.Uint64("slaveHeight", slaveHeight),
		zap.Uint64("slaveHeightAverage", uint64(t.slaveHeightAverage.Value())),
		zap.Uint64("validatorDistance", validatorDistance),
		zap.Uint64("validatorHeight", validatorHeight),
		zap.Uint64("validatorHeightAverage", uint64(t.validatorHeightAverage.Value())),
		zap.String("timeSinceLastBlock", timeSinceLastBlock.String()),
		zap.Uint64("blockHeightDelta", blockHeightDelta),
		zap.String("blockTimeDelta", blockTimeDelta.String()),
	)

	return nil
}

func (t *nodeCanaryTask) isPollerPresent() bool {
	for _, wf := range t.config.SLA.ExpectedWorkflows {
		if strings.Split(wf, "/")[0] == poller {
			return true
		}
	}
	return false
}
