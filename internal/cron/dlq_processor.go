package cron

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.uber.org/fx"
	"go.uber.org/zap"

	"github.com/coinbase/chainstorage/internal/blockchain/client"
	"github.com/coinbase/chainstorage/internal/dlq"
	"github.com/coinbase/chainstorage/internal/storage/blobstorage"
	"github.com/coinbase/chainstorage/internal/storage/metastorage"
	storage_utils "github.com/coinbase/chainstorage/internal/storage/utils"
	"github.com/coinbase/chainstorage/internal/utils/fxparams"
	"github.com/coinbase/chainstorage/internal/utils/log"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

type (
	DLQProcessorTaskParams struct {
		fx.In
		fxparams.Params
		DLQ              dlq.DLQ
		BlockchainClient client.Client `name:"slave"`
		BlobStorage      blobstorage.BlobStorage
		MetaStorage      metastorage.MetaStorage
	}

	dlqProcessorTask struct {
		enabled          bool
		logger           *zap.Logger
		dlq              dlq.DLQ
		blockchainClient client.Client
		blobStorage      blobstorage.BlobStorage
		metaStorage      metastorage.MetaStorage
	}
)

var (
	ErrSkipped = errors.New("skipped")
)

func NewDLQProcessor(params DLQProcessorTaskParams) Task {
	return &dlqProcessorTask{
		enabled:          !params.Config.Cron.DisableDLQProcessor,
		logger:           log.WithPackage(params.Logger),
		dlq:              params.DLQ,
		blockchainClient: params.BlockchainClient,
		blobStorage:      params.BlobStorage,
		metaStorage:      params.MetaStorage,
	}
}

func (t *dlqProcessorTask) Name() string {
	return "dlq_processor"
}

func (t *dlqProcessorTask) Spec() string {
	return "@every 1s"
}

func (t *dlqProcessorTask) Parallelism() int64 {
	return 1
}

func (t *dlqProcessorTask) Enabled() bool {
	return t.enabled
}

func (t *dlqProcessorTask) DelayStartDuration() time.Duration {
	return 0
}

func (t *dlqProcessorTask) Run(ctx context.Context) error {
	message, err := t.dlq.ReceiveMessage(ctx)
	if err != nil {
		if errors.Is(err, dlq.ErrNotFound) {
			t.logger.Info("dlq is empty")
			return nil
		}

		return fmt.Errorf("failed to receive message from dlq: %w", err)
	}

	switch message.Topic {
	case dlq.FailedTransactionTraceTopic:
		if err := t.processFailedTransactionTrace(ctx, message); err != nil {
			if !errors.Is(err, ErrSkipped) {
				t.logger.Warn(
					"failed to process message from failed_transaction_trace topic",
					zap.Error(err),
					zap.Reflect("msg", message),
				)
			}

			return t.dlq.ResendMessage(ctx, message)
		}

		return t.dlq.DeleteMessage(ctx, message)

	case dlq.FailedBlockTopic:
		// Obsolete topics go here.
		return t.dlq.DeleteMessage(ctx, message)

	default:
		t.logger.Error("received message with unknown topic", zap.Reflect("msg", message))
		return t.dlq.ResendMessage(ctx, message)
	}
}

func (t *dlqProcessorTask) processFailedTransactionTrace(ctx context.Context, message *dlq.Message) error {
	data := message.Data.(*dlq.FailedTransactionTraceData)
	if !t.blockchainClient.CanReprocess(data.Tag, data.Height) {
		// No fix yet. Resend the message back to the queue.
		return ErrSkipped
	}

	var block *api.Block
	var err error
	if data.Hash == "" {
		// Old messages do not have the hash field.
		block, err = t.blockchainClient.GetBlockByHeight(ctx, data.Tag, data.Height)
		if err != nil {
			return fmt.Errorf("failed to extract block: %w", err)
		}
	} else {
		// Use hash for lookup if available. This ensures it's processing the same block even after a chain reorg.
		block, err = t.blockchainClient.GetBlockByHash(ctx, data.Tag, data.Height, data.Hash)
		if err != nil {
			if errors.Is(err, client.ErrBlockNotFound) {
				// The block is orphaned; therefore the message should be removed.
				t.logger.Info("removed orphaned block from failed_transaction_trace topic", zap.Reflect("msg", message))
				return nil
			}

			return fmt.Errorf("failed to extract block: %w", err)
		}
	}

	metadata, err := t.metaStorage.GetBlockByHash(ctx, data.Tag, data.Height, data.Hash)
	if err != nil {
		return fmt.Errorf("failed to get metadata: %w", err)
	}
	objectKey := metadata.ObjectKeyMain

	compression := storage_utils.GetCompressionType(objectKey)
	_, err = t.blobStorage.Upload(ctx, block, compression)
	if err != nil {
		return fmt.Errorf("failed to upload to blob store with compression type %v: %w", compression.String(), err)
	}

	// Note that the block is already persisted in meta storage.
	// Since the S3 object key stays the same, we don't need to persist the block in meta storage again.

	t.logger.Info("processed message from failed_transaction_trace topic", zap.Reflect("msg", message))
	return nil
}
