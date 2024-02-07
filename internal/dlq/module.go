package dlq

import (
	"go.uber.org/fx"

	"github.com/coinbase/chainstorage/internal/dlq/firestore"
	"github.com/coinbase/chainstorage/internal/dlq/internal"
	"github.com/coinbase/chainstorage/internal/dlq/sqs"
)

const (
	FailedBlockTopic            = internal.FailedBlockTopic
	FailedTransactionTraceTopic = internal.FailedTransactionTraceTopic
)

type (
	DLQ                        = internal.DLQ
	Message                    = internal.Message
	FailedBlockData            = internal.FailedBlockData
	FailedTransactionTraceData = internal.FailedTransactionTraceData
)

var (
	ErrNotFound = internal.ErrNotFound
)

func NewNop() DLQ {
	return internal.NewNop()
}

var Module = fx.Options(
	sqs.Module,
	firestore.Module,
	fx.Provide(internal.WithDLQFactory),
)
