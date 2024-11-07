package internal

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.uber.org/fx"

	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/utils/fxparams"
)

type (
	DLQ interface {
		SendMessage(ctx context.Context, message *Message) error
		ResendMessage(ctx context.Context, message *Message) error
		ReceiveMessage(ctx context.Context) (*Message, error)
		DeleteMessage(ctx context.Context, message *Message) error
	}

	Message struct {
		Topic         string
		Retries       int
		SentTimestamp time.Time
		ReceiptHandle string
		Data          any
	}

	DLQFactory interface {
		Create() (DLQ, error)
	}

	DLQFactoryParams struct {
		fx.In
		fxparams.Params
		SQS       DLQFactory `name:"dlq/sqs"`
		Firestore DLQFactory `name:"dlq/firestore"`
	}
)

var (
	ErrNotFound = errors.New("not found")
)

func WithDLQFactory(params DLQFactoryParams) (DLQ, error) {
	var factory DLQFactory
	dlqType := params.Config.StorageType.DLQType
	switch dlqType {
	case config.DLQType_UNSPECIFIED, config.DLQType_SQS:
		factory = params.SQS
	case config.DLQType_FIRESTORE:
		factory = params.Firestore
	}
	if factory == nil {
		return nil, fmt.Errorf("dlq type is not implemented: %v", dlqType)
	}
	dlq, err := factory.Create()
	if err != nil {
		return nil, fmt.Errorf("failed to create dlq of type %v, error: %w", dlqType, err)
	}
	return dlq, nil
}

func FilterError(err error) bool {
	return errors.Is(err, ErrNotFound)
}
