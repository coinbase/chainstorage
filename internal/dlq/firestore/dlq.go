package firestore

import (
	"context"
	"encoding/json"

	"cloud.google.com/go/firestore"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/dlq/internal"
	"github.com/coinbase/chainstorage/internal/utils/fxparams"
	"github.com/coinbase/chainstorage/internal/utils/instrument"
	"github.com/coinbase/chainstorage/internal/utils/log"
)

// A firestore-based store only DLQ for storing unexpected blocks

type (
	DLQ     = internal.DLQ
	Message = internal.Message

	DLQParams struct {
		fx.In
		fxparams.Params
	}

	dlqImpl struct {
		config                   *config.Config
		logger                   *zap.Logger
		client                   *firestore.Client
		instrumentSendMessage    instrument.Instrument
		instrumentResendMessage  instrument.Instrument
		instrumentReceiveMessage instrument.Instrument
		instrumentDeleteMessage  instrument.Instrument
	}

	dlqFactory struct {
		params DLQParams
	}
)

var _ DLQ = (*dlqImpl)(nil)

func (f *dlqFactory) Create() (internal.DLQ, error) {
	return New(f.params)
}

func NewFactory(params DLQParams) internal.DLQFactory {
	return &dlqFactory{params}
}

func New(params DLQParams) (DLQ, error) {
	ctx := context.Background()
	config := params.Config.GCP
	if config == nil {
		return nil, xerrors.Errorf("failed to create firestore meta storage: missing GCP config")
	}

	client, err := firestore.NewClient(ctx, config.Project)
	if err != nil {
		return nil, xerrors.Errorf("failed to create firestore client: %w", err)
	}
	metrics := params.Metrics.SubScope("dlq").Tagged(map[string]string{
		"storage_type": "firestore",
	})
	return &dlqImpl{
		config:                   params.Config,
		logger:                   log.WithPackage(params.Logger).With(zap.String("storage_type", "firestore")),
		client:                   client,
		instrumentSendMessage:    instrument.New(metrics, "send_message"),
		instrumentResendMessage:  instrument.New(metrics, "resend_message"),
		instrumentReceiveMessage: instrument.New(metrics, "receive_message", instrument.WithFilter(internal.FilterError)),
		instrumentDeleteMessage:  instrument.New(metrics, "delete_message"),
	}, nil
}

// DeleteMessage implements internal.DLQ.
func (*dlqImpl) DeleteMessage(ctx context.Context, message *internal.Message) error {
	return internal.ErrNotFound
}

// ReceiveMessage implements internal.DLQ.
func (*dlqImpl) ReceiveMessage(ctx context.Context) (*internal.Message, error) {
	return nil, internal.ErrNotFound
}

// ResendMessage implements internal.DLQ.
func (*dlqImpl) ResendMessage(ctx context.Context, message *internal.Message) error {
	return internal.ErrNotFound
}

// SendMessage implements internal.DLQ.
func (q *dlqImpl) SendMessage(ctx context.Context, message *internal.Message) error {
	return q.instrumentSendMessage.Instrument(ctx, func(ctx context.Context) error {
		body, err := json.Marshal(message.Data)
		if err != nil {
			return xerrors.Errorf("failed to marshal body: %w", err)
		}
		messageBody := string(body)

		_, _, err = q.client.Collection("dlq").Add(ctx, map[string]any{
			"Topic":         message.Topic,
			"Retries":       message.Retries,
			"SentTimestamp": message.SentTimestamp,
			"ReceiptHandle": message.ReceiptHandle,
			"Data":          messageBody,
		})

		if err != nil {
			return xerrors.Errorf("failed to send message: %w", err)
		}

		q.logger.Info(
			"sent message to dlq",
			zap.String("topic", message.Topic),
			zap.Reflect("data", message.Data),
		)
		return nil
	})
}
