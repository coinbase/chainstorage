package sqs

import (
	"context"
	"encoding/json"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/dlq/internal"
	"github.com/coinbase/chainstorage/internal/utils/fxparams"
	"github.com/coinbase/chainstorage/internal/utils/instrument"
	"github.com/coinbase/chainstorage/internal/utils/log"
	"github.com/coinbase/chainstorage/internal/utils/pointer"
)

type (
	DLQ     = internal.DLQ
	Message = internal.Message

	DLQParams struct {
		fx.In
		fxparams.Params
		Session *session.Session
	}

	dlqImpl struct {
		config                   *config.Config
		logger                   *zap.Logger
		client                   *sqs.SQS
		queueURL                 string
		instrumentSendMessage    instrument.Call
		instrumentResendMessage  instrument.Call
		instrumentReceiveMessage instrument.Call
		instrumentDeleteMessage  instrument.Call
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

const (
	topicAttributeName       = "topic"
	topicAttributeDateType   = "String"
	retriesAttributeName     = "retries"
	retriesAttributeDataType = "Number"
)

var (
	ErrNotFound = xerrors.New("not found")
)

func New(params DLQParams) (DLQ, error) {
	client := sqs.New(params.Session)
	metrics := params.Metrics.SubScope("dlq")
	impl := &dlqImpl{
		config:                   params.Config,
		logger:                   log.WithPackage(params.Logger),
		client:                   client,
		instrumentSendMessage:    instrument.NewCall(metrics, "send_message"),
		instrumentResendMessage:  instrument.NewCall(metrics, "resend_message"),
		instrumentReceiveMessage: instrument.NewCall(metrics, "receive_message", instrument.WithFilter(filterError)),
		instrumentDeleteMessage:  instrument.NewCall(metrics, "delete_message"),
	}
	if params.Config.AWS.IsLocalStack {
		if err := impl.resetLocalResources(); err != nil {
			return nil, xerrors.Errorf("failed to reset local resources: %w", err)
		}
	}

	if err := impl.initQueueURL(); err != nil {
		return nil, xerrors.Errorf("failed to init queue url: %w", err)
	}
	return impl, nil
}

func (q *dlqImpl) SendMessage(ctx context.Context, message *Message) error {
	return q.instrumentSendMessage.Instrument(ctx, func(ctx context.Context) error {
		body, err := json.Marshal(message.Data)
		if err != nil {
			return xerrors.Errorf("failed to marshal body: %w", err)
		}
		messageBody := string(body)

		input := &sqs.SendMessageInput{
			QueueUrl: pointer.String(q.queueURL),
			MessageAttributes: map[string]*sqs.MessageAttributeValue{
				topicAttributeName: {
					DataType:    pointer.String(topicAttributeDateType),
					StringValue: pointer.String(message.Topic),
				},
			},
			MessageBody:  pointer.String(messageBody),
			DelaySeconds: pointer.Int64(q.config.AWS.DLQ.DelaySecs),
		}

		if _, err := q.client.SendMessageWithContext(ctx, input); err != nil {
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

func (q *dlqImpl) ResendMessage(ctx context.Context, message *Message) error {
	return q.instrumentResendMessage.Instrument(ctx, func(ctx context.Context) error {
		body, err := json.Marshal(message.Data)
		if err != nil {
			return xerrors.Errorf("failed to marshal body: %w", err)
		}
		messageBody := string(body)

		// Increment the retries counter and resend the same data.
		retries := strconv.Itoa(message.Retries + 1)
		input := &sqs.SendMessageInput{
			QueueUrl: pointer.String(q.queueURL),
			MessageAttributes: map[string]*sqs.MessageAttributeValue{
				topicAttributeName: {
					DataType:    pointer.String(topicAttributeDateType),
					StringValue: pointer.String(message.Topic),
				},
				retriesAttributeName: {
					DataType:    pointer.String(retriesAttributeDataType),
					StringValue: pointer.String(retries),
				},
			},
			MessageBody:  pointer.String(messageBody),
			DelaySeconds: pointer.Int64(q.config.AWS.DLQ.DelaySecs),
		}

		if _, err := q.client.SendMessageWithContext(ctx, input); err != nil {
			return xerrors.Errorf("failed to send message: %w", err)
		}

		// Delete the original message.
		if _, err := q.client.DeleteMessageWithContext(ctx, &sqs.DeleteMessageInput{
			QueueUrl:      pointer.String(q.queueURL),
			ReceiptHandle: pointer.String(message.ReceiptHandle),
		}); err != nil {
			return xerrors.Errorf("failed to delete message: %w", err)
		}

		q.logger.Info(
			"resent message to dlq",
			zap.String("topic", message.Topic),
			zap.String("retries", retries),
			zap.Reflect("data", message.Data),
		)
		return nil
	})
}

func (q *dlqImpl) ReceiveMessage(ctx context.Context) (*Message, error) {
	var message *Message
	if err := q.instrumentReceiveMessage.Instrument(ctx, func(ctx context.Context) error {
		input := &sqs.ReceiveMessageInput{
			QueueUrl:          pointer.String(q.queueURL),
			VisibilityTimeout: pointer.Int64(q.config.AWS.DLQ.VisibilityTimeoutSecs),
			AttributeNames: []*string{
				pointer.String(sqs.MessageSystemAttributeNameSentTimestamp),
			},
			MessageAttributeNames: []*string{
				pointer.String(topicAttributeName),
				pointer.String(retriesAttributeName),
			},
		}

		output, err := q.client.ReceiveMessageWithContext(ctx, input)
		if err != nil {
			return xerrors.Errorf("failed to receive message: %w", err)
		}

		numMessages := len(output.Messages)
		if numMessages == 0 {
			return ErrNotFound
		}

		if numMessages != 1 {
			return xerrors.Errorf("received more messages than expected: %v", numMessages)
		}

		outputMessage := output.Messages[0]
		receiptHandle := pointer.StringDeref(outputMessage.ReceiptHandle)

		sentTimestampAttribute := pointer.StringDeref(outputMessage.Attributes[sqs.MessageSystemAttributeNameSentTimestamp])
		sentTimestampEpoch, err := strconv.ParseInt(sentTimestampAttribute, 10, 64)
		if err != nil {
			return xerrors.Errorf("failed to parse sent timestamp: %v", sentTimestampAttribute)
		}
		sentTimestamp := time.Unix(sentTimestampEpoch/1000, 0)

		topicAttribute := outputMessage.MessageAttributes[topicAttributeName]
		if topicAttribute == nil {
			return xerrors.Errorf("topic not found: %v", outputMessage)
		}
		topic := pointer.StringDeref(topicAttribute.StringValue)

		var retries int
		if attr := outputMessage.MessageAttributes[retriesAttributeName]; attr != nil {
			if v, err := strconv.Atoi(pointer.StringDeref(attr.StringValue)); err == nil {
				retries = v
			}
		}

		var data interface{}
		switch topic {
		case internal.FailedBlockTopic:
			data = new(internal.FailedBlockData)
		case internal.FailedTransactionTraceTopic:
			data = new(internal.FailedTransactionTraceData)
		default:
			q.logger.Warn("unknown topic", zap.String("topic", topic))
		}

		if data != nil {
			body := []byte(pointer.StringDeref(outputMessage.Body))
			if err := json.Unmarshal(body, data); err != nil {
				return xerrors.Errorf("failed to unmarshal message: %w", err)
			}
		}

		q.logger.Info(
			"received message from dlq",
			zap.String("topic", topic),
			zap.Int("retries", retries),
			zap.Time("sent_timestamp", sentTimestamp),
			zap.Reflect("data", data),
		)
		message = &Message{
			Topic:         topic,
			Retries:       retries,
			SentTimestamp: sentTimestamp,
			ReceiptHandle: receiptHandle,
			Data:          data,
		}
		return nil
	}); err != nil {
		return nil, err
	}

	return message, nil
}

func (q *dlqImpl) DeleteMessage(ctx context.Context, message *Message) error {
	return q.instrumentDeleteMessage.Instrument(ctx, func(ctx context.Context) error {
		if _, err := q.client.DeleteMessageWithContext(ctx, &sqs.DeleteMessageInput{
			QueueUrl:      pointer.String(q.queueURL),
			ReceiptHandle: pointer.String(message.ReceiptHandle),
		}); err != nil {
			return xerrors.Errorf("failed to delete message: %w", err)
		}

		q.logger.Info(
			"deleted message from dlq",
			zap.String("topic", message.Topic),
			zap.Reflect("data", message.Data),
		)
		return nil
	})
}

func (q *dlqImpl) initQueueURL() error {
	output, err := q.client.GetQueueUrl(&sqs.GetQueueUrlInput{
		QueueName: pointer.String(q.config.AWS.DLQ.Name),
	})
	if err != nil {
		return xerrors.Errorf("failed to get queue url (name=%v): %w", q.config.AWS.DLQ.Name, err)
	}

	queueURL := pointer.StringDeref(output.QueueUrl)
	if queueURL == "" {
		return xerrors.New("empty queue url")
	}

	q.queueURL = queueURL

	q.logger.Info(
		"initialized dlq",
		zap.String("url", q.queueURL),
		zap.Reflect("config", q.config.AWS.DLQ),
	)
	return nil
}

func filterError(err error) bool {
	return xerrors.Is(err, ErrNotFound)
}
