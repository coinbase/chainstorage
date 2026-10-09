package sqs

import (
	"context"
	"errors"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"go.uber.org/zap"
	"golang.org/x/xerrors"
)

func (q *dlqImpl) resetLocalResources(ctx context.Context) error {
	q.logger.Info("initializing dlq")

	if err := q.initQueueURL(ctx); err != nil {
		var queueNotExist *types.QueueDoesNotExist
		if !errors.As(err, &queueNotExist) {
			return xerrors.Errorf("failed to init queue url: %w", err)
		}
	}

	if q.config.AWS.IsResetLocal && q.queueURL != "" {
		if _, err := q.client.DeleteQueue(ctx, &sqs.DeleteQueueInput{
			QueueUrl: aws.String(q.queueURL),
		}); err != nil {
			return xerrors.Errorf("failed to delete queue: %w", err)
		}

		q.logger.Info("deleted sqs queue")
		q.queueURL = ""
	}

	if q.queueURL == "" {
		output, err := q.client.CreateQueue(ctx, &sqs.CreateQueueInput{
			QueueName: aws.String(q.config.AWS.DLQ.Name),
		})
		if err != nil {
			return xerrors.Errorf("failed to create queue: %w", err)
		}

		q.logger.Info("created sqs queue", zap.Reflect("output", output))
	}

	return nil
}
