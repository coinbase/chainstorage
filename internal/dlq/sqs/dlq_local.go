package sqs

import (
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/sqs"
	"go.uber.org/zap"

	"github.com/coinbase/chainstorage/internal/utils/pointer"
)

func (q *dlqImpl) resetLocalResources() error {
	q.logger.Info("initializing dlq")

	if err := q.initQueueURL(); err != nil {
		var aerr awserr.Error
		if !errors.As(err, &aerr) || aerr.Code() != sqs.ErrCodeQueueDoesNotExist {
			return fmt.Errorf("failed to init queue url: %w", err)
		}
	}

	if q.config.AWS.IsResetLocal && q.queueURL != "" {
		if _, err := q.client.DeleteQueue(&sqs.DeleteQueueInput{
			QueueUrl: pointer.Ref(q.queueURL),
		}); err != nil {
			return fmt.Errorf("failed to delete queue: %w", err)
		}

		q.logger.Info("deleted sqs queue")
		q.queueURL = ""
	}

	if q.queueURL == "" {
		output, err := q.client.CreateQueue(&sqs.CreateQueueInput{
			QueueName: pointer.Ref(q.config.AWS.DLQ.Name),
		})
		if err != nil {
			return fmt.Errorf("failed to create queue: %w", err)
		}

		q.logger.Info("created sqs queue", zap.Reflect("output", output))
	}

	return nil
}
