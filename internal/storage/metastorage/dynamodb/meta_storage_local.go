package dynamodb

import (
	"context"
	"errors"

	"go.uber.org/zap"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

func initLocalDb(
	db DynamoAPI,
	log *zap.Logger,
	tableName string,
	keySchema []types.KeySchemaElement,
	attrDefs []types.AttributeDefinition,
	globalSecondaryIndexes []types.GlobalSecondaryIndex,
	reset bool,
) error {
	log.Debug("Initializing local db")
	ctx := context.Background()

	exists, err := doesTableExist(ctx, db, tableName)
	if err != nil {
		log.Info("Failed to check if the table exists in local db or not")
		return err
	}
	if exists {
		if !reset {
			// Keep the table intact.
			return nil
		}

		log.Info("Table already exists in local db, so dropping it first.")
		if err = deleteTable(ctx, db, log, tableName); err != nil {
			return err
		}

		return createTable(ctx, db, log, tableName, keySchema, attrDefs, globalSecondaryIndexes)
	}

	log.Info("Table does not exist.")
	return createTable(ctx, db, log, tableName, keySchema, attrDefs, globalSecondaryIndexes)
}

func createTable(ctx context.Context, db DynamoAPI, log *zap.Logger,
	tableName string,
	keySchema []types.KeySchemaElement,
	attrDefs []types.AttributeDefinition,
	globalSecondaryIndexes []types.GlobalSecondaryIndex,
) error {
	log.Info("Creating table " + tableName)

	input := &dynamodb.CreateTableInput{
		TableName:            aws.String(tableName),
		AttributeDefinitions: attrDefs,
		KeySchema:            keySchema,
		ProvisionedThroughput: &types.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(1),
			WriteCapacityUnits: aws.Int64(1),
		},
		GlobalSecondaryIndexes: globalSecondaryIndexes,
	}

	_, err := db.CreateTable(ctx, input)
	return err
}

func deleteTable(ctx context.Context, db DynamoAPI, log *zap.Logger, tableName string) error {
	log.Info("Deleting table " + tableName)

	input := &dynamodb.DeleteTableInput{
		TableName: aws.String(tableName),
	}

	_, err := db.DeleteTable(ctx, input)
	if err == nil {
		return nil
	}

	var resourceNotFoundErr *types.ResourceNotFoundException
	if errors.As(err, &resourceNotFoundErr) {
		return nil
	}

	return err
}

func doesTableExist(ctx context.Context, db DynamoAPI, tableName string) (exists bool, err error) {
	_, err = db.DescribeTable(ctx, &dynamodb.DescribeTableInput{
		TableName: aws.String(tableName),
	})

	if err == nil {
		return true, nil
	}

	var resourceNotFoundErr *types.ResourceNotFoundException
	if errors.As(err, &resourceNotFoundErr) {
		return false, nil
	}

	return false, err
}
