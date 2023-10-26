package dynamodb

import (
	"go.uber.org/zap"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
)

func initLocalDb(
	db dynamodbiface.DynamoDBAPI,
	log *zap.Logger,
	tableName string,
	keySchema []*dynamodb.KeySchemaElement,
	attrDefs []*dynamodb.AttributeDefinition,
	globalSecondaryIndexes []*dynamodb.GlobalSecondaryIndex,
	reset bool,
) error {
	log.Debug("Initializing local db")

	exists, err := doesTableExist(db, tableName)
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
		if err = deleteTable(db, log, tableName); err != nil {
			return err
		}

		return createTable(db, log, tableName, keySchema, attrDefs, globalSecondaryIndexes)
	}

	log.Info("Table does not exist.")
	return createTable(db, log, tableName, keySchema, attrDefs, globalSecondaryIndexes)
}

func createTable(db dynamodbiface.DynamoDBAPI, log *zap.Logger,
	tableName string,
	keySchema []*dynamodb.KeySchemaElement,
	attrDefs []*dynamodb.AttributeDefinition,
	globalSecondaryIndexes []*dynamodb.GlobalSecondaryIndex,
) error {
	log.Info("Creating table " + tableName)

	input := &dynamodb.CreateTableInput{
		TableName:            aws.String(tableName),
		AttributeDefinitions: attrDefs,
		KeySchema:            keySchema,
		ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(1),
			WriteCapacityUnits: aws.Int64(1),
		},
		GlobalSecondaryIndexes: globalSecondaryIndexes,
	}

	_, err := db.CreateTable(input)
	return err
}

func deleteTable(db dynamodbiface.DynamoDBAPI, log *zap.Logger, tableName string) error {
	log.Info("Deleting table " + tableName)

	input := &dynamodb.DeleteTableInput{
		TableName: aws.String(tableName),
	}

	_, err := db.DeleteTable(input)
	if err == nil {
		return nil
	}

	if aerr, ok := err.(awserr.Error); ok {
		if aerr.Code() == dynamodb.ErrCodeResourceNotFoundException {
			return nil
		}
	}

	return err
}

func doesTableExist(db dynamodbiface.DynamoDBAPI, tableName string) (exists bool, err error) {
	_, err = db.DescribeTable(&dynamodb.DescribeTableInput{
		TableName: aws.String(tableName),
	})

	if err == nil {
		return true, nil
	}

	if aerr, ok := err.(awserr.Error); ok {
		if aerr.Code() == dynamodb.ErrCodeResourceNotFoundException {
			return false, nil
		}
	}

	return false, err
}
