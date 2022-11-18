package metastorage

import (
	"context"
	"reflect"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/storage/internal/errors"
	"github.com/coinbase/chainstorage/internal/utils/log"
	"github.com/coinbase/chainstorage/internal/utils/retry"
	"github.com/coinbase/chainstorage/internal/utils/syncgroup"
)

var (
	awsStringType = aws.String("S")
	awsNumberType = aws.String("N")
	hashKeyType   = aws.String("HASH")
	rangeKeyType  = aws.String("RANGE")
)

const (
	maxWriteItemsSize       = 25
	maxTransactGetItemsSize = 25
	maxQueryIterations      = 10
	maxWriteWorkers         = 10
	maxGetWorkers           = 4
)

type (
	ddbTable interface {
		WriteItem(ctx context.Context, items interface{}) error
		// WriteItems will parallelize writing items under the hood, but no guarantee on order, may also result in partial write
		WriteItems(ctx context.Context, items []interface{}) error
		// TransactWriteItems guarantees all or nothing write for input items but does have size limit (maxWriteItemsSize)
		TransactWriteItems(ctx context.Context, items []interface{}) error
		GetItem(ctx context.Context, keyMap StringMap) (interface{}, error)
		GetItems(ctx context.Context, keys []StringMap) ([]interface{}, error)
		QueryItems(ctx context.Context, indexName string, keyConditionExpression string,
			expressionAttributeValues map[string]*dynamodb.AttributeValue) ([]interface{}, error)
	}

	// DynamoAPI For mock generation for testing purpose
	DynamoAPI = dynamodbiface.DynamoDBAPI

	ddbTableImpl struct {
		table        *tableDBAPI
		ddbEntryType reflect.Type
		retry        retry.Retry
	}

	tableDBAPI struct {
		TableName string
		DBAPI     DynamoAPI
	}

	StringMap map[string]interface{}
)

func newDDBTable(
	tableName string,
	ddbEntryType reflect.Type,
	keySchema []*dynamodb.KeySchemaElement,
	attrDefs []*dynamodb.AttributeDefinition,
	globalSecondaryIndexes []*dynamodb.GlobalSecondaryIndex,
	params Params,
) (ddbTable, error) {
	logger := log.WithPackage(params.Logger)
	retry := retry.New(retry.WithLogger(logger))
	awsTable := newTableAPI(tableName, params.Session)

	table := ddbTableImpl{
		table:        awsTable,
		ddbEntryType: ddbEntryType,
		retry:        retry,
	}
	if params.Config.AWS.IsLocalStack {
		err := initLocalDb(
			awsTable.DBAPI,
			params.Logger,
			awsTable.TableName,
			keySchema, attrDefs, globalSecondaryIndexes,
			params.Config.AWS.IsResetLocal,
		)
		if err != nil {
			return nil, xerrors.Errorf("failed to prepare local resources for event storage: %w", err)
		}
	}
	return &table, nil
}

func newTableAPI(tableName string, session *session.Session) *tableDBAPI {
	return &tableDBAPI{
		TableName: tableName,
		DBAPI:     dynamodb.New(session),
	}
}

func (d *ddbTableImpl) getTransactWriteItem(
	ddbEntry interface{}) (*dynamodb.TransactWriteItem, error) {
	item, err := dynamodbattribute.MarshalMap(ddbEntry)
	if err != nil {
		return nil, xerrors.Errorf("failed to get marshal ddb entry (%v): %w", ddbEntry, err)
	}
	writeItem := &dynamodb.TransactWriteItem{
		Put: &dynamodb.Put{
			TableName: aws.String(d.table.TableName),
			Item:      item,
		},
	}
	return writeItem, nil
}

func (d *ddbTableImpl) WriteItem(ctx context.Context, item interface{}) error {
	mItem, err := dynamodbattribute.MarshalMap(item)
	if err != nil {
		return xerrors.Errorf("failed to get marshal ddb entry (%v): %w", item, err)
	}
	_, err = d.table.DBAPI.PutItemWithContext(
		ctx,
		&dynamodb.PutItemInput{
			Item:      mItem,
			TableName: aws.String(d.table.TableName),
		},
	)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok && aerr.Code() == request.CanceledErrorCode {
			return errors.ErrRequestCanceled
		}
		return xerrors.Errorf("failed to write item: %w", err)
	}
	return nil
}

func (d *ddbTableImpl) TransactWriteItems(ctx context.Context, items []interface{}) error {
	if len(items) == 0 {
		return nil
	}
	batchWriteItems := make([]*dynamodb.TransactWriteItem, len(items))
	var err error
	for i, item := range items {
		batchWriteItems[i], err = d.getTransactWriteItem(item)
		if err != nil {
			return xerrors.Errorf("failed to transact write items: %w", err)
		}
	}

	_, err = d.table.DBAPI.TransactWriteItemsWithContext(
		ctx,
		&dynamodb.TransactWriteItemsInput{
			TransactItems: batchWriteItems,
		},
	)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok && aerr.Code() == request.CanceledErrorCode {
			return errors.ErrRequestCanceled
		}
		return xerrors.Errorf("failed to transact write items: %w", err)
	}
	return nil
}

func (d *ddbTableImpl) WriteItems(ctx context.Context, items []interface{}) error {
	// Limit parallel writes to reduce the chance of getting throttled.
	g, ctx := syncgroup.New(ctx, syncgroup.WithThrottling(maxWriteWorkers))
	for i := 0; i < len(items); i += maxWriteItemsSize {
		begin, end := i, i+maxWriteItemsSize
		if end > len(items) {
			end = len(items)
		}

		g.Go(func() error {
			if err := d.TransactWriteItems(ctx, items[begin:end]); err != nil {
				return xerrors.Errorf("failed to write items: %w", err)
			}
			return nil
		})
	}
	return g.Wait()
}

func (d *ddbTableImpl) transactGetItems(ctx context.Context, inputKeys []StringMap, outputItems []interface{}) error {
	if len(inputKeys) != len(outputItems) {
		return xerrors.New("inputKeys does not have the same size as outputItems")
	}
	if len(inputKeys) == 0 {
		return nil
	}
	inputItems := make([]*dynamodb.TransactGetItem, len(inputKeys))
	for i, keyMap := range inputKeys {
		dynamodbKey, err := dynamodbattribute.MarshalMap(keyMap)
		if err != nil {
			return xerrors.Errorf("could not marshal given key(%v):%w", keyMap, err)
		}
		inputItems[i] = &dynamodb.TransactGetItem{
			Get: &dynamodb.Get{
				Key:       dynamodbKey,
				TableName: aws.String(d.table.TableName),
			},
		}
	}

	if err := d.retry.Retry(ctx, func(ctx context.Context) error {
		output, err := d.table.DBAPI.TransactGetItemsWithContext(ctx, &dynamodb.TransactGetItemsInput{
			TransactItems: inputItems,
		})

		if err != nil {
			if transactionCanceledException, ok := err.(*dynamodb.TransactionCanceledException); ok {
				reasons := transactionCanceledException.CancellationReasons
				for _, reason := range reasons {
					if reason.Code != nil && *reason.Code == dynamodb.BatchStatementErrorCodeEnumTransactionConflict {
						return retry.Retryable(
							xerrors.Errorf("failed to TransactGetItems because of transaction conflict, reason=(%v)", reason))
					}
				}
			}

			if aerr, ok := err.(awserr.Error); ok && aerr.Code() == request.CanceledErrorCode {
				return errors.ErrRequestCanceled
			}
			return err
		}

		// verify requested items are retrieved
		// if missing then corresponding ItemResponse at same index will be empty
		for index, item := range inputItems {
			if len(output.Responses[index].Item) == 0 {
				return xerrors.Errorf("missing item key=%v: %w", item.Get.ProjectionExpression, errors.ErrItemNotFound)
			}
			err = dynamodbattribute.UnmarshalMap(output.Responses[index].Item, outputItems[index])
			if err != nil {
				return xerrors.Errorf("failed to unmarshal item (%v, %v): %w", output.Responses[index].Item, outputItems[index], err)
			}
		}

		return nil

	}); err != nil {
		return err
	}

	return nil
}

func (d *ddbTableImpl) GetItems(ctx context.Context,
	inputKeys []StringMap) ([]interface{}, error) {
	g, gCtx := syncgroup.New(ctx, syncgroup.WithThrottling(maxGetWorkers))
	outputItems := make([]interface{}, len(inputKeys))
	for i := range outputItems {
		outputItems[i] = reflect.New(d.ddbEntryType).Interface()
	}
	for i := 0; i < len(inputKeys); i += maxTransactGetItemsSize {
		begin, end := i, i+maxWriteItemsSize
		if end > len(inputKeys) {
			end = len(inputKeys)
		}
		g.Go(func() error {
			if err := d.transactGetItems(gCtx, inputKeys[begin:end], outputItems[begin:end]); err != nil {
				return xerrors.Errorf("failed to transact get items: %w", err)
			}
			return nil
		})
	}
	return outputItems, g.Wait()
}

func (d *ddbTableImpl) GetItem(ctx context.Context, keyMap StringMap) (interface{}, error) {
	dynamodbKey, err := dynamodbattribute.MarshalMap(keyMap)
	if err != nil {
		return nil, xerrors.Errorf("could not marshal given key(%v):%w", keyMap, err)
	}
	input := &dynamodb.GetItemInput{
		Key:            dynamodbKey,
		TableName:      aws.String(d.table.TableName),
		ConsistentRead: aws.Bool(true),
	}
	output, err := d.table.DBAPI.GetItemWithContext(ctx, input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok && aerr.Code() == request.CanceledErrorCode {
			return nil, errors.ErrRequestCanceled
		}
		return nil, xerrors.Errorf("failed to get item for key (%v): %w", keyMap, err)
	}
	if output.Item == nil {
		return nil, errors.ErrItemNotFound
	}
	outputItem := reflect.New(d.ddbEntryType).Interface()
	err = dynamodbattribute.UnmarshalMap(output.Item, outputItem)
	if err != nil {
		return nil, xerrors.Errorf("failed to unmarshal item (%v): %w", output.Item, err)
	}
	return outputItem, nil
}

func (d *ddbTableImpl) QueryItems(ctx context.Context, indexName string, keyConditionExpression string,
	expressionAttributeValues map[string]*dynamodb.AttributeValue) ([]interface{}, error) {
	queryInput := &dynamodb.QueryInput{
		ExclusiveStartKey:         nil,
		IndexName:                 aws.String(indexName),
		KeyConditionExpression:    aws.String(keyConditionExpression),
		Select:                    aws.String(dynamodb.SelectAllAttributes),
		ExpressionAttributeValues: expressionAttributeValues,
		TableName:                 aws.String(d.table.TableName),
	}
	outputItems := make([]interface{}, 0)
	iterations := 0
	for true {
		queryOutput, err := d.table.DBAPI.QueryWithContext(ctx, queryInput)
		if err != nil {
			if aerr, ok := err.(awserr.Error); ok && aerr.Code() == request.CanceledErrorCode {
				return nil, errors.ErrRequestCanceled
			}
			return nil, xerrors.Errorf("failed to get query items (index=%v, keyConditionExpression=%v): %w", indexName, keyConditionExpression, err)
		}
		for _, item := range queryOutput.Items {
			outputItem := reflect.New(d.ddbEntryType).Interface()
			err = dynamodbattribute.UnmarshalMap(item, outputItem)
			if err != nil {
				return nil, xerrors.Errorf("failed to unmarshal item (%v): %w", item, err)
			}
			outputItems = append(outputItems, outputItem)
		}
		if len(queryOutput.LastEvaluatedKey) == 0 {
			break
		}
		queryInput.ExclusiveStartKey = queryOutput.LastEvaluatedKey

		iterations += 1
		if iterations >= maxQueryIterations {
			return nil, xerrors.Errorf("too many query iterations (index=%v, keyConditionExpression=%v)", indexName, keyConditionExpression)
		}
	}
	return outputItems, nil
}
