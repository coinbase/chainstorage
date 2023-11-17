package dynamodb

import (
	"context"
	"reflect"

	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/storage/internal/errors"
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
		WriteItem(ctx context.Context, items any) error
		// WriteItems will parallelize writing items with TransactWriteItems, but no guarantee on order, may also result in partial write
		WriteItems(ctx context.Context, items []any) error
		// TransactWriteItems guarantees all or nothing write for input items but does have size limit (maxWriteItemsSize)
		TransactWriteItems(ctx context.Context, items []any) error
		GetItem(ctx context.Context, keyMap StringMap) (any, error)
		GetItems(ctx context.Context, keys []StringMap) ([]any, error)
		QueryItems(ctx context.Context, request *QueryItemsRequest) ([]any, error)
		// BatchWriteItems will parallelize writing items with BatchWriteItems, with a configurable parallelism
		BatchWriteItems(ctx context.Context, items []any, parallelism int) error
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

	QueryItemsRequest struct {
		ExclusiveStartKey         map[string]*dynamodb.AttributeValue
		KeyConditionExpression    *string
		ExpressionAttributeNames  map[string]*string
		ExpressionAttributeValues map[string]*dynamodb.AttributeValue
		IndexName                 string
		ConsistentRead            bool
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
	//logger := log.WithPackage(params.Logger)
	retry := retry.New()

	//TODO: enable assume-role if params.Config.AWS.DynamoDB.Arn is not empty
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
	ddbEntry any) (*dynamodb.TransactWriteItem, error) {
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

func (d *ddbTableImpl) WriteItem(ctx context.Context, item any) error {
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

func (d *ddbTableImpl) TransactWriteItems(ctx context.Context, items []any) error {
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

// WriteItems Perform TransactWriteItems in parallel
// TODO: Refactor this method as the public interface for TransactWriteItems without limit and change TransactWriteItems to a private helper method.
func (d *ddbTableImpl) WriteItems(ctx context.Context, items []any) error {
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

func (d *ddbTableImpl) transactGetItems(ctx context.Context, inputKeys []StringMap, outputItems []any) error {
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

	return d.retry.Retry(ctx, func(ctx context.Context) error {
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

	})
}

func (d *ddbTableImpl) GetItems(ctx context.Context,
	inputKeys []StringMap) ([]any, error) {
	g, gCtx := syncgroup.New(ctx, syncgroup.WithThrottling(maxGetWorkers))
	outputItems := make([]any, len(inputKeys))
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

func (d *ddbTableImpl) GetItem(ctx context.Context, keyMap StringMap) (any, error) {
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

func (d *ddbTableImpl) QueryItems(ctx context.Context, req *QueryItemsRequest) ([]any, error) {
	queryInput := &dynamodb.QueryInput{
		ExclusiveStartKey:         req.ExclusiveStartKey,
		KeyConditionExpression:    req.KeyConditionExpression,
		Select:                    aws.String(dynamodb.SelectAllAttributes),
		ExpressionAttributeNames:  req.ExpressionAttributeNames,
		ExpressionAttributeValues: req.ExpressionAttributeValues,
		TableName:                 aws.String(d.table.TableName),
		ConsistentRead:            aws.Bool(req.ConsistentRead),
	}

	if req.IndexName != "" {
		queryInput.IndexName = aws.String(req.IndexName)
	}

	outputItems := make([]any, 0)
	iterations := 0
	for true {
		queryOutput, err := d.table.DBAPI.QueryWithContext(ctx, queryInput)
		if err != nil {
			if aerr, ok := err.(awserr.Error); ok && aerr.Code() == request.CanceledErrorCode {
				return nil, errors.ErrRequestCanceled
			}
			return nil, xerrors.Errorf("failed to get query items (index=%v, keyConditionExpression=%v): %w", req.IndexName, req.KeyConditionExpression, err)
		}

		if len(queryOutput.Items) == 0 {
			return nil, errors.ErrItemNotFound
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
			return nil, xerrors.Errorf("too many query iterations (index=%v, keyConditionExpression=%v)", req.IndexName, req.KeyConditionExpression)
		}
	}

	return outputItems, nil
}

func (d *ddbTableImpl) BatchWriteItems(ctx context.Context, items []any, parallelism int) error {
	inputChannel := make(chan []any, len(items)/maxWriteItemsSize+1)
	for i := 0; i < len(items); i += maxWriteItemsSize {
		begin, end := i, i+maxWriteItemsSize
		if end > len(items) {
			end = len(items)
		}
		inputChannel <- items[begin:end]
	}
	close(inputChannel)

	group, ctx := syncgroup.New(ctx)
	for i := 0; i < parallelism; i++ {
		group.Go(func() error {
			for batchItems := range inputChannel {
				if err := d.batchWriteItemsWithLimit(ctx, batchItems); err != nil {
					return xerrors.Errorf("failed to batch write items: %w", err)
				}
			}

			return nil
		})
	}

	return group.Wait()
}

func (d *ddbTableImpl) batchWriteItemsWithLimit(ctx context.Context, items []any) error {
	numItems := len(items)
	if numItems == 0 {
		return nil
	}

	if numItems > maxWriteItemsSize {
		return xerrors.Errorf("too many items: %v", numItems)
	}

	writeRequests := make([]*dynamodb.WriteRequest, numItems)
	for i, item := range items {
		writeRequest, err := d.getWriteRequest(item)
		if err != nil {
			return xerrors.Errorf("failed to prepare write items: %w", err)
		}

		writeRequests[i] = writeRequest
	}

	tableName := d.table.TableName
	numProcessed := 0
	return d.retry.Retry(ctx, func(ctx context.Context) error {
		input := &dynamodb.BatchWriteItemInput{
			RequestItems: map[string][]*dynamodb.WriteRequest{
				tableName: writeRequests,
			},
		}
		output, err := d.table.DBAPI.BatchWriteItemWithContext(ctx, input)
		if err != nil {
			return xerrors.Errorf("failed to batch write items: %w", err)
		}

		unprocessed := output.UnprocessedItems[tableName]
		numProcessed += len(writeRequests) - len(unprocessed)
		if len(unprocessed) > 0 {
			// If DynamoDB returns any unprocessed items, back off and then retry the batch operation on those items.
			// Ref: https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_BatchWriteItem.html
			writeRequests = unprocessed
			return retry.Retryable(xerrors.Errorf("failed to process %v items during batch write items", len(unprocessed)))
		}

		if numItems != numProcessed {
			return xerrors.Errorf("failed to write all items: expected=%v, actual=%v", numItems, numProcessed)
		}

		return nil
	})
}

func (d *ddbTableImpl) getWriteRequest(
	ddbEntry any) (*dynamodb.WriteRequest, error) {
	item, err := dynamodbattribute.MarshalMap(ddbEntry)
	if err != nil {
		return nil, xerrors.Errorf("failed to get marshal ddb entry (%v): %w", ddbEntry, err)
	}
	writeRequest := &dynamodb.WriteRequest{
		PutRequest: &dynamodb.PutRequest{
			Item: item,
		},
	}
	return writeRequest, nil
}
