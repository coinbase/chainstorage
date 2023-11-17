package dynamodb

import (
	"context"
	"reflect"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
	"golang.org/x/xerrors"

	ddbmodel "github.com/coinbase/chainstorage/internal/storage/metastorage/dynamodb/model"
	"github.com/coinbase/chainstorage/internal/storage/metastorage/internal"
	"github.com/coinbase/chainstorage/internal/storage/metastorage/model"
	"github.com/coinbase/chainstorage/internal/utils/instrument"
)

type (
	transactionStorageImpl struct {
		transactionTable                 ddbTable
		instrumentAddOrUpdateTransaction instrument.Instrument
		instrumentGetTransaction         instrument.InstrumentWithResult[[]*model.Transaction]
	}
)

var _ internal.TransactionStorage = (*transactionStorageImpl)(nil)

func newTransactionStorage(params Params) (internal.TransactionStorage, error) {
	attrDefs := []*dynamodb.AttributeDefinition{
		{
			AttributeName: aws.String(ddbmodel.TransactionPidKeyName),
			AttributeType: awsStringType,
		},
		{
			AttributeName: aws.String(ddbmodel.TransactionSortKeyName),
			AttributeType: awsStringType,
		},
	}
	keySchema := []*dynamodb.KeySchemaElement{
		{
			AttributeName: aws.String(ddbmodel.TransactionPidKeyName),
			KeyType:       hashKeyType,
		},
		{
			AttributeName: aws.String(ddbmodel.TransactionSortKeyName),
			KeyType:       rangeKeyType,
		},
	}

	transactionTable, err := newDDBTable(
		params.Config.AWS.DynamoDB.TransactionTable,
		reflect.TypeOf(ddbmodel.TransactionDDBEntry{}),
		keySchema, attrDefs, nil,
		params,
	)
	if err != nil {
		return nil, xerrors.Errorf("failed to create transaction table accessor: %w", err)
	}

	metrics := params.Metrics.SubScope("transaction_storage")

	return &transactionStorageImpl{
		transactionTable:                 transactionTable,
		instrumentAddOrUpdateTransaction: instrument.New(metrics, "add_transactions"),
		instrumentGetTransaction:         instrument.NewWithResult[[]*model.Transaction](metrics, "get_transaction"),
	}, nil
}

func (t *transactionStorageImpl) AddTransactions(ctx context.Context, transactions []*model.Transaction, parallelism int) error {
	if len(transactions) == 0 {
		return nil
	}

	return t.instrumentAddOrUpdateTransaction.Instrument(ctx, func(ctx context.Context) error {
		entries := make([]any, len(transactions))
		for i, transaction := range transactions {
			entry := ddbmodel.NewTransactionDDBEntry(transaction)
			entries[i] = entry
		}

		if err := t.transactionTable.BatchWriteItems(ctx, entries, parallelism); err != nil {
			return xerrors.Errorf("failed to add transactions: %w", err)
		}

		return nil
	})
}

func (t *transactionStorageImpl) GetTransaction(ctx context.Context, tag uint32, transactionHash string) ([]*model.Transaction, error) {
	return t.instrumentGetTransaction.Instrument(ctx, func(ctx context.Context) ([]*model.Transaction, error) {
		partitionKey := ddbmodel.MakeTransactionPartitionKey(tag, transactionHash)
		pkCondition := expression.Key(ddbmodel.TransactionPidKeyName).
			Equal(expression.Value(partitionKey))
		builder := expression.NewBuilder().WithKeyCondition(pkCondition)
		expr, err := builder.Build()
		if err != nil {
			return nil, xerrors.Errorf("failed to build expression for GetTransaction - QueryItems: %w", err)
		}

		outputItems, err := t.transactionTable.QueryItems(ctx, &QueryItemsRequest{
			KeyConditionExpression:    expr.KeyCondition(),
			ExpressionAttributeNames:  expr.Names(),
			ExpressionAttributeValues: expr.Values(),
			ConsistentRead:            true,
		})
		if err != nil {
			return nil, xerrors.Errorf("failed to get transaction: %w", err)
		}

		transactionToBlocks := make([]*model.Transaction, len(outputItems))
		for i, item := range outputItems {
			transactionDDBEntry, ok := item.(*ddbmodel.TransactionDDBEntry)
			if !ok {
				return nil, xerrors.Errorf("failed to convert output (%+v) to TransactionDDBEntry", item)
			}

			transaction, err := ddbmodel.TransformToTransaction(transactionDDBEntry)
			if err != nil {
				return nil, xerrors.Errorf("failed to transform ddb entry (%+v) to transaction", transactionDDBEntry)
			}
			transactionToBlocks[i] = transaction
		}

		return transactionToBlocks, nil
	})
}
