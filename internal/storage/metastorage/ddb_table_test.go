package metastorage

import (
	"context"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"

	"github.com/coinbase/chainstorage/internal/services"
	metastoragemocks "github.com/coinbase/chainstorage/internal/storage/metastorage/mocks"
	"github.com/coinbase/chainstorage/internal/storage/metastorage/model"
	"github.com/coinbase/chainstorage/internal/utils/retry"
	"github.com/coinbase/chainstorage/internal/utils/testutil"
)

type (
	DDBTableTestSuite struct {
		suite.Suite
		ctrl      *gomock.Controller
		dynamoAPI *metastoragemocks.MockDynamoAPI
		table     ddbTable
	}
)

const (
	tableName = "test"
)

func TestDDBTableTestSuite(t *testing.T) {
	suite.Run(t, new(DDBTableTestSuite))
}

func (s *DDBTableTestSuite) SetupTest() {
	manager := services.NewMockSystemManager()
	logger := manager.Logger()
	retryer := retry.New(retry.WithLogger(logger))

	s.ctrl = gomock.NewController(s.T())
	dynamoAPIMock := metastoragemocks.NewMockDynamoAPI(s.ctrl)
	mockTable := &tableDBAPI{
		TableName: tableName,
		DBAPI:     dynamoAPIMock,
	}

	s.table = &ddbTableImpl{
		table:        mockTable,
		ddbEntryType: reflect.TypeOf(model.BlockMetaDataDDBEntry{}),
		retry:        retryer,
	}
	s.dynamoAPI = dynamoAPIMock
}

func (s *DDBTableTestSuite) TestGetItems_TransactionConflict_RetrySuccess() {
	require := testutil.Require(s.T())
	numItems := 20

	ctx := context.Background()
	entries := makeTestDDBEntries(numItems)
	attributesMap, err := testDDBEntriesToAttributeMaps(entries)
	require.NoError(err)
	keyMaps := makeKeyMapsForTestDDBEntries(numItems)

	seen := sync.Map{}
	attempts := 0
	s.dynamoAPI.EXPECT().TransactGetItemsWithContext(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, input *dynamodb.TransactGetItemsInput) (
			*dynamodb.TransactGetItemsOutput, error) {
			attempts += 1
			if attempts == 1 {
				require.Equal(numItems, len(input.TransactItems))
				mockCancelReasons := make([]*dynamodb.CancellationReason, numItems)
				for i := range mockCancelReasons {
					mockCancelReasons[i] = &dynamodb.CancellationReason{
						Code: aws.String("None"),
					}
				}

				mockCancelReasons[1] = &dynamodb.CancellationReason{
					Code:    aws.String("TransactionConflict"),
					Message: aws.String("Transaction is ongoing for the item"),
				}
				return &dynamodb.TransactGetItemsOutput{}, &dynamodb.TransactionCanceledException{
					CancellationReasons: mockCancelReasons,
				}
			}

			transactItems := input.TransactItems
			begin := numItems
			end := -1
			for _, item := range transactItems {
				var out StringMap
				require.Equal(tableName, *item.Get.TableName)
				err := dynamodbattribute.UnmarshalMap(item.Get.Key, &out)
				require.NoError(err)
				require.NotEmpty(out["pk"])
				require.NotEmpty(out["sk"])

				indexString := strings.Split(out["pk"].(string), "-")[1]
				index, err := strconv.ParseInt(indexString, 10, 64)
				require.NoError(err)
				_, ok := seen.LoadOrStore(index, struct{}{})
				require.False(ok)

				if int(index) > end {
					end = int(index)
				}
				if int(index) < begin {
					begin = int(index)
				}
			}

			responses := make([]*dynamodb.ItemResponse, end+1-begin)
			for i := begin; i <= end; i++ {
				responses[i-begin] = &dynamodb.ItemResponse{Item: attributesMap[i]}
			}
			return &dynamodb.TransactGetItemsOutput{
				Responses: responses,
			}, nil
		}).Times(2)

	items, err := s.table.GetItems(ctx, keyMaps)
	require.NoError(err)
	require.Equal(numItems, len(items))

	for i := 0; i < numItems; i++ {
		_, ok := seen.LoadOrStore(int64(i), struct{}{})
		require.True(ok, fmt.Sprintf("items[%v] not seen", i))
	}
}

func (s *DDBTableTestSuite) TestGetItems_TransactionConflict_RetryFailure() {
	require := testutil.Require(s.T())
	numItems := 20

	ctx := context.Background()
	keyMaps := makeKeyMapsForTestDDBEntries(numItems)

	s.dynamoAPI.EXPECT().TransactGetItemsWithContext(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, input *dynamodb.TransactGetItemsInput) (
			*dynamodb.TransactGetItemsOutput, error) {
			require.Equal(numItems, len(input.TransactItems))
			mockCancelReasons := make([]*dynamodb.CancellationReason, numItems)
			for i := range mockCancelReasons {
				mockCancelReasons[i] = &dynamodb.CancellationReason{
					Code: aws.String("None"),
				}
			}

			mockCancelReasons[1] = &dynamodb.CancellationReason{
				Code:    aws.String("TransactionConflict"),
				Message: aws.String("Transaction is ongoing for the item"),
			}
			return &dynamodb.TransactGetItemsOutput{}, &dynamodb.TransactionCanceledException{
				CancellationReasons: mockCancelReasons,
			}
		}).AnyTimes()
	_, err := s.table.GetItems(ctx, keyMaps)
	require.Error(err)
}

func testDDBEntriesToAttributeMaps(
	entries []interface{},
) ([]map[string]*dynamodb.AttributeValue, error) {
	attributes := make([]map[string]*dynamodb.AttributeValue, len(entries))
	for i := range entries {
		attribute, err := dynamodbattribute.MarshalMap(entries[i])
		if err != nil {
			return nil, err
		}
		attributes[i] = attribute
	}
	return attributes, nil
}

func makeTestDDBEntries(numEntries int) []interface{} {
	entries := make([]interface{}, numEntries)
	for i := 0; i < numEntries; i++ {
		pk := fmt.Sprintf("pk-%v", i)
		sk := fmt.Sprintf("sk-%v", i)
		entries[i] = makeTestDDBEntry(pk, sk)
	}

	return entries
}

func makeTestDDBEntry(pk string, sk string) interface{} {
	if pk == "" {
		pk = "pk"
	}

	if sk == "" {
		sk = "sk"
	}

	return struct {
		PartitionKey string `dynamodbav:"pk"`
		SortKey      string `dynamodbav:"sk"`
	}{
		PartitionKey: pk,
		SortKey:      sk,
	}
}

func makeKeyMapsForTestDDBEntries(numEntries int) []StringMap {
	results := make([]StringMap, numEntries)
	for i := 0; i < numEntries; i++ {
		results[i] = StringMap{
			"pk": fmt.Sprintf("pk-%v", i),
			"sk": fmt.Sprintf("sk-%v", i),
		}
	}
	return results
}
