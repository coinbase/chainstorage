package types

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"log"
	"math/big"
	"testing"
	"time"

	sdk "github.com/coinbase/rosetta-sdk-go/types"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestFromSDKBlock(t *testing.T) {
	require := require.New(t)

	block, expected := block(t)

	parsedBlock, err := FromSDKBlock(block)
	require.NoError(err)
	require.Equal(expected, parsedBlock)
}

func TestFromSDKBlocks(t *testing.T) {
	var (
		block1, expected1 = block(t)
		block2, expected2 = block(t)
		require           = require.New(t)
	)

	tests := []struct {
		name           string
		blocks         []*sdk.Block
		expectedBlocks []*Block
	}{
		{
			name:           "non-empty list",
			blocks:         []*sdk.Block{block1, block2},
			expectedBlocks: []*Block{expected1, expected2},
		},
		{
			name:           "empty list",
			blocks:         []*sdk.Block{},
			expectedBlocks: nil,
		},
		{
			name:           "nil list",
			blocks:         nil,
			expectedBlocks: nil,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			actual, err := FromSDKBlocks(test.blocks)
			require.NoError(err)
			require.Equal(test.expectedBlocks, actual)
		})
	}
}

func TestFromSDKBlock_Invalid(t *testing.T) {
	require := require.New(t)

	block := &sdk.Block{
		BlockIdentifier: nil,
	}

	_, err := FromSDKBlock(block)
	require.Error(err)
}

func TestToSDKOperation(t *testing.T) {
	require := require.New(t)

	rosettaOp := &Operation{
		OperationIdentifier: &OperationIdentifier{
			Index:        1,
			NetworkIndex: 0,
		},
		Type:              "OUTPUT",
		Status:            "SUCCESS",
		RelatedOperations: []*OperationIdentifier{},
		Account: &AccountIdentifier{
			Address: "DMr3fEiVrPWFpoCWS958zNtqgnFb7QWn9D",
			SubAccount: &SubAccountIdentifier{
				Address:  "DMr3fEiVrP",
				Metadata: map[string]*anypb.Any{},
			},
			Metadata: map[string]*anypb.Any{},
		},
		Amount: &Amount{
			Value: "1000000000000",
			Currency: &Currency{
				Symbol:   "DOGE",
				Decimals: 8,
				Metadata: map[string]*anypb.Any{},
			},
			Metadata: map[string]*anypb.Any{},
		},
		CoinChange: &CoinChange{
			CoinIdentifier: &CoinIdentifier{
				Identifier: "1e02ca1261cc5599582018c2278ab44f80c3d8f6e8ae5daa2a53afa65e530f1f",
			},
			CoinAction: CoinChange_COIN_CREATED,
		},
		Metadata: map[string]*anypb.Any{},
	}

	expectedOp := &sdk.Operation{
		OperationIdentifier: &sdk.OperationIdentifier{
			Index:        1,
			NetworkIndex: Int64Ref(0),
		},
		RelatedOperations: []*sdk.OperationIdentifier{},
		Type:              "OUTPUT",
		Status:            StringRef("SUCCESS"),
		Account: &sdk.AccountIdentifier{
			Address: "DMr3fEiVrPWFpoCWS958zNtqgnFb7QWn9D",
			SubAccount: &sdk.SubAccountIdentifier{
				Address:  "DMr3fEiVrP",
				Metadata: map[string]interface{}{},
			},
			Metadata: map[string]interface{}{},
		},
		Amount: &sdk.Amount{
			Value: "1000000000000",
			Currency: &sdk.Currency{
				Symbol:   "DOGE",
				Decimals: 8,
				Metadata: map[string]interface{}{},
			},
			Metadata: map[string]interface{}{},
		},
		CoinChange: &sdk.CoinChange{
			CoinIdentifier: &sdk.CoinIdentifier{
				Identifier: "1e02ca1261cc5599582018c2278ab44f80c3d8f6e8ae5daa2a53afa65e530f1f",
			},
			CoinAction: sdk.CoinCreated,
		},
		Metadata: map[string]interface{}{},
	}

	actualOp, err := ToSDKOperation(rosettaOp)
	require.NoError(err)
	require.NotNil(actualOp)
	require.Equal(expectedOp, actualOp)
}

func TestToSDKTransaction(t *testing.T) {
	require := require.New(t)

	size := 143.2
	sizeToAny, err := anypb.New(structpb.NewNumberValue(size))
	require.NoError(err)

	transaction := &Transaction{
		TransactionIdentifier: &TransactionIdentifier{
			Hash: "5b2a3f53f605d62c53e62932dac6925e3d74afa5a4b459745c36d42d0ed26a69",
		},
		Operations: []*Operation{
			{
				OperationIdentifier: &OperationIdentifier{
					Index:        0,
					NetworkIndex: 0,
				},
				RelatedOperations: []*OperationIdentifier{
					{
						Index:        0,
						NetworkIndex: 0,
					},
					{
						Index:        1,
						NetworkIndex: 1,
					},
				},
				Type:   "COINBASE",
				Status: "SUCCESS",
			},
			{
				OperationIdentifier: &OperationIdentifier{
					Index:        1,
					NetworkIndex: 0,
				},
				Type:              "OUTPUT",
				Status:            "SUCCESS",
				RelatedOperations: []*OperationIdentifier{},
				Account: &AccountIdentifier{
					Address: "DMr3fEiVrPWFpoCWS958zNtqgnFb7QWn9D",
					SubAccount: &SubAccountIdentifier{
						Address: "DMr3fEiVrP",
					},
				},
				Amount: &Amount{
					Value: "2345",
					Currency: &Currency{
						Symbol:   "DOGE",
						Decimals: 8,
					},
				},
				CoinChange: &CoinChange{
					CoinIdentifier: &CoinIdentifier{
						Identifier: "1e02ca1261cc5599582018c2278ab44f80c3d8f6e8ae5daa2a53afa65e530f1f",
					},
					CoinAction: CoinChange_COIN_CREATED,
				},
			},
		},
		RelatedTransactions: []*RelatedTransaction{
			{
				NetworkIdentifier: &NetworkIdentifier{
					Blockchain: "dogecoin",
					Network:    "mainnet",
					SubNetworkIdentifier: &SubNetworkIdentifier{
						Network: "mainnet",
					},
				},
				TransactionIdentifier: &TransactionIdentifier{
					Hash: "515f4f5c9e54541ec2e39e5d270347d5c6c5f2575e38663791daa44b5e8c2507",
				},
				Direction: RelatedTransaction_FORWARD,
			},
		},
		Metadata: map[string]*anypb.Any{
			"size": sizeToAny,
		},
	}

	expectedTransaction := &sdk.Transaction{
		TransactionIdentifier: &sdk.TransactionIdentifier{
			Hash: "5b2a3f53f605d62c53e62932dac6925e3d74afa5a4b459745c36d42d0ed26a69",
		},
		Operations: []*sdk.Operation{
			{
				OperationIdentifier: &sdk.OperationIdentifier{
					Index:        0,
					NetworkIndex: Int64Ref(0),
				},
				RelatedOperations: []*sdk.OperationIdentifier{
					{
						Index:        0,
						NetworkIndex: Int64Ref(0),
					},
					{
						Index:        1,
						NetworkIndex: Int64Ref(1),
					},
				},
				Type:     "COINBASE",
				Status:   StringRef("SUCCESS"),
				Metadata: map[string]interface{}{},
			},
			{
				OperationIdentifier: &sdk.OperationIdentifier{
					Index:        1,
					NetworkIndex: Int64Ref(0),
				},
				Type:              "OUTPUT",
				Status:            StringRef("SUCCESS"),
				RelatedOperations: []*sdk.OperationIdentifier{},
				Account: &sdk.AccountIdentifier{
					Address: "DMr3fEiVrPWFpoCWS958zNtqgnFb7QWn9D",
					SubAccount: &sdk.SubAccountIdentifier{
						Address:  "DMr3fEiVrP",
						Metadata: map[string]interface{}{},
					},
					Metadata: map[string]interface{}{},
				},
				Amount: &sdk.Amount{
					Value: "2345",
					Currency: &sdk.Currency{
						Symbol:   "DOGE",
						Decimals: 8,
						Metadata: map[string]interface{}{},
					},
					Metadata: map[string]interface{}{},
				},
				CoinChange: &sdk.CoinChange{
					CoinIdentifier: &sdk.CoinIdentifier{
						Identifier: "1e02ca1261cc5599582018c2278ab44f80c3d8f6e8ae5daa2a53afa65e530f1f",
					},
					CoinAction: sdk.CoinCreated,
				},
				Metadata: map[string]interface{}{},
			},
		},
		RelatedTransactions: []*sdk.RelatedTransaction{
			{
				NetworkIdentifier: &sdk.NetworkIdentifier{
					Blockchain: "dogecoin",
					Network:    "mainnet",
					SubNetworkIdentifier: &sdk.SubNetworkIdentifier{
						Network:  "mainnet",
						Metadata: map[string]interface{}{},
					},
				},
				TransactionIdentifier: &sdk.TransactionIdentifier{
					Hash: "515f4f5c9e54541ec2e39e5d270347d5c6c5f2575e38663791daa44b5e8c2507",
				},
				Direction: sdk.Forward,
			},
		},
		Metadata: map[string]interface{}{
			"size": size,
		},
	}

	sdkTransaction, err := ToSDKTransaction(transaction)
	require.NoError(err)
	require.Equal(sdkTransaction, expectedTransaction)
}

func mustTime(value string) time.Time {
	t, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		panic(err)
	}
	return t
}

func mustTimestamp(value string) *timestamppb.Timestamp {
	t := mustTime(value)
	return &timestamppb.Timestamp{
		Seconds: t.Unix(),
		Nanos:   int32(t.Nanosecond()),
	}
}

// block returns an sdk.Block and a
// corresponding Block.
func block(t *testing.T) (*sdk.Block, *Block) {
	require := require.New(t)

	difficulty := 3996375.432
	difficultyToAny, err := anypb.New(structpb.NewNumberValue(difficulty))
	require.NoError(err)

	bits := "1a0432b3"
	bitsToAny, err := anypb.New(structpb.NewStringValue(bits))
	require.NoError(err)

	index := int64(randInt(4))

	block := &sdk.Block{
		BlockIdentifier: &sdk.BlockIdentifier{
			Index: index,
			Hash:  randHash(),
		},
		ParentBlockIdentifier: &sdk.BlockIdentifier{
			Index: index - 1,
			Hash:  randHash(),
		},
		Timestamp: 1628103081050,
		Transactions: []*sdk.Transaction{
			{
				TransactionIdentifier: &sdk.TransactionIdentifier{
					Hash: randHash(),
				},
				Operations: []*sdk.Operation{
					{
						OperationIdentifier: &sdk.OperationIdentifier{
							Index:        0,
							NetworkIndex: Int64Ref(0),
						},
						RelatedOperations: []*sdk.OperationIdentifier{
							{
								Index:        0,
								NetworkIndex: Int64Ref(0),
							},
							{
								Index:        1,
								NetworkIndex: Int64Ref(1),
							},
						},
						Type:   "COINBASE",
						Status: StringRef("SUCCESS"),
					},
					{
						OperationIdentifier: &sdk.OperationIdentifier{
							Index:        1,
							NetworkIndex: Int64Ref(0),
						},
						Type:   "OUTPUT",
						Status: StringRef("SUCCESS"),
						Account: &sdk.AccountIdentifier{
							Address: "DMr3fEiVrPWFpoCWS958zNtqgnFb7QWn9D",
							SubAccount: &sdk.SubAccountIdentifier{
								Address: "DMr3fEiVrP",
							},
						},
						Amount: &sdk.Amount{
							Value: "1000000000000",
							Currency: &sdk.Currency{
								Symbol:   "DOGE",
								Decimals: 8,
							},
						},
						CoinChange: &sdk.CoinChange{
							CoinIdentifier: &sdk.CoinIdentifier{
								Identifier: "1e02ca1261cc5599582018c2278ab44f80c3d8f6e8ae5daa2a53afa65e530f1f",
							},
							CoinAction: sdk.CoinCreated,
						},
					},
				},
				RelatedTransactions: []*sdk.RelatedTransaction{
					{
						NetworkIdentifier: &sdk.NetworkIdentifier{
							Blockchain: "dogecoin",
							Network:    "mainnet",
						},
						TransactionIdentifier: &sdk.TransactionIdentifier{
							Hash: "515f4f5c9e54541ec2e39e5d270347d5c6c5f2575e38663791daa44b5e8c2507",
						},
						Direction: sdk.Forward,
					},
				},
			},
			{
				TransactionIdentifier: &sdk.TransactionIdentifier{
					Hash: randHash(),
				},
			},
		},
		Metadata: map[string]interface{}{
			"difficulty": difficulty,
			"bits":       bits,
		},
	}

	protoBlock := &Block{
		BlockIdentifier: &BlockIdentifier{
			Index: block.BlockIdentifier.Index,
			Hash:  block.BlockIdentifier.Hash,
		},
		ParentBlockIdentifier: &BlockIdentifier{
			Index: block.ParentBlockIdentifier.Index,
			Hash:  block.ParentBlockIdentifier.Hash,
		},
		Timestamp: mustTimestamp("2021-08-04T18:51:21.05Z"),
		Transactions: []*Transaction{
			{
				TransactionIdentifier: &TransactionIdentifier{
					Hash: block.Transactions[0].TransactionIdentifier.Hash,
				},
				Operations: []*Operation{
					{
						OperationIdentifier: &OperationIdentifier{
							Index:        0,
							NetworkIndex: 0,
						},
						RelatedOperations: []*OperationIdentifier{
							{
								Index:        0,
								NetworkIndex: 0,
							},
							{
								Index:        1,
								NetworkIndex: 1,
							},
						},
						Type:     "COINBASE",
						Status:   "SUCCESS",
						Metadata: map[string]*anypb.Any{},
					},
					{
						OperationIdentifier: &OperationIdentifier{
							Index:        1,
							NetworkIndex: 0,
						},
						Type:              "OUTPUT",
						Status:            "SUCCESS",
						RelatedOperations: []*OperationIdentifier{},
						Account: &AccountIdentifier{
							Address: "DMr3fEiVrPWFpoCWS958zNtqgnFb7QWn9D",
							SubAccount: &SubAccountIdentifier{
								Address:  "DMr3fEiVrP",
								Metadata: map[string]*anypb.Any{},
							},
							Metadata: map[string]*anypb.Any{},
						},
						Amount: &Amount{
							Value: "1000000000000",
							Currency: &Currency{
								Symbol:   "DOGE",
								Decimals: 8,
								Metadata: map[string]*anypb.Any{},
							},
							Metadata: map[string]*anypb.Any{},
						},
						CoinChange: &CoinChange{
							CoinIdentifier: &CoinIdentifier{
								Identifier: "1e02ca1261cc5599582018c2278ab44f80c3d8f6e8ae5daa2a53afa65e530f1f",
							},
							CoinAction: CoinChange_COIN_CREATED,
						},
						Metadata: map[string]*anypb.Any{},
					},
				},
				RelatedTransactions: []*RelatedTransaction{
					{
						NetworkIdentifier: &NetworkIdentifier{
							Blockchain: "dogecoin",
							Network:    "mainnet",
						},
						TransactionIdentifier: &TransactionIdentifier{
							Hash: "515f4f5c9e54541ec2e39e5d270347d5c6c5f2575e38663791daa44b5e8c2507",
						},
						Direction: RelatedTransaction_FORWARD,
					},
				},
				Metadata: map[string]*anypb.Any{},
			},
			{
				TransactionIdentifier: &TransactionIdentifier{
					Hash: block.Transactions[1].TransactionIdentifier.Hash,
				},
				RelatedTransactions: []*RelatedTransaction{},
				Operations:          []*Operation{},
				Metadata:            map[string]*anypb.Any{},
			},
		},
		Metadata: map[string]*anypb.Any{
			"difficulty": difficultyToAny,
			"bits":       bitsToAny,
		},
	}

	return block, protoBlock
}

// randhash generates a random hash
func randHash() string {
	randBytes := make([]byte, 64)
	_, err := rand.Read(randBytes)
	if err != nil {
		log.Fatal("error generating random hash")
	}
	hasher := sha256.New()
	_, err = hasher.Write(randBytes)
	if err != nil {
		log.Fatal("error generating random hash")
	}
	return base64.URLEncoding.EncodeToString(hasher.Sum(nil))
}

func randInt(n int) int {
	res, err := rand.Int(rand.Reader, big.NewInt(int64(n)))
	if err != nil {
		log.Printf("ERROR: Unable to generate random number: %v\n", err)
		return -1
	}
	return int(res.Int64())
}
