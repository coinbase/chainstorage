package parser

import (
	"context"
	"testing"

	"go.uber.org/fx"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/coinbase/chainstorage/internal/utils/testapp"
	"github.com/coinbase/chainstorage/internal/utils/testutil"
	"github.com/coinbase/chainstorage/protos/coinbase/c3/common"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
	rosetta "github.com/coinbase/chainstorage/protos/coinbase/crypto/rosetta/types"
)

func TestParseRosettaRosettaBlock(t *testing.T) {
	require := testutil.Require(t)

	block := &api.Block{
		Blockchain: common.Blockchain_BLOCKCHAIN_DOGECOIN,
		Network:    common.Network_NETWORK_DOGECOIN_MAINNET,
		Metadata:   rosettaMetadata,
		Blobdata: &api.Block_Rosetta{
			Rosetta: &api.RosettaBlobdata{
				Header:            rosettaFixtureHeader,
				OtherTransactions: [][]byte{rosettaFixtureOtherTransactions},
			},
		},
	}

	difficultyToAny, err := anypb.New(structpb.NewNumberValue(3996375))
	require.NoError(err)

	bitsToAny, err := anypb.New(structpb.NewStringValue("1a0432b3"))
	require.NoError(err)

	expected := &api.RosettaBlock{
		Block: &rosetta.Block{
			BlockIdentifier: &rosetta.BlockIdentifier{
				Index: 3840970,
				Hash:  "1DD0C843E9c487acc21af4504024c7ef9bb56220aac81a035b36517f78a02b0d",
			},
			ParentBlockIdentifier: &rosetta.BlockIdentifier{
				Index: 3840969,
				Hash:  "515F4F5C9e54541ec2e39e5d270347d5c6c5f2575e38663791daa44b5e8c2507",
			},
			Timestamp: testutil.MustTimestamp("2021-08-04T18:51:21.05Z"),
			Transactions: []*rosetta.Transaction{
				{
					TransactionIdentifier: &rosetta.TransactionIdentifier{
						Hash: "1E02CA1261cc5599582018c2278ab44f80c3d8f6e8ae5daa2a53afa65e530f1f",
					},
					Operations: []*rosetta.Operation{
						{
							OperationIdentifier: &rosetta.OperationIdentifier{
								Index:        0,
								NetworkIndex: 0,
							},
							RelatedOperations: []*rosetta.OperationIdentifier{
								{
									Index:        2,
									NetworkIndex: 3,
								},
								{
									Index:        3,
									NetworkIndex: 4,
								},
							},
							Type:     "COINBASE",
							Status:   "SUCCESS",
							Metadata: map[string]*anypb.Any{},
						},
						{
							OperationIdentifier: &rosetta.OperationIdentifier{
								Index:        1,
								NetworkIndex: 0,
							},
							Type:   "OUTPUT",
							Status: "SUCCESS",
							Account: &rosetta.AccountIdentifier{
								Address:  "DMr3fEiVrPWFpoCWS958zNtqgnFb7QWn9D",
								Metadata: map[string]*anypb.Any{},
							},
							Amount: &rosetta.Amount{
								Value: "1000000000000",
								Currency: &rosetta.Currency{
									Symbol:   "DOGE",
									Decimals: 8,
									Metadata: map[string]*anypb.Any{},
								},
								Metadata: map[string]*anypb.Any{},
							},
							CoinChange: &rosetta.CoinChange{
								CoinIdentifier: &rosetta.CoinIdentifier{
									Identifier: "1e02ca1261cc5599582018c2278ab44f80c3d8f6e8ae5daa2a53afa65e530f1f",
								},
								CoinAction: rosetta.CoinChange_COIN_CREATED,
							},
							RelatedOperations: []*rosetta.OperationIdentifier{},
							Metadata:          map[string]*anypb.Any{},
						},
					},
					RelatedTransactions: []*rosetta.RelatedTransaction{},
					Metadata:            map[string]*anypb.Any{},
				},
				{
					TransactionIdentifier: &rosetta.TransactionIdentifier{
						Hash: "0x2f23fd8cca835af21f3ac375bac601f97ead75f2e79143bdf71fe2c4be043e8f",
					},
					Operations: []*rosetta.Operation{
						{
							OperationIdentifier: &rosetta.OperationIdentifier{
								Index:        3,
								NetworkIndex: 0,
							},
							Type:              "Transfer",
							Status:            "Reverted",
							RelatedOperations: []*rosetta.OperationIdentifier{},
							Metadata:          map[string]*anypb.Any{},
						},
					},
					RelatedTransactions: []*rosetta.RelatedTransaction{
						{
							NetworkIdentifier: &rosetta.NetworkIdentifier{
								Blockchain: "dogecoin",
								Network:    "mainnet",
								SubNetworkIdentifier: &rosetta.SubNetworkIdentifier{
									Network:  "mainnet",
									Metadata: map[string]*anypb.Any{},
								},
							},
							TransactionIdentifier: &rosetta.TransactionIdentifier{
								Hash: "515f4f5c9e54541ec2e39e5d270347d5c6c5f2575e38663791daa44b5e8c2507",
							},
							Direction: rosetta.RelatedTransaction_FORWARD,
						},
					},
					Metadata: map[string]*anypb.Any{},
				},
			},
			Metadata: map[string]*anypb.Any{
				"difficulty": difficultyToAny,
				"bits":       bitsToAny,
			},
		},
	}

	var parser Parser
	app := testapp.New(
		t,
		testapp.WithBlockchainNetwork(common.Blockchain_BLOCKCHAIN_DOGECOIN, common.Network_NETWORK_DOGECOIN_MAINNET),
		Module,
		fx.Populate(&parser),
	)
	defer app.Close()
	require.NotNil(parser)

	actual, err := parser.ParseRosettaBlock(context.Background(), block)
	require.NoError(err)
	require.NotNil(actual)
	require.Equal(expected, actual)
}

func TestParseRosettaRosettaBlock_Nil(t *testing.T) {
	require := testutil.Require(t)

	block := &api.Block{
		Blockchain: common.Blockchain_BLOCKCHAIN_DOGECOIN,
		Network:    common.Network_NETWORK_DOGECOIN_MAINNET,
		Metadata:   rosettaMetadata,
		Blobdata: &api.Block_Rosetta{
			Rosetta: &api.RosettaBlobdata{
				Header: []byte(`{}`),
			},
		},
	}

	var parser Parser
	app := testapp.New(
		t,
		testapp.WithBlockchainNetwork(common.Blockchain_BLOCKCHAIN_DOGECOIN, common.Network_NETWORK_DOGECOIN_MAINNET),
		Module,
		fx.Populate(&parser),
	)
	defer app.Close()
	require.NotNil(parser)

	actual, err := parser.ParseRosettaBlock(context.Background(), block)
	require.Nil(actual)
	require.NotNil(err)
	require.Contains(err.Error(), "not found")
}
