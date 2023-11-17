package rosetta

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"testing"

	rt "github.com/coinbase/rosetta-sdk-go/types"

	"go.uber.org/fx"

	"github.com/coinbase/chainstorage/internal/blockchain/client/internal"
	"github.com/coinbase/chainstorage/internal/blockchain/parser"
	"github.com/coinbase/chainstorage/internal/blockchain/restapi"
	restapimocks "github.com/coinbase/chainstorage/internal/blockchain/restapi/mocks"
	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/dlq"
	"github.com/coinbase/chainstorage/internal/utils/fixtures"
	"github.com/coinbase/chainstorage/internal/utils/testapp"
	"github.com/coinbase/chainstorage/internal/utils/testutil"
	"github.com/coinbase/chainstorage/protos/coinbase/c3/common"
)

var (
	blockFixture = []byte(`
{
  "block": {
	"block_identifier": {
	  "index": 3840970,
	  "hash": "1dd0c843e9c487acc21af4504024c7ef9bb56220aac81a035b36517f78a02b0d"
	},
	"parent_block_identifier": {
	  "index": 3840969,
	  "hash": "515f4f5c9e54541ec2e39e5d270347d5c6c5f2575e38663791daa44b5e8c2507"
	},
	"timestamp": 1628103081000,
	"transactions": [
	  {
		"transaction_identifier": {
		  "hash": "1e02ca1261cc5599582018c2278ab44f80c3d8f6e8ae5daa2a53afa65e530f1f"
		},
		"operations": [
		  {
			"operation_identifier": {
			  "index": 0,
			  "network_index": 0
			},
			"type": "COINBASE",
			"status": "SUCCESS",
			"metadata": {
			  "coinbase": "03ca9b3a04610ae1a9",
			  "sequence": 4294967295
			}
		  },
		  {
			"operation_identifier": {
			  "index": 1,
			  "network_index": 0
			},
			"type": "OUTPUT",
			"status": "SUCCESS",
			"account": {
			  "address": "DMr3fEiVrPWFpoCWS958zNtqgnFb7QWn9D"
			},
			"amount": {
			  "value": "1000000000000",
			  "currency": {
				"symbol": "DOGE",
				"decimals": 8
			  }
			},
			"coin_change": {
			  "coin_identifier": {
				"identifier": "1e02ca1261cc5599582018c2278ab44f80c3d8f6e8ae5daa2a53afa65e530f1f:0"
			  },
			  "coin_action": "coin_created"
			},
			"metadata": {
			  "scriptPubKey": {
				"addresses": [
				  "DMr3fEiVrPWFpoCWS958zNtqgnFb7QWn9D"
				],
				"asm": "OP_DUP OP_HASH160 b740a9c3af9a4aa74d2974b6d79d63dc861eed34 OP_EQUALVERIFY OP_CHECKSIG",
				"hex": "76a914b740a9c3af9a4aa74d2974b6d79d63dc861eed3488ac",
				"reqSigs": 1,
				"type": "pubkeyhash"
			  }
			}
		  }
		],
		"metadata": {
		  "size": 94,
		  "version": 1,
		  "vsize": 94,
		  "weight": 376
		}
	  }
	],
	"metadata": {
	  "bits": "1a0432b3",
	  "difficulty": 3996375.623121295,
	  "mediantime": 1628102886,
	  "merkleroot": "1e02ca1261cc5599582018c2278ab44f80c3d8f6e8ae5daa2a53afa65e530f1f",
	  "size": 1009,
	  "version": 6422788,
	  "weight": 4036
	}
  }
}
	`)

	transactionFixture = []byte(`
{
  "transaction": {
    "transaction_identifier": {
      "hash": "1e02ca1261cc5599582018c2278ab44f80c3d8f6e8ae5daa2a53afa65e530f1f"
    },
    "operations": [
      {
        "operation_identifier": {
          "index": 0,
          "network_index": 0
        },
        "type": "COINBASE",
        "status": "SUCCESS",
        "metadata": {
          "coinbase": "03ca9b3a04610ae1a9",
          "sequence": 4294967295
        }
      },
      {
        "operation_identifier": {
          "index": 1,
          "network_index": 0
        },
        "type": "OUTPUT",
        "status": "SUCCESS",
        "account": {
          "address": "DMr3fEiVrPWFpoCWS958zNtqgnFb7QWn9D"
        },
        "amount": {
          "value": "1000000000000",
          "currency": {
            "symbol": "DOGE",
            "decimals": 8
          }
        },
        "coin_change": {
          "coin_identifier": {
            "identifier": "1e02ca1261cc5599582018c2278ab44f80c3d8f6e8ae5daa2a53afa65e530f1f:0"
          },
          "coin_action": "coin_created"
        },
        "metadata": {
          "scriptPubKey": {
            "addresses": [
              "DMr3fEiVrPWFpoCWS958zNtqgnFb7QWn9D"
            ],
            "asm": "OP_DUP OP_HASH160 b740a9c3af9a4aa74d2974b6d79d63dc861eed34 OP_EQUALVERIFY OP_CHECKSIG",
            "hex": "76a914b740a9c3af9a4aa74d2974b6d79d63dc861eed3488ac",
            "reqSigs": 1,
            "type": "pubkeyhash"
          }
        }
      }
    ],
    "metadata": {
      "size": 94,
      "version": 1,
      "vsize": 94,
      "weight": 376
    }
  }
}
`)
)

func TestGetRosettaBlock(t *testing.T) {
	const (
		tag uint32 = 1
	)

	require := testutil.Require(t)

	resp := rt.BlockResponse{}
	err := json.Unmarshal(blockFixture, &resp)
	require.NoError(err)

	txResp := rt.BlockTransactionResponse{}
	err = json.Unmarshal(transactionFixture, &txResp)
	require.NoError(err)

	cfg := &config.Config{}
	cfg.Chain.Blockchain = common.Blockchain_BLOCKCHAIN_DOGECOIN
	cfg.Chain.Network = common.Network_NETWORK_DOGECOIN_MAINNET

	rosettaClient := &rosettaClientImpl{
		config: cfg,
	}
	rawBlock, err := rosettaClient.getRawBlock(tag, &resp, []*rt.BlockTransactionResponse{&txResp})
	require.NoError(err)
	require.NotNil(rawBlock)
	require.Equal(common.Blockchain_BLOCKCHAIN_DOGECOIN, rawBlock.Blockchain)
	require.Equal(common.Network_NETWORK_DOGECOIN_MAINNET, rawBlock.Network)
	require.Equal(tag, rawBlock.Metadata.Tag)
	require.Equal(testutil.MustTimestamp("2021-08-04T18:51:21Z"), rawBlock.Metadata.Timestamp)

	var rosettaParser parser.Parser
	app := testapp.New(
		t,
		testapp.WithBlockchainNetwork(common.Blockchain_BLOCKCHAIN_DOGECOIN, common.Network_NETWORK_DOGECOIN_MAINNET),
		Module,
		parser.Module,
		fx.Populate(&rosettaParser),
	)
	defer app.Close()

	nativeBlock, err := rosettaParser.ParseNativeBlock(context.Background(), rawBlock)
	require.NoError(err)
	require.NotNil(nativeBlock)
	require.Equal(common.Blockchain_BLOCKCHAIN_DOGECOIN, nativeBlock.Blockchain)
	require.Equal(common.Network_NETWORK_DOGECOIN_MAINNET, nativeBlock.Network)
	require.NotNil(nativeBlock.Block)
	require.Equal(testutil.MustTimestamp("2021-08-04T18:51:21Z"), nativeBlock.Timestamp)
}

func testRestModule(client *restapimocks.MockClient) fx.Option {
	return fx.Options(
		internal.Module,
		fx.Provide(fx.Annotated{
			Name:   "master",
			Target: func() restapi.Client { return client },
		}),
		fx.Provide(fx.Annotated{
			Name:   "slave",
			Target: func() restapi.Client { return client },
		}),
		fx.Provide(fx.Annotated{
			Name:   "validator",
			Target: func() restapi.Client { return client },
		}),
		fx.Provide(fx.Annotated{
			Name:   "consensus",
			Target: func() restapi.Client { return client },
		}),
		fx.Provide(dlq.NewNop),
		fx.Provide(parser.NewNop),
	)
}

func startServer(fixturePath string) string {
	blockResponse := fixtures.MustReadFile(fixturePath)
	http.Handle("/block", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Add("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write(blockResponse)
	}))

	listener := newLocalListener()
	go func() {
		panic(http.Serve(listener, nil))
	}()

	return strconv.Itoa(listener.Addr().(*net.TCPAddr).Port)
}

func newLocalListener() net.Listener {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		if l, err = net.Listen("tcp6", "[::1]:0"); err != nil {
			panic(fmt.Sprintf("httptest: failed to listen on a port: %v", err))
		}
	}
	return l
}
