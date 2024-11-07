package ethereum

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"testing"

	geth "github.com/ethereum/go-ethereum/common"
	"go.uber.org/fx"
	"google.golang.org/protobuf/proto"

	"github.com/coinbase/chainstorage/internal/blockchain/parser/internal"
	"github.com/coinbase/chainstorage/internal/utils/fixtures"
	"github.com/coinbase/chainstorage/internal/utils/testapp"
	"github.com/coinbase/chainstorage/internal/utils/testutil"
	"github.com/coinbase/chainstorage/protos/coinbase/c3/common"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

func TestPolygonValidator_Success(t *testing.T) {
	require := testutil.Require(t)

	var parser internal.Parser
	app := testapp.New(
		t,
		Module,
		internal.Module,
		testapp.WithBlockchainNetwork(common.Blockchain_BLOCKCHAIN_POLYGON, common.Network_NETWORK_POLYGON_MAINNET),
		fx.Populate(&parser),
	)
	defer app.Close()
	require.NotNil(parser)

	// Generate the fixture with:
	// // go run ./cmd/admin block --blockchain polygon --network mainnet --env development --height 2000000 --out internal/utils/fixtures/parser/polygon/native_block_2000000.json

	// For this test, we cover multiple types of blocks:
	// 1. block 1000000: empty transactions/receipts.
	// 2. block 10000000: with state-sync transaction at the end.
	// 3. block 20000000: with state-sync transaction at the end.
	// 4. block 40000000: post EIP2930/EIP1559, new transaction types: DynamicFeeTx and AccessListTx.
	// 5. block 43200000: recent block.

	blocks := []int{1000000, 10000000, 20000000, 40000000, 43200000}
	for _, b := range blocks {
		var block api.NativeBlock
		path := fmt.Sprintf("parser/polygon/native_block_%d.json", b)

		err := fixtures.UnmarshalPB(path, &block)
		require.NoError(err)

		err = parser.ValidateBlock(context.Background(), &block)
		require.NoError(err)
	}
}

func TestPolygonValidator_Failures(t *testing.T) {
	require := testutil.Require(t)

	var parser internal.Parser
	app := testapp.New(
		t,
		Module,
		internal.Module,
		testapp.WithBlockchainNetwork(common.Blockchain_BLOCKCHAIN_POLYGON, common.Network_NETWORK_POLYGON_MAINNET),
		fx.Populate(&parser),
	)
	defer app.Close()
	require.NotNil(parser)

	ctx := context.Background()

	// 1. Modify the block headers in different ways and fail the block header verification.
	var block api.NativeBlock
	err := fixtures.UnmarshalPB("parser/polygon/native_block_1000000.json", &block)
	require.NoError(err)

	// Corrupt the transactions root (use a different root hash).
	corrupt_block := proto.Clone(&block).(*api.NativeBlock)
	corrupt_block.GetEthereum().GetHeader().TransactionsRoot = "0x14e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b367"
	err = parser.ValidateBlock(ctx, corrupt_block)
	require.True(errors.Is(err, ErrInvalidBlockHash))

	// Corrupt the block number (from 1000000 to 1000001)
	corrupt_block = proto.Clone(&block).(*api.NativeBlock)
	corrupt_block.GetEthereum().GetHeader().Number = 1000001
	err = parser.ValidateBlock(ctx, corrupt_block)
	require.True(errors.Is(err, ErrInvalidBlockHash))

	// Corrupt the miner (change the last char from 0 to 1)
	corrupt_block = proto.Clone(&block).(*api.NativeBlock)
	corrupt_block.GetEthereum().GetHeader().Miner = "0x0000000000000000000000000000000000000001"
	err = parser.ValidateBlock(ctx, corrupt_block)
	require.True(errors.Is(err, ErrInvalidBlockHash))

	// 2. Modify the transactions in different ways and fail the transactions verification.
	// Note that, this block has the state-sync transction at the end.
	err = fixtures.UnmarshalPB("parser/polygon/native_block_10000000.json", &block)
	require.NoError(err)

	// Corrupt the gasUsed (from 27998 to 28998).
	corrupt_block = proto.Clone(&block).(*api.NativeBlock)
	corrupt_block.GetEthereum().Transactions[0].Gas = 28998
	err = parser.ValidateBlock(ctx, corrupt_block)
	require.True(errors.Is(err, ErrInvalidTransactionsHash))

	// Corrupt the to (modify the first 3 chars).
	corrupt_block = proto.Clone(&block).(*api.NativeBlock)
	corrupt_block.GetEthereum().Transactions[0].To = "0xabcb23fd6bc0add59e62ac25578270cff1b9f619"
	err = parser.ValidateBlock(ctx, corrupt_block)
	require.True(errors.Is(err, ErrInvalidTransactionsHash))

	// Corrupt the last state-sync tx (modify the from address to non-nil address).
	corrupt_block = proto.Clone(&block).(*api.NativeBlock)
	corrupt_block.GetEthereum().Transactions[5].From = "0x1000000000000000000000000000000000000000"
	err = parser.ValidateBlock(ctx, corrupt_block)
	require.True(errors.Is(err, ErrInvalidTransactionsHash))

	// 3. Modify the receipts in different ways and fail the receipts verification.
	err = fixtures.UnmarshalPB("parser/polygon/native_block_43200000.json", &block)
	require.NoError(err)

	// Corrupt the cumulative gas used (from 95574 to 95575)
	corrupt_block = proto.Clone(&block).(*api.NativeBlock)
	corrupt_block.GetEthereum().Transactions[1].Receipt.CumulativeGasUsed = 95574
	err = parser.ValidateBlock(ctx, corrupt_block)
	require.True(errors.Is(err, ErrInvalidReceiptsHash))

	// Corrupt the status (from 1 to 0)
	corrupt_block = proto.Clone(&block).(*api.NativeBlock)
	corrupt_block.GetEthereum().Transactions[1].Receipt.OptionalStatus = &api.EthereumTransactionReceipt_Status{
		Status: 0,
	}
	err = parser.ValidateBlock(ctx, corrupt_block)
	require.True(errors.Is(err, ErrInvalidReceiptsHash))

	// Corrupt the logs (drop the last 4 bytes of logs[0].data)
	corrupt_block = proto.Clone(&block).(*api.NativeBlock)
	corrupt_block.GetEthereum().Transactions[1].Receipt.Logs[0].Data = "0x00000000000000000000000000000000000000000000000000000000002d"
	err = parser.ValidateBlock(ctx, corrupt_block)
	require.True(errors.Is(err, ErrInvalidReceiptsHash))
}

func TestPolygonValidateAccountState_Success(t *testing.T) {
	require := testutil.Require(t)

	var parser internal.Parser
	app := testapp.New(
		t,
		Module,
		internal.Module,
		testapp.WithBlockchainNetwork(common.Blockchain_BLOCKCHAIN_POLYGON, common.Network_NETWORK_POLYGON_MAINNET),
		fx.Populate(&parser),
	)
	defer app.Close()
	require.NotNil(parser)

	// Generate the fixture with:
	// curl -s https://long-small-butterfly.matic.quiknode.pro/XXX -H "Content-Type: application/json" -d '{ "jsonrpc": "2.0", "id": 1, "method": "eth_getProof", "params": ["0x8e5ca1872062bee63b8a46493f6de36d4870ff88", [], "0x103EE79"] }' | jq '.result' > internal/utils/fixtures/parser/polygon/account_proof_block_43200000.json

	// For this test, we cover multiple types of blocks:
	// 1. block 1000000: empty transactions/receipts.
	// 2. block 10000000: with state-sync transaction at the end.
	// 3. block 20000000: with state-sync transaction at the end.
	// 4. block 40000000: post EIP2930/EIP1559, new transaction types: DynamicFeeTx and AccessListTx.
	// 5. block 43200000: recent block.

	// For each test block, we pick an account in that block to test.
	blocks := []int{1000000, 10000000, 20000000, 40000000, 43200000}
	accounts := []string{
		"0x0375b2fc7140977c9c76D45421564e354ED42277",
		"0x03344de035f5923f87c54743e3915262064eee17",
		"0x6c161782321265d8f81ae9f4079e67507df18d30",
		"0x9a0b8cbae272600232240060f80c780a2cd9429d",
		"0xbb335dc5342311d4b6099e3ca4aaf9927086c833",
	}

	for i, b := range blocks {
		var block api.NativeBlock
		path := fmt.Sprintf("parser/polygon/native_block_%d.json", b)

		err := fixtures.UnmarshalPB(path, &block)
		require.NoError(err)

		path = fmt.Sprintf("parser/polygon/account_proof_block_%d.json", b)
		proofData := fixtures.MustReadFile(path)

		req := &api.ValidateAccountStateRequest{
			AccountReq: &api.InternalGetVerifiedAccountStateRequest{
				Account: accounts[i],
			},
			Block: &block,
			AccountProof: &api.GetAccountProofResponse{
				Response: &api.GetAccountProofResponse_Ethereum{
					Ethereum: &api.EthereumAccountStateProof{
						AccountProof: proofData,
					},
				},
			},
		}
		_, err = parser.ValidateAccountState(context.Background(), req)
		require.NoError(err)
	}
}

func TestPolygonValidateAccountState_Failure(t *testing.T) {
	require := testutil.Require(t)

	var parser internal.Parser
	app := testapp.New(
		t,
		Module,
		internal.Module,
		fx.Populate(&parser),
	)
	defer app.Close()
	require.NotNil(parser)

	ctx := context.Background()

	b := 43200000
	var block api.NativeBlock
	path := fmt.Sprintf("parser/polygon/native_block_%d.json", b)
	err := fixtures.UnmarshalPB(path, &block)
	require.NoError(err)

	path = fmt.Sprintf("parser/polygon/account_proof_block_%d.json", b)
	proofData := fixtures.MustReadFile(path)

	account := "0xbb335dc5342311d4b6099e3ca4aaf9927086c833"

	req := &api.ValidateAccountStateRequest{
		AccountReq: &api.InternalGetVerifiedAccountStateRequest{
			Account: account,
		},
		Block: &block,
		AccountProof: &api.GetAccountProofResponse{
			Response: &api.GetAccountProofResponse_Ethereum{
				Ethereum: &api.EthereumAccountStateProof{
					AccountProof: proofData,
				},
			},
		},
	}

	var accountResult AccountResult
	err = json.Unmarshal(proofData, &accountResult)
	require.NoError(err)

	// Corrupt the input address: change the first byte of address from b to c
	accountResult.Address = geth.HexToAddress("0xcb335dc5342311d4b6099e3ca4aaf9927086c833")
	newData, err := json.Marshal(accountResult)
	require.NoError(err)
	corruptReq := proto.Clone(req).(*api.ValidateAccountStateRequest)
	corruptReq.GetAccountProof().GetEthereum().AccountProof = newData
	_, err = parser.ValidateAccountState(ctx, corruptReq)
	require.Contains(err.Error(), "the input proofResult has different account address")

	// Corrupt the input state_root_hash: change the last 2 byte
	corruptStateRootHash := "0xc3f663562e2e542d1c04e8d202948b2683b571910e0eb0f4f559390f6e81d50u"
	corruptBlock := proto.Clone(&block).(*api.NativeBlock)
	corruptBlock.GetEthereum().Header.StateRoot = corruptStateRootHash
	corruptReq = proto.Clone(req).(*api.ValidateAccountStateRequest)
	corruptReq.Block = corruptBlock
	_, err = parser.ValidateAccountState(ctx, corruptReq)
	// When the state root hash is wrong, the verification process fails with the first node lookup.
	require.ErrorIs(err, ErrAccountVerifyProofFailure)

	// There are 8 nodes in the path, staring from the root.
	require.Equal(8, len(accountResult.AccountProof))

	// Delete the second node in the path
	err = json.Unmarshal(proofData, &accountResult)
	require.NoError(err)
	accountResult.AccountProof = append(accountResult.AccountProof[:1], accountResult.AccountProof[2:]...)
	newData, err = json.Marshal(accountResult)
	require.NoError(err)
	corruptReq = proto.Clone(req).(*api.ValidateAccountStateRequest)
	corruptReq.GetAccountProof().GetEthereum().AccountProof = newData
	_, err = parser.ValidateAccountState(ctx, corruptReq)
	require.ErrorIs(err, ErrAccountVerifyProofFailure)

	// Delete the last leaf node in the path
	err = json.Unmarshal(proofData, &accountResult)
	require.NoError(err)
	accountResult.AccountProof = accountResult.AccountProof[:len(accountResult.AccountProof)-1]
	newData, err = json.Marshal(accountResult)
	require.NoError(err)
	corruptReq = proto.Clone(req).(*api.ValidateAccountStateRequest)
	corruptReq.GetAccountProof().GetEthereum().AccountProof = newData
	_, err = parser.ValidateAccountState(ctx, corruptReq)
	require.ErrorIs(err, ErrAccountVerifyProofFailure)

	// Corrupt the input nonce
	err = json.Unmarshal(proofData, &accountResult)
	require.NoError(err)
	accountResult.Nonce = accountResult.Nonce + 1
	newData, err = json.Marshal(accountResult)
	require.NoError(err)
	corruptReq = proto.Clone(req).(*api.ValidateAccountStateRequest)
	corruptReq.GetAccountProof().GetEthereum().AccountProof = newData
	_, err = parser.ValidateAccountState(ctx, corruptReq)
	require.ErrorIs(err, ErrAccountNonceNotMatched)

	// Corrupt the input balance
	err = json.Unmarshal(proofData, &accountResult)
	require.NoError(err)
	accountResult.Balance.ToInt().Set(big.NewInt(123))
	newData, err = json.Marshal(accountResult)
	require.NoError(err)
	corruptReq = proto.Clone(req).(*api.ValidateAccountStateRequest)
	corruptReq.GetAccountProof().GetEthereum().AccountProof = newData
	_, err = parser.ValidateAccountState(ctx, corruptReq)
	require.ErrorIs(err, ErrAccountBalanceNotMatched)

	// Corrupt the input storage hash
	err = json.Unmarshal(proofData, &accountResult)
	require.NoError(err)
	accountResult.StorageHash = geth.HexToHash("abcd")
	newData, err = json.Marshal(accountResult)
	require.NoError(err)
	corruptReq = proto.Clone(req).(*api.ValidateAccountStateRequest)
	corruptReq.GetAccountProof().GetEthereum().AccountProof = newData
	_, err = parser.ValidateAccountState(ctx, corruptReq)
	require.ErrorIs(err, ErrAccountStorageHashNotMatched)

	// Corrupt the input code hash
	err = json.Unmarshal(proofData, &accountResult)
	require.NoError(err)
	accountResult.CodeHash = geth.HexToHash("efgh")
	newData, err = json.Marshal(accountResult)
	require.NoError(err)
	corruptReq = proto.Clone(req).(*api.ValidateAccountStateRequest)
	corruptReq.GetAccountProof().GetEthereum().AccountProof = newData
	_, err = parser.ValidateAccountState(ctx, corruptReq)
	require.ErrorIs(err, ErrAccountCodeHashNotMatched)
}
