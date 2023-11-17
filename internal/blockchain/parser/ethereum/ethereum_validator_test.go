package ethereum

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"testing"

	geth "github.com/ethereum/go-ethereum/common"
	"go.uber.org/fx"
	"golang.org/x/xerrors"
	"google.golang.org/protobuf/proto"

	"github.com/coinbase/chainstorage/internal/blockchain/parser/internal"
	"github.com/coinbase/chainstorage/internal/utils/fixtures"
	"github.com/coinbase/chainstorage/internal/utils/testapp"
	"github.com/coinbase/chainstorage/internal/utils/testutil"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

func TestEthereumValidator_Success(t *testing.T) {
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

	// Generate the fixture with:
	// // go run ./cmd/admin block --blockchain ethereum --network mainnet --env local --height 2000000 --out internal/utils/fixtures/parser/ethereum/native_block_2000000.json

	// For this test, we cover multiple types of blocks:
	// 1. block 1000: empty transactions/receipts.
	// 2. block 2000000: pre EIP-155, no chainid in transaction.
	// 3. block 4000000: pre byzantine, the receipt has PostState.
	// 4. block 8000000: post byzantine, the receipt has Status, not PostState.
	// 5. block 14000000: post EIP2930/EIP1559, new transaction types: DynamicFeeTx and AccessListTx.
	// 6. block 17000000: post merge, pre shanghai.
	// 7. block 17034873: post shanghai, no withdrawals in the block, but has withdrawalsRoot.
	// 8. block 17300000: post shanghai, has withdrawals in the block.

	blocks := []int{1000, 2000000, 4000000, 8000000, 14000000, 17000000, 17034873, 17300000}
	for _, b := range blocks {
		var block api.NativeBlock
		path := fmt.Sprintf("parser/ethereum/native_block_%d.json", b)

		err := fixtures.UnmarshalPB(path, &block)
		require.NoError(err)

		err = parser.ValidateBlock(ctx, &block)
		require.NoError(err)
	}
}

func TestEthereumValidator_Failures(t *testing.T) {
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

	// 1. Modify the block headers in different ways and fail the block header verification.
	var block api.NativeBlock
	err := fixtures.UnmarshalPB("parser/ethereum/native_block_17000000.json", &block)
	require.NoError(err)

	// Corrupt the transactions root (use a different root hash).
	corrupt_block := proto.Clone(&block).(*api.NativeBlock)
	corrupt_block.GetEthereum().GetHeader().TransactionsRoot = "0x8fcfe4d75266508496020675b9c3acfdf0074bf2d177c6366b40f669306310db"
	err = parser.ValidateBlock(ctx, corrupt_block)
	require.True(xerrors.Is(err, ErrInvalidBlockHash))

	// Corrupt the block number (from 17000000 to 16000000)
	corrupt_block = proto.Clone(&block).(*api.NativeBlock)
	corrupt_block.GetEthereum().GetHeader().Number = 16000000
	err = parser.ValidateBlock(ctx, corrupt_block)
	require.True(xerrors.Is(err, ErrInvalidBlockHash))

	// Corrupt the miner (drop the last 3 chars)
	corrupt_block = proto.Clone(&block).(*api.NativeBlock)
	corrupt_block.GetEthereum().GetHeader().Miner = "0x690b9a9e9aa1c9db991c7721a92d351db4fac"
	err = parser.ValidateBlock(ctx, corrupt_block)
	require.True(xerrors.Is(err, ErrInvalidBlockHash))

	// 2. Modify the transactions in different ways and fail the transactions verification.
	err = fixtures.UnmarshalPB("parser/ethereum/native_block_14000000.json", &block)
	require.NoError(err)

	// Corrupt the chain id (from 1 to 100).
	corrupt_block = proto.Clone(&block).(*api.NativeBlock)
	corrupt_block.GetEthereum().Transactions[0].OptionalChainId = &api.EthereumTransaction_ChainId{
		ChainId: 100,
	}
	err = parser.ValidateBlock(ctx, corrupt_block)
	require.True(xerrors.Is(err, ErrInvalidTransactionsHash))

	// Corrupt the gasUsed (from 1395940 to 1395941).
	corrupt_block = proto.Clone(&block).(*api.NativeBlock)
	corrupt_block.GetEthereum().Transactions[0].Gas = 1395941
	err = parser.ValidateBlock(ctx, corrupt_block)
	require.True(xerrors.Is(err, ErrInvalidTransactionsHash))

	// Corrupt the nounce (from 2760 to 3760).
	corrupt_block = proto.Clone(&block).(*api.NativeBlock)
	corrupt_block.GetEthereum().Transactions[0].Nonce = 3760
	err = parser.ValidateBlock(ctx, corrupt_block)
	require.True(xerrors.Is(err, ErrInvalidTransactionsHash))

	// 3. Modify the receipts in different ways and fail the receipts verification.
	err = fixtures.UnmarshalPB("parser/ethereum/native_block_8000000.json", &block)
	require.NoError(err)

	// Corrupt the cumulative gas used (from 73385 to 73384)
	corrupt_block = proto.Clone(&block).(*api.NativeBlock)
	corrupt_block.GetEthereum().Transactions[1].Receipt.CumulativeGasUsed = 73384
	err = parser.ValidateBlock(ctx, corrupt_block)
	require.True(xerrors.Is(err, ErrInvalidReceiptsHash))

	// Corrupt the status (from 1 to 0)
	corrupt_block = proto.Clone(&block).(*api.NativeBlock)
	corrupt_block.GetEthereum().Transactions[1].Receipt.OptionalStatus = &api.EthereumTransactionReceipt_Status{
		Status: 0,
	}
	err = parser.ValidateBlock(ctx, corrupt_block)
	require.True(xerrors.Is(err, ErrInvalidReceiptsHash))

	// Corrupt the logs (drop the last 4 bytes of logs[0].data)
	corrupt_block = proto.Clone(&block).(*api.NativeBlock)
	corrupt_block.GetEthereum().Transactions[1].Receipt.Logs[0].Data = "0x000000000000000000000000000000000000000000000000000003c89f3e"
	err = parser.ValidateBlock(ctx, corrupt_block)
	require.True(xerrors.Is(err, ErrInvalidReceiptsHash))

	// 4. Modify the withdrawals in different ways and fail the withdrawals verification.
	err = fixtures.UnmarshalPB("parser/ethereum/native_block_17300000.json", &block)
	require.NoError(err)

	// Corrupt the index of the first Withdrawals (from 4241882 to 4241881)
	corrupt_block = proto.Clone(&block).(*api.NativeBlock)
	corrupt_block.GetEthereum().GetHeader().Withdrawals[0].Index = 4241881
	err = parser.ValidateBlock(ctx, corrupt_block)
	require.True(xerrors.Is(err, ErrInvalidWithdrawalsHash))

	// Corrupt the validatorIndex of the second Withdrawals (from 551869 to 551870)
	corrupt_block = proto.Clone(&block).(*api.NativeBlock)
	corrupt_block.GetEthereum().GetHeader().Withdrawals[1].ValidatorIndex = 551870
	err = parser.ValidateBlock(ctx, corrupt_block)
	require.True(xerrors.Is(err, ErrInvalidWithdrawalsHash))

	// Corrupt the address of the third Withdrawals (add "abc" at the end)
	corrupt_block = proto.Clone(&block).(*api.NativeBlock)
	corrupt_block.GetEthereum().GetHeader().Withdrawals[2].Address = "0xb9d7934878b5fb9610b3fe8a5e441e8fad7e293fabc"
	err = parser.ValidateBlock(ctx, corrupt_block)
	require.True(xerrors.Is(err, ErrInvalidWithdrawalsHash))
}

func TestValidateAccountState_Success(t *testing.T) {
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

	// Generate the fixture with:
	// curl -s https://nodes2.nodeservice.us-east-1.development.cbhq.net:11800 -H "Content-Type: application/json" -d '{ "jsonrpc": "2.0", "id": 1, "method": "eth_getProof", "params": ["0x8e5ca1872062bee63b8a46493f6de36d4870ff88", [], "0x103EE79"] }' | jq '.result' > internal/utils/fixtures/parser/ethereum/account_proof_block_17034873.json

	// For this test, we cover multiple types of blocks:
	// 1. block 1000: empty transactions/receipts.
	// 2. block 2000000: pre EIP-155, no chainid in transaction.
	// 3. block 4000000: pre byzantine, the receipt has PostState.
	// 4. block 8000000: post byzantine, the receipt has Status, not PostState.
	// 5. block 14000000: post EIP2930/EIP1559, new transaction types: DynamicFeeTx and AccessListTx.
	// 6. block 17000000: post merge, pre shanghai.
	// 7. block 17034873: post shanghai, no withdrawals in the block, but has withdrawalsRoot.
	// 8. block 17300000: post shanghai, has withdrawals in the block.

	// For each test block, we pick an account in that block to test.
	blocks := []int{1000, 2000000, 4000000, 8000000, 14000000, 17000000, 17034873, 17300000}
	accounts := []string{
		"0xbb7B8287f3F0a933474a79eAe42CBCa977791171",
		"0x32be343b94f860124dc4fee278fdcbd38c102d88",
		"0x7ed1e469fcb3ee19c0366d829e291451be638e59",
		"0xbcd44f9795cddd1358dcc3bef160772fcd607ca4",
		"0xde1c59bc25d806ad9ddcbe246c4b5e5505645718",
		"0x8c8d7c46219d9205f056f28fee5950ad564d7465",
		"0x8e5ca1872062bee63b8a46493f6de36d4870ff88",
		"0x10c688962a007AD6Dfa7cd1FB32A38b83B73050a",
	}

	for i, b := range blocks {
		var block api.NativeBlock
		path := fmt.Sprintf("parser/ethereum/native_block_%d.json", b)

		err := fixtures.UnmarshalPB(path, &block)
		require.NoError(err)

		path = fmt.Sprintf("parser/ethereum/account_proof_block_%d.json", b)
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

func TestValidateAccountState_Failure(t *testing.T) {
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

	b := 17000000
	var block api.NativeBlock
	path := fmt.Sprintf("parser/ethereum/native_block_%d.json", b)
	err := fixtures.UnmarshalPB(path, &block)
	require.NoError(err)

	path = fmt.Sprintf("parser/ethereum/account_proof_block_%d.json", b)
	proofData := fixtures.MustReadFile(path)

	account := "0x8c8d7c46219d9205f056f28fee5950ad564d7465"

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

	// Corrupt the input address: change the first byte of address from 8 to 7
	accountResult.Address = geth.HexToAddress("0x7c8d7c46219d9205f056f28fee5950ad564d7465")
	newData, err := json.Marshal(accountResult)
	require.NoError(err)
	corruptReq := proto.Clone(req).(*api.ValidateAccountStateRequest)
	corruptReq.GetAccountProof().GetEthereum().AccountProof = newData
	_, err = parser.ValidateAccountState(ctx, corruptReq)
	require.Contains(err.Error(), "the input proofResult has different account address")

	// Corrupt the input state_root_hash: change the last 2 byte
	corruptStateRootHash := "0x72eea852168e4156811869d6e40789083891288161d5a110259cf4fc922b1a22"
	corruptBlock := proto.Clone(&block).(*api.NativeBlock)
	corruptBlock.GetEthereum().Header.StateRoot = corruptStateRootHash
	corruptReq = proto.Clone(req).(*api.ValidateAccountStateRequest)
	corruptReq.Block = corruptBlock
	_, err = parser.ValidateAccountState(ctx, corruptReq)
	// When the state root hash is wrong, the verification process fails with the first node lookup.
	require.ErrorIs(err, ErrAccountVerifyProofFailure)

	// There are 9 nodes in the path, staring from the root.
	require.Equal(9, len(accountResult.AccountProof))

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

	// Lastly, we use a non-exist account which has a very long common prefix as an existing account. This will test the failure of
	// proof of inclusion.
	proofData = fixtures.MustReadFile("parser/ethereum/account_proof_not_included_block_17000000.json")
	// Note that this account address has a long common prefix as the above account.
	account = "0x8c8d7c46219d9205f056f28fee5950ad564dffff"
	req = &api.ValidateAccountStateRequest{
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

	_, err = parser.ValidateAccountState(ctx, req)
	require.ErrorIs(err, ErrAccountVerifyProofFailure)
}

func TestValidateAccountState_ERC20_Success(t *testing.T) {
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

	// Generate the fixture with:
	// curl -s https://nodes2.nodeservice.us-east-1.development.cbhq.net:11800 -H "Content-Type: application/json" -d '{ "jsonrpc": "2.0", "id": 1, "method": "eth_getProof", "params": ["0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48", ["0x4065d4ec50c2a4fc400b75cca2760227b773c3e315ed2f2a7784cd505065cb07"], "0x107FA20"] }' | jq '.result' > internal/utils/fixtures/parser/ethereum/account_proof_block_17034873.json

	// For each test block, we pick an account in that block to test.
	blocks := []int{17300000}
	accounts := []string{
		"0x467d543e5e4e41aeddf3b6d1997350dd9820a173",
	}

	for i, b := range blocks {
		var block api.NativeBlock
		path := fmt.Sprintf("parser/ethereum/native_block_%d.json", b)

		err := fixtures.UnmarshalPB(path, &block)
		require.NoError(err)

		path = fmt.Sprintf("parser/ethereum/account_proof_erc20_block_%d.json", b)
		proofData := fixtures.MustReadFile(path)

		req := &api.ValidateAccountStateRequest{
			AccountReq: &api.InternalGetVerifiedAccountStateRequest{
				Account: accounts[i],
				ExtraInput: &api.InternalGetVerifiedAccountStateRequest_Ethereum{
					Ethereum: &api.EthereumExtraInput{
						// USDC contract address
						Erc20Contract: "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
					},
				},
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

func TestValidateAccountState_ERC20_Failure(t *testing.T) {
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

	b := 17300000
	var block api.NativeBlock
	path := fmt.Sprintf("parser/ethereum/native_block_%d.json", b)
	err := fixtures.UnmarshalPB(path, &block)
	require.NoError(err)

	path = fmt.Sprintf("parser/ethereum/account_proof_erc20_block_%d.json", b)
	proofData := fixtures.MustReadFile(path)

	account := "0x467d543e5e4e41aeddf3b6d1997350dd9820a173"

	req := &api.ValidateAccountStateRequest{
		AccountReq: &api.InternalGetVerifiedAccountStateRequest{
			Account: account,
			ExtraInput: &api.InternalGetVerifiedAccountStateRequest_Ethereum{
				Ethereum: &api.EthereumExtraInput{
					// USDC contract address
					Erc20Contract: "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
				},
			},
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

	// For this test, we focus on testing the storage proof part, not the contract proof part.

	// Corrupt the input contract address: change the first byte of address from a to b
	accountResult.Address = geth.HexToAddress("0xb0b86991c6218b36c1d19d4a2e9eb0ce3606eb48")
	newData, err := json.Marshal(accountResult)
	require.NoError(err)
	corruptReq := proto.Clone(req).(*api.ValidateAccountStateRequest)
	corruptReq.GetAccountProof().GetEthereum().AccountProof = newData
	_, err = parser.ValidateAccountState(ctx, corruptReq)
	require.Contains(err.Error(), "the input proofResult has different account address")

	// Corrupt the storage key: change the last byte
	err = json.Unmarshal(proofData, &accountResult)
	require.NoError(err)
	accountResult.StorageProof[0].Key = "0x4065d4ec50c2a4fc400b75cca2760227b773c3e315ed2f2a7784cd505065cb08"
	newData, err = json.Marshal(accountResult)
	require.NoError(err)
	corruptReq = proto.Clone(req).(*api.ValidateAccountStateRequest)
	corruptReq.GetAccountProof().GetEthereum().AccountProof = newData
	_, err = parser.ValidateAccountState(ctx, corruptReq)
	// When the state key is wrong, the verification process fails with the first node lookup.
	require.ErrorIs(err, ErrAccountVerifyProofFailure)

	// Delete the second node in the storage proof path
	err = json.Unmarshal(proofData, &accountResult)
	require.NoError(err)
	accountResult.StorageProof[0].Proof = append(accountResult.StorageProof[0].Proof[:1], accountResult.StorageProof[0].Proof[2:]...)
	newData, err = json.Marshal(accountResult)
	require.NoError(err)
	corruptReq = proto.Clone(req).(*api.ValidateAccountStateRequest)
	corruptReq.GetAccountProof().GetEthereum().AccountProof = newData
	_, err = parser.ValidateAccountState(ctx, corruptReq)
	require.ErrorIs(err, ErrAccountVerifyProofFailure)

	// Delete the last leaf node in the storage proof path
	err = json.Unmarshal(proofData, &accountResult)
	require.NoError(err)
	accountResult.StorageProof[0].Proof = accountResult.StorageProof[0].Proof[:len(accountResult.StorageProof[0].Proof)-1]
	newData, err = json.Marshal(accountResult)
	require.NoError(err)
	corruptReq = proto.Clone(req).(*api.ValidateAccountStateRequest)
	corruptReq.GetAccountProof().GetEthereum().AccountProof = newData
	_, err = parser.ValidateAccountState(ctx, corruptReq)
	require.ErrorIs(err, ErrAccountVerifyProofFailure)

	// Corrupt the token balance
	err = json.Unmarshal(proofData, &accountResult)
	require.NoError(err)
	accountResult.StorageProof[0].Value.ToInt().Set(big.NewInt(123))
	newData, err = json.Marshal(accountResult)
	require.NoError(err)
	corruptReq = proto.Clone(req).(*api.ValidateAccountStateRequest)
	corruptReq.GetAccountProof().GetEthereum().AccountProof = newData
	_, err = parser.ValidateAccountState(ctx, corruptReq)
	require.ErrorIs(err, ErrAccountBalanceNotMatched)
}
