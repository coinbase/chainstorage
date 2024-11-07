package ethereum

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"

	geth "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/trie"
	"go.uber.org/zap"

	"github.com/coinbase/chainstorage/internal/blockchain/parser/internal"
	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/utils/log"
	"github.com/coinbase/chainstorage/internal/utils/pointer"
	"github.com/coinbase/chainstorage/protos/coinbase/c3/common"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

type (
	ethereumValidator struct {
		config *config.Config
		logger *zap.Logger
	}

	// Result structs for eth_getProof
	AccountResult struct {
		Address      geth.Address    `json:"address"`
		AccountProof []string        `json:"accountProof"`
		Balance      *hexutil.Big    `json:"balance"`
		CodeHash     geth.Hash       `json:"codeHash"`
		Nonce        hexutil.Uint64  `json:"nonce"`
		StorageHash  geth.Hash       `json:"storageHash"`
		StorageProof []StorageResult `json:"storageProof"`
	}

	StorageResult struct {
		Key   string       `json:"key"`
		Value *hexutil.Big `json:"value"`
		Proof []string     `json:"proof"`
	}
)

var (
	_ internal.TrustlessValidator = (*ethereumValidator)(nil)

	ErrInvalidBlockHash             = errors.New("invalid block hash")
	ErrInvalidWithdrawalsHash       = errors.New("invalid withdrawals hash")
	ErrInvalidTransactionsHash      = errors.New("invalid transactions hash")
	ErrInvalidReceiptsHash          = errors.New("invalid receipts hash")
	ErrAccountVerifyProofFailure    = errors.New("account verification fails")
	ErrAccountNonceNotMatched       = errors.New("mismatched account nonce")
	ErrAccountBalanceNotMatched     = errors.New("mismatched account balance")
	ErrAccountStorageHashNotMatched = errors.New("mismatched account storage hash")
	ErrAccountCodeHashNotMatched    = errors.New("mismatched account code hash")
)

func NewEthereumValidator(params internal.ParserParams) internal.TrustlessValidator {
	return &ethereumValidator{
		config: params.Config,
		logger: log.WithPackage(params.Logger),
	}
}

// This is the core function to verify a ethereum block with cryptographic algorithm.
// It performs verification for three main structures in a block
// 1. block header
// 2. the transaction trie
// 3. the receipt trie
//
// For 1, we recompute the block header hash and compare that with the block header hash in the block.
// For 2, we reconstruct the transaction trie with the transactions in the block, calculate the
// trie root hash, and compare it with the transaction trie root hash in the block.
// Similar for receipt trie.
//
// All these main structures are here: https://github.com/ethereum/go-ethereum/blob/master/core/types/

func (v *ethereumValidator) ValidateBlock(ctx context.Context, block *api.NativeBlock) error {
	if block.Skipped {
		// By definition skipped blocks do not need to be validated.
		return nil
	}

	ethereumBlock := block.GetEthereum()
	if ethereumBlock == nil {
		return errors.New("not an ethereum block")
	}

	// Verify the block header.
	err := v.validateBlockHeader(ctx, ethereumBlock.Header)
	if err != nil {
		return fmt.Errorf("failed to validate block header: %w", err)
	}

	// Verify the Withdrawals in the block.
	err = v.validateWithdrawals(ctx, ethereumBlock.Header.Withdrawals, ethereumBlock.Header.WithdrawalsRoot)
	if err != nil {
		return fmt.Errorf("failed to validate withdrawals: %w", err)
	}

	// Verify the transactions in the block.
	err = v.validateTransactions(ctx, ethereumBlock.Transactions, ethereumBlock.Header.TransactionsRoot)
	if err != nil {
		return fmt.Errorf("failed to validate transactions: %w", err)
	}

	// Verify the receipts in the block.
	err = v.validateReceipts(ctx, ethereumBlock.Transactions, ethereumBlock.Header.ReceiptsRoot)
	if err != nil {
		return fmt.Errorf("failed to validate receipts: %w", err)
	}

	return nil
}

func (v *ethereumValidator) validateBlockHeader(ctx context.Context, header *api.EthereumHeader) error {
	if header == nil {
		return errors.New("block header is nil")
	}

	var nonce types.BlockNonce
	if err := nonce.UnmarshalText([]byte(header.Nonce)); err != nil {
		return fmt.Errorf("failed to decode nonce %v: %w", header.Nonce, err)
	}

	protocolHeader := types.Header{
		ParentHash:  geth.HexToHash(header.ParentHash),
		UncleHash:   geth.HexToHash(header.Sha3Uncles),
		Coinbase:    geth.HexToAddress(header.Miner),
		Root:        geth.HexToHash(header.StateRoot),
		TxHash:      geth.HexToHash(header.TransactionsRoot),
		ReceiptHash: geth.HexToHash(header.ReceiptsRoot),
		Bloom:       types.BytesToBloom(geth.FromHex(header.LogsBloom)),
		Difficulty:  big.NewInt(int64(header.Difficulty)),
		Number:      big.NewInt(int64(header.Number)),
		GasLimit:    header.GasLimit,
		GasUsed:     header.GasUsed,
		Time:        uint64(header.Timestamp.Seconds),
		Extra:       geth.FromHex(header.ExtraData),
		MixDigest:   geth.HexToHash(header.MixHash),
		Nonce:       nonce,
	}

	if header.GetOptionalBaseFeePerGas() != nil {
		// BaseFee was added by EIP-1559 and is ignored in legacy headers.
		protocolHeader.BaseFee = big.NewInt(int64(header.GetBaseFeePerGas()))
	}

	// EIP-4895: include withdrawals in the block header.
	if header.WithdrawalsRoot != "" {
		hash := geth.HexToHash(header.WithdrawalsRoot)
		protocolHeader.WithdrawalsHash = &hash
	}

	// Note that Hash returns the block hash of the header, which is simply the keccak256 hash of its RLP encoding.
	// We expect that the block hash recomputed following the protocol should match the one from the payload itself.
	expectedHash := protocolHeader.Hash()
	actualHash := geth.HexToHash(header.Hash)
	if expectedHash != actualHash {
		return fmt.Errorf("unexpected block hash (expected=%v, actual=%v): %w", expectedHash, actualHash, ErrInvalidBlockHash)
	}

	return nil
}

// Verify the withdrawals in the block with the withdrawals trie root hash.
func (v *ethereumValidator) validateWithdrawals(ctx context.Context, withdrawals []*api.EthereumWithdrawal, withdrawalsRoot string) error {
	if withdrawalsRoot != "" {
		// Note that, when len(withdrawals) is 0, the expected withdrawalsRoot will be the root hash of an empty trie.
		// Convert the native withdrawals to geth withdrawals.
		gethWithdrawals := make(types.Withdrawals, len(withdrawals))
		for i, w := range withdrawals {
			gethWithdrawals[i] = &types.Withdrawal{
				Index:     w.GetIndex(),
				Validator: w.GetValidatorIndex(),
				Address:   geth.HexToAddress(w.GetAddress()),
				Amount:    w.GetAmount(),
			}
		}

		expectedHash := geth.HexToHash(withdrawalsRoot)

		// This is how geth calculates the withdrawals trie hash. We just leverage this function of geth to recompute it.
		if actualHash := types.DeriveSha(gethWithdrawals, trie.NewStackTrie(nil)); actualHash != expectedHash {
			return fmt.Errorf("Withdrawals root hash mismatch (expected=%x, actual=%x): %w", expectedHash, actualHash, ErrInvalidWithdrawalsHash)
		}
	} else if len(withdrawals) != 0 {
		return fmt.Errorf("unexpected withdrawals in block body")
	}

	return nil
}

// Verify all the transactions in the block with the transaction trie root hash.
func (v *ethereumValidator) validateTransactions(ctx context.Context, transactions []*api.EthereumTransaction, transactionsRoot string) error {
	numTxs := len(transactions)

	v.logger.Debug("validateTransactions", zap.Int("numTxs", numTxs))
	switch v.config.Chain.Blockchain {
	case common.Blockchain_BLOCKCHAIN_POLYGON:
		// For Polygon, it is possible that there is a state-sync transaction at the end of transaction array.
		// It is an internal transaction used to read data from Ethereum in Polygon. It is an internal transaction, and
		// it is not used to calculate the transaction trie root hash. Once we identify such transaction, we need to
		// exclude it from the transaction and receipt verification.
		if hasStateSyncTx(transactions) {
			numTxs = numTxs - 1
		}
	}

	// Convert the native transactions to geth transactions.
	gethTxs := make(types.Transactions, numTxs)
	var err error
	for i := 0; i < numTxs; i++ {
		gethTxs[i], err = v.toGethTransaction(transactions[i])
		if err != nil {
			return fmt.Errorf("failed to convert to geth transaction: %w)", err)
		}
	}

	expectedHash := geth.HexToHash(transactionsRoot)

	// This is how geth calculates the transaction trie hash. We just leverage this function of geth to recompute it.
	if actualHash := types.DeriveSha(gethTxs, trie.NewStackTrie(nil)); actualHash != expectedHash {
		return fmt.Errorf("transaction root hash mismatch (expected=%x, actual=%x): %w", expectedHash, actualHash, ErrInvalidTransactionsHash)
	}

	return nil
}

func hasStateSyncTx(transactions []*api.EthereumTransaction) bool {
	num := len(transactions)
	if num == 0 {
		return false
	}

	// This is an state-sync transaction in Polygon. It only appears as the last transaction.
	return transactions[num-1].From == ethNullAddress && transactions[num-1].To == ethNullAddress
}

// Convert one EthereumTransaction to geth transaction.
func (v *ethereumValidator) toGethTransaction(transaction *api.EthereumTransaction) (*types.Transaction, error) {
	if transaction == nil {
		return nil, errors.New("input transaction is nil")
	}

	// When EthereumTransaction.To is "", return a null.
	to := convertTo(transaction.GetTo())

	value, ok := new(big.Int).SetString(transaction.GetValue(), 10)
	if !ok {
		return nil, fmt.Errorf("failed to convert value %s to big.Int", transaction.GetValue())
	}

	// Handle signatures: V, R, S.
	sv, ok := new(big.Int).SetString(transaction.GetV(), 0)
	if !ok {
		return nil, fmt.Errorf("failed to convert V %s to big.Int", transaction.GetV())
	}
	sr, ok := new(big.Int).SetString(transaction.GetR(), 0)
	if !ok {
		return nil, fmt.Errorf("failed to convert R %s to big.Int", transaction.GetR())
	}
	ss, ok := new(big.Int).SetString(transaction.GetS(), 0)
	if !ok {
		return nil, fmt.Errorf("failed to convert S %s to big.Int", transaction.GetS())
	}

	// Convert the input field. Note that, we need to remove the "0x" prefix.
	input, err := hexutil.Decode(transaction.GetInput())
	if err != nil {
		return nil, fmt.Errorf("failed to convert input %s to []byte, %w", transaction.GetInput(), err)
	}

	// ChainId is used in transaction types: DynamicFeeTx and AccessListTx.
	var chainId *big.Int
	if transaction.GetOptionalChainId() != nil {
		chainId = new(big.Int).SetUint64(transaction.GetChainId())
	}

	var mint *big.Int
	if m, ok := new(big.Int).SetString(transaction.GetMint(), 0); ok {
		mint = m
	}

	// There are three tranaction types in geth. Convert the native transaction to one of those.
	// The logic here is the same to geth.
	// https://github.com/ethereum/go-ethereum/blob/8013a494fe3f0812aa1661aba43898d22a6fc061/internal/ethapi/transaction_args.go#L284
	var data types.TxData
	switch {
	case transaction.GetOptionalMaxFeePerGas() != nil:
		v.logger.Debug(
			"toGethTransaction: DynamicFeeTx",
			zap.String("hash", transaction.GetHash()),
		)
		al := types.AccessList{}
		if transaction.GetTransactionAccessList() != nil {
			al = toGethAccessList(transaction.GetTransactionAccessList())
		}

		data = &types.DynamicFeeTx{
			To:         to,
			ChainID:    chainId,
			Nonce:      transaction.GetNonce(),
			Gas:        transaction.GetGas(),
			GasFeeCap:  new(big.Int).SetUint64(transaction.GetMaxFeePerGas()),
			GasTipCap:  new(big.Int).SetUint64(transaction.GetMaxPriorityFeePerGas()),
			Value:      value,
			Data:       input,
			AccessList: al,
			V:          sv,
			R:          sr,
			S:          ss,
		}

	case transaction.GetOptionalTransactionAccessList() != nil:
		v.logger.Debug(
			"toGethTransaction: AccessListTx",
			zap.String("hash", transaction.GetHash()),
		)
		data = &types.AccessListTx{
			To:         to,
			ChainID:    chainId,
			Nonce:      transaction.GetNonce(),
			Gas:        transaction.GetGas(),
			GasPrice:   new(big.Int).SetUint64(transaction.GetGasPrice()),
			Value:      value,
			Data:       input,
			AccessList: toGethAccessList(transaction.GetTransactionAccessList()),
			V:          sv,
			R:          sr,
			S:          ss,
		}

	case (v.config.Blockchain() == common.Blockchain_BLOCKCHAIN_OPTIMISM ||
		v.config.Blockchain() == common.Blockchain_BLOCKCHAIN_BASE) &&
		transaction.GetType() == types.DepositTxType:
		v.logger.Debug(
			"toGethTransaction: DepositTx",
			zap.String("hash", transaction.GetHash()),
			zap.String("sourceHash", transaction.GetSourceHash()),
			zap.Bool("isSystemTx", transaction.GetIsSystemTx()),
		)
		data = &types.DepositTx{
			SourceHash:          geth.HexToHash(transaction.GetSourceHash()),
			From:                geth.HexToAddress(transaction.GetFrom()),
			To:                  to,
			Mint:                mint,
			Value:               value,
			Gas:                 transaction.GetGas(),
			IsSystemTransaction: transaction.GetIsSystemTx(),
			Data:                input,
		}

	default:
		v.logger.Debug(
			"toGethTransaction: LegacyTx",
			zap.String("hash", transaction.GetHash()),
		)
		data = &types.LegacyTx{
			To:       to,
			Nonce:    transaction.GetNonce(),
			Gas:      transaction.GetGas(),
			GasPrice: new(big.Int).SetUint64(transaction.GetGasPrice()),
			Value:    value,
			Data:     input,
			V:        sv,
			R:        sr,
			S:        ss,
		}
	}

	return types.NewTx(data), nil
}

// Convert a hex string address to a *geth.Address. Need to return nil for "" input.
func convertTo(to string) *geth.Address {
	if to == "" {

		return nil
	}
	result := geth.HexToAddress(to)
	return &result
}

func toGethAccessList(accessList *api.EthereumTransactionAccessList) types.AccessList {
	gethList := make(types.AccessList, len(accessList.GetAccessList()))
	for i, a := range accessList.GetAccessList() {
		gethList[i] = types.AccessTuple{
			Address:     geth.HexToAddress(a.Address),
			StorageKeys: hexToHash(a.StorageKeys),
		}
	}

	return gethList
}

// Convert an array of hex hash to an array of geth.Hash.
func hexToHash(input []string) []geth.Hash {
	results := make([]geth.Hash, 0, len(input))
	for _, h := range input {
		results = append(results, geth.HexToHash(h))
	}

	return results
}

// Verify all the receipts in the block with the receipt trie root hash.
func (v *ethereumValidator) validateReceipts(ctx context.Context, transactions []*api.EthereumTransaction, receiptsRoot string) error {
	// Similar to validateTransactions(), we need to handle the receipts in state-sync transactions of Polygon.
	numTxs := len(transactions)
	if v.config.Chain.Blockchain == common.Blockchain_BLOCKCHAIN_POLYGON && hasStateSyncTx(transactions) {
		numTxs = numTxs - 1
	}

	// Convert the native receipts to geth receipts.
	gethReceipts := make(types.Receipts, numTxs)
	var err error
	for i := 0; i < numTxs; i++ {
		gethReceipts[i], err = toGethReceipt(transactions[i].GetReceipt())
		if err != nil {
			return fmt.Errorf("failed to convert receipt: %w", err)
		}
	}

	expectedHash := geth.HexToHash(receiptsRoot)

	// This is how geth calculates the receipt trie hash. We just leverage this function of geth to recompute it.
	if actualHash := types.DeriveSha(gethReceipts, trie.NewStackTrie(nil)); actualHash != expectedHash {
		return fmt.Errorf("receipt root hash mismatch (expected=%x, actual=%x): %w", expectedHash, actualHash, ErrInvalidReceiptsHash)
	}

	return nil
}

func toGethReceipt(receipt *api.EthereumTransactionReceipt) (*types.Receipt, error) {
	// Convert the bloom field. Note that, we need to remove the "0x" prefix.
	bloom, err := hexutil.Decode(receipt.GetLogsBloom())
	if err != nil {
		return nil, fmt.Errorf("failed to convert logs bloom %s, %w", receipt.GetLogsBloom(), err)
	}

	// Convert the receipt logs.
	logs, err := toGethLogs(receipt.GetLogs())
	if err != nil {
		return nil, fmt.Errorf("failed to convert receipt logs, %w", err)
	}

	result := &types.Receipt{
		Type:              uint8(receipt.GetType()),
		CumulativeGasUsed: receipt.GetCumulativeGasUsed(),
		Bloom:             types.BytesToBloom(bloom),
		Logs:              logs,
		TxHash:            geth.HexToHash(receipt.GetTransactionHash()),
		ContractAddress:   geth.HexToAddress(receipt.GetContractAddress()),
		GasUsed:           receipt.GetGasUsed(),
		BlockHash:         geth.HexToHash(receipt.GetBlockHash()),
		BlockNumber:       new(big.Int).SetUint64(receipt.GetBlockNumber()),
		TransactionIndex:  uint(receipt.GetTransactionIndex()),
	}

	// Status/PostState are used to calculate the trie hash.
	// Pre-Byzantium, the receipt has PostState. Afterwards, it hash Status.
	if receipt.GetOptionalStatus() != nil {
		result.Status = receipt.GetStatus()
	} else {
		// Need to remove the "0x" prefix.
		root, err := hexutil.Decode(receipt.GetRoot())
		if err != nil {
			return nil, fmt.Errorf("failed to convert poststate %s, %w", receipt.GetRoot(), err)
		}

		result.PostState = root
	}

	if receipt.GetOptionalDepositNonce() != nil {
		result.DepositNonce = pointer.Ref(receipt.GetDepositNonce())
	}

	if receipt.GetOptionalDepositReceiptVersion() != nil {
		result.DepositReceiptVersion = pointer.Ref(receipt.GetDepositReceiptVersion())
	}

	return result, nil
}

func toGethLogs(receiptLogs []*api.EthereumEventLog) ([]*types.Log, error) {
	gethLogs := make([]*types.Log, len(receiptLogs))
	for i, l := range receiptLogs {
		// Need to remove the "0x" prefix.
		data, err := hexutil.Decode(l.GetData())
		if err != nil {
			return nil, fmt.Errorf("failed to convert log data %s, %w", l.GetData(), err)
		}

		gethLogs[i] = &types.Log{
			Address:     geth.HexToAddress(l.GetAddress()),
			Topics:      hexToHash(l.GetTopics()),
			Data:        data,
			BlockNumber: l.GetBlockNumber(),
			TxHash:      geth.HexToHash(l.GetTransactionHash()),
			TxIndex:     uint(l.GetLogIndex()),
			BlockHash:   geth.HexToHash(l.GetBlockHash()),
			Index:       uint(l.GetLogIndex()),
			Removed:     l.GetRemoved(),
		}
	}

	return gethLogs, nil
}

func (v *ethereumValidator) ValidateAccountState(ctx context.Context, req *api.ValidateAccountStateRequest) (*api.ValidateAccountStateResponse, error) {
	// First, process the input request, and extract the usefule info for the verification.
	nativeBlock := req.GetBlock()
	if nativeBlock == nil {
		return nil, fmt.Errorf("nil input native block: %w", internal.ErrInvalidParameters)
	}
	ethereumBlock := nativeBlock.GetEthereum()
	if ethereumBlock == nil {
		return nil, fmt.Errorf("nil input ethereum block: %w", internal.ErrInvalidParameters)
	}
	stateRootHash := ethereumBlock.Header.StateRoot

	accountProof := req.GetAccountProof()
	if accountProof == nil {
		return nil, fmt.Errorf("nil input account proof: %w", internal.ErrInvalidParameters)
	}
	ethereumProof := accountProof.GetEthereum()
	if ethereumProof == nil {
		return nil, fmt.Errorf("nil input ethereum proof: %w", internal.ErrInvalidParameters)
	}
	proofResult := ethereumProof.AccountProof

	// Parse the eth_getProof result to AccountResult
	var accountResult AccountResult
	err := json.Unmarshal(proofResult, &accountResult)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal proofResult: %w", err)
	}

	// There are two cases here:
	// 1. the request is for native token (e.g., ether) verification. We just need to verify the target account state.
	// 2. the request is for erc20 token. Then, we need to verify the account state in two steps:
	//    a. verify the contract state is correct. This is a necessary step which verifies the storage hash is valid.
	//    b. verify the target token address state is correct. With the valid storage hash, we then verify the target
	//       token account state, running the proof verification against the storage state proof.
	accountReq := req.AccountReq
	contractAddr := accountReq.GetEthereum().GetErc20Contract()
	var account string

	// If the input contract addrss is empty, then the account would be the native token account address.
	// Otherwise, we need to verify the contract account address first.
	if contractAddr == "" {
		account = accountReq.Account
	} else {
		account = contractAddr
	}

	// Verify that this proofResult is for the target account
	if accountResult.Address != geth.HexToAddress(account) {
		return nil, fmt.Errorf("the input proofResult has different account address, address in proof: %s, expected: %s", accountResult.Address.Hex(), account)
	}

	// Create the in-memory DB state of the state trie proof
	proofDB := rawdb.NewMemoryDatabase()
	for _, node := range accountResult.AccountProof {
		// Need to remove the "0x" prefix.
		nodeData, err := hexutil.Decode(node)
		if err != nil {
			return nil, fmt.Errorf("failed to hexutil.Decode the node %s: %w", node, err)

		}
		// Reconstruct the node path information. The key is the hash of the node.
		err = proofDB.Put(crypto.Keccak256(nodeData), nodeData)
		if err != nil {
			return nil, fmt.Errorf("failed to construct the in-memory DB for proofResult: %w", err)
		}
	}

	// Need to remove the "0x" prefix.
	accountData, err := hexutil.Decode(account)
	if err != nil {
		return nil, fmt.Errorf("failed to hexutil.Decode the account %s: %w", account, err)
	}

	// Use state_root_hash to walk through the returned proof to verify the state
	// Note that, the input is the hash of the account.
	validAccountState, err := trie.VerifyProof(geth.HexToHash(stateRootHash), crypto.Keccak256(accountData), proofDB)
	if err != nil {
		return nil, fmt.Errorf("VerifyProof fails with %v for the account %s: %w", err, account, ErrAccountVerifyProofFailure)
	}

	// If the err is nil and returned account state is nil, then this means that the state trie doesn't include the target account, and the verification fails.
	if validAccountState == nil {
		return nil, fmt.Errorf("VerifyProof fails, the account %s is not included in the state trie: %w", account, ErrAccountVerifyProofFailure)
	}

	// If succsessful, decode the stored account state, and return it.
	var verifiedAccountState types.StateAccount
	if err := rlp.DecodeBytes(validAccountState, &verifiedAccountState); err != nil {
		return nil, fmt.Errorf("failed to rlp decode the verified account state: %w", err)
	}

	// After the veirifcation is successful, we further check the input account proof is the same as the returned verified account state.
	if accountResult.Nonce != hexutil.Uint64(verifiedAccountState.Nonce) {
		return nil, fmt.Errorf("account nonce is not matched, (nonce in proof=%v, nonce in verified result=%v): %w", accountResult.Nonce, hexutil.Uint64(verifiedAccountState.Nonce), ErrAccountNonceNotMatched)
	}
	if accountResult.Balance.ToInt().Cmp(verifiedAccountState.Balance) != 0 {
		return nil, fmt.Errorf("account balance is not matched, (balance in proof=%v, balance in verified result=%v): %w", accountResult.Balance.ToInt(), verifiedAccountState.Balance, ErrAccountBalanceNotMatched)
	}
	if accountResult.StorageHash != verifiedAccountState.Root {
		return nil, fmt.Errorf("account storage hash is not matched, (storage hash in proof=%v, storage hash in verified result=%v): %w", accountResult.StorageHash, verifiedAccountState.Root, ErrAccountStorageHashNotMatched)
	}
	if !bytes.Equal(accountResult.CodeHash.Bytes(), verifiedAccountState.CodeHash) {
		return nil, fmt.Errorf("account code hash is not matched, (code hash in proof=%v, code hash in verified result=%v): %w", accountResult.CodeHash.Bytes(), verifiedAccountState.CodeHash, ErrAccountCodeHashNotMatched)
	}

	// If the request is for the native token, return now.
	if contractAddr == "" {
		return &api.ValidateAccountStateResponse{
			Balance: accountResult.Balance.String(),
			Response: &api.ValidateAccountStateResponse_Ethereum{
				Ethereum: &api.EthereumAccountStateResponse{
					Nonce:       uint64(accountResult.Nonce),
					StorageHash: accountResult.StorageHash.String(),
					CodeHash:    accountResult.CodeHash.String(),
				},
			},
		}, nil
	}

	// Now, we need to handle the erc20 account verification.
	account = accountReq.Account
	if len(accountResult.StorageProof) == 0 {
		return nil, fmt.Errorf("the storage proof is empty for the token account: %s", account)
	}
	storageResult := accountResult.StorageProof[0]

	// Need to remove the "0x" prefix.
	key, err := hexutil.Decode(storageResult.Key)
	if err != nil {
		return nil, fmt.Errorf("failed to hexutil.Decode the key %s: %w", storageResult.Key, err)
	}
	// Need to pre-process the key.
	keyData := geth.LeftPadBytes(key, 32)

	// Create the in-memory DB state of the storage state trie proof
	proofDB = rawdb.NewMemoryDatabase()
	for _, node := range storageResult.Proof {
		// Need to remove the "0x" prefix.
		nodeData, err := hexutil.Decode(node)
		if err != nil {
			return nil, fmt.Errorf("failed to hexutil.Decode the node %s: %w", node, err)
		}
		// Reconstruct the node path information. The key is the hash of the node.
		err = proofDB.Put(crypto.Keccak256(nodeData), nodeData)
		if err != nil {
			return nil, fmt.Errorf("failed to construct the in-memory DB for proofResult: %w", err)
		}
	}

	// Use storage_root_hash to walk through the returned storage proof to verify the storage state
	// Note that, the input is the hash of the storage key.
	validAccountState, err = trie.VerifyProof(accountResult.StorageHash, crypto.Keccak256(keyData), proofDB)
	if err != nil {
		return nil, fmt.Errorf("VerifyProof fails with %v for the token account %s: %w", err, account, ErrAccountVerifyProofFailure)
	}

	// If the err is nil and returned account state is nil, then this means that the storage trie doesn't include the target account, and the verification fails.
	if validAccountState == nil {
		return nil, fmt.Errorf("VerifyProof fails, the token account %s is not included in the storage state trie: %w", account, ErrAccountVerifyProofFailure)
	}

	// If succsessful, decode the stored account state, and return it.
	var tokenBalance big.Int
	if err := rlp.DecodeBytes(validAccountState, &tokenBalance); err != nil {
		return nil, fmt.Errorf("failed to rlp decode the verified token account state: %w", err)
	}

	// After the veirifcation is successful, we further check the token balance is the same as the returned verified account state.
	if accountResult.StorageProof[0].Value.ToInt().Cmp(&tokenBalance) != 0 {
		return nil, fmt.Errorf("token account balance is not matched, (balance in proof=%v, balance in verified result=%v): %w", accountResult.StorageProof[0].Value.ToInt(), tokenBalance, ErrAccountBalanceNotMatched)
	}

	return &api.ValidateAccountStateResponse{
		// Note that, we return the token balance here.
		Balance: tokenBalance.String(),
		Response: &api.ValidateAccountStateResponse_Ethereum{
			Ethereum: &api.EthereumAccountStateResponse{
				Nonce:       uint64(accountResult.Nonce),
				StorageHash: accountResult.StorageHash.String(),
				CodeHash:    accountResult.CodeHash.String(),
			},
		},
	}, nil
}
