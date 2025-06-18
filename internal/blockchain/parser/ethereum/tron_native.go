package ethereum

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strconv"
	"strings"

	"golang.org/x/xerrors"

	"github.com/mr-tron/base58"

	"github.com/coinbase/chainstorage/internal/blockchain/parser/ethereum/types"
	"github.com/coinbase/chainstorage/internal/blockchain/parser/internal"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

func NewTronNativeParser(params internal.ParserParams, opts ...internal.ParserFactoryOption) (internal.NativeParser, error) {
	// Tron shares the same data schema as Ethereum since its an EVM chain except skip trace data
	opts = append(opts, WithEthereumNodeType(types.EthereumNodeType_ARCHIVAL), WithTraceType(types.TraceType_PARITY))
	return NewEthereumNativeParser(params, opts...)
}

type TronCallValueInfo struct {
	CallValue int64  `json:"callValue"`
	TokenId   string `json:"tokenId"`
}

type TronTransactionInfo struct {
	InternalTransactions []TronInternalTransaction `json:"internal_transactions"`
	Id                   string                    `json:"id"`
	BlockNumber          uint64                    `json:"blockNumber"`
	TransactionHash      string                    `json:"transactionHash"`
	Fee                  uint64                    `json:"fee"`
	Receipt              TronReceipt               `json:"receipt"`
}

type TronReceipt struct {
	Result string `json:"result"`
	// Bandwidth is represented as either net_fee or net_usage, only one will exist in the response
	NetFee             uint64 `json:"net_fee"`
	NetUsage           uint64 `json:"net_usage"`
	EnergyUsage        uint64 `json:"energy_usage"`
	EnergyFee          uint64 `json:"energy_fee"`
	OriginEnergyUsage  uint64 `json:"origin_energy_usage"`
	EnergyUsageTotal   uint64 `json:"energy_usage_total"`
	EnergyPenaltyTotal uint64 `json:"energy_penalty_total"`
}

type TronInternalTransaction struct {
	Hash              string              `json:"hash"`
	CallerAddress     string              `json:"caller_address"`
	TransferToAddress string              `json:"transferTo_address"`
	CallValueInfo     []TronCallValueInfo `json:"callValueInfo"`
	Note              string              `json:"note"`
	Rejected          bool                `json:"rejected"`
}

func convertInternalTransactionToTrace(itx *TronInternalTransaction) *api.EthereumTransactionFlattenedTrace {
	// only keep native values, ignore TRC10 token values
	var nativeTokenValue int64
	for _, callValueInfoItem := range itx.CallValueInfo {
		if callValueInfoItem.TokenId == "" {
			// If TokenId is empty, it means this is a native token transfer
			nativeTokenValue += callValueInfoItem.CallValue
		}
	}

	trace := &api.EthereumTransactionFlattenedTrace{
		Type:              "CALL",
		TraceType:         "CALL",
		CallType:          "CALL",
		From:              hexToTronAddress(itx.CallerAddress),
		To:                hexToTronAddress(itx.TransferToAddress),
		Value:             strconv.FormatInt(nativeTokenValue, 10),
		TraceId:           itx.Hash,
		TronCallValueInfo: convertTronCallValueInfo(itx.CallValueInfo),
	}
	if itx.Rejected {
		trace.Error = "Internal transaction is executed failed"
		trace.Status = 0
	} else {
		trace.Status = 1
	}

	return trace
}

func convertTronCallValueInfo(callValueInfo []TronCallValueInfo) []*api.TronCallValueInfo {
	result := make([]*api.TronCallValueInfo, len(callValueInfo))
	for i, info := range callValueInfo {
		result[i] = &api.TronCallValueInfo{
			TokenId:   info.TokenId,
			CallValue: info.CallValue,
		}
	}
	return result
}

func parseTronTxInfo(
	blobData *api.EthereumBlobdata,
	header *api.EthereumHeader,
	transactionToFlattenedTracesMap map[string][]*api.EthereumTransactionFlattenedTrace,
	txReceipts []*api.EthereumTransactionReceipt,
) error {
	if len(blobData.TransactionTraces) == 0 {
		return nil
	}

	// Ensure we have matching number of receipts and traces
	if len(blobData.TransactionTraces) != len(txReceipts) {
		return xerrors.Errorf(
			"mismatch between number of transaction traces (%d) and receipts (%d)",
			len(blobData.TransactionTraces),
			len(txReceipts),
		)
	}

	for txIndex, rawTxInfo := range blobData.TransactionTraces {
		var txInfo TronTransactionInfo
		if err := json.Unmarshal(rawTxInfo, &txInfo); err != nil {
			return xerrors.Errorf("failed to parse transaction trace at index %d: %w", txIndex, err)
		}

		traceTransactionHash := txInfo.Id
		txIdx := uint64(txIndex)
		fee := txInfo.Fee
		receipt := txInfo.Receipt
		// 1. enreach txReceipt with fee and net_fee (Bandwidth)fields from transactionInfo.receipt
		txReceipt := txReceipts[txIndex]
		if fee != 0 {
			txReceipt.OptionalFee = &api.EthereumTransactionReceipt_Fee{
				Fee: uint64(fee),
			}
		}
		if receipt.NetFee != 0 {
			txReceipt.OptionalNetFee = &api.EthereumTransactionReceipt_NetFee{
				NetFee: uint64(receipt.NetFee),
			}
		}
		if receipt.NetUsage != 0 {
			txReceipt.OptionalNetUsage = &api.EthereumTransactionReceipt_NetUsage{
				NetUsage: uint64(receipt.NetUsage),
			}
		}
		if receipt.EnergyUsage != 0 {
			txReceipt.OptionalEnergyUsage = &api.EthereumTransactionReceipt_EnergyUsage{
				EnergyUsage: uint64(receipt.EnergyUsage),
			}
		}
		if receipt.EnergyFee != 0 {
			txReceipt.OptionalEnergyFee = &api.EthereumTransactionReceipt_EnergyFee{
				EnergyFee: uint64(receipt.EnergyFee),
			}
		}
		if receipt.OriginEnergyUsage != 0 {
			txReceipt.OptionalOriginEnergyUsage = &api.EthereumTransactionReceipt_OriginEnergyUsage{
				OriginEnergyUsage: uint64(receipt.OriginEnergyUsage),
			}
		}
		if receipt.EnergyUsageTotal != 0 {
			txReceipt.OptionalEnergyUsageTotal = &api.EthereumTransactionReceipt_EnergyUsageTotal{
				EnergyUsageTotal: uint64(receipt.EnergyUsageTotal),
			}
		}
		if receipt.EnergyPenaltyTotal != 0 {
			txReceipt.OptionalEnergyPenaltyTotal = &api.EthereumTransactionReceipt_EnergyPenaltyTotal{
				EnergyPenaltyTotal: uint64(receipt.EnergyPenaltyTotal),
			}
		}

		// 2. mapping internalTransactions to trace
		internalTxs := txInfo.InternalTransactions
		traces := make([]*api.EthereumTransactionFlattenedTrace, len(internalTxs))
		for i, internalTx := range internalTxs {
			trace := convertInternalTransactionToTrace(&internalTx)
			trace.BlockHash = toTronHash(header.Hash)
			trace.BlockNumber = header.Number
			trace.TransactionHash = traceTransactionHash
			trace.TransactionIndex = txIdx
			traces[i] = trace
		}
		transactionToFlattenedTracesMap[traceTransactionHash] = traces
	}
	return nil
}

func toTronHash(hexHash string) string {
	// if hexHash == "" {
	// 	return ""
	// }
	// // Normalize the hash by ensuring it's lowercase and removing 0x prefix
	// hexHash = strings.ToLower(hexHash)
	return strings.Replace(hexHash, "0x", "", -1)
}

func hexToTronAddress(hexAddress string) string {
	if hexAddress == "" {
		return ""
	}

	// Ensure consistent format by cleaning the hex address
	hexAddress = strings.ToLower(hexAddress)
	if strings.HasPrefix(hexAddress, "0x") {
		hexAddress = "41" + hexAddress[2:]
	} else if !strings.HasPrefix(hexAddress, "41") {
		hexAddress = "41" + hexAddress
	}

	// Decode hex string to bytes
	rawBytes, err := hex.DecodeString(hexAddress)
	if err != nil {
		// If unable to decode, return the original address to avoid data loss
		return hexAddress
	}

	// Compute double SHA-256 checksum
	hash1 := sha256.Sum256(rawBytes)
	hash2 := sha256.Sum256(hash1[:])
	checksum := hash2[:4] // First 4 bytes as checksum
	// Append checksum to the raw bytes
	fullBytes := append(rawBytes, checksum...)
	// Base58Check encode
	tronAddress := base58.Encode(fullBytes)

	return tronAddress
}

func convertTokenTransfer(data *api.EthereumTokenTransfer) {
	data.TokenAddress = hexToTronAddress(data.TokenAddress)
	data.FromAddress = hexToTronAddress(data.FromAddress)
	data.ToAddress = hexToTronAddress(data.ToAddress)

	data.TransactionHash = toTronHash(data.TransactionHash)
	data.BlockHash = toTronHash(data.BlockHash)

	switch v := data.TokenTransfer.(type) {
	case *api.EthereumTokenTransfer_Erc20:
		if v.Erc20 != nil {
			v.Erc20.FromAddress = hexToTronAddress(v.Erc20.FromAddress)
			v.Erc20.ToAddress = hexToTronAddress(v.Erc20.ToAddress)
		}
	case *api.EthereumTokenTransfer_Erc721:
		if v.Erc721 != nil {
			v.Erc721.FromAddress = hexToTronAddress(v.Erc721.FromAddress)
			v.Erc721.ToAddress = hexToTronAddress(v.Erc721.ToAddress)
		}
	}
}

func postProcessTronBlock(
	blobData *api.EthereumBlobdata,
	header *api.EthereumHeader,
	transactions []*api.EthereumTransaction,
	txReceipts []*api.EthereumTransactionReceipt,
	tokenTransfers [][]*api.EthereumTokenTransfer,
	transactionToFlattenedTracesMap map[string][]*api.EthereumTransactionFlattenedTrace,
) error {
	if err := parseTronTxInfo(blobData, header, transactionToFlattenedTracesMap, txReceipts); err != nil {
		return xerrors.Errorf("failed to parse transaction parity traces: %w", err)
	}

	header.Hash = toTronHash(header.Hash)
	header.ParentHash = toTronHash(header.ParentHash)
	header.TransactionsRoot = toTronHash(header.TransactionsRoot)
	header.Miner = hexToTronAddress(header.Miner)

	for i := range header.Transactions {
		header.Transactions[i] = toTronHash(header.Transactions[i])
	}

	for _, tx := range transactions {
		tx.BlockHash = toTronHash(tx.BlockHash)
		tx.Hash = toTronHash(tx.Hash)
		if tx.From != "" {
			tx.From = hexToTronAddress(tx.From)
		}
		if tx.To != "" {
			tx.To = hexToTronAddress(tx.To)
		}
	}

	for _, txR := range txReceipts {
		txR.TransactionHash = toTronHash(txR.TransactionHash)
		txR.BlockHash = toTronHash(txR.BlockHash)
		if txR.From != "" {
			txR.From = hexToTronAddress(txR.From)
		}
		if txR.To != "" {
			txR.To = hexToTronAddress(txR.To)
		}
		if txR.Logs != nil {
			for _, txLog := range txR.Logs {
				txLog.TransactionHash = toTronHash(txLog.TransactionHash)
				txLog.BlockHash = toTronHash(txLog.BlockHash)
				txLog.Address = hexToTronAddress(txLog.Address)
			}
		}
	}
	for _, txTokenTransfers := range tokenTransfers {
		for _, tokenTransfer := range txTokenTransfers {
			convertTokenTransfer(tokenTransfer)
		}
	}
	return nil
}
