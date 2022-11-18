package parser

import (
	"context"
	"encoding/json"

	"github.com/gagliardetto/solana-go"
	"github.com/golang/protobuf/ptypes/timestamp"
	"go.uber.org/zap"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/utils/log"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

const (
	SolanaLegacyVersion = int32(-1)

	SolanaLegacyVersionStr = "legacy"
)

type (
	SolanaTransactionVersion int32

	solanaNativeParserImpl struct {
		logger *zap.Logger
		config *config.Config
	}

	SolanaBlockLit struct {
		BlockHash         string `json:"blockhash"`
		BlockHeight       uint64 `json:"blockHeight"`
		ParentSlot        uint64 `json:"parentSlot"`
		PreviousBlockHash string `json:"previousBlockhash"`
		BlockTime         int64  `json:"blockTime"`
	}

	SolanaBlock struct {
		BlockHash         string              `json:"blockhash"`
		BlockHeight       uint64              `json:"blockHeight"`
		ParentSlot        uint64              `json:"parentSlot"`
		PreviousBlockHash string              `json:"previousBlockhash"`
		BlockTime         int64               `json:"blockTime"`
		Transactions      []SolanaTransaction `json:"transactions"`
		Rewards           []SolanaReward      `json:"rewards"`
	}

	SolanaTransaction struct {
		Payload *SolanaTransactionPayload `json:"transaction"`
		Meta    *SolanaTransactionMeta    `json:"meta"`
		Version *SolanaTransactionVersion `json:"version"`
	}

	SolanaTransactionMeta struct {
		Err               SolanaTransactionError   `json:"err"`
		Fee               uint64                   `json:"fee"`
		PreBalances       []uint64                 `json:"preBalances"`
		PostBalances      []uint64                 `json:"postBalances"`
		PreTokenBalances  []SolanaTokenBalance     `json:"preTokenBalances"`
		PostTokenBalances []SolanaTokenBalance     `json:"postTokenBalances"`
		InnerInstructions []SolanaInnerInstruction `json:"innerInstructions"`
		LogMessages       []string                 `json:"logMessages"`
		Rewards           []SolanaReward           `json:"rewards"`
		LoadedAddresses   *SolanaLoadedAddresses   `json:"loadedAddresses"`
	}

	SolanaLoadedAddresses struct {
		Readonly []string `json:"readonly"`
		Writable []string `json:"writable"`
	}

	SolanaTokenBalance struct {
		AccountIndex uint64            `json:"accountIndex"`
		Mint         string            `json:"mint"`
		TokenAmount  SolanaTokenAmount `json:"uiTokenAmount"`
		Owner        string            `json:"owner"`
	}

	SolanaTokenAmount struct {
		Amount         string `json:"amount"`
		Decimals       uint64 `json:"decimals"`
		UIAmountString string `json:"uiAmountString"`
	}

	SolanaInnerInstruction struct {
		Index        uint64              `json:"index"`
		Instructions []SolanaInstruction `json:"instructions"`
	}

	SolanaTransactionPayload struct {
		Signatures []string      `json:"signatures"`
		Message    SolanaMessage `json:"message"`
	}

	SolanaMessage struct {
		Header          SolanaMessageHeader `json:"header"`
		AccountKeys     []string            `json:"accountKeys"`
		RecentBlockHash string              `json:"recentBlockhash"`
		Instructions    []SolanaInstruction `json:"instructions"`
	}

	SolanaMessageHeader struct {
		NumRequiredSignatures       uint64 `json:"numRequiredSignatures"`
		NumReadOnlySignedAccounts   uint64 `json:"numReadonlySignedAccounts"`
		NumReadOnlyUnsignedAccounts uint64 `json:"numReadonlyUnsignedAccounts"`
	}

	SolanaInstruction struct {
		ProgramIDIndex uint16   `json:"programIdIndex"`
		Accounts       []uint16 `json:"accounts"`
		Data           string   `json:"data"`
	}

	SolanaReward struct {
		Pubkey      string  `json:"pubkey"`
		Lamports    int64   `json:"lamports"`
		PostBalance uint64  `json:"postBalance"`
		RewardType  string  `json:"rewardType"`
		Commission  *uint64 `json:"commission"`
	}

	SolanaTransactionError = interface{}
)

func NewSolanaNativeParser(params ParserParams, opts ...ParserFactoryOption) (NativeParser, error) {
	return &solanaNativeParserImpl{
		logger: log.WithPackage(params.Logger),
		config: params.Config,
	}, nil
}

func (p *solanaNativeParserImpl) ParseBlock(ctx context.Context, rawBlock *api.Block) (*api.NativeBlock, error) {
	metadata := rawBlock.GetMetadata()
	if metadata == nil {
		return nil, xerrors.New("metadata not found")
	}

	if metadata.Skipped {
		return &api.NativeBlock{
			Blockchain: rawBlock.Blockchain,
			Network:    rawBlock.Network,
			Tag:        metadata.Tag,
			Height:     metadata.Height,
			Skipped:    true,
		}, nil
	}

	blobdata := rawBlock.GetSolana()
	if blobdata == nil {
		return nil, xerrors.Errorf("blobdata not found (metadata={%+v})", metadata)
	}

	var block SolanaBlock
	if err := json.Unmarshal(blobdata.Header, &block); err != nil {
		return nil, xerrors.Errorf("failed to unmarshal header (metadata={%+v}: %w", metadata, err)
	}

	nativeBlock, err := p.parseBlock(metadata.Height, &block)
	if err != nil {
		return nil, xerrors.Errorf("failed to parse block (metadata={%+v}: %w", metadata, err)
	}

	return &api.NativeBlock{
		Blockchain:      rawBlock.Blockchain,
		Network:         rawBlock.Network,
		Tag:             metadata.Tag,
		Hash:            metadata.Hash,
		ParentHash:      metadata.ParentHash,
		Height:          metadata.Height,
		ParentHeight:    metadata.ParentHeight,
		Timestamp:       p.parseTimestamp(block.BlockTime),
		NumTransactions: uint64(len(block.Transactions)),
		Block: &api.NativeBlock_Solana{
			Solana: nativeBlock,
		},
	}, nil
}

func (p *solanaNativeParserImpl) parseTimestamp(ts int64) *timestamp.Timestamp {
	return &timestamp.Timestamp{
		Seconds: ts,
	}
}

func (p *solanaNativeParserImpl) parseBlock(slot uint64, block *SolanaBlock) (*api.SolanaBlock, error) {
	header, err := p.parseHeader(slot, block)
	if err != nil {
		return nil, xerrors.Errorf("failed to parse header: %w", err)
	}

	transactions, err := p.parseTransactions(block.Transactions)
	if err != nil {
		return nil, xerrors.Errorf("failed to parse transactions: %w", err)
	}

	rewards, err := p.parseRewards(block.Rewards)
	if err != nil {
		return nil, xerrors.Errorf("failed to parse rewards: %w", err)
	}

	return &api.SolanaBlock{
		Header:       header,
		Transactions: transactions,
		Rewards:      rewards,
	}, nil
}

func (p *solanaNativeParserImpl) parseHeader(slot uint64, block *SolanaBlock) (*api.SolanaHeader, error) {
	if block.BlockHash == "" {
		return nil, xerrors.New("block hash is empty")
	}

	return &api.SolanaHeader{
		BlockHash:         block.BlockHash,
		PreviousBlockHash: block.PreviousBlockHash,
		Slot:              slot,
		ParentSlot:        block.ParentSlot,
		BlockTime:         p.parseTimestamp(block.BlockTime),
		BlockHeight:       block.BlockHeight,
	}, nil
}

func (p *solanaNativeParserImpl) parseTransactions(transactions []SolanaTransaction) ([]*api.SolanaTransaction, error) {
	result := make([]*api.SolanaTransaction, len(transactions))
	for i, v := range transactions {
		version := p.parseTransactionVersion(v.Version)

		payload, accounts, err := p.parseTransactionPayload(v.Payload, v.Meta)
		if err != nil {
			return nil, xerrors.Errorf("failed to parse transaction payload (payload={%+v}): %w", v.Payload, err)
		}

		// transactionID is the first signature in a transaction, which can be used to uniquely identify the transaction across the complete ledger.
		if len(payload.Signatures) == 0 {
			return nil, xerrors.New("signatures are empty")
		}

		transactionID := payload.Signatures[0]
		if transactionID == "" {
			return nil, xerrors.New("transaction id is empty")
		}

		meta, err := p.parseTransactionMeta(v.Meta, accounts)
		if err != nil {
			return nil, xerrors.Errorf("failed to parse transaction meta (transactionID=%v, meta={%+v}): %w", transactionID, v.Meta, err)
		}

		result[i] = &api.SolanaTransaction{
			TransactionId: transactionID,
			Payload:       payload,
			Meta:          meta,
			Version:       version,
		}
	}

	return result, nil
}

func (p *solanaNativeParserImpl) parseTransactionMeta(meta *SolanaTransactionMeta, accounts solana.AccountMetaSlice) (*api.SolanaTransactionMeta, error) {
	if meta == nil {
		// meta is optional.
		return nil, nil
	}

	rewards, err := p.parseRewards(meta.Rewards)
	if err != nil {
		return nil, xerrors.Errorf("failed to parse rewards: %w", err)
	}

	txErr, err := p.parseError(meta.Err)
	if err != nil {
		return nil, xerrors.Errorf("failed to parse error: %w", err)
	}

	innerInstructions, err := p.parseInnerInstructions(meta.InnerInstructions, accounts)
	if err != nil {
		return nil, xerrors.Errorf("failed to parse inner instructions: %w", err)
	}

	return &api.SolanaTransactionMeta{
		Err:               txErr,
		Fee:               meta.Fee,
		PreBalances:       meta.PreBalances,
		PostBalances:      meta.PostBalances,
		PreTokenBalances:  p.parseTokenBalances(meta.PreTokenBalances),
		PostTokenBalances: p.parseTokenBalances(meta.PostTokenBalances),
		InnerInstructions: innerInstructions,
		LogMessages:       meta.LogMessages,
		Rewards:           rewards,
	}, nil
}

// Append addresses loaded from address lookup tables.
// Should be moved to `AccountMetaList` later: https://github.com/gagliardetto/solana-go/commit/47bdad3ad7f9754e27f68ca79a6553e9dc2ee4bc
func (p *solanaNativeParserImpl) appendLookupAccounts(accounts *solana.AccountMetaSlice, meta *SolanaTransactionMeta) error {
	if meta == nil {
		return nil
	}

	loadedAddresses := meta.LoadedAddresses
	if loadedAddresses == nil {
		return nil
	}

	for _, accountKey := range loadedAddresses.Writable {
		publicKey, err := solana.PublicKeyFromBase58(accountKey)
		if err != nil {
			return xerrors.Errorf("failed to parse public key of writable address (%v): %w", accountKey, err)
		}

		accounts.Append(&solana.AccountMeta{
			PublicKey:  publicKey,
			IsSigner:   false,
			IsWritable: true,
		})
	}

	for _, accountKey := range loadedAddresses.Readonly {
		publicKey, err := solana.PublicKeyFromBase58(accountKey)
		if err != nil {
			return xerrors.Errorf("failed to parse public key of readonly address (%v): %w", accountKey, err)
		}

		accounts.Append(&solana.AccountMeta{
			PublicKey:  publicKey,
			IsSigner:   false,
			IsWritable: false,
		})
	}

	return nil
}

func (p *solanaNativeParserImpl) parseInnerInstructions(input []SolanaInnerInstruction, accounts solana.AccountMetaSlice) ([]*api.SolanaInnerInstruction, error) {
	output := make([]*api.SolanaInnerInstruction, len(input))
	for i, v := range input {
		instructions, err := p.parseInstructions(p.toRichInstructions(v.Instructions), accounts)
		if err != nil {
			return nil, xerrors.Errorf("failed to parse instructions: %w", err)
		}

		output[i] = &api.SolanaInnerInstruction{
			Index:        v.Index,
			Instructions: instructions,
		}
	}

	return output, nil
}

func (p *solanaNativeParserImpl) parseError(error SolanaTransactionError) (string, error) {
	switch v := error.(type) {
	case nil:
		return "", nil
	case string:
		// In rare circumstances, error is returned as a string.
		// "err": "InvalidRentPayingAccount"
		return v, nil
	case map[string]interface{}:
		// Extract the error type from the map.
		// "err": {
		//   "InstructionError": [
		//     0,
		//     {
		//       "Custom": 0
		//     }
		//   ]
		// }
		if len(v) > 1 {
			return "", xerrors.Errorf("failed to parse error: %v", error)
		}

		var result string
		for k := range v {
			// Return the error type and discard the value.
			result = k
			break
		}

		return result, nil
	default:
		return "", xerrors.Errorf("unexpected type %T: %v", v, v)
	}
}

func (p *solanaNativeParserImpl) parseTokenBalances(tokenBalances []SolanaTokenBalance) []*api.SolanaTokenBalance {
	result := make([]*api.SolanaTokenBalance, len(tokenBalances))
	for i, v := range tokenBalances {
		result[i] = &api.SolanaTokenBalance{
			AccountIndex: v.AccountIndex,
			Mint:         v.Mint,
			TokenAmount: &api.SolanaTokenAmount{
				Amount:         v.TokenAmount.Amount,
				Decimals:       v.TokenAmount.Decimals,
				UiAmountString: v.TokenAmount.UIAmountString,
			},
			Owner: v.Owner,
		}
	}

	return result
}

func (p *solanaNativeParserImpl) parseRewards(rewards []SolanaReward) ([]*api.SolanaReward, error) {
	result := make([]*api.SolanaReward, len(rewards))
	for i, v := range rewards {
		result[i] = &api.SolanaReward{
			Pubkey:      decodeBase58(v.Pubkey),
			Lamports:    v.Lamports,
			PostBalance: v.PostBalance,
			RewardType:  v.RewardType,
		}

		if v.Commission != nil {
			result[i].OptionalCommission = &api.SolanaReward_Commission{
				Commission: *v.Commission,
			}
		}
	}

	return result, nil
}

func (p *solanaNativeParserImpl) parseTransactionVersion(transactionVersion *SolanaTransactionVersion) int32 {
	// This is for backward compatible where there is no `version` field data when using `getConfirmedBlock`.
	if transactionVersion == nil {
		return SolanaLegacyVersion
	}
	return transactionVersion.Value()
}

func (p *solanaNativeParserImpl) parseTransactionPayload(payload *SolanaTransactionPayload, meta *SolanaTransactionMeta) (*api.SolanaTransactionPayload, solana.AccountMetaSlice, error) {
	if payload == nil {
		return nil, nil, xerrors.New("payload is null")
	}

	message, accounts, err := p.parseTransactionMessage(&payload.Message, meta)
	if err != nil {
		return nil, nil, xerrors.Errorf("failed to parse transaction message: %w", err)
	}

	return &api.SolanaTransactionPayload{
		Signatures: payload.Signatures,
		Message:    message,
	}, accounts, nil
}

func (p *solanaNativeParserImpl) parseTransactionMessage(message *SolanaMessage, meta *SolanaTransactionMeta) (*api.SolanaMessage, solana.AccountMetaSlice, error) {
	richMessage, err := p.toRichMessage(message)
	if err != nil {
		return nil, nil, xerrors.Errorf("failed to parse message: %w", err)
	}

	accounts := richMessage.AccountMetaList()

	// Need to append addresses from lookup tables for non-legacy transaction type.
	// https://docs.solana.com/developing/clients/jsonrpc-api#results-2
	// Example block height: 154808473
	// Transaction hash: 5X7cr7bxjwoWvWFvSmVfmF3PhYcid4ojtNGYiDsRRP6P5cfzkahxbZfwH4YDdhy9RnSEyaT255aDw14vis5Qxvqh
	err = p.appendLookupAccounts(&accounts, meta)
	if err != nil {
		return nil, nil, xerrors.Errorf("failed to append accounts from meta: %w", err)
	}

	instructions, err := p.parseInstructions(richMessage.Instructions, accounts)
	if err != nil {
		return nil, nil, xerrors.Errorf("failed to parse instructions: %w", err)
	}

	parsedMessage := &api.SolanaMessage{
		Header: &api.SolanaMessageHeader{
			NumRequiredSignatures:       uint64(richMessage.Header.NumRequiredSignatures),
			NumReadonlySignedAccounts:   uint64(richMessage.Header.NumReadonlySignedAccounts),
			NumReadonlyUnsignedAccounts: uint64(richMessage.Header.NumReadonlyUnsignedAccounts),
		},
		Accounts:        p.parseAccounts(accounts),
		RecentBlockHash: richMessage.RecentBlockhash.String(),
		Instructions:    instructions,
	}
	return parsedMessage, accounts, nil
}

func (p *solanaNativeParserImpl) parseAccounts(input solana.AccountMetaSlice) []*api.SolanaAccount {
	output := make([]*api.SolanaAccount, len(input))
	for i, v := range input {
		output[i] = &api.SolanaAccount{
			PublicKey: v.PublicKey.String(),
			Signer:    v.IsSigner,
			Writable:  v.IsWritable,
		}
	}
	return output
}

func (p *solanaNativeParserImpl) parseInstructions(instructions []solana.CompiledInstruction, accounts solana.AccountMetaSlice) ([]*api.SolanaInstruction, error) {
	result := make([]*api.SolanaInstruction, len(instructions))
	for i, instruction := range instructions {
		programID := accounts.Get(int(instruction.ProgramIDIndex))
		if programID == nil {
			return nil, xerrors.Errorf("failed to find program account at %v", instruction.ProgramIDIndex)
		}

		numAccounts := len(instruction.Accounts)
		accountIndexes := make([]uint64, numAccounts)
		accountKeys := make([]string, numAccounts)
		accountMetas := make([]*solana.AccountMeta, numAccounts)
		for j, accountIndex := range instruction.Accounts {
			account := accounts.Get(int(accountIndex))
			if account == nil {
				return nil, xerrors.Errorf("failed to find program account at %v", accountIndex)
			}

			accountIndexes[j] = uint64(accountIndex)
			accountKeys[j] = account.PublicKey.String()
			accountMetas[j] = account
		}

		parsedInstruction := &api.SolanaInstruction{
			ProgramIdIndex: uint64(instruction.ProgramIDIndex),
			ProgramId:      programID.PublicKey.String(),
			Accounts:       accountIndexes,
			AccountKeys:    accountKeys,
			Data:           instruction.Data,
		}

		result[i] = parsedInstruction
	}

	return result, nil
}

func (p *solanaNativeParserImpl) toRichMessage(input *SolanaMessage) (*solana.Message, error) {
	accountKeys := make([]solana.PublicKey, len(input.AccountKeys))
	for i, accountKey := range input.AccountKeys {
		publicKey, err := solana.PublicKeyFromBase58(accountKey)
		if err != nil {
			return nil, xerrors.Errorf("failed to parse public key (%v): %w", accountKey, err)
		}

		accountKeys[i] = publicKey
	}

	recentBlockHash, err := solana.HashFromBase58(input.RecentBlockHash)
	if err != nil {
		return nil, xerrors.Errorf("failed to parse recent block hash (%v): %w", input.RecentBlockHash, err)
	}

	instructions := p.toRichInstructions(input.Instructions)

	output := &solana.Message{
		AccountKeys: accountKeys,
		Header: solana.MessageHeader{
			NumRequiredSignatures:       uint8(input.Header.NumRequiredSignatures),
			NumReadonlySignedAccounts:   uint8(input.Header.NumReadOnlySignedAccounts),
			NumReadonlyUnsignedAccounts: uint8(input.Header.NumReadOnlyUnsignedAccounts),
		},
		RecentBlockhash: recentBlockHash,
		Instructions:    instructions,
	}
	return output, nil
}

func (p *solanaNativeParserImpl) toRichInstructions(input []SolanaInstruction) []solana.CompiledInstruction {
	output := make([]solana.CompiledInstruction, len(input))
	for i, instruction := range input {
		output[i] = solana.CompiledInstruction{
			ProgramIDIndex: instruction.ProgramIDIndex,
			Accounts:       instruction.Accounts,
			Data:           decodeBase58(instruction.Data),
		}
	}

	return output
}

func (v *SolanaTransactionVersion) UnmarshalJSON(input []byte) error {
	if len(input) == 0 {
		*v = SolanaTransactionVersion(SolanaLegacyVersion)
		return nil
	}

	if len(input) > 0 && input[0] != '"' {
		var i uint32
		if err := json.Unmarshal(input, &i); err != nil {
			return xerrors.Errorf("failed to unmarshal SolanaTransactionVersion into uint32: %w", err)
		}

		*v = SolanaTransactionVersion(i)
		return nil
	}

	var s string
	if err := json.Unmarshal(input, &s); err != nil {
		return xerrors.Errorf("failed to unmarshal SolanaTransactionVersion into string: %w", err)
	}

	if s == "" || s == SolanaLegacyVersionStr {
		*v = SolanaTransactionVersion(SolanaLegacyVersion)
		return nil
	}
	return xerrors.Errorf("unsupported transaction version: %v", s)
}

func (v SolanaTransactionVersion) Value() int32 {
	return int32(v)
}
