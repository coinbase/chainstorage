package solana

import (
	"context"
	"encoding/json"

	"github.com/gagliardetto/solana-go"
	"github.com/go-playground/validator/v10"
	"github.com/golang/protobuf/ptypes/timestamp"
	"go.uber.org/zap"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/blockchain/parser/internal"
	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/utils/log"
	"github.com/coinbase/chainstorage/protos/coinbase/c3/common"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

const (
	SolanaLegacyVersion = int32(-1)

	SolanaLegacyVersionStr = "legacy"
)

type (
	SolanaTransactionVersion int32

	solanaNativeParserImpl struct {
		logger   *zap.Logger
		config   *config.Config
		validate *validator.Validate
	}

	SolanaBlockLit struct {
		BlockHash         string                 `json:"blockhash"`
		BlockHeight       uint64                 `json:"blockHeight"`
		ParentSlot        uint64                 `json:"parentSlot"`
		PreviousBlockHash string                 `json:"previousBlockhash"`
		BlockTime         int64                  `json:"blockTime"`
		Transactions      []SolanaTransactionLit `json:"transactions"`
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

	SolanaBlockV2 struct {
		BlockHash         string                `json:"blockhash"`
		BlockHeight       uint64                `json:"blockHeight"`
		ParentSlot        uint64                `json:"parentSlot"`
		PreviousBlockHash string                `json:"previousBlockhash"`
		BlockTime         int64                 `json:"blockTime"`
		Transactions      []SolanaTransactionV2 `json:"transactions"`
		Rewards           []SolanaReward        `json:"rewards"`
	}

	SolanaTransaction struct {
		Payload *SolanaTransactionPayload `json:"transaction"`
		Meta    *SolanaTransactionMeta    `json:"meta"`
		Version *SolanaTransactionVersion `json:"version"`
	}

	SolanaTransactionV2 struct {
		Payload *SolanaTransactionPayloadV2 `json:"transaction"`
		Meta    *SolanaTransactionMetaV2    `json:"meta"`
		Version *SolanaTransactionVersion   `json:"version"`
	}

	SolanaTransactionLit struct {
		Payload *SolanaTransactionPayloadLit `json:"transaction"`
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

	SolanaTransactionMetaV2 struct {
		Err               SolanaTransactionError     `json:"err"`
		Fee               uint64                     `json:"fee"`
		PreBalances       []uint64                   `json:"preBalances"`
		PostBalances      []uint64                   `json:"postBalances"`
		PreTokenBalances  []SolanaTokenBalance       `json:"preTokenBalances"`
		PostTokenBalances []SolanaTokenBalance       `json:"postTokenBalances"`
		InnerInstructions []SolanaInnerInstructionV2 `json:"innerInstructions"`
		LogMessages       []string                   `json:"logMessages"`
		Rewards           []SolanaReward             `json:"rewards"`
		LoadedAddresses   *SolanaLoadedAddresses     `json:"loadedAddresses"`
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

	SolanaInnerInstructionV2 struct {
		Index        uint64                `json:"index"`
		Instructions []SolanaInstructionV2 `json:"instructions"`
	}

	SolanaTransactionPayload struct {
		Signatures []string      `json:"signatures"`
		Message    SolanaMessage `json:"message"`
	}

	SolanaTransactionPayloadV2 struct {
		Signatures []string        `json:"signatures"`
		Message    SolanaMessageV2 `json:"message"`
	}

	SolanaTransactionPayloadLit struct {
		Signatures []string `json:"signatures"`
	}

	SolanaMessage struct {
		Header          SolanaMessageHeader `json:"header"`
		AccountKeys     []string            `json:"accountKeys"`
		RecentBlockHash string              `json:"recentBlockhash"`
		Instructions    []SolanaInstruction `json:"instructions"`
	}

	SolanaMessageV2 struct {
		AccountKeys         []AccountKey          `json:"accountKeys"`
		AddressTableLookups []TableLookup         `json:"addressTableLookups"`
		Instructions        []SolanaInstructionV2 `json:"instructions"`
		RecentBlockHash     string                `json:"recentBlockhash"`
	}

	AccountKey struct {
		Pubkey   string `json:"pubkey"`
		Signer   bool   `json:"signer"`
		Source   string `json:"source"`
		Writable bool   `json:"writable"`
	}

	TableLookup struct {
		AccountKey      string   `json:"accountKey"`
		ReadonlyIndexes []uint64 `json:"readonlyIndexes"`
		WritableIndexes []uint64 `json:"writableIndexes"`
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

	SolanaRawInstructionV2 struct {
		Accounts  []string `json:"accounts"`
		Data      string   `json:"data"`
		ProgramId string   `json:"programId"`
	}

	SolanaInstructionV2 struct {
		Program           string          `json:"program"`
		ProgramId         string          `json:"programId"`
		ParsedInstruction json.RawMessage `json:"parsed"`
		SolanaRawInstructionV2
	}

	SolanaParsedInstruction struct {
		InstructionInfo json.RawMessage `json:"info"`
		InstructionType string          `json:"type"`
	}

	SolanaReward struct {
		Pubkey      string  `json:"pubkey"`
		Lamports    int64   `json:"lamports"`
		PostBalance uint64  `json:"postBalance"`
		RewardType  string  `json:"rewardType"`
		Commission  *uint64 `json:"commission"`
	}

	SolanaTransactionError = any
)

func NewSolanaNativeParser(params internal.ParserParams, opts ...internal.ParserFactoryOption) (internal.NativeParser, error) {
	return &solanaNativeParserImpl{
		logger:   log.WithPackage(params.Logger),
		config:   params.Config,
		validate: validator.New(),
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

	if metadata.Tag < 2 {
		return nil, internal.ErrNotImplemented
	}

	var block SolanaBlockV2
	if err := json.Unmarshal(blobdata.Header, &block); err != nil {
		return nil, xerrors.Errorf("failed to unmarshal header (metadata={%+v}: %w", metadata, err)
	}

	nativeBlock, err := p.parseBlockV2(metadata.Height, &block)
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
		Block: &api.NativeBlock_SolanaV2{
			SolanaV2: nativeBlock,
		},
	}, nil
}

func (p *solanaNativeParserImpl) GetTransaction(ctx context.Context, nativeBlock *api.NativeBlock, transactionHash string) (*api.NativeTransaction, error) {
	if nativeBlock.Blockchain != common.Blockchain_BLOCKCHAIN_SOLANA {
		return nil, xerrors.Errorf("invalid blockchain: %v", nativeBlock.Blockchain)
	}

	if nativeBlock.Skipped {
		return nil, xerrors.Errorf(
			"the slot (%v %v) is skipped: %w",
			nativeBlock.Height,
			nativeBlock.Hash,
			internal.ErrNotFound,
		)
	}

	solanaBlock := nativeBlock.GetSolana()
	if solanaBlock == nil {
		return nil, xerrors.Errorf(
			"failed to get solana block: %v %v",
			nativeBlock.Height,
			nativeBlock.Hash,
		)
	}

	for _, transaction := range solanaBlock.Transactions {
		if transactionHash == transaction.TransactionId {
			return &api.NativeTransaction{
				Blockchain:      nativeBlock.Blockchain,
				Network:         nativeBlock.Network,
				Tag:             nativeBlock.Tag,
				TransactionHash: transactionHash,
				BlockHeight:     nativeBlock.Height,
				BlockHash:       nativeBlock.Hash,
				BlockTimestamp:  nativeBlock.Timestamp,
				Transaction:     &api.NativeTransaction_Solana{Solana: transaction},
			}, nil
		}
	}

	return nil, xerrors.Errorf(
		"failed to find the transaction (%v) in the given slot (%v %v): %w",
		transactionHash,
		nativeBlock.Height,
		nativeBlock.Hash,
		internal.ErrNotFound,
	)
}

func (p *solanaNativeParserImpl) parseTimestamp(ts int64) *timestamp.Timestamp {
	return &timestamp.Timestamp{
		Seconds: ts,
	}
}

func (p *solanaNativeParserImpl) parseBlockV2(slot uint64, block *SolanaBlockV2) (*api.SolanaBlockV2, error) {
	header, err := p.parseHeaderV2(slot, block)
	if err != nil {
		return nil, xerrors.Errorf("failed to parse header: %w", err)
	}

	transactions, err := p.parseTransactionsV2(block.Transactions)
	if err != nil {
		return nil, xerrors.Errorf("failed to parse transactions: %w", err)
	}

	rewards, err := p.parseRewards(block.Rewards)
	if err != nil {
		return nil, xerrors.Errorf("failed to parse rewards: %w", err)
	}

	return &api.SolanaBlockV2{
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

func (p *solanaNativeParserImpl) parseHeaderV2(slot uint64, block *SolanaBlockV2) (*api.SolanaHeader, error) {
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

		transactionID, err := ValidateSolanaParsedTransactionId(payload.Signatures)
		if err != nil {
			return nil, err
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

func (p *solanaNativeParserImpl) parseTransactionsV2(transactions []SolanaTransactionV2) ([]*api.SolanaTransactionV2, error) {
	result := make([]*api.SolanaTransactionV2, len(transactions))
	for i, v := range transactions {
		version := p.parseTransactionVersion(v.Version)

		transactionID, err := ValidateSolanaParsedTransactionId(v.Payload.Signatures)
		if err != nil {
			return nil, err
		}

		payload, err := p.parseTransactionPayloadV2(v.Payload)
		if err != nil {
			return nil, xerrors.Errorf("failed to parse transaction payload (transactionID=%v, payload={%+v}): %w", transactionID, v.Payload, err)
		}

		meta, err := p.parseTransactionMetaV2(v.Meta)
		if err != nil {
			return nil, xerrors.Errorf("failed to parse transaction meta (transactionID=%v, meta={%+v}): %w", transactionID, v.Meta, err)
		}

		result[i] = &api.SolanaTransactionV2{
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

func (p *solanaNativeParserImpl) parseTransactionMetaV2(meta *SolanaTransactionMetaV2) (*api.SolanaTransactionMetaV2, error) {
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

	innerInstructions, err := p.parseInnerInstructionsV2(meta.InnerInstructions)
	if err != nil {
		return nil, xerrors.Errorf("failed to parse inner instructions: %w", err)
	}

	return &api.SolanaTransactionMetaV2{
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

func (p *solanaNativeParserImpl) parseInnerInstructionsV2(input []SolanaInnerInstructionV2) ([]*api.SolanaInnerInstructionV2, error) {
	output := make([]*api.SolanaInnerInstructionV2, len(input))
	for i, v := range input {
		instructions, err := p.parseInstructionsV2(v.Instructions)
		if err != nil {
			return nil, xerrors.Errorf("failed to parse instructions: %w", err)
		}

		output[i] = &api.SolanaInnerInstructionV2{
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
	case map[string]any:
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
			Pubkey:      internal.DecodeBase58(v.Pubkey),
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

func (p *solanaNativeParserImpl) parseTransactionPayloadV2(payload *SolanaTransactionPayloadV2) (*api.SolanaTransactionPayloadV2, error) {
	message, err := p.parseTransactionMessageV2(&payload.Message)
	if err != nil {
		return nil, xerrors.Errorf("failed to parse transaction message: %w", err)
	}

	return &api.SolanaTransactionPayloadV2{
		Signatures: payload.Signatures,
		Message:    message,
	}, nil
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

func (p *solanaNativeParserImpl) parseTransactionMessageV2(message *SolanaMessageV2) (*api.SolanaMessageV2, error) {
	accountKeys := make([]*api.AccountKey, len(message.AccountKeys))
	addressTableLookups := make([]*api.AddressTableLookup, len(message.AddressTableLookups))

	for i, accountKey := range message.AccountKeys {
		accountKeys[i] = &api.AccountKey{
			Pubkey:   accountKey.Pubkey,
			Signer:   accountKey.Signer,
			Source:   accountKey.Source,
			Writable: accountKey.Writable,
		}
	}

	for i, addressTableLookup := range message.AddressTableLookups {
		addressTableLookups[i] = &api.AddressTableLookup{
			AccountKey:      addressTableLookup.AccountKey,
			ReadonlyIndexes: addressTableLookup.ReadonlyIndexes,
			WritableIndexes: addressTableLookup.WritableIndexes,
		}
	}

	instructions, err := p.parseInstructionsV2(message.Instructions)
	if err != nil {
		return nil, xerrors.Errorf("failed to parse instructions: %w", err)
	}

	parsedMessage := &api.SolanaMessageV2{
		AccountKeys:         accountKeys,
		AddressTableLookups: addressTableLookups,
		Instructions:        instructions,
		RecentBlockHash:     message.RecentBlockHash,
	}

	return parsedMessage, nil
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

func (p *solanaNativeParserImpl) parseInstructionsV2(instructions []SolanaInstructionV2) ([]*api.SolanaInstructionV2, error) {
	result := make([]*api.SolanaInstructionV2, len(instructions))

	for i, instruction := range instructions {
		var err error
		var res *api.SolanaInstructionV2
		switch instruction.Program {
		case AddressLookupTableProgram:
			res, err = p.parseAddressLookupTableInstruction(instruction)
		case BpfLoaderProgram:
			res, err = p.parseBpfLoaderInstruction(instruction)
		case BpfUpgradeableLoaderProgram:
			res, err = p.parseBpfUpgradeableLoaderInstruction(instruction)
		case VoteProgram:
			res, err = p.parseVoteInstruction(instruction)
		case StakeProgram:
			res, err = p.parseStakeInstruction(instruction)
		case SystemProgram:
			res, err = p.parseSystemInstruction(instruction)
		case SplMemoProgram:
			res, err = p.parseSplMemoInstruction(instruction)
		case SplTokenProgram:
			res, err = p.parseSplTokenInstruction(instruction)
		case SplToken2022Program:
			res, err = p.parseSplToken2022Instruction(instruction)
		case SplAssociatedTokenAccountProgram:
			res, err = p.parseSplAssociatedTokenAccountInstruction(instruction)
		case UnparsedProgram:
			res, err = p.parseRawInstruction(instruction)
		default:
			return nil, xerrors.Errorf("unknown program %s", instruction.Program)
		}
		if err != nil {
			return nil, xerrors.Errorf("failed to parse instructions (program=%v): %w", instruction.Program, err)
		}

		result[i] = res
	}

	return result, nil
}

func (p *solanaNativeParserImpl) parseVoteInstruction(instruction SolanaInstructionV2) (*api.SolanaInstructionV2, error) {
	var parsedInstruction SolanaParsedInstruction
	err := json.Unmarshal(instruction.ParsedInstruction, &parsedInstruction)
	if err != nil {
		return nil, xerrors.Errorf("failed to unmarshal parsed vote instruction:%w", err)
	}

	instructionType := parsedInstruction.InstructionType
	res := &api.SolanaInstructionV2{
		Program:   api.SolanaProgram_VOTE,
		ProgramId: instruction.ProgramId,
	}

	switch instructionType {
	case VoteInitializeInstruction:
		var info VoteInitializeInstructionInfo
		err := json.Unmarshal(parsedInstruction.InstructionInfo, &info)
		if err != nil {
			return nil, xerrors.Errorf("failed to unmarshal vote initialize instruction: %w", err)
		}
		if err := p.validateStruct(info); err != nil {
			return nil, xerrors.Errorf("failed to validate vote initialize instruction: %w", err)
		}

		res.ProgramData = &api.SolanaInstructionV2_VoteProgram{
			VoteProgram: &api.SolanaVoteProgram{
				InstructionType: api.SolanaVoteProgram_INITIALIZE,
				Instruction: &api.SolanaVoteProgram_Initialize{
					Initialize: &api.SolanaVoteInitializeInstruction{
						VoteAccount:          info.VoteAccount,
						RentSysvar:           info.RentSysvar,
						ClockSysvar:          info.ClockSysvar,
						Node:                 info.Node,
						AuthorizedVoter:      info.AuthorizedVoter,
						AuthorizedWithdrawer: info.AuthorizedWithdrawer,
						Commission:           uint32(info.Commission),
					},
				},
			},
		}
	case VoteVoteInstruction:
		var info VoteVoteInstructionInfo
		err := json.Unmarshal(parsedInstruction.InstructionInfo, &info)
		if err != nil {
			return nil, xerrors.Errorf("failed to unmarshal vote vote instruction: %w", err)
		}
		if err := p.validateStruct(info); err != nil {
			return nil, xerrors.Errorf("failed to validate vote vote instruction: %w", err)
		}

		res.ProgramData = &api.SolanaInstructionV2_VoteProgram{
			VoteProgram: &api.SolanaVoteProgram{
				InstructionType: api.SolanaVoteProgram_VOTE,
				Instruction: &api.SolanaVoteProgram_Vote{
					Vote: &api.SolanaVoteVoteInstruction{
						VoteAccount:      info.VoteAccount,
						SlotHashesSysvar: info.SlotHashesSysvar,
						ClockSysvar:      info.ClockSysvar,
						VoteAuthority:    info.VoteAuthority,
						Vote: &api.SolanaVoteVoteInstruction_Vote{
							Slots:     info.Vote.Slots,
							Hash:      info.Vote.Hash,
							Timestamp: p.parseTimestamp(info.Vote.Timestamp),
						},
					},
				},
			},
		}
	case VoteWithdrawInstruction:
		var info VoteWithdrawInstructionInfo
		err := json.Unmarshal(parsedInstruction.InstructionInfo, &info)
		if err != nil {
			return nil, xerrors.Errorf("failed to unmarshal vote withdraw instruction: %w", err)
		}
		if err := p.validateStruct(info); err != nil {
			return nil, xerrors.Errorf("failed to validate vote withdraw instruction: %w", err)
		}

		res.ProgramData = &api.SolanaInstructionV2_VoteProgram{
			VoteProgram: &api.SolanaVoteProgram{
				InstructionType: api.SolanaVoteProgram_WITHDRAW,
				Instruction: &api.SolanaVoteProgram_Withdraw{
					Withdraw: &api.SolanaVoteWithdrawInstruction{
						VoteAccount:       info.VoteAccount,
						Destination:       info.Destination,
						WithdrawAuthority: info.WithdrawAuthority,
						Lamports:          info.Lamports,
					},
				},
			},
		}
	case VoteCompactUpdateVoteState:
		var info VoteCompactUpdateVoteStateInstructionInfo
		err := json.Unmarshal(parsedInstruction.InstructionInfo, &info)
		if err != nil {
			return nil, xerrors.Errorf("failed to unmarshal vote compact update vote state instruction: %w", err)
		}
		if err := p.validateStruct(info); err != nil {
			return nil, xerrors.Errorf("failed to validate vote compact update vote state instruction: %w", err)
		}

		lockouts := make([]*api.SolanaVoteCompactUpdateVoteStateInstruction_Lockout, len(info.VoteStateUpdate.Lockouts))
		for i, lockout := range info.VoteStateUpdate.Lockouts {
			lockouts[i] = &api.SolanaVoteCompactUpdateVoteStateInstruction_Lockout{
				ConfirmationCount: lockout.ConfirmationCount,
				Slot:              lockout.Slot,
			}
		}

		res.ProgramData = &api.SolanaInstructionV2_VoteProgram{
			VoteProgram: &api.SolanaVoteProgram{
				InstructionType: api.SolanaVoteProgram_COMPACT_UPDATE_VOTE_STATE,
				Instruction: &api.SolanaVoteProgram_CompactUpdateVoteState{
					CompactUpdateVoteState: &api.SolanaVoteCompactUpdateVoteStateInstruction{
						VoteAccount:   info.VoteAccount,
						VoteAuthority: info.VoteAuthority,
						VoteStateUpdate: &api.SolanaVoteCompactUpdateVoteStateInstruction_VoteStateUpdate{
							Hash:      info.VoteStateUpdate.Hash,
							Lockouts:  lockouts,
							Root:      info.VoteStateUpdate.Root,
							Timestamp: p.parseTimestamp(info.VoteStateUpdate.Timestamp),
						},
					},
				},
			},
		}
	default:
		res.ProgramData = &api.SolanaInstructionV2_VoteProgram{
			VoteProgram: &api.SolanaVoteProgram{
				InstructionType: api.SolanaVoteProgram_UNKNOWN,
				Instruction: &api.SolanaVoteProgram_Unknown{
					Unknown: &api.SolanaUnknownInstruction{
						Info: parsedInstruction.InstructionInfo,
					},
				},
			},
		}
	}

	return res, nil
}

func (p *solanaNativeParserImpl) parseSystemInstruction(instruction SolanaInstructionV2) (*api.SolanaInstructionV2, error) {
	var parsedInstruction SolanaParsedInstruction
	err := json.Unmarshal(instruction.ParsedInstruction, &parsedInstruction)
	if err != nil {
		return nil, xerrors.Errorf("failed to unmarshal parsed system instruction:%w", err)
	}

	instructionType := parsedInstruction.InstructionType
	res := &api.SolanaInstructionV2{
		Program:   api.SolanaProgram_SYSTEM,
		ProgramId: instruction.ProgramId,
	}

	switch instructionType {
	case SystemCreateAccountInstruction:
		var info SystemCreateAccountInstructionInfo
		err := json.Unmarshal(parsedInstruction.InstructionInfo, &info)
		if err != nil {
			return nil, xerrors.Errorf("failed to unmarshal system createAccount instruction: %w", err)
		}
		if err := p.validateStruct(info); err != nil {
			return nil, xerrors.Errorf("failed to validate system createAccount instruction: %w", err)
		}

		res.ProgramData = &api.SolanaInstructionV2_SystemProgram{
			SystemProgram: &api.SolanaSystemProgram{
				InstructionType: api.SolanaSystemProgram_CREATE_ACCOUNT,
				Instruction: &api.SolanaSystemProgram_CreateAccount{
					CreateAccount: &api.SolanaSystemCreateAccountInstruction{
						Source:     info.Source,
						NewAccount: info.NewAccount,
						Lamports:   info.Lamports,
						Space:      info.Space,
						Owner:      info.Owner,
					},
				},
			},
		}
	case SystemTransferInstruction:
		var info SystemTransferInstructionInfo
		err := json.Unmarshal(parsedInstruction.InstructionInfo, &info)
		if err != nil {
			return nil, xerrors.Errorf("failed to unmarshal system transfer instruction: %w", err)
		}
		if err := p.validateStruct(info); err != nil {
			return nil, xerrors.Errorf("failed to validate system transfer instruction: %w", err)
		}

		res.ProgramData = &api.SolanaInstructionV2_SystemProgram{
			SystemProgram: &api.SolanaSystemProgram{
				InstructionType: api.SolanaSystemProgram_TRANSFER,
				Instruction: &api.SolanaSystemProgram_Transfer{
					Transfer: &api.SolanaSystemTransferInstruction{
						Source:      info.Source,
						Destination: info.Destination,
						Lamports:    info.Lamports,
					},
				},
			},
		}
	case SystemCreateAccountWithSeedInstruction:
		var info SystemCreateAccountWithSeedInstructionInfo
		err := json.Unmarshal(parsedInstruction.InstructionInfo, &info)
		if err != nil {
			return nil, xerrors.Errorf("failed to unmarshal system createAccountWithSeed instruction: %w", err)
		}
		if err := p.validateStruct(info); err != nil {
			return nil, xerrors.Errorf("failed to validate system createAccountWithSeed instruction: %w", err)
		}

		res.ProgramData = &api.SolanaInstructionV2_SystemProgram{
			SystemProgram: &api.SolanaSystemProgram{
				InstructionType: api.SolanaSystemProgram_CREATE_ACCOUNT_WITH_SEED,
				Instruction: &api.SolanaSystemProgram_CreateAccountWithSeed{
					CreateAccountWithSeed: &api.SolanaSystemCreateAccountWithSeedInstruction{
						Source:     info.Source,
						NewAccount: info.NewAccount,
						Base:       info.Base,
						Seed:       info.Seed,
						Lamports:   info.Lamports,
						Space:      info.Space,
						Owner:      info.Owner,
					},
				},
			},
		}
	case SystemTransferWithSeedInstruction:
		var info SystemTransferWithSeedInstructionInfo
		err := json.Unmarshal(parsedInstruction.InstructionInfo, &info)
		if err != nil {
			return nil, xerrors.Errorf("failed to unmarshal system transferWithSeed instruction: %w", err)
		}
		if err := p.validateStruct(info); err != nil {
			return nil, xerrors.Errorf("failed to validate system transferWithSeed instruction: %w", err)
		}

		res.ProgramData = &api.SolanaInstructionV2_SystemProgram{
			SystemProgram: &api.SolanaSystemProgram{
				InstructionType: api.SolanaSystemProgram_TRANSFER_WITH_SEED,
				Instruction: &api.SolanaSystemProgram_TransferWithSeed{
					TransferWithSeed: &api.SolanaSystemTransferWithSeedInstruction{
						Source:      info.Source,
						SourceBase:  info.SourceBase,
						Destination: info.Destination,
						Lamports:    info.Lamports,
						SourceSeed:  info.SourceSeed,
						SourceOwner: info.SourceOwner,
					},
				},
			},
		}
	default:
		res.ProgramData = &api.SolanaInstructionV2_SystemProgram{
			SystemProgram: &api.SolanaSystemProgram{
				InstructionType: api.SolanaSystemProgram_UNKNOWN,
				Instruction: &api.SolanaSystemProgram_Unknown{
					Unknown: &api.SolanaUnknownInstruction{
						Info: parsedInstruction.InstructionInfo,
					},
				},
			},
		}
	}

	return res, nil
}

func (p *solanaNativeParserImpl) parseStakeInstruction(instruction SolanaInstructionV2) (*api.SolanaInstructionV2, error) {
	var parsedInstruction SolanaParsedInstruction
	err := json.Unmarshal(instruction.ParsedInstruction, &parsedInstruction)
	if err != nil {
		return nil, xerrors.Errorf("failed to unmarshal parsed stake instruction:%w", err)
	}

	instructionType := parsedInstruction.InstructionType
	res := &api.SolanaInstructionV2{
		Program:   api.SolanaProgram_STAKE,
		ProgramId: instruction.ProgramId,
	}

	switch instructionType {
	case StakeInitializeInstruction:
		var info StakeInitializeInstructionInfo
		err := json.Unmarshal(parsedInstruction.InstructionInfo, &info)
		if err != nil {
			return nil, xerrors.Errorf("failed to unmarshal stake initialize instruction: %w", err)
		}
		if err := p.validateStruct(info); err != nil {
			return nil, xerrors.Errorf("failed to validate stake initialize instruction: %w", err)
		}

		res.ProgramData = &api.SolanaInstructionV2_StakeProgram{
			StakeProgram: &api.SolanaStakeProgram{
				InstructionType: api.SolanaStakeProgram_INITIALIZE,
				Instruction: &api.SolanaStakeProgram_Initialize{
					Initialize: &api.SolanaStakeInitializeInstruction{
						StakeAccount: info.StakeAccount,
						RentSysvar:   info.RentSysvar,
						Authorized: &api.SolanaStakeInitializeInstruction_Authorized{
							Staker:     info.Authorized.Staker,
							Withdrawer: info.Authorized.Withdrawer,
						},
						Lockup: &api.SolanaStakeInitializeInstruction_Lockup{
							UnixTimestamp: info.Lockup.UnixTimestamp,
							Epoch:         info.Lockup.Epoch,
							Custodian:     info.Lockup.Custodian,
						},
					},
				},
			},
		}
	case StakeDelegateInstruction:
		var info StakeDelegateInstructionInfo
		err := json.Unmarshal(parsedInstruction.InstructionInfo, &info)
		if err != nil {
			return nil, xerrors.Errorf("failed to unmarshal stake delegate instruction: %w", err)
		}
		if err := p.validateStruct(info); err != nil {
			return nil, xerrors.Errorf("failed to validate stake delegate instruction: %w", err)
		}

		res.ProgramData = &api.SolanaInstructionV2_StakeProgram{
			StakeProgram: &api.SolanaStakeProgram{
				InstructionType: api.SolanaStakeProgram_DELEGATE,
				Instruction: &api.SolanaStakeProgram_Delegate{
					Delegate: &api.SolanaStakeDelegateInstruction{
						StakeAccount:       info.StakeAccount,
						VoteAccount:        info.VoteAccount,
						ClockSysvar:        info.ClockSysvar,
						StakeHistorySysvar: info.StakeHistorySysvar,
						StakeConfigAccount: info.StakeConfigAccount,
						StakeAuthority:     info.StakeAuthority,
					},
				},
			},
		}
	case StakeDeactivateInstruction:
		var info StakeDeactivateInstructionInfo
		err := json.Unmarshal(parsedInstruction.InstructionInfo, &info)
		if err != nil {
			return nil, xerrors.Errorf("failed to unmarshal stake deactivate instruction: %w", err)
		}
		if err := p.validateStruct(info); err != nil {
			return nil, xerrors.Errorf("failed to validate stake deactivate instruction: %w", err)
		}

		res.ProgramData = &api.SolanaInstructionV2_StakeProgram{
			StakeProgram: &api.SolanaStakeProgram{
				InstructionType: api.SolanaStakeProgram_DEACTIVATE,
				Instruction: &api.SolanaStakeProgram_Deactivate{
					Deactivate: &api.SolanaStakeDeactivateInstruction{
						StakeAccount:   info.StakeAccount,
						ClockSysvar:    info.ClockSysvar,
						StakeAuthority: info.StakeAuthority,
					},
				},
			},
		}
	case StakeMergeInstruction:
		var info StakeMergeInstructionInfo
		err := json.Unmarshal(parsedInstruction.InstructionInfo, &info)
		if err != nil {
			return nil, xerrors.Errorf("failed to unmarshal stake merge instruction: %w", err)
		}
		if err := p.validateStruct(info); err != nil {
			return nil, xerrors.Errorf("failed to validate stake merge instruction: %w", err)
		}

		res.ProgramData = &api.SolanaInstructionV2_StakeProgram{
			StakeProgram: &api.SolanaStakeProgram{
				InstructionType: api.SolanaStakeProgram_MERGE,
				Instruction: &api.SolanaStakeProgram_Merge{
					Merge: &api.SolanaStakeMergeInstruction{
						Destination:        info.Destination,
						Source:             info.Source,
						ClockSysvar:        info.ClockSysvar,
						StakeHistorySysvar: info.StakeHistorySysvar,
						StakeAuthority:     info.StakeAuthority,
					},
				},
			},
		}
	case StakeSplitInstruction:
		var info StakeSplitInstructionInfo
		err := json.Unmarshal(parsedInstruction.InstructionInfo, &info)
		if err != nil {
			return nil, xerrors.Errorf("failed to unmarshal stake split instruction: %w", err)
		}
		if err := p.validateStruct(info); err != nil {
			return nil, xerrors.Errorf("failed to validate stake split instruction: %w", err)
		}

		res.ProgramData = &api.SolanaInstructionV2_StakeProgram{
			StakeProgram: &api.SolanaStakeProgram{
				InstructionType: api.SolanaStakeProgram_SPLIT,
				Instruction: &api.SolanaStakeProgram_Split{
					Split: &api.SolanaStakeSplitInstruction{
						StakeAccount:    info.StakeAccount,
						NewSplitAccount: info.NewSplitAccount,
						StakeAuthority:  info.StakeAuthority,
						Lamports:        info.Lamports,
					},
				},
			},
		}
	case StakeWithdrawInstruction:
		var info StakeWithdrawInstructionInfo
		err := json.Unmarshal(parsedInstruction.InstructionInfo, &info)
		if err != nil {
			return nil, xerrors.Errorf("failed to unmarshal stake withdraw instruction: %w", err)
		}
		if err := p.validateStruct(info); err != nil {
			return nil, xerrors.Errorf("failed to validate stake withdraw instruction: %w", err)
		}

		res.ProgramData = &api.SolanaInstructionV2_StakeProgram{
			StakeProgram: &api.SolanaStakeProgram{
				InstructionType: api.SolanaStakeProgram_WITHDRAW,
				Instruction: &api.SolanaStakeProgram_Withdraw{
					Withdraw: &api.SolanaStakeWithdrawInstruction{
						StakeAccount:       info.StakeAccount,
						Destination:        info.Destination,
						ClockSysvar:        info.ClockSysvar,
						StakeHistorySysvar: info.StakeHistorySysvar,
						WithdrawAuthority:  info.WithdrawAuthority,
						Lamports:           info.Lamports,
					},
				},
			},
		}
	default:
		res.ProgramData = &api.SolanaInstructionV2_StakeProgram{
			StakeProgram: &api.SolanaStakeProgram{
				InstructionType: api.SolanaStakeProgram_UNKNOWN,
				Instruction: &api.SolanaStakeProgram_Unknown{
					Unknown: &api.SolanaUnknownInstruction{
						Info: parsedInstruction.InstructionInfo,
					},
				},
			},
		}
	}

	return res, nil
}

func (p *solanaNativeParserImpl) parseSplMemoInstruction(instruction SolanaInstructionV2) (*api.SolanaInstructionV2, error) {
	var memo string
	err := json.Unmarshal(instruction.ParsedInstruction, &memo)
	if err != nil {
		return nil, xerrors.Errorf("failed to unmarshal parsed spl-memo instruction:%w", err)
	}

	res := &api.SolanaInstructionV2{
		Program:   api.SolanaProgram_SPL_MEMO,
		ProgramId: instruction.ProgramId,
		ProgramData: &api.SolanaInstructionV2_SplMemoProgram{
			SplMemoProgram: &api.SolanaSplMemoProgram{
				InstructionType: api.SolanaSplMemoProgram_SPL_MEMO,
				Instruction: &api.SolanaSplMemoProgram_Memo{
					Memo: &api.SolanaSplMemoInstruction{
						Memo: memo,
					},
				},
			},
		},
	}

	return res, nil
}

func (p *solanaNativeParserImpl) parseSplTokenInstruction(instruction SolanaInstructionV2) (*api.SolanaInstructionV2, error) {
	var parsedInstruction SolanaParsedInstruction
	err := json.Unmarshal(instruction.ParsedInstruction, &parsedInstruction)
	if err != nil {
		return nil, xerrors.Errorf("failed to unmarshal parsed spl-token instruction:%w", err)
	}

	instructionType := parsedInstruction.InstructionType
	res := &api.SolanaInstructionV2{
		Program:   api.SolanaProgram_SPL_TOKEN,
		ProgramId: instruction.ProgramId,
	}

	switch instructionType {
	case SplTokenGetAccountDataSizeInstruction:
		var info SplTokenGetAccountDataSizeInstructionInfo
		err := json.Unmarshal(parsedInstruction.InstructionInfo, &info)
		if err != nil {
			return nil, xerrors.Errorf("failed to unmarshal token getAccountDataSize instruction: %w", err)
		}
		if err := p.validateStruct(info); err != nil {
			return nil, xerrors.Errorf("failed to validate token getAccountDataSize instruction: %w", err)
		}

		res.ProgramData = &api.SolanaInstructionV2_SplTokenProgram{
			SplTokenProgram: &api.SolanaSplTokenProgram{
				InstructionType: api.SolanaSplTokenProgram_GET_ACCOUNT_DATA_SIZE,
				Instruction: &api.SolanaSplTokenProgram_GetAccountDataSize{
					GetAccountDataSize: &api.SolanaSplTokenGetAccountDataSizeInstruction{
						Mint:           info.Mint,
						ExtensionTypes: info.ExtensionTypes,
					},
				},
			},
		}
	case SplTokenInitializeImmutableOwnerInstruction:
		var info SplTokenInitializeImmutableOwnerInstructionInfo
		err := json.Unmarshal(parsedInstruction.InstructionInfo, &info)
		if err != nil {
			return nil, xerrors.Errorf("failed to unmarshal token initializeImmutableOwner instruction: %w", err)
		}
		if err := p.validateStruct(info); err != nil {
			return nil, xerrors.Errorf("failed to validate token initializeImmutableOwner instruction: %w", err)
		}

		res.ProgramData = &api.SolanaInstructionV2_SplTokenProgram{
			SplTokenProgram: &api.SolanaSplTokenProgram{
				InstructionType: api.SolanaSplTokenProgram_INITIALIZE_IMMUTABLE_OWNER,
				Instruction: &api.SolanaSplTokenProgram_InitializeImmutableOwner{
					InitializeImmutableOwner: &api.SolanaSplTokenInitializeImmutableOwnerInstruction{
						Account: info.Account,
					},
				},
			},
		}
	case SplTokenTransferInstruction:
		var info SplTokenTransferInstructionInfo
		err := json.Unmarshal(parsedInstruction.InstructionInfo, &info)
		if err != nil {
			return nil, xerrors.Errorf("failed to unmarshal token transfer instruction: %w", err)
		}
		if err := p.validateStruct(info); err != nil {
			return nil, xerrors.Errorf("failed to validate token transfer instruction: %w", err)
		}

		res.ProgramData = &api.SolanaInstructionV2_SplTokenProgram{
			SplTokenProgram: &api.SolanaSplTokenProgram{
				InstructionType: api.SolanaSplTokenProgram_TRANSFER,
				Instruction: &api.SolanaSplTokenProgram_Transfer{
					Transfer: &api.SolanaSplTokenTransferInstruction{
						Source:      info.Source,
						Destination: info.Destination,
						Authority:   info.Authority,
						Amount:      info.Amount,
					},
				},
			},
		}
	default:
		res.ProgramData = &api.SolanaInstructionV2_SplTokenProgram{
			SplTokenProgram: &api.SolanaSplTokenProgram{
				InstructionType: api.SolanaSplTokenProgram_UNKNOWN,
				Instruction: &api.SolanaSplTokenProgram_Unknown{
					Unknown: &api.SolanaUnknownInstruction{
						Info: parsedInstruction.InstructionInfo,
					},
				},
			},
		}
	}

	return res, nil
}

func (p *solanaNativeParserImpl) parseSplToken2022Instruction(instruction SolanaInstructionV2) (*api.SolanaInstructionV2, error) {
	var parsedInstruction SolanaParsedInstruction
	err := json.Unmarshal(instruction.ParsedInstruction, &parsedInstruction)
	if err != nil {
		return nil, xerrors.Errorf("failed to unmarshal parsed spl-token-2022 instruction:%w", err)
	}

	instructionType := parsedInstruction.InstructionType
	res := &api.SolanaInstructionV2{
		Program:   api.SolanaProgram_SPL_TOKEN_2022,
		ProgramId: instruction.ProgramId,
	}

	switch instructionType {
	default:
		res.ProgramData = &api.SolanaInstructionV2_SplToken_2022Program{
			SplToken_2022Program: &api.SolanaSplToken2022Program{
				InstructionType: api.SolanaSplToken2022Program_UNKNOWN,
				Instruction: &api.SolanaSplToken2022Program_Unknown{
					Unknown: &api.SolanaUnknownInstruction{
						Info: parsedInstruction.InstructionInfo,
					},
				},
			},
		}
	}

	return res, nil
}

func (p *solanaNativeParserImpl) parseSplAssociatedTokenAccountInstruction(instruction SolanaInstructionV2) (*api.SolanaInstructionV2, error) {
	var parsedInstruction SolanaParsedInstruction
	err := json.Unmarshal(instruction.ParsedInstruction, &parsedInstruction)
	if err != nil {
		return nil, xerrors.Errorf("failed to unmarshal parsed spl-associated-token-account instruction:%w", err)
	}

	instructionType := parsedInstruction.InstructionType
	res := &api.SolanaInstructionV2{
		Program:   api.SolanaProgram_SPL_ASSOCIATED_TOKEN_ACCOUNT,
		ProgramId: instruction.ProgramId,
	}

	switch instructionType {
	default:
		res.ProgramData = &api.SolanaInstructionV2_SplAssociatedTokenAccountProgram{
			SplAssociatedTokenAccountProgram: &api.SolanaSplAssociatedTokenAccountProgram{
				InstructionType: api.SolanaSplAssociatedTokenAccountProgram_UNKNOWN,
				Instruction: &api.SolanaSplAssociatedTokenAccountProgram_Unknown{
					Unknown: &api.SolanaUnknownInstruction{
						Info: parsedInstruction.InstructionInfo,
					},
				},
			},
		}
	}

	return res, nil
}

func (p *solanaNativeParserImpl) parseAddressLookupTableInstruction(instruction SolanaInstructionV2) (*api.SolanaInstructionV2, error) {
	var parsedInstruction SolanaParsedInstruction
	err := json.Unmarshal(instruction.ParsedInstruction, &parsedInstruction)
	if err != nil {
		return nil, xerrors.Errorf("failed to unmarshal parsed address-lookup-table instruction:%w", err)
	}

	instructionType := parsedInstruction.InstructionType
	res := &api.SolanaInstructionV2{
		Program:   api.SolanaProgram_ADDRESS_LOOKUP_TABLE,
		ProgramId: instruction.ProgramId,
	}

	switch instructionType {
	default:
		res.ProgramData = &api.SolanaInstructionV2_AddressLookupTableProgram{
			AddressLookupTableProgram: &api.SolanaAddressLookupTableProgram{
				InstructionType: api.SolanaAddressLookupTableProgram_UNKNOWN,
				Instruction: &api.SolanaAddressLookupTableProgram_Unknown{
					Unknown: &api.SolanaUnknownInstruction{
						Info: parsedInstruction.InstructionInfo,
					},
				},
			},
		}
	}

	return res, nil
}

func (p *solanaNativeParserImpl) parseBpfLoaderInstruction(instruction SolanaInstructionV2) (*api.SolanaInstructionV2, error) {
	var parsedInstruction SolanaParsedInstruction
	err := json.Unmarshal(instruction.ParsedInstruction, &parsedInstruction)
	if err != nil {
		return nil, xerrors.Errorf("failed to unmarshal parsed bpf-loader instruction:%w", err)
	}

	instructionType := parsedInstruction.InstructionType
	res := &api.SolanaInstructionV2{
		Program:   api.SolanaProgram_BPF_Loader,
		ProgramId: instruction.ProgramId,
	}

	switch instructionType {
	default:
		res.ProgramData = &api.SolanaInstructionV2_BpfLoaderProgram{
			BpfLoaderProgram: &api.SolanaBpfLoaderProgram{
				InstructionType: api.SolanaBpfLoaderProgram_UNKNOWN,
				Instruction: &api.SolanaBpfLoaderProgram_Unknown{
					Unknown: &api.SolanaUnknownInstruction{
						Info: parsedInstruction.InstructionInfo,
					},
				},
			},
		}
	}

	return res, nil
}

func (p *solanaNativeParserImpl) parseBpfUpgradeableLoaderInstruction(instruction SolanaInstructionV2) (*api.SolanaInstructionV2, error) {
	var parsedInstruction SolanaParsedInstruction
	err := json.Unmarshal(instruction.ParsedInstruction, &parsedInstruction)
	if err != nil {
		return nil, xerrors.Errorf("failed to unmarshal parsed bpf-upgradeable-loader instruction:%w", err)
	}

	instructionType := parsedInstruction.InstructionType
	res := &api.SolanaInstructionV2{
		Program:   api.SolanaProgram_BPF_UPGRADEABLE_Loader,
		ProgramId: instruction.ProgramId,
	}

	switch instructionType {
	default:
		res.ProgramData = &api.SolanaInstructionV2_BpfUpgradeableLoaderProgram{
			BpfUpgradeableLoaderProgram: &api.SolanaBpfUpgradeableLoaderProgram{
				InstructionType: api.SolanaBpfUpgradeableLoaderProgram_UNKNOWN,
				Instruction: &api.SolanaBpfUpgradeableLoaderProgram_Unknown{
					Unknown: &api.SolanaUnknownInstruction{
						Info: parsedInstruction.InstructionInfo,
					},
				},
			},
		}
	}

	return res, nil
}

func (p *solanaNativeParserImpl) parseRawInstruction(instruction SolanaInstructionV2) (*api.SolanaInstructionV2, error) {
	return &api.SolanaInstructionV2{
		Program:   api.SolanaProgram_RAW,
		ProgramId: instruction.ProgramId,
		ProgramData: &api.SolanaInstructionV2_RawInstruction{
			RawInstruction: &api.SolanaRawInstruction{
				Accounts: instruction.Accounts,
				Data:     internal.DecodeBase58(instruction.Data),
			},
		},
	}, nil
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
			Data:           internal.DecodeBase58(instruction.Data),
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

func (b *solanaNativeParserImpl) validateStruct(s any) error {
	if err := b.validate.Struct(s); err != nil {
		// Errors returned by validator may be very long and get dropped by datadog.
		return internal.NewTruncatedError(err)
	}

	return nil
}

func ValidateSolanaParsedTransactionId(signatureList []string) (string, error) {
	// transactionID is the first signature in a transaction, which can be used to uniquely identify the transaction across the complete ledger.
	if len(signatureList) == 0 {
		return "", xerrors.New("signatures are empty")
	}

	transactionID := signatureList[0]
	if transactionID == "" {
		return "", xerrors.New("transaction id is empty")
	}

	return transactionID, nil
}
