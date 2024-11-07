package aptos

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/go-playground/validator/v10"
	"github.com/golang/protobuf/ptypes/timestamp"
	"go.uber.org/zap"

	"github.com/coinbase/chainstorage/internal/blockchain/parser/internal"
	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/utils/log"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

const (
	// Four types of transactions.
	typeBlockMetadataTransaction   = "block_metadata_transaction"
	typeStateCheckpointTransaction = "state_checkpoint_transaction"
	typeGenesisTransaction         = "genesis_transaction"
	typeUserTransaction            = "user_transaction"
	typeValidatorTransaction       = "validator_transaction"

	// Six types of write set changes.
	typeDeleteModuleChange    = "delete_module"
	typeDeleteResourceChange  = "delete_resource"
	typeDeleteTableItemChange = "delete_table_item"
	typeWriteModuleChange     = "write_module"
	typeWriteResourceChange   = "write_resource"
	typeWriteTableItemChange  = "write_table_item"

	// Five types of transaction payloads.
	typeEntryFunctionPayload = "entry_function_payload"
	typeScriptPayload        = "script_payload"
	typeModuleBundlePayload  = "module_bundle_payload"
	typeMultisigPayload      = "multisig_payload"
	typeWriteSetPayload      = "write_set_payload"

	// Two types of write sets.
	typeScriptWriteSet = "script_write_set"
	typeDirectWriteSet = "direct_write_set"

	typeEd25519Signature      = "ed25519_signature"
	typeMultiEd25519Signature = "multi_ed25519_signature"
	typeMultiAgentSignature   = "multi_agent_signature"
	typeFeePayerSignature     = "fee_payer_signature"
	typeSingleSenderSignature = "single_sender"

	// Account signature types:
	// https://github.com/aptos-labs/aptos-core/blob/c2b348206dea3949a8c0098b2365ab3d7867217e/api/doc/spec.yaml#L10441-L10462
	typeSingleKeySignature = "single_key_signature"
	typeMultiKeySignature  = "multi_key_signature"
)

type (
	AptosQuantity uint64

	aptosNativeParserImpl struct {
		logger   *zap.Logger
		validate *validator.Validate
		config   *config.Config
	}

	// An example:
	// "block_height": "100",
	// "block_hash": "0xa01474f8b0a1d9341ace5dfa74d949df19819d831964161fb73794345c340c6c",
	// "block_timestamp": "1665609849500427",
	// "first_version": "198",
	// "last_version": "199",
	AptosBlockLit struct {
		BlockHeight  string `json:"block_height"` // A string containing a 64-bit unsigned integer
		BlockHash    string `json:"block_hash" validate:"required"`
		BlockTime    string `json:"block_timestamp" validate:"required"` // A string containing a 64-bit unsigned integer
		FirstVersion string `json:"first_version" validate:"required"`   // A string containing a 64-bit unsigned integer
		LastVersion  string `json:"last_version" validate:"required"`    // A string containing a 64-bit unsigned integer
	}

	// The successful response of getting ledger info.
	// An example:
	// "chain_id": 1,
	// "epoch": "1925",
	// "ledger_version": "105687020",
	// "oldest_ledger_version": "0",
	// "ledger_timestamp": "1679445180598692",
	// "node_role": "full_node",
	// "oldest_block_height": "0",
	// "block_height": "40846989",
	// "git_hash": "cc30c46ad41cd1577935466036eb1903b7cbc973"
	AptosLedgerInfoLit struct {
		ChainID             int    `json:"chain_id"`
		Epoch               string `json:"epoch"`                 // A string containing a 64-bit unsigned integer
		OldestLedgerVersion string `json:"oldest_ledger_version"` // A string containing a 64-bit unsigned integer
		LedgerTimestamp     string `json:"ledger_timestamp"`      // A string containing a 64-bit unsigned integer
		NodeRole            string `json:"node_role"`
		OldestBlockHeight   string `json:"oldest_block_height"` // A string containing a 64-bit unsigned integer
		LatestBlockHeight   string `json:"block_height"`        // A string containing a 64-bit unsigned integer
		GitHash             string `json:"git_hash,omitempty"`
	}

	// A block includes a small header and array of transactions.
	AptosBlock struct {
		BlockHeight  AptosQuantity     `json:"block_height"`
		BlockHash    string            `json:"block_hash" validate:"required"`
		BlockTime    AptosQuantity     `json:"block_timestamp"`
		FirstVersion AptosQuantity     `json:"first_version"`
		LastVersion  AptosQuantity     `json:"last_version"`
		Transactions []json.RawMessage `json:"transactions"`
	}

	// The shared transaction header info. Each transaction has this shared information.
	AptosTransactionInfo struct {
		// The transaction type
		Type string `json:"type"`
		// The ledger version of the transaction.
		Version AptosQuantity `json:"version"`
		// The hash of this transaction.
		TransactionHash string `json:"hash"`
		// The root hash of Merkle Accumulator storing all events emitted during this transaction.
		EventRootHash string `json:"event_root_hash"`
		// The hash value summarizing all changes caused to the world state by this transaction.
		// i.e. hash of the output write set.
		StateChangeHash string `json:"state_change_hash"`
		// The root hash of the Sparse Merkle Tree describing the world state at the end of this
		// transaction. Depending on the protocol configuration, this can be generated periodical
		// only, like per block.
		StateCheckpointHash string `json:"state_checkpoint_hash,omitempty"`
		// The amount of gas used.
		GasUsed AptosQuantity `json:"gas_used"`
		// Whether the transaction was successful
		Success bool `json:"success"`
		// The vm status. If it is not `Executed`, this will provide the general error class. Execution
		// failures and Move abort's receive more detailed information. But other errors are generally
		// categorized with no status code or other information.
		VmStatus string `json:"vm_status"`
		// The accumulator root hash at this version.
		AccumulatorRootHash string `json:"accumulator_root_hash"`
		// Final state of resources changed by the transaction.
		Changes []json.RawMessage `json:"changes"`
	}

	// There are multiple structures having multiple types. We need to first parse its type, then we know what other
	// fields look like. This generic type is used to parse the specific "type" field.
	GenericType struct {
		// The transaction type
		Type string `json:"type"`
	}

	// There are 6 types of WriteSetChange, as defined below and in protos:
	// DeleteModuleChange, DeleteResourceChange, DeleteTableItemChange, WriteModuleChange, WriteResourceChange, and WriteTableItemChange.

	// Delete a module
	DeleteModuleChange struct {
		Address      string `json:"address"`
		StateKeyHash string `json:"state_key_hash"`
		Module       string `json:"module"`
	}

	// Delete a resource
	DeleteResourceChange struct {
		Address      string `json:"address"`
		StateKeyHash string `json:"state_key_hash"`
		Resource     string `json:"resource"`
	}

	// Delete a table item
	DeleteTableItemChange struct {
		Handle       string              `json:"handle"`
		StateKeyHash string              `json:"state_key_hash"`
		Key          string              `json:"key"`
		Data         DeleteTableItemData `json:"data"`
	}

	DeleteTableItemData struct {
		Key     string `json:"key"`
		KeyType string `json:"key_type"`
	}

	// Write a new module or update an existing one
	WriteModuleChange struct {
		Address      string             `json:"address"`
		StateKeyHash string             `json:"state_key_hash"`
		Data         MoveModuleBytecode `json:"data"`
	}

	MoveModule struct {
		Address string `json:"address"`
		Name    string `json:"name"`
		// Friends of the module.
		// Need to further parse to AptosMoveModuleId.
		Friends []string `json:"friends"`
		// Public functions of the module
		ExposedFunctions []MoveFunction `json:"exposed_functions"`
		// Structs of the module
		Structs []MoveStruct `json:"structs"`
	}

	MoveFunction struct {
		Name       string `json:"name"`
		Visibility string `json:"visibility"`
		// Whether the function can be called as an entry function directly in a transaction
		IsEntry bool `json:"is_entry"`
		// Generic type params associated with the Move function
		GenericTypePramas []MoveFunctionGenericTypeParam `json:"generic_type_params"`
		// Parameters associated with the move function
		Params []string `json:"params"`
		// Return type of the function
		Return []string `json:"return"`
	}

	MoveFunctionGenericTypeParam struct {
		Constraints []string `json:"constraints"`
	}

	MoveStruct struct {
		Name string `json:"name"`
		// Whether the struct is a native struct of Move
		IsNative  bool     `json:"is_native"`
		Abilities []string `json:"abilities"`
		// Generic types associated with the struct
		GenericTypePramas []MoveStructGenericTypeParam `json:"generic_type_params"`
		Fields            []MoveStructField            `json:"fields"`
	}

	MoveStructGenericTypeParam struct {
		Constraints []string `json:"constraints"`
	}

	MoveStructField struct {
		Name string `json:"name"`
		Type string `json:"type"`
	}

	// Write a resource or update an existing one
	WriteResourceChange struct {
		Address      string                  `json:"address"`
		StateKeyHash string                  `json:"state_key_hash"`
		Data         WriteResourceChangeData `json:"data"`
	}

	WriteResourceChangeData struct {
		Type string          `json:"type"`
		Data json.RawMessage `json:"data"`
	}

	// Change set to write a table item
	WriteTableItemChange struct {
		Handle       string                   `json:"handle"`
		StateKeyHash string                   `json:"state_key_hash"`
		Key          string                   `json:"key"`
		Value        string                   `json:"value"`
		Data         WriteTableItemChangeData `json:"data"`
	}

	WriteTableItemChangeData struct {
		Key       string `json:"key"`
		KeyType   string `json:"key_type"`
		Value     string `json:"value"`
		ValueType string `json:"value_type"`
	}

	ValidatorTransaction struct {
		Events    []Event       `json:"events"`
		TimeStamp AptosQuantity `json:"timestamp"`
	}

	// The block metadata transaction
	BlockMetadataTransaction struct {
		Id    string        `json:"id"`
		Epoch AptosQuantity `json:"epoch"`
		Round AptosQuantity `json:"round"`
		// The events emitted at the block creation.
		Events []Event `json:"events"`
		// Previous block votes.
		PreviousBlockVotesBitvec []uint8 `json:"previous_block_votes_bitvec"`
		Proposer                 string  `json:"proposer"`
		// The indices of the proposers who failed to propose.
		FailedProposerIndices []uint32      `json:"failed_proposer_indices"`
		TimeStamp             AptosQuantity `json:"timestamp"`
	}

	Event struct {
		// The globally unique identifier of this event stream.
		Guid EventGuid `json:"guid"`
		// The sequence number of the event.
		SequenceNumber AptosQuantity `json:"sequence_number"`
		// String representation of an on-chain Move type tag.
		Type string `json:"type"`
		// The JSON representation of the event.
		Data json.RawMessage `json:"data"`
	}

	EventGuid struct {
		CreationNumber AptosQuantity `json:"creation_number"`
		AccountAddress string        `json:"account_address"`
	}

	StateCheckpointTransaction struct {
		TimeStamp AptosQuantity `json:"timestamp"`
	}

	// A transaction submitted by a user to change the state of the blockchain.
	UserTransaction struct {
		Sender                  string        `json:"sender"`
		SequenceNumber          AptosQuantity `json:"sequence_number"`
		MaxGasAmount            AptosQuantity `json:"max_gas_amount"`
		GasUnitPrice            AptosQuantity `json:"gas_unit_price"`
		ExpirationTimestampSecs AptosQuantity `json:"expiration_timestamp_secs"`
		// Need to be further parsed based on the payload type.
		Payload json.RawMessage `json:"payload"`
		// Need to be further parsed based on the signature type.
		Signature json.RawMessage `json:"signature"`
		// Events generated by the transaction
		Events    []Event       `json:"events"`
		TimeStamp AptosQuantity `json:"timestamp"`
	}

	// There are 5 types of transaction payloads, as defined below and in protos:
	// EntryFunctionPayload, ScriptPayload, ModuleBundlePayload, WriteSetPayload, and MultisigPayload.

	// Payload which runs a single entry function
	EntryFunctionPayload struct {
		// Need to further decode to related proto AptosEntryFunctionId
		Function      string            `json:"function"`
		TypeArguments []string          `json:"type_arguments"`
		Arguments     []json.RawMessage `json:"arguments"`
	}

	// Payload which runs a script that can run multiple functions
	ScriptPayload struct {
		Code          MoveScriptBytecode `json:"code"`
		TypeArguments []string           `json:"type_arguments"`
		Arguments     []json.RawMessage  `json:"arguments"`
	}

	// Move script bytecode
	MoveScriptBytecode struct {
		ByteCode string       `json:"bytecode"`
		Abi      MoveFunction `json:"abi"`
	}

	ModuleBundlePayload struct {
		Modules []MoveModuleBytecode `json:"modules"`
	}

	// Move module bytecode along with it's ABI
	MoveModuleBytecode struct {
		ByteCode string     `json:"bytecode"`
		Abi      MoveModule `json:"abi"`
	}

	// A multisig transaction that allows an owner of a multisig account to execute a pre-approved
	// transaction as the multisig account.
	MultisigPayload struct {
		MultisigAddress string `json:"multisig_address"`
		// This field is optional.
		TransactionPayload EntryFunctionPayload `json:"transaction_payload,omitempty"`
	}

	// This is only used by Genesis block/transactions.
	WriteSetPayload struct {
		Payload json.RawMessage `json:"write_set"`
	}

	// There are 2 types of write set payloads: ScriptWriteSet and DirectWriteSet.
	ScriptWriteSet struct {
		ExecuteAs string        `json:"execute_as"`
		Payload   ScriptPayload `json:"script"`
	}

	DirectWriteSet struct {
		// Need to further parse Changes, depending different types.
		Changes []json.RawMessage `json:"changes"`
		Events  []Event           `json:"events"`
	}

	// There are 3 types of signatures: Ed25519Signature, MultiEd25519Signature and MultiAgentSignature.

	AptosPublicKey struct {
		Value string `json:"value"`
		Type  string `json:"type"`
	}

	// Used by the custom unmarshaler of AptosPublicKey
	aptosPublicKey = struct {
		Value string `json:"value"`
		Type  string `json:"type"`
	}

	AptosSignature struct {
		Value string `json:"value"`
		Type  string `json:"type"`
	}

	// Used by the custom unmarshaler of AptosSignature
	aptosSignature = struct {
		Value string `json:"value"`
		Type  string `json:"type"`
	}

	// A single Ed25519 signature
	AptosEd25519Signature struct {
		PublicKey AptosPublicKey `json:"public_key"`
		Signature AptosSignature `json:"signature"`
	}
	AptosSingleSignature = AptosEd25519Signature

	// A Ed25519 multi-sig signature. This allows k-of-n signing for a transaction
	AptosMultiEd25519Signature struct {
		PublicKeys []AptosPublicKey `json:"public_keys"`
		Signatures []AptosSignature `json:"signatures"`
		Threshold  uint32           `json:"threshold"`
		Bitmap     string           `json:"bitmap"`
	}

	// Single key signature for single key transactions.
	AptosSingleKeySignature = AptosEd25519Signature

	// Multi key signature for multi key transactions.
	AptosMultiKeySignature struct {
		PublicKeys         []AptosPublicKey `json:"public_keys"`
		Signatures         []AptosSignature `json:"signatures"`
		SignaturesRequired uint32           `json:"signatures_required"`
	}

	// Multi agent signature for multi agent transactions. This allows you to have transactions across multiple accounts.
	AptosMultiAgentSignature struct {
		// Need to be parsed into AptosEd25519Signature/AptosMultiEd25519Signature
		Sender json.RawMessage `json:"sender"`
		// The other involved parties' addresses
		SecondarySignerAddresses []string `json:"secondary_signer_addresses"`
		// Need to be parsed into multiple AptosEd25519Signature/AptosMultiEd25519Signature
		// The associated signatures, in the same order as the secondary addresses
		SecondarySigners []json.RawMessage `json:"secondary_signers"`
	}

	AptosFeePayerSignature struct {
		// Need to be parsed into AptosEd25519Signature/AptosMultiEd25519Signature
		Sender json.RawMessage `json:"sender"`
		// The other involved parties' addresses
		SecondarySignerAddresses []string `json:"secondary_signer_addresses"`
		// Need to be parsed into multiple AptosEd25519Signature/AptosMultiEd25519Signature
		// The associated signatures, in the same order as the secondary addresses
		SecondarySigners []json.RawMessage `json:"secondary_signers"`
		// The address of the paying party
		FeePayerAddress string `json:"fee_payer_address"`
		// The signature of the fee payer
		FeePayerSigner json.RawMessage `json:"fee_payer_signer"`
	}

	// The genesis transaction. This only occurs at the genesis transaction (version 0).
	GenesisTransaction struct {
		Payload GenesisTransactionPayload `json:"payload"`
		// Events generated by the transaction
		Events []Event `json:"events"`
	}

	// The write set payload of the Genesis transaction.
	GenesisTransactionPayload struct {
		Type string `json:"type"`
		// Need to be parsed based on write set type: ScriptWriteSet and DirectWriteSet.
		WriteSet json.RawMessage `json:"write_set"`
	}
)

func NewAptosNativeParser(params internal.ParserParams, opts ...internal.ParserFactoryOption) (internal.NativeParser, error) {
	return &aptosNativeParserImpl{
		logger:   log.WithPackage(params.Logger),
		validate: validator.New(),
		config:   params.Config,
	}, nil
}

func (v AptosQuantity) MarshalJSON() ([]byte, error) {
	s := fmt.Sprintf(`"%d"`, uint64(v))
	return []byte(s), nil
}

func (v *AptosQuantity) UnmarshalJSON(input []byte) error {
	var s string
	if err := json.Unmarshal(input, &s); err != nil {
		return fmt.Errorf("failed to unmarshal AptosQuantity into string: %w", err)
	}

	if s == "" {
		*v = 0
		return nil
	}

	// For Aptos, all the AptosQuantity values are base-10.
	i, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return fmt.Errorf("failed to decode AptosQuantity %v: %w", s, err)
	}

	*v = AptosQuantity(i)
	return nil
}

func (v AptosQuantity) Value() uint64 {
	return uint64(v)
}

func (v *AptosPublicKey) UnmarshalJSON(input []byte) error {
	if len(input) > 0 && input[0] == '"' {
		return json.Unmarshal(input, &v.Value)
	}

	// Use a different struct to avoid calling this custom unmarshaler recursively.
	var out aptosPublicKey
	if err := json.Unmarshal(input, &out); err != nil {
		return fmt.Errorf("failed to unmarshal struct: %w", err)
	}
	v.Value = out.Value
	v.Type = out.Type
	return nil
}

func (v *AptosSignature) UnmarshalJSON(input []byte) error {
	if len(input) > 0 && input[0] == '"' {
		return json.Unmarshal(input, &v.Value)
	}

	// Use a different struct to avoid calling this custom unmarshaler recursively.
	var out aptosSignature
	if err := json.Unmarshal(input, &out); err != nil {
		return fmt.Errorf("failed to unmarshal struct: %w", err)
	}
	v.Value = out.Value
	v.Type = out.Type
	return nil
}

func (p *aptosNativeParserImpl) ParseBlock(ctx context.Context, rawBlock *api.Block) (*api.NativeBlock, error) {
	metadata := rawBlock.GetMetadata()
	if metadata == nil {
		return nil, errors.New("metadata not found")
	}

	blobdata := rawBlock.GetAptos()
	if blobdata == nil {
		return nil, fmt.Errorf("blobdata not found (metadata={%+v})", metadata)
	}

	var block AptosBlock
	if err := json.Unmarshal(blobdata.Block, &block); err != nil {
		return nil, fmt.Errorf("failed to unmarshal header (metadata={%+v}: %w", metadata, err)
	}

	if err := p.validate.Struct(block); err != nil {
		return nil, fmt.Errorf("failed to parse block header on struct validate: %w", err)
	}

	nativeBlock, err := p.parseBlock(&block)
	if err != nil {
		return nil, fmt.Errorf("failed to parse block (metadata={%+v}: %w", metadata, err)
	}

	return &api.NativeBlock{
		Blockchain:      rawBlock.Blockchain,
		Network:         rawBlock.Network,
		Tag:             metadata.Tag,
		Hash:            metadata.Hash,
		ParentHash:      metadata.ParentHash,
		Height:          metadata.Height,
		ParentHeight:    metadata.ParentHeight,
		Timestamp:       p.parseTimestamp(int64(block.BlockTime.Value())),
		NumTransactions: uint64(len(block.Transactions)),
		Block: &api.NativeBlock_Aptos{
			Aptos: nativeBlock,
		},
	}, nil
}

func (p *aptosNativeParserImpl) GetTransaction(ctx context.Context, nativeBlock *api.NativeBlock, transactionHash string) (*api.NativeTransaction, error) {
	return nil, internal.ErrNotImplemented
}

// In Aptos, all timestamp values are in micro seconds.
func (p *aptosNativeParserImpl) parseTimestamp(ts int64) *timestamp.Timestamp {
	tsInSecs := ts / 1000000
	return &timestamp.Timestamp{
		Seconds: tsInSecs,
	}
}

func (p *aptosNativeParserImpl) parseBlock(block *AptosBlock) (*api.AptosBlock, error) {
	header, err := p.parseHeader(block)
	if err != nil {
		return nil, fmt.Errorf("failed to parse header: %w", err)
	}

	transactions, err := p.parseTransactions(block.BlockHeight.Value(), block.Transactions)
	if err != nil {
		return nil, fmt.Errorf("failed to parse transactions: %w", err)
	}

	return &api.AptosBlock{
		Header:       header,
		Transactions: transactions,
	}, nil
}

func (p *aptosNativeParserImpl) parseHeader(block *AptosBlock) (*api.AptosHeader, error) {
	return &api.AptosHeader{
		BlockHeight: block.BlockHeight.Value(),
		BlockHash:   block.BlockHash,
		BlockTime:   p.parseTimestamp(int64(block.BlockTime.Value())),
	}, nil
}

func (p *aptosNativeParserImpl) parseTransactions(blockHeight uint64, transactions []json.RawMessage) ([]*api.AptosTransaction, error) {
	result := make([]*api.AptosTransaction, len(transactions))
	for i, t := range transactions {
		// Different from Ethereum/Solana, in Aptos, there are 4 types of transactions. To parse a transaction, we first need to
		// know its type, then parse the the whole transaction.
		var transactionInfo AptosTransactionInfo
		if err := json.Unmarshal(t, &transactionInfo); err != nil {
			return nil, fmt.Errorf("failed to unmarshal transaction info: %w", err)
		}

		var transaction *api.AptosTransaction
		var err error
		switch transactionInfo.Type {
		case typeBlockMetadataTransaction:
			transaction, err = p.parseBlockMetadataTransaction(blockHeight, &transactionInfo, t)
			if err != nil {
				return nil, fmt.Errorf("failed to parse block metadata transaction with hash=%s: %w", transactionInfo.TransactionHash, err)
			}
		case typeStateCheckpointTransaction:
			transaction, err = p.parseStateCheckpointTransaction(blockHeight, &transactionInfo, t)
			if err != nil {
				return nil, fmt.Errorf("failed to parse state checkpoint transaction with hash=%s: %w", transactionInfo.TransactionHash, err)
			}
		case typeGenesisTransaction:
			transaction, err = p.parseGenesisTransaction(blockHeight, &transactionInfo, t)
			if err != nil {
				return nil, fmt.Errorf("failed to parse genesis transactions with hash=%s: %w", transactionInfo.TransactionHash, err)
			}
		case typeUserTransaction:
			transaction, err = p.parseUserTransaction(blockHeight, &transactionInfo, t)
			if err != nil {
				return nil, fmt.Errorf("failed to parse user transactions with hash=%s: %w", transactionInfo.TransactionHash, err)
			}
		case typeValidatorTransaction:
			transaction, err = p.parseBlockValidatorTransaction(blockHeight, &transactionInfo, t)
			if err != nil {
				return nil, fmt.Errorf("failed to parse validator transactions with hash=%s: %w", transactionInfo.TransactionHash, err)
			}
		default:
			return nil, fmt.Errorf("failed to parse transaction_hash=%s, unknown type: %s: %w", transactionInfo.TransactionHash, transactionInfo.Type, err)
		}

		result[i] = transaction
	}

	return result, nil
}

func (p *aptosNativeParserImpl) parseBlockValidatorTransaction(blockHeight uint64, transactionInfo *AptosTransactionInfo, data json.RawMessage) (*api.AptosTransaction, error) {
	var validatorTx ValidatorTransaction
	if err := json.Unmarshal(data, &validatorTx); err != nil {
		return nil, fmt.Errorf("failed to unmarshal validator transaction: %w", err)
	}

	apiTransactionInfo, err := p.parseTransactionInfo(transactionInfo)
	if err != nil {
		return nil, fmt.Errorf("failed to parse transaction info: %w", err)
	}

	events, err := p.parseEvents(validatorTx.Events)
	if err != nil {
		return nil, fmt.Errorf("failed to parse events: %w", err)
	}
	// Construct the api.AptosTransaction
	return &api.AptosTransaction{
		Info:        apiTransactionInfo,
		Timestamp:   p.parseTimestamp(int64(validatorTx.TimeStamp.Value())),
		Version:     transactionInfo.Version.Value(),
		BlockHeight: blockHeight,
		Type:        api.AptosTransaction_VALIDATOR,
		TxnData: &api.AptosTransaction_Validator{
			Validator: &api.AptosValidatorTransaction{
				Events: events,
			},
		},
	}, nil

}

func (p *aptosNativeParserImpl) parseBlockMetadataTransaction(blockHeight uint64, transactionInfo *AptosTransactionInfo, data json.RawMessage) (*api.AptosTransaction, error) {
	var blockMeta BlockMetadataTransaction
	if err := json.Unmarshal(data, &blockMeta); err != nil {
		return nil, fmt.Errorf("failed to unmarshal block metadata transaction: %w", err)
	}

	// Parse transactionInfo
	apiTransactionInfo, err := p.parseTransactionInfo(transactionInfo)
	if err != nil {
		return nil, fmt.Errorf("failed to parse transaction info: %w", err)
	}

	// Parse events
	events, err := p.parseEvents(blockMeta.Events)
	if err != nil {
		return nil, fmt.Errorf("failed to parse events: %w", err)
	}

	// Construct api.AptosBlockMetadataTransaction
	apiBlockMeta := &api.AptosBlockMetadataTransaction{
		Id:                       blockMeta.Id,
		Epoch:                    blockMeta.Epoch.Value(),
		Round:                    blockMeta.Round.Value(),
		Events:                   events,
		PreviousBlockVotesBitvec: blockMeta.PreviousBlockVotesBitvec,
		Proposer:                 blockMeta.Proposer,
		FailedProposerIndices:    blockMeta.FailedProposerIndices,
	}

	// Construct the api.AptosTransaction
	return &api.AptosTransaction{
		Info:        apiTransactionInfo,
		Timestamp:   p.parseTimestamp(int64(blockMeta.TimeStamp.Value())),
		Version:     transactionInfo.Version.Value(),
		BlockHeight: blockHeight,
		Type:        api.AptosTransaction_BLOCK_METADATA,
		TxnData: &api.AptosTransaction_BlockMetadata{
			BlockMetadata: apiBlockMeta,
		},
	}, nil
}

func (p *aptosNativeParserImpl) parseTransactionInfo(transactionInfo *AptosTransactionInfo) (*api.AptosTransactionInfo, error) {
	// Parse write changes
	apiChanges, err := p.parseChanges(transactionInfo.Changes)
	if err != nil {
		return nil, fmt.Errorf("failed to parse changes: %w", err)
	}

	// Get api.TransactionInfo
	apiTransactionInfo := &api.AptosTransactionInfo{
		Hash:                transactionInfo.TransactionHash,
		StateChangeHash:     transactionInfo.StateChangeHash,
		EventRootHash:       transactionInfo.EventRootHash,
		GasUsed:             transactionInfo.GasUsed.Value(),
		Success:             transactionInfo.Success,
		VmStatus:            transactionInfo.VmStatus,
		AccumulatorRootHash: transactionInfo.AccumulatorRootHash,
		Changes:             apiChanges,
	}
	if len(transactionInfo.StateCheckpointHash) > 0 {
		apiTransactionInfo.OptionalStateCheckpointHash = &api.AptosTransactionInfo_StateCheckpointHash{
			StateCheckpointHash: transactionInfo.StateCheckpointHash,
		}
	}

	return apiTransactionInfo, nil
}

func (p *aptosNativeParserImpl) parseChanges(changes []json.RawMessage) ([]*api.AptosWriteSetChange, error) {
	results := make([]*api.AptosWriteSetChange, len(changes))
	for i, c := range changes {
		// Similar as the transaction type, we also need to first par the write set change type, then we can
		// parse the data into different types of write set change.
		var wcType GenericType
		if err := json.Unmarshal(c, &wcType); err != nil {
			return nil, fmt.Errorf("failed to unmarshal write set change type: %w", err)
		}

		switch wcType.Type {
		case typeDeleteModuleChange:
			var change DeleteModuleChange
			if err := json.Unmarshal(c, &change); err != nil {
				return nil, fmt.Errorf("failed to unmarshal delete module change: %w", err)
			}

			module, err := p.parseMoveModuleId(change.Module)
			if err != nil {
				return nil, fmt.Errorf("failed to parse delete module change: %w", err)
			}

			results[i] = &api.AptosWriteSetChange{
				Type: api.AptosWriteSetChange_DELETE_MODULE,
				Change: &api.AptosWriteSetChange_DeleteModule{
					DeleteModule: &api.AptosDeleteModule{
						Address:      change.Address,
						StateKeyHash: change.StateKeyHash,
						Module:       module,
					},
				},
			}

		case typeDeleteResourceChange:
			var change DeleteResourceChange
			if err := json.Unmarshal(c, &change); err != nil {
				return nil, fmt.Errorf("failed to unmarshal delete resource change: %w", err)
			}

			results[i] = &api.AptosWriteSetChange{
				Type: api.AptosWriteSetChange_DELETE_RESOURCE,
				Change: &api.AptosWriteSetChange_DeleteResource{
					DeleteResource: &api.AptosDeleteResource{
						Address:      change.Address,
						StateKeyHash: change.StateKeyHash,
						Resource:     change.Resource,
					},
				},
			}

		case typeDeleteTableItemChange:
			var change DeleteTableItemChange
			if err := json.Unmarshal(c, &change); err != nil {
				return nil, fmt.Errorf("failed to unmarshal delete table item change: %w", err)
			}

			results[i] = &api.AptosWriteSetChange{
				Type: api.AptosWriteSetChange_DELETE_TABLE_ITEM,
				Change: &api.AptosWriteSetChange_DeleteTableItem{
					DeleteTableItem: &api.AptosDeleteTableItem{
						StateKeyHash: change.StateKeyHash,
						Handle:       change.Handle,
						Key:          change.Key,
						Data: &api.AptosDeleteTableData{
							Key:     change.Data.Key,
							KeyType: change.Data.KeyType,
						},
					},
				},
			}

		case typeWriteResourceChange:
			var change WriteResourceChange
			if err := json.Unmarshal(c, &change); err != nil {
				return nil, fmt.Errorf("failed to unmarshal write resource change: %w", err)
			}

			results[i] = &api.AptosWriteSetChange{
				Type: api.AptosWriteSetChange_WRITE_RESOURCE,
				Change: &api.AptosWriteSetChange_WriteResource{
					WriteResource: &api.AptosWriteResource{
						Address:      change.Address,
						StateKeyHash: change.StateKeyHash,
						TypeStr:      change.Data.Type,
						Data:         string(change.Data.Data),
					},
				},
			}
		case typeWriteModuleChange:
			var change WriteModuleChange
			if err := json.Unmarshal(c, &change); err != nil {
				return nil, fmt.Errorf("failed to unmarshal write module change: %w", err)
			}

			// Convert into api.AptosMoveModuleBytecode
			apiBytecode, err := p.parseMoveModuleBytecode(&change.Data)
			if err != nil {
				return nil, fmt.Errorf("failed to parse move module bytecode: %w", err)
			}

			results[i] = &api.AptosWriteSetChange{
				Type: api.AptosWriteSetChange_WRITE_MODULE,
				Change: &api.AptosWriteSetChange_WriteModule{
					WriteModule: &api.AptosWriteModule{
						Address:      change.Address,
						StateKeyHash: change.StateKeyHash,
						Data:         apiBytecode,
					},
				},
			}

		case typeWriteTableItemChange:
			var change WriteTableItemChange
			if err := json.Unmarshal(c, &change); err != nil {
				return nil, fmt.Errorf("failed to unmarshal write table item change: %w", err)
			}

			results[i] = &api.AptosWriteSetChange{
				Type: api.AptosWriteSetChange_WRITE_TABLE_ITEM,
				Change: &api.AptosWriteSetChange_WriteTableItem{
					WriteTableItem: &api.AptosWriteTableItem{
						StateKeyHash: change.StateKeyHash,
						Handle:       change.Handle,
						Key:          change.Key,
						Value:        change.Value,
						Data: &api.AptosWriteTableItemData{
							Key:       change.Data.Key,
							KeyType:   change.Data.KeyType,
							Value:     change.Data.Value,
							ValueType: change.Data.ValueType,
						},
					},
				},
			}

		default:
			return nil, fmt.Errorf("failed to parse change type %s", wcType.Type)
		}
	}

	return results, nil
}

// Parse the input string to AptosMoveModuleId. The input string format: "address::module_name".
func (p *aptosNativeParserImpl) parseMoveModuleId(name string) (*api.AptosMoveModuleId, error) {
	results := strings.Split(name, "::")
	if len(results) != 2 {
		return nil, fmt.Errorf("failed to parse module id, input name doesn't have two parts=%s", name)
	}

	return &api.AptosMoveModuleId{
		Address: results[0],
		Name:    results[1],
	}, nil
}

func (p *aptosNativeParserImpl) parseMoveModuleBytecode(data *MoveModuleBytecode) (*api.AptosMoveModuleBytecode, error) {
	apiAbi, err := p.parseMoveModule(&data.Abi)
	if err != nil {
		return nil, fmt.Errorf("failed to parse move module: %w", err)
	}

	return &api.AptosMoveModuleBytecode{
		Bytecode: data.ByteCode,
		Abi:      apiAbi,
	}, nil
}

func (p *aptosNativeParserImpl) parseMoveModule(data *MoveModule) (*api.AptosMoveModule, error) {

	// Get api.AptosMoveModuleId for friends
	apiFriends, err := p.parseMoveModuleIds(data.Friends)
	if err != nil {
		return nil, fmt.Errorf("failed to parse move module ids: %w", err)
	}

	// Parse move functions.
	apiMoveFunctions, err := p.parseMoveFunctions(data.ExposedFunctions)
	if err != nil {
		return nil, fmt.Errorf("failed to parse move module: %w", err)
	}

	// Parse move struct.
	apiMoveStruct, err := p.parseMoveStruct(data.Structs)
	if err != nil {
		return nil, fmt.Errorf("failed to parse move module: %w", err)
	}

	return &api.AptosMoveModule{
		Address:          data.Address,
		Name:             data.Name,
		Friends:          apiFriends,
		ExposedFunctions: apiMoveFunctions,
		Structs:          apiMoveStruct,
	}, nil
}

func (p *aptosNativeParserImpl) parseMoveModuleIds(modules []string) ([]*api.AptosMoveModuleId, error) {
	results := make([]*api.AptosMoveModuleId, len(modules))
	for i, m := range modules {
		result, err := p.parseMoveModuleId(m)
		if err != nil {
			return nil, fmt.Errorf("failed to parse move module id: %w", err)
		}

		results[i] = result
	}

	return results, nil
}

func (p *aptosNativeParserImpl) parseMoveFunctions(functions []MoveFunction) ([]*api.AptosMoveFunction, error) {
	results := make([]*api.AptosMoveFunction, len(functions))
	for i, f := range functions {
		f := f
		result, err := p.parseMoveFunction(&f)
		if err != nil {
			return nil, fmt.Errorf("failed to parse move function: %w", err)
		}
		results[i] = result
	}

	return results, nil
}

func (p *aptosNativeParserImpl) parseMoveFunction(function *MoveFunction) (*api.AptosMoveFunction, error) {
	var functionType api.AptosMoveFunction_Type
	switch function.Visibility {
	case "private":
		functionType = api.AptosMoveFunction_PRIVATE
	case "public":
		functionType = api.AptosMoveFunction_PUBLIC
	case "friend":
		functionType = api.AptosMoveFunction_FRIEND
	// The visibility can be empty in the case where a transaction has failed and the ABI is empty. If so, parse as
	// UNSPECIFIED.
	case "":
		functionType = api.AptosMoveFunction_UNSPECIFIED
	default:
		return nil, fmt.Errorf("failed to parse function type, type=%s", function.Visibility)
	}

	apiGenericTypeParams := p.parseMoveFunctionGenericTypeParams(function.GenericTypePramas)

	return &api.AptosMoveFunction{
		Name:              function.Name,
		Visibility:        functionType,
		IsEntry:           function.IsEntry,
		GenericTypeParams: apiGenericTypeParams,
		Params:            function.Params,
		Return:            function.Return,
	}, nil
}

func (p *aptosNativeParserImpl) parseMoveFunctionGenericTypeParams(params []MoveFunctionGenericTypeParam) []*api.AptosMoveFunctionGenericTypeParam {
	results := make([]*api.AptosMoveFunctionGenericTypeParam, len(params))
	for i, pa := range params {
		results[i] = &api.AptosMoveFunctionGenericTypeParam{
			Constraints: pa.Constraints,
		}
	}

	return results
}

func (p *aptosNativeParserImpl) parseMoveStruct(structs []MoveStruct) ([]*api.AptosMoveStruct, error) {
	results := make([]*api.AptosMoveStruct, len(structs))
	for i, s := range structs {

		apiGenericTypeParams := p.parseMoveStructGenericTypeParams(s.GenericTypePramas)
		apiFields := p.parseMoveStructFields(s.Fields)

		results[i] = &api.AptosMoveStruct{
			Name:              s.Name,
			IsNative:          s.IsNative,
			Abilities:         s.Abilities,
			GenericTypeParams: apiGenericTypeParams,
			Fields:            apiFields,
		}
	}

	return results, nil
}

func (p *aptosNativeParserImpl) parseMoveStructGenericTypeParams(params []MoveStructGenericTypeParam) []*api.AptosMoveStructGenericTypeParam {
	results := make([]*api.AptosMoveStructGenericTypeParam, len(params))
	for i, pa := range params {
		results[i] = &api.AptosMoveStructGenericTypeParam{
			Constraints: pa.Constraints,
		}
	}

	return results
}

func (p *aptosNativeParserImpl) parseMoveStructFields(fields []MoveStructField) []*api.AptosMoveStructField {
	results := make([]*api.AptosMoveStructField, len(fields))
	for i, f := range fields {
		results[i] = &api.AptosMoveStructField{
			Name: f.Name,
			Type: f.Type,
		}
	}

	return results
}

func (p *aptosNativeParserImpl) parseEvents(events []Event) ([]*api.AptosEvent, error) {
	results := make([]*api.AptosEvent, len(events))
	for i, e := range events {
		results[i] = &api.AptosEvent{
			Key: &api.AptosEventKey{
				CreationNumber: e.Guid.CreationNumber.Value(),
				AccountAddress: e.Guid.AccountAddress,
			},
			SequenceNumber: e.SequenceNumber.Value(),
			Type:           e.Type,
			Data:           string(e.Data),
		}
	}

	return results, nil
}

func (p *aptosNativeParserImpl) parseStateCheckpointTransaction(blockHeight uint64, transactionInfo *AptosTransactionInfo, data json.RawMessage) (*api.AptosTransaction, error) {
	var stateCheckpointT StateCheckpointTransaction
	if err := json.Unmarshal(data, &stateCheckpointT); err != nil {
		return nil, fmt.Errorf("failed to unmarshal state checkpoint transaction: %w", err)
	}

	// Parse transactionInfo
	apiTransactionInfo, err := p.parseTransactionInfo(transactionInfo)
	if err != nil {
		return nil, fmt.Errorf("failed to parse transaction info: %w", err)
	}

	// Construct the api.AptosTransaction
	return &api.AptosTransaction{
		Info:        apiTransactionInfo,
		Timestamp:   p.parseTimestamp(int64(stateCheckpointT.TimeStamp.Value())),
		Version:     transactionInfo.Version.Value(),
		BlockHeight: blockHeight,
		Type:        api.AptosTransaction_STATE_CHECKPOINT,
		TxnData: &api.AptosTransaction_StateCheckpoint{
			StateCheckpoint: &api.AptosStateCheckpointTransaction{},
		},
	}, nil
}

func (p *aptosNativeParserImpl) parseUserTransaction(blockHeight uint64, transactionInfo *AptosTransactionInfo, data json.RawMessage) (*api.AptosTransaction, error) {
	var userT UserTransaction
	if err := json.Unmarshal(data, &userT); err != nil {
		return nil, fmt.Errorf("failed to unmarshal user transaction: %w", err)
	}

	// Parse transaction info
	apiTransactionInfo, err := p.parseTransactionInfo(transactionInfo)
	if err != nil {
		return nil, fmt.Errorf("failed to parse transaction info: %w", err)
	}

	// Parse transaction payload
	apiPayload, err := p.parseTransactionPayload(userT.Payload)
	if err != nil {
		return nil, fmt.Errorf("failed to parse user transaction payload: %w", err)
	}

	// Parse transaction signature
	apiSignature, err := p.parseTransactionSignature(userT.Signature)
	if err != nil {
		return nil, fmt.Errorf("failed to parse user transaction signature: %w", err)
	}

	// Parse events
	events, err := p.parseEvents(userT.Events)
	if err != nil {
		return nil, fmt.Errorf("failed to parse events: %w", err)
	}

	// Construct api.AptosUserTransaction
	apiUser := &api.AptosUserTransaction{
		Request: &api.AptosUserTransactionRequest{
			Sender:                  userT.Sender,
			SequenceNumber:          userT.SequenceNumber.Value(),
			MaxGasAmount:            userT.MaxGasAmount.Value(),
			GasUnitPrice:            userT.GasUnitPrice.Value(),
			ExpirationTimestampSecs: p.parseTimestamp(int64(userT.ExpirationTimestampSecs.Value() * 1000000)),
			Payload:                 apiPayload,
			Signature:               apiSignature,
		},
		Events: events,
	}

	// Construct the api.AptosTransaction
	return &api.AptosTransaction{
		Info:        apiTransactionInfo,
		Timestamp:   p.parseTimestamp(int64(userT.TimeStamp.Value())),
		Version:     transactionInfo.Version.Value(),
		BlockHeight: blockHeight,
		Type:        api.AptosTransaction_USER,
		TxnData: &api.AptosTransaction_User{
			User: apiUser,
		},
	}, nil
}

func (p *aptosNativeParserImpl) parseTransactionPayload(payload json.RawMessage) (*api.AptosTransactionPayload, error) {
	// We also need to first parse the transaction payload type, then we can parse the data into different types of
	// transaction payloads.
	var payloadType GenericType
	if err := json.Unmarshal(payload, &payloadType); err != nil {
		return nil, fmt.Errorf("failed to unmarshal transaction payload type: %w", err)
	}

	var apiPayload *api.AptosTransactionPayload
	switch payloadType.Type {
	case typeEntryFunctionPayload:
		var result EntryFunctionPayload
		if err := json.Unmarshal(payload, &result); err != nil {
			return nil, fmt.Errorf("failed to unmarshal entry function payload: %w", err)
		}

		// Parse the entry function Id from the input string.
		apiEntryFunctionId, err := p.parseEntryFunctionId(result.Function)
		if err != nil {
			return nil, fmt.Errorf("failed to parse entry function id: %w", err)
		}

		apiPayload = &api.AptosTransactionPayload{
			Type: api.AptosTransactionPayload_ENTRY_FUNCTION_PAYLOAD,
			Payload: &api.AptosTransactionPayload_EntryFunctionPayload{
				EntryFunctionPayload: &api.AptosEntryFunctionPayload{
					Function:      apiEntryFunctionId,
					TypeArguments: result.TypeArguments,
					Arguments:     ConverRawMessageToBytes(result.Arguments),
				},
			},
		}

	case typeScriptPayload:
		var result ScriptPayload
		if err := json.Unmarshal(payload, &result); err != nil {
			return nil, fmt.Errorf("failed to unmarshal script payload: %w", err)
		}

		apiScriptPayload, err := p.parseScriptPayload(&result)
		if err != nil {
			return nil, fmt.Errorf("failed to parse script payload: %w", err)
		}

		apiPayload = &api.AptosTransactionPayload{
			Type: api.AptosTransactionPayload_SCRIPT_PAYLOAD,
			Payload: &api.AptosTransactionPayload_ScriptPayload{
				ScriptPayload: apiScriptPayload,
			},
		}

	case typeModuleBundlePayload:
		var result ModuleBundlePayload
		if err := json.Unmarshal(payload, &result); err != nil {
			return nil, fmt.Errorf("failed to unmarshal move bundle payload: %w", err)
		}

		apiMoveModuleBytecodes, err := p.parseMoveModuleBytecodes(result.Modules)
		if err != nil {
			return nil, fmt.Errorf("failed to parse move module bytecodes: %w", err)
		}

		apiPayload = &api.AptosTransactionPayload{
			Type: api.AptosTransactionPayload_MODULE_BUNDLE_PAYLOAD,
			Payload: &api.AptosTransactionPayload_ModuleBundlePayload{
				ModuleBundlePayload: &api.AptosModuleBundlePayload{
					Modules: apiMoveModuleBytecodes,
				},
			},
		}

	case typeMultisigPayload:
		var result MultisigPayload
		if err := json.Unmarshal(payload, &result); err != nil {
			return nil, fmt.Errorf("failed to unmarshal multisig payload: %w", err)
		}

		apiMultisigPayload := &api.AptosMultisigPayload{
			MultisigAddress: result.MultisigAddress,
		}

		if len(result.TransactionPayload.Function) > 0 {
			// Parse the entry function Id from the input string.
			apiEntryFunctionId, err := p.parseEntryFunctionId(result.TransactionPayload.Function)
			if err != nil {
				return nil, fmt.Errorf("failed to parse entry function id: %w", err)
			}

			apiMultisigPayload.OptionalTransactionPayload = &api.AptosMultisigPayload_TransactionPayload{
				TransactionPayload: &api.AptosMultisigTransactionPayload{
					Type: api.AptosMultisigTransactionPayload_ENTRY_FUNCTION_PAYLOAD,
					Payload: &api.AptosMultisigTransactionPayload_EntryFunctionPayload{
						EntryFunctionPayload: &api.AptosEntryFunctionPayload{
							Function:      apiEntryFunctionId,
							TypeArguments: result.TransactionPayload.TypeArguments,
							Arguments:     ConverRawMessageToBytes(result.TransactionPayload.Arguments),
						},
					},
				},
			}
		}

		apiPayload = &api.AptosTransactionPayload{
			Type: api.AptosTransactionPayload_MULTISIG_PAYLOAD,
			Payload: &api.AptosTransactionPayload_MultisigPayload{
				MultisigPayload: apiMultisigPayload,
			},
		}

	case typeWriteSetPayload:
		var result WriteSetPayload
		if err := json.Unmarshal(payload, &result); err != nil {
			return nil, fmt.Errorf("failed to unmarshal write set payload: %w", err)
		}

		apiWriteSet, err := p.parseWriteSet(result.Payload)
		if err != nil {
			return nil, fmt.Errorf("failed to parse write set: %w", err)
		}

		apiPayload = &api.AptosTransactionPayload{
			Type: api.AptosTransactionPayload_WRITE_SET_PAYLOAD,
			Payload: &api.AptosTransactionPayload_WriteSetPayload{
				WriteSetPayload: &api.AptosWriteSetPayload{
					WriteSet: apiWriteSet,
				},
			},
		}

	default:
		return nil, fmt.Errorf("failed to parse unknown transaction type=%s", payloadType.Type)
	}

	return apiPayload, nil
}

func ConverRawMessageToBytes(inputs []json.RawMessage) [][]byte {
	byteSlice := make([][]byte, len(inputs))
	for i, input := range inputs {
		byteSlice[i] = input
	}
	return byteSlice
}

// Parse the input string to AptosEntryFunctionId. The input string format: "address::module_name::function_name".
func (p *aptosNativeParserImpl) parseEntryFunctionId(name string) (*api.AptosEntryFunctionId, error) {
	results := strings.Split(name, "::")
	if len(results) != 3 {
		return nil, fmt.Errorf("failed to parse entry function id, input name doesn't have three parts=%s", name)
	}

	return &api.AptosEntryFunctionId{
		Module: &api.AptosMoveModuleId{
			Address: results[0],
			Name:    results[1],
		},
		FunctionName: results[2],
	}, nil
}

func (p *aptosNativeParserImpl) parseMoveScriptBytecode(code *MoveScriptBytecode) (*api.AptosMoveScriptBytecode, error) {
	apiAbi, err := p.parseMoveFunction(&code.Abi)
	if err != nil {
		return nil, fmt.Errorf("failed to parse move function: %w", err)
	}

	return &api.AptosMoveScriptBytecode{
		Bytecode: code.ByteCode,
		Abi:      apiAbi,
	}, nil
}

func (p *aptosNativeParserImpl) parseMoveModuleBytecodes(codes []MoveModuleBytecode) ([]*api.AptosMoveModuleBytecode, error) {
	results := make([]*api.AptosMoveModuleBytecode, len(codes))
	for i, c := range codes {
		c := c
		result, err := p.parseMoveModuleBytecode(&c)
		if err != nil {
			return nil, fmt.Errorf("failed to parse move module bytecode: %w", err)
		}

		results[i] = result
	}

	return results, nil
}

func (p *aptosNativeParserImpl) parseWriteSet(payload json.RawMessage) (*api.AptosWriteSet, error) {
	// We need to first parse the write set type, then we can parse the data into different types of
	// write sets.
	var wsType GenericType
	if err := json.Unmarshal(payload, &wsType); err != nil {
		return nil, fmt.Errorf("failed to unmarshal write set type: %w", err)
	}

	var apiWriteSet *api.AptosWriteSet
	switch wsType.Type {
	case typeScriptWriteSet:
		var result ScriptWriteSet
		if err := json.Unmarshal(payload, &result); err != nil {
			return nil, fmt.Errorf("failed to unmarshal script write set: %w", err)
		}

		apiScriptPayload, err := p.parseScriptPayload(&result.Payload)
		if err != nil {
			return nil, fmt.Errorf("failed to parse script payload: %w", err)
		}

		apiWriteSet = &api.AptosWriteSet{
			WriteSetType: api.AptosWriteSet_SCRIPT_WRITE_SET,
			WriteSet: &api.AptosWriteSet_ScriptWriteSet{
				ScriptWriteSet: &api.AptosScriptWriteSet{
					ExecuteAs: result.ExecuteAs,
					Script:    apiScriptPayload,
				},
			},
		}

	case typeDirectWriteSet:
		var result DirectWriteSet
		if err := json.Unmarshal(payload, &result); err != nil {
			return nil, fmt.Errorf("failed to unmarshal direct write set: %w", err)
		}

		apiWriteSetChanges, err := p.parseChanges(result.Changes)
		if err != nil {
			return nil, fmt.Errorf("failed to parse changes: %w", err)
		}

		apiEvents, err := p.parseEvents(result.Events)
		if err != nil {
			return nil, fmt.Errorf("failed to parse events: %w", err)
		}

		apiWriteSet = &api.AptosWriteSet{
			WriteSetType: api.AptosWriteSet_DIRECT_WRITE_SET,
			WriteSet: &api.AptosWriteSet_DirectWriteSet{
				DirectWriteSet: &api.AptosDirectWriteSet{
					WriteSetChange: apiWriteSetChanges,
					Events:         apiEvents,
				},
			},
		}

	default:
		return nil, fmt.Errorf("failed to parse unknown write set type: %s", wsType.Type)
	}

	return apiWriteSet, nil
}

func (p *aptosNativeParserImpl) parseScriptPayload(payload *ScriptPayload) (*api.AptosScriptPayload, error) {
	apiCode, err := p.parseMoveScriptBytecode(&payload.Code)
	if err != nil {
		return nil, fmt.Errorf("failed to parse move script bytecode: %w", err)
	}

	return &api.AptosScriptPayload{
		Code:          apiCode,
		TypeArguments: payload.TypeArguments,
		Arguments:     ConverRawMessageToBytes(payload.Arguments),
	}, nil
}

func (p *aptosNativeParserImpl) parseTransactionSignature(payload json.RawMessage) (*api.AptosSignature, error) {
	// We need to first parse the signature type, then we can parse the data into different types of signatures.
	var sType GenericType
	if err := json.Unmarshal(payload, &sType); err != nil {
		return nil, fmt.Errorf("failed to unmarshal signature type: %w", err)
	}

	var apiSignature *api.AptosSignature
	switch sType.Type {
	case typeEd25519Signature:
		var result AptosEd25519Signature
		if err := json.Unmarshal(payload, &result); err != nil {
			return nil, fmt.Errorf("failed to unmarshal ed25519 signature: %w", err)
		}

		apiSignature = &api.AptosSignature{
			Type: api.AptosSignature_ED25519,
			Signature: &api.AptosSignature_Ed25519{
				Ed25519: &api.AptosEd25519Signature{
					PublicKey: result.PublicKey.Value,
					Signature: result.Signature.Value,
				},
			},
		}

	case typeMultiEd25519Signature:
		var result AptosMultiEd25519Signature
		if err := json.Unmarshal(payload, &result); err != nil {
			return nil, fmt.Errorf("failed to unmarshal multi ed25519 signature: %w", err)
		}
		apiSignature = &api.AptosSignature{
			Type: api.AptosSignature_MULTI_ED25519,
			Signature: &api.AptosSignature_MultiEd25519{
				MultiEd25519: &api.AptosMultiEd25519Signature{
					PublicKeys:       parsePublicKeys(result.PublicKeys),
					Signatures:       parseSignatures(result.Signatures),
					Threshold:        result.Threshold,
					PublicKeyIndices: result.Bitmap,
				},
			},
		}

	case typeMultiAgentSignature:
		var result AptosMultiAgentSignature
		if err := json.Unmarshal(payload, &result); err != nil {
			return nil, fmt.Errorf("failed to unmarshal multi agent signature: %w", err)
		}

		// Parse the sender.
		apiSender, err := p.parseAccountSignature(result.Sender)
		if err != nil {
			return nil, fmt.Errorf("failed to parse sender account signature: %w", err)
		}

		// Parse the secondary signers.
		apiSecondarySigners, err := p.parseAccountSignatures(result.SecondarySigners)
		if err != nil {
			return nil, fmt.Errorf("failed to parse secondary signers: %w", err)
		}

		apiSignature = &api.AptosSignature{
			Type: api.AptosSignature_MULTI_AGENT,
			Signature: &api.AptosSignature_MultiAgent{
				MultiAgent: &api.AptosMultiAgentSignature{
					Sender:                   apiSender,
					SecondarySignerAddresses: result.SecondarySignerAddresses,
					SecondarySigners:         apiSecondarySigners,
				},
			},
		}

	case typeFeePayerSignature:
		var result AptosFeePayerSignature
		if err := json.Unmarshal(payload, &result); err != nil {
			return nil, fmt.Errorf("failed to unmarshal fee payer signature: %w", err)
		}

		// Parse the sender.
		apiSender, err := p.parseAccountSignature(result.Sender)
		if err != nil {
			return nil, fmt.Errorf("failed to parse sender account signature: %w", err)
		}

		// Parse the secondary signers.
		apiSecondarySigners, err := p.parseAccountSignatures(result.SecondarySigners)
		if err != nil {
			return nil, fmt.Errorf("failed to parse secondary signers: %w", err)
		}

		// Parse the fee payer.
		feePayerSender, err := p.parseAccountSignature(result.FeePayerSigner)
		if err != nil {
			return nil, fmt.Errorf("failed to parse fee payer account signature: %w", err)
		}

		apiSignature = &api.AptosSignature{
			Type: api.AptosSignature_FEE_PAYER,
			Signature: &api.AptosSignature_FeePayer{
				FeePayer: &api.AptosFeePayerSignature{
					Sender:                   apiSender,
					SecondarySignerAddresses: result.SecondarySignerAddresses,
					SecondarySigners:         apiSecondarySigners,
					FeePayerSigner:           feePayerSender,
					FeePayerAddress:          result.FeePayerAddress,
				},
			},
		}

	case typeSingleSenderSignature:
		var result AptosSingleSignature
		if err := json.Unmarshal(payload, &result); err != nil {
			return nil, fmt.Errorf("failed to unmarshal single sender signature: %w", err)
		}

		apiSignature = &api.AptosSignature{
			Type: api.AptosSignature_SINGLE_SENDER,
			Signature: &api.AptosSignature_SingleSender{
				SingleSender: &api.AptosSingleSenderSignature{
					PublicKey: result.PublicKey.Value,
					Signature: result.Signature.Value,
				},
			},
		}

	default:
		return nil, fmt.Errorf("failed to parse unknown transaction signature type: %s", sType.Type)
	}

	return apiSignature, nil
}

func (p *aptosNativeParserImpl) parseAccountSignature(signature json.RawMessage) (*api.AptosAccountSignature, error) {
	// We need to first parse the signature type, then we can parse the data into different types of signatures.
	var sType GenericType
	if err := json.Unmarshal(signature, &sType); err != nil {
		return nil, fmt.Errorf("failed to unmarshal signature type: %w", err)
	}

	// Note that, account signature only has two types:
	var apiAccountSignature *api.AptosAccountSignature
	switch sType.Type {
	case typeEd25519Signature:
		var result AptosEd25519Signature
		if err := json.Unmarshal(signature, &result); err != nil {
			return nil, fmt.Errorf("failed to unmarshal ed25519 signature: %w", err)
		}
		apiAccountSignature = &api.AptosAccountSignature{
			Type: api.AptosAccountSignature_ED25519,
			Signature: &api.AptosAccountSignature_Ed25519{
				Ed25519: &api.AptosEd25519Signature{
					PublicKey: result.PublicKey.Value,
					Signature: result.Signature.Value,
				},
			},
		}
	case typeSingleKeySignature:
		var result AptosSingleKeySignature
		if err := json.Unmarshal(signature, &result); err != nil {
			return nil, fmt.Errorf("failed to unmarshal single key signature: %w", err)
		}
		apiAccountSignature = &api.AptosAccountSignature{
			Type: api.AptosAccountSignature_SINGLE_KEY,
			Signature: &api.AptosAccountSignature_SingleKey{
				SingleKey: &api.AptosSingleKeySignature{
					PublicKey: result.PublicKey.Value,
					Signature: result.Signature.Value,
				},
			},
		}
	case typeMultiEd25519Signature:
		var result AptosMultiEd25519Signature
		if err := json.Unmarshal(signature, &result); err != nil {
			return nil, fmt.Errorf("failed to unmarshal multi ed25519 signature: %w", err)
		}
		apiAccountSignature = &api.AptosAccountSignature{
			Type: api.AptosAccountSignature_MULTI_ED25519,
			Signature: &api.AptosAccountSignature_MultiEd25519{
				MultiEd25519: &api.AptosMultiEd25519Signature{
					PublicKeys:       parsePublicKeys(result.PublicKeys),
					Signatures:       parseSignatures(result.Signatures),
					Threshold:        result.Threshold,
					PublicKeyIndices: result.Bitmap,
				},
			},
		}
	case typeMultiKeySignature:
		var result AptosMultiKeySignature
		if err := json.Unmarshal(signature, &result); err != nil {
			return nil, fmt.Errorf("failed to unmarshal multi ed25519 signature: %w", err)
		}

		apiAccountSignature = &api.AptosAccountSignature{
			Type: api.AptosAccountSignature_MULTI_KEY,
			Signature: &api.AptosAccountSignature_MultiKey{
				MultiKey: &api.AptosMultiKeySignature{
					PublicKeys:         parsePublicKeys(result.PublicKeys),
					Signatures:         parseSignatures(result.Signatures),
					SignaturesRequired: result.SignaturesRequired,
				},
			},
		}
	default:
		return nil, fmt.Errorf("failed to parse unknown account signature type: %s", sType.Type)
	}

	return apiAccountSignature, nil
}

func parsePublicKeys(publicKeys []AptosPublicKey) []string {
	keys := make([]string, len(publicKeys))
	for i, key := range publicKeys {
		keys[i] = key.Value
	}
	return keys
}

func parseSignatures(signature []AptosSignature) []string {
	signatures := make([]string, len(signature))
	for i, s := range signature {
		signatures[i] = s.Value
	}
	return signatures
}

func (p *aptosNativeParserImpl) parseAccountSignatures(signatures []json.RawMessage) ([]*api.AptosAccountSignature, error) {
	results := make([]*api.AptosAccountSignature, len(signatures))
	for i, s := range signatures {
		result, err := p.parseAccountSignature(s)
		if err != nil {
			return nil, fmt.Errorf("failed to parse account signature: %w", err)
		}

		results[i] = result
	}

	return results, nil
}

func (p *aptosNativeParserImpl) parseGenesisTransaction(blockHeight uint64, transactionInfo *AptosTransactionInfo, data json.RawMessage) (*api.AptosTransaction, error) {
	var genesisT GenesisTransaction
	if err := json.Unmarshal(data, &genesisT); err != nil {
		return nil, fmt.Errorf("failed to unmarshal genesis transaction: %w", err)
	}

	// Parse transaction info
	apiTransactionInfo, err := p.parseTransactionInfo(transactionInfo)
	if err != nil {
		return nil, fmt.Errorf("failed to parse transaction info: %w", err)
	}

	// Genesis transaction's type has to be typeWriteSetPayload. Verify this.
	if genesisT.Payload.Type != typeWriteSetPayload {
		return nil, fmt.Errorf("failed to parse genesis transaction payload, the type is: %s", genesisT.Payload.Type)
	}

	// Parse the write set payload
	apiPayload, err := p.parseWriteSet(genesisT.Payload.WriteSet)
	if err != nil {
		return nil, fmt.Errorf("failed to parse write set payload: %w", err)
	}

	// Parse events
	events, err := p.parseEvents(genesisT.Events)
	if err != nil {
		return nil, fmt.Errorf("failed to parse events: %w", err)
	}

	// Construct api.AptosGenesisTransaction
	apiGenesis := &api.AptosGenesisTransaction{
		Payload: apiPayload,
		Events:  events,
	}

	// Construct the api.AptosTransaction
	return &api.AptosTransaction{
		Info: apiTransactionInfo,
		// Note that, GenesisTransaction doesn't have a timestamp in the block. We use the block time 0 as the
		// transaction timestamp.
		Timestamp:   p.parseTimestamp(0),
		Version:     transactionInfo.Version.Value(),
		BlockHeight: blockHeight,
		Type:        api.AptosTransaction_GENESIS,
		TxnData: &api.AptosTransaction_Genesis{
			Genesis: apiGenesis,
		},
	}, nil
}
