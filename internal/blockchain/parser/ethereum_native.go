package parser

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"regexp"
	"strconv"
	"strings"

	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/go-playground/validator/v10"
	"github.com/golang/protobuf/ptypes/timestamp"
	"github.com/uber-go/tally"
	"go.uber.org/zap"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/blockchain/types"
	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/utils/log"
	"github.com/coinbase/chainstorage/protos/coinbase/c3/common"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

type (
	EthereumHexString   string
	EthereumQuantity    uint64
	EthereumBigQuantity big.Int
	EthereumBigFloat    big.Float

	EthereumBlock struct {
		Hash             EthereumHexString      `json:"hash" validate:"required"`
		ParentHash       EthereumHexString      `json:"parentHash" validate:"required"`
		Number           EthereumQuantity       `json:"number"`
		Timestamp        EthereumQuantity       `json:"timestamp" validate:"required_with=Number"`
		Transactions     []*EthereumTransaction `json:"transactions"`
		Nonce            EthereumHexString      `json:"nonce"`
		Sha3Uncles       EthereumHexString      `json:"sha3Uncles"`
		LogsBloom        EthereumHexString      `json:"logsBloom"`
		TransactionsRoot EthereumHexString      `json:"transactionsRoot"`
		StateRoot        EthereumHexString      `json:"stateRoot"`
		ReceiptsRoot     EthereumHexString      `json:"receiptsRoot"`
		Miner            EthereumHexString      `json:"miner"`
		Difficulty       EthereumQuantity       `json:"difficulty"`
		TotalDifficulty  EthereumBigQuantity    `json:"totalDifficulty"`
		ExtraData        EthereumHexString      `json:"extraData"`
		Size             EthereumQuantity       `json:"size"`
		GasLimit         EthereumQuantity       `json:"gasLimit"`
		GasUsed          EthereumQuantity       `json:"gasUsed"`
		Uncles           []EthereumHexString    `json:"uncles"`
		// The EIP-1559 base fee for the block, if it exists.
		BaseFeePerGas *EthereumQuantity `json:"baseFeePerGas"`
		ExtraHeader   PolygonHeader     `json:"extraHeader"`

		// EIP-4895 introduces new fields in the execution payload
		// https://eips.ethereum.org/EIPS/eip-4895
		// Note that the unit of withdrawal `amount` is in Gwei (1e9 wei).
		Withdrawals     []*EthereumWithdrawal `json:"withdrawals"`
		WithdrawalsRoot EthereumHexString     `json:"withdrawalsRoot"`
	}

	PolygonHeader struct {
		Author EthereumHexString `json:"author"`
	}

	// EthereumBlockLit is a light version of EthereumBlock.
	// Fields not used during data ingestion are removed.
	EthereumBlockLit struct {
		Hash         EthereumHexString         `json:"hash" validate:"required"`
		ParentHash   EthereumHexString         `json:"parentHash" validate:"required"`
		Number       EthereumQuantity          `json:"number"`
		Transactions []*EthereumTransactionLit `json:"transactions"`
		Uncles       []EthereumHexString       `json:"uncles"`
		Timestamp    EthereumQuantity          `json:"timestamp"`
	}

	EthereumWithdrawal struct {
		Index          EthereumQuantity  `json:"index"`
		ValidatorIndex EthereumQuantity  `json:"validatorIndex"`
		Address        EthereumHexString `json:"address"`
		Amount         EthereumQuantity  `json:"amount"`
	}

	EthereumTransactionAccess struct {
		Address     EthereumHexString   `json:"address"`
		StorageKeys []EthereumHexString `json:"storageKeys"`
	}

	EthereumTransaction struct {
		BlockHash   EthereumHexString   `json:"blockHash"`
		BlockNumber EthereumQuantity    `json:"blockNumber"`
		From        EthereumHexString   `json:"from"`
		Gas         EthereumQuantity    `json:"gas"`
		GasPrice    EthereumBigQuantity `json:"gasPrice"`
		Hash        EthereumHexString   `json:"hash"`
		Input       EthereumHexString   `json:"input"`
		To          EthereumHexString   `json:"to"`
		Index       EthereumQuantity    `json:"transactionIndex"`
		Value       EthereumBigQuantity `json:"value"`
		Nonce       EthereumQuantity    `json:"nonce"`
		// The EIP-2718 type of the transaction
		Type EthereumQuantity `json:"type"`
		// The EIP-1559 related fields
		MaxFeePerGas         *EthereumQuantity             `json:"maxFeePerGas"`
		MaxPriorityFeePerGas *EthereumQuantity             `json:"maxPriorityFeePerGas"`
		AccessList           *[]*EthereumTransactionAccess `json:"accessList"`
	}

	// EthereumTransactionLit is a light version of EthereumTransaction.
	// Fields not used during data ingestion are removed.
	EthereumTransactionLit struct {
		Hash EthereumHexString `json:"hash"`
		From EthereumHexString `json:"from"`
		To   EthereumHexString `json:"to"`
	}

	// Used by the custom unmarshaler of EthereumTransactionLit.
	ethereumTransactionLit struct {
		Hash EthereumHexString `json:"hash"`
		From EthereumHexString `json:"from"`
		To   EthereumHexString `json:"to"`
	}

	EthereumTransactionReceipt struct {
		TransactionHash   EthereumHexString    `json:"transactionHash"`
		TransactionIndex  EthereumQuantity     `json:"transactionIndex"`
		BlockHash         EthereumHexString    `json:"blockHash"`
		BlockNumber       EthereumQuantity     `json:"blockNumber"`
		From              EthereumHexString    `json:"from"`
		To                EthereumHexString    `json:"to"`
		CumulativeGasUsed EthereumQuantity     `json:"cumulativeGasUsed"`
		GasUsed           EthereumQuantity     `json:"gasUsed"`
		ContractAddress   EthereumHexString    `json:"contractAddress"`
		Logs              []*EthereumEventLog  `json:"logs"`
		LogsBloom         EthereumHexString    `json:"logsBloom"`
		Root              EthereumHexString    `json:"root"`
		Status            *EthereumQuantity    `json:"status"`
		Type              EthereumQuantity     `json:"type"`
		EffectiveGasPrice *EthereumQuantity    `json:"effectiveGasPrice"`
		GasUsedForL1      *EthereumQuantity    `json:"gasUsedForL1"` // For Arbitrum network https://github.com/OffchainLabs/arbitrum/blob/6ca0d163417470b9d2f7eea930c3ad71d702c0b2/packages/arb-evm/evm/result.go#L336
		L1GasUsed         *EthereumBigQuantity `json:"l1GasUsed"`    // For Optimism and Base networks https://github.com/ethereum-optimism/optimism/blob/3c3e1a88b234a68bcd59be0c123d9f3cc152a91e/l2geth/core/types/receipt.go#L73
		L1GasPrice        *EthereumBigQuantity `json:"l1GasPrice"`   //
		L1Fee             *EthereumBigQuantity `json:"l1Fee"`        //
		L1FeeScaler       *EthereumBigFloat    `json:"l1FeeScalar"`  //
	}

	EthereumTransactionReceiptLit struct {
		BlockHash EthereumHexString `json:"blockHash"`
	}

	EthereumEventLog struct {
		Removed          bool                `json:"removed"`
		LogIndex         EthereumQuantity    `json:"logIndex"`
		TransactionHash  EthereumHexString   `json:"transactionHash"`
		TransactionIndex EthereumQuantity    `json:"transactionIndex"`
		BlockHash        EthereumHexString   `json:"blockHash"`
		BlockNumber      EthereumQuantity    `json:"blockNumber"`
		Address          EthereumHexString   `json:"address"`
		Data             EthereumHexString   `json:"data"`
		Topics           []EthereumHexString `json:"topics"`
	}

	EthereumTransactionTrace struct {
		Error   string                     `json:"error"`
		Type    string                     `json:"type"`
		From    EthereumHexString          `json:"from"`
		To      EthereumHexString          `json:"to"`
		Value   EthereumBigQuantity        `json:"value"`
		Gas     EthereumQuantity           `json:"gas"`
		GasUsed EthereumQuantity           `json:"gasUsed"`
		Input   EthereumHexString          `json:"input"`
		Output  EthereumHexString          `json:"output"`
		Calls   []EthereumTransactionTrace `json:"calls"`
	}

	ParityTransactionTrace struct {
		Error               string            `json:"error"`
		Action              ParityTraceAction `json:"action"`
		Result              ParityTraceResult `json:"result"`
		Subtraces           uint64            `json:"subtraces"`
		TraceAddress        []uint64          `json:"traceAddress"`
		TraceType           string            `json:"type"`
		BlockHash           EthereumHexString `json:"blockHash"`
		BlockNumber         EthereumQuantity  `json:"blockNumber"`
		TransactionHash     EthereumHexString `json:"transactionHash"`
		TransactionPosition EthereumQuantity  `json:"transactionPosition"`
	}

	ParityTraceAction struct {
		CallType string              `json:"callType"`
		From     EthereumHexString   `json:"from"`
		Gas      EthereumQuantity    `json:"gas"`
		Input    EthereumHexString   `json:"input"`
		To       EthereumHexString   `json:"to"`
		Value    EthereumBigQuantity `json:"value"`
		Type     string              `json:"type"`
	}

	ParityTraceResult struct {
		GasUsed EthereumQuantity  `json:"gasUsed"`
		Output  EthereumHexString `json:"output"`
	}

	ethereumNativeParserImpl struct {
		Logger    *zap.Logger
		validate  *validator.Validate
		nodeType  types.EthereumNodeType
		traceType types.TraceType
		config    *config.Config
		metrics   *ethereumNativeParserMetrics
	}

	ethereumParserOptions struct {
		nodeType        types.EthereumNodeType
		traceType       types.TraceType
		checksumAddress bool
	}

	nestedParityTrace struct {
		TraceId        string
		TraceInfo      *api.EthereumTransactionFlattenedTrace
		ChildrenTraces []*nestedParityTrace
	}

	ethereumNativeParserMetrics struct {
		gasPriceOutOfRangeCounter tally.Counter
	}
)

var (
	// Ref: https://github.com/blockchain-etl/ethereum-etl/blob/25fc768f39d6a3175f8175d42b7699eb33166ebf/ethereumetl/mappers/trace_mapper.py#L151
	TraceCallTypeMap = map[string]bool{
		"CALL":         true,
		"CALLCODE":     true,
		"DELEGATECALL": true,
		"STATICCALL":   true,
	}

	ethereumValidDataPattern = regexp.MustCompile("(?:0[xX])?[0-9a-fA-F]{64}")
)

const (
	// TransferEventTopic is the hash of the Transfer event:
	// ERC-20: Transfer(address indexed _from, address indexed _to, uint256 _value)
	// ERC-721: Transfer(address indexed _from, address indexed _to, uint256 indexed _tokenId)
	TransferEventTopic = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"

	ethNullAddress = "0x0000000000000000000000000000000000000000"

	parserMetricsReasonKey    = "reason"
	gasPriceOutOfRangeFailure = "gas_price_out_of_range"
	parseFailure              = "parse_failure"
)

func (v EthereumHexString) MarshalJSON() ([]byte, error) {
	s := fmt.Sprintf(`"%s"`, v)
	return []byte(s), nil
}

func (v *EthereumHexString) UnmarshalJSON(input []byte) error {
	var s string
	if err := json.Unmarshal(input, &s); err != nil {
		return xerrors.Errorf("failed to unmarshal EthereumHexString: %w", err)
	}
	s = strings.ToLower(s)

	*v = EthereumHexString(s)
	return nil
}

func (v EthereumHexString) Value() string {
	return string(v)
}

func (v EthereumQuantity) MarshalJSON() ([]byte, error) {
	s := fmt.Sprintf(`"%s"`, hexutil.EncodeUint64(uint64(v)))
	return []byte(s), nil
}

func (v *EthereumQuantity) UnmarshalJSON(input []byte) error {
	if len(input) > 0 && input[0] != '"' {
		var i uint64
		if err := json.Unmarshal(input, &i); err != nil {
			return xerrors.Errorf("failed to unmarshal EthereumQuantity into uint64: %w", err)
		}

		*v = EthereumQuantity(i)
		return nil
	}

	var s string
	if err := json.Unmarshal(input, &s); err != nil {
		return xerrors.Errorf("failed to unmarshal EthereumQuantity into string: %w", err)
	}

	if s == "" {
		*v = 0
		return nil
	}

	i, err := hexutil.DecodeUint64(s)
	if err != nil {
		return xerrors.Errorf("failed to decode EthereumQuantity %v: %w", s, err)
	}

	*v = EthereumQuantity(i)
	return nil
}

func (v EthereumQuantity) Value() uint64 {
	return uint64(v)
}

func (v EthereumBigQuantity) MarshalJSON() ([]byte, error) {
	bi := big.Int(v)
	s := fmt.Sprintf(`"%s"`, hexutil.EncodeBig(&bi))
	return []byte(s), nil
}

func (v *EthereumBigQuantity) UnmarshalJSON(input []byte) error {
	var s string
	if err := json.Unmarshal(input, &s); err != nil {
		return xerrors.Errorf("failed to unmarshal EthereumBigQuantity: %w", err)
	}

	if s == "" {
		*v = EthereumBigQuantity{}
		return nil
	}

	i, err := hexutil.DecodeBig(s)
	if err != nil {
		return xerrors.Errorf("failed to decode EthereumBigQuantity %v: %w", s, err)
	}

	*v = EthereumBigQuantity(*i)
	return nil
}

func (v EthereumBigQuantity) Value() string {
	i := big.Int(v)
	return i.String()
}

func (v EthereumBigQuantity) Uint64() (uint64, error) {
	i := big.Int(v)
	if !i.IsUint64() {
		return 0, xerrors.Errorf("failed to parse EthereumBigQuantity to uint64 %v", v.Value())
	}
	return i.Uint64(), nil
}

func (v EthereumBigFloat) MarshalJSON() ([]byte, error) {
	bf := big.Float(v)
	s := fmt.Sprintf(`"%s"`, bf.String())
	return []byte(s), nil
}

func (v *EthereumBigFloat) UnmarshalJSON(input []byte) error {
	var s string
	if err := json.Unmarshal(input, &s); err != nil {
		return xerrors.Errorf("failed to unmarshal EthereumBigFloat: %w", err)
	}

	if s == "" {
		*v = EthereumBigFloat{}
		return nil
	}

	scalar := new(big.Float)
	scalar, ok := scalar.SetString(s)
	if !ok {
		return xerrors.Errorf("cannot parse EthereumBigFloat")
	}

	*v = EthereumBigFloat(*scalar)
	return nil
}

func (v EthereumBigFloat) Value() string {
	f := big.Float(v)
	return f.String()
}

func (v *EthereumTransactionLit) UnmarshalJSON(input []byte) error {
	// The transaction may be a string or a struct, depending on whether full transaction objects are requested.
	if len(input) > 0 && input[0] == '"' {
		return json.Unmarshal(input, &v.Hash)
	}

	// Use a different struct to avoid calling this custom unmarshaler recursively.
	var out ethereumTransactionLit
	if err := json.Unmarshal(input, &out); err != nil {
		return xerrors.Errorf("failed to unmarshal struct: %w", err)
	}

	v.Hash = out.Hash
	v.From = out.From
	v.To = out.To
	return nil
}

func NewEthereumNativeParser(params ParserParams, opts ...ParserFactoryOption) (NativeParser, error) {
	options := &ethereumParserOptions{
		nodeType:  types.EthereumNodeType_ARCHIVAL,
		traceType: types.TraceType_GETH,
	}
	for _, opt := range opts {
		opt(options)
	}

	return &ethereumNativeParserImpl{
		Logger:    log.WithPackage(params.Logger),
		validate:  validator.New(),
		nodeType:  options.nodeType,
		traceType: options.traceType,
		config:    params.Config,
		metrics:   newEthereumNativeParserMetrics(params.Metrics),
	}, nil
}

func newEthereumNativeParserMetrics(scope tally.Scope) *ethereumNativeParserMetrics {
	scope = scope.SubScope("ethereum_native_parser")
	return &ethereumNativeParserMetrics{
		gasPriceOutOfRangeCounter: newEthereumNativeParserCounter(scope, gasPriceOutOfRangeFailure),
	}
}

func newEthereumNativeParserCounter(scope tally.Scope, reason string) tally.Counter {
	return scope.Tagged(map[string]string{parserMetricsReasonKey: reason}).Counter(parseFailure)
}

func WithEthereumNodeType(nodeType types.EthereumNodeType) ParserFactoryOption {
	return func(options interface{}) {
		if v, ok := options.(*ethereumParserOptions); ok {
			v.nodeType = nodeType
		}
	}
}

func WithTraceType(traceType types.TraceType) ParserFactoryOption {
	return func(options interface{}) {
		if v, ok := options.(*ethereumParserOptions); ok {
			v.traceType = traceType
		}
	}
}

func WithEthereumChecksumAddress() ParserFactoryOption {
	return func(options interface{}) {
		if v, ok := options.(*ethereumParserOptions); ok {
			v.checksumAddress = true
		}
	}
}

func (p *ethereumNativeParserImpl) ParseBlock(ctx context.Context, rawBlock *api.Block) (*api.NativeBlock, error) {
	metadata := rawBlock.GetMetadata()
	if metadata == nil {
		return nil, xerrors.New("metadata not found")
	}

	blobdata := rawBlock.GetEthereum()
	if blobdata == nil {
		return nil, xerrors.New("blobdata not found")
	}

	header, transactions, err := p.parseHeader(blobdata.Header)
	if err != nil {
		return nil, xerrors.Errorf("failed to parse header: %w", err)
	}

	numTransactions := len(header.Transactions)
	if numTransactions != len(transactions) {
		return nil, xerrors.Errorf("unexpected number of transactions: expected=%v actual=%v", numTransactions, len(transactions))
	}

	var transactionReceipts []*api.EthereumTransactionReceipt
	if p.nodeType.ReceiptsEnabled() {
		transactionReceipts, err = p.parseTransactionReceipts(blobdata, transactions)
		if err != nil {
			return nil, xerrors.Errorf("failed to parse transaction receipts: %w", err)
		}
		if numTransactions != len(transactionReceipts) {
			return nil, xerrors.Errorf("unexpected number of transaction receipts: expected=%v actual=%v", numTransactions, len(transactionReceipts))
		}
		for i, transactionReceipt := range transactionReceipts {
			transaction := transactions[i]
			if transactionReceipt.TransactionHash != transaction.Hash ||
				transactionReceipt.TransactionIndex != transaction.Index ||
				transactionReceipt.BlockHash != header.Hash ||
				transactionReceipt.BlockNumber != header.Number {
				return nil, xerrors.Errorf("unexpected transaction receipt: transactionReceipt={%+v} transaction={%+v} block={%+v}", transactionReceipt, transaction, header)
			}
		}
	} else {
		transactionReceipts = make([]*api.EthereumTransactionReceipt, numTransactions)
	}

	var transactionTraces []*api.EthereumTransactionTrace
	if p.nodeType.TracesEnabled() && !p.traceType.ParityTraceEnabled() {
		transactionTraces, err = p.parseTransactionTraces(blobdata, transactions)
		if err != nil {
			return nil, xerrors.Errorf("failed to parse transaction traces: %w", err)
		}
	} else {
		transactionTraces = make([]*api.EthereumTransactionTrace, numTransactions)
	}

	if numTransactions != len(transactionTraces) {
		return nil, xerrors.Errorf("unexpected number of transaction traces: expected=%v actual=%v", numTransactions, len(transactionTraces))
	}

	tokenTransfers, err := p.parseTokenTransfers(transactionReceipts)
	if err != nil {
		return nil, xerrors.Errorf("failed to parse token transfer: %w", err)
	}
	if numTransactions != len(tokenTransfers) {
		return nil, xerrors.Errorf("unexpected number of token transfers: expected=%v actual=%v", numTransactions, len(tokenTransfers))
	}

	transactionToFlattenedTracesMap := make(map[string][]*api.EthereumTransactionFlattenedTrace, 0)
	if p.traceType.ParityTraceEnabled() {
		if err := p.parseTransactionFlattenedParityTraces(blobdata, transactionToFlattenedTracesMap); err != nil {
			return nil, xerrors.Errorf("failed to parse transaction parity traces: %w", err)
		}
	}

	for i, transaction := range transactions {
		transaction.Receipt = transactionReceipts[i]
		transaction.TokenTransfers = tokenTransfers[i]

		transactionTrace := transactionTraces[i]

		// Parse transaction flattened traces
		var transactionFlattenedTraces []*api.EthereumTransactionFlattenedTrace
		if p.traceType.ParityTraceEnabled() {
			if _, ok := transactionToFlattenedTracesMap[transaction.Hash]; !ok {
				continue
			}
			transactionFlattenedTraces = transactionToFlattenedTracesMap[transaction.Hash]
		} else {
			transactionFlattenedTraces = p.parseTransactionFlattenedTraces(transactionTrace, transaction, "", []uint64{})
		}
		transaction.FlattenedTraces = transactionFlattenedTraces
	}

	uncles, err := p.parseUncles(blobdata)
	if err != nil {
		return nil, xerrors.Errorf("failed to parse uncles from block blobdata: %w", err)
	}

	nativeBlock := &api.NativeBlock{
		Blockchain:      rawBlock.Blockchain,
		Network:         rawBlock.Network,
		Tag:             metadata.Tag,
		Hash:            metadata.Hash,
		ParentHash:      metadata.ParentHash,
		Height:          metadata.Height,
		ParentHeight:    metadata.ParentHeight,
		Timestamp:       header.Timestamp,
		NumTransactions: uint64(len(transactions)),
		Block: &api.NativeBlock_Ethereum{
			Ethereum: &api.EthereumBlock{
				Header:       header,
				Transactions: transactions,
				Uncles:       uncles,
			},
		},
	}

	// TODO: Remove this logic once this PR(https://github.com/maticnetwork/bor/pull/435) gets released,
	// So that we can directly get Author data from eth_getBlockByNumber API call
	if p.config.Chain.Blockchain == common.Blockchain_BLOCKCHAIN_POLYGON {
		authorData := blobdata.GetPolygon()
		// null check for backward compatibility.
		if authorData == nil {
			return nativeBlock, nil
		}

		var author string
		if err := json.Unmarshal(authorData.Author, &author); err != nil {
			return nil, xerrors.Errorf("failed to parse polygon author data on unmarshal: %w", err)
		}

		ethereumBlock := nativeBlock.GetEthereum()
		miner := ethereumBlock.Header.Miner
		if miner != "" && miner != ethNullAddress && miner != author {
			return nil, xerrors.Errorf("polygon: header mismatch between original miner(%s) and author(%s)", miner, author)
		}

		ethereumBlock.Header.Miner = author
	}

	return nativeBlock, nil
}

func (p *ethereumNativeParserImpl) parseHeader(data []byte) (*api.EthereumHeader, []*api.EthereumTransaction, error) {
	var block EthereumBlock
	if err := json.Unmarshal(data, &block); err != nil {
		return nil, nil, xerrors.Errorf("failed to parse block header on unmarshal: %w", err)
	}

	if err := p.validate.Struct(block); err != nil {
		return nil, nil, xerrors.Errorf("failed to parse block header on struct validate: %w", err)
	}

	network := p.config.Chain.Network

	transactionHashes := make([]string, len(block.Transactions))
	transactions := make([]*api.EthereumTransaction, len(block.Transactions))
	for i, transaction := range block.Transactions {
		transactionHashes[i] = transaction.Hash.Value()
		transactions[i] = &api.EthereumTransaction{
			BlockHash:      transaction.BlockHash.Value(),
			BlockNumber:    transaction.BlockNumber.Value(),
			From:           transaction.From.Value(),
			Gas:            transaction.Gas.Value(),
			Hash:           transaction.Hash.Value(),
			Input:          transaction.Input.Value(),
			Nonce:          transaction.Nonce.Value(),
			To:             transaction.To.Value(),
			Index:          transaction.Index.Value(),
			Value:          transaction.Value.Value(),
			Type:           transaction.Type.Value(),
			BlockTimestamp: &timestamp.Timestamp{Seconds: int64(block.Timestamp.Value())},
		}
		gasPrice, err := transaction.GasPrice.Uint64()
		if err != nil {
			// Ignore parse error for ethereum testnets and arbitrum
			// For arbitrum, see this example -> https://arbiscan.io/tx/0x4f30e3dfcd3ec55aa6a12284000598b728df29a5c561a3f81707eed155b288da
			if network == common.Network_NETWORK_ETHEREUM_GOERLI ||
				network == common.Network_NETWORK_ARBITRUM_MAINNET {
				gasPrice = math.MaxUint64

				p.metrics.gasPriceOutOfRangeCounter.Inc(1)
				p.Logger.Error("gas price is out of max range",
					zap.String("transaction hash", transaction.Hash.Value()),
				)
			} else {
				return nil, nil, xerrors.Errorf("failed to parse transaction GasPrice to uint64 %v", transaction.GasPrice.Value())
			}
		}
		transactions[i].GasPrice = gasPrice

		if transaction.MaxFeePerGas != nil {
			transactions[i].OptionalMaxFeePerGas = &api.EthereumTransaction_MaxFeePerGas{
				MaxFeePerGas: transaction.MaxFeePerGas.Value(),
			}
		}
		if transaction.MaxPriorityFeePerGas != nil {
			transactions[i].OptionalMaxPriorityFeePerGas = &api.EthereumTransaction_MaxPriorityFeePerGas{
				MaxPriorityFeePerGas: transaction.MaxPriorityFeePerGas.Value(),
			}
		}

		// Calculate PriorityFeePerGas Instead of using `effectiveGasPrice - baseFeePerGas` since effectiveGasPrice is in receipts only
		// Ref: https://medium.com/liquity/how-eip-1559-changes-the-transaction-fees-of-ethereum-47a6c513050c
		if block.BaseFeePerGas != nil {
			var priorityFeePerGas uint64
			if transaction.MaxFeePerGas != nil && transaction.MaxPriorityFeePerGas != nil {
				// EIP-1559 transaction
				priorityFeePerGas = transaction.MaxFeePerGas.Value() - block.BaseFeePerGas.Value()
				maxPriorityFeePerGas := transaction.MaxPriorityFeePerGas.Value()
				if priorityFeePerGas > maxPriorityFeePerGas {
					priorityFeePerGas = maxPriorityFeePerGas
				}
			} else {
				// Legacy transaction where effectiveGasPrice = gasPrice
				priorityFeePerGas = gasPrice - block.BaseFeePerGas.Value()
			}
			transactions[i].OptionalPriorityFeePerGas = &api.EthereumTransaction_PriorityFeePerGas{
				PriorityFeePerGas: priorityFeePerGas,
			}
		}

		if transaction.AccessList != nil {
			transactions[i].OptionalTransactionAccessList = &api.EthereumTransaction_TransactionAccessList{
				TransactionAccessList: &api.EthereumTransactionAccessList{
					AccessList: p.parseTransactionAccessList(transaction),
				},
			}
		}
	}
	uncles := p.copyEthereumHexStrings(block.Uncles)
	withdrawals := p.parseWithdrawals(block.Withdrawals)
	header := &api.EthereumHeader{
		Hash:             block.Hash.Value(),
		ParentHash:       block.ParentHash.Value(),
		Number:           block.Number.Value(),
		Timestamp:        &timestamp.Timestamp{Seconds: int64(block.Timestamp.Value())},
		Transactions:     transactionHashes,
		Nonce:            block.Nonce.Value(),
		Sha3Uncles:       block.Sha3Uncles.Value(),
		LogsBloom:        block.LogsBloom.Value(),
		TransactionsRoot: block.TransactionsRoot.Value(),
		StateRoot:        block.StateRoot.Value(),
		ReceiptsRoot:     block.ReceiptsRoot.Value(),
		Miner:            block.Miner.Value(),
		Difficulty:       block.Difficulty.Value(),
		TotalDifficulty:  block.TotalDifficulty.Value(),
		ExtraData:        block.ExtraData.Value(),
		Size:             block.Size.Value(),
		GasLimit:         block.GasLimit.Value(),
		GasUsed:          block.GasUsed.Value(),
		Uncles:           uncles,
		Withdrawals:      withdrawals,
		WithdrawalsRoot:  block.WithdrawalsRoot.Value(),
	}
	if block.BaseFeePerGas != nil {
		header.OptionalBaseFeePerGas = &api.EthereumHeader_BaseFeePerGas{
			BaseFeePerGas: block.BaseFeePerGas.Value(),
		}
	}

	return header, transactions, nil
}

func (p *ethereumNativeParserImpl) parseTransactionReceipts(blobdata *api.EthereumBlobdata, transactions []*api.EthereumTransaction) ([]*api.EthereumTransactionReceipt, error) {
	numReceipts := len(blobdata.TransactionReceipts)
	if numReceipts != len(transactions) {
		return nil, xerrors.Errorf("number of receipts and transactions do not match expected=%v actual=%v", len(transactions), numReceipts)
	}

	receipts := make([]*api.EthereumTransactionReceipt, numReceipts)
	for i, rawReceipt := range blobdata.TransactionReceipts {
		var receipt EthereumTransactionReceipt
		if err := json.Unmarshal(rawReceipt, &receipt); err != nil {
			return nil, xerrors.Errorf("failed to parse receipt: %w", err)
		}

		receipts[i] = &api.EthereumTransactionReceipt{
			TransactionHash:   receipt.TransactionHash.Value(),
			TransactionIndex:  receipt.TransactionIndex.Value(),
			BlockHash:         receipt.BlockHash.Value(),
			BlockNumber:       receipt.BlockNumber.Value(),
			From:              receipt.From.Value(),
			To:                receipt.To.Value(),
			CumulativeGasUsed: receipt.CumulativeGasUsed.Value(),
			GasUsed:           receipt.GasUsed.Value(),
			ContractAddress:   receipt.ContractAddress.Value(),
			Logs:              p.parseEventLogs(&receipt),
			LogsBloom:         receipt.LogsBloom.Value(),
			Type:              receipt.Type.Value(),
		}
		if receipt.Status != nil {
			receipts[i].OptionalStatus = &api.EthereumTransactionReceipt_Status{
				Status: receipt.Status.Value(),
			}
		}

		if receipt.L1GasUsed != nil {
			l1GasUsed, err := receipt.L1GasUsed.Uint64()
			if err != nil {
				return nil, xerrors.Errorf("failed to parse receipt L1GasUsed to uint64 %v", receipt.L1GasUsed.Value())
			}

			feeScalar := ""
			if receipt.L1FeeScaler != nil && receipt.L1FeeScaler.Value() != "0" {
				feeScalar = receipt.L1FeeScaler.Value()
			}

			optionalL1FeeInfo := &api.EthereumTransactionReceipt_L1FeeInfo_{
				L1FeeInfo: &api.EthereumTransactionReceipt_L1FeeInfo{
					L1GasUsed:   l1GasUsed,
					L1FeeScalar: feeScalar,
				},
			}
			receipts[i].OptionalL1FeeInfo = optionalL1FeeInfo

			if receipt.L1GasPrice != nil {
				l1GasPrice, err := receipt.L1GasPrice.Uint64()
				if err != nil {
					return nil, xerrors.Errorf("failed to parse receipt L1GasPrice to uint64 %v", receipt.L1GasPrice.Value())
				}

				optionalL1FeeInfo.L1FeeInfo.L1GasPrice = l1GasPrice
			}

			if receipt.L1Fee != nil {
				l1Fee, err := receipt.L1Fee.Uint64()
				if err != nil {
					return nil, xerrors.Errorf("failed to parse receipt L1Fee to uint64 %v", receipt.L1Fee.Value())
				}

				optionalL1FeeInfo.L1FeeInfo.L1Fee = l1Fee
			}

		}

		if receipt.GasUsedForL1 != nil {
			receipts[i].OptionalL1FeeInfo = &api.EthereumTransactionReceipt_L1FeeInfo_{
				L1FeeInfo: &api.EthereumTransactionReceipt_L1FeeInfo{
					L1GasUsed: receipt.GasUsedForL1.Value(),
				},
			}
		}

		// Field effectiveGasPrice is added to the eth_getTransactionReceipt call for EIP-1559.
		// Pre-London, it is equal to the transaction’s gasPrice.
		// Post-London, it is equal to the actual gas price paid for inclusion.
		// Since it's hard to backfill all old blocks, set `effectiveGasPrice` as gasPrice for Pre-London blocks.
		// Ref: https://hackmd.io/@timbeiko/1559-json-rpc
		// Geth code: https://github.com/ethereum/go-ethereum/blob/57feabea663496109e59df669238398239438fb1/internal/ethapi/api.go#L1641
		if receipt.EffectiveGasPrice != nil {
			receipts[i].EffectiveGasPrice = receipt.EffectiveGasPrice.Value()
		} else {
			receipts[i].EffectiveGasPrice = transactions[i].GasPrice
		}
	}

	return receipts, nil
}

func (p *ethereumNativeParserImpl) parseTransactionAccessList(transaction *EthereumTransaction) []*api.EthereumTransactionAccess {
	accessList := make([]*api.EthereumTransactionAccess, len(*transaction.AccessList))
	for i, access := range *transaction.AccessList {
		accessList[i] = &api.EthereumTransactionAccess{
			Address:     access.Address.Value(),
			StorageKeys: p.copyEthereumHexStrings(access.StorageKeys),
		}
	}

	return accessList
}

func (p *ethereumNativeParserImpl) parseWithdrawals(withdrawals []*EthereumWithdrawal) []*api.EthereumWithdrawal {
	result := make([]*api.EthereumWithdrawal, len(withdrawals))
	for i, withdrawal := range withdrawals {
		result[i] = &api.EthereumWithdrawal{
			Index:          withdrawal.Index.Value(),
			ValidatorIndex: withdrawal.ValidatorIndex.Value(),
			Address:        withdrawal.Address.Value(),
			Amount:         withdrawal.Amount.Value(),
		}
	}
	return result
}

func (p *ethereumNativeParserImpl) parseEventLogs(receipt *EthereumTransactionReceipt) []*api.EthereumEventLog {
	logs := make([]*api.EthereumEventLog, len(receipt.Logs))
	for i, log := range receipt.Logs {
		logs[i] = &api.EthereumEventLog{
			Removed:          log.Removed,
			LogIndex:         log.LogIndex.Value(),
			TransactionHash:  log.TransactionHash.Value(),
			TransactionIndex: log.TransactionIndex.Value(),
			BlockHash:        log.BlockHash.Value(),
			BlockNumber:      log.BlockNumber.Value(),
			Address:          log.Address.Value(),
			Data:             log.Data.Value(),
			Topics:           p.copyEthereumHexStrings(log.Topics),
		}
	}

	return logs
}

func (p *ethereumNativeParserImpl) parseTransactionTraces(blobdata *api.EthereumBlobdata, transactions []*api.EthereumTransaction) ([]*api.EthereumTransactionTrace, error) {
	numTransactions := len(transactions)
	if len(blobdata.TransactionTraces) > numTransactions {
		return nil, xerrors.Errorf("wrong number of traces (# traces=%d, #transactions=%d)", len(blobdata.TransactionTraces), numTransactions)
	}
	traces := make([]*api.EthereumTransactionTrace, numTransactions)
	for i, rawTrace := range blobdata.TransactionTraces {
		var trace EthereumTransactionTrace
		if err := json.Unmarshal(rawTrace, &trace); err != nil {
			return nil, xerrors.Errorf("failed to parse transaction trace: %w", err)
		}

		traces[i] = p.copyEthereumTransactionTrace(&trace)
	}

	switch len(blobdata.TransactionTraces) {
	case numTransactions:
		return traces, nil
	case numTransactions - 1:
		if p.config.Chain.Blockchain == common.Blockchain_BLOCKCHAIN_POLYGON &&
			p.config.Chain.Network == common.Network_NETWORK_POLYGON_MAINNET &&
			transactions[numTransactions-1].From == ethNullAddress &&
			transactions[numTransactions-1].To == ethNullAddress {
			// For polygon, if the last transaction is from 0x0 to 0x0, it will be ignored by TraceBlockByHash
			// e.g., https://polygonscan.com/block/2304
			// Adding a trace here to keep number of transactions and traces matched
			// The trace added is the result of traceTransaction in this case
			traces[numTransactions-1] = &api.EthereumTransactionTrace{Value: "0"}
			return traces, nil
		}
		fallthrough
	default:
		return nil, xerrors.Errorf("wrong number of traces (# traces=%d, #transactions=%d)", len(blobdata.TransactionTraces), numTransactions)
	}
}

func (p *ethereumNativeParserImpl) parseTransactionFlattenedTraces(trace *api.EthereumTransactionTrace, transaction *api.EthereumTransaction, parentError string, traceAddress []uint64) []*api.EthereumTransactionFlattenedTrace {
	if trace == nil {
		return nil
	}

	flattenedTrace := &api.EthereumTransactionFlattenedTrace{
		Error:            trace.Error,
		Type:             trace.Type,
		TraceType:        trace.Type,
		From:             trace.From,
		To:               trace.To,
		Value:            trace.Value,
		Gas:              trace.Gas,
		GasUsed:          trace.GasUsed,
		Input:            trace.Input,
		Output:           trace.Output,
		TraceAddress:     traceAddress,
		Subtraces:        uint64(len(trace.Calls)),
		BlockNumber:      transaction.BlockNumber,
		BlockHash:        transaction.BlockHash,
		TransactionHash:  transaction.Hash,
		TransactionIndex: transaction.Index,
	}

	// Errors on parent traces are set on children.
	// Child traces that already have errors are not overridden with the parent error.
	if parentError != "" && flattenedTrace.Error == "" {
		flattenedTrace.Error = parentError
	}

	// Process traceType and callType
	// Ref: https://github.com/blockchain-etl/ethereum-etl/blob/25fc768f39d6a3175f8175d42b7699eb33166ebf/ethereumetl/mappers/trace_mapper.py#L146
	// But no need to compatible with parity traces
	if TraceCallTypeMap[flattenedTrace.TraceType] {
		flattenedTrace.CallType = flattenedTrace.TraceType
		flattenedTrace.TraceType = "CALL"
	}

	// Process trace status
	// Ref: https://github.com/blockchain-etl/ethereum-etl/blob/b3fab3c089ff45de75450b886c43960c4b16403a/ethereumetl/service/trace_status_calculator.py#L26
	if len(flattenedTrace.Error) > 0 {
		flattenedTrace.Status = 0
	} else {
		flattenedTrace.Status = 1
	}

	// Process trace id
	p.processEthereumTraceId(flattenedTrace)

	flattenedTraces := []*api.EthereumTransactionFlattenedTrace{flattenedTrace}
	if len(trace.Calls) == 0 {
		return flattenedTraces
	}

	for callIndex, callTrace := range trace.Calls {
		address := p.copyAndAppendTraceAddress(traceAddress, uint64(callIndex))
		children := p.parseTransactionFlattenedTraces(callTrace, transaction, flattenedTrace.Error, address)
		flattenedTraces = append(flattenedTraces, children...)
	}

	return flattenedTraces
}

func (p *ethereumNativeParserImpl) processEthereumTraceId(trace *api.EthereumTransactionFlattenedTrace) {
	// Skip process trace id when there is no trace type
	// e.g. https://polygonscan.com/tx/0xef029df186fe80862876987b237d848734d534c1c857f49bb9606a1b38f2b6b8
	if trace.TraceType == "" {
		return
	}

	// Ref: https://github.com/blockchain-etl/ethereum-etl/blob/b3fab3c089ff45de75450b886c43960c4b16403a/ethereumetl/service/trace_id_calculator.py#L26
	traceId := trace.TraceType + "_" + trace.TransactionHash

	if trace.TraceAddress != nil {
		var sb strings.Builder
		for _, traceAddress := range trace.TraceAddress {
			sb.WriteString(strconv.FormatUint(traceAddress, 10))
			sb.WriteString("_")
		}
		trace.TraceId = strings.TrimSuffix(traceId+"_"+sb.String(), "_")
	} else {
		trace.TraceId = traceId
	}
}

// Structure and objects of parity trace are different from geth. For a single block, all traces are already flattened which is a list of single
// layer. e.g. See API doc here: https://www.quicknode.com/docs/arbitrum/arbtrace_block and response example: /internal/utils/fixtures/client/arbitrum/arb_gettracesresponse.json
func (p *ethereumNativeParserImpl) parseTransactionFlattenedParityTraces(blobData *api.EthereumBlobdata, transactionToFlattenedTracesMap map[string][]*api.EthereumTransactionFlattenedTrace) error {
	if blobData.TransactionTraces == nil || len(blobData.TransactionTraces) == 0 {
		return nil
	}

	for _, rawTrace := range blobData.TransactionTraces {
		var trace ParityTransactionTrace
		if err := json.Unmarshal(rawTrace, &trace); err != nil {
			return xerrors.Errorf("failed to parse transaction trace: %w", err)
		}
		traceTransactionHash := trace.TransactionHash.Value()

		tmpTrace := &api.EthereumTransactionFlattenedTrace{
			Error:            trace.Error,
			Type:             trace.TraceType,
			TraceType:        trace.TraceType,
			CallType:         trace.Action.CallType,
			From:             trace.Action.From.Value(),
			To:               trace.Action.To.Value(),
			Value:            trace.Action.Value.Value(),
			Gas:              trace.Action.Gas.Value(),
			GasUsed:          trace.Result.GasUsed.Value(),
			Input:            trace.Action.Input.Value(),
			Output:           trace.Result.Output.Value(),
			TraceAddress:     trace.TraceAddress,
			Subtraces:        trace.Subtraces,
			BlockNumber:      trace.BlockNumber.Value(),
			BlockHash:        trace.BlockHash.Value(),
			TransactionHash:  trace.TransactionHash.Value(),
			TransactionIndex: trace.TransactionPosition.Value(),
		}

		// Process trace status
		// Ref: https://github.com/blockchain-etl/ethereum-etl/blob/b3fab3c089ff45de75450b886c43960c4b16403a/ethereumetl/service/trace_status_calculator.py#L26
		if len(trace.Error) > 0 {
			tmpTrace.Status = 0
		} else {
			tmpTrace.Status = 1
		}

		// Process trace id
		p.processEthereumTraceId(tmpTrace)

		if _, ok := transactionToFlattenedTracesMap[traceTransactionHash]; !ok {
			transactionToFlattenedTracesMap[traceTransactionHash] = make([]*api.EthereumTransactionFlattenedTrace, 0)
		}
		transactionToFlattenedTracesMap[traceTransactionHash] = append(transactionToFlattenedTracesMap[traceTransactionHash], tmpTrace)
	}

	// Reprocess the traces since error(e.g. Revert) information is only in parent trace.
	// So we need to populate the error information on all children traces.
	if err := p.processParityTraceError(transactionToFlattenedTracesMap); err != nil {
		return xerrors.Errorf("Error processing parity trace: %w", err)
	}

	return nil
}

// Special handling for processing parity trace errors: the returned traces from API call could be out of order, meaning the subtrace of one trace would
// be located not after the index of its parent trace, instead before its parent trace. e.g. see delegatecall_0_1_3_0_0 in this tnx traces list:
// https://arbiscan.io/tx/0x3aa6f200e186492afff48dcad58a3a02c739cf98bb190a573d32c110b5d246a6#internal
func (p *ethereumNativeParserImpl) processParityTraceError(transactionToFlattenedTracesMap map[string][]*api.EthereumTransactionFlattenedTrace) error {
	traceIdSuffixToNestedParityTraceMap := make(map[string]*nestedParityTrace, 0)
	for _, traces := range transactionToFlattenedTracesMap {
		if len(traces) <= 1 {
			continue
		}

		errorNestedTraces := make([]*nestedParityTrace, 0)
		for _, curTrace := range traces {
			// we construct the partial trace id here as the map key to get its corresponding nestedParityTrace
			// and by the concatenations of TraceAddress. Root level trace will have '[]' as its address
			// and its children would have '[0]', '[1]', ... and children of those will add one to the address slice by its index.
			traceIdBuilder := strings.Builder{}
			for _, addr := range curTrace.GetTraceAddress() {
				traceIdBuilder.WriteString(strconv.Itoa(int(addr)))
			}
			traceId := traceIdBuilder.String()
			nestedTrace := &nestedParityTrace{
				TraceId:        traceId,
				TraceInfo:      curTrace,
				ChildrenTraces: make([]*nestedParityTrace, 0),
			}
			if curTrace.Error != "" {
				errorNestedTraces = append(errorNestedTraces, nestedTrace)
			}

			traceIdSuffixToNestedParityTraceMap[traceId] = nestedTrace
		}

		for _, nestedTrace := range traceIdSuffixToNestedParityTraceMap {
			if nestedTrace.TraceId == "" {
				continue
			}
			traceId := nestedTrace.TraceId
			parentId := traceId[:len(traceId)-1]
			parentNestedTrace, ok := traceIdSuffixToNestedParityTraceMap[parentId]
			if !ok {
				return xerrors.Errorf("failed to process trace error with txn hash: %s", nestedTrace.TraceInfo.TransactionHash)
			}
			parentNestedTrace.ChildrenTraces = append(parentNestedTrace.ChildrenTraces, nestedTrace)
		}

		for _, errorTrace := range errorNestedTraces {
			p.processParityTraceErrorHelper(errorTrace)
		}
	}

	return nil
}

// Recursion helper method to populate the children's error message based on their parent trace.
// Note: Child traces that already have errors are not overridden with the parent error.
func (p *ethereumNativeParserImpl) processParityTraceErrorHelper(parentTrace *nestedParityTrace) {
	if len(parentTrace.ChildrenTraces) == 0 {
		return
	}

	for _, childTrace := range parentTrace.ChildrenTraces {
		if childTrace.TraceInfo.Error == "" {
			childTrace.TraceInfo.Error = parentTrace.TraceInfo.Error
			childTrace.TraceInfo.Status = parentTrace.TraceInfo.Status
		} else {
			continue
		}

		p.processParityTraceErrorHelper(childTrace)
	}
}

func (p *ethereumNativeParserImpl) parseTokenTransfers(transactionReceipts []*api.EthereumTransactionReceipt) ([][]*api.EthereumTokenTransfer, error) {
	results := make([][]*api.EthereumTokenTransfer, len(transactionReceipts))
	for i, receipt := range transactionReceipts {
		if receipt == nil {
			continue
		}

		var tokenTransfers []*api.EthereumTokenTransfer
		for _, eventLog := range receipt.Logs {
			if len(eventLog.Topics) == 3 && eventLog.Topics[0] == TransferEventTopic {
				// Parse ERC-20 token
				// https://ethereum.org/en/developers/docs/standards/tokens/erc-20/

				tokenTransfer, err := p.parseERC20TokenTransfer(eventLog)
				if err != nil {
					return nil, xerrors.Errorf("failed to parse erc20 token transfer: %w", err)
				}
				if tokenTransfer != nil {
					tokenTransfers = append(tokenTransfers, tokenTransfer)
				}
			} else if len(eventLog.Topics) == 4 && eventLog.Topics[0] == TransferEventTopic {
				// Parse ERC-721 token
				// https://ethereum.org/en/developers/docs/standards/tokens/erc-721/
				tokenTransfer, err := p.parseERC721TokenTransfer(eventLog)
				if err != nil {
					return nil, xerrors.Errorf("failed to parse erc721 token transfer: %w", err)
				}
				if tokenTransfer != nil {
					tokenTransfers = append(tokenTransfers, tokenTransfer)
				}
			}
		}

		results[i] = tokenTransfers
	}

	return results, nil
}

func (p *ethereumNativeParserImpl) parseERC20TokenTransfer(eventLog *api.EthereumEventLog) (*api.EthereumTokenTransfer, error) {
	// Topic Indices
	// -------------
	// Index 0 - Event Signature
	// Index 1 - from
	// Index 2 - to

	if len(eventLog.Topics) != 3 || eventLog.Topics[0] != TransferEventTopic {
		return nil, xerrors.Errorf("invalid erc20 token transfer")
	}

	tokenAddress, err := cleanAddress(eventLog.Address)
	if err != nil {
		return nil, xerrors.Errorf("failed to decode token address for erc20 %v: %w", eventLog.Address, err)
	}

	fromAddress, err := cleanAddress(eventLog.Topics[1])
	if err != nil {
		return nil, xerrors.Errorf("failed to decode from address for erc20 %v: %w", eventLog.Topics[1], err)
	}

	toAddress, err := cleanAddress(eventLog.Topics[2])
	if err != nil {
		return nil, xerrors.Errorf("failed to decode to address for erc20 %v: %w", eventLog.Topics[2], err)
	}

	value, err := hexToBig(eventLog.Data)
	if err != nil {
		if xerrors.Is(err, hexutil.ErrBig256Range) {
			p.Logger.Warn("event log data hex value larger than 256 bits",
				zap.Uint64("height", eventLog.BlockNumber),
				zap.String("transaction_hash", eventLog.TransactionHash),
				zap.String("event_log_data", eventLog.Data),
			)
			return nil, nil
		}
		return nil, xerrors.Errorf("failed to decode from value for erc20 %v: %w", eventLog.Data, err)
	}

	return &api.EthereumTokenTransfer{
		TokenAddress:     tokenAddress,
		FromAddress:      fromAddress,
		ToAddress:        toAddress,
		Value:            value.String(),
		TransactionHash:  eventLog.TransactionHash,
		TransactionIndex: eventLog.TransactionIndex,
		LogIndex:         eventLog.LogIndex,
		BlockHash:        eventLog.BlockHash,
		BlockNumber:      eventLog.BlockNumber,
		TokenTransfer: &api.EthereumTokenTransfer_Erc20{
			Erc20: &api.ERC20TokenTransfer{
				FromAddress: fromAddress,
				ToAddress:   toAddress,
				Value:       value.String(),
			},
		},
	}, nil
}

func (p *ethereumNativeParserImpl) parseERC721TokenTransfer(eventLog *api.EthereumEventLog) (*api.EthereumTokenTransfer, error) {
	// Topic Indices
	// -------------
	// Index 0 - Event Signature
	// Index 1 - from
	// Index 2 - to
	// Index 3 - tokenID

	if len(eventLog.Topics) != 4 || eventLog.Topics[0] != TransferEventTopic {
		return nil, xerrors.Errorf("invalid erc721 token transfer")
	}

	tokenAddress, err := cleanAddress(eventLog.Address)
	if err != nil {
		return nil, xerrors.Errorf("failed to decode token address for erc721 %v: %w", eventLog.Address, err)
	}

	fromAddress, err := cleanAddress(eventLog.Topics[1])
	if err != nil {
		return nil, xerrors.Errorf("failed to decode from address for erc721 %v: %w", eventLog.Topics[1], err)
	}

	toAddress, err := cleanAddress(eventLog.Topics[2])
	if err != nil {
		return nil, xerrors.Errorf("failed to decode to address for erc721 %v: %w", eventLog.Topics[2], err)
	}

	encodedTokenID := eventLog.Topics[3]
	match := ethereumValidDataPattern.MatchString(encodedTokenID)
	if !match {
		// Ignore such event if it's invalid pattern
		return nil, nil
	}

	tokenID, err := hexToBig(encodedTokenID)
	if err != nil {
		return nil, xerrors.Errorf("failed to decode tokenID for erc721 %v: %w", eventLog.Topics[3], err)
	}

	// No value field for erc721
	return &api.EthereumTokenTransfer{
		TokenAddress:     tokenAddress,
		FromAddress:      fromAddress,
		ToAddress:        toAddress,
		TransactionHash:  eventLog.TransactionHash,
		TransactionIndex: eventLog.TransactionIndex,
		LogIndex:         eventLog.LogIndex,
		BlockHash:        eventLog.BlockHash,
		BlockNumber:      eventLog.BlockNumber,
		TokenTransfer: &api.EthereumTokenTransfer_Erc721{
			Erc721: &api.ERC721TokenTransfer{
				FromAddress: fromAddress,
				ToAddress:   toAddress,
				TokenId:     tokenID.String(),
			},
		},
	}, nil
}

func (p *ethereumNativeParserImpl) parseUncles(blobdata *api.EthereumBlobdata) ([]*api.EthereumHeader, error) {
	uncles := make([]*api.EthereumHeader, len(blobdata.Uncles))
	for i, rawUncle := range blobdata.Uncles {
		uncle, _, err := p.parseHeader(rawUncle)
		if err != nil {
			return nil, xerrors.Errorf("failed to parse uncle: %w", err)
		}

		uncles[i] = uncle
	}

	return uncles, nil
}

func (p *ethereumNativeParserImpl) copyEthereumHexStrings(arr []EthereumHexString) []string {
	res := make([]string, len(arr))
	for i, s := range arr {
		res[i] = s.Value()
	}
	return res
}

func (p *ethereumNativeParserImpl) copyEthereumTransactionTrace(input *EthereumTransactionTrace) *api.EthereumTransactionTrace {
	output := &api.EthereumTransactionTrace{
		Error:   input.Error,
		Type:    input.Type,
		From:    input.From.Value(),
		To:      input.To.Value(),
		Value:   input.Value.Value(),
		Gas:     input.Gas.Value(),
		GasUsed: input.GasUsed.Value(),
		Input:   input.Input.Value(),
		Output:  input.Output.Value(),
	}

	var calls []*api.EthereumTransactionTrace
	for i := range input.Calls {
		call := &input.Calls[i]
		calls = append(calls, p.copyEthereumTransactionTrace(call))
	}
	output.Calls = calls

	return output
}

// `append` is suggested only when append new value to given slices, not to create new one
// Ref: https://medium.com/@Jarema./golang-slice-append-gotcha-e9020ff37374
func (p *ethereumNativeParserImpl) copyAndAppendTraceAddress(traceAddress []uint64, value uint64) []uint64 {
	newTraceAddress := make([]uint64, len(traceAddress), len(traceAddress)+1)
	copy(newTraceAddress, traceAddress)

	return append(newTraceAddress, value)
}
