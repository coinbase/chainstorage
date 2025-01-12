package beacon

import (
	"context"
	"encoding/json"
	"strconv"

	"github.com/go-playground/validator/v10"
	"github.com/prysmaticlabs/prysm/v4/runtime/version"
	"go.uber.org/zap"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/blockchain/parser/internal"
	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/utils/log"
	"github.com/coinbase/chainstorage/internal/utils/utils"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

const (
	SlotsPerEpoch = 32
)

type (
	Quantity             uint64
	ExecutionTransaction []byte
	Blob                 []byte

	// BlockHeader https://github.com/prysmaticlabs/prysm/blob/44973b0bb3d4439e837110205e12facc7020e732/beacon-chain/rpc/eth/beacon/structs.go#L78
	BlockHeader struct {
		Data *SignedBlockHeaderContainer `json:"data" validate:"required"`
	}

	SignedBlockHeaderContainer struct {
		Header *SignedBlockHeader `json:"header" validate:"required"`
		Root   string             `json:"root" validate:"required"`
	}

	SignedBlockHeader struct {
		Message   *BlockHeaderMessage `json:"message" validate:"required"`
		Signature string              `json:"signature" validate:"required"`
	}

	BlockHeaderMessage struct {
		Slot          Quantity `json:"slot"`
		ProposerIndex Quantity `json:"proposer_index"`
		ParentRoot    string   `json:"parent_root" validate:"required"`
		StateRoot     string   `json:"state_root" validate:"required"`
		BodyRoot      string   `json:"body_root" validate:"required"`
	}

	BlockResponseLit struct {
		Version string          `json:"version" validate:"required"`
		Data    json.RawMessage `json:"data" validate:"required"`
	}

	Blobs struct {
		Data []*BlobSidecar `json:"data" validate:"required"`
	}

	// BlobSidecar https://github.com/prysmaticlabs/prysm/blob/develop/beacon-chain/rpc/eth/blob/structs.go#L9
	BlobSidecar struct {
		Index                    Quantity           `json:"index"`
		Blob                     Blob               `json:"blob" validate:"required"`
		SignedBeaconBlockHeader  *SignedBlockHeader `json:"signed_block_header" validate:"required"`
		KzgCommitment            string             `json:"kzg_commitment" validate:"required"`
		KzgProof                 string             `json:"kzg_proof" validate:"required"`
		CommitmentInclusionProof []string           `json:"kzg_commitment_inclusion_proof" validate:"required,dive"`
	}

	// TODO: Migrate to the struct types defined in prysmaticlabs/prysm repo
	Eth1Data struct {
		DepositRoot  string `json:"deposit_root" validate:"required"`
		DepositCount string `json:"deposit_count" validate:"required"`
		BlockHash    string `json:"block_hash" validate:"required"`
	}

	SignedBlockPhase0 struct {
		Message   *BlockPhase0 `json:"message" validate:"required"`
		Signature string       `json:"signature" validate:"required"`
	}

	BlockPhase0 struct {
		Slot          Quantity         `json:"slot"`
		ProposerIndex Quantity         `json:"proposer_index"`
		ParentRoot    string           `json:"parent_root" validate:"required"`
		StateRoot     string           `json:"state_root" validate:"required"`
		Body          *BlockBodyPhase0 `json:"body" validate:"required"`
	}

	BlockBodyPhase0 struct {
		RandaoReveal string    `json:"randao_reveal" validate:"required"`
		Eth1Data     *Eth1Data `json:"eth1_data" validate:"required"`
		Graffiti     string    `json:"graffiti" validate:"required"`
	}

	// SignedBlockAltair https://github.com/prysmaticlabs/prysm/blob/76fec1799e4a8d16dbd453f1ffb595262994221d/beacon-chain/rpc/eth/shared/structs_blocks.go#L27
	SignedBlockAltair struct {
		Message   *BlockAltair `json:"message" validate:"required"`
		Signature string       `json:"signature" validate:"required"`
	}

	BlockAltair struct {
		Slot          Quantity         `json:"slot"`
		ProposerIndex Quantity         `json:"proposer_index"`
		ParentRoot    string           `json:"parent_root" validate:"required"`
		StateRoot     string           `json:"state_root" validate:"required"`
		Body          *BlockBodyAltair `json:"body" validate:"required"`
	}

	BlockBodyAltair struct {
		RandaoReveal string    `json:"randao_reveal" validate:"required"`
		Eth1Data     *Eth1Data `json:"eth1_data" validate:"required"`
		Graffiti     string    `json:"graffiti" validate:"required"`
	}

	// SignedBlockBellatrix https://github.com/prysmaticlabs/prysm/blob/76fec1799e4a8d16dbd453f1ffb595262994221d/beacon-chain/rpc/eth/shared/structs_blocks.go#L52
	SignedBlockBellatrix struct {
		Message   *BlockBellatrix `json:"message" validate:"required"`
		Signature string          `json:"signature" validate:"required"`
	}

	BlockBellatrix struct {
		Slot          Quantity            `json:"slot"`
		ProposerIndex Quantity            `json:"proposer_index"`
		ParentRoot    string              `json:"parent_root" validate:"required"`
		StateRoot     string              `json:"state_root" validate:"required"`
		Body          *BlockBodyBellatrix `json:"body" validate:"required"`
	}

	BlockBodyBellatrix struct {
		RandaoReveal     string                     `json:"randao_reveal" validate:"required"`
		Eth1Data         *Eth1Data                  `json:"eth1_data" validate:"required"`
		Graffiti         string                     `json:"graffiti" validate:"required"`
		ExecutionPayload *ExecutionPayloadBellatrix `json:"execution_payload" validate:"required"`
	}

	ExecutionPayloadBellatrix struct {
		ParentHash    string                 `json:"parent_hash" validate:"required"`
		FeeRecipient  string                 `json:"fee_recipient" validate:"required"`
		StateRoot     string                 `json:"state_root" validate:"required"`
		ReceiptsRoot  string                 `json:"receipts_root" validate:"required"`
		LogsBloom     string                 `json:"logs_bloom" validate:"required"`
		PrevRandao    string                 `json:"prev_randao" validate:"required"`
		BlockNumber   Quantity               `json:"block_number"`
		GasLimit      Quantity               `json:"gas_limit"`
		GasUsed       Quantity               `json:"gas_used"`
		Timestamp     Quantity               `json:"timestamp" validate:"required_with=BlockNumber"`
		ExtraData     string                 `json:"extra_data" validate:"required"`
		BaseFeePerGas string                 `json:"base_fee_per_gas" validate:"required"`
		BlockHash     string                 `json:"block_hash" validate:"required"`
		Transactions  []ExecutionTransaction `json:"transactions" validate:"required"`
	}

	SignedBlockCapella struct {
		Message   *BlockCapella `json:"message" validate:"required"`
		Signature string        `json:"signature" validate:"required"`
	}

	BlockCapella struct {
		Slot          Quantity          `json:"slot"`
		ProposerIndex Quantity          `json:"proposer_index"`
		ParentRoot    string            `json:"parent_root" validate:"required"`
		StateRoot     string            `json:"state_root" validate:"required"`
		Body          *BlockBodyCapella `json:"body" validate:"required"`
	}

	BlockBodyCapella struct {
		RandaoReveal     string                   `json:"randao_reveal" validate:"required"`
		Eth1Data         *Eth1Data                `json:"eth1_data" validate:"required"`
		Graffiti         string                   `json:"graffiti" validate:"required"`
		ExecutionPayload *ExecutionPayloadCapella `json:"execution_payload" validate:"required"`
	}

	ExecutionPayloadCapella struct {
		ParentHash    string                 `json:"parent_hash" validate:"required"`
		FeeRecipient  string                 `json:"fee_recipient" validate:"required"`
		StateRoot     string                 `json:"state_root" validate:"required"`
		ReceiptsRoot  string                 `json:"receipts_root" validate:"required"`
		LogsBloom     string                 `json:"logs_bloom" validate:"required"`
		PrevRandao    string                 `json:"prev_randao" validate:"required"`
		BlockNumber   Quantity               `json:"block_number"`
		GasLimit      Quantity               `json:"gas_limit"`
		GasUsed       Quantity               `json:"gas_used"`
		Timestamp     Quantity               `json:"timestamp" validate:"required_with=BlockNumber"`
		ExtraData     string                 `json:"extra_data" validate:"required"`
		BaseFeePerGas string                 `json:"base_fee_per_gas" validate:"required"`
		BlockHash     string                 `json:"block_hash" validate:"required"`
		Transactions  []ExecutionTransaction `json:"transactions" validate:"required"`
		Withdrawals   []*Withdrawal          `json:"withdrawals" validate:"required,dive"`
	}

	Withdrawal struct {
		WithdrawalIndex  Quantity `json:"index"`
		ValidatorIndex   Quantity `json:"validator_index"`
		ExecutionAddress string   `json:"address" validate:"required"`
		Amount           Quantity `json:"amount"`
	}

	SignedBlockDeneb struct {
		Message   *BlockDeneb `json:"message" validate:"required"`
		Signature string      `json:"signature" validate:"required"`
	}

	BlockDeneb struct {
		Slot          Quantity        `json:"slot"`
		ProposerIndex Quantity        `json:"proposer_index"`
		ParentRoot    string          `json:"parent_root" validate:"required"`
		StateRoot     string          `json:"state_root" validate:"required"`
		Body          *BlockBodyDeneb `json:"body" validate:"required"`
	}

	BlockBodyDeneb struct {
		RandaoReveal       string                 `json:"randao_reveal" validate:"required"`
		Eth1Data           *Eth1Data              `json:"eth1_data" validate:"required"`
		Graffiti           string                 `json:"graffiti" validate:"required"`
		ExecutionPayload   *ExecutionPayloadDeneb `json:"execution_payload" validate:"required"`
		BlobKzgCommitments []string               `json:"blob_kzg_commitments" validate:"required,dive"`
	}

	ExecutionPayloadDeneb struct {
		ParentHash    string                 `json:"parent_hash" validate:"required"`
		FeeRecipient  string                 `json:"fee_recipient" validate:"required"`
		StateRoot     string                 `json:"state_root" validate:"required"`
		ReceiptsRoot  string                 `json:"receipts_root" validate:"required"`
		LogsBloom     string                 `json:"logs_bloom" validate:"required"`
		PrevRandao    string                 `json:"prev_randao" validate:"required"`
		BlockNumber   Quantity               `json:"block_number"`
		GasLimit      Quantity               `json:"gas_limit"`
		GasUsed       Quantity               `json:"gas_used"`
		Timestamp     Quantity               `json:"timestamp" validate:"required_with=BlockNumber"`
		ExtraData     string                 `json:"extra_data" validate:"required"`
		BaseFeePerGas string                 `json:"base_fee_per_gas" validate:"required"`
		BlobGasUsed   Quantity               `json:"blob_gas_used"`
		ExcessBlobGas Quantity               `json:"excess_blob_gas"`
		BlockHash     string                 `json:"block_hash" validate:"required"`
		Transactions  []ExecutionTransaction `json:"transactions" validate:"required"`
		Withdrawals   []*Withdrawal          `json:"withdrawals" validate:"required,dive"`
	}

	blockResultHolder struct {
		block              *api.EthereumBeaconBlockData
		blobKzgCommitments []string
	}

	nativeParserImpl struct {
		logger   *zap.Logger
		validate *validator.Validate
		config   *config.Config
	}
)

func NewNativeParser(params internal.ParserParams, opts ...internal.ParserFactoryOption) (internal.NativeParser, error) {
	return &nativeParserImpl{
		logger:   log.WithPackage(params.Logger),
		validate: validator.New(),
		config:   params.Config,
	}, nil
}

func (p *nativeParserImpl) ParseBlock(ctx context.Context, rawBlock *api.Block) (*api.NativeBlock, error) {
	metadata := rawBlock.GetMetadata()
	if metadata == nil {
		return nil, xerrors.New("metadata not found")
	}

	if metadata.Skipped {
		return &api.NativeBlock{
			Blockchain: rawBlock.Blockchain,
			Network:    rawBlock.Network,
			SideChain:  rawBlock.SideChain,
			Tag:        metadata.Tag,
			Height:     metadata.Height,
			Timestamp:  metadata.Timestamp,
			Skipped:    true,
		}, nil
	}

	blobdata := rawBlock.GetEthereumBeacon()
	if blobdata == nil {
		return nil, xerrors.New("blobdata not found")
	}

	header, err := p.parseHeader(blobdata.Header, metadata)
	if err != nil {
		return nil, xerrors.Errorf("failed to parse header: %w", err)
	}

	blockResult, err := p.parseBlock(blobdata.Block, metadata)
	if err != nil {
		return nil, xerrors.Errorf("failed to parse block data for slot height=%v, hash=%v: %w", metadata.Height, metadata.Hash, err)
	}

	if blockResult.block == nil {
		return nil, xerrors.Errorf("block data is nil for slot height=%v, hash=%v", metadata.Height, metadata.Hash)
	}

	blobs, err := p.parseBlobs(blobdata.Blobs, metadata, blockResult.blobKzgCommitments)
	if err != nil {
		return nil, xerrors.Errorf("failed to parse blobs for slot height=%v, hash=%v: %w", metadata.Height, metadata.Hash, err)
	}

	return &api.NativeBlock{
		Blockchain:   rawBlock.Blockchain,
		Network:      rawBlock.Network,
		SideChain:    rawBlock.SideChain,
		Tag:          metadata.Tag,
		Hash:         metadata.Hash,
		ParentHash:   metadata.ParentHash,
		Height:       metadata.Height,
		ParentHeight: metadata.ParentHeight,
		Timestamp:    metadata.Timestamp,
		Block: &api.NativeBlock_EthereumBeacon{
			EthereumBeacon: &api.EthereumBeaconBlock{
				Header: header,
				Block:  blockResult.block,
				Blobs:  blobs,
			},
		},
	}, nil
}

func (p *nativeParserImpl) parseHeader(data []byte, metadata *api.BlockMetadata) (*api.EthereumBeaconBlockHeader, error) {
	if len(data) == 0 {
		return nil, xerrors.New("block header is empty")
	}

	var header BlockHeader
	if err := json.Unmarshal(data, &header); err != nil {
		return nil, xerrors.Errorf("failed to parse block header on unmarshal: %w", err)
	}

	if err := p.validate.Struct(header); err != nil {
		return nil, xerrors.Errorf("failed to parse block header on struct validate: %w", err)
	}
	headerData := header.Data
	message := headerData.Header.Message

	slot := message.Slot.Value()
	if slot != metadata.Height {
		return nil, xerrors.Errorf("block slot=%d does not match metadata in header {%+v}", slot, metadata)
	}
	if headerData.Root != metadata.Hash {
		return nil, xerrors.Errorf("block root=%s does not match metadata in header {%+v}", headerData.Root, metadata)
	}

	epoch, err := calculateEpoch(slot)
	if err != nil {
		return nil, xerrors.Errorf("failed to calculate epoch for slot=%d: %w", slot, err)
	}

	return &api.EthereumBeaconBlockHeader{
		Slot:          slot,
		ProposerIndex: message.ProposerIndex.Value(),
		ParentRoot:    message.ParentRoot,
		StateRoot:     message.StateRoot,
		BodyRoot:      message.BodyRoot,
		Signature:     headerData.Header.Signature,
		Root:          headerData.Root,
		Epoch:         epoch,
	}, nil
}

func (p *nativeParserImpl) parseBlock(data []byte, metadata *api.BlockMetadata) (*blockResultHolder, error) {
	if len(data) == 0 {
		return nil, xerrors.New("block data is empty")
	}

	var blockLit BlockResponseLit
	if err := json.Unmarshal(data, &blockLit); err != nil {
		return nil, xerrors.Errorf("failed to parse block on unmarshal: %w", err)
	}

	v, err := version.FromString(blockLit.Version)
	if err != nil {
		return nil, xerrors.Errorf("failed to parse block version=%v: %w", blockLit.Version, err)
	}

	switch v {
	case version.Phase0:
		return p.parsePhase0Block(blockLit.Data, metadata)
	case version.Altair:
		return p.parseAltairBlock(blockLit.Data, metadata)
	case version.Bellatrix:
		return p.parseBellatrixBlock(blockLit.Data, metadata)
	case version.Capella:
		return p.parseCapellaBlock(blockLit.Data, metadata)
	case version.Deneb:
		return p.parseDenebBlock(blockLit.Data, metadata)
	default:
		return nil, xerrors.Errorf("unsupported block version=%v", blockLit.Version)
	}
}

func (p *nativeParserImpl) parsePhase0Block(data []byte, metadata *api.BlockMetadata) (*blockResultHolder, error) {
	var block SignedBlockPhase0
	if err := json.Unmarshal(data, &block); err != nil {
		return nil, xerrors.Errorf("failed to parse Phase0 block on unmarshal: %w", err)
	}

	if err := p.validate.Struct(block); err != nil {
		return nil, xerrors.Errorf("failed to parse Phase0 block on struct validate: %w", err)
	}

	blockMessage := block.Message
	blockBody := blockMessage.Body

	if blockMessage.Slot.Value() != metadata.Height {
		return nil, xerrors.Errorf("Phase0 block slot=%d does not match metadata {%+v}", blockMessage.Slot.Value(), metadata)
	}

	eth1Data, err := p.parseEth1Data(blockBody.Eth1Data)
	if err != nil {
		return nil, xerrors.Errorf("failed to parse eth1Data: %w", err)
	}

	return &blockResultHolder{
		block: &api.EthereumBeaconBlockData{
			Version:       api.EthereumBeaconVersion_PHASE0,
			Signature:     block.Signature,
			Slot:          blockMessage.Slot.Value(),
			ProposerIndex: blockMessage.ProposerIndex.Value(),
			ParentRoot:    blockMessage.ParentRoot,
			StateRoot:     blockMessage.StateRoot,
			BlockData: &api.EthereumBeaconBlockData_Phase0Block{
				Phase0Block: &api.EthereumBeaconBlockPhase0{
					RandaoReveal: blockBody.RandaoReveal,
					Eth1Data:     eth1Data,
				},
			},
		},
	}, nil
}

func (p *nativeParserImpl) parseAltairBlock(data []byte, metadata *api.BlockMetadata) (*blockResultHolder, error) {
	var block SignedBlockAltair
	if err := json.Unmarshal(data, &block); err != nil {
		return nil, xerrors.Errorf("failed to parse Altair block on unmarshal: %w", err)
	}

	if err := p.validate.Struct(block); err != nil {
		return nil, xerrors.Errorf("failed to parse Altair block on struct validate: %w", err)
	}

	blockMessage := block.Message
	blockBody := blockMessage.Body

	if blockMessage.Slot.Value() != metadata.Height {
		return nil, xerrors.Errorf("Altair block slot=%d does not match metadata {%+v}", blockMessage.Slot.Value(), metadata)
	}

	eth1Data, err := p.parseEth1Data(blockBody.Eth1Data)
	if err != nil {
		return nil, xerrors.Errorf("failed to parse eth1Data: %w", err)
	}

	return &blockResultHolder{
		block: &api.EthereumBeaconBlockData{
			Version:       api.EthereumBeaconVersion_ALTAIR,
			Signature:     block.Signature,
			Slot:          blockMessage.Slot.Value(),
			ProposerIndex: blockMessage.ProposerIndex.Value(),
			ParentRoot:    blockMessage.ParentRoot,
			StateRoot:     blockMessage.StateRoot,
			BlockData: &api.EthereumBeaconBlockData_AltairBlock{
				AltairBlock: &api.EthereumBeaconBlockAltair{
					RandaoReveal: blockBody.RandaoReveal,
					Eth1Data:     eth1Data,
				},
			},
		},
	}, nil
}

func (p *nativeParserImpl) parseBellatrixBlock(data []byte, metadata *api.BlockMetadata) (*blockResultHolder, error) {
	var block SignedBlockBellatrix
	if err := json.Unmarshal(data, &block); err != nil {
		return nil, xerrors.Errorf("failed to parse Bellatrix block on unmarshal: %w", err)
	}

	if err := p.validate.Struct(block); err != nil {
		return nil, xerrors.Errorf("failed to parse Bellatrix block on struct validate: %w", err)
	}

	blockMessage := block.Message
	blockBody := blockMessage.Body
	executionPayload := blockBody.ExecutionPayload

	if blockMessage.Slot.Value() != metadata.Height {
		return nil, xerrors.Errorf("Bellatrix block slot=%d does not match metadata {%+v}", blockMessage.Slot.Value(), metadata)
	}

	eth1Data, err := p.parseEth1Data(blockBody.Eth1Data)
	if err != nil {
		return nil, xerrors.Errorf("failed to parse eth1Data: %w", err)
	}

	return &blockResultHolder{
		block: &api.EthereumBeaconBlockData{
			Version:       api.EthereumBeaconVersion_BELLATRIX,
			Signature:     block.Signature,
			Slot:          blockMessage.Slot.Value(),
			ProposerIndex: blockMessage.ProposerIndex.Value(),
			ParentRoot:    blockMessage.ParentRoot,
			StateRoot:     blockMessage.StateRoot,
			BlockData: &api.EthereumBeaconBlockData_BellatrixBlock{
				BellatrixBlock: &api.EthereumBeaconBlockBellatrix{
					RandaoReveal: blockBody.RandaoReveal,
					Eth1Data:     eth1Data,
					ExecutionPayload: &api.EthereumBeaconExecutionPayloadBellatrix{
						ParentHash:    executionPayload.ParentHash,
						FeeRecipient:  executionPayload.FeeRecipient,
						StateRoot:     executionPayload.StateRoot,
						ReceiptsRoot:  executionPayload.ReceiptsRoot,
						LogsBloom:     executionPayload.LogsBloom,
						PrevRandao:    executionPayload.PrevRandao,
						BlockNumber:   executionPayload.BlockNumber.Value(),
						GasLimit:      executionPayload.GasLimit.Value(),
						GasUsed:       executionPayload.GasUsed.Value(),
						Timestamp:     utils.ToTimestamp(int64(executionPayload.Timestamp.Value())),
						ExtraData:     executionPayload.ExtraData,
						BaseFeePerGas: executionPayload.BaseFeePerGas,
						BlockHash:     executionPayload.BlockHash,
						Transactions:  p.parseExecutionTransactions(executionPayload.Transactions),
					},
				},
			},
		},
	}, nil
}

func (p *nativeParserImpl) parseCapellaBlock(data []byte, metadata *api.BlockMetadata) (*blockResultHolder, error) {
	var block SignedBlockCapella
	if err := json.Unmarshal(data, &block); err != nil {
		return nil, xerrors.Errorf("failed to parse Capella block on unmarshal: %w", err)
	}

	if err := p.validate.Struct(block); err != nil {
		return nil, xerrors.Errorf("failed to parse Capella block on struct validate: %w", err)
	}

	blockMessage := block.Message
	blockBody := blockMessage.Body
	executionPayload := blockBody.ExecutionPayload

	if blockMessage.Slot.Value() != metadata.Height {
		return nil, xerrors.Errorf("Capella block slot=%d does not match metadata {%+v}", blockMessage.Slot.Value(), metadata)
	}

	eth1Data, err := p.parseEth1Data(blockBody.Eth1Data)
	if err != nil {
		return nil, xerrors.Errorf("failed to parse eth1Data: %w", err)
	}

	withdrawals := p.parseWithdrawals(executionPayload.Withdrawals)

	return &blockResultHolder{
		block: &api.EthereumBeaconBlockData{
			Version:       api.EthereumBeaconVersion_CAPELLA,
			Signature:     block.Signature,
			Slot:          blockMessage.Slot.Value(),
			ProposerIndex: blockMessage.ProposerIndex.Value(),
			ParentRoot:    blockMessage.ParentRoot,
			StateRoot:     blockMessage.StateRoot,
			BlockData: &api.EthereumBeaconBlockData_CapellaBlock{
				CapellaBlock: &api.EthereumBeaconBlockCapella{
					RandaoReveal: blockBody.RandaoReveal,
					Eth1Data:     eth1Data,
					ExecutionPayload: &api.EthereumBeaconExecutionPayloadCapella{
						ParentHash:    executionPayload.ParentHash,
						FeeRecipient:  executionPayload.FeeRecipient,
						StateRoot:     executionPayload.StateRoot,
						ReceiptsRoot:  executionPayload.ReceiptsRoot,
						LogsBloom:     executionPayload.LogsBloom,
						PrevRandao:    executionPayload.PrevRandao,
						BlockNumber:   executionPayload.BlockNumber.Value(),
						GasLimit:      executionPayload.GasLimit.Value(),
						GasUsed:       executionPayload.GasUsed.Value(),
						Timestamp:     utils.ToTimestamp(int64(executionPayload.Timestamp.Value())),
						ExtraData:     executionPayload.ExtraData,
						BaseFeePerGas: executionPayload.BaseFeePerGas,
						BlockHash:     executionPayload.BlockHash,
						Transactions:  p.parseExecutionTransactions(executionPayload.Transactions),
						Withdrawals:   withdrawals,
					},
				},
			},
		},
	}, nil
}

func (p *nativeParserImpl) parseDenebBlock(data []byte, metadata *api.BlockMetadata) (*blockResultHolder, error) {
	var block SignedBlockDeneb
	if err := json.Unmarshal(data, &block); err != nil {
		return nil, xerrors.Errorf("failed to parse Deneb block on unmarshal: %w", err)
	}

	if err := p.validate.Struct(block); err != nil {
		return nil, xerrors.Errorf("failed to parse Deneb block on struct validate: %w", err)
	}

	blockMessage := block.Message
	blockBody := blockMessage.Body
	executionPayload := blockBody.ExecutionPayload

	if blockMessage.Slot.Value() != metadata.Height {
		return nil, xerrors.Errorf("block slot=%d does not match metadata {%+v}", blockMessage.Slot.Value(), metadata)
	}

	eth1Data, err := p.parseEth1Data(blockBody.Eth1Data)
	if err != nil {
		return nil, xerrors.Errorf("failed to parse eth1Data: %w", err)
	}

	withdrawals := p.parseWithdrawals(executionPayload.Withdrawals)

	return &blockResultHolder{
		block: &api.EthereumBeaconBlockData{
			Version:       api.EthereumBeaconVersion_DENEB,
			Signature:     block.Signature,
			Slot:          blockMessage.Slot.Value(),
			ProposerIndex: blockMessage.ProposerIndex.Value(),
			ParentRoot:    blockMessage.ParentRoot,
			StateRoot:     blockMessage.StateRoot,
			BlockData: &api.EthereumBeaconBlockData_DenebBlock{
				DenebBlock: &api.EthereumBeaconBlockDeneb{
					RandaoReveal:       blockBody.RandaoReveal,
					Eth1Data:           eth1Data,
					BlobKzgCommitments: blockBody.BlobKzgCommitments,
					ExecutionPayload: &api.EthereumBeaconExecutionPayloadDeneb{
						ParentHash:    executionPayload.ParentHash,
						FeeRecipient:  executionPayload.FeeRecipient,
						StateRoot:     executionPayload.StateRoot,
						ReceiptsRoot:  executionPayload.ReceiptsRoot,
						LogsBloom:     executionPayload.LogsBloom,
						PrevRandao:    executionPayload.PrevRandao,
						BlockNumber:   executionPayload.BlockNumber.Value(),
						GasLimit:      executionPayload.GasLimit.Value(),
						GasUsed:       executionPayload.GasUsed.Value(),
						Timestamp:     utils.ToTimestamp(int64(executionPayload.Timestamp.Value())),
						ExtraData:     executionPayload.ExtraData,
						BaseFeePerGas: executionPayload.BaseFeePerGas,
						BlockHash:     executionPayload.BlockHash,
						Transactions:  p.parseExecutionTransactions(executionPayload.Transactions),
						Withdrawals:   withdrawals,
						BlobGasUsed:   executionPayload.BlobGasUsed.Value(),
						ExcessBlobGas: executionPayload.ExcessBlobGas.Value(),
					},
				},
			},
		},
		blobKzgCommitments: blockBody.BlobKzgCommitments,
	}, nil
}

func (p *nativeParserImpl) parseEth1Data(eth1Data *Eth1Data) (*api.EthereumBeaconEth1Data, error) {
	depositCount, err := strconv.ParseUint(eth1Data.DepositCount, 10, 64)
	if err != nil {
		return nil, xerrors.Errorf("failed to parse depositCount=%v to uint64: %w", eth1Data.DepositCount, err)
	}
	return &api.EthereumBeaconEth1Data{
		DepositRoot:  eth1Data.DepositRoot,
		DepositCount: depositCount,
		BlockHash:    eth1Data.BlockHash,
	}, nil
}

func (p *nativeParserImpl) parseWithdrawals(withdrawals []*Withdrawal) []*api.EthereumWithdrawal {
	result := make([]*api.EthereumWithdrawal, len(withdrawals))
	for i, withdrawal := range withdrawals {
		result[i] = &api.EthereumWithdrawal{
			Index:          withdrawal.WithdrawalIndex.Value(),
			ValidatorIndex: withdrawal.ValidatorIndex.Value(),
			Address:        withdrawal.ExecutionAddress,
			Amount:         withdrawal.Amount.Value(),
		}
	}
	return result
}

func (p *nativeParserImpl) parseExecutionTransactions(transactions []ExecutionTransaction) [][]byte {
	result := make([][]byte, len(transactions))
	for i, tx := range transactions {
		result[i] = tx
	}
	return result
}

func (p *nativeParserImpl) parseBlobs(data []byte, metadata *api.BlockMetadata, blobKzgCommitments []string) ([]*api.EthereumBeaconBlob, error) {
	// For pre-Dencun blocks, blobs data is stored as nil in the database since the Blob API was not ready.
	if len(data) == 0 && len(blobKzgCommitments) == 0 {
		return nil, nil
	}

	if len(data) == 0 && len(blobKzgCommitments) != 0 {
		return nil, xerrors.Errorf("blobs data is empty but blobKzgCommitments is not, expected=%d", len(blobKzgCommitments))
	}

	var blobs Blobs
	if err := json.Unmarshal(data, &blobs); err != nil {
		return nil, xerrors.Errorf("failed to parse blobs on unmarshal: %w", err)
	}

	if err := p.validate.Struct(blobs); err != nil {
		return nil, xerrors.Errorf("failed to parse blobs on struct validate: %w", err)
	}

	if len(blobs.Data) != len(blobKzgCommitments) {
		return nil, xerrors.Errorf("blob count=%d does not match blobKzgCommitments count=%d", len(blobs.Data), len(blobKzgCommitments))
	}

	result := make([]*api.EthereumBeaconBlob, len(blobs.Data))
	for i, blob := range blobs.Data {
		blobIndex := blob.Index.Value()
		blockHeader := blob.SignedBeaconBlockHeader

		if blockHeader == nil {
			return nil, xerrors.Errorf("missing block header for blob index=%d", blobIndex)
		}

		slot := blockHeader.Message.Slot.Value()
		parentRoot := blockHeader.Message.ParentRoot

		if slot != metadata.Height {
			return nil, xerrors.Errorf("blob slot=%d does not match metadata {%+v} on blob index=%d", slot, metadata, blobIndex)
		}

		if parentRoot != metadata.ParentHash {
			return nil, xerrors.Errorf("blob parent root=%v does not match metadata {%+v} on blob index=%d", parentRoot, metadata, blobIndex)
		}

		if blobKzgCommitments[i] != blob.KzgCommitment {
			return nil, xerrors.Errorf("KzgCommitment does not match on blob index=%d, expected=%v, actual=%v", blobIndex, blobKzgCommitments[i], blob.KzgCommitment)
		}

		result[i] = &api.EthereumBeaconBlob{
			Slot:                        slot,
			ParentRoot:                  parentRoot,
			Index:                       blobIndex,
			Blob:                        blob.Blob,
			KzgCommitment:               blob.KzgCommitment,
			KzgProof:                    blob.KzgProof,
			KzgCommitmentInclusionProof: blob.CommitmentInclusionProof,
		}
	}
	return result, nil
}

func (p *nativeParserImpl) GetTransaction(ctx context.Context, nativeBlock *api.NativeBlock, transactionHash string) (*api.NativeTransaction, error) {
	return nil, internal.ErrNotImplemented
}
