package activity

import (
	"context"

	"go.temporal.io/sdk/workflow"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/blockchain/client"
	"github.com/coinbase/chainstorage/internal/blockchain/endpoints"
	"github.com/coinbase/chainstorage/internal/cadence"
	"github.com/coinbase/chainstorage/internal/storage"
	"github.com/coinbase/chainstorage/internal/storage/blobstorage"
	"github.com/coinbase/chainstorage/internal/storage/metastorage"
	"github.com/coinbase/chainstorage/internal/utils/syncgroup"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

type (
	Extractor struct {
		baseActivity
		blockchainClient client.Client
		blobStorage      blobstorage.BlobStorage
		metaStorage      metastorage.MetaStorage
		failoverManager  endpoints.FailoverManager
	}

	ExtractorParams struct {
		fx.In
		Runtime          cadence.Runtime
		BlockchainClient client.Client `name:"slave"`
		BlobStorage      blobstorage.BlobStorage
		MetaStorage      metastorage.MetaStorage
		FailoverManager  endpoints.FailoverManager
	}

	ExtractorRequest struct {
		Tag              uint32 `validate:"required"`
		Heights          []uint64
		WithBestEffort   bool
		RehydrateFromTag *uint32
		UpgradeFromTag   *uint32
		DataCompression  api.Compression
		Failover         bool
	}

	ExtractorResponse struct {
		Metadatas []*api.BlockMetadata
	}
)

func NewExtractor(params ExtractorParams) *Extractor {
	a := &Extractor{
		baseActivity:     newBaseActivity(ActivityExtractor, params.Runtime),
		blockchainClient: params.BlockchainClient,
		blobStorage:      params.BlobStorage,
		metaStorage:      params.MetaStorage,
		failoverManager:  params.FailoverManager,
	}
	a.register(a.execute)

	return a
}

func (a *Extractor) Execute(ctx workflow.Context, request *ExtractorRequest) (*ExtractorResponse, error) {
	var response ExtractorResponse
	err := a.executeActivity(ctx, request, &response)
	return &response, err
}

func (a *Extractor) execute(ctx context.Context, request *ExtractorRequest) (*ExtractorResponse, error) {
	if err := a.validateRequest(request); err != nil {
		return nil, err
	}

	logger := a.getLogger(ctx).With(zap.Reflect("request", request))

	if request.RehydrateFromTag != nil && request.UpgradeFromTag != nil {
		return nil, xerrors.Errorf("RehydrateFromTag and UpgradeFromTag cannot exist simultaneously")
	}

	if request.Failover {
		failoverCtx, err := a.failoverManager.WithFailoverContext(ctx, endpoints.MasterSlaveClusters)
		if err != nil {
			return nil, xerrors.Errorf("failed to create failover context: %w", err)
		}
		ctx = failoverCtx
	}

	result := make([]*api.BlockMetadata, len(request.Heights))
	group, ctx := syncgroup.New(ctx)
	for i, height := range request.Heights {
		i := i
		height := height
		group.Go(func() error {
			var block *api.Block
			var err error
			if request.UpgradeFromTag != nil {
				block, err = a.upgradeBlock(ctx, height, request, logger)
				if err != nil {
					logger.Error("failed to upgrade block", zap.Error(err))
					return xerrors.Errorf("failed to upgrade block: %w", err)
				}
			} else if request.RehydrateFromTag != nil {
				block, err = a.rehydrateBlock(ctx, height, request, logger)
				if err != nil {
					logger.Error("failed to rehydrate block", zap.Error(err))
					return xerrors.Errorf("failed to rehydrate block: %w", err)
				}
			}

			if block == nil {
				var opts []client.ClientOption
				if request.WithBestEffort {
					// This option is enabled while reprocessing extractors.
					// The block is queried in a best-effort manner and partial data may be returned.
					opts = append(opts, client.WithBestEffort())
				}

				block, err = a.blockchainClient.GetBlockByHeight(ctx, request.Tag, height, opts...)
				if err != nil {
					logger.Error("failed to extract block", zap.Error(err))
					return xerrors.Errorf("failed to extract block %v: %w", height, err)
				}
			}

			objectKey, err := a.blobStorage.Upload(ctx, block, request.DataCompression)
			if err != nil {
				logger.Error("failed to upload to blob store", zap.Error(err))
				return xerrors.Errorf("failed to upload to blob store: %w", err)
			}

			block.Metadata.ObjectKeyMain = objectKey
			result[i] = block.Metadata
			return nil
		})
	}

	if err := group.Wait(); err != nil {
		return nil, xerrors.Errorf("failed to finish extractor: %w", err)
	}

	response := &ExtractorResponse{
		Metadatas: result,
	}
	logger.Info(
		"extracted block",
		zap.Reflect("heights", request.Heights),
		zap.Reflect("response", response),
	)
	return response, nil
}

func (a *Extractor) upgradeBlock(ctx context.Context, height uint64, request *ExtractorRequest, logger *zap.Logger) (*api.Block, error) {
	oldTag := *request.UpgradeFromTag
	newTag := request.Tag

	if oldTag > newTag {
		return nil, xerrors.Errorf("invalid UpgradeFromTag (oldTag=%v, newTag=%v)", oldTag, newTag)
	}

	block, err := a.downloadBlock(ctx, oldTag, height)
	if err != nil {
		if xerrors.Is(err, storage.ErrItemNotFound) {
			return nil, nil
		}
		return nil, xerrors.Errorf("failed to get original block: %w", err)
	}

	block, err = a.blockchainClient.UpgradeBlock(ctx, block, newTag)
	if err != nil {
		return nil, xerrors.Errorf("failed to upgrade block: %w", err)
	}

	tag := block.GetMetadata().GetTag()
	if tag != newTag {
		return nil, xerrors.Errorf("unexpected block tag, expect=%v, actual=%v", newTag, tag)
	}

	logger.Info(
		"upgraded block",
		zap.Uint64("height", block.Metadata.Height),
		zap.Uint32("old_tag", oldTag),
		zap.Uint32("new_tag", newTag),
	)
	return block, nil
}

func (a *Extractor) rehydrateBlock(ctx context.Context, height uint64, request *ExtractorRequest, logger *zap.Logger) (*api.Block, error) {
	oldTag := *request.RehydrateFromTag
	newTag := request.Tag

	if oldTag >= newTag {
		return nil, xerrors.Errorf("invalid RehydrateFromTag (oldTag=%v, newTag=%v)", oldTag, newTag)
	}

	block, err := a.downloadBlock(ctx, oldTag, height)
	if err != nil {
		if xerrors.Is(err, storage.ErrItemNotFound) {
			return nil, nil
		}
		return nil, xerrors.Errorf("failed to get original block: %w", err)
	}

	block.Metadata.Tag = newTag

	logger.Info(
		"rehydrated block",
		zap.Uint64("height", block.Metadata.Height),
		zap.Uint32("old_tag", oldTag),
		zap.Uint32("new_tag", newTag),
	)
	return block, nil
}

func (a *Extractor) downloadBlock(ctx context.Context, tag uint32, height uint64) (*api.Block, error) {
	metadata, err := a.metaStorage.GetBlockByHeight(ctx, tag, height)
	if err != nil {
		return nil, xerrors.Errorf("failed to get block from meta storage: %w", err)
	}

	block, err := a.blobStorage.Download(ctx, metadata)
	if err != nil {
		return nil, xerrors.Errorf("failed to get block from blob storage: %w", err)
	}

	return block, nil
}
