package activity

import (
	"context"

	"go.temporal.io/sdk/workflow"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/cadence"
	"github.com/coinbase/chainstorage/internal/gateway"
	"github.com/coinbase/chainstorage/internal/storage/blobstorage"
	"github.com/coinbase/chainstorage/internal/storage/blobstorage/downloader"
	"github.com/coinbase/chainstorage/internal/storage/metastorage"
	"github.com/coinbase/chainstorage/internal/utils/fxparams"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

type (
	Replicator struct {
		baseActivity
		client          gateway.Client
		blockDownloader downloader.BlockDownloader
		metaStorage     metastorage.MetaStorage
		blobStorage     blobstorage.BlobStorage
	}

	ReplicatorParams struct {
		fx.In
		fxparams.Params
		Runtime         cadence.Runtime
		Client          gateway.Client
		BlockDownloader downloader.BlockDownloader
		MetaStorage     metastorage.MetaStorage
		BlobStorage     blobstorage.BlobStorage
	}

	ReplicatorRequest struct {
		Tag         uint32
		StartHeight uint64
		EndHeight   uint64
		Parallelism int
		Compression api.Compression
	}

	ReplicatorResponse struct {
		StartHeight uint64
		EndHeight   uint64
	}
)

func NewReplicator(params ReplicatorParams) *Replicator {
	a := &Replicator{
		baseActivity:    newBaseActivity(ActivityReplicator, params.Runtime),
		client:          params.Client,
		blockDownloader: params.BlockDownloader,
		metaStorage:     params.MetaStorage,
		blobStorage:     params.BlobStorage,
	}
	a.register(a.execute)
	return a
}

func (a *Replicator) Execute(ctx workflow.Context, request *ReplicatorRequest) (*ReplicatorResponse, error) {
	var response ReplicatorResponse
	err := a.executeActivity(ctx, request, &response)
	return &response, err
}

func (a *Replicator) execute(ctx context.Context, request *ReplicatorRequest) (*ReplicatorResponse, error) {
	if err := a.validateRequest(request); err != nil {
		return nil, err
	}
	logger := a.getLogger(ctx).With(zap.Reflect("request", request))
	logger.Info("Fetching block range",
		zap.Uint64("startHeight", request.StartHeight),
		zap.Uint64("endHeight", request.EndHeight))
	blocks, err := a.client.GetBlockFilesByRange(ctx, &api.GetBlockFilesByRangeRequest{
		Tag:         request.Tag,
		StartHeight: request.StartHeight,
		EndHeight:   request.EndHeight,
	})
	if err != nil {
		return nil, err
	}
	blockMetas := make([]*api.BlockMetadata, len(blocks.Files))
	logger.Info("Replicating block data")
	group, errgroupCtx := errgroup.WithContext(ctx)
	group.SetLimit(request.Parallelism)
	for i := range blocks.Files {
		i := i
		group.Go(func() error {
			blockFile := blocks.Files[i]
			logger.Debug(
				"downloading block",
				zap.Uint32("tag", blockFile.Tag),
				zap.Uint64("height", blockFile.Height),
				zap.String("hash", blockFile.Hash),
			)
			block, err := a.blockDownloader.Download(errgroupCtx, blockFile)
			if err != nil {
				return xerrors.Errorf("failed download block file from %s: %w", blockFile.GetFileUrl(), err)
			}
			_, err = a.blobStorage.Upload(errgroupCtx, block, request.Compression)
			blockMetas[i] = block.Metadata
			return err
		})
	}
	if err := group.Wait(); err != nil {
		return nil, xerrors.Errorf("failed to replicate block files: %w", err)
	}
	logger.Info("Persisting block metadata")
	err = a.metaStorage.PersistBlockMetas(ctx, false, blockMetas, nil)
	if err != nil {
		return nil, err
	}

	return &ReplicatorResponse{
		StartHeight: request.StartHeight,
		EndHeight:   request.EndHeight,
	}, nil
}
