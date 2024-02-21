package activity

import (
	"context"

	"go.temporal.io/sdk/workflow"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/cadence"
	"github.com/coinbase/chainstorage/internal/storage/blobstorage"
	"github.com/coinbase/chainstorage/internal/storage/metastorage"
	"github.com/coinbase/chainstorage/internal/utils/fxparams"
	"github.com/coinbase/chainstorage/internal/utils/syncgroup"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
	"github.com/coinbase/chainstorage/sdk"
)

type (
	Replicator struct {
		baseActivity

		client      sdk.Client
		metaStorage metastorage.MetaStorage
		blobStorage blobstorage.BlobStorage
	}

	ReplicatorParams struct {
		fx.In
		fxparams.Params
		Runtime     cadence.Runtime
		Client      sdk.Client
		MetaStorage metastorage.MetaStorage
		BlobStorage blobstorage.BlobStorage
	}

	ReplicatorRequest struct {
		Tag              uint32
		StartBlockHeight uint64
		EndBlockHeight   uint64
		UpdateWatermark  bool
		Parallelism      int
		Compression      api.Compression
	}

	ReplicatorResponse struct {
	}
)

func NewReplicator(params ReplicatorParams) *Replicator {
	a := &Replicator{
		baseActivity: newBaseActivity(ActivityReplicator, params.Runtime),
		client:       params.Client,
		metaStorage:  params.MetaStorage,
		blobStorage:  params.BlobStorage,
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
		zap.Uint64("startHeight", request.StartBlockHeight),
		zap.Uint64("endHeight", request.EndBlockHeight))
	blocks, err := a.client.GetBlocksByRangeWithTag(ctx, request.Tag, request.StartBlockHeight, request.EndBlockHeight)
	if err != nil {
		return nil, err
	}
	blockMetas := make([]*api.BlockMetadata, len(blocks))
	logger.Info("Uploading block data")
	group, sgctx := syncgroup.New(ctx, syncgroup.WithThrottling(request.Parallelism))
	for i, block := range blocks {
		blockMetas[i] = block.Metadata
		block := block
		group.Go(func() error {
			_, err := a.blobStorage.Upload(sgctx, block, request.Compression)
			return err
		})
	}
	if err := group.Wait(); err != nil {
		return nil, xerrors.Errorf("failed to upload blocks: %w", err)
	}
	logger.Info("Persisting block metadata")
	err = a.metaStorage.PersistBlockMetas(ctx, request.UpdateWatermark, blockMetas, nil)
	if err != nil {
		return nil, err
	}

	return &ReplicatorResponse{}, nil
}
