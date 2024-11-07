package main

import (
	"context"
	"fmt"

	"github.com/fatih/color"
	"github.com/spf13/cobra"
	"go.uber.org/fx"
	"go.uber.org/zap"

	"github.com/coinbase/chainstorage/internal/aws"
	"github.com/coinbase/chainstorage/internal/blockchain/client"
	"github.com/coinbase/chainstorage/internal/blockchain/endpoints"
	"github.com/coinbase/chainstorage/internal/blockchain/jsonrpc"
	"github.com/coinbase/chainstorage/internal/blockchain/parser"
	"github.com/coinbase/chainstorage/internal/blockchain/restapi"
	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/s3"
	"github.com/coinbase/chainstorage/internal/storage"
	"github.com/coinbase/chainstorage/internal/storage/blobstorage"
	"github.com/coinbase/chainstorage/internal/storage/metastorage"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

var (
	backfillFlags struct {
		tag             uint32
		startHeight     uint64
		endHeight       uint64
		updateWatermark bool
		dataCompression string
	}

	backfillCmd = &cobra.Command{
		Use:   "backfill",
		Short: "Backfill a block",
		RunE: func(cmd *cobra.Command, args []string) error {
			var deps struct {
				fx.In
				Config      *config.Config
				Client      client.Client `name:"slave"`
				BlobStorage blobstorage.BlobStorage
				MetaStorage metastorage.MetaStorage
			}

			app := startApp(
				client.Module,
				jsonrpc.Module,
				restapi.Module,
				endpoints.Module,
				aws.Module,
				s3.Module,
				storage.Module,
				parser.Module,
				fx.Populate(&deps),
			)
			defer app.Close()

			ctx := context.Background()
			tag := deps.Config.GetEffectiveBlockTag(backfillFlags.tag)
			updateWatermark := backfillFlags.updateWatermark
			compression := deps.Config.AWS.Storage.DataCompression
			startHeight := backfillFlags.startHeight
			endHeight := backfillFlags.endHeight
			if endHeight == 0 {
				endHeight = startHeight + 1
			}

			if startHeight >= endHeight {
				return fmt.Errorf("invalid block range: [%v, %v)", startHeight, endHeight)
			}

			chainInfo := fmt.Sprintf("%v-%v", commonFlags.blockchain, commonFlags.network)
			if commonFlags.sidechain != "" {
				chainInfo = fmt.Sprintf("%v-%v", chainInfo, commonFlags.sidechain)
			}

			prompt := color.CyanString(fmt.Sprintf(
				"Are you sure you want backfill [%v, %v) in %v::%v? (y/N) ",
				startHeight, endHeight, env, chainInfo,
			))
			if !confirm(prompt) {
				return nil
			}

			blockMetadatas := make([]*api.BlockMetadata, 0, endHeight-startHeight)
			for height := startHeight; height < endHeight; height++ {
				block, err := deps.Client.GetBlockByHeight(ctx, tag, height)
				if err != nil {
					return fmt.Errorf("failed to get block: %w", err)
				}

				objectKey, err := deps.BlobStorage.Upload(ctx, block, compression)
				if err != nil {
					return fmt.Errorf("failed to upload block in blob storage: %w", err)
				}
				block.Metadata.ObjectKeyMain = objectKey

				blockMetadatas = append(blockMetadatas, block.Metadata)
				logger.Info(
					"block is ingested successfully",
					zap.Uint32("tag", tag),
					zap.Uint64("height", height),
					zap.String("compression", compression.String()),
					zap.Reflect("metadata", block.Metadata),
				)
			}

			if err := deps.MetaStorage.PersistBlockMetas(ctx, updateWatermark, blockMetadatas, nil); err != nil {
				return fmt.Errorf("failed to persist block in meta storage: %w", err)
			}

			logger.Info(
				"blocks are persisted successfully",
				zap.Uint32("tag", tag),
				zap.Uint64("startHeight", startHeight),
				zap.Uint64("endHeight", endHeight),
				zap.Bool("updateWatermark", updateWatermark),
			)

			return nil
		},
	}
)

func init() {
	backfillCmd.Flags().Uint32Var(&backfillFlags.tag, "tag", 0, "tag")
	backfillCmd.Flags().Uint64Var(&backfillFlags.startHeight, "start-height", 0, "start height (inclusive)")
	backfillCmd.Flags().Uint64Var(&backfillFlags.endHeight, "end-height", 0, "end height (exclusive). if omitted, it defaults to start height + 1.")
	backfillCmd.Flags().BoolVar(&backfillFlags.updateWatermark, "update-watermark", false, "whether to update latest watermark")
	rootCmd.AddCommand(backfillCmd)
}
