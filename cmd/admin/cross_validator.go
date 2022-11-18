package main

import (
	"context"
	"fmt"

	"github.com/golang/protobuf/proto"

	"github.com/spf13/cobra"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/aws"
	"github.com/coinbase/chainstorage/internal/blockchain/client"
	"github.com/coinbase/chainstorage/internal/blockchain/endpoints"
	"github.com/coinbase/chainstorage/internal/blockchain/jsonrpc"
	"github.com/coinbase/chainstorage/internal/blockchain/parser"
	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/s3"
	"github.com/coinbase/chainstorage/internal/storage"
	"github.com/coinbase/chainstorage/internal/storage/blobstorage"
	"github.com/coinbase/chainstorage/internal/storage/blobstorage/downloader"
	"github.com/coinbase/chainstorage/internal/storage/metastorage"
)

var (
	crossValidatorFlags struct {
		tag    uint32
		height uint64
	}

	crossValidatorCmd = &cobra.Command{
		Use: "validator",
	}

	validateCmd = &cobra.Command{
		Use:   "validate",
		Short: "Validate a block",
		RunE: func(cmd *cobra.Command, args []string) error {
			var deps struct {
				fx.In
				Config      *config.Config
				Client      client.Client `name:"validator"`
				BlobStorage blobstorage.BlobStorage
				MetaStorage metastorage.MetaStorage
			}

			app := startApp(
				client.Module,
				jsonrpc.Module,
				endpoints.Module,
				aws.Module,
				s3.Module,
				storage.Module,
				parser.Module,
				fx.Populate(&deps),
			)
			defer app.Close()

			ctx := context.Background()
			tag := deps.Config.GetEffectiveBlockTag(crossValidatorFlags.tag)
			height := crossValidatorFlags.height

			persistedBlockMetaData, err := deps.MetaStorage.GetBlockByHeight(ctx, tag, height)
			if err != nil {
				return xerrors.Errorf("failed to get block meta data from meta storage (height=%d): %w", height, err)
			}
			persistedRawBlock, err := deps.BlobStorage.Download(ctx, persistedBlockMetaData)
			if err != nil {
				return xerrors.Errorf("failed to get block from blobStorage (key=%s): %w", persistedBlockMetaData.ObjectKeyMain, err)
			}

			hash := persistedBlockMetaData.Hash
			expectedRawBlock, err := deps.Client.GetBlockByHash(ctx, tag, height, hash)
			if err != nil {
				return xerrors.Errorf("failed to get block from validator client (tag=%v, height=%v, hash=%v): %w", tag, height, hash, err)
			}

			// Skip check of ObjectKeyMain
			persistedRawBlock.Metadata.ObjectKeyMain = expectedRawBlock.Metadata.ObjectKeyMain
			// Skip check for timestamp in metadata
			if persistedRawBlock.Metadata.GetTimestamp() == nil {
				persistedRawBlock.Metadata.Timestamp = expectedRawBlock.Metadata.Timestamp
			}

			if !proto.Equal(expectedRawBlock, persistedRawBlock) {
				logger.Error("cross validation failed",
					zap.String("hash", hash),
					zap.Uint64("height", height),
					zap.Reflect("expected_metadata", expectedRawBlock.Metadata),
					zap.Reflect("actual_metadata", persistedRawBlock.Metadata),
				)
			} else {
				logger.Info("cross validation passed")
			}

			out := commonFlags.out
			if out != "" {
				outExpected := fmt.Sprintf("%v_%v", out, "expected")
				if err := logBlock(expectedRawBlock.Metadata, expectedRawBlock, outExpected); err != nil {
					return xerrors.Errorf("failed to log expected block: %w", err)
				}

				outActual := fmt.Sprintf("%v_%v", out, "actual")
				if err := logBlock(persistedRawBlock.Metadata, persistedRawBlock, outActual); err != nil {
					return xerrors.Errorf("failed to log actual block: %w", err)
				}

				logger.Info("log blocks in files",
					zap.String("expected", outExpected),
					zap.String("actual", outActual),
				)
			}

			logger.Info(
				"validation finished",
				zap.Uint32("tag", tag),
				zap.Uint64("height", height),
			)

			return nil
		},
	}

	validatorBlockCmd = &cobra.Command{
		Use:   "block",
		Short: "Fetch a block using validator client",
		RunE: func(cmd *cobra.Command, args []string) error {
			var deps struct {
				fx.In
				Config *config.Config
				Client client.Client `name:"validator"`
				Parser parser.Parser
			}

			app := startApp(
				client.Module,
				downloader.Module,
				jsonrpc.Module,
				parser.Module,
				endpoints.Module,
				fx.Populate(&deps),
			)
			defer app.Close()

			ctx := context.Background()
			tag := deps.Config.GetEffectiveBlockTag(crossValidatorFlags.tag)
			height := crossValidatorFlags.height

			rawBlock, err := deps.Client.GetBlockByHeight(ctx, tag, height)
			if err != nil {
				return xerrors.Errorf("failed to get block using validator client: %w", err)
			}

			if err := printBlock(deps.Parser, rawBlock, false); err != nil {
				return xerrors.Errorf("failed to print block %v: %w", height, err)
			}
			return nil
		},
	}
)

func init() {
	// shared flags for sub-cmds
	crossValidatorCmd.PersistentFlags().Uint32Var(&crossValidatorFlags.tag, "tag", 0, "tag")
	crossValidatorCmd.PersistentFlags().Uint64Var(&crossValidatorFlags.height, "height", 0, "block height")

	crossValidatorCmd.AddCommand(validateCmd)
	crossValidatorCmd.AddCommand(validatorBlockCmd)

	rootCmd.AddCommand(crossValidatorCmd)
}
