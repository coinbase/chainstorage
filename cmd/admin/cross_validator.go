package main

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/aws"
	"github.com/coinbase/chainstorage/internal/blockchain/client"
	"github.com/coinbase/chainstorage/internal/blockchain/endpoints"
	"github.com/coinbase/chainstorage/internal/blockchain/jsonrpc"
	"github.com/coinbase/chainstorage/internal/blockchain/parser"
	"github.com/coinbase/chainstorage/internal/blockchain/restapi"
	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/s3"
	"github.com/coinbase/chainstorage/internal/storage"
	"github.com/coinbase/chainstorage/internal/storage/blobstorage/downloader"
	"github.com/coinbase/chainstorage/sdk"
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
		Short: "Validate a block with native format",
		RunE: func(cmd *cobra.Command, args []string) error {
			var deps struct {
				fx.In
				Config          *config.Config
				ValidatorClient client.Client `name:"validator"`
				StorageClient   sdk.Client
				Parser          parser.Parser
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
			tag := deps.Config.GetEffectiveBlockTag(crossValidatorFlags.tag)
			height := crossValidatorFlags.height

			persistedRawBlock, err := deps.StorageClient.GetBlockWithTag(ctx, tag, height, "")
			if err != nil {
				return xerrors.Errorf("failed to get block from storage: %w", err)
			}

			hash := persistedRawBlock.Metadata.Hash
			expectedRawBlock, err := deps.ValidatorClient.GetBlockByHash(ctx, tag, height, hash)
			if err != nil {
				return xerrors.Errorf("failed to get block from validator client (tag=%v, height=%v, hash=%v): %w", tag, height, hash, err)
			}

			persistedNativeBlock, err := deps.Parser.ParseNativeBlock(ctx, persistedRawBlock)
			if err != nil {
				return xerrors.Errorf("failed to parse actual raw block using native parser for block {%+v}: %w", persistedRawBlock.Metadata, err)
			}

			expectedNativeBlock, err := deps.Parser.ParseNativeBlock(ctx, expectedRawBlock)
			if err != nil {
				return xerrors.Errorf("failed to parse expected raw block using native parser for block {%+v}: %w", expectedRawBlock.Metadata, err)
			}

			if err := deps.Parser.CompareNativeBlocks(ctx, height, expectedNativeBlock, persistedNativeBlock); err != nil {
				var checkerErr *parser.ParityCheckFailedError
				if xerrors.As(err, &checkerErr) {
					fmt.Printf("cross validation diff report: %v", checkerErr.Diff)
				}

				logger.Error("cross validation failed",
					zap.Error(err),
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
				ext := filepath.Ext(out)
				outExpected := fmt.Sprintf("%v_%v%v", strings.TrimSuffix(out, ext), "expected", ext)
				if err := logBlock(expectedRawBlock.Metadata, expectedNativeBlock, outExpected); err != nil {
					return xerrors.Errorf("failed to log expected native block: %w", err)
				}

				outActual := fmt.Sprintf("%v_%v%v", strings.TrimSuffix(out, ext), "actual", ext)
				if err := logBlock(persistedRawBlock.Metadata, persistedNativeBlock, outActual); err != nil {
					return xerrors.Errorf("failed to log actual native block: %w", err)
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
				restapi.Module,
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
