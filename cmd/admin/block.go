package main

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"
	"go.uber.org/fx"
	"go.uber.org/zap"

	"github.com/coinbase/chainstorage/internal/blockchain/client"
	"github.com/coinbase/chainstorage/internal/blockchain/endpoints"
	"github.com/coinbase/chainstorage/internal/blockchain/jsonrpc"
	"github.com/coinbase/chainstorage/internal/blockchain/parser"
	"github.com/coinbase/chainstorage/internal/blockchain/restapi"
	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/storage/blobstorage/downloader"
)

var (
	blockFlags struct {
		tag    uint32
		height uint64
	}
)

var (
	blockCmd = &cobra.Command{
		Use:   "block",
		Short: "Fetch a block",
		RunE: func(cmd *cobra.Command, args []string) error {
			var deps struct {
				fx.In
				Config *config.Config
				Client client.Client `name:"slave"`
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

			tag := deps.Config.GetEffectiveBlockTag(blockFlags.tag)
			rawBlock, err := deps.Client.GetBlockByHeight(context.Background(), tag, blockFlags.height)
			if err != nil {
				return fmt.Errorf("failed to get block: %w", err)
			}

			if err := printBlock(deps.Parser, rawBlock, false); err != nil {
				return fmt.Errorf("failed to print block: %w", err)
			}
			return nil
		},
	}

	latestHeightCmd = &cobra.Command{
		Use:   "latest",
		Short: "Fetch the latest height",
		RunE: func(cmd *cobra.Command, args []string) error {
			var deps struct {
				fx.In
				Client client.Client `name:"slave"`
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

			height, err := deps.Client.GetLatestHeight(context.Background())
			if err != nil {
				return fmt.Errorf("failed to get block: %w", err)
			}

			logger.Info("latest height", zap.Uint64("height", height))
			return nil
		},
	}
)

func init() {
	blockCmd.Flags().Uint32Var(&blockFlags.tag, "tag", 0, "tag")
	blockCmd.Flags().Uint64Var(&blockFlags.height, "height", 0, "block height")
	rootCmd.AddCommand(blockCmd)
	blockCmd.AddCommand(latestHeightCmd)
}
