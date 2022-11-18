package main

import (
	"context"

	"github.com/spf13/cobra"
	"go.uber.org/fx"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/blockchain/client"
	"github.com/coinbase/chainstorage/internal/blockchain/endpoints"
	"github.com/coinbase/chainstorage/internal/blockchain/jsonrpc"
	"github.com/coinbase/chainstorage/internal/blockchain/parser"
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
				parser.Module,
				endpoints.Module,
				fx.Populate(&deps),
			)
			defer app.Close()

			tag := deps.Config.GetEffectiveBlockTag(blockFlags.tag)
			rawBlock, err := deps.Client.GetBlockByHeight(context.Background(), tag, blockFlags.height)
			if err != nil {
				return xerrors.Errorf("failed to get block: %w", err)
			}

			if err := printBlock(deps.Parser, rawBlock, false); err != nil {
				return xerrors.Errorf("failed to print block: %w", err)
			}
			return nil
		},
	}
)

func init() {
	blockCmd.Flags().Uint32Var(&blockFlags.tag, "tag", 0, "tag")
	blockCmd.Flags().Uint64Var(&blockFlags.height, "height", 0, "block height")
	rootCmd.AddCommand(blockCmd)
}
