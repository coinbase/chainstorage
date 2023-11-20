package main

import (
	"context"

	"github.com/coinbase/chainstorage/sdk/services"

	"github.com/spf13/cobra"
	"go.uber.org/zap"
	"golang.org/x/xerrors"

	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
	"github.com/coinbase/chainstorage/sdk"
)

var (
	sdkFlags struct {
		startHeight     uint64
		endHeight       uint64
		hash            string
		schema          string
		sequence        int64
		eventTag        uint32
		tag             uint32
		blockValidation bool
	}

	sdkCmd = &cobra.Command{
		Use: "sdk",
	}

	blocksByRangeCmd = &cobra.Command{
		Use:   "range",
		Short: "Fetch and parse blocks in the given range.",
		RunE: newSDKCommand(func(session sdk.Session) error {
			resp, err := session.Client().GetBlocksByRangeWithTag(context.Background(), sdkFlags.tag, sdkFlags.startHeight, sdkFlags.endHeight)
			if err != nil {
				return xerrors.Errorf("failed to get block file metadata: %w", err)
			}

			for _, rawBlock := range resp {
				if err := printBlock(session.Parser(), rawBlock, true); err != nil {
					return xerrors.Errorf("failed to output block: %w", err)
				}
			}
			return nil
		}),
	}

	getBlockCmd = &cobra.Command{
		Use:   "get",
		Short: "Fetch and parse a block.",
		RunE: newSDKCommand(func(session sdk.Session) error {
			client := session.Client()
			if sdkFlags.blockValidation {
				client.SetBlockValidation(true)
			}

			rawBlock, err := client.GetBlockWithTag(context.Background(), sdkFlags.tag, sdkFlags.startHeight, sdkFlags.hash)
			if err != nil {
				return xerrors.Errorf("failed to get block file metadata: %w", err)
			}

			if err := printBlock(session.Parser(), rawBlock, false); err != nil {
				return xerrors.Errorf("failed to output block: %w", err)
			}

			return nil
		}),
	}

	latestBlockCmd = &cobra.Command{
		Use:   "latest",
		Short: "Fetch and parse latest block",
		RunE: newSDKCommand(func(session sdk.Session) error {
			height, err := session.Client().GetLatestBlockWithTag(context.Background(), sdkFlags.tag)
			if err != nil {
				return xerrors.Errorf("failed to get height of the latest block: %w", err)
			}
			logger.Info("Height of the latest block is", zap.Uint64("latestHeight", height))

			return nil
		}),
	}

	streamBlockCmd = &cobra.Command{
		Use:   "stream",
		Short: "Stream blocks",
		RunE: newSDKCommand(func(session sdk.Session) error {
			ch, err := session.Client().StreamChainEvents(context.Background(), sdk.StreamingConfiguration{
				ChainEventsRequest: &api.ChainEventsRequest{
					SequenceNum: sdkFlags.sequence,
					EventTag:    sdkFlags.eventTag,
				},
			})
			if err != nil {
				return xerrors.Errorf("failed to invoke StreamChainEvents: %w", err)
			}

			for event := range ch {
				if event.Error != nil {
					return xerrors.Errorf("received an error from StreamChainEvents: %w", event.Error)
				}

				logger.Info("fetched event", zap.Reflect("event", event.BlockchainEvent))
				if err := printBlock(session.Parser(), event.Block, true); err != nil {
					return xerrors.Errorf("failed to output block: %w", err)
				}
			}
			return nil
		}),
	}

	getBlockByTransactionCmd = &cobra.Command{
		Use:   "get-by-transaction",
		Short: "Get block by transaction",
		RunE: newSDKCommand(func(session sdk.Session) error {
			rawBlocks, err := session.Client().GetBlockByTransaction(context.Background(), sdkFlags.tag, sdkFlags.hash)
			if err != nil {
				return xerrors.Errorf("failed to get block by transaction: %w", err)
			}

			if len(rawBlocks) == 0 {
				logger.Info("block not found")
				return nil
			}

			for _, rawBlock := range rawBlocks {
				if err := printBlock(session.Parser(), rawBlock, false); err != nil {
					return xerrors.Errorf("failed to output block: %w", err)
				}
			}

			return nil
		}),
	}
)

func newSDKCommand(fn func(session sdk.Session) error) func(cmd *cobra.Command, args []string) error {
	return func(cmd *cobra.Command, args []string) error {
		if err := initializeChainInfoFromFlags(); err != nil {
			return xerrors.Errorf("failed to init common flags: %w", err)
		}

		manager := services.NewManager()
		defer manager.Shutdown()

		session, err := sdk.New(manager, &sdk.Config{
			Blockchain: *blockchain,
			Network:    *network,
			Env:        env,
			Sidechain:  sidechain,
		})
		if err != nil {
			return xerrors.Errorf("failed to create session: %w", err)
		}

		return fn(session)
	}
}

func init() {
	// shared flags for sub-cmds
	sdkCmd.PersistentFlags().Uint64Var(&sdkFlags.startHeight, "start-height", 0, "block start height")
	sdkCmd.PersistentFlags().Uint64Var(&sdkFlags.endHeight, "end-height", 0, "block end height")
	sdkCmd.PersistentFlags().Uint32Var(&sdkFlags.tag, "tag", 0, "block tag")

	getBlockCmd.Flags().StringVar(&sdkFlags.hash, "hash", "", "block hash (optional)")
	getBlockCmd.Flags().BoolVar(&sdkFlags.blockValidation, "block-validation", false, "validate block (optional)")

	streamBlockCmd.Flags().Int64Var(&sdkFlags.sequence, "sequence", 0, "last processed event sequence")
	streamBlockCmd.Flags().Uint32Var(&sdkFlags.eventTag, "event-tag", 0, "event tag")
	if err := streamBlockCmd.MarkFlagRequired("event-tag"); err != nil {
		panic(err)
	}

	getBlockByTransactionCmd.Flags().StringVar(&sdkFlags.hash, "hash", "", "transaction hash")
	if err := getBlockByTransactionCmd.MarkFlagRequired("hash"); err != nil {
		panic(err)
	}

	sdkCmd.AddCommand(blocksByRangeCmd)
	sdkCmd.AddCommand(getBlockCmd)
	sdkCmd.AddCommand(latestBlockCmd)
	sdkCmd.AddCommand(streamBlockCmd)
	sdkCmd.AddCommand(getBlockByTransactionCmd)

	rootCmd.AddCommand(sdkCmd)
}
