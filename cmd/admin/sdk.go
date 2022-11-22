package main

import (
	"context"

	"github.com/spf13/cobra"
	"go.uber.org/zap"
	"golang.org/x/xerrors"

	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
	"github.com/coinbase/chainstorage/sdk"
)

var (
	sdkFlags struct {
		startHeight uint64
		endHeight   uint64
		hash        string
		schema      string
		sequence    int64
		eventTag    uint32
		tag         uint32
	}

	sdkCmd = &cobra.Command{
		Use: "sdk",
	}

	blocksByRangeCmd = &cobra.Command{
		Use:   "range",
		Short: "Fetch and parse blocks in the given range.",
		RunE: func(cmd *cobra.Command, args []string) error {
			manager := sdk.NewManager(sdk.WithLogger(logger))
			defer manager.Shutdown()

			session, err := newSession(manager)
			if err != nil {
				return xerrors.Errorf("failed to create session: %w", err)
			}

			resp, err := session.Client().GetBlocksByRange(context.Background(), sdkFlags.startHeight, sdkFlags.endHeight)
			if err != nil {
				return xerrors.Errorf("failed to get block file metadata: %w", err)
			}

			for _, rawBlock := range resp {
				if err := printBlock(session.Parser(), rawBlock, true); err != nil {
					return xerrors.Errorf("failed to output block: %w", err)
				}
			}
			return nil
		},
	}

	getBlockCmd = &cobra.Command{
		Use:   "get",
		Short: "Fetch and parse a block.",
		RunE: func(cmd *cobra.Command, args []string) error {
			manager := sdk.NewManager(sdk.WithLogger(logger))
			defer manager.Shutdown()

			session, err := newSession(manager)
			if err != nil {
				return xerrors.Errorf("failed to create session: %w", err)
			}

			rawBlock, err := session.Client().GetBlock(context.Background(), sdkFlags.startHeight, sdkFlags.hash)
			if err != nil {
				return xerrors.Errorf("failed to get block file metadata: %w", err)
			}

			if err := printBlock(session.Parser(), rawBlock, false); err != nil {
				return xerrors.Errorf("failed to output block: %w", err)
			}

			return nil
		},
	}

	latestBlockCmd = &cobra.Command{
		Use:   "latest",
		Short: "Fetch and parse latest block",
		RunE: func(cmd *cobra.Command, args []string) error {
			manager := sdk.NewManager(sdk.WithLogger(logger))
			defer manager.Shutdown()

			session, err := newSession(manager)
			if err != nil {
				return xerrors.Errorf("failed to create session: %w", err)
			}

			height, err := session.Client().GetLatestBlock(context.Background())
			if err != nil {
				return xerrors.Errorf("failed to get height of the latest block: %w", err)
			}
			logger.Info("Height of the latest block is", zap.Uint64("latestHeight", height))

			return nil
		},
	}

	streamBlockCmd = &cobra.Command{
		Use:   "stream",
		Short: "Stream blocks",
		RunE: func(cmd *cobra.Command, args []string) error {
			manager := sdk.NewManager(sdk.WithLogger(logger))
			defer manager.Shutdown()

			session, err := newSession(manager)
			if err != nil {
				return xerrors.Errorf("failed to create session: %w", err)
			}

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
		},
	}
)

func newSession(manager sdk.SystemManager) (sdk.Session, error) {
	err := initializeBlockchainNetworkEnvFromFlags()
	if err != nil {
		return nil, err
	}
	return sdk.New(manager, &sdk.Config{
		Blockchain: blockchain,
		Network:    network,
		Env:        env,
	})
}

func init() {
	// shared flags for sub-cmds
	sdkCmd.PersistentFlags().Uint64Var(&sdkFlags.startHeight, "start-height", 0, "block start height")
	sdkCmd.PersistentFlags().Uint64Var(&sdkFlags.endHeight, "end-height", 0, "block end height")

	getBlockCmd.Flags().StringVar(&sdkFlags.hash, "hash", "", "block hash (optional)")

	streamBlockCmd.Flags().Int64Var(&sdkFlags.sequence, "sequence", 0, "last processed event sequence")
	streamBlockCmd.Flags().Uint32Var(&sdkFlags.eventTag, "event-tag", 0, "event tag")
	if err := streamBlockCmd.MarkFlagRequired("event-tag"); err != nil {
		panic(err)
	}

	sdkCmd.AddCommand(blocksByRangeCmd)
	sdkCmd.AddCommand(getBlockCmd)
	sdkCmd.AddCommand(latestBlockCmd)
	sdkCmd.AddCommand(streamBlockCmd)

	rootCmd.AddCommand(sdkCmd)
}
