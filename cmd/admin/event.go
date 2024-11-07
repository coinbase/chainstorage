package main

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"strings"

	"go.uber.org/zap"

	"github.com/fatih/color"
	"github.com/spf13/cobra"
	"go.uber.org/fx"

	"github.com/coinbase/chainstorage/internal/aws"
	"github.com/coinbase/chainstorage/internal/storage"
)

const (
	eventIdFlagName  = "event-id"
	eventTagFlagName = "event-tag"
)

var (
	eventCmd = &cobra.Command{
		Use:   "event",
		Short: "tool for managing events storage",
	}

	resetWatermarkFlags struct {
		eventId  int64
		eventTag uint32
	}

	resetWatermarkCmd = &cobra.Command{
		Use:   "reset-watermark",
		Short: "reset watermark for events table",
		RunE: func(cmd *cobra.Command, args []string) error {
			var deps struct {
				fx.In
				EventStorage storage.EventStorage
			}
			app := startApp(
				storage.Module,
				aws.Module,
				fx.Populate(&deps),
			)
			defer app.Close()
			eventTag := resetWatermarkFlags.eventTag
			currentMaxEventId, err := deps.EventStorage.GetMaxEventId(context.Background(), eventTag)
			if err != nil {
				return fmt.Errorf("failed to get current max event id: %w", err)
			}
			logger.Info(fmt.Sprintf("current currentMaxEventId is %d", currentMaxEventId))

			chainInfo := fmt.Sprintf("%v-%v", commonFlags.blockchain, commonFlags.network)
			if commonFlags.sidechain != "" {
				chainInfo = fmt.Sprintf("%v-%v", chainInfo, commonFlags.sidechain)
			}
			fmt.Printf(
				"%v%v%v%v%v%v%v",
				color.CyanString("Are you sure you want to set max event id to be "),
				color.MagentaString(fmt.Sprintf("%d ", resetWatermarkFlags.eventId)),
				color.CyanString("with event tag "),
				color.MagentaString(fmt.Sprintf("%d ", eventTag)),
				color.CyanString("for "),
				color.MagentaString(fmt.Sprintf("%v::%v", env, chainInfo)),
				color.CyanString("? (y/N) "),
			)

			response, err := bufio.NewReader(os.Stdin).ReadString('\n')
			if err != nil {
				return fmt.Errorf("failed to read from console: %w", err)
			}

			if strings.ToLower(strings.TrimSpace(response)) != "y" {
				return nil
			}
			err = deps.EventStorage.SetMaxEventId(context.Background(), eventTag, resetWatermarkFlags.eventId)
			if err != nil {
				logger.Error("failed to set max event id",
					zap.Int64("maxEventId", resetWatermarkFlags.eventId),
					zap.Uint32("eventTag", eventTag),
				)
				return fmt.Errorf("failed to set max event id with eventTag=%v: %w", eventTag, err)
			} else {
				logger.Info("successfully set max event id",
					zap.Int64("maxEventId", resetWatermarkFlags.eventId),
					zap.Uint32("eventTag", eventTag),
				)
			}
			return nil
		},
	}
)

func init() {
	resetWatermarkCmd.Flags().Int64Var(&resetWatermarkFlags.eventId, eventIdFlagName, storage.EventIdDeleted, "max event id")
	resetWatermarkCmd.Flags().Uint32Var(&resetWatermarkFlags.eventTag, eventTagFlagName, uint32(0), "event tag")
	if err := resetWatermarkCmd.MarkFlagRequired(eventTagFlagName); err != nil {
		panic(err)
	}

	eventCmd.AddCommand(resetWatermarkCmd)
	rootCmd.AddCommand(eventCmd)
}
