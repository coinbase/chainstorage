package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var (
	rootCmd = &cobra.Command{
		Use:          "admin",
		Short:        "admin is a utility for managing chainstorage",
		SilenceUsage: true,
	}
)

func main() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "failed to execute command: %+v\n", err)
		os.Exit(1)
	}
}
