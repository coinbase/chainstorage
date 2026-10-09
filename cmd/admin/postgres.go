package main

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/storage/metastorage/postgres"
)

const (
	masterUserFlag     = "master-user"
	masterPassFlag     = "master-password"
	hostFlag           = "host"
	portFlag           = "port"
	workerUserFlag     = "worker-user"
	workerPassFlag     = "worker-password"
	serverUserFlag     = "server-user"
	serverPassFlag     = "server-password"
	sslModeFlag        = "ssl-mode"
	dbNameFlag         = "db-name"
	connectTimeoutFlag = "connect-timeout"
)

func newPostgresCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "setup-postgres",
		Short: "Create database and roles for a new network in PostgreSQL.",
		Long: `Create database and roles for a new network in PostgreSQL.

This command connects to PostgreSQL using master/admin credentials and creates:
1. A worker role (read/write permissions) 
2. A server role (read-only permissions)
3. A database owned by the worker role
4. Proper permissions for both roles

Example usage:
  # Set up database for ethereum-mainnet
  chainstorage admin setup-postgres --blockchain ethereum --network mainnet --env local --master-user postgres --master-password mypassword

  # Set up database with custom name
  chainstorage admin setup-postgres --blockchain ethereum --network mainnet --env local --db-name my_custom_db --master-user admin --master-password secret

  # Use with custom PostgreSQL instance
  chainstorage admin setup-postgres --blockchain bitcoin --network mainnet --env development --host mydb.example.com --port 5433 --ssl-mode require`,
		RunE: func(cmd *cobra.Command, args []string) error {
			app := startApp()
			defer app.Close()

			// Parse all flags
			masterUser, err := cmd.Flags().GetString(masterUserFlag)
			if err != nil {
				return err
			}
			masterPassword, err := cmd.Flags().GetString(masterPassFlag)
			if err != nil {
				return err
			}
			host, err := cmd.Flags().GetString(hostFlag)
			if err != nil {
				return err
			}
			port, err := cmd.Flags().GetInt(portFlag)
			if err != nil {
				return err
			}
			workerUser, err := cmd.Flags().GetString(workerUserFlag)
			if err != nil {
				return err
			}
			workerPassword, err := cmd.Flags().GetString(workerPassFlag)
			if err != nil {
				return err
			}
			serverUser, err := cmd.Flags().GetString(serverUserFlag)
			if err != nil {
				return err
			}
			serverPassword, err := cmd.Flags().GetString(serverPassFlag)
			if err != nil {
				return err
			}
			sslMode, err := cmd.Flags().GetString(sslModeFlag)
			if err != nil {
				return err
			}
			dbName, err := cmd.Flags().GetString(dbNameFlag)
			if err != nil {
				return err
			}
			connectTimeout, err := cmd.Flags().GetDuration(connectTimeoutFlag)
			if err != nil {
				return err
			}

			// Validation
			if masterUser == "" {
				return xerrors.New("master-user is required")
			}
			if masterPassword == "" {
				return xerrors.New("master-password is required")
			}
			if host == "" {
				return xerrors.New("host is required")
			}
			if port <= 0 || port > 65535 {
				return xerrors.New("port must be between 1 and 65535")
			}
			if workerUser == "" {
				return xerrors.New("worker-user is required")
			}
			if workerPassword == "" {
				return xerrors.New("worker-password is required")
			}
			if serverUser == "" {
				return xerrors.New("server-user is required")
			}
			if serverPassword == "" {
				return xerrors.New("server-password is required")
			}
			if workerUser == serverUser {
				return xerrors.New("worker-user and server-user must be different")
			}

			// Determine database name using global blockchain and network flags
			if dbName == "" {
				// Use global flags from common.go (commonFlags.blockchain and commonFlags.network)
				// e.g., blockchain="ethereum", network="mainnet" -> "chainstorage_ethereum_mainnet"
				dbName = fmt.Sprintf("chainstorage_%s_%s", commonFlags.blockchain, commonFlags.network)
				// Replace hyphens with underscores for valid database name
				dbName = replaceHyphensWithUnderscores(dbName)
			}

			// Build master config
			masterCfg := &config.PostgresConfig{
				Host:           host,
				Port:           port,
				Database:       "postgres", // Always connect to postgres database first
				User:           masterUser,
				Password:       masterPassword,
				SSLMode:        sslMode,
				ConnectTimeout: connectTimeout,
			}

			fmt.Printf("🚀 Setting up PostgreSQL for chainstorage...\n")
			fmt.Printf("   Database: %s\n", dbName)
			fmt.Printf("   Worker role: %s\n", workerUser)
			fmt.Printf("   Server role: %s\n", serverUser)
			fmt.Printf("   Host: %s:%d\n", host, port)
			fmt.Printf("   Blockchain: %s\n", commonFlags.blockchain)
			fmt.Printf("   Network: %s\n", commonFlags.network)
			fmt.Printf("   Environment: %s\n", commonFlags.env)
			fmt.Printf("\n")

			return postgres.SetupDatabase(context.Background(), masterCfg, workerUser, workerPassword, serverUser, serverPassword, dbName)
		},
	}

	// Define flags with reasonable defaults
	cmd.Flags().String(masterUserFlag, "postgres", "Master/admin user for PostgreSQL")
	cmd.Flags().String(masterPassFlag, "", "Master/admin password for PostgreSQL")
	cmd.Flags().String(hostFlag, "localhost", "PostgreSQL host")
	cmd.Flags().Int(portFlag, 5432, "PostgreSQL port")
	cmd.Flags().String(workerUserFlag, "chainstorage_worker", "Name for the read/write worker role")
	cmd.Flags().String(workerPassFlag, "", "Password for the worker role")
	cmd.Flags().String(serverUserFlag, "chainstorage_server", "Name for the read-only server role")
	cmd.Flags().String(serverPassFlag, "", "Password for the server role")
	cmd.Flags().String(dbNameFlag, "", "Directly specify the database name to create (overrides default naming)")
	cmd.Flags().String(sslModeFlag, "disable", "PostgreSQL SSL mode (disable, require, verify-ca, verify-full)")
	cmd.Flags().Duration(connectTimeoutFlag, 30*time.Second, "PostgreSQL connection timeout")

	// Mark required flags
	if err := cmd.MarkFlagRequired(masterPassFlag); err != nil {
		return nil
	}
	if err := cmd.MarkFlagRequired(workerPassFlag); err != nil {
		return nil
	}
	if err := cmd.MarkFlagRequired(serverPassFlag); err != nil {
		return nil
	}

	return cmd
}

// replaceHyphensWithUnderscores converts network names like "ethereum-mainnet" to "ethereum_mainnet"
// for valid PostgreSQL database naming
func replaceHyphensWithUnderscores(s string) string {
	return strings.ReplaceAll(s, "-", "_")
}

func init() {
	rootCmd.AddCommand(newPostgresCommand())
}
