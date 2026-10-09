package main

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"
	"go.uber.org/zap"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/utils/log"
)

func newEnvSetCommand() *cobra.Command {
	var (
		awsRegion string
		quiet     bool
	)

	cmd := &cobra.Command{
		Use:   "env-set",
		Short: "Set environment variables for PostgreSQL credentials from AWS Secrets Manager",
		Long: `Set environment variables for PostgreSQL credentials from AWS Secrets Manager.

This command fetches database credentials from AWS Secrets Manager and outputs export commands
that can be evaluated in your shell to set environment variables.

To use this command and set the environment variables in your current shell session:
  eval $(./admin env-set --blockchain ethereum --network mainnet --env dev --quiet)

Example usage:
  # Set environment variables for ethereum mainnet (worker role)
  eval $(./admin env-set --blockchain ethereum --network mainnet --env dev --quiet)

The command will output the following environment variables:
  - CHAINSTORAGE_AWS_POSTGRES_USER
  - CHAINSTORAGE_AWS_POSTGRES_PASSWORD
  - CHAINSTORAGE_AWS_POSTGRES_DATABASE
  - CHAINSTORAGE_AWS_POSTGRES_HOST (from master credentials)
  - CHAINSTORAGE_AWS_POSTGRES_PORT (from master credentials)
  - CHAINSTORAGE_AWS_POSTGRES_SSL_MODE (default: require)`,
		RunE: func(cmd *cobra.Command, args []string) error {
			// Use worker role by default
			role := "worker"
			return runEnvSet(commonFlags.blockchain, commonFlags.network, commonFlags.env, awsRegion, role, quiet)
		},
	}

	cmd.Flags().StringVar(&awsRegion, "aws-region", "us-east-1", "AWS region for Secrets Manager")
	cmd.Flags().BoolVar(&quiet, "quiet", false, "Suppress log output (use with eval)")

	return cmd
}

type EnvVars struct {
	User     string `json:"CHAINSTORAGE_AWS_POSTGRES_USER"`
	Password string `json:"CHAINSTORAGE_AWS_POSTGRES_PASSWORD"`
	Database string `json:"CHAINSTORAGE_AWS_POSTGRES_DATABASE"`
	Host     string `json:"CHAINSTORAGE_AWS_POSTGRES_HOST"`
	Port     string `json:"CHAINSTORAGE_AWS_POSTGRES_PORT"`
	SSLMode  string `json:"CHAINSTORAGE_AWS_POSTGRES_SSL_MODE"`
}

func runEnvSet(blockchain, network, env, awsRegion, role string, quiet bool) error {
	ctx := context.Background()
	logger := log.WithPackage(logger)

	if !quiet {
		logger.Info("Setting environment variables",
			zap.String("blockchain", blockchain),
			zap.String("network", network),
			zap.String("environment", env),
			zap.String("aws_region", awsRegion),
			zap.String("role", role))
	}

	// Get master credentials from environment variables
	masterHost := os.Getenv("CHAINSTORAGE_CLUSTER_ENDPOINT")
	masterPortStr := os.Getenv("CHAINSTORAGE_CLUSTER_PORT")

	if masterHost == "" || masterPortStr == "" {
		return xerrors.New("missing required environment variables: CHAINSTORAGE_CLUSTER_ENDPOINT, CHAINSTORAGE_CLUSTER_PORT")
	}

	// Construct secret name
	secretName := fmt.Sprintf("chainstorage/db-creds/%s", env)

	// Fetch secret from AWS Secrets Manager
	secretData, err := fetchSecret(ctx, secretName, awsRegion)
	if err != nil {
		return xerrors.Errorf("failed to fetch secret: %w", err)
	}

	// Construct lookup keys for this network
	// Replace hyphens with underscores for the key lookup
	networkKey := strings.ReplaceAll(fmt.Sprintf("%s_%s", blockchain, network), "-", "_")

	// Extract network-specific values from flat secret
	dbName, err := getStringFromSecret(secretData, fmt.Sprintf("%s_database_name", networkKey))
	if err != nil {
		return xerrors.Errorf("failed to get database name: %w", err)
	}

	var username, password string
	if role == "worker" {
		username, err = getStringFromSecret(secretData, fmt.Sprintf("%s_worker_username", networkKey))
		if err != nil {
			return xerrors.Errorf("failed to get worker username: %w", err)
		}
		password, err = getStringFromSecret(secretData, fmt.Sprintf("%s_worker_password", networkKey))
		if err != nil {
			return xerrors.Errorf("failed to get worker password: %w", err)
		}
	} else {
		username, err = getStringFromSecret(secretData, fmt.Sprintf("%s_server_username", networkKey))
		if err != nil {
			return xerrors.Errorf("failed to get server username: %w", err)
		}
		password, err = getStringFromSecret(secretData, fmt.Sprintf("%s_server_password", networkKey))
		if err != nil {
			return xerrors.Errorf("failed to get server password: %w", err)
		}
	}

	if !quiet {
		logger.Info("Successfully fetched credentials from AWS Secrets Manager",
			zap.String("database", dbName),
			zap.String("user", username))
	}

	// Create environment variables
	envVars := EnvVars{
		User:     username,
		Password: password,
		Database: dbName,
		Host:     masterHost,
		Port:     masterPortStr,
		SSLMode:  "require", // Default SSL mode
	}

	// Set environment variables in the current process
	if err := os.Setenv("CHAINSTORAGE_AWS_POSTGRES_USER", envVars.User); err != nil {
		return xerrors.Errorf("failed to set CHAINSTORAGE_AWS_POSTGRES_USER: %w", err)
	}
	if err := os.Setenv("CHAINSTORAGE_AWS_POSTGRES_PASSWORD", envVars.Password); err != nil {
		return xerrors.Errorf("failed to set CHAINSTORAGE_AWS_POSTGRES_PASSWORD: %w", err)
	}
	if err := os.Setenv("CHAINSTORAGE_AWS_POSTGRES_DATABASE", envVars.Database); err != nil {
		return xerrors.Errorf("failed to set CHAINSTORAGE_AWS_POSTGRES_DATABASE: %w", err)
	}
	if err := os.Setenv("CHAINSTORAGE_AWS_POSTGRES_HOST", envVars.Host); err != nil {
		return xerrors.Errorf("failed to set CHAINSTORAGE_AWS_POSTGRES_HOST: %w", err)
	}
	if err := os.Setenv("CHAINSTORAGE_AWS_POSTGRES_PORT", envVars.Port); err != nil {
		return xerrors.Errorf("failed to set CHAINSTORAGE_AWS_POSTGRES_PORT: %w", err)
	}
	if err := os.Setenv("CHAINSTORAGE_AWS_POSTGRES_SSL_MODE", envVars.SSLMode); err != nil {
		return xerrors.Errorf("failed to set CHAINSTORAGE_AWS_POSTGRES_SSL_MODE: %w", err)
	}

	// Output environment variables for reference
	// Shell export format (for sourcing in bash)
	fmt.Printf("export CHAINSTORAGE_AWS_POSTGRES_USER=\"%s\"\n", envVars.User)
	fmt.Printf("export CHAINSTORAGE_AWS_POSTGRES_PASSWORD=\"%s\"\n", envVars.Password)
	fmt.Printf("export CHAINSTORAGE_AWS_POSTGRES_DATABASE=\"%s\"\n", envVars.Database)
	fmt.Printf("export CHAINSTORAGE_AWS_POSTGRES_HOST=\"%s\"\n", envVars.Host)
	fmt.Printf("export CHAINSTORAGE_AWS_POSTGRES_PORT=\"%s\"\n", envVars.Port)
	fmt.Printf("export CHAINSTORAGE_AWS_POSTGRES_SSL_MODE=\"%s\"\n", envVars.SSLMode)

	if !quiet {
		logger.Info("Environment variables set successfully",
			zap.String("blockchain", blockchain),
			zap.String("network", network),
			zap.String("role", role),
			zap.String("database", dbName),
			zap.String("user", username))
	}

	return nil
}

func init() {
	rootCmd.AddCommand(newEnvSetCommand())
}
