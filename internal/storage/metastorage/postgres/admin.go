package postgres

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/lib/pq"
	"go.uber.org/zap"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/utils/log"
)

// SetupDatabase creates a database and roles for a new network in PostgreSQL
// This function is intended to be called by administrators during setup
func SetupDatabase(ctx context.Context, masterCfg *config.PostgresConfig, workerUser string, workerPassword string, serverUser string, serverPassword string, dbName string) error {
	logger := log.WithPackage(log.NewDevelopment())

	logger.Info("Setting up PostgreSQL database",
		zap.String("database", dbName),
		zap.String("worker_user", workerUser),
		zap.String("server_user", serverUser))

	// Connect to the default 'postgres' database with master credentials
	dsn := fmt.Sprintf(
		"host=%s port=%d dbname=postgres user=%s password=%s sslmode=%s",
		masterCfg.Host, masterCfg.Port, masterCfg.User, masterCfg.Password, masterCfg.SSLMode,
	)
	if masterCfg.ConnectTimeout > 0 {
		dsn += fmt.Sprintf(" connect_timeout=%d", int(masterCfg.ConnectTimeout.Seconds()))
	}

	adminDB, err := sql.Open("postgres", dsn)
	if err != nil {
		return xerrors.Errorf("failed to connect to postgres db with master user: %w", err)
	}
	defer func() {
		if closeErr := adminDB.Close(); closeErr != nil {
			logger.Warn("Failed to close admin database connection", zap.Error(closeErr))
		}
	}()

	if err := adminDB.PingContext(ctx); err != nil {
		return xerrors.Errorf("failed to ping postgres db with master user: %w", err)
	}

	logger.Info("Successfully connected to PostgreSQL as master user")

	// Create worker role with LOGIN capability and password
	logger.Info("Creating worker role", zap.String("username", workerUser))
	workerQuery := fmt.Sprintf("CREATE ROLE %s WITH LOGIN PASSWORD %s",
		pq.QuoteIdentifier(workerUser), pq.QuoteLiteral(workerPassword))
	if _, err := adminDB.ExecContext(ctx, workerQuery); err != nil {
		if pgErr, ok := err.(*pq.Error); ok && pgErr.Code.Name() == "duplicate_object" {
			logger.Info("Worker role already exists, updating password", zap.String("username", workerUser))
			// Update password for existing role
			alterQuery := fmt.Sprintf("ALTER ROLE %s WITH PASSWORD %s",
				pq.QuoteIdentifier(workerUser), pq.QuoteLiteral(workerPassword))
			if _, err := adminDB.ExecContext(ctx, alterQuery); err != nil {
				return xerrors.Errorf("failed to update password for worker role %s: %w", workerUser, err)
			}
		} else {
			return xerrors.Errorf("failed to create worker role %s: %w", workerUser, err)
		}
	} else {
		logger.Info("Successfully created worker role", zap.String("username", workerUser))
	}

	// Create server role with LOGIN capability and password
	logger.Info("Creating server role", zap.String("username", serverUser))
	serverQuery := fmt.Sprintf("CREATE ROLE %s WITH LOGIN PASSWORD %s",
		pq.QuoteIdentifier(serverUser), pq.QuoteLiteral(serverPassword))
	if _, err := adminDB.ExecContext(ctx, serverQuery); err != nil {
		if pgErr, ok := err.(*pq.Error); ok && pgErr.Code.Name() == "duplicate_object" {
			logger.Info("Server role already exists, updating password", zap.String("username", serverUser))
			// Update password for existing role
			alterQuery := fmt.Sprintf("ALTER ROLE %s WITH PASSWORD %s",
				pq.QuoteIdentifier(serverUser), pq.QuoteLiteral(serverPassword))
			if _, err := adminDB.ExecContext(ctx, alterQuery); err != nil {
				return xerrors.Errorf("failed to update password for server role %s: %w", serverUser, err)
			}
		} else {
			return xerrors.Errorf("failed to create server role %s: %w", serverUser, err)
		}
	} else {
		logger.Info("Successfully created server role", zap.String("username", serverUser))
	}

	// Create application database owned by the worker role
	logger.Info("Creating database", zap.String("database", dbName))
	ownerOpt := fmt.Sprintf("OWNER = %s", pq.QuoteIdentifier(workerUser))
	if _, err := adminDB.ExecContext(ctx, fmt.Sprintf(`CREATE DATABASE %s WITH %s`, pq.QuoteIdentifier(dbName), ownerOpt)); err != nil {
		if pgErr, ok := err.(*pq.Error); ok && pgErr.Code.Name() == "duplicate_database" {
			logger.Info("Database already exists, skipping creation", zap.String("database", dbName))
		} else {
			return xerrors.Errorf("failed to create database %s: %w", dbName, err)
		}
	} else {
		logger.Info("Successfully created database", zap.String("database", dbName))
	}

	// Connect to the application database to set up permissions
	logger.Info("Setting up permissions for database", zap.String("database", dbName))
	appDsn := fmt.Sprintf(
		"host=%s port=%d dbname=%s user=%s password=%s sslmode=%s",
		masterCfg.Host, masterCfg.Port, dbName, masterCfg.User, masterCfg.Password, masterCfg.SSLMode,
	)
	if masterCfg.ConnectTimeout > 0 {
		appDsn += fmt.Sprintf(" connect_timeout=%d", int(masterCfg.ConnectTimeout.Seconds()))
	}

	appDB, err := sql.Open("postgres", appDsn)
	if err != nil {
		return xerrors.Errorf("failed to connect to app database %s: %w", dbName, err)
	}
	defer func() {
		if closeErr := appDB.Close(); closeErr != nil {
			logger.Warn("Failed to close app database connection", zap.Error(closeErr))
		}
	}()

	if err := appDB.PingContext(ctx); err != nil {
		return xerrors.Errorf("failed to ping app database %s: %w", dbName, err)
	}

	// Grant connect permissions to server role
	logger.Info("Granting CONNECT permission on database to server role",
		zap.String("database", dbName),
		zap.String("server_user", serverUser))
	if _, err := appDB.ExecContext(ctx, fmt.Sprintf("GRANT CONNECT ON DATABASE %s TO %s", pq.QuoteIdentifier(dbName), pq.QuoteIdentifier(serverUser))); err != nil {
		return xerrors.Errorf("failed to grant connect on db %s to role %s: %w", dbName, serverUser, err)
	}

	// Grant usage on public schema
	logger.Info("Granting USAGE on schema public to server role", zap.String("server_user", serverUser))
	if _, err := appDB.ExecContext(ctx, fmt.Sprintf("GRANT USAGE ON SCHEMA public TO %s", pq.QuoteIdentifier(serverUser))); err != nil {
		return xerrors.Errorf("failed to grant usage on schema public to role %s: %w", serverUser, err)
	}

	// Grant SELECT on all current tables to server role
	logger.Info("Granting SELECT on all existing tables to server role", zap.String("server_user", serverUser))
	if _, err := appDB.ExecContext(ctx, fmt.Sprintf("GRANT SELECT ON ALL TABLES IN SCHEMA public TO %s", pq.QuoteIdentifier(serverUser))); err != nil {
		// This might fail if no tables exist yet, which is fine
		logger.Warn("Failed to grant select on existing tables (this is normal if no tables exist yet)", zap.Error(err))
	}

	// Grant SELECT on all future tables to server role
	logger.Info("Setting up default privileges for future tables for server role", zap.String("server_user", serverUser))
	if _, err := appDB.ExecContext(ctx, fmt.Sprintf("ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO %s", pq.QuoteIdentifier(serverUser))); err != nil {
		return xerrors.Errorf("failed to alter default privileges for role %s: %w", serverUser, err)
	}

	// Also need to ensure the worker role has the necessary permissions on the database
	logger.Info("Ensuring worker role has full access to database",
		zap.String("worker_user", workerUser),
		zap.String("database", dbName))
	if _, err := appDB.ExecContext(ctx, fmt.Sprintf("GRANT ALL PRIVILEGES ON DATABASE %s TO %s", pq.QuoteIdentifier(dbName), pq.QuoteIdentifier(workerUser))); err != nil {
		logger.Warn("Failed to grant all privileges on database to worker role", zap.Error(err))
	}

	if _, err := appDB.ExecContext(ctx, fmt.Sprintf("GRANT ALL PRIVILEGES ON SCHEMA public TO %s", pq.QuoteIdentifier(workerUser))); err != nil {
		logger.Warn("Failed to grant all privileges on schema to worker role", zap.Error(err))
	}

	logger.Info("Successfully set up database with roles",
		zap.String("database", dbName),
		zap.String("worker_user", workerUser),
		zap.String("server_user", serverUser))
	logger.Info("Database ready for use with the provided credentials")

	return nil
}
