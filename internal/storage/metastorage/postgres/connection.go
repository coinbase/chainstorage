package postgres

import (
	"context"
	"database/sql"
	"fmt"

	_ "github.com/lib/pq"
	"go.uber.org/zap"

	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/utils/log"
)

func newDBConnection(ctx context.Context, cfg *config.PostgresConfig) (*sql.DB, error) {
	logger := log.WithPackage(log.NewDevelopment())

	// Build PostgreSQL connection string with timeout
	dsn := fmt.Sprintf("host=%s port=%d dbname=%s user=%s password=%s sslmode=%s",
		cfg.Host, cfg.Port, cfg.Database, cfg.User, cfg.Password, cfg.SSLMode)

	// Add connect_timeout if specified
	if cfg.ConnectTimeout > 0 {
		dsn += fmt.Sprintf(" connect_timeout=%d", int(cfg.ConnectTimeout.Seconds()))
	}
	if cfg.StatementTimeout > 0 {
		dsn += fmt.Sprintf(" statement_timeout=%d", cfg.StatementTimeout.Milliseconds())
	}

	// Debug output for CI troubleshooting
	logger.Debug("Connecting to PostgreSQL",
		zap.String("host", cfg.Host),
		zap.Int("port", cfg.Port),
		zap.String("database", cfg.Database),
		zap.String("ssl_mode", cfg.SSLMode))

	// Open database connection
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		logger.Error("Failed to open connection", zap.Error(err))
		return nil, err
	}

	if pingErr := db.PingContext(ctx); pingErr != nil {
		logger.Error("Failed to ping database", zap.Error(pingErr))
		return nil, pingErr
	}

	logger.Debug("Successfully connected to PostgreSQL")

	// Configure connection pool and timeouts
	db.SetMaxOpenConns(cfg.MaxConnections)
	db.SetMaxIdleConns(cfg.MinConnections)
	db.SetConnMaxLifetime(cfg.MaxLifetime)
	db.SetConnMaxIdleTime(cfg.MaxIdleTime)

	// Do not run schema migrations from runtime service connections.
	// Server pods may connect through read replicas, and worker users may not own
	// existing tables. Run migrations explicitly through admin db-init/db-migrate.
	return db, nil
}
