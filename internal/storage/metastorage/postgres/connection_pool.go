package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"sync"

	"go.uber.org/zap"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/utils/log"
)

// ConnectionPool manages a shared database connection pool
type ConnectionPool struct {
	db     *sql.DB
	config *config.PostgresConfig
	mu     sync.RWMutex
	closed bool
	logger *zap.Logger
}

// connectionPoolManager manages singleton connection pools by connection string
var (
	poolManager = &ConnectionPoolManager{
		pools: make(map[string]*ConnectionPool),
	}
)

// ConnectionPoolManager manages multiple connection pools
type ConnectionPoolManager struct {
	mu    sync.RWMutex
	pools map[string]*ConnectionPool
}

// GetConnectionPool returns a shared connection pool for the given config
func GetConnectionPool(ctx context.Context, cfg *config.PostgresConfig) (*ConnectionPool, error) {
	return poolManager.GetOrCreate(ctx, cfg)
}

// GetOrCreate returns an existing connection pool or creates a new one
func (cpm *ConnectionPoolManager) GetOrCreate(ctx context.Context, cfg *config.PostgresConfig) (*ConnectionPool, error) {
	// Create a unique key for this configuration
	key := fmt.Sprintf("%s:%d/%s?user=%s", cfg.Host, cfg.Port, cfg.Database, cfg.User)

	cpm.mu.RLock()
	if pool, exists := cpm.pools[key]; exists && !pool.closed {
		cpm.mu.RUnlock()
		return pool, nil
	}
	cpm.mu.RUnlock()

	// Need to create new connection pool
	cpm.mu.Lock()
	defer cpm.mu.Unlock()

	// Double-check pattern
	if pool, exists := cpm.pools[key]; exists && !pool.closed {
		return pool, nil
	}

	// Create new connection pool
	pool, err := NewConnectionPool(ctx, cfg)
	if err != nil {
		return nil, err
	}

	cpm.pools[key] = pool
	return pool, nil
}

// CloseAll closes all connection pools
func (cpm *ConnectionPoolManager) CloseAll() error {
	cpm.mu.Lock()
	defer cpm.mu.Unlock()

	var errors []error
	for key, pool := range cpm.pools {
		if err := pool.Close(); err != nil {
			errors = append(errors, xerrors.Errorf("failed to close pool %s: %w", key, err))
		}
	}

	// Clear the pools map
	cpm.pools = make(map[string]*ConnectionPool)

	if len(errors) > 0 {
		return xerrors.Errorf("errors closing connection pools: %v", errors)
	}
	return nil
}

// NewConnectionPool creates a new connection pool
func NewConnectionPool(ctx context.Context, cfg *config.PostgresConfig) (*ConnectionPool, error) {
	logger := log.WithPackage(log.NewDevelopment())

	db, err := newDBConnection(ctx, cfg)
	if err != nil {
		return nil, xerrors.Errorf("failed to create database connection: %w", err)
	}

	pool := &ConnectionPool{
		db:     db,
		config: cfg,
		logger: logger,
	}

	logger.Debug("Created new connection pool",
		zap.String("host", cfg.Host),
		zap.Int("port", cfg.Port),
		zap.String("database", cfg.Database),
		zap.Int("max_connections", cfg.MaxConnections),
	)

	return pool, nil
}

// DB returns the underlying database connection
func (cp *ConnectionPool) DB() *sql.DB {
	cp.mu.RLock()
	defer cp.mu.RUnlock()

	if cp.closed {
		return nil
	}
	return cp.db
}

// Close closes the connection pool
func (cp *ConnectionPool) Close() error {
	cp.mu.Lock()
	defer cp.mu.Unlock()

	if cp.closed {
		return nil
	}

	cp.closed = true

	if cp.db != nil {
		if err := cp.db.Close(); err != nil {
			cp.logger.Error("Failed to close database connection", zap.Error(err))
			return err
		}
		cp.logger.Debug("Connection pool closed successfully")
	}

	return nil
}

// Stats returns connection pool statistics
func (cp *ConnectionPool) Stats() sql.DBStats {
	cp.mu.RLock()
	defer cp.mu.RUnlock()

	if cp.closed || cp.db == nil {
		return sql.DBStats{}
	}

	return cp.db.Stats()
}

// Health checks if the connection pool is healthy
func (cp *ConnectionPool) Health(ctx context.Context) error {
	cp.mu.RLock()
	defer cp.mu.RUnlock()

	if cp.closed {
		return xerrors.New("connection pool is closed")
	}

	if cp.db == nil {
		return xerrors.New("database connection is nil")
	}

	return cp.db.PingContext(ctx)
}

// CloseAllConnectionPools closes all managed connection pools
// This should be called during application shutdown
func CloseAllConnectionPools() error {
	return poolManager.CloseAll()
}
