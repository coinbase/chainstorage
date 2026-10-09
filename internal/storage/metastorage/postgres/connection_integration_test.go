package postgres

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/coinbase/chainstorage/internal/config"
)

func TestIntegrationNewDBConnectionAppliesStatementTimeoutToEveryConnection(t *testing.T) {
	cfg, err := config.New()
	require.NoError(t, err)
	if !cfg.IsIntegrationTest() || cfg.AWS.Postgres == nil {
		t.Skip("Postgres is not configured for integration tests")
	}

	postgresCfg := *cfg.AWS.Postgres
	postgresCfg.MaxConnections = 2
	postgresCfg.MinConnections = 0
	postgresCfg.StatementTimeout = 1234 * time.Millisecond

	db, err := newDBConnection(context.Background(), &postgresCfg)
	require.NoError(t, err)
	defer db.Close()

	first, err := db.Conn(context.Background())
	require.NoError(t, err)
	defer first.Close()
	second, err := db.Conn(context.Background())
	require.NoError(t, err)
	defer second.Close()

	for _, conn := range []*sql.Conn{first, second} {
		var timeout string
		require.NoError(t, conn.QueryRowContext(context.Background(), "SHOW statement_timeout").Scan(&timeout))
		require.Equal(t, "1234ms", timeout)
	}
}
