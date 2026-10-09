package main

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"regexp"
	"sort"
	"sync"
	"time"

	"github.com/lib/pq"
	"github.com/pressly/goose/v3"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/config"
	"github.com/coinbase/chainstorage/internal/storage/metastorage/postgres"
	"github.com/coinbase/chainstorage/internal/utils/log"
)

const maxMigrationVersion int64 = 1<<63 - 1

const defaultMigrationStatementTimeout = 2*time.Hour + 20*time.Minute
const defaultMigrationLockWaitTimeout = 5 * time.Minute

const migrationLockQuery = "SELECT pg_advisory_lock(hashtextextended('chainstorage-schema-migration:' || current_database(), 0))"

var errMigrationSessionLost = errors.New("migration database session lost; rerun the entire migration command")

// A migration run owns exactly one physical session. The advisory lock and all
// Goose statements use that session, so losing the lock cannot leave DDL running
// on an independent connection. Never reconnect within a run: Goose may already
// have cached a migration list that another invocation has since applied.
type migrationConnector struct {
	driver.Connector
	waitTimeout time.Duration
	mu          sync.Mutex
	connected   bool
}

func (c *migrationConnector) Connect(ctx context.Context) (driver.Conn, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.connected {
		return nil, errMigrationSessionLost
	}
	conn, err := c.Connector.Connect(ctx)
	if err != nil {
		return nil, migrationConnectionError(err)
	}
	lockCtx, cancel := context.WithTimeout(ctx, c.waitTimeout)
	defer cancel()
	_, err = conn.(driver.ExecerContext).ExecContext(lockCtx, migrationLockQuery, nil)
	if err != nil {
		_ = conn.Close()
		return nil, xerrors.Errorf("failed to wait for advisory lock: %w", migrationConnectionError(err))
	}
	c.connected = true
	return conn, nil
}

func migrationConnectionError(err error) error {
	// Connector.Connect must never return driver.ErrBadConn, even wrapped.
	// Acquisition failures are reported to the operator instead of retried by
	// database/sql behind the command's bounded lock-wait policy.
	if errors.Is(err, driver.ErrBadConn) {
		return xerrors.New("database connection failed before migration lock acquisition")
	}
	return err
}

func withMigrationDatabase(ctx context.Context, dsn string, waitTimeout time.Duration, run func(*sql.DB) error) error {
	connector, err := pq.NewConnector(dsn)
	if err != nil {
		return xerrors.Errorf("failed to configure migration connection: %w", err)
	}
	db := sql.OpenDB(&migrationConnector{Connector: connector, waitTimeout: waitTimeout})
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	defer func() { _ = db.Close() }()
	if err := db.PingContext(ctx); err != nil {
		return xerrors.Errorf("failed to acquire migration database session: %w", err)
	}
	return run(db)
}

var concurrentIndexPattern = regexp.MustCompile(`(?i)CREATE\s+(?:UNIQUE\s+)?INDEX\s+CONCURRENTLY\s+IF\s+NOT\s+EXISTS\s+([a-z_][a-z0-9_$]*)`)

type migrationExecer interface {
	ExecContext(context.Context, string, ...interface{}) (sql.Result, error)
}

func newDBMigrateCommand() *cobra.Command {
	var (
		masterUser       string
		masterPassword   string
		workerUser       string
		serverUser       string
		host             string
		port             int
		sslMode          string
		connectTimeout   time.Duration
		statementTimeout time.Duration
		lockWaitTimeout  time.Duration
		dryRun           bool
		dbName           string
	)

	cmd := &cobra.Command{
		Use:   "db-migrate",
		Short: "Run PostgreSQL schema migrations",
		Long: `Run PostgreSQL schema migrations using admin/master credentials.

This command MUST be run from the ChainStorage admin pod or with master credentials.
It applies any pending schema migrations to the database.

The migrations are tracked via the goose_db_version table, ensuring idempotency.
Running this command multiple times is safe - it will only apply new migrations.

Required flags:
- --master-user: PostgreSQL master/admin username
- --master-password: PostgreSQL master/admin password
- --worker-user: runtime worker username (required unless --dry-run)
- --server-user: runtime read-only server username (required unless --dry-run)

Optional flags:
- --host: PostgreSQL host (default: from environment)
- --port: PostgreSQL port (default: 5432)
- --db-name: Database name (default: chainstorage_{blockchain}_{network})
- --ssl-mode: SSL mode (default: require)
- --dry-run: Show pending migrations without applying them

Example usage:
  # Run migrations for ethereum-mainnet (from admin pod)
  ./admin db-migrate --blockchain ethereum --network mainnet --env dev --master-user postgres --master-password <password> --worker-user cs_ethereum_mainnet_worker --server-user cs_ethereum_mainnet_server

  # Dry run to see pending migrations
  ./admin db-migrate --blockchain ethereum --network mainnet --env dev --master-user postgres --master-password <password> --dry-run

  # Use custom database name
  ./admin db-migrate --blockchain ethereum --network mainnet --env dev --master-user postgres --master-password <password> --worker-user app_worker --server-user app_server --db-name my_custom_db`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runDBMigrate(masterUser, masterPassword, workerUser, serverUser, host, port, dbName, sslMode, connectTimeout, statementTimeout, lockWaitTimeout, dryRun)
		},
	}

	cmd.Flags().StringVar(&masterUser, "master-user", "", "PostgreSQL master/admin username (required)")
	cmd.Flags().StringVar(&masterPassword, "master-password", "", "PostgreSQL master/admin password (required)")
	cmd.Flags().StringVar(&workerUser, "worker-user", "", "Runtime worker username (required unless --dry-run)")
	cmd.Flags().StringVar(&serverUser, "server-user", "", "Runtime read-only server username (required unless --dry-run)")
	cmd.Flags().StringVar(&host, "host", "", "PostgreSQL host (uses environment if not specified)")
	cmd.Flags().IntVar(&port, "port", 5432, "PostgreSQL port")
	cmd.Flags().StringVar(&dbName, "db-name", "", "Database name (default: chainstorage_{blockchain}_{network})")
	cmd.Flags().StringVar(&sslMode, "ssl-mode", "require", "SSL mode (disable, require, verify-ca, verify-full)")
	cmd.Flags().DurationVar(&connectTimeout, "connect-timeout", 30*time.Second, "Connection timeout")
	cmd.Flags().DurationVar(&statementTimeout, "statement-timeout", defaultMigrationStatementTimeout, "Maximum duration of one migration or grant statement")
	cmd.Flags().DurationVar(&lockWaitTimeout, "lock-wait-timeout", defaultMigrationLockWaitTimeout, "Maximum time to wait for another migration invocation")
	cmd.Flags().BoolVar(&dryRun, "dry-run", false, "Show pending migrations without applying them")

	_ = cmd.MarkFlagRequired("master-user")
	_ = cmd.MarkFlagRequired("master-password")

	return cmd
}

func runDBMigrate(masterUser, masterPassword, workerUser, serverUser, host string, port int, dbName, sslMode string, connectTimeout, statementTimeout, lockWaitTimeout time.Duration, dryRun bool) error {
	ctx := context.Background()
	logger := log.WithPackage(logger)

	if !dryRun && (workerUser == "" || serverUser == "") {
		return xerrors.New("--worker-user and --server-user are required when applying migrations")
	}

	// Determine host from environment if not provided
	if host == "" {
		host = getEnvOrDefault("CHAINSTORAGE_AWS_POSTGRES_HOST", "localhost")
	}

	// Determine database name if not provided
	if dbName == "" {
		dbName = fmt.Sprintf("chainstorage_%s_%s", commonFlags.blockchain, commonFlags.network)
		dbName = replaceHyphensWithUnderscores(dbName)
	}

	logger.Info("Starting database migration",
		zap.String("blockchain", commonFlags.blockchain),
		zap.String("network", commonFlags.network),
		zap.String("env", commonFlags.env),
		zap.String("host", host),
		zap.Int("port", port),
		zap.String("database", dbName),
		zap.Bool("dry_run", dryRun),
	)

	// Build connection config
	cfg := &config.PostgresConfig{
		Host:             host,
		Port:             port,
		Database:         dbName,
		User:             masterUser,
		Password:         masterPassword,
		SSLMode:          sslMode,
		ConnectTimeout:   connectTimeout,
		StatementTimeout: statementTimeout,
	}

	// Connect only through a lock-owning migration session.
	dsn := fmt.Sprintf("host=%s port=%d dbname=%s user=%s password=%s sslmode=%s connect_timeout=%d statement_timeout=%d",
		cfg.Host, cfg.Port, cfg.Database, cfg.User, cfg.Password, cfg.SSLMode, int(cfg.ConnectTimeout.Seconds()), cfg.StatementTimeout.Milliseconds())

	if dryRun {
		if err := configureEmbeddedMigrations(); err != nil {
			return xerrors.Errorf("failed to set goose dialect: %w", err)
		}
		return withMigrationDatabase(ctx, dsn, lockWaitTimeout, func(db *sql.DB) error {
			// Show pending migrations
			logger.Info("Checking for pending migrations (dry run)")

			currentVersion, err := goose.GetDBVersion(db)
			if err != nil {
				return xerrors.Errorf("failed to get current database version: %w", err)
			}

			logger.Info("Current database version", zap.Int64("version", currentVersion))

			migrations, err := collectDBMigrations()
			if err != nil {
				return xerrors.Errorf("failed to collect migrations: %w", err)
			}

			fmt.Printf("\n📋 Migration Status:\n")
			fmt.Printf("Current version: %d\n\n", currentVersion)

			hasPending := false
			for _, migration := range migrations {
				if migration.Version > currentVersion {
					fmt.Printf("  [PENDING] %d: %s\n", migration.Version, migration.Source)
					hasPending = true
				} else {
					fmt.Printf("  [APPLIED] %d: %s\n", migration.Version, migration.Source)
				}
			}

			if !hasPending {
				fmt.Printf("\n✅ No pending migrations. Database is up to date!\n")
			} else {
				fmt.Printf("\n⚠️  Pending migrations found. Run without --dry-run to apply them.\n")
			}

			return nil
		})
	}

	currentVersion, err := runPrivilegedMigrations(
		ctx,
		dsn,
		masterUser,
		workerUser,
		serverUser,
		dbName,
		lockWaitTimeout,
		logger,
	)
	if err != nil {
		return xerrors.Errorf("failed to run locked database migration: %w", err)
	}

	logger.Info("Migrations completed successfully", zap.Int64("current_version", currentVersion))
	fmt.Printf("\n✅ Database migrations completed successfully!\n")
	fmt.Printf("Current version: %d\n", currentVersion)

	return nil
}

func configureEmbeddedMigrations() error {
	goose.SetBaseFS(postgres.GetEmbeddedMigrations())
	return goose.SetDialect("postgres")
}

func runPrivilegedMigrations(
	ctx context.Context,
	dsn string,
	masterUser string,
	workerUser string,
	serverUser string,
	dbName string,
	lockWaitTimeout time.Duration,
	logger *zap.Logger,
) (int64, error) {
	if err := configureEmbeddedMigrations(); err != nil {
		return 0, xerrors.Errorf("failed to set goose dialect: %w", err)
	}

	var currentVersion int64
	if err := withMigrationDatabase(ctx, dsn, lockWaitTimeout, func(db *sql.DB) error {
		var err error
		currentVersion, err = runMigrationSteps(
			ctx,
			db,
			masterUser,
			workerUser,
			serverUser,
			dbName,
			func() (int64, error) {
				return runPendingMigrations(ctx, db, logger)
			},
		)
		return err
	}); err != nil {
		return 0, err
	}

	return currentVersion, nil
}

func runMigrationSteps(
	ctx context.Context,
	db migrationExecer,
	masterUser string,
	workerUser string,
	serverUser string,
	dbName string,
	migrate func() (int64, error),
) (int64, error) {
	if err := ensureMigrationRoleMembership(ctx, db, masterUser, workerUser); err != nil {
		return 0, xerrors.Errorf("failed to authorize migrations over worker-owned objects: %w", err)
	}

	currentVersion, err := migrate()
	if err != nil {
		return 0, err
	}

	if err := grantMigrationPrivileges(ctx, db, masterUser, workerUser, serverUser, dbName); err != nil {
		return 0, xerrors.Errorf("failed to reconcile runtime role privileges: %w", err)
	}

	return currentVersion, nil
}

func runPendingMigrations(ctx context.Context, db *sql.DB, logger *zap.Logger) (int64, error) {
	currentVersion, err := goose.GetDBVersion(db)
	if err != nil {
		return 0, xerrors.Errorf("failed to get current database version before migration: %w", err)
	}
	pendingIndexNames, err := pendingConcurrentIndexNames(currentVersion)
	if err != nil {
		return 0, xerrors.Errorf("failed to inspect pending concurrent indexes: %w", err)
	}

	// CREATE INDEX CONCURRENTLY can leave an invalid index when its client is
	// interrupted. Remove only indexes owned by still-pending migrations;
	// otherwise CREATE INDEX ... IF NOT EXISTS skips the invalid relation and
	// can incorrectly mark the migration as applied.
	if err := dropInvalidIndexes(ctx, db, pendingIndexNames, logger); err != nil {
		return 0, xerrors.Errorf("failed to clean up invalid indexes: %w", err)
	}

	logger.Info("Applying database migrations")
	if err := goose.UpContext(ctx, db, "db/migrations"); err != nil {
		return 0, xerrors.Errorf("failed to run migrations: %w", err)
	}

	currentVersion, err = goose.GetDBVersion(db)
	if err != nil {
		return 0, xerrors.Errorf("failed to get current database version: %w", err)
	}

	return currentVersion, nil
}

func pendingConcurrentIndexNames(currentVersion int64) ([]string, error) {
	migrations, err := goose.CollectMigrations("db/migrations", currentVersion, maxMigrationVersion)
	if errors.Is(err, goose.ErrNoMigrationFiles) {
		return nil, nil
	}
	if err != nil {
		return nil, xerrors.Errorf("failed to collect pending migrations: %w", err)
	}

	migrationFS := postgres.GetEmbeddedMigrations()
	indexNames := make(map[string]struct{})
	for _, migration := range migrations {
		contents, err := fs.ReadFile(migrationFS, migration.Source)
		if err != nil {
			return nil, xerrors.Errorf("failed to read migration %s: %w", migration.Source, err)
		}
		for _, match := range concurrentIndexPattern.FindAllSubmatch(contents, -1) {
			indexNames[string(match[1])] = struct{}{}
		}
	}

	result := make([]string, 0, len(indexNames))
	for name := range indexNames {
		result = append(result, name)
	}
	sort.Strings(result)
	return result, nil
}

func dropInvalidIndexes(ctx context.Context, db *sql.DB, pendingIndexNames []string, logger *zap.Logger) error {
	if len(pendingIndexNames) == 0 {
		return nil
	}

	rows, err := db.QueryContext(ctx, `
SELECT n.nspname, c.relname
FROM pg_index AS i
JOIN pg_class AS c ON c.oid = i.indexrelid
JOIN pg_namespace AS n ON n.oid = c.relnamespace
LEFT JOIN pg_constraint AS con ON con.conindid = i.indexrelid
WHERE NOT i.indisvalid
  AND n.nspname = 'public'
  AND con.oid IS NULL
  AND c.relname = ANY($1)
ORDER BY c.relname`, pq.Array(pendingIndexNames))
	if err != nil {
		return xerrors.Errorf("failed to query invalid indexes: %w", err)
	}

	type indexName struct {
		schema string
		name   string
	}
	var indexes []indexName
	for rows.Next() {
		var index indexName
		if err := rows.Scan(&index.schema, &index.name); err != nil {
			_ = rows.Close()
			return xerrors.Errorf("failed to scan invalid index: %w", err)
		}
		indexes = append(indexes, index)
	}
	if err := rows.Close(); err != nil {
		return xerrors.Errorf("failed to close invalid index query: %w", err)
	}
	if err := rows.Err(); err != nil {
		return xerrors.Errorf("failed while reading invalid indexes: %w", err)
	}

	for _, index := range indexes {
		qualifiedName := pq.QuoteIdentifier(index.schema) + "." + pq.QuoteIdentifier(index.name)
		logger.Warn("Dropping invalid index left by an interrupted concurrent build", zap.String("index", qualifiedName))
		if _, err := db.ExecContext(ctx, "DROP INDEX CONCURRENTLY IF EXISTS "+qualifiedName); err != nil {
			return xerrors.Errorf("failed to drop invalid index %s: %w", qualifiedName, err)
		}
	}

	return nil
}

func grantMigrationPrivileges(ctx context.Context, db migrationExecer, masterUser, workerUser, serverUser, dbName string) error {
	for _, query := range migrationPrivilegeQueries(masterUser, workerUser, serverUser, dbName) {
		if _, err := db.ExecContext(ctx, query); err != nil {
			return xerrors.Errorf("failed query %q: %w", query, err)
		}
	}
	return nil
}

func ensureMigrationRoleMembership(ctx context.Context, db migrationExecer, masterUser, workerUser string) error {
	if masterUser == workerUser {
		return nil
	}

	query := fmt.Sprintf(
		"GRANT %s TO %s",
		pq.QuoteIdentifier(workerUser),
		pq.QuoteIdentifier(masterUser),
	)
	if _, err := db.ExecContext(ctx, query); err != nil {
		return xerrors.Errorf("failed query %q: %w", query, err)
	}

	return nil
}

func migrationPrivilegeQueries(masterUser, workerUser, serverUser, dbName string) []string {
	master := pq.QuoteIdentifier(masterUser)
	worker := pq.QuoteIdentifier(workerUser)
	server := pq.QuoteIdentifier(serverUser)
	database := pq.QuoteIdentifier(dbName)

	return []string{
		fmt.Sprintf("GRANT ALL PRIVILEGES ON DATABASE %s TO %s", database, worker),
		fmt.Sprintf("GRANT CONNECT ON DATABASE %s TO %s", database, server),
		fmt.Sprintf("GRANT ALL PRIVILEGES ON SCHEMA public TO %s", worker),
		fmt.Sprintf("GRANT USAGE ON SCHEMA public TO %s", server),
		fmt.Sprintf("GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO %s", worker),
		fmt.Sprintf("GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO %s", worker),
		fmt.Sprintf("GRANT ALL PRIVILEGES ON ALL FUNCTIONS IN SCHEMA public TO %s", worker),
		fmt.Sprintf("GRANT SELECT ON ALL TABLES IN SCHEMA public TO %s", server),
		fmt.Sprintf("GRANT SELECT ON ALL SEQUENCES IN SCHEMA public TO %s", server),
		fmt.Sprintf("ALTER DEFAULT PRIVILEGES FOR USER %s IN SCHEMA public GRANT ALL PRIVILEGES ON TABLES TO %s", master, worker),
		fmt.Sprintf("ALTER DEFAULT PRIVILEGES FOR USER %s IN SCHEMA public GRANT ALL PRIVILEGES ON SEQUENCES TO %s", master, worker),
		fmt.Sprintf("ALTER DEFAULT PRIVILEGES FOR USER %s IN SCHEMA public GRANT ALL PRIVILEGES ON FUNCTIONS TO %s", master, worker),
		fmt.Sprintf("ALTER DEFAULT PRIVILEGES FOR USER %s IN SCHEMA public GRANT SELECT ON TABLES TO %s", master, server),
		fmt.Sprintf("ALTER DEFAULT PRIVILEGES FOR USER %s IN SCHEMA public GRANT SELECT ON SEQUENCES TO %s", master, server),
		fmt.Sprintf("ALTER DEFAULT PRIVILEGES FOR USER %s IN SCHEMA public GRANT SELECT ON TABLES TO %s", worker, server),
		fmt.Sprintf("ALTER DEFAULT PRIVILEGES FOR USER %s IN SCHEMA public GRANT SELECT ON SEQUENCES TO %s", worker, server),
	}
}

func collectDBMigrations() (goose.Migrations, error) {
	return goose.CollectMigrations("db/migrations", 0, maxMigrationVersion)
}

func getEnvOrDefault(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func init() {
	rootCmd.AddCommand(newDBMigrateCommand())
}
