package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/lib/pq"
	"github.com/pressly/goose/v3"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// headMigrationVersion is the newest embedded migration and deployedCanaryVersion
// is the version staged by the cleanup scenario below. Adding a migration bumps
// headMigrationVersion: the assertions here prove the privileged path migrated
// all the way to head.
const (
	headMigrationVersion  = 20261009000001
	deployedCanaryVersion = 20260908000001
)

func TestIntegrationMigrationRoleMembershipAllowsWorkerOwnedDDL(t *testing.T) {
	if os.Getenv("TEST_TYPE") != "integration" {
		t.Skip("integration test")
	}

	host := getEnvOrDefault("CHAINSTORAGE_AWS_POSTGRES_HOST", "localhost")
	if host != "localhost" && host != "127.0.0.1" && host != "::1" && host != "postgres" {
		t.Fatalf("refusing to run migration ownership test against PostgreSQL host %q", host)
	}

	port := 5433
	if value := os.Getenv("CHAINSTORAGE_AWS_POSTGRES_PORT"); value != "" {
		parsed, err := strconv.Atoi(value)
		require.NoError(t, err)
		port = parsed
	}

	masterUser := getEnvOrDefault("CHAINSTORAGE_AWS_POSTGRES_USER", "postgres")
	masterPassword := getEnvOrDefault("CHAINSTORAGE_AWS_POSTGRES_PASSWORD", "postgres")

	unique := time.Now().UnixNano()
	workerUser := fmt.Sprintf("migration_worker_test_%d", unique)
	workerPassword := fmt.Sprintf("worker-password-%d", unique)
	serverUser := fmt.Sprintf("migration_server_test_%d", unique)
	serverPassword := fmt.Sprintf("server-password-%d", unique)
	migrationUser := fmt.Sprintf("migration_admin_test_%d", unique)
	migrationPassword := fmt.Sprintf("migration-password-%d", unique)
	dbName := fmt.Sprintf("migration_database_test_%d", unique)
	workerTable := "worker_owned_before_migration"
	adminTable := "admin_owned_during_migration"
	workerFutureTable := "worker_owned_after_migration"
	adminFutureTable := "admin_owned_after_migration"
	adminSequence := "admin_owned_sequence"
	triggerFunction := "admin_owned_trigger_function"
	triggerName := "admin_owned_trigger"

	masterDB := openIntegrationPostgres(t, host, port, "postgres", masterUser, masterPassword)
	var workerDB *sql.DB
	var serverDB *sql.DB
	var migrationDB *sql.DB
	t.Cleanup(func() {
		if serverDB != nil {
			_ = serverDB.Close()
		}
		if workerDB != nil {
			_ = workerDB.Close()
		}
		if migrationDB != nil {
			_ = migrationDB.Close()
		}
		for _, name := range []string{dbName, dbName + "_fresh", dbName + "_cleanup"} {
			_, _ = masterDB.ExecContext(context.Background(), "DROP DATABASE IF EXISTS "+pq.QuoteIdentifier(name))
		}
		_, _ = masterDB.ExecContext(
			context.Background(),
			fmt.Sprintf("REVOKE %s FROM %s", pq.QuoteIdentifier(workerUser), pq.QuoteIdentifier(migrationUser)),
		)
		_, _ = masterDB.ExecContext(
			context.Background(),
			"DROP ROLE IF EXISTS "+pq.QuoteIdentifier(migrationUser),
		)
		_, _ = masterDB.ExecContext(
			context.Background(),
			"DROP ROLE IF EXISTS "+pq.QuoteIdentifier(serverUser),
		)
		_, _ = masterDB.ExecContext(
			context.Background(),
			"DROP ROLE IF EXISTS "+pq.QuoteIdentifier(workerUser),
		)
		_ = masterDB.Close()
	})

	createWorkerRole := fmt.Sprintf(
		"CREATE ROLE %s WITH LOGIN INHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE PASSWORD %s",
		pq.QuoteIdentifier(workerUser),
		pq.QuoteLiteral(workerPassword),
	)
	_, err := masterDB.ExecContext(context.Background(), createWorkerRole)
	require.NoError(t, err)

	createServerRole := fmt.Sprintf(
		"CREATE ROLE %s WITH LOGIN INHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE PASSWORD %s",
		pq.QuoteIdentifier(serverUser),
		pq.QuoteLiteral(serverPassword),
	)
	_, err = masterDB.ExecContext(context.Background(), createServerRole)
	require.NoError(t, err)

	createRole := fmt.Sprintf(
		"CREATE ROLE %s WITH LOGIN INHERIT NOSUPERUSER CREATEDB CREATEROLE PASSWORD %s",
		pq.QuoteIdentifier(migrationUser),
		pq.QuoteLiteral(migrationPassword),
	)
	_, err = masterDB.ExecContext(context.Background(), createRole)
	require.NoError(t, err)

	migrationClusterDB := openIntegrationPostgres(t, host, port, "postgres", migrationUser, migrationPassword)
	require.NoError(t, initializePrivilegedDatabase(context.Background(), migrationClusterDB, dbName, migrationUser, workerUser, zap.NewNop()),
		"first-run db-init must create a worker-owned database using a non-superuser admin")
	require.NoError(t, migrationClusterDB.Close())
	// Revoke bootstrap membership to prove the independent db-migrate path
	// restores the owner authority required by existing worker-owned objects.
	_, err = masterDB.ExecContext(context.Background(), fmt.Sprintf("REVOKE %s FROM %s", pq.QuoteIdentifier(workerUser), pq.QuoteIdentifier(migrationUser)))
	require.NoError(t, err)

	workerDB = openIntegrationPostgres(t, host, port, dbName, workerUser, workerPassword)
	migrationDSN := integrationPostgresDSN(host, port, dbName, migrationUser, migrationPassword)
	migrationDB = openIntegrationPostgres(t, host, port, dbName, migrationUser, migrationPassword)
	serverDB = openIntegrationPostgres(t, host, port, dbName, serverUser, serverPassword)

	_, err = workerDB.ExecContext(
		context.Background(),
		fmt.Sprintf(
			"CREATE TABLE public.%s (id BIGINT, touched BOOLEAN NOT NULL DEFAULT FALSE)",
			pq.QuoteIdentifier(workerTable),
		),
	)
	require.NoError(t, err)

	alterWorkerTable := fmt.Sprintf(
		"ALTER TABLE public.%s ADD COLUMN migrated BOOLEAN NOT NULL DEFAULT TRUE",
		pq.QuoteIdentifier(workerTable),
	)
	_, err = migrationDB.ExecContext(context.Background(), alterWorkerTable)
	require.Error(t, err, "a non-owner migration role must not alter a worker-owned table")

	require.NoError(t, ensureMigrationRoleMembership(
		context.Background(),
		migrationDB,
		migrationUser,
		workerUser,
	))
	_, err = migrationDB.ExecContext(context.Background(), alterWorkerTable)
	require.NoError(t, err, "worker-role membership must authorize owner-only migration DDL")

	_, err = migrationDB.ExecContext(
		context.Background(),
		fmt.Sprintf("CREATE TABLE public.%s (id BIGINT)", pq.QuoteIdentifier(adminTable)),
	)
	require.NoError(t, err, "the migration role must create tables in the worker-owned database")
	_, err = migrationDB.ExecContext(
		context.Background(),
		fmt.Sprintf("CREATE SEQUENCE public.%s", pq.QuoteIdentifier(adminSequence)),
	)
	require.NoError(t, err, "the migration role must create sequences")
	_, err = migrationDB.ExecContext(
		context.Background(),
		fmt.Sprintf(`
CREATE FUNCTION public.%s()
RETURNS TRIGGER AS $$
BEGIN
    NEW.touched := TRUE;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql`, pq.QuoteIdentifier(triggerFunction)),
	)
	require.NoError(t, err, "the migration role must create trigger functions")
	_, err = migrationDB.ExecContext(
		context.Background(),
		fmt.Sprintf(
			"CREATE TRIGGER %s BEFORE INSERT ON public.%s FOR EACH ROW EXECUTE FUNCTION public.%s()",
			pq.QuoteIdentifier(triggerName),
			pq.QuoteIdentifier(workerTable),
			pq.QuoteIdentifier(triggerFunction),
		),
	)
	require.NoError(t, err, "the migration role must create triggers on worker-owned tables")

	require.NoError(t, grantMigrationPrivileges(
		context.Background(),
		migrationDB,
		migrationUser,
		workerUser,
		serverUser,
		dbName,
	))

	_, err = workerDB.ExecContext(
		context.Background(),
		fmt.Sprintf("INSERT INTO public.%s (id) VALUES (1)", pq.QuoteIdentifier(adminTable)),
	)
	require.NoError(t, err, "worker must receive write access to migration-created tables")
	_, err = workerDB.ExecContext(
		context.Background(),
		fmt.Sprintf("SELECT nextval('public.%s')", pq.QuoteIdentifier(adminSequence)),
	)
	require.NoError(t, err, "worker must receive usage access to migration-created sequences")
	_, err = workerDB.ExecContext(
		context.Background(),
		fmt.Sprintf("INSERT INTO public.%s (id) VALUES (1)", pq.QuoteIdentifier(workerTable)),
	)
	require.NoError(t, err)
	var touched bool
	require.NoError(t, workerDB.QueryRowContext(
		context.Background(),
		fmt.Sprintf("SELECT touched FROM public.%s WHERE id = 1", pq.QuoteIdentifier(workerTable)),
	).Scan(&touched))
	require.True(t, touched, "the migration-created trigger must run for the worker role")

	_, err = workerDB.ExecContext(
		context.Background(),
		fmt.Sprintf("CREATE TABLE public.%s (id BIGINT)", pq.QuoteIdentifier(workerFutureTable)),
	)
	require.NoError(t, err)
	_, err = migrationDB.ExecContext(
		context.Background(),
		fmt.Sprintf("CREATE TABLE public.%s (id BIGINT)", pq.QuoteIdentifier(adminFutureTable)),
	)
	require.NoError(t, err)

	for _, tableName := range []string{workerTable, adminTable, workerFutureTable, adminFutureTable} {
		_, err = serverDB.ExecContext(
			context.Background(),
			fmt.Sprintf("SELECT * FROM public.%s LIMIT 1", pq.QuoteIdentifier(tableName)),
		)
		require.NoError(t, err, "server must receive read access to %s", tableName)
	}
	_, err = serverDB.ExecContext(
		context.Background(),
		fmt.Sprintf("INSERT INTO public.%s (id) VALUES (2)", pq.QuoteIdentifier(adminFutureTable)),
	)
	require.Error(t, err, "server must remain read-only")

	// Upgrade a real worker-owned August schema through the current embedded
	// Goose chain. This exercises function replacement and concurrent indexes,
	// including later migrations that intentionally remove obsolete indexes.
	require.NoError(t, configureEmbeddedMigrations())
	t.Cleanup(func() { goose.SetBaseFS(nil) })
	require.NoError(t, goose.UpToContext(context.Background(), workerDB, "db/migrations", 20260810000001))
	// A failed concurrent unique build leaves a real invalid index. Give the
	// fixture a still-pending migration's index name to exercise retry cleanup.
	_, err = workerDB.Exec("INSERT INTO public." + pq.QuoteIdentifier(workerTable) + " (id) VALUES (1)")
	require.NoError(t, err)
	_, err = workerDB.Exec("CREATE UNIQUE INDEX CONCURRENTLY idx_block_consolidation_shadow_retention_due_generation ON public." + pq.QuoteIdentifier(workerTable) + " (id)")
	require.Error(t, err)
	var valid bool
	require.NoError(t, workerDB.QueryRow("SELECT indisvalid FROM pg_index WHERE indexrelid='public.idx_block_consolidation_shadow_retention_due_generation'::regclass").Scan(&valid))
	require.False(t, valid)
	version, err := runPrivilegedMigrations(context.Background(), migrationDSN, migrationUser, workerUser, serverUser, dbName, time.Second, zap.NewNop())
	require.NoError(t, err)
	require.EqualValues(t, headMigrationVersion, version)
	assertMigrationCanaryRemoved(t, migrationDB)
	version, err = runPrivilegedMigrations(context.Background(), migrationDSN, migrationUser, workerUser, serverUser, dbName, time.Second, zap.NewNop())
	require.NoError(t, err, "re-running the privileged migration path must be idempotent")
	require.EqualValues(t, headMigrationVersion, version)
	assertMigrationCanaryRemoved(t, migrationDB)

	var indexPresent bool
	require.NoError(t, migrationDB.QueryRow("SELECT to_regclass('public.idx_block_consolidation_shadow_retention_due_generation') IS NOT NULL").Scan(&indexPresent))
	require.True(t, indexPresent)
	require.NoError(t, migrationDB.QueryRow("SELECT indisvalid AND indrelid='public.block_consolidation_shadow'::regclass FROM pg_index WHERE indexrelid='public.idx_block_consolidation_shadow_retention_due_generation'::regclass").Scan(&valid))
	require.True(t, valid, "retry must rebuild the pending index on the intended table")
	for _, obsolete := range []string{"idx_block_consolidation_shadow_retention_due", "idx_block_consolidation_shadow_retention_watermark"} {
		require.NoError(t, migrationDB.QueryRow("SELECT to_regclass($1) IS NOT NULL", "public."+obsolete).Scan(&indexPresent))
		require.False(t, indexPresent, "obsolete index %s must stay removed", obsolete)
	}
	_, err = serverDB.Exec("SELECT * FROM public.block_metadata LIMIT 1")
	require.NoError(t, err)

	err = withMigrationDatabase(context.Background(), migrationDSN, time.Second, func(lockedDB *sql.DB) error {
		entered := false
		blockedErr := withMigrationDatabase(context.Background(), migrationDSN, 100*time.Millisecond, func(*sql.DB) error {
			entered = true
			return nil
		})
		require.Error(t, blockedErr, "a second migrator must not enter while the database lock is held")
		require.False(t, entered)
		var pid int
		require.NoError(t, lockedDB.QueryRow("SELECT pg_backend_pid()").Scan(&pid))
		_, killErr := masterDB.Exec("SELECT pg_terminate_backend($1)", pid)
		require.NoError(t, killErr)
		_, lostErr := lockedDB.Exec("SELECT 1")
		require.Error(t, lostErr, "a lost session must abort the current migration plan")
		_, lostErr = lockedDB.Exec("SELECT 1")
		require.ErrorIs(t, lostErr, errMigrationSessionLost, "reconnection must not resume a stale Goose plan")
		return nil
	})
	require.NoError(t, err)
	version, err = runPrivilegedMigrations(context.Background(), migrationDSN, migrationUser, workerUser, serverUser, dbName, time.Second, zap.NewNop())
	require.NoError(t, err, "a fresh invocation must recover after session loss")
	require.EqualValues(t, headMigrationVersion, version)

	// Stage the deployed create-canary version, then apply only the pending
	// cleanup through the real privileged path with a non-superuser admin.
	cleanupName := dbName + "_cleanup"
	migrationClusterDB = openIntegrationPostgres(t, host, port, "postgres", migrationUser, migrationPassword)
	require.NoError(t, initializePrivilegedDatabase(context.Background(), migrationClusterDB, cleanupName, migrationUser, workerUser, zap.NewNop()))
	require.NoError(t, migrationClusterDB.Close())
	cleanupMigration := openIntegrationPostgres(t, host, port, cleanupName, migrationUser, migrationPassword)
	defer func() { _ = cleanupMigration.Close() }()
	cleanupWorker := openIntegrationPostgres(t, host, port, cleanupName, workerUser, workerPassword)
	defer func() { _ = cleanupWorker.Close() }()
	cleanupServer := openIntegrationPostgres(t, host, port, cleanupName, serverUser, serverPassword)
	defer func() { _ = cleanupServer.Close() }()
	require.NoError(t, goose.UpToContext(context.Background(), cleanupMigration, "db/migrations", deployedCanaryVersion))
	require.NoError(t, grantMigrationPrivileges(context.Background(), cleanupMigration, migrationUser, workerUser, serverUser, cleanupName))
	assertMigrationCanary(t, cleanupMigration, cleanupWorker, cleanupServer, migrationUser)
	_, err = cleanupWorker.Exec("TRUNCATE public.inf1133_migration_canary")
	require.NoError(t, err, "the deployed canary is empty before cleanup")
	_, err = cleanupWorker.Exec("INSERT INTO public.block_metadata (height,tag,hash,timestamp) VALUES (7,2,'preserved-hash',1)")
	require.NoError(t, err)
	relationsBefore := migrationRelationSnapshot(t, cleanupMigration)

	// A dependency must fail closed, without removing either object or
	// recording the cleanup as applied. No CASCADE belongs in this migration.
	_, err = cleanupMigration.Exec("CREATE VIEW public.canary_dependency AS SELECT id FROM public.inf1133_migration_canary")
	require.NoError(t, err)
	cleanupDSN := integrationPostgresDSN(host, port, cleanupName, migrationUser, migrationPassword)
	_, err = runPrivilegedMigrations(context.Background(), cleanupDSN, migrationUser, workerUser, serverUser, cleanupName, time.Second, zap.NewNop())
	var dependencyErr *pq.Error
	require.ErrorAs(t, err, &dependencyErr)
	require.Equal(t, pq.ErrorCode("2BP01"), dependencyErr.Code)
	version, err = goose.GetDBVersion(cleanupMigration)
	require.NoError(t, err)
	require.EqualValues(t, deployedCanaryVersion, version, "failed cleanup must not advance Goose")
	var count int
	require.NoError(t, cleanupServer.QueryRow("SELECT count(*) FROM public.inf1133_migration_canary").Scan(&count))
	require.Zero(t, count)
	require.NoError(t, cleanupMigration.QueryRow("SELECT count(*) FROM public.canary_dependency").Scan(&count))
	require.Zero(t, count, "the dependent view must survive the failed cleanup")
	_, err = cleanupMigration.Exec("DROP VIEW public.canary_dependency")
	require.NoError(t, err)
	for attempt := 0; attempt < 2; attempt++ {
		version, err = runPrivilegedMigrations(context.Background(), cleanupDSN, migrationUser, workerUser, serverUser, cleanupName, time.Second, zap.NewNop())
		require.NoError(t, err)
		require.EqualValues(t, headMigrationVersion, version)
		assertMigrationCanaryRemoved(t, cleanupMigration)
	}
	require.Equal(t, relationsBefore, migrationRelationSnapshot(t, cleanupMigration),
		"cleanup must preserve all unrelated public relations, owners, and permissions")
	var preservedHash string
	require.NoError(t, cleanupServer.QueryRow("SELECT hash FROM public.block_metadata WHERE height=7 AND tag=2").Scan(&preservedHash))
	require.Equal(t, "preserved-hash", preservedHash)
	_, err = cleanupWorker.Exec("INSERT INTO public.block_metadata (height,tag,hash,timestamp) VALUES (8,2,'worker-still-writes',1)")
	require.NoError(t, err)
	_, err = cleanupServer.Exec("INSERT INTO public.block_metadata (height,tag,hash,timestamp) VALUES (9,2,'server-cannot-write',1)")
	var permissionErr *pq.Error
	require.ErrorAs(t, err, &permissionErr)
	require.Equal(t, pq.ErrorCode("42501"), permissionErr.Code)

	// Local rollback to the staged version restores the canary schema, and a
	// forward retry removes it again. Roll back to an explicit version rather
	// than one step: a plain Down unwinds only whatever migration is currently
	// head, so every migration added after the canary pair would otherwise
	// leave the canary dropped and break the two assertions below.
	require.NoError(t, goose.DownToContext(context.Background(), cleanupMigration, "db/migrations", deployedCanaryVersion))
	version, err = goose.GetDBVersion(cleanupMigration)
	require.NoError(t, err)
	require.EqualValues(t, deployedCanaryVersion, version)
	assertMigrationCanary(t, cleanupMigration, cleanupWorker, cleanupServer, migrationUser)
	version, err = runPrivilegedMigrations(context.Background(), cleanupDSN, migrationUser, workerUser, serverUser, cleanupName, time.Second, zap.NewNop())
	require.NoError(t, err)
	require.EqualValues(t, headMigrationVersion, version)
	assertMigrationCanaryRemoved(t, cleanupMigration)

	// Exercise db-init's entire empty-database migration path, not just an
	// upgrade of a schema created by the runtime worker.
	freshName := dbName + "_fresh"
	migrationClusterDB = openIntegrationPostgres(t, host, port, "postgres", migrationUser, migrationPassword)
	require.NoError(t, initializePrivilegedDatabase(context.Background(), migrationClusterDB, freshName, migrationUser, workerUser, zap.NewNop()))
	require.NoError(t, migrationClusterDB.Close())
	// PostgreSQL <15 grants CREATE on public to PUBLIC by default. Establish
	// the same restricted fixture baseline on every version so this checks
	// that migration grants do not add server DDL privileges, not server defaults.
	freshMaster := openIntegrationPostgres(t, host, port, freshName, masterUser, masterPassword)
	_, err = freshMaster.Exec("REVOKE CREATE ON SCHEMA public FROM PUBLIC")
	require.NoError(t, err)
	_, err = freshMaster.Exec("GRANT CREATE ON SCHEMA public TO " + pq.QuoteIdentifier(workerUser))
	require.NoError(t, err)
	require.NoError(t, freshMaster.Close())
	freshServer := openIntegrationPostgres(t, host, port, freshName, serverUser, serverPassword)
	defer func() { _ = freshServer.Close() }()
	var serverCanCreate bool
	require.NoError(t, freshServer.QueryRow("SELECT has_schema_privilege(current_user, 'public', 'CREATE')").Scan(&serverCanCreate))
	require.False(t, serverCanCreate, "fixture must deny server DDL before migrations")
	require.NoError(t, runMigrations(context.Background(), host, port, migrationUser, migrationPassword, workerUser, serverUser, freshName, zap.NewNop()))
	freshWorker := openIntegrationPostgres(t, host, port, freshName, workerUser, workerPassword)
	defer func() { _ = freshWorker.Close() }()
	freshMigration := openIntegrationPostgres(t, host, port, freshName, migrationUser, migrationPassword)
	defer func() { _ = freshMigration.Close() }()
	assertMigrationCanaryRemoved(t, freshMigration)
	_, err = freshWorker.Exec("INSERT INTO public.block_metadata (height,tag,hash,timestamp) VALUES (1,2,'fixture-hash',1)")
	require.NoError(t, err, "worker must use admin-created tables and serial sequences")
	require.NoError(t, freshServer.QueryRow("SELECT count(*) FROM public.block_metadata").Scan(&count))
	require.Equal(t, 1, count)
	_, err = freshServer.Exec("CREATE TABLE public.server_cannot_create (id INT)")
	require.Error(t, err, "server must not acquire schema DDL privileges")
}

func migrationRelationSnapshot(t *testing.T, db *sql.DB) map[string][2]string {
	t.Helper()
	rows, err := db.Query(`
SELECT relname, pg_get_userbyid(relowner), COALESCE(relacl::text, '')
FROM pg_class
WHERE relnamespace='public'::regnamespace
  AND relname NOT IN ('inf1133_migration_canary', 'inf1133_migration_canary_id_seq', 'inf1133_migration_canary_pkey')`)
	require.NoError(t, err)
	defer func() { _ = rows.Close() }()
	result := make(map[string][2]string)
	for rows.Next() {
		var name, owner, acl string
		require.NoError(t, rows.Scan(&name, &owner, &acl))
		result[name] = [2]string{owner, acl}
	}
	require.NoError(t, rows.Err())
	return result
}

func assertMigrationCanaryRemoved(t *testing.T, db *sql.DB) {
	t.Helper()
	for _, relation := range []string{"public.inf1133_migration_canary", "public.inf1133_migration_canary_id_seq"} {
		var present bool
		require.NoError(t, db.QueryRow("SELECT to_regclass($1) IS NOT NULL", relation).Scan(&present))
		require.False(t, present, "cleanup must remove %s", relation)
	}
	var applied int
	require.NoError(t, db.QueryRow("SELECT count(*) FROM public.goose_db_version WHERE version_id=20260909000001 AND is_applied").Scan(&applied))
	require.Equal(t, 1, applied, "Goose must record the cleanup exactly once")
}

func assertMigrationCanary(t *testing.T, migrationDB, workerDB, serverDB *sql.DB, migrationUser string) {
	t.Helper()
	var owner, sequence string
	require.NoError(t, migrationDB.QueryRow(`
SELECT pg_get_userbyid(relowner), pg_get_serial_sequence('public.inf1133_migration_canary', 'id')
FROM pg_class WHERE oid = 'public.inf1133_migration_canary'::regclass`).Scan(&owner, &sequence))
	require.Equal(t, migrationUser, owner, "the privileged migrator must own the new table")
	require.NotEmpty(t, sequence, "the canary must exercise sequence creation as well as table creation")
	require.NoError(t, migrationDB.QueryRow("SELECT pg_get_userbyid(relowner) FROM pg_class WHERE oid = $1::regclass", sequence).Scan(&owner))
	require.Equal(t, migrationUser, owner, "the privileged migrator must own the new sequence")
	var count int
	require.NoError(t, serverDB.QueryRow("SELECT count(*) FROM public.inf1133_migration_canary").Scan(&count))
	require.Zero(t, count, "the migration must not insert probe data")
	var id int64
	require.NoError(t, workerDB.QueryRow("INSERT INTO public.inf1133_migration_canary DEFAULT VALUES RETURNING id").Scan(&id),
		"worker must be able to insert into the new table with a generated identity")
	require.Positive(t, id)
	var nextID int64
	require.NoError(t, workerDB.QueryRow("SELECT nextval($1::regclass)", sequence).Scan(&nextID),
		"worker must also receive explicit sequence access; identity INSERT bypasses sequence ACLs")
	require.Greater(t, nextID, id)
	var createdAt time.Time
	require.NoError(t, serverDB.QueryRow("SELECT created_at FROM public.inf1133_migration_canary WHERE id = $1", id).Scan(&createdAt))
	require.False(t, createdAt.IsZero())
	for _, query := range []string{
		"INSERT INTO public.inf1133_migration_canary DEFAULT VALUES",
		"UPDATE public.inf1133_migration_canary SET created_at = CURRENT_TIMESTAMP",
		"DELETE FROM public.inf1133_migration_canary",
	} {
		_, err := serverDB.Exec(query)
		var pgErr *pq.Error
		require.ErrorAs(t, err, &pgErr)
		require.Equal(t, pq.ErrorCode("42501"), pgErr.Code, "server writes must fail for insufficient privilege")
	}
	_, err := serverDB.Exec("SELECT nextval($1::regclass)", sequence)
	var pgErr *pq.Error
	require.ErrorAs(t, err, &pgErr)
	require.Equal(t, pq.ErrorCode("42501"), pgErr.Code, "server must not advance the canary sequence")
}

func openIntegrationPostgres(t *testing.T, host string, port int, dbName, user, password string) *sql.DB {
	t.Helper()
	db, err := sql.Open("postgres", integrationPostgresDSN(host, port, dbName, user, password))
	require.NoError(t, err)
	require.NoError(t, db.PingContext(context.Background()))
	return db
}

func integrationPostgresDSN(host string, port int, dbName, user, password string) string {
	return fmt.Sprintf(
		"host=%s port=%d dbname=%s user=%s password=%s sslmode=require connect_timeout=10",
		host,
		port,
		dbName,
		user,
		password,
	)
}
