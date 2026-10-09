package postgres

import (
	"context"
	"database/sql"
	"embed"

	"github.com/pressly/goose/v3"
	"golang.org/x/xerrors"
)

//go:embed db/migrations/*.sql
var embedMigrations embed.FS

// GetEmbeddedMigrations returns the embedded migrations filesystem
// This is used by the admin db-migrate command
func GetEmbeddedMigrations() embed.FS {
	return embedMigrations
}

func runMigrations(ctx context.Context, db *sql.DB) error {
	goose.SetBaseFS(embedMigrations)

	if err := goose.SetDialect("postgres"); err != nil {
		return xerrors.Errorf("failed to set goose dialect: %w", err)
	}

	if err := goose.UpContext(ctx, db, "db/migrations"); err != nil {
		return xerrors.Errorf("failed to run migrations: %w", err)
	}

	return nil
}
