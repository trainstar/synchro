package synctest

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/trainstar/synchro/migrate"
)

// appTableDDL creates the application tables used by NewTestRegistry.
var appTableDDL = []string{
	`CREATE TABLE IF NOT EXISTS items (
		id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
		user_id UUID NOT NULL,
		name TEXT NOT NULL DEFAULT '',
		description TEXT NOT NULL DEFAULT '',
		created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
		updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
		deleted_at TIMESTAMPTZ
	)`,
	`CREATE TABLE IF NOT EXISTS item_details (
		id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
		item_id UUID NOT NULL REFERENCES items(id),
		notes TEXT NOT NULL DEFAULT '',
		created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
		updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
		deleted_at TIMESTAMPTZ
	)`,
	`CREATE TABLE IF NOT EXISTS categories (
		id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
		name TEXT NOT NULL DEFAULT '',
		created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
		updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
		deleted_at TIMESTAMPTZ
	)`,
	`CREATE TABLE IF NOT EXISTS tags (
		id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
		user_id UUID,
		name TEXT NOT NULL DEFAULT '',
		created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
		updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
		deleted_at TIMESTAMPTZ
	)`,
}

// TestDB connects to TEST_DATABASE_URL, runs migrations and app DDL, and
// returns a *sql.DB. It calls t.Skip if TEST_DATABASE_URL is not set.
// Cleanup truncates all tables.
func TestDB(t *testing.T) *sql.DB {
	t.Helper()

	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL not set, skipping integration test")
	}

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("opening database: %v", err)
	}

	ctx := context.Background()

	// Run synchro infrastructure migrations
	for _, stmt := range migrate.Migrations() {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			db.Close()
			t.Fatalf("running migration: %v", err)
		}
	}

	// Create app tables
	for _, stmt := range appTableDDL {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			db.Close()
			t.Fatalf("creating app table: %v", err)
		}
	}

	t.Cleanup(func() {
		tables := []string{"item_details", "items", "categories", "tags", "sync_changelog", "sync_clients"}
		for _, table := range tables {
			_, _ = db.ExecContext(ctx, fmt.Sprintf("TRUNCATE %s CASCADE", table))
		}
		db.Close()
	})

	return db
}
