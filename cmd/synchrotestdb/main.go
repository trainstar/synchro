package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/trainstar/synchro/migrate"
	"github.com/trainstar/synchro/synctest"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
	fmt.Println("test database prepared")
}

func run() error {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		dsn = os.Getenv("TEST_DATABASE_URL")
	}
	if dsn == "" {
		return fmt.Errorf("DATABASE_URL or TEST_DATABASE_URL is required")
	}
	slotName := envOr("SLOT_NAME", "synchro_slot")
	publicationName := envOr("PUBLICATION_NAME", "synchro_pub")

	ctx := context.Background()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return fmt.Errorf("opening database: %w", err)
	}
	defer func() { _ = db.Close() }()

	resetStatements := []string{
		fmt.Sprintf(`DROP PUBLICATION IF EXISTS %s`, publicationName),
		`DROP SCHEMA IF EXISTS public CASCADE`,
		`CREATE SCHEMA public`,
		`CREATE EXTENSION IF NOT EXISTS pgcrypto`,
	}
	for _, stmt := range resetStatements {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("resetting test database with %q: %w", stmt, err)
		}
	}
	if _, err := db.ExecContext(ctx, `SELECT pg_drop_replication_slot($1) WHERE EXISTS (
		SELECT 1 FROM pg_replication_slots WHERE slot_name = $1 AND active = false
	)`, slotName); err != nil {
		return fmt.Errorf("dropping replication slot %q: %w", slotName, err)
	}

	for _, stmt := range migrate.Migrations() {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("running migration: %w", err)
		}
	}

	if err := synctest.SetupTestSchema(ctx, db); err != nil {
		return fmt.Errorf("setting up app schema: %w", err)
	}

	return nil
}

func envOr(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
