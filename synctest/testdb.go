package synctest

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand"
	"net/url"
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

// CreateTempDB creates a uniquely-named database for test isolation.
// Returns the database name and a connection to it. The caller must
// call DropTempDB in cleanup.
func CreateTempDB(t *testing.T, dsn string) (string, *sql.DB) {
	t.Helper()

	adminDB, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("opening admin connection: %v", err)
	}

	dbName := fmt.Sprintf("synchro_test_%d", rand.Int63())

	if _, err := adminDB.ExecContext(context.Background(), fmt.Sprintf("CREATE DATABASE %q", dbName)); err != nil {
		adminDB.Close()
		t.Fatalf("creating temp database %s: %v", dbName, err)
	}
	adminDB.Close()

	tempDSN := ReplaceDSNDatabase(dsn, dbName)
	db, err := sql.Open("pgx", tempDSN)
	if err != nil {
		// Best-effort drop
		DropTempDB(dsn, dbName)
		t.Fatalf("connecting to temp database %s: %v", dbName, err)
	}

	return dbName, db
}

// DropTempDB connects to the base DSN and drops the named database.
func DropTempDB(baseDSN, dbName string) {
	db, err := sql.Open("pgx", baseDSN)
	if err != nil {
		return
	}
	defer db.Close()
	// Terminate any lingering connections before dropping.
	db.ExecContext(context.Background(), fmt.Sprintf(
		"SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '%s' AND pid <> pg_backend_pid()", dbName))
	db.ExecContext(context.Background(), fmt.Sprintf("DROP DATABASE IF EXISTS %q", dbName))
}

// ReplaceDSNDatabase returns a new DSN with the database name replaced.
func ReplaceDSNDatabase(dsn, newDB string) string {
	parsed, err := url.Parse(dsn)
	if err != nil {
		return dsn
	}
	parsed.Path = "/" + newDB
	return parsed.String()
}

// SetupTestSchema creates the application tables and sets REPLICA IDENTITY FULL
// for WAL logical replication. Idempotent — safe to call on every startup.
func SetupTestSchema(ctx context.Context, db *sql.DB) error {
	for _, stmt := range appTableDDL {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("creating app table: %w", err)
		}
	}
	for _, table := range []string{"items", "item_details", "categories", "tags"} {
		if _, err := db.ExecContext(ctx, fmt.Sprintf("ALTER TABLE %s REPLICA IDENTITY FULL", table)); err != nil {
			return fmt.Errorf("setting replica identity on %s: %w", table, err)
		}
	}
	return nil
}

// TestDB creates an isolated temporary database, runs migrations and app DDL,
// and returns a *sql.DB. It calls t.Skip if TEST_DATABASE_URL is not set.
// The temporary database is dropped on cleanup.
func TestDB(t *testing.T) *sql.DB {
	t.Helper()

	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL not set, skipping integration test")
	}

	dbName, db := CreateTempDB(t, dsn)

	ctx := context.Background()

	// Run synchro infrastructure migrations
	for _, stmt := range migrate.Migrations() {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			db.Close()
			DropTempDB(dsn, dbName)
			t.Fatalf("running migration: %v", err)
		}
	}

	// Create app tables
	for _, stmt := range appTableDDL {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			db.Close()
			DropTempDB(dsn, dbName)
			t.Fatalf("creating app table: %v", err)
		}
	}

	t.Cleanup(func() {
		db.Close()
		DropTempDB(dsn, dbName)
	})

	return db
}

// TestDBWithAppRole creates an isolated temporary database and returns two
// connections: adminDB (superuser, for DDL/assertions) and appDB (non-superuser,
// for engine operations under RLS).
//
// PostgreSQL superusers always bypass RLS, so RLS tests must use a non-superuser
// connection. The function creates the "synchro_app" role (if not exists) and
// grants it access to all tables in the temp database.
func TestDBWithAppRole(t *testing.T) (adminDB *sql.DB, appDB *sql.DB) {
	t.Helper()

	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL not set, skipping integration test")
	}

	dbName, adminConn := CreateTempDB(t, dsn)

	ctx := context.Background()

	// Run migrations and create tables via superuser.
	for _, stmt := range migrate.Migrations() {
		if _, err := adminConn.ExecContext(ctx, stmt); err != nil {
			adminConn.Close()
			DropTempDB(dsn, dbName)
			t.Fatalf("running migration: %v", err)
		}
	}
	for _, stmt := range appTableDDL {
		if _, err := adminConn.ExecContext(ctx, stmt); err != nil {
			adminConn.Close()
			DropTempDB(dsn, dbName)
			t.Fatalf("creating app table: %v", err)
		}
	}

	// Create non-superuser role for RLS testing.
	roleSetup := []string{
		`DO $$ BEGIN
			IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'synchro_app') THEN
				CREATE ROLE synchro_app LOGIN PASSWORD 'synchro_app' NOSUPERUSER NOBYPASSRLS;
			END IF;
		END $$`,
		`GRANT ALL ON ALL TABLES IN SCHEMA public TO synchro_app`,
		`GRANT USAGE ON ALL SEQUENCES IN SCHEMA public TO synchro_app`,
		`ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO synchro_app`,
		`ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT USAGE ON SEQUENCES TO synchro_app`,
	}
	for _, stmt := range roleSetup {
		if _, err := adminConn.ExecContext(ctx, stmt); err != nil {
			adminConn.Close()
			DropTempDB(dsn, dbName)
			t.Fatalf("setting up app role: %v", err)
		}
	}

	// Open connection as the non-superuser role.
	parsed, _ := url.Parse(dsn)
	appDSN := fmt.Sprintf("postgres://synchro_app:synchro_app@%s/%s?sslmode=disable", parsed.Host, dbName)
	appConn, err := sql.Open("pgx", appDSN)
	if err != nil {
		adminConn.Close()
		DropTempDB(dsn, dbName)
		t.Fatalf("opening app database: %v", err)
	}

	if err := appConn.PingContext(ctx); err != nil {
		appConn.Close()
		adminConn.Close()
		DropTempDB(dsn, dbName)
		t.Fatalf("pinging app database: %v", err)
	}

	t.Cleanup(func() {
		appConn.Close()
		adminConn.Close()
		DropTempDB(dsn, dbName)
	})

	return adminConn, appConn
}
