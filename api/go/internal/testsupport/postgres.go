package testsupport

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

var nonIdentifierByte = regexp.MustCompile(`[^a-z0-9_]+`)
var testRunNonce = fmt.Sprintf("%d-%d", os.Getpid(), time.Now().UnixNano())
var testNameCounter atomic.Uint64

// OpenPostgres opens the configured integration database after validating its server contract.
func OpenPostgres(t testing.TB) *sql.DB {
	t.Helper()
	databaseURL := strings.TrimSpace(getenv(t, "TEST_DATABASE_URL"))
	if databaseURL == "" {
		t.Fatal("TEST_DATABASE_URL is required")
	}

	db, err := sql.Open("pgx", databaseURL)
	if err != nil {
		t.Fatalf("opening postgres database: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	ctx := context.Background()
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("pinging postgres database: %v", err)
	}
	if _, err := db.ExecContext(ctx, "CREATE EXTENSION IF NOT EXISTS synchro_pg CASCADE"); err != nil {
		t.Fatalf("ensuring synchro_pg extension: %v", err)
	}
	verifyExtensionObjects(ctx, t, db)

	workerLogin := configuredWorkerLogin(ctx, t, db)
	ensureWorkerLogin(ctx, t, db, workerLogin)
	verifyServerContract(ctx, t, db)
	return db
}

// UniqueName returns a valid, test-owned PostgreSQL identifier derived from t.Name.
func UniqueName(t testing.TB, prefix string) string {
	t.Helper()
	cleanPrefix := nonIdentifierByte.ReplaceAllString(strings.ToLower(prefix), "_")
	cleanPrefix = strings.Trim(cleanPrefix, "_")
	if cleanPrefix == "" {
		cleanPrefix = "test"
	}
	cleanTestName := nonIdentifierByte.ReplaceAllString(strings.ToLower(t.Name()), "_")
	cleanTestName = strings.Trim(cleanTestName, "_")
	if cleanTestName == "" {
		cleanTestName = "case"
	}
	sequence := testNameCounter.Add(1)
	hash := fmt.Sprintf("%x", sha256.Sum256([]byte(fmt.Sprintf("%s:%s:%d", t.Name(), testRunNonce, sequence))))[:10]
	const separator = "_"
	const maxIdentifierBytes = 63
	reserved := len(cleanPrefix) + len(separator) + len(separator) + len(hash)
	available := maxIdentifierBytes - reserved
	if available < 1 {
		cleanPrefix = cleanPrefix[:maxIdentifierBytes-len(separator)-len(hash)-1]
		available = 1
	}
	if len(cleanTestName) > available {
		cleanTestName = cleanTestName[:available]
	}
	return cleanPrefix + separator + cleanTestName + separator + hash
}

func getenv(t testing.TB, key string) string {
	t.Helper()
	return strings.TrimSpace(os.Getenv(key))
}

func configuredWorkerLogin(ctx context.Context, t testing.TB, db *sql.DB) string {
	t.Helper()
	var configured sql.NullString
	if err := db.QueryRowContext(ctx, "SELECT pg_catalog.current_setting('synchro.worker_login', true)").Scan(&configured); err != nil {
		t.Fatalf("checking synchro.worker_login: %v", err)
	}
	if configured.Valid && strings.TrimSpace(configured.String) != "" {
		return configured.String
	}
	t.Fatal("integration database requires synchro.worker_login to name a worker login role")
	return ""
}

func ensureWorkerLogin(ctx context.Context, t testing.TB, db *sql.DB, workerLogin string) {
	t.Helper()
	quotedLogin := quoteIdentifier(workerLogin)
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("starting worker login setup transaction: %v", err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, "SELECT pg_catalog.pg_advisory_xact_lock(781234567890123456)"); err != nil {
		t.Fatalf("locking worker login setup: %v", err)
	}
	roleStatement := fmt.Sprintf(`
		DO $roles$
		BEGIN
			IF NOT EXISTS (
				SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = %s
			) THEN
				CREATE ROLE %s LOGIN REPLICATION NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOBYPASSRLS;
			ELSIF EXISTS (
				SELECT 1
				FROM pg_catalog.pg_roles
				WHERE rolname = %s
				  AND rolcanlogin
				  AND rolreplication
				  AND NOT rolinherit
				  AND NOT rolsuper
				  AND NOT rolcreatedb
				  AND NOT rolcreaterole
				  AND NOT rolbypassrls
			) THEN
				NULL;
			ELSE
				ALTER ROLE %s LOGIN REPLICATION NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOBYPASSRLS;
			END IF;
		END
		$roles$`, quoteLiteral(workerLogin), quotedLogin, quoteLiteral(workerLogin), quotedLogin)
	if _, err := tx.ExecContext(ctx, roleStatement); err != nil {
		t.Fatalf("ensuring worker login role and grants: %v", err)
	}
	if _, err := tx.ExecContext(ctx, fmt.Sprintf("GRANT synchro_worker TO %s", quotedLogin)); err != nil {
		t.Fatalf("granting worker login role: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("committing worker login setup: %v", err)
	}
}

func verifyServerContract(ctx context.Context, t testing.TB, db *sql.DB) {
	t.Helper()
	settings := map[string]string{
		"wal_level":                showSetting(ctx, t, db, "wal_level"),
		"shared_preload_libraries": showSetting(ctx, t, db, "shared_preload_libraries"),
		"synchro.auto_start":       showSetting(ctx, t, db, "synchro.auto_start"),
		"synchro.database":         showSetting(ctx, t, db, "synchro.database"),
	}
	if settings["wal_level"] != "logical" {
		t.Fatalf("integration database requires wal_level=logical, got %q", settings["wal_level"])
	}
	if !containsLibrary(settings["shared_preload_libraries"], "synchro_pg") {
		t.Fatalf("integration database requires synchro_pg in shared_preload_libraries, got %q", settings["shared_preload_libraries"])
	}
	if settings["synchro.auto_start"] != "on" {
		t.Fatalf("integration database requires synchro.auto_start=on, got %q", settings["synchro.auto_start"])
	}
	var databaseName string
	if err := db.QueryRowContext(ctx, "SELECT pg_catalog.current_database()").Scan(&databaseName); err != nil {
		t.Fatalf("checking connected database name: %v", err)
	}
	if settings["synchro.database"] != databaseName {
		t.Fatalf("integration database requires synchro.database=%q, got %q", databaseName, settings["synchro.database"])
	}
}

func verifyExtensionObjects(ctx context.Context, t testing.TB, db *sql.DB) {
	t.Helper()
	if err := extensionObjectsError(ctx, db); err != nil {
		t.Fatal(err)
	}
}

func extensionObjectsError(ctx context.Context, db *sql.DB) error {
	var raw []byte
	if err := db.QueryRowContext(ctx, "SELECT synchro.synchro_contract_info()").Scan(&raw); err != nil {
		return fmt.Errorf("loading synchro_pg contract info: %w", err)
	}
	var info struct {
		LibraryBuildFingerprint   string `json:"library_build_fingerprint"`
		InstalledBuildFingerprint string `json:"installed_build_fingerprint"`
		ExtensionObjectsCurrent   bool   `json:"extension_objects_current"`
	}
	if err := json.Unmarshal(raw, &info); err != nil {
		return fmt.Errorf("decoding synchro_pg contract info: %w", err)
	}
	library := strings.TrimSpace(info.LibraryBuildFingerprint)
	installed := strings.TrimSpace(info.InstalledBuildFingerprint)
	objectsCurrent := info.ExtensionObjectsCurrent && library != "" && installed != ""
	if library == "" {
		library = "missing"
	}
	if installed == "" {
		installed = "missing"
	}
	if !objectsCurrent || library != installed {
		return fmt.Errorf(
			"synchro_pg extension objects are stale: library fingerprint %q, installed objects fingerprint %q; recreate or update the extension",
			library,
			installed,
		)
	}
	return nil
}

func showSetting(ctx context.Context, t testing.TB, db *sql.DB, name string) string {
	t.Helper()
	var value string
	if err := db.QueryRowContext(ctx, "SELECT pg_catalog.current_setting($1)", name).Scan(&value); err != nil {
		t.Fatalf("checking %s: %v", name, err)
	}
	return strings.TrimSpace(value)
}

func containsLibrary(value, expected string) bool {
	for _, library := range strings.Split(value, ",") {
		if strings.TrimSpace(library) == expected {
			return true
		}
	}
	return false
}

func quoteIdentifier(value string) string {
	return `"` + strings.ReplaceAll(value, `"`, `""`) + `"`
}

func quoteLiteral(value string) string {
	return `'` + strings.ReplaceAll(value, `'`, `''`) + `'`
}
