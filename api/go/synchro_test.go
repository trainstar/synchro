package synchroapi

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

// testServer creates a test HTTP server backed by a real PG with the extension.
// Skips the test if TEST_DATABASE_URL is not set.
func testServer(t *testing.T) *httptest.Server {
	t.Helper()

	dbURL := os.Getenv("TEST_DATABASE_URL")
	if dbURL == "" {
		t.Skip("TEST_DATABASE_URL not set (requires PG with synchro_pg extension)")
	}

	db, err := sql.Open("pgx", dbURL)
	if err != nil {
		t.Fatalf("opening database: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	if err := db.PingContext(context.Background()); err != nil {
		t.Fatalf("pinging database: %v", err)
	}

	// Verify extension is installed.
	var extExists bool
	if err := db.QueryRow(
		"SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'synchro_pg')",
	).Scan(&extExists); err != nil || !extExists {
		t.Skip("synchro_pg extension not installed")
	}

	// Clean up stale test clients from prior runs.
	_, _ = db.ExecContext(context.Background(),
		"DELETE FROM sync_clients WHERE client_id LIKE 'test-%' OR client_id LIKE '%-client'")

	handler := Routes(Config{
		DB:               db,
		JWTSecret:        []byte("test-secret-for-integration-tests"),
		MinClientVersion: "1.0.0",
	})

	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)

	return srv
}

// testToken generates a valid HS256 JWT for the given user ID.
func testToken(userID string) string {
	header := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"HS256","typ":"JWT"}`))
	payload := base64.RawURLEncoding.EncodeToString(
		[]byte(fmt.Sprintf(`{"sub":"%s","iat":1700000000,"exp":9999999999}`, userID)),
	)
	sigInput := header + "." + payload
	mac := hmac.New(sha256.New, []byte("test-secret-for-integration-tests"))
	mac.Write([]byte(sigInput))
	sig := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	return sigInput + "." + sig
}

// doJSON sends a JSON request and returns status code + parsed body.
func doJSON(t *testing.T, method, url, token string, body any) (int, map[string]any) {
	t.Helper()
	var reqBody io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("marshaling request: %v", err)
		}
		reqBody = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, url, reqBody)
	if err != nil {
		t.Fatalf("creating request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("sending request: %v", err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	var result map[string]any
	_ = json.Unmarshal(raw, &result)
	return resp.StatusCode, result
}

// assertISO8601 validates that a field in the response body is an ISO 8601 UTC timestamp.
func assertISO8601(t *testing.T, body map[string]any, field string) {
	t.Helper()
	v, ok := body[field].(string)
	if !ok {
		t.Errorf("%s is not a string: %v", field, body[field])
		return
	}
	isUTC := strings.HasSuffix(v, "Z") || strings.HasSuffix(v, "+00:00")
	if !strings.Contains(v, "T") || !isUTC {
		t.Errorf("%s is not ISO 8601 UTC: %s", field, v)
	}
}

// ---------------------------------------------------------------------------
// Passthrough tests
// ---------------------------------------------------------------------------

func TestRegisterPassthrough(t *testing.T) {
	srv := testServer(t)
	token := testToken("user-1")

	status, body := doJSON(t, "POST", srv.URL+"/sync/register", token, map[string]any{
		"client_id":      "test-client-1",
		"platform":       "test",
		"app_version":    "1.0.0",
		"schema_version": 0,
		"schema_hash":    "",
	})

	if status != 200 {
		t.Fatalf("expected 200, got %d: %v", status, body)
	}
	if body["id"] == nil {
		t.Error("response missing 'id'")
	}
	if body["server_time"] == nil {
		t.Error("response missing 'server_time'")
	}
	assertISO8601(t, body, "server_time")
}

func TestPullPassthrough(t *testing.T) {
	srv := testServer(t)
	token := testToken("user-1")

	// Register first.
	doJSON(t, "POST", srv.URL+"/sync/register", token, map[string]any{
		"client_id": "pull-client", "schema_version": 0, "schema_hash": "",
	})

	status, body := doJSON(t, "POST", srv.URL+"/sync/pull", token, map[string]any{
		"client_id":      "pull-client",
		"checkpoint":     0,
		"schema_version": 0,
		"schema_hash":    "",
	})

	if status != 200 {
		t.Fatalf("expected 200, got %d: %v", status, body)
	}
	if body["changes"] == nil {
		t.Error("response missing 'changes'")
	}
	if body["deletes"] == nil {
		t.Error("response missing 'deletes'")
	}
	if body["checkpoint"] == nil {
		t.Error("response missing 'checkpoint'")
	}
}

func TestPullKnownBucketsPassthrough(t *testing.T) {
	srv := testServer(t)
	token := testToken("user-1")

	doJSON(t, "POST", srv.URL+"/sync/register", token, map[string]any{
		"client_id": "kb-client", "schema_version": 0, "schema_hash": "",
	})

	// Send known_buckets that's missing the "global" bucket the server knows about.
	status, body := doJSON(t, "POST", srv.URL+"/sync/pull", token, map[string]any{
		"client_id":      "kb-client",
		"checkpoint":     0,
		"known_buckets":  []string{"user:user-1"},
		"schema_version": 0,
		"schema_hash":    "",
	})

	if status != 200 {
		t.Fatalf("expected 200, got %d: %v", status, body)
	}
	// bucket_updates should be present with "global" in added.
	updates, ok := body["bucket_updates"].(map[string]any)
	if !ok {
		t.Fatal("response missing 'bucket_updates'")
	}
	added, ok := updates["added"].([]any)
	if !ok {
		t.Fatal("bucket_updates missing 'added'")
	}
	found := false
	for _, v := range added {
		if v == "global" {
			found = true
		}
	}
	if !found {
		t.Errorf("expected 'global' in bucket_updates.added, got %v", added)
	}
}

func TestPushPassthrough(t *testing.T) {
	srv := testServer(t)
	token := testToken("user-1")

	doJSON(t, "POST", srv.URL+"/sync/register", token, map[string]any{
		"client_id": "push-client", "schema_version": 0, "schema_hash": "",
	})

	status, body := doJSON(t, "POST", srv.URL+"/sync/push", token, map[string]any{
		"client_id": "push-client",
		"changes": []map[string]any{
			{
				"id":         "a1000000-0001-0001-0001-000000000001",
				"table_name": "test_push_table",
				"operation":  "create",
				"data":       map[string]any{"name": "test"},
			},
		},
		"schema_version": 0,
		"schema_hash":    "",
	})

	if status != 200 {
		t.Fatalf("expected 200, got %d: %v", status, body)
	}
	// Push response should have accepted and rejected arrays (even if the table
	// doesn't exist, the per-record result goes to rejected).
	if body["accepted"] == nil {
		t.Error("response missing 'accepted'")
	}
	if body["rejected"] == nil {
		t.Error("response missing 'rejected'")
	}
}

func TestPushResponseEnvelopePassthrough(t *testing.T) {
	srv := testServer(t)
	token := testToken("user-1")

	doJSON(t, "POST", srv.URL+"/sync/register", token, map[string]any{
		"client_id": "envelope-client", "schema_version": 0, "schema_hash": "",
	})

	status, body := doJSON(t, "POST", srv.URL+"/sync/push", token, map[string]any{
		"client_id":      "envelope-client",
		"changes":        []map[string]any{},
		"schema_version": 0,
		"schema_hash":    "",
	})

	if status != 200 {
		t.Fatalf("expected 200, got %d: %v", status, body)
	}
	for _, field := range []string{"accepted", "rejected", "checkpoint", "server_time", "schema_version", "schema_hash"} {
		if body[field] == nil {
			t.Errorf("push response missing '%s'", field)
		}
	}
	assertISO8601(t, body, "server_time")
}

func TestRebuildPassthrough(t *testing.T) {
	srv := testServer(t)
	token := testToken("user-1")

	doJSON(t, "POST", srv.URL+"/sync/register", token, map[string]any{
		"client_id": "rebuild-client", "schema_version": 0, "schema_hash": "",
	})

	status, body := doJSON(t, "POST", srv.URL+"/sync/rebuild", token, map[string]any{
		"client_id": "rebuild-client",
		"bucket_id": "user:user-1",
	})

	if status != 200 {
		t.Fatalf("expected 200, got %d: %v", status, body)
	}
	if body["records"] == nil {
		t.Error("response missing 'records'")
	}
	if body["checkpoint"] == nil {
		t.Error("response missing 'checkpoint'")
	}
}

func TestSchemaNoAuth(t *testing.T) {
	srv := testServer(t)

	// No token. Should succeed.
	status, body := doJSON(t, "GET", srv.URL+"/sync/schema", "", nil)

	if status != 200 {
		t.Fatalf("expected 200, got %d: %v", status, body)
	}
	if body["tables"] == nil {
		t.Error("response missing 'tables'")
	}
	if body["schema_version"] == nil {
		t.Error("response missing 'schema_version'")
	}

	// Validate table-level contract if tables are present.
	if tables, ok := body["tables"].([]any); ok && len(tables) > 0 {
		tbl, ok := tables[0].(map[string]any)
		if !ok {
			t.Fatal("first table entry is not an object")
		}
		if tbl["primary_key"] == nil {
			t.Error("table missing 'primary_key'")
		}
		if cols, ok := tbl["columns"].([]any); ok && len(cols) > 0 {
			col, ok := cols[0].(map[string]any)
			if !ok {
				t.Fatal("first column entry is not an object")
			}
			if col["logical_type"] == nil {
				t.Error("column missing 'logical_type'")
			}
			if col["default_kind"] == nil {
				t.Error("column missing 'default_kind'")
			}
		}
	}
}

func TestTablesNoAuth(t *testing.T) {
	srv := testServer(t)

	status, body := doJSON(t, "GET", srv.URL+"/sync/tables", "", nil)

	if status != 200 {
		t.Fatalf("expected 200, got %d: %v", status, body)
	}
	if body["tables"] == nil {
		t.Error("response missing 'tables'")
	}
}

func TestDebugRequiresAuth(t *testing.T) {
	srv := testServer(t)

	// No token. Should fail with 401.
	status, _ := doJSON(t, "GET", srv.URL+"/sync/debug?client_id=test", "", nil)

	if status != 401 {
		t.Errorf("expected 401, got %d", status)
	}
}

func TestSchemaMismatch409Body(t *testing.T) {
	dbURL := os.Getenv("TEST_DATABASE_URL")
	if dbURL == "" {
		t.Skip("TEST_DATABASE_URL not set")
	}

	db, err := sql.Open("pgx", dbURL)
	if err != nil {
		t.Fatalf("opening database: %v", err)
	}
	defer db.Close()

	// Create a test table and register it to populate the schema manifest.
	_, _ = db.Exec("CREATE TABLE IF NOT EXISTS test_mismatch_tbl (id UUID PRIMARY KEY DEFAULT gen_random_uuid(), name TEXT, updated_at TIMESTAMPTZ DEFAULT now(), deleted_at TIMESTAMPTZ)")
	_, _ = db.Exec("SELECT synchro_register_table('test_mismatch_tbl', $$SELECT ARRAY['global'] FROM test_mismatch_tbl WHERE id = $1::uuid$$, 'id', 'updated_at', 'deleted_at', 'read_only')")
	t.Cleanup(func() {
		_, _ = db.Exec("SELECT synchro_unregister_table('test_mismatch_tbl')")
		_, _ = db.Exec("DROP TABLE IF EXISTS test_mismatch_tbl")
	})

	handler := Routes(Config{
		DB:        db,
		JWTSecret: []byte("test-secret-for-integration-tests"),
	})
	srv := httptest.NewServer(handler)
	defer srv.Close()

	token := testToken("user-1")

	// Register the client (with no schema validation).
	doJSON(t, "POST", srv.URL+"/sync/register", token, map[string]any{
		"client_id": "mismatch-client", "schema_version": 0, "schema_hash": "",
	})

	// Push with wrong schema. Manifest has entries now, so this should return 409.
	status, body := doJSON(t, "POST", srv.URL+"/sync/push", token, map[string]any{
		"client_id":      "mismatch-client",
		"changes":        []map[string]any{},
		"schema_version": 999,
		"schema_hash":    "definitely_wrong_hash",
	})

	if status != 409 {
		t.Fatalf("expected 409, got %d: %v", status, body)
	}
	if body["error"] != "schema_mismatch" {
		t.Errorf("expected error='schema_mismatch', got %v", body["error"])
	}
	if body["server_schema_version"] == nil {
		t.Error("409 response missing 'server_schema_version'")
	}
	if body["server_schema_hash"] == nil {
		t.Error("409 response missing 'server_schema_hash'")
	}
}

func TestClientNotFound404(t *testing.T) {
	srv := testServer(t)
	token := testToken("nonexistent-user")

	status, body := doJSON(t, "POST", srv.URL+"/sync/pull", token, map[string]any{
		"client_id":      "nonexistent-client",
		"checkpoint":     0,
		"schema_version": 0,
		"schema_hash":    "",
	})

	if status != 404 {
		t.Fatalf("expected 404, got %d: %v", status, body)
	}
	if body["error"] != "client_not_found" {
		t.Errorf("expected error='client_not_found', got %v", body["error"])
	}
}

func TestSQLError503(t *testing.T) {
	// Use a broken database URL to simulate connection failure.
	dbURL := os.Getenv("TEST_DATABASE_URL")
	if dbURL == "" {
		t.Skip("TEST_DATABASE_URL not set")
	}

	// Open a connection then close it to simulate a dead pool.
	db, err := sql.Open("pgx", dbURL)
	if err != nil {
		t.Fatalf("opening database: %v", err)
	}
	_ = db.Close() // Close immediately.

	handler := Routes(Config{
		DB:        db,
		JWTSecret: []byte("test-secret-for-integration-tests"),
	})
	srv := httptest.NewServer(handler)
	defer srv.Close()

	status, body := doJSON(t, "GET", srv.URL+"/sync/schema", "", nil)

	// Closed DB should produce 500 or 503.
	if status != 500 && status != 503 {
		t.Errorf("expected 500 or 503, got %d: %v", status, body)
	}
}

// ---------------------------------------------------------------------------
// End-to-end pipeline test: push -> WAL consumer -> changelog -> pull
// ---------------------------------------------------------------------------

func TestEndToEndPushThenPull(t *testing.T) {
	dbURL := os.Getenv("TEST_DATABASE_URL")
	if dbURL == "" {
		t.Skip("TEST_DATABASE_URL not set")
	}

	db, err := sql.Open("pgx", dbURL)
	if err != nil {
		t.Fatalf("opening database: %v", err)
	}
	defer db.Close()

	// Verify extension and WAL consumer prerequisites.
	var extExists bool
	if err := db.QueryRow(
		"SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'synchro_pg')",
	).Scan(&extExists); err != nil || !extExists {
		t.Skip("synchro_pg extension not installed")
	}

	// Check wal_level is logical.
	var walLevel string
	if err := db.QueryRow("SHOW wal_level").Scan(&walLevel); err != nil {
		t.Fatalf("checking wal_level: %v", err)
	}
	if walLevel != "logical" {
		t.Skip("wal_level is not logical (required for WAL consumer E2E test)")
	}

	// Create a dedicated test table for E2E.
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS test_e2e_items (
			id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
			user_id TEXT NOT NULL,
			title TEXT NOT NULL DEFAULT '',
			created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			deleted_at TIMESTAMPTZ
		)`)
	if err != nil {
		t.Fatalf("creating test table: %v", err)
	}
	// Clean up from any prior failed runs.
	_, _ = db.Exec("DELETE FROM sync_changelog WHERE table_name = 'test_e2e_items'")
	_, _ = db.Exec("DELETE FROM sync_bucket_edges WHERE table_name = 'test_e2e_items'")
	_, _ = db.Exec("DELETE FROM test_e2e_items")
	_, _ = db.Exec("DELETE FROM sync_clients WHERE client_id = 'e2e-client'")

	t.Cleanup(func() {
		_, _ = db.Exec("DELETE FROM sync_changelog WHERE table_name = 'test_e2e_items'")
		_, _ = db.Exec("DELETE FROM sync_bucket_edges WHERE table_name = 'test_e2e_items'")
		_, _ = db.Exec("SELECT synchro_unregister_table('test_e2e_items')")
		_, _ = db.Exec("DROP TABLE IF EXISTS test_e2e_items")
		_, _ = db.Exec("DELETE FROM sync_clients WHERE client_id = 'e2e-client'")
	})

	// Register table for sync.
	_, err = db.Exec(`SELECT synchro_register_table(
		'test_e2e_items',
		$$SELECT ARRAY['user:' || user_id] FROM test_e2e_items WHERE id = $1::uuid$$,
		'id', 'updated_at', 'deleted_at', 'enabled'
	)`)
	if err != nil {
		t.Fatalf("registering table: %v", err)
	}

	// Signal the WAL background worker to reload its registry so it
	// picks up the newly registered table.
	if _, err := db.Exec("SELECT pg_reload_conf()"); err != nil {
		t.Fatalf("reloading config: %v", err)
	}
	time.Sleep(500 * time.Millisecond) // Give bgworker time to reload.

	// Set up HTTP server.
	handler := Routes(Config{
		DB:        db,
		JWTSecret: []byte("test-secret-for-integration-tests"),
	})
	srv := httptest.NewServer(handler)
	defer srv.Close()

	token := testToken("e2e-user")

	// Register client.
	status, body := doJSON(t, "POST", srv.URL+"/sync/register", token, map[string]any{
		"client_id": "e2e-client", "schema_version": 0, "schema_hash": "",
	})
	if status != 200 {
		t.Fatalf("register failed: %d %v", status, body)
	}

	// Push a record.
	pushID := "e2e00000-0001-0001-0001-000000000001"
	status, body = doJSON(t, "POST", srv.URL+"/sync/push", token, map[string]any{
		"client_id": "e2e-client",
		"changes": []map[string]any{
			{
				"id":         pushID,
				"table_name": "test_e2e_items",
				"operation":  "create",
				"data":       map[string]any{"user_id": "e2e-user", "title": "E2E Test Record"},
			},
		},
		"schema_version": 0,
		"schema_hash":    "",
	})
	if status != 200 {
		t.Fatalf("push failed: %d %v", status, body)
	}
	accepted, _ := body["accepted"].([]any)
	if len(accepted) == 0 {
		t.Fatalf("push returned no accepted results: %v", body)
	}
	if s, _ := accepted[0].(map[string]any)["status"].(string); s != "applied" {
		t.Fatalf("push not applied: %v", accepted[0])
	}

	// Verify record exists in the data table.
	var title string
	err = db.QueryRow("SELECT title FROM test_e2e_items WHERE id = $1::uuid", pushID).Scan(&title)
	if err != nil {
		t.Fatalf("record not in data table: %v", err)
	}
	if title != "E2E Test Record" {
		t.Fatalf("wrong title: %s", title)
	}

	// Wait for the WAL consumer to process the change into the changelog.
	// The bgworker polls every 100ms. Give it up to 5 seconds.
	var changelogCount int64
	for attempt := 0; attempt < 50; attempt++ {
		err = db.QueryRow(
			"SELECT count(*) FROM sync_changelog WHERE table_name = 'test_e2e_items' AND record_id = $1",
			pushID,
		).Scan(&changelogCount)
		if err == nil && changelogCount > 0 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if changelogCount == 0 {
		t.Fatal("WAL consumer did not populate changelog within 5 seconds. " +
			"Verify synchro.auto_start = on and wal_level = logical.")
	}

	// Pull and verify the record comes back.
	status, body = doJSON(t, "POST", srv.URL+"/sync/pull", token, map[string]any{
		"client_id":      "e2e-client",
		"checkpoint":     0,
		"schema_version": 0,
		"schema_hash":    "",
	})
	if status != 200 {
		t.Fatalf("pull failed: %d %v", status, body)
	}

	changes, _ := body["changes"].([]any)
	found := false
	for _, c := range changes {
		cm, _ := c.(map[string]any)
		if cm["id"] == pushID {
			found = true
			// Verify the hydrated data.
			data, _ := cm["data"].(map[string]any)
			if data["title"] != "E2E Test Record" {
				t.Errorf("pulled record has wrong title: %v", data["title"])
			}
			if data["user_id"] != "e2e-user" {
				t.Errorf("pulled record has wrong user_id: %v", data["user_id"])
			}
			// Verify timestamps are ISO 8601.
			if ts, ok := data["updated_at"].(string); ok {
				isUTC := strings.HasSuffix(ts, "Z") || strings.HasSuffix(ts, "+00:00")
			if !strings.Contains(ts, "T") || !isUTC {
					t.Errorf("pulled updated_at not ISO 8601 UTC: %s", ts)
				}
			}
			break
		}
	}
	if !found {
		t.Fatalf("pushed record not found in pull response. changes=%d, deletes=%v",
			len(changes), body["deletes"])
	}
}

// ---------------------------------------------------------------------------
// Unit tests (no database required)
// ---------------------------------------------------------------------------

func TestPqTextArrayEncoding(t *testing.T) {
	// Unit test for pqTextArray helper (no DB needed).
	tests := []struct {
		input    []string
		expected string
	}{
		{nil, ""},
		{[]string{}, "{}"},
		{[]string{"a", "b"}, `{"a","b"}`},
		{[]string{"has space"}, `{"has space"}`},
		{[]string{`has"quote`}, `{"has\"quote"}`},
		{[]string{`has\backslash`}, `{"has\\backslash"}`},
	}

	for _, tt := range tests {
		result := pqTextArray(tt.input)
		if tt.input == nil {
			if result != nil {
				t.Errorf("nil input: expected nil, got %v", result)
			}
			continue
		}
		str, ok := result.(string)
		if !ok {
			t.Errorf("expected string, got %T", result)
			continue
		}
		if str != tt.expected {
			t.Errorf("pqTextArray(%v) = %q, want %q", tt.input, str, tt.expected)
		}
	}
}
