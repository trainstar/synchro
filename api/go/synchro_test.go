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
	"testing"

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
