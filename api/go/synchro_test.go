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
	return testServerWithConfig(t, func(cfg *Config) {
		cfg.JWTSecret = []byte("test-secret-for-integration-tests")
	})
}

func testServerWithConfig(t *testing.T, configure func(*Config)) *httptest.Server {
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

	var extExists bool
	if err := db.QueryRow(
		"SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'synchro_pg')",
	).Scan(&extExists); err != nil || !extExists {
		t.Skip("synchro_pg extension not installed")
	}

	_, _ = db.ExecContext(context.Background(),
		"DELETE FROM sync_clients WHERE client_id LIKE 'test-%' OR client_id LIKE '%-client'")

	cfg := Config{
		DB:               db,
		MinClientVersion: "1.0.0",
	}
	if configure != nil {
		configure(&cfg)
	}

	handler := Routes(cfg)

	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)

	return srv
}

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

func connectClient(t *testing.T, srv *httptest.Server, token, clientID string) {
	t.Helper()
	status, body := doJSON(t, "POST", srv.URL+"/sync/connect", token, map[string]any{
		"client_id":         clientID,
		"platform":          "ios",
		"app_version":       "1.0.0",
		"protocol_version":  1,
		"schema":            map[string]any{"version": 0, "hash": ""},
		"scope_set_version": 0,
		"known_scopes":      map[string]any{},
	})
	if status != 200 {
		t.Fatalf("connect failed with %d: %v", status, body)
	}
}

func TestConnectPassthrough(t *testing.T) {
	srv := testServer(t)
	token := testToken("user-1")

	status, body := doJSON(t, "POST", srv.URL+"/sync/connect", token, map[string]any{
		"client_id":         "test-vnext-connect-client",
		"platform":          "ios",
		"app_version":       "1.0.0",
		"protocol_version":  1,
		"schema":            map[string]any{"version": 0, "hash": ""},
		"scope_set_version": 0,
		"known_scopes":      map[string]any{},
	})

	if status != 200 {
		t.Fatalf("expected 200, got %d: %v", status, body)
	}
	if body["protocol_version"] == nil {
		t.Error("response missing 'protocol_version'")
	}
	if body["schema"] == nil {
		t.Error("response missing 'schema'")
	}
	if body["scopes"] == nil {
		t.Error("response missing 'scopes'")
	}
}

func TestConnectPassthroughTrustedUpstreamAuth(t *testing.T) {
	srv := testServerWithConfig(t, func(cfg *Config) {
		cfg.UserIDResolver = func(r *http.Request) (string, error) {
			return "user-1", nil
		}
	})

	status, body := doJSON(t, "POST", srv.URL+"/sync/connect", "", map[string]any{
		"client_id":         "test-vnext-connect-upstream-client",
		"platform":          "ios",
		"app_version":       "1.0.0",
		"protocol_version":  1,
		"schema":            map[string]any{"version": 0, "hash": ""},
		"scope_set_version": 0,
		"known_scopes":      map[string]any{},
	})

	if status != 200 {
		t.Fatalf("expected 200, got %d: %v", status, body)
	}
	if body["protocol_version"] == nil {
		t.Error("response missing 'protocol_version'")
	}
}

func TestConnectUpgradeRequired426(t *testing.T) {
	srv := testServer(t)
	token := testToken("user-1")

	status, body := doJSON(t, "POST", srv.URL+"/sync/connect", token, map[string]any{
		"client_id":         "test-vnext-upgrade-client",
		"platform":          "ios",
		"app_version":       "1.0.0",
		"protocol_version":  99,
		"schema":            map[string]any{"version": 0, "hash": ""},
		"scope_set_version": 0,
		"known_scopes":      map[string]any{},
	})

	if status != http.StatusUpgradeRequired {
		t.Fatalf("expected 426, got %d: %v", status, body)
	}

	errBody, ok := body["error"].(map[string]any)
	if !ok {
		t.Fatalf("expected nested error object, got %v", body["error"])
	}
	if errBody["code"] != "upgrade_required" {
		t.Errorf("expected error.code=upgrade_required, got %v", errBody["code"])
	}
}

func TestPullPassthrough(t *testing.T) {
	srv := testServer(t)
	token := testToken("user-1")
	connectClient(t, srv, token, "test-vnext-pull-client")

	status, body := doJSON(t, "POST", srv.URL+"/sync/pull", token, map[string]any{
		"client_id":         "test-vnext-pull-client",
		"schema":            map[string]any{"version": 0, "hash": ""},
		"scope_set_version": 0,
		"scopes":            map[string]any{},
		"limit":             100,
		"checksum_mode":     "requested",
	})

	if status != 200 {
		t.Fatalf("expected 200, got %d: %v", status, body)
	}
	for _, field := range []string{"changes", "scope_set_version", "scope_cursors", "scope_updates", "rebuild", "has_more"} {
		if body[field] == nil {
			t.Errorf("response missing '%s'", field)
		}
	}
}

func TestPushPassthrough(t *testing.T) {
	srv := testServer(t)
	token := testToken("user-1")
	connectClient(t, srv, token, "test-vnext-push-client")

	status, body := doJSON(t, "POST", srv.URL+"/sync/push", token, map[string]any{
		"client_id": "test-vnext-push-client",
		"batch_id":  "batch-1",
		"schema":    map[string]any{"version": 0, "hash": ""},
		"mutations": []map[string]any{},
	})

	if status != 200 {
		t.Fatalf("expected 200, got %d: %v", status, body)
	}
	for _, field := range []string{"accepted", "rejected", "server_time"} {
		if body[field] == nil {
			t.Errorf("response missing '%s'", field)
		}
	}
}

func TestRebuildPassthrough(t *testing.T) {
	srv := testServer(t)
	token := testToken("user-1")
	connectClient(t, srv, token, "test-vnext-rebuild-client")

	status, body := doJSON(t, "POST", srv.URL+"/sync/rebuild", token, map[string]any{
		"client_id": "test-vnext-rebuild-client",
		"scope":     "user:user-1",
		"limit":     100,
	})

	if status != 200 {
		t.Fatalf("expected 200, got %d: %v", status, body)
	}
	for _, field := range []string{"scope", "records", "has_more"} {
		if body[field] == nil {
			t.Errorf("response missing '%s'", field)
		}
	}
}

func TestSchemaNoAuth(t *testing.T) {
	srv := testServer(t)

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

	status, _ := doJSON(t, "GET", srv.URL+"/sync/debug?client_id=test", "", nil)
	if status != 401 {
		t.Errorf("expected 401, got %d", status)
	}
}

func TestTrustedUpstreamAuthRequiresUser(t *testing.T) {
	srv := testServerWithConfig(t, func(cfg *Config) {
		cfg.UserIDResolver = func(r *http.Request) (string, error) {
			return "", ErrAuthRequired
		}
	})

	status, body := doJSON(t, "POST", srv.URL+"/sync/connect", "", map[string]any{
		"client_id":         "test-vnext-connect-missing-upstream-user",
		"platform":          "ios",
		"app_version":       "1.0.0",
		"protocol_version":  1,
		"schema":            map[string]any{"version": 0, "hash": ""},
		"scope_set_version": 0,
		"known_scopes":      map[string]any{},
	})

	if status != 401 {
		t.Fatalf("expected 401, got %d: %v", status, body)
	}
}

func TestRequestContextUserIDResolver(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/sync/connect", nil)

	_, err := RequestContextUserIDResolver(req)
	if err == nil || err != ErrAuthRequired {
		t.Fatalf("expected ErrAuthRequired, got %v", err)
	}

	req = req.WithContext(WithUserID(req.Context(), "USER-1"))
	userID, err := RequestContextUserIDResolver(req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if userID != "user-1" {
		t.Fatalf("expected normalized user ID, got %q", userID)
	}
}

func TestRoutesPanicsOnMixedAuthModes(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic for mixed auth configuration")
		}
	}()

	_ = Routes(Config{
		DB: &sql.DB{},
		UserIDResolver: func(r *http.Request) (string, error) {
			return "user-1", nil
		},
		JWTSecret: []byte("test-secret-for-integration-tests"),
	})
}

func TestSchemaMismatch422Body(t *testing.T) {
	dbURL := os.Getenv("TEST_DATABASE_URL")
	if dbURL == "" {
		t.Skip("TEST_DATABASE_URL not set")
	}

	db, err := sql.Open("pgx", dbURL)
	if err != nil {
		t.Fatalf("opening database: %v", err)
	}
	defer db.Close()

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
	connectClient(t, srv, token, "mismatch-client")

	status, body := doJSON(t, "POST", srv.URL+"/sync/push", token, map[string]any{
		"client_id": "mismatch-client",
		"batch_id":  "mismatch-batch",
		"schema":    map[string]any{"version": 999, "hash": "definitely_wrong_hash"},
		"mutations": []map[string]any{},
	})

	if status != http.StatusUnprocessableEntity {
		t.Fatalf("expected 422, got %d: %v", status, body)
	}

	errBody, ok := body["error"].(map[string]any)
	if !ok {
		t.Fatalf("expected nested error object, got %v", body["error"])
	}
	if errBody["code"] != "schema_mismatch" {
		t.Errorf("expected error.code=schema_mismatch, got %v", errBody["code"])
	}
}

func TestSQLError503(t *testing.T) {
	dbURL := os.Getenv("TEST_DATABASE_URL")
	if dbURL == "" {
		t.Skip("TEST_DATABASE_URL not set")
	}

	db, err := sql.Open("pgx", dbURL)
	if err != nil {
		t.Fatalf("opening database: %v", err)
	}
	_ = db.Close()

	handler := Routes(Config{
		DB:        db,
		JWTSecret: []byte("test-secret-for-integration-tests"),
	})
	srv := httptest.NewServer(handler)
	defer srv.Close()

	status, body := doJSON(t, "GET", srv.URL+"/sync/schema", "", nil)
	if status != 500 && status != 503 {
		t.Errorf("expected 500 or 503, got %d: %v", status, body)
	}
}

func TestMapPGErrorVNextStatusMapping(t *testing.T) {
	tests := []struct {
		name           string
		raw            string
		wantStatus     int
		wantRetryAfter string
		wantHandled    bool
	}{
		{
			name:        "upgrade required",
			raw:         `{"error":{"code":"upgrade_required","message":"unsupported protocol version","retryable":false}}`,
			wantStatus:  http.StatusUpgradeRequired,
			wantHandled: true,
		},
		{
			name:        "schema mismatch",
			raw:         `{"error":{"code":"schema_mismatch","message":"schema mismatch","retryable":false}}`,
			wantStatus:  http.StatusUnprocessableEntity,
			wantHandled: true,
		},
		{
			name:           "retry later",
			raw:            `{"error":{"code":"retry_later","message":"slow down","retryable":true}}`,
			wantStatus:     http.StatusTooManyRequests,
			wantRetryAfter: "5",
			wantHandled:    true,
		},
		{
			name:        "success payload ignored",
			raw:         `{"ok":true}`,
			wantHandled: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := httptest.NewRecorder()
			handled := mapPGError(w, []byte(tt.raw))
			if handled != tt.wantHandled {
				t.Fatalf("handled = %v, want %v", handled, tt.wantHandled)
			}
			if !tt.wantHandled {
				return
			}
			if w.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d", w.Code, tt.wantStatus)
			}
			if got := w.Header().Get("Retry-After"); got != tt.wantRetryAfter {
				t.Fatalf("Retry-After = %q, want %q", got, tt.wantRetryAfter)
			}
		})
	}
}
