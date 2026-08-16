package synchroapi

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/sha512"
	"database/sql"
	"database/sql/driver"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
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

	if err := RequireCompatibleExtension(context.Background(), db); err != nil {
		t.Fatalf("verifying compatible synchro_pg extension: %v", err)
	}

	_, _ = db.ExecContext(context.Background(),
		"DELETE FROM synchro.sync_clients WHERE client_id LIKE 'test-%' OR client_id LIKE '%-client'")

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
	return testTokenHS256(userID, []byte("test-secret-for-integration-tests"))
}

func testTokenHS256(userID string, secret []byte) string {
	header := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"HS256","typ":"JWT"}`))
	payload := base64.RawURLEncoding.EncodeToString(
		[]byte(fmt.Sprintf(`{"sub":"%s","iat":1700000000,"exp":9999999999}`, userID)),
	)
	sigInput := header + "." + payload
	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(sigInput))
	sig := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	return sigInput + "." + sig
}

func testTokenHS384(userID string, secret []byte) string {
	header := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"HS384","typ":"JWT"}`))
	payload := base64.RawURLEncoding.EncodeToString(
		[]byte(fmt.Sprintf(`{"sub":"%s","iat":1700000000,"exp":9999999999}`, userID)),
	)
	sigInput := header + "." + payload
	mac := hmac.New(sha512.New384, secret)
	mac.Write([]byte(sigInput))
	sig := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	return sigInput + "." + sig
}

func TestParseSemver(t *testing.T) {
	valid := []string{
		"0.0.0",
		"1.2.3-alpha.1",
		"1.2.3+build.7",
		"1.2.3-rc.1+build.7",
		"123456789012345678901234567890.0.0",
	}
	for _, version := range valid {
		if _, err := parseSemver(version); err != nil {
			t.Errorf("parseSemver(%q) returned error: %v", version, err)
		}
	}

	invalid := []string{
		"v1.2.3",
		" 1.2.3",
		"1.2.3 ",
		"+1.2.3",
		"1.2",
		"1.2.3.4",
		"01.2.3",
		"1.02.3",
		"1.2.03",
		"1.2.3-01",
		"1.2.3-alpha..1",
		"1.2.3-α",
		"1.2.3+",
		"1.2.3-+build",
	}
	for _, version := range invalid {
		if _, err := parseSemver(version); err == nil {
			t.Errorf("parseSemver(%q) succeeded", version)
		}
	}
}

func TestSemverPrecedence(t *testing.T) {
	tests := []struct {
		lower  string
		higher string
	}{
		{"1.0.0-alpha", "1.0.0-alpha.1"},
		{"1.0.0-alpha.1", "1.0.0-alpha.beta"},
		{"1.0.0-alpha.beta", "1.0.0-beta"},
		{"1.0.0-beta", "1.0.0-beta.2"},
		{"1.0.0-beta.2", "1.0.0-beta.11"},
		{"1.0.0-beta.11", "1.0.0-rc.1"},
		{"1.0.0-rc.1", "1.0.0"},
		{"999999999999999999999999999999.0.0", "1000000000000000000000000000000.0.0"},
	}
	for _, tt := range tests {
		lower, err := parseSemver(tt.lower)
		if err != nil {
			t.Fatalf("parseSemver(%q): %v", tt.lower, err)
		}
		higher, err := parseSemver(tt.higher)
		if err != nil {
			t.Fatalf("parseSemver(%q): %v", tt.higher, err)
		}
		if !lower.lessThan(higher) {
			t.Errorf("%q should precede %q", tt.lower, tt.higher)
		}
		if higher.lessThan(lower) {
			t.Errorf("%q should not precede %q", tt.higher, tt.lower)
		}
	}

	for _, versions := range [][2]string{{"1.0.0+one", "1.0.0+two"}} {
		left, err := parseSemver(versions[0])
		if err != nil {
			t.Fatalf("parseSemver(%q): %v", versions[0], err)
		}
		right, err := parseSemver(versions[1])
		if err != nil {
			t.Fatalf("parseSemver(%q): %v", versions[1], err)
		}
		if left.lessThan(right) || right.lessThan(left) {
			t.Errorf("build metadata must not affect precedence: %q and %q", versions[0], versions[1])
		}
	}
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
	req.Header.Set("X-Client-Version", "1.0.0")
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

func doRawJSON(t *testing.T, method, url, token string, raw string) (int, map[string]any) {
	t.Helper()
	req, err := http.NewRequest(method, url, bytes.NewBufferString(raw))
	if err != nil {
		t.Fatalf("creating request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Client-Version", "1.0.0")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("sending request: %v", err)
	}
	defer resp.Body.Close()
	rawResp, _ := io.ReadAll(resp.Body)
	var result map[string]any
	_ = json.Unmarshal(rawResp, &result)
	return resp.StatusCode, result
}

type connectedClient struct {
	Generation      int64
	Schema          map[string]any
	ScopeSetVersion int64
	Scopes          map[string]any
}

func connectClient(t *testing.T, srv *httptest.Server, token, clientID string) connectedClient {
	t.Helper()
	status, body := doJSON(t, "POST", srv.URL+"/sync/connect", token, map[string]any{
		"client_id":         clientID,
		"platform":          "ios",
		"app_version":       "1.0.0",
		"protocol_version":  ExpectedProtocolVersion,
		"schema":            map[string]any{"version": 0, "hash": ""},
		"scope_set_version": 0,
		"known_scopes":      map[string]any{},
	})
	if status != 200 {
		t.Fatalf("connect failed with %d: %v", status, body)
	}
	generation, ok := body["client_generation"].(float64)
	if !ok || generation <= 0 {
		t.Fatalf("connect returned invalid client_generation: %v", body["client_generation"])
	}
	schema, ok := body["schema"].(map[string]any)
	if !ok {
		t.Fatalf("connect returned invalid schema: %T", body["schema"])
	}
	delete(schema, "action")
	delete(schema, "reason")
	scopeSetVersion, ok := body["scope_set_version"].(float64)
	if !ok || scopeSetVersion < 0 {
		t.Fatalf("connect returned invalid scope_set_version: %v", body["scope_set_version"])
	}
	scopes := make(map[string]any)
	if delta, ok := body["scopes"].(map[string]any); ok {
		if additions, ok := delta["add"].([]any); ok {
			for _, raw := range additions {
				assignment, ok := raw.(map[string]any)
				if !ok {
					continue
				}
				id, _ := assignment["id"].(string)
				if id != "" {
					scopes[id] = map[string]any{"cursor": assignment["cursor"]}
				}
			}
		}
	}
	return connectedClient{
		Generation:      int64(generation),
		Schema:          schema,
		ScopeSetVersion: int64(scopeSetVersion),
		Scopes:          scopes,
	}
}

func waitForRegisteredTable(t *testing.T, db *sql.DB, tableName string) {
	t.Helper()
	if waitForRegisteredTableState(db, tableName, true) {
		return
	}
	t.Fatalf("registered table %q did not activate", tableName)
}

func waitForRegisteredTableState(db *sql.DB, tableName string, expected bool) bool {
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		var active bool
		err := db.QueryRow(`
			SELECT EXISTS (
				SELECT 1
				FROM synchro.sync_registry r
				JOIN synchro.sync_registry_generations g ON g.generation = r.registry_generation
				WHERE g.state = 'active' AND r.table_name = $1
			)
		`, tableName).Scan(&active)
		if err == nil && active == expected {
			return true
		}
		time.Sleep(25 * time.Millisecond)
	}
	return false
}

func TestConnectPassthrough(t *testing.T) {
	srv := testServer(t)
	token := testToken("user-1")

	status, body := doJSON(t, "POST", srv.URL+"/sync/connect", token, map[string]any{
		"client_id":         "test-canonical-connect-client",
		"platform":          "ios",
		"app_version":       "1.0.0",
		"protocol_version":  ExpectedProtocolVersion,
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

func TestRequireCompatibleExtension(t *testing.T) {
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

	if err := RequireCompatibleExtension(context.Background(), db); err != nil {
		t.Fatalf("expected compatible synchro_pg extension, got %v", err)
	}
}

func TestConnectPassthroughTrustedUpstreamAuth(t *testing.T) {
	srv := testServerWithConfig(t, func(cfg *Config) {
		cfg.UserIDResolver = func(r *http.Request) (string, error) {
			return "user-1", nil
		}
	})

	status, body := doJSON(t, "POST", srv.URL+"/sync/connect", "", map[string]any{
		"client_id":         "test-canonical-connect-upstream-client",
		"platform":          "ios",
		"app_version":       "1.0.0",
		"protocol_version":  ExpectedProtocolVersion,
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
		"client_id":         "test-canonical-upgrade-client",
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
	client := connectClient(t, srv, token, "test-canonical-pull-client")

	status, body := doJSON(t, "POST", srv.URL+"/sync/pull", token, map[string]any{
		"client_id":         "test-canonical-pull-client",
		"client_generation": client.Generation,
		"schema":            client.Schema,
		"scope_set_version": client.ScopeSetVersion,
		"scopes":            client.Scopes,
		"limit":             100,
	})

	if status != 200 {
		t.Fatalf("expected 200, got %d: %v", status, body)
	}
	for _, field := range []string{"changes", "scope_set_version", "scope_cursors", "scope_updates", "rebuild", "has_more"} {
		if body[field] == nil {
			t.Errorf("response missing '%s'", field)
		}
	}
	if _, ok := body["checksums"]; !ok {
		t.Error("response missing 'checksums'")
	}
}

func TestPushRejectsEmptyBatch(t *testing.T) {
	srv := testServer(t)
	token := testToken("user-1")
	client := connectClient(t, srv, token, "test-canonical-push-client")

	status, body := doJSON(t, "POST", srv.URL+"/sync/push", token, map[string]any{
		"client_id":         "test-canonical-push-client",
		"client_generation": client.Generation,
		"batch_id":          "018f2b5e-7c42-7a1d-9d31-8a95bd674001",
		"schema":            client.Schema,
		"mutations":         []map[string]any{},
	})

	if status != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %v", status, body)
	}
	errorBody, ok := body["error"].(map[string]any)
	if !ok || errorBody["code"] != "invalid_request" {
		t.Fatalf("expected invalid_request, got %v", body)
	}
}

func TestRebuildPassthrough(t *testing.T) {
	srv := testServer(t)
	token := testToken("user-1")
	client := connectClient(t, srv, token, "test-canonical-rebuild-client")

	status, body := doJSON(t, "POST", srv.URL+"/sync/rebuild", token, map[string]any{
		"client_id":         "test-canonical-rebuild-client",
		"client_generation": client.Generation,
		"schema":            client.Schema,
		"scope":             "user:user-1",
		"rebuild_id":        "018f2b5e-7c42-7a1d-9d31-8a95bd674101",
		"cursor":            nil,
		"limit":             100,
	})

	if status != 200 {
		t.Fatalf("expected 200, got %d: %v", status, body)
	}
	for _, field := range []string{"scope", "records", "has_more", "final_scope_cursor", "checksum"} {
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
	for _, field := range []string{"schema_version", "schema_hash", "manifest"} {
		if _, ok := body[field]; !ok {
			t.Errorf("response missing %q", field)
		}
	}
	manifest, ok := body["manifest"].(map[string]any)
	if !ok {
		t.Fatalf("expected manifest object, got %T", body["manifest"])
	}
	for _, field := range []string{"schema_version", "schema_hash", "parent_schema", "transition_class", "compatibility_floor", "tables"} {
		if _, ok := manifest[field]; !ok {
			t.Errorf("manifest missing %q", field)
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

func TestInvalidRequestBodiesReturn400(t *testing.T) {
	srv := testServer(t)
	token := testToken("user-1")

	tests := []struct {
		name string
		path string
		body string
	}{
		{
			name: "connect missing client id",
			path: "/sync/connect",
			body: `{"platform":"ios","app_version":"1.0.0","protocol_version":3,"schema":{"version":0,"hash":""},"scope_set_version":0,"known_scopes":{}}`,
		},
		{
			name: "pull missing client id",
			path: "/sync/pull",
			body: `{"schema":{"version":0,"hash":""},"scope_set_version":0,"scopes":{},"limit":100}`,
		},
		{
			name: "push invalid body type",
			path: "/sync/push",
			body: `{"client_id":1,"batch_id":"batch-1","schema":{"version":0,"hash":""},"mutations":[]}`,
		},
		{
			name: "rebuild missing scope",
			path: "/sync/rebuild",
			body: `{"client_id":"test-rebuild-client","limit":100}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			status, body := doRawJSON(t, "POST", srv.URL+tt.path, token, tt.body)
			if status != http.StatusBadRequest {
				t.Fatalf("expected 400, got %d: %v", status, body)
			}
			errorBody, ok := body["error"].(map[string]any)
			if !ok {
				t.Fatalf("expected protocol error body, got %T", body["error"])
			}
			if errorBody["code"] != "invalid_request" {
				t.Fatalf("expected invalid_request, got %v", errorBody["code"])
			}
		})
	}
}

func TestPushRejectsMalformedClientVersionTimestamp(t *testing.T) {
	srv := testServer(t)
	token := testToken("user-1")
	client := connectClient(t, srv, token, "test-invalid-timestamp-client")

	status, response := doJSON(t, "POST", srv.URL+"/sync/push", token, map[string]any{
		"client_id":         "test-invalid-timestamp-client",
		"client_generation": client.Generation,
		"batch_id":          "018f2b5e-7c42-7a1d-9d31-8a95bd674201",
		"schema":            client.Schema,
		"mutations": []map[string]any{
			{
				"mutation_id":     "018f2b5e-7c42-7a1d-9d31-8a95bd674202",
				"table":           "tbl_orders",
				"op":              "update",
				"pk":              map[string]any{"fld_orders_id": "00000000-0000-0000-0000-000000000001"},
				"authored_schema": client.Schema,
				"base_version":    "opaque-version",
				"client_version":  "not-a-timestamp",
				"columns":         map[string]any{"fld_ship_address": "bad"},
			},
		},
	})
	if status != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %v", status, response)
	}

	errBody, ok := response["error"].(map[string]any)
	if !ok {
		t.Fatalf("expected nested error object, got %T", response["error"])
	}
	if errBody["code"] != "invalid_request" {
		t.Fatalf("expected error.code=invalid_request, got %v", errBody["code"])
	}
}

func TestTrustedUpstreamAuthRequiresUser(t *testing.T) {
	srv := testServerWithConfig(t, func(cfg *Config) {
		cfg.UserIDResolver = func(r *http.Request) (string, error) {
			return "", ErrAuthRequired
		}
	})

	status, body := doJSON(t, "POST", srv.URL+"/sync/connect", "", map[string]any{
		"client_id":         "test-canonical-connect-missing-upstream-user",
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

func TestRoutesAuthenticateBeforeVersionGate(t *testing.T) {
	handler := Routes(Config{
		DB:               &sql.DB{},
		JWTSecret:        []byte("test-secret"),
		MinClientVersion: "1.0.0",
	})

	for _, test := range []struct {
		name    string
		version string
	}{
		{name: "missing version"},
		{name: "malformed version", version: "not-semver"},
	} {
		t.Run(test.name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodPost, "/sync/connect", nil)
			if test.version != "" {
				request.Header.Set("X-Client-Version", test.version)
			}
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			if response.Code != http.StatusUnauthorized {
				t.Fatalf("status = %d, want %d", response.Code, http.StatusUnauthorized)
			}
		})
	}
}

func TestJWTRejectsHS384Token(t *testing.T) {
	secret := []byte("test-secret")
	called := false
	handler := jwtMiddleware(Config{JWTSecret: secret}, http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		called = true
	}))
	request := httptest.NewRequest(http.MethodPost, "/sync/connect", nil)
	request.Header.Set("Authorization", "Bearer "+testTokenHS384("user", secret))
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, request)

	if response.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusUnauthorized)
	}
	if called {
		t.Fatal("HS384 token reached the protected handler")
	}
}

func TestJWTRejectsDuplicateAuthorizationHeaders(t *testing.T) {
	secret := []byte("test-secret")
	called := false
	handler := jwtMiddleware(Config{JWTSecret: secret}, http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		called = true
	}))
	request := httptest.NewRequest(http.MethodPost, "/sync/connect", nil)
	request.Header.Add("Authorization", "Bearer "+testTokenHS256("first-user", secret))
	request.Header.Add("Authorization", "Bearer "+testTokenHS256("second-user", secret))
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, request)

	if response.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusUnauthorized)
	}
	if called {
		t.Fatal("duplicate authorization headers reached the protected handler")
	}
}

func TestRoutesRejectNegativeDatabaseQueryTimeout(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("Routes accepted a negative database query timeout")
		}
	}()
	_ = Routes(Config{
		DB:                   &sql.DB{},
		JWTSecret:            []byte("test-secret"),
		DatabaseQueryTimeout: -time.Second,
	})
}

func TestRoutesRequireJWKSContext(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("Routes did not require JWKSContext")
		}
	}()
	_ = Routes(Config{DB: &sql.DB{}, JWKSURL: "https://keys.example.test/jwks"})
}

func TestJWKSKeyLookupUsesRequestContext(t *testing.T) {
	var requests atomic.Int64
	jwksServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"keys":[]}`))
	}))
	defer jwksServer.Close()

	lifecycle, cancelLifecycle := context.WithCancel(context.Background())
	defer cancelLifecycle()
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate RSA key: %v", err)
	}
	token := jwt.NewWithClaims(jwt.SigningMethodRS256, jwt.RegisteredClaims{
		Subject:   "user",
		ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour)),
	})
	token.Header["kid"] = "unknown"
	tokenString, err := token.SignedString(privateKey)
	if err != nil {
		t.Fatalf("sign JWT: %v", err)
	}

	handler := jwtMiddleware(Config{
		JWKSURL:     jwksServer.URL,
		JWKSContext: lifecycle,
	}, http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("unknown key token reached the protected handler")
	}))
	if got := requests.Load(); got != 1 {
		t.Fatalf("initial JWKS requests = %d, want 1", got)
	}

	requestContext, cancelRequest := context.WithCancel(context.Background())
	cancelRequest()
	request := httptest.NewRequest(http.MethodPost, "/sync/connect", nil).WithContext(requestContext)
	request.Header.Set("Authorization", "Bearer "+tokenString)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)

	if response.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusUnauthorized)
	}
	if got := requests.Load(); got != 1 {
		t.Fatalf("JWKS requests after canceled lookup = %d, want 1", got)
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
	if userID != "USER-1" {
		t.Fatalf("expected canonical resolver user ID, got %q", userID)
	}
}

func TestRoutesPanicsOnMixedAuthModes(t *testing.T) {
	resolver := func(r *http.Request) (string, error) {
		return "user-1", nil
	}
	tests := []struct {
		name string
		cfg  Config
	}{
		{
			name: "resolver and secret",
			cfg:  Config{UserIDResolver: resolver, JWTSecret: []byte("test-secret")},
		},
		{
			name: "resolver and JWKS",
			cfg:  Config{UserIDResolver: resolver, JWKSURL: "https://example.invalid/jwks"},
		},
		{
			name: "secret and JWKS",
			cfg:  Config{JWTSecret: []byte("test-secret"), JWKSURL: "https://example.invalid/jwks"},
		},
		{
			name: "all modes",
			cfg: Config{
				UserIDResolver: resolver,
				JWTSecret:      []byte("test-secret"),
				JWKSURL:        "https://example.invalid/jwks",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.cfg.DB = &sql.DB{}
			defer func() {
				if recover() == nil {
					t.Fatal("expected panic for mixed auth configuration")
				}
			}()
			_ = Routes(tt.cfg)
		})
	}
}

func TestVersionCheckRequiresExactlyOneSupportedHeader(t *testing.T) {
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})
	tests := []struct {
		name       string
		headers    http.Header
		wantStatus int
	}{
		{name: "missing", headers: http.Header{}, wantStatus: http.StatusUpgradeRequired},
		{name: "client header", headers: http.Header{"X-Client-Version": {"1.0.0"}}, wantStatus: http.StatusNoContent},
		{name: "app header", headers: http.Header{"X-App-Version": {"1.1.0"}}, wantStatus: http.StatusNoContent},
		{
			name: "conflicting headers",
			headers: http.Header{
				"X-Client-Version": {"1.0.0"},
				"X-App-Version":    {"2.0.0"},
			},
			wantStatus: http.StatusUpgradeRequired,
		},
		{name: "duplicate header", headers: http.Header{"X-Client-Version": {"1.0.0", "1.0.0"}}, wantStatus: http.StatusUpgradeRequired},
		{name: "invalid", headers: http.Header{"X-Client-Version": {"not-semver"}}, wantStatus: http.StatusUpgradeRequired},
		{name: "below minimum", headers: http.Header{"X-Client-Version": {"0.9.9"}}, wantStatus: http.StatusUpgradeRequired},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodPost, "/sync/connect", nil)
			request.Header = tt.headers.Clone()
			response := httptest.NewRecorder()
			versionCheckMiddleware("1.0.0", next).ServeHTTP(response, request)
			if response.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d", response.Code, tt.wantStatus)
			}
		})
	}
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
	t.Cleanup(func() { _ = db.Close() })

	if _, err := db.Exec("CREATE TABLE IF NOT EXISTS test_mismatch_tbl (id UUID PRIMARY KEY DEFAULT gen_random_uuid(), name TEXT, updated_at TIMESTAMPTZ DEFAULT now(), deleted_at TIMESTAMPTZ)"); err != nil {
		t.Fatalf("creating mismatch table: %v", err)
	}
	if _, err := db.Exec(`
		CREATE OR REPLACE FUNCTION public.test_mismatch_membership(p_id uuid)
		RETURNS SETOF text
		LANGUAGE SQL STABLE SECURITY INVOKER
		SET search_path = pg_catalog, synchro
		BEGIN ATOMIC
			SELECT 'global'::text WHERE p_id IS NOT NULL;
		END;
		REVOKE ALL ON FUNCTION public.test_mismatch_membership(uuid) FROM PUBLIC;
		GRANT EXECUTE ON FUNCTION public.test_mismatch_membership(uuid) TO synchro_owner, synchro_worker;
		GRANT USAGE ON SCHEMA public TO synchro_owner, synchro_worker;
		GRANT SELECT ON TABLE public.test_mismatch_tbl TO synchro_owner;
		GRANT SELECT ON TABLE public.test_mismatch_tbl TO synchro_worker;
		ALTER TABLE public.test_mismatch_tbl ENABLE ROW LEVEL SECURITY;
		CREATE POLICY synchro_owner_all ON public.test_mismatch_tbl
			AS PERMISSIVE FOR ALL TO synchro_owner USING (true) WITH CHECK (true)
		;
		CREATE POLICY synchro_worker_select ON public.test_mismatch_tbl
			AS PERMISSIVE FOR SELECT TO synchro_worker USING (true)
	`); err != nil {
		t.Fatalf("creating mismatch membership function: %v", err)
	}
	if _, err := db.Exec("SELECT synchro.synchro_register_table('public.test_mismatch_tbl', 'public.test_mismatch_membership', 'single_scope', 'id', 'updated_at', 'deleted_at', 'read_only')"); err != nil {
		t.Fatalf("registering mismatch table: %v", err)
	}
	waitForRegisteredTable(t, db, "test_mismatch_tbl")
	t.Cleanup(func() {
		_, _ = db.Exec("SELECT synchro.synchro_unregister_table('test_mismatch_tbl')")
		if !waitForRegisteredTableState(db, "test_mismatch_tbl", false) {
			t.Errorf("registered table %q did not deactivate", "test_mismatch_tbl")
			return
		}
		_, _ = db.Exec("DROP FUNCTION IF EXISTS public.test_mismatch_membership(uuid)")
		_, _ = db.Exec("DROP TABLE IF EXISTS test_mismatch_tbl")
	})

	handler := Routes(Config{
		DB:        db,
		JWTSecret: []byte("test-secret-for-integration-tests"),
	})
	srv := httptest.NewServer(handler)
	defer srv.Close()

	token := testToken("user-1")
	client := connectClient(t, srv, token, "mismatch-client")

	status, body := doJSON(t, "POST", srv.URL+"/sync/push", token, map[string]any{
		"client_id":         "mismatch-client",
		"client_generation": client.Generation,
		"batch_id":          "018f2b5e-7c42-7a1d-9d31-8a95bd674301",
		"schema":            map[string]any{"version": 999, "hash": "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"},
		"mutations": []map[string]any{
			{
				"mutation_id":     "018f2b5e-7c42-7a1d-9d31-8a95bd674302",
				"table":           "tbl_unknown",
				"pk":              map[string]any{"fld_unknown_id": "row-1"},
				"authored_schema": client.Schema,
				"op":              "insert",
				"client_version":  "2026-08-14T00:00:00.000000Z",
				"columns":         map[string]any{"fld_unknown_value": "value"},
			},
		},
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

func TestPullSchemaMismatch422Body(t *testing.T) {
	dbURL := os.Getenv("TEST_DATABASE_URL")
	if dbURL == "" {
		t.Skip("TEST_DATABASE_URL not set")
	}

	db, err := sql.Open("pgx", dbURL)
	if err != nil {
		t.Fatalf("opening database: %v", err)
	}
	defer db.Close()

	handler := Routes(Config{
		DB:        db,
		JWTSecret: []byte("test-secret-for-integration-tests"),
	})
	srv := httptest.NewServer(handler)
	defer srv.Close()

	token := testToken("user-1")
	client := connectClient(t, srv, token, "pull-mismatch-client")

	status, body := doJSON(t, "POST", srv.URL+"/sync/pull", token, map[string]any{
		"client_id":         "pull-mismatch-client",
		"client_generation": client.Generation,
		"schema":            map[string]any{"version": 999, "hash": "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"},
		"scope_set_version": client.ScopeSetVersion,
		"scopes":            client.Scopes,
		"limit":             100,
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

func TestRebuildUnsubscribedScopeReturns400(t *testing.T) {
	srv := testServer(t)
	token := testToken("user-1")
	client := connectClient(t, srv, token, "rebuild-unsubscribed-client")

	status, body := doJSON(t, "POST", srv.URL+"/sync/rebuild", token, map[string]any{
		"client_id":         "rebuild-unsubscribed-client",
		"client_generation": client.Generation,
		"schema":            client.Schema,
		"scope":             "team:other",
		"rebuild_id":        "018f2b5e-7c42-7a1d-9d31-8a95bd674401",
		"cursor":            nil,
		"limit":             100,
	})

	if status != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %v", status, body)
	}

	errBody, ok := body["error"].(map[string]any)
	if !ok {
		t.Fatalf("expected nested error object, got %v", body["error"])
	}
	if errBody["code"] != "invalid_request" {
		t.Errorf("expected error.code=invalid_request, got %v", errBody["code"])
	}
}

func TestClosedDatabaseError500(t *testing.T) {
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
	if status != http.StatusInternalServerError {
		t.Errorf("expected 500, got %d: %v", status, body)
	}
}

func TestTablesClosedDatabaseError500(t *testing.T) {
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

	status, body := doJSON(t, "GET", srv.URL+"/sync/tables", "", nil)
	if status != http.StatusInternalServerError {
		t.Errorf("expected 500, got %d: %v", status, body)
	}
}

var readinessDriverSequence atomic.Uint64

type readinessDriverState struct {
	mu      sync.Mutex
	query   string
	queries int
	result  driver.Value
	err     error
}

type readinessDriver struct {
	state *readinessDriverState
}

func (d readinessDriver) Open(string) (driver.Conn, error) {
	return readinessConn{state: d.state}, nil
}

type readinessConn struct {
	state *readinessDriverState
}

func (c readinessConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("prepare is not supported")
}

func (c readinessConn) Close() error {
	return nil
}

func (c readinessConn) Begin() (driver.Tx, error) {
	return nil, errors.New("transactions are not supported")
}

func (c readinessConn) QueryContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Rows, error) {
	c.state.mu.Lock()
	defer c.state.mu.Unlock()
	c.state.queries++
	c.state.query = query
	if c.state.err != nil {
		return nil, c.state.err
	}
	return &readinessRows{result: c.state.result}, nil
}

type readinessRows struct {
	result driver.Value
	done   bool
}

func (r *readinessRows) Columns() []string {
	return []string{"synchro_readiness"}
}

func (r *readinessRows) Close() error {
	return nil
}

func (r *readinessRows) Next(values []driver.Value) error {
	if r.done {
		return io.EOF
	}
	r.done = true
	values[0] = r.result
	return nil
}

func newReadinessTestDB(t *testing.T, result []byte, queryErr error) (*sql.DB, *readinessDriverState) {
	t.Helper()
	state := &readinessDriverState{result: result, err: queryErr}
	driverName := fmt.Sprintf("synchro-readiness-%d", readinessDriverSequence.Add(1))
	sql.Register(driverName, readinessDriver{state: state})
	db, err := sql.Open(driverName, "")
	if err != nil {
		t.Fatalf("open readiness database: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db, state
}

func TestReadinessIsPublicBoundedAndFailClosed(t *testing.T) {
	tests := []struct {
		name       string
		result     string
		queryErr   error
		wantStatus int
		wantBody   string
	}{
		{name: "ready", result: `{"ready":true}`, wantStatus: http.StatusOK, wantBody: `{"ready":true}`},
		{name: "unready", result: `{"ready":false}`, wantStatus: http.StatusServiceUnavailable, wantBody: `{"ready":false}`},
		{name: "missing state", result: `{}`, wantStatus: http.StatusServiceUnavailable, wantBody: `{"ready":false}`},
		{name: "malformed", result: `{"ready":"yes"}`, wantStatus: http.StatusServiceUnavailable, wantBody: `{"ready":false}`},
		{name: "extra state", result: `{"ready":true,"detail":"secret"}`, wantStatus: http.StatusServiceUnavailable, wantBody: `{"ready":false}`},
		{name: "duplicate state", result: `{"ready":true,"ready":false}`, wantStatus: http.StatusServiceUnavailable, wantBody: `{"ready":false}`},
		{name: "SQL error", queryErr: errors.New("private database state"), wantStatus: http.StatusServiceUnavailable, wantBody: `{"ready":false}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, state := newReadinessTestDB(t, []byte(tt.result), tt.queryErr)
			handler := Routes(Config{DB: db, JWTSecret: []byte("unused-secret")})
			request := httptest.NewRequest(http.MethodGet, "/ready", nil)
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)

			if response.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d", response.Code, tt.wantStatus)
			}
			if response.Body.String() != tt.wantBody {
				t.Fatalf("body = %q, want %q", response.Body.String(), tt.wantBody)
			}
			state.mu.Lock()
			defer state.mu.Unlock()
			if state.queries != 1 {
				t.Fatalf("query count = %d, want 1", state.queries)
			}
			if state.query != "SELECT synchro.synchro_readiness()" {
				t.Fatalf("query = %q, want canonical readiness call", state.query)
			}
		})
	}
}

func TestMapPGErrorProtocolStatusMapping(t *testing.T) {
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

func TestWriteRawJSON(t *testing.T) {
	w := httptest.NewRecorder()
	payload := []byte(`{"ok":true, "message":"raw"}
`)

	writeRawJSON(w, http.StatusAccepted, payload)

	if w.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusAccepted)
	}
	if got := w.Header().Get("Content-Type"); got != "application/json" {
		t.Fatalf("Content-Type = %q, want %q", got, "application/json")
	}
	if got := w.Header().Get("Content-Length"); got != strconv.Itoa(len(payload)) {
		t.Fatalf("Content-Length = %q, want %q", got, strconv.Itoa(len(payload)))
	}
	if got := w.Body.Bytes(); !bytes.Equal(got, payload) {
		t.Fatalf("body = %q, want %q", got, payload)
	}
}

type writeResultResponseWriter struct {
	header http.Header
	write  func([]byte) (int, error)
}

func (w *writeResultResponseWriter) Header() http.Header {
	return w.header
}

func (w *writeResultResponseWriter) WriteHeader(int) {}

func (w *writeResultResponseWriter) Write(data []byte) (int, error) {
	return w.write(data)
}

func TestWriteRawJSONLogsWriteResults(t *testing.T) {
	originalWriter := log.Writer()
	originalFlags := log.Flags()
	originalPrefix := log.Prefix()
	var logs bytes.Buffer
	log.SetOutput(&logs)
	log.SetFlags(0)
	log.SetPrefix("")
	t.Cleanup(func() {
		log.SetOutput(originalWriter)
		log.SetFlags(originalFlags)
		log.SetPrefix(originalPrefix)
	})

	tests := []struct {
		name  string
		write func([]byte) (int, error)
	}{
		{
			name: "write error",
			write: func([]byte) (int, error) {
				return 0, errors.New("write failed")
			},
		},
		{
			name: "short write",
			write: func(data []byte) (int, error) {
				return len(data) - 1, nil
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logs.Reset()
			w := &writeResultResponseWriter{
				header: make(http.Header),
				write:  tt.write,
			}

			writeRawJSON(w, http.StatusAccepted, []byte(`{"ok":true}`))

			if logs.Len() == 0 {
				t.Fatal("expected write result to be logged")
			}
		})
	}
}
