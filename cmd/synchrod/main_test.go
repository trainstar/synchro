package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"

	"github.com/trainstar/synchro"
	"github.com/trainstar/synchro/handler"
	"github.com/trainstar/synchro/synctest"
)

var testJWTSecret = []byte("test-secret-for-synchrod-tests")

func signTestJWT(userID string, secret []byte) string {
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"sub": userID,
		"iat": time.Now().Unix(),
		"exp": time.Now().Add(time.Hour).Unix(),
	})
	signed, err := token.SignedString(secret)
	if err != nil {
		panic(fmt.Sprintf("signing test JWT: %v", err))
	}
	return signed
}

func setupTestServer(t *testing.T) (*httptest.Server, *sql.DB) {
	t.Helper()

	db := synctest.TestDB(t)

	engine, err := synchro.NewEngine(context.Background(), &synchro.Config{
		DB:     db,
		Tables: synctest.NewTestTables(),
		AuthorizeWrite: synchro.Chain(
			synchro.ReadOnly("products"),
			synchro.StampColumn("user_id"),
			synchro.VerifyOwner("user_id"),
		),
		BucketFunc:       synchro.UserBucket("user_id"),
		MinClientVersion: "1.0.0",
	})
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}

	h := handler.New(engine)
	syncHandler := handler.JWTAuthMiddleware(
		handler.JWTAuthConfig{
			Secret:    testJWTSecret,
			UserClaim: "sub",
		},
		handler.VersionCheckMiddleware("X-App-Version", "1.0.0", h.Routes()),
	)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})
	mux.Handle("/sync/", syncHandler)

	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv, db
}

func TestHTTP_HealthCheck(t *testing.T) {
	srv, _ := setupTestServer(t)

	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, srv.URL+"/healthz", nil)
	if err != nil {
		t.Fatalf("creating request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET /healthz: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusOK)
	}
}

func TestHTTP_MissingAuth_401(t *testing.T) {
	srv, _ := setupTestServer(t)

	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, srv.URL+"/sync/register", bytes.NewBufferString(`{}`))
	if err != nil {
		t.Fatalf("creating request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST /sync/register: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusUnauthorized)
	}
}

func TestHTTP_InvalidJWT_401(t *testing.T) {
	srv, _ := setupTestServer(t)

	req, _ := http.NewRequestWithContext(context.Background(), http.MethodPost, srv.URL+"/sync/register", bytes.NewBufferString(`{}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer invalid-token")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusUnauthorized)
	}
}

func TestHTTP_ExpiredJWT_401(t *testing.T) {
	srv, _ := setupTestServer(t)

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"sub": "user-1",
		"iat": time.Now().Add(-2 * time.Hour).Unix(),
		"exp": time.Now().Add(-1 * time.Hour).Unix(),
	})
	expired, _ := token.SignedString(testJWTSecret)

	req, _ := http.NewRequestWithContext(context.Background(), http.MethodPost, srv.URL+"/sync/register", bytes.NewBufferString(`{}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+expired)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusUnauthorized)
	}
}

func TestHTTP_WrongSigningKey_401(t *testing.T) {
	srv, _ := setupTestServer(t)

	wrongKey := []byte("wrong-secret")
	tokenStr := signTestJWT("user-1", wrongKey)

	req, _ := http.NewRequestWithContext(context.Background(), http.MethodPost, srv.URL+"/sync/register", bytes.NewBufferString(`{}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+tokenStr)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusUnauthorized)
	}
}

func TestHTTP_UpgradeRequired_426(t *testing.T) {
	srv, _ := setupTestServer(t)

	tokenStr := signTestJWT("user-1", testJWTSecret)

	req, _ := http.NewRequestWithContext(context.Background(), http.MethodPost, srv.URL+"/sync/register", bytes.NewBufferString(`{}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+tokenStr)
	req.Header.Set("X-App-Version", "0.1.0")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusUpgradeRequired {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusUpgradeRequired)
	}
}

func TestHTTP_RegisterPushPullRoundTrip(t *testing.T) {
	srv, db := setupTestServer(t)

	userID := "00000000-0000-0000-0000-000000000001"
	clientID := "client-roundtrip"

	// --- Register ---
	regResp := doJSON(t, srv, http.MethodPost, "/sync/register", userID, "1.0.0", map[string]any{
		"client_id":      clientID,
		"platform":       "test",
		"app_version":    "1.0.0",
		"schema_version": 0,
		"schema_hash":    "",
	})
	if regResp.StatusCode != http.StatusOK {
		t.Fatalf("register status = %d, want %d", regResp.StatusCode, http.StatusOK)
	}
	var regBody map[string]any
	_ = json.NewDecoder(regResp.Body).Decode(&regBody)
	_ = regResp.Body.Close()

	schemaVersion := int64(regBody["schema_version"].(float64))
	schemaHash := regBody["schema_hash"].(string)

	// Complete initial bootstrap before validating incremental pull semantics.
	snapshotResp := doJSON(t, srv, http.MethodPost, "/sync/snapshot", userID, "1.0.0", map[string]any{
		"client_id":      clientID,
		"schema_version": schemaVersion,
		"schema_hash":    schemaHash,
		"limit":          100,
	})
	if snapshotResp.StatusCode != http.StatusOK {
		var body map[string]any
		_ = json.NewDecoder(snapshotResp.Body).Decode(&body)
		_ = snapshotResp.Body.Close()
		t.Fatalf("snapshot status = %d, want %d, body: %v", snapshotResp.StatusCode, http.StatusOK, body)
	}
	_ = snapshotResp.Body.Close()

	// --- Push create ---
	orderID := "00000000-0000-0000-0000-aaaaaaaaaaaa"
	pushResp := doJSON(t, srv, http.MethodPost, "/sync/push", userID, "1.0.0", map[string]any{
		"client_id":      clientID,
		"schema_version": schemaVersion,
		"schema_hash":    schemaHash,
		"changes": []map[string]any{
			{
				"id":                orderID,
				"table_name":        "orders",
				"operation":         "create",
				"data":              map[string]any{"id": orderID, "user_id": userID, "ship_address": "123 Main St"},
				"client_updated_at": "2026-03-07T00:00:00Z",
			},
		},
	})
	if pushResp.StatusCode != http.StatusOK {
		var body map[string]any
		_ = json.NewDecoder(pushResp.Body).Decode(&body)
		_ = pushResp.Body.Close()
		t.Fatalf("push status = %d, want %d, body: %v", pushResp.StatusCode, http.StatusOK, body)
	}
	var pushBody map[string]any
	_ = json.NewDecoder(pushResp.Body).Decode(&pushBody)
	_ = pushResp.Body.Close()

	accepted := pushBody["accepted"].([]any)
	if len(accepted) != 1 {
		t.Fatalf("accepted count = %d, want 1", len(accepted))
	}

	// --- Simulate WAL: insert changelog entry directly ---
	// The push wrote the row to `orders` via RLS. Now simulate the WAL consumer
	// by inserting a changelog entry so the pull finds it.
	_, execErr := db.ExecContext(context.Background(), `
		INSERT INTO sync_changelog (bucket_id, table_name, record_id, operation)
		VALUES ($1, $2, $3, $4)
	`, fmt.Sprintf("user:%s", userID), "orders", orderID, 1)
	if execErr != nil {
		t.Fatalf("inserting changelog: %v", execErr)
	}

	// --- Pull ---
	pullResp := doJSON(t, srv, http.MethodPost, "/sync/pull", userID, "1.0.0", map[string]any{
		"client_id":      clientID,
		"checkpoint":     0,
		"schema_version": schemaVersion,
		"schema_hash":    schemaHash,
	})
	if pullResp.StatusCode != http.StatusOK {
		var body map[string]any
		_ = json.NewDecoder(pullResp.Body).Decode(&body)
		_ = pullResp.Body.Close()
		t.Fatalf("pull status = %d, want %d, body: %v", pullResp.StatusCode, http.StatusOK, body)
	}
	var pullBody map[string]any
	_ = json.NewDecoder(pullResp.Body).Decode(&pullBody)
	_ = pullResp.Body.Close()

	changes := pullBody["changes"].([]any)
	if len(changes) == 0 {
		t.Fatal("expected at least one change in pull response")
	}

	found := false
	for _, c := range changes {
		rec := c.(map[string]any)
		if rec["id"] == orderID {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("pushed order %s not found in pull response", orderID)
	}
}

func TestHTTP_SchemaMismatch_409(t *testing.T) {
	srv, _ := setupTestServer(t)

	userID := "00000000-0000-0000-0000-000000000002"
	clientID := "client-schema-mismatch"

	// Register first
	regResp := doJSON(t, srv, http.MethodPost, "/sync/register", userID, "1.0.0", map[string]any{
		"client_id":      clientID,
		"platform":       "test",
		"app_version":    "1.0.0",
		"schema_version": 0,
		"schema_hash":    "",
	})
	if regResp.StatusCode != http.StatusOK {
		t.Fatalf("register status = %d", regResp.StatusCode)
	}
	var regBody map[string]any
	_ = json.NewDecoder(regResp.Body).Decode(&regBody)
	_ = regResp.Body.Close()

	schemaVersion := int64(regBody["schema_version"].(float64))

	// Pull with wrong schema_version
	pullResp := doJSON(t, srv, http.MethodPost, "/sync/pull", userID, "1.0.0", map[string]any{
		"client_id":      clientID,
		"checkpoint":     0,
		"schema_version": schemaVersion + 999,
		"schema_hash":    regBody["schema_hash"],
	})
	defer func() { _ = pullResp.Body.Close() }()

	if pullResp.StatusCode != http.StatusConflict {
		var body map[string]any
		_ = json.NewDecoder(pullResp.Body).Decode(&body)
		t.Fatalf("status = %d, want %d, body: %v", pullResp.StatusCode, http.StatusConflict, body)
	}

	var body map[string]any
	_ = json.NewDecoder(pullResp.Body).Decode(&body)
	if body["code"] != "schema_mismatch" {
		t.Fatalf("code = %v, want schema_mismatch", body["code"])
	}
}

func TestHTTP_BadRequest_400(t *testing.T) {
	srv, _ := setupTestServer(t)

	tokenStr := signTestJWT("user-1", testJWTSecret)

	req, _ := http.NewRequestWithContext(context.Background(), http.MethodPost, srv.URL+"/sync/push", bytes.NewBufferString(`not json`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+tokenStr)
	req.Header.Set("X-App-Version", "1.0.0")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusBadRequest)
	}
}

func TestHTTP_TablesEndpoint(t *testing.T) {
	srv, _ := setupTestServer(t)

	tokenStr := signTestJWT("user-tables", testJWTSecret)

	req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, srv.URL+"/sync/tables", nil)
	req.Header.Set("Authorization", "Bearer "+tokenStr)
	req.Header.Set("X-App-Version", "1.0.0")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET /sync/tables: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusOK)
	}

	var body map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decoding response: %v", err)
	}

	tables, ok := body["tables"].([]any)
	if !ok {
		t.Fatal("response missing 'tables' array")
	}

	// Verify expected tables exist
	tableMap := make(map[string]map[string]any)
	for _, entry := range tables {
		tbl := entry.(map[string]any)
		name := tbl["table_name"].(string)
		tableMap[name] = tbl
	}

	for _, expected := range []string{"orders", "order_details", "products", "categories"} {
		if _, ok := tableMap[expected]; !ok {
			t.Errorf("table %q not found in /sync/tables response", expected)
		}
	}

	// Each entry should have table_name, push_policy, dependencies
	for _, entry := range tables {
		tbl := entry.(map[string]any)
		if _, ok := tbl["table_name"]; !ok {
			t.Error("table entry missing 'table_name'")
		}
		if _, ok := tbl["push_policy"]; !ok {
			t.Error("table entry missing 'push_policy'")
		}
		if _, ok := tbl["dependencies"]; !ok {
			t.Error("table entry missing 'dependencies'")
		}
	}

	// products has push_policy = "disabled"
	if prod, ok := tableMap["products"]; ok {
		if prod["push_policy"] != "disabled" {
			t.Errorf("products push_policy = %v, want %q", prod["push_policy"], "disabled")
		}
	}

	// order_details has dependencies = ["orders"]
	if detail, ok := tableMap["order_details"]; ok {
		deps, ok := detail["dependencies"].([]any)
		if !ok {
			t.Fatal("order_details 'dependencies' is not an array")
		}
		if len(deps) != 1 || deps[0] != "orders" {
			t.Errorf("order_details dependencies = %v, want [orders]", deps)
		}
	}
}

func TestHTTP_SchemaEndpoint(t *testing.T) {
	srv, _ := setupTestServer(t)

	tokenStr := signTestJWT("user-schema", testJWTSecret)

	req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, srv.URL+"/sync/schema", nil)
	req.Header.Set("Authorization", "Bearer "+tokenStr)
	req.Header.Set("X-App-Version", "1.0.0")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET /sync/schema: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusOK)
	}

	var body map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decoding response: %v", err)
	}

	// Must have schema_version, schema_hash, tables
	if _, ok := body["schema_version"]; !ok {
		t.Error("response missing 'schema_version'")
	}
	if _, ok := body["schema_hash"]; !ok {
		t.Error("response missing 'schema_hash'")
	}

	tables, ok := body["tables"].([]any)
	if !ok {
		t.Fatal("response missing 'tables' array")
	}

	if len(tables) == 0 {
		t.Fatal("expected at least one table in schema response")
	}

	// Each table has columns array with name, db_type, nullable
	for _, entry := range tables {
		tbl := entry.(map[string]any)
		tableName, _ := tbl["table_name"].(string)

		columns, ok := tbl["columns"].([]any)
		if !ok {
			t.Errorf("table %q missing 'columns' array", tableName)
			continue
		}

		if len(columns) == 0 {
			t.Errorf("table %q has empty columns array", tableName)
			continue
		}

		for _, colEntry := range columns {
			col := colEntry.(map[string]any)
			if _, ok := col["name"]; !ok {
				t.Errorf("table %q: column missing 'name'", tableName)
			}
			if _, ok := col["db_type"]; !ok {
				t.Errorf("table %q: column missing 'db_type'", tableName)
			}
			// nullable should be present (can be true or false)
			if _, ok := col["nullable"]; !ok {
				t.Errorf("table %q: column missing 'nullable'", tableName)
			}
		}
	}
}

func doJSON(t *testing.T, srv *httptest.Server, method, path, userID, clientVersion string, body any) *http.Response {
	t.Helper()
	payload, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	req, err := http.NewRequestWithContext(context.Background(), method, srv.URL+path, bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+signTestJWT(userID, testJWTSecret))
	if clientVersion != "" {
		req.Header.Set("X-App-Version", clientVersion)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	return resp
}
