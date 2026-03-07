package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/trainstar/synchro"
	"github.com/trainstar/synchro/handler"
	"github.com/trainstar/synchro/synctest"
)

func setupTestServer(t *testing.T) (*httptest.Server, *sql.DB) {
	t.Helper()

	db := synctest.TestDB(t)
	reg := synctest.NewTestRegistry()

	engine, err := synchro.NewEngine(synchro.Config{
		DB:               db,
		Registry:         reg,
		MinClientVersion: "1.0.0",
	})
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}

	h := handler.New(engine)
	syncHandler := handler.UserIDMiddleware("X-User-ID",
		handler.VersionCheckMiddleware("X-Client-Version", "1.0.0", h.Routes()))

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"ok"}`))
	})
	mux.Handle("/sync/", syncHandler)

	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv, db
}

func TestHTTP_HealthCheck(t *testing.T) {
	srv, _ := setupTestServer(t)

	resp, err := http.Get(srv.URL + "/healthz")
	if err != nil {
		t.Fatalf("GET /healthz: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusOK)
	}
}

func TestHTTP_MissingUserID_401(t *testing.T) {
	srv, _ := setupTestServer(t)

	resp, err := http.Post(srv.URL+"/sync/register", "application/json", bytes.NewBufferString(`{}`))
	if err != nil {
		t.Fatalf("POST /sync/register: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusUnauthorized)
	}
}

func TestHTTP_UpgradeRequired_426(t *testing.T) {
	srv, _ := setupTestServer(t)

	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/sync/register", bytes.NewBufferString(`{}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-User-ID", "user-1")
	req.Header.Set("X-Client-Version", "0.1.0")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	defer resp.Body.Close()
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
	json.NewDecoder(regResp.Body).Decode(&regBody)
	regResp.Body.Close()

	schemaVersion := int64(regBody["schema_version"].(float64))
	schemaHash := regBody["schema_hash"].(string)

	// --- Push create ---
	itemID := "00000000-0000-0000-0000-aaaaaaaaaaaa"
	pushResp := doJSON(t, srv, http.MethodPost, "/sync/push", userID, "1.0.0", map[string]any{
		"client_id":      clientID,
		"schema_version": schemaVersion,
		"schema_hash":    schemaHash,
		"changes": []map[string]any{
			{
				"id":                itemID,
				"table_name":        "items",
				"operation":         "create",
				"data":              map[string]any{"id": itemID, "user_id": userID, "name": "Test Item", "description": "A test"},
				"client_updated_at": "2026-03-07T00:00:00Z",
			},
		},
	})
	if pushResp.StatusCode != http.StatusOK {
		var body map[string]any
		json.NewDecoder(pushResp.Body).Decode(&body)
		pushResp.Body.Close()
		t.Fatalf("push status = %d, want %d, body: %v", pushResp.StatusCode, http.StatusOK, body)
	}
	var pushBody map[string]any
	json.NewDecoder(pushResp.Body).Decode(&pushBody)
	pushResp.Body.Close()

	accepted := pushBody["accepted"].([]any)
	if len(accepted) != 1 {
		t.Fatalf("accepted count = %d, want 1", len(accepted))
	}

	// --- Simulate WAL: insert changelog entry directly ---
	// The push wrote the row to `items` via RLS. Now simulate the WAL consumer
	// by inserting a changelog entry so the pull finds it.
	_, execErr := db.Exec(`
		INSERT INTO sync_changelog (bucket_id, table_name, record_id, operation)
		VALUES ($1, $2, $3, $4)
	`, fmt.Sprintf("user:%s", userID), "items", itemID, 1)
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
		json.NewDecoder(pullResp.Body).Decode(&body)
		pullResp.Body.Close()
		t.Fatalf("pull status = %d, want %d, body: %v", pullResp.StatusCode, http.StatusOK, body)
	}
	var pullBody map[string]any
	json.NewDecoder(pullResp.Body).Decode(&pullBody)
	pullResp.Body.Close()

	changes := pullBody["changes"].([]any)
	if len(changes) == 0 {
		t.Fatal("expected at least one change in pull response")
	}

	found := false
	for _, c := range changes {
		rec := c.(map[string]any)
		if rec["id"] == itemID {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("pushed item %s not found in pull response", itemID)
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
	json.NewDecoder(regResp.Body).Decode(&regBody)
	regResp.Body.Close()

	schemaVersion := int64(regBody["schema_version"].(float64))

	// Pull with wrong schema_version
	pullResp := doJSON(t, srv, http.MethodPost, "/sync/pull", userID, "1.0.0", map[string]any{
		"client_id":      clientID,
		"checkpoint":     0,
		"schema_version": schemaVersion + 999,
		"schema_hash":    regBody["schema_hash"],
	})
	defer pullResp.Body.Close()

	if pullResp.StatusCode != http.StatusConflict {
		var body map[string]any
		json.NewDecoder(pullResp.Body).Decode(&body)
		t.Fatalf("status = %d, want %d, body: %v", pullResp.StatusCode, http.StatusConflict, body)
	}

	var body map[string]any
	json.NewDecoder(pullResp.Body).Decode(&body)
	if body["code"] != "schema_mismatch" {
		t.Fatalf("code = %v, want schema_mismatch", body["code"])
	}
}

func TestHTTP_BadRequest_400(t *testing.T) {
	srv, _ := setupTestServer(t)

	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/sync/push", bytes.NewBufferString(`not json`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-User-ID", "user-1")
	req.Header.Set("X-Client-Version", "1.0.0")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusBadRequest)
	}
}

func doJSON(t *testing.T, srv *httptest.Server, method, path, userID, clientVersion string, body any) *http.Response {
	t.Helper()
	payload, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	req, err := http.NewRequest(method, srv.URL+path, bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-User-ID", userID)
	if clientVersion != "" {
		req.Header.Set("X-Client-Version", clientVersion)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	return resp
}
