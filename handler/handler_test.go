package handler_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/trainstar/synchro"
	"github.com/trainstar/synchro/handler"
	"github.com/trainstar/synchro/synctest"
)

func TestHandler_Routes_MountsAllEndpoints(t *testing.T) {
	h := handler.New(nil)
	srv := httptest.NewServer(h.Routes())
	defer srv.Close()

	routes := []struct {
		path   string
		method string
		want   int
	}{
		// POST endpoints should return 401 (missing user identity) not 404
		{"/sync/register", http.MethodPost, http.StatusUnauthorized},
		{"/sync/pull", http.MethodPost, http.StatusUnauthorized},
		{"/sync/push", http.MethodPost, http.StatusUnauthorized},
		{"/sync/snapshot", http.MethodPost, http.StatusUnauthorized},
		// GET endpoints — engine is nil so these will panic/500, but
		// wrong method should return 405, confirming the route exists
		{"/sync/tables", http.MethodPost, http.StatusMethodNotAllowed},
		{"/sync/schema", http.MethodPost, http.StatusMethodNotAllowed},
	}

	for _, tc := range routes {
		t.Run(tc.method+" "+tc.path, func(t *testing.T) {
			req, _ := http.NewRequest(tc.method, srv.URL+tc.path, bytes.NewBufferString(`{}`))
			req.Header.Set("Content-Type", "application/json")
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatalf("request failed: %v", err)
			}
			_ = resp.Body.Close()
			if resp.StatusCode != tc.want {
				t.Fatalf("status = %d, want %d", resp.StatusCode, tc.want)
			}
		})
	}

	// Verify unregistered path returns 404
	postReq, err := http.NewRequestWithContext(context.Background(), http.MethodPost, srv.URL+"/sync/nonexistent", bytes.NewBufferString(`{}`))
	if err != nil {
		t.Fatalf("creating request: %v", err)
	}
	postReq.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(postReq)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("unregistered path: status = %d, want %d", resp.StatusCode, http.StatusNotFound)
	}
}

func TestServePull_MethodNotAllowed(t *testing.T) {
	h := handler.New(nil)

	req := httptest.NewRequest(http.MethodGet, "/sync/pull", bytes.NewBufferString(`{}`))
	rec := httptest.NewRecorder()
	h.ServePull(rec, req)

	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusMethodNotAllowed)
	}
	if got := rec.Header().Get("Allow"); got != http.MethodPost {
		t.Fatalf("Allow header = %q, want %q", got, http.MethodPost)
	}
}

func TestServePush_MissingSchemaFields(t *testing.T) {
	h := handler.New(nil)

	req := httptest.NewRequest(http.MethodPost, "/sync/push", bytes.NewBufferString(`{"client_id":"c1","changes":[]}`))
	req = req.WithContext(handler.WithUserID(req.Context(), "user-1"))

	rec := httptest.NewRecorder()
	h.ServePush(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}

	var body map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if body["error"] != "schema_version is required" {
		t.Fatalf("error = %q, want schema_version is required", body["error"])
	}
}

func TestServePull_SchemaMismatchIncludesServerManifest(t *testing.T) {
	db := synctest.TestDB(t)
	ctx := context.Background()

	reg := synctest.NewTestRegistry()
	engine, err := synchro.NewEngine(ctx, &synchro.Config{DB: db, Registry: reg})
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}

	userID := "00000000-0000-0000-0000-00000000abcd"
	clientID := "client-schema-mismatch"

	if _, err := engine.RegisterClient(ctx, userID, synctest.MakeRegisterRequest(clientID, "ios", "1.0.0")); err != nil {
		t.Fatalf("RegisterClient: %v", err)
	}
	version, hash, err := engine.CurrentSchemaManifest(ctx)
	if err != nil {
		t.Fatalf("CurrentSchemaManifest: %v", err)
	}

	h := handler.New(engine)
	pullReq := map[string]any{
		"client_id":      clientID,
		"checkpoint":     0,
		"schema_version": version + 1,
		"schema_hash":    hash,
	}
	payload, _ := json.Marshal(pullReq)
	req := httptest.NewRequest(http.MethodPost, "/sync/pull", bytes.NewReader(payload))
	req = req.WithContext(handler.WithUserID(req.Context(), userID))

	rec := httptest.NewRecorder()
	h.ServePull(rec, req)

	if rec.Code != http.StatusConflict {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusConflict)
	}

	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if body["code"] != "schema_mismatch" {
		t.Fatalf("code = %v, want schema_mismatch", body["code"])
	}
	if int64(body["server_schema_version"].(float64)) != version {
		t.Fatalf("server_schema_version = %v, want %d", body["server_schema_version"], version)
	}
	if body["server_schema_hash"] != hash {
		t.Fatalf("server_schema_hash = %v, want %q", body["server_schema_hash"], hash)
	}
}

func TestHandler_TransientError_Returns503(t *testing.T) {
	db := synctest.TestDB(t)
	ctx := context.Background()

	reg := synctest.NewTestRegistry()
	engine, err := synchro.NewEngine(ctx, &synchro.Config{DB: db, Registry: reg})
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}

	userID := "00000000-0000-0000-0000-000000000001"
	clientID := "client-503"

	if _, err := engine.RegisterClient(ctx, userID, synctest.MakeRegisterRequest(clientID, "test", "1.0.0")); err != nil {
		t.Fatalf("RegisterClient: %v", err)
	}
	sv, sh, err := engine.CurrentSchemaManifest(ctx)
	if err != nil {
		t.Fatalf("CurrentSchemaManifest: %v", err)
	}

	// Close the DB to force sql.ErrConnDone on next call.
	_ = db.Close()

	h := handler.New(engine, handler.WithDefaultRetryAfter(10))
	pullReq := map[string]any{
		"client_id":      clientID,
		"checkpoint":     0,
		"schema_version": sv,
		"schema_hash":    sh,
	}
	payload, _ := json.Marshal(pullReq)
	req := httptest.NewRequest(http.MethodPost, "/sync/pull", bytes.NewReader(payload))
	req = req.WithContext(handler.WithUserID(req.Context(), userID))

	rec := httptest.NewRecorder()
	h.ServePull(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusServiceUnavailable)
	}

	retryAfter := rec.Header().Get("Retry-After")
	if retryAfter != "10" {
		t.Fatalf("Retry-After header = %q, want %q", retryAfter, "10")
	}

	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if body["retry_after"] != float64(10) {
		t.Fatalf("retry_after = %v, want 10", body["retry_after"])
	}
	if body["error"] == nil || body["error"] == "" {
		t.Fatal("expected error message in body")
	}
}

func TestHandler_RetryAfterMiddleware_Rejects(t *testing.T) {
	check := func(r *http.Request) (bool, int, int) {
		return true, http.StatusTooManyRequests, 60
	}

	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatal("inner handler should not be called")
	})

	mw := handler.RetryAfterMiddleware(check, inner)

	req := httptest.NewRequest(http.MethodPost, "/sync/pull", nil)
	rec := httptest.NewRecorder()
	mw.ServeHTTP(rec, req)

	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusTooManyRequests)
	}
	if got := rec.Header().Get("Retry-After"); got != "60" {
		t.Fatalf("Retry-After = %q, want %q", got, "60")
	}

	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if body["retry_after"] != float64(60) {
		t.Fatalf("retry_after = %v, want 60", body["retry_after"])
	}
}

func TestHandler_RetryAfterMiddleware_Passes(t *testing.T) {
	check := func(r *http.Request) (bool, int, int) {
		return false, 0, 0
	}

	called := false
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		w.WriteHeader(http.StatusOK)
		_, _ = fmt.Fprint(w, "ok")
	})

	mw := handler.RetryAfterMiddleware(check, inner)

	req := httptest.NewRequest(http.MethodPost, "/sync/pull", nil)
	rec := httptest.NewRecorder()
	mw.ServeHTTP(rec, req)

	if !called {
		t.Fatal("inner handler was not called")
	}
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
}
