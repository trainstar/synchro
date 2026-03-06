package handler_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/trainstar/synchro"
	"github.com/trainstar/synchro/handler"
	"github.com/trainstar/synchro/synctest"
)

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
	engine, err := synchro.NewEngine(synchro.Config{DB: db, Registry: reg})
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
