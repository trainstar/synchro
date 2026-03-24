package synchroapi

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
)

// serveConnect handles POST /sync/connect.
func (h *Handler) serveConnect(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}

	userID := UserIDFromContext(r.Context())
	if userID == "" {
		writeJSONError(w, http.StatusUnauthorized, "missing user identity")
		return
	}

	raw, _, ok := decodeJSONBodyObject(w, r)
	if !ok {
		return
	}

	var req ConnectRequestVNext
	if err := json.Unmarshal(raw, &req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if req.ClientID == "" {
		writeJSONError(w, http.StatusBadRequest, "client_id is required")
		return
	}

	resp, err := h.queryJSONB(
		r.Context(),
		"SELECT synchro_connect($1, $2::jsonb)",
		userID,
		string(raw),
	)
	if err != nil {
		mapSQLError(w, err)
		return
	}
	if mapPGError(w, resp) {
		return
	}

	writeRawJSON(w, http.StatusOK, resp)
}

// servePull handles POST /sync/pull.
func (h *Handler) servePull(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}

	userID := UserIDFromContext(r.Context())
	if userID == "" {
		writeJSONError(w, http.StatusUnauthorized, "missing user identity")
		return
	}

	raw, _, ok := decodeJSONBodyObject(w, r)
	if !ok {
		return
	}

	var req PullRequestVNext
	if err := json.Unmarshal(raw, &req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if req.ClientID == "" {
		writeJSONError(w, http.StatusBadRequest, "client_id is required")
		return
	}
	resp, err := h.queryJSONB(
		r.Context(),
		"SELECT synchro_pull_vnext($1, $2::jsonb)",
		userID,
		string(raw),
	)
	if err != nil {
		mapSQLError(w, err)
		return
	}
	if mapPGError(w, resp) {
		return
	}

	writeRawJSON(w, http.StatusOK, resp)
}

// servePush handles POST /sync/push.
func (h *Handler) servePush(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}

	userID := UserIDFromContext(r.Context())
	if userID == "" {
		writeJSONError(w, http.StatusUnauthorized, "missing user identity")
		return
	}

	raw, _, ok := decodeJSONBodyObject(w, r)
	if !ok {
		return
	}

	var req PushRequestVNext
	if err := json.Unmarshal(raw, &req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if req.ClientID == "" {
		writeJSONError(w, http.StatusBadRequest, "client_id is required")
		return
	}

	resp, err := h.queryJSONB(
		r.Context(),
		"SELECT synchro_push_vnext($1, $2::jsonb)",
		userID,
		string(raw),
	)
	if err != nil {
		mapSQLError(w, err)
		return
	}
	if mapPGError(w, resp) {
		return
	}

	writeRawJSON(w, http.StatusOK, resp)
}

// serveRebuild handles POST /sync/rebuild.
func (h *Handler) serveRebuild(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}

	userID := UserIDFromContext(r.Context())
	if userID == "" {
		writeJSONError(w, http.StatusUnauthorized, "missing user identity")
		return
	}

	raw, _, ok := decodeJSONBodyObject(w, r)
	if !ok {
		return
	}

	var req RebuildRequestVNext
	if err := json.Unmarshal(raw, &req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if req.ClientID == "" {
		writeJSONError(w, http.StatusBadRequest, "client_id is required")
		return
	}
	if req.Scope == "" {
		writeJSONError(w, http.StatusBadRequest, "scope is required")
		return
	}
	resp, err := h.queryJSONB(
		r.Context(),
		"SELECT synchro_rebuild_vnext($1, $2::jsonb)",
		userID,
		string(raw),
	)
	if err != nil {
		mapSQLError(w, err)
		return
	}
	if mapPGError(w, resp) {
		return
	}

	writeRawJSON(w, http.StatusOK, resp)
}

// serveSchema handles GET /sync/schema.
func (h *Handler) serveSchema(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodGet) {
		return
	}

	raw, err := h.queryJSONB(r.Context(), "SELECT synchro_schema()")
	if err != nil {
		mapSQLError(w, err)
		return
	}

	writeRawJSON(w, http.StatusOK, raw)
}

// serveTables handles GET /sync/tables.
func (h *Handler) serveTables(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodGet) {
		return
	}

	raw, err := h.queryJSONB(r.Context(), "SELECT synchro_tables()")
	if err != nil {
		mapSQLError(w, err)
		return
	}

	writeRawJSON(w, http.StatusOK, raw)
}

// serveDebug handles GET /sync/debug?client_id=xxx.
func (h *Handler) serveDebug(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodGet) {
		return
	}

	userID := UserIDFromContext(r.Context())
	if userID == "" {
		writeJSONError(w, http.StatusUnauthorized, "missing user identity")
		return
	}

	clientID := r.URL.Query().Get("client_id")
	if clientID == "" {
		writeJSONError(w, http.StatusBadRequest, "client_id query parameter is required")
		return
	}

	raw, err := h.queryJSONB(r.Context(),
		"SELECT synchro_debug($1, $2)",
		userID, clientID,
	)
	if err != nil {
		mapSQLError(w, err)
		return
	}
	if mapPGError(w, raw) {
		return
	}

	writeRawJSON(w, http.StatusOK, raw)
}

// queryJSONB executes a SQL function that returns JSONB and scans the raw bytes.
// Zero intermediate marshaling on the response path.
func (h *Handler) queryJSONB(ctx context.Context, query string, args ...any) ([]byte, error) {
	var raw []byte
	err := h.db.QueryRowContext(ctx, query, args...).Scan(&raw)
	if err != nil {
		return nil, err
	}
	return raw, nil
}

// writeRawJSON writes pre-encoded JSON bytes as an HTTP response.
func writeRawJSON(w http.ResponseWriter, status int, data []byte) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(data)
}

func decodeJSONBodyObject(w http.ResponseWriter, r *http.Request) ([]byte, map[string]json.RawMessage, bool) {
	raw, err := io.ReadAll(r.Body)
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid request body")
		return nil, nil, false
	}
	if len(bytes.TrimSpace(raw)) == 0 {
		writeJSONError(w, http.StatusBadRequest, "invalid request body")
		return nil, nil, false
	}

	var body map[string]json.RawMessage
	if err := json.Unmarshal(raw, &body); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid request body")
		return nil, nil, false
	}

	return raw, body, true
}

func requireMethod(w http.ResponseWriter, r *http.Request, allowed string) bool {
	if r.Method == allowed {
		return true
	}
	w.Header().Set("Allow", allowed)
	writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
	return false
}
