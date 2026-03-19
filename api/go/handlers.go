package synchroapi

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
)

// serveRegister handles POST /sync/register.
func (h *Handler) serveRegister(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}

	userID := UserIDFromContext(r.Context())
	if userID == "" {
		writeJSONError(w, http.StatusUnauthorized, "missing user identity")
		return
	}

	var req RegisterRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if req.ClientID == "" {
		writeJSONError(w, http.StatusBadRequest, "client_id is required")
		return
	}

	raw, err := h.queryJSONB(r.Context(),
		"SELECT synchro_register_client($1, $2, $3, $4, $5, $6)",
		userID, req.ClientID, req.Platform, req.AppVersion,
		req.SchemaVersion, req.SchemaHash,
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

	var req PullRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if req.ClientID == "" {
		writeJSONError(w, http.StatusBadRequest, "client_id is required")
		return
	}

	// Marshal complex parameters to JSONB for the SQL function.
	bucketCPs, _ := json.Marshal(req.BucketCheckpoints)
	var bucketCPsArg any
	if req.BucketCheckpoints != nil {
		bucketCPsArg = string(bucketCPs)
	}

	raw, err := h.queryJSONB(r.Context(),
		"SELECT synchro_pull($1, $2, $3, $4::jsonb, $5, $6, $7, $8, $9)",
		userID, req.ClientID, req.Checkpoint, bucketCPsArg,
		req.Limit, pqTextArray(req.Tables), pqTextArray(req.KnownBuckets),
		req.SchemaVersion, req.SchemaHash,
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

	var req PushRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if req.ClientID == "" {
		writeJSONError(w, http.StatusBadRequest, "client_id is required")
		return
	}

	// Pass the changes array as a raw JSONB parameter.
	changesJSON, err := json.Marshal(req.Changes)
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid changes array")
		return
	}

	raw, err := h.queryJSONB(r.Context(),
		"SELECT synchro_push($1, $2, $3::jsonb, $4, $5)",
		userID, req.ClientID, string(changesJSON),
		req.SchemaVersion, req.SchemaHash,
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

	var req RebuildRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if req.ClientID == "" {
		writeJSONError(w, http.StatusBadRequest, "client_id is required")
		return
	}
	if req.BucketID == "" {
		writeJSONError(w, http.StatusBadRequest, "bucket_id is required")
		return
	}

	raw, err := h.queryJSONB(r.Context(),
		"SELECT synchro_rebuild($1, $2, $3, $4, $5, $6, $7)",
		userID, req.ClientID, req.BucketID, req.Cursor,
		req.Limit, req.SchemaVersion, req.SchemaHash,
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

func requireMethod(w http.ResponseWriter, r *http.Request, allowed string) bool {
	if r.Method == allowed {
		return true
	}
	w.Header().Set("Allow", allowed)
	writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
	return false
}

// pqTextArray converts a Go string slice to a PostgreSQL text array literal.
// Returns nil if the slice is nil (maps to SQL NULL).
func pqTextArray(ss []string) any {
	if ss == nil {
		return nil
	}
	// Use PostgreSQL array literal format: {elem1,elem2,...}
	if len(ss) == 0 {
		return "{}"
	}
	var buf strings.Builder
	buf.WriteByte('{')
	for i, s := range ss {
		if i > 0 {
			buf.WriteByte(',')
		}
		buf.WriteByte('"')
		for _, c := range s {
			if c == '"' || c == '\\' {
				buf.WriteByte('\\')
			}
			buf.WriteRune(c)
		}
		buf.WriteByte('"')
	}
	buf.WriteByte('}')
	return buf.String()
}
