package synchroapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"mime"
	"net/http"
	"strings"
	"unicode/utf8"
)

const (
	maxJSONBodyBytes    = 1_048_576
	maxJSONNestingDepth = 128
)

// serveConnect handles POST /sync/connect.
func (h *Handler) serveConnect(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}
	if !requireJSONMediaType(w, r) {
		return
	}

	userID := UserIDFromContext(r.Context())
	if userID == "" {
		writeJSONError(w, http.StatusUnauthorized, "missing user identity")
		return
	}

	raw, _, ok := decodeJSONBodyObject(w, r, connectRequestMembers...)
	if !ok {
		return
	}

	var req ConnectRequest
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
		"SELECT synchro.synchro_connect($1, $2::pg_catalog.jsonb)",
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
	if !requireJSONMediaType(w, r) {
		return
	}

	userID := UserIDFromContext(r.Context())
	if userID == "" {
		writeJSONError(w, http.StatusUnauthorized, "missing user identity")
		return
	}

	raw, _, ok := decodeJSONBodyObject(w, r, pullRequestMembers...)
	if !ok {
		return
	}

	var req PullRequest
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
		"SELECT synchro.synchro_pull($1, $2::pg_catalog.jsonb)",
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
	if !requireJSONMediaType(w, r) {
		return
	}

	userID := UserIDFromContext(r.Context())
	if userID == "" {
		writeJSONError(w, http.StatusUnauthorized, "missing user identity")
		return
	}

	raw, _, ok := decodeJSONBodyObject(w, r, pushRequestMembers...)
	if !ok {
		return
	}

	var req PushRequest
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
		"SELECT synchro.synchro_push($1, $2::pg_catalog.jsonb)",
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
	if !requireJSONMediaType(w, r) {
		return
	}

	userID := UserIDFromContext(r.Context())
	if userID == "" {
		writeJSONError(w, http.StatusUnauthorized, "missing user identity")
		return
	}

	raw, _, ok := decodeJSONBodyObject(w, r, rebuildRequestMembers...)
	if !ok {
		return
	}

	var req RebuildRequest
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
		"SELECT synchro.synchro_rebuild($1, $2::pg_catalog.jsonb)",
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

	raw, err := h.queryJSONB(r.Context(), "SELECT synchro.synchro_schema_manifest()")
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

	raw, err := h.queryJSONB(r.Context(), "SELECT synchro.synchro_tables()")
	if err != nil {
		mapSQLError(w, err)
		return
	}

	writeRawJSON(w, http.StatusOK, raw)
}

// queryJSONB executes a SQL function that returns JSONB and scans the raw bytes.
// Zero intermediate marshaling on the response path.
func (h *Handler) queryJSONB(ctx context.Context, query string, args ...any) ([]byte, error) {
	queryTimeout := h.queryTimeout
	if queryTimeout <= 0 {
		queryTimeout = DefaultDatabaseQueryTimeout
	}
	queryContext, cancel := context.WithTimeout(ctx, queryTimeout)
	defer cancel()

	var raw []byte
	err := h.db.QueryRowContext(queryContext, query, args...).Scan(&raw)
	if err != nil {
		if ctx.Err() == nil && queryContext.Err() == context.DeadlineExceeded {
			return nil, fmt.Errorf("%w: %w", errAdapterQueryTimeout, err)
		}
		return nil, err
	}
	return raw, nil
}

// writeRawJSON writes pre-encoded JSON bytes as an HTTP response.
func writeRawJSON(w http.ResponseWriter, status int, data []byte) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Content-Length", fmt.Sprintf("%d", len(data)))
	w.WriteHeader(status)
	n, err := w.Write(data)
	if err != nil {
		log.Printf("[synchro] writeRawJSON error: wrote %d/%d bytes, err=%v", n, len(data), err)
	} else if n != len(data) {
		log.Printf("[synchro] writeRawJSON partial write: wrote %d/%d bytes", n, len(data))
	}
}

func decodeJSONBodyObject(w http.ResponseWriter, r *http.Request, allowedMembers ...string) ([]byte, map[string]json.RawMessage, bool) {
	requestBody := r.Body
	if requestBody == nil {
		requestBody = http.NoBody
	}
	raw, err := io.ReadAll(io.LimitReader(requestBody, maxJSONBodyBytes+1))
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid request body")
		return nil, nil, false
	}
	if len(raw) > maxJSONBodyBytes {
		writeJSONError(w, http.StatusBadRequest, "invalid request body")
		return nil, nil, false
	}
	if len(bytes.TrimSpace(raw)) == 0 {
		writeJSONError(w, http.StatusBadRequest, "invalid request body")
		return nil, nil, false
	}
	if !utf8.Valid(raw) || validateJSONObjectJSON(raw) != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid request body")
		return nil, nil, false
	}

	var body map[string]json.RawMessage
	if err := json.Unmarshal(raw, &body); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid request body")
		return nil, nil, false
	}
	for key := range body {
		if !containsRequestMember(allowedMembers, key) {
			writeJSONError(w, http.StatusBadRequest, "invalid request body")
			return nil, nil, false
		}
	}

	return raw, body, true
}

func requireJSONMediaType(w http.ResponseWriter, r *http.Request) bool {
	values := r.Header.Values("Content-Type")
	if len(values) != 1 {
		writeJSONError(w, http.StatusBadRequest, "invalid request body")
		return false
	}
	mediaType, _, err := mime.ParseMediaType(values[0])
	if err != nil || !strings.EqualFold(mediaType, "application/json") {
		writeJSONError(w, http.StatusBadRequest, "invalid request body")
		return false
	}
	return true
}

func containsRequestMember(allowedMembers []string, member string) bool {
	for _, allowed := range allowedMembers {
		if member == allowed {
			return true
		}
	}
	return false
}

// validateJSONObjectJSON validates JSON structure without unmarshaling or rewriting the input.
// It rejects duplicate names after JSON string decoding at every object nesting level.
func validateJSONObjectJSON(raw []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	if token != json.Delim('{') {
		return errors.New("request envelope must be an object")
	}
	if err := validateJSONObjectMembers(decoder, 1); err != nil {
		return err
	}
	if _, err := decoder.Token(); err != io.EOF {
		if err == nil {
			return errors.New("multiple top-level JSON values")
		}
		return err
	}
	return nil
}

func validateJSONObjectMembers(decoder *json.Decoder, depth int) error {
	seen := make(map[string]struct{})
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return err
		}
		name, ok := token.(string)
		if !ok {
			return errors.New("object member name is not a string")
		}
		if _, exists := seen[name]; exists {
			return errors.New("duplicate object member")
		}
		seen[name] = struct{}{}
		if err := validateJSONValue(decoder, depth); err != nil {
			return err
		}
	}
	closing, err := decoder.Token()
	if err != nil {
		return err
	}
	if closing != json.Delim('}') {
		return errors.New("object is not closed")
	}
	return nil
}

func validateJSONArrayItems(decoder *json.Decoder, depth int) error {
	for decoder.More() {
		if err := validateJSONValue(decoder, depth); err != nil {
			return err
		}
	}
	closing, err := decoder.Token()
	if err != nil {
		return err
	}
	if closing != json.Delim(']') {
		return errors.New("array is not closed")
	}
	return nil
}

func validateJSONValue(decoder *json.Decoder, depth int) error {
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	delim, ok := token.(json.Delim)
	if !ok {
		return nil
	}
	switch delim {
	case json.Delim('{'):
		if depth >= maxJSONNestingDepth {
			return errors.New("JSON nesting limit exceeded")
		}
		return validateJSONObjectMembers(decoder, depth+1)
	case json.Delim('['):
		if depth >= maxJSONNestingDepth {
			return errors.New("JSON nesting limit exceeded")
		}
		return validateJSONArrayItems(decoder, depth+1)
	default:
		return errors.New("unexpected JSON delimiter")
	}
}

func requireMethod(w http.ResponseWriter, r *http.Request, allowed string) bool {
	if r.Method == allowed {
		return true
	}
	w.Header().Set("Allow", allowed)
	writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
	return false
}
