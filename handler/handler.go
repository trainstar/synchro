package handler

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"strconv"

	"github.com/trainstar/synchro"
)

// Handler provides net/http handlers for sync endpoints.
type Handler struct {
	engine            *synchro.Engine
	defaultRetryAfter int
}

// Option configures a Handler.
type Option func(*Handler)

// WithDefaultRetryAfter sets the default Retry-After seconds for transient errors.
func WithDefaultRetryAfter(seconds int) Option {
	return func(h *Handler) { h.defaultRetryAfter = seconds }
}

// New creates a new sync HTTP handler.
func New(engine *synchro.Engine, opts ...Option) *Handler {
	h := &Handler{engine: engine, defaultRetryAfter: 5}
	for _, opt := range opts {
		opt(h)
	}
	return h
}

// Routes returns an http.Handler with all sync endpoints mounted.
func (h *Handler) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/sync/register", h.ServeRegister)
	mux.HandleFunc("/sync/pull", h.ServePull)
	mux.HandleFunc("/sync/push", h.ServePush)
	mux.HandleFunc("/sync/resync", h.ServeResync)
	mux.HandleFunc("/sync/tables", h.ServeTableMeta)
	mux.HandleFunc("/sync/schema", h.ServeSchema)
	return mux
}

// ServeRegister handles POST /sync/register.
func (h *Handler) ServeRegister(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}

	ctx := r.Context()
	userID := UserIDFromContext(ctx)
	if userID == "" {
		writeError(w, http.StatusUnauthorized, "missing user identity")
		return
	}

	body, err := decodeJSONObject(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if _, ok := body["schema_version"]; !ok {
		writeError(w, http.StatusBadRequest, "schema_version is required")
		return
	}
	if _, ok := body["schema_hash"]; !ok {
		writeError(w, http.StatusBadRequest, "schema_hash is required")
		return
	}

	var req synchro.RegisterRequest
	if err := remarshal(body, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	resp, err := h.engine.RegisterClient(ctx, userID, &req)
	if err != nil {
		if err == synchro.ErrUpgradeRequired {
			writeError(w, http.StatusUpgradeRequired, "client upgrade required")
			return
		}
		if err == synchro.ErrSchemaMismatch {
			h.writeSchemaMismatch(w, r)
			return
		}
		if isTransientError(err) {
			writeRetryError(w, http.StatusServiceUnavailable, "service temporarily unavailable", h.defaultRetryAfter)
			return
		}
		writeError(w, http.StatusInternalServerError, "failed to register client")
		return
	}

	writeJSON(w, http.StatusOK, resp)
}

// ServePull handles POST /sync/pull.
func (h *Handler) ServePull(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}

	ctx := r.Context()
	userID := UserIDFromContext(ctx)
	if userID == "" {
		writeError(w, http.StatusUnauthorized, "missing user identity")
		return
	}

	body, err := decodeJSONObject(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	for _, k := range []string{"client_id", "schema_version", "schema_hash"} {
		if _, ok := body[k]; !ok {
			writeError(w, http.StatusBadRequest, k+" is required")
			return
		}
	}

	var req synchro.PullRequest
	if err := remarshal(body, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	resp, err := h.engine.Pull(ctx, userID, &req)
	if err != nil {
		if err == synchro.ErrSchemaMismatch {
			h.writeSchemaMismatch(w, r)
			return
		}
		if isTransientError(err) {
			writeRetryError(w, http.StatusServiceUnavailable, "service temporarily unavailable", h.defaultRetryAfter)
			return
		}
		writeError(w, http.StatusInternalServerError, "failed to pull changes")
		return
	}

	writeJSON(w, http.StatusOK, resp)
}

// ServePush handles POST /sync/push.
func (h *Handler) ServePush(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}

	ctx := r.Context()
	userID := UserIDFromContext(ctx)
	if userID == "" {
		writeError(w, http.StatusUnauthorized, "missing user identity")
		return
	}

	body, err := decodeJSONObject(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if _, ok := body["schema_version"]; !ok {
		writeError(w, http.StatusBadRequest, "schema_version is required")
		return
	}
	if _, ok := body["schema_hash"]; !ok {
		writeError(w, http.StatusBadRequest, "schema_hash is required")
		return
	}

	var req synchro.PushRequest
	if err := remarshal(body, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	resp, err := h.engine.Push(ctx, userID, &req)
	if err != nil {
		if err == synchro.ErrSchemaMismatch {
			h.writeSchemaMismatch(w, r)
			return
		}
		if isTransientError(err) {
			writeRetryError(w, http.StatusServiceUnavailable, "service temporarily unavailable", h.defaultRetryAfter)
			return
		}
		writeError(w, http.StatusInternalServerError, "failed to push changes")
		return
	}

	writeJSON(w, http.StatusOK, resp)
}

// ServeResync handles POST /sync/resync.
func (h *Handler) ServeResync(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}

	ctx := r.Context()
	userID := UserIDFromContext(ctx)
	if userID == "" {
		writeError(w, http.StatusUnauthorized, "missing user identity")
		return
	}

	body, err := decodeJSONObject(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	for _, k := range []string{"client_id", "schema_version", "schema_hash"} {
		if _, ok := body[k]; !ok {
			writeError(w, http.StatusBadRequest, k+" is required")
			return
		}
	}

	var req synchro.ResyncRequest
	if err := remarshal(body, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	resp, err := h.engine.Resync(ctx, userID, &req)
	if err != nil {
		if err == synchro.ErrSchemaMismatch {
			h.writeSchemaMismatch(w, r)
			return
		}
		if err == synchro.ErrClientNotRegistered {
			writeError(w, http.StatusNotFound, "client not registered")
			return
		}
		if isTransientError(err) {
			writeRetryError(w, http.StatusServiceUnavailable, "service temporarily unavailable", h.defaultRetryAfter)
			return
		}
		writeError(w, http.StatusInternalServerError, "failed to resync")
		return
	}

	writeJSON(w, http.StatusOK, resp)
}

// ServeTableMeta handles GET /sync/tables.
func (h *Handler) ServeTableMeta(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodGet) {
		return
	}

	resp, err := h.engine.TableMetadata(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to load table metadata")
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

// ServeSchema handles GET /sync/schema.
func (h *Handler) ServeSchema(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodGet) {
		return
	}

	resp, err := h.engine.Schema(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to load schema")
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]string{"error": msg})
}

func writeRetryError(w http.ResponseWriter, status int, msg string, retryAfter int) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Retry-After", strconv.Itoa(retryAfter))
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]any{
		"error":       msg,
		"retry_after": retryAfter,
	})
}

func isTransientError(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, sql.ErrConnDone) {
		return true
	}
	var netErr net.Error
	return errors.As(err, &netErr)
}

func requireMethod(w http.ResponseWriter, r *http.Request, allowed string) bool {
	if r.Method == allowed {
		return true
	}
	w.Header().Set("Allow", allowed)
	writeError(w, http.StatusMethodNotAllowed, "method not allowed")
	return false
}

func decodeJSONObject(r *http.Request) (map[string]json.RawMessage, error) {
	var body map[string]json.RawMessage
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		return nil, err
	}
	if body == nil {
		return nil, errors.New("empty body")
	}
	return body, nil
}

func remarshal(src map[string]json.RawMessage, dst any) error {
	b, err := json.Marshal(src)
	if err != nil {
		return err
	}
	return json.Unmarshal(b, dst)
}

func (h *Handler) writeSchemaMismatch(w http.ResponseWriter, r *http.Request) {
	version, hash, err := h.engine.CurrentSchemaManifest(r.Context())
	if err != nil {
		writeError(w, http.StatusConflict, "schema mismatch")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusConflict)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"code":                  "schema_mismatch",
		"message":               "client schema does not match server schema",
		"server_schema_version": version,
		"server_schema_hash":    hash,
	})
}
