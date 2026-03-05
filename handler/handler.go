package handler

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"

	"github.com/trainstar/synchro"
)

// Handler provides net/http handlers for sync endpoints.
type Handler struct {
	engine *synchro.Engine
}

// New creates a new sync HTTP handler.
func New(engine *synchro.Engine) *Handler {
	return &Handler{engine: engine}
}

// ServeRegister handles POST /sync/register.
func (h *Handler) ServeRegister(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	userID := UserIDFromContext(ctx)
	if userID == "" {
		writeError(w, http.StatusUnauthorized, "missing user identity")
		return
	}

	var req synchro.RegisterRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	resp, err := h.engine.RegisterClient(ctx, userID, &req)
	if err != nil {
		if err == synchro.ErrUpgradeRequired {
			writeError(w, http.StatusUpgradeRequired, "client upgrade required")
			return
		}
		writeError(w, http.StatusInternalServerError, "failed to register client")
		return
	}

	writeJSON(w, http.StatusOK, resp)
}

// ServePull handles GET /sync/pull.
// Query parameters: client_id (required), checkpoint (int64), tables (comma-separated), limit (int).
func (h *Handler) ServePull(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	userID := UserIDFromContext(ctx)
	if userID == "" {
		writeError(w, http.StatusUnauthorized, "missing user identity")
		return
	}

	q := r.URL.Query()
	clientID := q.Get("client_id")
	if clientID == "" {
		writeError(w, http.StatusBadRequest, "client_id is required")
		return
	}

	var checkpoint int64
	if v := q.Get("checkpoint"); v != "" {
		var err error
		checkpoint, err = strconv.ParseInt(v, 10, 64)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid checkpoint")
			return
		}
	}

	var tables []string
	if v := q.Get("tables"); v != "" {
		tables = strings.Split(v, ",")
	}

	var limit int
	if v := q.Get("limit"); v != "" {
		var err error
		limit, err = strconv.Atoi(v)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid limit")
			return
		}
	}

	var knownBuckets []string
	if v := q.Get("known_buckets"); v != "" {
		knownBuckets = strings.Split(v, ",")
	}

	req := &synchro.PullRequest{
		ClientID:     clientID,
		Checkpoint:   checkpoint,
		Tables:       tables,
		Limit:        limit,
		KnownBuckets: knownBuckets,
	}

	resp, err := h.engine.Pull(ctx, userID, req)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to pull changes")
		return
	}

	writeJSON(w, http.StatusOK, resp)
}

// ServePush handles POST /sync/push.
func (h *Handler) ServePush(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	userID := UserIDFromContext(ctx)
	if userID == "" {
		writeError(w, http.StatusUnauthorized, "missing user identity")
		return
	}

	var req synchro.PushRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	resp, err := h.engine.Push(ctx, userID, &req)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to push changes")
		return
	}

	writeJSON(w, http.StatusOK, resp)
}

// ServeTableMeta handles GET /sync/tables.
func (h *Handler) ServeTableMeta(w http.ResponseWriter, _ *http.Request) {
	resp := h.engine.TableMetadata()
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
