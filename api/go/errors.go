package synchroapi

import (
	"encoding/json"
	"net/http"
	"strings"
)

// mapPGError inspects the raw JSONB response from a synchro_*() call.
// If the response contains an "error" key, it writes the appropriate HTTP
// status and returns true. Otherwise it returns false and the caller should
// forward the raw JSONB as a success response.
func mapPGError(w http.ResponseWriter, raw []byte) bool {
	// Quick check: avoid parsing if the response is clearly not an error.
	if len(raw) == 0 || raw[0] != '{' {
		return false
	}

	var probe struct {
		Error string `json:"error"`
	}
	if err := json.Unmarshal(raw, &probe); err != nil || probe.Error == "" {
		return false
	}

	status := classifyError(probe.Error)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(raw)
	return true
}

// classifyError maps a structured error string from the extension to an HTTP
// status code.
func classifyError(errMsg string) int {
	lower := strings.ToLower(errMsg)

	switch {
	case strings.Contains(lower, "schema_mismatch") || strings.Contains(lower, "schema version"):
		return http.StatusConflict
	case strings.Contains(lower, "not_found") || strings.Contains(lower, "inactive"):
		return http.StatusNotFound
	case strings.Contains(lower, "read_only"):
		return http.StatusForbidden
	default:
		return http.StatusInternalServerError
	}
}

// mapSQLError converts a database/sql error into an HTTP response.
// Returns true if the error was handled.
func mapSQLError(w http.ResponseWriter, err error) bool {
	if err == nil {
		return false
	}

	msg := err.Error()

	switch {
	case strings.Contains(msg, "schema mismatch") || strings.Contains(msg, "schema version"):
		writeJSONError(w, http.StatusConflict, "schema mismatch")
	case strings.Contains(msg, "not found") || strings.Contains(msg, "inactive"):
		writeJSONError(w, http.StatusNotFound, "not found")
	case strings.Contains(msg, "read_only"):
		writeJSONError(w, http.StatusForbidden, "read only")
	case isTransientError(msg):
		w.Header().Set("Retry-After", "5")
		writeJSONError(w, http.StatusServiceUnavailable, "service temporarily unavailable")
	default:
		writeJSONError(w, http.StatusInternalServerError, "internal error")
	}
	return true
}

func isTransientError(msg string) bool {
	return strings.Contains(msg, "connection") ||
		strings.Contains(msg, "timeout") ||
		strings.Contains(msg, "database is closed")
}

func writeJSONError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": msg})
}
