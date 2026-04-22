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
		Error json.RawMessage `json:"error"`
	}
	if err := json.Unmarshal(raw, &probe); err != nil || len(probe.Error) == 0 {
		return false
	}

	status, retryAfter, ok := classifyPGError(probe.Error)
	if !ok {
		return false
	}
	w.Header().Set("Content-Type", "application/json")
	if retryAfter != "" {
		w.Header().Set("Retry-After", retryAfter)
	}
	w.WriteHeader(status)
	_, _ = w.Write(raw)
	return true
}

func classifyPGError(raw json.RawMessage) (int, string, bool) {
	var protocol struct {
		Code      string `json:"code"`
		Message   string `json:"message"`
		Retryable bool   `json:"retryable"`
	}
	if err := json.Unmarshal(raw, &protocol); err == nil && protocol.Code != "" {
		return classifyProtocolError(protocol.Code, protocol.Retryable), retryAfterForCode(protocol.Code), true
	}

	return 0, "", false
}

func classifyProtocolError(code string, retryable bool) int {
	switch strings.ToLower(code) {
	case "invalid_request":
		return http.StatusBadRequest
	case "auth_required":
		return http.StatusUnauthorized
	case "schema_mismatch":
		return http.StatusUnprocessableEntity
	case "upgrade_required":
		return http.StatusUpgradeRequired
	case "retry_later":
		return http.StatusTooManyRequests
	case "temporary_unavailable":
		return http.StatusServiceUnavailable
	default:
		if retryable {
			return http.StatusServiceUnavailable
		}
		return http.StatusInternalServerError
	}
}

func retryAfterForCode(code string) string {
	switch strings.ToLower(code) {
	case "retry_later", "temporary_unavailable":
		return "5"
	default:
		return ""
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
