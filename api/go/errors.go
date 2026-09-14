package synchroapi

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"strings"
)

type protocolErrorEnvelope struct {
	Error protocolError `json:"error"`
}

type protocolError struct {
	Code      string `json:"code"`
	Message   string `json:"message"`
	Retryable bool   `json:"retryable"`
}

var errAdapterQueryTimeout = errors.New("adapter database query timeout")

// mapPGError inspects the raw JSONB response from a synchro_*() call.
// If the response contains an "error" key, it writes the appropriate HTTP
// status and returns true. Otherwise it returns false and the caller should
// forward the raw JSONB as a success response.
func mapPGError(w http.ResponseWriter, raw []byte) bool {
	var probe struct {
		Error json.RawMessage `json:"error"`
	}
	if err := json.Unmarshal(raw, &probe); err != nil || len(probe.Error) == 0 {
		return false
	}

	status, retryAfter, ok := classifyPGError(probe.Error)
	if !ok {
		writeJSONError(w, http.StatusInternalServerError, "sync operation failed")
		return true
	}
	w.Header().Set("Content-Type", "application/json")
	if retryAfter != "" {
		w.Header().Set("Retry-After", retryAfter)
	}
	writeRawJSON(w, status, raw)
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
	case "invalid_request", "invalid_schema_reference":
		return http.StatusBadRequest
	case "auth_required":
		return http.StatusUnauthorized
	case "idempotency_conflict", "client_retired", "client_generation_expired", "rebuild_restart_required":
		return http.StatusConflict
	case "schema_mismatch":
		return http.StatusUnprocessableEntity
	case "upgrade_required":
		return http.StatusUpgradeRequired
	case "retry_later":
		return http.StatusTooManyRequests
	case "capture_pending", "temporary_unavailable":
		return http.StatusServiceUnavailable
	case "sync_integrity_failure":
		return http.StatusInternalServerError
	default:
		if retryable {
			return http.StatusServiceUnavailable
		}
		return http.StatusInternalServerError
	}
}

func retryAfterForCode(code string) string {
	switch strings.ToLower(code) {
	case "retry_later", "capture_pending", "temporary_unavailable":
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

	if isTransientError(err) {
		w.Header().Set("Retry-After", "5")
		writeProtocolError(w, http.StatusServiceUnavailable, "temporary_unavailable", "service temporarily unavailable", true)
		return true
	}
	writeProtocolError(w, http.StatusInternalServerError, "sync_integrity_failure", "sync operation failed", false)
	return true
}

func isTransientError(err error) bool {
	if errors.Is(err, errAdapterQueryTimeout) ||
		errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, sql.ErrConnDone) ||
		errors.Is(err, driver.ErrBadConn) {
		return true
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, sql.ErrNoRows) || errors.Is(err, sql.ErrTxDone) {
		return false
	}

	var networkError net.Error
	if errors.As(err, &networkError) && (networkError.Timeout() || networkError.Temporary()) {
		return true
	}

	var stateError interface{ SQLState() string }
	if !errors.As(err, &stateError) {
		return false
	}
	state := stateError.SQLState()
	if len(state) != 5 {
		return false
	}
	switch state {
	case "08000", "08001", "08003", "08004", "08006", "08007", "08P01",
		"40001", "40P01", "53300", "55P03", "57P01", "57P02", "57P03":
		return true
	default:
		return false
	}
}

func writeJSONError(w http.ResponseWriter, status int, msg string) {
	code := "invalid_request"
	retryable := false
	switch status {
	case http.StatusUnauthorized:
		code = "auth_required"
	case http.StatusUpgradeRequired:
		code = "upgrade_required"
	case http.StatusServiceUnavailable:
		code = "temporary_unavailable"
		retryable = true
	case http.StatusInternalServerError:
		code = "sync_integrity_failure"
	}
	writeProtocolError(w, status, code, msg, retryable)
}

func writeProtocolError(w http.ResponseWriter, status int, code, message string, retryable bool) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(protocolErrorEnvelope{Error: protocolError{
		Code:      code,
		Message:   message,
		Retryable: retryable,
	}})
}
