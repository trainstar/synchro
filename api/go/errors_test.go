package synchroapi

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestMapPGErrorUsesProtocolThreeStatusDispatch(t *testing.T) {
	tests := []struct {
		code       string
		retryable  bool
		status     int
		retryAfter string
	}{
		{code: "invalid_schema_reference", status: http.StatusBadRequest},
		{code: "idempotency_conflict", status: http.StatusConflict},
		{code: "client_retired", status: http.StatusConflict},
		{code: "client_generation_expired", status: http.StatusConflict},
		{code: "rebuild_restart_required", status: http.StatusConflict},
		{code: "capture_pending", retryable: true, status: http.StatusServiceUnavailable, retryAfter: "5"},
		{code: "sync_integrity_failure", status: http.StatusInternalServerError},
	}

	for _, tt := range tests {
		t.Run(tt.code, func(t *testing.T) {
			raw, err := json.Marshal(protocolErrorEnvelope{Error: protocolError{
				Code:      tt.code,
				Message:   "bounded error",
				Retryable: tt.retryable,
			}})
			if err != nil {
				t.Fatalf("marshal protocol error: %v", err)
			}
			response := httptest.NewRecorder()
			if !mapPGError(response, append([]byte(" \n"), raw...)) {
				t.Fatal("protocol error was not mapped")
			}
			if response.Code != tt.status {
				t.Fatalf("status = %d, want %d", response.Code, tt.status)
			}
			if got := response.Header().Get("Retry-After"); got != tt.retryAfter {
				t.Fatalf("Retry-After = %q, want %q", got, tt.retryAfter)
			}
			if got := response.Body.Bytes(); string(got) != " \n"+string(raw) {
				t.Fatalf("response changed canonical extension body: %q", got)
			}
		})
	}
}

func TestMapPGErrorFailsClosedOnUnclassifiableEnvelope(t *testing.T) {
	const canary = "private extension error detail"
	response := httptest.NewRecorder()
	raw := []byte(`{"error":{"message":"` + canary + `","retryable":false}}`)

	if !mapPGError(response, raw) {
		t.Fatal("unclassifiable extension error was not mapped")
	}
	if response.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusInternalServerError)
	}
	var envelope protocolErrorEnvelope
	if err := json.Unmarshal(response.Body.Bytes(), &envelope); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if envelope.Error != (protocolError{
		Code:      "sync_integrity_failure",
		Message:   "sync operation failed",
		Retryable: false,
	}) {
		t.Fatalf("error = %#v, want bounded sync_integrity_failure", envelope.Error)
	}
	if strings.Contains(response.Body.String(), canary) {
		t.Fatal("public error contains extension error details")
	}
}

func TestMapSQLErrorDoesNotInferServerSemantics(t *testing.T) {
	const canary = "secret relation value"
	response := httptest.NewRecorder()
	mapSQLError(response, errors.New("schema mismatch for "+canary))

	if response.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusInternalServerError)
	}
	var envelope protocolErrorEnvelope
	if err := json.Unmarshal(response.Body.Bytes(), &envelope); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if envelope.Error.Code != "sync_integrity_failure" {
		t.Fatalf("code = %q, want sync_integrity_failure", envelope.Error.Code)
	}
	if envelope.Error.Message != "sync operation failed" {
		t.Fatalf("message = %q, want bounded generic message", envelope.Error.Message)
	}
	if strings.Contains(response.Body.String(), canary) {
		t.Fatal("public error contains database error details")
	}
	if response.Body.Len() > 256 {
		t.Fatalf("public error length = %d, want at most 256", response.Body.Len())
	}
}

func TestMapTransientSQLErrorIsBoundedAndRedacted(t *testing.T) {
	const canary = "private connection target"
	response := httptest.NewRecorder()
	mapSQLError(response, fmt.Errorf("query failed for %s: %w", canary, driver.ErrBadConn))

	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusServiceUnavailable)
	}
	if response.Header().Get("Retry-After") != "5" {
		t.Fatalf("Retry-After = %q, want 5", response.Header().Get("Retry-After"))
	}
	var envelope protocolErrorEnvelope
	if err := json.Unmarshal(response.Body.Bytes(), &envelope); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if envelope.Error != (protocolError{
		Code:      "temporary_unavailable",
		Message:   "service temporarily unavailable",
		Retryable: true,
	}) {
		t.Fatalf("error = %#v, want bounded temporary_unavailable", envelope.Error)
	}
	if strings.Contains(response.Body.String(), canary) {
		t.Fatal("public error contains database error details")
	}
	if response.Body.Len() > 256 {
		t.Fatalf("public error length = %d, want at most 256", response.Body.Len())
	}
}

type testSQLStateError string

func (e testSQLStateError) Error() string {
	return "database operation failed"
}

func (e testSQLStateError) SQLState() string {
	return string(e)
}

type nonRetryableNetworkError struct{}

func (nonRetryableNetworkError) Error() string   { return "network error" }
func (nonRetryableNetworkError) Timeout() bool   { return false }
func (nonRetryableNetworkError) Temporary() bool { return false }

func TestTransientErrorUsesTypedClassification(t *testing.T) {
	tests := []struct {
		name      string
		err       error
		transient bool
	}{
		{name: "context deadline", err: context.DeadlineExceeded, transient: true},
		{name: "adapter query timeout with query canceled SQLSTATE", err: fmt.Errorf("%w: %w", errAdapterQueryTimeout, testSQLStateError("57014")), transient: true},
		{name: "context canceled", err: context.Canceled, transient: false},
		{name: "sql connection closed", err: sql.ErrConnDone, transient: true},
		{name: "driver bad connection wrapped", err: fmt.Errorf("query: %w", driver.ErrBadConn), transient: true},
		{name: "temporary network", err: &net.DNSError{IsTemporary: true}, transient: true},
		{name: "timeout network", err: &net.DNSError{IsTimeout: true}, transient: true},
		{name: "non-retryable network", err: nonRetryableNetworkError{}, transient: false},
		{name: "exact connection SQLSTATE", err: testSQLStateError("08006"), transient: true},
		{name: "exact transaction SQLSTATE", err: testSQLStateError("40001"), transient: true},
		{name: "unknown connection SQLSTATE", err: testSQLStateError("08002"), transient: false},
		{name: "unknown transaction SQLSTATE", err: testSQLStateError("40003"), transient: false},
		{name: "deadlock SQLSTATE", err: testSQLStateError("40P01"), transient: true},
		{name: "too many connections", err: testSQLStateError("53300"), transient: true},
		{name: "lock unavailable", err: testSQLStateError("55P03"), transient: true},
		{name: "admin shutdown", err: testSQLStateError("57P01"), transient: true},
		{name: "no rows", err: sql.ErrNoRows, transient: false},
		{name: "transaction done", err: sql.ErrTxDone, transient: false},
		{name: "unique violation", err: testSQLStateError("23505"), transient: false},
		{name: "query canceled SQLSTATE", err: testSQLStateError("57014"), transient: false},
		{name: "malformed SQLSTATE", err: testSQLStateError("08"), transient: false},
		{name: "connection text", err: errors.New("connection timeout: database is closed"), transient: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isTransientError(tt.err); got != tt.transient {
				t.Fatalf("isTransientError() = %v, want %v", got, tt.transient)
			}
		})
	}
}

type adapterTimeoutDriver struct{}

var adapterTimeoutDriverSequence atomic.Uint64

func (adapterTimeoutDriver) Open(string) (driver.Conn, error) {
	return adapterTimeoutConn{}, nil
}

type adapterTimeoutConn struct{}

func (adapterTimeoutConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("prepare is not supported")
}

func (adapterTimeoutConn) Close() error {
	return nil
}

func (adapterTimeoutConn) Begin() (driver.Tx, error) {
	return nil, errors.New("transactions are not supported")
}

func (adapterTimeoutConn) QueryContext(ctx context.Context, _ string, _ []driver.NamedValue) (driver.Rows, error) {
	<-ctx.Done()
	return nil, testSQLStateError("57014")
}

func TestAdapterDatabaseQueryTimeoutReturnsRetryableTemporaryUnavailable(t *testing.T) {
	driverName := fmt.Sprintf("synchro-adapter-timeout-%d", adapterTimeoutDriverSequence.Add(1))
	sql.Register(driverName, adapterTimeoutDriver{})
	db, err := sql.Open(driverName, "")
	if err != nil {
		t.Fatalf("open timeout database: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	handler := Routes(Config{
		DB:                   db,
		JWTSecret:            []byte("unused-secret"),
		DatabaseQueryTimeout: 10 * time.Millisecond,
	})
	for _, test := range []struct {
		path     string
		body     string
		response protocolError
	}{
		{
			path: "/sync/schema",
			response: protocolError{
				Code:      "temporary_unavailable",
				Message:   "service temporarily unavailable",
				Retryable: true,
			},
		},
		{path: "/ready", body: `{"ready":false}`},
	} {
		t.Run(test.path, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodGet, test.path, nil)
			response := httptest.NewRecorder()
			started := time.Now()
			handler.ServeHTTP(response, request)
			if elapsed := time.Since(started); elapsed > time.Second {
				t.Fatalf("query timeout took %s, want less than one second", elapsed)
			}
			if response.Code != http.StatusServiceUnavailable {
				t.Fatalf("status = %d, want %d", response.Code, http.StatusServiceUnavailable)
			}
			if test.body != "" {
				if response.Body.String() != test.body {
					t.Fatalf("body = %q, want %q", response.Body.String(), test.body)
				}
				return
			}
			var envelope protocolErrorEnvelope
			if err := json.Unmarshal(response.Body.Bytes(), &envelope); err != nil {
				t.Fatalf("decode timeout response: %v", err)
			}
			if envelope.Error != test.response {
				t.Fatalf("timeout response = %#v", envelope.Error)
			}
		})
	}
}

func TestMapSQLErrorUsesExactTypedStatus(t *testing.T) {
	tests := []struct {
		name   string
		err    error
		status int
	}{
		{name: "transient", err: testSQLStateError("40001"), status: http.StatusServiceUnavailable},
		{name: "permanent", err: testSQLStateError("23505"), status: http.StatusInternalServerError},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			response := httptest.NewRecorder()
			if !mapSQLError(response, tt.err) {
				t.Fatal("mapSQLError() = false")
			}
			if response.Code != tt.status {
				t.Fatalf("status = %d, want %d", response.Code, tt.status)
			}
		})
	}
}
